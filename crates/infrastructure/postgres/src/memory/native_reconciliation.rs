//! Deliberate operator recovery, not a second execution or an online write API.
//! Only exact, previously approved Native facts can be recovered. Existing heads
//! require an explicit CAS; revoked/erased content can never be reactivated here.
use super::*;
use musteragent_identity::{Actor, ContentId, InteractionId, PrincipalKind};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMemoryReconciliation {
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub agent_id: Uuid,
    pub interaction_id: Uuid,
    pub memory_id: Uuid,
    pub memory_key: String,
    pub scope: String,
    pub kind: String,
    pub source: i16,
    pub source_version_id: Uuid,
    pub source_version: u64,
    pub confidence_bps: u16,
    pub source_created_at_ms: i64,
    pub content_id: Uuid,
    pub media_type: String,
    pub content_sha256: String,
    pub bytes: Vec<u8>,
    pub source_run_ids: Vec<Uuid>,
    pub evidence_record_ids: Vec<Uuid>,
    pub expected_head: Option<Uuid>,
}
impl PostgresMemoryEvolutionStore {
    /// Requires database maintenance authority. No HTTP/Tool surface exposes
    /// this importer; approval comes from the original signed Native projection.
    /// Dry runs perform the same scope/head/approval checks without uploading.
    pub async fn reconcile_native_memory(
        &self,
        content: &dyn ContentStore,
        source: &NativeMemoryReconciliation,
        apply: bool,
    ) -> Result<MemoryVersionId, PostgresMemoryEvolutionError> {
        let invalid = || PostgresMemoryEvolutionError::MemoryConflict;
        if source.bytes.len() > 64 * 1024
            || source.source_version == 0
            || !matches!(source.source, 1 | 2)
            || source.source_run_ids.len() > 100
            || source.evidence_record_ids.len() > 100
        {
            return Err(invalid());
        }
        let actor = Actor {
            tenant_id: id(source.tenant_id, "tenant")?,
            principal_id: id(source.principal_id, "principal")?,
            principal_kind: PrincipalKind::User,
        };
        let agent: AgentId = id(source.agent_id, "agent")?;
        let interaction: InteractionId = id(source.interaction_id, "interaction")?;
        let original_version: MemoryVersionId = id(source.source_version_id, "source version")?;
        let target = MemoryVersionId::from_uuid(Uuid::new_v5(
            &source.source_version_id,
            b"canonical-memory-reconciliation@1",
        ))
        .map_err(|_| invalid())?;
        let reference = ContentRef {
            content_id: ContentId::from_uuid(source.content_id).map_err(|_| invalid())?,
            media_type: MediaType::new(&source.media_type).map_err(|_| invalid())?,
            byte_length: source.bytes.len() as u64,
            sha256: Sha256::digest(&source.bytes),
        };
        verify_content(&reference, &source.bytes).map_err(|_| invalid())?;
        if reference.sha256.to_string() != source.content_sha256 {
            return Err(invalid());
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
            .await?;
        if !crate::memory_management_authority::authorize(&mut tx, actor, agent).await? {
            return Err(invalid());
        }
        let (approved_actor, approved_agent, projection) =
            crate::PostgresNativeDialogueInteractionStore::approved_memory_interaction(
                &mut tx,
                interaction,
            )
            .await
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        if approved_actor != actor
            || approved_agent != agent
            || projection
                .interaction
                .responded_at
                .is_none_or(|at| at.unix_millis() > source.source_created_at_ms)
        {
            return Err(invalid());
        }
        let envelope: serde_json::Value =
            serde_json::from_slice(&projection.prompt_bytes).map_err(|_| invalid())?;
        let detail = &envelope["capability_detail"]["content_json"];
        let body: serde_json::Value =
            serde_json::from_slice(&source.bytes).map_err(|_| invalid())?;
        let scope = match source.scope.as_str() {
            "account" => MemoryScope::User {
                principal_id: actor.principal_id,
            },
            "agent" => MemoryScope::Agent { agent_id: agent },
            _ => return Err(invalid()),
        };
        if source.source == 1 {
            if envelope["tool"] != "user_memory_remember"
                || reference.media_type.as_str() != "application/json"
                || detail["schema"] != "musteragent.user-memory.approval.v1"
                || source.scope != "account"
                || detail["scope"] != "account"
                || body["fact_key"] != detail["fact_key"]
                || body["content"] != detail["content"]
                || body.as_object().is_none_or(|v| v.len() != 2)
                || source.memory_key
                    != format!(
                        "user-memory:{}",
                        detail["fact_key"].as_str().ok_or_else(invalid)?
                    )
            {
                return Err(invalid());
            }
        } else if envelope["tool"] != "experience__publish__v1"
            || !musteragent_runtime_memory::is_experience_claim_media_type(
                reference.media_type.as_str(),
            )
            || detail["schema"] != "musteragent.agent-memory.publication-approval.v1"
            || detail["scope"] != source.scope
            || detail["scope_id"]
                != if source.scope == "agent" {
                    source.agent_id.to_string()
                } else {
                    source.principal_id.to_string()
                }
            || detail["claim"]["kind"] != source.kind
            || ["claim", "conditions", "outcome", "sensitivity"]
                .iter()
                .any(|key| detail["claim"][key] != body[key])
            || musteragent_runtime_memory::ExperienceClaimDocument::decode(&source.bytes).is_err()
            || detail["claim_ref"]["sha256"] != reference.sha256.to_string()
            || detail["claim_ref"]["content_id"] != source.content_id.to_string()
            || source.memory_key
                != format!(
                    "learned:{}",
                    detail["proposal_id"].as_str().ok_or_else(invalid)?
                )
        {
            return Err(invalid());
        }
        let erased:bool=sqlx::query_scalar("SELECT memory.search_content_is_erased($1,$2,$3) OR EXISTS(SELECT 1 FROM memory.search_erasure_tombstones WHERE tenant_id=$1 AND memory_id=$4)")
            .bind(source.tenant_id).bind(source.content_id).bind(reference.sha256.into_bytes().to_vec()).bind(source.memory_id).fetch_one(&mut *tx).await?;
        if erased {
            return Err(invalid());
        }
        let replay:Option<(Uuid,Uuid,Uuid,Vec<u8>)>=sqlx::query_as("SELECT canonical_version_id,memory_id,interaction_id,source_content_hash FROM memory.native_publication_reconciliations WHERE source_version_id=$1")
            .bind(source.source_version_id).fetch_optional(&mut *tx).await?;
        if let Some(row) = replay {
            if row
                != (
                    target.into_uuid(),
                    source.memory_id,
                    source.interaction_id,
                    reference.sha256.into_bytes().to_vec(),
                )
            {
                return Err(invalid());
            }
            return Ok(target);
        }
        let previous=sqlx::query("SELECT m.current_version_id,m.lifecycle,m.memory_key,m.scope_kind,m.scope_id,m.created_at_ms,v.version::text AS version,v.valid_from_ms FROM memory.memories m JOIN memory.memory_versions v ON v.memory_version_id=m.current_version_id WHERE m.memory_id=$1 AND m.tenant_id=$2 FOR UPDATE OF m")
            .bind(source.memory_id).bind(source.tenant_id).fetch_optional(&mut *tx).await?;
        let (ordinal, created_at) = if let Some(row) = previous {
            let (scope_kind, scope_id) = memory_scope(&scope);
            if source.expected_head != Some(row.try_get("current_version_id")?)
                || row.try_get::<i16, _>("lifecycle")? != 1
                || row.try_get::<String, _>("memory_key")? != source.memory_key
                || row.try_get::<i16, _>("scope_kind")? != scope_kind
                || row.try_get::<Option<Uuid>, _>("scope_id")? != scope_id
                || row.try_get::<i64, _>("valid_from_ms")? > source.source_created_at_ms
            {
                return Err(invalid());
            }
            (
                parse_u64(row.try_get("version")?, "memory version")?
                    .checked_add(1)
                    .ok_or_else(invalid)?,
                row.try_get("created_at_ms")?,
            )
        } else {
            if source.expected_head.is_some() {
                return Err(invalid());
            }
            (1, source.source_created_at_ms)
        };
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| invalid())?;
        let record = MemoryRecord {
            memory_id: id(source.memory_id, "memory")?,
            tenant_id: actor.tenant_id,
            scope,
            memory_key: IdempotencyKey::new(&source.memory_key).map_err(|_| invalid())?,
            kind: match source.kind.as_str() {
                "preference" => MemoryKind::Preference,
                "semantic" => MemoryKind::Semantic,
                "procedure" => MemoryKind::Procedure,
                "failure" => MemoryKind::FailureLesson,
                _ => return Err(invalid()),
            },
            lifecycle: MemoryLifecycle::Active,
            current_version_id: target,
            created_at: UtcTimestamp::from_unix_millis(created_at),
            updated_at: now,
            revoked_at: None,
        };
        let version = MemoryVersion {
            memory_id: record.memory_id,
            memory_version_id: target,
            version: VersionNumber::new(ordinal).map_err(|_| invalid())?,
            source: MemorySource::GovernedImport,
            content_ref: reference.clone(),
            confidence: MemoryConfidenceBps::new(source.confidence_bps)?,
            data_classification: DataClassification::Confidential,
            source_run_ids: source
                .source_run_ids
                .iter()
                .map(|v| id(*v, "source run"))
                .collect::<Result<_, _>>()?,
            evidence_record_ids: source
                .evidence_record_ids
                .iter()
                .map(|v| id(*v, "source evidence"))
                .collect::<Result<_, _>>()?,
            created_at: now,
            valid_until: None,
        };
        let search = MemorySearchDocument::from_content(&record, &version, &source.bytes)
            .map_err(|_| invalid())?;
        if !apply {
            return Ok(target);
        }
        let stored = content
            .put_at(
                reference.content_id,
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: reference.media_type.clone(),
                    bytes: source.bytes.clone(),
                },
            )
            .await
            .map_err(|_| invalid())?;
        if stored != reference {
            return Err(invalid());
        }
        persist_memory(&mut tx, &record, &version, &search).await?;
        sqlx::query("INSERT INTO memory.native_publication_reconciliations(source_version_id,memory_id,canonical_version_id,tenant_id,principal_id,interaction_id,source_kind,source_version,source_created_at_ms,source_content_id,source_content_hash,expected_head,reconciled_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
            .bind(original_version.into_uuid()).bind(source.memory_id).bind(target.into_uuid()).bind(source.tenant_id).bind(source.principal_id).bind(source.interaction_id)
            .bind(source.source).bind(i64::try_from(source.source_version).map_err(|_|invalid())?).bind(source.source_created_at_ms).bind(source.content_id).bind(reference.sha256.into_bytes().to_vec()).bind(source.expected_head).bind(now.unix_millis())
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(target)
    }
}
