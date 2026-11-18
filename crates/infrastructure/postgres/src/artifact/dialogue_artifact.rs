//! Account reads are rooted in a published entry, never in tenant coincidence or
//! membership of the private execution conversation.
use super::*;
use musteragent_identity::{AgentDialogueId, DialogueEntryId, PrincipalKind};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::{
    AgentArtifactAuthor, AgentArtifactContent, AgentArtifactDescriptor, AgentArtifactEditing,
    AgentArtifactHistoryQuery, AgentArtifactOrigin, AgentArtifactReadPort, AgentArtifactSummary,
    AgentArtifactVersion, AgentArtifactVersionPage,
};
use sqlx::PgConnection;

pub(crate) struct Binding {
    pub(crate) artifact: Artifact,
    pub(crate) delivered: ArtifactVersionId,
    /// Cross-partner return grants only the version on the original accepted entry.
    pub(crate) returned_by: Option<musteragent_identity::AgentId>,
    pub(crate) agent_id: musteragent_identity::AgentId,
}

pub(crate) async fn bind(
    db: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<Binding, PostgresArtifactError> {
    bind_with_custody(db, actor, dialogue, entry, false).await
}

/// Sharing is account custody, not permission to execute an archived Agent.
/// The same public-entry lineage is used; only that lifecycle policy differs.
pub(super) async fn bind_with_custody(
    db: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    allow_archived: bool,
) -> Result<Binding, PostgresArtifactError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(PostgresArtifactError::NotFound);
    }
    let row = sqlx::query(
        "SELECT a.artifact_id,a.tenant_id,a.conversation_id,a.workspace_id,a.activity_id,
          a.created_by_run_id,a.origin,a.kind,a.lifecycle,a.current_version::text AS current_version,
          COALESCE((SELECT n.name FROM resource.nodes n
             WHERE n.tenant_id=a.tenant_id AND n.file_artifact_id=a.artifact_id
               AND n.scope_kind=4 AND n.scope_id=t.agent_id AND n.kind=2 AND n.lifecycle=1
               AND n.name_erasure_job_id IS NULL),
             CASE WHEN a.title_erasure_job_id IS NULL THEN a.title ELSE 'Untitled' END) AS title,
          a.created_at_ms,a.updated_at_ms,e.artifact_version_id AS delivered,t.agent_id
         FROM agent.agent_dialogue_entries e
         JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id
           AND p.owner_id=e.owner_id AND p.dialogue_id=e.dialogue_id AND p.agent_id=e.author_agent_id
         JOIN agent.dialogue_sessions t ON t.conversation_id=p.source_conversation_id
           AND t.tenant_id=p.tenant_id AND t.owner_id=p.owner_id AND t.agent_id=p.agent_id
           AND t.dialogue_id=p.dialogue_id AND t.source_intent_id=p.source_intent_id
         JOIN runtime.runs r ON r.run_id=p.source_run_id AND r.tenant_id=t.tenant_id
           AND r.conversation_id=t.conversation_id
         JOIN conversation.conversations c ON c.conversation_id=t.conversation_id
           AND c.tenant_id=t.tenant_id AND c.created_by=t.owner_id AND c.kind=4
           AND c.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report') AND c.workspace_id IS NULL
         JOIN agent.owned_agents agent ON agent.agent_id=t.agent_id
           AND agent.dialogue_id=t.dialogue_id
         JOIN agent.owner_spaces s ON s.space_id=agent.space_id
           AND s.tenant_id=t.tenant_id AND s.owner_id=t.owner_id
         JOIN artifact.artifacts a ON a.artifact_id=e.artifact_id AND a.tenant_id=t.tenant_id
           AND resource.agent_artifact_owned(t.tenant_id,t.agent_id,a.artifact_id)
         JOIN artifact.artifact_versions delivered ON delivered.artifact_id=a.artifact_id
           AND delivered.artifact_version_id=e.artifact_version_id AND delivered.version<=a.current_version
         WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4
           AND p.source_kind<>'task_result_owner' AND e.kind='artifact' AND (agent.status='active' OR ($5 AND agent.status='archived')) AND c.lifecycle=1 AND a.lifecycle IN (1,2)",
    )
    .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
    .bind(dialogue.into_uuid()).bind(entry.into_uuid()).bind(allow_archived)
    .fetch_optional(&mut *db).await?;
    let Some(row) = row else {
        if let Some(binding) =
            super::task_result::bind_return(db, actor, dialogue, entry, allow_archived).await?
        {
            return Ok(binding);
        }

        if let Some(binding) =
            super::dialogue_device::bind(db, actor, dialogue, entry, allow_archived).await?
        {
            return Ok(binding);
        }
        return super::account::retained_binding(db, actor, dialogue, entry).await;
    };
    Ok(Binding {
        artifact: decode_artifact(&row)?,
        returned_by: None,
        delivered: id(row.try_get("delivered")?)?,
        agent_id: musteragent_identity::AgentId::from_uuid(row.try_get("agent_id")?)
            .map_err(|_| PostgresArtifactError::Corrupt)?,
    })
}

// This is the original File Owner's closed authoring registry. Merely storing
// UTF-8 bytes (notably SVG) does not grant the product a document-history UI.
pub(crate) fn history_supported(
    origin: &ArtifactOrigin,
    kind: ArtifactKind,
    media_type: &MediaType,
) -> bool {
    if matches!(origin, ArtifactOrigin::GovernedImport) || kind != ArtifactKind::File {
        return false;
    }
    let capability = musteragent_artifact::file_capability(media_type);
    capability.supports_direct_text()
        && !matches!(capability.preview, musteragent_artifact::FilePreview::Image)
}

async fn version_roots(
    db: &mut PgConnection,
    binding: &Binding,
    actor: Actor,
    at: VersionNumber,
    before: Option<VersionNumber>,
    exact: Option<ArtifactVersionId>,
    limit: u32,
) -> Result<
    (
        Vec<ArtifactVersion>,
        BTreeMap<ArtifactVersionId, AgentArtifactAuthor>,
        std::collections::BTreeSet<ArtifactVersionId>,
        BTreeMap<ArtifactVersionId, ArtifactVersionId>,
    ),
    PostgresArtifactError,
> {
    let exact = match (binding.returned_by, exact) {
        (Some(_), Some(id)) if id != binding.delivered => {
            return Err(PostgresArtifactError::NotFound);
        }
        (Some(_), _) => Some(binding.delivered),
        (None, requested) => requested,
    };
    let authorizing_agent = binding.returned_by.unwrap_or(binding.agent_id);
    let rows=sqlx::query(
        "SELECT v.artifact_version_id,v.artifact_id,v.version::text AS version,v.content_id,
           v.content_hash,v.restored_from_version_id,v.source_invocation_id,v.committed_by,v.committed_by_principal_id,v.committed_at_ms,
           content.media_type,content.byte_length::text AS byte_length,content.content_hash AS ref_hash,
           v.work_item_erasure_job_id IS NOT NULL AS content_erased,
           author.kind AS author_kind,author.principal_ref,
           COALESCE(author.agent_ref,device.agent_id,canonical_command.agent_id,task_device.producer_id) AS agent_ref,
           COALESCE(config.display_name,device_config.display_name,canonical_config.display_name,task_device_config.display_name) AS display_name,
           (device.artifact_version_id IS NOT NULL OR canonical.artifact_version_id IS NOT NULL OR task_device.version_id IS NOT NULL) AS device_authored
         FROM artifact.artifact_versions v JOIN artifact.artifacts a ON a.artifact_id=v.artifact_id
         JOIN artifact.content_refs content ON content.tenant_id=a.tenant_id AND content.content_id=v.content_id AND (content.state=2 OR v.work_item_erasure_job_id IS NOT NULL)
         LEFT JOIN artifact.device_versions device ON device.artifact_version_id=v.artifact_version_id
           AND device.artifact_id=a.artifact_id AND device.tenant_id=a.tenant_id AND device.owner_id=$2 AND device.agent_id=$7
           AND v.source_invocation_id IS NULL AND v.committed_by= device.source_participant_id
           AND artifact.device_version_owned(a.tenant_id,$2,$7,a.artifact_id,v.artifact_version_id)
         LEFT JOIN agent.owned_agent_configurations device_config ON device_config.agent_id=device.agent_id
         LEFT JOIN artifact.device_resource_versions canonical ON canonical.artifact_version_id=v.artifact_version_id AND canonical.artifact_id=a.artifact_id AND canonical.source_participant_id=v.committed_by AND v.source_invocation_id IS NULL AND artifact.device_resource_owned(a.tenant_id,$2,$7,a.artifact_id,v.artifact_version_id)
         LEFT JOIN resource.device_commands canonical_command ON canonical_command.invocation_id=canonical.invocation_id
         LEFT JOIN agent.owned_agent_configurations canonical_config ON canonical_config.agent_id=canonical_command.agent_id
         LEFT JOIN LATERAL (SELECT ret.producer_id,ret.version_id FROM artifact.work_item_result_asset_returns ret
           WHERE ret.version_id=v.artifact_version_id AND ret.artifact_id=a.artifact_id AND ret.tenant_id=a.tenant_id AND ret.owner_id=$2 AND ret.producer_id=$7
             AND artifact.verified_task_native_version_owned(ret.tenant_id,ret.owner_id,ret.producer_id,ret.artifact_id,ret.version_id) LIMIT 1) task_device ON true
         LEFT JOIN agent.owned_agent_configurations task_device_config ON task_device_config.agent_id=task_device.producer_id
         LEFT JOIN conversation.participants author ON author.tenant_id=a.tenant_id
           AND author.participant_id=v.committed_by
           AND EXISTS(SELECT 1 FROM agent.dialogue_sessions authored WHERE authored.tenant_id=author.tenant_id AND authored.conversation_id=author.conversation_id AND authored.agent_id=$7 AND authored.owner_id=$2)
         LEFT JOIN agent.owned_agents agent ON agent.agent_id=author.agent_ref
           AND EXISTS(SELECT 1 FROM agent.owner_spaces s WHERE s.space_id=agent.space_id AND s.tenant_id=a.tenant_id AND s.owner_id=$2)
         LEFT JOIN agent.owned_agent_configurations config ON config.agent_id=agent.agent_id
         WHERE v.artifact_id=$1 AND v.version<=$3::numeric
           AND ($4::numeric IS NULL OR v.version<$4::numeric)
           AND ($5::uuid IS NULL OR v.artifact_version_id=$5)
           AND (a.origin<>1 OR device.artifact_version_id IS NOT NULL OR canonical.artifact_version_id IS NOT NULL OR task_device.version_id IS NOT NULL OR (v.source_invocation_id IS NULL AND v.committed_by_principal_id=$2) OR (v.source_invocation_id IS NULL AND author.kind=1 AND author.principal_ref=$2) OR EXISTS (SELECT 1 FROM tool.tool_invocations tool
             WHERE tool.invocation_id=v.source_invocation_id AND tool.tenant_id=a.tenant_id
               AND tool.conversation_id=author.conversation_id AND tool.status=4))
         ORDER BY v.version DESC LIMIT $6",
    ).bind(binding.artifact.artifact_id.into_uuid()).bind(actor.principal_id.into_uuid())
      .bind(at.get().to_string()).bind(before.map(|v|v.get().to_string()))
      .bind(exact.map(ArtifactVersionId::into_uuid)).bind(i64::from(limit)).bind(authorizing_agent.into_uuid())
      .fetch_all(&mut *db).await?;
    let mut restored = BTreeMap::new();
    let mut authors = BTreeMap::new();
    let mut erased = std::collections::BTreeSet::new();
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        let author = if row.try_get::<bool, _>("device_authored")? {
            AgentArtifactAuthor::Agent {
                agent_id: musteragent_identity::AgentId::from_uuid(
                    row.try_get::<Option<uuid::Uuid>, _>("agent_ref")?
                        .ok_or(PostgresArtifactError::Corrupt)?,
                )
                .map_err(|_| PostgresArtifactError::Corrupt)?,
                display_name: row
                    .try_get::<Option<String>, _>("display_name")?
                    .ok_or(PostgresArtifactError::Corrupt)?,
            }
        } else if row.try_get::<Option<uuid::Uuid>, _>("committed_by_principal_id")?
            == Some(actor.principal_id.into_uuid())
            && row
                .try_get::<Option<uuid::Uuid>, _>("source_invocation_id")?
                .is_none()
        {
            AgentArtifactAuthor::AccountOwner
        } else {
            match row.try_get::<Option<i16>, _>("author_kind")? {
                Some(1)
                    if row.try_get::<Option<uuid::Uuid>, _>("principal_ref")?
                        == Some(actor.principal_id.into_uuid()) =>
                {
                    AgentArtifactAuthor::AccountOwner
                }
                Some(2) => AgentArtifactAuthor::Agent {
                    agent_id: musteragent_identity::AgentId::from_uuid(
                        row.try_get::<Option<uuid::Uuid>, _>("agent_ref")?
                            .ok_or(PostgresArtifactError::Corrupt)?,
                    )
                    .map_err(|_| PostgresArtifactError::Corrupt)?,
                    display_name: row
                        .try_get::<Option<String>, _>("display_name")?
                        .ok_or(PostgresArtifactError::Corrupt)?,
                },
                _ => return Err(PostgresArtifactError::Corrupt),
            }
        };
        let content_erased: bool = row.try_get("content_erased")?;
        let restored_from = row.try_get::<Option<uuid::Uuid>, _>("restored_from_version_id")?;
        let version = decode_version(row)?;
        if let Some(source) = restored_from {
            restored.insert(version.artifact_version_id, id(source)?);
        }
        if content_erased {
            erased.insert(version.artifact_version_id);
        }
        authors.insert(version.artifact_version_id, author);
        decoded.push(version);
    }
    Ok((decoded, authors, erased, restored))
}

pub(super) async fn versions(
    db: &mut PgConnection,
    binding: &Binding,
    actor: Actor,
    at: VersionNumber,
    before: Option<VersionNumber>,
    exact: Option<ArtifactVersionId>,
    limit: u32,
) -> Result<Vec<AgentArtifactVersion>, PostgresArtifactError> {
    let (decoded, mut authors, erased, mut restored) =
        version_roots(db, binding, actor, at, before, exact, limit).await?;
    let available: Vec<_> = decoded
        .iter()
        .filter(|v| !erased.contains(&v.artifact_version_id))
        .cloned()
        .collect();
    let mut ready: BTreeMap<_, _> = load_version_dependencies(&mut *db, available)
        .await?
        .into_iter()
        .map(|v| (v.artifact_version_id, v))
        .collect();
    decoded
        .into_iter()
        .map(|v| {
            let content = if erased.contains(&v.artifact_version_id) {
                AgentArtifactContent::Erased {
                    media_type: v.content_ref.media_type,
                }
            } else {
                let value = ready
                    .remove(&v.artifact_version_id)
                    .ok_or(PostgresArtifactError::Corrupt)?;
                AgentArtifactContent::Ready {
                    root: value.content_ref,
                    dependencies: value.dependency_refs,
                }
            };
            Ok(AgentArtifactVersion {
                artifact_version_id: v.artifact_version_id,
                version: v.version,
                content,
                committed_at: v.committed_at,
                restored_from_version_id: restored.remove(&v.artifact_version_id),
                author: authors
                    .remove(&v.artifact_version_id)
                    .ok_or(PostgresArtifactError::Corrupt)?,
            })
        })
        .collect()
}

/// Shared account-read projection: directory and message entry reads use the
/// same authorization and head/version snapshot in their caller's transaction.
pub(super) async fn descriptor_in_transaction(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<AgentArtifactDescriptor, PostgresArtifactError> {
    let binding = bind(tx, actor, dialogue, entry).await?;
    let current = versions(
        tx,
        &binding,
        actor,
        binding.artifact.current_version,
        None,
        None,
        1,
    )
    .await?
    .pop()
    .filter(|v| v.version == binding.artifact.current_version)
    .ok_or(PostgresArtifactError::Corrupt)?;
    let supports_text = history_supported(
        &binding.artifact.origin,
        binding.artifact.kind,
        current.content.media_type(),
    );
    let editable = binding.returned_by.is_none() && supports_text && !current.content.is_erased() && sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=4 AND scope_id=$2 AND file_artifact_id=$3 AND kind=2 AND lifecycle=1)")
        .bind(actor.tenant_id.into_uuid()).bind(binding.agent_id.into_uuid()).bind(binding.artifact.artifact_id.into_uuid()).fetch_one(&mut *tx).await?;
    let max_utf8_bytes = musteragent_artifact::file_capability(current.content.media_type())
        .maximum_bytes
        .min(10 * 1024 * 1024);
    let within_budget = matches!(&current.content, AgentArtifactContent::Ready { root, .. } if root.byte_length <= max_utf8_bytes);
    let editing = if editable {
        AgentArtifactEditing {
            save_text: within_budget,
            restore: within_budget,
            max_utf8_bytes,
        }
    } else {
        AgentArtifactEditing::default()
    };
    Ok(AgentArtifactDescriptor {
        artifact_id: binding.artifact.artifact_id,
        kind: binding.artifact.kind,
        title: binding.artifact.title,
        lifecycle: binding.artifact.lifecycle,
        origin: match binding.artifact.origin {
            ArtifactOrigin::Runtime { .. } => AgentArtifactOrigin::Agent,
            ArtifactOrigin::Product => AgentArtifactOrigin::AccountOwner,
            ArtifactOrigin::GovernedImport => AgentArtifactOrigin::Imported,
        },
        history_supported: binding.returned_by.is_none()
            && history_supported(
                &binding.artifact.origin,
                binding.artifact.kind,
                current.content.media_type(),
            ),
        current_version: current,
        editing,
        delivered_version_id: binding.delivered,
    })
}

/// Immutable bytes delivered on this exact public entry. File discovery must
/// not join a frozen history page to a later mutable Artifact head.
pub(crate) async fn delivered_media_in_transaction(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<(String, ArtifactKind, AgentArtifactVersion), PostgresArtifactError> {
    let binding = bind(tx, actor, dialogue, entry).await?;
    let version = versions(
        tx,
        &binding,
        actor,
        binding.artifact.current_version,
        None,
        Some(binding.delivered),
        1,
    )
    .await?
    .pop()
    .ok_or(PostgresArtifactError::NotFound)?;
    if version.artifact_version_id != binding.delivered {
        return Err(PostgresArtifactError::Corrupt);
    }
    Ok((binding.artifact.title, binding.artifact.kind, version))
}

/// The directory reads the same authorized root/version provenance but never
/// loads the dependency closure, keeping metadata cost independent of file size.
pub(super) async fn summary_in_transaction(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
) -> Result<AgentArtifactSummary, PostgresArtifactError> {
    let binding = bind(tx, actor, dialogue, entry).await?;
    let (mut roots, _, erased, _) = version_roots(
        tx,
        &binding,
        actor,
        binding.artifact.current_version,
        None,
        None,
        1,
    )
    .await?;
    let current = roots
        .pop()
        .filter(|v| v.version == binding.artifact.current_version)
        .ok_or(PostgresArtifactError::Corrupt)?;
    Ok(AgentArtifactSummary {
        artifact_id: binding.artifact.artifact_id,
        kind: binding.artifact.kind,
        title: binding.artifact.title,
        lifecycle: binding.artifact.lifecycle,
        origin: match binding.artifact.origin {
            ArtifactOrigin::Runtime { .. } => AgentArtifactOrigin::Agent,
            ArtifactOrigin::Product => AgentArtifactOrigin::AccountOwner,
            ArtifactOrigin::GovernedImport => AgentArtifactOrigin::Imported,
        },
        history_supported: binding.returned_by.is_none()
            && history_supported(
                &binding.artifact.origin,
                binding.artifact.kind,
                &current.content_ref.media_type,
            ),
        current_version_id: current.artifact_version_id,
        delivered_version_id: binding.delivered,
        media_type: current.content_ref.media_type.as_str().to_owned(),
        byte_length: if erased.contains(&current.artifact_version_id) {
            0
        } else {
            current.content_ref.byte_length
        },
        content_erased: erased.contains(&current.artifact_version_id),
    })
}

impl AgentArtifactReadPort for PostgresArtifactStore {
    fn describe(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactDescriptor, RuntimeError>> {
        Box::pin(async move {
            let operation = async {
                let mut tx = self
                    .ledger
                    .database()
                    .read_unit_of_work(
                        musteragent_telemetry::DbCommitKind::Resource,
                        crate::PostgresTransactionIsolation::RepeatableRead,
                    )
                    .await?;
                descriptor_in_transaction(&mut tx, actor, dialogue, entry).await
            };
            operation
                .await
                .map_err(|e| map_public_query_error(e, correlation))
        })
    }
    fn history(
        &self,
        query: AgentArtifactHistoryQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentArtifactVersionPage, RuntimeError>> {
        Box::pin(async move {
            if !(1..=50).contains(&query.limit) {
                return Err(runtime_error(
                    ErrorCode::InvalidCommand,
                    correlation,
                    false,
                    "History page size must be between 1 and 50",
                ));
            }
            let operation = async {
                let mut tx = self
                    .ledger
                    .database()
                    .read_unit_of_work(
                        musteragent_telemetry::DbCommitKind::Resource,
                        crate::PostgresTransactionIsolation::RepeatableRead,
                    )
                    .await?;
                let binding = bind(&mut tx, query.actor, query.dialogue_id, query.entry_id).await?;
                if binding.returned_by.is_some() {
                    return Err(PostgresArtifactError::NotFound);
                }
                let at = query.at_version.unwrap_or(binding.artifact.current_version);
                if at > binding.artifact.current_version
                    || query.before_version.is_some_and(|before| before > at)
                {
                    return Err(PostgresArtifactError::NotFound);
                }
                let current = versions(
                    &mut tx,
                    &binding,
                    query.actor,
                    binding.artifact.current_version,
                    None,
                    None,
                    1,
                )
                .await?
                .pop()
                .ok_or(PostgresArtifactError::Corrupt)?;
                if !history_supported(
                    &binding.artifact.origin,
                    binding.artifact.kind,
                    current.content.media_type(),
                ) {
                    return Err(PostgresArtifactError::NotFound);
                }
                let mut page = versions(
                    &mut tx,
                    &binding,
                    query.actor,
                    at,
                    query.before_version,
                    None,
                    query.limit + 1,
                )
                .await?;
                if page.iter().any(|v| {
                    !history_supported(
                        &binding.artifact.origin,
                        binding.artifact.kind,
                        v.content.media_type(),
                    )
                }) {
                    return Err(PostgresArtifactError::Corrupt);
                }
                let more = page.len() > query.limit as usize;
                page.truncate(query.limit as usize);
                let next_before_version = more.then(|| page.last().map(|v| v.version)).flatten();
                Ok(AgentArtifactVersionPage {
                    at_version: at,
                    versions: page,
                    next_before_version,
                })
            };
            operation
                .await
                .map_err(|e| map_public_query_error(e, correlation))
        })
    }
    fn authorize_content(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        version: ArtifactVersionId,
        reference: ContentRef,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<ContentRef, RuntimeError>> {
        Box::pin(async move {
            let operation = async {
                let mut tx = self
                    .ledger
                    .database()
                    .read_unit_of_work(
                        musteragent_telemetry::DbCommitKind::Resource,
                        crate::PostgresTransactionIsolation::RepeatableRead,
                    )
                    .await?;
                let binding = bind(&mut tx, actor, dialogue, entry).await?;
                let selected = versions(
                    &mut tx,
                    &binding,
                    actor,
                    binding.artifact.current_version,
                    None,
                    Some(version),
                    1,
                )
                .await?
                .pop()
                .ok_or(PostgresArtifactError::NotFound)?;
                let AgentArtifactContent::Ready { root, dependencies } = selected.content else {
                    return Err(PostgresArtifactError::NotFound);
                };
                if root != reference && !dependencies.contains(&reference) {
                    return Err(PostgresArtifactError::NotFound);
                }
                Ok(reference)
            };
            operation
                .await
                .map_err(|e| map_public_query_error(e, correlation))
        })
    }
}

#[cfg(test)]
mod history_policy_tests {
    use super::*;
    use musteragent_identity::ContentId;

    #[test]
    fn only_managed_supported_text_has_product_history() {
        let origin = ArtifactOrigin::Runtime {
            activity_id: ActivityId::from_uuid(uuid::Uuid::now_v7()).unwrap(),
            created_by_run_id: RunId::from_uuid(uuid::Uuid::now_v7()).unwrap(),
        };
        for (mime, supported) in [
            ("text/markdown", true),
            ("text/plain", true),
            ("application/json", true),
            ("text/csv", true),
            ("text/html", true),
            ("image/svg+xml", false),
            ("image/png", false),
            ("audio/mpeg", false),
            ("video/mp4", false),
            ("application/pdf", false),
            ("application/zip", false),
        ] {
            let reference = ContentRef {
                content_id: ContentId::from_uuid(uuid::Uuid::now_v7()).unwrap(),
                media_type: MediaType::new(mime).unwrap(),
                byte_length: 0,
                sha256: Sha256::digest(b""),
            };
            assert_eq!(
                history_supported(&origin, ArtifactKind::File, &reference.media_type),
                supported,
                "{mime}"
            );
            assert!(
                !history_supported(
                    &ArtifactOrigin::GovernedImport,
                    ArtifactKind::File,
                    &reference.media_type
                ),
                "imported {mime}"
            );
            assert!(
                !history_supported(&origin, ArtifactKind::Image, &reference.media_type),
                "image with {mime}"
            );
        }
    }
}
