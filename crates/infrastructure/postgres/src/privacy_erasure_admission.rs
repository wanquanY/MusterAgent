use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    Actor, ContentId, DottedKey, ErasureJobId, PrincipalKind, TenantId, UtcTimestamp,
};
use musteragent_runtime_privacy::{
    AgentMemoryErasureAdmission, AgentMemoryErasureAdmissionStore, AgentMemoryErasureLineage,
    ErasureAppendDisposition, ErasureJobStatus, ErasureJobStore, ErasureTransition,
    ErasureTransitionSequence, RequestAgentMemoryErasure, build_agent_memory_erasure_facts,
};
use sqlx::Row as _;

use crate::privacy_erasure::{insert_plan_exception, insert_root_resource, insert_transition};
use crate::{PostgresPrivacyErasureError, PostgresPrivacyErasureStore};

impl musteragent_runtime_privacy::AgentMemoryErasureQueryStore for PostgresPrivacyErasureStore {
    type Error = PostgresPrivacyErasureError;
    fn list_agent_memory_erasure_jobs(
        &self,
        query: musteragent_runtime_privacy::ListAgentMemoryErasureJobs,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<musteragent_runtime_privacy::AgentMemoryErasureJobPage, Self::Error>,
    > {
        Box::pin(async move {
            use musteragent_runtime_privacy::{
                AgentMemoryErasureJobPage, ErasureJobCursor, ManagedAgentMemoryErasureJob,
            };
            if query.limit.get() > 50 {
                return Err(PostgresPrivacyErasureError::CorruptState(
                    "invalid management page size",
                ));
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            if !crate::memory_management_authority::authorize(&mut tx, query.actor, query.agent_id)
                .await?
            {
                return Err(PostgresPrivacyErasureError::AgentMemoryUnauthorized);
            }
            let mut rows:Vec<(uuid::Uuid,i64)>=sqlx::query_as(
                "SELECT r.erasure_job_id,r.requested_at_ms FROM privacy_erasure.requests r
                 JOIN memory.agent_memory_erasure_lineage l ON l.tenant_id=r.tenant_id AND l.erasure_job_id=r.erasure_job_id
                 WHERE r.tenant_id=$1 AND l.agent_id=$2 AND r.actor_principal_id=$3 AND l.requested_by=$3
                   AND r.actor_principal_kind=1 AND l.requested_by_kind=1
                   AND ($4::bigint IS NULL OR (r.requested_at_ms,r.erasure_job_id)>($4,$5))
                 ORDER BY r.requested_at_ms,r.erasure_job_id LIMIT $6")
                .bind(query.actor.tenant_id.into_uuid()).bind(query.agent_id.into_uuid()).bind(query.actor.principal_id.into_uuid())
                .bind(query.after.map(|c|c.requested_at.unix_millis())).bind(query.after.map(|c|c.erasure_job_id.into_uuid()))
                .bind(i64::from(query.limit.get())+1).fetch_all(&mut *tx).await?;
            let more = rows.len() > usize::from(query.limit.get());
            rows.truncate(usize::from(query.limit.get()));
            let ids = rows.iter().map(|r| r.0).collect::<Vec<_>>();
            let mut lineages = load_lineages(&mut tx, query.actor.tenant_id, &ids).await?;
            tx.rollback().await?;
            let jobs = self.load_jobs(query.actor.tenant_id, &ids).await?;
            if jobs.len() != ids.len() || lineages.len() != ids.len() {
                return Err(PostgresPrivacyErasureError::CorruptState(
                    "erasure management page lost immutable facts",
                ));
            }
            let mut items = Vec::with_capacity(jobs.len());
            for job in jobs {
                let lineage = lineages
                    .remove(&job.request.erasure_job_id.into_uuid())
                    .ok_or(PostgresPrivacyErasureError::CorruptState(
                        "erasure lineage disappeared",
                    ))?;
                if job.request.requested_by != query.actor
                    || lineage.requested_by != query.actor
                    || lineage.agent_id != query.agent_id
                {
                    return Err(PostgresPrivacyErasureError::AgentMemoryUnauthorized);
                }
                items.push(ManagedAgentMemoryErasureJob { job, lineage });
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            if !crate::memory_management_authority::authorize(&mut tx, query.actor, query.agent_id)
                .await?
            {
                return Err(PostgresPrivacyErasureError::AgentMemoryUnauthorized);
            }
            tx.rollback().await?;
            let next_cursor = more.then(|| {
                let row = rows.last().expect("nonzero page size");
                ErasureJobCursor {
                    requested_at: UtcTimestamp::from_unix_millis(row.1),
                    erasure_job_id: ErasureJobId::from_uuid(row.0).expect("validated persisted id"),
                }
            });
            Ok(AgentMemoryErasureJobPage {
                jobs: items,
                next_cursor,
            })
        })
    }
}

impl AgentMemoryErasureAdmissionStore for PostgresPrivacyErasureStore {
    type Error = PostgresPrivacyErasureError;

    fn request_agent_memory_erasure(
        &self,
        command: RequestAgentMemoryErasure,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<AgentMemoryErasureAdmission, Self::Error>,
    > {
        Box::pin(async move { self.request_agent_memory_erasure_inner(command).await })
    }

    fn load_agent_memory_erasure_lineage(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<AgentMemoryErasureLineage>, Self::Error>,
    > {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
                .await?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_lineage
                 WHERE tenant_id=$1 AND erasure_job_id=$2)",
            )
            .bind(tenant_id.into_uuid())
            .bind(erasure_job_id.into_uuid())
            .fetch_one(&mut *transaction)
            .await?;
            let lineage = if exists {
                Some(load_lineage(&mut transaction, tenant_id, erasure_job_id).await?)
            } else {
                None
            };
            transaction.rollback().await?;
            Ok(lineage)
        })
    }
}

impl PostgresPrivacyErasureStore {
    async fn request_agent_memory_erasure_inner(
        &self,
        command: RequestAgentMemoryErasure,
    ) -> Result<AgentMemoryErasureAdmission, PostgresPrivacyErasureError> {
        command.validate()?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        if !crate::memory_management_authority::authorize(
            &mut transaction,
            command.requested_by,
            command.agent_id,
        )
        .await?
        {
            return Err(PostgresPrivacyErasureError::AgentMemoryUnauthorized);
        }
        let mut lock_keys = [
            format!(
                "agent-memory-erasure:idempotency:{}",
                command.idempotency_key.as_str()
            ),
            format!("agent-memory-erasure:memory:{}", command.memory_id),
        ];
        lock_keys.sort();
        for lock_key in lock_keys {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(format!("{}:{lock_key}", command.tenant_id))
                .execute(&mut *transaction)
                .await?;
        }
        let existing_job_id: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT request.erasure_job_id
             FROM privacy_erasure.requests AS request
             LEFT JOIN memory.agent_memory_erasure_lineage AS lineage
               ON lineage.tenant_id=request.tenant_id
              AND lineage.erasure_job_id=request.erasure_job_id
             WHERE request.tenant_id=$1
               AND (request.idempotency_key=$2 OR lineage.memory_id=$3)
             FOR UPDATE OF request",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.idempotency_key.as_str())
        .bind(command.memory_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing_job_id) = existing_job_id {
            let existing_job_id = ErasureJobId::from_uuid(existing_job_id)
                .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid erasure job id"))?;
            let lineage =
                load_lineage(&mut transaction, command.tenant_id, existing_job_id).await?;
            transaction.rollback().await?;
            let job = ErasureJobStore::load(self, command.tenant_id, existing_job_id)
                .await?
                .ok_or(PostgresPrivacyErasureError::CorruptState(
                    "existing erasure job disappeared",
                ))?;
            let exact = command.matches_admitted_intent(&job, &lineage)?;
            if exact {
                return Ok(AgentMemoryErasureAdmission {
                    disposition: ErasureAppendDisposition::Duplicate,
                    lineage,
                    job,
                });
            }
            return Err(PostgresPrivacyErasureError::IdempotencyConflict);
        }

        let row = sqlx::query(
            "SELECT memory.lifecycle,memory.current_version_id,
                    version.content_id,version.content_hash,
                    content.tenant_id AS content_tenant_id,content.media_type,
                    content.byte_length::text AS content_byte_length,
                    content.content_hash AS stored_content_hash
             FROM memory.memories AS memory
             JOIN memory.memory_versions AS version
               ON version.memory_id=memory.memory_id
              AND version.memory_version_id=memory.current_version_id
             JOIN artifact.content_refs AS content ON content.content_id=version.content_id
             WHERE memory.tenant_id=$1 AND memory.memory_id=$2
               AND memory.scope_kind=4 AND memory.scope_id=$3
             FOR UPDATE OF memory,version,content",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.agent_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresPrivacyErasureError::AgentMemoryNotFound)?;
        let current_version_id: uuid::Uuid = row.try_get("current_version_id")?;
        if current_version_id != command.expected_memory_version_id.into_uuid() {
            return Err(PostgresPrivacyErasureError::AgentMemoryVersionConflict);
        }
        let lifecycle: i16 = row.try_get("lifecycle")?;
        if !matches!(lifecycle, 1 | 3) {
            return Err(PostgresPrivacyErasureError::AgentMemoryLifecycleConflict);
        }
        let content_tenant_id: Option<uuid::Uuid> = row.try_get("content_tenant_id")?;
        let version_hash: Vec<u8> = row.try_get("content_hash")?;
        let stored_hash: Vec<u8> = row.try_get("stored_content_hash")?;
        if content_tenant_id != Some(command.tenant_id.into_uuid()) || version_hash != stored_hash {
            return Err(PostgresPrivacyErasureError::CorruptState(
                "Memory Content identity drift",
            ));
        }
        let content_ref = ContentRef {
            content_id: ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid content id"))?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid media type"))?,
            byte_length: row
                .try_get::<String, _>("content_byte_length")?
                .parse()
                .map_err(|_| {
                    PostgresPrivacyErasureError::CorruptState("invalid content byte length")
                })?,
            sha256: Sha256::from_bytes(stored_hash)
                .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid content digest"))?,
        };
        let related_content_refs = load_memory_content_closure(
            &mut transaction,
            command.tenant_id,
            command.memory_id,
            &content_ref,
        )
        .await?;
        let erasure_job_id = ErasureJobId::generate();
        let lineage = AgentMemoryErasureLineage {
            tenant_id: command.tenant_id,
            erasure_job_id,
            agent_id: command.agent_id,
            memory_id: command.memory_id,
            memory_version_id: command.expected_memory_version_id,
            content_ref,
            related_content_refs,
            requested_by: command.requested_by,
            reason_code: command.reason_code.clone(),
            frozen_at: command.requested_at,
        };
        let (request, plan) = build_agent_memory_erasure_facts(&command, &lineage)?;
        if lifecycle == 1 {
            let updated = sqlx::query(
                "UPDATE memory.memories SET lifecycle=3,revoked_at_ms=$4,updated_at_ms=$4
                 WHERE tenant_id=$1 AND memory_id=$2 AND current_version_id=$3 AND lifecycle=1",
            )
            .bind(command.tenant_id.into_uuid())
            .bind(command.memory_id.into_uuid())
            .bind(command.expected_memory_version_id.into_uuid())
            .bind(command.requested_at.unix_millis())
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresPrivacyErasureError::AgentMemoryLifecycleConflict);
            }
            sqlx::query(
                "INSERT INTO memory.agent_memory_revocations (
                     tenant_id,idempotency_key,memory_id,memory_version_id,agent_id,
                     revoked_by,reason_code,revoked_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            )
            .bind(command.tenant_id.into_uuid())
            .bind(command.idempotency_key.as_str())
            .bind(command.memory_id.into_uuid())
            .bind(command.expected_memory_version_id.into_uuid())
            .bind(command.agent_id.into_uuid())
            .bind(command.requested_by.principal_id.into_uuid())
            .bind(command.reason_code.as_str())
            .bind(command.requested_at.unix_millis())
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE evolution.proposals SET status=4
             WHERE tenant_id=$1 AND target_memory_id=$2 AND status IN (1,2,3)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE memory.agent_memory_drafts
             SET status=5,proposal_id=NULL,updated_at_ms=GREATEST(updated_at_ms,$3)
             WHERE tenant_id=$1 AND memory_id=$2 AND status IN (1,2,3)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.requested_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        let request_fingerprint = request.fingerprint()?;
        sqlx::query(
            "INSERT INTO privacy_erasure.requests (
                 erasure_job_id,schema_version,tenant_id,actor_principal_id,
                 actor_principal_kind,retention_policy_ref,idempotency_key,
                 requested_at_ms,complete_by_ms,root_resource_count,request_fingerprint
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(erasure_job_id.into_uuid())
        .bind(request.schema_version.as_str())
        .bind(command.tenant_id.into_uuid())
        .bind(command.requested_by.principal_id.into_uuid())
        .bind(principal_kind_code(command.requested_by.principal_kind))
        .bind(request.retention_policy_ref.as_str())
        .bind(command.idempotency_key.as_str())
        .bind(command.requested_at.unix_millis())
        .bind(command.complete_by.unix_millis())
        .bind(
            i16::try_from(request.root_resource_refs.len())
                .expect("validated Agent Memory erasure root bound fits SMALLINT"),
        )
        .bind(request_fingerprint.into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        for (ordinal, root) in request.root_resource_refs.iter().enumerate() {
            insert_root_resource(
                &mut transaction,
                command.tenant_id,
                erasure_job_id,
                ordinal,
                root,
            )
            .await?;
        }
        insert_transition(
            &mut transaction,
            command.tenant_id,
            erasure_job_id,
            transition(1, ErasureJobStatus::PendingPlan, command.requested_at),
        )
        .await?;
        sqlx::query(
            "INSERT INTO privacy_erasure.plans (
                 tenant_id,erasure_job_id,plan_version,schema_version,data_class_count,
                 retention_exception_count,lineage_fingerprint,plan_hash,frozen_at_ms
             ) VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(erasure_job_id.into_uuid())
        .bind(plan.schema_version.as_str())
        .bind(i16::try_from(plan.data_classes.len()).expect("closed data classes fit i16"))
        .bind(
            i16::try_from(plan.retention_exceptions.len())
                .expect("closed retention exceptions fit i16"),
        )
        .bind(plan.lineage_fingerprint.into_bytes().as_slice())
        .bind(plan.plan_hash.into_bytes().as_slice())
        .bind(plan.frozen_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        for data_class in &plan.data_classes {
            sqlx::query(
                "INSERT INTO privacy_erasure.plan_data_classes (
                     tenant_id,erasure_job_id,plan_version,data_class
                 ) VALUES ($1,$2,1,$3)",
            )
            .bind(command.tenant_id.into_uuid())
            .bind(erasure_job_id.into_uuid())
            .bind(data_class.code())
            .execute(&mut *transaction)
            .await?;
        }
        for exception in &plan.retention_exceptions {
            insert_plan_exception(&mut transaction, command.tenant_id, &plan, exception).await?;
        }
        crate::loop_journal::request_tenant_completion_intent_privacy_erasure(
            &mut transaction,
            command.tenant_id,
            erasure_job_id,
            1,
            command.requested_at,
        )
        .await?;
        insert_transition(
            &mut transaction,
            command.tenant_id,
            erasure_job_id,
            transition(2, ErasureJobStatus::Planned, command.requested_at),
        )
        .await?;
        insert_transition(
            &mut transaction,
            command.tenant_id,
            erasure_job_id,
            transition(3, ErasureJobStatus::Fenced, command.requested_at),
        )
        .await?;
        sqlx::query(
            "INSERT INTO memory.agent_memory_erasure_lineage (
                 tenant_id,erasure_job_id,agent_id,memory_id,memory_version_id,
                 content_id,content_media_type,content_byte_length,content_sha256,
                 requested_by,requested_by_kind,reason_code,frozen_at_ms,lineage_fingerprint
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::bigint,$9,$10,$11,$12,$13,$14)",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(erasure_job_id.into_uuid())
        .bind(command.agent_id.into_uuid())
        .bind(command.memory_id.into_uuid())
        .bind(command.expected_memory_version_id.into_uuid())
        .bind(lineage.content_ref.content_id.into_uuid())
        .bind(lineage.content_ref.media_type.as_str())
        .bind(lineage.content_ref.byte_length.to_string())
        .bind(lineage.content_ref.sha256.into_bytes().as_slice())
        .bind(command.requested_by.principal_id.into_uuid())
        .bind(principal_kind_code(command.requested_by.principal_kind))
        .bind(command.reason_code.as_str())
        .bind(command.requested_at.unix_millis())
        .bind(lineage.fingerprint().into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        for (ordinal, content) in lineage.root_content_refs().iter().enumerate() {
            sqlx::query(
                "INSERT INTO memory.agent_memory_erasure_content_roots (
                     tenant_id,erasure_job_id,ordinal,content_id,content_media_type,
                     content_byte_length,content_sha256,is_requested_head
                 ) VALUES ($1,$2,$3,$4,$5,$6::bigint,$7,$8)",
            )
            .bind(command.tenant_id.into_uuid())
            .bind(erasure_job_id.into_uuid())
            .bind(i32::try_from(ordinal).map_err(|_| {
                PostgresPrivacyErasureError::CorruptState("erasure root ordinal overflow")
            })?)
            .bind(content.content_id.into_uuid())
            .bind(content.media_type.as_str())
            .bind(content.byte_length.to_string())
            .bind(content.sha256.into_bytes().as_slice())
            .bind(ordinal == 0)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        let job = ErasureJobStore::load(self, command.tenant_id, erasure_job_id)
            .await?
            .ok_or(PostgresPrivacyErasureError::CorruptState(
                "admitted erasure job disappeared",
            ))?;
        Ok(AgentMemoryErasureAdmission {
            disposition: ErasureAppendDisposition::Applied,
            lineage,
            job,
        })
    }
}

async fn load_lineage(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
) -> Result<AgentMemoryErasureLineage, PostgresPrivacyErasureError> {
    load_lineages(transaction, tenant_id, &[erasure_job_id.into_uuid()])
        .await?
        .remove(&erasure_job_id.into_uuid())
        .ok_or(PostgresPrivacyErasureError::CorruptState(
            "erasure lineage is missing",
        ))
}

async fn load_lineages(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    ids: &[uuid::Uuid],
) -> Result<
    std::collections::HashMap<uuid::Uuid, AgentMemoryErasureLineage>,
    PostgresPrivacyErasureError,
> {
    let rows = sqlx::query(
        "SELECT erasure_job_id,agent_id,memory_id,memory_version_id,content_id,content_media_type,
                content_byte_length::text AS content_byte_length,content_sha256,
                requested_by,requested_by_kind,reason_code,frozen_at_ms,lineage_fingerprint
         FROM memory.agent_memory_erasure_lineage
         WHERE tenant_id=$1 AND erasure_job_id=ANY($2)",
    )
    .bind(tenant_id.into_uuid())
    .bind(ids)
    .fetch_all(&mut *transaction)
    .await?;
    let root_rows = sqlx::query(
        "SELECT erasure_job_id,content_id,content_media_type AS media_type,
        content_byte_length::text AS byte_length,content_sha256 AS content_hash
        FROM memory.agent_memory_erasure_content_roots
        WHERE tenant_id=$1 AND erasure_job_id=ANY($2) ORDER BY erasure_job_id,ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(ids)
    .fetch_all(&mut *transaction)
    .await?;
    let mut roots = std::collections::HashMap::<uuid::Uuid, Vec<ContentRef>>::new();
    for row in root_rows {
        roots
            .entry(row.try_get("erasure_job_id")?)
            .or_default()
            .push(content_ref_from_closure_row(&row)?);
    }
    let mut result = std::collections::HashMap::new();
    for row in rows {
        let id: uuid::Uuid = row.try_get("erasure_job_id")?;
        let lineage = decode_lineage(
            &row,
            tenant_id,
            parse_id(id, "erasure job id")?,
            roots.remove(&id).unwrap_or_default(),
        )?;
        result.insert(id, lineage);
    }
    Ok(result)
}

fn decode_lineage(
    row: &sqlx::postgres::PgRow,
    tenant_id: TenantId,
    erasure_job_id: ErasureJobId,
    roots: Vec<ContentRef>,
) -> Result<AgentMemoryErasureLineage, PostgresPrivacyErasureError> {
    let content_ref = ContentRef {
        content_id: parse_id(row.try_get("content_id")?, "content id")?,
        media_type: MediaType::new(row.try_get::<String, _>("content_media_type")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid media type"))?,
        byte_length: row
            .try_get::<String, _>("content_byte_length")?
            .parse()
            .map_err(|_| {
                PostgresPrivacyErasureError::CorruptState("invalid content byte length")
            })?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_sha256")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid content digest"))?,
    };
    if roots.first() != Some(&content_ref) {
        return Err(PostgresPrivacyErasureError::CorruptState(
            "erasure requested head drift",
        ));
    }
    let lineage = AgentMemoryErasureLineage {
        tenant_id,
        erasure_job_id,
        agent_id: parse_id(row.try_get("agent_id")?, "agent id")?,
        memory_id: parse_id(row.try_get("memory_id")?, "memory id")?,
        memory_version_id: parse_id(row.try_get("memory_version_id")?, "memory version id")?,
        content_ref,
        related_content_refs: roots.into_iter().skip(1).collect(),
        requested_by: Actor {
            tenant_id,
            principal_id: parse_id(row.try_get("requested_by")?, "requested by")?,
            principal_kind: principal_kind(row.try_get("requested_by_kind")?)?,
        },
        reason_code: DottedKey::new(row.try_get::<String, _>("reason_code")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid reason code"))?,
        frozen_at: UtcTimestamp::from_unix_millis(row.try_get("frozen_at_ms")?),
    };
    let persisted_fingerprint: Vec<u8> = row.try_get("lineage_fingerprint")?;
    if persisted_fingerprint != lineage.fingerprint().into_bytes() {
        return Err(PostgresPrivacyErasureError::CorruptState(
            "erasure lineage fingerprint drift",
        ));
    }
    Ok(lineage)
}

async fn load_memory_content_closure(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    memory_id: musteragent_identity::MemoryId,
    requested_head: &ContentRef,
) -> Result<Vec<ContentRef>, PostgresPrivacyErasureError> {
    let roots = memory_content_closure(transaction, tenant_id, memory_id).await?;
    if !roots.contains(requested_head) {
        return Err(PostgresPrivacyErasureError::CorruptState(
            "requested Memory head missing from erasure closure",
        ));
    }
    Ok(roots
        .into_iter()
        .filter(|reference| reference != requested_head)
        .collect())
}

/// Original Memory owner inventory, shared by exact consumer-owned erasure
/// preparation. This is a read of roots, not authorization to erase a memory.
pub(crate) async fn memory_content_closure(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    memory_id: musteragent_identity::MemoryId,
) -> Result<Vec<ContentRef>, PostgresPrivacyErasureError> {
    memory_content_closures(transaction, tenant_id, &[memory_id.into_uuid()]).await
}

pub(crate) async fn memory_content_closures(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    memory_ids: &[uuid::Uuid],
) -> Result<Vec<ContentRef>, PostgresPrivacyErasureError> {
    let rows = sqlx::query(
        "WITH root_keys(content_id,content_hash) AS (
             SELECT content_id,content_hash FROM memory.memory_versions
             WHERE memory_id=ANY($2)
             UNION
             SELECT content_id,content_sha256 FROM memory.agent_memory_drafts
             WHERE tenant_id=$1 AND memory_id=ANY($2)
             UNION
             SELECT candidate_content_id,candidate_content_hash FROM evolution.proposals
             WHERE tenant_id=$1 AND target_memory_id=ANY($2)
             UNION
             SELECT evaluation.report_content_id,evaluation.report_content_hash
             FROM evolution.evaluations AS evaluation
             JOIN evolution.proposals AS proposal
               ON proposal.proposal_id=evaluation.proposal_id
             WHERE proposal.tenant_id=$1 AND proposal.target_memory_id=ANY($2)
             UNION
             SELECT approval.prompt_content_id,approval.prompt_content_hash
             FROM evolution.approvals AS approval
             JOIN evolution.proposals AS proposal
               ON proposal.proposal_id=approval.proposal_id
             WHERE proposal.tenant_id=$1 AND proposal.target_memory_id=ANY($2)
         )
         SELECT content.content_id,content.media_type,
                content.byte_length::text AS byte_length,content.content_hash
         FROM root_keys AS root
         JOIN artifact.content_refs AS content
           ON content.content_id=root.content_id AND content.content_hash=root.content_hash
         WHERE content.tenant_id=$1
         ORDER BY content.content_id,content.content_hash",
    )
    .bind(tenant_id.into_uuid())
    .bind(memory_ids)
    .fetch_all(&mut *transaction)
    .await?;
    rows.iter().map(content_ref_from_closure_row).collect()
}

/// A Task can own one version of an otherwise user-owned Memory. Published
/// proposal byproducts follow that exact version, never all drafts of its key.
pub(crate) async fn memory_version_content_closures(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    versions: &[uuid::Uuid],
) -> Result<Vec<ContentRef>, PostgresPrivacyErasureError> {
    let rows=sqlx::query(
        "WITH owned_proposals AS (
           SELECT p.proposal_id FROM evolution.proposals p JOIN evolution.promotions m USING(proposal_id)
           WHERE p.tenant_id=$1 AND m.published_memory_version_id=ANY($2)
         ), keys(content_id,content_hash) AS (
           SELECT v.content_id,v.content_hash FROM memory.memory_versions v
           JOIN memory.memories m USING(memory_id) WHERE m.tenant_id=$1 AND v.memory_version_id=ANY($2)
           UNION SELECT p.candidate_content_id,p.candidate_content_hash FROM evolution.proposals p
             JOIN owned_proposals o USING(proposal_id)
           UNION SELECT e.report_content_id,e.report_content_hash FROM evolution.evaluations e
             JOIN owned_proposals o USING(proposal_id)
           UNION SELECT a.prompt_content_id,a.prompt_content_hash FROM evolution.approvals a
             JOIN owned_proposals o USING(proposal_id)
         ) SELECT DISTINCT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
           FROM keys k JOIN artifact.content_refs c USING(content_id,content_hash)
           WHERE c.tenant_id=$1 ORDER BY c.content_id")
        .bind(tenant.into_uuid()).bind(versions).fetch_all(&mut *tx).await?;
    rows.iter().map(content_ref_from_closure_row).collect()
}

fn content_ref_from_closure_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ContentRef, PostgresPrivacyErasureError> {
    Ok(ContentRef {
        content_id: parse_id(row.try_get("content_id")?, "content id")?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid media type"))?,
        byte_length: row
            .try_get::<String, _>("byte_length")?
            .parse()
            .map_err(|_| {
                PostgresPrivacyErasureError::CorruptState("invalid content byte length")
            })?,
        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
            .map_err(|_| PostgresPrivacyErasureError::CorruptState("invalid content digest"))?,
    })
}

fn transition(
    sequence: u64,
    status: ErasureJobStatus,
    occurred_at: UtcTimestamp,
) -> ErasureTransition {
    ErasureTransition {
        sequence: ErasureTransitionSequence::new(sequence).expect("fixed sequence is valid"),
        status,
        occurred_at,
    }
}

fn parse_id<T>(value: uuid::Uuid, field: &'static str) -> Result<T, PostgresPrivacyErasureError>
where
    T: std::str::FromStr,
{
    value
        .to_string()
        .parse()
        .map_err(|_| PostgresPrivacyErasureError::CorruptState(field))
}

const fn principal_kind_code(kind: PrincipalKind) -> i16 {
    match kind {
        PrincipalKind::User => 1,
        PrincipalKind::Service => 2,
        PrincipalKind::System => 3,
    }
}

fn principal_kind(code: i16) -> Result<PrincipalKind, PostgresPrivacyErasureError> {
    match code {
        1 => Ok(PrincipalKind::User),
        2 => Ok(PrincipalKind::Service),
        3 => Ok(PrincipalKind::System),
        _ => Err(PostgresPrivacyErasureError::CorruptState(
            "invalid principal kind",
        )),
    }
}
