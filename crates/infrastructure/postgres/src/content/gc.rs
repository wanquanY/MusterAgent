use std::time::Duration;

use musteragent_identity::{ContentId, TenantId, UtcTimestamp};
use musteragent_runtime_content::{BlobIdentity, BlobStoreError, StorageLocator};
use musteragent_storage_protection::ProtectedOpaqueRef;
use sqlx::Row as _;

use super::{
    CONTENT_OWNED_BY_TENANT, CONTENT_READY, CONTENT_STAGED, PostgresContentError,
    PostgresContentStore, lock_blob, other_blob_holders_exist, protection_context,
    tenant_blob_identity,
};

const GC_JOB_PENDING: i16 = 1;
const GC_JOB_COMPLETED: i16 = 2;
const GC_RETENTION_POLICY: &str = "content.unreferenced@1";
const COMPACTION_RETENTION_POLICY: &str = "content.shared-blob-compaction@1";
/// Readers resolve a locator before object I/O without a transaction; the
/// superseded copy outlives every such in-flight read.
const COMPACTED_COPY_DELETE_DELAY_MS: i64 = 60 * 60 * 1_000;

/// Bounded production policy for unreachable tenant-content reclamation.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContentGcPolicy {
    pub batch_size: u16,
    pub referenced_recheck_interval: Duration,
    pub failed_delete_retry_interval: Duration,
}

impl Default for ContentGcPolicy {
    fn default() -> Self {
        Self {
            batch_size: 128,
            referenced_recheck_interval: Duration::from_secs(7 * 24 * 60 * 60),
            failed_delete_retry_interval: Duration::from_secs(60),
        }
    }
}

impl ContentGcPolicy {
    fn validate(self) -> Result<Self, PostgresContentError> {
        if self.batch_size == 0
            || self.batch_size > 1_024
            || self.referenced_recheck_interval.is_zero()
            || self.failed_delete_retry_interval.is_zero()
        {
            return Err(PostgresContentError::Corrupt);
        }
        Ok(self)
    }
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct ContentGcReport {
    pub scanned: u64,
    pub still_referenced: u64,
    pub reclaimed: u64,
    pub delete_deferred: u64,
    pub pending_retried: u64,
    /// Tenants whose Ledger still lacks a root set; none of their reclaimable
    /// Content can be collected until projection converges.
    pub barrier_tenants: u64,
    /// Historical duplicate copies re-pointed to an existing same-tenant object.
    pub compacted: u64,
}

#[derive(Debug, Clone)]
struct ContentGcJob {
    tenant_id: TenantId,
    content_id: ContentId,
    locator: StorageLocator,
}

enum CandidateOutcome {
    None,
    Referenced,
    /// The row is gone; its bytes stay with other holders of the same object.
    Released,
    Job(ContentGcJob),
}

impl PostgresContentStore {
    /// Reclaims only rows that PostgreSQL proves have no canonical foreign-key
    /// references. The ContentRef removal and durable blob-deletion job are one
    /// transaction; a crash can leak neither a readable dangling reference nor
    /// an untracked object-store deletion.
    pub async fn sweep_unreferenced_content(
        &self,
        now: UtcTimestamp,
        policy: ContentGcPolicy,
    ) -> Result<ContentGcReport, PostgresContentError> {
        let policy = policy.validate()?;
        let now_ms = now.unix_millis();
        let retry_ms = duration_millis(policy.failed_delete_retry_interval)?;
        let recheck_ms = duration_millis(policy.referenced_recheck_interval)?;
        let mut report = ContentGcReport::default();
        // Mixed-version writers leave pending coverage. Converge a bounded
        // batch using the same canonical protocol decoder before collecting.
        crate::ledger_content::backfill(&self.database, 1).await?;
        let barrier_tenants: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT tenant_id) FROM event_store.ledger_records
              WHERE content_reference_coverage NOT IN (1,6)",
        )
        .fetch_one(&self.pool)
        .await?;
        report.barrier_tenants = u64::try_from(barrier_tenants).unwrap_or(0);
        for _ in 0..policy.batch_size {
            let Some(job) = self.claim_pending_gc_job(now_ms, retry_ms).await? else {
                break;
            };
            report.pending_retried += 1;
            match self.complete_gc_job(&job, now_ms).await {
                Ok(()) => report.reclaimed += 1,
                Err(error) => {
                    self.defer_gc_job(job.content_id, now_ms, retry_ms).await?;
                    tracing::warn!(
                        tenant_id = %job.tenant_id,
                        content_id = %job.content_id,
                        error = %error,
                        "unreferenced Content blob deletion deferred"
                    );
                    report.delete_deferred += 1;
                }
            }
        }

        let candidate_budget = policy
            .batch_size
            .saturating_sub(u16::try_from(report.pending_retried).unwrap_or(u16::MAX));
        for _ in 0..candidate_budget {
            match self
                .claim_unreferenced_candidate(now_ms, recheck_ms)
                .await?
            {
                CandidateOutcome::None => break,
                CandidateOutcome::Referenced => {
                    report.scanned += 1;
                    report.still_referenced += 1;
                }
                CandidateOutcome::Released => {
                    report.scanned += 1;
                    report.reclaimed += 1;
                }
                CandidateOutcome::Job(job) => {
                    report.scanned += 1;
                    match self.complete_gc_job(&job, now_ms).await {
                        Ok(()) => report.reclaimed += 1,
                        Err(error) => {
                            self.defer_gc_job(job.content_id, now_ms, retry_ms).await?;
                            tracing::warn!(
                                tenant_id = %job.tenant_id,
                                content_id = %job.content_id,
                                error = %error,
                                "unreferenced Content blob deletion deferred"
                            );
                            report.delete_deferred += 1;
                        }
                    }
                }
            }
        }
        report.compacted = self
            .compact_duplicate_copies(now_ms, policy.batch_size)
            .await?;
        Ok(report)
    }

    async fn claim_unreferenced_candidate(
        &self,
        now_ms: i64,
        referenced_recheck_ms: i64,
    ) -> Result<CandidateOutcome, PostgresContentError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let Some(row) = sqlx::query(
            "SELECT content_id, tenant_id, media_type,
                    byte_length::text AS byte_length, content_hash,
                    storage_ref, storage_ref_protected, state, blob_content_id
               FROM artifact.content_refs
              WHERE tenant_id IS NOT NULL
                AND ownership_kind = $1
                AND state IN ($2, $3)
                AND gc_eligible_at_ms IS NOT NULL
                AND gc_eligible_at_ms <= $4
              ORDER BY gc_eligible_at_ms, content_id
              LIMIT 1
              FOR UPDATE SKIP LOCKED",
        )
        .bind(CONTENT_OWNED_BY_TENANT)
        .bind(CONTENT_STAGED)
        .bind(CONTENT_READY)
        .bind(now_ms)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.rollback().await?;
            return Ok(CandidateOutcome::None);
        };
        let content_id = ContentId::from_uuid(row.try_get("content_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        let retained: bool = sqlx::query_scalar("SELECT
            EXISTS(SELECT 1 FROM event_store.ledger_content_references WHERE tenant_id=$1 AND content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.dialogue_content_references WHERE tenant_id=$1 AND content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.dialogue_tool_publication_groups WHERE tenant_id=$1 AND (device_receipt_content_id=$2 OR device_resource_manifest_id=$2))
            OR EXISTS(SELECT 1 FROM resource.device_commands WHERE tenant_id=$1 AND (command_content_id=$2 OR request_content_id=$2 OR result_content_id=$2 OR receipt_content_id=$2))
            OR EXISTS(SELECT 1 FROM resource.device_command_reads r JOIN resource.device_commands c USING(invocation_id) WHERE c.tenant_id=$1 AND r.content_id=$2)
            OR EXISTS(SELECT 1 FROM capability.runtime_snapshot_content_references WHERE tenant_id=$1 AND content_id=$2)
            OR EXISTS(SELECT 1 FROM runtime.dialogue_context_content_references WHERE tenant_id=$1 AND content_id=$2)
            OR EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings WHERE tenant_id=$1 AND source_page_content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.dialogue_context_checkpoints WHERE tenant_id=$1 AND (summary_content_id=$2 OR source_manifest_content_id=$2))
            OR EXISTS(SELECT 1 FROM runtime.model_action_rejections rejection JOIN runtime.model_invocations invocation USING(invocation_id) WHERE invocation.tenant_id=$1 AND rejection.repair_content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.public_delivery_candidates WHERE tenant_id=$1 AND source_content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.public_delivery_candidates WHERE tenant_id=$1 AND body_content_id=$2)
            OR EXISTS(SELECT 1 FROM agent.agent_versions WHERE persona_content_id=$2 OR manifest_content_id=$2 OR policy_content_id=$2 OR memory_policy_content_id=$2)
            OR EXISTS(SELECT 1 FROM event_store.ledger_records WHERE tenant_id=$1 AND content_reference_coverage NOT IN (1,6))")
            .bind(tenant_id.into_uuid()).bind(content_id.into_uuid())
            .fetch_one(&mut *transaction).await?;
        if retained {
            sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=$3 WHERE tenant_id=$1 AND content_id=$2")
                .bind(tenant_id.into_uuid()).bind(content_id.into_uuid())
                .bind(now_ms.saturating_add(referenced_recheck_ms))
                .execute(&mut *transaction).await?;
            transaction.commit().await?;
            return Ok(CandidateOutcome::Referenced);
        }
        let identity =
            tenant_blob_identity(tenant_id, content_id, row.try_get("blob_content_id")?)?;
        let state = row.try_get::<i16, _>("state")?;
        let protected_locator = protected_locator(self, identity, state, &row)?;
        let locator = StorageLocator::new(self.reference_keys.open(
            &protection_context(identity),
            &ProtectedOpaqueRef::from_bytes(protected_locator.clone())?,
        )?)?;

        let delete = transaction
            .execute_savepoint(|connection| {
                Box::pin(async move {
                    sqlx::query(
                        "DELETE FROM artifact.content_refs
                          WHERE tenant_id = $1 AND content_id = $2 AND state = $3",
                    )
                    .bind(tenant_id.into_uuid())
                    .bind(content_id.into_uuid())
                    .bind(state)
                    .execute(connection)
                    .await
                })
            })
            .await;
        match delete {
            Ok(result) if result.rows_affected() == 1 => {}
            Ok(_) => return Err(PostgresContentError::Corrupt),
            Err(crate::PostgresSavepointError::Operation(error))
                if is_foreign_key_violation(&error) =>
            {
                sqlx::query(
                    "UPDATE artifact.content_refs
                        SET gc_eligible_at_ms = $3
                      WHERE tenant_id = $1 AND content_id = $2 AND state = $4",
                )
                .bind(tenant_id.into_uuid())
                .bind(content_id.into_uuid())
                .bind(now_ms.saturating_add(referenced_recheck_ms))
                .bind(state)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Ok(CandidateOutcome::Referenced);
            }
            Err(crate::PostgresSavepointError::Operation(error)) => return Err(error.into()),
            Err(crate::PostgresSavepointError::Transaction(error)) => return Err(error.into()),
        }
        lock_blob(&mut transaction, identity).await?;
        if other_blob_holders_exist(&mut transaction, identity, content_id).await? {
            transaction.commit().await?;
            return Ok(CandidateOutcome::Released);
        }

        enqueue_blob_deletion(
            &mut transaction,
            identity,
            &row,
            &protected_locator,
            GC_RETENTION_POLICY,
            now_ms,
            now_ms,
        )
        .await?;
        transaction.commit().await?;
        Ok(CandidateOutcome::Job(ContentGcJob {
            tenant_id,
            content_id: identity.content_id,
            locator,
        }))
    }

    /// Re-points historical copies to the oldest same-tenant owner of identical
    /// verified bytes. The cursor only moves forward; a row that is busy or has
    /// borrowers of its own simply stays a correct, separate copy.
    async fn compact_duplicate_copies(
        &self,
        now_ms: i64,
        budget: u16,
    ) -> Result<u64, PostgresContentError> {
        let mut compacted = 0;
        for _ in 0..budget {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
                .await?;
            let cursor = sqlx::query(
                "SELECT created_at_ms, content_id
                   FROM artifact.content_blob_compaction_cursor
                  WHERE singleton FOR UPDATE SKIP LOCKED",
            )
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(cursor) = cursor else {
                transaction.rollback().await?;
                break;
            };
            let Some(next) = sqlx::query(
                "SELECT content_id, created_at_ms
                   FROM artifact.content_refs
                  WHERE tenant_id IS NOT NULL AND state = 2 AND blob_content_id IS NULL
                    AND (created_at_ms, content_id) > ($1, $2)
                  ORDER BY created_at_ms, content_id
                  LIMIT 1",
            )
            .bind(cursor.try_get::<i64, _>("created_at_ms")?)
            .bind(cursor.try_get::<uuid::Uuid, _>("content_id")?)
            .fetch_optional(&mut *transaction)
            .await?
            else {
                transaction.rollback().await?;
                break;
            };
            let next_id: uuid::Uuid = next.try_get("content_id")?;
            let next_created: i64 = next.try_get("created_at_ms")?;
            if self.compact_copy(&mut transaction, next_id, now_ms).await? {
                compacted += 1;
            }
            sqlx::query(
                "UPDATE artifact.content_blob_compaction_cursor
                    SET created_at_ms = $1, content_id = $2 WHERE singleton",
            )
            .bind(next_created)
            .bind(next_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        Ok(compacted)
    }

    async fn compact_copy(
        &self,
        transaction: &mut sqlx::PgConnection,
        content_id: uuid::Uuid,
        now_ms: i64,
    ) -> Result<bool, PostgresContentError> {
        let Some(copy) = sqlx::query(
            "SELECT content_id, tenant_id, created_at_ms, media_type,
                    byte_length::text AS byte_length, content_hash, storage_ref_protected
               FROM artifact.content_refs
              WHERE content_id = $1 AND tenant_id IS NOT NULL AND state = $2
                AND blob_content_id IS NULL
                AND storage_ref IS NULL AND storage_ref_protected IS NOT NULL
              FOR UPDATE SKIP LOCKED",
        )
        .bind(content_id)
        .bind(CONTENT_READY)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            return Ok(false);
        };
        let tenant_id = TenantId::from_uuid(copy.try_get("tenant_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        let Some(owner) = sqlx::query(
            "SELECT content_id, storage_ref_protected
               FROM artifact.content_refs
              WHERE tenant_id = $1 AND content_hash = $2 AND byte_length = $3::bigint
                AND state = $4 AND blob_content_id IS NULL
                AND storage_ref IS NULL AND storage_ref_protected IS NOT NULL
                AND (created_at_ms, content_id) < ($5, $6)
              ORDER BY created_at_ms, content_id
              LIMIT 1
              FOR SHARE SKIP LOCKED",
        )
        .bind(tenant_id.into_uuid())
        .bind(copy.try_get::<Vec<u8>, _>("content_hash")?)
        .bind(copy.try_get::<String, _>("byte_length")?)
        .bind(CONTENT_READY)
        .bind(copy.try_get::<i64, _>("created_at_ms")?)
        .bind(content_id)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            return Ok(false);
        };
        let borrowed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM artifact.content_refs
              WHERE tenant_id = $1 AND blob_content_id = $2)",
        )
        .bind(tenant_id.into_uuid())
        .bind(content_id)
        .fetch_one(&mut *transaction)
        .await?;
        if borrowed {
            return Ok(false);
        }
        let superseded: Vec<u8> = copy.try_get("storage_ref_protected")?;
        sqlx::query(
            "UPDATE artifact.content_refs
                SET blob_content_id = $3, storage_ref_protected = $4,
                    updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
              WHERE tenant_id = $1 AND content_id = $2 AND blob_content_id IS NULL",
        )
        .bind(tenant_id.into_uuid())
        .bind(content_id)
        .bind(owner.try_get::<uuid::Uuid, _>("content_id")?)
        .bind(owner.try_get::<Vec<u8>, _>("storage_ref_protected")?)
        .execute(&mut *transaction)
        .await?;
        let identity = BlobIdentity {
            tenant_id,
            content_id: ContentId::from_uuid(content_id)
                .map_err(|_| PostgresContentError::Corrupt)?,
        };
        enqueue_blob_deletion(
            transaction,
            identity,
            &copy,
            &superseded,
            COMPACTION_RETENTION_POLICY,
            now_ms,
            now_ms.saturating_add(COMPACTED_COPY_DELETE_DELAY_MS),
        )
        .await?;
        Ok(true)
    }

    async fn claim_pending_gc_job(
        &self,
        now_ms: i64,
        retry_ms: i64,
    ) -> Result<Option<ContentGcJob>, PostgresContentError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let Some(row) = sqlx::query(
            "SELECT content_id, tenant_id, storage_ref_protected
               FROM artifact.content_gc_jobs
              WHERE status = $1 AND updated_at_ms <= $2
              ORDER BY updated_at_ms, content_id
              LIMIT 1
              FOR UPDATE SKIP LOCKED",
        )
        .bind(GC_JOB_PENDING)
        .bind(now_ms)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.rollback().await?;
            return Ok(None);
        };
        let content_id = ContentId::from_uuid(row.try_get("content_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresContentError::Corrupt)?;
        let identity = BlobIdentity {
            tenant_id,
            content_id,
        };
        let protected =
            ProtectedOpaqueRef::from_bytes(row.try_get::<Vec<u8>, _>("storage_ref_protected")?)?;
        let locator = StorageLocator::new(
            self.reference_keys
                .open(&protection_context(identity), &protected)?,
        )?;
        sqlx::query(
            "UPDATE artifact.content_gc_jobs
                SET attempt_count = attempt_count + 1,
                    updated_at_ms = $2
              WHERE content_id = $1 AND status = $3",
        )
        .bind(content_id.into_uuid())
        .bind(now_ms.saturating_add(retry_ms))
        .bind(GC_JOB_PENDING)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(ContentGcJob {
            tenant_id,
            content_id,
            locator,
        }))
    }

    async fn complete_gc_job(
        &self,
        job: &ContentGcJob,
        now_ms: i64,
    ) -> Result<(), PostgresContentError> {
        let identity = BlobIdentity {
            tenant_id: job.tenant_id,
            content_id: job.content_id,
        };
        match self.blobs.delete(identity, &job.locator).await {
            Ok(()) | Err(BlobStoreError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        sqlx::query(
            "UPDATE artifact.content_gc_jobs
                SET status = $2, storage_ref_protected = NULL,
                    updated_at_ms = $3, completed_at_ms = $3
              WHERE content_id = $1 AND status = $4",
        )
        .bind(job.content_id.into_uuid())
        .bind(GC_JOB_COMPLETED)
        .bind(now_ms)
        .bind(GC_JOB_PENDING)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn defer_gc_job(
        &self,
        content_id: ContentId,
        now_ms: i64,
        retry_ms: i64,
    ) -> Result<(), PostgresContentError> {
        sqlx::query(
            "UPDATE artifact.content_gc_jobs
                SET attempt_count = attempt_count + 1, updated_at_ms = $2
              WHERE content_id = $1 AND status = $3",
        )
        .bind(content_id.into_uuid())
        .bind(now_ms.saturating_add(retry_ms))
        .bind(GC_JOB_PENDING)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn protected_locator(
    store: &PostgresContentStore,
    identity: BlobIdentity,
    state: i16,
    row: &sqlx::postgres::PgRow,
) -> Result<Vec<u8>, PostgresContentError> {
    if state == CONTENT_STAGED {
        let locator = store.blobs.derive_locator(identity)?;
        return Ok(store
            .reference_keys
            .seal(&protection_context(identity), locator.as_str())?
            .as_bytes()
            .to_vec());
    }
    if state != CONTENT_READY {
        return Err(PostgresContentError::Corrupt);
    }
    if let Some(value) = row.try_get::<Option<Vec<u8>>, _>("storage_ref_protected")? {
        return Ok(value);
    }
    let legacy = row
        .try_get::<Option<String>, _>("storage_ref")?
        .ok_or(PostgresContentError::Corrupt)?;
    let locator = StorageLocator::new(legacy)?;
    Ok(store
        .reference_keys
        .seal(&protection_context(identity), locator.as_str())?
        .as_bytes()
        .to_vec())
}

/// Jobs are keyed by the object they delete. A completed job for the same
/// identity is reopened, because a later write may have re-created the object.
async fn enqueue_blob_deletion(
    connection: &mut sqlx::PgConnection,
    identity: BlobIdentity,
    row: &sqlx::postgres::PgRow,
    protected_locator: &[u8],
    policy: &str,
    now_ms: i64,
    due_ms: i64,
) -> Result<(), PostgresContentError> {
    sqlx::query(
        "INSERT INTO artifact.content_gc_jobs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref_protected, status, attempt_count,
             retention_policy_ref, created_at_ms, updated_at_ms,
             completed_at_ms
         ) VALUES ($1, $2, $3, $4::bigint, $5, $6, $7, 0, $8, $9, $10, NULL)
         ON CONFLICT (content_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id, media_type = EXCLUDED.media_type,
                byte_length = EXCLUDED.byte_length, content_hash = EXCLUDED.content_hash,
                storage_ref_protected = EXCLUDED.storage_ref_protected,
                status = EXCLUDED.status, attempt_count = 0,
                retention_policy_ref = EXCLUDED.retention_policy_ref,
                created_at_ms = EXCLUDED.created_at_ms,
                updated_at_ms = EXCLUDED.updated_at_ms, completed_at_ms = NULL
          WHERE artifact.content_gc_jobs.status = $11",
    )
    .bind(identity.content_id.into_uuid())
    .bind(identity.tenant_id.into_uuid())
    .bind(row.try_get::<String, _>("media_type")?)
    .bind(row.try_get::<String, _>("byte_length")?)
    .bind(row.try_get::<Vec<u8>, _>("content_hash")?)
    .bind(protected_locator)
    .bind(GC_JOB_PENDING)
    .bind(policy)
    .bind(now_ms)
    .bind(due_ms)
    .bind(GC_JOB_COMPLETED)
    .execute(connection)
    .await?;
    Ok(())
}

fn duration_millis(value: Duration) -> Result<i64, PostgresContentError> {
    i64::try_from(value.as_millis()).map_err(|_| PostgresContentError::Corrupt)
}

fn is_foreign_key_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == "23503")
}
