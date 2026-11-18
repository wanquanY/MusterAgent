use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, MemoryId, MemoryVersionId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_runtime_memory::{
    ClaimedMemorySearchBackfill, MemoryPersistenceDisposition, MemorySearchBackfillSource,
    MemorySearchBackfillStatus, MemorySearchDocument, MemorySearchIndexBackfillStore,
};
use sqlx::Row as _;

use crate::memory::{PostgresMemoryEvolutionError, PostgresMemoryEvolutionStore};
use crate::memory_search_index::{persist_search_document, search_document_matches};

const ELIGIBLE_MEMORY_SQL: &str = "memory.lifecycle=1
     AND memory.current_version_id=version.memory_version_id
     AND NOT EXISTS (
         SELECT 1 FROM memory.search_erasure_tombstones AS tombstone
         WHERE tombstone.memory_version_id=version.memory_version_id
     )
     AND NOT memory.search_content_is_erased(
         memory.tenant_id,version.content_id,version.content_hash
     )
     AND (version.source <> 1 OR EXISTS (
         SELECT 1 FROM memory.explicit_publications AS publication
         WHERE publication.memory_version_id=version.memory_version_id
           AND publication.memory_id=memory.memory_id
           AND publication.tenant_id=memory.tenant_id
     ) OR EXISTS (
         SELECT 1 FROM memory.governance_publication_outbox AS publication
         WHERE publication.memory_version_id=version.memory_version_id
           AND publication.memory_id=memory.memory_id
           AND publication.tenant_id=memory.tenant_id
     ))";

impl MemorySearchIndexBackfillStore for PostgresMemoryEvolutionStore {
    type Error = PostgresMemoryEvolutionError;

    fn enqueue_missing(
        &self,
        now: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<u64, Self::Error>> {
        Box::pin(async move {
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await?;
            let settle_sql = format!(
                "UPDATE memory.search_backfill_jobs AS job
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     completed_at_ms=$1,updated_at_ms=$1,last_error_code=NULL
                 WHERE status <> 3 AND (
                     EXISTS(SELECT 1 FROM memory.search_index AS search
                            WHERE search.memory_version_id=job.memory_version_id)
                     OR NOT EXISTS(
                         SELECT 1 FROM memory.memories AS memory
                         JOIN memory.memory_versions AS version
                           ON version.memory_version_id=job.memory_version_id
                          AND version.memory_id=memory.memory_id
                         WHERE memory.memory_id=job.memory_id AND {ELIGIBLE_MEMORY_SQL}
                     )
                 )"
            );
            sqlx::query(&settle_sql)
                .bind(now.unix_millis())
                .execute(&mut *transaction)
                .await?;
            let enqueue_sql = format!(
                "INSERT INTO memory.search_backfill_jobs (
                     memory_version_id,tenant_id,memory_id,status,attempt_count,
                     lease_owner,lease_expires_at_ms,available_at_ms,last_error_code,
                     created_at_ms,updated_at_ms,completed_at_ms
                 )
                 SELECT version.memory_version_id,memory.tenant_id,memory.memory_id,1,0,
                        NULL,NULL,$1,NULL,$1,$1,NULL
                 FROM memory.memories AS memory
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=memory.current_version_id
                  AND version.memory_id=memory.memory_id
                 LEFT JOIN memory.search_index AS search
                   ON search.memory_version_id=version.memory_version_id
                 WHERE search.memory_version_id IS NULL AND {ELIGIBLE_MEMORY_SQL}
                 ON CONFLICT(memory_version_id) DO UPDATE SET
                     status=1,attempt_count=0,lease_owner=NULL,lease_expires_at_ms=NULL,
                     available_at_ms=EXCLUDED.available_at_ms,last_error_code=NULL,
                     updated_at_ms=EXCLUDED.updated_at_ms,completed_at_ms=NULL
                 WHERE search_backfill_jobs.status=3"
            );
            let changed = sqlx::query(&enqueue_sql)
                .bind(now.unix_millis())
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            Ok(changed)
        })
    }

    fn claim_backfill(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<ClaimedMemorySearchBackfill>, Self::Error>,
    > {
        Box::pin(async move {
            if worker_id.is_empty() || lease_expires_at <= now {
                return Err(PostgresMemoryEvolutionError::InvalidBackfillClaim);
            }
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await?;
            sqlx::query(
                "UPDATE memory.search_backfill_jobs
                 SET status=4,lease_owner=NULL,lease_expires_at_ms=NULL,
                     updated_at_ms=$1,last_error_code='musteragent.legacy.memory-index.lease-exhausted@1'
                 WHERE status=2 AND lease_expires_at_ms <= $1 AND attempt_count >= 10",
            )
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await?;
            let claim_sql = format!(
                "SELECT job.memory_version_id,job.tenant_id,job.memory_id,
                        job.attempt_count,version.valid_from_ms,
                        content.content_id,content.media_type,
                        content.byte_length::text AS byte_length,content.content_hash
                 FROM memory.search_backfill_jobs AS job
                 JOIN memory.memories AS memory ON memory.memory_id=job.memory_id
                 JOIN memory.memory_versions AS version
                   ON version.memory_version_id=job.memory_version_id
                  AND version.memory_id=memory.memory_id
                 JOIN artifact.content_refs AS content
                   ON content.content_id=version.content_id
                  AND content.tenant_id=memory.tenant_id
                  AND content.content_hash=version.content_hash
                 WHERE ((job.status=1 AND job.available_at_ms <= $1)
                     OR (job.status=2 AND job.lease_expires_at_ms <= $1))
                   AND job.attempt_count < 10 AND {ELIGIBLE_MEMORY_SQL}
                 ORDER BY job.available_at_ms,job.memory_version_id
                 LIMIT 1 FOR UPDATE OF job SKIP LOCKED"
            );
            let row = sqlx::query(&claim_sql)
                .bind(now.unix_millis())
                .fetch_optional(&mut *transaction)
                .await?;
            let Some(row) = row else {
                transaction.rollback().await?;
                return Ok(None);
            };
            let memory_version_uuid: uuid::Uuid = row.try_get("memory_version_id")?;
            let prior_attempts: i16 = row.try_get("attempt_count")?;
            let next_attempt = prior_attempts
                .checked_add(1)
                .ok_or(PostgresMemoryEvolutionError::NumericRange)?;
            let updated = sqlx::query(
                "UPDATE memory.search_backfill_jobs
                 SET status=2,attempt_count=$2,lease_owner=$3,lease_expires_at_ms=$4,
                     updated_at_ms=$5,last_error_code=NULL
                 WHERE memory_version_id=$1 AND attempt_count=$6
                   AND ((status=1 AND available_at_ms <= $5)
                     OR (status=2 AND lease_expires_at_ms <= $5))",
            )
            .bind(memory_version_uuid)
            .bind(next_attempt)
            .bind(&worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(now.unix_millis())
            .bind(prior_attempts)
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresMemoryEvolutionError::StaleBackfillLease);
            }
            let claimed = ClaimedMemorySearchBackfill {
                source: MemorySearchBackfillSource {
                    tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    memory_id: MemoryId::from_uuid(row.try_get("memory_id")?)
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    memory_version_id: MemoryVersionId::from_uuid(memory_version_uuid)
                        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    content_ref: ContentRef {
                        content_id: ContentId::from_uuid(row.try_get("content_id")?)
                            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                        media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                        byte_length: row
                            .try_get::<String, _>("byte_length")?
                            .parse()
                            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                        sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                            .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?,
                    },
                    version_created_at: UtcTimestamp::from_unix_millis(
                        row.try_get("valid_from_ms")?,
                    ),
                },
                attempt_count: u16::try_from(next_attempt)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                lease_owner: worker_id,
                lease_expires_at,
            };
            claimed
                .validate()
                .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)?;
            transaction.commit().await?;
            Ok(Some(claimed))
        })
    }

    fn complete_backfill(
        &self,
        claimed: ClaimedMemorySearchBackfill,
        document: MemorySearchDocument,
        completed_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            claimed
                .validate()
                .map_err(|_| PostgresMemoryEvolutionError::InvalidBackfillClaim)?;
            document
                .validate_for_backfill_source(&claimed.source)
                .map_err(|_| PostgresMemoryEvolutionError::InvalidBackfillClaim)?;
            if completed_at > claimed.lease_expires_at {
                return Err(PostgresMemoryEvolutionError::StaleBackfillLease);
            }
            let mut transaction = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                .await?;
            let status: Option<i16> = sqlx::query_scalar(
                "SELECT status FROM memory.search_backfill_jobs WHERE memory_version_id=$1",
            )
            .bind(claimed.source.memory_version_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await?;
            if status == Some(3) {
                let exact = search_document_matches(
                    &mut transaction,
                    &document,
                    claimed.source.version_created_at.unix_millis(),
                )
                .await?;
                transaction.rollback().await?;
                return if exact {
                    Ok(MemoryPersistenceDisposition::Duplicate)
                } else {
                    Err(PostgresMemoryEvolutionError::MemoryConflict)
                };
            }
            let authorized: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM memory.search_backfill_jobs
                 WHERE memory_version_id=$1 AND tenant_id=$2 AND memory_id=$3 AND status=2
                   AND attempt_count=$4 AND lease_owner=$5 AND lease_expires_at_ms=$6
                   AND lease_expires_at_ms >= $7)",
            )
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(claimed.source.tenant_id.into_uuid())
            .bind(claimed.source.memory_id.into_uuid())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .bind(completed_at.unix_millis())
            .fetch_one(&mut *transaction)
            .await?;
            if !authorized {
                return Err(PostgresMemoryEvolutionError::StaleBackfillLease);
            }
            persist_search_document(
                &mut transaction,
                &document,
                claimed.source.version_created_at.unix_millis(),
            )
            .await?;
            let updated = sqlx::query(
                "UPDATE memory.search_backfill_jobs
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     completed_at_ms=$2,updated_at_ms=$2,last_error_code=NULL
                 WHERE memory_version_id=$1 AND status=2 AND attempt_count=$3
                   AND lease_owner=$4 AND lease_expires_at_ms=$5",
            )
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(completed_at.unix_millis())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresMemoryEvolutionError::StaleBackfillLease);
            }
            transaction.commit().await?;
            Ok(MemoryPersistenceDisposition::Applied)
        })
    }

    fn fail_backfill(
        &self,
        claimed: ClaimedMemorySearchBackfill,
        error_code: VersionedKey,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemoryPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            claimed
                .validate()
                .map_err(|_| PostgresMemoryEvolutionError::InvalidBackfillClaim)?;
            if retry_at <= failed_at || failed_at > claimed.lease_expires_at {
                return Err(PostgresMemoryEvolutionError::InvalidBackfillClaim);
            }
            let updated = sqlx::query(
                "UPDATE memory.search_backfill_jobs
                 SET status=CASE WHEN attempt_count >= 10 THEN 4 ELSE 1 END,
                     available_at_ms=$2,lease_owner=NULL,lease_expires_at_ms=NULL,
                     last_error_code=$3,updated_at_ms=$4
                 WHERE memory_version_id=$1 AND status=2 AND attempt_count=$5
                   AND lease_owner=$6 AND lease_expires_at_ms=$7",
            )
            .bind(claimed.source.memory_version_id.into_uuid())
            .bind(retry_at.unix_millis())
            .bind(error_code.as_str())
            .bind(failed_at.unix_millis())
            .bind(
                i16::try_from(claimed.attempt_count)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            )
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&self.pool)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresMemoryEvolutionError::StaleBackfillLease);
            }
            Ok(MemoryPersistenceDisposition::Applied)
        })
    }

    fn backfill_status(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<MemorySearchBackfillStatus, Self::Error>,
    > {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT
                     count(*) FILTER (WHERE status=1) AS pending,
                     count(*) FILTER (WHERE status=2) AS running,
                     count(*) FILTER (WHERE status=3) AS completed,
                     count(*) FILTER (WHERE status=4) AS dead_letter
                 FROM memory.search_backfill_jobs",
            )
            .fetch_one(&self.pool)
            .await?;
            Ok(MemorySearchBackfillStatus {
                pending: u64::try_from(row.try_get::<i64, _>("pending")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                running: u64::try_from(row.try_get::<i64, _>("running")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                completed: u64::try_from(row.try_get::<i64, _>("completed")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
                dead_letter: u64::try_from(row.try_get::<i64, _>("dead_letter")?)
                    .map_err(|_| PostgresMemoryEvolutionError::NumericRange)?,
            })
        })
    }
}
