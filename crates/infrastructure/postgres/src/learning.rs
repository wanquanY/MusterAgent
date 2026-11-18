use std::collections::BTreeSet;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    BranchEvidenceState, DreamJob, DreamJobStatus, DreamWorkload, EvidenceSpan,
    ExperienceCandidate, LearningOutcome, LearningSignal, LearningSignalKind, RunTerminalEvidence,
};
use musteragent_identity::{
    AgentId, ContextBranchId, ConversationId, DreamJobId, LearningSignalId, LedgerOffset,
    LedgerRecordId, MemoryVersionId, RunId, TenantId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_runtime_memory::{
    ClaimedDreamJob, ClaimedTerminalRunLearning, DreamJobCreationDisposition, DreamLearningInput,
    LearningPersistenceDisposition, LearningWorkerFailureClass, LearningWorkflowStore,
    PendingDreamSchedule, RunLearningEvidenceSource, TerminalRunLearningInput,
    automatic_dream_collection_id, dream_maintenance_run_id,
};
use musteragent_runtime_snapshot::SnapshotCodec as _;
use sqlx::Row as _;
use thiserror::Error;
use uuid::Uuid;

use crate::PostgresMemoryEvolutionStore;
use crate::memory::{PostgresMemoryEvolutionError, propose_transaction};

const COMPLETED_OUTPUT_RECOVERY_ACTIVE: &str =
    "musteragent.legacy.dream.completed-output-recovery@1";
const COMPLETED_OUTPUT_RECOVERY_FAILED: &str =
    "musteragent.legacy.dream.completed-output-recovery-failed@1";
const DREAM_SUBMISSION_WIRE_NAME: &str = "submit_experience_candidates";
const LAST_MANUAL_REQUEST_GENERATION_SQL: &str =
    "SELECT COALESCE(max(manual_request_generation)::text, '0')
     FROM evolution.dream_jobs
     WHERE tenant_id=$1 AND target_agent_id=$2";
const AUTOMATIC_DREAM_JOBS_LAST_24H_SQL: &str = "SELECT count(*) FROM evolution.dream_jobs AS job
     WHERE job.tenant_id=$1
       AND job.target_agent_id=$2
       AND job.created_at_ms >= $3
       AND job.manual_request_generation = 0
       AND (
         job.status = 3
         OR EXISTS (
           SELECT 1
           FROM runtime.evolution_maintenance_executions AS execution
           JOIN runtime.model_invocations AS invocation
             ON invocation.invocation_id = execution.invocation_id
           WHERE execution.dream_job_id = job.dream_job_id
             AND invocation.provider_dispatched_at_ms IS NOT NULL
         )
       )";

impl LearningWorkflowStore for PostgresMemoryEvolutionStore {
    type Error = PostgresLearningError;

    fn claim_terminal_run(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<ClaimedTerminalRunLearning>, Self::Error>,
    > {
        Box::pin(async move {
            if worker_id.is_empty() || lease_expires_at <= now {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            sqlx::query(
                "UPDATE evolution.learning_run_queue
                 SET status=4,lease_owner=NULL,lease_expires_at_ms=NULL,
                     updated_at_ms=$1,last_error_code='musteragent.legacy.learning.lease-exhausted@1'
                 WHERE status=2 AND lease_expires_at_ms <= $1 AND attempt_count >= 10",
            )
            .bind(now.unix_millis())
            .execute(&mut *tx)
            .await?;
            let row = sqlx::query(
                "SELECT queued.run_id,queued.tenant_id,queued.conversation_id,
                        queued.ledger_watermark::text AS ledger_watermark,queued.attempt_count
                 FROM evolution.learning_run_queue AS queued
                 JOIN runtime.runs AS run ON run.run_id=queued.run_id
                 WHERE run.activity_kind BETWEEN 1 AND 4 AND (
                       (queued.status=1 AND queued.available_at_ms <= $1 AND queued.attempt_count < 10)
                    OR (queued.status=2 AND queued.lease_expires_at_ms <= $1 AND queued.attempt_count < 10))
                 ORDER BY queued.available_at_ms,queued.run_id
                 FOR UPDATE SKIP LOCKED LIMIT 1",
            )
            .bind(now.unix_millis())
            .fetch_optional(&mut *tx)
            .await?;
            let Some(row) = row else {
                tx.rollback().await?;
                return Ok(None);
            };
            let run_id: Uuid = row.try_get("run_id")?;
            let prior_attempts: i32 = row.try_get("attempt_count")?;
            let next_attempt = prior_attempts
                .checked_add(1)
                .ok_or(PostgresLearningError::Corrupt)?;
            let updated = sqlx::query(
                "UPDATE evolution.learning_run_queue
                 SET status=2,attempt_count=$2,lease_owner=$3,lease_expires_at_ms=$4,
                     updated_at_ms=$1,last_error_code=NULL
                 WHERE run_id=$5 AND attempt_count=$6 AND status IN (1,2)",
            )
            .bind(now.unix_millis())
            .bind(next_attempt)
            .bind(&worker_id)
            .bind(lease_expires_at.unix_millis())
            .bind(run_id)
            .bind(prior_attempts)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresLearningError::StaleLease);
            }
            let claimed = ClaimedTerminalRunLearning {
                tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                    .map_err(|_| PostgresLearningError::Corrupt)?,
                conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
                    .map_err(|_| PostgresLearningError::Corrupt)?,
                run_id: RunId::from_uuid(run_id).map_err(|_| PostgresLearningError::Corrupt)?,
                ledger_watermark: LedgerOffset::new(parse_u64(row.try_get("ledger_watermark")?)?),
                attempt_count: u16::try_from(next_attempt)?,
                lease_owner: worker_id,
                lease_expires_at,
            };
            claimed
                .validate()
                .map_err(|_| PostgresLearningError::Corrupt)?;
            tx.commit().await?;
            Ok(Some(claimed))
        })
    }

    fn complete_terminal_run(
        &self,
        claimed: ClaimedTerminalRunLearning,
        signals: Vec<LearningSignal>,
        completed_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            claimed
                .validate()
                .map_err(|_| PostgresLearningError::InvalidCommand)?;
            validate_terminal_signal_set(&claimed, &signals, completed_at)?;
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            let status: Option<i16> = sqlx::query_scalar(
                "SELECT status FROM evolution.learning_run_queue WHERE run_id=$1 FOR UPDATE",
            )
            .bind(claimed.run_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?;
            if status == Some(3) {
                let exact = terminal_signal_links_match(&mut tx, claimed.run_id, &signals).await?;
                tx.rollback().await?;
                return if exact {
                    Ok(LearningPersistenceDisposition::Duplicate)
                } else {
                    Err(PostgresLearningError::Conflict)
                };
            }
            let authorized: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM evolution.learning_run_queue
                 WHERE run_id=$1 AND tenant_id=$2 AND conversation_id=$3
                   AND ledger_watermark=$4::numeric AND status=2 AND lease_owner=$5
                   AND lease_expires_at_ms=$6 AND lease_expires_at_ms >= $7)",
            )
            .bind(claimed.run_id.into_uuid())
            .bind(claimed.tenant_id.into_uuid())
            .bind(claimed.conversation_id.into_uuid())
            .bind(claimed.ledger_watermark.get().to_string())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .bind(completed_at.unix_millis())
            .fetch_one(&mut *tx)
            .await?;
            if !authorized {
                return Err(PostgresLearningError::StaleLease);
            }
            for (ordinal, signal) in signals.iter().enumerate() {
                record_signal_transaction(&mut tx, signal).await?;
                sqlx::query(
                    "INSERT INTO evolution.learning_run_signals
                     (run_id,signal_id,signal_ordinal) VALUES ($1,$2,$3)",
                )
                .bind(claimed.run_id.into_uuid())
                .bind(signal.signal_id.into_uuid())
                .bind(i32::try_from(ordinal)?)
                .execute(&mut *tx)
                .await?;
            }
            let updated = sqlx::query(
                "UPDATE evolution.learning_run_queue
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     updated_at_ms=$2,completed_at_ms=$2
                 WHERE run_id=$1 AND status=2 AND lease_owner=$3 AND lease_expires_at_ms=$4",
            )
            .bind(claimed.run_id.into_uuid())
            .bind(completed_at.unix_millis())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresLearningError::StaleLease);
            }
            insert_terminal_learning_outbox(&mut tx, &claimed, &signals, completed_at).await?;
            tx.commit().await?;
            Ok(LearningPersistenceDisposition::Applied)
        })
    }

    fn fail_terminal_run(
        &self,
        claimed: ClaimedTerminalRunLearning,
        failure_class: LearningWorkerFailureClass,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            claimed
                .validate()
                .map_err(|_| PostgresLearningError::InvalidCommand)?;
            if retry_at < failed_at || failed_at > claimed.lease_expires_at {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let updated = sqlx::query(
                "UPDATE evolution.learning_run_queue
                 SET status=CASE WHEN NOT $7 OR attempt_count >= 10 THEN 4 ELSE 1 END,
                     available_at_ms=$5,lease_owner=NULL,lease_expires_at_ms=NULL,
                     last_error_code=$6,updated_at_ms=$4
                 WHERE run_id=$1 AND status=2 AND lease_owner=$2 AND lease_expires_at_ms=$3",
            )
            .bind(claimed.run_id.into_uuid())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .bind(failed_at.unix_millis())
            .bind(retry_at.unix_millis())
            .bind(failure_class.terminal_durable_error_code())
            .bind(failure_class.retryable())
            .execute(&self.pool)
            .await?;
            if updated.rows_affected() == 1 {
                Ok(LearningPersistenceDisposition::Applied)
            } else {
                Err(PostgresLearningError::StaleLease)
            }
        })
    }

    fn record_signal(
        &self,
        signal: LearningSignal,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            let disposition = record_signal_transaction(&mut tx, &signal).await?;
            if disposition == LearningPersistenceDisposition::Duplicate {
                tx.rollback().await?;
                return Ok(disposition);
            }
            tx.commit().await?;
            Ok(disposition)
        })
    }

    fn create_dream_job(
        &self,
        job: DreamJob,
        signal_ids: BTreeSet<LearningSignalId>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<DreamJobCreationDisposition, Self::Error>,
    > {
        Box::pin(async move {
            job.validate()?;
            if job.status != DreamJobStatus::Pending || signal_ids.is_empty() {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(format!(
                    "musteragent.legacy.dream-lane:{}:{}",
                    job.tenant_id, job.target_agent_id
                ))
                .execute(&mut *tx)
                .await?;
            let active_job_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT dream_job_id FROM evolution.dream_jobs
                 WHERE tenant_id=$1 AND target_agent_id=$2 AND status IN (1,2)
                 ORDER BY created_at_ms,dream_job_id LIMIT 1",
            )
            .bind(job.tenant_id.into_uuid())
            .bind(job.target_agent_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(active_job_id) = active_job_id {
                if active_job_id == job.dream_job_id.into_uuid() {
                    let persisted = load_input(&mut tx, active_job_id).await?;
                    let persisted_ids = persisted
                        .signals
                        .iter()
                        .map(|value| value.signal_id)
                        .collect::<BTreeSet<_>>();
                    tx.rollback().await?;
                    return if persisted.job == job && persisted_ids == signal_ids {
                        Ok(DreamJobCreationDisposition::Duplicate)
                    } else {
                        Err(PostgresLearningError::Conflict)
                    };
                }
                tx.rollback().await?;
                return Ok(DreamJobCreationDisposition::LaneBusy {
                    active_dream_job_id: DreamJobId::from_uuid(active_job_id)
                        .map_err(|_| PostgresLearningError::Corrupt)?,
                });
            }
            authorize_content(&mut tx, job.tenant_id, &job.input_manifest_ref).await?;
            let ids = signal_ids
                .iter()
                .map(|value| value.into_uuid())
                .collect::<Vec<_>>();
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM evolution.learning_signals
                 WHERE tenant_id=$1 AND target_agent_id=$2 AND signal_id=ANY($3)
                   AND NOT EXISTS (
                       SELECT 1 FROM evolution.learning_signal_exclusions AS excluded
                       WHERE excluded.signal_id=evolution.learning_signals.signal_id
                   )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM evolution.learning_signal_evidence AS evidence
                       JOIN runtime.runs AS run ON run.run_id=evidence.run_id
                       WHERE evidence.signal_id=evolution.learning_signals.signal_id
                         AND run.activity_kind NOT BETWEEN 1 AND 4
                   )",
            )
            .bind(job.tenant_id.into_uuid())
            .bind(job.target_agent_id.into_uuid())
            .bind(&ids)
            .fetch_one(&mut *tx)
            .await?;
            if usize::try_from(count)? != signal_ids.len() {
                return Err(PostgresLearningError::InvalidEvidence);
            }
            let inserted = insert_job(&mut tx, &job).await?;
            if inserted == 0 {
                let persisted = load_input(&mut tx, job.dream_job_id.into_uuid()).await?;
                let persisted_ids = persisted
                    .signals
                    .iter()
                    .map(|value| value.signal_id)
                    .collect::<BTreeSet<_>>();
                tx.rollback().await?;
                return if persisted.job == job && persisted_ids == signal_ids {
                    Ok(DreamJobCreationDisposition::Duplicate)
                } else {
                    Err(PostgresLearningError::Conflict)
                };
            }
            for (ordinal, signal_id) in signal_ids.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO evolution.dream_job_signals
                     (dream_job_id,signal_id,signal_ordinal) VALUES ($1,$2,$3)",
                )
                .bind(job.dream_job_id.into_uuid())
                .bind(signal_id.into_uuid())
                .bind(i32::try_from(ordinal)?)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(DreamJobCreationDisposition::Applied)
        })
    }

    fn claim_dream_job(
        &self,
        worker_id: String,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<ClaimedDreamJob>, Self::Error>,
    > {
        Box::pin(async move {
            if worker_id.is_empty() || lease_expires_at <= now {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            sqlx::query(
                "UPDATE evolution.dream_jobs
                 SET status=4,retry_count=10,lease_owner=NULL,lease_expires_at_ms=NULL,
                     last_error_code='musteragent.legacy.dream.lease-exhausted@1'
                 WHERE status=2 AND lease_expires_at_ms <= $1 AND retry_count >= 9",
            )
            .bind(now.unix_millis())
            .execute(&mut *tx)
            .await?;
            let id: Option<Uuid> = sqlx::query_scalar(
                "SELECT dream_job_id FROM evolution.dream_jobs
                 WHERE (status=1 AND available_at_ms <= $1 AND retry_count < 10)
                    OR (status=2 AND lease_expires_at_ms <= $1 AND retry_count < 9)
                 ORDER BY available_at_ms,dream_job_id FOR UPDATE SKIP LOCKED LIMIT 1",
            )
            .bind(now.unix_millis())
            .fetch_optional(&mut *tx)
            .await?;
            let (id, completed_output_recovery) = match id {
                Some(id) => (id, false),
                None => match recoverable_completed_dream_job(&mut tx).await? {
                    Some(id) => (id, true),
                    None => {
                        tx.rollback().await?;
                        return Ok(None);
                    }
                },
            };
            let updated = if completed_output_recovery {
                sqlx::query(
                    "UPDATE evolution.dream_jobs
                     SET status=2,lease_owner=$2,lease_expires_at_ms=$3,
                         last_error_code=$4
                     WHERE dream_job_id=$1 AND status=4
                       AND last_error_code='musteragent.legacy.dream.contract-invalid@1'",
                )
                .bind(id)
                .bind(&worker_id)
                .bind(lease_expires_at.unix_millis())
                .bind(COMPLETED_OUTPUT_RECOVERY_ACTIVE)
                .execute(&mut *tx)
                .await?
            } else {
                sqlx::query(
                    "UPDATE evolution.dream_jobs
                     SET status=2,lease_owner=$2,lease_expires_at_ms=$3,
                         retry_count=retry_count+CASE WHEN status=2 THEN 1 ELSE 0 END,
                         last_error_code=NULL
                     WHERE dream_job_id=$1
                       AND (status=1 OR (status=2 AND lease_expires_at_ms <= $4))
                       AND ((status=1 AND retry_count < 10) OR (status=2 AND retry_count < 9))",
                )
                .bind(id)
                .bind(&worker_id)
                .bind(lease_expires_at.unix_millis())
                .bind(now.unix_millis())
                .execute(&mut *tx)
                .await?
            };
            if updated.rows_affected() != 1 {
                tx.rollback().await?;
                return Ok(None);
            }
            let input = load_input(&mut tx, id).await?;
            tx.commit().await?;
            Ok(Some(ClaimedDreamJob {
                input,
                lease_owner: worker_id,
                lease_expires_at,
            }))
        })
    }

    fn complete_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        candidates: Vec<ExperienceCandidate>,
        completed_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            validate_completion(&claimed, &candidates, completed_at)?;
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Learning)
                .await?;
            let status: Option<i16> = sqlx::query_scalar(
                "SELECT status FROM evolution.dream_jobs WHERE dream_job_id=$1 FOR UPDATE",
            )
            .bind(claimed.input.job.dream_job_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?;
            if status == Some(3) {
                let exact =
                    candidate_ids_match(&mut tx, claimed.input.job.dream_job_id, &candidates)
                        .await?;
                tx.rollback().await?;
                return if exact {
                    Ok(LearningPersistenceDisposition::Duplicate)
                } else {
                    Err(PostgresLearningError::Conflict)
                };
            }
            if status != Some(2) {
                return Err(PostgresLearningError::StaleLease);
            }
            for (ordinal, candidate) in candidates.iter().enumerate() {
                if propose_transaction(&mut tx, candidate).await?
                    != musteragent_runtime_memory::MemoryPersistenceDisposition::Applied
                {
                    return Err(PostgresLearningError::Conflict);
                }
                sqlx::query(
                    "INSERT INTO evolution.dream_job_candidates
                     (dream_job_id,proposal_id,proposal_ordinal) VALUES ($1,$2,$3)",
                )
                .bind(claimed.input.job.dream_job_id.into_uuid())
                .bind(candidate.proposal_id.into_uuid())
                .bind(i32::try_from(ordinal)?)
                .execute(&mut *tx)
                .await?;
            }
            let updated = sqlx::query(
                "UPDATE evolution.dream_jobs
                 SET status=3,lease_owner=NULL,lease_expires_at_ms=NULL,
                     completed_at_ms=$2,last_error_code=NULL
                 WHERE dream_job_id=$1 AND status=2 AND lease_owner=$3
                   AND lease_expires_at_ms=$4 AND lease_expires_at_ms >= $2",
            )
            .bind(claimed.input.job.dream_job_id.into_uuid())
            .bind(completed_at.unix_millis())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresLearningError::StaleLease);
            }
            insert_outbox(&mut tx, &claimed, &candidates, completed_at).await?;
            tx.commit().await?;
            Ok(LearningPersistenceDisposition::Applied)
        })
    }

    fn fail_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        failure_class: musteragent_runtime_memory::LearningWorkerFailureClass,
        failed_at: UtcTimestamp,
        retry_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            if retry_at <= failed_at {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let retryable = failure_class.retryable();
            let updated = sqlx::query(
                "UPDATE evolution.dream_jobs
                 SET retry_count=retry_count+1,
                     status=CASE WHEN NOT $5 OR retry_count+1 >= 10 THEN 4 ELSE 1 END,
                     available_at_ms=$6,
                     last_error_code=CASE
                         WHEN last_error_code=$7 THEN $8 ELSE $9 END,
                     lease_owner=NULL,lease_expires_at_ms=NULL
                 WHERE dream_job_id=$1 AND status=2 AND lease_owner=$2
                   AND lease_expires_at_ms=$3 AND lease_expires_at_ms >= $4",
            )
            .bind(claimed.input.job.dream_job_id.into_uuid())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .bind(failed_at.unix_millis())
            .bind(retryable)
            .bind(retry_at.unix_millis())
            .bind(COMPLETED_OUTPUT_RECOVERY_ACTIVE)
            .bind(COMPLETED_OUTPUT_RECOVERY_FAILED)
            .bind(failure_class.durable_error_code())
            .execute(&self.pool)
            .await?;
            if updated.rows_affected() == 1 {
                Ok(LearningPersistenceDisposition::Applied)
            } else {
                Err(PostgresLearningError::StaleLease)
            }
        })
    }
    fn suspend_dream_job(
        &self,
        claimed: ClaimedDreamJob,
        suspended_at: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<LearningPersistenceDisposition, Self::Error>,
    > {
        Box::pin(async move {
            let updated = sqlx::query(
                "UPDATE evolution.dream_jobs
                 SET status=CASE WHEN last_error_code=$5 THEN 4 ELSE 1 END,
                     last_error_code=CASE WHEN last_error_code=$5
                         THEN 'musteragent.legacy.dream.contract-invalid@1' ELSE NULL END,
                     available_at_ms=$4,lease_owner=NULL,lease_expires_at_ms=NULL
                 WHERE dream_job_id=$1 AND status=2 AND lease_owner=$2
                   AND lease_expires_at_ms=$3 AND lease_expires_at_ms >= $4",
            )
            .bind(claimed.input.job.dream_job_id.into_uuid())
            .bind(&claimed.lease_owner)
            .bind(claimed.lease_expires_at.unix_millis())
            .bind(suspended_at.unix_millis())
            .bind(COMPLETED_OUTPUT_RECOVERY_ACTIVE)
            .execute(&self.pool)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(PostgresLearningError::StaleLease);
            }
            Ok(LearningPersistenceDisposition::Applied)
        })
    }
}

/// Finds a failed Dream whose deterministic Maintenance Run and accepted Tool
/// output are already terminal. The returned Job can be reclaimed once to
/// finish validation/publication without issuing another Provider request.
async fn recoverable_completed_dream_job(
    transaction: &mut sqlx::PgConnection,
) -> Result<Option<Uuid>, PostgresLearningError> {
    let candidates: Vec<Uuid> = sqlx::query_scalar(
        "SELECT dream_job_id FROM evolution.dream_jobs
         WHERE status = 4
           AND last_error_code = 'musteragent.legacy.dream.contract-invalid@1'
         ORDER BY available_at_ms, dream_job_id
         FOR UPDATE SKIP LOCKED
         LIMIT 32",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for candidate in candidates {
        let dream_job_id =
            DreamJobId::from_uuid(candidate).map_err(|_| PostgresLearningError::Corrupt)?;
        let run_id =
            dream_maintenance_run_id(dream_job_id).map_err(|_| PostgresLearningError::Corrupt)?;
        let has_completed_output: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1
                 FROM runtime.runs AS run
                 JOIN runtime.pending_tool_calls AS pending
                   ON pending.run_id = run.run_id
                 WHERE run.run_id = $1 AND run.activity_kind = 5 AND run.status = 5
                   AND pending.completed_at_ms IS NOT NULL
                   AND pending.wire_name = $2
                   AND pending.result_content_id IS NOT NULL
             )",
        )
        .bind(run_id.into_uuid())
        .bind(DREAM_SUBMISSION_WIRE_NAME)
        .fetch_one(&mut *transaction)
        .await?;
        if has_completed_output {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

impl RunLearningEvidenceSource for PostgresMemoryEvolutionStore {
    type Error = PostgresLearningError;

    fn classify_error(error: &Self::Error) -> LearningWorkerFailureClass {
        match error {
            PostgresLearningError::Database(_)
            | PostgresLearningError::Transaction(_)
            | PostgresLearningError::Memory(_) => LearningWorkerFailureClass::DependencyUnavailable,
            PostgresLearningError::InvalidCommand
            | PostgresLearningError::InvalidEvidence
            | PostgresLearningError::ContentIntegrity
            | PostgresLearningError::StaleLease
            | PostgresLearningError::NotFound
            | PostgresLearningError::Conflict
            | PostgresLearningError::Corrupt
            | PostgresLearningError::Contract(_)
            | PostgresLearningError::Evolution(_)
            | PostgresLearningError::Json(_)
            | PostgresLearningError::Numeric(_) => LearningWorkerFailureClass::ContractInvalid,
        }
    }

    fn load_terminal_run<'a>(
        &'a self,
        claimed: &'a ClaimedTerminalRunLearning,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<Option<TerminalRunLearningInput>, Self::Error>,
    > {
        Box::pin(async move {
            claimed
                .validate()
                .map_err(|_| PostgresLearningError::InvalidCommand)?;
            let run = sqlx::query(
                "SELECT run.tenant_id,run.conversation_id,run.activity_kind,run.status,run.finished_at_ms,
                        participant.agent_ref,state.active_branch_id
                 FROM runtime.runs AS run
                 JOIN conversation.participants AS participant
                   ON participant.participant_id=run.executor_participant_id
                  AND participant.tenant_id=run.tenant_id
                  AND participant.conversation_id=run.conversation_id
                 LEFT JOIN conversation.context_branch_state AS state
                   ON state.tenant_id=run.tenant_id
                  AND state.conversation_id=run.conversation_id
                 WHERE run.run_id=$1",
            )
            .bind(claimed.run_id.into_uuid())
            .fetch_optional(&self.pool)
            .await?;
            let Some(run) = run else {
                return Ok(None);
            };
            let tenant_id = TenantId::from_uuid(run.try_get("tenant_id")?)
                .map_err(|_| PostgresLearningError::Corrupt)?;
            let conversation_id = ConversationId::from_uuid(run.try_get("conversation_id")?)
                .map_err(|_| PostgresLearningError::Corrupt)?;
            let status: i16 = run.try_get("status")?;
            let activity_kind = decode_learning_activity_kind(run.try_get("activity_kind")?)?;
            let Some(agent_uuid) = run.try_get::<Option<Uuid>, _>("agent_ref")? else {
                return Ok(None);
            };
            if tenant_id != claimed.tenant_id
                || conversation_id != claimed.conversation_id
                || !activity_kind.is_ordinary_learning_eligible()
                || !(5..=8).contains(&status)
            {
                return Err(PostgresLearningError::InvalidEvidence);
            }
            let active_branch_uuid: Option<Uuid> = run.try_get("active_branch_id")?;
            let records = sqlx::query(
                "SELECT record_id,payload_hash,conversation_seq::text AS conversation_seq,
                        context_branch_id
                 FROM event_store.ledger_records
                 WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3
                   AND conversation_seq <= $4::bigint
                   AND ($5::uuid IS NULL OR context_branch_id IS NULL OR context_branch_id=$5)
                 ORDER BY conversation_seq,record_id",
            )
            .bind(claimed.tenant_id.into_uuid())
            .bind(claimed.conversation_id.into_uuid())
            .bind(claimed.run_id.into_uuid())
            .bind(claimed.ledger_watermark.get().to_string())
            .bind(active_branch_uuid)
            .fetch_all(&self.pool)
            .await?;
            if records.is_empty() {
                return Err(PostgresLearningError::InvalidEvidence);
            }
            let mut record_ids = Vec::with_capacity(records.len());
            let mut record_hashes = Vec::with_capacity(records.len());
            let mut first_sequence = None;
            let mut last_sequence = 0_u64;
            let mut has_active_branch_record = false;
            for record in records {
                let sequence = parse_u64(record.try_get("conversation_seq")?)?;
                first_sequence.get_or_insert(sequence);
                last_sequence = sequence;
                has_active_branch_record |= record
                    .try_get::<Option<Uuid>, _>("context_branch_id")?
                    .is_some_and(|value| Some(value) == active_branch_uuid);
                record_ids.push(
                    LedgerRecordId::from_uuid(record.try_get("record_id")?)
                        .map_err(|_| PostgresLearningError::Corrupt)?,
                );
                record_hashes.push(parse_hash(record.try_get("payload_hash")?)?);
            }
            let first_sequence = first_sequence.ok_or(PostgresLearningError::InvalidEvidence)?;
            let branch_id = if has_active_branch_record {
                active_branch_uuid
                    .map(ContextBranchId::from_uuid)
                    .transpose()
                    .map_err(|_| PostgresLearningError::Corrupt)?
            } else {
                None
            };
            let observed_user_instruction: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM conversation.messages
                 WHERE tenant_id=$1 AND conversation_id=$2 AND run_id=$3 AND role=1
                   AND committed_sequence <= $4::bigint)",
            )
            .bind(claimed.tenant_id.into_uuid())
            .bind(claimed.conversation_id.into_uuid())
            .bind(claimed.run_id.into_uuid())
            .bind(claimed.ledger_watermark.get().to_string())
            .fetch_one(&self.pool)
            .await?;
            let observed_user_correction = if let Some(branch_uuid) = active_branch_uuid {
                sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM conversation.context_branches
                     WHERE branch_id=$1 AND tenant_id=$2 AND conversation_id=$3 AND origin=3)",
                )
                .bind(branch_uuid)
                .bind(claimed.tenant_id.into_uuid())
                .bind(claimed.conversation_id.into_uuid())
                .fetch_one(&self.pool)
                .await?
            } else {
                false
            };
            let observed_tool_failure: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tool.tool_invocations
                 WHERE tenant_id=$1 AND run_id=$2 AND status=5)",
            )
            .bind(claimed.tenant_id.into_uuid())
            .bind(claimed.run_id.into_uuid())
            .fetch_one(&self.pool)
            .await?;
            let observed_tool_success: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tool.tool_invocations
                 WHERE tenant_id=$1 AND run_id=$2 AND status=4)",
            )
            .bind(claimed.tenant_id.into_uuid())
            .bind(claimed.run_id.into_uuid())
            .fetch_one(&self.pool)
            .await?;
            let (terminal, outcome) = terminal_evidence(status)?;
            let evidence = EvidenceSpan {
                conversation_id: claimed.conversation_id,
                run_id: claimed.run_id,
                branch_id,
                branch_state: BranchEvidenceState::Active,
                cursor_from: LedgerOffset::new(first_sequence.saturating_sub(1)),
                cursor_to: LedgerOffset::new(last_sequence),
                record_ids,
                record_hashes,
                terminal,
                outcome,
            };
            evidence.validate()?;
            Ok(Some(TerminalRunLearningInput {
                tenant_id: claimed.tenant_id,
                run_id: claimed.run_id,
                activity_kind,
                target_agent_id: AgentId::from_uuid(agent_uuid)
                    .map_err(|_| PostgresLearningError::Corrupt)?,
                evidence,
                observed_user_correction,
                observed_user_instruction,
                observed_tool_failure,
                observed_tool_success,
                created_at: UtcTimestamp::from_unix_millis(
                    run.try_get::<Option<i64>, _>("finished_at_ms")?
                        .ok_or(PostgresLearningError::Corrupt)?,
                ),
            }))
        })
    }

    fn load_pending_dream_schedule(
        &self,
        max_signals: u16,
        now: UtcTimestamp,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Option<PendingDreamSchedule>, Self::Error>,
    > {
        Box::pin(async move {
            if max_signals == 0 || max_signals > 256 {
                return Err(PostgresLearningError::InvalidCommand);
            }
            let rows = sqlx::query(
                "WITH pending_signal AS (
                     SELECT signal.signal_id,signal.tenant_id,signal.target_agent_id,
                            signal.created_at_ms,evidence.run_id,
                            evidence.cursor_to::text AS cursor_to
                     FROM evolution.learning_signals AS signal
                     JOIN evolution.learning_signal_evidence AS evidence
                       ON evidence.signal_id=signal.signal_id AND evidence.span_ordinal=0
                     JOIN runtime.runs AS run ON run.run_id=evidence.run_id
                     WHERE NOT EXISTS (
                         SELECT 1 FROM evolution.dream_job_signals AS assigned
                         WHERE assigned.signal_id=signal.signal_id
                     )
                       AND NOT EXISTS (
                           SELECT 1 FROM evolution.learning_signal_exclusions AS excluded
                           WHERE excluded.signal_id=signal.signal_id
                       )
                       AND NOT EXISTS (
                           SELECT 1
                           FROM evolution.learning_signal_evidence AS other_evidence
                           JOIN runtime.runs AS other_run ON other_run.run_id=other_evidence.run_id
                           WHERE other_evidence.signal_id=signal.signal_id
                             AND other_run.activity_kind NOT BETWEEN 1 AND 4
                       )
                       AND run.activity_kind BETWEEN 1 AND 4
                 ), first_signal AS (
                     SELECT tenant_id,target_agent_id
                     FROM pending_signal
                     ORDER BY created_at_ms,signal_id
                     LIMIT 1
                 )
                 SELECT signal.signal_id,signal.tenant_id,signal.target_agent_id,
                        signal.created_at_ms,signal.run_id,signal.cursor_to
                 FROM pending_signal AS signal
                 JOIN first_signal AS first
                   ON first.tenant_id=signal.tenant_id
                  AND first.target_agent_id=signal.target_agent_id
                 ORDER BY signal.created_at_ms,signal.signal_id
                 LIMIT $1",
            )
            .bind(i64::from(max_signals))
            .fetch_all(&self.pool)
            .await?;
            let Some(first) = rows.first() else {
                return Ok(None);
            };
            let tenant_id = TenantId::from_uuid(first.try_get("tenant_id")?)
                .map_err(|_| PostgresLearningError::Corrupt)?;
            let target_agent_id = AgentId::from_uuid(first.try_get("target_agent_id")?)
                .map_err(|_| PostgresLearningError::Corrupt)?;
            let mut agent_version_id = None;
            let mut resolved_collection_id = None;
            let mut signal_ids = BTreeSet::new();
            let mut source_run_ids = BTreeSet::new();
            let mut watermark = 0_u64;
            let mut oldest_signal_at_ms = i64::MAX;
            let mut newest_signal_at_ms = i64::MIN;
            for row in rows {
                let run_id: Uuid = row.try_get("run_id")?;
                let snapshot = load_postgres_run_snapshot(&self.pool, tenant_id, run_id).await?;
                if snapshot.agent.agent_id != target_agent_id {
                    return Err(PostgresLearningError::InvalidEvidence);
                }
                if agent_version_id.is_some_and(|value| value != snapshot.agent.agent_version_id) {
                    continue;
                }
                agent_version_id.get_or_insert(snapshot.agent.agent_version_id);
                resolved_collection_id.get_or_insert_with(|| {
                    snapshot.agent.resolved_v5.as_ref().and_then(|resolved| {
                        resolved
                            .long_term_memory_collection_id
                            .or(resolved.user_memory_collection_id)
                    })
                });
                signal_ids.insert(
                    LearningSignalId::from_uuid(row.try_get("signal_id")?)
                        .map_err(|_| PostgresLearningError::Corrupt)?,
                );
                source_run_ids.insert(run_id);
                watermark = watermark.max(parse_u64(row.try_get("cursor_to")?)?);
                let signal_at_ms: i64 = row.try_get("created_at_ms")?;
                oldest_signal_at_ms = oldest_signal_at_ms.min(signal_at_ms);
                newest_signal_at_ms = newest_signal_at_ms.max(signal_at_ms);
            }
            let Some(agent_version_id) = agent_version_id else {
                return Err(PostgresLearningError::InvalidEvidence);
            };
            let memory_ids: Vec<Uuid> = sqlx::query_scalar(
                "SELECT DISTINCT memory_version_id FROM memory.memory_refs
                 WHERE tenant_id=$1 AND run_id=ANY($2) ORDER BY memory_version_id",
            )
            .bind(tenant_id.into_uuid())
            .bind(source_run_ids.iter().copied().collect::<Vec<_>>())
            .fetch_all(&self.pool)
            .await?;
            let memory_version_ids = memory_ids
                .into_iter()
                .map(|value| {
                    MemoryVersionId::from_uuid(value).map_err(|_| PostgresLearningError::Corrupt)
                })
                .collect::<Result<BTreeSet<_>, _>>()?;
            let memory_collection_id = resolved_collection_id
                .flatten()
                .map_or_else(
                    || automatic_dream_collection_id(tenant_id, target_agent_id),
                    Ok,
                )
                .map_err(|_| PostgresLearningError::Corrupt)?;
            let active_ordinary_run: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM runtime.runs AS run
                     JOIN conversation.participants AS participant
                       ON participant.tenant_id=run.tenant_id
                      AND participant.conversation_id=run.conversation_id
                      AND participant.participant_id=run.executor_participant_id
                     WHERE run.tenant_id=$1 AND participant.agent_ref=$2
                       AND run.activity_kind BETWEEN 1 AND 4
                       AND run.status BETWEEN 1 AND 4
                 )",
            )
            .bind(tenant_id.into_uuid())
            .bind(target_agent_id.into_uuid())
            .fetch_one(&self.pool)
            .await?;
            let active_dream_job: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM evolution.dream_jobs
                     WHERE tenant_id=$1 AND target_agent_id=$2 AND status IN (1,2)
                 )",
            )
            .bind(tenant_id.into_uuid())
            .bind(target_agent_id.into_uuid())
            .fetch_one(&self.pool)
            .await?;
            let jobs_created_last_24h: i64 = sqlx::query_scalar(AUTOMATIC_DREAM_JOBS_LAST_24H_SQL)
                .bind(tenant_id.into_uuid())
                .bind(target_agent_id.into_uuid())
                .bind(now.unix_millis().saturating_sub(86_400_000))
                .fetch_one(&self.pool)
                .await?;
            let last_dream_job_created_at_ms: Option<i64> = sqlx::query_scalar(
                "SELECT max(created_at_ms) FROM evolution.dream_jobs
                 WHERE tenant_id=$1 AND target_agent_id=$2",
            )
            .bind(tenant_id.into_uuid())
            .bind(target_agent_id.into_uuid())
            .fetch_one(&self.pool)
            .await?;
            // Runtime generations are unsigned domain values stored as
            // NUMERIC so PostgreSQL can represent the complete u64 range.
            // Keep the adapter boundary lossless by decoding through text;
            // SQLx deliberately does not treat NUMERIC as an i64.
            let last_manual_request_generation: String =
                sqlx::query_scalar(LAST_MANUAL_REQUEST_GENERATION_SQL)
                    .bind(tenant_id.into_uuid())
                    .bind(target_agent_id.into_uuid())
                    .fetch_one(&self.pool)
                    .await?;
            Ok(Some(PendingDreamSchedule {
                tenant_id,
                target_agent_id,
                agent_version_id,
                memory_collection_id,
                memory_collection_version: VersionNumber::new(watermark)
                    .map_err(|_| PostgresLearningError::Corrupt)?,
                ledger_watermark: LedgerOffset::new(watermark),
                signal_ids,
                memory_version_ids,
                oldest_signal_at: UtcTimestamp::from_unix_millis(oldest_signal_at_ms),
                newest_signal_at: UtcTimestamp::from_unix_millis(newest_signal_at_ms),
                source_run_count: u16::try_from(source_run_ids.len())?,
                active_ordinary_run,
                active_dream_job,
                jobs_created_last_24h: u16::try_from(jobs_created_last_24h)?,
                last_manual_request_generation: parse_u64(last_manual_request_generation)?,
                last_dream_job_created_at: last_dream_job_created_at_ms
                    .map(UtcTimestamp::from_unix_millis),
            }))
        })
    }
}

async fn load_postgres_run_snapshot(
    pool: &sqlx::PgPool,
    tenant_id: TenantId,
    run_id: Uuid,
) -> Result<musteragent_runtime_snapshot::RuntimeSnapshotContent, PostgresLearningError> {
    let row = sqlx::query(
        "SELECT encoded_snapshot,content_hash FROM capability.runtime_snapshots
         WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id)
    .fetch_optional(pool)
    .await?
    .ok_or(PostgresLearningError::InvalidEvidence)?;
    let bytes: Vec<u8> = row.try_get("encoded_snapshot")?;
    if Sha256::digest(&bytes) != parse_hash(row.try_get("content_hash")?)? {
        return Err(PostgresLearningError::InvalidEvidence);
    }
    ProtobufSnapshotCodec
        .decode_content(&bytes)
        .map_err(|_| PostgresLearningError::InvalidEvidence)
}

async fn record_signal_transaction(
    tx: &mut sqlx::PgConnection,
    signal: &LearningSignal,
) -> Result<LearningPersistenceDisposition, PostgresLearningError> {
    signal.validate()?;
    validate_evidence(tx, signal).await?;
    let inserted = sqlx::query(
        "INSERT INTO evolution.learning_signals (
             signal_id, tenant_id, target_agent_id, signal_kind,
             extractor_version_ref, policy_version_ref, dedup_key, created_at_ms
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (signal_id) DO NOTHING",
    )
    .bind(signal.signal_id.into_uuid())
    .bind(signal.tenant_id.into_uuid())
    .bind(signal.target_agent_id.into_uuid())
    .bind(signal_kind_code(signal.kind))
    .bind(signal.extractor_version_ref.as_str())
    .bind(signal.policy_version_ref.as_str())
    .bind(signal.dedup_key.into_bytes().as_slice())
    .bind(signal.created_at.unix_millis())
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return if load_signal(tx, signal.signal_id).await? == *signal {
            Ok(LearningPersistenceDisposition::Duplicate)
        } else {
            Err(PostgresLearningError::Conflict)
        };
    }
    insert_evidence(tx, signal).await?;
    Ok(LearningPersistenceDisposition::Applied)
}

fn validate_terminal_signal_set(
    claimed: &ClaimedTerminalRunLearning,
    signals: &[LearningSignal],
    completed_at: UtcTimestamp,
) -> Result<(), PostgresLearningError> {
    if signals.is_empty() || completed_at > claimed.lease_expires_at {
        return Err(PostgresLearningError::InvalidCommand);
    }
    let mut ids = BTreeSet::new();
    for signal in signals {
        signal.validate()?;
        if signal.tenant_id != claimed.tenant_id
            || !ids.insert(signal.signal_id)
            || signal.evidence.iter().any(|span| {
                span.run_id != claimed.run_id
                    || span.conversation_id != claimed.conversation_id
                    || span.cursor_to > claimed.ledger_watermark
            })
        {
            return Err(PostgresLearningError::InvalidEvidence);
        }
    }
    Ok(())
}

async fn terminal_signal_links_match(
    tx: &mut sqlx::PgConnection,
    run_id: RunId,
    signals: &[LearningSignal],
) -> Result<bool, PostgresLearningError> {
    let ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT signal_id FROM evolution.learning_run_signals
         WHERE run_id=$1 ORDER BY signal_ordinal",
    )
    .bind(run_id.into_uuid())
    .fetch_all(&mut *tx)
    .await?;
    Ok(ids
        == signals
            .iter()
            .map(|signal| signal.signal_id.into_uuid())
            .collect::<Vec<_>>())
}

async fn insert_terminal_learning_outbox(
    tx: &mut sqlx::PgConnection,
    claimed: &ClaimedTerminalRunLearning,
    signals: &[LearningSignal],
    completed_at: UtcTimestamp,
) -> Result<(), PostgresLearningError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema":"musteragent.legacy.learning.run-extracted.v1",
        "run_id":claimed.run_id.to_string(),
        "ledger_watermark":claimed.ledger_watermark.get(),
        "signal_ids":signals.iter().map(|signal| signal.signal_id.to_string()).collect::<Vec<_>>(),
    }))?;
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("learning-run-extracted:{}", claimed.run_id).as_bytes(),
    );
    sqlx::query(
        "INSERT INTO event_store.outbox
         (message_id,tenant_id,conversation_id,topic,payload,payload_hash,
          committed_at_ms,available_at_ms)
         VALUES ($1,$2,$3,'learning.run.extracted.v1',$4,$5,$6,$6)
         ON CONFLICT (message_id) DO NOTHING",
    )
    .bind(message_id)
    .bind(claimed.tenant_id.into_uuid())
    .bind(claimed.conversation_id.into_uuid())
    .bind(&payload)
    .bind(Sha256::digest(&payload).into_bytes().as_slice())
    .bind(completed_at.unix_millis())
    .execute(&mut *tx)
    .await?;
    Ok(())
}

fn terminal_evidence(
    status: i16,
) -> Result<(RunTerminalEvidence, LearningOutcome), PostgresLearningError> {
    match status {
        5 => Ok((RunTerminalEvidence::Completed, LearningOutcome::Success)),
        6 => Ok((RunTerminalEvidence::Failed, LearningOutcome::Failure)),
        7 => Ok((RunTerminalEvidence::Cancelled, LearningOutcome::Cancelled)),
        8 => Ok((
            RunTerminalEvidence::Interrupted,
            LearningOutcome::Interrupted,
        )),
        _ => Err(PostgresLearningError::InvalidEvidence),
    }
}

fn decode_learning_activity_kind(value: i16) -> Result<ActivityKind, PostgresLearningError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(PostgresLearningError::Corrupt),
    }
}

async fn validate_evidence(
    tx: &mut sqlx::PgConnection,
    signal: &LearningSignal,
) -> Result<(), PostgresLearningError> {
    for span in &signal.evidence {
        if span.branch_state == BranchEvidenceState::Active
            && let Some(branch_id) = span.branch_id
        {
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM conversation.context_branch_state
                     WHERE tenant_id=$1 AND conversation_id=$2 AND active_branch_id=$3)",
            )
            .bind(signal.tenant_id.into_uuid())
            .bind(span.conversation_id.into_uuid())
            .bind(branch_id.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
            if !active {
                return Err(PostgresLearningError::InvalidEvidence);
            }
        }
        for (record_id, hash) in span.record_ids.iter().zip(&span.record_hashes) {
            let exact: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1
                     FROM event_store.ledger_records AS record
                     JOIN evolution.learning_run_queue AS queued
                       ON queued.run_id=record.run_id
                      AND queued.tenant_id=record.tenant_id
                      AND queued.conversation_id=record.conversation_id
                     JOIN runtime.runs AS run
                       ON run.run_id=record.run_id
                      AND run.tenant_id=record.tenant_id
                     JOIN conversation.participants AS participant
                       ON participant.participant_id=run.executor_participant_id
                      AND participant.tenant_id=run.tenant_id
                      AND participant.conversation_id=run.conversation_id
                     WHERE record.record_id=$1
                       AND record.tenant_id=$2
                       AND record.conversation_id=$3
                       AND record.payload_hash=$4
                       AND record.run_id=$5
                       AND record.conversation_seq > $6::bigint
                       AND record.conversation_seq <= $7::bigint
                       AND queued.ledger_watermark >= $7::bigint
                       AND queued.terminal_status=$8
                       AND run.status=$8 + 4
                       AND run.activity_kind BETWEEN 1 AND 4
                       AND participant.agent_ref=$9
                       AND (($10::uuid IS NULL AND record.context_branch_id IS NULL)
                            OR ($10::uuid IS NOT NULL
                                AND (record.context_branch_id IS NULL
                                     OR record.context_branch_id=$10)))
                 )",
            )
            .bind(record_id.into_uuid())
            .bind(signal.tenant_id.into_uuid())
            .bind(span.conversation_id.into_uuid())
            .bind(hash.into_bytes().as_slice())
            .bind(span.run_id.into_uuid())
            .bind(span.cursor_from.get().to_string())
            .bind(span.cursor_to.get().to_string())
            .bind(terminal_code(span.terminal))
            .bind(signal.target_agent_id.into_uuid())
            .bind(span.branch_id.map(|value| value.into_uuid()))
            .fetch_one(&mut *tx)
            .await?;
            if !exact {
                return Err(PostgresLearningError::InvalidEvidence);
            }
        }
    }
    Ok(())
}

async fn insert_evidence(
    tx: &mut sqlx::PgConnection,
    signal: &LearningSignal,
) -> Result<(), PostgresLearningError> {
    for (span_ordinal, span) in signal.evidence.iter().enumerate() {
        sqlx::query(
            "INSERT INTO evolution.learning_signal_evidence
             (signal_id,span_ordinal,conversation_id,run_id,branch_id,branch_state,
              cursor_from,cursor_to,terminal_status,outcome)
             VALUES ($1,$2,$3,$4,$5,$6,$7::bigint,$8::bigint,$9,$10)",
        )
        .bind(signal.signal_id.into_uuid())
        .bind(i32::try_from(span_ordinal)?)
        .bind(span.conversation_id.into_uuid())
        .bind(span.run_id.into_uuid())
        .bind(span.branch_id.map(|value| value.into_uuid()))
        .bind(branch_state_code(span.branch_state))
        .bind(span.cursor_from.get().to_string())
        .bind(span.cursor_to.get().to_string())
        .bind(terminal_code(span.terminal))
        .bind(outcome_code(span.outcome))
        .execute(&mut *tx)
        .await?;
        for (record_ordinal, (record_id, hash)) in
            span.record_ids.iter().zip(&span.record_hashes).enumerate()
        {
            sqlx::query(
                "INSERT INTO evolution.learning_signal_evidence_records
                 (signal_id,span_ordinal,record_ordinal,record_id,record_hash)
                 VALUES ($1,$2,$3,$4,$5)",
            )
            .bind(signal.signal_id.into_uuid())
            .bind(i32::try_from(span_ordinal)?)
            .bind(i32::try_from(record_ordinal)?)
            .bind(record_id.into_uuid())
            .bind(hash.into_bytes().as_slice())
            .execute(&mut *tx)
            .await?;
        }
    }
    Ok(())
}

async fn insert_job(
    tx: &mut sqlx::PgConnection,
    job: &DreamJob,
) -> Result<u64, PostgresLearningError> {
    // manual_request_generation is an explicit full-u64 NUMERIC contract;
    // keeping the whole legacy row type-aligned also avoids an accidental
    // BIGINT narrowing before the column-by-column migration is approved.
    Ok(sqlx::query(
        "INSERT INTO evolution.dream_jobs (
         dream_job_id,tenant_id,target_agent_id,agent_version_id,memory_collection_id,
         memory_collection_version,ledger_watermark,input_manifest_content_id,
         input_manifest_media_type,input_manifest_byte_length,input_manifest_hash,
         policy_version_ref,growth_profile_ref,growth_profile_hash,manual_request_generation,
         model_route_ref,model_route_hash,
         model_timeout_ms,budget_input_tokens,budget_output_tokens,
         budget_max_proposals,idempotency_key,status,retry_count,lease_owner,
         lease_expires_at_ms,available_at_ms,last_error_code,created_at_ms,completed_at_ms)
         VALUES ($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8,$9,$10::numeric,$11,
                 $12,$13,$14,$15::numeric,$16,$17,$18::numeric,$19::numeric,$20::numeric,$21,$22,$23,$24,$25,$26,
                 $27,NULL,$28,$29)
         ON CONFLICT (dream_job_id) DO NOTHING",
    )
    .bind(job.dream_job_id.into_uuid())
    .bind(job.tenant_id.into_uuid())
    .bind(job.target_agent_id.into_uuid())
    .bind(job.agent_version_id.into_uuid())
    .bind(job.memory_collection_id.into_uuid())
    .bind(job.memory_collection_version.get().to_string())
    .bind(job.ledger_watermark.get().to_string())
    .bind(job.input_manifest_ref.content_id.into_uuid())
    .bind(job.input_manifest_ref.media_type.as_str())
    .bind(job.input_manifest_ref.byte_length.to_string())
    .bind(job.input_manifest_ref.sha256.into_bytes().as_slice())
    .bind(job.policy_version_ref.as_str())
    .bind(job.growth_profile_ref.as_str())
    .bind(job.growth_profile_hash.into_bytes().as_slice())
    .bind(job.manual_request_generation.to_string())
    .bind(job.model_route_ref.as_str())
    .bind(job.model_route_hash.into_bytes().as_slice())
    .bind(job.model_timeout_ms.to_string())
    .bind(job.workload.evidence_context_tokens.to_string())
    // Legacy v1 persistence column; ignored when reading new runtime state.
    .bind(job.workload.evidence_context_tokens.to_string())
    .bind(i32::from(job.workload.candidate_batch_capacity))
    .bind(job.idempotency_key.into_bytes().as_slice())
    .bind(dream_status_code(job.status))
    .bind(i32::from(job.retry_count))
    .bind(&job.lease_owner)
    .bind(job.lease_expires_at.map(UtcTimestamp::unix_millis))
    .bind(job.created_at.unix_millis())
    .bind(job.created_at.unix_millis())
    .bind(job.completed_at.map(UtcTimestamp::unix_millis))
    .execute(&mut *tx)
    .await?
    .rows_affected())
}

async fn authorize_content(
    tx: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), PostgresLearningError> {
    let exact: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM artifact.content_refs
         WHERE tenant_id=$1 AND content_id=$2 AND media_type=$3
           AND byte_length=$4::numeric AND content_hash=$5)",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .fetch_one(&mut *tx)
    .await?;
    if exact {
        Ok(())
    } else {
        Err(PostgresLearningError::ContentIntegrity)
    }
}

async fn load_input(
    tx: &mut sqlx::PgConnection,
    job_id: Uuid,
) -> Result<DreamLearningInput, PostgresLearningError> {
    let row = sqlx::query(
        "SELECT dream_job_id::text AS dream_job_id,tenant_id::text AS tenant_id,
         target_agent_id::text AS target_agent_id,agent_version_id::text AS agent_version_id,
         memory_collection_id::text AS memory_collection_id,
         memory_collection_version::text AS memory_collection_version,
         ledger_watermark::text AS ledger_watermark,
         input_manifest_content_id::text AS input_manifest_content_id,input_manifest_media_type,
         input_manifest_byte_length::text AS input_manifest_byte_length,input_manifest_hash,
         policy_version_ref,growth_profile_ref,growth_profile_hash,
         manual_request_generation::text AS manual_request_generation,model_route_ref,model_route_hash,
         model_timeout_ms::text AS model_timeout_ms,budget_input_tokens::text AS budget_input_tokens,
         budget_output_tokens::text AS budget_output_tokens,budget_max_proposals,
         idempotency_key,status,retry_count,lease_owner,lease_expires_at_ms,
         created_at_ms,completed_at_ms FROM evolution.dream_jobs WHERE dream_job_id=$1",
    )
    .bind(job_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(PostgresLearningError::NotFound)?;
    let job = DreamJob {
        dream_job_id: parse_id(row.try_get("dream_job_id")?)?,
        tenant_id: parse_id(row.try_get("tenant_id")?)?,
        target_agent_id: parse_id(row.try_get("target_agent_id")?)?,
        agent_version_id: parse_id(row.try_get("agent_version_id")?)?,
        memory_collection_id: parse_id(row.try_get("memory_collection_id")?)?,
        memory_collection_version: VersionNumber::new(parse_u64(
            row.try_get("memory_collection_version")?,
        )?)
        .map_err(|_| PostgresLearningError::Corrupt)?,
        ledger_watermark: LedgerOffset::new(parse_u64(row.try_get("ledger_watermark")?)?),
        input_manifest_ref: ContentRef {
            content_id: parse_id(row.try_get("input_manifest_content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("input_manifest_media_type")?)
                .map_err(|_| PostgresLearningError::Corrupt)?,
            byte_length: parse_u64(row.try_get("input_manifest_byte_length")?)?,
            sha256: parse_hash(row.try_get("input_manifest_hash")?)?,
        },
        policy_version_ref: VersionedKey::new(row.try_get::<String, _>("policy_version_ref")?)
            .map_err(|_| PostgresLearningError::Corrupt)?,
        growth_profile_ref: VersionedKey::new(row.try_get::<String, _>("growth_profile_ref")?)
            .map_err(|_| PostgresLearningError::Corrupt)?,
        growth_profile_hash: parse_hash(row.try_get("growth_profile_hash")?)?,
        manual_request_generation: parse_u64(row.try_get("manual_request_generation")?)?,
        model_route_ref: VersionedKey::new(row.try_get::<String, _>("model_route_ref")?)
            .map_err(|_| PostgresLearningError::Corrupt)?,
        model_route_hash: parse_hash(row.try_get("model_route_hash")?)?,
        model_timeout_ms: parse_u64(row.try_get("model_timeout_ms")?)?,
        workload: DreamWorkload {
            evidence_context_tokens: parse_u64(row.try_get("budget_input_tokens")?)?,
            candidate_batch_capacity: u16::try_from(
                row.try_get::<i32, _>("budget_max_proposals")?,
            )?,
        },
        idempotency_key: parse_hash(row.try_get("idempotency_key")?)?,
        status: decode_dream_status(row.try_get("status")?)?,
        retry_count: u16::try_from(row.try_get::<i32, _>("retry_count")?)?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row
            .try_get::<Option<i64>, _>("lease_expires_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
    };
    job.validate()?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT signal_id FROM evolution.dream_job_signals
         WHERE dream_job_id=$1 ORDER BY signal_ordinal",
    )
    .bind(job_id)
    .fetch_all(&mut *tx)
    .await?;
    let mut signals = Vec::with_capacity(ids.len());
    for id in ids {
        signals.push(
            load_signal(
                tx,
                LearningSignalId::from_uuid(id).map_err(|_| PostgresLearningError::Corrupt)?,
            )
            .await?,
        );
    }
    Ok(DreamLearningInput { job, signals })
}

async fn load_signal(
    tx: &mut sqlx::PgConnection,
    signal_id: LearningSignalId,
) -> Result<LearningSignal, PostgresLearningError> {
    let row = sqlx::query(
        "SELECT tenant_id::text AS tenant_id,target_agent_id::text AS target_agent_id,
         signal_kind,extractor_version_ref,policy_version_ref,dedup_key,created_at_ms
         FROM evolution.learning_signals WHERE signal_id=$1",
    )
    .bind(signal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(PostgresLearningError::NotFound)?;
    let spans = sqlx::query(
        "SELECT span_ordinal,conversation_id::text AS conversation_id,run_id::text AS run_id,
         branch_id::text AS branch_id,branch_state,cursor_from::text AS cursor_from,
         cursor_to::text AS cursor_to,terminal_status,outcome
         FROM evolution.learning_signal_evidence WHERE signal_id=$1 ORDER BY span_ordinal",
    )
    .bind(signal_id.into_uuid())
    .fetch_all(&mut *tx)
    .await?;
    let mut evidence = Vec::with_capacity(spans.len());
    for span in spans {
        let ordinal: i32 = span.try_get("span_ordinal")?;
        let records = sqlx::query(
            "SELECT record_id::text AS record_id,record_hash
             FROM evolution.learning_signal_evidence_records
             WHERE signal_id=$1 AND span_ordinal=$2 ORDER BY record_ordinal",
        )
        .bind(signal_id.into_uuid())
        .bind(ordinal)
        .fetch_all(&mut *tx)
        .await?;
        let mut record_ids = Vec::with_capacity(records.len());
        let mut record_hashes = Vec::with_capacity(records.len());
        for record in records {
            record_ids.push(parse_id(record.try_get("record_id")?)?);
            record_hashes.push(parse_hash(record.try_get("record_hash")?)?);
        }
        evidence.push(EvidenceSpan {
            conversation_id: parse_id(span.try_get("conversation_id")?)?,
            run_id: parse_id(span.try_get("run_id")?)?,
            branch_id: span
                .try_get::<Option<String>, _>("branch_id")?
                .map(parse_id)
                .transpose()?,
            branch_state: decode_branch_state(span.try_get("branch_state")?)?,
            cursor_from: LedgerOffset::new(parse_u64(span.try_get("cursor_from")?)?),
            cursor_to: LedgerOffset::new(parse_u64(span.try_get("cursor_to")?)?),
            record_ids,
            record_hashes,
            terminal: decode_terminal(span.try_get("terminal_status")?)?,
            outcome: decode_outcome(span.try_get("outcome")?)?,
        });
    }
    let signal = LearningSignal {
        signal_id,
        tenant_id: parse_id(row.try_get("tenant_id")?)?,
        target_agent_id: parse_id(row.try_get("target_agent_id")?)?,
        kind: decode_signal_kind(row.try_get("signal_kind")?)?,
        evidence,
        extractor_version_ref: VersionedKey::new(
            row.try_get::<String, _>("extractor_version_ref")?,
        )
        .map_err(|_| PostgresLearningError::Corrupt)?,
        policy_version_ref: VersionedKey::new(row.try_get::<String, _>("policy_version_ref")?)
            .map_err(|_| PostgresLearningError::Corrupt)?,
        dedup_key: parse_hash(row.try_get("dedup_key")?)?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
    };
    signal.validate()?;
    Ok(signal)
}

fn validate_completion(
    claimed: &ClaimedDreamJob,
    candidates: &[ExperienceCandidate],
    completed_at: UtcTimestamp,
) -> Result<(), PostgresLearningError> {
    if claimed.input.job.status != DreamJobStatus::Running
        || claimed.input.job.lease_owner.as_deref() != Some(claimed.lease_owner.as_str())
        || claimed.input.job.lease_expires_at != Some(claimed.lease_expires_at)
        || completed_at > claimed.lease_expires_at
        || candidates.len() > usize::from(claimed.input.job.workload.candidate_batch_capacity)
        || candidates.iter().any(|candidate| {
            candidate.tenant_id != claimed.input.job.tenant_id || candidate.validate().is_err()
        })
    {
        Err(PostgresLearningError::InvalidCommand)
    } else {
        Ok(())
    }
}

async fn candidate_ids_match(
    tx: &mut sqlx::PgConnection,
    job_id: DreamJobId,
    candidates: &[ExperienceCandidate],
) -> Result<bool, PostgresLearningError> {
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT proposal_id FROM evolution.dream_job_candidates
         WHERE dream_job_id=$1 ORDER BY proposal_ordinal",
    )
    .bind(job_id.into_uuid())
    .fetch_all(&mut *tx)
    .await?;
    Ok(ids
        == candidates
            .iter()
            .map(|value| value.proposal_id.into_uuid())
            .collect::<Vec<_>>())
}

async fn insert_outbox(
    tx: &mut sqlx::PgConnection,
    claimed: &ClaimedDreamJob,
    candidates: &[ExperienceCandidate],
    completed_at: UtcTimestamp,
) -> Result<(), PostgresLearningError> {
    let conversation_id = claimed
        .input
        .signals
        .first()
        .and_then(|signal| signal.evidence.first())
        .map(|span| span.conversation_id)
        .ok_or(PostgresLearningError::InvalidEvidence)?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema":"musteragent.legacy.learning.dream-completed.v1",
        "dream_job_id":claimed.input.job.dream_job_id.to_string(),
        "proposal_ids":candidates.iter().map(|value| value.proposal_id.to_string()).collect::<Vec<_>>(),
    }))?;
    let message_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("dream-completed:{}", claimed.input.job.dream_job_id).as_bytes(),
    );
    sqlx::query(
        "INSERT INTO event_store.outbox
         (message_id,tenant_id,conversation_id,topic,payload,payload_hash,committed_at_ms,available_at_ms)
         VALUES ($1,$2,$3,'learning.dream.completed.v1',$4,$5,$6,$6)
         ON CONFLICT (message_id) DO NOTHING",
    )
    .bind(message_id)
    .bind(claimed.input.job.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(&payload)
    .bind(Sha256::digest(&payload).into_bytes().as_slice())
    .bind(completed_at.unix_millis())
    .execute(&mut *tx)
    .await?;
    Ok(())
}

fn parse_id<T: std::str::FromStr>(value: String) -> Result<T, PostgresLearningError> {
    value.parse().map_err(|_| PostgresLearningError::Corrupt)
}
fn parse_u64(value: String) -> Result<u64, PostgresLearningError> {
    value.parse().map_err(|_| PostgresLearningError::Corrupt)
}
fn parse_hash(value: Vec<u8>) -> Result<Sha256, PostgresLearningError> {
    Sha256::from_bytes(value).map_err(|_| PostgresLearningError::Corrupt)
}

const fn signal_kind_code(v: LearningSignalKind) -> i16 {
    match v {
        LearningSignalKind::UserInstruction => 1,
        LearningSignalKind::UserCorrection => 2,
        LearningSignalKind::ToolFailure => 3,
        LearningSignalKind::ToolSuccess => 4,
        LearningSignalKind::TaskOutcome => 5,
        LearningSignalKind::UserAcceptance => 6,
        LearningSignalKind::NegativeEvidence => 7,
        LearningSignalKind::Retraction => 8,
    }
}
fn decode_signal_kind(v: i16) -> Result<LearningSignalKind, PostgresLearningError> {
    match v {
        1 => Ok(LearningSignalKind::UserInstruction),
        2 => Ok(LearningSignalKind::UserCorrection),
        3 => Ok(LearningSignalKind::ToolFailure),
        4 => Ok(LearningSignalKind::ToolSuccess),
        5 => Ok(LearningSignalKind::TaskOutcome),
        6 => Ok(LearningSignalKind::UserAcceptance),
        7 => Ok(LearningSignalKind::NegativeEvidence),
        8 => Ok(LearningSignalKind::Retraction),
        _ => Err(PostgresLearningError::Corrupt),
    }
}
const fn branch_state_code(v: BranchEvidenceState) -> i16 {
    match v {
        BranchEvidenceState::Active => 1,
        BranchEvidenceState::Preserved => 2,
        BranchEvidenceState::Abandoned => 3,
    }
}
fn decode_branch_state(v: i16) -> Result<BranchEvidenceState, PostgresLearningError> {
    match v {
        1 => Ok(BranchEvidenceState::Active),
        2 => Ok(BranchEvidenceState::Preserved),
        3 => Ok(BranchEvidenceState::Abandoned),
        _ => Err(PostgresLearningError::Corrupt),
    }
}
const fn terminal_code(v: RunTerminalEvidence) -> i16 {
    match v {
        RunTerminalEvidence::Completed => 1,
        RunTerminalEvidence::Failed => 2,
        RunTerminalEvidence::Cancelled => 3,
        RunTerminalEvidence::Interrupted => 4,
    }
}
fn decode_terminal(v: i16) -> Result<RunTerminalEvidence, PostgresLearningError> {
    match v {
        1 => Ok(RunTerminalEvidence::Completed),
        2 => Ok(RunTerminalEvidence::Failed),
        3 => Ok(RunTerminalEvidence::Cancelled),
        4 => Ok(RunTerminalEvidence::Interrupted),
        _ => Err(PostgresLearningError::Corrupt),
    }
}
const fn outcome_code(v: LearningOutcome) -> i16 {
    match v {
        LearningOutcome::Success => 1,
        LearningOutcome::Failure => 2,
        LearningOutcome::Cancelled => 3,
        LearningOutcome::Interrupted => 4,
        LearningOutcome::Unknown => 5,
    }
}
fn decode_outcome(v: i16) -> Result<LearningOutcome, PostgresLearningError> {
    match v {
        1 => Ok(LearningOutcome::Success),
        2 => Ok(LearningOutcome::Failure),
        3 => Ok(LearningOutcome::Cancelled),
        4 => Ok(LearningOutcome::Interrupted),
        5 => Ok(LearningOutcome::Unknown),
        _ => Err(PostgresLearningError::Corrupt),
    }
}
const fn dream_status_code(v: DreamJobStatus) -> i16 {
    match v {
        DreamJobStatus::Pending => 1,
        DreamJobStatus::Running => 2,
        DreamJobStatus::Completed => 3,
        DreamJobStatus::Failed => 4,
        DreamJobStatus::Cancelled => 5,
    }
}
fn decode_dream_status(v: i16) -> Result<DreamJobStatus, PostgresLearningError> {
    match v {
        1 => Ok(DreamJobStatus::Pending),
        2 => Ok(DreamJobStatus::Running),
        3 => Ok(DreamJobStatus::Completed),
        4 => Ok(DreamJobStatus::Failed),
        5 => Ok(DreamJobStatus::Cancelled),
        _ => Err(PostgresLearningError::Corrupt),
    }
}

#[derive(Debug, Error)]
pub enum PostgresLearningError {
    #[error("Learning workflow command is invalid")]
    InvalidCommand,
    #[error("Learning evidence is unavailable or hash-mismatched")]
    InvalidEvidence,
    #[error("Learning Content reference is unavailable")]
    ContentIntegrity,
    #[error("Dream Job lease is stale")]
    StaleLease,
    #[error("Learning workflow state was not found")]
    NotFound,
    #[error("Learning workflow idempotency conflicts with durable state")]
    Conflict,
    #[error("Learning workflow durable state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Contract(#[from] musteragent_evolution::LearningContractError),
    #[error(transparent)]
    Evolution(#[from] musteragent_evolution::EvolutionContractError),
    #[error(transparent)]
    Memory(#[from] PostgresMemoryEvolutionError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Learning Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("Learning numeric value is out of range")]
    Numeric(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_request_generation_query_preserves_the_unsigned_numeric_boundary() {
        assert!(
            LAST_MANUAL_REQUEST_GENERATION_SQL.contains("max(manual_request_generation)::text")
        );
        assert_eq!(
            parse_u64(u64::MAX.to_string()).expect("u64 maximum remains representable"),
            u64::MAX
        );
        assert!(matches!(
            parse_u64("-1".to_owned()),
            Err(PostgresLearningError::Corrupt)
        ));
    }

    #[test]
    fn automatic_quota_query_excludes_manual_and_pre_dispatch_jobs() {
        assert!(AUTOMATIC_DREAM_JOBS_LAST_24H_SQL.contains("manual_request_generation = 0"));
        assert!(
            AUTOMATIC_DREAM_JOBS_LAST_24H_SQL.contains("provider_dispatched_at_ms IS NOT NULL")
        );
    }
}
