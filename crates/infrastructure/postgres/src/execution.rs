pub(crate) mod cascade;
use musteragent_activity::ActivityKind;
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, CorrelationId, FencingToken, HomeEpoch,
    InteractionId, InvocationId, LeaseId, RunId, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AcknowledgeCancellationBatch, AppendReceipt, AttemptFence, CancelMode, CancelRunBatch,
    CancelRunReceipt, ClaimAttemptBatch, ExecutionMutationError, ExecutionReceipt, ExecutionStore,
    FencedAppendBatch, RecoverAttemptBatch, RenewLeaseRequest, validate_lease_horizon,
};
use musteragent_run::{AttemptTrigger, WorkerId};
use musteragent_runtime_contract::{ApplicationFuture, TraceContext, TraceParent, TraceState};
use musteragent_telemetry::RunTelemetryContext;
use sqlx::Row;
use thiserror::Error;

use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

pub(crate) struct CancelMutation {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub expected_last_run_sequence: RunSeq,
    pub mode: CancelMode,
    pub pending_interaction_id: Option<InteractionId>,
    pub pending_tool_invocation_id: Option<InvocationId>,
    pub unresolved_tool_invocation_id: Option<InvocationId>,
    pub record_count: usize,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum RunControlDiscardReason {
    Cancelled,
    Failed,
    Interrupted,
}

impl RunControlDiscardReason {
    fn code(self) -> i16 {
        match self {
            Self::Cancelled => 1,
            Self::Failed => 2,
            Self::Interrupted => 3,
        }
    }
}

impl CancelMutation {
    pub(crate) fn from_batch(batch: &CancelRunBatch) -> Self {
        Self {
            tenant_id: batch.append.tenant_id,
            conversation_id: batch.append.conversation_id,
            home_epoch: batch.append.expected_home_epoch,
            run_id: batch.run_id,
            activity_id: batch.activity_id,
            expected_last_run_sequence: batch.expected_last_run_sequence,
            mode: batch.mode,
            pending_interaction_id: batch.pending_interaction_id,
            pending_tool_invocation_id: batch.pending_tool_invocation_id,
            unresolved_tool_invocation_id: batch.unresolved_tool_invocation_id,
            record_count: batch
                .append
                .records
                .iter()
                .filter(|record| record.run_id == Some(batch.run_id))
                .count(),
        }
    }
}

impl PostgresLedgerStore {
    pub async fn claim_attempt(
        &self,
        batch: ClaimAttemptBatch,
    ) -> Result<ExecutionReceipt, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                return load_execution_receipt(self.pool(), batch.attempt_id, receipt).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        validate_lease_horizon(now, batch.lease_expires_at)
            .map_err(PostgresExecutionError::Contract)?;
        validate_claimable_run(prepared.transaction_mut(), &batch, now).await?;
        insert_attempt(
            prepared.transaction_mut(),
            &batch,
            now,
            batch.lease_expires_at,
        )
        .await?;
        insert_lease(
            prepared.transaction_mut(),
            &batch,
            now,
            batch.lease_expires_at,
        )
        .await?;
        advance_run_for_claim(prepared.transaction_mut(), &batch, now).await?;
        bind_loop_checkpoint(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            batch.append.conversation_id,
            batch.run_id,
            batch.attempt_id,
            batch.trigger,
            now,
        )
        .await?;
        let receipt = prepared.commit(&batch.append).await?;
        let telemetry = batch.telemetry.for_attempt(
            batch.attempt_id,
            batch.attempt_number,
            batch.fencing_token,
        );
        Ok(ExecutionReceipt {
            ledger: receipt,
            fence: batch_fence(&batch),
            activity_kind: batch.activity_kind,
            lease_expires_at: batch.lease_expires_at,
            telemetry: Box::new(telemetry),
        })
    }

    pub async fn renew_lease(
        &self,
        request: RenewLeaseRequest,
    ) -> Result<UtcTimestamp, PostgresExecutionError> {
        let mut transaction = self
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await?;
        let now = database_now_ms(&mut transaction).await?;
        validate_lease_horizon(now, request.new_expires_at)
            .map_err(PostgresExecutionError::Contract)?;
        let current_expiry: i64 = sqlx::query_scalar(
            "SELECT lease.expires_at_ms
             FROM runtime.leases AS lease
             JOIN runtime.runs AS run
               ON run.tenant_id = lease.tenant_id AND run.run_id = lease.run_id
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = lease.tenant_id
              AND attempt.attempt_id = lease.attempt_id
             WHERE lease.run_id = $1
               AND lease.attempt_id = $2
               AND lease.worker_id = $3
               AND lease.lease_id = $4
               AND lease.fencing_token = $5::bigint
               AND run.home_epoch = $6::bigint
               AND run.status IN (2, 4)
               AND attempt.status = 1
             FOR UPDATE OF lease, run, attempt",
        )
        .bind(request.fence.run_id.into_uuid())
        .bind(request.fence.attempt_id.into_uuid())
        .bind(request.fence.worker_id.as_str())
        .bind(request.fence.lease_id.into_uuid())
        .bind(request.fence.fencing_token.get().to_string())
        .bind(request.fence.home_epoch.get().to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresExecutionError::StaleFence)?;
        if current_expiry <= now.unix_millis()
            || request.new_expires_at.unix_millis() <= current_expiry
        {
            return Err(PostgresExecutionError::LeaseExpiredOrNotExtended);
        }
        sqlx::query(
            "UPDATE runtime.leases
             SET expires_at_ms = $2, renewed_at_ms = $3
             WHERE run_id = $1",
        )
        .bind(request.fence.run_id.into_uuid())
        .bind(request.new_expires_at.unix_millis())
        .bind(now.unix_millis())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE runtime.attempts
             SET lease_expires_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(request.fence.attempt_id.into_uuid())
        .bind(request.new_expires_at.unix_millis())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(request.new_expires_at)
    }

    pub async fn append_fenced(
        &self,
        batch: FencedAppendBatch,
    ) -> Result<AppendReceipt, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => return Ok(receipt),
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &batch.fence,
            batch.expected_last_run_sequence,
            now,
        )
        .await?;
        let next_run_sequence = batch
            .expected_last_run_sequence
            .get()
            .checked_add(
                u64::try_from(batch.append.records.len())
                    .map_err(|_| PostgresExecutionError::RunSequenceOverflow)?,
            )
            .ok_or(PostgresExecutionError::RunSequenceOverflow)?;
        sqlx::query(
            "UPDATE runtime.runs
             SET last_run_sequence = $2::bigint, run_version = run_version + 1
             WHERE run_id = $1",
        )
        .bind(batch.fence.run_id.into_uuid())
        .bind(next_run_sequence.to_string())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        prepared
            .commit(&batch.append)
            .await
            .map_err(PostgresExecutionError::from)
    }

    pub async fn recover_attempt(
        &self,
        batch: RecoverAttemptBatch,
    ) -> Result<ExecutionReceipt, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                return load_execution_receipt(self.pool(), batch.new_attempt_id, receipt).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        validate_lease_horizon(now, batch.lease_expires_at)
            .map_err(PostgresExecutionError::Contract)?;
        validate_expired_recovery(prepared.transaction_mut(), &batch, now).await?;
        sqlx::query(
            "UPDATE runtime.attempts
             SET status = 6, finished_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(batch.stale_fence.attempt_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1")
            .bind(batch.run_id.into_uuid())
            .execute(&mut *prepared.transaction_mut())
            .await?;
        insert_recovery_attempt(prepared.transaction_mut(), &batch, now).await?;
        insert_recovery_lease(prepared.transaction_mut(), &batch, now).await?;
        advance_run_for_recovery(prepared.transaction_mut(), &batch).await?;
        bind_loop_checkpoint(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            batch.append.conversation_id,
            batch.run_id,
            batch.new_attempt_id,
            musteragent_run::AttemptTrigger::Recovery,
            now,
        )
        .await?;
        let receipt = prepared.commit(&batch.append).await?;
        let telemetry = batch.telemetry.for_attempt(
            batch.new_attempt_id,
            batch.new_attempt_number,
            batch.fencing_token,
        );
        Ok(ExecutionReceipt {
            ledger: receipt,
            fence: AttemptFence {
                run_id: batch.run_id,
                attempt_id: batch.new_attempt_id,
                attempt_number: batch.new_attempt_number,
                worker_id: batch.worker_id,
                lease_id: batch.lease_id,
                fencing_token: batch.fencing_token,
                home_epoch: batch.append.expected_home_epoch,
            },
            activity_kind: batch.activity_kind,
            lease_expires_at: batch.lease_expires_at,
            telemetry: Box::new(telemetry),
        })
    }

    pub async fn cancel_run(
        &self,
        batch: CancelRunBatch,
    ) -> Result<CancelRunReceipt, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                return Ok(CancelRunReceipt {
                    ledger: receipt,
                    terminal: batch.mode == CancelMode::CancelWithoutAttempt,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        cascade::apply(prepared.transaction_mut(), &batch, now).await?;
        let receipt = prepared.commit(&batch.append).await?;
        Ok(CancelRunReceipt {
            ledger: receipt,
            terminal: batch.mode == CancelMode::CancelWithoutAttempt,
        })
    }

    pub async fn acknowledge_cancellation(
        &self,
        batch: AcknowledgeCancellationBatch,
    ) -> Result<CancelRunReceipt, PostgresExecutionError> {
        batch.validate().map_err(PostgresExecutionError::Contract)?;
        let mut prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                return Ok(CancelRunReceipt {
                    ledger: receipt,
                    terminal: true,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &batch.fence,
            batch.expected_last_run_sequence,
            now,
        )
        .await?;
        let next_run_sequence = batch
            .expected_last_run_sequence
            .get()
            .checked_add(3)
            .ok_or(PostgresExecutionError::RunSequenceOverflow)?;
        sqlx::query(
            "UPDATE runtime.attempts
             SET status = 5, finished_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(batch.fence.attempt_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1")
            .bind(batch.fence.run_id.into_uuid())
            .execute(&mut *prepared.transaction_mut())
            .await?;
        sqlx::query(
            "UPDATE runtime.runs
             SET status = 7,
                 finished_at_ms = $2,
                 last_run_sequence = $3::bigint,
                 run_version = run_version + 1
             WHERE run_id = $1 AND status = 4",
        )
        .bind(batch.fence.run_id.into_uuid())
        .bind(now.unix_millis())
        .bind(next_run_sequence.to_string())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        discard_pending_run_controls(
            prepared.transaction_mut(),
            batch.append.tenant_id,
            batch.fence.run_id,
            RunControlDiscardReason::Cancelled,
            now,
        )
        .await?;
        sqlx::query(
            "UPDATE activity.activities
             SET status = 6, finished_at_ms = $2
             WHERE activity_id = $1 AND status IN (1, 2, 3)",
        )
        .bind(batch.activity_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        let receipt = prepared.commit(&batch.append).await?;
        Ok(CancelRunReceipt {
            ledger: receipt,
            terminal: true,
        })
    }
}

async fn bind_loop_checkpoint(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: musteragent_identity::ConversationId,
    run_id: musteragent_identity::RunId,
    attempt_id: AttemptId,
    trigger: musteragent_run::AttemptTrigger,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let result = sqlx::query(
        "INSERT INTO runtime.loop_checkpoints (
             run_id, tenant_id, conversation_id, attempt_id, iteration, phase,
             model_invocation_id, remaining_tool_call_ids,
             waiting_interaction_id, completed_tool_calls, total_tokens,
             continuation_ref, checkpoint_version, updated_at_ms
         ) VALUES ($1, $2, $3, $4, 0, 1, NULL, '{}', NULL, 0, 0, NULL, 1, $5)
         ON CONFLICT (run_id) DO UPDATE
         SET attempt_id = EXCLUDED.attempt_id,
             phase = CASE
                 WHEN runtime.loop_checkpoints.phase = 4 AND $6 = 4 THEN 3
                 ELSE runtime.loop_checkpoints.phase
             END,
             waiting_interaction_id = CASE
                 WHEN runtime.loop_checkpoints.phase = 4 AND $6 = 4 THEN NULL
                 ELSE runtime.loop_checkpoints.waiting_interaction_id
             END,
             checkpoint_version = runtime.loop_checkpoints.checkpoint_version + 1,
             updated_at_ms = EXCLUDED.updated_at_ms
         WHERE runtime.loop_checkpoints.tenant_id = EXCLUDED.tenant_id
           AND runtime.loop_checkpoints.conversation_id = EXCLUDED.conversation_id
           AND (
               runtime.loop_checkpoints.phase IN (1, 2, 3)
               OR (runtime.loop_checkpoints.phase = 4 AND $6 = 4)
           )",
    )
    .bind(run_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(attempt_id.into_uuid())
    .bind(now.unix_millis())
    .bind(attempt_trigger_code(trigger))
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresExecutionError::CheckpointNotClaimable)
    }
}

impl ExecutionStore for PostgresLedgerStore {
    type Error = PostgresExecutionError;

    fn claim_attempt(
        &self,
        batch: ClaimAttemptBatch,
    ) -> ApplicationFuture<'_, Result<ExecutionReceipt, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::claim_attempt(self, batch).await })
    }

    fn renew_lease(
        &self,
        request: RenewLeaseRequest,
    ) -> ApplicationFuture<'_, Result<UtcTimestamp, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::renew_lease(self, request).await })
    }

    fn append_fenced(
        &self,
        batch: FencedAppendBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::append_fenced(self, batch).await })
    }

    fn recover_attempt(
        &self,
        batch: RecoverAttemptBatch,
    ) -> ApplicationFuture<'_, Result<ExecutionReceipt, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::recover_attempt(self, batch).await })
    }

    fn cancel_run(
        &self,
        batch: CancelRunBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::cancel_run(self, batch).await })
    }

    fn acknowledge_cancellation(
        &self,
        batch: AcknowledgeCancellationBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::acknowledge_cancellation(self, batch).await })
    }
}

async fn lock_cancel_target(
    transaction: &mut sqlx::PgConnection,
    mutation: &CancelMutation,
) -> Result<(), PostgresExecutionError> {
    let row = sqlx::query(
        "SELECT run.status, run.wait_reason, run.wait_interaction_id,
                CASE
                    WHEN EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) THEN NULL
                    WHEN run.wait_interaction_id IS NULL
                        THEN run.wait_invocation_id
                    WHEN pending_interaction.requested_by_invocation_id
                            = run.wait_invocation_id
                         AND pending_tool.invocation_id IS NOT NULL
                        THEN run.wait_invocation_id
                    ELSE NULL
                END AS pending_tool_invocation_id,
                (SELECT t.invocation_id FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) AS unresolved_tool_invocation_id,
                run.last_run_sequence::text AS last_run_sequence,
                EXISTS (
                    SELECT 1 FROM runtime.leases WHERE leases.run_id = run.run_id
                ) AS has_lease
         FROM runtime.runs AS run
         LEFT JOIN approval.interactions AS pending_interaction
           ON pending_interaction.tenant_id = run.tenant_id
          AND pending_interaction.conversation_id = run.conversation_id
          AND pending_interaction.run_id = run.run_id
          AND pending_interaction.interaction_id = run.wait_interaction_id
          AND pending_interaction.status = 1
         LEFT JOIN tool.tool_invocations AS pending_tool
           ON pending_tool.tenant_id = run.tenant_id
          AND pending_tool.conversation_id = run.conversation_id
          AND pending_tool.run_id = run.run_id
          AND pending_tool.invocation_id = run.wait_invocation_id
          AND pending_tool.interaction_id = run.wait_interaction_id
          AND pending_tool.status IN (2, 3, 6)
         WHERE run.run_id = $1
           AND run.tenant_id = $2
           AND run.conversation_id = $3
           AND run.activity_id = $4
           AND run.home_epoch = $5::bigint
         FOR UPDATE OF run",
    )
    .bind(mutation.run_id.into_uuid())
    .bind(mutation.tenant_id.into_uuid())
    .bind(mutation.conversation_id.into_uuid())
    .bind(mutation.activity_id.into_uuid())
    .bind(mutation.home_epoch.get().to_string())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresExecutionError::RunNotCancellable)?;
    if row.try_get::<String, _>("last_run_sequence")?
        != mutation.expected_last_run_sequence.get().to_string()
    {
        return Err(PostgresExecutionError::RunNotCancellable);
    }
    let status: i16 = row.try_get("status")?;
    let has_lease: bool = row.try_get("has_lease")?;
    let persisted_interaction = row
        .try_get::<Option<uuid::Uuid>, _>("wait_interaction_id")?
        .map(InteractionId::from_uuid)
        .transpose()
        .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    let persisted_invocation = row
        .try_get::<Option<uuid::Uuid>, _>("pending_tool_invocation_id")?
        .map(InvocationId::from_uuid)
        .transpose()
        .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    let unresolved = row
        .try_get::<Option<uuid::Uuid>, _>("unresolved_tool_invocation_id")?
        .map(InvocationId::from_uuid)
        .transpose()
        .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    if unresolved != mutation.unresolved_tool_invocation_id {
        return Err(PostgresExecutionError::RunNotCancellable);
    }
    let wait_reason: Option<i16> = row.try_get("wait_reason")?;
    let valid = match mutation.mode {
        CancelMode::RequestActiveAttempt => {
            (status == 4 || (status == 2 && has_lease))
                && mutation.pending_interaction_id.is_none()
                && persisted_interaction.is_none()
        }
        CancelMode::CancelWithoutAttempt | CancelMode::QueueCancellation => {
            matches!(status, 1 | 3)
                && !has_lease
                && persisted_interaction == mutation.pending_interaction_id
                && persisted_invocation == mutation.pending_tool_invocation_id
                && !invalid_wait_shape(
                    mutation.mode,
                    wait_reason,
                    persisted_interaction.is_some(),
                    persisted_invocation.is_some(),
                )
        }
    };
    if valid {
        Ok(())
    } else {
        Err(PostgresExecutionError::RunNotCancellable)
    }
}

pub(crate) async fn apply_cancel_mutation(
    transaction: &mut sqlx::PgConnection,
    mutation: &CancelMutation,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    lock_cancel_target(transaction, mutation).await?;
    let record_count = u64::try_from(mutation.record_count)
        .map_err(|_| PostgresExecutionError::RunSequenceOverflow)?;
    let next_run_sequence = mutation
        .expected_last_run_sequence
        .get()
        .checked_add(record_count)
        .ok_or(PostgresExecutionError::RunSequenceOverflow)?;
    match mutation.mode {
        CancelMode::RequestActiveAttempt => {
            sqlx::query(
                "UPDATE runtime.runs
                 SET status = 4,
                     cancel_requested_at_ms = COALESCE(cancel_requested_at_ms, $2),
                     last_run_sequence = $3::bigint,
                     run_version = run_version + 1
                 WHERE run_id = $1 AND status IN (2, 4)",
            )
            .bind(mutation.run_id.into_uuid())
            .bind(now.unix_millis())
            .bind(next_run_sequence.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        CancelMode::QueueCancellation => {
            cascade::queue(transaction, mutation, now, next_run_sequence).await?;
        }
        CancelMode::CancelWithoutAttempt => {
            if let Some(interaction_id) = mutation.pending_interaction_id {
                let invocation_id = mutation
                    .pending_tool_invocation_id
                    .ok_or(PostgresExecutionError::RunNotCancellable)?;
                cancel_waiting_interaction(
                    transaction,
                    mutation.tenant_id,
                    mutation.run_id,
                    interaction_id,
                    invocation_id,
                    now,
                )
                .await?;
            }
            if let Some(invocation_id) = mutation.pending_tool_invocation_id {
                cancel_waiting_tool(
                    transaction,
                    mutation.tenant_id,
                    mutation.run_id,
                    invocation_id,
                    mutation.pending_interaction_id,
                    now,
                )
                .await?;
            }
            discard_pending_run_controls(
                transaction,
                mutation.tenant_id,
                mutation.run_id,
                RunControlDiscardReason::Cancelled,
                now,
            )
            .await?;
            sqlx::query(
                "UPDATE runtime.runs
                 SET status = 7,
                     cancel_requested_at_ms = $2,
                     finished_at_ms = $2,
                     wait_reason = NULL, wait_error_code = NULL,
                     wait_interaction_id = NULL,
                     wait_invocation_id = NULL,
                     wait_child_activity_id = NULL,
                     not_before_ms = NULL,
                     last_run_sequence = $3::bigint,
                     run_version = run_version + 1
                 WHERE run_id = $1 AND status IN (1, 3)",
            )
            .bind(mutation.run_id.into_uuid())
            .bind(now.unix_millis())
            .bind(next_run_sequence.to_string())
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE activity.activities
                 SET status = 6, finished_at_ms = $2
                 WHERE activity_id = $1 AND status IN (1, 2, 3)",
            )
            .bind(mutation.activity_id.into_uuid())
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn discard_pending_run_controls(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
    reason: RunControlDiscardReason,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    sqlx::query(
        "UPDATE runtime.loop_controls
         SET status = 3, discarded_at_ms = $3, discard_reason = $4
         WHERE tenant_id = $1 AND run_id = $2 AND status = 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(now.unix_millis())
    .bind(reason.code())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn cancel_waiting_interaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    run_id: musteragent_identity::RunId,
    interaction_id: InteractionId,
    invocation_id: InvocationId,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let interaction = sqlx::query(
        "UPDATE approval.interactions
         SET status = 4, closed_at_ms = $4
         WHERE tenant_id = $1 AND run_id = $2
           AND interaction_id = $3 AND requested_by_invocation_id = $5
           AND status = 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(interaction_id.into_uuid())
    .bind(now.unix_millis())
    .bind(invocation_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    if interaction.rows_affected() != 1 {
        return Err(PostgresExecutionError::RunNotCancellable);
    }
    crate::dialogue_publication::interaction::changed(
        transaction,
        tenant_id,
        interaction_id,
        run_id,
    )
    .await
    .map_err(|error| PostgresExecutionError::InteractionPublication(Box::new(error)))?;
    sqlx::query(
        "UPDATE runtime.loop_checkpoints
         SET phase = 7, model_invocation_id = NULL,
             remaining_tool_call_ids = '{}', waiting_interaction_id = NULL,
             checkpoint_version = checkpoint_version + 1, updated_at_ms = $3
         WHERE tenant_id = $1 AND run_id = $2 AND phase = 4
           AND waiting_interaction_id = $4",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(now.unix_millis())
    .bind(interaction_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "DELETE FROM runtime.pending_tool_calls
         WHERE run_id = $1 AND completed_at_ms IS NULL",
    )
    .bind(run_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn cancel_waiting_tool(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    run_id: musteragent_identity::RunId,
    invocation_id: InvocationId,
    interaction_id: Option<InteractionId>,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let tool = sqlx::query(
        "UPDATE tool.tool_invocations
         SET status = 7, finished_at_ms = $4,
             terminal_error = NULL, terminal_reason = NULL,
             reconciliation_ref_protected = NULL
         WHERE tenant_id = $1 AND run_id = $2
           AND invocation_id = $3 AND interaction_id IS NOT DISTINCT FROM $5
           AND status IN (2, 3)",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(now.unix_millis())
    .bind(interaction_id.map(|value| value.into_uuid()))
    .execute(&mut *transaction)
    .await?;
    if tool.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresExecutionError::RunNotCancellable)
    }
}

const fn invalid_wait_shape(
    mode: CancelMode,
    wait_reason: Option<i16>,
    has_pending_interaction: bool,
    has_pending_tool: bool,
) -> bool {
    (has_pending_interaction && !has_pending_tool)
        || (has_pending_tool && matches!(mode, CancelMode::RequestActiveAttempt))
        || (has_pending_tool
            && !has_pending_interaction
            && !matches!(wait_reason, Some(3) | Some(7) | Some(11)))
}

async fn validate_claimable_run(
    transaction: &mut sqlx::PgConnection,
    batch: &ClaimAttemptBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let row = sqlx::query(
        "SELECT tenant_id, conversation_id, activity_id, activity_kind,
                home_epoch::text AS home_epoch,
                last_run_sequence::text AS last_run_sequence,
                next_fencing_token::text AS next_fencing_token,
                next_attempt_number, next_attempt_trigger,
                status, wait_reason, not_before_ms
         FROM runtime.runs
         WHERE run_id = $1
         FOR UPDATE",
    )
    .bind(batch.run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresExecutionError::RunNotClaimable)?;
    let status = row.try_get::<i16, _>("status")?;
    let persisted_trigger: i16 = row.try_get("next_attempt_trigger")?;
    let status_matches_trigger = match batch.trigger {
        AttemptTrigger::Retry => {
            status == 3 && row.try_get::<Option<i16>, _>("wait_reason")? == Some(4)
        }
        AttemptTrigger::ReconciliationDue => {
            status == 3 && row.try_get::<Option<i16>, _>("wait_reason")? == Some(7)
        }
        AttemptTrigger::Initial
        | AttemptTrigger::InteractionResumed
        | AttemptTrigger::BudgetAmended
        | AttemptTrigger::DelegationCompleted => {
            status == 1 && row.try_get::<Option<i16>, _>("wait_reason")?.is_none()
        }
        AttemptTrigger::Recovery => {
            (status == 1 && row.try_get::<Option<i16>, _>("wait_reason")?.is_none())
                || (status == 3
                    && row.try_get::<i16, _>("activity_kind")? == 5
                    && row.try_get::<Option<i16>, _>("wait_reason")? == Some(5))
        }
    };
    let identity_matches = row.try_get::<uuid::Uuid, _>("tenant_id")?
        == batch.append.tenant_id.into_uuid()
        && row.try_get::<uuid::Uuid, _>("conversation_id")?
            == batch.append.conversation_id.into_uuid()
        && row.try_get::<uuid::Uuid, _>("activity_id")? == batch.activity_id.into_uuid()
        && row.try_get::<String, _>("home_epoch")?
            == batch.append.expected_home_epoch.get().to_string()
        && row.try_get::<String, _>("last_run_sequence")?
            == batch.expected_last_run_sequence.get().to_string()
        && row.try_get::<String, _>("next_fencing_token")? == batch.fencing_token.get().to_string()
        && row.try_get::<i32, _>("next_attempt_number")?
            == i32::try_from(batch.attempt_number.get())?
        && persisted_trigger == attempt_trigger_code(batch.trigger)
        && status_matches_trigger
        && row
            .try_get::<Option<i64>, _>("not_before_ms")?
            .is_none_or(|value| value <= now.unix_millis());
    if !identity_matches {
        return Err(PostgresExecutionError::RunNotClaimable);
    }
    let competing_active: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM runtime.runs
             WHERE tenant_id = $1
               AND conversation_id = $2
               AND run_id <> $3
               AND activity_kind = 1
               AND status IN (2, 3, 4)
         )",
    )
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.run_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    // A private delegation is admitted under the parent conversation lane,
    // but executes independently while its foreground parent is suspended.
    // Only another foreground Chat competes for the conversation turn.
    if batch.activity_kind == ActivityKind::Chat && competing_active {
        return Err(PostgresExecutionError::ConversationRunBusy);
    }
    Ok(())
}

async fn insert_attempt(
    transaction: &mut sqlx::PgConnection,
    batch: &ClaimAttemptBatch,
    now: UtcTimestamp,
    expires_at: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms, supports_model_routing_v1, supports_control_compaction_v1
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::bigint, $9, 1, $10, NULL, TRUE, TRUE)",
    )
    .bind(batch.attempt_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.run_id.into_uuid())
    .bind(i32::try_from(batch.attempt_number.get())?)
    .bind(attempt_trigger_code(batch.trigger))
    .bind(batch.worker_id.as_str())
    .bind(batch.lease_id.into_uuid())
    .bind(batch.fencing_token.get().to_string())
    .bind(expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn insert_lease(
    transaction: &mut sqlx::PgConnection,
    batch: &ClaimAttemptBatch,
    now: UtcTimestamp,
    expires_at: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6::bigint, $7, $8)",
    )
    .bind(batch.run_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.attempt_id.into_uuid())
    .bind(batch.worker_id.as_str())
    .bind(batch.lease_id.into_uuid())
    .bind(batch.fencing_token.get().to_string())
    .bind(expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn advance_run_for_claim(
    transaction: &mut sqlx::PgConnection,
    batch: &ClaimAttemptBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let last_run_sequence = batch
        .expected_last_run_sequence
        .get()
        .checked_add(2)
        .ok_or(PostgresExecutionError::RunSequenceOverflow)?;
    let next_attempt_number = batch
        .attempt_number
        .get()
        .checked_add(1)
        .ok_or(PostgresExecutionError::AttemptNumberOverflow)?;
    let next_fencing_token = batch
        .fencing_token
        .get()
        .checked_add(1)
        .ok_or(PostgresExecutionError::FencingTokenOverflow)?;
    sqlx::query(
        "UPDATE runtime.runs
         SET status = 2,
             wait_reason = NULL, wait_error_code = NULL,
             wait_interaction_id = NULL,
             wait_invocation_id = NULL,
             wait_child_activity_id = NULL,
             not_before_ms = NULL,
             last_run_sequence = $2::bigint,
             next_attempt_number = $3,
             next_fencing_token = $4::bigint,
             started_at_ms = COALESCE(started_at_ms, $5),
             run_version = run_version + 1
         WHERE run_id = $1",
    )
    .bind(batch.run_id.into_uuid())
    .bind(last_run_sequence.to_string())
    .bind(i32::try_from(next_attempt_number)?)
    .bind(next_fencing_token.to_string())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn lock_and_validate_fence(
    transaction: &mut sqlx::PgConnection,
    fence: &AttemptFence,
    expected_last_run_sequence: RunSeq,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let valid = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT lease.run_id
         FROM runtime.leases AS lease
         JOIN runtime.runs AS run
           ON run.tenant_id = lease.tenant_id AND run.run_id = lease.run_id
         JOIN runtime.attempts AS attempt
           ON attempt.tenant_id = lease.tenant_id
          AND attempt.attempt_id = lease.attempt_id
         WHERE lease.run_id = $1
           AND lease.attempt_id = $2
           AND attempt.attempt_number = $3
           AND lease.worker_id = $4
           AND lease.lease_id = $5
           AND lease.fencing_token = $6::bigint
           AND run.home_epoch = $7::bigint
           AND run.last_run_sequence = $8::bigint
           AND run.status IN (2, 4)
           AND attempt.status = 1
           AND lease.expires_at_ms > $9
         FOR UPDATE OF lease, run, attempt",
    )
    .bind(fence.run_id.into_uuid())
    .bind(fence.attempt_id.into_uuid())
    .bind(i32::try_from(fence.attempt_number.get())?)
    .bind(fence.worker_id.as_str())
    .bind(fence.lease_id.into_uuid())
    .bind(fence.fencing_token.get().to_string())
    .bind(fence.home_epoch.get().to_string())
    .bind(expected_last_run_sequence.get().to_string())
    .bind(now.unix_millis())
    .fetch_optional(&mut *transaction)
    .await?;
    if valid.is_some() {
        Ok(())
    } else {
        Err(PostgresExecutionError::StaleFence)
    }
}

async fn validate_expired_recovery(
    transaction: &mut sqlx::PgConnection,
    batch: &RecoverAttemptBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    let valid = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT lease.run_id
         FROM runtime.leases AS lease
         JOIN runtime.runs AS run
           ON run.tenant_id = lease.tenant_id AND run.run_id = lease.run_id
         JOIN runtime.attempts AS attempt
           ON attempt.tenant_id = lease.tenant_id
          AND attempt.attempt_id = lease.attempt_id
         WHERE lease.run_id = $1
           AND lease.attempt_id = $2
           AND attempt.attempt_number = $3
           AND lease.worker_id = $4
           AND lease.lease_id = $5
           AND lease.fencing_token = $6::bigint
           AND run.home_epoch = $7::bigint
           AND run.last_run_sequence = $8::bigint
           AND run.next_attempt_number = $9
           AND run.next_fencing_token = $10::bigint
           AND run.status IN (2, 4)
           AND attempt.status = 1
           AND lease.expires_at_ms <= $11
         FOR UPDATE OF lease, run, attempt",
    )
    .bind(batch.run_id.into_uuid())
    .bind(batch.stale_fence.attempt_id.into_uuid())
    .bind(i32::try_from(batch.stale_fence.attempt_number.get())?)
    .bind(batch.stale_fence.worker_id.as_str())
    .bind(batch.stale_fence.lease_id.into_uuid())
    .bind(batch.stale_fence.fencing_token.get().to_string())
    .bind(batch.append.expected_home_epoch.get().to_string())
    .bind(batch.expected_last_run_sequence.get().to_string())
    .bind(i32::try_from(batch.new_attempt_number.get())?)
    .bind(batch.fencing_token.get().to_string())
    .bind(now.unix_millis())
    .fetch_optional(&mut *transaction)
    .await?;
    if valid.is_some() {
        Ok(())
    } else {
        Err(PostgresExecutionError::LeaseNotExpiredOrStale)
    }
}

async fn insert_recovery_attempt(
    transaction: &mut sqlx::PgConnection,
    batch: &RecoverAttemptBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms, supports_model_routing_v1, supports_control_compaction_v1
         ) VALUES ($1, $2, $3, $4, 3, $5, $6, $7::bigint, $8, 1, $9, NULL, TRUE, TRUE)",
    )
    .bind(batch.new_attempt_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.run_id.into_uuid())
    .bind(i32::try_from(batch.new_attempt_number.get())?)
    .bind(batch.worker_id.as_str())
    .bind(batch.lease_id.into_uuid())
    .bind(batch.fencing_token.get().to_string())
    .bind(batch.lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn insert_recovery_lease(
    transaction: &mut sqlx::PgConnection,
    batch: &RecoverAttemptBatch,
    now: UtcTimestamp,
) -> Result<(), PostgresExecutionError> {
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6::bigint, $7, $8)",
    )
    .bind(batch.run_id.into_uuid())
    .bind(batch.append.tenant_id.into_uuid())
    .bind(batch.new_attempt_id.into_uuid())
    .bind(batch.worker_id.as_str())
    .bind(batch.lease_id.into_uuid())
    .bind(batch.fencing_token.get().to_string())
    .bind(batch.lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn advance_run_for_recovery(
    transaction: &mut sqlx::PgConnection,
    batch: &RecoverAttemptBatch,
) -> Result<(), PostgresExecutionError> {
    let last_run_sequence = batch
        .expected_last_run_sequence
        .get()
        .checked_add(3)
        .ok_or(PostgresExecutionError::RunSequenceOverflow)?;
    let next_attempt_number = batch
        .new_attempt_number
        .get()
        .checked_add(1)
        .ok_or(PostgresExecutionError::AttemptNumberOverflow)?;
    let next_fencing_token = batch
        .fencing_token
        .get()
        .checked_add(1)
        .ok_or(PostgresExecutionError::FencingTokenOverflow)?;
    sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $2::bigint,
             next_attempt_number = $3,
             next_fencing_token = $4::bigint,
             run_version = run_version + 1
         WHERE run_id = $1",
    )
    .bind(batch.run_id.into_uuid())
    .bind(last_run_sequence.to_string())
    .bind(i32::try_from(next_attempt_number)?)
    .bind(next_fencing_token.to_string())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn load_execution_receipt(
    pool: &sqlx::PgPool,
    attempt_id: AttemptId,
    ledger: AppendReceipt,
) -> Result<ExecutionReceipt, PostgresExecutionError> {
    let row = sqlx::query(
        "SELECT attempt.run_id, attempt.tenant_id, run.conversation_id, run.activity_id,
                run.activity_kind,
                run.traceparent, run.tracestate, admission.correlation_id,
                attempt.attempt_number, attempt.worker_id,
                attempt.lease_id, attempt.fencing_token::text AS fencing_token,
                attempt.lease_expires_at_ms, run.home_epoch::text AS home_epoch
         FROM runtime.attempts AS attempt
         JOIN runtime.runs AS run
           ON run.tenant_id = attempt.tenant_id AND run.run_id = attempt.run_id
         JOIN event_store.ledger_records AS admission
           ON admission.tenant_id = run.tenant_id
          AND admission.run_id = run.run_id
          AND admission.run_seq = 1::bigint
         WHERE attempt.attempt_id = $1",
    )
    .bind(attempt_id.into_uuid())
    .fetch_optional(pool)
    .await?
    .ok_or(PostgresExecutionError::StaleFence)?;
    let run_id = RunId::from_uuid(row.try_get("run_id")?)
        .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    let attempt_number =
        AttemptNumber::new(u32::try_from(row.try_get::<i32, _>("attempt_number")?)?)
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    let fencing_token = FencingToken::new(
        row.try_get::<String, _>("fencing_token")?
            .parse()
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
    )
    .map_err(|_| PostgresExecutionError::CorruptIdentity)?;
    let telemetry = RunTelemetryContext {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        run_id,
        correlation_id: CorrelationId::from_uuid(row.try_get("correlation_id")?)
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        trace_context: TraceContext {
            traceparent: TraceParent::new(row.try_get::<String, _>("traceparent")?)
                .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
            tracestate: row
                .try_get::<Option<String>, _>("tracestate")?
                .map(TraceState::new)
                .transpose()
                .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        },
    }
    .for_attempt(attempt_id, attempt_number, fencing_token);
    Ok(ExecutionReceipt {
        ledger,
        fence: AttemptFence {
            run_id,
            attempt_id,
            attempt_number,
            worker_id: WorkerId::new(row.try_get::<String, _>("worker_id")?)
                .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
            lease_id: LeaseId::from_uuid(row.try_get("lease_id")?)
                .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
            fencing_token,
            home_epoch: HomeEpoch::new(
                row.try_get::<String, _>("home_epoch")?
                    .parse()
                    .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
            )
            .map_err(|_| PostgresExecutionError::CorruptIdentity)?,
        },
        activity_kind: decode_activity_kind(row.try_get("activity_kind")?)?,
        lease_expires_at: UtcTimestamp::from_unix_millis(row.try_get("lease_expires_at_ms")?),
        telemetry: Box::new(telemetry),
    })
}

fn decode_activity_kind(value: i16) -> Result<ActivityKind, PostgresExecutionError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(PostgresExecutionError::CorruptIdentity),
    }
}

fn batch_fence(batch: &ClaimAttemptBatch) -> AttemptFence {
    AttemptFence {
        run_id: batch.run_id,
        attempt_id: batch.attempt_id,
        attempt_number: batch.attempt_number,
        worker_id: batch.worker_id.clone(),
        lease_id: batch.lease_id,
        fencing_token: batch.fencing_token,
        home_epoch: batch.append.expected_home_epoch,
    }
}

const fn attempt_trigger_code(value: AttemptTrigger) -> i16 {
    match value {
        AttemptTrigger::Initial => 1,
        AttemptTrigger::Retry => 2,
        AttemptTrigger::Recovery => 3,
        AttemptTrigger::InteractionResumed => 4,
        AttemptTrigger::BudgetAmended => 5,
        AttemptTrigger::DelegationCompleted => 6,
        AttemptTrigger::ReconciliationDue => 7,
    }
}

#[derive(Debug, Error)]
pub enum PostgresExecutionError {
    #[error(transparent)]
    Contract(ExecutionMutationError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error("Run is not queued with the expected epoch, sequence, Attempt and fencing")]
    RunNotClaimable,
    #[error("another foreground Chat Run owns this Conversation")]
    ConversationRunBusy,
    #[error("Attempt lease is stale, expired, or does not own this Run")]
    StaleFence,
    #[error("lease is expired or renewal did not strictly extend it")]
    LeaseExpiredOrNotExtended,
    #[error("recovery lease is not expired or the presented fence is stale")]
    LeaseNotExpiredOrStale,
    #[error("Run loop checkpoint cannot be rebound to this Attempt")]
    CheckpointNotClaimable,
    #[error("Run status, lease, epoch, or sequence does not permit this cancellation mode")]
    RunNotCancellable,
    #[error("run sequence overflow")]
    RunSequenceOverflow,
    #[error("attempt number overflow")]
    AttemptNumberOverflow,
    #[error("fencing token overflow")]
    FencingTokenOverflow,
    #[error("persisted execution identity is corrupt")]
    CorruptIdentity,
    #[error("cancelled Interaction publication could not be committed")]
    InteractionPublication(#[source] Box<crate::PostgresInteractionError>),
    #[error("PostgreSQL execution transaction failed")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Execution Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("numeric execution field exceeds its database representation")]
    NumericConversion(#[from] std::num::TryFromIntError),
}
