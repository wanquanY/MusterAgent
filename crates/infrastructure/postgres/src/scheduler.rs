use std::sync::Arc;
use std::time::Duration;

use musteragent_activity::ActivityKind;
use musteragent_approval::{InteractionClosure, InteractionEvent};
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, CorrelationId, DottedKey, FencingToken,
    HomeEpoch, IdempotencyKey, InteractionId, LeaseId, LedgerOffset, LedgerRecordId,
    OutboxMessageId, RunId, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, ClaimAttemptBatch, DomainEvent, ExecutionReceipt, ProposedOutbox,
    ProposedRecord, RecoverAttemptBatch, validate_lease_horizon,
};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptTrigger, RunEvent, WorkerId};
use musteragent_runtime_contract::{
    ApplicationFuture, InterruptionReason, ResumeReason, TraceContext, TraceParent, TraceState,
};
use musteragent_runtime_interaction::{
    CloseInteractionCommit, InteractionSemanticRecords, InteractionStore,
};
use musteragent_scheduler::{
    AttemptControlError, AttemptControlStore, AttemptStopIntent, ClaimOutcome, DispatchError,
    DispatchErrorClass, DispatchStore, LeaseHeartbeat, SchedulerEventEncoder,
};
use musteragent_telemetry::RunTelemetryContext;
use sqlx::{PgPool, Row};

use crate::{
    PostgresExecutionError, PostgresInteractionError, PostgresInteractionStore,
    PostgresLedgerError, PostgresLedgerStore,
};
use musteragent_storage_protection::OpaqueRefKeyring;

const CANDIDATE_SCAN_LIMIT: i64 = 16;
mod observation;
mod owner_permission;
mod task_result_review;

#[derive(Clone)]
pub struct PostgresDispatchStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn SchedulerEventEncoder>,
    interaction_expiry: Option<PostgresInteractionStore>,
}

impl PostgresDispatchStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn SchedulerEventEncoder>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database.clone()),
            pool,
            database,
            encoder,
            interaction_expiry: None,
        }
    }

    /// Enables scheduler-owned Interaction expiry using the same durable
    /// Interaction store as request/response handling.
    #[must_use]
    pub fn with_interaction_expiry(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn SchedulerEventEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database.clone()),
            interaction_expiry: Some(PostgresInteractionStore::new(
                database.clone(),
                reference_keys,
                decoder,
            )),
            pool,
            database,
            encoder,
        }
    }

    async fn advance_expired_interactions(&self) -> Result<u64, DispatchError> {
        let Some(interactions) = &self.interaction_expiry else {
            return Ok(0);
        };
        let mut advanced = self.cancel_revoked_dialogue_waits().await?;
        advanced = advanced.saturating_add(self.retire_task_result_reviews().await?);
        let now = database_now(&self.pool).await?;
        let candidates = self.expired_interaction_candidates(now).await?;
        for candidate in candidates {
            let command = self.expire_interaction_command(candidate, now)?;
            match interactions.close(command).await {
                Ok(_) => advanced = advanced.saturating_add(1),
                Err(error) if is_interaction_expiry_race(&error) => {}
                Err(error) => return Err(map_interaction_expiry_error(error)),
            }
        }
        Ok(advanced)
    }

    async fn expired_interaction_candidates(
        &self,
        now: UtcTimestamp,
    ) -> Result<Vec<ExpiredInteractionCandidate>, DispatchError> {
        let rows = sqlx::query(
            "SELECT interaction.tenant_id, interaction.conversation_id,
                    interaction.activity_id, interaction.run_id, run.activity_kind,
                    interaction.interaction_id, run.traceparent, run.tracestate,
                    admission.correlation_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset
             FROM approval.interactions AS interaction
             JOIN runtime.runs AS run
               ON run.tenant_id = interaction.tenant_id
              AND run.run_id = interaction.run_id
              AND run.conversation_id = interaction.conversation_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = interaction.tenant_id
              AND stream.conversation_id = interaction.conversation_id
             JOIN event_store.ledger_records AS admission
               ON admission.tenant_id = run.tenant_id
              AND admission.run_id = run.run_id
              AND admission.run_seq = 1::bigint
             WHERE interaction.status = 1
               AND interaction.expires_at_ms IS NOT NULL
               AND interaction.expires_at_ms <= $1
               AND run.status = 3
               AND run.wait_interaction_id = interaction.interaction_id
             ORDER BY interaction.expires_at_ms, interaction.interaction_id
             LIMIT $2",
        )
        .bind(now.unix_millis())
        .bind(CANDIDATE_SCAN_LIMIT)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(decode_expired_interaction).collect()
    }

    fn expire_interaction_command(
        &self,
        candidate: ExpiredInteractionCandidate,
        now: UtcTimestamp,
    ) -> Result<CloseInteractionCommit, DispatchError> {
        let record_id =
            LedgerRecordId::from_uuid(derived_id(candidate.interaction_id, b"expired-record"))
                .map_err(|_| contract_error())?;
        let correlation_id =
            CorrelationId::from_uuid(derived_id(candidate.interaction_id, b"expired-correlation"))
                .map_err(|_| contract_error())?;
        let payload = self.encoder.encode_domain_event(DomainEvent::Interaction(
            InteractionEvent::Expired {
                interaction_id: candidate.interaction_id,
                expired_at: now,
            },
        ))?;
        let record = ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| contract_error())?,
            record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(candidate.identity.activity_id),
            run_id: Some(candidate.identity.run_id),
            run_seq: Some(
                RunSeq::new(next_run_sequence(candidate.identity.last_run_sequence, 1)?)
                    .map_err(|_| contract_error())?,
            ),
            attempt_id: None,
            participant_id: None,
            producer: Producer::Scheduler,
            occurred_at: now,
            causation_id: None,
            correlation_id,
            payload_hash: Sha256::digest(&payload),
            payload,
        };
        let dispatch = format!("run:{}", candidate.identity.run_id).into_bytes();
        let outbox_id =
            OutboxMessageId::from_uuid(derived_id(candidate.interaction_id, b"expired-dispatch"))
                .map_err(|_| contract_error())?;
        let idempotency = IdempotencyKey::new(format!(
            "scheduler:interaction-expire:{}",
            candidate.interaction_id
        ))
        .map_err(|_| contract_error())?;
        let append = AppendRequest {
            tenant_id: candidate.identity.tenant_id,
            conversation_id: candidate.identity.conversation_id,
            expected_home_epoch: candidate.identity.home_epoch,
            expected_offset: candidate.identity.conversation_offset,
            command_fingerprint: Sha256::digest(format!(
                "{}:{}:{}",
                candidate.identity.run_id,
                candidate.identity.last_run_sequence.get(),
                idempotency.as_str()
            )),
            idempotency_key: idempotency,
            records: vec![record],
            outbox: vec![ProposedOutbox {
                message_id: outbox_id,
                topic: DottedKey::new("run.dispatch").map_err(|_| contract_error())?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        };
        Ok(CloseInteractionCommit {
            tenant_id: candidate.identity.tenant_id,
            interaction_id: candidate.interaction_id,
            closure: InteractionClosure::Expire,
            closed_at: now,
            expected_last_run_sequence: candidate.identity.last_run_sequence,
            append,
            semantic_records: InteractionSemanticRecords {
                interaction_record_id: record_id,
                run_record_id: None,
                tool_record_id: None,
            },
        })
    }

    async fn claim(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<ClaimOutcome, DispatchError> {
        let now = database_now(&self.pool).await?;
        let lease_millis =
            i64::try_from(lease_duration.as_millis()).map_err(|_| contract_error())?;
        let lease_expires_at = now
            .checked_add_millis(lease_millis)
            .ok_or_else(contract_error)?;

        for candidate in
            observation::observe("recovery_candidates", self.recovery_candidates(now, 2)).await?
        {
            let batch = self.recovery_batch(candidate, worker_id.clone(), now, lease_expires_at)?;
            match self.ledger.recover_attempt(batch).await {
                Ok(receipt) => return Ok(ClaimOutcome::Claimed(receipt)),
                Err(error) if is_claim_race(&error) => continue,
                Err(error) => return Err(map_execution_error(error)),
            }
        }

        for candidate in
            observation::observe("ready_candidates", self.ready_candidates(now)).await?
        {
            let batch = self.claim_batch(candidate, worker_id.clone(), now, lease_expires_at)?;
            match observation::observe("claim_commit", self.ledger.claim_attempt(batch)).await {
                Ok(receipt) => return Ok(ClaimOutcome::Claimed(receipt)),
                Err(error) if is_claim_race(&error) => continue,
                Err(error) => return Err(map_execution_error(error)),
            }
        }

        Ok(ClaimOutcome::Idle {
            next_due_at: self.next_due_at(now).await?,
            database_now: now,
        })
    }

    async fn claim_cancellation(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> Result<Option<ExecutionReceipt>, DispatchError> {
        let now = database_now(&self.pool).await?;
        let lease_millis =
            i64::try_from(lease_duration.as_millis()).map_err(|_| contract_error())?;
        let lease_expires_at = now
            .checked_add_millis(lease_millis)
            .ok_or_else(contract_error)?;

        for candidate in self.recovery_candidates(now, 4).await? {
            let batch = self.recovery_batch(candidate, worker_id.clone(), now, lease_expires_at)?;
            match self.ledger.recover_attempt(batch).await {
                Ok(receipt) => return Ok(Some(receipt)),
                Err(error) if is_claim_race(&error) => continue,
                Err(error) => return Err(map_execution_error(error)),
            }
        }
        Ok(None)
    }

    async fn ready_candidates(
        &self,
        now: UtcTimestamp,
    ) -> Result<Vec<ReadyCandidate>, DispatchError> {
        let rows = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id, run.run_id,
                    run.activity_kind,
                    run.traceparent, run.tracestate, admission.correlation_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    run.next_attempt_number,
                    run.next_fencing_token::text AS next_fencing_token,
                    run.next_attempt_trigger,
                    stream.last_sequence::text AS conversation_offset
             FROM runtime.runs AS run
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             JOIN event_store.ledger_records AS admission
               ON admission.tenant_id = run.tenant_id
              AND admission.run_id = run.run_id
              AND admission.run_seq = 1::bigint
             WHERE (
                    run.status = 1
                    OR (
                        run.status = 3
                        AND (
                            (run.wait_reason = 4 AND run.next_attempt_trigger = 2)
                            OR (run.wait_reason = 7 AND run.wait_invocation_id IS NOT NULL
                                AND run.next_attempt_trigger = 7)
                            OR (run.activity_kind = 5 AND run.wait_reason = 5
                                AND run.next_attempt_trigger = 3)
                        )
                    )
               )
               AND (run.not_before_ms IS NULL OR run.not_before_ms <= $1)
             ORDER BY COALESCE(run.not_before_ms, run.admitted_at_ms),
                      run.admitted_at_ms, run.run_id
             LIMIT $2",
        )
        .bind(now.unix_millis())
        .bind(CANDIDATE_SCAN_LIMIT)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(decode_ready).collect()
    }

    async fn recovery_candidates(
        &self,
        now: UtcTimestamp,
        run_status: i16,
    ) -> Result<Vec<RecoveryCandidate>, DispatchError> {
        let rows = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id, run.run_id,
                    run.activity_kind,
                    run.traceparent, run.tracestate, admission.correlation_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    run.next_attempt_number,
                    run.next_fencing_token::text AS next_fencing_token,
                    stream.last_sequence::text AS conversation_offset,
                    attempt.attempt_id, attempt.attempt_number,
                    attempt.worker_id, attempt.lease_id,
                    attempt.fencing_token::text AS stale_fencing_token,
                    lease.surrender_reason
             FROM runtime.leases AS lease
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = lease.tenant_id
              AND attempt.attempt_id = lease.attempt_id
             JOIN runtime.runs AS run
               ON run.tenant_id = lease.tenant_id
              AND run.run_id = lease.run_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             JOIN event_store.ledger_records AS admission
               ON admission.tenant_id = run.tenant_id
              AND admission.run_id = run.run_id
              AND admission.run_seq = 1::bigint
             WHERE lease.expires_at_ms <= $1
               AND attempt.status = 1
               AND run.status = $3
             ORDER BY lease.expires_at_ms, run.run_id
             LIMIT $2",
        )
        .bind(now.unix_millis())
        .bind(CANDIDATE_SCAN_LIMIT)
        .bind(run_status)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter().map(decode_recovery).collect()
    }

    async fn next_due_at(&self, now: UtcTimestamp) -> Result<Option<UtcTimestamp>, DispatchError> {
        let run_due: Option<i64> = sqlx::query_scalar(
            "SELECT min(due_at)
             FROM (
                 SELECT not_before_ms AS due_at
                 FROM runtime.runs
                 WHERE status IN (1, 3) AND not_before_ms > $1
                 UNION ALL
                 SELECT expires_at_ms AS due_at
                 FROM runtime.leases
                 WHERE expires_at_ms > $1
             ) AS future_work",
        )
        .bind(now.unix_millis())
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)?;
        let interaction_due = if self.interaction_expiry.is_some() {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT min(expires_at_ms)
                 FROM approval.interactions
                 WHERE status = 1 AND expires_at_ms IS NOT NULL",
            )
            .fetch_one(&self.pool)
            .await
            .map_err(database_error)?
        } else {
            None
        };
        Ok(run_due
            .into_iter()
            .chain(interaction_due)
            .min()
            .map(UtcTimestamp::from_unix_millis))
    }

    fn claim_batch(
        &self,
        candidate: ReadyCandidate,
        worker_id: WorkerId,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> Result<ClaimAttemptBatch, DispatchError> {
        let attempt_id = AttemptId::generate();
        let lease_id = LeaseId::generate();
        let claimed = RunEvent::AttemptClaimed {
            attempt_number: candidate.attempt_number,
            trigger: candidate.trigger,
            lease_id,
            fencing_token: candidate.fencing_token,
            lease_expires_at,
        };
        let second = if candidate.trigger == AttemptTrigger::Initial {
            RunEvent::Started {
                attempt_id,
                attempt_number: candidate.attempt_number,
            }
        } else {
            RunEvent::Resumed {
                attempt_id,
                attempt_number: candidate.attempt_number,
                reason: match candidate.trigger {
                    AttemptTrigger::Retry => ResumeReason::RetryBackoffElapsed,
                    AttemptTrigger::InteractionResumed => ResumeReason::InteractionResponded,
                    AttemptTrigger::Recovery => ResumeReason::Recovery,
                    AttemptTrigger::BudgetAmended => ResumeReason::BudgetAmended,
                    AttemptTrigger::DelegationCompleted => ResumeReason::DelegationCompleted,
                    AttemptTrigger::ReconciliationDue => ResumeReason::ReconciliationDue,
                    AttemptTrigger::Initial => return Err(contract_error()),
                },
            }
        };
        let records = vec![
            self.record(
                &candidate.identity,
                attempt_id,
                next_run_sequence(candidate.identity.last_run_sequence, 1)?,
                now,
                claimed,
            )?,
            self.record(
                &candidate.identity,
                attempt_id,
                next_run_sequence(candidate.identity.last_run_sequence, 2)?,
                now,
                second,
            )?,
        ];
        let append = append_request(
            &candidate.identity,
            format!(
                "scheduler:claim:{}:{}",
                candidate.identity.run_id,
                candidate.attempt_number.get()
            ),
            records,
        )?;
        Ok(ClaimAttemptBatch {
            append,
            run_id: candidate.identity.run_id,
            activity_id: candidate.identity.activity_id,
            activity_kind: candidate.identity.activity_kind,
            expected_last_run_sequence: candidate.identity.last_run_sequence,
            attempt_id,
            attempt_number: candidate.attempt_number,
            trigger: candidate.trigger,
            worker_id,
            lease_id,
            fencing_token: candidate.fencing_token,
            lease_expires_at,
            telemetry: candidate.identity.telemetry(),
        })
    }

    fn recovery_batch(
        &self,
        candidate: RecoveryCandidate,
        worker_id: WorkerId,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> Result<RecoverAttemptBatch, DispatchError> {
        let new_attempt_id = AttemptId::generate();
        let lease_id = LeaseId::generate();
        let records = vec![
            self.record(
                &candidate.identity,
                candidate.stale_fence.attempt_id,
                next_run_sequence(candidate.identity.last_run_sequence, 1)?,
                now,
                RunEvent::AttemptAbandoned {
                    attempt_number: candidate.stale_fence.attempt_number,
                    reason: candidate.interruption_reason,
                },
            )?,
            self.record(
                &candidate.identity,
                new_attempt_id,
                next_run_sequence(candidate.identity.last_run_sequence, 2)?,
                now,
                RunEvent::AttemptClaimed {
                    attempt_number: candidate.new_attempt_number,
                    trigger: AttemptTrigger::Recovery,
                    lease_id,
                    fencing_token: candidate.new_fencing_token,
                    lease_expires_at,
                },
            )?,
            self.record(
                &candidate.identity,
                new_attempt_id,
                next_run_sequence(candidate.identity.last_run_sequence, 3)?,
                now,
                RunEvent::Resumed {
                    attempt_id: new_attempt_id,
                    attempt_number: candidate.new_attempt_number,
                    reason: ResumeReason::Recovery,
                },
            )?,
        ];
        let append = append_request(
            &candidate.identity,
            format!(
                "scheduler:recover:{}:{}",
                candidate.identity.run_id,
                candidate.new_attempt_number.get()
            ),
            records,
        )?;
        Ok(RecoverAttemptBatch {
            append,
            run_id: candidate.identity.run_id,
            activity_id: candidate.identity.activity_id,
            activity_kind: candidate.identity.activity_kind,
            expected_last_run_sequence: candidate.identity.last_run_sequence,
            stale_fence: candidate.stale_fence,
            new_attempt_id,
            new_attempt_number: candidate.new_attempt_number,
            worker_id,
            lease_id,
            fencing_token: candidate.new_fencing_token,
            lease_expires_at,
            telemetry: candidate.identity.telemetry(),
        })
    }

    fn record(
        &self,
        identity: &RunIdentity,
        attempt_id: AttemptId,
        run_sequence: u64,
        now: UtcTimestamp,
        event: RunEvent,
    ) -> Result<ProposedRecord, DispatchError> {
        let payload = self.encoder.encode_run_event(event)?;
        Ok(ProposedRecord {
            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                .map_err(|_| contract_error())?,
            record_id: LedgerRecordId::generate(),
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(identity.activity_id),
            run_id: Some(identity.run_id),
            run_seq: Some(RunSeq::new(run_sequence).map_err(|_| contract_error())?),
            attempt_id: Some(attempt_id),
            participant_id: None,
            producer: Producer::Scheduler,
            occurred_at: now,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&payload),
            payload,
        })
    }
}

impl DispatchStore for PostgresDispatchStore {
    fn advance_due_work(&self) -> ApplicationFuture<'_, Result<u64, DispatchError>> {
        Box::pin(async move {
            observation::observe("due_work", self.advance_expired_interactions()).await
        })
    }

    fn claim_one(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<ClaimOutcome, DispatchError>> {
        let worker_id = worker_id.clone();
        Box::pin(async move {
            observation::observe("claim", self.claim(&worker_id, lease_duration)).await
        })
    }

    fn claim_one_cancellation(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionReceipt>, DispatchError>> {
        let worker_id = worker_id.clone();
        Box::pin(async move {
            observation::observe(
                "cancellation",
                self.claim_cancellation(&worker_id, lease_duration),
            )
            .await
        })
    }
}

impl AttemptControlStore for PostgresDispatchStore {
    fn heartbeat(
        &self,
        fence: &AttemptFence,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
        let fence = fence.clone();
        Box::pin(async move { self.heartbeat_inner(&fence, lease_duration).await })
    }

    fn surrender(
        &self,
        fence: &AttemptFence,
        reason: InterruptionReason,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
        let fence = fence.clone();
        Box::pin(async move { self.surrender_inner(&fence, reason).await })
    }
}

impl PostgresDispatchStore {
    async fn heartbeat_inner(
        &self,
        fence: &AttemptFence,
        lease_duration: Duration,
    ) -> Result<LeaseHeartbeat, AttemptControlError> {
        let extension_ms = i64::try_from(lease_duration.as_millis())
            .map_err(|_| AttemptControlError::ContractViolation)?;
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let now_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let now = UtcTimestamp::from_unix_millis(now_ms);
        let new_expires_at = now
            .checked_add_millis(extension_ms)
            .ok_or(AttemptControlError::ContractViolation)?;
        validate_lease_horizon(now, new_expires_at)
            .map_err(|_| AttemptControlError::ContractViolation)?;

        let row = sqlx::query(
            "SELECT lease.expires_at_ms, run.status
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
        .bind(fence.run_id.into_uuid())
        .bind(fence.attempt_id.into_uuid())
        .bind(fence.worker_id.as_str())
        .bind(fence.lease_id.into_uuid())
        .bind(fence.fencing_token.get().to_string())
        .bind(fence.home_epoch.get().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?
        .ok_or(AttemptControlError::LeaseLost)?;
        let current_expiry: i64 = row
            .try_get("expires_at_ms")
            .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let run_status: i16 = row
            .try_get("status")
            .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        if current_expiry <= now_ms {
            return Err(AttemptControlError::LeaseLost);
        }
        if new_expires_at.unix_millis() <= current_expiry {
            transaction
                .rollback()
                .await
                .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
            return Ok(LeaseHeartbeat {
                database_now: now,
                lease_expires_at: UtcTimestamp::from_unix_millis(current_expiry),
                stop_intent: (run_status == 4).then_some(AttemptStopIntent::UserCancellation),
            });
        }
        let lease = sqlx::query(
            "UPDATE runtime.leases
             SET expires_at_ms = $2, renewed_at_ms = $3
             WHERE run_id = $1",
        )
        .bind(fence.run_id.into_uuid())
        .bind(new_expires_at.unix_millis())
        .bind(now_ms)
        .execute(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let attempt = sqlx::query(
            "UPDATE runtime.attempts
             SET lease_expires_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(fence.attempt_id.into_uuid())
        .bind(new_expires_at.unix_millis())
        .execute(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        if lease.rows_affected() != 1 || attempt.rows_affected() != 1 {
            return Err(AttemptControlError::LeaseLost);
        }
        transaction
            .commit()
            .await
            .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        Ok(LeaseHeartbeat {
            database_now: now,
            lease_expires_at: new_expires_at,
            stop_intent: (run_status == 4).then_some(AttemptStopIntent::UserCancellation),
        })
    }

    async fn surrender_inner(
        &self,
        fence: &AttemptFence,
        reason: InterruptionReason,
    ) -> Result<(), AttemptControlError> {
        let encoded_reason = match reason {
            InterruptionReason::ControlHomeMoved => 3_i16,
            InterruptionReason::SystemShutdown => 4_i16,
            InterruptionReason::LeaseLost
            | InterruptionReason::WorkerLost
            | InterruptionReason::ExternalOutcomeUnknown
            | InterruptionReason::CoordinationRequested
            | InterruptionReason::CoordinationForcedAbort => {
                return Err(AttemptControlError::ContractViolation);
            }
        };
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Scheduler)
            .await
            .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let now_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let owned = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT lease.run_id
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
               AND lease.expires_at_ms > $7
             FOR UPDATE OF lease, run, attempt",
        )
        .bind(fence.run_id.into_uuid())
        .bind(fence.attempt_id.into_uuid())
        .bind(fence.worker_id.as_str())
        .bind(fence.lease_id.into_uuid())
        .bind(fence.fencing_token.get().to_string())
        .bind(fence.home_epoch.get().to_string())
        .bind(now_ms)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        if owned.is_none() {
            return Err(AttemptControlError::LeaseLost);
        }
        let lease = sqlx::query(
            "UPDATE runtime.leases
             SET expires_at_ms = $2, renewed_at_ms = $2, surrender_reason = $3
             WHERE run_id = $1",
        )
        .bind(fence.run_id.into_uuid())
        .bind(now_ms)
        .bind(encoded_reason)
        .execute(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        let attempt = sqlx::query(
            "UPDATE runtime.attempts
             SET lease_expires_at_ms = $2
             WHERE attempt_id = $1 AND status = 1",
        )
        .bind(fence.attempt_id.into_uuid())
        .bind(now_ms)
        .execute(&mut *transaction)
        .await
        .map_err(|_| AttemptControlError::DatabaseUnavailable)?;
        if lease.rows_affected() != 1 || attempt.rows_affected() != 1 {
            return Err(AttemptControlError::LeaseLost);
        }
        transaction
            .commit()
            .await
            .map_err(|_| AttemptControlError::DatabaseUnavailable)
    }
}

#[derive(Debug, Clone)]
struct RunIdentity {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    activity_kind: ActivityKind,
    run_id: RunId,
    home_epoch: HomeEpoch,
    conversation_offset: LedgerOffset,
    last_run_sequence: RunSeq,
    correlation_id: CorrelationId,
    trace_context: TraceContext,
}

impl RunIdentity {
    fn telemetry(&self) -> RunTelemetryContext {
        RunTelemetryContext {
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            activity_id: self.activity_id,
            run_id: self.run_id,
            correlation_id: self.correlation_id,
            trace_context: self.trace_context.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ReadyCandidate {
    identity: RunIdentity,
    attempt_number: AttemptNumber,
    fencing_token: FencingToken,
    trigger: AttemptTrigger,
}

#[derive(Debug, Clone)]
struct RecoveryCandidate {
    identity: RunIdentity,
    stale_fence: musteragent_ledger::AttemptFence,
    new_attempt_number: AttemptNumber,
    new_fencing_token: FencingToken,
    interruption_reason: InterruptionReason,
}

#[derive(Debug, Clone)]
struct ExpiredInteractionCandidate {
    identity: RunIdentity,
    interaction_id: InteractionId,
}

fn decode_ready(row: sqlx::postgres::PgRow) -> Result<ReadyCandidate, DispatchError> {
    Ok(ReadyCandidate {
        identity: decode_identity(&row)?,
        attempt_number: attempt_number(
            row.try_get("next_attempt_number").map_err(database_error)?,
        )?,
        fencing_token: fencing_token(row.try_get("next_fencing_token").map_err(database_error)?)?,
        trigger: decode_trigger(
            row.try_get("next_attempt_trigger")
                .map_err(database_error)?,
        )?,
    })
}

fn decode_recovery(row: sqlx::postgres::PgRow) -> Result<RecoveryCandidate, DispatchError> {
    let identity = decode_identity(&row)?;
    let run_id = identity.run_id;
    let home_epoch = identity.home_epoch;
    let stale_attempt_number =
        attempt_number(row.try_get("attempt_number").map_err(database_error)?)?;
    Ok(RecoveryCandidate {
        identity,
        stale_fence: musteragent_ledger::AttemptFence {
            run_id,
            attempt_id: AttemptId::from_uuid(row.try_get("attempt_id").map_err(database_error)?)
                .map_err(|_| contract_error())?,
            attempt_number: stale_attempt_number,
            worker_id: WorkerId::new(
                row.try_get::<String, _>("worker_id")
                    .map_err(database_error)?,
            )
            .map_err(|_| contract_error())?,
            lease_id: LeaseId::from_uuid(row.try_get("lease_id").map_err(database_error)?)
                .map_err(|_| contract_error())?,
            fencing_token: fencing_token(
                row.try_get("stale_fencing_token").map_err(database_error)?,
            )?,
            home_epoch,
        },
        new_attempt_number: attempt_number(
            row.try_get("next_attempt_number").map_err(database_error)?,
        )?,
        new_fencing_token: fencing_token(
            row.try_get("next_fencing_token").map_err(database_error)?,
        )?,
        interruption_reason: decode_surrender_reason(
            row.try_get("surrender_reason").map_err(database_error)?,
        )?,
    })
}

fn decode_expired_interaction(
    row: sqlx::postgres::PgRow,
) -> Result<ExpiredInteractionCandidate, DispatchError> {
    Ok(ExpiredInteractionCandidate {
        identity: decode_identity(&row)?,
        interaction_id: InteractionId::from_uuid(
            row.try_get("interaction_id").map_err(database_error)?,
        )
        .map_err(|_| contract_error())?,
    })
}

fn decode_surrender_reason(value: Option<i16>) -> Result<InterruptionReason, DispatchError> {
    match value {
        None => Ok(InterruptionReason::WorkerLost),
        Some(3) => Ok(InterruptionReason::ControlHomeMoved),
        Some(4) => Ok(InterruptionReason::SystemShutdown),
        Some(_) => Err(contract_error()),
    }
}

fn decode_identity(row: &sqlx::postgres::PgRow) -> Result<RunIdentity, DispatchError> {
    Ok(RunIdentity {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(database_error)?)
            .map_err(|_| contract_error())?,
        conversation_id: ConversationId::from_uuid(
            row.try_get("conversation_id").map_err(database_error)?,
        )
        .map_err(|_| contract_error())?,
        activity_id: ActivityId::from_uuid(row.try_get("activity_id").map_err(database_error)?)
            .map_err(|_| contract_error())?,
        activity_kind: decode_activity_kind(row.try_get("activity_kind").map_err(database_error)?)?,
        run_id: RunId::from_uuid(row.try_get("run_id").map_err(database_error)?)
            .map_err(|_| contract_error())?,
        home_epoch: HomeEpoch::new(parse_u64(
            row.try_get("home_epoch").map_err(database_error)?,
        )?)
        .map_err(|_| contract_error())?,
        conversation_offset: LedgerOffset::new(parse_u64(
            row.try_get("conversation_offset").map_err(database_error)?,
        )?),
        last_run_sequence: RunSeq::new(parse_u64(
            row.try_get("last_run_sequence").map_err(database_error)?,
        )?)
        .map_err(|_| contract_error())?,
        correlation_id: CorrelationId::from_uuid(
            row.try_get("correlation_id").map_err(database_error)?,
        )
        .map_err(|_| contract_error())?,
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                row.try_get::<String, _>("traceparent")
                    .map_err(database_error)?,
            )
            .map_err(|_| contract_error())?,
            tracestate: row
                .try_get::<Option<String>, _>("tracestate")
                .map_err(database_error)?
                .map(TraceState::new)
                .transpose()
                .map_err(|_| contract_error())?,
        },
    })
}

fn decode_activity_kind(value: i16) -> Result<ActivityKind, DispatchError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(contract_error()),
    }
}

fn append_request(
    identity: &RunIdentity,
    idempotency: String,
    records: Vec<ProposedRecord>,
) -> Result<AppendRequest, DispatchError> {
    let idempotency_key = IdempotencyKey::new(idempotency).map_err(|_| contract_error())?;
    Ok(AppendRequest {
        tenant_id: identity.tenant_id,
        conversation_id: identity.conversation_id,
        expected_home_epoch: identity.home_epoch,
        expected_offset: identity.conversation_offset,
        command_fingerprint: Sha256::digest(format!(
            "{}:{}:{}",
            identity.run_id,
            identity.last_run_sequence.get(),
            idempotency_key.as_str()
        )),
        idempotency_key,
        records,
        outbox: Vec::new(),
    })
}

async fn database_now(pool: &PgPool) -> Result<UtcTimestamp, DispatchError> {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .map_err(database_error)?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

fn parse_u64(value: String) -> Result<u64, DispatchError> {
    value.parse().map_err(|_| contract_error())
}

fn next_run_sequence(current: RunSeq, increment: u64) -> Result<u64, DispatchError> {
    current
        .get()
        .checked_add(increment)
        .ok_or_else(contract_error)
}

fn attempt_number(value: i32) -> Result<AttemptNumber, DispatchError> {
    AttemptNumber::new(u32::try_from(value).map_err(|_| contract_error())?)
        .map_err(|_| contract_error())
}

fn fencing_token(value: String) -> Result<FencingToken, DispatchError> {
    FencingToken::new(parse_u64(value)?).map_err(|_| contract_error())
}

fn decode_trigger(value: i16) -> Result<AttemptTrigger, DispatchError> {
    match value {
        1 => Ok(AttemptTrigger::Initial),
        2 => Ok(AttemptTrigger::Retry),
        3 => Ok(AttemptTrigger::Recovery),
        4 => Ok(AttemptTrigger::InteractionResumed),
        5 => Ok(AttemptTrigger::BudgetAmended),
        6 => Ok(AttemptTrigger::DelegationCompleted),
        7 => Ok(AttemptTrigger::ReconciliationDue),
        _ => Err(contract_error()),
    }
}

fn is_claim_race(error: &PostgresExecutionError) -> bool {
    matches!(
        error,
        PostgresExecutionError::RunNotClaimable
            | PostgresExecutionError::ConversationRunBusy
            | PostgresExecutionError::LeaseNotExpiredOrStale
            | PostgresExecutionError::StaleFence
            | PostgresExecutionError::Ledger(crate::PostgresLedgerError::Contract(
                musteragent_ledger::LedgerError::SequenceConflict { .. }
            ))
    )
}

fn is_interaction_expiry_race(error: &PostgresInteractionError) -> bool {
    matches!(
        error,
        PostgresInteractionError::NotFound
            | PostgresInteractionError::TransitionConflict
            | PostgresInteractionError::NotExpired
            | PostgresInteractionError::RunTransitionConflict
            | PostgresInteractionError::Ledger(PostgresLedgerError::Contract(
                musteragent_ledger::LedgerError::SequenceConflict { .. }
                    | musteragent_ledger::LedgerError::HomeEpochMismatch
            ))
    )
}

fn map_interaction_expiry_error(error: PostgresInteractionError) -> DispatchError {
    if is_interaction_expiry_race(&error) {
        DispatchError {
            class: DispatchErrorClass::SerializationConflict,
        }
    } else if matches!(
        error,
        PostgresInteractionError::Sql(_)
            | PostgresInteractionError::Transaction(_)
            | PostgresInteractionError::Ledger(PostgresLedgerError::Database(_))
            | PostgresInteractionError::Ledger(PostgresLedgerError::Transaction(_))
    ) {
        database_error(error)
    } else {
        contract_error()
    }
}

fn derived_id(interaction_id: InteractionId, label: &[u8]) -> uuid::Uuid {
    let mut seed = b"musteragent.legacy.scheduler.interaction.v1\0".to_vec();
    seed.extend_from_slice(interaction_id.into_uuid().as_bytes());
    seed.extend_from_slice(label);
    let digest = Sha256::digest(seed).into_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn map_execution_error(error: PostgresExecutionError) -> DispatchError {
    if is_claim_race(&error) {
        DispatchError {
            class: DispatchErrorClass::SerializationConflict,
        }
    } else if matches!(
        error,
        PostgresExecutionError::Transaction(_)
            | PostgresExecutionError::Ledger(crate::PostgresLedgerError::Transaction(_))
    ) {
        database_error(error)
    } else if let Some(source) = execution_database_source(&error) {
        classify_database_error(source)
    } else {
        contract_error()
    }
}

fn execution_database_source(error: &PostgresExecutionError) -> Option<&sqlx::Error> {
    match error {
        PostgresExecutionError::Database(source)
        | PostgresExecutionError::Ledger(crate::PostgresLedgerError::Database(source)) => {
            Some(source)
        }
        _ => None,
    }
}

fn classify_database_error(error: &sqlx::Error) -> DispatchError {
    let class = match error.as_database_error().map(|source| source.kind()) {
        Some(sqlx::error::ErrorKind::UniqueViolation) => DispatchErrorClass::SerializationConflict,
        Some(
            sqlx::error::ErrorKind::ForeignKeyViolation
            | sqlx::error::ErrorKind::NotNullViolation
            | sqlx::error::ErrorKind::CheckViolation,
        ) => DispatchErrorClass::ContractViolation,
        _ => DispatchErrorClass::DatabaseUnavailable,
    };
    DispatchError { class }
}

fn database_error<T>(_error: T) -> DispatchError {
    DispatchError {
        class: DispatchErrorClass::DatabaseUnavailable,
    }
}

fn contract_error() -> DispatchError {
    DispatchError {
        class: DispatchErrorClass::ContractViolation,
    }
}
