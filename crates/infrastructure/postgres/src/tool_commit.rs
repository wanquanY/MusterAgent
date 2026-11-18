use std::sync::Arc;

use musteragent_approval::{
    ApprovalDecision, Interaction, InteractionEvent, InteractionKind, InteractionStatus,
    SecureResponseRef,
};
use musteragent_capability::CapabilityKey;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, AuthorizationReceiptId, CapabilityProviderId, ConversationId,
    CorrelationId, IdempotencyKey, InteractionId, InvocationId, LedgerRecordId, PrincipalId,
    RunSeq, TenantId, ToolCallId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_policy::{ApprovalRequirement, Effect, PermissionMode};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptContext, RunEvent, RunWaitReason};
use musteragent_runtime_capability::{
    ApprovalRequest, AuthorizationReceipt, AuthorizationReceiptQuery, CapabilityResult,
    DurableToolInvocation, DurableToolObservation, FencedToolInvocationJournal,
    InputInteractionRequest, InteractionResolution, ReconciliationWaitRequest,
    ToolExecutionPortError, ToolExecutionStart, ToolStartMode, ToolTerminalOutcome,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, OpaqueExternalRef};
use musteragent_runtime_interaction::{InteractionSemanticRecords, RequestInteractionCommit};
use musteragent_storage_protection::{
    OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef,
};
use musteragent_tool::{
    ToolEvent, ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract,
};
use sqlx::{PgConnection, PgPool, Row};
use thiserror::Error;

use crate::commit_lane::{CommitLaneError, CommitLaneState};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};
use crate::{PostgresInteractionError, PostgresInteractionStore};

pub(crate) mod atomic_effect;
mod codec;
mod interaction;
mod original_receipt;
mod retained;
use interaction::JournalInteractionRequest;
mod liveness;
mod persistence;
mod sandbox_route;
use persistence::{insert_invocation, persist_terminal};
mod failure_classification;
#[path = "tool_commit/support.rs"]
mod support;

use codec::*;
use failure_classification::{database_port, transaction_port};
use support::*;

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresFencedToolInvocationJournal {
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    receipt_snapshots: Option<Arc<dyn musteragent_context::RuntimeSnapshotDecoder>>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    reference_keys: Arc<OpaqueRefKeyring>,
    liveness: Option<crate::PostgresFencedExecutionLivenessJournal>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedToolInvocationJournal {
    pub(crate) fn database(&self) -> &crate::PostgresDatabaseRuntime {
        self.ledger.database()
    }
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(database, encoder, decoder, reference_keys, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database),
            pool,
            encoder,
            decoder,
            receipt_snapshots: None,
            application_catalogs: None,
            reference_keys,
            liveness: None,
            audit,
        }
    }

    #[must_use]
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.liveness = self
            .liveness
            .take()
            .map(|journal| journal.with_application_catalogs(source.clone()));
        self.application_catalogs = source;
        self
    }

    async fn load_or_begin_inner(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        let prepared = self
            .prepare_liveness_material(attempt, Some(&invocation.tool_key), None)
            .await?;
        prepared
            .load_or_begin_prepared(attempt, invocation, retry_contract)
            .await
    }

    async fn load_or_begin_prepared(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        if invocation.run_id != attempt.run_id
            || invocation.status != ToolInvocationStatus::Requested
            || invocation.execution_attempt_id.is_some()
            || invocation.result_ref.is_some()
            || invocation.interaction_id.is_some()
        {
            return Err(ToolCommitError::InvalidTransition);
        }
        let (mut transaction, state) = self.begin_commit_lane(attempt).await?;
        if let Some(existing) = self
            .load_invocation_on(&mut transaction, state.tenant_id, invocation.invocation_id)
            .await?
        {
            ensure_same_request(&existing, &invocation, retry_contract)?;
            return Ok(existing);
        }
        if invocation.requested_by_attempt_id != attempt.attempt_id {
            return Err(ToolCommitError::InvalidTransition);
        }
        self.validate_liveness_intent(
            &mut transaction,
            &state,
            attempt,
            &invocation,
            retry_contract,
        )
        .await?;
        let event = DomainEvent::Tool(ToolEvent::InvocationRequested {
            invocation_id: invocation.invocation_id,
            tool_call_id: invocation.tool_call_id.clone(),
            tool_key: invocation.tool_key.clone(),
            effect: invocation.effect,
            request_ref: invocation.request_ref.clone(),
            timing: invocation.timing.clone(),
        });
        let requested_at = database_now_ms(&mut transaction).await?;
        let append = self.append_for_event(
            &state,
            attempt,
            invocation.invocation_id,
            "requested",
            requested_at,
            event,
        )?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                let existing = self
                    .load_invocation(state.tenant_id, invocation.invocation_id)
                    .await?
                    .ok_or(ToolCommitError::IdempotencyConflict)?;
                ensure_same_request(&existing, &invocation, retry_contract)?;
                return Ok(existing);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        authorize_content(
            prepared.transaction_mut(),
            state.tenant_id,
            &invocation.request_ref,
        )
        .await?;
        insert_invocation(
            prepared.transaction_mut(),
            state.tenant_id,
            state.conversation_id,
            &invocation,
            retry_contract,
        )
        .await?;
        advance_run_sequence(
            prepared.transaction_mut(),
            attempt.run_id,
            state.next_sequence(1)?,
        )
        .await?;
        prepared.commit(&append).await?;
        self.required_invocation(state.tenant_id, invocation.invocation_id)
            .await
    }

    async fn start_inner(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        reconciliation_ref: Option<OpaqueExternalRef>,
        start: ToolExecutionStart,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        let prepared = self
            .prepare_liveness_material(attempt, None, Some(invocation_id))
            .await?;
        prepared
            .start_prepared(attempt, invocation_id, mode, reconciliation_ref, start)
            .await
    }

    async fn start_prepared(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        reconciliation_ref: Option<OpaqueExternalRef>,
        start: ToolExecutionStart,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        let (mut transaction, state) = self.begin_commit_lane(attempt).await?;
        let existing = self
            .load_invocation_on(&mut transaction, state.tenant_id, invocation_id)
            .await?;
        let existing = existing.ok_or(ToolCommitError::NotFound)?;
        validate_start_reconciliation_identity(&existing, mode, reconciliation_ref.as_ref())?;
        validate_execution_start(&existing, mode, &start)?;
        if existing.invocation.status == ToolInvocationStatus::Running
            && existing.invocation.execution_attempt_id == Some(attempt.attempt_id)
        {
            return Ok(existing);
        }
        validate_start(&existing, attempt, mode)?;
        let liveness = self
            .prepare_liveness_start(
                &mut transaction,
                &state,
                attempt,
                &existing.invocation,
                existing.retry_contract,
                mode,
            )
            .await?;
        let event = DomainEvent::Tool(ToolEvent::InvocationStarted {
            invocation_id,
            execution_attempt_id: attempt.attempt_id,
            capability_provider_id: existing.invocation.capability_provider_id,
            execution_timing: Some(start.timing.clone()),
        });
        let start_tag = format!("started:{}", attempt.attempt_id);
        let mut events = vec![event];
        if let Some(change) = &liveness {
            events.push(change.event());
        }
        let records = self.records(
            &state,
            attempt,
            invocation_id,
            start.started_at,
            &start_tag,
            events,
        )?;
        let append = state.append(format!("tool:{start_tag}:{invocation_id}"), records)?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self
                    .required_invocation(state.tenant_id, invocation_id)
                    .await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        let protected_reconciliation_ref = reconciliation_ref
            .as_ref()
            .map(|reference| {
                protect(
                    &self.reference_keys,
                    invocation_id,
                    "reconciliation",
                    reference.as_str(),
                )
            })
            .transpose()?;
        let changed = match mode {
            ToolStartMode::Initial => {
                // The shared domain transition was validated under the commit
                // lane above. A cancelled/expired question has no response row;
                // requiring one here would contradict that validated outcome.
                sqlx::query(
                    "UPDATE tool.tool_invocations AS invocation
                     SET status = 3, execution_attempt_id = $3,
                         started_at_ms = COALESCE(started_at_ms, $4),
                         reconciliation_ref_protected = $5,
                         deadline_ms = $6
                     WHERE tenant_id = $1 AND invocation_id = $2
                       AND status IN (1, 2)",
                )
                .bind(state.tenant_id.into_uuid())
                .bind(invocation_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .bind(start.started_at.unix_millis())
                .bind(protected_reconciliation_ref.as_deref())
                .bind(
                    start
                        .timing
                        .legacy_deadline()
                        .map(UtcTimestamp::unix_millis),
                )
                .execute(&mut *prepared.transaction_mut())
                .await?
            }
            ToolStartMode::ReplaySafeRecovery => {
                sqlx::query(
                    "UPDATE tool.tool_invocations AS invocation
                     SET status = 3, execution_attempt_id = $3, started_at_ms = $4,
                         terminal_reason = NULL, reconciliation_ref_protected = NULL,
                         indeterminate_observed_at_ms = NULL, deadline_ms = $5
                     WHERE tenant_id = $1 AND invocation_id = $2 AND status IN (3, 6)
                       AND execution_attempt_id <> $3
                       AND retry_contract IN (1, 2)
                       AND NOT EXISTS (
                           SELECT 1 FROM runtime.attempts AS old_attempt
                           WHERE old_attempt.attempt_id = invocation.execution_attempt_id
                             AND old_attempt.status = 1
                       )",
                )
                .bind(state.tenant_id.into_uuid())
                .bind(invocation_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .bind(start.started_at.unix_millis())
                .bind(
                    start
                        .timing
                        .legacy_deadline()
                        .map(UtcTimestamp::unix_millis),
                )
                .execute(&mut *prepared.transaction_mut())
                .await?
            }
            ToolStartMode::ReconciliationProvenNotApplied => {
                sqlx::query(
                    "UPDATE tool.tool_invocations
                     SET status = 3, execution_attempt_id = $3,
                         started_at_ms = $4, terminal_reason = NULL,
                         reconciliation_ref_protected = $5,
                         indeterminate_observed_at_ms = NULL, deadline_ms = $6
                     WHERE tenant_id = $1 AND invocation_id = $2 AND status = 6
                       AND retry_contract = 3",
                )
                .bind(state.tenant_id.into_uuid())
                .bind(invocation_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .bind(start.started_at.unix_millis())
                .bind(protected_reconciliation_ref.as_deref())
                .bind(
                    start
                        .timing
                        .legacy_deadline()
                        .map(UtcTimestamp::unix_millis),
                )
                .execute(&mut *prepared.transaction_mut())
                .await?
            }
        };
        require_one(changed.rows_affected())?;
        if let Some(change) = &liveness {
            change
                .persist(prepared.transaction_mut())
                .await
                .map_err(ToolCommitError::Liveness)?;
        }
        advance_run_sequence(
            prepared.transaction_mut(),
            attempt.run_id,
            state.next_sequence(if liveness.is_some() { 2 } else { 1 })?,
        )
        .await?;
        prepared.commit(&append).await?;
        self.required_invocation(state.tenant_id, invocation_id)
            .await
    }

    async fn complete_inner(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        let (mut transaction, state) = self.begin_commit_lane(attempt).await?;
        let existing = self
            .load_invocation_on(&mut transaction, state.tenant_id, invocation_id)
            .await?;
        let existing = existing.ok_or(ToolCommitError::NotFound)?;
        if existing.invocation.status.is_terminal() {
            if existing.recorded_outcome.as_ref() == Some(&outcome) {
                self.audit
                    .append_tool_fact_in_transaction(
                        &mut transaction,
                        state.tenant_id,
                        invocation_id,
                    )
                    .await?;
                transaction.commit().await?;
                return Ok(existing);
            }
            return Err(ToolCommitError::IdempotencyConflict);
        }
        if existing.invocation.status == ToolInvocationStatus::Indeterminate
            && existing.recorded_outcome.as_ref() == Some(&outcome)
        {
            self.audit
                .append_tool_fact_in_transaction(&mut transaction, state.tenant_id, invocation_id)
                .await?;
            transaction.commit().await?;
            return Ok(existing);
        }
        validate_terminal(&outcome)?;
        validate_completion(&existing, attempt, &outcome, finished_at)?;
        validate_completion_reconciliation_identity(&existing, &outcome)?;
        let event = terminal_event(invocation_id, &outcome);
        let completion_tag = match (existing.invocation.status, &outcome) {
            (ToolInvocationStatus::Running, ToolTerminalOutcome::Indeterminate { .. }) => {
                format!("indeterminate:{}", attempt.attempt_id)
            }
            (ToolInvocationStatus::Indeterminate, _) => {
                format!("reconciled:{}", attempt.attempt_id)
            }
            _ => "terminal".to_owned(),
        };
        let liveness = self
            .prepare_liveness_terminal(
                &mut transaction,
                &state,
                attempt,
                &existing.invocation,
                existing.retry_contract,
                &outcome,
            )
            .await?;
        let mut events = vec![event];
        if let Some(change) = &liveness {
            events.push(change.event());
        }
        let records = self.records(
            &state,
            attempt,
            invocation_id,
            finished_at,
            &completion_tag,
            events,
        )?;
        let append = state.append(format!("tool:{completion_tag}:{invocation_id}"), records)?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self
                    .required_invocation(state.tenant_id, invocation_id)
                    .await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        persist_terminal(
            prepared.transaction_mut(),
            &self.reference_keys,
            state.tenant_id,
            invocation_id,
            &outcome,
            finished_at,
        )
        .await?;
        if let Some(change) = &liveness {
            change
                .persist(prepared.transaction_mut())
                .await
                .map_err(ToolCommitError::Liveness)?;
        }
        advance_run_sequence(
            prepared.transaction_mut(),
            attempt.run_id,
            state.next_sequence(if liveness.is_some() { 2 } else { 1 })?,
        )
        .await?;
        self.audit
            .append_tool_fact_in_transaction(
                prepared.transaction_mut(),
                state.tenant_id,
                invocation_id,
            )
            .await?;
        prepared.commit(&append).await?;
        self.required_invocation(state.tenant_id, invocation_id)
            .await
    }

    async fn wait_for_reconciliation_inner(
        &self,
        attempt: &AttemptContext,
        request: ReconciliationWaitRequest,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        if request.resume_not_before <= request.observed_at {
            return Err(ToolCommitError::InvalidTransition);
        }
        let (mut transaction, state) = self.begin_commit_lane(attempt).await?;
        let durable = self
            .load_invocation_on(&mut transaction, state.tenant_id, request.invocation_id)
            .await?
            .ok_or(ToolCommitError::NotFound)?;
        let reconciliation_is_pending = durable.invocation.status
            == ToolInvocationStatus::Indeterminate
            && matches!(
                durable.recorded_outcome.as_ref(),
                Some(ToolTerminalOutcome::Indeterminate { reconciliation_ref, .. })
                    if durable.prebound_reconciliation_ref.as_ref() == Some(reconciliation_ref)
            );
        // Authority reconciliation is independent of the Worker transaction
        // and may commit a terminal outcome after the Worker observed
        // `Indeterminate` but before this suspension fence. Suspending the
        // Attempt remains correct: the next Attempt replays the already-sealed
        // terminal outcome without dispatching the provider again.
        let reconciliation_won_the_race = durable.invocation.status.is_terminal()
            && durable.recorded_outcome.as_ref().is_some_and(|outcome| {
                !matches!(outcome, ToolTerminalOutcome::Indeterminate { .. })
            });
        let valid = durable.invocation.run_id == attempt.run_id
            && durable.retry_contract == ToolRetryContract::QueryBeforeRetry
            && (reconciliation_is_pending || reconciliation_won_the_race);
        if !valid {
            return Err(ToolCommitError::InvalidTransition);
        }
        let events = vec![
            DomainEvent::Run(RunEvent::AttemptSuspended {
                attempt_number: attempt.attempt_number,
                interaction_id: None,
                invocation_id: Some(request.invocation_id),
            }),
            DomainEvent::Run(RunEvent::Waiting {
                reason: RunWaitReason::ExternalOperation {
                    invocation_id: request.invocation_id,
                },
                resume_not_before: Some(request.resume_not_before),
            }),
        ];
        let tag = format!("reconciliation-wait:{}", attempt.attempt_id);
        let records = self.records(
            &state,
            attempt,
            request.invocation_id,
            request.observed_at,
            &tag,
            events,
        )?;
        let append = state.append(
            format!("tool:reconciliation-wait:{}", attempt.attempt_id),
            records,
        )?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self
                    .required_invocation(state.tenant_id, request.invocation_id)
                    .await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        let checkpoint = sqlx::query(
            "UPDATE runtime.loop_checkpoints
             SET checkpoint_version = checkpoint_version + 1, updated_at_ms = $3
             WHERE run_id = $1 AND attempt_id = $2 AND phase = 3
               AND model_invocation_id IS NOT NULL
               AND cardinality(remaining_tool_call_ids) > 0",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(request.observed_at.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        require_one(checkpoint.rows_affected())?;
        let suspended = sqlx::query(
            "UPDATE runtime.attempts SET status = 2, finished_at_ms = $2
             WHERE attempt_id = $1 AND run_id = $3 AND status = 1",
        )
        .bind(attempt.attempt_id.into_uuid())
        .bind(request.observed_at.unix_millis())
        .bind(attempt.run_id.into_uuid())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        require_one(suspended.rows_affected())?;
        sqlx::query("DELETE FROM runtime.leases WHERE run_id = $1 AND attempt_id = $2")
            .bind(attempt.run_id.into_uuid())
            .bind(attempt.attempt_id.into_uuid())
            .execute(&mut *prepared.transaction_mut())
            .await?;
        let next_sequence = state.next_sequence(2)?;
        let waiting = sqlx::query(
            "UPDATE runtime.runs
             SET status = 3, wait_reason = 7,
                 wait_interaction_id = NULL, wait_invocation_id = $2,
                 wait_child_activity_id = NULL, not_before_ms = $3,
                 next_attempt_trigger = 7, last_run_sequence = $4::bigint,
                 run_version = run_version + 1
             WHERE tenant_id = $1 AND run_id = $5 AND status IN (2, 4)
               AND last_run_sequence = $6::bigint",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(request.invocation_id.into_uuid())
        .bind(request.resume_not_before.unix_millis())
        .bind(next_sequence.to_string())
        .bind(attempt.run_id.into_uuid())
        .bind(state.last_run_sequence.get().to_string())
        .execute(&mut *prepared.transaction_mut())
        .await?;
        require_one(waiting.rows_affected())?;
        prepared.commit(&append).await?;
        self.required_invocation(state.tenant_id, request.invocation_id)
            .await
    }

    /// Acquires the durable per-Conversation commit lane before reading any
    /// sequence-bearing state. The stream row lock and the subsequent Ledger
    /// append live in the same transaction, so another Worker can neither
    /// interleave a transition nor force this writer to act on a stale offset.
    async fn begin_commit_lane(
        &self,
        attempt: &AttemptContext,
    ) -> Result<(crate::GovernedPostgresTransaction<'static>, CommitLaneState), ToolCommitError>
    {
        crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(ToolCommitError::from)
    }

    async fn required_invocation(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        self.load_invocation(tenant_id, invocation_id)
            .await?
            .ok_or(ToolCommitError::NotFound)
    }

    async fn load_invocation(
        &self,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<Option<DurableToolInvocation>, ToolCommitError> {
        let mut connection = self.pool.acquire().await?;
        self.load_invocation_on(&mut connection, tenant_id, invocation_id)
            .await
    }

    async fn load_invocation_on(
        &self,
        connection: &mut PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
    ) -> Result<Option<DurableToolInvocation>, ToolCommitError> {
        let row = sqlx::query(
            "SELECT invocation.conversation_id, invocation.run_id,
                    invocation.requested_by_attempt_id,
                    invocation.execution_attempt_id, invocation.tool_call_id,
                    invocation.tool_ref, invocation.capability_provider_id,
                    invocation.status, invocation.effect,
                    invocation.approval_policy, invocation.retry_contract,
                    invocation.idempotency_key, invocation.deadline_ms, invocation.activity_timing, invocation.activity_timing_hash,
                    invocation.sandbox_executor, invocation.sandbox_reason, invocation.sandbox_compatibility,
                    invocation.sandbox_decided_at_ms, invocation.sandbox_route_hash,
                    invocation.interaction_id,
                    invocation.reconciliation_ref_protected,
                    request.content_id AS request_content_id,
                    request.media_type AS request_media_type,
                    request.byte_length::text AS request_byte_length,
                    request.content_hash AS request_hash,
                    result_content.content_id AS result_content_id,
                    result_content.media_type AS result_media_type,
                    result_content.byte_length::text AS result_byte_length,
                    result_content.content_hash AS result_content_hash,
                    result.result_schema_key, result.result_schema_hash, result.provider_request_ref_protected,
                    interaction.status AS interaction_status,
                    retired.interaction_id AS retired_review_id,
                    response.response_kind, response.approval_decision,
                    response.content_id AS interaction_response_content_id,
                    response.secure_response_ref_protected,
                    response_content.media_type AS interaction_response_media_type,
                    response_content.byte_length::text AS interaction_response_byte_length,
                    response_content.content_hash AS interaction_response_hash,
                    requested.record_id AS request_record_id
             FROM tool.tool_invocations AS invocation
             JOIN event_store.command_idempotency AS requested_command
               ON requested_command.tenant_id = invocation.tenant_id
              AND requested_command.idempotency_key =
                  'tool:requested:' || invocation.invocation_id::text
             JOIN event_store.ledger_records AS requested
               ON requested.tenant_id = requested_command.tenant_id
              AND requested.conversation_id = requested_command.conversation_id
              AND requested.home_epoch = requested_command.home_epoch
              AND requested.conversation_seq = requested_command.first_sequence
              AND requested.run_id = invocation.run_id
              AND requested.correlation_id = invocation.invocation_id
             JOIN artifact.content_refs AS request
               ON request.content_id = invocation.request_content_id
              AND request.tenant_id = invocation.tenant_id
             LEFT JOIN tool.tool_results AS result
               ON result.invocation_id = invocation.invocation_id
             LEFT JOIN artifact.content_refs AS result_content
               ON result_content.content_id = result.result_content_id
              AND result_content.tenant_id = invocation.tenant_id
             LEFT JOIN approval.interactions AS interaction
               ON interaction.interaction_id = invocation.interaction_id
             LEFT JOIN approval.task_result_review_retirements AS retired
               ON retired.interaction_id=interaction.interaction_id
              AND retired.tenant_id=invocation.tenant_id
              AND retired.invocation_id=invocation.invocation_id
              AND retired.run_id=invocation.run_id AND invocation.tool_ref='tasks.verify@1'
             LEFT JOIN approval.interaction_responses AS response
               ON response.interaction_id = interaction.interaction_id
             LEFT JOIN artifact.content_refs AS response_content
               ON response_content.content_id = response.content_id
              AND response_content.tenant_id = invocation.tenant_id
             WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status = decode_status(row.try_get("status")?)?;
        let result_ref = optional_content_ref(
            row.try_get("result_content_id")?,
            row.try_get("result_media_type")?,
            row.try_get("result_byte_length")?,
            row.try_get("result_content_hash")?,
        )?;
        let interaction_id = optional_id(row.try_get("interaction_id")?)?;
        let interaction_resolution = match (
            interaction_id,
            row.try_get::<Option<i16>, _>("interaction_status")?,
            row.try_get::<Option<i16>, _>("response_kind")?,
            row.try_get::<Option<i16>, _>("approval_decision")?,
        ) {
            (Some(_), Some(1), None, None) => Some(InteractionResolution::Pending),
            (Some(_), Some(2), Some(1), Some(1)) => {
                Some(InteractionResolution::Approval(ApprovalDecision::Approve))
            }
            (Some(_), Some(2), Some(1), Some(2)) => {
                Some(InteractionResolution::Approval(ApprovalDecision::Deny))
            }
            (Some(_), Some(2), Some(1), Some(3)) => Some(InteractionResolution::Approval(
                ApprovalDecision::ApproveSession,
            )),
            (Some(_), Some(2), Some(2), None) => Some(InteractionResolution::Content(content_ref(
                row.try_get::<Option<uuid::Uuid>, _>("interaction_response_content_id")?
                    .ok_or(ToolCommitError::Corrupt)?,
                row.try_get::<Option<String>, _>("interaction_response_media_type")?
                    .ok_or(ToolCommitError::Corrupt)?,
                row.try_get::<Option<String>, _>("interaction_response_byte_length")?
                    .ok_or(ToolCommitError::Corrupt)?,
                row.try_get::<Option<Vec<u8>>, _>("interaction_response_hash")?
                    .ok_or(ToolCommitError::Corrupt)?,
            )?)),
            (Some(interaction_id), Some(2), Some(3), None) => {
                let protected = ProtectedOpaqueRef::from_bytes(
                    row.try_get::<Option<Vec<u8>>, _>("secure_response_ref_protected")?
                        .ok_or(ToolCommitError::Corrupt)?,
                )?;
                let context = format!("interaction:{interaction_id}:secure-response");
                let value = self.reference_keys.open(context.as_bytes(), &protected)?;
                Some(InteractionResolution::Secure(
                    SecureResponseRef::new(value).map_err(|_| ToolCommitError::Corrupt)?,
                ))
            }
            (Some(_), Some(3), None, None) => Some(InteractionResolution::Expired),
            (Some(_), Some(4), None, None) => Some(
                if row
                    .try_get::<Option<uuid::Uuid>, _>("retired_review_id")?
                    .is_some()
                {
                    InteractionResolution::RetiredResultReview
                } else {
                    InteractionResolution::Cancelled
                },
            ),
            (None, None, None, None) => None,
            _ => return Err(ToolCommitError::Corrupt),
        };
        let retry_contract = decode_retry(row.try_get("retry_contract")?)?;
        let prebound_reconciliation_ref = row
            .try_get::<Option<Vec<u8>>, _>("reconciliation_ref_protected")?
            .map(|protected| {
                unprotect(
                    &self.reference_keys,
                    invocation_id,
                    "reconciliation",
                    protected,
                )
            })
            .transpose()?;
        let recorded_outcome =
            if status.is_terminal() || status == ToolInvocationStatus::Indeterminate {
                Some(
                    self.load_recorded_outcome_on(
                        connection,
                        tenant_id,
                        invocation_id,
                        result_ref.as_ref(),
                        StoredResultMetadata {
                            schema_key: row.try_get("result_schema_key")?,
                            schema_hash: row.try_get("result_schema_hash")?,
                            provider_request_ref: row
                                .try_get::<Option<Vec<u8>>, _>("provider_request_ref_protected")?
                                .map(|protected| {
                                    unprotect(
                                        &self.reference_keys,
                                        invocation_id,
                                        "provider-request",
                                        protected,
                                    )
                                })
                                .transpose()?,
                        },
                    )
                    .await?,
                )
            } else {
                None
            };
        let invocation = ToolInvocation {
            invocation_id,
            run_id: id(row.try_get("run_id")?)?,
            requested_by_attempt_id: id(row.try_get("requested_by_attempt_id")?)?,
            execution_attempt_id: optional_id(row.try_get("execution_attempt_id")?)?,
            tool_call_id: ToolCallId::new(row.try_get::<String, _>("tool_call_id")?)
                .map_err(|_| ToolCommitError::Corrupt)?,
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new(row.try_get::<String, _>("tool_ref")?)
                    .map_err(|_| ToolCommitError::Corrupt)?,
            ),
            capability_provider_id: id(row.try_get("capability_provider_id")?)?,
            status,
            effect: decode_effect(row.try_get("effect")?)?,
            approval: decode_approval(row.try_get("approval_policy")?)?,
            idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
                .map_err(|_| ToolCommitError::Corrupt)?,
            request_ref: content_ref(
                row.try_get("request_content_id")?,
                row.try_get("request_media_type")?,
                row.try_get("request_byte_length")?,
                row.try_get("request_hash")?,
            )?,
            result_ref,
            interaction_id,
            timing: musteragent_persistence_codec::decode_invocation_timing_columns(
                musteragent_persistence_codec::EncodedInvocationTiming {
                    legacy_deadline_ms: row.try_get("deadline_ms")?,
                    activity: row.try_get("activity_timing")?,
                    activity_hash: row.try_get("activity_timing_hash")?,
                },
            )
            .map_err(|_| ToolCommitError::Corrupt)?,
        };
        let durable = DurableToolInvocation {
            sandbox_route: sandbox_route::decode(&row)?,
            request_record_id: LedgerRecordId::from_uuid(row.try_get("request_record_id")?)
                .map_err(|_| ToolCommitError::Corrupt)?,
            invocation,
            retry_contract,
            prebound_reconciliation_ref,
            recorded_outcome,
            interaction_resolution,
        };
        durable.validate().map_err(|_| ToolCommitError::Corrupt)?;
        Ok(Some(durable))
    }

    async fn load_recorded_outcome_on(
        &self,
        connection: &mut PgConnection,
        tenant_id: TenantId,
        invocation_id: InvocationId,
        result_ref: Option<&ContentRef>,
        metadata: StoredResultMetadata,
    ) -> Result<ToolTerminalOutcome, ToolCommitError> {
        let rows = sqlx::query(
            "SELECT event.encoded_event, record.payload_hash
             FROM event_store.ledger_records AS record
             JOIN event_store.domain_events AS event
               ON event.record_id = record.record_id
             WHERE record.tenant_id = $1 AND record.run_id = (
                 SELECT run_id FROM tool.tool_invocations WHERE invocation_id = $2
             )
             ORDER BY record.run_seq DESC NULLS LAST",
        )
        .bind(tenant_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .fetch_all(&mut *connection)
        .await?;
        for row in rows {
            let bytes: Vec<u8> = row.try_get("encoded_event")?;
            let hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_hash")?)
                .map_err(|_| ToolCommitError::Corrupt)?;
            if Sha256::digest(&bytes) != hash {
                return Err(ToolCommitError::Corrupt);
            }
            let payload = self
                .decoder
                .decode(LedgerRecordKind::DomainEvent, &bytes)
                .map_err(|_| ToolCommitError::Corrupt)?;
            let LedgerPayload::DomainEvent(DomainEvent::Tool(event)) = payload else {
                continue;
            };
            let outcome = match event {
                ToolEvent::InvocationSucceeded {
                    invocation_id: current,
                    result_ref: event_ref,
                    result_hash,
                    provider_request_ref,
                    authorized_content,
                } if current == invocation_id => {
                    let persisted_ref = result_ref.ok_or(ToolCommitError::Corrupt)?;
                    if &event_ref != persisted_ref
                        || result_hash != persisted_ref.sha256
                        || provider_request_ref != metadata.provider_request_ref
                    {
                        return Err(ToolCommitError::Corrupt);
                    }
                    let schema = SchemaRef {
                        schema_key: VersionedKey::new(
                            metadata.schema_key.ok_or(ToolCommitError::Corrupt)?,
                        )
                        .map_err(|_| ToolCommitError::Corrupt)?,
                        schema_hash: Sha256::from_bytes(
                            metadata.schema_hash.ok_or(ToolCommitError::Corrupt)?,
                        )
                        .map_err(|_| ToolCommitError::Corrupt)?,
                    };
                    let result = CapabilityResult {
                        result_ref: event_ref,
                        result_schema: schema,
                        result_hash,
                        provider_request_ref,
                    };
                    if authorized_content.is_empty() {
                        ToolTerminalOutcome::Succeeded(result)
                    } else {
                        ToolTerminalOutcome::SucceededWithAuthorizedContent {
                            result,
                            authorized_content,
                        }
                    }
                }
                ToolEvent::InvocationFailed {
                    invocation_id: current,
                    error,
                    retry_disposition,
                    diagnostic_result_ref,
                    diagnostic_result_hash,
                } if current == invocation_id => ToolTerminalOutcome::Failed {
                    error,
                    retry_disposition,
                    diagnostic_result: decode_diagnostic_result(
                        diagnostic_result_ref,
                        diagnostic_result_hash,
                        result_ref,
                        metadata,
                    )?,
                },
                ToolEvent::InvocationIndeterminate {
                    invocation_id: current,
                    reason,
                    reconciliation_ref,
                    observed_error,
                    diagnostic_result_ref,
                    diagnostic_result_hash,
                } if current == invocation_id => ToolTerminalOutcome::Indeterminate {
                    reason,
                    reconciliation_ref,
                    observed_error,
                    diagnostic_result: decode_diagnostic_result(
                        diagnostic_result_ref,
                        diagnostic_result_hash,
                        result_ref,
                        metadata,
                    )?,
                },
                ToolEvent::InvocationCancelled {
                    invocation_id: current,
                    reason,
                } if current == invocation_id => ToolTerminalOutcome::Cancelled(reason),
                _ => continue,
            };
            return Ok(outcome);
        }
        Err(ToolCommitError::Corrupt)
    }

    async fn load_observation_inner(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> Result<DurableToolObservation, ToolCommitError> {
        // Observation is a read of an already committed transition. It must
        // authenticate the immutable request identity, but it must not require
        // the originating Attempt to remain live: approval/input transitions
        // atomically suspend that Attempt before telemetry observes them.
        if invocation.run_id != attempt.run_id
            || invocation.status != ToolInvocationStatus::Requested
            || invocation.execution_attempt_id.is_some()
            || invocation.result_ref.is_some()
            || invocation.interaction_id.is_some()
        {
            return Err(ToolCommitError::InvalidTransition);
        }
        let tenant_id: uuid::Uuid = sqlx::query_scalar(
            "SELECT invocation.tenant_id
             FROM tool.tool_invocations AS invocation
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = invocation.tenant_id
              AND attempt.run_id = invocation.run_id
             WHERE invocation.invocation_id = $1
               AND invocation.run_id = $2
               AND attempt.attempt_id = $3
               AND attempt.attempt_number = $4
               AND attempt.worker_id = $5
               AND attempt.lease_id = $6
               AND attempt.fencing_token = $7::bigint",
        )
        .bind(invocation.invocation_id.into_uuid())
        .bind(invocation.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get()).map_err(|_| ToolCommitError::Corrupt)?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ToolCommitError::NotFound)?;
        let tenant_id = TenantId::from_uuid(tenant_id).map_err(|_| ToolCommitError::Corrupt)?;
        let durable = self
            .required_invocation(tenant_id, invocation.invocation_id)
            .await?;
        ensure_same_request(&durable, &invocation, retry_contract)?;
        let rows = sqlx::query(
            "SELECT record.record_id, event.encoded_event, record.payload_hash
             FROM event_store.ledger_records AS record
             JOIN event_store.domain_events AS event
               ON event.record_id = record.record_id
             WHERE record.run_id = $1
               AND record.tenant_id = (
                   SELECT tenant_id FROM tool.tool_invocations WHERE invocation_id = $2
               )
             ORDER BY record.run_seq DESC NULLS LAST",
        )
        .bind(durable.invocation.run_id.into_uuid())
        .bind(durable.invocation.invocation_id.into_uuid())
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let bytes: Vec<u8> = row.try_get("encoded_event")?;
            let hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_hash")?)
                .map_err(|_| ToolCommitError::Corrupt)?;
            if Sha256::digest(&bytes) != hash {
                return Err(ToolCommitError::Corrupt);
            }
            let payload = self
                .decoder
                .decode(LedgerRecordKind::DomainEvent, &bytes)
                .map_err(|_| ToolCommitError::Corrupt)?;
            let LedgerPayload::DomainEvent(DomainEvent::Tool(event)) = payload else {
                continue;
            };
            if tool_event_matches_state(&event, &durable) {
                let record_id = LedgerRecordId::from_uuid(row.try_get("record_id")?)
                    .map_err(|_| ToolCommitError::Corrupt)?;
                return Ok(DurableToolObservation::from_journal(record_id, durable));
            }
        }
        Err(ToolCommitError::Corrupt)
    }

    fn append_for_event(
        &self,
        state: &CommitLaneState,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        tag: &str,
        occurred_at: UtcTimestamp,
        event: DomainEvent,
    ) -> Result<AppendRequest, ToolCommitError> {
        let records = self.records(state, attempt, invocation_id, occurred_at, tag, vec![event])?;
        Ok(state.append(format!("tool:{tag}:{invocation_id}"), records)?)
    }

    fn records(
        &self,
        state: &CommitLaneState,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        occurred_at: UtcTimestamp,
        tag: &str,
        events: Vec<DomainEvent>,
    ) -> Result<Vec<ProposedRecord>, ToolCommitError> {
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let payload = LedgerPayload::DomainEvent(event);
                let encoded = self.encoder.encode(payload);
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| ToolCommitError::Corrupt)?,
                    record_id: deterministic_record_id(invocation_id, tag, index)?,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(state.activity_id),
                    run_id: Some(attempt.run_id),
                    run_seq: Some(
                        RunSeq::new(state.next_sequence(u64::try_from(index)?.saturating_add(1))?)
                            .map_err(|_| ToolCommitError::Corrupt)?,
                    ),
                    attempt_id: Some(attempt.attempt_id),
                    participant_id: None,
                    producer: Producer::Worker,
                    occurred_at,
                    causation_id: None,
                    correlation_id: CorrelationId::from_uuid(invocation_id.into_uuid())
                        .map_err(|_| ToolCommitError::Corrupt)?,
                    payload_hash: Sha256::digest(&encoded),
                    payload: encoded,
                })
            })
            .collect()
    }
}

impl FencedToolInvocationJournal for PostgresFencedToolInvocationJournal {
    fn load_or_begin(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.load_or_begin_inner(&attempt, invocation, retry_contract)
                .await
                .map_err(ToolCommitError::port)
        })
    }

    fn load_observation(
        &self,
        attempt: &AttemptContext,
        invocation: ToolInvocation,
        retry_contract: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolObservation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.load_observation_inner(&attempt, invocation, retry_contract)
                .await
                .map_err(ToolCommitError::port)
        })
    }

    fn request_approval(
        &self,
        attempt: &AttemptContext,
        request: ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.request_interaction_inner(
                &attempt,
                JournalInteractionRequest {
                    interaction_id: request.interaction_id,
                    invocation_id: request.invocation_id,
                    kind: InteractionKind::Approval,
                    prompt_ref: request.prompt_ref,
                    response_schema: request.response_schema,
                    requested_at: request.requested_at,
                    expires_at: request.expires_at,
                },
            )
            .await
            .map_err(ToolCommitError::port)
        })
    }

    fn request_input(
        &self,
        attempt: &AttemptContext,
        request: InputInteractionRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.request_interaction_inner(
                &attempt,
                JournalInteractionRequest {
                    interaction_id: request.interaction_id,
                    invocation_id: request.invocation_id,
                    kind: request.kind,
                    prompt_ref: request.prompt_ref,
                    response_schema: request.response_schema,
                    requested_at: request.requested_at,
                    expires_at: request.expires_at,
                },
            )
            .await
            .map_err(ToolCommitError::port)
        })
    }

    fn find_active_authorization_receipt(
        &self,
        query: AuthorizationReceiptQuery,
    ) -> ApplicationFuture<'_, Result<Option<AuthorizationReceipt>, ToolExecutionPortError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT receipt_id, tenant_id, principal_id, conversation_id,
                        workspace_id, source_interaction_id, capability_key,
                        capability_provider_id, permission_mode, scope_fingerprint,
                        issued_at_ms, expires_at_ms
                 FROM approval.authorization_receipts
                 WHERE tenant_id = $1 AND principal_id = $2
                   AND conversation_id = $3
                   AND workspace_id IS NOT DISTINCT FROM $4
                   AND capability_key = $5 AND capability_provider_id = $6
                   AND permission_mode = $7 AND scope_fingerprint = $8
                   AND revoked_at_ms IS NULL AND expires_at_ms > $9
                 ORDER BY issued_at_ms DESC, receipt_id
                 LIMIT 1",
            )
            .bind(query.tenant_id.into_uuid())
            .bind(query.principal_id.into_uuid())
            .bind(query.conversation_id.into_uuid())
            .bind(query.workspace_id.map(WorkspaceId::into_uuid))
            .bind(query.capability_key.as_str())
            .bind(query.capability_provider_id.into_uuid())
            .bind(permission_mode_code(query.permission_mode))
            .bind(query.scope_fingerprint.into_bytes().to_vec())
            .bind(query.observed_at.unix_millis())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| ToolCommitError::Database(error).port())?;
            let receipt = row
                .as_ref()
                .map(authorization_receipt_from_pg_row)
                .transpose()
                .map_err(ToolCommitError::port)?;
            if receipt
                .as_ref()
                .is_some_and(|receipt| !authorization_receipt_matches_query(receipt, &query))
            {
                return Err(ToolCommitError::Corrupt.port());
            }
            Ok(receipt)
        })
    }

    fn issue_authorization_receipt(
        &self,
        attempt: &AttemptContext,
        receipt: AuthorizationReceipt,
    ) -> ApplicationFuture<'_, Result<AuthorizationReceipt, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            receipt
                .validate()
                .map_err(|_| ToolCommitError::InvalidTransition.port())?;
            let lifetime_ms = receipt
                .expires_at
                .unix_millis()
                .checked_sub(receipt.issued_at.unix_millis())
                .filter(|value| *value > 0)
                .ok_or_else(|| ToolCommitError::InvalidTransition.port())?;
            let mut transaction = self
                .ledger
                .database()
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::Interaction)
                .await
                .map_err(|error| transaction_port(&error))?;
            sqlx::query(
                "INSERT INTO approval.authorization_receipts (
                     receipt_id, tenant_id, principal_id, conversation_id,
                     workspace_id, source_interaction_id, capability_key,
                     capability_provider_id, permission_mode, scope_fingerprint,
                     issued_at_ms, expires_at_ms, revoked_at_ms
                 )
                 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        interaction.responded_at_ms,
                        interaction.responded_at_ms + $11,
                        NULL
                 FROM approval.interactions AS interaction
                 JOIN approval.interaction_responses AS response
                   ON response.interaction_id = interaction.interaction_id
                 JOIN tool.tool_invocations AS invocation
                   ON invocation.invocation_id = interaction.requested_by_invocation_id
                  AND invocation.tenant_id = interaction.tenant_id
                 WHERE interaction.interaction_id = $6
                   AND interaction.tenant_id = $2
                   AND interaction.conversation_id = $4
                   AND interaction.run_id = $12
                   AND interaction.kind = 1 AND interaction.status = 2
                   AND interaction.responded_by = $3
                   AND response.response_kind = 1
                   AND response.approval_decision = 3
                   AND response.responded_by = $3
                   AND invocation.capability_provider_id = $8
                 ON CONFLICT (source_interaction_id) DO NOTHING",
            )
            .bind(receipt.receipt_id.into_uuid())
            .bind(receipt.tenant_id.into_uuid())
            .bind(receipt.principal_id.into_uuid())
            .bind(receipt.conversation_id.into_uuid())
            .bind(receipt.workspace_id.map(WorkspaceId::into_uuid))
            .bind(receipt.source_interaction_id.into_uuid())
            .bind(receipt.capability_key.as_str())
            .bind(receipt.capability_provider_id.into_uuid())
            .bind(permission_mode_code(receipt.permission_mode))
            .bind(receipt.scope_fingerprint.into_bytes().to_vec())
            .bind(lifetime_ms)
            .bind(attempt.run_id.into_uuid())
            .execute(&mut *transaction)
            .await
            .map_err(|error| ToolCommitError::Database(error).port())?;
            let row = sqlx::query(
                "SELECT receipt.receipt_id, receipt.tenant_id, receipt.principal_id,
                        receipt.conversation_id, receipt.workspace_id,
                        receipt.source_interaction_id, receipt.capability_key,
                        receipt.capability_provider_id, receipt.permission_mode,
                        receipt.scope_fingerprint, receipt.issued_at_ms,
                        receipt.expires_at_ms,
                        interaction.responded_at_ms AS source_responded_at_ms
                 FROM approval.authorization_receipts AS receipt
                 JOIN approval.interactions AS interaction
                   ON interaction.interaction_id = receipt.source_interaction_id
                 WHERE receipt.source_interaction_id = $1",
            )
            .bind(receipt.source_interaction_id.into_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| ToolCommitError::Database(error).port())?
            .ok_or_else(|| ToolCommitError::InvalidTransition.port())?;
            let persisted =
                authorization_receipt_from_pg_row(&row).map_err(ToolCommitError::port)?;
            let source_responded_at = UtcTimestamp::from_unix_millis(
                row.try_get("source_responded_at_ms")
                    .map_err(|error| ToolCommitError::Database(error).port())?,
            );
            if !authorization_receipt_matches_issuance(
                &persisted,
                &receipt,
                source_responded_at,
                lifetime_ms,
            ) {
                return Err(ToolCommitError::IdempotencyConflict.port());
            }
            transaction
                .commit()
                .await
                .map_err(|error| transaction_port(&error))?;
            Ok(persisted)
        })
    }

    fn start(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        start: ToolExecutionStart,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.start_inner(&attempt, invocation_id, mode, None, start)
                .await
                .map_err(ToolCommitError::port)
        })
    }

    fn start_with_reconciliation_identity(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        mode: ToolStartMode,
        reconciliation_ref: Option<OpaqueExternalRef>,
        start: ToolExecutionStart,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.start_inner(&attempt, invocation_id, mode, reconciliation_ref, start)
                .await
                .map_err(ToolCommitError::port)
        })
    }

    fn wait_for_reconciliation(
        &self,
        attempt: &AttemptContext,
        request: ReconciliationWaitRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.wait_for_reconciliation_inner(&attempt, request)
                .await
                .map_err(ToolCommitError::port)
        })
    }

    fn complete(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        outcome: ToolTerminalOutcome,
        finished_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            self.complete_inner(&attempt, invocation_id, outcome, finished_at)
                .await
                .map_err(ToolCommitError::port)
        })
    }
}

fn validate_start(
    existing: &DurableToolInvocation,
    attempt: &AttemptContext,
    mode: ToolStartMode,
) -> Result<(), ToolCommitError> {
    if existing.permits_start(mode, attempt.attempt_id) {
        Ok(())
    } else {
        Err(ToolCommitError::InvalidTransition)
    }
}

fn validate_start_reconciliation_identity(
    existing: &DurableToolInvocation,
    mode: ToolStartMode,
    proposed: Option<&OpaqueExternalRef>,
) -> Result<(), ToolCommitError> {
    let valid_contract = match (existing.retry_contract, mode) {
        (
            ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable,
            ToolStartMode::Initial | ToolStartMode::ReconciliationProvenNotApplied,
        ) => proposed.is_some(),
        (
            ToolRetryContract::ReadOnly | ToolRetryContract::IdempotentByKey,
            ToolStartMode::Initial | ToolStartMode::ReplaySafeRecovery,
        ) => proposed.is_none(),
        _ => false,
    };
    let agrees_with_existing = match existing.retry_contract {
        ToolRetryContract::QueryBeforeRetry | ToolRetryContract::NotReplayable => existing
            .prebound_reconciliation_ref
            .as_ref()
            .is_none_or(|bound| proposed == Some(bound)),
        ToolRetryContract::ReadOnly | ToolRetryContract::IdempotentByKey => true,
    };
    if valid_contract && agrees_with_existing {
        Ok(())
    } else {
        Err(ToolCommitError::IdempotencyConflict)
    }
}

fn validate_execution_start(
    existing: &DurableToolInvocation,
    mode: ToolStartMode,
    start: &ToolExecutionStart,
) -> Result<(), ToolCommitError> {
    let valid = start.preserves_timing(&existing.invocation.timing, mode);
    if valid {
        Ok(())
    } else {
        Err(ToolCommitError::InvalidTransition)
    }
}

fn validate_completion(
    existing: &DurableToolInvocation,
    attempt: &AttemptContext,
    outcome: &ToolTerminalOutcome,
    finished_at: UtcTimestamp,
) -> Result<(), ToolCommitError> {
    if existing.permits_read_only_recovery_timeout(attempt, outcome, finished_at) {
        return Ok(());
    }
    // Liveness validates the exact recovery proof in this same transaction.
    if existing.recorded_outcome.is_none()
        && outcome.is_activity_read_recovery_candidate(
            &existing.invocation,
            existing.retry_contract,
            attempt,
        )
    {
        return Ok(());
    }
    match (
        existing.invocation.status,
        existing.invocation.execution_attempt_id,
        existing.interaction_resolution.clone(),
        outcome,
    ) {
        (ToolInvocationStatus::Running, Some(owner), _, _) if owner == attempt.attempt_id => Ok(()),
        (
            ToolInvocationStatus::WaitingInteraction,
            _,
            Some(InteractionResolution::Approval(ApprovalDecision::Deny)),
            ToolTerminalOutcome::Failed { error, .. },
        ) if error.code == ErrorCode::PolicyDenied => Ok(()),
        (
            ToolInvocationStatus::WaitingInteraction,
            _,
            Some(InteractionResolution::Expired),
            ToolTerminalOutcome::Failed { error, .. },
        ) if error.code == ErrorCode::InteractionExpired => Ok(()),
        (
            ToolInvocationStatus::WaitingInteraction,
            _,
            Some(InteractionResolution::Cancelled),
            ToolTerminalOutcome::Cancelled(_),
        ) => Ok(()),
        (
            ToolInvocationStatus::WaitingInteraction,
            _,
            Some(InteractionResolution::RetiredResultReview),
            ToolTerminalOutcome::Cancelled(
                musteragent_runtime_contract::CancelReason::PolicyRevoked,
            ),
        ) if existing.invocation.tool_key.as_str() == "tasks.verify@1" => Ok(()),
        (ToolInvocationStatus::Requested, None, None, ToolTerminalOutcome::Failed { .. }) => {
            // A Requested invocation has not crossed the physical-dispatch
            // boundary. Runtime validation/authorization may reject it with a
            // definite Tool failure (including malformed model arguments)
            // without first transitioning it through Running.
            Ok(())
        }
        (
            ToolInvocationStatus::Running,
            Some(owner),
            _,
            ToolTerminalOutcome::Indeterminate { .. },
        ) if owner != attempt.attempt_id
            // A successor Attempt seals any write its lost predecessor left
            // Running as unknown, whatever its retry contract: the model then
            // inspects the target before repeating. Only read-only work is
            // simply retried.
            && !matches!(existing.retry_contract, ToolRetryContract::ReadOnly) =>
        {
            Ok(())
        }
        (
            ToolInvocationStatus::Indeterminate,
            _,
            _,
            ToolTerminalOutcome::Succeeded(_)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { .. }
            | ToolTerminalOutcome::Failed { .. }
            | ToolTerminalOutcome::Cancelled(_),
        ) if existing.permits_reconciled_outcome(outcome) => Ok(()),
        _ => Err(ToolCommitError::InvalidTransition),
    }
}

fn validate_completion_reconciliation_identity(
    existing: &DurableToolInvocation,
    outcome: &ToolTerminalOutcome,
) -> Result<(), ToolCommitError> {
    if let (
        Some(bound),
        ToolTerminalOutcome::Indeterminate {
            reconciliation_ref, ..
        },
    ) = (existing.prebound_reconciliation_ref.as_ref(), outcome)
        && bound != reconciliation_ref
    {
        return Err(ToolCommitError::IdempotencyConflict);
    }
    Ok(())
}

fn ensure_same_request(
    existing: &DurableToolInvocation,
    proposed: &ToolInvocation,
    retry_contract: ToolRetryContract,
) -> Result<(), ToolCommitError> {
    if existing.matches_request(proposed, retry_contract) {
        Ok(())
    } else {
        Err(ToolCommitError::IdempotencyConflict)
    }
}

fn terminal_event(invocation_id: InvocationId, outcome: &ToolTerminalOutcome) -> DomainEvent {
    let event = match outcome {
        ToolTerminalOutcome::Succeeded(result) => ToolEvent::InvocationSucceeded {
            invocation_id,
            result_ref: result.result_ref.clone(),
            result_hash: result.result_hash,
            provider_request_ref: result.provider_request_ref.clone(),
            authorized_content: Vec::new(),
        },
        ToolTerminalOutcome::SucceededWithAuthorizedContent {
            result,
            authorized_content,
        } => ToolEvent::InvocationSucceeded {
            invocation_id,
            result_ref: result.result_ref.clone(),
            result_hash: result.result_hash,
            provider_request_ref: result.provider_request_ref.clone(),
            authorized_content: authorized_content.clone(),
        },
        ToolTerminalOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        } => ToolEvent::InvocationFailed {
            invocation_id,
            error: error.clone(),
            retry_disposition: *retry_disposition,
            diagnostic_result_ref: diagnostic_result
                .as_ref()
                .map(|result| result.result_ref.clone()),
            diagnostic_result_hash: diagnostic_result.as_ref().map(|result| result.result_hash),
        },
        ToolTerminalOutcome::Indeterminate {
            reason,
            reconciliation_ref,
            observed_error,
            diagnostic_result,
        } => ToolEvent::InvocationIndeterminate {
            invocation_id,
            reason: reason.clone(),
            reconciliation_ref: reconciliation_ref.clone(),
            observed_error: observed_error.clone(),
            diagnostic_result_ref: diagnostic_result
                .as_ref()
                .map(|result| result.result_ref.clone()),
            diagnostic_result_hash: diagnostic_result.as_ref().map(|result| result.result_hash),
        },
        ToolTerminalOutcome::Cancelled(reason) => ToolEvent::InvocationCancelled {
            invocation_id,
            reason: *reason,
        },
    };
    DomainEvent::Tool(event)
}

fn tool_event_matches_state(event: &ToolEvent, durable: &DurableToolInvocation) -> bool {
    let invocation_id = durable.invocation.invocation_id;
    let status = durable.invocation.status;
    match (status, event) {
        (
            ToolInvocationStatus::Requested,
            ToolEvent::InvocationRequested {
                invocation_id: current,
                ..
            },
        )
        | (
            ToolInvocationStatus::WaitingInteraction,
            ToolEvent::InvocationWaitingInteraction {
                invocation_id: current,
                ..
            },
        ) => *current == invocation_id,
        (
            ToolInvocationStatus::Running,
            ToolEvent::InvocationStarted {
                invocation_id: current,
                execution_attempt_id,
                capability_provider_id,
                execution_timing,
            },
        ) => {
            *current == invocation_id
                && Some(*execution_attempt_id) == durable.invocation.execution_attempt_id
                && *capability_provider_id == durable.invocation.capability_provider_id
                && execution_timing.as_ref().map_or(
                    durable.invocation.timing.legacy_deadline().is_some(),
                    |timing| *timing == durable.invocation.timing,
                )
        }
        (
            ToolInvocationStatus::Succeeded,
            ToolEvent::InvocationSucceeded {
                invocation_id: current,
                ..
            },
        )
        | (
            ToolInvocationStatus::Failed,
            ToolEvent::InvocationFailed {
                invocation_id: current,
                ..
            },
        )
        | (
            ToolInvocationStatus::Indeterminate,
            ToolEvent::InvocationIndeterminate {
                invocation_id: current,
                ..
            },
        )
        | (
            ToolInvocationStatus::Cancelled,
            ToolEvent::InvocationCancelled {
                invocation_id: current,
                ..
            },
        ) => *current == invocation_id,
        _ => false,
    }
}

fn validate_terminal(outcome: &ToolTerminalOutcome) -> Result<(), ToolCommitError> {
    match outcome {
        ToolTerminalOutcome::Succeeded(result)
        | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. } => result
            .validate()
            .map_err(|_| ToolCommitError::InvalidTransition),
        ToolTerminalOutcome::Failed {
            error,
            diagnostic_result,
            ..
        } => {
            error
                .validate()
                .map_err(|_| ToolCommitError::InvalidTransition)?;
            if let Some(result) = diagnostic_result {
                result
                    .validate()
                    .map_err(|_| ToolCommitError::InvalidTransition)?;
            }
            Ok(())
        }
        ToolTerminalOutcome::Indeterminate {
            observed_error,
            diagnostic_result,
            ..
        } => {
            if let Some(error) = observed_error {
                error
                    .validate()
                    .map_err(|_| ToolCommitError::InvalidTransition)?;
            }
            if let Some(result) = diagnostic_result {
                result
                    .validate()
                    .map_err(|_| ToolCommitError::InvalidTransition)?;
            }
            Ok(())
        }
        ToolTerminalOutcome::Cancelled(_) => Ok(()),
    }
}

#[cfg(test)]
mod sandbox_route_tests;

#[cfg(test)]
#[path = "model_tests.rs"]
mod model_tests;
#[cfg(test)]
mod test_fixture;

#[cfg(test)]
mod sandbox_store_tests;
