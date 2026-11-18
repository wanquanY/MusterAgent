use musteragent_activity::ActivityKind;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, FencingToken, HomeEpoch, InteractionId, InvocationId,
    LeaseId, RunId, RunSeq, UtcTimestamp,
};
use musteragent_run::{AttemptTrigger, WorkerId};
use musteragent_runtime_contract::{ApplicationFuture, CancelReason};
use musteragent_telemetry::{AttemptTelemetryContext, RunTelemetryContext};
use thiserror::Error;

use crate::{AppendDisposition, AppendReceipt, AppendRequest, LedgerError, ProposedRecord};

const MIN_LEASE_MILLIS: i64 = 1_000;
const MAX_LEASE_MILLIS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttemptFence {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub attempt_number: AttemptNumber,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub home_epoch: HomeEpoch,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimAttemptBatch {
    pub append: AppendRequest,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub activity_kind: ActivityKind,
    pub expected_last_run_sequence: RunSeq,
    pub attempt_id: AttemptId,
    pub attempt_number: AttemptNumber,
    pub trigger: AttemptTrigger,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub lease_expires_at: UtcTimestamp,
    pub telemetry: RunTelemetryContext,
}

impl ClaimAttemptBatch {
    pub fn validate(&self) -> Result<(), ExecutionMutationError> {
        self.append
            .validate()
            .map_err(ExecutionMutationError::Ledger)?;
        if self.append.records.len() != 2 || !self.append.outbox.is_empty() {
            return Err(ExecutionMutationError::InvalidClaimShape);
        }
        validate_telemetry_identity(&self.telemetry, &self.append, self.run_id, self.activity_id)?;
        validate_run_records(
            &self.append,
            self.run_id,
            self.activity_id,
            self.attempt_id,
            self.expected_last_run_sequence,
            Producer::Scheduler,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenewLeaseRequest {
    pub fence: AttemptFence,
    pub new_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FencedAppendBatch {
    pub fence: AttemptFence,
    pub expected_last_run_sequence: RunSeq,
    pub append: AppendRequest,
}

impl FencedAppendBatch {
    pub fn validate(&self) -> Result<(), ExecutionMutationError> {
        self.append
            .validate()
            .map_err(ExecutionMutationError::Ledger)?;
        validate_run_records(
            &self.append,
            self.fence.run_id,
            self.append.records[0]
                .activity_id
                .ok_or(ExecutionMutationError::RecordIdentityMismatch)?,
            self.fence.attempt_id,
            self.expected_last_run_sequence,
            Producer::Worker,
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecoverAttemptBatch {
    pub append: AppendRequest,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub activity_kind: ActivityKind,
    pub expected_last_run_sequence: RunSeq,
    pub stale_fence: AttemptFence,
    pub new_attempt_id: AttemptId,
    pub new_attempt_number: AttemptNumber,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub lease_expires_at: UtcTimestamp,
    pub telemetry: RunTelemetryContext,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CancelMode {
    RequestActiveAttempt,
    CancelWithoutAttempt,
    /// A delegated descendant has no live Attempt. It is made dispatchable
    /// with `cancellation_requested = true` so the ordinary worker terminal
    /// path still emits its bounded child report and resolves the delegation
    /// graph instead of persistence inventing a terminal state off-ledger.
    QueueCancellation,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CascadeCancelTarget {
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub expected_last_run_sequence: RunSeq,
    pub mode: CancelMode,
    pub pending_interaction_id: Option<InteractionId>,
    pub pending_tool_invocation_id: Option<InvocationId>,
    /// Original unknown outcome: retained, never emitted as ToolCancelled.
    pub unresolved_tool_invocation_id: Option<InvocationId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CancelRunBatch {
    pub append: AppendRequest,
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub expected_last_run_sequence: RunSeq,
    pub mode: CancelMode,
    pub reason: CancelReason,
    pub pending_interaction_id: Option<InteractionId>,
    /// Known pending Tool associated with an interaction or device wait.
    /// Indeterminate outcomes use `unresolved_tool_invocation_id` instead.
    pub pending_tool_invocation_id: Option<InvocationId>,
    /// Original unknown outcome: retained, never emitted as ToolCancelled.
    pub unresolved_tool_invocation_id: Option<InvocationId>,
    /// Active delegated descendants ordered parent-before-child. Every target
    /// belongs to the same Conversation and is committed under the root
    /// command's single Ledger offset compare-and-swap.
    pub descendants: Vec<CascadeCancelTarget>,
}

impl CancelRunBatch {
    pub fn validate(&self) -> Result<(), ExecutionMutationError> {
        self.append
            .validate()
            .map_err(ExecutionMutationError::Ledger)?;
        if (self.unresolved_tool_invocation_id.is_some()
            && (self.pending_tool_invocation_id.is_some()
                || self.pending_interaction_id.is_some()
                || self.mode == CancelMode::RequestActiveAttempt))
            || self.mode == CancelMode::QueueCancellation
            || invalid_wait_shape(
                self.mode,
                self.pending_interaction_id.is_some(),
                self.pending_tool_invocation_id.is_some(),
            )
        {
            return Err(ExecutionMutationError::InvalidCancellationShape);
        }
        let expected_count = cancellation_record_count(
            self.mode,
            self.pending_interaction_id.is_some(),
            self.pending_tool_invocation_id.is_some(),
        )?;
        let descendant_count = self.descendants.iter().try_fold(
            0_usize,
            |count, target| -> Result<usize, ExecutionMutationError> {
                if (target.unresolved_tool_invocation_id.is_some()
                    && (target.pending_tool_invocation_id.is_some()
                        || target.pending_interaction_id.is_some()
                        || target.mode == CancelMode::RequestActiveAttempt))
                    || target.mode == CancelMode::CancelWithoutAttempt
                    || invalid_wait_shape(
                        target.mode,
                        target.pending_interaction_id.is_some(),
                        target.pending_tool_invocation_id.is_some(),
                    )
                    || target.run_id == self.run_id
                {
                    return Err(ExecutionMutationError::InvalidCancellationShape);
                }
                let increment = cancellation_record_count(
                    target.mode,
                    target.pending_interaction_id.is_some(),
                    target.pending_tool_invocation_id.is_some(),
                )?;
                count
                    .checked_add(increment)
                    .ok_or(ExecutionMutationError::InvalidCancellationShape)
            },
        )?;
        let expected_count = expected_count
            .checked_add(descendant_count)
            .ok_or(ExecutionMutationError::InvalidCancellationShape)?;
        if self.append.records.len() != expected_count || !self.append.outbox.is_empty() {
            return Err(ExecutionMutationError::InvalidCancellationShape);
        }
        let producer = self.append.records[0].producer;
        // A scheduler may close an unleased revoked interaction. It may not
        // impersonate a user cancellation or take over active/cascaded Runs.
        if producer != Producer::RuntimeApi
            && !(producer == Producer::Scheduler
                && self.reason == CancelReason::PolicyRevoked
                && self.mode == CancelMode::CancelWithoutAttempt
                && self.pending_interaction_id.is_some()
                && self.pending_tool_invocation_id.is_some()
                && self.descendants.is_empty())
        {
            return Err(ExecutionMutationError::RecordIdentityMismatch);
        }
        let mut cursor = 0_usize;
        cursor = validate_cancellation_records(
            &self.append.records,
            cursor,
            self.run_id,
            self.activity_id,
            self.expected_last_run_sequence,
            producer,
            cancellation_record_count(
                self.mode,
                self.pending_interaction_id.is_some(),
                self.pending_tool_invocation_id.is_some(),
            )?,
        )?;
        let mut seen_runs = std::collections::HashSet::from([self.run_id]);
        for target in &self.descendants {
            if !seen_runs.insert(target.run_id) {
                return Err(ExecutionMutationError::InvalidCancellationShape);
            }
            cursor = validate_cancellation_records(
                &self.append.records,
                cursor,
                target.run_id,
                target.activity_id,
                target.expected_last_run_sequence,
                producer,
                cancellation_record_count(
                    target.mode,
                    target.pending_interaction_id.is_some(),
                    target.pending_tool_invocation_id.is_some(),
                )?,
            )?;
        }
        if cursor != self.append.records.len() {
            return Err(ExecutionMutationError::InvalidCancellationShape);
        }
        Ok(())
    }
}

fn cancellation_record_count(
    mode: CancelMode,
    has_pending_interaction: bool,
    has_pending_tool: bool,
) -> Result<usize, ExecutionMutationError> {
    match (mode, has_pending_interaction, has_pending_tool) {
        (CancelMode::RequestActiveAttempt, false, false)
        | (CancelMode::QueueCancellation, false, false) => Ok(1),
        (CancelMode::CancelWithoutAttempt, false, false) => Ok(3),
        (CancelMode::CancelWithoutAttempt, false, true) => Ok(4),
        (CancelMode::CancelWithoutAttempt, true, true) => Ok(5),
        (CancelMode::QueueCancellation, false, true) => Ok(2),
        (CancelMode::QueueCancellation, true, true) => Ok(3),
        _ => Err(ExecutionMutationError::InvalidCancellationShape),
    }
}

const fn invalid_wait_shape(
    mode: CancelMode,
    has_pending_interaction: bool,
    has_pending_tool: bool,
) -> bool {
    (has_pending_interaction && !has_pending_tool)
        || (has_pending_tool && matches!(mode, CancelMode::RequestActiveAttempt))
}

fn validate_cancellation_records(
    records: &[ProposedRecord],
    start: usize,
    run_id: RunId,
    activity_id: ActivityId,
    last_run_sequence: RunSeq,
    producer: Producer,
    record_count: usize,
) -> Result<usize, ExecutionMutationError> {
    let end = start
        .checked_add(record_count)
        .ok_or(ExecutionMutationError::InvalidCancellationShape)?;
    let slice = records
        .get(start..end)
        .ok_or(ExecutionMutationError::InvalidCancellationShape)?;
    let expected_sequences = consecutive_run_sequences(last_run_sequence, record_count)?;
    for (record, expected) in slice.iter().zip(expected_sequences) {
        if record.record_kind != LedgerRecordKind::DomainEvent
            || record.producer != producer
            || record.run_id != Some(run_id)
            || record.activity_id != Some(activity_id)
            || record.attempt_id.is_some()
            || record.run_seq != Some(expected)
        {
            return Err(ExecutionMutationError::RecordIdentityMismatch);
        }
    }
    Ok(end)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CancelRunReceipt {
    pub ledger: AppendReceipt,
    pub terminal: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AcknowledgeCancellationBatch {
    pub append: AppendRequest,
    pub fence: AttemptFence,
    pub activity_id: ActivityId,
    pub expected_last_run_sequence: RunSeq,
}

impl AcknowledgeCancellationBatch {
    pub fn validate(&self) -> Result<(), ExecutionMutationError> {
        self.append
            .validate()
            .map_err(ExecutionMutationError::Ledger)?;
        if self.append.records.len() != 3 || !self.append.outbox.is_empty() {
            return Err(ExecutionMutationError::InvalidCancellationShape);
        }
        validate_run_records(
            &self.append,
            self.fence.run_id,
            self.activity_id,
            self.fence.attempt_id,
            self.expected_last_run_sequence,
            Producer::Worker,
        )
    }
}

impl RecoverAttemptBatch {
    pub fn validate(&self) -> Result<(), ExecutionMutationError> {
        self.append
            .validate()
            .map_err(ExecutionMutationError::Ledger)?;
        if self.append.records.len() != 3 || !self.append.outbox.is_empty() {
            return Err(ExecutionMutationError::InvalidRecoveryShape);
        }
        validate_telemetry_identity(&self.telemetry, &self.append, self.run_id, self.activity_id)?;
        if self.stale_fence.run_id != self.run_id
            || self.new_attempt_number.get()
                != self
                    .stale_fence
                    .attempt_number
                    .get()
                    .checked_add(1)
                    .ok_or(ExecutionMutationError::SequenceOverflow)?
            || self.fencing_token.get() <= self.stale_fence.fencing_token.get()
        {
            return Err(ExecutionMutationError::RecoveryIdentityMismatch);
        }
        let expected_sequences =
            consecutive_run_sequences(self.expected_last_run_sequence, self.append.records.len())?;
        for (index, (record, expected)) in self
            .append
            .records
            .iter()
            .zip(expected_sequences)
            .enumerate()
        {
            let expected_attempt = if index == 0 {
                self.stale_fence.attempt_id
            } else {
                self.new_attempt_id
            };
            if record.record_kind != LedgerRecordKind::DomainEvent
                || record.producer != Producer::Scheduler
                || record.run_id != Some(self.run_id)
                || record.activity_id != Some(self.activity_id)
                || record.attempt_id != Some(expected_attempt)
                || record.run_seq != Some(expected)
            {
                return Err(ExecutionMutationError::RecordIdentityMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionReceipt {
    pub ledger: AppendReceipt,
    pub fence: AttemptFence,
    pub activity_kind: ActivityKind,
    pub lease_expires_at: UtcTimestamp,
    pub telemetry: Box<AttemptTelemetryContext>,
}

impl ExecutionReceipt {
    #[must_use]
    pub const fn is_duplicate(&self) -> bool {
        matches!(self.ledger.disposition, AppendDisposition::Duplicate)
    }
}

fn validate_telemetry_identity(
    telemetry: &RunTelemetryContext,
    append: &AppendRequest,
    run_id: RunId,
    activity_id: ActivityId,
) -> Result<(), ExecutionMutationError> {
    if telemetry.tenant_id != append.tenant_id
        || telemetry.conversation_id != append.conversation_id
        || telemetry.run_id != run_id
        || telemetry.activity_id != activity_id
    {
        return Err(ExecutionMutationError::RecordIdentityMismatch);
    }
    Ok(())
}

pub trait ExecutionStore: Send + Sync {
    type Error: Send;

    fn claim_attempt(
        &self,
        batch: ClaimAttemptBatch,
    ) -> ApplicationFuture<'_, Result<ExecutionReceipt, Self::Error>>;

    fn renew_lease(
        &self,
        request: RenewLeaseRequest,
    ) -> ApplicationFuture<'_, Result<UtcTimestamp, Self::Error>>;

    fn append_fenced(
        &self,
        batch: FencedAppendBatch,
    ) -> ApplicationFuture<'_, Result<AppendReceipt, Self::Error>>;

    fn recover_attempt(
        &self,
        batch: RecoverAttemptBatch,
    ) -> ApplicationFuture<'_, Result<ExecutionReceipt, Self::Error>>;

    fn cancel_run(
        &self,
        batch: CancelRunBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, Self::Error>>;

    fn acknowledge_cancellation(
        &self,
        batch: AcknowledgeCancellationBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, Self::Error>>;
}

fn validate_run_records(
    append: &AppendRequest,
    run_id: RunId,
    activity_id: ActivityId,
    attempt_id: AttemptId,
    previous_run_sequence: RunSeq,
    producer: Producer,
) -> Result<(), ExecutionMutationError> {
    let expected_sequences =
        consecutive_run_sequences(previous_run_sequence, append.records.len())?;
    for (record, expected) in append.records.iter().zip(expected_sequences) {
        if record.record_kind != LedgerRecordKind::DomainEvent
            || record.producer != producer
            || record.run_id != Some(run_id)
            || record.activity_id != Some(activity_id)
            || record.attempt_id != Some(attempt_id)
            || record.run_seq != Some(expected)
        {
            return Err(ExecutionMutationError::RecordIdentityMismatch);
        }
    }
    Ok(())
}

fn consecutive_run_sequences(
    previous: RunSeq,
    count: usize,
) -> Result<Vec<RunSeq>, ExecutionMutationError> {
    (1..=count)
        .map(|increment| {
            let increment =
                u64::try_from(increment).map_err(|_| ExecutionMutationError::SequenceOverflow)?;
            let value = previous
                .get()
                .checked_add(increment)
                .ok_or(ExecutionMutationError::SequenceOverflow)?;
            RunSeq::new(value).map_err(|_| ExecutionMutationError::SequenceOverflow)
        })
        .collect()
}

#[doc(hidden)]
pub fn validate_lease_horizon(
    now: UtcTimestamp,
    expires_at: UtcTimestamp,
) -> Result<(), ExecutionMutationError> {
    let duration = expires_at
        .unix_millis()
        .checked_sub(now.unix_millis())
        .ok_or(ExecutionMutationError::InvalidLeaseHorizon)?;
    if (MIN_LEASE_MILLIS..=MAX_LEASE_MILLIS).contains(&duration) {
        Ok(())
    } else {
        Err(ExecutionMutationError::InvalidLeaseHorizon)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionMutationError {
    #[error(transparent)]
    Ledger(LedgerError),
    #[error("claim requires two records and no Outbox")]
    InvalidClaimShape,
    #[error("recovery requires three records and no Outbox")]
    InvalidRecoveryShape,
    #[error("cancellation record count or Outbox shape is invalid")]
    InvalidCancellationShape,
    #[error("worker record identity or run sequence is inconsistent")]
    RecordIdentityMismatch,
    #[error("recovery Attempt or fencing identity is not monotonic")]
    RecoveryIdentityMismatch,
    #[error("run sequence space is exhausted")]
    SequenceOverflow,
    #[error("lease expiry must be 1 second to 5 minutes after database time")]
    InvalidLeaseHorizon,
}
