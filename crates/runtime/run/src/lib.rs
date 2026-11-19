//! Run, Attempt and Invocation identity plus their closed lifecycle contracts.

#![forbid(unsafe_code)]

mod events;
mod recovery;
pub use recovery::{ResumeRun, RunRecoveryAction};
mod execution_plan;

pub use events::RunEvent;
pub use execution_plan::{
    EXECUTION_PLAN_SCHEMA, ExecutionPlan, ExecutionPlanDocument, ExecutionPlanError,
    ExecutionPlanLineage, ExecutionPlanStep, INHERITED_EXECUTION_PLAN_SCHEMA,
    LEGACY_EXECUTION_PLAN_SCHEMA, MAX_EXECUTION_PLAN_STEPS, PlanStepId, PlanStepStatus,
};

use musteragent_activity::ActivityKind;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, DelegationGroupId, DelegationId,
    FencingToken, HomeEpoch, InteractionId, InvocationId, LeaseId, LedgerRecordId, MessageId,
    ParticipantId, RunId, RuntimeSnapshotId, TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::{
    ApplicationFuture, AttachmentRef, CancelReason, CommandAck, CommandEnvelope, CommandPayload,
    CommandType, ErrorInfo, InputContent, ModelClass, RuntimeError,
};
use std::collections::BTreeSet;
use thiserror::Error;

const MAX_WORKER_ID_BYTES: usize = 128;

/// Immutable identity and routing for one Run.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct RunContext {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub activity_id: ActivityId,
    pub activity_kind: ActivityKind,
    pub run_id: RunId,
    pub executor_participant_id: ParticipantId,
    pub requested_by_participant_id: ParticipantId,
    /// Optional causal predecessor. This is general Run lineage used by
    /// Standing Goal continuation and other durable follow-up admissions; it
    /// does not by itself mean that this Run was delegated.
    pub parent_run_id: Option<RunId>,
    /// Present only for a delegated child Run. A Delegation always has a
    /// `parent_run_id`, while non-delegation lineage may omit this field.
    pub delegation_id: Option<DelegationId>,
    pub execution_scope: ExecutionScope,
    pub control_home: RuntimeHome,
    pub execution_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub runtime_snapshot_id: RuntimeSnapshotId,
}

/// Bounded runtime instance identity. It is operational, not a domain UUID.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkerId(String);

impl WorkerId {
    /// Creates a non-empty, log-safe worker identity.
    pub fn new(value: impl Into<String>) -> Result<Self, WorkerIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(WorkerIdError::Empty);
        }
        if value.len() > MAX_WORKER_ID_BYTES {
            return Err(WorkerIdError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(WorkerIdError::ControlCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the stable operational value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An invalid operational worker identity.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum WorkerIdError {
    #[error("worker id cannot be empty")]
    Empty,
    #[error("worker id exceeds 128 bytes")]
    TooLong,
    #[error("worker id cannot contain control characters")]
    ControlCharacter,
}

/// Mutable worker ownership kept separate from `RunContext`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttemptContext {
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub attempt_number: AttemptNumber,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub lease_expires_at: UtcTimestamp,
}

/// Identity and frozen execution timing for a model, tool, MCP or device invocation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvocationContext {
    pub invocation_id: InvocationId,
    pub run_context: RunContext,
    pub attempt_context: AttemptContext,
    pub parent_invocation_id: Option<InvocationId>,
    pub causation_id: LedgerRecordId,
    pub correlation_id: musteragent_identity::CorrelationId,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
}

/// Public cancel use-case payload. Free text cannot drive machine state.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct CancelRun {
    pub run_id: RunId,
    pub reason: CancelReason,
}

/// User-requested re-admission of one terminal failed Run.
///
/// The source Message remains the only user-authored turn. A successful
/// command creates a fresh Activity and Run with causal provenance, never a
/// replacement Message or Context branch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetryRun {
    pub source_run_id: RunId,
    pub source_message_id: MessageId,
    pub expected_attempt_number: AttemptNumber,
    pub participant_id: ParticipantId,
    pub requested_model_class: ModelClass,
    pub execution_scope: ExecutionScope,
}

impl CommandPayload for RetryRun {
    const COMMAND_TYPE: CommandType = CommandType::RetryRun;
}

impl CommandPayload for CancelRun {
    const COMMAND_TYPE: CommandType = CommandType::CancelRun;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RunControlKind {
    Steering,
    FollowUp,
}

/// A user-authored message targeted at an existing non-terminal Run.
///
/// Pending control messages are already durable Conversation facts but remain
/// excluded from Context until the Loop atomically records their consumption.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnqueueRunControl {
    pub run_id: RunId,
    pub message_id: MessageId,
    pub participant_id: ParticipantId,
    pub kind: RunControlKind,
    pub content: InputContent,
    pub attachment_refs: Vec<AttachmentRef>,
}

impl EnqueueRunControl {
    pub fn validate(&self) -> Result<(), RunControlError> {
        if self.attachment_refs.len() > 64 {
            return Err(RunControlError::TooManyAttachments);
        }
        Ok(())
    }
}

impl CommandPayload for EnqueueRunControl {
    const COMMAND_TYPE: CommandType = CommandType::EnqueueRunControl;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RunControlError {
    #[error("Run control message has more than 64 attachments")]
    TooManyAttachments,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RunBudgetResource {
    Steps,
    ToolCalls,
    Tokens,
    Cost,
}

/// Durable Run aggregate independent from Worker process lifetime.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Run {
    pub context: RunContext,
    pub state: RunState,
    pub next_attempt_number: AttemptNumber,
    pub not_before: Option<UtcTimestamp>,
    pub cancel_requested_at: Option<UtcTimestamp>,
    pub admitted_at: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub finished_at: Option<UtcTimestamp>,
    pub terminal_error: Option<ErrorInfo>,
}

impl Run {
    pub fn validate(&self) -> Result<(), RunAggregateError> {
        self.state.validate().map_err(RunAggregateError::State)?;
        if self.state.status().is_terminal() != self.finished_at.is_some() {
            return Err(RunAggregateError::TerminalTimestampMismatch);
        }
        if (self.state.status() == RunStatus::Failed) != self.terminal_error.is_some() {
            return Err(RunAggregateError::TerminalErrorMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RunAggregateError {
    #[error(transparent)]
    State(#[from] RunStateInvariantError),
    #[error("finished_at must be present exactly for terminal Runs")]
    TerminalTimestampMismatch,
    #[error("terminal_error must be present exactly for Failed Runs")]
    TerminalErrorMismatch,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct GetRun {
    pub actor: musteragent_identity::Actor,
    pub run_id: RunId,
}

/// Public admission inputs frozen by the source Run, never provider credentials.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunRetryConfiguration {
    pub source_product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub source_capability_keys: Vec<String>,
    pub requested_model_class: ModelClass,
    pub reasoning_effort: Option<String>,
    pub reasoning_summary: bool,
    pub permission_mode: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunView {
    /// Input Conversation Message that started or retried this Run, when one exists.
    pub input_message_id: Option<MessageId>,
    pub retry_configuration: Option<RunRetryConfiguration>,
    pub run: Run,
    pub active_attempt: Option<AttemptContext>,
    pub pending_interaction_ids: Vec<InteractionId>,
    pub latest_cursor: musteragent_event_envelope::LedgerCursor,
}

pub trait RunApplication: Send + Sync {
    fn resume_run(
        &self,
        command: CommandEnvelope<ResumeRun>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;
    fn cancel_run(
        &self,
        command: CommandEnvelope<CancelRun>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn enqueue_run_control(
        &self,
        command: CommandEnvelope<EnqueueRunControl>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn get_run(&self, query: GetRun) -> ApplicationFuture<'_, Result<RunView, RuntimeError>>;
}

/// Canonical Run status shared by every chat and orchestration path.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RunStatus {
    Queued,
    Running,
    Waiting,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl RunStatus {
    /// Every status, for exhaustive contract tests and adapters.
    pub const ALL: [Self; 8] = [
        Self::Queued,
        Self::Running,
        Self::Waiting,
        Self::Cancelling,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::Interrupted,
    ];

    /// Whether this status is immutable and terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

/// Why a Run is waiting without multiplying the status vocabulary.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RunWaitReason {
    Approval {
        interaction_id: InteractionId,
    },
    Input {
        interaction_id: InteractionId,
    },
    Device {
        invocation_id: InvocationId,
    },
    /// A provider-owned asynchronous operation was accepted or remains
    /// indeterminate. The scheduler wakes a new fenced Attempt only to
    /// reconcile this exact Invocation; it is not an ordinary failure retry.
    ExternalOperation {
        invocation_id: InvocationId,
    },
    /// A deterministic failure needs an explicit repair before execution.
    RecoveryRequired {
        code: musteragent_runtime_contract::ErrorCode,
    },
    RetryBackoff,
    Budget,
    /// The parent released its active Attempt while an explicitly linked
    /// delegated child Activity is producing a bounded ChildReport.
    Delegation {
        child_activity_id: ActivityId,
    },
    /// The parent released its Attempt at a durable all-of barrier over an
    /// ordered set of previously spawned delegated children.
    DelegationGroup {
        group_id: DelegationGroupId,
    },
    /// The parent has completed all immediately available work and released
    /// its Attempt while one or more asynchronously spawned children still
    /// owe a durable completion delivery.
    DelegationCompletions,
    /// The authoritative execution owner fenced a lost Worker. Continuing
    /// requires explicit reconciliation and a new assignment, never a timer.
    ExecutionRecovery,
}

/// A semantic transition. Callers cannot provide an arbitrary target status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RunTransition {
    Claim { attempt_id: AttemptId },
    Suspend { reason: RunWaitReason },
    MakeReady,
    RequestCancellation,
    Complete,
    Fail,
    AcknowledgeCancellation,
    Interrupt,
}

impl RunTransition {
    /// Every transition shape with deterministic fixture identities.
    #[must_use]
    pub fn all_for_contract_tests() -> [Self; 8] {
        [
            Self::Claim {
                attempt_id: AttemptId::generate(),
            },
            Self::Suspend {
                reason: RunWaitReason::RetryBackoff,
            },
            Self::MakeReady,
            Self::RequestCancellation,
            Self::Complete,
            Self::Fail,
            Self::AcknowledgeCancellation,
            Self::Interrupt,
        ]
    }
}

/// Current state, wait condition and active ownership identity.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct RunState {
    status: RunStatus,
    wait_reason: Option<RunWaitReason>,
    active_attempt_id: Option<AttemptId>,
}

impl RunState {
    /// Initial state after a committed `run.admitted` event.
    #[must_use]
    pub const fn queued() -> Self {
        Self {
            status: RunStatus::Queued,
            wait_reason: None,
            active_attempt_id: None,
        }
    }

    /// Rehydrates a persisted state while enforcing all conditional fields.
    pub fn rehydrate(
        status: RunStatus,
        wait_reason: Option<RunWaitReason>,
        active_attempt_id: Option<AttemptId>,
    ) -> Result<Self, RunStateInvariantError> {
        validate_state_parts(status, wait_reason, active_attempt_id)?;
        Ok(Self {
            status,
            wait_reason,
            active_attempt_id,
        })
    }

    /// Current canonical status.
    #[must_use]
    pub const fn status(self) -> RunStatus {
        self.status
    }

    /// Current wait reason, present only for `Waiting`.
    #[must_use]
    pub const fn wait_reason(self) -> Option<RunWaitReason> {
        self.wait_reason
    }

    /// Active Attempt, present only for `Running` and `Cancelling`.
    #[must_use]
    pub const fn active_attempt_id(self) -> Option<AttemptId> {
        self.active_attempt_id
    }

    /// Re-validates the in-memory representation.
    pub fn validate(self) -> Result<(), RunStateInvariantError> {
        validate_state_parts(self.status, self.wait_reason, self.active_attempt_id)
    }

    /// Applies one typed transition atomically.
    pub fn transition(&mut self, transition: RunTransition) -> Result<(), RunTransitionError> {
        let next = match (self.status, transition) {
            (RunStatus::Queued, RunTransition::Claim { attempt_id }) => Self {
                status: RunStatus::Running,
                wait_reason: None,
                active_attempt_id: Some(attempt_id),
            },
            (RunStatus::Running, RunTransition::Suspend { reason }) => Self {
                status: RunStatus::Waiting,
                wait_reason: Some(reason),
                active_attempt_id: None,
            },
            (RunStatus::Waiting, RunTransition::MakeReady) => Self::queued(),
            (RunStatus::Queued | RunStatus::Waiting, RunTransition::RequestCancellation) => Self {
                status: RunStatus::Cancelled,
                wait_reason: None,
                active_attempt_id: None,
            },
            (RunStatus::Running, RunTransition::RequestCancellation) => Self {
                status: RunStatus::Cancelling,
                wait_reason: None,
                active_attempt_id: self.active_attempt_id,
            },
            (RunStatus::Running, RunTransition::Complete) => Self {
                status: RunStatus::Completed,
                wait_reason: None,
                active_attempt_id: None,
            },
            (
                RunStatus::Queued | RunStatus::Running | RunStatus::Waiting | RunStatus::Cancelling,
                RunTransition::Fail,
            ) => Self {
                status: RunStatus::Failed,
                wait_reason: None,
                active_attempt_id: None,
            },
            (RunStatus::Cancelling, RunTransition::AcknowledgeCancellation) => Self {
                status: RunStatus::Cancelled,
                wait_reason: None,
                active_attempt_id: None,
            },
            (
                RunStatus::Running | RunStatus::Waiting | RunStatus::Cancelling,
                RunTransition::Interrupt,
            ) => Self {
                status: RunStatus::Interrupted,
                wait_reason: None,
                active_attempt_id: None,
            },
            _ => {
                return Err(RunTransitionError::InvalidTransition {
                    current: self.status,
                    transition,
                });
            }
        };
        next.validate()
            .map_err(RunTransitionError::InvariantViolation)?;
        *self = next;
        Ok(())
    }
}

/// Persisted Run fields violated their conditional contract.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RunStateInvariantError {
    #[error("waiting status requires a wait reason")]
    MissingWaitReason,
    #[error("wait reason is only valid for waiting status")]
    UnexpectedWaitReason,
    #[error("running and cancelling status require an active attempt")]
    MissingActiveAttempt,
    #[error("active attempt is only valid for running and cancelling status")]
    UnexpectedActiveAttempt,
}

/// A transition violated the closed Run contract.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RunTransitionError {
    #[error("invalid run transition from {current:?}: {transition:?}")]
    InvalidTransition {
        current: RunStatus,
        transition: RunTransition,
    },
    #[error("run transition produced an invalid state: {0}")]
    InvariantViolation(RunStateInvariantError),
}

fn validate_state_parts(
    status: RunStatus,
    wait_reason: Option<RunWaitReason>,
    active_attempt_id: Option<AttemptId>,
) -> Result<(), RunStateInvariantError> {
    match (status, wait_reason) {
        (RunStatus::Waiting, None) => return Err(RunStateInvariantError::MissingWaitReason),
        (RunStatus::Waiting, Some(_)) | (_, None) => {}
        (_, Some(_)) => return Err(RunStateInvariantError::UnexpectedWaitReason),
    }
    match (status, active_attempt_id) {
        (RunStatus::Running | RunStatus::Cancelling, None) => {
            Err(RunStateInvariantError::MissingActiveAttempt)
        }
        (RunStatus::Running | RunStatus::Cancelling, Some(_)) | (_, None) => Ok(()),
        (_, Some(_)) => Err(RunStateInvariantError::UnexpectedActiveAttempt),
    }
}

/// Lifecycle of one Worker ownership attempt.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttemptStatus {
    Running,
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    Abandoned,
}

/// Why a new Attempt exists without creating a new Run.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttemptTrigger {
    Initial,
    Retry,
    Recovery,
    InteractionResumed,
    BudgetAmended,
    /// A delegated child reached a terminal state and either committed its
    /// synchronous Tool result or made an asynchronous completion available.
    DelegationCompleted,
    /// The same provider-owned operation is due for another bounded status
    /// query. This trigger never authorizes a second physical submit.
    ReconciliationDue,
}

/// A valid Worker-owned end to an active Attempt.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttemptOutcome {
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
}

impl AttemptOutcome {
    const fn status(self) -> AttemptStatus {
        match self {
            Self::Suspended => AttemptStatus::Suspended,
            Self::Succeeded => AttemptStatus::Succeeded,
            Self::Failed => AttemptStatus::Failed,
            Self::Cancelled => AttemptStatus::Cancelled,
        }
    }
}

/// Candidate ownership supplied by the lease adapter.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttemptClaim {
    pub attempt_id: AttemptId,
    pub trigger: AttemptTrigger,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub lease_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ActiveAttempt {
    context: AttemptContext,
    trigger: AttemptTrigger,
}

/// Pure aggregate enforcing Attempt numbering, lease ownership and fencing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttemptTracker {
    run_id: RunId,
    home_epoch: HomeEpoch,
    last_attempt_number: Option<AttemptNumber>,
    highest_fencing_token: Option<FencingToken>,
    claimed_attempt_ids: BTreeSet<AttemptId>,
    active: Option<ActiveAttempt>,
    last_finished_status: Option<AttemptStatus>,
}

impl AttemptTracker {
    #[must_use]
    pub fn new(run_id: RunId, home_epoch: HomeEpoch) -> Self {
        Self {
            run_id,
            home_epoch,
            last_attempt_number: None,
            highest_fencing_token: None,
            claimed_attempt_ids: BTreeSet::new(),
            active: None,
            last_finished_status: None,
        }
    }

    /// Claims a fresh Attempt with a strictly newer fencing token.
    pub fn claim(
        &mut self,
        claim: AttemptClaim,
        now: UtcTimestamp,
    ) -> Result<AttemptContext, AttemptLeaseError> {
        if self.active.is_some() {
            return Err(AttemptLeaseError::ActiveAttemptExists);
        }
        if self.claimed_attempt_ids.contains(&claim.attempt_id) {
            return Err(AttemptLeaseError::DuplicateAttempt);
        }
        if claim.lease_expires_at <= now {
            return Err(AttemptLeaseError::LeaseAlreadyExpired);
        }
        if self
            .highest_fencing_token
            .is_some_and(|current| claim.fencing_token <= current)
        {
            return Err(AttemptLeaseError::FencingTokenNotMonotonic);
        }
        let attempt_number = self.next_attempt_number()?;
        let context = AttemptContext {
            attempt_id: claim.attempt_id,
            run_id: self.run_id,
            attempt_number,
            worker_id: claim.worker_id,
            lease_id: claim.lease_id,
            fencing_token: claim.fencing_token,
            lease_expires_at: claim.lease_expires_at,
        };
        self.claimed_attempt_ids.insert(context.attempt_id);
        self.last_attempt_number = Some(attempt_number);
        self.highest_fencing_token = Some(context.fencing_token);
        self.active = Some(ActiveAttempt {
            context: context.clone(),
            trigger: claim.trigger,
        });
        Ok(context)
    }

    /// Validates every ownership field required before a Worker commit.
    pub fn validate_commit(
        &self,
        run_context: &RunContext,
        presented: &AttemptContext,
        now: UtcTimestamp,
    ) -> Result<(), AttemptLeaseError> {
        if run_context.run_id != self.run_id || presented.run_id != self.run_id {
            return Err(AttemptLeaseError::RunMismatch);
        }
        if run_context.home_epoch != self.home_epoch {
            return Err(AttemptLeaseError::HomeEpochMismatch);
        }
        let active = self
            .active
            .as_ref()
            .ok_or(AttemptLeaseError::NoActiveAttempt)?;
        if presented.attempt_id != active.context.attempt_id {
            return Err(AttemptLeaseError::AttemptMismatch);
        }
        if presented.attempt_number != active.context.attempt_number {
            return Err(AttemptLeaseError::AttemptNumberMismatch);
        }
        if presented.worker_id != active.context.worker_id {
            return Err(AttemptLeaseError::WorkerMismatch);
        }
        if presented.lease_id != active.context.lease_id {
            return Err(AttemptLeaseError::LeaseMismatch);
        }
        if presented.fencing_token != active.context.fencing_token {
            return Err(AttemptLeaseError::FencingTokenMismatch);
        }
        if active.context.lease_expires_at <= now {
            return Err(AttemptLeaseError::LeaseExpired);
        }
        Ok(())
    }

    /// Extends the current lease without changing its ownership or fencing.
    pub fn renew(
        &mut self,
        run_context: &RunContext,
        presented: &AttemptContext,
        new_expires_at: UtcTimestamp,
        now: UtcTimestamp,
    ) -> Result<AttemptContext, AttemptLeaseError> {
        self.validate_commit(run_context, presented, now)?;
        let active = self
            .active
            .as_mut()
            .ok_or(AttemptLeaseError::NoActiveAttempt)?;
        if new_expires_at <= active.context.lease_expires_at || new_expires_at <= now {
            return Err(AttemptLeaseError::LeaseNotExtended);
        }
        active.context.lease_expires_at = new_expires_at;
        Ok(active.context.clone())
    }

    /// Ends a valid active Attempt. A later retry or recovery receives a new number.
    pub fn finish_active(
        &mut self,
        run_context: &RunContext,
        presented: &AttemptContext,
        outcome: AttemptOutcome,
        now: UtcTimestamp,
    ) -> Result<AttemptContext, AttemptLeaseError> {
        self.validate_commit(run_context, presented, now)?;
        let active = self
            .active
            .take()
            .ok_or(AttemptLeaseError::NoActiveAttempt)?;
        self.last_finished_status = Some(outcome.status());
        Ok(active.context)
    }

    /// Abandons an expired ownership without accepting a stale Worker commit.
    pub fn abandon_expired(
        &mut self,
        now: UtcTimestamp,
    ) -> Result<AttemptContext, AttemptLeaseError> {
        let active = self
            .active
            .as_ref()
            .ok_or(AttemptLeaseError::NoActiveAttempt)?;
        if active.context.lease_expires_at > now {
            return Err(AttemptLeaseError::LeaseStillValid);
        }
        let abandoned = self
            .active
            .take()
            .ok_or(AttemptLeaseError::NoActiveAttempt)?;
        self.last_finished_status = Some(AttemptStatus::Abandoned);
        Ok(abandoned.context)
    }

    pub fn next_attempt_number(&self) -> Result<AttemptNumber, AttemptLeaseError> {
        let next = match self.last_attempt_number {
            Some(current) => current
                .get()
                .checked_add(1)
                .ok_or(AttemptLeaseError::AttemptNumberExhausted)?,
            None => 1,
        };
        AttemptNumber::new(next).map_err(|_| AttemptLeaseError::AttemptNumberExhausted)
    }

    #[must_use]
    pub fn active_context(&self) -> Option<&AttemptContext> {
        self.active.as_ref().map(|attempt| &attempt.context)
    }

    #[must_use]
    pub fn active_trigger(&self) -> Option<AttemptTrigger> {
        self.active.as_ref().map(|attempt| attempt.trigger)
    }

    #[must_use]
    pub const fn last_attempt_number(&self) -> Option<AttemptNumber> {
        self.last_attempt_number
    }

    #[must_use]
    pub const fn highest_fencing_token(&self) -> Option<FencingToken> {
        self.highest_fencing_token
    }

    #[must_use]
    pub const fn last_finished_status(&self) -> Option<AttemptStatus> {
        self.last_finished_status
    }
}

/// Attempt ownership or fencing rejected a mutation.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum AttemptLeaseError {
    #[error("another Attempt is already active")]
    ActiveAttemptExists,
    #[error("Attempt identity was already claimed")]
    DuplicateAttempt,
    #[error("Attempt number space is exhausted")]
    AttemptNumberExhausted,
    #[error("fencing token must be strictly greater than every prior token")]
    FencingTokenNotMonotonic,
    #[error("new Attempt lease is already expired")]
    LeaseAlreadyExpired,
    #[error("Run identity does not match the active Attempt")]
    RunMismatch,
    #[error("Conversation home epoch is stale")]
    HomeEpochMismatch,
    #[error("there is no active Attempt")]
    NoActiveAttempt,
    #[error("Attempt identity is stale")]
    AttemptMismatch,
    #[error("Attempt number is stale")]
    AttemptNumberMismatch,
    #[error("Worker identity is stale")]
    WorkerMismatch,
    #[error("lease identity is stale")]
    LeaseMismatch,
    #[error("fencing token is stale")]
    FencingTokenMismatch,
    #[error("Attempt lease has expired")]
    LeaseExpired,
    #[error("renewal must strictly extend a valid lease")]
    LeaseNotExtended,
    #[error("Attempt lease is still valid")]
    LeaseStillValid,
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{AttemptId, FencingToken, HomeEpoch, LeaseId, RunId, UtcTimestamp};

    use super::{
        AttemptClaim, AttemptLeaseError, AttemptOutcome, AttemptStatus, AttemptTracker,
        AttemptTrigger, RunState, RunStatus, RunTransition, RunWaitReason, WorkerId,
    };

    #[test]
    fn typed_run_transitions_preserve_conditional_fields_and_terminal_immutability() {
        let attempt_id = AttemptId::generate();
        let mut state = RunState::queued();
        state
            .transition(RunTransition::Claim { attempt_id })
            .expect("queued Run is claimable");
        state
            .transition(RunTransition::Suspend {
                reason: RunWaitReason::RetryBackoff,
            })
            .expect("running Run is suspendable");
        state
            .transition(RunTransition::MakeReady)
            .expect("waiting Run becomes ready");
        state
            .transition(RunTransition::Claim {
                attempt_id: AttemptId::generate(),
            })
            .expect("ready Run is claimable again");
        state
            .transition(RunTransition::Complete)
            .expect("running Run can complete");

        assert_eq!(state.status(), RunStatus::Completed);
        assert!(state.active_attempt_id().is_none());
        assert!(
            state
                .transition(RunTransition::Claim {
                    attempt_id: AttemptId::generate(),
                })
                .is_err()
        );
    }

    #[test]
    fn worker_identity_is_bounded_and_log_safe() {
        assert!(WorkerId::new("worker-1").is_ok());
        assert!(WorkerId::new("").is_err());
        assert!(WorkerId::new("worker\n2").is_err());
    }

    #[test]
    fn attempt_recovery_strictly_advances_number_and_fencing() {
        let now = UtcTimestamp::from_unix_millis(100_000);
        let run_id = RunId::generate();
        let mut tracker =
            AttemptTracker::new(run_id, HomeEpoch::new(1).expect("positive home epoch"));
        let first = tracker
            .claim(
                AttemptClaim {
                    attempt_id: AttemptId::generate(),
                    trigger: AttemptTrigger::Initial,
                    worker_id: WorkerId::new("worker-1").expect("valid worker"),
                    lease_id: LeaseId::generate(),
                    fencing_token: FencingToken::new(1).expect("positive token"),
                    lease_expires_at: now.checked_add_millis(10_000).expect("valid expiry"),
                },
                now,
            )
            .expect("first claim");
        let recovery_time = now.checked_add_millis(10_000).expect("valid recovery time");
        tracker
            .abandon_expired(recovery_time)
            .expect("expired Attempt is abandoned");
        let second = tracker
            .claim(
                AttemptClaim {
                    attempt_id: AttemptId::generate(),
                    trigger: AttemptTrigger::Recovery,
                    worker_id: WorkerId::new("worker-2").expect("valid worker"),
                    lease_id: LeaseId::generate(),
                    fencing_token: FencingToken::new(2).expect("positive token"),
                    lease_expires_at: now.checked_add_millis(20_000).expect("valid expiry"),
                },
                recovery_time,
            )
            .expect("recovery claim");

        assert_eq!(first.attempt_number.get(), 1);
        assert_eq!(second.attempt_number.get(), 2);
        assert_eq!(
            tracker.last_finished_status(),
            Some(AttemptStatus::Abandoned)
        );
        assert_eq!(
            tracker.finish_active(
                &run_context(run_id),
                &first,
                AttemptOutcome::Succeeded,
                recovery_time,
            ),
            Err(AttemptLeaseError::AttemptMismatch)
        );
    }

    fn run_context(run_id: RunId) -> super::RunContext {
        let tenant_id = musteragent_identity::TenantId::generate();
        super::RunContext {
            tenant_id,
            conversation_id: musteragent_identity::ConversationId::generate(),
            activity_id: musteragent_identity::ActivityId::generate(),
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id,
            executor_participant_id: musteragent_identity::ParticipantId::generate(),
            requested_by_participant_id: musteragent_identity::ParticipantId::generate(),
            parent_run_id: None,
            delegation_id: None,
            execution_scope: musteragent_event_envelope::ExecutionScope::Tenant { tenant_id },
            control_home: musteragent_event_envelope::RuntimeHome::Cloud,
            execution_home: musteragent_event_envelope::RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).expect("positive home epoch"),
            runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId::generate(),
        }
    }
}
