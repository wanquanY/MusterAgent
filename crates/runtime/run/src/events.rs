use std::num::NonZeroU64;

use musteragent_content_ref::ContentRef;
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    AttemptId, AttemptNumber, CommandId, DelegationId, FencingToken, InteractionId, InvocationId,
    LeaseId, MessageId, PrincipalId, RunId, RuntimeSnapshotId, UtcTimestamp,
};
use musteragent_runtime_contract::{
    CancelReason, ErrorInfo, InterruptionReason, OpaqueExternalRef, ResourceRef, ResumeReason,
    RetryDisposition,
};

use crate::{AttemptTrigger, RunBudgetResource, RunControlKind, RunWaitReason};

/// Run- and Attempt-owned facts. Previous status is deliberately not repeated.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RunEvent {
    /// Internal supervision phase/ownership fact. It does not change Run status,
    /// produce model context, or create visible progress/unread attention.
    ExecutionLivenessRecorded {
        record: musteragent_execution_liveness::ExecutionRecord,
    },
    Admitted {
        runtime_snapshot_id: RuntimeSnapshotId,
        execution_home: RuntimeHome,
        parent_run_id: Option<RunId>,
        delegation_id: Option<DelegationId>,
        retry_source_message_id: Option<MessageId>,
        retry_source_attempt_number: Option<AttemptNumber>,
    },
    Started {
        attempt_id: AttemptId,
        attempt_number: AttemptNumber,
    },
    Waiting {
        reason: RunWaitReason,
        resume_not_before: Option<UtcTimestamp>,
    },
    BudgetExceeded {
        resource: RunBudgetResource,
    },
    BudgetAmended {
        amendment_id: CommandId,
        additional_steps: u32,
        additional_tool_calls: u32,
        additional_tokens: u64,
        additional_cost_microusd: Option<u64>,
    },
    ControlQueued {
        kind: RunControlKind,
        message_id: MessageId,
    },
    ControlConsumed {
        kind: RunControlKind,
        message_ids: Vec<MessageId>,
    },
    /// Durable user intent; the Scheduler emits Resumed only after claiming
    /// the replacement Attempt. No Attempt or completion is fabricated here.
    RecoveryRequested {
        expected_attempt_number: AttemptNumber,
        action: crate::RunRecoveryAction,
        requested_by: PrincipalId,
    },
    Resumed {
        attempt_id: AttemptId,
        attempt_number: AttemptNumber,
        reason: ResumeReason,
    },
    CancelRequested {
        reason: CancelReason,
        requested_by: PrincipalId,
    },
    RetryScheduled {
        failed_attempt_id: AttemptId,
        next_attempt_number: AttemptNumber,
        not_before: UtcTimestamp,
        error: ErrorInfo,
    },
    Completed {
        assistant_message_id: Option<MessageId>,
        result_refs: Vec<ResourceRef>,
    },
    Failed {
        error: ErrorInfo,
        last_attempt_id: Option<AttemptId>,
    },
    Cancelled {
        reason: CancelReason,
        last_attempt_id: Option<AttemptId>,
    },
    Interrupted {
        reason: InterruptionReason,
        last_attempt_id: Option<AttemptId>,
        reconciliation_ref: Option<OpaqueExternalRef>,
    },
    ExecutionPlanUpdated {
        revision: NonZeroU64,
        plan_ref: ContentRef,
        source_invocation_id: InvocationId,
    },
    AttemptClaimed {
        attempt_number: AttemptNumber,
        trigger: AttemptTrigger,
        lease_id: LeaseId,
        fencing_token: FencingToken,
        lease_expires_at: UtcTimestamp,
    },
    AttemptSucceeded {
        attempt_number: AttemptNumber,
    },
    AttemptFailed {
        attempt_number: AttemptNumber,
        error: ErrorInfo,
        retry_disposition: RetryDisposition,
    },
    AttemptAbandoned {
        attempt_number: AttemptNumber,
        reason: InterruptionReason,
    },
    AttemptSuspended {
        attempt_number: AttemptNumber,
        interaction_id: Option<InteractionId>,
        invocation_id: Option<InvocationId>,
    },
}
