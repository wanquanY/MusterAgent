use musteragent_identity::{
    ActivityId, AttemptId, DelegationId, FencingToken, InteractionId, InvocationId, RunId,
    ToolCallId,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_model::{CanonicalMessage, ContinuationRef, StopReason};
use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;
use thiserror::Error;

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum LoopStateError {
    #[error("Runtime Snapshot execution policy is invalid")]
    InvalidExecutionPolicy,
    #[error("checkpoint empty response count exceeds its pinned execution policy")]
    EmptyResponsesExceeded,
    #[error("Provider continuation reference and source Invocation must be present together")]
    InvalidContinuationAnchor,
}

/// Durable orchestration phase. It is a checkpoint, not an in-memory task state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LoopPhase {
    ReadyForModel,
    ModelInFlight {
        invocation_id: InvocationId,
    },
    ToolsPending {
        invocation_id: InvocationId,
        remaining_tool_call_ids: Vec<ToolCallId>,
    },
    Waiting {
        interaction_id: InteractionId,
    },
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl LoopPhase {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

/// Minimum durable state required to restart at an Attempt boundary.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableLoopCheckpoint {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub iteration: u32,
    /// Zero-based index into the admission-sealed primary + fallback route
    /// chain. It advances only through an atomic fallback transition.
    pub active_model_route_ordinal: u32,
    pub phase: LoopPhase,
    pub completed_tool_calls: u32,
    /// Consecutive Provider completions that contained neither Assistant text
    /// nor a Tool call. This is durable so crash recovery cannot reset the
    /// bounded protocol-recovery count.
    pub consecutive_empty_responses: u32,
    pub total_tokens: u64,
    /// Cumulative price derived from admission-pinned pricing inputs. Zero when
    /// the Run has no price attribution.
    pub total_cost_microusd: u64,
    pub continuation_ref: Option<ContinuationRef>,
    pub continuation_source_invocation_id: Option<InvocationId>,
}

impl DurableLoopCheckpoint {
    pub fn validate(&self, policy: RunExecutionPolicySnapshot) -> Result<(), LoopStateError> {
        policy
            .validate()
            .map_err(|_| LoopStateError::InvalidExecutionPolicy)?;
        if self.continuation_ref.is_some() != self.continuation_source_invocation_id.is_some() {
            return Err(LoopStateError::InvalidContinuationAnchor);
        }
        if self.consecutive_empty_responses > policy.max_consecutive_empty_responses.get() {
            return Err(LoopStateError::EmptyResponsesExceeded);
        }
        Ok(())
    }
}

/// Durable Tool result projected back into the next model context.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultCommit {
    pub call_id: ToolCallId,
    pub message: CanonicalMessage,
}

/// State returned after the Journal reconciles a crash boundary.
///
/// `ModelInFlight` requires a bounded same-invocation recovery ticket. Otherwise
/// the Journal must first make the
/// physical invocation terminal and either rewind to `ReadyForModel` or
/// reconstruct its durable `ToolsPending` calls. Tool requests come from
/// persisted invocation facts, never from process memory.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredLoopState {
    pub checkpoint: DurableLoopCheckpoint,
    /// A bounded ticket to retrieve the original physical result.
    pub pending_model_recovery: Option<RecoverableModelInvocation>,
    pub pending_tool_calls: Vec<crate::PlannedToolCall>,
    /// A Provider terminal was durably observed before the prior worker lost
    /// the finish acknowledgement. The replacement resumes after that exact
    /// physical invocation instead of dispatching it again.
    pub recovered_model_completion: Option<RecoveredModelCompletion>,
    /// Recovery atomically closed the old physical invocation and scheduled a
    /// replacement Attempt. The current worker must exit without dispatching.
    pub replacement_attempt_scheduled: bool,
}

/// Immutable original request plus a Home-issued, fenced recovery ticket.
#[derive(Debug, Clone, PartialEq)]
pub struct RecoverableModelInvocation {
    pub iteration: crate::ModelIteration,
    pub binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    pub nonce: musteragent_content_ref::Sha256,
    pub timeout: std::time::Duration,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ModelRecoveryConstraints {
    pub recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RecoveredModelCompletion {
    AssistantContinuation,
    ReasoningContinuation,
    AssistantResponse,
    ToolCalls,
    EmptyResponse,
}

/// Durable boundary at which one Attempt stopped making progress.
///
/// The value is intentionally protocol-neutral and log-safe. It lets the
/// Worker distinguish a model/tool failure that was durably committed from a
/// Journal failure that left recovery work for the lease scheduler.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AgentLoopFailureStage {
    LoadPolicy,
    PolicyGate,
    PolicyHook,
    LoadCheckpoint,
    ValidateCheckpoint,
    RecoverCheckpoint,
    ContextPlanning,
    BeginModel,
    ModelExecution,
    FallbackPolicy,
    FallbackCommit,
    FinishModel,
    ToolExecution,
    CommitToolResult,
    CompleteRun,
    CancellationCommit,
}

/// Attempt state proven by the Journal mutation that produced an exit.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DurableAttemptOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Waiting,
    Interrupted,
    ExternalOutcomeUnknown,
}

/// Run state proven by the Journal mutation that produced an exit.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DurableRunOutcome {
    Completed,
    Failed,
    Cancelled,
    Waiting,
    RetryBackoff,
    Interrupted,
    ExternalOutcomeUnknown,
}

impl DurableRunOutcome {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Interrupted
                | Self::ExternalOutcomeUnknown
        )
    }
}

/// Sealed observation receipt created only after the Journal owner confirms a
/// durable Attempt/Run transition for the active fence.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DurableExecutionReceipt {
    run_id: RunId,
    attempt_id: AttemptId,
    fencing_token: FencingToken,
    attempt_outcome: DurableAttemptOutcome,
    run_outcome: DurableRunOutcome,
}

impl DurableExecutionReceipt {
    #[must_use]
    pub(crate) const fn new(
        attempt: &AttemptContext,
        attempt_outcome: DurableAttemptOutcome,
        run_outcome: DurableRunOutcome,
    ) -> Self {
        Self {
            run_id: attempt.run_id,
            attempt_id: attempt.attempt_id,
            fencing_token: attempt.fencing_token,
            attempt_outcome,
            run_outcome,
        }
    }

    /// Constructs the receipt only after the Runtime's forced-abort Port has
    /// atomically fenced and terminalized the exact Attempt. This narrow
    /// constructor prevents callers from manufacturing arbitrary outcome
    /// combinations while allowing the process supervisor to report the
    /// Scheduler-owned forced terminal fact.
    #[must_use]
    pub const fn coordination_forced_abort(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::Interrupted,
            DurableRunOutcome::Interrupted,
        )
    }

    /// Constructs the cancellation proof only after the Worker's dedicated
    /// cancellation Port atomically terminalizes the exact Attempt and Run.
    #[must_use]
    pub const fn worker_cancelled(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::Cancelled,
            DurableRunOutcome::Cancelled,
        )
    }

    /// Constructs the success proof only after the dedicated Maintenance
    /// persistence owner atomically terminalizes its Attempt, Run, Activity,
    /// model invocation, usage and metering publication.
    #[must_use]
    pub const fn maintenance_completed(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::Succeeded,
            DurableRunOutcome::Completed,
        )
    }

    /// Constructs the terminal failure proof after the same dedicated
    /// Maintenance durability unit commits.
    #[must_use]
    pub const fn maintenance_failed(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::Failed,
            DurableRunOutcome::Failed,
        )
    }

    /// Constructs the cancellation proof after the Maintenance durability
    /// owner commits cancellation under the live Attempt fence.
    #[must_use]
    pub const fn maintenance_cancelled(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::Cancelled,
            DurableRunOutcome::Cancelled,
        )
    }

    /// A Provider may have accepted the request but Runtime has no terminal
    /// proof. The Run is durably interrupted and is never redispatched.
    #[must_use]
    pub const fn maintenance_dispatched_unknown(attempt: &AttemptContext) -> Self {
        Self::new(
            attempt,
            DurableAttemptOutcome::ExternalOutcomeUnknown,
            DurableRunOutcome::ExternalOutcomeUnknown,
        )
    }

    #[must_use]
    pub const fn run_id(self) -> RunId {
        self.run_id
    }

    #[must_use]
    pub const fn attempt_id(self) -> AttemptId {
        self.attempt_id
    }

    #[must_use]
    pub const fn fencing_token(self) -> FencingToken {
        self.fencing_token
    }

    #[must_use]
    pub const fn attempt_outcome(self) -> DurableAttemptOutcome {
        self.attempt_outcome
    }

    #[must_use]
    pub const fn run_outcome(self) -> DurableRunOutcome {
        self.run_outcome
    }
}

/// Why the supervised task returned. A durable receipt is present only when
/// the unique Journal owner confirmed the corresponding state transition.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentLoopExit {
    Completed {
        receipt: DurableExecutionReceipt,
    },
    Waiting {
        interaction_id: InteractionId,
        /// `None` means recovery observed an already-waiting checkpoint and
        /// must not emit the transition a second time.
        receipt: Option<DurableExecutionReceipt>,
    },
    DelegationWaiting {
        delegation_id: DelegationId,
        child_activity_id: ActivityId,
        child_run_id: RunId,
        receipt: DurableExecutionReceipt,
    },
    DelegationGroupWaiting {
        group_id: musteragent_identity::DelegationGroupId,
        receipt: DurableExecutionReceipt,
    },
    DelegationCompletionWaiting {
        receipt: DurableExecutionReceipt,
    },
    NeedsAction {
        reason: musteragent_run::RunWaitReason,
        receipt: DurableExecutionReceipt,
    },
    ReconciliationWaiting {
        invocation_id: InvocationId,
        receipt: DurableExecutionReceipt,
    },
    Cancelled {
        receipt: DurableExecutionReceipt,
    },
    CoordinationInterrupted {
        receipt: DurableExecutionReceipt,
    },
    CoordinationForcedAbort {
        receipt: DurableExecutionReceipt,
    },
    AlreadyTerminal,
    Failed {
        stage: AgentLoopFailureStage,
        code: musteragent_runtime_contract::ErrorCode,
        retryable: bool,
        /// `Some` means the failure transition itself could not be committed.
        /// The scheduler must recover the fenced Attempt after its lease ends.
        failure_commit_error: Option<musteragent_runtime_contract::ErrorCode>,
        /// A committed retry-backoff or terminal-failure receipt. Absent when
        /// the failure transition itself did not commit.
        receipt: Option<DurableExecutionReceipt>,
    },
    Interrupted {
        cause: crate::CancellationCause,
    },
    ExternalOutcomeUnknown {
        reconciliation_ref: musteragent_runtime_contract::OpaqueExternalRef,
        receipt: DurableExecutionReceipt,
    },
}

impl AgentLoopExit {
    #[must_use]
    pub const fn maintenance_completed(attempt: &AttemptContext) -> Self {
        Self::Completed {
            receipt: DurableExecutionReceipt::maintenance_completed(attempt),
        }
    }

    #[must_use]
    pub const fn maintenance_failed(
        attempt: &AttemptContext,
        code: musteragent_runtime_contract::ErrorCode,
    ) -> Self {
        Self::Failed {
            stage: AgentLoopFailureStage::ModelExecution,
            code,
            retryable: false,
            failure_commit_error: None,
            receipt: Some(DurableExecutionReceipt::maintenance_failed(attempt)),
        }
    }

    #[must_use]
    pub const fn maintenance_cancelled(attempt: &AttemptContext) -> Self {
        Self::Cancelled {
            receipt: DurableExecutionReceipt::maintenance_cancelled(attempt),
        }
    }

    #[must_use]
    pub fn maintenance_dispatched_unknown(attempt: &AttemptContext) -> Self {
        let reconciliation_ref = musteragent_runtime_contract::OpaqueExternalRef::new(format!(
            "runtime-operator:evolution-maintenance:{}",
            attempt.run_id
        ))
        .expect("deterministic Maintenance reconciliation ref is valid");
        Self::ExternalOutcomeUnknown {
            reconciliation_ref,
            receipt: DurableExecutionReceipt::maintenance_dispatched_unknown(attempt),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ModelIterationOutcome {
    pub tool_calls: Vec<PlannedToolCallInternal>,
    pub reasoning_replay: Option<musteragent_runtime_model::ModelReplayText>,
    pub stop_reason: StopReason,
    pub usage: Option<musteragent_runtime_model::ModelUsage>,
    pub provider_response_ref: musteragent_runtime_model::ProviderRequestRef,
    pub response_model_key: musteragent_model::ModelKey,
    pub continuation_ref: Option<ContinuationRef>,
    pub assistant_text_emitted: bool,
    /// Short assistant text of this response, for progress narration.
    pub assistant_text: Option<String>,
    pub public_output_observed: bool,
    pub replay_barrier_observed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedToolCallInternal {
    pub planning_failure: Option<crate::ToolCallPlanningFailure>,
    pub argument_diagnostic: Option<musteragent_runtime_model::ArgumentDiagnostic>,
    pub call_id: ToolCallId,
    pub name: musteragent_tool::WireName,
    pub arguments: musteragent_runtime_model::JsonValue,
    pub provider_replay: Option<musteragent_runtime_model::ProviderToolCallReplay>,
}
