//! Durable, supervised Musteragent Agent Loop shared by Cloud and Device runtimes.
//!
//! The loop owns orchestration, but never owns durable truth. Every boundary
//! transition is acknowledged by [`AgentLoopJournal`] before execution
//! continues, so a Tokio task may disappear without losing the Run.

#![forbid(unsafe_code)]

mod compaction_command;
mod context_recovery;
mod engine;
mod engine_support;
mod execution_context;
pub use compaction_command::AgentCompactionCommand;
pub use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionReactor, ExecutionSupervision, ExecutionSupervisionError,
    ExecutionSupervisionFailure, ExecutionSupervisionOutcome, close_unobserved_execution,
};
mod journal_command;
pub use execution_context::{AgentContextCommand, AgentContextReply};
mod model_consumption;
mod output_coalescer;
mod policy;
mod port;
mod recovery_budget;
mod retry_schedule;
mod state;
mod supervisor;
mod tool_recovery;

pub use context_recovery::{
    CONTEXT_COMPACTION_POLICY_V7, ContextCapacitySignal, admits_explicit_overflow_recovery,
    recoverable_overflow_failure,
};
pub use engine::{
    AgentLoopClock, DurableAgentLoop, InvocationCompletionObserver, InvocationSpanFactory,
};
pub use journal_command::{AgentLoopJournalCommand, AgentLoopJournalReply};
pub use musteragent_context::{
    ContextPlanError, ContextPlanInput, ContextPlanMode, ContextPlanner, ModelIteration,
    RunProgressCoordinator,
};
pub use policy::{
    DefaultLoopPolicy, ExecutionPolicySource, FallbackDecision, GateVerdict, LoopControlQueue,
    LoopPolicy, ModelFailure, NextTurnDecision, PinnedRunExecutionPolicy, StepContext,
    ToolExecutionPolicyDecision, ToolGate, TurnSummary,
};
pub use port::{
    AgentLoopJournal, DurableToolCompletion, LoopPortError, ModelActionRejection,
    ModelActionRejectionCommit, ModelCompletionCommit, ModelCompletionKind, ModelEventBatch,
    ModelFailureCommit, ModelFailureResolution, ModelInvocationObservation, PlannedToolCall,
    RunCompletionDisposition, RunCompletionReadiness, ToolArgumentValidation,
    ToolCallPlanningFailure, ToolDocumentPreview, ToolDocumentPreviewOperation,
    ToolExecutionOutcome, ToolExecutor,
};
pub use recovery_budget::{
    NoProgressRecovery, ProviderRecoveryHistory, ProviderRoundHistory, RecoveryBudget,
    RepairRecovery, runtime_failure_message,
};
pub use retry_schedule::{replacement_attempt_delay_ms, replacement_attempt_not_before};
pub use state::{
    AgentLoopExit, AgentLoopFailureStage, DurableAttemptOutcome, DurableExecutionReceipt,
    DurableLoopCheckpoint, DurableRunOutcome, LoopPhase, LoopStateError, ModelRecoveryConstraints,
    RecoverableModelInvocation, RecoveredLoopState, RecoveredModelCompletion, ToolResultCommit,
};
pub use supervisor::{
    CancellationCause, InvocationAborted, InvocationSupervisor, RuntimeTaskSupervisor,
    RuntimeTaskSupervisorError,
};
pub use tool_recovery::{
    REPEATED_TOOL_FAILURE_LIMIT, ToolRecoveryHistory, ToolRecoveryOutcome, ToolRecoveryStop,
    ToolRecoveryStopReason,
};

mod evolution_storage;
pub use evolution_storage::*;
