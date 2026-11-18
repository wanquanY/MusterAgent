//! Goal admission protocol. The host prepares current authority, then atomically
//! commits a granted Run together with its pursuit fence and append-only ledger.
use musteragent_content_ref::ContentRef;
use musteragent_event_envelope::ExecutionScope;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::{
    ActivityId, Actor, CommandId, ConversationId, GoalId, GoalPursuitId, RunId, UtcTimestamp,
};
use musteragent_ledger::AppendDisposition;
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError, TraceContext};

use crate::OwnerGoalScope;
pub use musteragent_ledger::StandingGoalAdmissionFence;

/// Receipt of an atomic Run and pursuit admission, including duplicate replay.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandingGoalAdmissionReceipt {
    pub command_id: CommandId,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub disposition: AppendDisposition,
}

/// Admission either committed or lost its claim fence to a concurrent owner.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[must_use = "a superseded admission may still own a claim that needs to be returned"]
pub enum StandingGoalAdmissionOutcome {
    Admitted(StandingGoalAdmissionReceipt),
    Superseded,
}

/// Required admission boundary for terminal-run continuations and independent judges.
/// Implementations recheck owner membership, terminal source, current authority
/// and claim fences. They must never dispatch an ordinary message as fallback.
pub trait StandingGoalAdmission: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn prepare_standing_goal_continuation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        objective_ref: ContentRef,
        previous_evaluation_summary_ref: Option<ContentRef>,
        input_refs: Vec<ContentRef>,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>>;

    #[allow(clippy::too_many_arguments)]
    fn prepare_standing_goal_judge(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        source_run_id: RunId,
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
        fence: StandingGoalAdmissionFence,
        judge_input_ref: ContentRef,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> ApplicationFuture<'_, Result<Box<dyn StandingGoalPreparedAdmission>, RuntimeError>>;
}

/// A host-owned preparation retains the exact authorization and context read by
/// the admission owner. Consuming it commits with the newly issued signed grant;
/// implementations must recheck the fence in the same transaction as Run creation.
/// No unverified field bag or mutable application configuration crosses this port.
pub trait StandingGoalPreparedAdmission: Send + Sync {
    fn target_command_id(&self) -> CommandId;
    fn target_run_id(&self) -> RunId;
    fn execution_scope(&self) -> ExecutionScope;
    fn already_admitted(&self) -> bool;
    /// Valid only when `already_admitted` is true; returns the durable receipt.
    fn replay_receipt(&self) -> StandingGoalAdmissionReceipt;
    fn admit(
        self: Box<Self>,
        signed_grant: SignedExecutionGrant,
        continuation_authority: Option<Vec<u8>>,
    ) -> ApplicationFuture<'static, Result<StandingGoalAdmissionOutcome, RuntimeError>>;
}

/// An original Goal supervisor claim. It names authority to re-read; it is not
/// permission to execute and carries no caller-selected model or application pins.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerGoalContinuationRequest {
    pub scope: OwnerGoalScope,
    pub pursuit_id: GoalPursuitId,
    pub fence: StandingGoalAdmissionFence,
    pub phase: OwnerGoalAdmissionPhase,
}

/// Evaluation is an independently metered, capability-free Run over the
/// original frozen evidence. It never consumes another input batch.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum OwnerGoalAdmissionPhase {
    #[default]
    Execution,
    Evaluation {
        source_run_id: RunId,
        judge_input_ref: ContentRef,
    },
}

/// Source-specific admission on the existing supervisor and Run/Ledger path.
/// It resolves fresh host authority, obtains a signed execution grant and
/// repeats the Goal/source fences in the same original admission transaction.
pub trait OwnerGoalContinuationAdmission: Send + Sync {
    fn admit(
        &self,
        request: OwnerGoalContinuationRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<crate::StandingGoalAdmissionOutcome, musteragent_runtime_contract::RuntimeError>,
    >;
}
