//! Application service for durable standing Goals.
//!
//! This is the sole command path used by user Goal mode, Agent goal tools and
//! background continuation workers. Product adapters cannot mutate Goal or
//! Pursuit status directly.

use std::sync::Arc;

mod admission;
mod blocking;
mod result_review;
pub use result_review::{StandingGoalReviewRetirement, StandingGoalReviewRetirementPage};
mod change;
pub use change::{PreparedStandingGoalChange, StandingGoalChangeMutation};
mod health;
mod input;
mod rules;
mod status;
mod terminal_observation;
pub use health::{
    StandingGoalDeletion, StandingGoalEvaluationFailure, StandingGoalEvaluationRetry,
};
pub use input::{AcceptStandingGoalInput, StandingGoalInputOutcome};

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_execution_grant::EncodedExecutionGrantProof;
use musteragent_goal::{
    GoalBudget, GoalBudgetUsage, GoalCompletionContract, GoalEvaluation, GoalEvaluationProposal,
    GoalOrigin, GoalProcessingRules, GoalPursuit, GoalPursuitStatus, GoalVerdict, GoalWaitBarrier,
    GoalWaitKind, StandingGoal, StandingGoalContractError, StandingGoalStatus,
};
use musteragent_identity::{
    AgentId, ConversationId, GoalEvaluationId, GoalId, GoalPursuitId, MessageId, PrincipalId,
    RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalDefinition {
    pub mode: musteragent_goal::StandingGoalMode,
    pub target_agent_id: AgentId,
    pub conversation_id: ConversationId,
    pub title: String,
    pub objective: Vec<u8>,
    pub constraints: Vec<String>,
    pub completion: GoalCompletionContract,
    pub budget: GoalBudget,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreateStandingGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub principal_id: PrincipalId,
    pub origin: GoalOrigin,
    pub creator_agent_id: Option<AgentId>,
    pub definition: StandingGoalDefinition,
    pub activate: bool,
    /// Binds an activated Goal to the Run that created it. Agent-originated
    /// active Goals must use the authenticated current Run here so the first
    /// Pursuit is neither duplicated nor admitted without a source grant.
    pub initial_run_id: Option<RunId>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplaceStandingGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_revision: u64,
    pub definition: StandingGoalDefinition,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ChangeStandingGoalStatus {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_goal_revision: u64,
    pub expected_pursuit_revision: Option<u64>,
    /// First Pursuit identity when binding an already admitted source Run.
    /// An independently sourced continuous Goal may omit it: the original
    /// lifecycle owner assigns a stable identity after source qualification.
    pub new_pursuit_id: Option<GoalPursuitId>,
    /// Binds the Draft activation to an already admitted user-visible Run.
    /// When absent, the Pursuit becomes due for the continuation worker.
    pub initial_run_id: Option<RunId>,
    /// User-facing Draft activations bind to the durable admitted message,
    /// never to a browser-process cache of the most recently observed Run.
    /// The Runtime resolves this immutable message to its source Run inside
    /// the authoritative Product store before creating or reauthorizing the
    /// Pursuit.
    pub initial_message_id: Option<MessageId>,
    pub status: StandingGoalStatus,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DeleteStandingGoal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub principal_id: PrincipalId,
    pub expected_revision: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConfirmStandingGoalCompletion {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub principal_id: PrincipalId,
    pub expected_goal_revision: u64,
    pub expected_pursuit_revision: u64,
    pub evaluation_id: GoalEvaluationId,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimStandingGoalPursuits {
    pub tenant_id: TenantId,
    pub worker_id: String,
    pub now: UtcTimestamp,
    pub lease_millis: u64,
    pub limit: u16,
}

/// A resumed Conversation Run and an owner event Goal have different source
/// authority. The latter must never borrow a historical chat Run/proof.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StandingGoalPursuitSource {
    TerminalRun {
        source_run_id: RunId,
        source_execution_grant_proof: EncodedExecutionGrantProof,
        continuation_authority: Option<Vec<u8>>,
    },
    OwnerGoal(crate::OwnerGoalScope),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedStandingGoalPursuit {
    pub processing_rules: Option<GoalProcessingRules>,
    pub input_refs: Vec<ContentRef>,
    pub inspection: StandingGoalInspection,
    pub claim_token: String,
    pub source: StandingGoalPursuitSource,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StoredStandingGoalPursuitClaim {
    pub processing_rules: Option<GoalProcessingRules>,
    pub input_refs: Vec<ContentRef>,
    pub goal: StandingGoal,
    pub pursuit: GoalPursuit,
    pub claim_token: String,
    pub source: StandingGoalPursuitSource,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordStandingGoalAdmissionFailure {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub claim_token: String,
    /// A permanent denial blocks execution until explicit user recovery. A transient dependency failure
    /// releases the claim and schedules this exact Pursuit again.
    pub permanent: bool,
    pub retry_at: Option<UtcTimestamp>,
    pub occurred_at: UtcTimestamp,
    /// Optional host-supplied refusal message stored in the `Waiting` barrier when `permanent` is true.
    pub reason: Option<musteragent_runtime_contract::SafeDisplayText>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeferStandingGoalAdmission {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub claim_token: String,
    pub retry_at: UtcTimestamp,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProposeStandingGoalEvaluation {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub run_id: RunId,
    pub principal_id: PrincipalId,
    pub verdict: GoalVerdict,
    /// Existing immutable Runtime content produced by the bounded Run.
    ///
    /// The proposal API deliberately does not accept free-form evidence text:
    /// an Agent claim is not evidence. Every reference is content-addressed
    /// and re-materialized below before the proposal can be persisted. The
    /// terminal evaluator still owns provenance/requirement verification.
    pub evidence: Vec<ContentRef>,
    pub summary: Vec<u8>,
    pub wait: Option<StandingGoalWaitDefinition>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalWaitDefinition {
    pub kind: GoalWaitKind,
    pub correlation_key: String,
    pub reason: Vec<u8>,
    pub resume_not_before: Option<UtcTimestamp>,
    pub expires_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimStandingGoalEvaluations {
    pub tenant_id: TenantId,
    pub worker_id: String,
    pub now: UtcTimestamp,
    pub lease_millis: u64,
    pub limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeferStandingGoalEvaluation {
    pub tenant_id: TenantId,
    pub pursuit_id: GoalPursuitId,
    pub run_id: RunId,
    pub claim_token: String,
    pub retry_at: UtcTimestamp,
    pub failure: Option<StandingGoalEvaluationFailure>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalRunTerminalStatus {
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GoalRunTerminalObservation {
    pub status: GoalRunTerminalStatus,
    pub failure_code: Option<musteragent_runtime_contract::ErrorCode>,
    pub usage: GoalBudgetUsage,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimedStandingGoalEvaluation {
    pub owner_goal_scope: Option<crate::OwnerGoalScope>,
    pub processing_rules: Option<GoalProcessingRules>,
    pub inspection: StandingGoalInspection,
    pub claim_token: String,
    pub run: GoalRunTerminalObservation,
    pub proposal: Option<GoalEvaluationProposal>,
    pub proposal_summary: Option<Vec<u8>>,
    /// Frozen proof of the terminal Run whose outcome is being judged.
    pub source_execution_grant_proof: EncodedExecutionGrantProof,
    /// Goal/Pursuit-scoped authority already exchanged by an earlier bounded
    /// Run, when present. The independent Judge may redeem the same bounded
    /// lineage but can never broaden it.
    pub continuation_authority: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StoredStandingGoalEvaluationClaim {
    pub owner_goal_scope: Option<crate::OwnerGoalScope>,
    pub processing_rules: Option<GoalProcessingRules>,
    pub goal: StandingGoal,
    pub pursuit: GoalPursuit,
    pub claim_token: String,
    pub run: GoalRunTerminalObservation,
    pub proposal: Option<GoalEvaluationProposal>,
    pub source_execution_grant_proof: EncodedExecutionGrantProof,
    pub continuation_authority: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvaluateStandingGoalRun {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub run_id: RunId,
    pub claim_token: String,
    pub evaluation_id: GoalEvaluationId,
    pub verdict: GoalVerdict,
    pub evidence: Vec<ContentRef>,
    pub summary: Vec<u8>,
    pub deterministic: bool,
    /// Historical internal contract only. True is rejected; current policy
    /// records an evidence dependency through the existing wait owner.
    pub completion_confirmation_required: bool,
    pub wait_barrier: Option<GoalWaitBarrier>,
    pub continuation_due_at: Option<UtcTimestamp>,
    pub usage_delta: GoalBudgetUsage,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResumeStandingGoalWait {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub principal_id: PrincipalId,
    pub correlation_key: String,
    pub occurred_at: UtcTimestamp,
}

/// Atomically wakes time-based wait barriers that have become due.
///
/// The persistence adapter owns selection and transition in one transaction so
/// two supervisors cannot both turn the same barrier into a continuation.
/// Event-correlated waits are resumed through `resume_wait`; this command is
/// deliberately limited to `RetryBackoff` barriers with an explicit deadline.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WakeDueStandingGoalWaits {
    pub tenant_id: TenantId,
    pub now: UtcTimestamp,
    pub limit: u16,
}

/// Atomically resumes interaction-correlated UserInput and Approval waits
/// after the authoritative Runtime interaction has a durable response.
///
/// The correlation key is the canonical Interaction UUID. Persistence joins
/// the wait against its own Runtime interaction authority, so UI presence,
/// websocket delivery and supervisor restarts cannot lose the wake-up.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WakeEventStandingGoalWaits {
    pub tenant_id: TenantId,
    pub now: UtcTimestamp,
    pub limit: u16,
}

/// Records one provider- or plugin-issued durable event before attempting to
/// wake any matching external-dependency wait. `signal_id` is the immutable
/// source event identity and makes at-least-once delivery idempotent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordStandingGoalWakeSignal {
    pub tenant_id: TenantId,
    pub signal_id: String,
    pub correlation_key: String,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalWakeSignalDisposition {
    Recorded,
    Existing,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalInspection {
    pub goal: StandingGoal,
    pub pursuit: Option<GoalPursuit>,
    pub objective: Vec<u8>,
    pub latest_evaluation: Option<GoalEvaluation>,
    pub progress: StandingGoalProgress,
}

/// User-visible phase of the durable Goal controller. This is deliberately
/// separate from Goal/Pursuit lifecycle state: a `Running` Pursuit can be
/// executing its bounded Run or privately evaluating the terminal result.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalProgressPhase {
    Ready,
    Continuing,
    Executing,
    Evaluating,
    Waiting,
    Paused,
    Achieved,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalProgress {
    pub phase: StandingGoalProgressPhase,
    pub run_number: u32,
    pub phase_started_at: UtcTimestamp,
    pub evaluation_retry: Option<StandingGoalEvaluationRetry>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandingGoalRunPhaseObservation {
    pub terminal: bool,
    pub changed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalListPage {
    pub goals: Vec<StandingGoalInspection>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalMutationOutcome {
    Applied,
    Conflict,
    NotFound,
}

pub type StandingGoalWithPursuit = (StandingGoal, Option<GoalPursuit>);

pub trait StandingGoalProductStore: Send + Sync {
    fn load_processing_rules(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<GoalProcessingRules>, StandingGoalStoreError>>;
    fn accept_input(
        &self,
        request: AcceptStandingGoalInput,
    ) -> ApplicationFuture<'_, Result<StandingGoalInputOutcome, StandingGoalStoreError>>;
    fn load_deletion(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalDeletion>, StandingGoalStoreError>>;

    fn load_evaluation_retry(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalEvaluationRetry>, StandingGoalStoreError>>;

    fn create(
        &self,
        goal: StandingGoal,
        pursuit: Option<GoalPursuit>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn load(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> ApplicationFuture<'_, Result<Option<StandingGoalWithPursuit>, StandingGoalStoreError>>;

    fn list_for_conversation(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<Vec<StandingGoalWithPursuit>, StandingGoalStoreError>>;

    fn load_latest_evaluation(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
    ) -> ApplicationFuture<'_, Result<Option<GoalEvaluation>, StandingGoalStoreError>>;

    fn load_run_phase(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<StandingGoalRunPhaseObservation>, StandingGoalStoreError>,
    >;

    fn resolve_message_source_run(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        message_id: MessageId,
    ) -> ApplicationFuture<'_, Result<Option<RunId>, StandingGoalStoreError>>;

    /// Returns whether this Goal crossed a trust boundary that requires a new
    /// Runtime-authorized source Run before it can become active. Ordinary
    /// paused Goals retain automatic continuation semantics.
    fn requires_explicit_activation_source(
        &self,
        _tenant_id: TenantId,
        _goal_id: GoalId,
    ) -> ApplicationFuture<'_, Result<bool, StandingGoalStoreError>> {
        Box::pin(async { Ok(false) })
    }

    /// Whether the original source owner permits waiting for the first input
    /// without borrowing a chat Run. Activation must recheck this atomically.
    fn has_independent_input_source(
        &self,
        _goal: StandingGoal,
    ) -> ApplicationFuture<'_, Result<bool, StandingGoalStoreError>> {
        Box::pin(async { Ok(false) })
    }

    fn replace_goal(
        &self,
        goal: StandingGoal,
        expected_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn replace_goal_and_pursuit(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn activate_goal_with_pursuit(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn list_completion_reviews(
        &self,
        tenant: TenantId,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<(GoalId, GoalPursuitId)>, StandingGoalStoreError>>;
    fn retire_completion_review(
        &self,
        retirement: StandingGoalReviewRetirement,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn delete(
        &self,
        request: DeleteStandingGoal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn claim_due(
        &self,
        request: ClaimStandingGoalPursuits,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalPursuitClaim>, StandingGoalStoreError>>;

    fn wake_due_waits(
        &self,
        request: WakeDueStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>>;

    fn wake_event_waits(
        &self,
        request: WakeEventStandingGoalWaits,
    ) -> ApplicationFuture<'_, Result<u16, StandingGoalStoreError>>;

    fn record_wake_signal(
        &self,
        request: RecordStandingGoalWakeSignal,
    ) -> ApplicationFuture<'_, Result<StandingGoalWakeSignalDisposition, StandingGoalStoreError>>;

    fn settle_admission_claim(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        claim_token: String,
        occurred_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn submit_evaluation_proposal(
        &self,
        proposal: GoalEvaluationProposal,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    fn claim_terminal_evaluations(
        &self,
        request: ClaimStandingGoalEvaluations,
    ) -> ApplicationFuture<'_, Result<Vec<StoredStandingGoalEvaluationClaim>, StandingGoalStoreError>>;

    fn defer_terminal_evaluation(
        &self,
        request: DeferStandingGoalEvaluation,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;

    #[expect(
        clippy::too_many_arguments,
        reason = "Existing host implementations use this port signature."
    )]
    fn commit_evaluation(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        evaluation: GoalEvaluation,
        expected_goal_revision: u64,
        expected_pursuit_revision: u64,
        claim_token: String,
        input_settlement: Option<RunId>,
    ) -> ApplicationFuture<'_, Result<StandingGoalMutationOutcome, StandingGoalStoreError>>;
}

pub struct StandingGoalProductApplication {
    store: Arc<dyn StandingGoalProductStore>,
    content: Arc<dyn ContentStore>,
}

impl StandingGoalProductApplication {
    #[must_use]
    pub fn new(store: Arc<dyn StandingGoalProductStore>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn create(
        &self,
        request: CreateStandingGoal,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        if (request.origin == GoalOrigin::Agent) != request.creator_agent_id.is_some() {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        // A Conversation is a single durable Goal lane. This application-level
        // check gives every caller the same product behavior and avoids writing
        // orphaned objective content during ordinary conflicts. Persistence
        // also owns a partial unique index because concurrent creators can both
        // pass this read before either transaction commits.
        if self
            .store
            .list_for_conversation(request.tenant_id, request.definition.conversation_id)
            .await?
            .into_iter()
            .any(|(goal, _)| !goal.status.terminal())
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let objective_ref = self
            .put_text(request.tenant_id, &request.definition.objective)
            .await?;
        if !request.activate && request.initial_run_id.is_some() {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        if request.activate && request.initial_run_id.is_none() {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let pursuit = request.activate.then(|| {
            let initial_run_id = request.initial_run_id;
            GoalPursuit {
                tenant_id: request.tenant_id,
                pursuit_id: request.pursuit_id,
                goal_id: request.goal_id,
                revision: 1,
                status: if initial_run_id.is_some() {
                    GoalPursuitStatus::Running
                } else {
                    GoalPursuitStatus::Pending
                },
                active_run_id: initial_run_id,
                continuation_source_run_id: None,
                wait_barrier: None,
                budget_usage: GoalBudgetUsage {
                    runs: u32::from(initial_run_id.is_some()),
                    ..GoalBudgetUsage::default()
                },
                continuation_due_at: initial_run_id.is_none().then_some(request.occurred_at),
                started_at: request.occurred_at,
                updated_at: request.occurred_at,
                completed_at: None,
            }
        });
        let goal = StandingGoal {
            input_progress: Default::default(),
            tenant_id: request.tenant_id,
            goal_id: request.goal_id,
            revision: 1,
            owner: request.principal_id,
            origin: request.origin,
            mode: request.definition.mode,
            creator_agent_id: request.creator_agent_id,
            target_agent_id: request.definition.target_agent_id,
            conversation_id: request.definition.conversation_id,
            title: request.definition.title,
            objective_ref,
            constraints: request.definition.constraints,
            completion: request.definition.completion,
            budget: request.definition.budget,
            status: if request.activate {
                StandingGoalStatus::Active
            } else {
                StandingGoalStatus::Draft
            },
            active_pursuit_id: pursuit.as_ref().map(|value| value.pursuit_id),
            created_at: request.occurred_at,
            updated_at: request.occurred_at,
        };
        goal.validate()?;
        if let Some(value) = &pursuit {
            value.validate()?;
        }
        let progress = self.observe_progress(&goal, pursuit.as_ref(), None).await?;
        mutation(self.store.create(goal.clone(), pursuit.clone()).await?)?;
        Ok(StandingGoalInspection {
            goal,
            pursuit,
            objective: request.definition.objective,
            latest_evaluation: None,
            progress,
        })
    }

    pub async fn replace(
        &self,
        request: ReplaceStandingGoal,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let prepared = self.prepare_replace(request).await?;
        self.commit_prepared_change(prepared).await
    }

    pub async fn prepare_replace(
        &self,
        request: ReplaceStandingGoal,
    ) -> Result<PreparedStandingGoalChange, StandingGoalProductError> {
        let mut inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if inspection.goal.revision != request.expected_revision
            || request.definition.mode != inspection.goal.mode
            || request.definition.conversation_id != inspection.goal.conversation_id
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let objective_ref = self
            .put_text(request.tenant_id, &request.definition.objective)
            .await?;
        inspection.goal.revision = next_revision(inspection.goal.revision)?;
        inspection.goal.target_agent_id = request.definition.target_agent_id;
        inspection.goal.title = request.definition.title;
        inspection.goal.objective_ref = objective_ref;
        inspection.goal.constraints = request.definition.constraints;
        inspection.goal.completion = request.definition.completion;
        inspection.goal.budget = request.definition.budget;
        inspection.goal.updated_at = request.occurred_at;
        inspection.goal.validate()?;
        inspection.progress = self
            .observe_progress(
                &inspection.goal,
                inspection.pursuit.as_ref(),
                inspection.latest_evaluation.as_ref(),
            )
            .await?;
        inspection.objective = request.definition.objective;
        Ok(PreparedStandingGoalChange {
            inspection,
            mutation: StandingGoalChangeMutation::Replace {
                goal_revision: request.expected_revision,
            },
        })
    }

    pub async fn delete(
        &self,
        request: DeleteStandingGoal,
    ) -> Result<(), StandingGoalProductError> {
        let inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        if inspection.goal.revision != request.expected_revision {
            return Err(StandingGoalProductError::Conflict);
        }
        if inspection.goal.status == StandingGoalStatus::Active {
            return Err(StandingGoalProductError::GoalStillActive);
        }
        mutation(self.store.delete(request).await?)
    }

    /// Retained RPC compatibility only: a human click can no longer certify
    /// completion. Existing result-review waits are retired by the Goal owner.
    pub async fn confirm_completion(
        &self,
        _request: ConfirmStandingGoalCompletion,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        Err(StandingGoalProductError::InvalidRequest)
    }

    pub async fn inspect(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let (goal, pursuit) = self
            .store
            .load(tenant_id, goal_id)
            .await?
            .ok_or(StandingGoalProductError::NotFound)?;
        self.materialize(goal, pursuit).await
    }

    pub async fn list_for_conversation(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> Result<StandingGoalListPage, StandingGoalProductError> {
        let rows = self
            .store
            .list_for_conversation(tenant_id, conversation_id)
            .await?;
        let mut goals = Vec::with_capacity(rows.len());
        for (goal, pursuit) in rows {
            goals.push(self.materialize(goal, pursuit).await?);
        }
        Ok(StandingGoalListPage { goals })
    }

    /// Lists only Goals owned by the authenticated principal. Product and
    /// Tool adapters must use this boundary instead of filtering after data
    /// has crossed the application authorization boundary.
    pub async fn list_owned_for_conversation(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        principal_id: PrincipalId,
    ) -> Result<StandingGoalListPage, StandingGoalProductError> {
        let page = self
            .list_for_conversation(tenant_id, conversation_id)
            .await?;
        Ok(StandingGoalListPage {
            goals: page
                .goals
                .into_iter()
                .filter(|inspection| inspection.goal.owner == principal_id)
                .collect(),
        })
    }

    /// Reads one Goal while enforcing principal ownership inside the product
    /// application, before its objective or evaluation material is returned.
    pub async fn inspect_owned(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        self.load_owned(tenant_id, goal_id, principal_id).await
    }

    pub async fn claim_due(
        &self,
        request: ClaimStandingGoalPursuits,
    ) -> Result<Vec<ClaimedStandingGoalPursuit>, StandingGoalProductError> {
        if request.worker_id.is_empty()
            || request.worker_id.len() > 255
            || !(1_000..=10 * 60_000).contains(&request.lease_millis)
            || request.limit == 0
            || request.limit > 100
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let rows = self.store.claim_due(request).await?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            claims.push(ClaimedStandingGoalPursuit {
                processing_rules: row.processing_rules,
                input_refs: row.input_refs,
                inspection: self.materialize(row.goal, Some(row.pursuit)).await?,
                claim_token: row.claim_token,
                source: row.source,
            });
        }
        Ok(claims)
    }

    pub async fn wake_due_waits(
        &self,
        request: WakeDueStandingGoalWaits,
    ) -> Result<u16, StandingGoalProductError> {
        if request.limit == 0 || request.limit > 100 {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        self.store.wake_due_waits(request).await.map_err(Into::into)
    }

    pub async fn wake_event_waits(
        &self,
        request: WakeEventStandingGoalWaits,
    ) -> Result<u16, StandingGoalProductError> {
        if request.limit == 0 || request.limit > 100 {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        self.store
            .wake_event_waits(request)
            .await
            .map_err(Into::into)
    }

    pub async fn record_wake_signal(
        &self,
        request: RecordStandingGoalWakeSignal,
    ) -> Result<StandingGoalWakeSignalDisposition, StandingGoalProductError> {
        if request.correlation_key.starts_with("standing-goal-input:")
            || request.signal_id.is_empty()
            || request.signal_id.len() > 512
            || request.correlation_key.is_empty()
            || request.correlation_key.len() > 512
            || request.signal_id.contains('\0')
            || request.correlation_key.contains('\0')
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        self.store
            .record_wake_signal(request)
            .await
            .map_err(Into::into)
    }

    /// Records an Agent-authored proposal without changing Goal state. The
    /// terminal evaluator is the only owner allowed to accept a proposal.
    pub async fn propose_evaluation(
        &self,
        request: ProposeStandingGoalEvaluation,
    ) -> Result<GoalEvaluationProposal, StandingGoalProductError> {
        if request
            .wait
            .as_ref()
            .is_some_and(|wait| wait.kind == GoalWaitKind::CompletionConfirmation)
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        let pursuit = inspection
            .pursuit
            .ok_or(StandingGoalProductError::Corrupt)?;
        if !inspection.goal.allows_existing_work()
            || pursuit.pursuit_id != request.pursuit_id
            || pursuit.status != GoalPursuitStatus::Running
            || pursuit.active_run_id != Some(request.run_id)
            || (request.verdict == GoalVerdict::Waiting) != request.wait.is_some()
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let summary_ref = self.put_text(request.tenant_id, &request.summary).await?;
        for reference in &request.evidence {
            let bytes = self.content.read(request.tenant_id, reference).await?;
            musteragent_runtime_content::verify_content(reference, &bytes)
                .map_err(|_| StandingGoalProductError::Corrupt)?;
        }
        let wait_barrier = match request.wait {
            Some(wait) => Some(GoalWaitBarrier {
                kind: wait.kind,
                correlation_key: wait.correlation_key,
                reason_ref: self.put_text(request.tenant_id, &wait.reason).await?,
                resume_not_before: wait.resume_not_before,
                expires_at: wait.expires_at,
            }),
            None => None,
        };
        let proposal = GoalEvaluationProposal {
            tenant_id: request.tenant_id,
            goal_id: request.goal_id,
            pursuit_id: request.pursuit_id,
            run_id: request.run_id,
            verdict: request.verdict,
            evidence: request.evidence,
            summary_ref,
            wait_barrier,
            proposed_at: request.occurred_at,
        };
        let completion = self
            .processing_completion(&inspection.goal, request.run_id)
            .await?;
        proposal.validate(&completion)?;
        mutation(
            self.store
                .submit_evaluation_proposal(proposal.clone())
                .await?,
        )?;
        Ok(proposal)
    }

    pub async fn claim_terminal_evaluations(
        &self,
        request: ClaimStandingGoalEvaluations,
    ) -> Result<Vec<ClaimedStandingGoalEvaluation>, StandingGoalProductError> {
        validate_claim_request(&request.worker_id, request.lease_millis, request.limit)?;
        let rows = self.store.claim_terminal_evaluations(request).await?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let proposal_summary = match &row.proposal {
                Some(proposal) => Some(
                    self.content
                        .read(row.goal.tenant_id, &proposal.summary_ref)
                        .await?,
                ),
                None => None,
            };
            claims.push(ClaimedStandingGoalEvaluation {
                owner_goal_scope: row.owner_goal_scope,
                processing_rules: row.processing_rules,
                inspection: self.materialize(row.goal, Some(row.pursuit)).await?,
                claim_token: row.claim_token,
                run: row.run,
                proposal: row.proposal,
                proposal_summary,
                source_execution_grant_proof: row.source_execution_grant_proof,
                continuation_authority: row.continuation_authority,
            });
        }
        Ok(claims)
    }

    pub async fn defer_terminal_evaluation(
        &self,
        request: DeferStandingGoalEvaluation,
    ) -> Result<(), StandingGoalProductError> {
        if request.claim_token.is_empty()
            || request.claim_token.len() > 512
            || request.retry_at.unix_millis() <= 0
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        mutation(self.store.defer_terminal_evaluation(request).await?)
    }

    pub async fn evaluate_run(
        &self,
        request: EvaluateStandingGoalRun,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let (mut goal, pursuit) = self
            .store
            .load(request.tenant_id, request.goal_id)
            .await?
            .ok_or(StandingGoalProductError::NotFound)?;
        let mut pursuit = pursuit.ok_or(StandingGoalProductError::Corrupt)?;
        if request.claim_token.is_empty()
            || request.claim_token.len() > 512
            || pursuit.pursuit_id != request.pursuit_id
            || pursuit.status != GoalPursuitStatus::Running
            || pursuit.active_run_id != Some(request.run_id)
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let completion = self.processing_completion(&goal, request.run_id).await?;
        let summary_ref = self.put_text(request.tenant_id, &request.summary).await?;
        if request.completion_confirmation_required
            || request
                .wait_barrier
                .as_ref()
                .is_some_and(is_completion_confirmation_barrier)
        {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        let wait_barrier = request.wait_barrier;
        let completed_batch = goal.mode == musteragent_goal::StandingGoalMode::Continuous
            && request.verdict == GoalVerdict::Achieved;
        if completed_batch
            && request.evidence.len() < usize::from(completion.minimum_evidence_items)
        {
            return Err(StandingGoalProductError::EvaluationRequired);
        }
        let waiting_for_input = completed_batch
            || (goal.mode == musteragent_goal::StandingGoalMode::Continuous
                && goal.status == StandingGoalStatus::Paused
                && goal.input_progress.pending() == 0
                && request.verdict == GoalVerdict::Continue);
        let verdict = if waiting_for_input {
            GoalVerdict::Waiting
        } else {
            request.verdict
        };
        let wait_barrier = if waiting_for_input {
            Some(goal.input_wait(summary_ref.clone()))
        } else {
            wait_barrier
        };
        let evaluation = GoalEvaluation {
            tenant_id: request.tenant_id,
            evaluation_id: request.evaluation_id,
            goal_id: request.goal_id,
            pursuit_id: request.pursuit_id,
            run_id: Some(request.run_id),
            verdict,
            evidence: request.evidence,
            summary_ref,
            deterministic: request.deterministic,
            occurred_at: request.occurred_at,
        };
        evaluation.validate(&completion)?;
        let expected_goal_revision = goal.revision;
        let expected_pursuit_revision = pursuit.revision;
        let mut resource_usage_delta = request.usage_delta;
        // `elapsed_millis` is authoritative Pursuit active time, not provider
        // execution duration. Count the Running state interval exactly once;
        // model and tool usage remain sourced from the observed Run.
        resource_usage_delta.elapsed_millis = 0;
        pursuit.budget_usage = add_usage(pursuit.budget_usage, resource_usage_delta)?;
        pursuit.budget_usage.elapsed_millis = pursuit
            .budget_usage
            .elapsed_millis
            .checked_add(active_interval_millis(
                pursuit.updated_at,
                request.occurred_at,
            )?)
            .ok_or(StandingGoalProductError::Corrupt)?;
        pursuit.continuation_source_run_id = pursuit.active_run_id;
        pursuit.active_run_id = None;
        pursuit.updated_at = request.occurred_at;
        pursuit.revision = next_revision(pursuit.revision)?;
        if goal.status == StandingGoalStatus::Paused
            && goal.mode != musteragent_goal::StandingGoalMode::Continuous
        {
            pursuit.status = GoalPursuitStatus::Paused;
            pursuit.continuation_due_at = None;
            pursuit.wait_barrier = None;
        } else if goal.status == StandingGoalStatus::Cancelled {
            pursuit.status = GoalPursuitStatus::Cancelled;
            pursuit.completed_at = Some(request.occurred_at);
            pursuit.continuation_due_at = None;
            pursuit.wait_barrier = None;
        } else {
            match verdict {
                GoalVerdict::Achieved => {
                    pursuit.status = GoalPursuitStatus::Achieved;
                    pursuit.completed_at = Some(request.occurred_at);
                    pursuit.continuation_due_at = None;
                    goal.status = StandingGoalStatus::Achieved;
                    goal.active_pursuit_id = None;
                }
                GoalVerdict::Failed => {
                    pursuit.status = GoalPursuitStatus::Failed;
                    pursuit.completed_at = Some(request.occurred_at);
                    pursuit.continuation_due_at = None;
                    goal.status = StandingGoalStatus::Failed;
                    goal.active_pursuit_id = None;
                }
                GoalVerdict::Waiting => {
                    pursuit.status = GoalPursuitStatus::Waiting;
                    pursuit.wait_barrier =
                        Some(wait_barrier.ok_or(StandingGoalProductError::InvalidRequest)?);
                    pursuit.continuation_due_at = None;
                }
                GoalVerdict::Continue => {
                    // `Continue` is authoritative: a 24x7 Goal must always
                    // schedule the next bounded Run. Cumulative usage remains
                    // telemetry and has no lifecycle authority.
                    pursuit.status = GoalPursuitStatus::Pending;
                    if request.usage_delta.consecutive_failures == 0 {
                        pursuit.budget_usage.consecutive_failures = 0;
                    }
                    pursuit.continuation_due_at =
                        Some(request.continuation_due_at.unwrap_or(request.occurred_at));
                }
            }
        }
        goal.revision = next_revision(goal.revision)?;
        goal.updated_at = request.occurred_at;
        goal.validate()?;
        pursuit.validate()?;
        mutation(
            self.store
                .commit_evaluation(
                    goal.clone(),
                    pursuit.clone(),
                    evaluation.clone(),
                    expected_goal_revision,
                    expected_pursuit_revision,
                    request.claim_token,
                    completed_batch.then_some(request.run_id),
                )
                .await?,
        )?;
        if goal.mode == musteragent_goal::StandingGoalMode::Continuous {
            return self
                .inspect_owned(goal.tenant_id, goal.goal_id, goal.owner)
                .await;
        }
        let objective = self
            .content
            .read(goal.tenant_id, &goal.objective_ref)
            .await?;
        let progress = standing_goal_progress(&goal, Some(&pursuit), Some(&evaluation), None);
        Ok(StandingGoalInspection {
            goal,
            pursuit: Some(pursuit),
            objective,
            latest_evaluation: Some(evaluation),
            progress,
        })
    }

    pub async fn resume_wait(
        &self,
        request: ResumeStandingGoalWait,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let inspection = self
            .load_owned(request.tenant_id, request.goal_id, request.principal_id)
            .await?;
        let pursuit = inspection
            .pursuit
            .as_ref()
            .ok_or(StandingGoalProductError::Corrupt)?;
        let barrier = pursuit
            .wait_barrier
            .as_ref()
            .ok_or(StandingGoalProductError::Conflict)?;
        if is_completion_confirmation_barrier(barrier)
            || barrier.correlation_key == inspection.goal.input_wait_correlation()
        {
            // Historical result reviews must first pass the exact retirement
            // owner; a generic wake cannot discard their retained evidence.
            return Err(StandingGoalProductError::InvalidRequest);
        }
        if pursuit.pursuit_id != request.pursuit_id
            || barrier.correlation_key != request.correlation_key
            || !inspection.goal.allows_existing_work()
            || pursuit.status != GoalPursuitStatus::Waiting
        {
            return Err(StandingGoalProductError::Conflict);
        }
        let change = ChangeStandingGoalStatus {
            tenant_id: request.tenant_id,
            goal_id: request.goal_id,
            principal_id: request.principal_id,
            expected_goal_revision: inspection.goal.revision,
            expected_pursuit_revision: Some(pursuit.revision),
            new_pursuit_id: None,
            initial_run_id: None,
            initial_message_id: None,
            status: inspection.goal.status,
            occurred_at: request.occurred_at,
        };
        // Keep the snapshot whose barrier was checked. The original Goal and
        // Pursuit CAS rejects any replacement or concurrent resume of it.
        self.change_status_from_inspection(
            inspection,
            change,
            status::StatusChangeIntent::ResumeWait,
        )
        .await
    }

    pub async fn inspect_deletion_owned(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
        conversation_id: ConversationId,
    ) -> Result<Option<StandingGoalDeletion>, StandingGoalProductError> {
        Ok(self
            .store
            .load_deletion(tenant_id, goal_id, principal_id, conversation_id)
            .await?)
    }

    async fn load_owned(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let inspection = self.inspect(tenant_id, goal_id).await?;
        if inspection.goal.owner != principal_id {
            return Err(StandingGoalProductError::Unauthorized);
        }
        Ok(inspection)
    }

    async fn materialize(
        &self,
        goal: StandingGoal,
        pursuit: Option<GoalPursuit>,
    ) -> Result<StandingGoalInspection, StandingGoalProductError> {
        let objective = self
            .content
            .read(goal.tenant_id, &goal.objective_ref)
            .await?;
        let latest_evaluation = match pursuit.as_ref() {
            Some(value) => {
                self.store
                    .load_latest_evaluation(goal.tenant_id, value.pursuit_id)
                    .await?
            }
            None => None,
        };
        let progress = self
            .observe_progress(&goal, pursuit.as_ref(), latest_evaluation.as_ref())
            .await?;
        Ok(StandingGoalInspection {
            goal,
            pursuit,
            objective,
            latest_evaluation,
            progress,
        })
    }

    /// Reads and mutation responses use the same execution observation. A Goal
    /// edit/activation timestamp cannot replace the source Run's phase clock.
    /// Mutations collect this before their CAS so a read failure cannot turn a
    /// committed write into an apparent rejection. This is an observation, not
    /// a Run fence; the source may legitimately progress after it was sampled.
    async fn observe_progress(
        &self,
        goal: &StandingGoal,
        pursuit: Option<&GoalPursuit>,
        latest_evaluation: Option<&GoalEvaluation>,
    ) -> Result<StandingGoalProgress, StandingGoalProductError> {
        let run_phase = match pursuit.and_then(|value| value.active_run_id) {
            Some(run_id) => self.store.load_run_phase(goal.tenant_id, run_id).await?,
            None => None,
        };
        let mut progress = standing_goal_progress(goal, pursuit, latest_evaluation, run_phase);
        if progress.phase == StandingGoalProgressPhase::Evaluating
            && let Some(pursuit) = pursuit
            && let Some(run_id) = pursuit.active_run_id
        {
            progress.evaluation_retry = self
                .store
                .load_evaluation_retry(goal.tenant_id, pursuit.pursuit_id, run_id)
                .await?;
        }
        Ok(progress)
    }

    async fn put_text(
        &self,
        tenant_id: TenantId,
        bytes: &[u8],
    ) -> Result<ContentRef, StandingGoalProductError> {
        if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 {
            return Err(StandingGoalProductError::InvalidRequest);
        }
        Ok(self
            .content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("text/markdown")
                    .map_err(|_| StandingGoalProductError::Corrupt)?,
                bytes: bytes.to_vec(),
            })
            .await?)
    }
}

fn completion_confirmation_correlation(
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    run_id: RunId,
) -> String {
    format!("standing-goal-completion:{goal_id}:{pursuit_id}:{run_id}")
}

fn is_completion_confirmation_barrier(barrier: &GoalWaitBarrier) -> bool {
    barrier.kind == GoalWaitKind::CompletionConfirmation
}

fn add_usage(
    current: GoalBudgetUsage,
    delta: GoalBudgetUsage,
) -> Result<GoalBudgetUsage, StandingGoalProductError> {
    Ok(GoalBudgetUsage {
        runs: current
            .runs
            .checked_add(delta.runs)
            .ok_or(StandingGoalProductError::Corrupt)?,
        elapsed_millis: current
            .elapsed_millis
            .checked_add(delta.elapsed_millis)
            .ok_or(StandingGoalProductError::Corrupt)?,
        model_tokens: current
            .model_tokens
            .checked_add(delta.model_tokens)
            .ok_or(StandingGoalProductError::Corrupt)?,
        tool_invocations: current
            .tool_invocations
            .checked_add(delta.tool_invocations)
            .ok_or(StandingGoalProductError::Corrupt)?,
        consecutive_failures: current
            .consecutive_failures
            .checked_add(delta.consecutive_failures)
            .ok_or(StandingGoalProductError::Corrupt)?,
    })
}

fn active_interval_millis(
    started_at: UtcTimestamp,
    ended_at: UtcTimestamp,
) -> Result<u64, StandingGoalProductError> {
    let elapsed = ended_at
        .unix_millis()
        .checked_sub(started_at.unix_millis())
        .ok_or(StandingGoalProductError::InvalidRequest)?;
    u64::try_from(elapsed).map_err(|_| StandingGoalProductError::InvalidRequest)
}

fn standing_goal_progress(
    goal: &StandingGoal,
    pursuit: Option<&GoalPursuit>,
    latest_evaluation: Option<&GoalEvaluation>,
    run_phase: Option<StandingGoalRunPhaseObservation>,
) -> StandingGoalProgress {
    let phase = match goal.status {
        StandingGoalStatus::Draft => StandingGoalProgressPhase::Ready,
        StandingGoalStatus::Paused
            if !goal.allows_existing_work()
                || (goal.input_progress.pending() == 0
                    && !pursuit.is_some_and(|p| p.status == GoalPursuitStatus::Running)) =>
        {
            StandingGoalProgressPhase::Paused
        }
        StandingGoalStatus::Achieved => StandingGoalProgressPhase::Achieved,
        StandingGoalStatus::Failed => StandingGoalProgressPhase::Failed,
        StandingGoalStatus::Cancelled => StandingGoalProgressPhase::Cancelled,
        StandingGoalStatus::Active | StandingGoalStatus::Paused => match pursuit
            .map(|value| value.status)
        {
            Some(GoalPursuitStatus::Pending) => StandingGoalProgressPhase::Continuing,
            Some(GoalPursuitStatus::Running) if run_phase.is_some_and(|value| value.terminal) => {
                StandingGoalProgressPhase::Evaluating
            }
            Some(GoalPursuitStatus::Running) => StandingGoalProgressPhase::Executing,
            Some(GoalPursuitStatus::Waiting)
                if goal.status == StandingGoalStatus::Paused
                    && goal.input_progress.pending() == 0
                    && pursuit
                        .and_then(|p| p.wait_barrier.as_ref())
                        .is_some_and(|wait| {
                            wait.correlation_key == goal.input_wait_correlation()
                        }) =>
            {
                StandingGoalProgressPhase::Paused
            }
            Some(GoalPursuitStatus::Waiting) => StandingGoalProgressPhase::Waiting,
            Some(GoalPursuitStatus::Paused) => StandingGoalProgressPhase::Paused,
            Some(GoalPursuitStatus::Achieved) => StandingGoalProgressPhase::Achieved,
            Some(GoalPursuitStatus::Failed) => StandingGoalProgressPhase::Failed,
            Some(GoalPursuitStatus::Cancelled) => StandingGoalProgressPhase::Cancelled,
            None => StandingGoalProgressPhase::Ready,
        },
    };
    let phase_started_at = run_phase
        .map(|value| value.changed_at)
        .or_else(|| match phase {
            StandingGoalProgressPhase::Continuing
            | StandingGoalProgressPhase::Waiting
            | StandingGoalProgressPhase::Achieved
            | StandingGoalProgressPhase::Failed => latest_evaluation.map(|value| value.occurred_at),
            _ => None,
        })
        .or_else(|| pursuit.map(|value| value.updated_at))
        .unwrap_or(goal.updated_at);
    let completed_or_active_runs = pursuit.map_or(0, |value| value.budget_usage.runs);
    StandingGoalProgress {
        phase,
        // Pending means the evaluator has already settled the prior Run and
        // the controller is preparing its successor, so expose the successor
        // number. Running/evaluating/terminal phases expose the admitted Run.
        run_number: if phase == StandingGoalProgressPhase::Continuing {
            completed_or_active_runs.saturating_add(1)
        } else {
            completed_or_active_runs
        },
        phase_started_at,
        evaluation_retry: None,
    }
}

fn next_revision(value: u64) -> Result<u64, StandingGoalProductError> {
    value
        .checked_add(1)
        .ok_or(StandingGoalProductError::Corrupt)
}

fn mutation(value: StandingGoalMutationOutcome) -> Result<(), StandingGoalProductError> {
    match value {
        StandingGoalMutationOutcome::Applied => Ok(()),
        StandingGoalMutationOutcome::Conflict => Err(StandingGoalProductError::Conflict),
        StandingGoalMutationOutcome::NotFound => Err(StandingGoalProductError::NotFound),
    }
}

fn validate_claim_request(
    worker_id: &str,
    lease_millis: u64,
    limit: u16,
) -> Result<(), StandingGoalProductError> {
    if worker_id.is_empty()
        || worker_id.len() > 255
        || !(1_000..=10 * 60_000).contains(&lease_millis)
        || limit == 0
        || limit > 100
    {
        Err(StandingGoalProductError::InvalidRequest)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalStoreError {
    #[error("standing Goal store is unavailable")]
    Unavailable,
    #[error("standing Goal store returned corrupt state")]
    Corrupt,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalProductError {
    #[error("standing Goal request is invalid")]
    InvalidRequest,
    #[error("standing Goal was not found")]
    NotFound,
    #[error("standing Goal is not authorized")]
    Unauthorized,
    #[error("standing Goal state changed concurrently")]
    Conflict,
    #[error("standing Goal cannot mutate while its Run is active")]
    RunStillActive,
    #[error("standing Goal is still active")]
    GoalStillActive,
    #[error("standing Goal terminal state requires an Evaluation")]
    EvaluationRequired,
    #[error("standing Goal dependency is unavailable")]
    Unavailable,
    #[error("standing Goal state is corrupt")]
    Corrupt,
}

impl From<StandingGoalContractError> for StandingGoalProductError {
    fn from(error: StandingGoalContractError) -> Self {
        match error {
            StandingGoalContractError::InsufficientEvidence => Self::EvaluationRequired,
            _ => Self::InvalidRequest,
        }
    }
}

impl From<ContentStoreError> for StandingGoalProductError {
    fn from(error: ContentStoreError) -> Self {
        match error {
            ContentStoreError::Unauthorized => Self::Unauthorized,
            ContentStoreError::TooLarge => Self::InvalidRequest,
            ContentStoreError::Unavailable => Self::Unavailable,
            ContentStoreError::NotFound
            | ContentStoreError::LengthMismatch
            | ContentStoreError::HashMismatch
            | ContentStoreError::IdentityConflict
            | ContentStoreError::Invariant => Self::Corrupt,
        }
    }
}

impl From<StandingGoalStoreError> for StandingGoalProductError {
    fn from(error: StandingGoalStoreError) -> Self {
        match error {
            StandingGoalStoreError::Unavailable => Self::Unavailable,
            StandingGoalStoreError::Corrupt => Self::Corrupt,
        }
    }
}
