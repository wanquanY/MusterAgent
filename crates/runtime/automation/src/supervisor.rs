//! Home-neutral supervisor for standing-Goal evaluation and continuation.
//!
//! Runtime terminal observations drive evaluation. Agent progress proposals
//! are optional compatibility hints. This worker owns the two authoritative
//! transitions: evaluating a terminal Execution Run and admitting the next
//! independently granted Run.

use std::sync::Arc;
use std::time::Duration;

use crate::{
    ClaimStandingGoalEvaluations, ClaimStandingGoalPursuits, ClaimedStandingGoalEvaluation,
    ClaimedStandingGoalPursuit, DeferStandingGoalAdmission, DeferStandingGoalEvaluation,
    EvaluateStandingGoalRun, GoalRunTerminalObservation, GoalRunTerminalStatus,
    RecordStandingGoalAdmissionFailure, StandingGoalEvaluationFailure,
    StandingGoalProductApplication, StandingGoalProductError, WakeDueStandingGoalWaits,
    WakeEventStandingGoalWaits,
};
use crate::{StandingGoalAdmission, StandingGoalAdmissionFence, StandingGoalAdmissionOutcome};
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::ExecutionScope;
use musteragent_execution_grant::{EncodedExecutionGrantProof, SignedExecutionGrant};
use musteragent_goal::{
    GoalEvaluationProposal, GoalPursuit, GoalRiskClass, GoalVerdict, GoalWaitBarrier, StandingGoal,
};
use musteragent_identity::{
    Actor, CommandId, ConversationId, GoalEvaluationId, GoalId, GoalPursuitId, RunId, TenantId,
    UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_contract::{ErrorCode, RuntimeError, TraceContext, TraceParent};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

mod run_loop;

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const ERROR_RETRY_BASE_INTERVAL: Duration = Duration::from_secs(1);
const ERROR_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(60);
const CLAIM_LEASE_MILLIS: u64 = 5 * 60_000;
const CLAIM_LIMIT: u16 = 16;
const ADMISSION_DEADLINE_MILLIS: i64 = 5 * 60_000;
const PREPARATION_CONFLICT_RETRY_MILLIS: i64 = 1_000;
const FAILURE_BACKOFF_BASE_MILLIS: i64 = 1_000;
const FAILURE_BACKOFF_MAX_MILLIS: i64 = 5 * 60_000;
const EVALUATION_PENDING_RECHECK_MILLIS: i64 = 1_000;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StandingGoalAdmissionPurpose {
    Continuation,
    TerminalJudge,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalContinuationAuthorityRequest {
    pub purpose: StandingGoalAdmissionPurpose,
    pub source_execution_grant_proof: Option<EncodedExecutionGrantProof>,
    pub durable_continuation_authority: Option<Vec<u8>>,
    pub conversation_id: ConversationId,
    pub workspace_id: Option<WorkspaceId>,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub target_command_id: CommandId,
    pub target_run_id: RunId,
}

#[derive(Debug, Clone)]
pub struct StandingGoalContinuationGrant {
    pub signed_execution_grant: SignedExecutionGrant,
    pub durable_continuation_authority: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalContinuationAuthorityFailure {
    Suspended,
    InvalidRequest,
    DependencyUnavailable,
    Denied,
    InvalidResponse,
}

impl StandingGoalContinuationAuthorityFailure {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Suspended => "owner_suspended",
            Self::InvalidRequest => "invalid_request",
            Self::DependencyUnavailable => "dependency_unavailable",
            Self::Denied => "denied",
            Self::InvalidResponse => "invalid_response",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("standing Goal continuation authority failed: {kind:?}")]
pub struct StandingGoalContinuationAuthorityError {
    pub runtime_code: Option<ErrorCode>,
    pub correlation_id: Option<musteragent_identity::CorrelationId>,
    pub kind: StandingGoalContinuationAuthorityFailure,
    pub retryable: bool,
}

pub trait StandingGoalContinuationAuthority: Send + Sync {
    /// Optional local owner eligibility, not authentication or grant extension.
    /// A long-lived service owner has no extra local lifecycle. Native hosts
    /// override this and callers recheck after IO, immediately before admission.
    fn require_active_owner(&self) -> Result<(), StandingGoalContinuationAuthorityError> {
        Ok(())
    }

    fn grant(
        &self,
        request: StandingGoalContinuationAuthorityRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<StandingGoalContinuationGrant, StandingGoalContinuationAuthorityError>,
    >;
}

/// Immutable input to the authoritative terminal evaluator.
///
/// A Goal Run may optionally propose an outcome, but the proposal remains
/// untrusted input and is never required to wake evaluation. Only an
/// implementation of this port may return a terminal verdict that mutates the
/// Goal/Pursuit authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalTerminalEvaluationRequest {
    pub owner_goal_scope: Option<crate::OwnerGoalScope>,
    pub claim_token: String,
    pub goal: StandingGoal,
    pub pursuit: GoalPursuit,
    pub run: GoalRunTerminalObservation,
    pub proposal: Option<GoalEvaluationProposal>,
    pub proposal_summary: Option<Vec<u8>>,
    pub source_execution_grant_proof: EncodedExecutionGrantProof,
    pub continuation_authority: Option<Vec<u8>>,
    pub observed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalTerminalEvaluationDecision {
    pub verdict: GoalVerdict,
    pub evidence: Vec<musteragent_content_ref::ContentRef>,
    pub summary: Vec<u8>,
    pub deterministic: bool,
    pub completion_confirmation_required: bool,
    pub wait_barrier: Option<GoalWaitBarrier>,
    pub continuation_due_at: Option<UtcTimestamp>,
}

/// Closed result of observing a private terminal-evaluation workflow.
///
/// `Pending` is a healthy, non-terminal state: the independently admitted
/// Judge Run still owns the decision. It must not be reported or repaired as
/// an evaluator failure. `Decided` is the only outcome allowed to mutate Goal
/// authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StandingGoalTerminalEvaluationOutcome {
    Pending,
    /// The exact Goal/claim was superseded; never settle a newer owner's state.
    Superseded,
    Decided(Box<StandingGoalTerminalEvaluationDecision>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalTerminalEvaluationStage {
    BuildJudgeInput,
    PrepareJudgeRun,
    LoadJudgeRun,
    GrantJudgeAuthority,
    PersistJudgeAuthority,
    AdmitJudgeRun,
    ReadJudgeResponse,
}

impl StandingGoalTerminalEvaluationStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BuildJudgeInput => "build_judge_input",
            Self::PrepareJudgeRun => "prepare_judge_run",
            Self::LoadJudgeRun => "load_judge_run",
            Self::GrantJudgeAuthority => "grant_judge_authority",
            Self::PersistJudgeAuthority => "persist_judge_authority",
            Self::AdmitJudgeRun => "admit_judge_run",
            Self::ReadJudgeResponse => "read_judge_response",
        }
    }
}

impl std::fmt::Display for StandingGoalTerminalEvaluationStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
#[error("standing Goal terminal evaluator failed at {stage}: {message}")]
pub struct StandingGoalTerminalEvaluatorError {
    pub stage: StandingGoalTerminalEvaluationStage,
    pub message: String,
    pub retryable: bool,
    pub runtime_code: Option<ErrorCode>,
    pub correlation_id: Option<musteragent_identity::CorrelationId>,
}

pub trait StandingGoalTerminalEvaluator: Send + Sync {
    fn evaluate(
        &self,
        request: StandingGoalTerminalEvaluationRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<StandingGoalTerminalEvaluationOutcome, StandingGoalTerminalEvaluatorError>,
    >;
}

/// Safe baseline evaluator used until a deployment composes an independent
/// evidence verifier/Judge. It deliberately accepts only non-terminal Agent
/// proposals. Agent-authored `Achieved`/`Failed` claims are converted into a
/// continuation. Operational failures are retried outside semantic Goal
/// state and are never interpreted as an evaluator verdict.
#[derive(Debug, Default)]
pub struct FailClosedStandingGoalTerminalEvaluator;

impl StandingGoalTerminalEvaluator for FailClosedStandingGoalTerminalEvaluator {
    fn evaluate(
        &self,
        request: StandingGoalTerminalEvaluationRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<StandingGoalTerminalEvaluationOutcome, StandingGoalTerminalEvaluatorError>,
    > {
        Box::pin(async move {
            Ok(StandingGoalTerminalEvaluationOutcome::Decided(Box::new(
                fail_closed_terminal_evaluation(request),
            )))
        })
    }
}

pub struct StandingGoalSupervisor {
    worker_id: String,
    goals: Arc<StandingGoalProductApplication>,
    runtime: Arc<dyn StandingGoalAdmission>,
    authority: Arc<dyn StandingGoalContinuationAuthority>,
    terminal_evaluator: Arc<dyn StandingGoalTerminalEvaluator>,
    dialogue_admission: Option<Arc<dyn crate::OwnerGoalContinuationAdmission>>,
    clock: Arc<dyn RuntimeClock>,
}

impl StandingGoalSupervisor {
    #[must_use]
    pub fn new(
        worker_id: String,
        goals: Arc<StandingGoalProductApplication>,
        runtime: Arc<dyn StandingGoalAdmission>,
        authority: Arc<dyn StandingGoalContinuationAuthority>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            worker_id,
            goals,
            runtime,
            authority,
            terminal_evaluator: Arc::new(FailClosedStandingGoalTerminalEvaluator),
            dialogue_admission: None,
            clock,
        }
    }

    #[must_use]
    pub fn with_terminal_evaluator(
        mut self,
        terminal_evaluator: Arc<dyn StandingGoalTerminalEvaluator>,
    ) -> Self {
        self.terminal_evaluator = terminal_evaluator;
        self
    }

    #[must_use]
    pub fn with_dialogue_admission(
        mut self,
        admission: Arc<dyn crate::OwnerGoalContinuationAdmission>,
    ) -> Self {
        self.dialogue_admission = Some(admission);
        self
    }

    pub async fn run(&self, actor: Actor, cancellation: CancellationToken) {
        run_loop::run(cancellation, || self.run_pass(actor)).await;
    }

    pub async fn run_pass(&self, actor: Actor) -> Result<usize, StandingGoalSupervisorError> {
        let retired = self
            .goals
            .retire_completion_review_page(actor.tenant_id, CLAIM_LIMIT, self.clock.now())
            .await?;
        let event_woken = self
            .goals
            .wake_event_waits(WakeEventStandingGoalWaits {
                tenant_id: actor.tenant_id,
                now: self.clock.now(),
                limit: CLAIM_LIMIT,
            })
            .await?;
        let woken = self
            .goals
            .wake_due_waits(WakeDueStandingGoalWaits {
                tenant_id: actor.tenant_id,
                now: self.clock.now(),
                limit: CLAIM_LIMIT,
            })
            .await?;
        let evaluation = self.evaluate_terminal_runs(actor).await?;
        let admission = self.admit_due_continuations(actor).await?;
        Ok(usize::from(retired.retired)
            .saturating_add(usize::from(event_woken))
            .saturating_add(usize::from(woken))
            .saturating_add(evaluation)
            .saturating_add(admission))
    }

    async fn evaluate_terminal_runs(
        &self,
        actor: Actor,
    ) -> Result<usize, StandingGoalSupervisorError> {
        let now = self.clock.now();
        let claims = self
            .goals
            .claim_terminal_evaluations(ClaimStandingGoalEvaluations {
                tenant_id: actor.tenant_id,
                worker_id: format!("{}:evaluation", self.worker_id),
                now,
                lease_millis: CLAIM_LEASE_MILLIS,
                limit: CLAIM_LIMIT,
            })
            .await?;
        let count = claims.len();
        for claim in claims {
            let goal_id = claim.inspection.goal.goal_id;
            if let Err(error) = self.evaluate_terminal_run(claim).await {
                tracing::warn!(%goal_id, %error, "standing Goal terminal Run evaluation deferred");
            }
        }
        Ok(count)
    }

    async fn evaluate_terminal_run(
        &self,
        claim: ClaimedStandingGoalEvaluation,
    ) -> Result<(), StandingGoalSupervisorError> {
        // The asynchronous claim may observe a Pursuit committed after the
        // claim's clock sample. Never use query-start time to settle that state;
        // each item also needs its own observation after earlier batch work.
        let observed_at = self.clock.now();
        let pursuit = claim
            .inspection
            .pursuit
            .as_ref()
            .ok_or(StandingGoalSupervisorError::Corrupt)?;
        let run_id = pursuit
            .active_run_id
            .ok_or(StandingGoalSupervisorError::Corrupt)?;
        let tenant_id = claim.inspection.goal.tenant_id;
        let claim_token = claim.claim_token.clone();
        // Deterministic execution failures never require another paid Judge.
        if claim.run.status != GoalRunTerminalStatus::Completed {
            self.goals
                .block_execution(
                    &claim,
                    evaluation_id(run_id)?,
                    claim.run.failure_code,
                    observed_at,
                )
                .await?;
            return Ok(());
        }
        let evaluation_request = StandingGoalTerminalEvaluationRequest {
            owner_goal_scope: claim.owner_goal_scope,
            claim_token: claim.claim_token.clone(),
            goal: claim
                .processing_rules
                .as_ref()
                .map(|rules| rules.execution_view(&claim.inspection.goal))
                .unwrap_or_else(|| claim.inspection.goal.clone()),
            pursuit: pursuit.clone(),
            run: claim.run,
            proposal: claim.proposal.clone(),
            proposal_summary: claim.proposal_summary.clone(),
            source_execution_grant_proof: claim.source_execution_grant_proof.clone(),
            continuation_authority: claim.continuation_authority.clone(),
            observed_at,
        };
        let outcome = self.terminal_evaluator.evaluate(evaluation_request).await;
        // Evaluation may perform slow IO. Settlement/lease checks, active-time
        // accounting and retry backoff all start at this new boundary, not at
        // the original observation. The store still owns the exact claim fence.
        let now = self.clock.now();
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if !error.retryable {
                    self.goals
                        .block_execution(&claim, evaluation_id(run_id)?, error.runtime_code, now)
                        .await?;
                    return Ok(());
                }
                // Transient private evaluation outages retain the same claim
                // lineage; they do not admit another execution Run.
                tracing::warn!(
                    goal_id = %claim.inspection.goal.goal_id,
                    pursuit_id = %pursuit.pursuit_id,
                    %run_id,
                    stage = %error.stage,
                    retryable = error.retryable,
                    runtime_code = ?error.runtime_code,
                    correlation_id = ?error.correlation_id,
                    evaluator_message = %error.message,
                    "standing Goal private terminal evaluation deferred"
                );
                let retry_millis = EVALUATION_PENDING_RECHECK_MILLIS;
                self.defer_terminal_evaluation(
                    tenant_id,
                    pursuit.pursuit_id,
                    run_id,
                    &claim_token,
                    now,
                    retry_millis,
                    Some(StandingGoalEvaluationFailure {
                        stage: error.stage.to_string(),
                        retryable: error.retryable,
                        runtime_code: error.runtime_code.map(|code| code.as_str().to_owned()),
                        correlation_id: error.correlation_id.map(|id| id.to_string()),
                        occurred_at: now,
                    }),
                )
                .await?;
                return Ok(());
            }
        };
        let decision = match outcome {
            StandingGoalTerminalEvaluationOutcome::Pending
            | StandingGoalTerminalEvaluationOutcome::Superseded => {
                tracing::trace!(
                    goal_id = %claim.inspection.goal.goal_id,
                    pursuit_id = %pursuit.pursuit_id,
                    %run_id,
                    "standing Goal private terminal evaluation will recheck its claim"
                );
                self.defer_terminal_evaluation(
                    tenant_id,
                    pursuit.pursuit_id,
                    run_id,
                    &claim_token,
                    now,
                    EVALUATION_PENDING_RECHECK_MILLIS,
                    None,
                )
                .await?;
                return Ok(());
            }
            StandingGoalTerminalEvaluationOutcome::Decided(decision) => *decision,
        };
        let risk = claim
            .processing_rules
            .as_ref()
            .map(|rules| rules.completion.risk_class)
            .unwrap_or(claim.inspection.goal.completion.risk_class);
        if requires_completion_evidence(risk, &decision) {
            self.goals
                .block_completion_evidence(&claim, evaluation_id(run_id)?, decision.evidence, now)
                .await?;
            return Ok(());
        }
        self.goals
            .evaluate_run(EvaluateStandingGoalRun {
                tenant_id: claim.inspection.goal.tenant_id,
                goal_id: claim.inspection.goal.goal_id,
                pursuit_id: pursuit.pursuit_id,
                run_id,
                claim_token: claim.claim_token,
                evaluation_id: evaluation_id(run_id)?,
                verdict: decision.verdict,
                evidence: decision.evidence,
                summary: decision.summary,
                deterministic: decision.deterministic,
                completion_confirmation_required: decision.completion_confirmation_required,
                wait_barrier: decision.wait_barrier,
                continuation_due_at: decision.continuation_due_at,
                usage_delta: claim.run.usage,
                occurred_at: now,
            })
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn defer_terminal_evaluation(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
        run_id: RunId,
        claim_token: &str,
        now: UtcTimestamp,
        delay_millis: i64,
        failure: Option<StandingGoalEvaluationFailure>,
    ) -> Result<(), StandingGoalSupervisorError> {
        let retry_at = now
            .checked_add_millis(delay_millis)
            .ok_or(StandingGoalSupervisorError::Corrupt)?;
        self.goals
            .defer_terminal_evaluation(DeferStandingGoalEvaluation {
                tenant_id,
                pursuit_id,
                run_id,
                claim_token: claim_token.to_owned(),
                retry_at,
                failure,
            })
            .await?;
        Ok(())
    }

    async fn admit_due_continuations(
        &self,
        actor: Actor,
    ) -> Result<usize, StandingGoalSupervisorError> {
        let now = self.clock.now();
        let claims = self
            .goals
            .claim_due(ClaimStandingGoalPursuits {
                tenant_id: actor.tenant_id,
                worker_id: format!("{}:admission", self.worker_id),
                now,
                lease_millis: CLAIM_LEASE_MILLIS,
                limit: CLAIM_LIMIT,
            })
            .await?;
        let count = claims.len();
        for claim in claims {
            let goal_id = claim.inspection.goal.goal_id;
            let admission = self.admit_due_continuation(actor, &claim).await;
            if let Some(disposition) = continuation_settlement(&admission) {
                if disposition != AdmissionFailureDisposition::Yield {
                    let pursuit = claim
                        .inspection
                        .pursuit
                        .as_ref()
                        .ok_or(StandingGoalSupervisorError::Corrupt)?;
                    let occurred_at = self.clock.now();
                    let retry_at = retry_at(
                        occurred_at,
                        pursuit.budget_usage.consecutive_failures.saturating_add(1),
                    );
                    let settlement = if matches!(
                        disposition,
                        AdmissionFailureDisposition::Suspend
                            | AdmissionFailureDisposition::Reprepare
                    ) {
                        // A known version race is not a failed Goal attempt.
                        // Return only our still-current claim, then prepare fresh
                        // authority after a bounded delay instead of waiting for
                        // the five-minute crash-recovery lease to expire.
                        let retry_at = if disposition == AdmissionFailureDisposition::Reprepare {
                            occurred_at
                                .checked_add_millis(PREPARATION_CONFLICT_RETRY_MILLIS)
                                .ok_or(StandingGoalSupervisorError::Corrupt)?
                        } else {
                            occurred_at
                        };
                        self.goals
                            .defer_admission(DeferStandingGoalAdmission {
                                tenant_id: actor.tenant_id,
                                goal_id,
                                pursuit_id: pursuit.pursuit_id,
                                claim_token: claim.claim_token.clone(),
                                retry_at,
                                occurred_at,
                            })
                            .await
                    } else {
                        self.goals
                            .record_admission_failure(RecordStandingGoalAdmissionFailure {
                                tenant_id: actor.tenant_id,
                                goal_id,
                                pursuit_id: pursuit.pursuit_id,
                                claim_token: claim.claim_token.clone(),
                                permanent: disposition == AdmissionFailureDisposition::Block,
                                retry_at: (disposition == AdmissionFailureDisposition::Retry)
                                    .then_some(retry_at),
                                occurred_at,
                            })
                            .await
                    };
                    if let Err(settlement_error) = settlement {
                        tracing::warn!(%goal_id, %settlement_error, "standing Goal admission failure could not be settled");
                    }
                }
                if let Err(error) = admission {
                    tracing::warn!(%goal_id, %error, "standing Goal continuation admission deferred");
                } else {
                    tracing::info!(goal_id = %goal_id, goal.admission.stage = "commit", "standing Goal admission superseded; requested fresh preparation");
                }
            }
        }
        Ok(count)
    }

    async fn admit_due_continuation(
        &self,
        actor: Actor,
        claim: &ClaimedStandingGoalPursuit,
    ) -> Result<StandingGoalAdmissionOutcome, StandingGoalSupervisorError> {
        let goal = &claim.inspection.goal;
        let pursuit = claim
            .inspection
            .pursuit
            .as_ref()
            .ok_or(StandingGoalSupervisorError::Corrupt)?;
        let (source_run_id, source_execution_grant_proof, continuation_authority) =
            match &claim.source {
                crate::StandingGoalPursuitSource::TerminalRun {
                    source_run_id,
                    source_execution_grant_proof,
                    continuation_authority,
                } => (
                    *source_run_id,
                    source_execution_grant_proof,
                    continuation_authority,
                ),
                crate::StandingGoalPursuitSource::OwnerGoal(scope) => {
                    if scope.tenant_id != goal.tenant_id
                        || scope.owner_id != goal.owner
                        || scope.goal_id != goal.goal_id
                        || scope.conversation_id() != goal.conversation_id
                        || scope.agent_id
                            != claim
                                .processing_rules
                                .as_ref()
                                .map_or(goal.target_agent_id, |rules| rules.target_agent_id)
                    {
                        return Err(StandingGoalSupervisorError::Corrupt);
                    }
                    return self
                        .dialogue_admission
                        .as_ref()
                        .ok_or(StandingGoalSupervisorError::Corrupt)?
                        .admit(crate::OwnerGoalContinuationRequest {
                            phase: crate::OwnerGoalAdmissionPhase::Execution,
                            scope: *scope,
                            pursuit_id: pursuit.pursuit_id,
                            fence: StandingGoalAdmissionFence {
                                goal_revision: goal.revision,
                                pursuit_revision: pursuit.revision,
                                claim_token: claim.claim_token.clone(),
                            },
                        })
                        .await
                        .map_err(Into::into);
                }
            };
        let command_id = continuation_command_id(pursuit.pursuit_id, source_run_id)?;
        let deadline = self
            .clock
            .now()
            .checked_add_millis(ADMISSION_DEADLINE_MILLIS)
            .ok_or(StandingGoalSupervisorError::Corrupt)?;
        let preparation = self
            .runtime
            .prepare_standing_goal_continuation(
                Actor {
                    tenant_id: goal.tenant_id,
                    principal_id: goal.owner,
                    principal_kind: actor.principal_kind,
                },
                goal.conversation_id,
                source_run_id,
                goal.goal_id,
                pursuit.pursuit_id,
                StandingGoalAdmissionFence {
                    goal_revision: goal.revision,
                    pursuit_revision: pursuit.revision,
                    claim_token: claim.claim_token.clone(),
                },
                claim
                    .processing_rules
                    .as_ref()
                    .map(|rules| rules.objective_ref.clone())
                    .unwrap_or_else(|| goal.objective_ref.clone()),
                claim
                    .inspection
                    .latest_evaluation
                    .as_ref()
                    .map(|evaluation| evaluation.summary_ref.clone()),
                claim.input_refs.clone(),
                command_id,
                deadline,
                trace_context(command_id)?,
            )
            .await?;
        if preparation.already_admitted() {
            // Run and Pursuit committed together. A duplicate acknowledgement
            // needs neither fresh authority nor another product mutation.
            return Ok(StandingGoalAdmissionOutcome::Admitted(
                preparation.replay_receipt(),
            ));
        }
        let workspace_id = match preparation.execution_scope() {
            ExecutionScope::Tenant { .. } => None,
            ExecutionScope::Workspace { workspace_id } => Some(workspace_id),
            ExecutionScope::DevicePrivate { workspace_id, .. } => workspace_id,
            // Placement metadata only; source proof retains complete project authority.
            ExecutionScope::HostedProject { scope } => Some(scope.workspace_id),
        };
        let grant = self
            .authority
            .grant(StandingGoalContinuationAuthorityRequest {
                purpose: StandingGoalAdmissionPurpose::Continuation,
                source_execution_grant_proof: continuation_authority
                    .is_none()
                    .then(|| source_execution_grant_proof.clone()),
                durable_continuation_authority: continuation_authority.clone(),
                conversation_id: goal.conversation_id,
                workspace_id,
                goal_id: goal.goal_id,
                pursuit_id: pursuit.pursuit_id,
                target_command_id: preparation.target_command_id(),
                target_run_id: preparation.target_run_id(),
            })
            .await?;
        self.authority.require_active_owner()?;
        // The admission transaction also binds Run and authority to the exact
        // Goal claim. Supersession acknowledges a no-op, not a Goal failure.
        preparation
            .admit(
                grant.signed_execution_grant,
                (!grant.durable_continuation_authority.is_empty())
                    .then_some(grant.durable_continuation_authority),
            )
            .await
            .map_err(Into::into)
    }
}

fn requires_completion_evidence(
    risk_class: GoalRiskClass,
    decision: &StandingGoalTerminalEvaluationDecision,
) -> bool {
    risk_class == GoalRiskClass::HighRisk
        && decision.verdict == GoalVerdict::Achieved
        && !decision.deterministic
}

pub(crate) fn fail_closed_terminal_evaluation(
    request: StandingGoalTerminalEvaluationRequest,
) -> StandingGoalTerminalEvaluationDecision {
    match (
        request.run.status,
        request.proposal,
        request.proposal_summary,
    ) {
        (GoalRunTerminalStatus::Completed, Some(proposal), Some(summary))
            if matches!(proposal.verdict, GoalVerdict::Continue | GoalVerdict::Waiting) =>
        {
            StandingGoalTerminalEvaluationDecision {
                verdict: proposal.verdict,
                evidence: proposal.evidence,
                summary,
                deterministic: false,
                completion_confirmation_required: false,
                wait_barrier: proposal.wait_barrier,
                continuation_due_at: None,
            }
        }
        (GoalRunTerminalStatus::Completed, Some(proposal), Some(_)) => {
            StandingGoalTerminalEvaluationDecision {
                verdict: GoalVerdict::Continue,
                evidence: Vec::new(),
                summary: format!(
                    "The Agent proposed {:?}, but no independent terminal verifier receipt was available. Continue from durable Goal state instead of accepting an Agent-authored terminal claim.",
                    proposal.verdict
                )
                .into_bytes(),
                deterministic: true,
                completion_confirmation_required: false,
                wait_barrier: None,
                continuation_due_at: Some(request.observed_at),
            }
        }
        (GoalRunTerminalStatus::Completed, None, None) => {
            StandingGoalTerminalEvaluationDecision {
                verdict: GoalVerdict::Continue,
                evidence: Vec::new(),
                summary: b"The bounded Goal Run completed without an outcome proposal; continue from the durable Runtime state."
                    .to_vec(),
                deterministic: true,
                completion_confirmation_required: false,
                wait_barrier: None,
                continuation_due_at: Some(request.observed_at),
            }
        }
        (status, _, _) if status != GoalRunTerminalStatus::Completed => {
            let consecutive_failures = request
                .pursuit
                .budget_usage
                .consecutive_failures
                .saturating_add(request.run.usage.consecutive_failures);
            StandingGoalTerminalEvaluationDecision {
                verdict: GoalVerdict::Continue,
                evidence: Vec::new(),
                summary: format!(
                    "The bounded Goal Run ended as {status:?}; retry from its durable terminal state."
                )
                .into_bytes(),
                deterministic: true,
                completion_confirmation_required: false,
                wait_barrier: None,
                continuation_due_at: Some(retry_at(request.observed_at, consecutive_failures)),
            }
        }
        _ => StandingGoalTerminalEvaluationDecision {
            verdict: GoalVerdict::Continue,
            evidence: Vec::new(),
            summary: b"The bounded Goal Run returned an inconsistent outcome proposal; continue without accepting it."
                .to_vec(),
            deterministic: true,
            completion_confirmation_required: false,
            wait_barrier: None,
            continuation_due_at: Some(request.observed_at),
        },
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AdmissionFailureDisposition {
    Suspend,
    Reprepare,
    Retry,
    Block,
    Yield,
}

/// A rolled-back commit may still own its lease (e.g. new dialogue context).
/// Only the fenced store can distinguish that case from a replaced owner.
fn continuation_settlement(
    result: &Result<StandingGoalAdmissionOutcome, StandingGoalSupervisorError>,
) -> Option<AdmissionFailureDisposition> {
    match result {
        Ok(StandingGoalAdmissionOutcome::Admitted(_)) => None,
        Ok(StandingGoalAdmissionOutcome::Superseded) => {
            Some(AdmissionFailureDisposition::Reprepare)
        }
        Err(error) => Some(admission_failure_disposition(error)),
    }
}

fn admission_failure_disposition(
    error: &StandingGoalSupervisorError,
) -> AdmissionFailureDisposition {
    match error {
        StandingGoalSupervisorError::Authority(error)
            if error.kind == StandingGoalContinuationAuthorityFailure::Suspended =>
        {
            AdmissionFailureDisposition::Suspend
        }
        StandingGoalSupervisorError::Authority(error)
            if error.retryable
                && error.kind
                    == StandingGoalContinuationAuthorityFailure::DependencyUnavailable =>
        {
            AdmissionFailureDisposition::Retry
        }
        StandingGoalSupervisorError::Runtime(error)
            if error.retryable
                && matches!(
                    error.code,
                    ErrorCode::ConversationSeqConflict | ErrorCode::ResourceRevisionConflict
                ) =>
        {
            AdmissionFailureDisposition::Reprepare
        }
        StandingGoalSupervisorError::Runtime(error)
            if error.retryable
                && matches!(
                    error.code,
                    ErrorCode::DependencyUnavailable
                        | ErrorCode::DependencyTimeout
                        | ErrorCode::CredentialRefreshRequired
                        | ErrorCode::ContentUnavailable
                ) =>
        {
            AdmissionFailureDisposition::Retry
        }
        StandingGoalSupervisorError::Authority(_) | StandingGoalSupervisorError::Runtime(_) => {
            AdmissionFailureDisposition::Block
        }
        // No mutation is safe when the ownership/store contract itself failed.
        StandingGoalSupervisorError::Product(_) | StandingGoalSupervisorError::Corrupt => {
            AdmissionFailureDisposition::Yield
        }
    }
}

fn retry_at(now: UtcTimestamp, consecutive_failures: u32) -> UtcTimestamp {
    let exponent = consecutive_failures.saturating_sub(1).min(16);
    let delay = FAILURE_BACKOFF_BASE_MILLIS
        .saturating_mul(1_i64 << exponent)
        .min(FAILURE_BACKOFF_MAX_MILLIS);
    UtcTimestamp::from_unix_millis(now.unix_millis().saturating_add(delay))
}

fn evaluation_id(run_id: RunId) -> Result<GoalEvaluationId, StandingGoalSupervisorError> {
    // These domains are frozen retry identities for pursuits admitted before
    // and after the Product rename; the bytes are never exposed as a schema.
    GoalEvaluationId::from_uuid(derived_uuid(
        b"musteragent.legacy.standing-goal.evaluation.v1\0",
        run_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| StandingGoalSupervisorError::Corrupt)
}

fn continuation_command_id(
    pursuit_id: GoalPursuitId,
    source_run_id: RunId,
) -> Result<CommandId, StandingGoalSupervisorError> {
    let mut material = pursuit_id.into_uuid().as_bytes().to_vec();
    material.extend_from_slice(source_run_id.into_uuid().as_bytes());
    CommandId::from_uuid(derived_uuid(
        b"musteragent.legacy.standing-goal.continuation.v1\0",
        &material,
    ))
    .map_err(|_| StandingGoalSupervisorError::Corrupt)
}

fn derived_uuid(domain: &[u8], material: &[u8]) -> uuid::Uuid {
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(material);
    let digest = Sha256::digest(bytes).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

fn trace_context(command_id: CommandId) -> Result<TraceContext, StandingGoalSupervisorError> {
    let digest = Sha256::digest(command_id.into_uuid().as_bytes()).to_string();
    Ok(TraceContext {
        traceparent: TraceParent::new(format!("00-{}-{}-01", &digest[..32], &digest[32..48]))
            .map_err(|_| StandingGoalSupervisorError::Corrupt)?,
        tracestate: None,
    })
}

#[derive(Debug, Error)]
pub enum StandingGoalSupervisorError {
    #[error("standing Goal product operation failed: {0}")]
    Product(#[from] StandingGoalProductError),
    #[error("standing Goal Runtime admission failed: {0:?}")]
    Runtime(RuntimeError),
    #[error("standing Goal continuation authority failed: {0}")]
    Authority(#[from] StandingGoalContinuationAuthorityError),
    #[error("standing Goal durable state is corrupt")]
    Corrupt,
}

impl From<RuntimeError> for StandingGoalSupervisorError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

fn supervisor_retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(16);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let millis = u64::try_from(ERROR_RETRY_BASE_INTERVAL.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(multiplier)
        .min(u64::try_from(ERROR_RETRY_MAX_INTERVAL.as_millis()).unwrap_or(u64::MAX));
    Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use musteragent_goal::{GoalRiskClass, GoalVerdict};
    use musteragent_identity::{GoalPursuitId, RunId, UtcTimestamp};

    use super::{
        StandingGoalContinuationAuthorityError, StandingGoalContinuationAuthorityFailure,
        StandingGoalSupervisorError, StandingGoalTerminalEvaluationDecision,
        admission_failure_disposition, continuation_command_id, requires_completion_evidence,
        retry_at, supervisor_retry_delay,
    };

    #[test]
    fn continuation_identity_is_stable_for_one_source_run() {
        let pursuit = GoalPursuitId::generate();
        let source = RunId::generate();
        assert_eq!(
            continuation_command_id(pursuit, source).expect("stable continuation command ID"),
            continuation_command_id(pursuit, source).expect("stable continuation command ID")
        );
        assert_ne!(
            continuation_command_id(pursuit, source).expect("source continuation command ID"),
            continuation_command_id(pursuit, RunId::generate())
                .expect("distinct continuation command ID")
        );
    }

    #[test]
    fn failure_backoff_is_bounded() {
        let now = UtcTimestamp::from_unix_millis(1_000);
        assert_eq!(retry_at(now, 1).unix_millis(), 2_000);
        assert_eq!(retry_at(now, 64).unix_millis(), 301_000);
    }

    #[test]
    fn supervisor_pass_failure_backoff_prevents_a_hot_retry_loop() {
        assert_eq!(supervisor_retry_delay(1), std::time::Duration::from_secs(1));
        assert_eq!(supervisor_retry_delay(2), std::time::Duration::from_secs(2));
        assert_eq!(
            supervisor_retry_delay(7),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            supervisor_retry_delay(u32::MAX),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn admission_recovery_is_closed_even_with_an_incorrect_retry_flag() {
        use super::AdmissionFailureDisposition::{Block, Retry, Suspend};
        for (kind, retryable, expected) in [
            (
                StandingGoalContinuationAuthorityFailure::Suspended,
                true,
                Suspend,
            ),
            (
                StandingGoalContinuationAuthorityFailure::Suspended,
                false,
                Suspend,
            ),
            (
                StandingGoalContinuationAuthorityFailure::DependencyUnavailable,
                true,
                Retry,
            ),
            (
                StandingGoalContinuationAuthorityFailure::DependencyUnavailable,
                false,
                Block,
            ),
            (
                StandingGoalContinuationAuthorityFailure::Denied,
                true,
                Block,
            ),
            (
                StandingGoalContinuationAuthorityFailure::Denied,
                false,
                Block,
            ),
            (
                StandingGoalContinuationAuthorityFailure::InvalidRequest,
                true,
                Block,
            ),
        ] {
            let error =
                StandingGoalSupervisorError::Authority(StandingGoalContinuationAuthorityError {
                    runtime_code: None,
                    correlation_id: None,
                    kind,
                    retryable,
                });
            assert_eq!(admission_failure_disposition(&error), expected);
        }
    }

    #[test]
    fn only_retryable_version_races_release_the_claim_for_fresh_preparation() {
        use super::AdmissionFailureDisposition::{Block, Reprepare, Yield};
        use crate::runtime_error;
        use musteragent_runtime_contract::ErrorCode;
        for code in [
            ErrorCode::ConversationSeqConflict,
            ErrorCode::ResourceRevisionConflict,
        ] {
            for (retryable, expected) in [(true, Reprepare), (false, Block)] {
                let error = StandingGoalSupervisorError::Runtime(runtime_error(
                    code,
                    musteragent_identity::CorrelationId::generate(),
                    retryable,
                    "Source revision changed",
                ));
                assert_eq!(admission_failure_disposition(&error), expected);
            }
        }
        assert_eq!(
            super::continuation_settlement(&Ok(super::StandingGoalAdmissionOutcome::Superseded)),
            Some(Reprepare)
        );
        // Unknown store/ownership failures must still leave the claim alone.
        assert_eq!(
            admission_failure_disposition(&StandingGoalSupervisorError::Corrupt),
            Yield
        );
    }

    #[test]
    fn high_risk_goal_keeps_deterministic_evidence_boundary_without_human_gate() {
        let mut decision = StandingGoalTerminalEvaluationDecision {
            verdict: GoalVerdict::Achieved,
            evidence: Vec::new(),
            summary: b"semantic completion".to_vec(),
            deterministic: false,
            completion_confirmation_required: false,
            wait_barrier: None,
            continuation_due_at: None,
        };
        assert!(requires_completion_evidence(
            GoalRiskClass::HighRisk,
            &decision
        ));
        decision.deterministic = true;
        assert!(!requires_completion_evidence(
            GoalRiskClass::HighRisk,
            &decision
        ));
        decision.verdict = GoalVerdict::Continue;
        decision.deterministic = false;
        assert!(!requires_completion_evidence(
            GoalRiskClass::HighRisk,
            &decision
        ));
    }
}
