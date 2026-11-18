//! Durable standing-goal contracts.
//!
//! A standing Goal is not a long-lived Run and is not a schedule. It owns a
//! sequence of bounded Pursuits, each of which may admit one Run at a time,
//! wait on a typed barrier, and resume after process restart. Completion is an
//! evidence-backed Evaluation; an Agent can propose evidence but cannot write
//! `Achieved` directly.

use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    AgentId, ConversationId, GoalEvaluationId, GoalId, GoalPursuitId, InteractionId, PrincipalId,
    RunId, TenantId, UtcTimestamp,
};
use thiserror::Error;

pub const MAX_STANDING_GOAL_TITLE_BYTES: usize = 240;
pub const MAX_STANDING_GOAL_CONSTRAINT_BYTES: usize = 1_000;
pub const MAX_STANDING_GOAL_CRITERIA: usize = 64;
pub const MAX_STANDING_GOAL_EVIDENCE: usize = 128;
pub const MAX_STANDING_GOAL_RUNS: u32 = 10_000;
pub const MAX_STANDING_GOAL_ELAPSED_MILLIS: u64 = 365 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalOrigin {
    User,
    Agent,
}

/// Finite Goals end when their completion contract is satisfied. Continuous
/// Goals satisfy that same contract for each accepted batch and then wait for
/// another durable input; a successful batch never ends the subscription.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum StandingGoalMode {
    #[default]
    Finite,
    Continuous,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalStatus {
    Draft,
    Active,
    Paused,
    Achieved,
    Failed,
    Cancelled,
}

impl StandingGoalStatus {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Achieved | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalPursuitStatus {
    Pending,
    Running,
    Waiting,
    Paused,
    Achieved,
    Failed,
    Cancelled,
}

impl GoalPursuitStatus {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Achieved | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalWaitKind {
    UserInput,
    Approval,
    ExternalDependency,
    RetryBackoff,
    /// A semantic Judge has accepted sufficient evidence for a HighRisk Goal,
    /// but only the owning principal may commit the terminal Evaluation.
    CompletionConfirmation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalWaitBarrier {
    pub kind: GoalWaitKind,
    pub correlation_key: String,
    pub reason_ref: ContentRef,
    pub resume_not_before: Option<UtcTimestamp>,
    pub expires_at: Option<UtcTimestamp>,
}

impl GoalWaitBarrier {
    pub fn validate(&self) -> Result<(), StandingGoalContractError> {
        bounded(&self.correlation_key, 512)?;
        match self.kind {
            GoalWaitKind::UserInput | GoalWaitKind::Approval => {
                if self.correlation_key.parse::<InteractionId>().is_err()
                    || self.resume_not_before.is_some()
                {
                    return Err(StandingGoalContractError::InvalidWaitBarrier);
                }
            }
            GoalWaitKind::ExternalDependency => {
                if self.resume_not_before.is_some() {
                    return Err(StandingGoalContractError::InvalidWaitBarrier);
                }
            }
            GoalWaitKind::RetryBackoff => {
                if self.resume_not_before.is_none() {
                    return Err(StandingGoalContractError::InvalidWaitBarrier);
                }
            }
            GoalWaitKind::CompletionConfirmation => {
                if !valid_completion_confirmation_correlation(&self.correlation_key)
                    || self.resume_not_before.is_some()
                    || self.expires_at.is_some()
                {
                    return Err(StandingGoalContractError::InvalidWaitBarrier);
                }
            }
        }
        if self
            .expires_at
            .zip(self.resume_not_before)
            .is_some_and(|(expires, resume)| expires < resume)
        {
            return Err(StandingGoalContractError::InvalidWaitBarrier);
        }
        Ok(())
    }
}

fn valid_completion_confirmation_correlation(value: &str) -> bool {
    let mut parts = value.split(':');
    parts.next() == Some("standing-goal-completion")
        && parts
            .next()
            .is_some_and(|part| part.parse::<GoalId>().is_ok())
        && parts
            .next()
            .is_some_and(|part| part.parse::<GoalPursuitId>().is_ok())
        && parts
            .next()
            .is_some_and(|part| part.parse::<RunId>().is_ok())
        && parts.next().is_none()
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GoalBudget {
    pub max_runs: u32,
    pub max_elapsed_millis: u64,
    pub max_model_tokens: u64,
    pub max_tool_invocations: u64,
    pub max_consecutive_failures: u32,
}

impl GoalBudget {
    pub fn validate(self) -> Result<(), StandingGoalContractError> {
        if self.max_runs == 0
            || self.max_runs > MAX_STANDING_GOAL_RUNS
            || self.max_elapsed_millis == 0
            || self.max_elapsed_millis > MAX_STANDING_GOAL_ELAPSED_MILLIS
            || self.max_model_tokens == 0
            || self.max_tool_invocations == 0
            || self.max_consecutive_failures == 0
            || self.max_consecutive_failures > self.max_runs
        {
            return Err(StandingGoalContractError::InvalidBudget);
        }
        Ok(())
    }
}

impl Default for GoalBudget {
    fn default() -> Self {
        Self {
            // Deprecated wire/storage compatibility sentinels. Standing Goals
            // never use these values to stop a Pursuit; cumulative usage is
            // observability only.
            max_runs: MAX_STANDING_GOAL_RUNS,
            max_elapsed_millis: MAX_STANDING_GOAL_ELAPSED_MILLIS,
            max_model_tokens: u64::MAX,
            max_tool_invocations: u64::MAX,
            max_consecutive_failures: MAX_STANDING_GOAL_RUNS,
        }
    }
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct GoalBudgetUsage {
    pub runs: u32,
    pub elapsed_millis: u64,
    pub model_tokens: u64,
    pub tool_invocations: u64,
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalCompletionContract {
    /// Product risk classification controls who may authoritatively declare
    /// completion. A semantic Judge may complete a Standard Goal, while a
    /// HighRisk Goal requires a deterministic verifier or explicit human
    /// confirmation recorded as a deterministic evaluation.
    pub risk_class: GoalRiskClass,
    pub success_criteria: Vec<String>,
    pub evidence_requirements: Vec<String>,
    pub failure_conditions: Vec<String>,
    pub minimum_evidence_items: u16,
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum GoalRiskClass {
    #[default]
    Standard,
    HighRisk,
}

impl GoalCompletionContract {
    pub fn validate(&self) -> Result<(), StandingGoalContractError> {
        if self.success_criteria.is_empty()
            || self.success_criteria.len() > MAX_STANDING_GOAL_CRITERIA
            || self.evidence_requirements.len() > MAX_STANDING_GOAL_CRITERIA
            || self.failure_conditions.len() > MAX_STANDING_GOAL_CRITERIA
            || usize::from(self.minimum_evidence_items) > MAX_STANDING_GOAL_EVIDENCE
        {
            return Err(StandingGoalContractError::InvalidCompletionContract);
        }
        for value in self
            .success_criteria
            .iter()
            .chain(&self.evidence_requirements)
            .chain(&self.failure_conditions)
        {
            bounded(value, MAX_STANDING_GOAL_CONSTRAINT_BYTES)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoal {
    pub input_progress: crate::GoalInputProgress,
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub revision: u64,
    pub owner: PrincipalId,
    pub origin: GoalOrigin,
    pub mode: StandingGoalMode,
    pub creator_agent_id: Option<AgentId>,
    pub target_agent_id: AgentId,
    pub conversation_id: ConversationId,
    pub title: String,
    pub objective_ref: ContentRef,
    pub constraints: Vec<String>,
    pub completion: GoalCompletionContract,
    pub budget: GoalBudget,
    pub status: StandingGoalStatus,
    pub active_pursuit_id: Option<GoalPursuitId>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl StandingGoal {
    /// Woken only by the Goal's durable input owner. Provider signals cannot
    /// masquerade as an accepted business input.
    #[must_use]
    pub fn input_wait(&self, reason_ref: ContentRef) -> GoalWaitBarrier {
        GoalWaitBarrier {
            kind: GoalWaitKind::ExternalDependency,
            correlation_key: self.input_wait_correlation(),
            reason_ref,
            resume_not_before: None,
            expires_at: None,
        }
    }

    pub fn input_wait_correlation(&self) -> String {
        format!("standing-goal-input:{}", self.goal_id)
    }

    /// Pausing a continuous Goal closes input admission but preserves settlement
    /// of its accepted work. Continuation admission additionally requires a
    /// nonempty durable input cursor; this predicate alone is not a Run grant.
    pub fn allows_existing_work(&self) -> bool {
        self.status == StandingGoalStatus::Active
            || (self.mode == StandingGoalMode::Continuous
                && self.status == StandingGoalStatus::Paused)
    }

    pub fn validate(&self) -> Result<(), StandingGoalContractError> {
        if self.revision == 0 {
            return Err(StandingGoalContractError::InvalidIdentity);
        }
        self.input_progress.validate()?;
        crate::processing_rules::validate_fields(&self.title, &self.constraints, &self.completion)?;
        self.budget.validate()?;
        if self.mode == StandingGoalMode::Continuous && self.status == StandingGoalStatus::Achieved
        {
            return Err(StandingGoalContractError::InvalidTransition);
        }
        if (self.origin == GoalOrigin::Agent) != self.creator_agent_id.is_some() {
            return Err(StandingGoalContractError::InvalidOrigin);
        }
        if self.status == StandingGoalStatus::Active && self.active_pursuit_id.is_none() {
            return Err(StandingGoalContractError::InvalidPursuitLink);
        }
        if self.status != StandingGoalStatus::Active && self.active_pursuit_id.is_some() {
            return Err(StandingGoalContractError::InvalidPursuitLink);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalPursuit {
    pub tenant_id: TenantId,
    pub pursuit_id: GoalPursuitId,
    pub goal_id: GoalId,
    pub revision: u64,
    pub status: GoalPursuitStatus,
    /// The Run currently owned by this Pursuit. It is present only while the
    /// Pursuit is `Running` and must be cleared as soon as execution reaches a
    /// terminal boundary or the Goal is paused.
    pub active_run_id: Option<RunId>,
    /// The most recent execution Run whose terminal context can seed an
    /// autonomous continuation. Unlike `active_run_id`, this remains durable
    /// while the Pursuit is Pending or Paused.
    pub continuation_source_run_id: Option<RunId>,
    pub wait_barrier: Option<GoalWaitBarrier>,
    pub budget_usage: GoalBudgetUsage,
    pub continuation_due_at: Option<UtcTimestamp>,
    pub started_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub completed_at: Option<UtcTimestamp>,
}

impl GoalPursuit {
    pub fn validate(&self) -> Result<(), StandingGoalContractError> {
        if self.revision == 0 {
            return Err(StandingGoalContractError::InvalidIdentity);
        }
        match self.status {
            GoalPursuitStatus::Running if self.active_run_id.is_none() => {
                return Err(StandingGoalContractError::InvalidPursuitState);
            }
            GoalPursuitStatus::Waiting if self.wait_barrier.is_none() => {
                return Err(StandingGoalContractError::InvalidPursuitState);
            }
            status if status.terminal() && self.completed_at.is_none() => {
                return Err(StandingGoalContractError::InvalidPursuitState);
            }
            _ => {}
        }
        if self.status != GoalPursuitStatus::Running && self.active_run_id.is_some() {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        if self.status != GoalPursuitStatus::Waiting && self.wait_barrier.is_some() {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        if self.status.terminal() && self.continuation_due_at.is_some() {
            return Err(StandingGoalContractError::InvalidPursuitState);
        }
        if let Some(barrier) = &self.wait_barrier {
            barrier.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalVerdict {
    Continue,
    Waiting,
    Achieved,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalEvaluation {
    pub tenant_id: TenantId,
    pub evaluation_id: GoalEvaluationId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub run_id: Option<RunId>,
    pub verdict: GoalVerdict,
    pub evidence: Vec<ContentRef>,
    pub summary_ref: ContentRef,
    pub deterministic: bool,
    pub occurred_at: UtcTimestamp,
}

/// Agent-authored, non-authoritative outcome proposal for one bounded Goal
/// Run. The terminal evaluator may accept, override or reject it after the Run
/// reaches a durable terminal state; this record can never mutate Goal state
/// by itself.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GoalEvaluationProposal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub pursuit_id: GoalPursuitId,
    pub run_id: RunId,
    pub verdict: GoalVerdict,
    pub evidence: Vec<ContentRef>,
    pub summary_ref: ContentRef,
    pub wait_barrier: Option<GoalWaitBarrier>,
    pub proposed_at: UtcTimestamp,
}

impl GoalEvaluationProposal {
    pub fn validate(
        &self,
        completion: &GoalCompletionContract,
    ) -> Result<(), StandingGoalContractError> {
        if self.evidence.len() > MAX_STANDING_GOAL_EVIDENCE
            || (self.verdict == GoalVerdict::Achieved
                && self.evidence.len() < usize::from(completion.minimum_evidence_items))
            || (self.verdict == GoalVerdict::Waiting) != self.wait_barrier.is_some()
        {
            return Err(StandingGoalContractError::InvalidEvaluation);
        }
        if let Some(barrier) = &self.wait_barrier {
            barrier.validate()?;
        }
        Ok(())
    }
}

impl GoalEvaluation {
    pub fn validate(
        &self,
        completion: &GoalCompletionContract,
    ) -> Result<(), StandingGoalContractError> {
        if self.evidence.len() > MAX_STANDING_GOAL_EVIDENCE {
            return Err(StandingGoalContractError::InvalidEvaluation);
        }
        if self.verdict == GoalVerdict::Achieved
            && self.evidence.len() < usize::from(completion.minimum_evidence_items)
        {
            return Err(StandingGoalContractError::InsufficientEvidence);
        }
        Ok(())
    }
}

pub fn validate_goal_transition(
    from: StandingGoalStatus,
    to: StandingGoalStatus,
) -> Result<(), StandingGoalContractError> {
    let valid = from == to
        || matches!(
            (from, to),
            (StandingGoalStatus::Draft, StandingGoalStatus::Active)
                | (StandingGoalStatus::Draft, StandingGoalStatus::Cancelled)
                | (StandingGoalStatus::Active, StandingGoalStatus::Paused)
                | (StandingGoalStatus::Active, StandingGoalStatus::Achieved)
                | (StandingGoalStatus::Active, StandingGoalStatus::Failed)
                | (StandingGoalStatus::Active, StandingGoalStatus::Cancelled)
                | (StandingGoalStatus::Paused, StandingGoalStatus::Active)
                | (StandingGoalStatus::Paused, StandingGoalStatus::Cancelled)
        );
    if valid {
        Ok(())
    } else {
        Err(StandingGoalContractError::InvalidTransition)
    }
}

pub(crate) fn bounded(value: &str, maximum: usize) -> Result<(), StandingGoalContractError> {
    if value.is_empty() || value.len() > maximum || value.contains('\0') {
        Err(StandingGoalContractError::InvalidText)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalContractError {
    #[error("standing Goal input progress is invalid")]
    InvalidInputCursor,
    #[error("standing Goal identity is invalid")]
    InvalidIdentity,
    #[error("standing Goal text is invalid")]
    InvalidText,
    #[error("standing Goal origin is invalid")]
    InvalidOrigin,
    #[error("standing Goal completion contract is invalid")]
    InvalidCompletionContract,
    #[error("standing Goal budget is invalid")]
    InvalidBudget,
    #[error("standing Goal active Pursuit link is invalid")]
    InvalidPursuitLink,
    #[error("standing Goal Pursuit state is invalid")]
    InvalidPursuitState,
    #[error("standing Goal wait barrier is invalid")]
    InvalidWaitBarrier,
    #[error("standing Goal Evaluation is invalid")]
    InvalidEvaluation,
    #[error("standing Goal completion evidence is insufficient")]
    InsufficientEvidence,
    #[error("standing Goal transition is invalid")]
    InvalidTransition,
}

#[cfg(test)]
mod tests {
    use super::{StandingGoalStatus, validate_goal_transition};

    #[test]
    fn achieved_goal_cannot_be_reactivated() {
        assert!(
            validate_goal_transition(StandingGoalStatus::Achieved, StandingGoalStatus::Active)
                .is_err()
        );
    }
}
