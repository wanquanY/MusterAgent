//! Evaluation infrastructure health is independent of the user's Goal verdict.
use musteragent_identity::UtcTimestamp;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalEvaluationFailure {
    pub stage: String,
    pub retryable: bool,
    pub runtime_code: Option<String>,
    pub correlation_id: Option<String>,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalEvaluationRetry {
    pub failure: StandingGoalEvaluationFailure,
    pub first_failed_at: UtcTimestamp,
    pub retry_at: UtcTimestamp,
    pub attempts: u32,
    pub recovery_run_id: Option<musteragent_identity::RunId>,
    pub recovery_attempt: Option<u32>,
}

/// A deletion receipt preserves existence without retaining deleted objective text.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandingGoalDeletion {
    pub created_at: UtcTimestamp,
    pub deleted_at: UtcTimestamp,
}
