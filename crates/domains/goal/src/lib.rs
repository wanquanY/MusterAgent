//! Goal and Schedule contracts shared by Device and Cloud deployments.
//!
//! UTC instants are persisted; cron expressions are evaluated in the declared
//! IANA timezone. The shared schedule domain owns occurrence evaluation;
//! compatibility exports below keep existing goal and automation callers intact.

#![forbid(unsafe_code)]

mod admission;
mod input;
mod processing_rules;
mod standing;
mod start;

pub use input::*;
pub use processing_rules::*;
pub use standing::*;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    AgentId, ConversationId, GoalId, PrincipalId, RunId, ScheduleOccurrenceId, TenantId,
    UtcTimestamp,
};
use thiserror::Error;

pub const MAX_GOAL_NAME_BYTES: usize = 200;
pub const MAX_GOAL_DESCRIPTION_BYTES: usize = 2_000;
pub use musteragent_schedule::{
    MAX_CRON_EXPRESSION_BYTES, MAX_INTERVAL_MILLIS, MAX_TIMEZONE_BYTES, MIN_INTERVAL_MILLIS,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalStatus {
    Draft,
    Active,
    Paused,
    Achieved,
    Failed,
    Archived,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GoalPriority {
    Low,
    Normal,
    High,
    Critical,
}

pub use musteragent_schedule::{
    AgentTurnExecution, ConcurrencyPolicy, ExecutionSpec, LatenessPolicy, RetrySpec, ScheduleSpec,
    SessionTarget, UnattendedApprovalPolicy,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Goal {
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub revision: u64,
    pub owner: PrincipalId,
    pub target_agent: AgentId,
    pub name: String,
    pub description: String,
    pub objective_ref: ContentRef,
    pub constraints: Vec<String>,
    pub success_metrics: Vec<String>,
    pub priority: GoalPriority,
    pub status: GoalStatus,
    pub schedule: ScheduleSpec,
    pub execution: ExecutionSpec,
    pub concurrency_policy: ConcurrencyPolicy,
    pub lateness_policy: LatenessPolicy,
    pub lateness_window_millis: u64,
    pub retry: RetrySpec,
    pub timezone: String,
    pub delete_after_run: bool,
    pub last_run_at: Option<UtcTimestamp>,
    pub next_run_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl Goal {
    pub fn validate(&self) -> Result<(), GoalContractError> {
        bounded(&self.name, MAX_GOAL_NAME_BYTES)?;
        if self.description.len() > MAX_GOAL_DESCRIPTION_BYTES || self.description.contains('\0') {
            return Err(GoalContractError::InvalidText);
        }
        if self.revision == 0
            || self.owner.to_string().is_empty()
            || self.target_agent.to_string().is_empty()
        {
            return Err(GoalContractError::InvalidIdentity);
        }
        validate_timezone(&self.timezone)?;
        validate_schedule(&self.schedule, &self.timezone)?;
        let ExecutionSpec::AgentTurn(execution) = &self.execution;
        bounded(&execution.requested_model_class, 255)?;
        if execution
            .workdir
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 4_096 || value.contains('\0'))
        {
            return Err(GoalContractError::InvalidExecution);
        }
        if execution.session_target == SessionTarget::Current && execution.conversation_id.is_none()
        {
            return Err(GoalContractError::InvalidExecution);
        }
        if self.lateness_window_millis > MAX_INTERVAL_MILLIS {
            return Err(GoalContractError::InvalidLatenessWindow);
        }
        self.retry
            .validate()
            .map_err(|_| GoalContractError::InvalidRetry)?;
        if !matches!(self.status, GoalStatus::Active) && self.next_run_at.is_some() {
            return Err(GoalContractError::InvalidNextRun);
        }
        if self.status == GoalStatus::Active && self.next_run_at.is_none() {
            return Err(GoalContractError::InvalidNextRun);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OccurrenceStatus {
    Due,
    Claimed,
    Admitted,
    Completed,
    Failed,
    DeadLettered,
    Skipped,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduleOccurrence {
    pub occurrence_id: ScheduleOccurrenceId,
    pub tenant_id: TenantId,
    pub goal_id: GoalId,
    pub trigger_identity: String,
    pub scheduled_at: UtcTimestamp,
    pub status: OccurrenceStatus,
    pub claim_token: Option<String>,
    pub claim_attempt: u32,
    pub claimed_at: Option<UtcTimestamp>,
    pub lease_expires_at: Option<UtcTimestamp>,
    pub conversation_id: Option<ConversationId>,
    pub run_id: Option<RunId>,
    pub error: Option<String>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

fn bounded(value: &str, maximum: usize) -> Result<(), GoalContractError> {
    if value.is_empty() || value.len() > maximum || value.contains('\0') {
        Err(GoalContractError::InvalidText)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum GoalContractError {
    #[error("Goal identity is invalid")]
    InvalidIdentity,
    #[error("Goal text is invalid")]
    InvalidText,
    #[error("Goal schedule is invalid")]
    InvalidSchedule,
    #[error("Goal timezone is invalid")]
    InvalidTimezone,
    #[error("Goal execution is invalid")]
    InvalidExecution,
    #[error("Goal lateness window is invalid")]
    InvalidLatenessWindow,
    #[error("Goal retry policy is invalid")]
    InvalidRetry,
    #[error("Goal next_run_at conflicts with its status")]
    InvalidNextRun,
    #[error("Goal timestamp cannot be represented")]
    InvalidTimestamp,
    #[error("Goal schedule exceeded the timestamp range")]
    ScheduleOverflow,
    #[error("Goal schedule has no computable occurrence")]
    ScheduleUnavailable,
}

pub fn initial_next_run(
    schedule: &ScheduleSpec,
    timezone: &str,
    now: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, GoalContractError> {
    Ok(musteragent_schedule::initial_next_run(
        schedule, timezone, now,
    )?)
}

pub fn next_run_after(
    schedule: &ScheduleSpec,
    timezone: &str,
    after: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, GoalContractError> {
    Ok(musteragent_schedule::next_run_after(
        schedule, timezone, after,
    )?)
}

pub fn latest_run_not_after(
    schedule: &ScheduleSpec,
    timezone: &str,
    now: UtcTimestamp,
) -> Result<Option<UtcTimestamp>, GoalContractError> {
    Ok(musteragent_schedule::latest_run_not_after(
        schedule, timezone, now,
    )?)
}

pub fn validate_schedule(schedule: &ScheduleSpec, timezone: &str) -> Result<(), GoalContractError> {
    Ok(musteragent_schedule::validate_schedule(schedule, timezone)?)
}

pub fn validate_timezone(value: &str) -> Result<chrono_tz::Tz, GoalContractError> {
    Ok(musteragent_schedule::validate_timezone(value)?)
}

impl From<musteragent_schedule::ScheduleContractError> for GoalContractError {
    fn from(value: musteragent_schedule::ScheduleContractError) -> Self {
        use musteragent_schedule::ScheduleContractError as E;
        match value {
            E::InvalidSchedule => Self::InvalidSchedule,
            E::InvalidTimezone => Self::InvalidTimezone,
            E::InvalidTimestamp => Self::InvalidTimestamp,
            E::ScheduleOverflow => Self::ScheduleOverflow,
            E::ScheduleUnavailable => Self::ScheduleUnavailable,
        }
    }
}

pub mod event_subscription;
pub mod source_binding;
