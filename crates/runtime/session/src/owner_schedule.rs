//! Account-owned recurring instructions. Timing uses the original Goal domain;
//! delivery is an ordinary, durable Dialogue intent, never a second Run engine.
use musteragent_goal::{ScheduleSpec, initial_next_run, validate_schedule, validate_timezone};
use musteragent_identity::{Actor, AgentId, CommandId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerScheduleDefinition {
    pub title: String,
    pub instructions: String,
    pub timezone: String,
    pub timing: ScheduleSpec,
}
impl OwnerScheduleDefinition {
    pub fn validate(&self) -> Result<(), OwnerScheduleError> {
        if self.title.trim().is_empty()
            || self.title.len() > 200
            || self.instructions.trim().is_empty()
            || self.instructions.len() > 16_000
            || self.title.contains('\0')
            || self.instructions.contains('\0')
            || matches!(self.timing, ScheduleSpec::Interval { .. })
        {
            return Err(OwnerScheduleError::Invalid);
        }
        validate_timezone(&self.timezone).map_err(|_| OwnerScheduleError::Invalid)?;
        validate_schedule(&self.timing, &self.timezone).map_err(|_| OwnerScheduleError::Invalid)?;
        // The product supports minute granularity. Avoid unbounded second-level schedules.
        if let ScheduleSpec::Cron { expression } = &self.timing
            && expression.split_whitespace().count() != 5
        {
            return Err(OwnerScheduleError::Invalid);
        }
        Ok(())
    }
    pub fn next_after(
        &self,
        now: UtcTimestamp,
    ) -> Result<Option<UtcTimestamp>, OwnerScheduleError> {
        self.validate()?;
        initial_next_run(&self.timing, &self.timezone, now).map_err(|_| OwnerScheduleError::Invalid)
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnerScheduleAction {
    Create,
    Update,
    Pause,
    Resume,
    Delete,
    RunNow,
}
#[derive(Debug, Clone)]
pub struct ChangeOwnerSchedule {
    pub actor: Actor,
    pub agent_id: AgentId,
    pub command_id: CommandId,
    pub schedule_id: uuid::Uuid,
    pub expected_revision: u64,
    pub action: OwnerScheduleAction,
    pub definition: Option<OwnerScheduleDefinition>,
    pub now: UtcTimestamp,
}
#[derive(Debug, Clone)]
pub struct OwnerScheduleOccurrence {
    pub occurrence_id: uuid::Uuid,
    pub scheduled_at: UtcTimestamp,
    pub status: String,
    pub entry_id: Option<uuid::Uuid>,
    pub error_code: Option<String>,
}
#[derive(Debug, Clone)]
pub struct OwnerSchedule {
    pub schedule_id: uuid::Uuid,
    pub agent_id: AgentId,
    pub revision: u64,
    pub definition: OwnerScheduleDefinition,
    pub status: String,
    pub next_run_at: Option<UtcTimestamp>,
    pub recent_runs: Vec<OwnerScheduleOccurrence>,
}
#[derive(Debug, thiserror::Error)]
pub enum OwnerScheduleError {
    #[error("invalid schedule")]
    Invalid,
    #[error("schedule has changed")]
    Conflict,
    #[error("schedule is unavailable")]
    NotFound,
    #[error("schedule limit reached")]
    Limit,
    #[error("schedule storage unavailable")]
    Unavailable,
}
pub trait OwnerScheduleStore: Send + Sync {
    fn list(
        &self,
        actor: Actor,
        agent: AgentId,
    ) -> ApplicationFuture<'_, Result<Vec<OwnerSchedule>, OwnerScheduleError>>;
    fn change(
        &self,
        command: ChangeOwnerSchedule,
    ) -> ApplicationFuture<'_, Result<OwnerSchedule, OwnerScheduleError>>;
}
