//! Cloud/device schedule ownership and host-callable management contracts.
//! Time calculations are shared with standing goals through the schedule domain.
use musteragent_identity::{Actor, CommandId, ConversationId, RunId, UtcTimestamp};
use musteragent_run::AttemptContext;
use musteragent_schedule::{ConcurrencyPolicy, LatenessPolicy, RetrySpec, ScheduleSpec};

/// Complete repeatable task configuration; the execution authority is supplied
/// separately by the host's existing admitted Run.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledDefinition {
    pub title: String,
    pub instructions: String,
    pub timing: ScheduleSpec,
    pub timezone: String,
    pub concurrency: ConcurrencyPolicy,
    pub lateness: LatenessPolicy,
    pub lateness_window_millis: u64,
    pub retry: RetrySpec,
}
impl ScheduledDefinition {
    /// All ingress surfaces call the same validation before persistence.
    pub fn validate(&self) -> Result<(), ScheduleServiceError> {
        if self.title.trim().is_empty()
            || self.title.len() > 200
            || self.instructions.trim().is_empty()
            || self.instructions.len() > 16_000
            || self.title.contains('\0')
            || self.instructions.contains('\0')
            || self.lateness_window_millis > 365 * 24 * 60 * 60 * 1_000
        {
            return Err(ScheduleServiceError::Invalid);
        }
        if serde_json::to_vec(self)
            .map_err(|_| ScheduleServiceError::Invalid)?
            .len()
            > 20_000
        {
            return Err(ScheduleServiceError::Invalid);
        }
        musteragent_schedule::validate_schedule(&self.timing, &self.timezone)
            .map_err(|_| ScheduleServiceError::Invalid)?;
        self.retry
            .validate()
            .map_err(|_| ScheduleServiceError::Invalid)
    }
}

/// One trigger owner. A device identifier is an opaque host identity, never a
/// request to use a browser executor. Scheduled sandboxes always use the server.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScheduleTriggerOwner {
    Cloud,
    Device(String),
}
impl ScheduleTriggerOwner {
    pub fn validate(&self) -> Result<(), ScheduleServiceError> {
        if matches!(self, Self::Device(id) if id.trim().is_empty() || id.len() > 160 || id.contains('\0'))
        {
            return Err(ScheduleServiceError::Invalid);
        }
        Ok(())
    }
}

/// Trusted service scope. A Tool caller must supply its current attempt; host
/// calls use authenticated actor membership. Origin is always read from storage.
#[derive(Debug, Clone)]
pub struct ScheduleScope {
    pub actor: Actor,
    pub owner_conversation_id: ConversationId,
    pub source_run_id: RunId,
    pub attempt: Option<AttemptContext>,
}

/// Optimistic, idempotent management command. Deletes retain definitions and
/// history; command receipts and emitted events are immutable.
#[derive(Debug, Clone)]
pub struct ScheduleCommand {
    pub scope: ScheduleScope,
    pub command_id: CommandId,
    pub schedule_id: uuid::Uuid,
    pub expected_revision: u64,
    pub action: ScheduleAction,
    pub now: UtcTimestamp,
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScheduleAction {
    Create {
        definition: ScheduledDefinition,
        owner: ScheduleTriggerOwner,
    },
    Update {
        definition: ScheduledDefinition,
    },
    Pause,
    Resume,
    Delete,
    RunNow,
    /// Commits a pause and revokes the old epoch. The new owner cannot fire yet.
    BeginHandover {
        owner: ScheduleTriggerOwner,
    },
    /// A second explicit operation activates the new owner at the new epoch.
    CompleteHandover {
        epoch: u64,
    },
}
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScheduleView {
    pub schedule_id: uuid::Uuid,
    pub revision: u64,
    pub definition: ScheduledDefinition,
    pub status: String,
    pub next_run_at_unix_ms: Option<i64>,
    pub owner: ScheduleTriggerOwner,
    pub owner_epoch: u64,
    pub pending_owner: Option<ScheduleTriggerOwner>,
    pub handover_due_at_unix_ms: Option<i64>,
}

/// Stable, exclusive history position; UUID disambiguates simultaneous facts.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleHistoryCursor {
    pub occurred_at_ms: i64,
    pub event_id: uuid::Uuid,
}

/// A bounded page. A present cursor means additional facts were observed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduleHistoryPage {
    pub events: Vec<serde_json::Value>,
    pub next: Option<ScheduleHistoryCursor>,
}

/// Typed failure reasons shared by host API and kernel tools.
#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum ScheduleServiceError {
    #[error("invalid schedule request")]
    Invalid,
    /// Product application catalogs cannot be inherited by kernel schedules.
    #[error("schedule source contains product application catalogs")]
    ApplicationCatalogUnsupported,
    #[error("schedule source authority has been revoked")]
    SourceRevoked,
    #[error("schedule is not accessible")]
    NotFound,
    #[error("schedule revision or command conflicts")]
    Conflict,
    #[error("schedule-triggered executions cannot manage schedules")]
    ScheduledContextDenied,
    #[error("schedule trigger ownership fence is stale or paused")]
    StaleOwner,
    #[error("execution attempt fence is stale")]
    StaleAttempt,
    #[error("schedule storage unavailable")]
    Unavailable,
    #[error("schedule state violates its contract")]
    Corrupt,
}
