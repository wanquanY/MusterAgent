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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_safe_display_text"
    )]
    pub pause_reason: Option<musteragent_runtime_contract::SafeDisplayText>,
}

impl ScheduleView {
    /// Projects this view to the frozen `@2` schedule tool schema shape, which
    /// forbids additional properties on tool responses.
    #[must_use]
    pub fn without_pause_reason(mut self) -> Self {
        self.pause_reason = None;
        self
    }
}

mod optional_safe_display_text {
    use musteragent_runtime_contract::SafeDisplayText;
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(value: &Option<SafeDisplayText>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(text) => serializer.serialize_some(text.as_str()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SafeDisplayText>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|raw| SafeDisplayText::new(raw).map_err(D::Error::custom))
            .transpose()
    }
}

/// Immutable occurrence and source-Run facts supplied to [`ScheduleAdmissionHook`].
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleTriggerContext {
    pub actor: Actor,
    pub owner_conversation_id: ConversationId,
    pub workspace_id: Option<musteragent_identity::WorkspaceId>,
    pub schedule: ScheduleView,
    pub occurrence_id: uuid::Uuid,
    pub scheduled_at: UtcTimestamp,
    pub manual: bool,
    pub target_run_id: RunId,
    pub source_run_id: RunId,
    pub source_snapshot: musteragent_runtime_snapshot::RuntimeSnapshotContent,
}

/// Refreshed capability, tool, skill, discovery and prompt material for a
/// scheduled Run. Non-partition Snapshot fields (Agent version, model routes,
/// policy mode, memory/instinct selections and Goal authority) stay pinned to
/// the frozen source Run, and the resulting Snapshot still undergoes full
/// admission sealing and derived-grant validation.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshedAutomationPartition {
    pub capabilities: Vec<musteragent_capability::CapabilityGrantSnapshot>,
    pub tools: Vec<musteragent_runtime_snapshot::ToolDefinitionSnapshot>,
    pub skills: Vec<musteragent_skill::SkillDefinition>,
    pub discovery: Option<musteragent_discovery::DiscoverySnapshot>,
    pub prompt: musteragent_runtime_snapshot::PromptAssemblySnapshot,
}

impl RefreshedAutomationPartition {
    /// Clones the refreshable partition from an existing Snapshot.
    #[must_use]
    pub fn from_snapshot(snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent) -> Self {
        Self {
            capabilities: snapshot.capabilities.clone(),
            tools: snapshot.tools.clone(),
            skills: snapshot.skills.clone(),
            discovery: snapshot.discovery.clone(),
            prompt: snapshot.prompt.clone(),
        }
    }
}

/// Host admission decision returned before a due schedule occurrence creates a Run.
#[derive(Debug, Clone, PartialEq)]
pub enum PreTriggerDecision {
    UseFrozenSource,
    Refresh(Box<RefreshedAutomationPartition>),
    Pause {
        reason: musteragent_runtime_contract::SafeDisplayText,
    },
}

impl PreTriggerDecision {
    #[must_use]
    pub fn refresh(partition: RefreshedAutomationPartition) -> Self {
        Self::Refresh(Box::new(partition))
    }
}

/// Explicit failure from [`ScheduleAdmissionHook::before_trigger`].
///
/// Retryable errors defer dispatch without consuming the occurrence; permanent
/// errors dead-letter the occurrence without retrying or falling back to the
/// frozen source Snapshot.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error(
    "schedule admission hook failed ({}): {}",
    if *retryable { "retryable" } else { "permanent" },
    message.as_str()
)]
pub struct ScheduleAdmissionHookError {
    pub retryable: bool,
    pub message: musteragent_runtime_contract::SafeDisplayText,
    pub runtime_code: Option<musteragent_runtime_contract::ErrorCode>,
}

impl ScheduleAdmissionHookError {
    #[must_use]
    pub fn retryable(message: musteragent_runtime_contract::SafeDisplayText) -> Self {
        Self {
            retryable: true,
            message,
            runtime_code: None,
        }
    }

    #[must_use]
    pub fn permanent(message: musteragent_runtime_contract::SafeDisplayText) -> Self {
        Self {
            retryable: false,
            message,
            runtime_code: None,
        }
    }

    #[must_use]
    pub fn with_runtime_code(
        mut self,
        runtime_code: musteragent_runtime_contract::ErrorCode,
    ) -> Self {
        self.runtime_code = Some(runtime_code);
        self
    }
}

/// Optional host gate consulted before admitting a scheduled Run.
///
/// Delegated child sub-agent Runs do not invoke this hook.
pub trait ScheduleAdmissionHook: Send + Sync {
    fn before_trigger(
        &self,
        context: ScheduleTriggerContext,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<PreTriggerDecision, ScheduleAdmissionHookError>,
    >;
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
#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
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
    #[error(transparent)]
    AdmissionHookFailed(#[from] ScheduleAdmissionHookError),
}
