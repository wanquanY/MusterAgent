/// Closed low-cardinality dimensions accepted by the Runtime metric API.
///
/// Typed UUIDs, correlation IDs, model keys, Tool arguments, refs and provider
/// request IDs cannot be expressed by this API and therefore cannot become
/// metric labels. Those identities remain trace/log attributes.
pub struct MetricSchema;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetricOperation {
    Runtime,
    Command,
    Run,
    Attempt,
    AttemptLifecycle,
    Model,
    Tool,
    ToolObservation,
    ToolMaterialization,
    Interaction,
    DataEdge,
    LedgerCommit,
    Outbox,
    Projection,
}

impl MetricOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Command => "command",
            Self::Run => "run",
            Self::Attempt => "attempt",
            Self::AttemptLifecycle => "attempt_lifecycle",
            Self::Model => "model",
            Self::Tool => "tool",
            Self::ToolObservation => "tool_observation",
            Self::ToolMaterialization => "tool_materialization",
            Self::Interaction => "interaction",
            Self::DataEdge => "data_edge",
            Self::LedgerCommit => "ledger_commit",
            Self::Outbox => "outbox",
            Self::Projection => "projection",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::Runtime,
            Self::Command,
            Self::Run,
            Self::Attempt,
            Self::AttemptLifecycle,
            Self::Model,
            Self::Tool,
            Self::ToolObservation,
            Self::ToolMaterialization,
            Self::Interaction,
            Self::DataEdge,
            Self::LedgerCommit,
            Self::Outbox,
            Self::Projection,
        ]
        .into_iter()
        .find(|operation| operation.as_str() == value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DbCommitKind {
    Admission,
    BrowserGateway,
    Channel,
    LedgerOutbox,
    Checkpoint,
    Content,
    Context,
    Control,
    Evolution,
    Learning,
    Memory,
    Migration,
    ModelCredential,
    ModelTerminalUsage,
    PrivacyErasure,
    Resource,
    Scheduler,
    SecurityAudit,
    Skill,
    StandingGoal,
    ToolTerminal,
    Interaction,
    Projection,
}

impl DbCommitKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::BrowserGateway => "browser_gateway",
            Self::Channel => "channel",
            Self::LedgerOutbox => "ledger_outbox",
            Self::Checkpoint => "checkpoint",
            Self::Content => "content",
            Self::Context => "context",
            Self::Control => "control",
            Self::Evolution => "evolution",
            Self::Learning => "learning",
            Self::Memory => "memory",
            Self::Migration => "migration",
            Self::ModelCredential => "model_credential",
            Self::ModelTerminalUsage => "model_terminal_usage",
            Self::PrivacyErasure => "privacy_erasure",
            Self::Resource => "resource",
            Self::Scheduler => "scheduler",
            Self::SecurityAudit => "security_audit",
            Self::Skill => "skill",
            Self::StandingGoal => "standing_goal",
            Self::ToolTerminal => "tool_terminal",
            Self::Interaction => "interaction",
            Self::Projection => "projection",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::Admission,
            Self::BrowserGateway,
            Self::Channel,
            Self::LedgerOutbox,
            Self::Checkpoint,
            Self::Content,
            Self::Context,
            Self::Control,
            Self::Evolution,
            Self::Learning,
            Self::Memory,
            Self::Migration,
            Self::ModelCredential,
            Self::ModelTerminalUsage,
            Self::PrivacyErasure,
            Self::Resource,
            Self::Scheduler,
            Self::SecurityAudit,
            Self::Skill,
            Self::StandingGoal,
            Self::ToolTerminal,
            Self::Interaction,
            Self::Projection,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DbTransactionPhase {
    Acquire,
    LockWait,
    Work,
    Commit,
    Rollback,
    RetryBackoff,
    Total,
}

impl DbTransactionPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acquire => "acquire",
            Self::LockWait => "lock_wait",
            Self::Work => "work",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
            Self::RetryBackoff => "retry_backoff",
            Self::Total => "total",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DbTransactionResult {
    Success,
    Aborted,
    Error,
    Timeout,
    CommitUnknown,
    Deadlock,
    SerializationFailure,
    Constraint,
    Unavailable,
}

impl DbTransactionResult {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Aborted => "aborted",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::CommitUnknown => "commit_unknown",
            Self::Deadlock => "deadlock",
            Self::SerializationFailure => "serialization_failure",
            Self::Constraint => "constraint",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QueueKind {
    SchedulerDispatch,
    WorkerInflight,
    DataEdgeSubscriber,
    Projection,
    TelemetryTrace,
    TelemetryLog,
}

impl QueueKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SchedulerDispatch => "scheduler_dispatch",
            Self::WorkerInflight => "worker_inflight",
            Self::DataEdgeSubscriber => "data_edge_subscriber",
            Self::Projection => "projection",
            Self::TelemetryTrace => "telemetry_trace",
            Self::TelemetryLog => "telemetry_log",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::SchedulerDispatch,
            Self::WorkerInflight,
            Self::DataEdgeSubscriber,
            Self::Projection,
            Self::TelemetryTrace,
            Self::TelemetryLog,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ErrorClass {
    Validation,
    Authentication,
    Authorization,
    Conflict,
    NotFound,
    Policy,
    Quota,
    Dependency,
    Timeout,
    Cancelled,
    Fence,
    Invariant,
}

impl ErrorClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Policy => "policy",
            Self::Quota => "quota",
            Self::Dependency => "dependency",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Fence => "fence",
            Self::Invariant => "invariant",
        }
    }

    #[must_use]
    pub const fn from_error_code(code: ErrorCode) -> Self {
        match code {
            ErrorCode::InvalidCommand
            | ErrorCode::UnsupportedSchemaVersion
            | ErrorCode::RuntimeSnapshotInvalid
            | ErrorCode::InteractionResponseInvalid
            | ErrorCode::ContentIntegrityFailed
            | ErrorCode::ModelContextExceeded
            | ErrorCode::ResourceMoveCycle => Self::Validation,
            ErrorCode::ToolNotExposed => Self::Validation,
            ErrorCode::Unauthenticated => Self::Authentication,
            ErrorCode::ParticipantNotAuthorized => Self::Authorization,
            ErrorCode::IdempotencyConflict
            | ErrorCode::ConversationArchived
            | ErrorCode::ConversationEpochStale
            | ErrorCode::ConversationSeqConflict
            | ErrorCode::RunTerminal
            | ErrorCode::RunTransitionInvalid
            | ErrorCode::InteractionNotPending
            | ErrorCode::InteractionExpired
            | ErrorCode::CursorEpochStale
            | ErrorCode::CursorCompacted
            | ErrorCode::CatalogCursorInvalidated
            | ErrorCode::ResourceRevisionConflict
            | ErrorCode::ResourceNameConflict
            | ErrorCode::ResourceFileUnavailable
            | ErrorCode::MemoryConflict
            | ErrorCode::MemoryEvaluationPending
            | ErrorCode::MemoryNotPublishable => Self::Conflict,
            ErrorCode::AttemptLeaseStale => Self::Fence,
            ErrorCode::ConversationNotFound
            | ErrorCode::AgentNotAvailable
            | ErrorCode::RunNotFound
            | ErrorCode::InteractionNotFound
            | ErrorCode::ArtifactNotFound
            | ErrorCode::ResourceNodeNotFound
            | ErrorCode::ResourceParentNotFound
            | ErrorCode::MemoryNotFound => Self::NotFound,
            ErrorCode::ModelNotEntitled
            | ErrorCode::PolicyDenied
            | ErrorCode::NativePermissionReduced => Self::Policy,
            ErrorCode::CreditsExhausted
            | ErrorCode::QuotaExceeded
            | ErrorCode::ModelPaymentRequired => Self::Quota,
            ErrorCode::ModelRouteUnavailable
            | ErrorCode::ModelResponseInvalid
            | ErrorCode::PublicDeliveryMissing
            | ErrorCode::ModelRequestRejected
            | ErrorCode::CapabilityUnavailable
            | ErrorCode::BrowserExecutionUnavailable
            | ErrorCode::DependencyUnavailable
            | ErrorCode::CredentialRefreshRequired
            | ErrorCode::ContentUnavailable => Self::Dependency,
            ErrorCode::DependencyTimeout => Self::Timeout,
            ErrorCode::InternalInvariantViolated => Self::Invariant,
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::Validation,
            Self::Authentication,
            Self::Authorization,
            Self::Conflict,
            Self::NotFound,
            Self::Policy,
            Self::Quota,
            Self::Dependency,
            Self::Timeout,
            Self::Cancelled,
            Self::Fence,
            Self::Invariant,
        ]
        .into_iter()
        .find(|class| class.as_str() == value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WaitReasonClass {
    Approval,
    Input,
    Delegation,
    Device,
    RetryBackoff,
    Budget,
}

impl WaitReasonClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::Input => "input",
            Self::Delegation => "delegation",
            Self::Device => "device",
            Self::RetryBackoff => "retry_backoff",
            Self::Budget => "budget",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::Approval,
            Self::Input,
            Self::Delegation,
            Self::Device,
            Self::RetryBackoff,
            Self::Budget,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TelemetrySignal {
    Trace,
    Metric,
    Log,
}

impl TelemetrySignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Metric => "metric",
            Self::Log => "log",
        }
    }
}
use musteragent_runtime_contract::ErrorCode;
