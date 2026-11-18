use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, CommandId, ConversationId, CorrelationId, FencingToken,
    InvocationId, RunId, TenantId,
};
use musteragent_runtime_contract::CommandMetadata;
use musteragent_runtime_contract::TraceContext;

/// Canonical command identity recorded on ingress spans and logs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandTelemetryContext {
    pub tenant_id: TenantId,
    pub conversation_id: Option<ConversationId>,
    pub command_id: CommandId,
    pub correlation_id: CorrelationId,
    pub trace_context: TraceContext,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CommandOperation {
    General,
    Submit,
    Cancel,
}

impl CommandOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::General => "command",
            Self::Submit => "command.submit",
            Self::Cancel => "command.cancel",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CommandTerminalOutcome {
    Completed,
    Rejected,
    Failed,
}

impl CommandTerminalOutcome {
    #[must_use]
    pub const fn telemetry_outcome(self) -> TelemetryOutcome {
        match self {
            Self::Completed => TelemetryOutcome::Success,
            Self::Rejected | Self::Failed => TelemetryOutcome::Error,
        }
    }

    #[must_use]
    pub const fn from_error_code(code: musteragent_runtime_contract::ErrorCode) -> Self {
        use musteragent_runtime_contract::ErrorCategory;
        match code.category() {
            ErrorCategory::Validation
            | ErrorCategory::Authentication
            | ErrorCategory::Authorization
            | ErrorCategory::Conflict
            | ErrorCategory::NotFound
            | ErrorCategory::Policy
            | ErrorCategory::Quota => Self::Rejected,
            ErrorCategory::Dependency | ErrorCategory::Timeout | ErrorCategory::Internal => {
                Self::Failed
            }
        }
    }
}

impl From<&CommandMetadata> for CommandTelemetryContext {
    fn from(metadata: &CommandMetadata) -> Self {
        Self {
            tenant_id: metadata.actor.tenant_id,
            conversation_id: metadata
                .conversation_guard
                .map(|guard| guard.conversation_id),
            command_id: metadata.command_id,
            correlation_id: metadata.correlation_id,
            trace_context: metadata.trace_context.clone(),
        }
    }
}

/// Canonical Run/Attempt identity reconstructed from durable execution facts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunTelemetryContext {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub correlation_id: CorrelationId,
    pub trace_context: TraceContext,
}

impl RunTelemetryContext {
    #[must_use]
    pub fn for_attempt(
        &self,
        attempt_id: AttemptId,
        attempt_number: AttemptNumber,
        fencing_token: FencingToken,
    ) -> AttemptTelemetryContext {
        AttemptTelemetryContext {
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            activity_id: self.activity_id,
            run_id: self.run_id,
            attempt_id,
            attempt_number,
            fencing_token,
            correlation_id: self.correlation_id,
            trace_context: self.trace_context.clone(),
        }
    }
}

/// Canonical Run/Attempt identity reconstructed from durable execution facts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttemptTelemetryContext {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub attempt_number: AttemptNumber,
    pub fencing_token: FencingToken,
    pub correlation_id: CorrelationId,
    pub trace_context: TraceContext,
}

/// Canonical physical side-effect identity. It never carries request or result data.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvocationTelemetryContext {
    pub attempt: AttemptTelemetryContext,
    pub invocation_id: InvocationId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InvocationOperation {
    Model,
    Tool,
}

impl InvocationOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Tool => "tool",
        }
    }
}

/// Canonical operation keys accepted by span and structured-log schemas.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TelemetryOperation {
    RuntimeBootstrap,
    CommandGeneral,
    CommandSubmit,
    CommandCancel,
    Run,
    Attempt,
    AttemptExecute,
    Model,
    Tool,
    ToolObservation,
    ToolMaterialization,
    Interaction,
    DataEdge,
    DataEdgeProjectionRead,
    DataEdgeConversationBootstrapRead,
    DataEdgeRecordList,
    DataEdgeRecordSubscribe,
    DataEdgeContentRead,
    DataEdgeContentBatchRead,
    DbCommit,
    LedgerCommit,
    Outbox,
    Projection,
    EvolutionLearningSchedule,
    EvolutionLearningInduction,
}

/// Closed, privacy-safe stage taxonomy for autonomous Learning failures.
/// The stage is independent from retry class so evidence corruption cannot be
/// mistaken for a backend control-plane outage.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LearningFailureStage {
    EvidenceLoad,
    PolicyResolution,
    ContentWrite,
    WorkflowCommit,
    ContractValidation,
    DreamInduction,
    MaintenanceAuthorization,
    MaintenanceAdmission,
    MaintenanceExecution,
    ModelOutput,
}

impl LearningFailureStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceLoad => "evidence_load",
            Self::PolicyResolution => "policy_resolution",
            Self::ContentWrite => "content_write",
            Self::WorkflowCommit => "workflow_commit",
            Self::ContractValidation => "contract_validation",
            Self::DreamInduction => "dream_induction",
            Self::MaintenanceAuthorization => "maintenance_authorization",
            Self::MaintenanceAdmission => "maintenance_admission",
            Self::MaintenanceExecution => "maintenance_execution",
            Self::ModelOutput => "model_output",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::EvidenceLoad,
            Self::PolicyResolution,
            Self::ContentWrite,
            Self::WorkflowCommit,
            Self::ContractValidation,
            Self::DreamInduction,
            Self::MaintenanceAuthorization,
            Self::MaintenanceAdmission,
            Self::MaintenanceExecution,
            Self::ModelOutput,
        ]
        .into_iter()
        .find(|stage| stage.as_str() == value)
    }
}

impl TelemetryOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeBootstrap => "runtime.bootstrap",
            Self::CommandGeneral => "command",
            Self::CommandSubmit => "command.submit",
            Self::CommandCancel => "command.cancel",
            Self::Run => "run",
            Self::Attempt => "attempt",
            Self::AttemptExecute => "attempt.execute",
            Self::Model => "model",
            Self::Tool => "tool",
            Self::ToolObservation => "tool.observation",
            Self::ToolMaterialization => "tool.materialization",
            Self::Interaction => "interaction",
            Self::DataEdge => "data_edge",
            Self::DataEdgeProjectionRead => "data_edge.projection.read",
            Self::DataEdgeConversationBootstrapRead => "data_edge.conversation.bootstrap_read",
            Self::DataEdgeRecordList => "data_edge.record.list",
            Self::DataEdgeRecordSubscribe => "data_edge.record.subscribe",
            Self::DataEdgeContentRead => "data_edge.content.read",
            Self::DataEdgeContentBatchRead => "data_edge.content.batch_read",
            Self::DbCommit => "db.commit",
            Self::LedgerCommit => "ledger_commit",
            Self::Outbox => "outbox",
            Self::Projection => "projection",
            Self::EvolutionLearningSchedule => "evolution.learning.schedule",
            Self::EvolutionLearningInduction => "evolution.learning.induction",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::RuntimeBootstrap,
            Self::CommandGeneral,
            Self::CommandSubmit,
            Self::CommandCancel,
            Self::Run,
            Self::Attempt,
            Self::AttemptExecute,
            Self::Model,
            Self::Tool,
            Self::ToolObservation,
            Self::ToolMaterialization,
            Self::Interaction,
            Self::DataEdge,
            Self::DataEdgeProjectionRead,
            Self::DataEdgeConversationBootstrapRead,
            Self::DataEdgeRecordList,
            Self::DataEdgeRecordSubscribe,
            Self::DataEdgeContentRead,
            Self::DataEdgeContentBatchRead,
            Self::DbCommit,
            Self::LedgerCommit,
            Self::Outbox,
            Self::Projection,
            Self::EvolutionLearningSchedule,
            Self::EvolutionLearningInduction,
        ]
        .into_iter()
        .find(|operation| operation.as_str() == value)
    }
}

/// Canonical public Data Edge identity extracted after ticket authentication.
///
/// Browser request paths, ticket material, query values and response content are
/// intentionally absent. An invalid or missing inbound trace context produces a
/// new local trace without changing the data-plane request result.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DataEdgeTelemetryContext {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub correlation_id: CorrelationId,
    pub trace_context: Option<TraceContext>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DataEdgeOperation {
    ProjectionRead,
    ConversationBootstrapRead,
    RecordList,
    RecordSubscribe,
    ContentRead,
    ContentBatchRead,
}

impl DataEdgeOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectionRead => "data_edge.projection.read",
            Self::ConversationBootstrapRead => "data_edge.conversation.bootstrap_read",
            Self::RecordList => "data_edge.record.list",
            Self::RecordSubscribe => "data_edge.record.subscribe",
            Self::ContentRead => "data_edge.content.read",
            Self::ContentBatchRead => "data_edge.content.batch_read",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TelemetryOutcome {
    Success,
    Error,
    Waiting,
    Cancelled,
    Interrupted,
    Duplicate,
    FenceRejected,
    Dropped,
    ExternalOutcomeUnknown,
}

impl TelemetryOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Waiting => "waiting",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::Duplicate => "duplicate",
            Self::FenceRejected => "fence_rejected",
            Self::Dropped => "dropped",
            Self::ExternalOutcomeUnknown => "external_outcome_unknown",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::Success,
            Self::Error,
            Self::Waiting,
            Self::Cancelled,
            Self::Interrupted,
            Self::Duplicate,
            Self::FenceRejected,
            Self::Dropped,
            Self::ExternalOutcomeUnknown,
        ]
        .into_iter()
        .find(|outcome| outcome.as_str() == value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_runtime_contract::ErrorCode;

    #[test]
    fn command_terminal_classification_is_closed_over_runtime_error_codes() {
        let rejected = [
            ErrorCode::InvalidCommand,
            ErrorCode::Unauthenticated,
            ErrorCode::ParticipantNotAuthorized,
            ErrorCode::IdempotencyConflict,
            ErrorCode::ConversationNotFound,
            ErrorCode::PolicyDenied,
            ErrorCode::QuotaExceeded,
        ];
        for code in rejected {
            assert_eq!(
                CommandTerminalOutcome::from_error_code(code),
                CommandTerminalOutcome::Rejected
            );
        }
        for code in [
            ErrorCode::DependencyUnavailable,
            ErrorCode::DependencyTimeout,
            ErrorCode::InternalInvariantViolated,
        ] {
            assert_eq!(
                CommandTerminalOutcome::from_error_code(code),
                CommandTerminalOutcome::Failed
            );
        }
    }
}
