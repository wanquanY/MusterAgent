/// Closed, low-cardinality event keys accepted by the production log schema.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TelemetryEventKind {
    RuntimeStarted,
    CommandCompleted,
    CommandFailed,
    CommandRejected,
    AttemptCompleted,
    AttemptLifecycleObserved,
    ModelInvocationCompleted,
    ToolInvocationCompleted,
    ToolObservationFailed,
    ToolMaterializationFailed,
    DataEdgeCompleted,
    DbCommitCompleted,
    LogSinkReady,
    ExportFailed,
}

impl TelemetryEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeStarted => "runtime.started",
            Self::CommandCompleted => "command.completed",
            Self::CommandFailed => "command.failed",
            Self::CommandRejected => "command.rejected",
            Self::AttemptCompleted => "attempt.completed",
            Self::AttemptLifecycleObserved => "attempt.lifecycle_observed",
            Self::ModelInvocationCompleted => "model_invocation.completed",
            Self::ToolInvocationCompleted => "tool_invocation.completed",
            Self::ToolObservationFailed => "tool_observation.failed",
            Self::ToolMaterializationFailed => "tool_materialization.failed",
            Self::DataEdgeCompleted => "data_edge.completed",
            Self::DbCommitCompleted => "db_commit.completed",
            Self::LogSinkReady => "telemetry.log_sink.ready",
            Self::ExportFailed => "telemetry.export.failed",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::RuntimeStarted,
            Self::CommandCompleted,
            Self::CommandFailed,
            Self::CommandRejected,
            Self::AttemptCompleted,
            Self::AttemptLifecycleObserved,
            Self::ModelInvocationCompleted,
            Self::ToolInvocationCompleted,
            Self::ToolObservationFailed,
            Self::ToolMaterializationFailed,
            Self::DataEdgeCompleted,
            Self::DbCommitCompleted,
            Self::LogSinkReady,
            Self::ExportFailed,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == value)
    }
}

#[cfg(test)]
mod tests {
    use super::TelemetryEventKind;

    #[test]
    fn event_keys_are_a_closed_taxonomy() {
        assert_eq!(
            TelemetryEventKind::from_safe_key("tool_invocation.completed"),
            Some(TelemetryEventKind::ToolInvocationCompleted)
        );
        assert_eq!(
            TelemetryEventKind::from_safe_key("provider supplied message"),
            None
        );
    }
}
