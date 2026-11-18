//! Preparation failures preserve their owner and recovery advice. They happen
//! before dispatch; they must never be described as failed user code or CAS.
use musteragent_loop::LoopPortError;
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_resource::ExecutionWorkspacePreparationError;

pub(super) struct PreparationFailure {
    pub code: ErrorCode,
    pub message: &'static str,
}
impl From<ExecutionWorkspacePreparationError> for PreparationFailure {
    fn from(error: ExecutionWorkspacePreparationError) -> Self {
        use ExecutionWorkspacePreparationError as E;
        let (code, message) = match error {
            E::WriterBusy => (
                ErrorCode::CapabilityUnavailable,
                "Workspace writer is held by an unfinished or unconfirmed execution. No user code was dispatched and no files changed. Read committed files while the original execution is reconciled; changing inputs, resetting packages or retrying writes cannot release this writer.",
            ),
            E::RequestConflict => (
                ErrorCode::IdempotencyConflict,
                "The original execution request differs from its frozen preparation. No user code was dispatched. Recover the original invocation; do not replace its arguments or replay it.",
            ),
            E::BoundsExceeded => (
                ErrorCode::QuotaExceeded,
                "The selected workspace exceeds this executor's file or byte capacity. No user code was dispatched. Select a smaller authorized subtree or a capable executor; no files were omitted or changed.",
            ),
            E::Corrupt => (
                ErrorCode::ContentIntegrityFailed,
                "The authoritative workspace baseline failed integrity validation. No user code was dispatched. Committed files are unchanged; the workspace service needs repair, not different script arguments.",
            ),
            E::Unavailable => (
                ErrorCode::PolicyDenied,
                "This invocation no longer has authority to prepare the workspace. No user code was dispatched. Refresh the current conversation and execution authority before starting a new invocation.",
            ),
            E::DependencyUnavailable => (
                ErrorCode::DependencyUnavailable,
                "Workspace storage is unavailable during preparation. No user code was dispatched and no files changed. Retry preparation after storage recovers.",
            ),
        };
        Self { code, message }
    }
}
impl From<LoopPortError> for PreparationFailure {
    fn from(error: LoopPortError) -> Self {
        let message = match error.code {
            ErrorCode::ResourceRevisionConflict => {
                "A selected input version changed before execution. No user code was dispatched. Query its current file reference and update the input selection."
            }
            ErrorCode::ResourceFileUnavailable
            | ErrorCode::ResourceNodeNotFound
            | ErrorCode::ContentUnavailable => {
                "A selected script or input is unavailable in the authorized execution scope. No user code was dispatched. Query available files and correct the path or mount selection."
            }
            ErrorCode::QuotaExceeded => {
                "The selected execution exceeds this provider's capacity. No user code was dispatched. Reduce the authorized selection or choose a capable provider; do not truncate input data."
            }
            ErrorCode::DependencyUnavailable => {
                "An execution preparation dependency is unavailable. No user code was dispatched. Retry preparation after the dependency recovers."
            }
            ErrorCode::ContentIntegrityFailed | ErrorCode::InternalInvariantViolated => {
                "Execution preparation failed integrity validation. No user code was dispatched. The provider or workspace service needs repair; changing user code cannot repair this state."
            }
            ErrorCode::PolicyDenied => {
                "A selected execution resource is no longer authorized. No user code was dispatched. Refresh its authorization or select an accessible resource."
            }
            _ => {
                "Execution preparation rejected the request before user code was dispatched. Check the selected script, cwd, mounts and environment against the tool contract; committed files are unchanged."
            }
        };
        Self {
            code: error.code,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn writer_wait_is_not_a_version_conflict_or_an_environment_reset_request() {
        let failure = PreparationFailure::from(ExecutionWorkspacePreparationError::WriterBusy);
        assert_eq!(failure.code, ErrorCode::CapabilityUnavailable);
        assert!(failure.message.contains("No user code was dispatched"));
        assert!(failure.message.contains("cannot release this writer"));
        assert_ne!(
            PreparationFailure::from(ExecutionWorkspacePreparationError::RequestConflict).code,
            failure.code
        );
    }
}
