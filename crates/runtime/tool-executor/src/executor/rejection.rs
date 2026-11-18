use musteragent_loop::LoopPortError;
use musteragent_runtime_capability::ToolTerminalOutcome;
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText};
use musteragent_tool::ToolRetryContract;

pub(super) fn failed_outcome(
    code: ErrorCode,
    message: &str,
) -> Result<ToolTerminalOutcome, LoopPortError> {
    Ok(ToolTerminalOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: safe_text(message)?,
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    })
}

pub(super) fn safe_text(value: &str) -> Result<SafeDisplayText, LoopPortError> {
    SafeDisplayText::new(value).map_err(|_| port(ErrorCode::InternalInvariantViolated))
}

const fn port(code: ErrorCode) -> LoopPortError {
    LoopPortError {
        code,
        retryable: false,
    }
}

/// Callee failures become durable observations for the model. Only runtime
/// ownership/integrity faults and account admission denials remain Ports. A transport failure after a write
/// may have been applied and must be recorded as indeterminate by the executor.
pub(super) fn dispatch_rejection_outcome(
    error: &LoopPortError,
    retry_contract: ToolRetryContract,
) -> Result<Option<ToolTerminalOutcome>, LoopPortError> {
    match error.code {
        ErrorCode::InvalidCommand => failed_outcome(
            ErrorCode::InvalidCommand,
            "Tool request is invalid; inspect the Tool definition, correct the request, and retry",
        )
        .map(Some),
        ErrorCode::ArtifactNotFound | ErrorCode::ResourceNodeNotFound | ErrorCode::ResourceParentNotFound => failed_outcome(
            error.code, "The requested object does not exist or is unavailable to this task; inspect the available objects and correct the reference",
        ).map(Some),
        ErrorCode::ResourceRevisionConflict | ErrorCode::ResourceNameConflict | ErrorCode::ResourceMoveCycle => failed_outcome(
            error.code, "The requested change conflicts with the current resource state; read the latest state and adjust the operation",
        ).map(Some),
        ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout if retry_contract == ToolRetryContract::ReadOnly => {
            let mut outcome = failed_outcome(error.code, if error.code == ErrorCode::DependencyTimeout { "The read-only Tool timed out; choose whether to retry or use another approach" } else { "The read-only Tool dependency is unavailable; choose whether to retry or use another source" })?;
            if let ToolTerminalOutcome::Failed { error: info, .. } = &mut outcome { info.retryable = error.retryable; }
            Ok(Some(outcome))
        }
        ErrorCode::Unauthenticated | ErrorCode::CredentialRefreshRequired | ErrorCode::PolicyDenied
        | ErrorCode::QuotaExceeded | ErrorCode::CapabilityUnavailable | ErrorCode::ToolNotExposed
        | ErrorCode::ModelRouteUnavailable | ErrorCode::ModelResponseInvalid | ErrorCode::ModelRequestRejected
        | ErrorCode::ModelContextExceeded | ErrorCode::ContentUnavailable | ErrorCode::ResourceFileUnavailable => {
            let mut outcome = failed_outcome(error.code, "Tool execution failed; inspect the error code and choose whether to correct the request, retry, or use another approach")?;
            if let ToolTerminalOutcome::Failed { error: info, .. } = &mut outcome { info.retryable = error.retryable; }
            Ok(Some(outcome))
        }
        _ => Ok(None),
    }
}

/// A Tool that could not be started at all: its runtime binding or contract is
/// inconsistent with this Run. No physical work was built or polled, so the
/// model can safely be told the Tool was not run and choose another approach.
/// Only used for undispatched invocations; after dispatch these codes remain
/// Attempt-level integrity failures.
pub(super) fn undispatched_binding_outcome(
    error: &LoopPortError,
) -> Result<Option<ToolTerminalOutcome>, LoopPortError> {
    match error.code {
        ErrorCode::RuntimeSnapshotInvalid | ErrorCode::UnsupportedSchemaVersion => failed_outcome(
            error.code,
            "This Tool could not be started because its runtime binding is inconsistent; it was not run. Use another approach or tell the user this Tool is unavailable",
        )
        .map(Some),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_capability::ToolTerminalOutcome;
    use musteragent_runtime_contract::{ErrorCode, ErrorInfo, RetryDisposition};

    use super::*;

    #[test]
    fn account_denial_is_not_returned_to_the_model_as_a_repairable_tool_error() {
        for code in [ErrorCode::CreditsExhausted, ErrorCode::ModelNotEntitled] {
            assert_eq!(
                dispatch_rejection_outcome(&port(code), ToolRetryContract::ReadOnly)
                    .expect("classification"),
                None
            );
        }
    }

    #[test]
    fn invalid_command_dispatch_errors_become_repairable_tool_failures() {
        let outcome = dispatch_rejection_outcome(
            &port(ErrorCode::InvalidCommand),
            ToolRetryContract::QueryBeforeRetry,
        )
        .expect("safe failure message")
        .expect("invalid command is a Tool rejection");

        assert!(matches!(
            outcome,
            ToolTerminalOutcome::Failed {
                error: ErrorInfo {
                    code: ErrorCode::InvalidCommand,
                    retryable: false,
                    ..
                },
                retry_disposition: RetryDisposition::DoNotRetry,
                diagnostic_result: None,
            }
        ));
    }

    #[test]
    fn read_dependency_failure_is_model_visible_but_uncertain_writes_are_not_retried() {
        for code in [
            ErrorCode::DependencyTimeout,
            ErrorCode::DependencyUnavailable,
        ] {
            let error = LoopPortError {
                code,
                retryable: true,
            };
            let Some(ToolTerminalOutcome::Failed {
                error: feedback,
                retry_disposition,
                ..
            }) = dispatch_rejection_outcome(&error, ToolRetryContract::ReadOnly)
                .expect("valid rejection fixture")
            else {
                panic!("read failure should reach the model");
            };
            assert_eq!(feedback.code, code);
            assert!(feedback.retryable);
            assert!(feedback.safe_message.as_str().contains("read-only"));
            assert_eq!(
                retry_disposition,
                RetryDisposition::DoNotRetry,
                "the model owns the next operation, not a replaying executor"
            );
            for contract in [
                ToolRetryContract::QueryBeforeRetry,
                ToolRetryContract::IdempotentByKey,
                ToolRetryContract::NotReplayable,
            ] {
                assert!(
                    dispatch_rejection_outcome(&error, contract)
                        .expect("valid rejection fixture")
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn an_undispatched_binding_failure_is_reported_as_a_tool_that_did_not_run() {
        for code in [
            ErrorCode::RuntimeSnapshotInvalid,
            ErrorCode::UnsupportedSchemaVersion,
        ] {
            let Some(ToolTerminalOutcome::Failed {
                error,
                retry_disposition,
                ..
            }) = undispatched_binding_outcome(&port(code)).expect("infallible")
            else {
                panic!("{code:?} must reach the model")
            };
            assert_eq!(error.code, code);
            assert!(error.safe_message.as_str().contains("was not run"));
            assert_eq!(retry_disposition, RetryDisposition::DoNotRetry);
        }
        for code in [
            ErrorCode::AttemptLeaseStale,
            ErrorCode::ContentIntegrityFailed,
            ErrorCode::InternalInvariantViolated,
            ErrorCode::RunTransitionInvalid,
            ErrorCode::CreditsExhausted,
        ] {
            assert_eq!(
                undispatched_binding_outcome(&port(code)).expect("infallible"),
                None,
                "{code:?}"
            );
        }
    }

    #[test]
    fn runtime_dispatch_errors_are_never_hidden_as_tool_rejections() {
        for code in [
            ErrorCode::RuntimeSnapshotInvalid,
            ErrorCode::ContentIntegrityFailed,
            ErrorCode::DependencyUnavailable,
            ErrorCode::AttemptLeaseStale,
            ErrorCode::InternalInvariantViolated,
        ] {
            assert_eq!(
                dispatch_rejection_outcome(&port(code), ToolRetryContract::QueryBeforeRetry)
                    .expect("classification is infallible"),
                None,
                "{code:?} must remain an Attempt-level port failure"
            );
        }
    }
}
