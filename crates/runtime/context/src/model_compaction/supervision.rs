//! One reactor for Cloud and Device Compression. The caller retains the
//! original dispatch fence, content authority, usage and terminal transaction.
use super::*;
use musteragent_execution_liveness::{
    Closure, ControlStop, ExecutionRecord, ExecutionStopConfirmation, ExecutionStopControl,
    ExecutionStopError, StopReason as ControlReason,
};
use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionSupervision, ExecutionSupervisionError, ExecutionSupervisionOutcome,
};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::future::Future;

impl ContextCompactionModelInvoker {
    /// `complete` returns Committed only after the original terminal/usage and
    /// liveness transaction succeeds. It is called exactly once at most.
    pub async fn invoke_supervised<D, C, F>(
        &self,
        request: &ContextCompactionRequest,
        record: ExecutionRecord,
        supervision: &ExecutionSupervision,
        dispatch: D,
        complete: C,
    ) -> Result<crate::ContextCompactionStep, ContextPlanError>
    where
        D: Future<Output = Result<(), ContextPlanError>>,
        C: FnOnce(Result<ContextCompactionModelOutput, ContextCompactionModelFailure>, bool) -> F,
        F: Future<
            Output = ExecutionBoundary<Result<crate::ContextCompactionStep, ContextPlanError>>,
        >,
    {
        let windows = record.policy().windows();
        let binding = record.binding();
        let prepared = self.execution(request, request.invocation.clone(), binding);
        let control: Arc<dyn ExecutionStopControl> = match &prepared {
            Ok(operation) => operation.stop_control(),
            Err(_) => Arc::new(NeverDispatched(binding)),
        };
        let (reactor, sink) = supervision
            .attach(request.invocation.attempt_context.clone(), record, control)
            .map_err(|_| ContextPlanError::invariant())?;
        let mut observed = CompactionExecutionObservation::default();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut failure = None;
        let mut complete = Some(complete);
        let cancellation = async {
            failure = receiver.await.ok();
            if failure
                .as_ref()
                .is_some_and(|value: &ContextCompactionModelFailure| {
                    value.error.code == ErrorCode::DependencyTimeout
                })
            {
                ControlStop::RequestTimedOut
            } else {
                ControlStop::ExecutionFailed
            }
        };
        let boundary = async {
            let mut result = match prepared {
                Err(error) => Err(error),
                Ok(mut operation) => match operation.preflight().await {
                    Err(error) => Err(ContextCompactionModelFailure::without_usage(
                        provider_failure(&error, false),
                    )),
                    Ok(()) => match dispatch.await {
                        Err(error) => Err(ContextCompactionModelFailure::without_usage(error)),
                        Ok(()) => {
                            self.consume_execution(
                                request,
                                &request.invocation,
                                operation,
                                sink.as_ref(),
                                &mut observed,
                            )
                            .await
                        }
                    },
                },
            };
            if !request.capture_provider_failure
                && let Err(failure) = &mut result
            {
                failure.error.early_provider_cause = None;
            }
            if result.is_ok() || observed.terminal_observed() {
                let Some(complete) = complete.take() else {
                    return interrupted(ContextPlanError::invariant());
                };
                return complete(result, observed.terminal_observed()).await;
            }
            if let Err(error) = result {
                let _ = sender.send(error);
            }
            std::future::pending().await
        };
        match reactor.run(boundary, cancellation).await {
            Ok(ExecutionSupervisionOutcome::Boundary(result)) => result,
            Ok(ExecutionSupervisionOutcome::Stopped { closure, .. }) => {
                let mut error = failure.unwrap_or_else(|| ContextCompactionModelFailure {
                    error: stopped(closure),
                    usage: observed.usage(),
                });
                if matches!(closure, Closure::StopUnconfirmed(_)) {
                    error.error.early_provider_cause = None;
                }
                let complete = complete
                    .take()
                    .ok_or_else(ContextPlanError::indeterminate)?;
                let result =
                    tokio::time::timeout(windows.post_exit_drain, complete(Err(error), false))
                        .await
                        .map_err(|_| ContextPlanError::indeterminate())?;
                match result {
                    ExecutionBoundary::Committed(result) => result,
                    ExecutionBoundary::Interrupted(error) => Err(supervision_error(error)),
                }
            }
            Err(error) => Err(supervision_error(error.cause)),
        }
    }
}

fn interrupted(
    error: ContextPlanError,
) -> ExecutionBoundary<Result<crate::ContextCompactionStep, ContextPlanError>> {
    ExecutionBoundary::Interrupted(
        musteragent_execution_liveness::LivenessJournalError {
            code: error.code,
            retryable: error.retryable,
            rejection: None,
        }
        .into(),
    )
}
fn supervision_error(error: ExecutionSupervisionError) -> ContextPlanError {
    match error {
        ExecutionSupervisionError::Journal(error) => ContextPlanError {
            early_provider_cause: None,
            code: error.code,
            retryable: error.retryable,
            retry_after_ms: None,
        },
        _ => ContextPlanError::indeterminate(),
    }
}
fn stopped(closure: Closure) -> ContextPlanError {
    match closure {
        Closure::StopConfirmed(
            ControlReason::StartupExpired
            | ControlReason::IdleExpired
            | ControlReason::BusinessDeadlineExpired
            | ControlReason::PostExitDrainExpired
            | ControlReason::RequestTimedOut,
        )
        | Closure::StopUnconfirmed(
            ControlReason::StartupExpired
            | ControlReason::IdleExpired
            | ControlReason::BusinessDeadlineExpired
            | ControlReason::PostExitDrainExpired
            | ControlReason::RequestTimedOut,
        ) => ContextPlanError::timeout(),
        _ => ContextPlanError::unavailable(),
    }
}
struct NeverDispatched(ExecutionBinding);
impl ExecutionStopControl for NeverDispatched {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }
    fn stop(
        &self,
        _: ControlReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async { Ok(ExecutionStopConfirmation { binding: self.0 }) })
    }
}
