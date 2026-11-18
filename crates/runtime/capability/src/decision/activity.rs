use super::*;
use crate::auxiliary_execution::{AuxiliaryActivity, NeverDispatched, stopped_code};
use musteragent_execution_liveness::{
    Closure, ControlStop, ExecutionStopControl, LivenessJournalError,
};
use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionSupervisionError, ExecutionSupervisionOutcome,
};
use musteragent_runtime_model::{SupervisedModelAdmission, SupervisedModelStoreError};

impl<S: AdmittedModelInvocationStore> PersistedDecisionCapability<S> {
    pub(super) async fn execute_activity(
        &self,
        request: &DecisionExecutionRequest,
        mut invocation: ModelInvocation,
        admission: AdmittedModelInvocation,
        authority: ModelRequestAuthority,
        evaluation: DecisionEvaluation,
        parent: Option<crate::OperationContext>,
    ) -> Result<CapabilityResult, DecisionCapabilityError> {
        let supervision = self
            .supervision
            .as_ref()
            .ok_or(ErrorCode::RuntimeSnapshotInvalid)?;
        let context = &request.model_invocation_context;
        let start = self
            .store
            .begin_supervised(SupervisedModelAdmission {
                context: context.clone(),
                admission,
            })
            .await
            .map_err(store_error::<S>)?;
        if start.disposition == ModelPersistenceDisposition::Duplicate {
            return Err(ErrorCode::IdempotencyConflict.into());
        }
        let windows = start.record.policy().windows();
        let binding = start.record.binding();
        let operation = self.provider.execution(
            DecisionModelRequest {
                authority,
                evaluation: evaluation.clone(),
            },
            binding,
        );
        let control: Arc<dyn ExecutionStopControl> = match &operation {
            Ok(operation) => operation.stop_control(),
            Err(_) => Arc::new(NeverDispatched(binding)),
        };
        let mut dependency = parent.as_ref().map(crate::OperationContext::dependency);
        let (reactor, sink) = supervision
            .attach(context.attempt_context.clone(), start.record, control)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?;
        let activity = AuxiliaryActivity::new(sink, parent.clone());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut native_failure = None;
        let cancellation = async {
            tokio::select! {
                biased;
                () = async { match &parent { Some(parent) => parent.cancelled().await, None => std::future::pending().await } } => ControlStop::ParentCancelled,
                value = receiver => {
                    native_failure = value.ok();
                    if native_failure.as_ref().is_some_and(|value: &musteragent_runtime_model::DecisionModelFailure| value.error.safe_code.runtime_code() == ErrorCode::DependencyTimeout) { ControlStop::RequestTimedOut } else { ControlStop::ExecutionFailed }
                }
            }
        };
        let boundary = async {
            let result = match operation {
                Ok(operation) => operation.execute(activity.clone()).await,
                Err(error) => Err(error.into()),
            };
            match result {
                Ok(response) if activity.terminal_observed() => {
                    let result = self
                        .finish_response_boundary(
                            request,
                            &mut invocation,
                            &evaluation,
                            response,
                            Some((context, true)),
                        )
                        .await;
                    match result {
                        Ok(result) => return ExecutionBoundary::Committed(result),
                        Err(error) => return ExecutionBoundary::Interrupted(boundary_error(error)),
                    }
                }
                Ok(response) => {
                    // Returning a value is not physical-stop evidence. Keep paid
                    // usage while the original owner settles the broken adapter.
                    let _ = sender.send(musteragent_runtime_model::DecisionModelFailure {
                        error: musteragent_runtime_model::ProviderError {
                            kind: musteragent_runtime_model::ProviderErrorKind::Protocol,
                            safe_code:
                                musteragent_runtime_model::ProviderErrorCode::ProtocolViolation,
                            retryable: false,
                            retry_after_ms: None,
                            diagnostic_ref: None,
                        },
                        usage: Some(response.usage),
                    });
                }
                Err(failure) if activity.terminal_observed() => {
                    invocation.usage = failure.usage.and_then(model_usage);
                    let error = provider_error(failure.error);
                    match self
                        .complete(&mut invocation, Some(&error), Some((context, true)))
                        .await
                    {
                        Ok(()) => return ExecutionBoundary::Committed(Err(error)),
                        Err(error) => return ExecutionBoundary::Interrupted(boundary_error(error)),
                    }
                }
                Err(error) => {
                    let _ = sender.send(error);
                }
            }
            std::future::pending().await
        };
        match reactor.run(boundary, cancellation).await {
            Ok(ExecutionSupervisionOutcome::Boundary(result)) => {
                if let Some(dependency) = &mut dependency {
                    dependency.confirm_stopped();
                }
                result
            }
            Ok(ExecutionSupervisionOutcome::Stopped { closure, .. }) => {
                if matches!(closure, Closure::StopConfirmed(_))
                    && let Some(dependency) = &mut dependency
                {
                    dependency.confirm_stopped();
                }
                let error = match native_failure {
                    Some(failure) => {
                        invocation.usage = failure.usage.and_then(model_usage);
                        provider_error(failure.error)
                    }
                    None => DecisionCapabilityError {
                        code: stopped_code(closure),
                        retryable: true,
                        retry_after_ms: None,
                    },
                };
                tokio::time::timeout(
                    windows.post_exit_drain,
                    self.complete(&mut invocation, Some(&error), Some((context, false))),
                )
                .await
                .map_err(|_| ErrorCode::DependencyUnavailable)??;
                Err(error)
            }
            Err(error) => {
                if error.physical_stop.is_some()
                    && let Some(dependency) = &mut dependency
                {
                    dependency.confirm_stopped();
                }
                Err(match error.cause {
                    ExecutionSupervisionError::Journal(error) => DecisionCapabilityError {
                        code: error.code,
                        retryable: error.retryable,
                        retry_after_ms: None,
                    },
                    _ => ErrorCode::DependencyUnavailable.into(),
                })
            }
        }
    }
}
fn provider_error(error: musteragent_runtime_model::ProviderError) -> DecisionCapabilityError {
    DecisionCapabilityError {
        code: error.safe_code.runtime_code(),
        retryable: error.permits_dependency_recovery(),
        retry_after_ms: error.retry_after_ms,
    }
}
fn boundary_error(error: DecisionCapabilityError) -> ExecutionSupervisionError {
    LivenessJournalError {
        code: error.code,
        retryable: error.retryable,
        rejection: None,
    }
    .into()
}
pub(super) fn store_error<S: AdmittedModelInvocationStore>(
    error: SupervisedModelStoreError<S::Error>,
) -> DecisionCapabilityError {
    match error {
        SupervisedModelStoreError::Storage(error) => storage_error::<S>(error),
        SupervisedModelStoreError::Invalid(code) => code.into(),
        SupervisedModelStoreError::Liveness(error) => DecisionCapabilityError {
            code: error.code,
            retryable: error.retryable,
            retry_after_ms: None,
        },
        SupervisedModelStoreError::Unsupported => ErrorCode::RuntimeSnapshotInvalid.into(),
    }
}
