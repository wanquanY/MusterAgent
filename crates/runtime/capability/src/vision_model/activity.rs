//! Auxiliary vision uses the same Home reactor as the main model call. Its
//! original model row, usage and liveness closure form one durable boundary.
use super::*;
use crate::auxiliary_execution::{NeverDispatched, stopped_code};
use musteragent_execution_liveness::{
    Closure, ControlStop, ExecutionActivitySink, ExecutionStopControl, LivenessError,
};
use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionSupervisionError, ExecutionSupervisionOutcome,
};
use musteragent_runtime_contract::execution_liveness::ActivitySource;
use musteragent_runtime_model::{
    ModelStreamProgress, SupervisedModelAdmission, SupervisedModelCompletion,
    SupervisedModelStoreError,
};

#[derive(Default)]
pub(super) struct Observed {
    pub usage: Option<ModelUsage>,
    terminal: bool,
    cursor: u64,
}
impl Observed {
    pub(super) fn record(
        &mut self,
        progress: ModelStreamProgress,
        sink: &dyn ExecutionActivitySink,
        parent: Option<&crate::OperationContext>,
    ) -> Result<(), LivenessError> {
        match progress {
            ModelStreamProgress::Accepted => {
                if sink.accepted()?
                    == musteragent_execution_liveness::ExecutionAcceptance::AlreadyObserved
                {
                    return Ok(());
                }
            }
            ModelStreamProgress::Advanced => {
                self.cursor = self
                    .cursor
                    .checked_add(1)
                    .ok_or(LivenessError::ScopeClosed)?;
                sink.advanced(
                    ActivitySource::CanonicalModelEvent,
                    NonZeroU64::new(self.cursor).ok_or(LivenessError::ScopeClosed)?,
                )?;
            }
            ModelStreamProgress::Terminal => {
                self.terminal = true;
                sink.ended()?;
            }
            ModelStreamProgress::Unchanged => return Ok(()),
        }
        if let Some(parent) = parent {
            parent.completed_stage()?;
        }
        Ok(())
    }
}

impl<S: AdmittedModelInvocationStore> PersistedVisionModelInvoker<S> {
    pub(super) async fn persist_completion(
        &self,
        completion: CompleteModelInvocation,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<(), VisionCapabilityError> {
        if let Some((context, native_terminal)) = supervised {
            self.store
                .complete_supervised(SupervisedModelCompletion {
                    context: context.clone(),
                    completion,
                    native_terminal,
                })
                .await
                .map_err(store_failure::<S>)?;
        } else {
            self.store
                .complete(completion)
                .await
                .map_err(|error| classified_storage_failure::<S>(&error))?;
        }
        Ok(())
    }

    pub(super) async fn analyze_activity(
        &self,
        request: VisionModelInvocation,
        canonical: CanonicalModelRequest,
        mut persisted: ModelInvocation,
        admission: AdmittedModelInvocation,
        parent: Option<crate::OperationContext>,
    ) -> Result<VisionModelResult, VisionCapabilityError> {
        let supervision = self
            .supervision
            .as_ref()
            .ok_or(VisionCapabilityError::IdentityMismatch)?;
        let context = &request.invocation_context;
        let start = self
            .store
            .begin_supervised(SupervisedModelAdmission {
                context: context.clone(),
                admission,
            })
            .await
            .map_err(store_failure::<S>)?;
        if start.disposition == ModelPersistenceDisposition::Duplicate {
            return Err(failure(ErrorCode::IdempotencyConflict, false));
        }
        let windows = start.record.policy().windows();
        let binding = start.record.binding();
        let prepared = self.provider.execution(canonical, binding);
        let control: Arc<dyn ExecutionStopControl> = match &prepared {
            Ok(operation) => operation.stop_control(),
            // The execution factory is forbidden from doing I/O. Its error
            // proves this original call was never dispatched, not a remote stop.
            Err(_) => Arc::new(NeverDispatched(binding)),
        };
        let mut dependency = parent.as_ref().map(crate::OperationContext::dependency);
        let (reactor, sink) = supervision
            .attach(context.attempt_context.clone(), start.record, control)
            .map_err(|_| failure(ErrorCode::RuntimeSnapshotInvalid, false))?;
        let mut observed = Observed::default();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut provider_failure = None;
        let cancellation = async {
            tokio::select! {
                biased;
                () = parent_cancelled(parent.as_ref()) => ControlStop::ParentCancelled,
                value = receiver => {
                    provider_failure = value.ok();
                    if provider_failure.as_ref().is_some_and(|value: &VisionStreamFailure| value.code == ErrorCode::DependencyTimeout) {
                        ControlStop::RequestTimedOut
                    } else { ControlStop::ExecutionFailed }
                }
            }
        };
        let boundary = async {
            let mut output = match prepared {
                Err(error) => Err(VisionStreamFailure::provider(error, None, true)),
                Ok(mut operation) => match operation.preflight().await {
                    Err(error) => Err(VisionStreamFailure::provider(error, None, true)),
                    Ok(()) => {
                        consume_events(
                            operation.into_stream(),
                            None,
                            Some(sink.as_ref()),
                            parent.as_ref(),
                            &mut observed,
                        )
                        .await
                    }
                },
            };
            if !request.capture_provider_failure
                && let Err(failure) = &mut output
            {
                failure.early_provider_cause = None;
            }
            match output {
                Ok(output) => {
                    match self
                        .complete_success(&mut persisted, output, &request, Some((context, true)))
                        .await
                    {
                        Ok(result) => return ExecutionBoundary::Committed(Ok(result)),
                        Err(error) => return ExecutionBoundary::Interrupted(boundary_error(error)),
                    }
                }
                Err(error) if observed.terminal => {
                    match self
                        .complete_failure(&mut persisted, &error, Some((context, true)))
                        .await
                    {
                        Ok(()) => {
                            return ExecutionBoundary::Committed(Err(error.capability_error()));
                        }
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
                let mut error =
                    provider_failure.unwrap_or_else(|| stopped_failure(closure, observed.usage));
                if matches!(closure, Closure::StopUnconfirmed(_)) {
                    error.early_provider_cause = None;
                }
                tokio::time::timeout(
                    windows.post_exit_drain,
                    self.complete_failure(&mut persisted, &error, Some((context, false))),
                )
                .await
                .map_err(|_| failure(ErrorCode::DependencyUnavailable, true))??;
                Err(error.capability_error())
            }
            Err(error) => {
                if error.physical_stop.is_some()
                    && let Some(dependency) = &mut dependency
                {
                    dependency.confirm_stopped();
                }
                Err(supervision_failure(error.cause))
            }
        }
    }
}

async fn parent_cancelled(parent: Option<&crate::OperationContext>) {
    match parent {
        Some(parent) => parent.cancelled().await,
        None => std::future::pending().await,
    }
}
fn store_failure<S: AdmittedModelInvocationStore>(
    error: SupervisedModelStoreError<S::Error>,
) -> VisionCapabilityError {
    match error {
        SupervisedModelStoreError::Storage(error) => classified_storage_failure::<S>(&error),
        SupervisedModelStoreError::Liveness(error) => failure(error.code, error.retryable),
        SupervisedModelStoreError::Invalid(code) => failure(code, false),
        SupervisedModelStoreError::Unsupported => failure(ErrorCode::RuntimeSnapshotInvalid, false),
    }
}
fn failure(code: ErrorCode, retryable: bool) -> VisionCapabilityError {
    VisionCapabilityError::ModelFailure {
        early_provider_cause: None,
        code,
        retryable,
        retry_after_ms: None,
    }
}
fn boundary_error(error: VisionCapabilityError) -> ExecutionSupervisionError {
    let (code, retryable) = match error {
        VisionCapabilityError::ModelFailure {
            code, retryable, ..
        } => (code, retryable),
        _ => (ErrorCode::DependencyUnavailable, true),
    };
    musteragent_execution_liveness::LivenessJournalError {
        code,
        retryable,
        rejection: None,
    }
    .into()
}
fn supervision_failure(error: ExecutionSupervisionError) -> VisionCapabilityError {
    match error {
        ExecutionSupervisionError::Journal(error) => failure(error.code, error.retryable),
        _ => failure(ErrorCode::DependencyUnavailable, true),
    }
}
fn stopped_failure(closure: Closure, usage: Option<ModelUsage>) -> VisionStreamFailure {
    VisionStreamFailure {
        early_provider_cause: None,
        reason: "supervised_stop",
        code: stopped_code(closure),
        retryable: true,
        retry_after_ms: None,
        usage,
    }
}
