//! The complete physical model operation and its original durable boundary are
//! supervised in the existing Loop task. No wrapper drops the reactor on cancel.
use super::model_step::{ModelStep, ModelStepContext};
use super::*;
use musteragent_execution_liveness::{
    Closure, ControlStop, LivenessJournalError, StopReason as ExecutionStopReason,
};
use musteragent_execution_supervision::{
    ExecutionBoundary, ExecutionSupervisionError, ExecutionSupervisionFailureOrigin,
    ExecutionSupervisionOutcome,
};
use musteragent_runtime_contract::execution_liveness::ExecutionScope;

mod output_flush;
mod pump;
mod supervision;
mod telemetry;
use telemetry::ModelTelemetry;

impl DurableAgentLoop {
    async fn execute_activity_model_inner(
        &self,
        context: ModelStepContext<'_>,
        observed: &mut Option<ModelTelemetry>,
    ) -> ModelStep {
        let ModelStepContext {
            attempt,
            iteration,
            execution,
            supervisor,
            ..
        } = context;
        let Some(profile) = execution.snapshot.liveness else {
            return interrupted(CancellationCause::SystemShutdown);
        };
        let Some(supervision) = &self.execution_supervision else {
            return interrupted(CancellationCause::SystemShutdown);
        };
        let policy = profile.model_policy();
        let windows = policy.windows();
        let record = match supervision
            .load(
                attempt,
                ExecutionScope::Invocation(iteration.invocation_id),
                &policy,
            )
            .await
        {
            Ok(record) => record,
            Err(error) => {
                return self
                    .finish_supervision_error(
                        context,
                        error,
                        ExecutionSupervisionFailureOrigin::OriginalOwner,
                        false,
                        observed,
                    )
                    .await;
            }
        };
        let binding = record.binding();
        let mut operation = match self.model.execution(iteration.request.clone(), binding) {
            Ok(operation) => operation,
            Err(error) => {
                // Creating an execution performs no I/O. The original Journal
                // independently verifies its never-dispatched marker.
                let failure = provider_error(iteration.invocation_id, error);
                *observed = Some((TelemetryOutcome::Error, Some(failure.error.code)));
                return match tokio::time::timeout(
                    windows.post_exit_drain,
                    self.finish_activity_failure(context, failure),
                )
                .await
                {
                    Ok(step) => step,
                    Err(_) => interrupted(CancellationCause::SystemShutdown),
                };
            }
        };
        let (reactor, sink) =
            match supervision.attach(attempt.clone(), record, operation.stop_control()) {
                Ok(value) => value,
                Err(error) => {
                    return self
                        .finish_supervision_error(
                            context,
                            error.into(),
                            ExecutionSupervisionFailureOrigin::OriginalOwner,
                            false,
                            observed,
                        )
                        .await;
                }
            };
        let (failure_sender, failure_receiver) = tokio::sync::oneshot::channel();
        let mut stopped_result = None;
        let output_flush = std::sync::Arc::new(output_flush::OutputFlush::default());
        let cancellation = async {
            tokio::select! {
                biased;
                aborted = supervisor.cancelled(CancellationCause::UserRequested) => {
                    // Commit received output while the call is still live; a
                    // committed stop admits no further public output.
                    output_flush.before_stop(windows.cleanup).await;
                    cancellation_stop(aborted.cause)
                }
                result = failure_receiver => {
                    stopped_result = result.ok();
                    ControlStop::ExecutionFailed
                }
            }
        };
        let mut uncommitted_exit = None;
        let mut rejected_action = None;
        // Scheduling evidence only; the original Journal independently checks
        // its never-dispatched marker before it accepts a failure receipt.
        let mut boundary_polled = false;
        let boundary = async {
            boundary_polled = true;
            let consumption = match operation.preflight().await {
                Err(error) => {
                    let failure = provider_error(iteration.invocation_id, error);
                    *observed = Some((TelemetryOutcome::Error, Some(failure.error.code)));
                    drop(operation);
                    Some(ModelConsumption::Failed {
                        error: failure.error,
                        retry_after_ms: failure.retry_after_ms,
                        provider_code: failure.provider_code,
                        capacity_signal: failure.capacity_signal,
                    })
                }
                Ok(()) => {
                    let dispatched = match tokio::time::timeout(
                        windows.observation_request,
                        self.journal
                            .mark_model_dispatched(attempt, iteration.invocation_id),
                    )
                    .await
                    {
                        Ok(Ok(checkpoint)) if checkpoint == *context.started => checkpoint,
                        Ok(Err(error)) => {
                            return ExecutionBoundary::Interrupted(journal_error(error));
                        }
                        _ => {
                            return ExecutionBoundary::Interrupted(
                                ExecutionSupervisionError::JournalRequestExpired,
                            );
                        }
                    };
                    let _ = dispatched;
                    let _pumping = output_flush.pumping();
                    let consumption = self
                        .pump_activity_model(
                            context,
                            operation.into_stream(),
                            sink.as_ref(),
                            windows,
                            &mut rejected_action,
                            &output_flush,
                        )
                        .await;
                    if !output_flush.stop_requested() {
                        *observed = Some(telemetry::consumption_observation(&consumption));
                    }
                    match consumption {
                        Ok(ModelConsumption::Completed(outcome)) => {
                            return committed_boundary(
                                self.finalize_completed_model(context, outcome).await,
                                &mut uncommitted_exit,
                            );
                        }
                        Ok(ModelConsumption::ActionRejected {
                            rejection,
                            native_terminal: true,
                        }) => {
                            let step = match self
                                .finalize_model_action_rejection(
                                    attempt,
                                    context.started,
                                    *rejection,
                                )
                                .await
                            {
                                Ok(checkpoint) => ModelStep::Continue(checkpoint),
                                Err(exit) => ModelStep::Exit(exit),
                            };
                            return committed_boundary(step, &mut uncommitted_exit);
                        }
                        Ok(result) => Some(result),
                        Err(cause) => return ExecutionBoundary::Interrupted(cause_error(cause)),
                    }
                }
            };
            if let Some(consumption) = consumption {
                let _ = failure_sender.send(consumption);
            }
            // The owned operation has been released. Keep the boundary pending
            // until the reactor persists and verifies this same physical stop.
            std::future::pending().await
        };
        let outcome = reactor.run(boundary, cancellation).await;
        if let Some(exit) = uncommitted_exit {
            // Preserve the original failed commit and recovery evidence. The
            // reactor has finished its owned cleanup without asserting success.
            return ModelStep::Exit(exit);
        }
        match outcome {
            Ok(ExecutionSupervisionOutcome::Boundary(step)) => step,
            Ok(ExecutionSupervisionOutcome::Stopped { closure, .. }) => {
                let finish =
                    self.finish_stopped_model(context, closure, stopped_result, rejected_action);
                match tokio::time::timeout(windows.post_exit_drain, finish).await {
                    Ok(step) => step,
                    Err(_) => interrupted(CancellationCause::SystemShutdown),
                }
            }
            Err(failure) => {
                self.finish_supervision_error(
                    context,
                    failure.cause,
                    failure.origin,
                    boundary_polled,
                    observed,
                )
                .await
            }
        }
    }

    pub(super) async fn finish_activity_failure(
        &self,
        context: ModelStepContext<'_>,
        failure: crate::engine_support::ModelConsumptionFailure,
    ) -> ModelStep {
        let ModelStepContext {
            attempt,
            iteration,
            execution,
            started,
            ..
        } = context;
        let observation = match self
            .journal
            .load_model_observation(attempt, iteration.invocation_id)
            .await
        {
            Ok(value) => value,
            Err(error) => return interrupted(port_cause(&error)),
        };
        match self
            .finalize_model_failure(
                attempt,
                execution,
                started,
                observation,
                ClassifiedModelFailure {
                    failure: ModelFailure {
                        code: failure.error.code,
                        retryable: failure.error.retryable,
                    },
                    retry_after_ms: failure.retry_after_ms,
                    provider_code: failure.provider_code,
                    capacity_signal: failure.capacity_signal,
                },
            )
            .await
        {
            Ok(checkpoint) => ModelStep::Continue(checkpoint),
            Err(exit) => ModelStep::Exit(exit),
        }
    }

    async fn finish_stopped_model(
        &self,
        context: ModelStepContext<'_>,
        closure: Closure,
        result: Option<ModelConsumption>,
        rejected_action: Option<crate::ModelActionRejection>,
    ) -> ModelStep {
        if let Closure::StopConfirmed(reason) | Closure::StopUnconfirmed(reason) = closure {
            match reason {
                ExecutionStopReason::UserCancelled
                | ExecutionStopReason::ParentCancelled
                | ExecutionStopReason::CoordinationRequested => {
                    // Cancellation must retain the original committed provider
                    // prefix, including usage already observed before the stop.
                    let observation = match self
                        .journal
                        .load_model_observation(context.attempt, context.iteration.invocation_id)
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => return interrupted(port_cause(&error)),
                    };
                    let cause = if reason == ExecutionStopReason::CoordinationRequested {
                        CancellationCause::CoordinationRequested
                    } else {
                        CancellationCause::UserRequested
                    };
                    return self
                        .stop_for_cancellation(
                            context.attempt,
                            context.supervisor,
                            cause,
                            Some(observation),
                        )
                        .await
                        .into();
                }
                ExecutionStopReason::OwnershipLost => {
                    return interrupted(CancellationCause::LeaseLost);
                }
                ExecutionStopReason::AuthorizationLost | ExecutionStopReason::SystemShutdown => {
                    return interrupted(CancellationCause::SystemShutdown);
                }
                _ => {}
            }
            // A timeout or cancellation can drop the owned pump before it
            // returns. Preserve its first semantic rejection without inventing
            // any physical stop evidence. User cancellation above still wins.
            let rejection = match &result {
                Some(ModelConsumption::ActionRejected { rejection, .. }) => {
                    Some(rejection.rejection.clone())
                }
                _ => rejected_action,
            };
            if let Some(rejection) = rejection {
                let observation = match self
                    .journal
                    .load_model_observation(context.attempt, context.iteration.invocation_id)
                    .await
                {
                    Ok(value) => value,
                    Err(error) => return interrupted(port_cause(&error)),
                };
                return match self
                    .finalize_model_action_rejection(
                        context.attempt,
                        context.started,
                        crate::ModelActionRejectionCommit {
                            observation,
                            rejection,
                        },
                    )
                    .await
                {
                    Ok(checkpoint) => ModelStep::Continue(checkpoint),
                    Err(exit) => ModelStep::Exit(exit),
                };
            }
        }
        let failure = match result {
            Some(ModelConsumption::Failed {
                error,
                retry_after_ms,
                provider_code,
                capacity_signal,
                ..
            }) => crate::engine_support::ModelConsumptionFailure {
                error,
                retry_after_ms,
                provider_code,
                capacity_signal,
            },
            Some(ModelConsumption::ActionRejected { rejection, .. }) => {
                port(rejection.rejection.code(), false).into()
            }
            _ => {
                let timeout = matches!(
                    closure,
                    Closure::StopConfirmed(
                        ExecutionStopReason::StartupExpired
                            | ExecutionStopReason::IdleExpired
                            | ExecutionStopReason::BusinessDeadlineExpired
                            | ExecutionStopReason::PostExitDrainExpired
                    ) | Closure::StopUnconfirmed(
                        ExecutionStopReason::StartupExpired
                            | ExecutionStopReason::IdleExpired
                            | ExecutionStopReason::BusinessDeadlineExpired
                            | ExecutionStopReason::PostExitDrainExpired
                    )
                );
                port(
                    if timeout {
                        ErrorCode::DependencyTimeout
                    } else {
                        ErrorCode::DependencyUnavailable
                    },
                    true,
                )
                .into()
            }
        };
        // The original invocation is sealed, including any unknown remote stop.
        // Recoverable dependency failures use the existing model recovery policy;
        // cleanup uncertainty does not impose a lifetime on the Agent's Run.
        self.finish_activity_failure(context, failure).await
    }
}
fn interrupted(cause: CancellationCause) -> ModelStep {
    ModelStep::Exit(AgentLoopExit::Interrupted { cause })
}
fn port_cause(error: &LoopPortError) -> CancellationCause {
    if error.code == ErrorCode::AttemptLeaseStale {
        CancellationCause::LeaseLost
    } else {
        CancellationCause::SystemShutdown
    }
}
fn journal_error(error: LoopPortError) -> ExecutionSupervisionError {
    ExecutionSupervisionError::Journal(LivenessJournalError {
        code: error.code,
        retryable: error.retryable,
        rejection: None,
    })
}
fn cause_error(cause: CancellationCause) -> ExecutionSupervisionError {
    journal_error(port(
        if cause == CancellationCause::LeaseLost {
            ErrorCode::AttemptLeaseStale
        } else {
            ErrorCode::DependencyUnavailable
        },
        true,
    ))
}
fn cancellation_stop(cause: CancellationCause) -> ControlStop {
    match cause {
        CancellationCause::UserRequested => ControlStop::UserCancelled,
        CancellationCause::ParentCancelled => ControlStop::ParentCancelled,
        CancellationCause::LeaseLost => ControlStop::OwnershipLost,
        CancellationCause::SystemShutdown => ControlStop::SystemShutdown,
        CancellationCause::CoordinationRequested => ControlStop::CoordinationRequested,
    }
}
fn committed_boundary(
    step: ModelStep,
    uncommitted_exit: &mut Option<AgentLoopExit>,
) -> ExecutionBoundary<ModelStep> {
    match &step {
        ModelStep::Continue(_) | ModelStep::Completed { .. } => ExecutionBoundary::Committed(step),
        ModelStep::Exit(
            AgentLoopExit::Failed {
                receipt: Some(_), ..
            }
            | AgentLoopExit::Cancelled { .. }
            | AgentLoopExit::CoordinationInterrupted { .. }
            | AgentLoopExit::ExternalOutcomeUnknown { .. },
        ) => ExecutionBoundary::Committed(step),
        ModelStep::Exit(AgentLoopExit::Interrupted { cause }) => {
            ExecutionBoundary::Interrupted(cause_error(*cause))
        }
        _ => {
            if let ModelStep::Exit(exit) = step {
                *uncommitted_exit = Some(exit);
            }
            ExecutionBoundary::Interrupted(cause_error(CancellationCause::SystemShutdown))
        }
    }
}
