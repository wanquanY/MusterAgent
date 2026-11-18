//! Fetch the original physical result under Home's bounded recovery ticket.
//! Replay is buffered until a validated terminal; it never renews activity.
use super::model_step::{ModelStep, ModelStepContext};
use super::*;
use crate::RecoverableModelInvocation;
use crate::engine_support::{ModelConsumptionFailure, unexposed_provider_tool_name};
use crate::model_consumption::CanonicalConsumptionState;

impl DurableAgentLoop {
    pub(super) async fn execute_recovered_model_step(
        &self,
        context: ModelStepContext<'_>,
        recovery: &RecoverableModelInvocation,
    ) -> ModelStep {
        let result = tokio::select! {
            biased;
            aborted = context.supervisor.cancelled(CancellationCause::UserRequested) => {
                if aborted.cause == CancellationCause::LeaseLost {
                    return ModelStep::Exit(AgentLoopExit::Interrupted { cause: aborted.cause });
                }
                if self.journal.close_model_recovery(context.attempt, recovery.iteration.invocation_id).await.is_err() {
                    return ModelStep::Exit(AgentLoopExit::Interrupted { cause: aborted.cause });
                }
                return ModelStep::Exit(self.stop_for_cancellation(context.attempt, context.supervisor, aborted.cause, None).await);
            }
            result = tokio::time::timeout(recovery.timeout, self.collect_recovered_model(context, recovery)) => {
                result.unwrap_or_else(|_| Err(provider_error(recovery.iteration.invocation_id,
                    musteragent_runtime_model::ProviderError {
                        kind: musteragent_runtime_model::ProviderErrorKind::Timeout,
                        safe_code: musteragent_runtime_model::ProviderErrorCode::ExistingInvocationTimeout,
                        retryable: false, retry_after_ms: None, diagnostic_ref: None,
                    })))
            }
        };
        match result {
            Ok((outcome, events)) => {
                if let Err(error) = self
                    .journal
                    .append_recovered_model_events(context.attempt, recovery, events)
                    .await
                {
                    tracing::warn!(
                        code = error.code.as_str(),
                        "Recovered model terminal batch was not committed"
                    );
                    return ModelStep::Exit(AgentLoopExit::Interrupted {
                        cause: if error.code == ErrorCode::AttemptLeaseStale {
                            CancellationCause::LeaseLost
                        } else {
                            CancellationCause::SystemShutdown
                        },
                    });
                }
                self.finalize_completed_model(context, outcome).await
            }
            Err(mut failure) => {
                // A lookup failure is never permission to create a new paid
                // generation, including malformed or truncated native replay.
                failure.error.retryable = false;
                if let Err(error) = self
                    .journal
                    .close_model_recovery(context.attempt, recovery.iteration.invocation_id)
                    .await
                {
                    return ModelStep::Exit(AgentLoopExit::Interrupted {
                        cause: if error.code == ErrorCode::AttemptLeaseStale {
                            CancellationCause::LeaseLost
                        } else {
                            CancellationCause::SystemShutdown
                        },
                    });
                }
                self.finish_activity_failure(context, failure).await
            }
        }
    }

    async fn collect_recovered_model(
        &self,
        context: ModelStepContext<'_>,
        recovery: &RecoverableModelInvocation,
    ) -> Result<
        (
            ModelIterationOutcome,
            Vec<musteragent_runtime_model::ModelStreamEvent>,
        ),
        ModelConsumptionFailure,
    > {
        let iteration = context.iteration;
        let invalid = || port(ErrorCode::ModelResponseInvalid, false);
        if iteration.request.stream_budget != context.execution.snapshot.model_stream {
            return Err(invalid().into());
        }
        let mut operation = self
            .model
            .recover_execution(iteration.request.clone(), recovery.binding)
            .map_err(|error| provider_error(iteration.invocation_id, error))?;
        if operation.binding() != recovery.binding
            || operation.stop_control().binding() != recovery.binding
        {
            return Err(invalid().into());
        }
        operation
            .preflight()
            .await
            .map_err(|error| provider_error(iteration.invocation_id, error))?;
        let mut stream = operation.into_stream();
        let budget = iteration.request.stream_budget;
        let mut state = CanonicalConsumptionState::new(
            context.execution.snapshot.max_output_frame_bytes.get(),
            context.execution.snapshot.max_model_events_per_commit.get(),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            let event = event.map_err(|error| provider_error(iteration.invocation_id, error))?;
            if unexposed_provider_tool_name(&iteration.request, &event).is_some()
                || super::delivery_repair::tool_violation(
                    iteration.delivery_repair.as_ref(),
                    &event,
                )
                .is_some()
            {
                return Err(invalid().into());
            }
            let staged = state.stage_event(
                iteration.invocation_id,
                event,
                budget,
                |call, name, json| {
                    self.policy
                        .preview_tool_arguments(context.attempt, call, name, json)
                },
            )?;
            if let Some(error) = staged.provider_failure {
                return Err(provider_error(iteration.invocation_id, error));
            }
            state = staged.state;
            let count = state.coalescer.ready_len();
            events.extend(state.coalescer.ready_batch(count));
            state
                .coalescer
                .acknowledge_ready(count)
                .map_err(|_| invalid())?;
        }
        state.coalescer.flush_pending().map_err(|_| invalid())?;
        let count = state.coalescer.ready_len();
        events.extend(state.coalescer.ready_batch(count));
        state
            .coalescer
            .acknowledge_ready(count)
            .map_err(|_| invalid())?;
        let outcome = state.into_outcome(iteration.request.route.provider_state_policy, budget)?;
        Ok((outcome, events))
    }
}
