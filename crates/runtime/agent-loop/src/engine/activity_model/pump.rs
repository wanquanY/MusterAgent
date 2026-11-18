//! Single-task canonical reader and ordered Journal writer. A slow commit never
//! prevents polling the model; bounded backlog exhaustion is a local fault,
//! never a manufactured Provider-idle observation.
use super::*;
use crate::engine::model_execution::ModelDrainError;
use crate::engine_support::{
    ModelConsumptionFailure, checkpoint_matches, unexposed_provider_tool_name,
};
use crate::model_consumption::CanonicalConsumptionState;
use musteragent_execution_liveness::ExecutionActivitySink;
use musteragent_runtime_contract::{ApplicationFuture, execution_liveness::ActivitySource};
use musteragent_runtime_model::{ModelEventStream, ModelStreamProgress};
use std::num::NonZeroU64;

enum Ending {
    Eof,
    Failed(ModelConsumptionFailure),
    Rejected(Box<crate::ModelActionRejection>),
}
type Commit<'a> = ApplicationFuture<'a, Result<DurableLoopCheckpoint, ModelDrainError>>;

impl DurableAgentLoop {
    pub(super) async fn pump_activity_model(
        &self,
        context: ModelStepContext<'_>,
        mut stream: ModelEventStream<'_>,
        sink: &dyn ExecutionActivitySink,
        windows: musteragent_runtime_contract::execution_liveness::ExecutionLivenessWindows,
        rejected_action: &mut Option<crate::ModelActionRejection>,
        output_flush: &super::output_flush::OutputFlush,
    ) -> Result<ModelConsumption, CancellationCause> {
        let ModelStepContext {
            attempt,
            iteration,
            execution,
            ..
        } = context;
        if iteration.request.stream_budget != execution.snapshot.model_stream {
            return Err(CancellationCause::SystemShutdown);
        }
        let budget = execution.snapshot.model_stream;
        let batch_limit = execution.snapshot.max_model_events_per_commit.get();
        // Two ordinary batches plus one maximum-size canonical event, its
        // possible document preview, and the terminal release of all admitted
        // Tool boundaries. Every term comes from the original sealed limits.
        let backlog_limit = batch_limit
            .saturating_mul(2)
            .saturating_add(
                budget
                    .max_event_bytes
                    .get()
                    .div_ceil(execution.snapshot.max_output_frame_bytes.get()),
            )
            .saturating_add(
                budget
                    .max_tool_argument_bytes
                    .get()
                    .div_ceil(execution.snapshot.max_output_frame_bytes.get()),
            )
            .saturating_add((budget.max_tool_calls.get() as usize).saturating_mul(2));
        let mut state = CanonicalConsumptionState::new(
            execution.snapshot.max_output_frame_bytes.get(),
            batch_limit,
        );
        let mut pending: Option<Commit<'_>> = None;
        let mut pending_count = 0;
        let mut batch_index = 0_u32;
        let mut flush_due = None;
        let mut native_ended = false;
        let mut rejection_due = None;
        let mut flush_ready = false;
        let mut work = 0_u64;
        let mut stop_flush_requested = false;
        let mut stop_flush_reported = false;
        let ending = loop {
            if stop_flush_requested
                && !stop_flush_reported
                && pending.is_none()
                && state.coalescer.ready_len() == 0
            {
                output_flush.flushed();
                stop_flush_reported = true;
            }
            if state.coalescer.ready_len() > backlog_limit {
                tracing::error!(invocation_id = %iteration.invocation_id,
                    "Model Journal backlog reached its sealed bound");
                return Err(CancellationCause::SystemShutdown);
            }
            if pending.is_none()
                && state.coalescer.ready_len() > 0
                && (flush_ready || state.coalescer.ready_len() >= batch_limit)
            {
                let events = state.coalescer.ready_batch(batch_limit);
                pending_count = events.len();
                let batch = ModelEventBatch {
                    invocation_id: iteration.invocation_id,
                    iteration: iteration.iteration,
                    batch_index,
                    events,
                };
                pending = Some(Box::pin(async move {
                    tokio::time::timeout(
                        windows.observation_request,
                        self.append_model_batch_confirmed(attempt, batch),
                    )
                    .await
                    .map_err(|_| ModelDrainError::Persistence(CancellationCause::SystemShutdown))?
                }));
            }
            tokio::select! {
                biased;
                committed = async { pending.as_mut().expect("guarded pending commit").await }, if pending.is_some() => {
                    let checkpoint = committed.map_err(drain_cause)?;
                    if !checkpoint_matches(attempt, &checkpoint) { return Err(CancellationCause::SystemShutdown); }
                    state.coalescer.acknowledge_ready(pending_count).map_err(|_| CancellationCause::SystemShutdown)?;
                    batch_index = batch_index.checked_add(1).ok_or(CancellationCause::SystemShutdown)?;
                    pending = None;
                    flush_ready = state.coalescer.ready_len() > 0;
                }
                () = optional_deadline(flush_due), if flush_due.is_some() => {
                    state.coalescer.flush_pending().map_err(|_| CancellationCause::SystemShutdown)?;
                    flush_due = None;
                    flush_ready = true;
                }
                () = output_flush.requested(), if !stop_flush_requested => {
                    state.coalescer.flush_pending().map_err(|_| CancellationCause::SystemShutdown)?;
                    flush_due = None;
                    flush_ready = true;
                    stop_flush_requested = true;
                }
                () = optional_deadline(rejection_due), if rejection_due.is_some() => {
                    break Ending::Rejected(Box::new(rejected_action.clone().expect("rejection deadline")));
                }
                next = stream.next() => {
                    let event = match next {
                        None => break rejected_action.clone().map(|rejection| Ending::Rejected(Box::new(rejection))).unwrap_or(Ending::Eof),
                        Some(Err(error)) => break rejected_action.clone().map(|rejection| Ending::Rejected(Box::new(rejection))).unwrap_or_else(|| Ending::Failed(provider_error(iteration.invocation_id, error))),
                        Some(Ok(event)) => event,
                    };
                    if rejected_action.is_none() {
                        let rejection = if let Some(violation) = crate::engine::delivery_repair::tool_violation(iteration.delivery_repair.as_ref(), &event) {
                            let binding = iteration.delivery_repair.as_ref().expect("validated repair stage").clone();
                            Some(crate::ModelActionRejection::PublicDeliveryRepairViolation { binding, violation })
                        } else {
                            unexposed_provider_tool_name(&iteration.request, &event)
                                .map(|name| crate::ModelActionRejection::ToolNotExposed { attempted_name: name.clone() })
                        };
                        if let Some(rejection) = rejection {
                            *rejected_action = Some(rejection);
                            state.reject_tool_batch();
                            // Fixed once: later bytes cannot extend this drain.
                            rejection_due = Some(tokio::time::Instant::now() + windows.post_exit_drain);
                        }
                    }
                    let ended = matches!(&event, musteragent_runtime_model::ModelStreamEvent::ResponseCompleted { .. });
                    let staged = if rejected_action.is_some() {
                        state.stage_rejected_event(event, budget)
                    } else {
                        state.stage_event(iteration.invocation_id, event, budget,
                            |call, name, json| self.policy.preview_tool_arguments(attempt, call, name, json))
                    };
                    let staged = match staged {
                        Ok(staged) => staged,
                        Err(error) => break rejected_action.clone().map(|rejection| Ending::Rejected(Box::new(rejection))).unwrap_or_else(|| Ending::Failed(error.into())),
                    };
                    // Exposure, cumulative budgets, Tool identities, arguments,
                    // previews and canonical order all passed before stamping.
                    match staged.progress {
                        ModelStreamProgress::Accepted => sink.accepted().map(|_| ()),
                        ModelStreamProgress::Advanced => {
                            work = work.checked_add(1).ok_or(CancellationCause::SystemShutdown)?;
                            sink.advanced(ActivitySource::CanonicalModelEvent,
                                NonZeroU64::new(work).ok_or(CancellationCause::SystemShutdown)?)
                        }
                        ModelStreamProgress::Terminal | ModelStreamProgress::Unchanged => Ok(()),
                    }.map_err(|_| CancellationCause::SystemShutdown)?;
                    if ended {
                        sink.ended().map_err(|_| CancellationCause::SystemShutdown)?;
                        native_ended = true;
                    }
                    if staged.update.has_pending_output && flush_due.is_none() {
                        flush_due = Some(tokio::time::Instant::now() + execution.snapshot.output_frame_flush_interval);
                    } else if !staged.update.has_pending_output { flush_due = None; }
                    flush_ready |= staged.update.should_commit;
                    let failure = staged.provider_failure;
                    state = staged.state;
                    if let Some(rejection) = rejected_action.as_ref()
                        && (ended || failure.is_some()) {
                        break Ending::Rejected(Box::new(rejection.clone()));
                    }
                    if let Some(error) = failure { break Ending::Failed(provider_error(iteration.invocation_id, error)); }
                }
            }
            tokio::task::consume_budget().await;
        };
        // Release this exact physical stream before asking for stop proof. No
        // detached reader or writer survives cancellation of the enclosing task.
        drop(stream);
        let drain = async {
            if let Some(pending) = pending {
                let checkpoint = pending.await.map_err(drain_cause)?;
                if !checkpoint_matches(attempt, &checkpoint) {
                    return Err(CancellationCause::SystemShutdown);
                }
                state
                    .coalescer
                    .acknowledge_ready(pending_count)
                    .map_err(|_| CancellationCause::SystemShutdown)?;
                batch_index = batch_index
                    .checked_add(1)
                    .ok_or(CancellationCause::SystemShutdown)?;
            }
            self.flush_partial_model_events(
                attempt,
                iteration,
                execution,
                &mut batch_index,
                &mut state.coalescer,
            )
            .await
            .map_err(drain_cause)?;
            Ok::<_, CancellationCause>(())
        };
        if native_ended {
            // The original Home reactor owns the single persisted drain due,
            // including this ordered write and the final result transaction.
            drain.await?;
        } else {
            tokio::time::timeout(windows.post_exit_drain, drain)
                .await
                .map_err(|_| CancellationCause::SystemShutdown)??;
        }
        let observation = state.observation(iteration.invocation_id);
        match ending {
            Ending::Rejected(rejection) => Ok(ModelConsumption::ActionRejected {
                native_terminal: native_ended,
                rejection: Box::new(crate::ModelActionRejectionCommit {
                    observation,
                    rejection: *rejection,
                }),
            }),
            Ending::Failed(failure) => Ok(ModelConsumption::Failed {
                error: failure.error,
                retry_after_ms: failure.retry_after_ms,
                provider_code: failure.provider_code,
                capacity_signal: failure.capacity_signal,
            }),
            Ending::Eof => {
                match state.into_outcome(iteration.request.route.provider_state_policy, budget) {
                    Ok(outcome) => Ok(ModelConsumption::Completed(outcome)),
                    Err(error) => Ok(ModelConsumption::Failed {
                        error,
                        retry_after_ms: None,
                        provider_code: None,
                        capacity_signal: crate::ContextCapacitySignal::NotCapacity,
                    }),
                }
            }
        }
    }
}
fn drain_cause(error: ModelDrainError) -> CancellationCause {
    match error {
        ModelDrainError::Persistence(cause) => cause,
        ModelDrainError::Local(error) => {
            tracing::error!(
                code = error.code.as_str(),
                "Invalid original model Journal boundary"
            );
            CancellationCause::SystemShutdown
        }
    }
}
async fn optional_deadline(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending().await
    }
}
