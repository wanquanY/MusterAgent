use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;

use super::DurableAgentLoop;
use super::model_execution::ModelDrainError;
use crate::engine_support::{checkpoint_matches, persistence_interruption, port};
use crate::output_coalescer::ModelEventCoalescer;
use crate::{
    CancellationCause, DurableLoopCheckpoint, LoopPortError, ModelCompletionCommit,
    ModelEventBatch, ModelIteration, PinnedRunExecutionPolicy,
};

impl DurableAgentLoop {
    pub(super) async fn flush_partial_model_events(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        execution: PinnedRunExecutionPolicy,
        batch_index: &mut u32,
        coalescer: &mut ModelEventCoalescer,
    ) -> Result<(), ModelDrainError> {
        coalescer.flush_pending().map_err(|_| {
            ModelDrainError::Local(port(ErrorCode::InternalInvariantViolated, false))
        })?;
        self.commit_ready_model_events(attempt, iteration, execution, batch_index, coalescer, true)
            .await
    }

    pub(super) async fn commit_ready_model_events(
        &self,
        attempt: &AttemptContext,
        iteration: &ModelIteration,
        execution: PinnedRunExecutionPolicy,
        batch_index: &mut u32,
        coalescer: &mut ModelEventCoalescer,
        drain_all: bool,
    ) -> Result<(), ModelDrainError> {
        while coalescer.ready_len() > 0
            && (drain_all
                || coalescer.ready_len() >= execution.snapshot.max_model_events_per_commit.get())
        {
            let next_batch_index = (*batch_index).checked_add(1).ok_or_else(|| {
                ModelDrainError::Local(port(ErrorCode::InternalInvariantViolated, false))
            })?;
            let events =
                coalescer.ready_batch(execution.snapshot.max_model_events_per_commit.get());
            let event_count = events.len();
            let batch = ModelEventBatch {
                invocation_id: iteration.invocation_id,
                iteration: iteration.iteration,
                batch_index: *batch_index,
                events,
            };
            let checkpoint = self.append_model_batch_confirmed(attempt, batch).await?;
            if !checkpoint_matches(attempt, &checkpoint) {
                return Err(ModelDrainError::Local(port(
                    ErrorCode::InternalInvariantViolated,
                    false,
                )));
            }
            coalescer.acknowledge_ready(event_count).map_err(|_| {
                ModelDrainError::Local(port(ErrorCode::InternalInvariantViolated, false))
            })?;
            *batch_index = next_batch_index;
        }
        Ok(())
    }

    pub(super) async fn finish_model_confirmed(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> Result<DurableLoopCheckpoint, CancellationCause> {
        let first = self.journal.finish_model(attempt, completion.clone()).await;
        match first {
            Ok(checkpoint) => Ok(checkpoint),
            Err(first_error) => match self.journal.finish_model(attempt, completion).await {
                Ok(checkpoint) => Ok(checkpoint),
                Err(second_error) => {
                    log_model_completion_commit_failure(
                        attempt,
                        "finish_model",
                        &first_error,
                        &second_error,
                    );
                    Err(persistence_interruption(&first_error, &second_error))
                }
            },
        }
    }

    pub(super) async fn stage_model_completion_confirmed(
        &self,
        attempt: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> Result<DurableLoopCheckpoint, CancellationCause> {
        let first = self
            .journal
            .stage_model_completion(attempt, completion.clone())
            .await;
        match first {
            Ok(checkpoint) => Ok(checkpoint),
            Err(first_error) => match self
                .journal
                .stage_model_completion(attempt, completion)
                .await
            {
                Ok(checkpoint) => Ok(checkpoint),
                Err(second_error) => {
                    log_model_completion_commit_failure(
                        attempt,
                        "stage_model_completion",
                        &first_error,
                        &second_error,
                    );
                    Err(persistence_interruption(&first_error, &second_error))
                }
            },
        }
    }
}

/// Model completion commits fail-closed into fenced recovery, which otherwise
/// hides the real Journal error behind a generic recovery failure. Log the
/// exact codes before the Attempt exits so operations can diagnose the cause.
fn log_model_completion_commit_failure(
    attempt: &AttemptContext,
    operation: &str,
    first_error: &LoopPortError,
    second_error: &LoopPortError,
) {
    tracing::error!(
        run_id = %attempt.run_id,
        attempt_id = %attempt.attempt_id,
        operation,
        first_error.code = %first_error.code.as_str(),
        second_error.code = %second_error.code.as_str(),
        "Journal model completion commit failed twice; exiting to fenced recovery"
    );
}
