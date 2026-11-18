//! Model-consumption outcomes and invocation telemetry contracts.

use super::*;
use crate::ModelActionRejectionCommit;
use crate::engine_support::persistence_interruption;

/// Process-composition bridge for canonical model and Tool invocation spans.
///
/// The Runtime owns the typed identity while the infrastructure layer owns
/// OpenTelemetry parent attachment and export.
pub type InvocationSpanFactory =
    dyn Fn(InvocationOperation, &InvocationTelemetryContext) -> RuntimeSpan + Send + Sync;
pub type InvocationCompletionObserver = dyn Fn(
        InvocationOperation,
        &InvocationTelemetryContext,
        TelemetryOutcome,
        Option<ErrorCode>,
        Option<ErrorCode>,
        Option<musteragent_identity::LedgerRecordId>,
        Duration,
    ) + Send
    + Sync;

pub(super) enum ModelConsumption {
    Completed(ModelIterationOutcome),
    ActionRejected {
        native_terminal: bool,
        rejection: Box<ModelActionRejectionCommit>,
    },
    Failed {
        provider_code: Option<musteragent_runtime_model::ProviderErrorCode>,
        error: LoopPortError,
        retry_after_ms: Option<u64>,
        capacity_signal: crate::ContextCapacitySignal,
    },
}

pub(super) enum ModelDrainError {
    Local(LoopPortError),
    Persistence(CancellationCause),
}

pub(super) fn model_telemetry_outcome(
    result: &Result<ModelConsumption, CancellationCause>,
) -> TelemetryOutcome {
    match result {
        Ok(ModelConsumption::Completed(_)) => TelemetryOutcome::Success,
        Ok(ModelConsumption::ActionRejected { .. }) => TelemetryOutcome::Error,
        Err(_) => TelemetryOutcome::Interrupted,
        Ok(ModelConsumption::Failed { .. }) => TelemetryOutcome::Error,
    }
}

impl DurableAgentLoop {
    pub(super) async fn append_model_batch_confirmed(
        &self,
        attempt: &AttemptContext,
        batch: ModelEventBatch,
    ) -> Result<DurableLoopCheckpoint, ModelDrainError> {
        let first = self
            .journal
            .append_model_events(attempt, batch.clone())
            .await;
        match first {
            Ok(checkpoint) => Ok(checkpoint),
            Err(first_error) => match self.journal.append_model_events(attempt, batch).await {
                Ok(checkpoint) => Ok(checkpoint),
                Err(second_error) => Err(ModelDrainError::Persistence(persistence_interruption(
                    &first_error,
                    &second_error,
                ))),
            },
        }
    }
}
