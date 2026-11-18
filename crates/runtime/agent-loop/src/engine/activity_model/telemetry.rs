//! One observation for the entire owned model operation, including cancellation
//! and the original result transaction. Dropping its reader never emits success.
use super::*;
pub(super) type ModelTelemetry = (TelemetryOutcome, Option<ErrorCode>);
impl DurableAgentLoop {
    pub(in crate::engine) async fn execute_activity_model_step(
        &self,
        context: ModelStepContext<'_>,
    ) -> ModelStep {
        let identity = InvocationTelemetryContext {
            attempt: context.telemetry.clone(),
            invocation_id: context.iteration.invocation_id,
        };
        let operation = InvocationOperation::Model;
        let span = (self.invocation_span)(operation, &identity);
        let started = Instant::now();
        let mut observed = None;
        let step = span
            .instrument(self.execute_activity_model_inner(context, &mut observed))
            .await;
        let (outcome, error) = match &step {
            ModelStep::Exit(AgentLoopExit::Cancelled { .. }) => (TelemetryOutcome::Cancelled, None),
            ModelStep::Exit(
                AgentLoopExit::Interrupted { .. }
                | AgentLoopExit::CoordinationInterrupted { .. }
                | AgentLoopExit::CoordinationForcedAbort { .. }
                | AgentLoopExit::ExternalOutcomeUnknown { .. },
            ) => (
                TelemetryOutcome::Interrupted,
                observed.and_then(|(_, error)| error),
            ),
            ModelStep::Exit(AgentLoopExit::Failed { code, .. }) => {
                (TelemetryOutcome::Error, Some(*code))
            }
            _ => observed.unwrap_or((TelemetryOutcome::Interrupted, None)),
        };
        span.record_outcome(outcome);
        if let Some(code) = error {
            span.record_error(ErrorClass::from_error_code(code));
        }
        span.in_scope(|| {
            (self.invocation_completion)(
                operation,
                &identity,
                outcome,
                error,
                None,
                None,
                started.elapsed(),
            )
        });
        step
    }
}
pub(super) fn consumption_observation(
    result: &Result<ModelConsumption, CancellationCause>,
) -> ModelTelemetry {
    let error = match result {
        Ok(ModelConsumption::Failed { error, .. }) => Some(error.code),
        Ok(ModelConsumption::ActionRejected { rejection, .. }) => Some(rejection.rejection.code()),
        _ => None,
    };
    (model_telemetry_outcome(result), error)
}
