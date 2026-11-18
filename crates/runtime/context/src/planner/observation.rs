//! Monotonic stage measurements without context or request material in traces.

use std::future::Future;
use std::time::Instant;

use musteragent_identity::RunId;
use tracing::{Instrument as _, Span};

use crate::ContextPlanError;

struct StageObservation {
    span: Span,
    run_id: RunId,
    stage: &'static str,
    started: Instant,
    outcome: &'static str,
}

impl StageObservation {
    fn new(run_id: RunId, stage: &'static str) -> Self {
        Self {
            run_id,
            stage,
            span: tracing::info_span!(
                "runtime.context.stage",
                run_id = run_id.to_string().as_str(),
                context.plan.stage = stage,
                context.plan.duration_us = tracing::field::Empty,
                context.plan.outcome = tracing::field::Empty,
                error.code = tracing::field::Empty,
            ),
            started: Instant::now(),
            // Future cancellation closes the same span without pretending a
            // successfully measured stage or manufacturing a business error.
            outcome: "cancelled",
        }
    }

    fn complete<T>(&mut self, result: &Result<T, ContextPlanError>) {
        self.outcome = if let Err(error) = result {
            self.span.record("error.code", error.code.as_str());
            "error"
        } else {
            "ok"
        };
    }
}

impl Drop for StageObservation {
    fn drop(&mut self) {
        let elapsed_us = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.span.record("context.plan.duration_us", elapsed_us);
        self.span.record("context.plan.outcome", self.outcome);
        // Keep routine timing in spans. Local diagnostic detail is opt-in so
        // ordinary planning does not turn per-stage latency into log traffic.
        tracing::debug!(
            parent: &self.span,
            run_id = self.run_id.to_string().as_str(),
            context.plan.stage = self.stage,
            context.plan.duration_us = elapsed_us,
            context.plan.outcome = self.outcome,
            "Context planning stage completed",
        );
    }
}

pub(super) fn observe_context_plan_stage<T>(
    run_id: RunId,
    stage: &'static str,
    operation: impl FnOnce() -> Result<T, ContextPlanError>,
) -> Result<T, ContextPlanError> {
    let mut observation = StageObservation::new(run_id, stage);
    let result = observation.span.in_scope(operation);
    observation.complete(&result);
    result.map_err(|error| log_context_plan_error(run_id, stage, error))
}

pub(super) async fn observe_context_plan_future<T>(
    run_id: RunId,
    stage: &'static str,
    operation: impl Future<Output = Result<T, ContextPlanError>>,
) -> Result<T, ContextPlanError> {
    let mut observation = StageObservation::new(run_id, stage);
    let result = operation.instrument(observation.span.clone()).await;
    observation.complete(&result);
    result.map_err(|error| log_context_plan_error(run_id, stage, error))
}

pub(super) fn log_context_plan_error(
    run_id: RunId,
    stage: &'static str,
    error: ContextPlanError,
) -> ContextPlanError {
    tracing::warn!(
        run_id = %run_id,
        context.plan.stage = stage,
        error.code = error.code.as_str(),
        "Context planning stage failed",
    );
    error
}
