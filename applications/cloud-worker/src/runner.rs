use musteragent_runtime_worker::{
    WorkerAttemptGuard, WorkerAttemptObservation, WorkerRunnerObserver,
};
use musteragent_telemetry::{
    AttemptTelemetry, AttemptTelemetryContext, MetricOperation, QueueKind, RuntimeSpan,
};
use musteragent_telemetry_otlp::RuntimeMetrics;

pub use musteragent_runtime_worker::{
    AttemptExecutor, DispatchedAttempt, RuntimeWorkerRunner as CloudWorkerRunner,
    WorkerRunnerError, WorkerRunnerPolicy, WorkerRunnerPolicyError,
};

/// Cloud-owned observability adapter for the canonical Runtime worker core.
///
/// The shared supervisor owns execution lifecycle and shutdown correctness;
/// this adapter projects its closed observations into Cloud OTLP metrics and
/// typed Attempt telemetry without introducing a second lifecycle runner.
pub struct CloudWorkerRunnerObserver {
    runtime_metrics: RuntimeMetrics,
    attempt_telemetry: AttemptTelemetry,
}

impl CloudWorkerRunnerObserver {
    pub fn new(runtime_metrics: RuntimeMetrics, attempt_telemetry: AttemptTelemetry) -> Self {
        Self {
            runtime_metrics,
            attempt_telemetry,
        }
    }
}

impl WorkerRunnerObserver for CloudWorkerRunnerObserver {
    fn span(&self, context: &AttemptTelemetryContext) -> RuntimeSpan {
        self.attempt_telemetry.span(context)
    }

    fn attempt_started(
        &self,
        _context: &AttemptTelemetryContext,
        retry: bool,
    ) -> WorkerAttemptGuard {
        if retry {
            self.runtime_metrics.record_retry(MetricOperation::Attempt);
        }
        Box::new(InflightMetricGuard::new(self.runtime_metrics.clone()))
    }

    fn attempt_finished(
        &self,
        context: &AttemptTelemetryContext,
        observation: WorkerAttemptObservation,
    ) {
        self.attempt_telemetry.observe(
            context,
            observation.attempt_terminal,
            observation.attempt_outcome,
            observation.attempt_error,
        );
        self.runtime_metrics.record_operation(
            if observation.attempt_terminal {
                MetricOperation::Attempt
            } else {
                MetricOperation::AttemptLifecycle
            },
            observation.attempt_outcome,
            observation.duration,
        );
        if observation.retry_run {
            self.runtime_metrics.record_retry(MetricOperation::Run);
        }
        if let Some(outcome) = observation.interaction_outcome {
            self.runtime_metrics.record_operation(
                MetricOperation::Interaction,
                outcome,
                observation.duration,
            );
        }
        if let Some(outcome) = observation.run_terminal_outcome {
            self.runtime_metrics.record_operation(
                MetricOperation::Run,
                outcome,
                observation.duration,
            );
        }
    }
}

struct InflightMetricGuard {
    runtime_metrics: RuntimeMetrics,
}

impl InflightMetricGuard {
    fn new(runtime_metrics: RuntimeMetrics) -> Self {
        runtime_metrics.change_queue_depth(QueueKind::WorkerInflight, 1);
        Self { runtime_metrics }
    }
}

impl Drop for InflightMetricGuard {
    fn drop(&mut self) {
        self.runtime_metrics
            .change_queue_depth(QueueKind::WorkerInflight, -1);
    }
}
