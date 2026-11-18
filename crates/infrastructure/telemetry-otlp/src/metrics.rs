use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use musteragent_telemetry::{
    DataEdgeOperation, DbCommitKind, DbTransactionPhase, DbTransactionResult, MetricOperation,
    QueueKind, TelemetryOutcome,
};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

use crate::TelemetryHealth;
use musteragent_telemetry::TelemetrySignal;

pub(crate) struct HealthMetricExporter<E> {
    inner: E,
    health: TelemetryHealth,
}

impl<E> HealthMetricExporter<E> {
    pub(crate) fn new(inner: E, health: TelemetryHealth) -> Self {
        Self { inner, health }
    }
}

impl<E: PushMetricExporter> PushMetricExporter for HealthMetricExporter<E> {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        self.health.export_started(TelemetrySignal::Metric);
        let result = self.inner.export(metrics).await;
        self.health
            .export_result(TelemetrySignal::Metric, 1, result.is_ok());
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

#[derive(Clone)]
pub struct RuntimeMetrics {
    operations: Counter<u64>,
    operation_duration: Histogram<f64>,
    db_commit_duration: Histogram<f64>,
    db_transaction_duration: Histogram<f64>,
    db_transaction_outcomes: Counter<u64>,
    db_pool_connections: Gauge<u64>,
    retries: Counter<u64>,
    fence_rejections: Counter<u64>,
    queue_depth: UpDownCounter<i64>,
    queue_capacity: Gauge<u64>,
    observed_transitions: Arc<Mutex<ObservationDeduper>>,
}

const OBSERVATION_DEDUPE_CAPACITY: usize = 65_536;

struct ObservationDeduper {
    order: VecDeque<musteragent_identity::LedgerRecordId>,
    members: HashSet<musteragent_identity::LedgerRecordId>,
}

impl ObservationDeduper {
    fn claim(&mut self, observation_id: musteragent_identity::LedgerRecordId) -> bool {
        if !self.members.insert(observation_id) {
            return false;
        }
        self.order.push_back(observation_id);
        if self.order.len() > OBSERVATION_DEDUPE_CAPACITY {
            let oldest = self.order.pop_front().expect("dedupe order is nonempty");
            let removed = self.members.remove(&oldest);
            debug_assert!(removed, "dedupe membership follows insertion order");
        }
        true
    }
}

impl RuntimeMetrics {
    pub(crate) fn new(meter: Meter) -> Self {
        Self {
            operations: meter
                .u64_counter("kernel.runtime.operations")
                .with_description("Completed Runtime operations")
                .build(),
            operation_duration: meter
                .f64_histogram("kernel.runtime.operation.duration")
                .with_unit("s")
                .with_description("Runtime operation latency")
                .build(),
            db_commit_duration: meter
                .f64_histogram("kernel.runtime.db.commit.duration")
                .with_unit("s")
                .with_description("Authoritative PostgreSQL transaction commit latency")
                .build(),
            db_transaction_duration: meter
                .f64_histogram("kernel.runtime.db.transaction.phase.duration")
                .with_unit("s")
                .with_description("Authoritative database transaction latency by bounded phase")
                .build(),
            db_transaction_outcomes: meter
                .u64_counter("kernel.runtime.db.transaction.outcomes")
                .with_description("Authoritative database transaction phase outcomes")
                .build(),
            db_pool_connections: meter
                .u64_gauge("kernel.runtime.db.pool.connections")
                .with_description("Current governed database pool connections by state")
                .build(),
            retries: meter
                .u64_counter("kernel.runtime.retries")
                .with_description("Bounded Runtime retries")
                .build(),
            fence_rejections: meter
                .u64_counter("kernel.runtime.fence.rejections")
                .with_description("Writes rejected by a stale execution fence")
                .build(),
            queue_depth: meter
                .i64_up_down_counter("kernel.runtime.queue.depth")
                .with_description("Bounded in-process queue occupancy")
                .build(),
            queue_capacity: meter
                .u64_gauge("kernel.runtime.queue.capacity")
                .with_description("Configured bounded in-process queue capacity")
                .build(),
            observed_transitions: Arc::new(Mutex::new(ObservationDeduper {
                order: VecDeque::with_capacity(OBSERVATION_DEDUPE_CAPACITY),
                members: HashSet::with_capacity(OBSERVATION_DEDUPE_CAPACITY),
            })),
        }
    }

    pub fn record_operation(
        &self,
        operation: MetricOperation,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) {
        let attributes = [
            KeyValue::new("operation", operation.as_str()),
            KeyValue::new("outcome", outcome.as_str()),
        ];
        self.operations.add(1, &attributes);
        self.operation_duration
            .record(duration.as_secs_f64(), &attributes);
    }

    /// Preserves the existing `operation=data_edge` Grafana contract while
    /// adding one closed, low-cardinality dimension for endpoint-level SLOs.
    pub fn record_data_edge(
        &self,
        operation: DataEdgeOperation,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) {
        let attributes = [
            KeyValue::new("operation", MetricOperation::DataEdge.as_str()),
            KeyValue::new("outcome", outcome.as_str()),
            KeyValue::new("data_edge.operation", operation.as_str()),
        ];
        self.operations.add(1, &attributes);
        self.operation_duration
            .record(duration.as_secs_f64(), &attributes);
    }

    /// Records a durable transition once per process. The canonical Ledger
    /// record ID is never exported as a metric label; it only protects the
    /// low-cardinality counter from same-process resume/replay duplication.
    /// Cross-process at-least-once delivery retains this ID on the machine
    /// event so the collector can apply the same idempotency key.
    pub fn record_operation_once(
        &self,
        observation_id: musteragent_identity::LedgerRecordId,
        operation: MetricOperation,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) -> bool {
        let claimed = self
            .observed_transitions
            .lock()
            .is_ok_and(|mut deduper| deduper.claim(observation_id));
        if claimed {
            self.record_operation(operation, outcome, duration);
        }
        claimed
    }

    pub fn record_db_commit(
        &self,
        kind: DbCommitKind,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) {
        self.db_commit_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("commit.kind", kind.as_str()),
                KeyValue::new("outcome", outcome.as_str()),
            ],
        );
    }

    pub fn record_db_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        let attributes = [
            KeyValue::new("database.backend", "postgresql"),
            KeyValue::new("transaction.kind", kind.as_str()),
            KeyValue::new("phase", phase.as_str()),
            KeyValue::new("result", result.as_str()),
        ];
        self.db_transaction_outcomes.add(1, &attributes);
        self.db_transaction_duration
            .record(duration.as_secs_f64(), &attributes);
    }

    pub fn record_retry(&self, operation: MetricOperation) {
        self.retries
            .add(1, &[KeyValue::new("operation", operation.as_str())]);
    }

    pub fn record_db_pool_connections(&self, used: u32, idle: u32, max: u32) {
        for (state, value) in [("used", used), ("idle", idle), ("max", max)] {
            self.db_pool_connections.record(
                u64::from(value),
                &[
                    KeyValue::new("database.backend", "postgresql"),
                    KeyValue::new("state", state),
                ],
            );
        }
    }

    pub fn record_fence_rejection(&self, operation: MetricOperation) {
        self.fence_rejections
            .add(1, &[KeyValue::new("operation", operation.as_str())]);
    }

    pub fn change_queue_depth(&self, queue: QueueKind, delta: i64) {
        self.queue_depth
            .add(delta, &[KeyValue::new("queue", queue.as_str())]);
    }

    pub fn record_queue_capacity(&self, queue: QueueKind, capacity: usize) {
        self.queue_capacity.record(
            u64::try_from(capacity).expect("queue capacity fits u64"),
            &[KeyValue::new("queue", queue.as_str())],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_transition_metrics_are_claimed_once_without_identity_labels() {
        let metrics = RuntimeMetrics::new(opentelemetry::global::meter("metric-dedupe-test"));
        let observation_id = musteragent_identity::LedgerRecordId::generate();
        assert!(metrics.record_operation_once(
            observation_id,
            MetricOperation::Tool,
            TelemetryOutcome::Success,
            Duration::ZERO,
        ));
        assert!(!metrics.record_operation_once(
            observation_id,
            MetricOperation::Tool,
            TelemetryOutcome::Error,
            Duration::ZERO,
        ));
        metrics.record_db_pool_connections(1, 2, 3);
    }
}
