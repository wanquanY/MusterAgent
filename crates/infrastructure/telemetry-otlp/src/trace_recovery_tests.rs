use super::*;
use crate::SignalHealthPhase;
use opentelemetry::KeyValue;
use opentelemetry_sdk::testing::trace::new_test_export_span_data;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

const FAIL: usize = 0;
const SUCCEED: usize = 1;
const HANG: usize = 2;

#[derive(Debug)]
struct ExportRecord {
    at: Instant,
    batch: Vec<SpanData>,
    resource: Option<Resource>,
}

#[derive(Debug, Default)]
struct Collector {
    mode: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    shutdowns: AtomicUsize,
    records: Mutex<Vec<ExportRecord>>,
    resource: Mutex<Option<Resource>>,
}

#[derive(Debug, Clone)]
struct ControllableExporter(Arc<Collector>);

struct Inflight(Arc<Collector>);

impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SpanExporter for ControllableExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _inflight = Inflight(Arc::clone(&self.0));
        self.0.max_active.fetch_max(active, Ordering::SeqCst);
        self.0.records.lock().expect("records").push(ExportRecord {
            at: Instant::now(),
            batch,
            resource: self.0.resource.lock().expect("resource").clone(),
        });
        match self.0.mode.load(Ordering::SeqCst) {
            SUCCEED => Ok(()),
            HANG => std::future::pending().await,
            _ => Err(OTelSdkError::InternalFailure(
                "test collector unavailable".into(),
            )),
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        *self.0.resource.lock().expect("resource") = Some(resource.clone());
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.0.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn fixture(
    interval: Duration,
) -> (
    tokio::runtime::Runtime,
    BoundedTraceProcessor,
    TelemetryHealth,
    Arc<Collector>,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("runtime");
    let collector = Arc::new(Collector::default());
    let health = TelemetryHealth::default();
    health.configure(TelemetrySignal::Trace, true, true);
    let mut processor = BoundedTraceProcessor::new(
        ControllableExporter(Arc::clone(&collector)),
        runtime.handle().clone(),
        8,
        4,
        interval,
        Duration::from_millis(400),
        health.clone(),
    )
    .expect("processor");
    processor.set_resource(
        &Resource::builder_empty()
            .with_attribute(KeyValue::new("service.name", "test-runtime"))
            .build(),
    );
    (runtime, processor, health, collector)
}

fn wait_until(description: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out: {description}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn startup_failure_recovers_without_business_spans_or_restart() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(60));
    processor.on_end(new_test_export_span_data());
    assert!(processor.force_flush().is_err());
    let failed = health.snapshot().trace;
    assert_eq!(failed.phase, SignalHealthPhase::Degraded);
    assert!(!failed.has_succeeded);
    assert!(failed.last_export_attempt_unix_ms.is_some());
    assert!(failed.last_export_success_unix_ms.is_none());
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    wait_until("idle recovery", || health.ready());
    let recovered = health.snapshot().trace;
    assert_eq!(recovered.phase, SignalHealthPhase::Healthy);
    assert!(recovered.last_export_success_unix_ms.is_some());
    assert_eq!(recovered.export_failures, 1);
    assert_eq!(recovered.dropped_export_failure, 1);
    assert_eq!(
        recovered.accepted, 1,
        "probe does not consume business admission"
    );
    assert_eq!(recovered.exported_items, 0);
    assert_eq!(recovered.queue_depth, 0);
    assert_eq!(recovered.recovery_attempts, 1);
    let records = collector.records.lock().expect("records");
    assert_eq!(records.len(), 2);
    let probe = &records[1];
    assert_eq!(probe.batch.len(), 1, "empty OTLP success is not readiness");
    assert_eq!(probe.batch[0].name, "musteragent.telemetry.trace.recovery");
    assert!(
        probe
            .resource
            .as_ref()
            .expect("same resource")
            .iter()
            .any(|(key, value)| key.as_str() == "service.name" && value.as_str() == "test-runtime")
    );
    drop(records);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
    assert_eq!(health.snapshot().trace.phase, SignalHealthPhase::Stopped);
}

#[test]
fn later_failure_recovers_but_metric_success_alone_cannot_recover_trace() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(60));
    health.configure(TelemetrySignal::Metric, true, true);
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    processor.on_end(new_test_export_span_data());
    processor.force_flush().expect("initial success");
    health.export_result(TelemetrySignal::Metric, 1, true);
    assert!(health.ready());

    collector.mode.store(FAIL, Ordering::SeqCst);
    processor.on_end(new_test_export_span_data());
    assert!(processor.force_flush().is_err());
    health.export_result(TelemetrySignal::Metric, 1, true);
    assert!(!health.ready());
    assert!(health.snapshot().trace.has_succeeded);
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    wait_until("recovery after prior success", || health.ready());
    let snapshot = health.snapshot().trace;
    assert_eq!(snapshot.accepted, 2);
    assert_eq!(snapshot.exported_items, 1);
    assert_eq!(snapshot.dropped_export_failure, 1);
    assert_eq!(snapshot.recovery_attempts, 1);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn a_successful_business_export_disarms_unnecessary_idle_probes() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(200));
    processor.on_end(new_test_export_span_data());
    assert!(processor.force_flush().is_err());
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    processor.on_end(new_test_export_span_data());
    processor
        .force_flush()
        .expect("real business export recovers");
    assert!(health.ready());
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(health.snapshot().trace.recovery_attempts, 0);
    assert_eq!(collector.records.lock().expect("records").len(), 2);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn recovery_does_not_erase_an_unreported_business_batch_failure() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(60));
    for _ in 0..4 {
        processor.on_end(new_test_export_span_data());
    }
    wait_until("automatic batch failure", || {
        health.snapshot().trace.export_failures == 1
    });
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    wait_until("idle recovery", || health.ready());
    assert!(
        processor.force_flush().is_err(),
        "flush still reports its accepted batch failure"
    );
    processor
        .force_flush()
        .expect("historical batch error was reported once");
    let snapshot = health.snapshot().trace;
    assert_eq!(snapshot.accepted, 4);
    assert_eq!(snapshot.dropped_export_failure, 4);
    assert_eq!(snapshot.exported_items, 0);
    assert_eq!(snapshot.recovery_attempts, 1);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn sustained_failure_has_bounded_serial_probes_without_queue_growth() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(40));
    processor.on_end(new_test_export_span_data());
    assert!(processor.force_flush().is_err());
    // Repeated empty flushes must neither forge readiness nor reset the
    // independent backoff deadline and starve automatic recovery.
    let until = Instant::now() + Duration::from_millis(500);
    while Instant::now() < until {
        processor
            .force_flush()
            .expect("empty flush has no admitted failure");
        assert!(!health.ready());
        std::thread::sleep(Duration::from_millis(5));
    }
    let snapshot = health.snapshot().trace;
    assert!((3..=6).contains(&snapshot.recovery_attempts));
    assert_eq!(snapshot.recovery_attempts, snapshot.recovery_failures);
    assert_eq!(snapshot.accepted, 1);
    assert_eq!(snapshot.dropped_export_failure, 1);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(processor.admission.counts(), (1, 1, 0));
    assert_eq!(collector.max_active.load(Ordering::SeqCst), 1);
    let records = collector.records.lock().expect("records");
    for pair in records.windows(2) {
        assert!(pair[1].at.duration_since(pair[0].at) >= Duration::from_millis(18));
        assert_eq!(pair[1].batch.len(), 1);
    }
    drop(records);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
    let calls = collector.records.lock().expect("records").len();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(collector.records.lock().expect("records").len(), calls);
    assert_eq!(collector.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_flush_and_no_resource_never_forge_startup_success() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("runtime");
    let collector = Arc::new(Collector::default());
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    let health = TelemetryHealth::default();
    health.configure(TelemetrySignal::Trace, true, true);
    let processor = BoundedTraceProcessor::new(
        ControllableExporter(Arc::clone(&collector)),
        runtime.handle().clone(),
        8,
        4,
        Duration::from_millis(20),
        Duration::from_millis(400),
        health.clone(),
    )
    .expect("processor");
    for _ in 0..10 {
        processor.force_flush().expect("empty flush");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!health.ready());
    assert_eq!(health.snapshot().trace.phase, SignalHealthPhase::Starting);
    assert!(collector.records.lock().expect("records").is_empty());
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn resource_configured_startup_automatically_checks_a_nonempty_pipeline() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(40));
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    wait_until("internal startup pipeline check", || health.ready());
    assert_eq!(health.snapshot().trace.accepted, 0);
    assert_eq!(health.snapshot().trace.recovery_attempts, 1);
    assert!(
        collector
            .records
            .lock()
            .expect("records")
            .iter()
            .all(|record| { record.batch.len() == 1 && record.resource.is_some() })
    );
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn hanging_probe_times_out_without_spawned_tasks_then_recovers() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(40));
    collector.mode.store(HANG, Ordering::SeqCst);
    wait_until("probe in flight", || {
        health.snapshot().trace.phase == SignalHealthPhase::Recovering
    });
    assert!(!health.ready());
    wait_until("probe timeout", || {
        health.snapshot().trace.recovery_failures == 1
    });
    assert_eq!(collector.active.load(Ordering::SeqCst), 0);
    assert_eq!(health.snapshot().trace.queue_depth, 0);
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    wait_until("probe recovery after timeout", || health.ready());
    assert_eq!(collector.max_active.load(Ordering::SeqCst), 1);
    assert!(health.snapshot().trace.export_failures >= 1);
    processor
        .shutdown_with_timeout(Duration::from_millis(400))
        .expect("shutdown");
}

#[test]
fn shutdown_cancels_hanging_probe_and_drains_preclosed_business_queue() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(40));
    collector.mode.store(HANG, Ordering::SeqCst);
    wait_until("probe in flight", || {
        collector.active.load(Ordering::SeqCst) == 1
    });
    assert_eq!(health.snapshot().trace.phase, SignalHealthPhase::Recovering);
    processor.on_end(new_test_export_span_data());
    collector.mode.store(SUCCEED, Ordering::SeqCst);
    let start = Instant::now();
    processor
        .shutdown_with_timeout(Duration::from_millis(100))
        .expect("cancel and drain");
    assert!(start.elapsed() < Duration::from_millis(100));
    let snapshot = health.snapshot().trace;
    assert_eq!(snapshot.recovery_cancellations, 1);
    assert_eq!(snapshot.recovery_failures, 0);
    assert_eq!(snapshot.accepted, 1);
    assert_eq!(snapshot.exported_items, 1);
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.phase, SignalHealthPhase::Stopped);
    assert!(!health.ready());
    assert_eq!(collector.active.load(Ordering::SeqCst), 0);
    assert_eq!(collector.max_active.load(Ordering::SeqCst), 1);
    assert_eq!(collector.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_processor_cancels_probe_and_exits_worker() {
    let (_runtime, processor, health, collector) = fixture(Duration::from_millis(40));
    collector.mode.store(HANG, Ordering::SeqCst);
    wait_until("probe in flight", || {
        collector.active.load(Ordering::SeqCst) == 1
    });
    drop(processor);
    wait_until("worker exits", || {
        health.snapshot().trace.phase == SignalHealthPhase::Stopped
    });
    assert_eq!(collector.active.load(Ordering::SeqCst), 0);
    assert_eq!(health.snapshot().trace.recovery_cancellations, 1);
    let calls = collector.records.lock().expect("records").len();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(collector.records.lock().expect("records").len(), calls);
}
