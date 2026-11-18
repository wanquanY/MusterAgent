use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded, select};
use musteragent_telemetry::TelemetrySignal;
use opentelemetry::Context;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Span, SpanData, SpanExporter, SpanProcessor};

use crate::TelemetryHealth;
use crate::bounded_queue::{LinearizedAdmission, Sequenced};
use crate::trace_recovery::{RecoveryCancellation, RecoverySchedule, recovery_span};

enum Control {
    SetResource {
        resource: Resource,
        ack: Sender<OTelSdkResult>,
    },
    Flush {
        cutoff: u64,
        ack: Sender<OTelSdkResult>,
    },
    Shutdown {
        cutoff: u64,
        ack: Sender<OTelSdkResult>,
    },
}

struct TraceWorker {
    runtime: tokio::runtime::Handle,
    spans: Receiver<Sequenced<SpanData>>,
    controls: Receiver<Control>,
    batch_size: usize,
    export_interval: Duration,
    export_timeout: Duration,
    admission: LinearizedAdmission<SpanData>,
    health: TelemetryHealth,
    recovery_cancellation: Arc<RecoveryCancellation>,
}

pub(crate) struct BoundedTraceProcessor {
    admission: LinearizedAdmission<SpanData>,
    controls: Sender<Control>,
    health: TelemetryHealth,
    control_timeout: Duration,
    recovery_cancellation: Arc<RecoveryCancellation>,
}

impl Debug for BoundedTraceProcessor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedTraceProcessor")
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

impl BoundedTraceProcessor {
    pub(crate) fn new<E: SpanExporter + 'static>(
        exporter: E,
        runtime: tokio::runtime::Handle,
        queue_capacity: usize,
        batch_size: usize,
        export_interval: Duration,
        control_timeout: Duration,
        health: TelemetryHealth,
    ) -> Result<Self, std::io::Error> {
        let (admission, span_rx) =
            LinearizedAdmission::new(queue_capacity, TelemetrySignal::Trace, health.clone());
        let (controls, control_rx) = bounded(2);
        let worker_health = health.clone();
        let worker_admission = admission.clone();
        let export_timeout = (control_timeout / 2).max(Duration::from_millis(1));
        let recovery_cancellation = Arc::new(RecoveryCancellation::default());
        let worker_cancellation = Arc::clone(&recovery_cancellation);
        std::thread::Builder::new()
            .name("musteragent-otlp-trace-export".to_owned())
            .spawn(move || {
                export_loop(
                    exporter,
                    TraceWorker {
                        runtime,
                        spans: span_rx,
                        controls: control_rx,
                        batch_size,
                        export_interval,
                        export_timeout,
                        admission: worker_admission,
                        health: worker_health,
                        recovery_cancellation: worker_cancellation,
                    },
                );
            })?;
        Ok(Self {
            admission,
            controls,
            health,
            control_timeout,
            recovery_cancellation,
        })
    }

    fn control(&self, make: impl FnOnce(Sender<OTelSdkResult>) -> Control) -> OTelSdkResult {
        let (ack_tx, ack_rx) = bounded(1);
        if let Err(error) = self
            .controls
            .send_timeout(make(ack_tx), self.control_timeout)
        {
            self.health.export_result(TelemetrySignal::Trace, 0, false);
            return Err(OTelSdkError::InternalFailure(error.to_string()));
        }
        match ack_rx.recv_timeout(self.control_timeout) {
            Ok(result) => result,
            Err(_) => {
                self.health.export_result(TelemetrySignal::Trace, 0, false);
                Err(OTelSdkError::Timeout(self.control_timeout))
            }
        }
    }
}

impl SpanProcessor for BoundedTraceProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        self.admission.admit(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        let cutoff = self
            .admission
            .cutoff()
            .ok_or(OTelSdkError::AlreadyShutdown)?;
        self.control(|ack| Control::Flush { cutoff, ack })
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let timeout = timeout.min(self.control_timeout);
        let Some(cutoff) = self.admission.close() else {
            return Ok(());
        };
        // Cancel only an internal probe. Already admitted business batches
        // retain their existing cutoff-based, bounded shutdown drain.
        self.recovery_cancellation.stop();
        let (ack_tx, ack_rx) = bounded(1);
        let transport = self
            .controls
            .send_timeout(
                Control::Shutdown {
                    cutoff,
                    ack: ack_tx,
                },
                timeout,
            )
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
            .and_then(|()| {
                ack_rx
                    .recv_timeout(timeout)
                    .map_err(|_| OTelSdkError::Timeout(timeout))
            });
        match transport {
            Ok(result) => result,
            Err(error) => {
                self.admission.abort();
                Err(error)
            }
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        let _ = self.control(|ack| Control::SetResource {
            resource: resource.clone(),
            ack,
        });
    }
}

impl Drop for BoundedTraceProcessor {
    fn drop(&mut self) {
        // A dropped processor must not leave an idle recovery loop/export
        // alive. Dropping the control sender then lets its worker exit.
        self.recovery_cancellation.stop();
    }
}

fn export_loop<E: SpanExporter>(exporter: E, worker: TraceWorker) {
    let mut state = TraceExportState {
        exporter,
        runtime: &worker.runtime,
        spans: &worker.spans,
        health: &worker.health,
        batch: Vec::with_capacity(worker.batch_size),
        batch_size: worker.batch_size,
        export_timeout: worker.export_timeout,
        admission: &worker.admission,
        handled: 0,
        pending_error: None,
        recovery: RecoverySchedule::new(worker.export_interval),
        recovery_cancellation: &worker.recovery_cancellation,
        resource_ready: false,
    };
    let mut deadline = Instant::now() + worker.export_interval;
    loop {
        let recovery_deadline = state
            .recovery
            .deadline()
            .filter(|_| state.resource_ready && !state.recovery_cancellation.is_stopped());
        let next = recovery_deadline.map_or(deadline, |recovery| deadline.min(recovery));
        let wait = next.saturating_duration_since(Instant::now());
        select! {
            recv(worker.controls) -> control => match control {
                Ok(Control::SetResource { resource, ack }) => {
                    state.exporter.set_resource(&resource);
                    state.resource_ready = true;
                    if !state.health.trace_is_healthy() {
                        state.recovery.arm_startup();
                    }
                    let _ = ack.send(Ok(()));
                }
                Ok(Control::Flush { cutoff, ack }) => {
                    let result = state.flush_to(cutoff);
                    let _ = ack.send(result);
                    deadline = Instant::now() + worker.export_interval;
                }
                Ok(Control::Shutdown { cutoff, ack }) => {
                    let result = state.flush_to(cutoff);
                    let shutdown_result = state.exporter.shutdown();
                    let result = result.and(shutdown_result);
                    state.health.trace_worker_stopped();
                    let _ = ack.send(result);
                    return;
                }
                Err(_) => {
                    state.health.trace_worker_stopped();
                    return;
                },
            },
            recv(worker.spans) -> span => match span {
                Ok(span) => {
                    state.batch.push(span);
                    if state.batch.len() == state.batch_size {
                        state.export_ready();
                        deadline = Instant::now() + worker.export_interval;
                    }
                }
                Err(_) => {
                    state.health.trace_worker_stopped();
                    return;
                },
            },
            default(wait) => {
                state.export_ready();
                deadline = Instant::now() + worker.export_interval;
            }
        }
        // Check independently of the batch timer and flush requests so empty
        // flush traffic cannot indefinitely postpone an unhealthy recovery.
        state.recover_if_due();
    }
}

struct TraceExportState<'a, E> {
    exporter: E,
    runtime: &'a tokio::runtime::Handle,
    spans: &'a Receiver<Sequenced<SpanData>>,
    health: &'a TelemetryHealth,
    batch: Vec<Sequenced<SpanData>>,
    batch_size: usize,
    export_timeout: Duration,
    admission: &'a LinearizedAdmission<SpanData>,
    handled: u64,
    pending_error: Option<OTelSdkError>,
    recovery: RecoverySchedule,
    recovery_cancellation: &'a RecoveryCancellation,
    resource_ready: bool,
}

impl<E: SpanExporter> TraceExportState<'_, E> {
    fn export_ready(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let last = self.batch.last().map(|item| item.sequence);
        let result = export(
            &self.exporter,
            self.runtime,
            self.health,
            self.admission,
            self.export_timeout,
            &mut self.batch,
        );
        self.recovery.record_result(result.is_ok());
        if let Err(error) = result
            && self.pending_error.is_none()
        {
            self.pending_error = Some(error);
        }
        if let Some(last) = last {
            self.handled = last;
        }
    }

    fn recover_if_due(&mut self) {
        if !self.resource_ready || self.recovery_cancellation.is_stopped() {
            return;
        }
        // A control-path timeout can also mark export health degraded. Do not
        // leave it latched forever after the worker has resumed processing.
        if !self.health.trace_is_healthy() {
            self.recovery.arm_startup();
        }
        if self
            .recovery
            .deadline()
            .is_none_or(|deadline| Instant::now() < deadline)
        {
            return;
        }
        // Business export progress is also valid recovery evidence and gets
        // priority. The internal probe never consumes queue capacity.
        drain(self.spans, &mut self.batch, self.batch_size);
        if !self.batch.is_empty() {
            self.export_ready();
            return;
        }
        self.health.trace_recovery_started();
        let result = self.runtime.block_on(async {
            tokio::select! {
                biased;
                () = self.recovery_cancellation.cancelled() => None,
                result = tokio::time::timeout(
                    self.export_timeout,
                    self.exporter.export(vec![recovery_span()]),
                ) => Some(result.is_ok_and(|result| result.is_ok())),
            }
        });
        self.health.trace_recovery_finished(result);
        if let Some(success) = result {
            self.recovery.record_result(success);
        }
        // Probe failure does not change flush's error for accepted business
        // spans. Its separate health counters preserve the failure evidence.
    }

    fn flush_to(&mut self, cutoff: u64) -> OTelSdkResult {
        while self.handled < cutoff {
            drain(self.spans, &mut self.batch, self.batch_size);
            if self.batch.is_empty() {
                return Err(self.pending_error.take().unwrap_or_else(|| {
                    OTelSdkError::InternalFailure(
                        "trace flush cutoff exceeded the available accepted queue".to_owned(),
                    )
                }));
            }
            self.export_ready();
        }
        self.pending_error.take().map_or(Ok(()), Err)
    }
}

fn drain(
    spans: &Receiver<Sequenced<SpanData>>,
    batch: &mut Vec<Sequenced<SpanData>>,
    limit: usize,
) {
    while batch.len() < limit {
        let Ok(span) = spans.try_recv() else {
            break;
        };
        batch.push(span);
    }
}

fn export<E: SpanExporter>(
    exporter: &E,
    runtime: &tokio::runtime::Handle,
    health: &TelemetryHealth,
    admission: &LinearizedAdmission<SpanData>,
    export_timeout: Duration,
    batch: &mut Vec<Sequenced<SpanData>>,
) -> OTelSdkResult {
    if batch.is_empty() {
        return Ok(());
    }
    let count = batch.len();
    let first = batch.first().expect("nonempty trace batch").sequence;
    let last = batch.last().expect("nonempty trace batch").sequence;
    let outgoing = std::mem::replace(batch, Vec::with_capacity(batch.capacity()))
        .into_iter()
        .map(|item| item.value)
        .collect();
    health.export_started(TelemetrySignal::Trace);
    let result = runtime.block_on(async {
        tokio::select! {
            result = tokio::time::timeout(export_timeout, exporter.export(outgoing)) => {
                result.map_err(|_| OTelSdkError::Timeout(export_timeout))?
            }
            () = wait_for_abort(admission) => Err(OTelSdkError::AlreadyShutdown),
        }
    });
    if admission.complete(first, last, count) {
        health.export_result(TelemetrySignal::Trace, count, result.is_ok());
    }
    result
}

async fn wait_for_abort(admission: &LinearizedAdmission<SpanData>) {
    while !admission.is_aborted() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[cfg(test)]
#[path = "trace_recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_queue::TestAdmissionGate;
    use opentelemetry_sdk::testing::trace::new_test_export_span_data;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    #[derive(Debug, Clone)]
    struct RecordingExporter {
        fail: bool,
        exported: Arc<AtomicUsize>,
    }

    impl SpanExporter for RecordingExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            if self.fail {
                return Err(OTelSdkError::InternalFailure(
                    "collector unavailable".to_owned(),
                ));
            }
            self.exported.fetch_add(batch.len(), Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct GatedExporter {
        started: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
        release: Arc<Barrier>,
    }

    #[derive(Debug, Clone)]
    struct IndexedExporter {
        fail_batch: Option<usize>,
        calls: Arc<AtomicUsize>,
        received: Arc<AtomicUsize>,
    }

    impl SpanExporter for IndexedExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            let batch_number = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            self.received.fetch_add(batch.len(), Ordering::Relaxed);
            if self.fail_batch == Some(batch_number) {
                Err(OTelSdkError::InternalFailure(
                    "selected batch failed".to_owned(),
                ))
            } else {
                Ok(())
            }
        }
    }

    impl SpanExporter for GatedExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            let first_export = self.started.lock().expect("started lock").take();
            if let Some(started) = first_export {
                let _ = started.send(());
                self.release.wait();
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NeverCompletesExporter;

    impl SpanExporter for NeverCompletesExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            std::future::pending().await
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn empty_startup_stays_red_until_a_real_bootstrap_span_exports() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, true);
        health.configure(TelemetrySignal::Metric, false, false);
        health.configure(TelemetrySignal::Log, false, false);
        let exported = Arc::new(AtomicUsize::new(0));
        let processor = BoundedTraceProcessor::new(
            RecordingExporter {
                fail: false,
                exported: Arc::clone(&exported),
            },
            runtime.handle().clone(),
            8,
            4,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        processor.force_flush().expect("empty flush");
        assert!(!health.ready(), "an empty flush is not collector readiness");
        processor.on_end(new_test_export_span_data());
        processor.force_flush().expect("bootstrap export");
        assert!(health.ready());
        assert_eq!(exported.load(Ordering::Relaxed), 1);
        processor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("trace shutdown");
    }

    #[test]
    fn collector_failure_keeps_readiness_red_without_mutating_business_semantics() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, true);
        let business_commits = AtomicUsize::new(0);
        let processor = BoundedTraceProcessor::new(
            RecordingExporter {
                fail: true,
                exported: Arc::new(AtomicUsize::new(0)),
            },
            runtime.handle().clone(),
            8,
            4,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        business_commits.fetch_add(1, Ordering::Relaxed);
        processor.on_end(new_test_export_span_data());
        assert!(processor.force_flush().is_err());
        assert_eq!(business_commits.load(Ordering::Relaxed), 1);
        let snapshot = health.snapshot();
        assert!(!snapshot.ready);
        assert_eq!(snapshot.trace.export_failures, 1);
        let _ = processor.shutdown_with_timeout(Duration::from_secs(1));
    }

    #[test]
    fn force_flush_drains_every_batch_accepted_before_its_cutoff() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(AtomicUsize::new(0));
        let processor = BoundedTraceProcessor::new(
            IndexedExporter {
                fail_batch: None,
                calls: Arc::clone(&calls),
                received: Arc::clone(&received),
            },
            runtime.handle().clone(),
            16,
            2,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        for _ in 0..5 {
            processor.on_end(new_test_export_span_data());
        }
        processor.force_flush().expect("flush every accepted span");
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(received.load(Ordering::Relaxed), 5);
        assert_eq!(health.snapshot().trace.queue_depth, 0);
        processor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("trace shutdown");
    }

    #[test]
    fn force_flush_reports_a_middle_batch_failure_after_draining_the_cutoff() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(AtomicUsize::new(0));
        let processor = BoundedTraceProcessor::new(
            IndexedExporter {
                fail_batch: Some(2),
                calls: Arc::clone(&calls),
                received: Arc::clone(&received),
            },
            runtime.handle().clone(),
            16,
            2,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        for _ in 0..5 {
            processor.on_end(new_test_export_span_data());
        }
        assert!(processor.force_flush().is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(received.load(Ordering::Relaxed), 5);
        let snapshot = health.snapshot();
        assert_eq!(snapshot.trace.queue_depth, 0);
        assert_eq!(snapshot.trace.export_failures, 1);
        assert_eq!(snapshot.trace.accepted, 5);
        assert_eq!(snapshot.trace.exported_items, 3);
        assert_eq!(snapshot.trace.dropped_export_failure, 2);
        processor
            .shutdown_with_timeout(Duration::from_secs(1))
            .expect("shutdown after reported failure");
    }

    #[test]
    fn shutdown_reports_a_middle_batch_failure_after_draining_the_cutoff() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(AtomicUsize::new(0));
        let processor = BoundedTraceProcessor::new(
            IndexedExporter {
                fail_batch: Some(2),
                calls: Arc::clone(&calls),
                received: Arc::clone(&received),
            },
            runtime.handle().clone(),
            16,
            2,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        for _ in 0..5 {
            processor.on_end(new_test_export_span_data());
        }
        assert!(
            processor
                .shutdown_with_timeout(Duration::from_secs(1))
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(received.load(Ordering::Relaxed), 5);
        let snapshot = health.snapshot().trace;
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(snapshot.accepted, 5);
        assert_eq!(snapshot.exported_items, 3);
        assert_eq!(snapshot.dropped_export_failure, 2);
    }

    #[test]
    fn queue_pressure_drops_nonblocking_and_shutdown_flushes_remaining_spans() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, false);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new(Barrier::new(2));
        let processor = BoundedTraceProcessor::new(
            GatedExporter {
                started: Mutex::new(Some(started_tx)),
                release: Arc::clone(&release),
            },
            runtime.handle().clone(),
            1,
            1,
            Duration::from_secs(60),
            Duration::from_secs(2),
            health.clone(),
        )
        .expect("trace processor");
        processor.on_end(new_test_export_span_data());
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first export started");
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());
        assert_eq!(health.snapshot().trace.dropped_queue_full, 1);
        release.wait();
        processor
            .shutdown_with_timeout(Duration::from_secs(2))
            .expect("shutdown flush");
        let snapshot = health.snapshot();
        assert_eq!(snapshot.trace.accepted, 2);
        assert_eq!(snapshot.trace.exported_items, 2);
        assert_eq!(snapshot.trace.queue_depth, 0);
    }

    #[test]
    fn shutdown_is_linearized_after_an_inflight_trace_admission() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, false);
        let exported = Arc::new(AtomicUsize::new(0));
        let processor = Arc::new(
            BoundedTraceProcessor::new(
                RecordingExporter {
                    fail: false,
                    exported: Arc::clone(&exported),
                },
                runtime.handle().clone(),
                8,
                4,
                Duration::from_secs(60),
                Duration::from_secs(1),
                health.clone(),
            )
            .expect("trace processor"),
        );
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        processor.admission.install_test_gate(TestAdmissionGate {
            entered: entered_tx,
            release: release_rx,
        });

        let producer = Arc::clone(&processor);
        let producer = std::thread::spawn(move || {
            producer.on_end(new_test_export_span_data());
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("trace admission paused");
        let closer = Arc::clone(&processor);
        let (closed_tx, closed_rx) = std::sync::mpsc::sync_channel(1);
        let closer = std::thread::spawn(move || {
            closed_tx
                .send(closer.shutdown_with_timeout(Duration::from_secs(1)))
                .expect("shutdown result");
        });
        assert!(closed_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).expect("release trace admission");
        producer.join().expect("trace producer");
        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown result")
            .expect("shutdown flush");
        closer.join().expect("trace closer");

        processor.on_end(new_test_export_span_data());
        let snapshot = health.snapshot().trace;
        assert_eq!(snapshot.accepted, 1);
        assert_eq!(snapshot.exported_items, 1);
        assert_eq!(snapshot.dropped_shutdown, 1);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(processor.admission.counts(), (1, 1, 0));
    }

    #[test]
    fn exporter_timeout_accounts_every_admitted_span_as_dropped() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, true);
        let processor = BoundedTraceProcessor::new(
            NeverCompletesExporter,
            runtime.handle().clone(),
            8,
            4,
            Duration::from_secs(60),
            Duration::from_millis(50),
            health.clone(),
        )
        .expect("trace processor");
        processor.on_end(new_test_export_span_data());
        assert!(processor.force_flush().is_err());
        let snapshot = health.snapshot().trace;
        assert_eq!(snapshot.accepted, 1);
        assert_eq!(snapshot.exported_items, 0);
        assert_eq!(snapshot.dropped_export_failure, 1);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(processor.admission.counts(), (1, 1, 0));
        let _ = processor.shutdown_with_timeout(Duration::from_millis(50));
    }

    #[test]
    fn shutdown_deadline_aborts_and_accounts_every_admitted_span_once() {
        let runtime = runtime();
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, true);
        let processor = BoundedTraceProcessor::new(
            NeverCompletesExporter,
            runtime.handle().clone(),
            8,
            4,
            Duration::from_secs(60),
            Duration::from_millis(200),
            health.clone(),
        )
        .expect("trace processor");
        for _ in 0..3 {
            processor.on_end(new_test_export_span_data());
        }
        assert!(
            processor
                .shutdown_with_timeout(Duration::from_millis(20))
                .is_err()
        );
        let snapshot = health.snapshot().trace;
        assert_eq!(snapshot.accepted, 3);
        assert_eq!(snapshot.exported_items, 0);
        assert_eq!(snapshot.dropped_export_failure, 3);
        assert_eq!(snapshot.export_failures, 1);
        assert_eq!(snapshot.queue_depth, 0);
        assert_eq!(processor.admission.counts(), (3, 3, 0));
    }
}
