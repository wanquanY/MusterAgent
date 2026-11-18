use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use musteragent_telemetry::TelemetrySignal;
use serde::Serialize;

#[derive(Debug, Default)]
struct SignalState {
    enabled: AtomicBool,
    required: AtomicBool,
    has_succeeded: AtomicBool,
    healthy: AtomicBool,
    queue_depth: AtomicUsize,
    accepted: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_invalid: AtomicU64,
    dropped_shutdown: AtomicU64,
    dropped_export_failure: AtomicU64,
    export_batches: AtomicU64,
    export_failures: AtomicU64,
    exported_items: AtomicU64,
    recovering: AtomicBool,
    stopped: AtomicBool,
    last_export_attempt_unix_ms: AtomicU64,
    last_export_success_unix_ms: AtomicU64,
    recovery_attempts: AtomicU64,
    recovery_failures: AtomicU64,
    recovery_cancellations: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct TelemetryHealth {
    trace: Arc<SignalState>,
    metric: Arc<SignalState>,
    log: Arc<SignalState>,
}

impl TelemetryHealth {
    pub(crate) fn configure(&self, signal: TelemetrySignal, enabled: bool, required: bool) {
        let state = self.state(signal);
        state.enabled.store(enabled, Ordering::Release);
        state.required.store(required, Ordering::Release);
        state.healthy.store(!enabled, Ordering::Release);
    }

    pub(crate) fn accepted(&self, signal: TelemetrySignal) {
        self.state(signal).accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn queue_increment(&self, signal: TelemetrySignal) {
        self.state(signal)
            .queue_depth
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn queue_decrement(&self, signal: TelemetrySignal, count: usize) {
        let result = self.state(signal).queue_depth.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |depth| depth.checked_sub(count),
        );
        assert!(result.is_ok(), "telemetry queue depth underflow");
    }

    pub(crate) fn dropped_full(&self, signal: TelemetrySignal) {
        self.state(signal)
            .dropped_queue_full
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dropped_invalid(&self, signal: TelemetrySignal) {
        self.state(signal)
            .dropped_invalid
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dropped_shutdown(&self, signal: TelemetrySignal) {
        self.state(signal)
            .dropped_shutdown
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn export_result(&self, signal: TelemetrySignal, item_count: usize, success: bool) {
        let state = self.state(signal);
        if success {
            state
                .last_export_success_unix_ms
                .store(now_unix_ms(), Ordering::Release);
            state.export_batches.fetch_add(1, Ordering::Relaxed);
            state
                .exported_items
                .fetch_add(item_count as u64, Ordering::Relaxed);
            // Publish healthy last, after its supporting timestamp/counters.
            state.has_succeeded.store(true, Ordering::Release);
            state.healthy.store(true, Ordering::Release);
        } else {
            state.healthy.store(false, Ordering::Release);
            state.export_failures.fetch_add(1, Ordering::Relaxed);
            state
                .dropped_export_failure
                .fetch_add(item_count as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn export_started(&self, signal: TelemetrySignal) {
        self.state(signal)
            .last_export_attempt_unix_ms
            .store(now_unix_ms(), Ordering::Release);
    }

    pub(crate) fn trace_recovery_started(&self) {
        self.export_started(TelemetrySignal::Trace);
        self.trace.recovery_attempts.fetch_add(1, Ordering::Relaxed);
        self.trace.recovering.store(true, Ordering::Release);
    }

    pub(crate) fn trace_is_healthy(&self) -> bool {
        self.trace.healthy.load(Ordering::Acquire)
    }

    pub(crate) fn trace_recovery_finished(&self, result: Option<bool>) {
        if let Some(success) = result {
            // A genuine nonempty internal span was exported. Probe items are
            // not admitted business spans: track attempts/failures separately
            // without corrupting accepted/exported/dropped item accounting.
            self.export_result(TelemetrySignal::Trace, 0, success);
            if !success {
                self.trace.recovery_failures.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.trace
                .recovery_cancellations
                .fetch_add(1, Ordering::Relaxed);
        }
        self.trace.recovering.store(false, Ordering::Release);
    }

    pub(crate) fn trace_worker_stopped(&self) {
        self.trace.stopped.store(true, Ordering::Release);
        self.trace.healthy.store(false, Ordering::Release);
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        [
            TelemetrySignal::Trace,
            TelemetrySignal::Metric,
            TelemetrySignal::Log,
        ]
        .into_iter()
        .all(|signal| {
            let state = self.state(signal);
            !state.required.load(Ordering::Acquire)
                || (state.has_succeeded.load(Ordering::Acquire)
                    && state.healthy.load(Ordering::Acquire)
                    && !state.recovering.load(Ordering::Acquire)
                    && !state.stopped.load(Ordering::Acquire))
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> TelemetryHealthSnapshot {
        TelemetryHealthSnapshot {
            ready: self.ready(),
            trace: snapshot(&self.trace),
            metric: snapshot(&self.metric),
            log: snapshot(&self.log),
        }
    }

    fn state(&self, signal: TelemetrySignal) -> &SignalState {
        match signal {
            TelemetrySignal::Trace => &self.trace,
            TelemetrySignal::Metric => &self.metric,
            TelemetrySignal::Log => &self.log,
        }
    }
}

fn snapshot(state: &SignalState) -> SignalHealth {
    let enabled = state.enabled.load(Ordering::Acquire);
    let healthy = state.healthy.load(Ordering::Acquire);
    let recovering = state.recovering.load(Ordering::Acquire);
    let phase = if !enabled {
        SignalHealthPhase::Disabled
    } else if state.stopped.load(Ordering::Acquire) {
        SignalHealthPhase::Stopped
    } else if recovering {
        SignalHealthPhase::Recovering
    } else if healthy {
        SignalHealthPhase::Healthy
    } else if state.export_failures.load(Ordering::Acquire) > 0 {
        SignalHealthPhase::Degraded
    } else {
        SignalHealthPhase::Starting
    };
    SignalHealth {
        enabled,
        required: state.required.load(Ordering::Acquire),
        has_succeeded: state.has_succeeded.load(Ordering::Acquire),
        healthy,
        phase,
        queue_depth: state.queue_depth.load(Ordering::Acquire),
        accepted: state.accepted.load(Ordering::Acquire),
        dropped_queue_full: state.dropped_queue_full.load(Ordering::Acquire),
        dropped_invalid: state.dropped_invalid.load(Ordering::Acquire),
        dropped_shutdown: state.dropped_shutdown.load(Ordering::Acquire),
        dropped_export_failure: state.dropped_export_failure.load(Ordering::Acquire),
        export_batches: state.export_batches.load(Ordering::Acquire),
        export_failures: state.export_failures.load(Ordering::Acquire),
        exported_items: state.exported_items.load(Ordering::Acquire),
        last_export_attempt_unix_ms: optional_timestamp(&state.last_export_attempt_unix_ms),
        last_export_success_unix_ms: optional_timestamp(&state.last_export_success_unix_ms),
        recovery_attempts: state.recovery_attempts.load(Ordering::Acquire),
        recovery_failures: state.recovery_failures.load(Ordering::Acquire),
        recovery_cancellations: state.recovery_cancellations.load(Ordering::Acquire),
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |elapsed| {
            u64::try_from(elapsed.as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
        })
}

fn optional_timestamp(value: &AtomicU64) -> Option<u64> {
    let value = value.load(Ordering::Acquire);
    (value != 0).then_some(value)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalHealthPhase {
    Disabled,
    Starting,
    Healthy,
    Degraded,
    Recovering,
    Stopped,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct SignalHealth {
    pub enabled: bool,
    pub required: bool,
    pub has_succeeded: bool,
    pub healthy: bool,
    pub phase: SignalHealthPhase,
    pub queue_depth: usize,
    pub accepted: u64,
    pub dropped_queue_full: u64,
    pub dropped_invalid: u64,
    pub dropped_shutdown: u64,
    pub dropped_export_failure: u64,
    pub export_batches: u64,
    pub export_failures: u64,
    pub exported_items: u64,
    /// Actual exporter start/success times; a probe uses the same exporter.
    pub last_export_attempt_unix_ms: Option<u64>,
    pub last_export_success_unix_ms: Option<u64>,
    /// Internal nonempty probes do not count as accepted/exported/dropped
    /// business items. Their completed exports do count as batches/failures.
    pub recovery_attempts: u64,
    pub recovery_failures: u64,
    pub recovery_cancellations: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct TelemetryHealthSnapshot {
    pub ready: bool,
    pub trace: SignalHealth,
    pub metric: SignalHealth,
    pub log: SignalHealth,
}
