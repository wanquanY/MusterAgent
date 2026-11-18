//! Recovery belongs to the existing serial trace exporter, not to business
//! traffic, readiness requests, or a second retry queue.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use opentelemetry::InstrumentationScope;
use opentelemetry::trace::{SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceState};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, SpanData};

#[derive(Debug, Default)]
pub(crate) struct RecoveryCancellation {
    stopped: AtomicBool,
    wake: tokio::sync::Notify,
}

impl RecoveryCancellation {
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        // There is exactly one consumer: the serial trace worker. A stored
        // permit also covers cancellation before it starts awaiting.
        self.wake.notify_one();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) async fn cancelled(&self) {
        while !self.is_stopped() {
            self.wake.notified().await;
        }
    }
}

pub(crate) struct RecoverySchedule {
    base: Duration,
    ceiling: Duration,
    next: Option<Instant>,
    failures: u32,
    ids: RandomIdGenerator,
}

impl RecoverySchedule {
    pub(crate) fn new(export_interval: Duration) -> Self {
        Self {
            base: export_interval,
            ceiling: export_interval.max(Duration::from_secs(30)),
            next: None,
            failures: 0,
            ids: RandomIdGenerator::default(),
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.next
    }

    pub(crate) fn arm_startup(&mut self) {
        if self.next.is_none() {
            self.schedule();
        }
    }

    pub(crate) fn record_result(&mut self, success: bool) {
        if success {
            self.failures = 0;
            self.next = None;
        } else {
            self.schedule();
            self.failures = self.failures.saturating_add(1);
        }
    }

    fn schedule(&mut self) {
        // Equal jitter keeps a positive floor, avoids coordinated retries
        // across replicas, and caps retries at one per 15–30s at saturation
        // with the production 5s export interval. No missed-tick catch-up.
        let random = u64::from_be_bytes(self.ids.new_span_id().to_bytes());
        self.next = Some(Instant::now() + self.delay(random));
    }

    fn delay(&self, random: u64) -> Duration {
        let upper = self
            .base
            .saturating_mul(1_u32 << self.failures.min(31))
            .min(self.ceiling);
        let lower = upper / 2;
        let jitter = (upper - lower).mul_f64((random as f64) / (u64::MAX as f64));
        lower + jitter
    }
}

/// A real, sampled root span with a closed name/scope, no attributes, no
/// baggage, no links, and no ambient business context. It passes through the
/// very same exporter and its configured Resource as ordinary trace batches.
pub(crate) fn recovery_span() -> SpanData {
    let ids = RandomIdGenerator::default();
    let now = SystemTime::now();
    SpanData {
        span_context: SpanContext::new(
            ids.new_trace_id(),
            ids.new_span_id(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::INVALID,
        parent_span_is_remote: false,
        span_kind: SpanKind::Internal,
        name: "musteragent.telemetry.trace.recovery".into(),
        start_time: now,
        end_time: now,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: Default::default(),
        links: Default::default(),
        // Export success has not happened yet, so do not mark it successful.
        status: Status::Unset,
        instrumentation_scope: InstrumentationScope::builder("musteragent-telemetry-internal")
            .build(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_has_positive_jitter_bounds_and_saturates() {
        let mut schedule = RecoverySchedule::new(Duration::from_secs(5));
        for (failures, lower, upper) in [
            (0, 2.5, 5.0),
            (1, 5.0, 10.0),
            (2, 10.0, 20.0),
            (3, 15.0, 30.0),
            (u32::MAX, 15.0, 30.0),
        ] {
            schedule.failures = failures;
            assert_eq!(schedule.delay(0), Duration::from_secs_f64(lower));
            assert_eq!(schedule.delay(u64::MAX), Duration::from_secs_f64(upper));
            assert!(schedule.delay(u64::MAX / 2) > schedule.delay(0));
            assert!(schedule.delay(u64::MAX / 2) < schedule.delay(u64::MAX));
        }
        schedule.record_result(true);
        assert_eq!(schedule.failures, 0);
        assert!(schedule.deadline().is_none());
    }

    #[test]
    fn internal_probe_is_nonempty_privacy_safe_and_not_a_business_child() {
        let span = recovery_span();
        assert!(span.span_context.is_valid());
        assert!(span.span_context.is_sampled());
        assert_eq!(span.parent_span_id, SpanId::INVALID);
        assert!(span.attributes.is_empty());
        assert!(span.events.is_empty());
        assert!(span.links.is_empty());
        assert_eq!(span.span_context.trace_state(), &TraceState::default());
        assert_eq!(span.name, "musteragent.telemetry.trace.recovery");
    }
}
