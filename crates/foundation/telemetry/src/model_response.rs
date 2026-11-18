use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use musteragent_identity::{InvocationId, RunId};

/// Closed vocabulary shared by response instrumentation and safe log filtering.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ModelResponseStage {
    RequestPrepared,
    HttpSendStarted,
    ResponseHeaders,
    FirstTransportByte,
    FirstProviderEvent,
    FirstText,
    FirstAction,
}

impl ModelResponseStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestPrepared => "request_prepared",
            Self::HttpSendStarted => "http_send_started",
            Self::ResponseHeaders => "response_headers",
            Self::FirstTransportByte => "first_transport_byte",
            Self::FirstProviderEvent => "first_provider_event",
            Self::FirstText => "first_text",
            Self::FirstAction => "first_action",
        }
    }

    #[must_use]
    pub fn from_safe_key(value: &str) -> Option<Self> {
        [
            Self::RequestPrepared,
            Self::HttpSendStarted,
            Self::ResponseHeaders,
            Self::FirstTransportByte,
            Self::FirstProviderEvent,
            Self::FirstText,
            Self::FirstAction,
        ]
        .into_iter()
        .find(|stage| stage.as_str() == value)
    }

    const fn flag(self) -> u8 {
        1 << self as u8
    }
}

/// Per-invocation monotonic diagnostics, bounded to one event per stage.
/// Elapsed time starts before local request preparation, not at HTTP dispatch.
/// Request/response contents cannot be passed to this instrumentation boundary.
pub struct ModelResponseTiming {
    started: Instant,
    run_id: RunId,
    invocation_id: InvocationId,
    observed: AtomicU8,
}

impl ModelResponseTiming {
    #[must_use]
    pub fn new(run_id: RunId, invocation_id: InvocationId) -> Self {
        Self {
            started: Instant::now(),
            run_id,
            invocation_id,
            observed: AtomicU8::new(0),
        }
    }

    #[must_use]
    pub fn has_observed(&self, stage: ModelResponseStage) -> bool {
        self.observed.load(Ordering::Relaxed) & stage.flag() != 0
    }

    pub fn observe(&self, stage: ModelResponseStage) {
        if self.observed.fetch_or(stage.flag(), Ordering::Relaxed) & stage.flag() != 0 {
            return;
        }
        let elapsed_us = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        // Safe diagnostic visitors accept typed strings, never Display/Debug
        // fields. Keep UUIDs as strings so stages can be joined to durable runs.
        tracing::info!(name: "runtime.model.response.stage",
            run_id = self.run_id.to_string().as_str(),
            invocation_id = self.invocation_id.to_string().as_str(),
            model.response.stage = stage.as_str(),
            model.response.elapsed_us = elapsed_us);
    }
}
