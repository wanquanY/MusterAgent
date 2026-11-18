//! Production OpenTelemetry SDK and OTLP adapter for the Runtime telemetry owner.
//!
//! Export is observational: bounded queue pressure, collector outages and
//! shutdown deadlines update explicit health/counters but never change domain,
//! persistence, provider or capability outcomes.

#![forbid(unsafe_code)]

mod bounded_log;
mod bounded_queue;
mod bounded_trace;
mod config;
mod events;
mod health;
mod logging;
mod metrics;
mod propagation;
mod resource;
mod runtime;
mod trace_recovery;

pub use config::{OtlpMode, TelemetryConfig, TelemetryConfigError, TelemetryDeploymentSnapshot};
pub use health::{SignalHealth, SignalHealthPhase, TelemetryHealth, TelemetryHealthSnapshot};
pub use metrics::RuntimeMetrics;
pub use propagation::{attach_controlled_baggage, current_trace_ids};
pub use runtime::{TelemetryInstallError, TelemetryRuntime};
