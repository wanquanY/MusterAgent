//! Stable trace, metric, log and propagation contracts for every Runtime target.
//!
//! This crate owns semantic telemetry names and privacy/cardinality policy. It
//! deliberately owns no exporter, task, transport, repository or global SDK.

#![forbid(unsafe_code)]

mod baggage;
mod events;
mod identity;
#[doc(hidden)]
pub mod machine;
mod metrics;
mod model_response;
mod provider_protocol;

pub use baggage::{ControlledBaggage, ControlledBaggageError};
pub use events::TelemetryEventKind;
pub use identity::{
    AttemptTelemetryContext, CommandOperation, CommandTelemetryContext, CommandTerminalOutcome,
    DataEdgeOperation, DataEdgeTelemetryContext, InvocationOperation, InvocationTelemetryContext,
    LearningFailureStage, RunTelemetryContext, TelemetryOperation, TelemetryOutcome,
};
pub use machine::{
    AttemptTelemetry, CommandTelemetry, DataEdgeTelemetry, DbCommitTelemetry,
    DbCommitTelemetryContext, InvocationTelemetry, RuntimeSpan, RuntimeSpanBackend,
    RuntimeTelemetry,
};
pub use metrics::{
    DbCommitKind, DbTransactionPhase, DbTransactionResult, ErrorClass, MetricOperation,
    MetricSchema, QueueKind, TelemetrySignal, WaitReasonClass,
};
pub use model_response::{ModelResponseStage, ModelResponseTiming};
pub use provider_protocol::ProviderProtocolError;

/// Schema version shared by spans, log envelopes, local evidence and dashboards.
pub const TELEMETRY_SCHEMA_VERSION: &str = "musteragent.runtime.telemetry.v2";
