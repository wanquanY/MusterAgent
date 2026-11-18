//! Deterministic, failure-aware integration infrastructure for Runtime tests.

#![forbid(unsafe_code)]

mod container_readiness;
mod controlled_openai_chat;
mod deterministic;
mod fault;
mod otlp;
mod postgres;
mod provider_sse;
mod provider_wire;

pub use controlled_openai_chat::{ControlledOpenAiChatProvider, ControlledProviderError};
pub use deterministic::{DeterministicIdFactory, ManualClock};
pub use fault::{FaultInjector, FaultPoint, InjectedFault};
pub use otlp::{CapturedMetric, CapturedSpan, LocalOtlpCollector};
pub use postgres::{IsolatedPostgres, PostgresHarnessError};
pub use provider_sse::{
    MockProviderSse, ProviderSseFrame, ProviderSseScenario, ProviderSseServerError,
};
pub use provider_wire::{MockProviderWire, ProviderWireScenario, ProviderWireServerError};

mod minio;
pub use minio::{IsolatedMinio, MINIO_TEST_ACCESS_KEY, MINIO_TEST_SECRET_KEY};

mod timing;
pub use timing::{integration_timeout, test_budget};
