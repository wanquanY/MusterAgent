//! Home-owned observation and clock mechanisms; policy belongs to the kernel.
#![forbid(unsafe_code)]
mod clock;
mod observations;
pub use clock::SystemLivenessHomeClock;
pub use musteragent_execution_liveness::ObservationBatch;
pub use observations::{ExecutionActivityInbox, HomeExecutionActivityFactory, activity_channel};
