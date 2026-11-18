//! Owned physical work, polled by its existing Runtime task. No admission,
//! replacement dispatch, timeout policy or background task is created here.
use crate::{ExecutionActivitySink, ExecutionStopControl};
use musteragent_runtime_contract::{ApplicationFuture, execution_liveness::ExecutionBinding};
use std::sync::Arc;

/// A resource owner exposes the same exact binding through its work and stop
/// ports. Returning from work does not establish a durable business result;
/// the original caller still owns its Journal settlement. Only adapters may
/// produce source evidence, and only validated native end may call sink.ended.
pub trait ExecutionOperation<'owner, T>: Send {
    fn binding(&self) -> ExecutionBinding;
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl>;
    /// Consumes the original prepared object once, after original admission.
    /// Stop requests must permanently prevent future dispatch, including in
    /// the gap between object construction and this future's first poll.
    fn execute(
        self: Box<Self>,
        activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'owner, T>;
}

/// Read-only cancellation edge passed down the original physical call. It owns
/// no timeout policy and cannot itself claim that remote work has stopped.
pub trait ExecutionStopSignal: Send + Sync {
    fn binding(&self) -> ExecutionBinding;
    fn stopping(&self) -> bool;
    fn cancelled(&self) -> ApplicationFuture<'_, ()>;
}
