//! Optional physical-host authority on the existing execution boundary. This
//! port cannot admit a Run or invent a stop proof. Cloud compositions omit it.
use musteragent_execution_liveness::{ExecutionStopControl, LivenessJournalError};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ApplicationFuture;
use std::{sync::Arc, time::Duration};

/// Returned only after the original host qualified this exact Attempt and
/// registered this operation's real stop control atomically with revocation.
pub trait HostExecutionPermit: Send + Sync {
    fn check(&self) -> Result<(), LivenessJournalError>;
    fn revoked(&self) -> ApplicationFuture<'_, ()>;
    /// The original boundary has committed and drained, or the original stop
    /// adapter positively confirmed the exact operation. Drop is not evidence.
    fn settled(&self);
}
pub trait HostExecutionFence: Send + Sync {
    fn acquire<'a>(
        &'a self,
        owner: &'a AttemptContext,
        control: Arc<dyn ExecutionStopControl>,
        cleanup_budget: Duration,
    ) -> ApplicationFuture<'a, Result<Box<dyn HostExecutionPermit>, LivenessJournalError>>;
}
