//! Resource-owner stop contract. A cancelled waiter or dropped Future is not
//! proof that its process tree or remote operation has stopped.
use crate::{LivenessError, StopReason};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

/// Positive adapter evidence for this exact physical generation. The adapter
/// owns verification (including child processes/remote acknowledgement); the
/// reactor owns durable StopConfirmed and never infers proof from task Drop.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionStopConfirmation {
    pub binding: ExecutionBinding,
}
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionStopError {
    pub code: ErrorCode,
    pub rejection: Option<LivenessError>,
}
/// Bind once at dispatch. Repeated requests must stop/query the same operation,
/// never redispatch it. A local scope may confirm only after all owned resources
/// have quiesced. A remote scope requires the admitted result/stop proof.
pub trait ExecutionStopControl: Send + Sync {
    fn binding(&self) -> ExecutionBinding;
    fn stop(
        &self,
        reason: StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>>;
}
