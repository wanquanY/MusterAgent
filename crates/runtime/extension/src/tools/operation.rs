//! Panic/error containment while retaining the physical operation's stop owner.
use super::*;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopConfirmation, ExecutionStopControl,
    ExecutionStopError,
};
use musteragent_tool_executor::DispatchedCapabilityOutcome;

pub(super) fn wrap<'a>(
    result: Result<ToolPhysicalExecution<'a>, HookFailure>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
    owner: String,
    key: String,
    binding: ExecutionBinding,
) -> ToolPhysicalExecution<'a> {
    match result {
        Ok(inner) => Box::new(GuardedOperation {
            inner,
            host,
            context,
            owner,
            key,
        }),
        Err(failure) => Box::new(RejectedPreparation {
            failure,
            host,
            context,
            key,
            binding,
        }),
    }
}
struct GuardedOperation<'a> {
    inner: ToolPhysicalExecution<'a>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
    owner: String,
    key: String,
}
impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for GuardedOperation<'a>
{
    fn binding(&self) -> ExecutionBinding {
        self.inner.binding()
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.inner.stop_control()
    }
    fn execute(
        self: Box<Self>,
        activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        let Self {
            inner,
            host,
            context,
            owner,
            key,
        } = *self;
        Box::pin(async move {
            let result = guard_async(&owner, Hook::Tool, || {
                Box::pin(async { Ok(inner.execute(activity).await) })
            })
            .await;
            host.required(&context, &key, result)
                .await
                .map_err(crate::policy::port_error)?
        })
    }
}
// Construction is explicitly forbidden from doing I/O. A failed construction
// has no physical operation; this control confirms only that never-dispatched
// local state, while execute returns the recorded failure (never success).
struct RejectedPreparation {
    failure: HookFailure,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
    key: String,
    binding: ExecutionBinding,
}
struct NeverDispatched(ExecutionBinding);
impl ExecutionStopControl for NeverDispatched {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }
    fn stop(
        &self,
        _: musteragent_execution_liveness::StopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async { Ok(ExecutionStopConfirmation { binding: self.0 }) })
    }
}
impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for RejectedPreparation
{
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        Arc::new(NeverDispatched(self.binding))
    }
    fn execute(
        self: Box<Self>,
        _: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            self.host
                .required(&self.context, &self.key, Err(self.failure))
                .await
                .map_err(crate::policy::port_error)
        })
    }
}
