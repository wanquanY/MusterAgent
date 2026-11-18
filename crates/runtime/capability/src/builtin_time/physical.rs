//! Local computation and content I/O share the cooperative resource owner.
use super::*;
use crate::{
    CapabilityExecution, CapabilityRuntimeError, CooperativeExecution, OperationContext,
    validate_capability_execution_binding,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use std::num::NonZeroU64;

pub(super) struct TimeProgress<'a> {
    context: &'a OperationContext,
}
impl TimeProgress<'_> {
    pub(super) fn checkpoint(&self, cursor: NonZeroU64) -> Result<(), ErrorCode> {
        if self.context.stopping() {
            return Err(ErrorCode::RunTransitionInvalid);
        }
        self.context
            .advanced(cursor)
            .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)
    }
}
pub(super) fn execution(
    provider: Arc<BuiltinTimeCapability>,
    owner: AttemptContext,
    invocation: CapabilityInvocation,
    binding: ExecutionBinding,
) -> Result<CapabilityExecution, CapabilityRuntimeError> {
    validate_capability_execution_binding(&invocation, &owner, binding)?;
    Ok(Box::new(CooperativeExecution::new(
        binding,
        stopped,
        move |context| {
            Box::pin(async move {
                let progress = TimeProgress { context: &context };
                let outcome = provider.invoke_inner(invocation, Some(&progress)).await;
                // Each local computation/content request actually returned.
                context.quiescent();
                outcome
            })
        },
    )))
}
fn stopped() -> CapabilityOutcome {
    failed(
        ErrorCode::RunTransitionInvalid,
        "The original Time execution was stopped",
    )
}
