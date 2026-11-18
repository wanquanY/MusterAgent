//! Physical Capability ports share the common liveness evidence/stop contract.
use crate::{CapabilityInvocation, CapabilityOutcome, CapabilityRuntimeError};
use musteragent_execution_liveness::ExecutionOperation;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionBinding, ExecutionScope, InvocationExecutionTiming,
};

pub type CapabilityExecution = Box<dyn ExecutionOperation<'static, CapabilityOutcome>>;

/// The original Home owns policy admission and operation identity. An adapter
/// additionally validates the exact invocation and live Attempt before I/O;
/// this check does not substitute for a Grant, membership or owner-fence check.
pub fn validate_capability_execution_binding(
    invocation: &CapabilityInvocation,
    owner: &AttemptContext,
    binding: ExecutionBinding,
) -> Result<(), CapabilityRuntimeError> {
    if !matches!(invocation.timing, InvocationExecutionTiming::ActivityV1(_))
        || invocation.tenant_id != binding.tenant_id
        || invocation.actor.tenant_id != invocation.tenant_id
        || invocation.run_id != binding.run_id
        || owner.run_id != binding.run_id
        || ExecutionScope::Invocation(invocation.invocation_id) != binding.scope
        || invocation.execution_attempt_id != binding.owner_attempt_id
        || owner.attempt_id != binding.owner_attempt_id
        || owner.fencing_token != binding.owner_fence
        || invocation.execution_attempt_number != owner.attempt_number
    {
        return Err(CapabilityRuntimeError::ExecutionBindingMismatch);
    }
    Ok(())
}

mod cooperative;
pub use cooperative::{CooperativeExecution, OperationContext, OperationDependency};
