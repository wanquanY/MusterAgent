use super::*;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopControl,
};
use musteragent_runtime_capability::{CapabilityExecution, validate_capability_execution_binding};
use musteragent_runtime_contract::execution_liveness::{
    ExecutionBinding, InvocationExecutionTiming,
};
use musteragent_tool::ToolExecutionPolicy;

pub type ToolPhysicalExecution<'a> =
    Box<dyn ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> + 'a>;

pub(crate) fn validate(
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
    binding: ExecutionBinding,
) -> Result<(), LoopPortError> {
    validate_invocation_timing(durable, invocation)?;
    validate_capability_execution_binding(invocation, attempt, binding)
        .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
    let (ToolExecutionPolicy::ActivityV1(policy), InvocationExecutionTiming::ActivityV1(timing)) =
        (&material.tool.manifest.execution_policy, &invocation.timing)
    else {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    };
    if policy.reference() != &timing.policy
        || material.actor != invocation.actor
        || durable.invocation.status != musteragent_tool::ToolInvocationStatus::Running
        || durable.invocation.run_id != invocation.run_id
        || durable.invocation.requested_by_attempt_id != invocation.requested_by_attempt_id
        || durable.invocation.execution_attempt_id != Some(invocation.execution_attempt_id)
        || durable.invocation.tool_call_id != invocation.tool_call_id
        || durable.invocation.idempotency_key != invocation.idempotency_key
        || durable.invocation.capability_provider_id != invocation.capability_provider_id
        || material.tool.manifest.input_schema != invocation.request_schema
        || material.tool.manifest.output_schema != invocation.result_schema
        || material.authorized_content != invocation.authorized_content
        || material.execution_grant_proof != invocation.execution_grant_proof
        || material.run_context.tenant_id != invocation.tenant_id
        || material.run_context.run_id != invocation.run_id
        || material.run_context.conversation_id != invocation.conversation_id
        || durable.invocation.invocation_id != invocation.invocation_id
        || durable.invocation.request_ref != invocation.request_ref
        || durable.invocation.tool_key != invocation.tool_key
        || material.tool.manifest.key != invocation.tool_key
        || material.capability_grant.provider_id != invocation.capability_provider_id
        || material.capability_grant.key != invocation.capability_key
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}

pub(super) fn validate_operation<'a>(
    operation: ToolPhysicalExecution<'a>,
    binding: ExecutionBinding,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    if operation.binding() != binding || operation.stop_control().binding() != binding {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(operation)
}
pub(super) fn provider_operation<'a>(
    operation: CapabilityExecution,
    binding: ExecutionBinding,
) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
    validate_operation(Box::new(ProviderOperation(operation)), binding)
}
struct ProviderOperation(CapabilityExecution);
impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for ProviderOperation
{
    fn binding(&self) -> ExecutionBinding {
        self.0.binding()
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.0.stop_control()
    }
    fn execute(
        self: Box<Self>,
        activity: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            Ok(DispatchedCapabilityOutcome::Provider(
                self.0.execute(activity).await,
            ))
        })
    }
}
