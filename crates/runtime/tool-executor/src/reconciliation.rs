//! Rebuild the original invocation identity for result reconciliation.
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityReconciliation, DurableToolInvocation, ToolExecutionMaterial,
};

pub(crate) fn reconciliation_invocation(
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    request: &CapabilityReconciliation,
) -> CapabilityInvocation {
    CapabilityInvocation {
        actor: material.actor,
        tenant_id: material.run_context.tenant_id,
        conversation_id: material.run_context.conversation_id,
        run_id: material.run_context.run_id,
        requested_by_attempt_id: durable.invocation.requested_by_attempt_id,
        execution_attempt_id: attempt.attempt_id,
        execution_attempt_number: attempt.attempt_number,
        invocation_id: durable.invocation.invocation_id,
        tool_call_id: durable.invocation.tool_call_id.clone(),
        tool_key: durable.invocation.tool_key.clone(),
        capability_key: material.capability_grant.key.clone(),
        capability_provider_id: durable.invocation.capability_provider_id,
        idempotency_key: durable.invocation.idempotency_key.clone(),
        request_ref: durable.invocation.request_ref.clone(),
        request_schema: material.tool.manifest.input_schema.clone(),
        result_schema: material.tool.manifest.output_schema.clone(),
        authorized_content: material.authorized_content.clone(),
        workspace_id: request.workspace_id,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: request.deadline },
        trace_context: request.trace_context.clone(),
        execution_grant_proof: material.execution_grant_proof.clone(),
    }
}
