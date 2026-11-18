//! Sealed-invocation authorization, separate from physical execution ownership.
use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, DurableToolInvocation, InteractionResolution, InvocationAuthorization,
    ToolExecutionMaterial, authorize_invocation,
};
use musteragent_runtime_contract::ErrorCode;
pub(crate) fn validate_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    if material.tool.execution_target != musteragent_tool::CapabilityExecutionTarget::RustRuntime {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    validate_sealed_invocation(manifest, attempt, material, durable, invocation)
}

/// Shared identity/grant checks. The owning adapter separately qualifies the
/// physical execution target; an atomic commit does not imply local execution.
pub(crate) fn validate_sealed_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    let mut authorization = material.authorization.clone();
    authorization.approval_satisfied = matches!(
        durable.interaction_resolution,
        Some(InteractionResolution::Approval(
            musteragent_approval::ApprovalDecision::Approve
                | musteragent_approval::ApprovalDecision::ApproveSession
        ))
    );
    validate_authorized_invocation(
        manifest,
        attempt,
        material,
        durable,
        invocation,
        &authorization,
    )
}

/// Exact verified request arguments are evaluated by the original dispatch
/// authorizer; this never turns an explicit grant or pending approval into Allow.
pub(crate) fn validate_argument_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
    arguments: &serde_json::Value,
    observed_at: musteragent_identity::UtcTimestamp,
) -> Result<(), LoopPortError> {
    if material.tool.execution_target != musteragent_tool::CapabilityExecutionTarget::RustRuntime {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    let authorization = crate::ToolDispatchAuthorizer::default().effective_authorization(
        material,
        durable,
        arguments,
        None,
        observed_at,
    )?;
    validate_authorized_invocation(
        manifest,
        attempt,
        material,
        durable,
        invocation,
        &authorization,
    )
}

fn validate_authorized_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
    authorization: &InvocationAuthorization,
) -> Result<(), LoopPortError> {
    durable
        .validate()
        .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
    let tool = &material.tool.manifest;
    let grant = &material.capability_grant;
    let persisted = &durable.invocation;
    if manifest.location != grant.location
        || authorize_invocation(tool, grant, &material.tool.execution_target, authorization)
            != musteragent_policy::Decision::Allow
        || manifest.provider_id != grant.provider_id
        || manifest.key != grant.key
        || manifest.effect != grant.effect
        || manifest.provider_id != tool.capability_provider_id
        || manifest.key.as_str() != tool.capability_key.as_str()
        || manifest.required_scopes != tool.required_scopes
        || manifest.required_contexts != tool.required_contexts
        || invocation.tenant_id != material.run_context.tenant_id
        || invocation.conversation_id != material.run_context.conversation_id
        || invocation.run_id != attempt.run_id
        || invocation.execution_attempt_id != attempt.attempt_id
        || invocation.execution_attempt_number != attempt.attempt_number
        || invocation.invocation_id != persisted.invocation_id
        || invocation.tool_call_id != persisted.tool_call_id
        || invocation.tool_key != tool.key
        || invocation.capability_key != manifest.key
        || invocation.capability_provider_id != manifest.provider_id
        || invocation.timing != persisted.timing
        || invocation.idempotency_key != persisted.idempotency_key
        || invocation.request_ref != persisted.request_ref
        || invocation.request_schema != tool.input_schema
        || invocation.result_schema != tool.output_schema
        || persisted.status != musteragent_tool::ToolInvocationStatus::Running
        || durable.retry_contract != tool.retry_contract
        || durable.recorded_outcome.is_some()
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

/// Dialogue storage and orchestration retain their account namespace even when
/// a Device Run has a physical working directory. Only the original sealed
/// Dialogue Device policy may use that directory; a Work project is distinct.
pub fn dialogue_workspace_matches(
    material: &ToolExecutionMaterial,
    invocation_workspace: Option<musteragent_identity::WorkspaceId>,
) -> bool {
    dialogue_workspace_scope_matches(
        &material.run_context,
        material
            .discovery
            .as_ref()
            .map(|d| d.discovery_policy_ref.as_str()),
        material.execution_grant_proof.is_some(),
        invocation_workspace,
    )
}
fn dialogue_workspace_scope_matches(
    run: &musteragent_run::RunContext,
    policy: Option<&str>,
    has_device_proof: bool,
    invocation_workspace: Option<musteragent_identity::WorkspaceId>,
) -> bool {
    use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
    match run.execution_scope {
        ExecutionScope::Tenant { .. } => invocation_workspace.is_none(),
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id,
        } => {
            workspace_id == invocation_workspace
                && (workspace_id.is_none()
                    || (has_device_proof
                        && policy.is_some_and(
                            musteragent_runtime_snapshot::is_dialogue_device_discovery_policy,
                        )
                        && run.control_home == (RuntimeHome::Device { device_id })
                        && run.execution_home == (RuntimeHome::Device { device_id })))
        }
        ExecutionScope::Workspace { .. } | ExecutionScope::HostedProject { .. } => false,
    }
}
#[cfg(test)]
#[path = "atomic_authorization/dialogue_workspace_tests.rs"]
mod dialogue_workspace_tests;
