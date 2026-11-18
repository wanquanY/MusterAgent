use musteragent_event_envelope::ExecutionScope;
use musteragent_execution_grant::{ExecutionGrantAuthorityScope, ExecutionGrantIdentityBinding};
use musteragent_identity::{PrincipalId, TenantId};
use musteragent_runtime_contract::ErrorCode;

/// Maps an already validated product execution scope into the exact identity
/// tuple authenticated by an Execution Grant. Hosted scope retains every
/// project and Home binding in V9; it cannot become tenant/private authority.
pub(crate) fn execution_grant_identity_binding(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    execution_scope: ExecutionScope,
) -> Result<ExecutionGrantIdentityBinding, ErrorCode> {
    let (authority_scope, device_id, device_workspace_id) = match execution_scope {
        ExecutionScope::Tenant {
            tenant_id: scope_tenant,
        } if scope_tenant == tenant_id => (
            ExecutionGrantAuthorityScope::Tenant { tenant_id },
            None,
            None,
        ),
        ExecutionScope::Workspace { workspace_id } => (
            ExecutionGrantAuthorityScope::Workspace { workspace_id },
            None,
            None,
        ),
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id: None,
        } => (
            ExecutionGrantAuthorityScope::Tenant { tenant_id },
            Some(device_id),
            None,
        ),
        ExecutionScope::DevicePrivate {
            device_id,
            workspace_id: Some(workspace_id),
        } => (
            ExecutionGrantAuthorityScope::Tenant { tenant_id },
            Some(device_id),
            Some(workspace_id),
        ),
        ExecutionScope::Tenant { .. } | ExecutionScope::HostedProject { .. } => {
            return Err(ErrorCode::PolicyDenied);
        }
    };
    Ok(ExecutionGrantIdentityBinding {
        principal_id,
        authority_scope,
        device_id,
        device_workspace_id,
    })
}
