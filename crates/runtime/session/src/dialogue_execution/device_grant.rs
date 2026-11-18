//! One source-to-grant binding shared by Cloud custody and native admission.
//! Signature, live membership and current policy remain their original owners.
use super::*;
use musteragent_execution_context::DialogueInputSurface;
use musteragent_execution_grant::{ExecutionGrant, ExecutionGrantAuthorityScope};
use musteragent_identity::DeviceId;

pub fn validate_device_grant(
    source: &DialogueExecutionPreparation,
    device: DeviceId,
    grant: &ExecutionGrant,
) -> Result<(), DialogueStoreError> {
    source.validate()?;
    grant.validate().map_err(|_| DialogueStoreError::Corrupt)?;
    let local = source.local_execution_binding(device)?;
    let surface = match source.intent.origin.surface {
        DialogueInputSurface::Desktop => "desktop",
        DialogueInputSurface::Mobile => "mobile",
        _ => return Err(DialogueStoreError::Corrupt),
    };
    let identity = grant.identity_binding.ok_or(DialogueStoreError::Corrupt)?;
    if grant.tenant_id != source.scope.tenant_id
        || grant.run_id != source.identities.run_id()?
        || grant.audience.as_str() != format!("musteragent.runtime.device.id-{device}")
        || grant.nonce.as_str() != local.grant_nonce()
        || grant.surface.as_ref().map(|s| s.as_str()) != Some(surface)
        || grant.permission_mode != Some(source.configuration.permission_mode)
        || identity.principal_id != source.scope.owner_id
        || identity.authority_scope
            != (ExecutionGrantAuthorityScope::Tenant {
                tenant_id: source.scope.tenant_id,
            })
        || identity.device_id != Some(device)
        || identity.device_workspace_id != source.device_workspace_id
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}

/// Reauthorization preserves every business fact. Its finite time window is a
/// new authority decision; it is never a mutation of the retained old proof.
pub fn validate_device_reauthorization(
    source: &DialogueExecutionPreparation,
    device: DeviceId,
    previous: &ExecutionGrant,
    successor: &ExecutionGrant,
) -> Result<(), DialogueStoreError> {
    validate_device_grant(source, device, previous)?;
    validate_device_grant(source, device, successor)?;
    let mut business = successor.clone();
    business.issued_at = previous.issued_at;
    business.expires_at = previous.expires_at;
    if business != *previous
        || successor.issued_at < previous.expires_at
        || successor.expires_at <= successor.issued_at
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(())
}
