//! Persistence codec for the kernel multi-device home identity.
use crate::wire;
use musteragent_event_envelope::{HostedHomeBinding, HostedProjectScope};
use musteragent_identity::{HomeEpoch, IdentityError};
use thiserror::Error;
#[derive(Debug, Error)]
pub enum HostedScopeCodecError {
    #[error("missing or invalid hosted field: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Identity(#[from] IdentityError),
}

pub(crate) type Result<T> = std::result::Result<T, HostedScopeCodecError>;
pub(crate) fn required<T>(value: Option<T>, field: &'static str) -> Result<T> {
    value.ok_or(HostedScopeCodecError::Invalid(field))
}

pub fn decode_hosted_scope(
    value: Option<wire::HostedAuthorityScope>,
) -> Result<HostedProjectScope> {
    let value = required(value, "scope")?;
    Ok(HostedProjectScope {
        project_id: required(value.project_id, "project_id")?.value.parse()?,
        workspace_id: required(value.workspace_id, "workspace_id")?
            .value
            .parse()?,
        resource_tenant_id: required(value.resource_tenant_id, "resource_tenant_id")?
            .value
            .parse()?,
        owner_principal_id: required(value.owner_principal_id, "owner_principal_id")?
            .value
            .parse()?,
        home: decode_home(value.home)?,
    })
}

pub fn encode_hosted_scope(value: HostedProjectScope) -> wire::HostedAuthorityScope {
    wire::HostedAuthorityScope {
        project_id: Some(wire::ProjectId {
            value: value.project_id.to_string(),
        }),
        workspace_id: Some(wire::WorkspaceId {
            value: value.workspace_id.to_string(),
        }),
        resource_tenant_id: Some(wire::TenantId {
            value: value.resource_tenant_id.to_string(),
        }),
        owner_principal_id: Some(wire::PrincipalId {
            value: value.owner_principal_id.to_string(),
        }),
        home: Some(encode_home(value.home)),
    }
}

pub(crate) fn decode_home(value: Option<wire::HostedHomeBinding>) -> Result<HostedHomeBinding> {
    let home = required(value, "home")?;
    Ok(HostedHomeBinding {
        binding_id: required(home.binding_id, "home.binding_id")?
            .value
            .parse()?,
        device_id: required(home.device_id, "home.device_id")?.value.parse()?,
        home_epoch: HomeEpoch::new(home.home_epoch)
            .map_err(|_| HostedScopeCodecError::Invalid("home.home_epoch"))?,
    })
}

pub(crate) fn encode_home(value: HostedHomeBinding) -> wire::HostedHomeBinding {
    wire::HostedHomeBinding {
        binding_id: Some(wire::HostedHomeBindingId {
            value: value.binding_id.to_string(),
        }),
        device_id: Some(wire::DeviceId {
            value: value.device_id.to_string(),
        }),
        home_epoch: value.home_epoch.get(),
    }
}
