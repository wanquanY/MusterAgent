//! Strict scalar and logical scope decoding for durable contracts.
use crate::{
    wire,
    wire_decode::{WireDecodeError, invalid, parse_id, required},
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::ExecutionScope;
use musteragent_identity::{Actor, PrincipalKind, WorkspaceId};
fn principal_kind(value: i32) -> Result<PrincipalKind, WireDecodeError> {
    match wire::PrincipalKind::try_from(value) {
        Ok(wire::PrincipalKind::User) => Ok(PrincipalKind::User),
        Ok(wire::PrincipalKind::Service) => Ok(PrincipalKind::Service),
        Ok(wire::PrincipalKind::System) => Ok(PrincipalKind::System),
        _ => Err(WireDecodeError::UnsupportedEnum {
            field: "actor.principal_kind",
            value,
        }),
    }
}

pub fn actor(value: Option<wire::Actor>) -> Result<Actor, WireDecodeError> {
    let value = required(value, "actor")?;
    Ok(Actor {
        tenant_id: parse_id(value.tenant_id.map(|item| item.value), "actor.tenant_id")?,
        principal_id: parse_id(
            value.principal_id.map(|item| item.value),
            "actor.principal_id",
        )?,
        principal_kind: principal_kind(value.principal_kind)?,
    })
}

pub fn sha256(value: Option<wire::Sha256>, field: &'static str) -> Result<Sha256, WireDecodeError> {
    Sha256::from_bytes(required(value, field)?.value).map_err(|error| invalid(field, error))
}

pub fn content_ref(
    value: Option<wire::ContentRef>,
    field: &'static str,
) -> Result<ContentRef, WireDecodeError> {
    let value = required(value, field)?;
    Ok(ContentRef {
        content_id: parse_id(
            value.content_id.map(|item| item.value),
            "content_ref.content_id",
        )?,
        media_type: MediaType::new(value.media_type)
            .map_err(|error| invalid("content_ref.media_type", error))?,
        byte_length: value.byte_length,
        sha256: sha256(value.sha256, "content_ref.sha256")?,
    })
}

pub fn execution_scope(
    value: Option<wire::ExecutionScope>,
) -> Result<ExecutionScope, WireDecodeError> {
    match required(value, "execution_scope")?.scope {
        Some(wire::execution_scope::Scope::Tenant(scope)) => Ok(ExecutionScope::Tenant {
            tenant_id: parse_id(
                scope.tenant_id.map(|item| item.value),
                "execution_scope.tenant_id",
            )?,
        }),
        Some(wire::execution_scope::Scope::Workspace(scope)) => Ok(ExecutionScope::Workspace {
            workspace_id: parse_id(
                scope.workspace_id.map(|item| item.value),
                "execution_scope.workspace_id",
            )?,
        }),
        Some(wire::execution_scope::Scope::DevicePrivate(scope)) => {
            Ok(ExecutionScope::DevicePrivate {
                device_id: parse_id(
                    scope.device_id.map(|item| item.value),
                    "execution_scope.device_id",
                )?,
                workspace_id: scope
                    .workspace_id
                    .map(|item| {
                        item.value
                            .parse::<WorkspaceId>()
                            .map_err(|error| invalid("execution_scope.workspace_id", error))
                    })
                    .transpose()?,
            })
        }
        Some(wire::execution_scope::Scope::HostedProject(scope)) => {
            Ok(ExecutionScope::HostedProject {
                scope: crate::decode_hosted_scope(Some(scope))
                    .map_err(|error| invalid("execution_scope.hosted_project", error))?,
            })
        }
        Some(wire::execution_scope::Scope::HostedProjectExecution(_)) => Err(invalid(
            "execution_scope",
            "assigned execution authority cannot be a command's logical scope",
        )),
        None => Err(WireDecodeError::Missing("execution_scope.scope")),
    }
}
