//! One structural ExecutionGrant decoder for gRPC, native Home and peer admission.
//! A decoded SignedExecutionGrant remains unverified until its signature and
//! expected admission bindings have been checked by the authority composition.
mod capability_entitlement;
#[cfg(test)]
mod hosted_execution_tests;
mod model_route;
use crate::wire;
use crate::wire_decode::{WireDecodeError, invalid, parse_id, required, timestamp};
pub(crate) use capability_entitlement::decode as capability_entitlement;
use model_route::{decode as granted_model_routes, decode_auxiliary};
use musteragent_execution_grant::{
    DeviceDirectProvider, EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, GrantedAuxiliaryModelRoute, MAX_EXECUTION_GRANT_PROOF_BYTES,
    SignedExecutionGrant,
};
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelProviderId,
};
use musteragent_policy::PermissionMode;
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use prost::Message as _;
const NANOS_PER_MILLISECOND: i32 = 1_000_000;

/// Strict V10 raw boundary. The result is still unverified: callers must verify
/// the signature, complete assignment, current authority and execution lease.
pub fn distributed_execution_grant_bytes(
    bytes: &[u8],
) -> Result<SignedExecutionGrant, WireDecodeError> {
    let field = "signed_execution_grant.v10";
    if bytes.is_empty() || bytes.len() > MAX_EXECUTION_GRANT_PROOF_BYTES {
        return Err(invalid(field, "proof bounds"));
    }
    let wire = wire::SignedExecutionGrant::decode(bytes)
        .map_err(|_| invalid(field, "invalid protobuf"))?;
    if wire
        .claims
        .as_ref()
        .is_none_or(|claims| claims.schema_version != EXECUTION_GRANT_SCHEMA_V10)
    {
        return Err(invalid(field, "V10 assignment authority required"));
    }
    let signed = signed_execution_grant(Some(wire))?;
    signed
        .claims
        .validate()
        .map_err(|error| invalid(field, error))?;
    if crate::signed_execution_grant_message(&signed).encode_to_vec() != bytes {
        return Err(invalid(field, "noncanonical, duplicate or unknown fields"));
    }
    Ok(signed)
}

pub fn signed_execution_grant(
    value: Option<wire::SignedExecutionGrant>,
) -> Result<SignedExecutionGrant, WireDecodeError> {
    let value = required(value, "signed_execution_grant")?;
    if value.context_transport_annex.len() > 64 * 1024 {
        return Err(invalid(
            "signed_execution_grant.context_transport_annex",
            "annex exceeds admission bound",
        ));
    }
    let claims = required(value.claims, "signed_execution_grant.claims")?;
    if matches!(claims.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10) {
        for (value, field) in [
            (&claims.issued_at, "signed_execution_grant.claims.issued_at"),
            (
                &claims.expires_at,
                "signed_execution_grant.claims.expires_at",
            ),
        ] {
            let value = value.as_ref().ok_or(WireDecodeError::Missing(field))?;
            if !(-62_135_596_800..=253_402_300_799).contains(&value.seconds)
                || !(0..1_000_000_000).contains(&value.nanos)
                || value.nanos % NANOS_PER_MILLISECOND != 0
            {
                return Err(invalid(
                    field,
                    "hosted grants require a valid millisecond-precision Protobuf timestamp",
                ));
            }
        }
    }
    let signature: [u8; 64] = value.signature.try_into().map_err(|value: Vec<u8>| {
        invalid(
            "signed_execution_grant.signature",
            format!("signature must contain 64 bytes, received {}", value.len()),
        )
    })?;
    let (model_route, fallback_model_routes) = granted_model_routes(claims.model_route)?;
    let identity_binding = execution_grant_identity_binding(
        claims.principal_id,
        claims.authority_scope,
        claims.device_id,
        claims.device_workspace_id,
    )?;
    let capability_entitlements = claims
        .capability_entitlements
        .into_iter()
        .map(capability_entitlement)
        .collect::<Result<Vec<_>, _>>()?;
    let auxiliary_model_routes = claims
        .auxiliary_model_routes
        .into_iter()
        .map(|auxiliary| {
            let role = match wire::AuxiliaryModelRole::try_from(auxiliary.role) {
                Ok(wire::AuxiliaryModelRole::Compression) => AuxiliaryModelRole::Compression,
                Ok(wire::AuxiliaryModelRole::Vision) => AuxiliaryModelRole::Vision,
                Ok(wire::AuxiliaryModelRole::Embedding) => AuxiliaryModelRole::Embedding,
                Ok(wire::AuxiliaryModelRole::Title) => AuxiliaryModelRole::Title,
                Ok(wire::AuxiliaryModelRole::Routing) => AuxiliaryModelRole::Routing,
                Ok(wire::AuxiliaryModelRole::Evolution) => AuxiliaryModelRole::Evolution,
                Ok(wire::AuxiliaryModelRole::GoalJudge) => AuxiliaryModelRole::GoalJudge,
                Ok(wire::AuxiliaryModelRole::Decision) => AuxiliaryModelRole::Decision,
                _ => {
                    return Err(WireDecodeError::UnsupportedEnum {
                        field: "signed_execution_grant.claims.auxiliary_model_routes.role",
                        value: auxiliary.role,
                    });
                }
            };
            Ok(GrantedAuxiliaryModelRoute {
                role,
                route: decode_auxiliary(auxiliary.model_route)?,
                credential_grant_ref: CredentialGrantRef::new(auxiliary.credential_grant_ref)
                    .map_err(|error| {
                        invalid(
                            "signed_execution_grant.claims.auxiliary_model_routes.credential_grant_ref",
                            error,
                        )
                    })?,
            })
        })
        .collect::<Result<Vec<_>, WireDecodeError>>()?;
    let tool_contract_releases =
        claims
            .tool_contract_releases
            .into_iter()
            .map(|release| {
                let content_sha256: [u8; 32] = release.content_sha256.try_into().map_err(
                    |value: Vec<u8>| {
                        invalid(
                            "signed_execution_grant.claims.tool_contract_releases.content_sha256",
                            format!(
                                "content_sha256 must contain 32 bytes, received {}",
                                value.len()
                            ),
                        )
                    },
                )?;
                Ok(musteragent_execution_grant::GrantedToolContractRelease {
                    release_ref: OpaqueExternalRef::new(release.release_ref).map_err(|error| {
                        invalid(
                            "signed_execution_grant.claims.tool_contract_releases.release_ref",
                            error,
                        )
                    })?,
                    schema_version: DottedKey::new(release.schema_version).map_err(|error| {
                        invalid(
                            "signed_execution_grant.claims.tool_contract_releases.schema_version",
                            error,
                        )
                    })?,
                    tool_key: VersionedKey::new(release.tool_key).map_err(|error| {
                        invalid(
                            "signed_execution_grant.claims.tool_contract_releases.tool_key",
                            error,
                        )
                    })?,
                    available: release.available,
                    description: release.description,
                    input_schema_json: release.input_schema_json,
                    content_sha256,
                    execution_binding_ref: release.execution_binding_ref,
                })
            })
            .collect::<Result<Vec<_>, WireDecodeError>>()?;
    let device_direct_provider = claims
        .device_direct_provider
        .map(|provider| {
            let wire::DeviceDirectProvider {
                base_url,
                api_format,
                model,
                credential_ref,
            } = provider;
            Ok::<DeviceDirectProvider, WireDecodeError>(DeviceDirectProvider {
                base_url,
                api_format: ApiFormat::parse_control_plane(&api_format).map_err(|error| {
                    invalid(
                        "signed_execution_grant.claims.device_direct_provider.api_format",
                        error,
                    )
                })?,
                model,
                credential_ref,
            })
        })
        .transpose()?;
    let vision_direct_provider = claims
        .vision_direct_provider
        .map(|provider| {
            let wire::DeviceDirectProvider {
                base_url,
                api_format,
                model,
                credential_ref,
            } = provider;
            Ok::<DeviceDirectProvider, WireDecodeError>(DeviceDirectProvider {
                base_url,
                api_format: ApiFormat::parse_control_plane(&api_format).map_err(|error| {
                    invalid(
                        "signed_execution_grant.claims.vision_direct_provider.api_format",
                        error,
                    )
                })?,
                model,
                credential_ref,
            })
        })
        .transpose()?;
    Ok(SignedExecutionGrant {
        context_transport_annex: value.context_transport_annex,
        key_id: DottedKey::new(value.key_id)
            .map_err(|error| invalid("signed_execution_grant.key_id", error))?,
        claims: ExecutionGrant {
            reviewed_application_source_sha256: claims
                .reviewed_application_source_sha256
                .into_iter()
                .map(|value| {
                    musteragent_content_ref::Sha256::from_bytes(value).map_err(|_| {
                        invalid(
                            "signed_execution_grant.claims.reviewed_application_source_sha256",
                            "invalid SHA256",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            product_release: claims
                .product_release
                .map(crate::product_release_codec::decode)
                .transpose()
                .map_err(|_| {
                    invalid(
                        "signed_execution_grant.claims.product_release",
                        "invalid release binding",
                    )
                })?,
            schema_version: DottedKey::new(if claims.schema_version.is_empty() {
                EXECUTION_GRANT_SCHEMA_V10
            } else {
                claims.schema_version.as_str()
            })
            .map_err(|error| invalid("signed_execution_grant.claims.schema_version", error))?,
            grant_ref: CredentialGrantRef::new(claims.grant_ref)
                .map_err(|error| invalid("signed_execution_grant.claims.grant_ref", error))?,
            issuer: DottedKey::new(claims.issuer)
                .map_err(|error| invalid("signed_execution_grant.claims.issuer", error))?,
            audience: DottedKey::new(claims.audience)
                .map_err(|error| invalid("signed_execution_grant.claims.audience", error))?,
            tenant_id: parse_id(
                claims.tenant_id.map(|item| item.value),
                "signed_execution_grant.claims.tenant_id",
            )?,
            model_provider_id: claims
                .model_provider_id
                .parse::<ModelProviderId>()
                .map_err(|error| {
                    invalid("signed_execution_grant.claims.model_provider_id", error)
                })?,
            model_entry_id: claims
                .model_entry_id
                .parse::<ModelEntryId>()
                .map_err(|error| invalid("signed_execution_grant.claims.model_entry_id", error))?,
            run_id: parse_id(
                claims.run_id.map(|item| item.value),
                "signed_execution_grant.claims.run_id",
            )?,
            issued_at: timestamp(claims.issued_at, "signed_execution_grant.claims.issued_at")?,
            expires_at: timestamp(
                claims.expires_at,
                "signed_execution_grant.claims.expires_at",
            )?,
            nonce: OpaqueExternalRef::new(claims.nonce)
                .map_err(|error| invalid("signed_execution_grant.claims.nonce", error))?,
            requested_model_class: ModelClass::new(claims.requested_model_class).map_err(
                |error| invalid("signed_execution_grant.claims.requested_model_class", error),
            )?,
            model_route,
            fallback_model_routes,
            auxiliary_model_routes,
            agent_contract: (!claims.agent_contract.is_empty())
                .then(|| VersionedKey::new(claims.agent_contract))
                .transpose()
                .map_err(|error| invalid("signed_execution_grant.claims.agent_contract", error))?,
            surface: (!claims.surface.is_empty())
                .then(|| DottedKey::new(claims.surface))
                .transpose()
                .map_err(|error| invalid("signed_execution_grant.claims.surface", error))?,
            capability_entitlements,
            tool_contract_releases,
            identity_binding,
            permission_mode: grant_permission_mode(claims.permission_mode)?,
            device_direct_provider,
            vision_direct_provider,
        },
        signature,
    })
}

/// Decode one independently resolved no-fallback model route carried by a
/// control-plane RPC. This reuses the exact strict mapping used for signed
/// auxiliary routes, so scheduling cannot accept wire material that admission
/// would later reject.
pub fn independent_model_route(
    value: Option<wire::GrantedModelRoute>,
) -> Result<musteragent_model::ModelRoute, WireDecodeError> {
    decode_auxiliary(value)
}

fn grant_permission_mode(value: i32) -> Result<Option<PermissionMode>, WireDecodeError> {
    match wire::PermissionMode::try_from(value) {
        Ok(wire::PermissionMode::Unspecified) => Ok(None),
        Ok(wire::PermissionMode::Supervised) => Ok(Some(PermissionMode::Supervised)),
        Ok(wire::PermissionMode::Trusted) => Ok(Some(PermissionMode::Trusted)),
        Ok(wire::PermissionMode::Deny) => Ok(Some(PermissionMode::Deny)),
        Ok(wire::PermissionMode::FullAccess) => Ok(Some(PermissionMode::FullAccess)),
        Ok(wire::PermissionMode::ServiceManaged) => Ok(Some(PermissionMode::ServiceManaged)),
        Err(_) => Err(WireDecodeError::UnsupportedEnum {
            field: "signed_execution_grant.claims.permission_mode",
            value,
        }),
    }
}

pub fn execution_grant_identity_binding(
    principal_id: Option<wire::PrincipalId>,
    authority_scope: Option<wire::ExecutionScope>,
    device_id: Option<wire::DeviceId>,
    device_workspace_id: Option<wire::WorkspaceId>,
) -> Result<Option<ExecutionGrantIdentityBinding>, WireDecodeError> {
    let (principal_id, authority_scope) = match (principal_id, authority_scope) {
        (Some(principal_id), Some(authority_scope)) => (principal_id, authority_scope),
        (None, None) if device_id.is_none() && device_workspace_id.is_none() => return Ok(None),
        _ => {
            return Err(invalid(
                "signed_execution_grant.claims.identity_binding",
                "principal_id and authority_scope must be present together",
            ));
        }
    };
    let authority_scope = match authority_scope.scope {
        Some(wire::execution_scope::Scope::Tenant(scope)) => ExecutionGrantAuthorityScope::Tenant {
            tenant_id: parse_id(
                scope.tenant_id.map(|item| item.value),
                "signed_execution_grant.claims.authority_scope.tenant_id",
            )?,
        },
        Some(wire::execution_scope::Scope::Workspace(scope)) => {
            ExecutionGrantAuthorityScope::Workspace {
                workspace_id: parse_id(
                    scope.workspace_id.map(|item| item.value),
                    "signed_execution_grant.claims.authority_scope.workspace_id",
                )?,
            }
        }
        Some(wire::execution_scope::Scope::HostedProject(scope)) => {
            ExecutionGrantAuthorityScope::HostedProject {
                scope: crate::decode_hosted_scope(Some(scope)).map_err(|error| {
                    invalid("signed_execution_grant.claims.authority_scope", error)
                })?,
            }
        }
        Some(wire::execution_scope::Scope::HostedProjectExecution(value)) => {
            ExecutionGrantAuthorityScope::HostedProjectExecution {
                scope: crate::decode_hosted_scope(value.scope).map_err(|error| {
                    invalid("signed_execution_grant.claims.authority_scope", error)
                })?,
                assignment_digest: required(
                    value.execution_assignment_digest,
                    "signed_execution_grant.claims.execution_assignment_digest",
                )?
                .value
                .try_into()
                .map_err(|_| {
                    invalid(
                        "signed_execution_grant.claims.execution_assignment_digest",
                        "must contain exactly 32 bytes",
                    )
                })?,
            }
        }
        Some(wire::execution_scope::Scope::DevicePrivate(_)) | None => {
            return Err(invalid(
                "signed_execution_grant.claims.authority_scope",
                "must be tenant, workspace or hosted project",
            ));
        }
    };
    Ok(Some(ExecutionGrantIdentityBinding {
        principal_id: parse_id(
            Some(principal_id.value),
            "signed_execution_grant.claims.principal_id",
        )?,
        authority_scope,
        device_id: device_id
            .map(|item| parse_id(Some(item.value), "signed_execution_grant.claims.device_id"))
            .transpose()?,
        device_workspace_id: device_workspace_id
            .map(|item| {
                parse_id(
                    Some(item.value),
                    "signed_execution_grant.claims.device_workspace_id",
                )
            })
            .transpose()?,
    }))
}
