use musteragent_capability::{CapabilityEntitlement, CapabilityLocation};
use musteragent_execution_grant::{
    EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1, EncodedExecutionGrantProof,
    ExecutionGrantAuthorityScope, ExecutionGrantProofEncoder, ExecutionGrantProofError,
    SignedExecutionGrant,
};
use musteragent_identity::DottedKey;
use musteragent_model::{AuxiliaryModelRole, ModelParameter, ModelRoute, ProviderStatePolicy};
use musteragent_policy::{Decision, Effect, PermissionMode};
use prost::Message as _;

use crate::wire;
use crate::wire_common::timestamp;

#[cfg(test)]
#[path = "execution_grant_application_sources_tests.rs"]
mod application_sources_tests;

/// Canonical Protobuf proof encoder shared by every Runtime ingress.
///
/// This is deliberately separate from Runtime Snapshot encoding: the Snapshot
/// contains execution material, while this envelope retains the control-plane
/// signature needed by a Device calling a Backend-owned Capability.
#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufExecutionGrantProofEncoder;

impl ExecutionGrantProofEncoder for ProtobufExecutionGrantProofEncoder {
    fn encode(
        &self,
        signed: &SignedExecutionGrant,
    ) -> Result<EncodedExecutionGrantProof, ExecutionGrantProofError> {
        let schema_version = DottedKey::new(EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1)
            .map_err(|_| ExecutionGrantProofError::UnsupportedSchema)?;
        EncodedExecutionGrantProof::seal(
            schema_version,
            signed_execution_grant_message(signed).encode_to_vec(),
            signed,
        )
    }
}

#[must_use]
pub fn signed_execution_grant_message(value: &SignedExecutionGrant) -> wire::SignedExecutionGrant {
    let claims = &value.claims;
    let identity_binding = claims.identity_binding;
    let mut primary_route = model_route(&claims.model_route);
    primary_route.fallback_candidates = claims
        .fallback_model_routes
        .iter()
        .map(model_route)
        .collect();
    wire::SignedExecutionGrant {
        context_transport_annex: value.context_transport_annex.clone(),
        key_id: value.key_id.as_str().to_owned(),
        claims: Some(wire::ExecutionGrantClaims {
            reviewed_application_source_sha256: claims
                .reviewed_application_source_sha256
                .iter()
                .map(|digest| digest.into_bytes().to_vec())
                .collect(),
            product_release: claims
                .product_release
                .as_ref()
                .map(crate::product_release_codec::encode),
            grant_ref: claims.grant_ref.as_str().to_owned(),
            issuer: claims.issuer.as_str().to_owned(),
            audience: claims.audience.as_str().to_owned(),
            tenant_id: Some(wire::TenantId {
                value: claims.tenant_id.to_string(),
            }),
            model_provider_id: claims.model_provider_id.to_string(),
            model_entry_id: claims.model_entry_id.to_string(),
            run_id: Some(wire::RunId {
                value: claims.run_id.to_string(),
            }),
            issued_at: Some(timestamp(claims.issued_at)),
            expires_at: Some(timestamp(claims.expires_at)),
            nonce: claims.nonce.as_str().to_owned(),
            requested_model_class: claims.requested_model_class.as_str().to_owned(),
            model_route: Some(primary_route),
            schema_version: claims.schema_version.as_str().to_owned(),
            agent_contract: claims
                .agent_contract
                .as_ref()
                .map_or_else(String::new, |contract| contract.as_str().to_owned()),
            surface: claims
                .surface
                .as_ref()
                .map_or_else(String::new, |surface| surface.as_str().to_owned()),
            capability_entitlements: claims
                .capability_entitlements
                .iter()
                .map(capability_entitlement)
                .collect(),
            tool_contract_releases: claims
                .tool_contract_releases
                .iter()
                .map(|release| wire::GrantedToolContractRelease {
                    release_ref: release.release_ref.as_str().to_owned(),
                    schema_version: release.schema_version.as_str().to_owned(),
                    tool_key: release.tool_key.as_str().to_owned(),
                    available: release.available,
                    description: release.description.clone(),
                    input_schema_json: release.input_schema_json.clone(),
                    content_sha256: release.content_sha256.to_vec(),
                    execution_binding_ref: release.execution_binding_ref.clone(),
                })
                .collect(),
            auxiliary_model_routes: claims
                .auxiliary_model_routes
                .iter()
                .map(|auxiliary| wire::GrantedAuxiliaryModelRoute {
                    role: auxiliary_role(auxiliary.role).into(),
                    model_route: Some(model_route(&auxiliary.route)),
                    credential_grant_ref: auxiliary.credential_grant_ref.as_str().to_owned(),
                })
                .collect(),
            device_direct_provider: claims.device_direct_provider.as_ref().map(|provider| {
                wire::DeviceDirectProvider {
                    base_url: provider.base_url.clone(),
                    api_format: provider.api_format.as_str().to_owned(),
                    model: provider.model.clone(),
                    credential_ref: provider.credential_ref.clone(),
                }
            }),
            vision_direct_provider: claims.vision_direct_provider.as_ref().map(|provider| {
                wire::DeviceDirectProvider {
                    base_url: provider.base_url.clone(),
                    api_format: provider.api_format.as_str().to_owned(),
                    model: provider.model.clone(),
                    credential_ref: provider.credential_ref.clone(),
                }
            }),
            principal_id: identity_binding.map(|binding| wire::PrincipalId {
                value: binding.principal_id.to_string(),
            }),
            authority_scope: identity_binding.map(|binding| wire::ExecutionScope {
                scope: Some(match binding.authority_scope {
                    ExecutionGrantAuthorityScope::HostedProjectExecution {
                        scope,
                        assignment_digest,
                    } => wire::execution_scope::Scope::HostedProjectExecution(
                        wire::HostedProjectExecutionAuthorityScope {
                            scope: Some(crate::encode_hosted_scope(scope)),
                            execution_assignment_digest: Some(wire::Sha256 {
                                value: assignment_digest.to_vec(),
                            }),
                        },
                    ),
                    ExecutionGrantAuthorityScope::HostedProject { scope } => {
                        wire::execution_scope::Scope::HostedProject(crate::encode_hosted_scope(
                            scope,
                        ))
                    }
                    ExecutionGrantAuthorityScope::Tenant { tenant_id } => {
                        wire::execution_scope::Scope::Tenant(wire::TenantScope {
                            tenant_id: Some(wire::TenantId {
                                value: tenant_id.to_string(),
                            }),
                        })
                    }
                    ExecutionGrantAuthorityScope::Workspace { workspace_id } => {
                        wire::execution_scope::Scope::Workspace(wire::WorkspaceScope {
                            workspace_id: Some(wire::WorkspaceId {
                                value: workspace_id.to_string(),
                            }),
                        })
                    }
                }),
            }),
            device_id: identity_binding.and_then(|binding| {
                binding.device_id.map(|device_id| wire::DeviceId {
                    value: device_id.to_string(),
                })
            }),
            permission_mode: claims.permission_mode.map_or(
                wire::PermissionMode::Unspecified,
                |mode| match mode {
                    PermissionMode::Supervised => wire::PermissionMode::Supervised,
                    PermissionMode::Trusted => wire::PermissionMode::Trusted,
                    PermissionMode::Deny => wire::PermissionMode::Deny,
                    PermissionMode::FullAccess => wire::PermissionMode::FullAccess,
                    PermissionMode::ServiceManaged => wire::PermissionMode::ServiceManaged,
                },
            ) as i32,
            device_workspace_id: identity_binding.and_then(|binding| {
                binding
                    .device_workspace_id
                    .map(|workspace_id| wire::WorkspaceId {
                        value: workspace_id.to_string(),
                    })
            }),
        }),
        signature: value.signature.to_vec(),
    }
}

fn model_route(value: &ModelRoute) -> wire::GrantedModelRoute {
    wire::GrantedModelRoute {
        route_ref: Some(wire::ModelRouteRef {
            route_key: value.route_ref.as_str().to_owned(),
        }),
        resolved_model_key: value.resolved_model_key.clone(),
        model_provider_id: value.model_provider_id.to_string(),
        model_entry_id: value.model_entry_id.to_string(),
        api_format: value.api_format.as_str().to_owned(),
        adapter_version: value.adapter_version.as_str().to_owned(),
        protocol_version: value.protocol_version.as_str().to_owned(),
        context_window: value.context_window.get(),
        max_output_tokens: value.max_output_tokens.get(),
        capabilities: value
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        parameters: value
            .parameters
            .iter()
            .map(|(name, parameter)| wire::ModelParameter {
                name: name.as_str().to_owned(),
                value: Some(match parameter {
                    ModelParameter::Bool(value) => {
                        wire::model_parameter::Value::BoolValue(wire::BoolParameter {
                            value: *value,
                        })
                    }
                    ModelParameter::Int(value) => {
                        wire::model_parameter::Value::IntValue(wire::IntParameter { value: *value })
                    }
                    ModelParameter::Double(value) => {
                        wire::model_parameter::Value::DoubleValue(wire::DoubleParameter {
                            value: *value,
                        })
                    }
                    ModelParameter::String(value) => {
                        wire::model_parameter::Value::StringValue(wire::StringParameter {
                            value: value.clone(),
                        })
                    }
                }),
            })
            .collect(),
        pricing_version_ref: Some(wire::PricingVersionRef {
            value: value.pricing_version_ref.as_str().to_owned(),
        }),
        provider_state_policy: match value.provider_state_policy {
            ProviderStatePolicy::Stateless => wire::ProviderStatePolicy::Stateless,
            ProviderStatePolicy::OpaqueContinuation => {
                wire::ProviderStatePolicy::OpaqueContinuation
            }
        } as i32,
        fallback_candidates: Vec::new(),
    }
}

pub(crate) fn capability_entitlement(value: &CapabilityEntitlement) -> wire::CapabilityEntitlement {
    wire::CapabilityEntitlement {
        capability_key: value.key.as_str().to_owned(),
        provider_id: Some(wire::CapabilityProviderId {
            value: value.provider_id.to_string(),
        }),
        location: match value.location {
            CapabilityLocation::Cloud => wire::CapabilityLocation::Cloud,
            CapabilityLocation::Backend => wire::CapabilityLocation::Backend,
            CapabilityLocation::Device => wire::CapabilityLocation::Device,
            CapabilityLocation::RemoteDevice => wire::CapabilityLocation::RemoteDevice,
            CapabilityLocation::Browser => wire::CapabilityLocation::Browser,
        } as i32,
        effect: match value.effect {
            Effect::Read => wire::ToolEffect::Read,
            Effect::WriteReversible => wire::ToolEffect::WriteReversible,
            Effect::WriteExternal => wire::ToolEffect::WriteExternal,
            Effect::Destructive => wire::ToolEffect::Destructive,
        } as i32,
        allowed_scopes: value
            .allowed_scopes
            .iter()
            .map(|scope| scope.as_str().to_owned())
            .collect(),
        policy_decision: match value.policy_decision {
            Decision::Allow => wire::CapabilityPolicyDecision::Allow,
            Decision::Deny => wire::CapabilityPolicyDecision::Deny,
            Decision::RequireApproval => wire::CapabilityPolicyDecision::RequireApproval,
        } as i32,
    }
}

const fn auxiliary_role(value: AuxiliaryModelRole) -> wire::AuxiliaryModelRole {
    match value {
        AuxiliaryModelRole::Compression => wire::AuxiliaryModelRole::Compression,
        AuxiliaryModelRole::Vision => wire::AuxiliaryModelRole::Vision,
        AuxiliaryModelRole::Embedding => wire::AuxiliaryModelRole::Embedding,
        AuxiliaryModelRole::Title => wire::AuxiliaryModelRole::Title,
        AuxiliaryModelRole::Routing => wire::AuxiliaryModelRole::Routing,
        AuxiliaryModelRole::Evolution => wire::AuxiliaryModelRole::Evolution,
        AuxiliaryModelRole::GoalJudge => wire::AuxiliaryModelRole::GoalJudge,
        AuxiliaryModelRole::Decision => wire::AuxiliaryModelRole::Decision,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use musteragent_capability::{CapabilityKey, CapabilityLocation};
    use musteragent_execution_grant::{
        DeviceDirectProvider, EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant,
        ExecutionGrantAuthorityScope, ExecutionGrantIdentityBinding, ExecutionGrantProofHash,
        GrantedAuxiliaryModelRoute, GrantedToolContractRelease, TOOL_CONTRACT_RELEASE_SCHEMA_V1,
    };
    use musteragent_identity::{
        DeviceId, PrincipalId, RunId, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
    };
    use musteragent_model::{ApiFormat, CredentialGrantRef, PricingVersionRef};
    use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
    use prost::Message as _;

    use super::*;

    fn route(
        route_ref: &str,
        provider_id: &str,
        entry_id: &str,
        api_format: ApiFormat,
    ) -> ModelRoute {
        let (adapter, protocol, state_policy) = match api_format {
            ApiFormat::OpenAiResponses => (
                "openai.responses-adapter@1",
                "openai.responses@1",
                ProviderStatePolicy::OpaqueContinuation,
            ),
            ApiFormat::AnthropicMessages => (
                "anthropic.messages-adapter@1",
                "anthropic.messages@1",
                ProviderStatePolicy::Stateless,
            ),
            _ => panic!("unsupported proof fixture protocol"),
        };
        ModelRoute {
            route_ref: VersionedKey::new(route_ref).expect("route ref"),
            resolved_model_key: format!("model-{entry_id}"),
            model_provider_id: provider_id.parse().expect("provider ID"),
            model_entry_id: entry_id.parse().expect("entry ID"),
            api_format,
            adapter_version: VersionedKey::new(adapter).expect("adapter"),
            protocol_version: VersionedKey::new(protocol).expect("protocol"),
            context_window: NonZeroU64::new(128_000).expect("context window"),
            max_output_tokens: NonZeroU64::new(16_384).expect("output tokens"),
            capabilities: BTreeSet::from([
                DottedKey::new("reasoning").expect("capability"),
                DottedKey::new("tools").expect("capability"),
            ]),
            parameters: BTreeMap::from([
                (
                    DottedKey::new("feature.enabled").expect("parameter"),
                    ModelParameter::Bool(true),
                ),
                (
                    DottedKey::new("max.parallel").expect("parameter"),
                    ModelParameter::Int(4),
                ),
                (
                    DottedKey::new("reasoning.effort").expect("parameter"),
                    ModelParameter::String("high".to_owned()),
                ),
                (
                    DottedKey::new("temperature").expect("parameter"),
                    ModelParameter::Double(0.2),
                ),
            ]),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:proof.price@1").expect("pricing"),
            provider_state_policy: state_policy,
        }
    }

    fn signed_v6_grant() -> SignedExecutionGrant {
        let tenant_id = "019fa6de-38fd-7000-8000-000000000501"
            .parse::<TenantId>()
            .expect("tenant");
        let principal_id = "019fa6de-38fd-7000-8000-000000000502"
            .parse::<PrincipalId>()
            .expect("principal");
        let device_id = "019fa6de-38fd-7000-8000-000000000503"
            .parse::<DeviceId>()
            .expect("device");
        let device_workspace_id = "019fa6de-38fd-7000-8000-000000000504"
            .parse::<WorkspaceId>()
            .expect("device workspace");
        let mut primary = route(
            "device.chat-primary@1",
            "019fa6de-38fd-7000-8000-000000000505",
            "019fa6de-38fd-7000-8000-000000000506",
            ApiFormat::OpenAiResponses,
        );
        let fallback = route(
            "device.chat-fallback@1",
            "019fa6de-38fd-7000-8000-000000000507",
            "019fa6de-38fd-7000-8000-000000000508",
            ApiFormat::AnthropicMessages,
        );
        primary.fallback_route_refs = vec![fallback.route_ref.clone()];
        let auxiliary = route(
            "device.compression@1",
            "019fa6de-38fd-7000-8000-000000000509",
            "019fa6de-38fd-7000-8000-000000000510",
            ApiFormat::OpenAiResponses,
        );
        let run_id = "019fa6de-38fd-7000-8000-000000000511"
            .parse::<RunId>()
            .expect("Run");
        let claims = ExecutionGrant {
            reviewed_application_source_sha256: Vec::new(),
            product_release: None,
            schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("schema"),
            grant_ref: CredentialGrantRef::new("grant_device-proof-v6").expect("grant ref"),
            issuer: DottedKey::new("musteragent.backend").expect("issuer"),
            audience: DottedKey::new("musteragent.runtime.device").expect("audience"),
            tenant_id,
            model_provider_id: primary.model_provider_id,
            model_entry_id: primary.model_entry_id,
            run_id,
            issued_at: UtcTimestamp::from_unix_millis(1_788_000_000_000),
            expires_at: UtcTimestamp::from_unix_millis(1_788_000_060_000),
            nonce: OpaqueExternalRef::new("nonce_device-proof-v6").expect("nonce"),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            model_route: primary,
            fallback_model_routes: vec![fallback],
            auxiliary_model_routes: vec![GrantedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Compression,
                route: auxiliary,
                credential_grant_ref: CredentialGrantRef::new("grant_device-compression")
                    .expect("auxiliary credential grant"),
            }],
            agent_contract: Some(
                VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"),
            ),
            surface: Some(DottedKey::new("desktop").expect("surface")),
            capability_entitlements: vec![CapabilityEntitlement {
                key: CapabilityKey::from_versioned_key(
                    VersionedKey::new("web.search@2").expect("Capability key"),
                ),
                provider_id: "019fa6de-38fd-7000-8000-000000000512"
                    .parse()
                    .expect("Capability provider"),
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("web.read").expect("scope")]),
                policy_decision: Decision::RequireApproval,
            }],
            tool_contract_releases: Vec::new(),
            identity_binding: Some(ExecutionGrantIdentityBinding {
                principal_id,
                authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
                device_id: Some(device_id),
                device_workspace_id: Some(device_workspace_id),
            }),
            permission_mode: Some(PermissionMode::Supervised),
            device_direct_provider: None,
            vision_direct_provider: None,
        };
        claims.validate().expect("valid V6 grant");
        SignedExecutionGrant {
            context_transport_annex: Vec::new(),
            key_id: DottedKey::new("musteragent.execution-grant.test-key.v1").expect("key ID"),
            claims,
            signature: [0x5a; 64],
        }
    }

    #[test]
    fn protobuf_proof_preserves_the_complete_signed_v6_grant() {
        let signed = signed_v6_grant();
        let proof = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("encode proof");
        let repeated = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("repeat encoding");
        assert_eq!(proof, repeated, "proof serialization must be deterministic");
        assert_eq!(
            proof.schema_version().as_str(),
            EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1
        );
        assert_eq!(
            proof.content_hash(),
            ExecutionGrantProofHash::digest(proof.encoded_envelope())
        );
        assert_eq!(proof.validate_for(&signed.claims), Ok(()));
        let debug = format!("{proof:?}");
        assert!(debug.contains("encoded_bytes"));
        assert!(!debug.contains("encoded_envelope"));

        let decoded = wire::SignedExecutionGrant::decode(proof.encoded_envelope())
            .expect("decode signed proof");
        assert_eq!(decoded.key_id, signed.key_id.as_str());
        assert_eq!(decoded.signature, signed.signature);
        let claims = decoded.claims.expect("grant claims");
        assert_eq!(claims.schema_version, EXECUTION_GRANT_SCHEMA_V10);
        assert_eq!(
            claims.tenant_id.as_ref().expect("tenant").value,
            signed.claims.tenant_id.to_string()
        );
        assert_eq!(
            claims.run_id.as_ref().expect("Run").value,
            signed.claims.run_id.to_string()
        );
        assert_eq!(
            claims.principal_id.as_ref().expect("principal").value,
            signed
                .claims
                .identity_binding
                .expect("identity")
                .principal_id
                .to_string()
        );
        assert_eq!(
            claims.device_id.as_ref().expect("device").value,
            signed
                .claims
                .identity_binding
                .expect("identity")
                .device_id
                .expect("device")
                .to_string()
        );
        assert_eq!(
            claims
                .device_workspace_id
                .as_ref()
                .expect("workspace")
                .value,
            signed
                .claims
                .identity_binding
                .expect("identity")
                .device_workspace_id
                .expect("workspace")
                .to_string()
        );
        assert!(claims.authority_scope.is_some());
        assert_eq!(
            claims.permission_mode,
            wire::PermissionMode::Supervised as i32
        );
        assert_eq!(claims.capability_entitlements.len(), 1);
        assert_eq!(
            claims.capability_entitlements[0].location,
            wire::CapabilityLocation::Backend as i32
        );
        assert_eq!(
            claims.capability_entitlements[0].policy_decision,
            wire::CapabilityPolicyDecision::RequireApproval as i32
        );
        assert_eq!(claims.auxiliary_model_routes.len(), 1);
        assert_eq!(
            claims.auxiliary_model_routes[0].role,
            wire::AuxiliaryModelRole::Compression as i32
        );
        let primary = claims.model_route.expect("primary route");
        assert_eq!(primary.fallback_candidates.len(), 1);
        assert_eq!(
            primary
                .parameters
                .iter()
                .map(|parameter| parameter.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "feature.enabled",
                "max.parallel",
                "reasoning.effort",
                "temperature",
            ])
        );

        let mut tampered_claims = signed.claims.clone();
        tampered_claims.requested_model_class = ModelClass::new("chat.fast").expect("model class");
        assert_eq!(
            proof.validate_for(&tampered_claims),
            Err(ExecutionGrantProofError::ClaimsHashMismatch)
        );
    }

    #[test]
    fn separate_transport_annex_survives_durable_proof_without_changing_execution_signature() {
        let mut signed = signed_v6_grant();
        let original = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("original proof");
        signed.context_transport_annex =
            b"independently signed opaque transport admission".to_vec();
        let with_annex = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("proof with annex");
        assert_eq!(original.claims_hash(), with_annex.claims_hash());
        assert_ne!(original.content_hash(), with_annex.content_hash());
        let decoded =
            wire::SignedExecutionGrant::decode(with_annex.encoded_envelope()).expect("wire proof");
        assert_eq!(
            decoded.context_transport_annex,
            signed.context_transport_annex
        );
        assert_eq!(decoded.signature, signed.signature);
        assert!(!format!("{signed:?}").contains("opaque transport admission"));
        signed.context_transport_annex = vec![0; 64 * 1024 + 1];
        assert!(ProtobufExecutionGrantProofEncoder.encode(&signed).is_err());
    }

    #[test]
    fn protobuf_proof_preserves_v8_tool_material_and_private_binding() {
        let mut signed = signed_v6_grant();
        signed.claims.schema_version =
            DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("V8 schema");
        signed.claims.issuer =
            DottedKey::new("musteragent.backend.control-plane").expect("current issuer");
        signed.claims.audience =
            DottedKey::new("musteragent.runtime.device").expect("current audience");
        let schema_version =
            DottedKey::new(TOOL_CONTRACT_RELEASE_SCHEMA_V1).expect("release schema");
        let tool_key = VersionedKey::new("artifact.image.create@1").expect("Tool key");
        let description = "Create images with t2i and i2i.";
        let input_schema = br#"{"type":"object"}"#;
        let binding = "musteragent.image-execution-binding.v1:44444444-4444-4444-8444-444444444444:11111111-1111-4111-8111-111111111111:22222222-2222-4222-8222-222222222222:-";
        let content_sha256 = GrantedToolContractRelease::digest(
            &schema_version,
            &tool_key,
            true,
            Some(description),
            Some(input_schema),
            Some(binding),
        );
        signed.claims.tool_contract_releases = vec![GrantedToolContractRelease {
            release_ref: OpaqueExternalRef::new(format!(
                "toolrel_{}",
                content_sha256
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ))
            .expect("release ref"),
            schema_version,
            tool_key,
            available: true,
            description: Some(description.to_owned()),
            input_schema_json: Some(input_schema.to_vec()),
            content_sha256,
            execution_binding_ref: Some(binding.to_owned()),
        }];
        signed.claims.validate().expect("valid V8 grant");

        let proof = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("encode proof");
        let decoded = wire::SignedExecutionGrant::decode(proof.encoded_envelope())
            .expect("decode signed proof");
        let release = &decoded.claims.expect("claims").tool_contract_releases[0];

        assert_eq!(release.description.as_deref(), Some(description));
        assert_eq!(
            release.input_schema_json.as_deref(),
            Some(input_schema.as_slice())
        );
        assert_eq!(release.execution_binding_ref.as_deref(), Some(binding));
        assert_eq!(release.content_sha256, content_sha256);
    }

    #[test]
    fn protobuf_proof_preserves_the_device_direct_provider_binding() {
        let mut signed = signed_v6_grant();
        let mut primary = route(
            "byok.primary.conn-test-abcdef123456@1",
            "019fa6de-38fd-7000-8000-000000000505",
            "019fa6de-38fd-7000-8000-000000000506",
            ApiFormat::AnthropicMessages,
        );
        primary.pricing_version_ref =
            PricingVersionRef::new("catalog:byok.device-direct.v1").expect("BYOK pricing sentinel");
        signed.claims.model_provider_id = primary.model_provider_id;
        signed.claims.model_entry_id = primary.model_entry_id;
        signed.claims.fallback_model_routes = Vec::new();
        signed.claims.auxiliary_model_routes = Vec::new();
        signed.claims.device_direct_provider = Some(DeviceDirectProvider {
            base_url: "https://api.anthropic.com".to_owned(),
            api_format: ApiFormat::AnthropicMessages,
            model: primary.resolved_model_key.clone(),
            credential_ref: format!(
                "{}cred_test01",
                musteragent_execution_grant::DEVICE_DIRECT_CREDENTIAL_REF_PREFIX
            ),
        });
        signed.claims.model_route = primary;
        signed.claims.validate().expect("valid BYOK V6 grant");

        let proof = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("encode proof");
        assert_eq!(proof.validate_for(&signed.claims), Ok(()));

        let decoded = wire::SignedExecutionGrant::decode(proof.encoded_envelope())
            .expect("decode signed proof");
        let claims = decoded.claims.expect("grant claims");
        let provider = claims
            .device_direct_provider
            .expect("device-direct provider");
        let expected = signed.claims.device_direct_provider.expect("provider");
        assert_eq!(provider.base_url, expected.base_url);
        assert_eq!(provider.api_format, expected.api_format.as_str());
        assert_eq!(provider.model, expected.model);
        assert_eq!(provider.credential_ref, expected.credential_ref);
        assert!(claims.vision_direct_provider.is_none());
        let primary_route = claims.model_route.expect("primary route");
        assert_eq!(
            primary_route.route_ref.expect("route ref").route_key,
            "byok.primary.conn-test-abcdef123456@1"
        );
        assert_eq!(primary_route.api_format, "anthropic.messages");
        assert!(primary_route.fallback_candidates.is_empty());
        assert!(claims.auxiliary_model_routes.is_empty());
    }

    #[test]
    fn protobuf_proof_preserves_the_vision_direct_provider_binding() {
        let mut signed = signed_v6_grant();
        let mut primary = route(
            "byok.primary.conn-test-abcdef123456@1",
            "019fa6de-38fd-7000-8000-000000000505",
            "019fa6de-38fd-7000-8000-000000000506",
            ApiFormat::AnthropicMessages,
        );
        primary.pricing_version_ref =
            PricingVersionRef::new("catalog:byok.device-direct.v1").expect("BYOK pricing sentinel");
        primary.capabilities = BTreeSet::new();
        primary.parameters = BTreeMap::new();
        signed.claims.model_provider_id = primary.model_provider_id;
        signed.claims.model_entry_id = primary.model_entry_id;
        signed.claims.fallback_model_routes = Vec::new();
        signed.claims.device_direct_provider = Some(DeviceDirectProvider {
            base_url: "https://api.anthropic.com".to_owned(),
            api_format: ApiFormat::AnthropicMessages,
            model: primary.resolved_model_key.clone(),
            credential_ref: format!(
                "{}cred_test01",
                musteragent_execution_grant::DEVICE_DIRECT_CREDENTIAL_REF_PREFIX
            ),
        });
        let mut compression = primary.clone();
        compression.route_ref =
            VersionedKey::new("byok.compression.conn-test-abcdef123456@1").expect("compression");
        compression.capabilities =
            BTreeSet::from([DottedKey::new("streaming").expect("capability")]);
        let mut vision = primary.clone();
        vision.route_ref =
            VersionedKey::new("byok.vision.conn-openai-personal-0123456789ab@1").expect("vision");
        vision.resolved_model_key = "gpt-5.1".to_owned();
        vision.api_format = ApiFormat::OpenAiChatCompletions;
        vision.adapter_version =
            VersionedKey::new("openai.chat-completions-adapter@1").expect("adapter");
        vision.protocol_version = VersionedKey::new("openai.chat-completions@1").expect("protocol");
        vision.capabilities = BTreeSet::from([
            DottedKey::new("streaming").expect("capability"),
            DottedKey::new("vision").expect("capability"),
        ]);
        signed.claims.auxiliary_model_routes = vec![
            GrantedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Compression,
                route: compression,
                credential_grant_ref: signed.claims.grant_ref.clone(),
            },
            GrantedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Vision,
                route: vision,
                credential_grant_ref: signed.claims.grant_ref.clone(),
            },
        ];
        signed.claims.vision_direct_provider = Some(DeviceDirectProvider {
            base_url: "https://api.openai.com/v1".to_owned(),
            api_format: ApiFormat::OpenAiChatCompletions,
            model: "gpt-5.1".to_owned(),
            credential_ref: format!(
                "{}cred_test02",
                musteragent_execution_grant::DEVICE_DIRECT_CREDENTIAL_REF_PREFIX
            ),
        });
        signed.claims.model_route = primary;
        signed
            .claims
            .validate()
            .expect("valid BYOK V6 vision grant");

        let proof = ProtobufExecutionGrantProofEncoder
            .encode(&signed)
            .expect("encode proof");
        assert_eq!(proof.validate_for(&signed.claims), Ok(()));

        let decoded = wire::SignedExecutionGrant::decode(proof.encoded_envelope())
            .expect("decode signed proof");
        let claims = decoded.claims.expect("grant claims");
        let provider = claims
            .vision_direct_provider
            .expect("vision device-direct provider");
        let expected = signed
            .claims
            .vision_direct_provider
            .expect("vision provider");
        assert_eq!(provider.base_url, expected.base_url);
        assert_eq!(provider.api_format, expected.api_format.as_str());
        assert_eq!(provider.model, expected.model);
        assert_eq!(provider.credential_ref, expected.credential_ref);
        assert_eq!(claims.auxiliary_model_routes.len(), 2);
        assert_eq!(
            claims.auxiliary_model_routes[1].role,
            wire::AuxiliaryModelRole::Vision as i32
        );
    }
}
