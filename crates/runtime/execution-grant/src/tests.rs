use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_identity::{
    DeviceId, DottedKey, PrincipalId, RunId, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelParameter, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{Decision, Effect, PermissionMode};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};

use super::{
    DEVICE_DIRECT_PRICING_VERSION_REF, DeviceDirectProvider, EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1,
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantContractError, ExecutionGrantIdentityBinding, GrantAuthorizationRequest,
    GrantedAuxiliaryModelRoute, GrantedToolContractRelease, STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1,
    TOOL_CONTRACT_RELEASE_SCHEMA_V1, canonical_claims,
};

mod admitted_transport;
mod hosted_execution;
mod hosted_project;
mod tool_contract_release;

fn primary_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
        resolved_model_key: "gpt-runtime-golden".to_owned(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000401"
            .parse()
            .expect("model provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000402"
            .parse()
            .expect("model entry"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(128_000).expect("context window"),
        max_output_tokens: NonZeroU64::new(16_384).expect("output tokens"),
        capabilities: BTreeSet::from([
            DottedKey::new("reasoning").expect("capability"),
            DottedKey::new("tools").expect("capability"),
        ]),
        parameters: BTreeMap::from([
            (
                DottedKey::new("reasoning.effort").expect("parameter"),
                ModelParameter::String("high".to_owned()),
            ),
            (
                DottedKey::new("temperature").expect("parameter"),
                ModelParameter::Double(0.25),
            ),
        ]),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:golden.price@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    }
}

fn base_grant(schema: &str) -> ExecutionGrant {
    let model_route = primary_route();
    ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(schema).expect("grant schema"),
        grant_ref: CredentialGrantRef::new("grant_019fa6de-38fd-7000-8000-000000000403")
            .expect("grant ref"),
        issuer: DottedKey::new("musteragent.backend").expect("issuer"),
        audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
        tenant_id: "019fa6de-38fd-7000-8000-000000000404"
            .parse::<TenantId>()
            .expect("tenant"),
        model_provider_id: model_route.model_provider_id,
        model_entry_id: model_route.model_entry_id,
        run_id: "019fa6de-38fd-7000-8000-000000000405"
            .parse::<RunId>()
            .expect("Run"),
        issued_at: UtcTimestamp::from_unix_millis(1_788_000_000_000),
        expires_at: UtcTimestamp::from_unix_millis(1_788_000_060_000),
        nonce: OpaqueExternalRef::new("nonce_execution-grant-golden@1").expect("nonce"),
        requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
        model_route,
        fallback_model_routes: Vec::new(),
        auxiliary_model_routes: Vec::new(),
        agent_contract: None,
        surface: None,
        capability_entitlements: Vec::new(),
        tool_contract_releases: Vec::new(),
        identity_binding: None,
        permission_mode: None,
        device_direct_provider: None,
        vision_direct_provider: None,
    }
}

fn device_grant() -> (ExecutionGrant, GrantAuthorizationRequest) {
    let mut grant = base_grant(EXECUTION_GRANT_SCHEMA_V10);
    let principal_id = PrincipalId::generate();
    let device_id = DeviceId::generate();
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("agent"));
    grant.surface = Some(DottedKey::new("desktop").expect("surface"));
    grant.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("core.time.read@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Device,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
        policy_decision: Decision::Allow,
    }];
    let identity_binding = ExecutionGrantIdentityBinding {
        principal_id,
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        device_id: Some(device_id),
        device_workspace_id: None,
    };
    grant.identity_binding = Some(identity_binding);
    grant.permission_mode = Some(PermissionMode::Supervised);
    let request = GrantAuthorizationRequest {
        runtime_audience: grant.audience.clone(),
        tenant_id: grant.tenant_id,
        identity_binding,
        run_id: grant.run_id,
        model_provider_id: grant.model_provider_id,
        model_entry_id: grant.model_entry_id,
        now: grant.issued_at,
    };
    (grant, request)
}

#[test]
fn evolution_maintenance_grant_is_one_trusted_private_submission_protocol() {
    let mut grant = base_grant(EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1);
    grant.requested_model_class = ModelClass::new("chat.evolution").expect("model class");
    grant.model_route.route_ref =
        VersionedKey::new("registry.maintenance.evolution@1").expect("route");
    grant.model_route.fallback_route_refs.clear();
    grant.model_route.provider_state_policy = ProviderStatePolicy::Stateless;
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"));
    grant.surface = Some(DottedKey::new("maintenance").expect("surface"));
    grant.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: PrincipalId::generate(),
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        device_id: None,
        device_workspace_id: None,
    });
    grant.permission_mode = Some(PermissionMode::Trusted);

    assert_eq!(grant.validate(), Ok(()));
    assert!(
        !canonical_claims(&grant)
            .expect("canonical claims")
            .is_empty()
    );

    let mut without_private_protocol = grant.clone();
    without_private_protocol
        .model_route
        .capabilities
        .retain(|capability| capability.as_str() != "tools");
    assert_eq!(
        without_private_protocol.validate(),
        Err(ExecutionGrantContractError::InvalidEvolutionMaintenanceGrant)
    );

    let mut with_product_capability = grant.clone();
    with_product_capability.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("core.time.read@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Cloud,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
        policy_decision: Decision::Allow,
    }];
    assert_eq!(
        with_product_capability.validate(),
        Err(ExecutionGrantContractError::InvalidEvolutionMaintenanceGrant)
    );

    let mut with_fallback = grant;
    with_fallback.model_route.fallback_route_refs =
        vec![VersionedKey::new("registry.maintenance.fallback@1").expect("fallback")];
    assert_eq!(
        with_fallback.validate(),
        Err(ExecutionGrantContractError::FallbackChainMismatch)
    );
}

#[test]
fn current_web_surface_and_service_managed_permission_mode_are_bijective() {
    let mut grant = base_grant(EXECUTION_GRANT_SCHEMA_V10);
    grant.issuer = DottedKey::new("musteragent.backend").expect("issuer");
    grant.audience = DottedKey::new("musteragent.runtime.cloud").expect("audience");
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"));
    grant.surface = Some(DottedKey::new("web").expect("surface"));
    grant.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("core.time.read@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Cloud,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
        policy_decision: Decision::Allow,
    }];
    grant.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: PrincipalId::generate(),
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        device_id: None,
        device_workspace_id: None,
    });
    grant.permission_mode = Some(PermissionMode::ServiceManaged);

    assert_eq!(grant.validate(), Ok(()));

    let mut web_with_desktop_mode = grant.clone();
    web_with_desktop_mode.permission_mode = Some(PermissionMode::Supervised);
    assert_eq!(
        web_with_desktop_mode.validate(),
        Err(ExecutionGrantContractError::InvalidPermissionSurface)
    );

    let mut service_managed_desktop = grant;
    service_managed_desktop.surface = Some(DottedKey::new("desktop").expect("surface"));
    assert_eq!(
        service_managed_desktop.validate(),
        Err(ExecutionGrantContractError::InvalidPermissionSurface)
    );
}

#[test]
fn authorization_binds_principal_tenant_device_and_runtime_home() {
    let (grant, request) = device_grant();
    assert_eq!(grant.authorize(&request), Ok(()));

    let mut wrong_principal = request.clone();
    wrong_principal.identity_binding.principal_id = PrincipalId::generate();
    assert_eq!(
        grant.authorize(&wrong_principal),
        Err(ExecutionGrantContractError::BindingMismatch)
    );

    let mut wrong_device = request.clone();
    wrong_device.identity_binding.device_id = Some(DeviceId::generate());
    assert_eq!(
        grant.authorize(&wrong_device),
        Err(ExecutionGrantContractError::BindingMismatch)
    );

    let mut missing_workspace = request.clone();
    missing_workspace.identity_binding.authority_scope = ExecutionGrantAuthorityScope::Tenant {
        tenant_id: TenantId::generate(),
    };
    assert_eq!(
        grant.authorize(&missing_workspace),
        Err(ExecutionGrantContractError::BindingMismatch)
    );

    let mut cloud_scope = request;
    cloud_scope.identity_binding = ExecutionGrantIdentityBinding {
        principal_id: cloud_scope.identity_binding.principal_id,
        authority_scope: ExecutionGrantAuthorityScope::Workspace {
            workspace_id: WorkspaceId::generate(),
        },
        device_id: None,
        device_workspace_id: None,
    };
    assert_eq!(
        grant.authorize(&cloud_scope),
        Err(ExecutionGrantContractError::BindingMismatch)
    );
}

#[test]
fn rejects_missing_binding_retired_schema_and_wrong_tenant() {
    let (mut grant, _) = device_grant();
    grant.identity_binding = None;
    assert_eq!(
        grant.validate(),
        Err(ExecutionGrantContractError::MissingIdentityBinding)
    );

    let (mut legacy, _) = device_grant();
    legacy.schema_version =
        DottedKey::new("musteragent.legacy.execution-grant.v4").expect("retired schema");
    assert_eq!(
        legacy.validate(),
        Err(ExecutionGrantContractError::UnsupportedSchema)
    );

    let (mut wrong_tenant_scope, _) = device_grant();
    wrong_tenant_scope
        .identity_binding
        .as_mut()
        .expect("binding")
        .authority_scope = ExecutionGrantAuthorityScope::Tenant {
        tenant_id: TenantId::generate(),
    };
    assert_eq!(
        wrong_tenant_scope.validate(),
        Err(ExecutionGrantContractError::BindingMismatch)
    );
}

#[test]
fn binds_device_local_workspace_under_tenant_authority() {
    let mut grant = base_grant(EXECUTION_GRANT_SCHEMA_V10);
    let principal_id = PrincipalId::generate();
    let device_id = DeviceId::generate();
    let device_workspace_id = WorkspaceId::generate();
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("agent"));
    grant.surface = Some(DottedKey::new("desktop").expect("surface"));
    grant.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("device.workspace.file.read@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Device,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([
            DottedKey::new("device.workspace.file.read").expect("scope")
        ]),
        policy_decision: Decision::Allow,
    }];
    let identity_binding = ExecutionGrantIdentityBinding {
        principal_id,
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        device_id: Some(device_id),
        device_workspace_id: Some(device_workspace_id),
    };
    grant.identity_binding = Some(identity_binding);
    grant.permission_mode = Some(PermissionMode::Supervised);
    let request = GrantAuthorizationRequest {
        runtime_audience: grant.audience.clone(),
        tenant_id: grant.tenant_id,
        identity_binding,
        run_id: grant.run_id,
        model_provider_id: grant.model_provider_id,
        model_entry_id: grant.model_entry_id,
        now: grant.issued_at,
    };
    assert_eq!(grant.authorize(&request), Ok(()));

    let mut wrong_workspace = request.clone();
    wrong_workspace.identity_binding.device_workspace_id = Some(WorkspaceId::generate());
    assert_eq!(
        grant.authorize(&wrong_workspace),
        Err(ExecutionGrantContractError::BindingMismatch)
    );

    let mut workspace_authority = grant.clone();
    workspace_authority
        .identity_binding
        .as_mut()
        .expect("binding")
        .authority_scope = ExecutionGrantAuthorityScope::Workspace {
        workspace_id: device_workspace_id,
    };
    assert_eq!(
        workspace_authority.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceWorkspaceBinding)
    );
}

fn byok_device_grant() -> ExecutionGrant {
    let mut grant = base_grant(EXECUTION_GRANT_SCHEMA_V10);
    grant.agent_contract =
        Some(VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"));
    grant.surface = Some(DottedKey::new("desktop").expect("surface"));
    grant.capability_entitlements = vec![CapabilityEntitlement {
        key: CapabilityKey::from_versioned_key(
            VersionedKey::new("device.workspace.file.read@1").expect("Capability key"),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431"
            .parse()
            .expect("provider"),
        location: CapabilityLocation::Device,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([
            DottedKey::new("device.workspace.file.read").expect("scope")
        ]),
        policy_decision: Decision::Allow,
    }];
    grant.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: PrincipalId::generate(),
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: grant.tenant_id,
        },
        device_id: Some(DeviceId::generate()),
        device_workspace_id: Some(WorkspaceId::generate()),
    });
    grant.permission_mode = Some(PermissionMode::Supervised);
    grant.model_route.route_ref =
        VersionedKey::new("byok.primary.conn-test-abcdef123456@1").expect("BYOK route ref");
    grant.model_route.resolved_model_key = "claude-sonnet-4-5".to_owned();
    grant.model_route.api_format = ApiFormat::AnthropicMessages;
    grant.model_route.adapter_version =
        VersionedKey::new("anthropic.messages-adapter@1").expect("adapter");
    grant.model_route.protocol_version =
        VersionedKey::new("anthropic.messages@1").expect("protocol");
    grant.model_route.pricing_version_ref =
        PricingVersionRef::new(DEVICE_DIRECT_PRICING_VERSION_REF).expect("BYOK pricing sentinel");
    grant.model_route.provider_state_policy = ProviderStatePolicy::Stateless;
    grant.device_direct_provider = Some(DeviceDirectProvider {
        base_url: "https://api.anthropic.com".to_owned(),
        api_format: ApiFormat::AnthropicMessages,
        model: "claude-sonnet-4-5".to_owned(),
        credential_ref: "musteragent-secure://model-credentials/cred_test01".to_owned(),
    });
    // The Compression sidecar shares the BYOK provider binding and the
    // primary credential budget, so context compaction stays on the
    // user's own model without cloud metering.
    let mut compression_route = grant.model_route.clone();
    compression_route.route_ref = VersionedKey::new("byok.compression.conn-test-abcdef123456@1")
        .expect("BYOK compression route ref");
    compression_route.capabilities =
        BTreeSet::from([DottedKey::new("streaming").expect("capability")]);
    compression_route.parameters = BTreeMap::new();
    grant.auxiliary_model_routes = vec![GrantedAuxiliaryModelRoute {
        role: musteragent_model::AuxiliaryModelRole::Compression,
        route: compression_route,
        credential_grant_ref: grant.grant_ref.clone(),
    }];
    grant
}

#[test]
fn device_direct_grant_requires_the_current_credential_namespace() {
    let mut grant = byok_device_grant();
    grant.schema_version = DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("V8 schema");
    grant.issuer = DottedKey::new("musteragent.backend.control-plane").expect("issuer");
    grant.audience =
        DottedKey::new("musteragent.runtime.device.id-019fa6de-38fd-7000-8000-000000000001")
            .expect("audience");
    grant
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .credential_ref = "retired-secure://model-credentials/cred_test01".to_owned();
    assert_eq!(
        grant.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    grant
        .device_direct_provider
        .as_mut()
        .expect("device-direct provider")
        .credential_ref = "musteragent-secure://model-credentials/cred_test01".to_owned();
    assert_eq!(grant.validate(), Ok(()));
}

#[test]
fn standing_goal_judge_can_reuse_a_frozen_device_direct_route() {
    let mut grant = byok_device_grant();
    grant.schema_version =
        DottedKey::new(STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1).expect("Standing Goal Judge schema");
    grant.requested_model_class = ModelClass::new("aux.goal_judge").expect("Judge model class");
    grant.capability_entitlements.clear();
    grant.auxiliary_model_routes.clear();
    grant.fallback_model_routes.clear();
    grant.model_route.fallback_route_refs.clear();
    grant.vision_direct_provider = None;

    assert_eq!(grant.validate(), Ok(()));
    assert!(
        !canonical_claims(&grant)
            .expect("canonical Judge claims")
            .is_empty()
    );
}

#[test]
fn device_direct_provider_binding_fails_closed_outside_its_signed_shape() {
    let valid = byok_device_grant();

    let mut retired = valid.clone();
    retired.schema_version =
        DottedKey::new("musteragent.legacy.execution-grant.v5").expect("retired schema");
    assert_eq!(
        retired.validate(),
        Err(ExecutionGrantContractError::UnsupportedSchema)
    );
    let mut byok_route_without_provider = valid.clone();
    byok_route_without_provider.device_direct_provider = None;
    assert_eq!(
        byok_route_without_provider.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut api_format_mismatch = valid.clone();
    api_format_mismatch
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .api_format = ApiFormat::OpenAiResponses;
    assert_eq!(
        api_format_mismatch.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut model_mismatch = valid.clone();
    model_mismatch
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .model = "claude-opus-4-1".to_owned();
    assert_eq!(
        model_mismatch.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut with_fallback = valid.clone();
    let mut fallback = with_fallback.model_route.clone();
    fallback.route_ref =
        VersionedKey::new("byok.primary.conn-test-fallback00000@1").expect("fallback ref");
    with_fallback.model_route.fallback_route_refs = vec![fallback.route_ref.clone()];
    with_fallback.fallback_model_routes = vec![fallback];
    assert_eq!(
        with_fallback.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut with_foreign_auxiliary = valid.clone();
    let mut foreign_auxiliary_route = with_foreign_auxiliary.model_route.clone();
    foreign_auxiliary_route.route_ref =
        VersionedKey::new("device.chat-compression@1").expect("compression route");
    with_foreign_auxiliary.auxiliary_model_routes = vec![GrantedAuxiliaryModelRoute {
        role: musteragent_model::AuxiliaryModelRole::Compression,
        route: foreign_auxiliary_route,
        credential_grant_ref: CredentialGrantRef::new("grant_device-compression")
            .expect("auxiliary credential grant"),
    }];
    assert_eq!(
        with_foreign_auxiliary.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // A Vision role pinned to a Compression-shaped route ref is not the
    // BYOK vision sidecar and still fails closed.
    let mut with_extra_auxiliary = valid.clone();
    let mut vision_route = with_extra_auxiliary.model_route.clone();
    vision_route.route_ref =
        VersionedKey::new("byok.compression.conn-test-vision00000@1").expect("vision ref");
    with_extra_auxiliary
        .auxiliary_model_routes
        .push(GrantedAuxiliaryModelRoute {
            role: musteragent_model::AuxiliaryModelRole::Vision,
            route: vision_route,
            credential_grant_ref: with_extra_auxiliary.grant_ref.clone(),
        });
    assert_eq!(
        with_extra_auxiliary.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // The Compression sidecar must reuse the primary credential budget.
    let mut foreign_credential = valid.clone();
    foreign_credential
        .auxiliary_model_routes
        .first_mut()
        .expect("compression auxiliary")
        .credential_grant_ref =
        CredentialGrantRef::new("grant_device-compression").expect("credential grant");
    assert_eq!(
        foreign_credential.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut http_base_url = valid.clone();
    http_base_url
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .base_url = "http://api.anthropic.com".to_owned();
    assert_eq!(
        http_base_url.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut userinfo_base_url = valid.clone();
    userinfo_base_url
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .base_url = "https://user@api.anthropic.com".to_owned();
    assert_eq!(
        userinfo_base_url.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut wrong_credential_prefix = valid;
    wrong_credential_prefix
        .device_direct_provider
        .as_mut()
        .expect("provider")
        .credential_ref = "musteragent-secure://other/cred_test01".to_owned();
    assert_eq!(
        wrong_credential_prefix.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );
}

/// Signs the BYOK vision sidecar into the grant: a separate user-owned
/// connection pinned under its own provider binding, sharing the primary
/// credential budget exactly like the Compression sidecar.
fn byok_vision_sidecar(
    grant: &ExecutionGrant,
) -> (GrantedAuxiliaryModelRoute, DeviceDirectProvider) {
    let mut vision_route = grant.model_route.clone();
    vision_route.route_ref = VersionedKey::new("byok.vision.conn-openai-personal-0123456789ab@1")
        .expect("BYOK vision route ref");
    vision_route.resolved_model_key = "gpt-5.1".to_owned();
    vision_route.api_format = ApiFormat::OpenAiChatCompletions;
    vision_route.adapter_version =
        VersionedKey::new("openai.chat-completions-adapter@1").expect("adapter");
    vision_route.protocol_version =
        VersionedKey::new("openai.chat-completions@1").expect("protocol");
    vision_route.capabilities = BTreeSet::from([
        DottedKey::new("streaming").expect("capability"),
        DottedKey::new("vision").expect("capability"),
    ]);
    vision_route.parameters = BTreeMap::new();
    (
        GrantedAuxiliaryModelRoute {
            role: musteragent_model::AuxiliaryModelRole::Vision,
            route: vision_route,
            credential_grant_ref: grant.grant_ref.clone(),
        },
        DeviceDirectProvider {
            base_url: "https://api.openai.com/v1".to_owned(),
            api_format: ApiFormat::OpenAiChatCompletions,
            model: "gpt-5.1".to_owned(),
            credential_ref: "musteragent-secure://model-credentials/cred_test02".to_owned(),
        },
    )
}

#[test]
fn device_direct_vision_sidecar_fails_closed_outside_its_signed_shape() {
    let valid = byok_device_grant();

    // A BYOK vision route without the signed vision provider binding.
    let mut route_without_binding = valid.clone();
    let (vision_auxiliary, _) = byok_vision_sidecar(&route_without_binding);
    route_without_binding
        .auxiliary_model_routes
        .push(vision_auxiliary);
    assert_eq!(
        route_without_binding.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // The vision binding without its BYOK vision route widens the shape.
    let mut binding_without_route = valid.clone();
    let (_, vision_provider) = byok_vision_sidecar(&binding_without_route);
    binding_without_route.vision_direct_provider = Some(vision_provider);
    assert_eq!(
        binding_without_route.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // The vision binding cannot ride a grant without the primary
    // device-direct binding.
    let (mut cloud_with_vision, _) = device_grant();
    let (_, vision_provider) = byok_vision_sidecar(&cloud_with_vision);
    cloud_with_vision.vision_direct_provider = Some(vision_provider);
    assert_eq!(
        cloud_with_vision.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // The vision route must mirror the signed vision provider shape.
    let mut model_mismatch = valid.clone();
    let (vision_auxiliary, vision_provider) = byok_vision_sidecar(&model_mismatch);
    model_mismatch.auxiliary_model_routes.push(vision_auxiliary);
    model_mismatch.vision_direct_provider = Some(DeviceDirectProvider {
        model: "gpt-5.2".to_owned(),
        ..vision_provider
    });
    assert_eq!(
        model_mismatch.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    let mut api_format_mismatch = valid.clone();
    let (vision_auxiliary, vision_provider) = byok_vision_sidecar(&api_format_mismatch);
    api_format_mismatch
        .auxiliary_model_routes
        .push(vision_auxiliary);
    api_format_mismatch.vision_direct_provider = Some(DeviceDirectProvider {
        api_format: ApiFormat::AnthropicMessages,
        ..vision_provider
    });
    assert_eq!(
        api_format_mismatch.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // The Vision sidecar must reuse the primary credential budget.
    let mut foreign_credential = valid.clone();
    let (mut vision_auxiliary, vision_provider) = byok_vision_sidecar(&foreign_credential);
    vision_auxiliary.credential_grant_ref =
        CredentialGrantRef::new("grant_device-vision").expect("credential grant");
    foreign_credential
        .auxiliary_model_routes
        .push(vision_auxiliary);
    foreign_credential.vision_direct_provider = Some(vision_provider);
    assert_eq!(
        foreign_credential.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // A plain registry vision route is not the BYOK vision sidecar.
    let mut registry_vision = valid.clone();
    let (mut vision_auxiliary, vision_provider) = byok_vision_sidecar(&registry_vision);
    vision_auxiliary.route.route_ref =
        VersionedKey::new("registry.aux.vision.id-019fa6de-38fd-7000-8000-000000000511@1")
            .expect("registry vision ref");
    registry_vision
        .auxiliary_model_routes
        .push(vision_auxiliary);
    registry_vision.vision_direct_provider = Some(vision_provider);
    assert_eq!(
        registry_vision.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );

    // An invalid vision binding endpoint is rejected on its own.
    let mut bad_endpoint = valid;
    let (vision_auxiliary, vision_provider) = byok_vision_sidecar(&bad_endpoint);
    bad_endpoint.auxiliary_model_routes.push(vision_auxiliary);
    bad_endpoint.vision_direct_provider = Some(DeviceDirectProvider {
        base_url: "http://api.openai.com/v1".to_owned(),
        ..vision_provider
    });
    assert_eq!(
        bad_endpoint.validate(),
        Err(ExecutionGrantContractError::InvalidDeviceDirectProvider)
    );
}
