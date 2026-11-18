use musteragent_event_envelope::{HostedHomeBinding, HostedProjectScope};
use musteragent_execution_grant::*;
use musteragent_identity::*;
use musteragent_model::*;
use musteragent_policy::PermissionMode;
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};
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

pub(crate) fn base_grant(schema: &str) -> ExecutionGrant {
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

pub(crate) fn assigned_grant() -> SignedExecutionGrant {
    let mut claims = base_grant(EXECUTION_GRANT_SCHEMA_V10);
    let executor = DeviceId::generate();
    let scope = HostedProjectScope {
        project_id: ProjectId::generate(),
        workspace_id: WorkspaceId::generate(),
        resource_tenant_id: claims.tenant_id,
        owner_principal_id: PrincipalId::generate(),
        home: HostedHomeBinding {
            binding_id: HomeBindingId::generate(),
            device_id: DeviceId::generate(),
            home_epoch: HomeEpoch::new(7).unwrap(),
        },
    };
    claims.issuer = DottedKey::new("musteragent.backend.control-plane").unwrap();
    claims.audience = DottedKey::new(format!("musteragent.runtime.device.id-{executor}")).unwrap();
    claims.agent_contract = Some(VersionedKey::new("kernel.agent@1").unwrap());
    claims.surface = Some(DottedKey::new("desktop").unwrap());
    claims.permission_mode = Some(PermissionMode::Supervised);
    claims.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: PrincipalId::generate(),
        authority_scope: ExecutionGrantAuthorityScope::HostedProjectExecution {
            scope,
            assignment_digest: [19; 32],
        },
        device_id: Some(executor),
        device_workspace_id: Some(scope.workspace_id),
    });
    claims.capability_entitlements = vec![musteragent_capability::CapabilityEntitlement {
        key: musteragent_capability::CapabilityKey::from_versioned_key(
            VersionedKey::new("core.time.read@1").unwrap(),
        ),
        provider_id: "019fa6de-38fd-7000-8000-000000000431".parse().unwrap(),
        location: musteragent_capability::CapabilityLocation::Device,
        effect: musteragent_policy::Effect::Read,
        allowed_scopes: BTreeSet::from([DottedKey::new("time.read").unwrap()]),
        policy_decision: musteragent_policy::Decision::Allow,
    }];
    claims.validate().unwrap();
    SignedExecutionGrant {
        key_id: DottedKey::new("test.signer").unwrap(),
        claims,
        signature: [1; 64],
        context_transport_annex: Vec::new(),
    }
}
