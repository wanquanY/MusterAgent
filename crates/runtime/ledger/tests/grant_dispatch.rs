//! Current grant envelopes route by authority; the shared envelope version
//! cannot identify whether an assigned hosted executor owns the Run.
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_event_envelope::{HostedHomeBinding, HostedProjectScope};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding,
};
use musteragent_identity::{
    DeviceId, DottedKey, HomeBindingId, HomeEpoch, PrincipalId, ProjectId, RunId, TenantId,
    UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_ledger::run_dispatch_topic;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelRoute, PricingVersionRef, ProviderStatePolicy,
};
use musteragent_policy::{Decision, Effect, PermissionMode};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};

pub fn sample_grants() -> Vec<(&'static str, ExecutionGrant)> {
    let tenant_id = "019fa6de-38fd-7000-8000-000000000001"
        .parse::<TenantId>()
        .expect("fixture identity");
    let home_device = "019fa6de-38fd-7000-8000-000000000002"
        .parse::<DeviceId>()
        .expect("fixture identity");
    let scope = HostedProjectScope {
        project_id: "019fa6de-38fd-7000-8000-000000000003"
            .parse::<ProjectId>()
            .expect("fixture identity"),
        workspace_id: "019fa6de-38fd-7000-8000-000000000004"
            .parse::<WorkspaceId>()
            .expect("fixture identity"),
        resource_tenant_id: tenant_id,
        owner_principal_id: "019fa6de-38fd-7000-8000-000000000005"
            .parse::<PrincipalId>()
            .expect("fixture identity"),
        home: HostedHomeBinding {
            binding_id: "019fa6de-38fd-7000-8000-000000000006"
                .parse::<HomeBindingId>()
                .expect("fixture identity"),
            device_id: home_device,
            home_epoch: HomeEpoch::new(7).expect("home epoch"),
        },
    };
    let route = ModelRoute {
        route_ref: VersionedKey::new("fixture.chat@1").expect("route"),
        resolved_model_key: "fixture-model".into(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000511"
            .parse()
            .expect("provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000512"
            .parse()
            .expect("model"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(32_768).expect("context"),
        max_output_tokens: NonZeroU64::new(2_048).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1").expect("price"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    };
    [
        (
            "tenant_cloud",
            ExecutionGrantAuthorityScope::Tenant { tenant_id },
            None,
            None,
        ),
        (
            "tenant_device",
            ExecutionGrantAuthorityScope::Tenant { tenant_id },
            Some(home_device),
            None,
        ),
        (
            "workspace",
            ExecutionGrantAuthorityScope::Workspace {
                workspace_id: scope.workspace_id,
            },
            None,
            None,
        ),
        (
            "hosted_home",
            ExecutionGrantAuthorityScope::HostedProject { scope },
            Some(home_device),
            Some(scope.workspace_id),
        ),
        (
            "hosted_assigned_executor",
            ExecutionGrantAuthorityScope::HostedProjectExecution {
                scope,
                assignment_digest: [19; 32],
            },
            Some(
                "019fa6de-38fd-7000-8000-000000000007"
                    .parse::<DeviceId>()
                    .expect("fixture identity"),
            ),
            Some(scope.workspace_id),
        ),
    ]
    .into_iter()
    .map(|(name, authority_scope, device_id, device_workspace_id)| {
        let grant = ExecutionGrant {
            schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("schema"),
            product_release: None,
            grant_ref: CredentialGrantRef::new("grant_fixture").expect("grant"),
            issuer: DottedKey::new("musteragent.backend.control-plane").expect("issuer"),
            audience: DottedKey::new(device_id.map_or_else(
                || "musteragent.runtime.cloud".into(),
                |id| format!("musteragent.runtime.device.id-{id}"),
            ))
            .expect("audience"),
            tenant_id,
            model_provider_id: route.model_provider_id,
            model_entry_id: route.model_entry_id,
            run_id: "019fa6de-38fd-7000-8000-000000000008"
                .parse::<RunId>()
                .expect("fixture identity"),
            issued_at: UtcTimestamp::from_unix_millis(100),
            expires_at: UtcTimestamp::from_unix_millis(200),
            nonce: OpaqueExternalRef::new("fixture_nonce").expect("nonce"),
            requested_model_class: ModelClass::new("chat.balanced").expect("class"),
            model_route: route.clone(),
            fallback_model_routes: vec![],
            auxiliary_model_routes: vec![],
            agent_contract: Some(
                VersionedKey::new("musteragent.runtime-contract@1").expect("contract"),
            ),
            surface: Some(
                DottedKey::new(if device_id.is_some() {
                    "desktop"
                } else {
                    "web"
                })
                .expect("surface"),
            ),
            capability_entitlements: vec![CapabilityEntitlement {
                key: CapabilityKey::from_versioned_key(
                    VersionedKey::new("system.time.current@1").expect("capability"),
                ),
                provider_id: "019fa6de-38fd-7000-8000-000000000513"
                    .parse()
                    .expect("capability provider"),
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("scope")]),
                policy_decision: Decision::Allow,
            }],
            identity_binding: Some(ExecutionGrantIdentityBinding {
                principal_id: "019fa6de-38fd-7000-8000-000000000009"
                    .parse::<PrincipalId>()
                    .expect("fixture identity"),
                authority_scope,
                device_id,
                device_workspace_id,
            }),
            permission_mode: Some(if device_id.is_some() {
                PermissionMode::Supervised
            } else {
                PermissionMode::ServiceManaged
            }),
            device_direct_provider: None,
            vision_direct_provider: None,
            tool_contract_releases: vec![],
            reviewed_application_source_sha256: vec![],
        };
        (name, grant)
    })
    .collect()
}

#[test]
fn current_grant_routes_only_assigned_hosted_execution_to_executor_dispatch() {
    for (name, grant) in sample_grants() {
        grant.validate().expect(name);
        assert_eq!(
            run_dispatch_topic(&grant),
            if name == "hosted_assigned_executor" {
                "run.execution.dispatch"
            } else {
                "run.dispatch"
            },
            "{name}"
        );
        let mut missing_binding = grant.clone();
        missing_binding.identity_binding = None;
        assert!(
            missing_binding.validate().is_err(),
            "{name} missing binding"
        );
        let mut retired = grant;
        retired.schema_version =
            DottedKey::new("musteragent.execution-grant.v9").expect("retired version");
        assert!(retired.validate().is_err(), "{name} retired envelope");
    }
}
