use super::*;
use musteragent_capability::CapabilityGrantSnapshot;
use musteragent_policy::{Decision, PermissionMode};
use musteragent_runtime_capability::InvocationAuthorization;
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, RunExecutionPolicySnapshot, ToolDefinitionSnapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};

pub(super) fn material(
    f: &Fixture,
    run_context: RunContext,
    definition: &musteragent_tool::BuiltinToolDefinition,
    provider_id: CapabilityProviderId,
) -> ToolExecutionMaterial {
    let manifest =
        builtin_capability_manifest_at_location(definition, provider_id, location(definition))
            .expect("valid durable execution fixture");
    let tool = ToolDefinitionSnapshot {
        manifest: definition.pin(provider_id),
        execution_target: definition.target.clone(),
        concurrency_class: definition.concurrency,
        idempotency: definition.idempotency,
        file_operation: definition.file_operation.clone(),
        title: definition.title.clone(),
        description: definition.description.clone(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: definition.input_schema_json.clone(),
        output_schema_json: definition.output_schema_json.clone(),
    };
    let grant = CapabilityGrantSnapshot {
        key: manifest.key.clone(),
        provider_id,
        location: manifest.location,
        effect: manifest.effect,
        allowed_scopes: manifest.required_scopes.clone(),
        policy_decision: Decision::Allow,
    };
    let authorized_tools = musteragent_runtime_snapshot::authorized_tool_catalog(
        std::slice::from_ref(&tool),
        std::slice::from_ref(&grant),
    )
    .expect("valid durable execution fixture");
    let exposure_state = musteragent_discovery::ExposureState::from_initial(&authorized_tools, &[])
        .expect("valid durable execution fixture");
    ToolExecutionMaterial {
        application_capabilities: vec![],
        product_release: Some(musteragent_tool::compiled_product_release(
            ProductId::KERNEL,
        )),
        run_context,
        recovery_policy: NonZeroU32::new(8)
            .expect("valid durable execution fixture")
            .into(),
        actor: f.owner,
        agent_id: f.activation.agent.agent_id,
        model: model(),
        active_model_route_ordinal: 0,
        auxiliary_models: vec![],
        model_stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        model_liveness: None,
        tool,
        authorized_tools,
        discovery: None,
        exposure_state,
        capability_grant: grant,
        authorization: InvocationAuthorization {
            actor_scopes: manifest.required_scopes.clone(),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: true,
        },
        permission_mode: PermissionMode::Supervised,
        policy_basis: None,
        authorized_content: vec![],
        prior_tool_results: vec![],
        execution_grant_proof: None,
    }
}
pub(super) fn model() -> ModelRouteSnapshot {
    use musteragent_model::*;
    let key = |s| VersionedKey::new(s).expect("valid durable execution fixture");
    ModelRouteSnapshot {
        requested_model_class: musteragent_runtime_contract::ModelClass::new("chat.balanced")
            .expect("valid durable execution fixture"),
        route: ModelRoute {
            route_ref: key("fixture.route@1"),
            resolved_model_key: "fixture".into(),
            model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::now_v7())
                .expect("valid durable execution fixture"),
            model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::now_v7())
                .expect("valid durable execution fixture"),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: key("fixture.adapter@1"),
            protocol_version: key("fixture.protocol@1"),
            context_window: NonZeroU64::new(4096).expect("valid durable execution fixture"),
            max_output_tokens: NonZeroU64::new(256).expect("valid durable execution fixture"),
            capabilities: BTreeSet::new(),
            parameters: BTreeMap::new(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1")
                .expect("valid durable execution fixture"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        credential_grant_ref: CredentialGrantRef::new("grant_fixture")
            .expect("valid durable execution fixture"),
        fallback_candidates: vec![],
    }
}
