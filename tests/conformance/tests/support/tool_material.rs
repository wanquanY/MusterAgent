use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_identity::{
    Actor, AgentId, CapabilityProviderId, DottedKey, PrincipalKind, VersionedKey,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy};
use musteragent_postgres::PostgresToolExecutionMaterialSource;
use musteragent_runtime_capability::{InvocationAuthorization, ToolExecutionMaterial};
use musteragent_runtime_contract::ModelClass;
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, RunExecutionPolicySnapshot, ToolDefinitionSnapshot,
};
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, ToolKey, ToolManifest,
    ToolRetryContract, WireName,
};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufSnapshotCodec;

use crate::running_loop::{Fixture, run_context};

pub fn production_tool_material_source(
    pool: &sqlx::PgPool,
    codec: Arc<ProtobufLedgerPayloadCodec>,
) -> Arc<PostgresToolExecutionMaterialSource> {
    Arc::new(PostgresToolExecutionMaterialSource::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec,
    ))
}

#[allow(dead_code)]
pub fn fixture_tool_material(
    fixture: Fixture,
    provider_id: CapabilityProviderId,
) -> ToolExecutionMaterial {
    let input_schema_json = br#"{"type":"object"}"#.to_vec();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    let input_schema = SchemaRef {
        schema_key: VersionedKey::new("web.search.input@1").expect("input schema"),
        schema_hash: Sha256::digest(&input_schema_json),
    };
    let output_schema = SchemaRef {
        schema_key: VersionedKey::new("web.search.output@1").expect("output schema"),
        schema_hash: Sha256::digest(&output_schema_json),
    };
    let capability_key = VersionedKey::new("web.search@2").expect("capability key");
    let scope = DottedKey::new("web.search").expect("scope");
    let tool = ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(VersionedKey::new("web.search@1").expect("Tool key")),
            wire_name: WireName::new("web_search").expect("wire name"),
            input_schema,
            output_schema,
            capability_key: capability_key.clone(),
            capability_provider_id: provider_id,
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            execution_policy: musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_secs(30),
            },
            max_attempts: NonZeroU32::new(2).expect("attempts"),
            retry_contract: ToolRetryContract::ReadOnly,
            required_scopes: BTreeSet::from([scope.clone()]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::Internet,
        },
        execution_target: CapabilityExecutionTarget::RustRuntime,
        concurrency_class: ConcurrencyClass::ParallelRead,
        idempotency: BuiltinIdempotency::Native,
        file_operation: None,
        title: "Web search".into(),
        description: "Search the web.".into(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json,
        output_schema_json,
    };
    let grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(capability_key),
        provider_id,
        location: CapabilityLocation::Cloud,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([scope.clone()]),
        policy_decision: Decision::Allow,
    };
    let authorized_tools = musteragent_runtime_snapshot::authorized_tool_catalog(
        std::slice::from_ref(&tool),
        std::slice::from_ref(&grant),
    )
    .expect("authorized Tool catalog");
    let exposure_state = musteragent_discovery::ExposureState::from_initial(&authorized_tools, &[])
        .expect("empty legacy exposure");
    ToolExecutionMaterial {
        application_capabilities: vec![],
        product_release: None,
        model_liveness: None,
        recovery_policy: std::num::NonZeroU32::new(8)
            .expect("bounded fixture recovery")
            .into(),
        run_context: run_context(fixture),
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        agent_id: AgentId::generate(),
        model: fixture_model_route(),
        active_model_route_ordinal: 0,
        auxiliary_models: Vec::new(),
        model_stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        tool,
        authorized_tools,
        discovery: None,
        exposure_state,
        capability_grant: grant,
        authorization: InvocationAuthorization {
            actor_scopes: BTreeSet::from([scope]),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        },
        permission_mode: musteragent_policy::PermissionMode::Supervised,
        policy_basis: None,
        authorized_content: Vec::new(),
        prior_tool_results: Vec::new(),
        execution_grant_proof: None,
    }
}

#[allow(dead_code)]
pub fn fixture_model_route() -> ModelRouteSnapshot {
    ModelRouteSnapshot {
        requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
        route: ModelRoute {
            route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
            resolved_model_key: "test-model".into(),
            model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::now_v7())
                .expect("model provider"),
            model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::now_v7()).expect("model entry"),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
            protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
            context_window: NonZeroU64::new(128_000).expect("context window"),
            max_output_tokens: NonZeroU64::new(8_192).expect("output tokens"),
            capabilities: BTreeSet::from([DottedKey::new("tools").expect("capability")]),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:test-model.price@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        credential_grant_ref: CredentialGrantRef::new("grant_test").expect("credential grant"),
        fallback_candidates: Vec::new(),
    }
}
