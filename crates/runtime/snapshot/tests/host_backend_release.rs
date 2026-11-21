//! Release-bound snapshot validation and derived-run narrowing tests for
//! host-dispatched `BackendOperation` capabilities.
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::Duration;

use musteragent_capability::{
    CapabilityEntitlement, CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation,
};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_definition::{
    AgentDefinition, AgentKind, AgentLifecycle, AgentVersion, AgentVersionMaterialV1,
    ReleaseChannel,
};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding,
};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, CapabilityProviderId, ContentId, DottedKey, PrincipalId,
    ProductId, ProductReleaseRef, RunId, RuntimeSnapshotId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy, PermissionMode,
};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, CONTEXT_COMPACTION_POLICY_V11,
    ContextCompactionPolicyV11Spec, ContextPolicySnapshot, ContextSourceSelectionPolicy,
    HostCatalogSnapshotBuilder, HostToolDescriptor, MODEL_PROTOCOL_V2,
    MUSTERAGENT_ASSISTANT_PROMPT_SECTION, ModelRouteSnapshot, OutputPolicySnapshot, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot,
    RunExecutionPolicySnapshot, RuntimeSnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
    SnapshotCodec, SnapshotContractError, SnapshotSealError, ToolDefinitionSnapshot,
    authorized_tool_catalog, host_capability_provider_id,
};
use musteragent_tool::{
    BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID, BackendOperation, BuiltinIdempotency,
    BuiltinToolCatalog, CapabilityExecutionTarget, ConcurrencyClass, HostProductRelease,
    ProductExecutionTarget, ToolContractError, ToolExecutionPolicy, ToolKey, ToolManifest,
    ToolRetryContract, WireName, compiled_product_release, install_host_product_releases,
    product_release_metadata,
};

struct DeterministicTestCodec;

impl SnapshotCodec for DeterministicTestCodec {
    type Error = &'static str;

    fn encode_content(&self, content: &RuntimeSnapshotContent) -> Result<Vec<u8>, Self::Error> {
        Ok(format!(
            "snapshot:{}:{}:{}",
            content.schema_version.as_str(),
            content.tools.len(),
            content.capabilities.len()
        )
        .into_bytes())
    }

    fn decode_content(&self, _bytes: &[u8]) -> Result<RuntimeSnapshotContent, Self::Error> {
        Err("decode unused in seal verification")
    }
}

fn content_ref(id: &str, media_type: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::from_str(id).expect("valid content id"),
        media_type: MediaType::new(media_type).expect("valid media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn host_inventory_provider_id() -> CapabilityProviderId {
    host_capability_provider_id(
        &uuid::Uuid::NAMESPACE_OID,
        "example.test/capability-provider/inventory.v1",
    )
    .expect("deterministic non-nil UUID")
}

fn quote_input_schema() -> Vec<u8> {
    br#"{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}"#.to_vec()
}

fn quote_output_schema() -> Vec<u8> {
    br#"{"type":"object","properties":{"unit_price":{"type":"number"}},"required":["unit_price"],"additionalProperties":false}"#.to_vec()
}

fn dynamic_read_descriptor(key_str: &str, wire: &str, method: &str) -> HostToolDescriptor {
    HostToolDescriptor::new(
        key_str,
        "Look up an inventory quote via a host capability provider.",
        quote_input_schema(),
        quote_output_schema(),
        Effect::Read,
        ApprovalRequirement::Never,
        method,
    )
    .with_title("Lookup Inventory Quote")
    .with_wire_name(wire)
    .with_scopes(["inventory.quote.read"])
}

fn dynamic_backend_read_tool(
    key_str: &str,
    wire: &str,
    method: &str,
    provider_id: CapabilityProviderId,
) -> (ToolDefinitionSnapshot, CapabilityGrantSnapshot) {
    let key_v = VersionedKey::new(key_str).expect("valid versioned key");
    let tool_key = ToolKey::from_versioned_key(key_v.clone());
    let input_schema_json = quote_input_schema();
    let output_schema_json = quote_output_schema();
    let scope = DottedKey::new("inventory.quote.read").expect("valid scope");
    let target = CapabilityExecutionTarget::BackendNamed(
        BackendOperation::new(method, None).expect("valid Service.Method backend operation"),
    );
    let manifest = ToolManifest {
        application: None,
        key: tool_key,
        wire_name: WireName::new(wire).expect("valid wire name"),
        input_schema: SchemaRef {
            schema_key: VersionedKey::new(format!(
                "{}.input@{}",
                key_v.logical_key(),
                key_v.version()
            ))
            .expect("schema key"),
            schema_hash: Sha256::digest(&input_schema_json),
        },
        output_schema: SchemaRef {
            schema_key: VersionedKey::new(format!(
                "{}.output@{}",
                key_v.logical_key(),
                key_v.version()
            ))
            .expect("schema key"),
            schema_hash: Sha256::digest(&output_schema_json),
        },
        capability_key: key_v.clone(),
        capability_provider_id: provider_id,
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        execution_policy: ToolExecutionPolicy::builtin_activity(
            key_v.clone(),
            Duration::from_secs(30),
            None,
        ),
        max_attempts: NonZeroU32::MIN,
        retry_contract: ToolRetryContract::ReadOnly,
        required_scopes: BTreeSet::from([scope.clone()]),
        required_contexts: BTreeSet::new(),
        data_classification: DataClassification::Internal,
        egress_policy: EgressPolicy::ProviderBound,
    };
    let tool = ToolDefinitionSnapshot {
        manifest,
        execution_target: target,
        concurrency_class: ConcurrencyClass::BoundedNetworkRead,
        idempotency: BuiltinIdempotency::Native,
        file_operation: None,
        title: "Lookup Inventory Quote".to_owned(),
        description: "Look up an inventory quote via a host capability provider.".to_owned(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json,
        output_schema_json,
    };
    tool.validate()
        .expect("dynamic BackendNamed read tool validates");
    let grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(key_v),
        provider_id,
        location: CapabilityLocation::Backend,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([scope]),
        policy_decision: Decision::Allow,
    };
    (tool, grant)
}

fn dynamic_backend_write_tool(
    provider_id: CapabilityProviderId,
    approval: ApprovalRequirement,
) -> ToolDefinitionSnapshot {
    let key_v = VersionedKey::new("inventory.reservation.create@1").expect("key");
    let input_schema_json = quote_input_schema();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    let scope = DottedKey::new("inventory.reservation.write").expect("scope");
    ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(key_v.clone()),
            wire_name: WireName::new("inventory_reservation_create").expect("wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("inventory.reservation.create.input@1").expect("in"),
                schema_hash: Sha256::digest(&input_schema_json),
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("inventory.reservation.create.output@1")
                    .expect("out"),
                schema_hash: Sha256::digest(&output_schema_json),
            },
            capability_key: key_v.clone(),
            capability_provider_id: provider_id,
            effect: Effect::WriteExternal,
            approval,
            execution_policy: ToolExecutionPolicy::builtin_activity(
                key_v,
                Duration::from_secs(30),
                None,
            ),
            max_attempts: NonZeroU32::MIN,
            retry_contract: ToolRetryContract::QueryBeforeRetry,
            required_scopes: BTreeSet::from([scope]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::ProviderBound,
        },
        execution_target: CapabilityExecutionTarget::BackendNamed(
            BackendOperation::new("InventoryService.CreateReservation", None).expect("operation"),
        ),
        concurrency_class: ConcurrencyClass::SequentialWrite,
        idempotency: BuiltinIdempotency::ProviderTaskRecord,
        file_operation: None,
        title: "Create Inventory Reservation".to_owned(),
        description: "Reserve inventory through the host capability provider.".to_owned(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json,
        output_schema_json,
    }
}

fn build_orchestration_snapshot_from_helper(
    product_release: ProductReleaseRef,
    host_tools: Vec<HostToolDescriptor>,
) -> RuntimeSnapshotContent {
    let partitions = HostCatalogSnapshotBuilder::new(host_inventory_provider_id())
        .with_product_release(product_release.clone())
        .with_host_tools(host_tools)
        .build()
        .expect("helper partitions");
    let mut content =
        build_orchestration_envelope(Some(product_release), partitions.base_skill_refs());
    partitions.apply_to(&mut content);
    content
}

fn build_orchestration_snapshot(
    product_release: Option<ProductReleaseRef>,
    extra_tools: Vec<(ToolDefinitionSnapshot, CapabilityGrantSnapshot)>,
) -> RuntimeSnapshotContent {
    let base_release = product_release
        .clone()
        .unwrap_or_else(|| compiled_product_release(ProductId::KERNEL));
    let mut partitions = HostCatalogSnapshotBuilder::new(host_inventory_provider_id())
        .with_product_release(base_release)
        .build()
        .expect("baseline partitions");

    let mut grants: BTreeMap<CapabilityKey, CapabilityGrantSnapshot> = partitions
        .capabilities
        .into_iter()
        .map(|g| (g.key.clone(), g))
        .collect();
    for (tool, grant) in extra_tools {
        grants.insert(grant.key.clone(), grant);
        partitions.tools.push(tool);
    }
    partitions
        .tools
        .sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    partitions.capabilities = grants.into_values().collect();
    partitions.discovery.authorized_tool_catalog_hash =
        authorized_tool_catalog(&partitions.tools, &partitions.capabilities)
            .map(|c| c.catalog_hash())
            .unwrap_or_else(|_| Sha256::digest(b"unvalidated-catalog"));

    let mut content =
        build_orchestration_envelope(product_release.clone(), partitions.base_skill_refs());
    content.capabilities = partitions.capabilities;
    content.tools = partitions.tools;
    content.skills = partitions.skills;
    content.discovery = Some(partitions.discovery);
    content.product_release = product_release;
    content
}

fn build_orchestration_envelope(
    product_release: Option<ProductReleaseRef>,
    base_skill_refs: Vec<VersionedKey>,
) -> RuntimeSnapshotContent {
    let policy_bytes = b"safety policy bytes";
    let persona_bytes = b"assistant persona bytes";
    let memory_bytes = b"memory policy bytes";
    let manifest_bytes = b"agent manifest bytes";
    let safety_ref = content_ref(
        "01982f80-0000-7000-8000-000000000010",
        "text/plain;charset=utf-8",
        policy_bytes,
    );
    let persona_ref = content_ref(
        "01982f80-0000-7000-8000-000000000003",
        "text/markdown;charset=utf-8",
        persona_bytes,
    );
    let memory_ref = content_ref(
        "01982f80-0000-7000-8000-00000000000f",
        "text/plain;charset=utf-8",
        memory_bytes,
    );
    let manifest_ref = content_ref(
        "01982f80-0000-7000-8000-000000000015",
        "application/json",
        manifest_bytes,
    );
    let agent_def = AgentDefinition {
        agent_id: AgentId::from_str("01982f80-0000-7000-8000-000000000001").expect("agent id"),
        agent_key: AgentKey::new("assistant").expect("agent key"),
        system_slot: Some("main".to_owned()),
        display_name: "Assistant".to_owned(),
        kind: AgentKind::SystemBuiltin,
        built_in: true,
        deletable: false,
        replaceable: false,
        default_for_product: true,
        lifecycle: AgentLifecycle::Active,
    };
    let agent_ver = AgentVersion {
        agent_version_id: AgentVersionId::from_str("01982f80-0000-7000-8000-000000000101")
            .expect("version id"),
        agent_id: agent_def.agent_id,
        version: VersionNumber::new(1).expect("v1"),
        persona_version: VersionNumber::new(1).expect("p1"),
        persona_content_ref: persona_ref.clone(),
        base_skill_refs,
        policy_ref: VersionedKey::new("musteragent.runtime-policy@1").expect("policy ref"),
        policy_hash: safety_ref.sha256,
        memory_policy_ref: VersionedKey::new(
            "musteragent.memory-selection.scope-confidence-recency@1",
        )
        .expect("memory policy"),
        model_requirement_ref: VersionedKey::new("musteragent.chat-model@1").expect("model req"),
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")
            .expect("contract"),
        release_channel: ReleaseChannel::Stable,
        published_at: UtcTimestamp::from_unix_millis(1_790_874_000_000),
        activated_at: UtcTimestamp::from_unix_millis(1_790_874_000_000),
        supersedes: None,
        material: Some(AgentVersionMaterialV1 {
            manifest_hash: manifest_ref.sha256,
            manifest_ref,
            policy_content_ref: safety_ref.clone(),
            memory_policy_hash: memory_ref.sha256,
            memory_policy_content_ref: memory_ref,
            user_memory_collection_id: None,
            long_term_memory_collection_id: None,
        }),
    };

    let route = ModelRoute {
        route_ref: VersionedKey::new("host.chat@1").expect("route ref"),
        resolved_model_key: "gpt-test".parse().expect("model key"),
        model_provider_id: ModelProviderId::from_str("01982f80-0000-7000-8000-000000000411")
            .expect("provider id"),
        model_entry_id: ModelEntryId::from_str("01982f80-0000-7000-8000-000000000412")
            .expect("entry id"),
        api_format: ApiFormat::OpenAiChatCompletions,
        adapter_version: VersionedKey::new("openai.chat-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.chat@1").expect("protocol"),
        context_window: NonZeroU64::new(128_000).expect("window"),
        max_output_tokens: NonZeroU64::new(8_192).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:host.unpriced@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    };
    let budgets = ContextCompactionPolicyV11Spec::resolve(&route).expect("budgets");

    let mut prompt_bytes = policy_bytes.to_vec();
    prompt_bytes.extend_from_slice(persona_bytes);
    let mut execution_policy = RunExecutionPolicySnapshot::production_defaults();
    execution_policy.liveness =
        Some(musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default());

    RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: Vec::new(),
        product_release,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
        agent: AgentVersionSnapshot::from_resolved(&agent_def, &agent_ver).expect("agent snapshot"),
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("safety"),
                    content_ref: safety_ref,
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new(MUSTERAGENT_ASSISTANT_PROMPT_SECTION)
                        .expect("assistant"),
                    content_ref: persona_ref,
                },
            ],
            assembled_hash: Sha256::digest(&prompt_bytes),
        },
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("host.chat@1").expect("model class"),
            route,
            credential_grant_ref: CredentialGrantRef::new("grant_root_1").expect("grant ref"),
            fallback_candidates: Vec::new(),
        },
        auxiliary_models: Vec::new(),
        skills: Vec::new(),
        policy: PolicySnapshot {
            policy_ref: agent_ver.policy_ref,
            policy_hash: agent_ver.policy_hash,
            permission_mode: PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: agent_ver.memory_policy_ref,
            selection_policy_hash: agent_ver
                .material
                .as_ref()
                .expect("agent material")
                .memory_policy_hash,
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: budgets.hard_input_budget,
            compaction_trigger_tokens: budgets.trigger_budget,
            reserved_output_tokens: budgets.reserved_output_budget,
            compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V11)
                .expect("compaction"),
            resolved_v2: Some(budgets),
            source_selection: Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1),
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy,
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2").expect("cmd"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("rec"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snap"),
            model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2).expect("proto"),
        },
        discovery: None,
        tools: Vec::new(),
        capabilities: Vec::new(),
    }
}

fn install_fixture_host_releases() -> (ProductReleaseRef, ProductReleaseRef) {
    let kernel = compiled_product_release(ProductId::KERNEL);
    let metadata = product_release_metadata(&kernel).expect("kernel metadata");
    let cloud_catalog =
        BuiltinToolCatalog::for_product_release(&kernel, ProductExecutionTarget::Cloud)
            .expect("cloud catalog");
    let device_catalog =
        BuiltinToolCatalog::for_product_release(&kernel, ProductExecutionTarget::Device)
            .expect("device catalog");
    let cloud_only_backend = cloud_catalog
        .definitions()
        .iter()
        .find(|d| d.target.backend_operation().is_some() && device_catalog.by_key(&d.key).is_none())
        .expect("compiled cloud-only backend tool");
    let contract: serde_json::Value =
        serde_json::from_str(include_str!("../../../../contracts/kernel-tools.json"))
            .expect("kernel tools json");
    let bootstrap_logical: BTreeSet<&str> = [
        "tool.catalog.search",
        "tool.catalog.activate",
        "interaction.request",
        "skills.search",
        "skills.list",
        "skills.view",
        "plan.get",
        "plan.update",
    ]
    .into_iter()
    .collect();
    let subset: Vec<serde_json::Value> = contract["products"]["musteragent"]["cloud"]["tools"]
        .as_array()
        .expect("cloud tools")
        .iter()
        .filter(|tool| {
            tool["tool_key"]
                .as_str()
                .and_then(|k| k.split_once('@'))
                .is_some_and(|(logical, _)| bootstrap_logical.contains(logical))
        })
        .cloned()
        .collect();
    let mut asymmetric_cloud = subset.clone();
    asymmetric_cloud.push(
        contract["products"]["musteragent"]["cloud"]["tools"]
            .as_array()
            .expect("cloud tools")
            .iter()
            .find(|tool| tool["tool_key"].as_str() == Some(cloud_only_backend.key.as_str()))
            .expect("raw cloud-only backend tool")
            .clone(),
    );
    let minimal_doc = serde_json::json!({
        "schema_version": "musteragent.host-tool-release.v1",
        "product": "minimal-host",
        "release_id": "minimal-host.tools@1",
        "profile_id": "minimal-host",
        "cloud_head": "minimal.cloud@1",
        "device_head": "minimal.device@1",
        "skill_schema": metadata.skill_schema,
        "skill_release": {
            "product": kernel.product.as_str(),
            "release_id": kernel.release_id.as_str(),
            "digest": kernel.digest
        },
        "cloud": subset,
        "device": subset
    });
    let asymmetric_doc = serde_json::json!({
        "schema_version": "musteragent.host-tool-release.v1",
        "product": "asymmetric-host",
        "release_id": "asymmetric-host.tools@1",
        "profile_id": "asymmetric-host",
        "cloud_head": "asymmetric.cloud@1",
        "device_head": "asymmetric.device@1",
        "skill_schema": metadata.skill_schema,
        "skill_release": {
            "product": kernel.product.as_str(),
            "release_id": kernel.release_id.as_str(),
            "digest": kernel.digest
        },
        "cloud": asymmetric_cloud,
        "device": subset
    });
    let minimal_pub =
        HostProductRelease::from_json(&serde_json::to_vec(&minimal_doc).expect("json"))
            .expect("minimal release");
    let asymmetric_pub =
        HostProductRelease::from_json(&serde_json::to_vec(&asymmetric_doc).expect("json"))
            .expect("asymmetric release");
    let minimal_ref = minimal_pub.reference().clone();
    let asymmetric_ref = asymmetric_pub.reference().clone();
    install_host_product_releases(vec![minimal_pub, asymmetric_pub])
        .expect("install host releases");
    (minimal_ref, asymmetric_ref)
}

fn install_minimal_host_release() -> ProductReleaseRef {
    install_fixture_host_releases().0
}

#[test]
fn dynamic_backend_tool_seals_with_compiled_and_host_product_releases() {
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let provider = host_inventory_provider_id();
    let read_desc = dynamic_read_descriptor(
        "inventory.quote.lookup@1",
        "inventory_quote_lookup",
        "InventoryService.LookupQuote",
    );
    let write_desc = HostToolDescriptor::new(
        "inventory.reservation.create@1",
        "Reserve inventory through the host capability provider.",
        quote_input_schema(),
        br#"{"type":"object"}"#.to_vec(),
        Effect::WriteExternal,
        ApprovalRequirement::ByPolicy,
        "InventoryService.CreateReservation",
    )
    .with_title("Create Inventory Reservation")
    .with_scopes(["inventory.reservation.write"]);

    // Non-read tool without approval floor is rejected by ToolManifest validation.
    let unsafe_write = dynamic_backend_write_tool(provider, ApprovalRequirement::Never);
    assert!(matches!(
        unsafe_write.validate(),
        Err(SnapshotContractError::Tool(
            ToolContractError::UnsafeApproval(_)
        ))
    ));

    // Generic ExecuteTool method names remain forbidden.
    assert!(BackendOperation::new("InventoryService.ExecuteTool", None).is_err());
    assert!(BackendOperation::new("InventoryService.execute_tool", None).is_err());

    for release in [
        compiled_product_release(ProductId::KERNEL),
        install_minimal_host_release(),
    ] {
        let content = build_orchestration_snapshot_from_helper(
            release,
            vec![read_desc.clone(), write_desc.clone()],
        );
        RuntimeSnapshot::seal(
            RuntimeSnapshotId::generate(),
            content,
            now,
            &DeterministicTestCodec,
        )
        .expect("dynamic host-dispatched backend tools seal with product_release");
    }
}

#[test]
fn dynamic_backend_tool_rejects_builtin_provider_ids() {
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let release = compiled_product_release(ProductId::KERNEL);

    for raw_provider in [BUILTIN_RUST_PROVIDER_ID, BUILTIN_BACKEND_PROVIDER_ID] {
        let builtin_provider: CapabilityProviderId =
            raw_provider.parse().expect("builtin provider id");
        let pair = dynamic_backend_read_tool(
            "inventory.quote.lookup@1",
            "inventory_quote_lookup",
            "InventoryService.LookupQuote",
            builtin_provider,
        );
        let content = build_orchestration_snapshot(Some(release.clone()), vec![pair]);
        let err = RuntimeSnapshot::seal(
            RuntimeSnapshotId::generate(),
            content,
            now,
            &DeterministicTestCodec,
        )
        .expect_err("builtin provider id must be rejected for unlisted backend tool");
        assert!(
            matches!(
                err,
                SnapshotSealError::Contract(SnapshotContractError::ProductNamespaceMismatch)
            ),
            "unexpected error for {raw_provider}: {err:?}"
        );
    }
}

#[test]
fn dynamic_backend_tool_rejects_collision_with_compiled_cloud_or_device_catalogs() {
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let minimal_host_release = install_minimal_host_release();
    let provider = host_inventory_provider_id();

    let cloud_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Cloud)
            .expect("cloud catalog");
    let device_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Device)
            .expect("device catalog");

    let cloud_backend_collision = cloud_catalog
        .definitions()
        .iter()
        .find(|d| {
            d.target.backend_operation().is_some()
                && d.target.media_artifact().is_none()
                && musteragent_runtime_snapshot::capability_available_for_dialogue_policy(
                    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
                    d.capability_key.as_str(),
                )
        })
        .expect("compiled cloud backend tool");

    let device_only_collision = device_catalog
        .definitions()
        .iter()
        .find(|d| {
            cloud_catalog.by_key(&d.key).is_none()
                && !d.key.as_str().starts_with("device.host.desktop.")
                && !d.key.as_str().starts_with("device.workspace.video.render")
        })
        .expect("compiled device-only tool");

    for colliding_key in [
        cloud_backend_collision.key.as_str(),
        "system.time.current@1",
        device_only_collision.key.as_str(),
    ] {
        let pair = dynamic_backend_read_tool(
            colliding_key,
            "colliding_host_tool",
            "InventoryService.LookupQuote",
            provider,
        );
        for release in [&kernel_release, &minimal_host_release] {
            let mut content =
                build_orchestration_snapshot(Some(release.clone()), vec![pair.clone()]);
            let err = content
                .validate()
                .expect_err("collision with compiled catalog must fail validation");
            // `validate_product_release` runs before discovery policy checks, rejecting
            // any compiled Cloud or Device key collision with ProductNamespaceMismatch.
            assert!(
                matches!(err, SnapshotContractError::ProductNamespaceMismatch),
                "expected ProductNamespaceMismatch for key {colliding_key} under {:?}, got {err:?}",
                release.product
            );
            let seal_err = RuntimeSnapshot::seal(
                RuntimeSnapshotId::generate(),
                std::mem::replace(&mut content, build_orchestration_snapshot(None, Vec::new())),
                now,
                &DeterministicTestCodec,
            )
            .expect_err("seal must also fail");
            assert!(matches!(
                seal_err,
                SnapshotSealError::Contract(SnapshotContractError::ProductNamespaceMismatch)
            ));
        }
    }
}

#[test]
fn compiled_key_with_mismatched_contract_is_rejected_even_with_host_provider() {
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let release = compiled_product_release(ProductId::KERNEL);
    let cloud_catalog =
        BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)
            .expect("cloud catalog");
    let host_provider = host_inventory_provider_id();

    let compiled_backend = cloud_catalog
        .definitions()
        .iter()
        .find(|d| {
            d.target.backend_operation().is_some()
                && d.target.media_artifact().is_none()
                && d.required_contexts.is_empty()
                && musteragent_runtime_snapshot::capability_available_for_dialogue_policy(
                    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
                    d.capability_key.as_str(),
                )
        })
        .expect("compiled backend tool")
        .clone();

    let exact_tool = ToolDefinitionSnapshot {
        manifest: compiled_backend.pin(host_provider),
        execution_target: compiled_backend.target.clone(),
        concurrency_class: compiled_backend.concurrency,
        idempotency: compiled_backend.idempotency,
        file_operation: compiled_backend.file_operation.clone(),
        title: compiled_backend.title.clone(),
        description: compiled_backend.description.clone(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: compiled_backend.input_schema_json.clone(),
        output_schema_json: compiled_backend.output_schema_json.clone(),
    };
    let exact_grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(compiled_backend.capability_key.clone()),
        provider_id: host_provider,
        location: CapabilityLocation::Backend,
        effect: compiled_backend.effect,
        allowed_scopes: compiled_backend.required_scopes.clone(),
        policy_decision: Decision::Allow,
    };

    // Exact compiled contract pinned to a host provider seals cleanly.
    let exact_snapshot = build_orchestration_snapshot(
        Some(release.clone()),
        vec![(exact_tool.clone(), exact_grant.clone())],
    );
    RuntimeSnapshot::seal(
        RuntimeSnapshotId::generate(),
        exact_snapshot,
        now,
        &DeterministicTestCodec,
    )
    .expect("compiled backend tool with exact contract seals");

    // 1. Mismatched BackendOperation method on compiled key -> rejected.
    let mut wrong_method = exact_tool.clone();
    wrong_method.execution_target = CapabilityExecutionTarget::BackendNamed(
        BackendOperation::new("InventoryService.DifferentMethod", None).expect("method"),
    );
    let err = build_orchestration_snapshot(
        Some(release.clone()),
        vec![(wrong_method, exact_grant.clone())],
    )
    .validate()
    .expect_err("wrong backend method on compiled key must fail");
    assert!(matches!(
        err,
        SnapshotContractError::ProductNamespaceMismatch
    ));

    // 2. Mismatched input schema on compiled key -> rejected.
    let mut wrong_schema = exact_tool;
    let alt_schema =
        br#"{"type":"object","properties":{"unexpected":{"type":"string"}},"additionalProperties":false}"#
            .to_vec();
    wrong_schema.manifest.input_schema.schema_hash = Sha256::digest(&alt_schema);
    wrong_schema.input_schema_json = alt_schema;
    let err = build_orchestration_snapshot(Some(release), vec![(wrong_schema, exact_grant)])
        .validate()
        .expect_err("wrong schema on compiled key must fail");
    assert!(matches!(
        err,
        SnapshotContractError::ProductNamespaceMismatch
    ));
}

#[test]
fn derived_child_narrowing_retains_dynamic_backend_capability_and_seals_with_product_release() {
    let release = compiled_product_release(ProductId::KERNEL);
    let now = UtcTimestamp::from_unix_millis(1_790_874_100_000);
    let child_run_id = RunId::generate();
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let provider = host_inventory_provider_id();

    let dyn_desc = dynamic_read_descriptor(
        "inventory.quote.lookup@1",
        "inventory_quote_lookup",
        "InventoryService.LookupQuote",
    );
    let mut derived = build_orchestration_snapshot_from_helper(release, vec![dyn_desc]);
    let grant_ref =
        CredentialGrantRef::new(format!("grant_derived_{child_run_id}")).expect("grant ref");
    derived.model.credential_grant_ref = grant_ref.clone();
    derived
        .capabilities
        .retain(|c| c.location != CapabilityLocation::Device);
    derived.tools.retain(|tool| {
        derived
            .capabilities
            .iter()
            .any(|cap| cap.key.as_str() == tool.manifest.capability_key.as_str())
    });
    if let Some(discovery) = &mut derived.discovery {
        discovery
            .initial_exposed_tool_keys
            .retain(|key| derived.tools.iter().any(|t| t.manifest.key == *key));
        discovery.authorized_tool_catalog_hash =
            authorized_tool_catalog(&derived.tools, &derived.capabilities)
                .expect("recomputed catalog hash")
                .catalog_hash();
    }

    assert!(derived.capabilities.iter().any(|c| {
        c.key.as_str() == "inventory.quote.lookup@1"
            && c.location == CapabilityLocation::Backend
            && c.provider_id == provider
    }));

    let grant = ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: derived.product_release.clone(),
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
        grant_ref,
        issuer: DottedKey::new("musteragent.cloud.orchestration").expect("issuer"),
        audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
        tenant_id,
        model_provider_id: derived.model.route.model_provider_id,
        model_entry_id: derived.model.route.model_entry_id,
        run_id: child_run_id,
        issued_at: now,
        expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 600_000),
        nonce: OpaqueExternalRef::new(format!("grant_derived_{child_run_id}")).expect("nonce"),
        requested_model_class: derived.model.requested_model_class.clone(),
        model_route: derived.model.route.clone(),
        fallback_model_routes: Vec::new(),
        auxiliary_model_routes: Vec::new(),
        agent_contract: Some(
            derived
                .agent
                .resolved_v5
                .as_ref()
                .expect("resolved agent v5")
                .supported_runtime_contract
                .clone(),
        ),
        surface: Some(DottedKey::new("background").expect("surface")),
        capability_entitlements: derived
            .capabilities
            .iter()
            .map(|c| CapabilityEntitlement {
                key: c.key.clone(),
                provider_id: c.provider_id,
                location: c.location,
                effect: c.effect,
                allowed_scopes: c.allowed_scopes.clone(),
                policy_decision: c.policy_decision,
            })
            .collect(),
        tool_contract_releases: Vec::new(),
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id,
            authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
            device_id: None,
            device_workspace_id: None,
        }),
        permission_mode: Some(derived.policy.permission_mode),
        device_direct_provider: None,
        vision_direct_provider: None,
    };
    grant
        .validate()
        .expect("re-derived ExecutionGrant validates with host Backend capability");

    RuntimeSnapshot::seal(
        RuntimeSnapshotId::generate(),
        derived,
        now,
        &DeterministicTestCodec,
    )
    .expect("derived child snapshot seals with product_release = Some(KERNEL)");
}

#[test]
fn device_home_snapshot_with_cloud_only_backend_tool_is_rejected() {
    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let cloud_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Cloud)
            .expect("cloud catalog");
    let device_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Device)
            .expect("device catalog");

    let cloud_only_backend = cloud_catalog
        .definitions()
        .iter()
        .find(|d| d.target.backend_operation().is_some() && device_catalog.by_key(&d.key).is_none())
        .expect("compiled cloud-only backend tool")
        .clone();

    let (_, asymmetric_host_release) = install_fixture_host_releases();

    let backend_provider: CapabilityProviderId = BUILTIN_BACKEND_PROVIDER_ID
        .parse()
        .expect("backend provider");
    let host_provider = host_inventory_provider_id();

    for release in [&kernel_release, &asymmetric_host_release] {
        for provider in [backend_provider, host_provider] {
            let tool = ToolDefinitionSnapshot {
                manifest: cloud_only_backend.pin(provider),
                execution_target: cloud_only_backend.target.clone(),
                concurrency_class: cloud_only_backend.concurrency,
                idempotency: cloud_only_backend.idempotency,
                file_operation: cloud_only_backend.file_operation.clone(),
                title: cloud_only_backend.title.clone(),
                description: cloud_only_backend.description.clone(),
                tags: Vec::new(),
                when_to_use: None,
                input_schema_json: cloud_only_backend.input_schema_json.clone(),
                output_schema_json: cloud_only_backend.output_schema_json.clone(),
            };
            // Granting `CapabilityLocation::Device` selects `ProductExecutionTarget::Device`
            // in `validate_product_release`, which must NOT fall back to the Cloud catalog.
            let device_grant = CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(cloud_only_backend.capability_key.clone()),
                provider_id: provider,
                location: CapabilityLocation::Device,
                effect: cloud_only_backend.effect,
                allowed_scopes: cloud_only_backend.required_scopes.clone(),
                policy_decision: Decision::Allow,
            };
            let err =
                build_orchestration_snapshot(Some(release.clone()), vec![(tool, device_grant)])
                    .validate()
                    .expect_err("Device-home snapshot must not match Cloud-only backend contract");
            assert!(
                matches!(err, SnapshotContractError::ProductNamespaceMismatch),
                "expected ProductNamespaceMismatch from validate_product_release under {:?}, got {err:?}",
                release.product
            );
        }
    }
}
