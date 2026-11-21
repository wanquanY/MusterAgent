use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::{
    AgentDefinition, AgentKind, AgentLifecycle, AgentVersion, AgentVersionMaterialV1,
    ReleaseChannel,
};
use musteragent_discovery::{
    ActivationConstraints, ActivationRequest, ExposureState, SkillCatalogSearchRequest,
    ToolCatalogSearchRequest,
};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding,
};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, CapabilityProviderId, ContentId, DottedKey, IdentityError,
    InvocationId, PrincipalId, ProductId, RunId, RuntimeSnapshotId, SkillId, SkillVersionId,
    TenantId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{ApprovalRequirement, Decision, Effect, PermissionMode};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use musteragent_skill::{
    InstalledSkillSource, SkillIdentity, SkillKey, SkillProvenance, SkillSourceFormat, SkillTrust,
    SkillVersion, Surface,
};
use musteragent_tool::{
    BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID, BuiltinToolCatalog, HostProductRelease,
    MediaArtifactKind, ProductExecutionTarget, compiled_product_release,
    install_host_product_releases, product_release_metadata, require_product_release_admission,
};

use super::*;
use crate::{
    AgentVersionSnapshot, CONTEXT_COMPACTION_POLICY_V11, ContextCompactionPolicyV11Spec,
    ContextPolicySnapshot, ContextSourceSelectionPolicy, MODEL_PROTOCOL_V2,
    MUSTERAGENT_ASSISTANT_PROMPT_SECTION, ModelRouteSnapshot, OutputPolicySnapshot, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot,
    RunExecutionPolicySnapshot, RunLivenessPolicyV1, RuntimeSnapshot, RuntimeSnapshotContent,
    SNAPSHOT_SCHEMA_VERSION, SnapshotCodec, authorized_tool_catalog,
};

struct DeterministicCodec;

impl SnapshotCodec for DeterministicCodec {
    type Error = &'static str;

    fn encode_content(&self, content: &RuntimeSnapshotContent) -> Result<Vec<u8>, Self::Error> {
        Ok(format!(
            "snapshot:{}:{}:{}:{}",
            content.schema_version.as_str(),
            content.tools.len(),
            content.capabilities.len(),
            content.skills.len(),
        )
        .into_bytes())
    }

    fn decode_content(&self, _bytes: &[u8]) -> Result<RuntimeSnapshotContent, Self::Error> {
        Err("unused in seal test")
    }
}

fn provider_id() -> CapabilityProviderId {
    host_capability_provider_id(
        &uuid::Uuid::NAMESPACE_OID,
        "example.test/capability-provider/inventory.v1",
    )
    .expect("deterministic provider id")
}

fn sample_input_schema() -> Vec<u8> {
    br#"{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}"#.to_vec()
}

fn sample_output_schema() -> Vec<u8> {
    br#"{"type":"object","properties":{"unit_price":{"type":"integer"}},"required":["unit_price"],"additionalProperties":false}"#.to_vec()
}

fn sample_read_tool(key: &str, method: &str) -> HostToolDescriptor {
    HostToolDescriptor::new(
        key,
        "Look up live inventory pricing by SKU.",
        sample_input_schema(),
        sample_output_schema(),
        Effect::Read,
        ApprovalRequirement::Never,
        method,
    )
}

fn content_ref(id: &str, media_type: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::from_str(id).expect("valid content id"),
        media_type: MediaType::new(media_type).expect("valid media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn sample_host_skill(
    slot: u8,
    skill_ref: &str,
    tenant_id: TenantId,
    provenance: HostSkillProvenance,
) -> HostSkillDescriptor {
    let instructions = format!("# {skill_ref}\nFollow neutral host instructions.");
    let manifest =
        format!(r#"{{"schema_version":"musteragent.skill-package.v2","key":"{skill_ref}"}}"#);
    let package = format!("PK-archive-{skill_ref}");
    HostSkillDescriptor::new(
        SkillId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("example.test/skill-id/{slot}/{skill_ref}").as_bytes(),
        ))
        .expect("skill id"),
        SkillVersionId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("example.test/skill-ver/{slot}/{skill_ref}").as_bytes(),
        ))
        .expect("skill version id"),
        skill_ref,
        format!("Host Skill {slot}"),
        format!("Neutral guidance for {skill_ref}."),
        format!("Use when handling workflow {slot}."),
        tenant_id,
        provenance,
        ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("example.test/skill-instr/{slot}/{skill_ref}").as_bytes(),
            ))
            .expect("instructions content id"),
            media_type: MediaType::new("text/markdown;charset=utf-8").expect("media type"),
            byte_length: instructions.len() as u64,
            sha256: Sha256::digest(instructions.as_bytes()),
        },
        ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("example.test/skill-manifest/{slot}/{skill_ref}").as_bytes(),
            ))
            .expect("manifest content id"),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: manifest.len() as u64,
            sha256: Sha256::digest(manifest.as_bytes()),
        },
        ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("example.test/skill-pkg/{slot}/{skill_ref}").as_bytes(),
            ))
            .expect("package content id"),
            media_type: MediaType::new("application/zip").expect("media type"),
            byte_length: package.len() as u64,
            sha256: Sha256::digest(package.as_bytes()),
        },
    )
}

fn assemble_snapshot_from_partitions(
    partitions: HostCatalogSnapshotPartitions,
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
        base_skill_refs: partitions.base_skill_refs(),
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
    execution_policy.liveness = Some(RunLivenessPolicyV1::production_default());

    let mut content = RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
        agent: AgentVersionSnapshot::from_resolved(&agent_def, &agent_ver).expect("agent"),
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
        tools: Vec::new(),
        capabilities: Vec::new(),
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
                .expect("material")
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
    };
    partitions.apply_to(&mut content);
    content
}

#[test]
fn seals_snapshot_built_exclusively_from_helper_and_passes_worker_validation() {
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let run_id = RunId::generate();

    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let metadata = product_release_metadata(&kernel_release).expect("kernel metadata");
    let contract: serde_json::Value =
        serde_json::from_str(include_str!("../../../../contracts/kernel-tools.json"))
            .expect("kernel tools json");
    let baseline: BTreeSet<&str> = F4_CLOUD_BASELINE_TOOLS.into_iter().collect();
    let subset: Vec<serde_json::Value> = contract["products"]["musteragent"]["cloud"]["tools"]
        .as_array()
        .expect("cloud tools")
        .iter()
        .filter(|tool| {
            tool["tool_key"]
                .as_str()
                .and_then(|k| k.split_once('@'))
                .is_some_and(|(logical, _)| baseline.contains(logical))
        })
        .cloned()
        .collect();
    let host_doc = serde_json::json!({
        "schema_version": "musteragent.host-tool-release.v1",
        "product": "k3-helper-host",
        "release_id": "k3-helper-host.tools@1",
        "profile_id": "k3-helper-host",
        "cloud_head": "k3.cloud@1",
        "device_head": "k3.device@1",
        "skill_schema": metadata.skill_schema,
        "skill_release": {
            "product": kernel_release.product.as_str(),
            "release_id": kernel_release.release_id.as_str(),
            "digest": kernel_release.digest
        },
        "cloud": subset,
        "device": subset
    });
    let host_pub =
        HostProductRelease::from_json(&serde_json::to_vec(&host_doc).expect("json")).expect("pub");
    let host_release = host_pub.reference().clone();
    install_host_product_releases(vec![host_pub]).expect("install host release");

    let secondary_provider = host_capability_provider_id(
        &uuid::Uuid::NAMESPACE_OID,
        "example.test/capability-provider/fulfillment.v1",
    )
    .expect("secondary provider");

    for release in [kernel_release.clone(), host_release] {
        let mut builder = HostCatalogSnapshotBuilder::new(provider_id())
            .with_product_release(release.clone())
            .with_host_tools([
                sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuote")
                    .with_title("Lookup Inventory Quote")
                    .with_when_to_use("Use when quoting live SKU pricing.")
                    .with_tags(["pricing", "warehouse"])
                    .with_scopes(["inventory.quote.read"]),
                HostToolDescriptor::new(
                    "inventory.reservation.create@1",
                    "Reserve inventory units for an order.",
                    sample_input_schema(),
                    br#"{"type":"object","properties":{"reservation_id":{"type":"string"}},"required":["reservation_id"],"additionalProperties":false}"#.to_vec(),
                    Effect::WriteExternal,
                    ApprovalRequirement::ByPolicy,
                    "InventoryService.CreateReservation",
                )
                .with_provider_id(secondary_provider)
                .with_policy_decision(Decision::RequireApproval),
            ])
            .with_host_skills([
                sample_host_skill(
                    1,
                    "inventory-hub-guide@1",
                    tenant_id,
                    HostSkillProvenance::hub("https://hub.example.test/skills/inv@1", "example.test"),
                )
                .with_tags(["warehouse_ops", "仓库盘点"])
                .with_required_capabilities(["inventory.quote.lookup@1"]),
                sample_host_skill(
                    2,
                    "inventory-url-guide@1",
                    tenant_id,
                    HostSkillProvenance::url("https://example.test/skills/inv-url@1", "example.test"),
                ),
                sample_host_skill(
                    3,
                    "inventory-git-guide@1",
                    tenant_id,
                    HostSkillProvenance::github("https://example.test/git/inv@1", "example.test"),
                ),
                sample_host_skill(
                    4,
                    "inventory-local-guide@1",
                    tenant_id,
                    HostSkillProvenance::custom(principal_id),
                ),
            ])
            .with_initial_exposed_tools(["inventory.quote.lookup@1"])
            .with_explicitly_selected_skills(["inventory-hub-guide@1"]);

        if release == kernel_release {
            builder = builder.with_builtin_tools(["skills.resource.read@2"]);
        }

        let partitions = builder.build().expect("valid helper partitions");
        let entitlements = partitions.capability_entitlements();
        let content = assemble_snapshot_from_partitions(partitions);

        // Admission sealing validation (`Conformance::Admission`).
        let sealed = RuntimeSnapshot::seal(
            RuntimeSnapshotId::generate(),
            content,
            now,
            &DeterministicCodec,
        )
        .expect("helper-built snapshot seals cleanly");

        // Worker runtime validation (`require_product_release_admission` + `Conformance::Sealed` + skill catalog).
        require_product_release_admission(
            sealed
                .content
                .product_release
                .as_ref()
                .expect("product release"),
        )
        .expect("product release admitted by worker");
        sealed
            .content
            .validate_sealed()
            .expect("sealed worker validation passes");
        let skill_catalog = sealed
            .content
            .frozen_authorized_skill_catalog()
            .expect("frozen skill catalog validates");
        assert!(skill_catalog.executable("inventory-hub-guide@1").is_ok());
        assert!(skill_catalog.executable("inventory-url-guide@1").is_ok());
        assert!(skill_catalog.executable("inventory-git-guide@1").is_ok());
        assert!(skill_catalog.executable("inventory-local-guide@1").is_ok());
        let hub_skill = skill_catalog
            .get("inventory-hub-guide@1")
            .expect("hub skill descriptor");
        assert_eq!(
            hub_skill.description,
            "Neutral guidance for inventory-hub-guide@1."
        );
        assert_eq!(
            hub_skill.tags,
            vec!["warehouse_ops".to_owned(), "仓库盘点".to_owned()]
        );
        let quote_tool = sealed
            .content
            .tools
            .iter()
            .find(|t| t.manifest.key.as_str() == "inventory.quote.lookup@1")
            .expect("quote tool snapshot");
        assert_eq!(
            quote_tool.description, "Look up live inventory pricing by SKU.",
            "description must remain unmutated without appended When to use / Tags suffixes"
        );
        assert_eq!(
            quote_tool.when_to_use.as_deref(),
            Some("Use when quoting live SKU pricing.")
        );
        assert_eq!(
            quote_tool.tags,
            vec!["pricing".to_owned(), "warehouse".to_owned()]
        );
        let reservation_tool = sealed
            .content
            .tools
            .iter()
            .find(|t| t.manifest.key.as_str() == "inventory.reservation.create@1")
            .expect("reservation tool snapshot");
        assert!(reservation_tool.tags.is_empty());
        assert!(reservation_tool.when_to_use.is_none());

        let grant = ExecutionGrant {
            reviewed_application_source_sha256: Vec::new(),
            product_release: sealed.content.product_release.clone(),
            schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
            grant_ref: sealed.content.model.credential_grant_ref.clone(),
            issuer: DottedKey::new("musteragent.host.admission").expect("issuer"),
            audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
            tenant_id,
            model_provider_id: sealed.content.model.route.model_provider_id,
            model_entry_id: sealed.content.model.route.model_entry_id,
            run_id,
            issued_at: now,
            expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 600_000),
            nonce: OpaqueExternalRef::new(format!("grant_{run_id}")).expect("nonce"),
            requested_model_class: sealed.content.model.requested_model_class.clone(),
            model_route: sealed.content.model.route.clone(),
            fallback_model_routes: Vec::new(),
            auxiliary_model_routes: Vec::new(),
            agent_contract: Some(
                sealed
                    .content
                    .agent
                    .resolved_v5
                    .as_ref()
                    .expect("resolved v5")
                    .supported_runtime_contract
                    .clone(),
            ),
            surface: Some(DottedKey::new("api").expect("surface")),
            capability_entitlements: entitlements,
            tool_contract_releases: Vec::new(),
            identity_binding: Some(ExecutionGrantIdentityBinding {
                principal_id,
                authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
                device_id: None,
                device_workspace_id: None,
            }),
            permission_mode: Some(sealed.content.policy.permission_mode),
            device_direct_provider: None,
            vision_direct_provider: None,
        };
        grant
            .validate()
            .expect("ExecutionGrant over helper entitlements validates");
    }
}

#[test]
fn deferred_exposure_grants_all_tools_exposes_only_initial_set_and_supports_search_and_activate() {
    let partitions = HostCatalogSnapshotBuilder::new(provider_id())
        .with_host_tools([
            sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuote"),
            sample_read_tool(
                "inventory.tariff.estimate@1",
                "InventoryService.EstimateTariff",
            )
            .with_when_to_use("Use when computing cross-border customs duties.")
            .with_tags(["customs", "harmonized_code"]),
            HostToolDescriptor::new(
                "inventory.shelf.adjust@1",
                "Adjust warehouse shelf count.",
                sample_input_schema(),
                sample_output_schema(),
                Effect::WriteReversible,
                ApprovalRequirement::ByPolicy,
                "InventoryService.AdjustShelf",
            ),
        ])
        .with_initial_exposed_tools(["inventory.quote.lookup@1"])
        .build()
        .expect("build partitions");

    // Every host tool is authorized (granted) with CapabilityLocation::Backend.
    for expected_key in [
        "inventory.quote.lookup@1",
        "inventory.tariff.estimate@1",
        "inventory.shelf.adjust@1",
    ] {
        assert!(
            partitions.capabilities.iter().any(|g| {
                g.key.as_str() == expected_key
                    && g.location == CapabilityLocation::Backend
                    && g.provider_id == provider_id()
                    && g.policy_decision == Decision::Allow
            }),
            "expected backend grant for {expected_key}"
        );
        assert!(
            partitions
                .tools
                .iter()
                .any(|t| t.manifest.key.as_str() == expected_key)
        );
    }

    let tariff_snapshot = partitions
        .tools
        .iter()
        .find(|t| t.manifest.key.as_str() == "inventory.tariff.estimate@1")
        .expect("tariff tool snapshot");
    assert_eq!(
        tariff_snapshot.description,
        "Look up live inventory pricing by SKU."
    );
    assert_eq!(
        tariff_snapshot.tags,
        vec!["customs".to_owned(), "harmonized_code".to_owned()]
    );
    assert_eq!(
        tariff_snapshot.when_to_use.as_deref(),
        Some("Use when computing cross-border customs duties.")
    );

    // Only bootstrap tools (8) + `inventory.quote.lookup@1` are exposed initially.
    assert_eq!(partitions.discovery.bootstrap_tool_keys.len(), 8);
    assert_eq!(partitions.discovery.initial_exposed_tool_keys.len(), 9);
    assert!(
        partitions
            .discovery
            .initial_exposed_tool_keys
            .iter()
            .any(|k| k.as_str() == "inventory.quote.lookup@1")
    );
    assert!(
        !partitions
            .discovery
            .initial_exposed_tool_keys
            .iter()
            .any(|k| k.as_str() == "inventory.tariff.estimate@1")
    );
    assert!(
        !partitions
            .discovery
            .initial_exposed_tool_keys
            .iter()
            .any(|k| k.as_str() == "inventory.shelf.adjust@1")
    );

    let catalog = authorized_tool_catalog(&partitions.tools, &partitions.capabilities)
        .expect("authorized tool catalog");
    let mut exposure =
        ExposureState::from_initial(&catalog, &partitions.discovery.initial_exposed_tool_keys)
            .expect("initial exposure state");
    assert!(exposure.contains_key("inventory.quote.lookup@1"));
    assert!(!exposure.contains_key("inventory.tariff.estimate@1"));

    // Deferred tool is discoverable via tag-only and when_to_use-only queries in `tool.catalog.search`.
    let by_tag = catalog
        .search(&ToolCatalogSearchRequest {
            query: "harmonized_code".to_owned(),
            locations: BTreeSet::from([CapabilityLocation::Backend]),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("search by tag");
    assert_eq!(by_tag.items.len(), 1);
    assert_eq!(
        by_tag.items[0].tool_ref.as_str(),
        "inventory.tariff.estimate@1"
    );
    assert_eq!(
        by_tag.items[0].description,
        "Look up live inventory pricing by SKU."
    );

    let by_when_to_use = catalog
        .search(&ToolCatalogSearchRequest {
            query: "cross-border customs duties".to_owned(),
            locations: BTreeSet::from([CapabilityLocation::Backend]),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("search by when_to_use");
    assert_eq!(
        by_when_to_use.items[0].tool_ref.as_str(),
        "inventory.tariff.estimate@1"
    );

    // Activating the deferred tool transitions it into ExposureState.
    let deferred_key = ToolKey::from_versioned_key(
        VersionedKey::new("inventory.tariff.estimate@1").expect("versioned key"),
    );
    let outcome = catalog
        .activate(
            &exposure,
            ActivationRequest {
                tool_refs: vec![deferred_key],
                reason_ref: content_ref(
                    "01982f80-0000-7000-8000-000000000099",
                    "text/plain;charset=utf-8",
                    b"activate tariff estimator",
                ),
                source_invocation_id: InvocationId::generate(),
                exposed_at: UtcTimestamp::from_unix_millis(1_790_874_001_000),
            },
            &ActivationConstraints {
                available_locations: BTreeSet::from([
                    CapabilityLocation::Cloud,
                    CapabilityLocation::Backend,
                ]),
                available_contexts: BTreeSet::new(),
                max_exposed_tools: partitions.discovery.max_exposed_tools,
            },
        )
        .expect("activate deferred tool");
    let event = outcome.event.expect("exposure event emitted");
    exposure
        .apply(&catalog, &event)
        .expect("apply exposure event");
    assert!(exposure.contains_key("inventory.tariff.estimate@1"));
}

#[test]
fn builder_populates_native_discovery_metadata_and_ranks_via_k7_field_weights() {
    let tenant_id = TenantId::generate();
    let partitions = HostCatalogSnapshotBuilder::new(provider_id())
        .with_host_tools([
            HostToolDescriptor::new(
                "library.a_desc.helper@1",
                "Supports 馆藏检索 queries across reading rooms.",
                sample_input_schema(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "LibraryService.AlphaHelper",
            )
            .with_title("Alpha Helper"),
            HostToolDescriptor::new(
                "library.b_when.helper@1",
                "General reading room helper.",
                sample_input_schema(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "LibraryService.BetaHelper",
            )
            .with_title("Beta Helper")
            .with_when_to_use("当用户询问馆藏检索方案时调用"),
            HostToolDescriptor::new(
                "library.c_tags.helper@1",
                "Supports 馆藏检索 in description too.",
                sample_input_schema(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "LibraryService.GammaHelper",
            )
            .with_title("Gamma Helper")
            .with_tags(["馆藏检索", "meteorology_radar"])
            .with_when_to_use("也可用于馆藏检索"),
            HostToolDescriptor::new(
                "library.z_name.helper@1",
                "General reading room helper.",
                sample_input_schema(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "LibraryService.ZetaHelper",
            )
            .with_title("馆藏检索工具"),
        ])
        .with_host_skills([
            HostSkillDescriptor::new(
                SkillId::generate(),
                SkillVersionId::generate(),
                "library-a-desc@1",
                "Routine A",
                "Formats 馆藏编目 bibliography entries.",
                "Use for periodic library review.",
                tenant_id,
                HostSkillProvenance::hub("https://hub.example.test/skills/lib-a@1", "example.test"),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b1",
                    "text/markdown;charset=utf-8",
                    b"# Routine A",
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b2",
                    "application/json",
                    br#"{"schema_version":"musteragent.skill-package.v2","k":"a"}"#,
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b3",
                    "application/zip",
                    b"PK-lib-a",
                ),
            ),
            HostSkillDescriptor::new(
                SkillId::generate(),
                SkillVersionId::generate(),
                "library-b-when@1",
                "Routine B",
                "Formats citation entries.",
                "当用户需要处理馆藏编目核对时使用。",
                tenant_id,
                HostSkillProvenance::hub("https://hub.example.test/skills/lib-b@1", "example.test"),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b4",
                    "text/markdown;charset=utf-8",
                    b"# Routine B",
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b5",
                    "application/json",
                    br#"{"schema_version":"musteragent.skill-package.v2","k":"b"}"#,
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b6",
                    "application/zip",
                    b"PK-lib-b",
                ),
            ),
            HostSkillDescriptor::new(
                SkillId::generate(),
                SkillVersionId::generate(),
                "library-c-tags@1",
                "Routine C",
                "Formats 馆藏编目 citation entries.",
                "当用户需要处理馆藏编目核对时使用。",
                tenant_id,
                HostSkillProvenance::hub("https://hub.example.test/skills/lib-c@1", "example.test"),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b7",
                    "text/markdown;charset=utf-8",
                    b"# Routine C",
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b8",
                    "application/json",
                    br#"{"schema_version":"musteragent.skill-package.v2","k":"c"}"#,
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000b9",
                    "application/zip",
                    b"PK-lib-c",
                ),
            )
            .with_tags(["馆藏编目", "古籍修复"]),
            HostSkillDescriptor::new(
                SkillId::generate(),
                SkillVersionId::generate(),
                "library-z-name@1",
                "馆藏编目指南",
                "Formats citation entries.",
                "Use for periodic library review.",
                tenant_id,
                HostSkillProvenance::hub("https://hub.example.test/skills/lib-z@1", "example.test"),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000ba",
                    "text/markdown;charset=utf-8",
                    b"# Routine Z",
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000bb",
                    "application/json",
                    br#"{"schema_version":"musteragent.skill-package.v2","k":"z"}"#,
                ),
                content_ref(
                    "01982f80-0000-7000-8000-0000000000bc",
                    "application/zip",
                    b"PK-lib-z",
                ),
            ),
        ])
        .build()
        .expect("build weighted metadata partitions");

    // All four host tools are deferred (not in initial_exposed_tool_keys).
    let tool_catalog = authorized_tool_catalog(&partitions.tools, &partitions.capabilities)
        .expect("authorized tool catalog");
    let exposure = ExposureState::from_initial(
        &tool_catalog,
        &partitions.discovery.initial_exposed_tool_keys,
    )
    .expect("initial exposure");
    assert!(!exposure.contains_key("library.c_tags.helper@1"));

    // Deferred tool is found by a tag-only query ("meteorology_radar" appears ONLY in tags).
    let tag_only_hit = tool_catalog
        .search(&ToolCatalogSearchRequest {
            query: "meteorology_radar".to_owned(),
            locations: BTreeSet::from([CapabilityLocation::Backend]),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("tag-only tool search");
    assert_eq!(
        tag_only_hit
            .items
            .iter()
            .map(|i| i.tool_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["library.c_tags.helper@1"]
    );

    // K7 weighted ranking (`key/name > tags > when_to_use > description`) orders z > c > b > a.
    let ranked_tools = tool_catalog
        .search(&ToolCatalogSearchRequest {
            query: "馆藏检索".to_owned(),
            locations: BTreeSet::from([CapabilityLocation::Backend]),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("weighted tool search");
    assert_eq!(
        ranked_tools
            .items
            .iter()
            .map(|i| i.tool_ref.as_str())
            .collect::<Vec<_>>(),
        vec![
            "library.z_name.helper@1",
            "library.c_tags.helper@1",
            "library.b_when.helper@1",
            "library.a_desc.helper@1",
        ]
    );

    // Host skill tags are searchable by tag-only query and ranked with K7 field weights.
    let skill_catalog =
        AuthorizedSkillCatalog::new(partitions.discovery.authorized_skills).expect("skill catalog");
    let skill_tag_only = skill_catalog
        .search(&SkillCatalogSearchRequest {
            query: "古籍修复".to_owned(),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("tag-only skill search");
    assert_eq!(
        skill_tag_only
            .items
            .iter()
            .map(|i| i.skill_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["library-c-tags@1"]
    );

    let ranked_skills = skill_catalog
        .search(&SkillCatalogSearchRequest {
            query: "馆藏编目".to_owned(),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("weighted skill search");
    assert_eq!(
        ranked_skills
            .items
            .iter()
            .map(|i| i.skill_ref.as_str())
            .collect::<Vec<_>>(),
        vec![
            "library-z-name@1",
            "library-c-tags@1",
            "library-b-when@1",
            "library-a-desc@1",
        ]
    );
}

#[test]
fn rejects_nil_empty_and_builtin_provider_ids() {
    assert_eq!(
        CapabilityProviderId::from_uuid(uuid::Uuid::nil()),
        Err(IdentityError::Nil {
            type_name: "CapabilityProviderId"
        })
    );
    assert_eq!(
        host_capability_provider_id(&uuid::Uuid::NAMESPACE_OID, "   "),
        Err(HostCatalogBuildError::EmptyProviderName)
    );

    for raw_builtin in [BUILTIN_RUST_PROVIDER_ID, BUILTIN_BACKEND_PROVIDER_ID] {
        let builtin_id: CapabilityProviderId = raw_builtin.parse().expect("builtin provider");
        assert_eq!(
            HostCatalogSnapshotBuilder::new(builtin_id).build(),
            Err(HostCatalogBuildError::BuiltinProviderId {
                provider_id: builtin_id
            })
        );
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([sample_read_tool(
                    "inventory.quote.lookup@1",
                    "InventoryService.LookupQuote"
                )
                .with_provider_id(builtin_id),])
                .build(),
            Err(HostCatalogBuildError::BuiltinProviderId {
                provider_id: builtin_id
            })
        );
    }
}

#[test]
fn rejects_unknown_and_malformed_keys_in_initial_bootstrap_and_builtin_sets() {
    assert!(matches!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_initial_exposed_tools(["not-versioned"])
            .build(),
        Err(HostCatalogBuildError::InvalidInitialExposureKey { key, .. }) if key == "not-versioned"
    ));

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )])
            .with_initial_exposed_tools(["inventory.missing.tool@1"])
            .build(),
        Err(HostCatalogBuildError::UnknownInitialExposureKey {
            key: "inventory.missing.tool@1".to_owned()
        })
    );

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_builtin_tools(["nonexistent.kernel.tool@1"])
            .build(),
        Err(HostCatalogBuildError::UnknownBuiltinToolKey {
            key: "nonexistent.kernel.tool@1".to_owned()
        })
    );

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_bootstrap_tool_keys(["unknown.bootstrap@1"])
            .build(),
        Err(HostCatalogBuildError::UnknownBootstrapToolKey {
            key: "unknown.bootstrap@1".to_owned()
        })
    );

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_bootstrap_tool_keys(["tool.catalog.search@2"])
            .build(),
        Err(HostCatalogBuildError::InvalidBootstrapToolSet)
    );
}

#[test]
fn rejects_duplicates_never_silently_dropping_entries() {
    // Duplicate exact host tool key.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([
                sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuote"),
                sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuoteV2"),
            ])
            .build(),
        Err(HostCatalogBuildError::DuplicateToolKey {
            key: "inventory.quote.lookup@1".to_owned()
        })
    );

    // Duplicate logical key across versions.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([
                sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuote"),
                sample_read_tool("inventory.quote.lookup@2", "InventoryService.LookupQuoteV2"),
            ])
            .build(),
        Err(HostCatalogBuildError::DuplicateToolKey {
            key: "inventory.quote.lookup@2".to_owned()
        })
    );

    // Duplicate wire name across distinct keys.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([
                sample_read_tool("inventory.quote.lookup@1", "InventoryService.LookupQuote")
                    .with_wire_name("shared_wire_name"),
                sample_read_tool("inventory.audit.ping@1", "InventoryService.PingAudit")
                    .with_wire_name("shared_wire_name"),
            ])
            .build(),
        Err(HostCatalogBuildError::DuplicateWireName {
            key: "inventory.audit.ping@1".to_owned(),
            wire_name: "shared_wire_name".to_owned(),
        })
    );

    // Duplicate entry in initial_exposed_tools.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )])
            .with_initial_exposed_tools(["inventory.quote.lookup@1", "inventory.quote.lookup@1",])
            .build(),
        Err(HostCatalogBuildError::DuplicateInitialExposureKey {
            key: "inventory.quote.lookup@1".to_owned()
        })
    );

    // Repeating an already-bootstrapped tool in initial_exposed_tools.
    let cloud_catalog = BuiltinToolCatalog::for_product_release(
        &compiled_product_release(ProductId::KERNEL),
        ProductExecutionTarget::Cloud,
    )
    .expect("cloud catalog");
    let search_key = cloud_catalog
        .definitions()
        .iter()
        .find(|d| d.key.logical_key() == "tool.catalog.search")
        .expect("search tool")
        .key
        .as_str()
        .to_owned();
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_initial_exposed_tools([search_key.clone()])
            .build(),
        Err(HostCatalogBuildError::DuplicateInitialExposureKey { key: search_key })
    );

    // Duplicate bootstrap tool keys.
    let search_key_dup = cloud_catalog
        .definitions()
        .iter()
        .find(|d| d.key.logical_key() == "tool.catalog.search")
        .expect("search tool")
        .key
        .as_str()
        .to_owned();
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_bootstrap_tool_keys([search_key_dup.clone(), search_key_dup.clone()])
            .build(),
        Err(HostCatalogBuildError::DuplicateBootstrapToolKey {
            key: search_key_dup
        })
    );

    // Duplicate builtin tool keys.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_builtin_tools(["delegation.spawn@5", "delegation.spawn@5"])
            .build(),
        Err(HostCatalogBuildError::DuplicateBuiltinToolKey {
            key: "delegation.spawn@5".to_owned()
        })
    );

    // Duplicate tag and duplicate scope.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_tags(["pricing", "PRICING"]),])
            .build(),
        Err(HostCatalogBuildError::DuplicateToolTag {
            key: "inventory.quote.lookup@1".to_owned(),
            tag: "PRICING".to_owned(),
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_scopes(["inventory.read", "inventory.read"]),])
            .build(),
        Err(HostCatalogBuildError::DuplicateToolScope {
            key: "inventory.quote.lookup@1".to_owned(),
            scope: "inventory.read".to_owned(),
        })
    );
}

#[test]
fn rejects_collisions_with_compiled_cloud_or_device_catalogs_and_closed_namespaces() {
    let kernel_release = compiled_product_release(ProductId::KERNEL);
    let cloud_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Cloud)
            .expect("cloud catalog");
    let device_catalog =
        BuiltinToolCatalog::for_product_release(&kernel_release, ProductExecutionTarget::Device)
            .expect("device catalog");

    let cloud_backend_key = cloud_catalog
        .definitions()
        .iter()
        .find(|d| d.target.backend_operation().is_some())
        .expect("compiled cloud backend tool")
        .key
        .as_str()
        .to_owned();
    let device_only_key = device_catalog
        .definitions()
        .iter()
        .find(|d| cloud_catalog.by_key(&d.key).is_none())
        .expect("compiled device-only tool")
        .key
        .as_str()
        .to_owned();

    for colliding in [
        "system.time.current@1",
        "tool.catalog.search@99",
        cloud_backend_key.as_str(),
        device_only_key.as_str(),
    ] {
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([sample_read_tool(colliding, "InventoryService.LookupQuote")
                    .with_wire_name("non_colliding_wire"),])
                .build(),
            Err(HostCatalogBuildError::CompiledCatalogCollision {
                key: colliding.to_owned()
            }),
            "expected CompiledCatalogCollision for {colliding}"
        );
    }

    for disallowed in [
        "agenda.custom_item@1",
        "tasks.custom_run@1",
        "schedules.custom_job@1",
        "device.custom.sensor@1",
        "mcp.custom.tool@1",
        "component.custom.widget@1",
    ] {
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([sample_read_tool(disallowed, "InventoryService.LookupQuote")])
                .build(),
            Err(HostCatalogBuildError::DisallowedByDiscoveryPolicy {
                key: disallowed.to_owned()
            }),
            "expected DisallowedByDiscoveryPolicy for {disallowed}"
        );
    }
}

#[test]
fn rejects_execute_tool_and_malformed_backend_operations() {
    for bad_op in [
        "",
        "ExecuteTool",
        "InventoryService.ExecuteTool",
        "InventoryService.execute_tool",
        "InventoryService.EXECUTE_TOOL",
        "InventoryService.Execute_Tool",
        "InventoryService",
        "Inventory.Service.Lookup",
        "1Invalid.Lookup",
        "Inventory.1Invalid",
        &format!("Inventory.{}", "a".repeat(120)),
    ] {
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([sample_read_tool("inventory.quote.lookup@1", bad_op)])
                .build(),
            Err(HostCatalogBuildError::InvalidBackendOperation {
                key: "inventory.quote.lookup@1".to_owned(),
                operation: bad_op.to_owned(),
            }),
            "expected InvalidBackendOperation for {bad_op:?}"
        );
    }
}

#[test]
fn enforces_approval_floor_on_all_non_read_effects() {
    for effect in [
        Effect::WriteReversible,
        Effect::WriteExternal,
        Effect::Destructive,
    ] {
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([HostToolDescriptor::new(
                    "inventory.mutation.apply@1",
                    "Apply an inventory mutation.",
                    sample_input_schema(),
                    sample_output_schema(),
                    effect,
                    ApprovalRequirement::Never,
                    "InventoryService.ApplyMutation",
                )])
                .build(),
            Err(HostCatalogBuildError::MissingApprovalFloor {
                key: "inventory.mutation.apply@1".to_owned(),
                effect,
            }),
            "non-read effect {effect:?} with ApprovalRequirement::Never must fail"
        );

        for approval in [ApprovalRequirement::ByPolicy, ApprovalRequirement::Always] {
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_tools([HostToolDescriptor::new(
                    "inventory.mutation.apply@1",
                    "Apply an inventory mutation.",
                    sample_input_schema(),
                    sample_output_schema(),
                    effect,
                    approval,
                    "InventoryService.ApplyMutation",
                )])
                .build()
                .expect("non-read effect with approval floor succeeds");
        }
    }
}

#[test]
fn rejects_empty_metadata_invalid_schemas_and_incoherent_contracts() {
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "   ",
                sample_input_schema(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )])
            .build(),
        Err(HostCatalogBuildError::EmptyToolMetadata {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "description",
        })
    );

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_when_to_use("  "),])
            .build(),
        Err(HostCatalogBuildError::EmptyToolMetadata {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "when_to_use",
        })
    );

    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_tags(["valid", ""]),])
            .build(),
        Err(HostCatalogBuildError::EmptyToolMetadata {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "tags",
        })
    );

    // K7 bounds on tool tags (<= 64 tags, <= 256 bytes per tag) and when_to_use (<= 4000 bytes).
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_tags((0..65).map(|i| format!("tag_{i}")),),])
            .build(),
        Err(HostCatalogBuildError::TooManyToolTags {
            key: "inventory.quote.lookup@1".to_owned(),
            count: 65,
            max_tags: 64,
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_tags(["x".repeat(257)]),])
            .build(),
        Err(HostCatalogBuildError::ToolMetadataTooLong {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "tags",
            max_bytes: 256,
            actual_bytes: 257,
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_when_to_use("w".repeat(4_001)),])
            .build(),
        Err(HostCatalogBuildError::ToolMetadataTooLong {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "when_to_use",
            max_bytes: 4_000,
            actual_bytes: 4_001,
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_title("t".repeat(513)),])
            .build(),
        Err(HostCatalogBuildError::ToolMetadataTooLong {
            key: "inventory.quote.lookup@1".to_owned(),
            field: "title",
            max_bytes: 512,
            actual_bytes: 513,
        })
    );

    // K7 bounds and duplicate checks on host skill tags and when_to_use.
    let skill_tenant = TenantId::generate();
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([sample_host_skill(
                10,
                "inventory-tag-empty@1",
                skill_tenant,
                HostSkillProvenance::hub("https://hub.example.test/skills/e@1", "example.test"),
            )
            .with_tags(["valid", "   "]),])
            .build(),
        Err(HostCatalogBuildError::EmptySkillMetadata {
            skill_ref: "inventory-tag-empty@1".to_owned(),
            field: "tags",
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([sample_host_skill(
                11,
                "inventory-tag-dup@1",
                skill_tenant,
                HostSkillProvenance::hub("https://hub.example.test/skills/d@1", "example.test"),
            )
            .with_tags(["archival", "ARCHIVAL"]),])
            .build(),
        Err(HostCatalogBuildError::DuplicateSkillTag {
            skill_ref: "inventory-tag-dup@1".to_owned(),
            tag: "ARCHIVAL".to_owned(),
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([sample_host_skill(
                12,
                "inventory-tag-count@1",
                skill_tenant,
                HostSkillProvenance::hub("https://hub.example.test/skills/c@1", "example.test"),
            )
            .with_tags((0..65).map(|i| format!("skill_tag_{i}"))),])
            .build(),
        Err(HostCatalogBuildError::TooManySkillTags {
            skill_ref: "inventory-tag-count@1".to_owned(),
            count: 65,
            max_tags: 64,
        })
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([sample_host_skill(
                13,
                "inventory-tag-len@1",
                skill_tenant,
                HostSkillProvenance::hub("https://hub.example.test/skills/l@1", "example.test"),
            )
            .with_tags(["s".repeat(257)]),])
            .build(),
        Err(HostCatalogBuildError::SkillMetadataTooLong {
            skill_ref: "inventory-tag-len@1".to_owned(),
            field: "tags",
            max_bytes: 256,
            actual_bytes: 257,
        })
    );

    assert!(matches!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "Look up quote.",
                b"[]".to_vec(),
                sample_output_schema(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )])
            .build(),
        Err(HostCatalogBuildError::InvalidToolSchema {
            direction: "input",
            ..
        })
    ));

    assert!(matches!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([HostToolDescriptor::new(
                "inventory.quote.lookup@1",
                "Look up quote.",
                sample_input_schema(),
                b"{not-json".to_vec(),
                Effect::Read,
                ApprovalRequirement::Never,
                "InventoryService.LookupQuote",
            )])
            .build(),
        Err(HostCatalogBuildError::InvalidToolSchema {
            direction: "output",
            ..
        })
    ));

    // Media artifact requires WriteExternal + ProviderTaskRecord.
    assert!(matches!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.diagram.render@1",
                "InventoryService.RenderDiagram"
            )
            .with_media_artifact(MediaArtifactKind::Image),])
            .build(),
        Err(HostCatalogBuildError::InvalidToolContract { .. })
    ));

    // Timeout == 0 rejected.
    assert!(matches!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )
            .with_timeout(Duration::ZERO),])
            .build(),
        Err(HostCatalogBuildError::InvalidToolContract { .. })
    ));

    // Exposure limit exceeded.
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_tools([sample_read_tool(
                "inventory.quote.lookup@1",
                "InventoryService.LookupQuote"
            )])
            .with_initial_exposed_tools(["inventory.quote.lookup@1"])
            .with_max_exposed_tools(NonZeroU32::new(8).expect("8"))
            .build(),
        Err(HostCatalogBuildError::ExposureLimitExceeded {
            requested: 9,
            max_exposed_tools: 8,
        })
    );
}

#[test]
fn rejects_marketplace_skill_source_and_validates_skill_contracts() {
    let tenant_id = TenantId::generate();
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);

    // 1. Direct `source = "marketplace"` (and case variants) rejected.
    for raw_source in ["marketplace", "Marketplace", " MARKETPLACE "] {
        let market_skill = sample_host_skill(
            1,
            "inventory-market-skill@1",
            tenant_id,
            HostSkillProvenance::installed(
                raw_source,
                "https://market.example.test/skills/inv@1",
                "example.test",
                HostSkillTrust::Verified,
            ),
        );
        assert_eq!(
            HostCatalogSnapshotBuilder::new(provider_id())
                .with_host_skills([market_skill])
                .build(),
            Err(HostCatalogBuildError::MarketplaceSkillSourceUnsupported {
                skill_ref: "inventory-market-skill@1".to_owned()
            })
        );
    }

    // 2. `HostSkillDescriptor::from_catalog_entry` rejects `InstalledSkillSource::Marketplace`
    // and accepts `InstalledSkillSource::Hub`.
    let make_catalog_entry = |source: InstalledSkillSource| {
        let skill_id = SkillId::generate();
        let identity = SkillIdentity {
            skill_id,
            skill_key: SkillKey::new("inventory-entry-skill").expect("skill key"),
            provenance: SkillProvenance::Installed {
                source,
                source_ref: "https://hub.example.test/skills/entry@1".to_owned(),
                publisher_ref: "example.test".to_owned(),
            },
            trust: SkillTrust::Community,
            created_at: now,
        };
        let instructions_ref = content_ref(
            "01982f80-0000-7000-8000-0000000000a1",
            "text/markdown;charset=utf-8",
            b"# Skill\nInstructions.",
        );
        let manifest_ref = content_ref(
            "01982f80-0000-7000-8000-0000000000a2",
            "application/json",
            br#"{"schema_version":"musteragent.skill-package.v2"}"#,
        );
        let package_ref = content_ref(
            "01982f80-0000-7000-8000-0000000000a3",
            "application/zip",
            b"PK-entry-archive",
        );
        let version = SkillVersion {
            skill_version_id: SkillVersionId::generate(),
            skill_id,
            version: VersionNumber::new(1).expect("v1"),
            skill_ref: VersionedKey::new("inventory-entry-skill@1").expect("skill ref"),
            title: "Entry Skill".to_owned(),
            description: "Skill converted from SkillCatalogEntry.".to_owned(),
            when_to_use: "Use when testing catalog entry conversion.".to_owned(),
            declared_version: None,
            source_format: SkillSourceFormat::GenericSkillFolderV1,
            instructions_hash: instructions_ref.sha256,
            instructions_ref,
            package_hash: package_ref.sha256,
            package_ref,
            manifest_hash: manifest_ref.sha256,
            manifest_ref,
            payload_hash: Sha256::digest(b"entry-payload"),
            package_dependencies: BTreeSet::new(),
            required_tools: BTreeSet::new(),
            optional_tools: BTreeSet::new(),
            compatible_agent_contracts: BTreeSet::from([VersionedKey::new(
                "musteragent.runtime-contract@1",
            )
            .expect("contract")]),
            compatible_surfaces: BTreeSet::from([Surface::Api]),
            audit_digest: Sha256::digest(b"entry-audit"),
            created_at: now,
            supersedes: None,
        };
        version
            .as_active_catalog_entry(&identity, tenant_id)
            .expect("active catalog entry")
    };

    let market_entry = make_catalog_entry(InstalledSkillSource::Marketplace);
    assert_eq!(
        HostSkillDescriptor::from_catalog_entry(&market_entry),
        Err(HostCatalogBuildError::MarketplaceSkillSourceUnsupported {
            skill_ref: "inventory-entry-skill@1".to_owned()
        })
    );

    let hub_entry = make_catalog_entry(InstalledSkillSource::Hub);
    let hub_desc = HostSkillDescriptor::from_catalog_entry(&hub_entry).expect("hub entry converts");
    let partitions = HostCatalogSnapshotBuilder::new(provider_id())
        .with_host_skills([hub_desc])
        .build()
        .expect("hub skill builds");
    let catalog =
        AuthorizedSkillCatalog::new(partitions.discovery.authorized_skills).expect("skill catalog");
    let found = catalog
        .search(&SkillCatalogSearchRequest {
            query: "catalog entry conversion".to_owned(),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("skill search");
    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].skill_ref.as_str(), "inventory-entry-skill@1");

    // 3. Unknown source string rejected.
    let invalid_source = sample_host_skill(
        5,
        "inventory-s3-skill@1",
        tenant_id,
        HostSkillProvenance::installed(
            "s3",
            "https://example.test/s3",
            "example.test",
            HostSkillTrust::Verified,
        ),
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([invalid_source])
            .build(),
        Err(HostCatalogBuildError::InvalidSkillSource {
            skill_ref: "inventory-s3-skill@1".to_owned(),
            skill_source: "s3".to_owned(),
        })
    );

    // 4. Collision with official bundled skill key rejected.
    let colliding_skill = sample_host_skill(
        6,
        "assistant-base@1",
        tenant_id,
        HostSkillProvenance::hub("https://hub.example.test/base", "example.test"),
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([colliding_skill])
            .build(),
        Err(HostCatalogBuildError::OfficialSkillCollision {
            skill_ref: "assistant-base@1".to_owned()
        })
    );

    // 5. Duplicate host skill rejected.
    let dup_a = sample_host_skill(
        7,
        "inventory-dup@1",
        tenant_id,
        HostSkillProvenance::hub("https://hub.example.test/dup", "example.test"),
    );
    let dup_b = sample_host_skill(
        8,
        "inventory-dup@1",
        tenant_id,
        HostSkillProvenance::hub("https://hub.example.test/dup2", "example.test"),
    );
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([dup_a, dup_b])
            .build(),
        Err(HostCatalogBuildError::DuplicateSkill {
            skill_ref: "inventory-dup@1".to_owned()
        })
    );

    // 6. Missing required capability on host skill fails closed.
    let unmet_cap = sample_host_skill(
        9,
        "inventory-unmet@1",
        tenant_id,
        HostSkillProvenance::hub("https://hub.example.test/unmet", "example.test"),
    )
    .with_required_capabilities(["inventory.missing.cap@1"]);
    assert_eq!(
        HostCatalogSnapshotBuilder::new(provider_id())
            .with_host_skills([unmet_cap])
            .build(),
        Err(HostCatalogBuildError::SkillCapabilityNotGranted {
            skill_ref: "inventory-unmet@1".to_owned(),
            capability: "inventory.missing.cap@1".to_owned(),
        })
    );
}
