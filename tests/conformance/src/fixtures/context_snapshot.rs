use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use super::agent::{system_definition, system_version};
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
const FIXTURE_POLICY: &str = "synthetic runtime policy";
const FIXTURE_PERSONA: &str = "synthetic persona";
use musteragent_discovery::DiscoverySnapshot;
use musteragent_identity::{CapabilityProviderId, DottedKey, RunId, VersionedKey};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelParameter, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{ApprovalRequirement, DataClassification, Effect, EgressPolicy};
use musteragent_runtime_contract::ModelClass;
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, ContextPolicySnapshot, ModelRouteSnapshot, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot,
    RunExecutionPolicySnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
    ToolDefinitionSnapshot, authorized_tool_catalog, hash_skill_catalog,
};
use musteragent_skill::{SkillAuthorizationRequest, Surface, builtin_skill_catalog};
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, ToolKey, ToolManifest,
    ToolRetryContract, WireName,
};

pub fn snapshot_content(run_id: RunId, policy_ref: ContentRef) -> RuntimeSnapshotContent {
    snapshot_content_for_route(run_id, policy_ref, openai_responses_route())
}

pub fn expose_parallel_history_tools(
    mut snapshot: RuntimeSnapshotContent,
) -> RuntimeSnapshotContent {
    let input_schema_json = br#"{"type":"object"}"#.to_vec();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    let specifications = [
        (
            "web.search@2",
            "web_search",
            "web.search@2",
            CapabilityExecutionTarget::parse("Web.Search", None)
                .expect("fixture backend operation"),
            EgressPolicy::Internet,
        ),
        (
            "system.time.current@1",
            "current_time",
            "core.time.read@1",
            CapabilityExecutionTarget::RustRuntime,
            EgressPolicy::None,
        ),
        (
            "web.fetch@2",
            "web_fetch",
            "web.fetch@2",
            CapabilityExecutionTarget::parse("Web.Fetch", None).expect("fixture backend operation"),
            EgressPolicy::Internet,
        ),
    ];
    let extra_tools: Vec<_> = specifications
        .into_iter()
        .map(
            |(tool_key, wire_name, capability_key, execution_target, egress_policy)| {
                let schema_prefix = tool_key
                    .split_once('@')
                    .map_or(tool_key, |(prefix, _)| prefix);
                let grant = snapshot
                    .capabilities
                    .iter()
                    .find(|grant| grant.key.as_str() == capability_key)
                    .expect("parallel-history Capability Grant");
                ToolDefinitionSnapshot {
                    manifest: ToolManifest {
                        application: None,
                        key: ToolKey::from_versioned_key(
                            VersionedKey::new(tool_key).expect("parallel-history Tool key"),
                        ),
                        wire_name: WireName::new(wire_name).expect("parallel-history wire name"),
                        input_schema: SchemaRef {
                            schema_key: VersionedKey::new(format!("{schema_prefix}.input@1"))
                                .expect("parallel-history input schema"),
                            schema_hash: Sha256::digest(&input_schema_json),
                        },
                        output_schema: SchemaRef {
                            schema_key: VersionedKey::new(format!("{schema_prefix}.output@1"))
                                .expect("parallel-history output schema"),
                            schema_hash: Sha256::digest(&output_schema_json),
                        },
                        capability_key: VersionedKey::new(capability_key)
                            .expect("parallel-history Capability key"),
                        capability_provider_id: grant.provider_id,
                        effect: Effect::Read,
                        approval: ApprovalRequirement::Never,
                        execution_policy: musteragent_tool::ToolExecutionPolicy::admitted_activity(
                            VersionedKey::new(tool_key).expect("current fixture"),
                            Duration::from_secs(30),
                        ),
                        max_attempts: NonZeroU32::new(2).expect("parallel-history attempts"),
                        retry_contract: ToolRetryContract::ReadOnly,
                        required_scopes: grant.allowed_scopes.clone(),
                        required_contexts: BTreeSet::new(),
                        data_classification: DataClassification::Internal,
                        egress_policy,
                    },
                    execution_target,
                    concurrency_class: ConcurrencyClass::ParallelRead,
                    idempotency: BuiltinIdempotency::Native,
                    file_operation: None,
                    title: format!("Historical {wire_name}"),
                    description: format!(
                        "Conformance Tool used to verify portable parallel history for {wire_name}."
                    ),
                    tags: Vec::new(),
                    when_to_use: None,
                    input_schema_json: input_schema_json.clone(),
                    output_schema_json: output_schema_json.clone(),
                }
            },
        )
        .collect();
    let extra_keys = extra_tools
        .iter()
        .map(|tool| tool.manifest.key.clone())
        .collect::<Vec<_>>();
    snapshot.tools.extend(extra_tools);
    snapshot
        .discovery
        .as_mut()
        .expect("current fixture")
        .initial_exposed_tool_keys
        .extend(extra_keys);
    refresh_catalogs(&mut snapshot);
    snapshot
        .validate()
        .expect("valid parallel-history Snapshot");
    snapshot
}

pub fn snapshot_content_with_discovery(
    run_id: RunId,
    policy_ref: ContentRef,
) -> RuntimeSnapshotContent {
    let mut snapshot = raw_snapshot_content_for_route(run_id, policy_ref, openai_responses_route());
    snapshot.capabilities.clear();
    pin_discovery(snapshot)
}

fn pin_discovery(mut snapshot: RuntimeSnapshotContent) -> RuntimeSnapshotContent {
    let runtime_provider = CapabilityProviderId::generate();
    let backend_provider = CapabilityProviderId::generate();
    let bootstrap = [
        "tool.catalog.search",
        "tool.catalog.activate",
        "interaction.request",
        "skills.search",
        "skills.list",
        "skills.view",
        "plan.get",
        "plan.update",
        "context.read",
        "files.list",
        "files.inspect",
        "files.read",
    ];
    let definitions = musteragent_tool::BuiltinToolCatalog::for_product_release(
        &musteragent_tool::compiled_product_release(musteragent_identity::ProductId::KERNEL),
        musteragent_tool::ProductExecutionTarget::Cloud,
    )
    .expect("kernel catalog");
    let mut grants = snapshot
        .capabilities
        .iter()
        .cloned()
        .map(|grant| {
            (
                VersionedKey::new(grant.key.as_str()).expect("current fixture"),
                grant,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut tools = Vec::new();
    for definition in definitions
        .definitions()
        .iter()
        .filter(|definition| bootstrap.contains(&definition.key.logical_key()))
    {
        let (provider_id, location) = match definition.target {
            CapabilityExecutionTarget::RustRuntime => (runtime_provider, CapabilityLocation::Cloud),
            CapabilityExecutionTarget::BackendNamed(_) => {
                (backend_provider, CapabilityLocation::Backend)
            }
        };
        let grant = CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
            provider_id,
            location,
            effect: definition.effect,
            allowed_scopes: definition.required_scopes.clone(),
            policy_decision: musteragent_policy::Decision::Allow,
        };
        if let Some(existing) = grants.insert(definition.capability_key.clone(), grant.clone()) {
            assert_eq!(existing, grant, "shared capability Grant must be canonical");
        }
        let manifest = definition.pin(provider_id);
        tools.push(ToolDefinitionSnapshot {
            manifest,
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
        });
    }
    tools.sort_by(|left, right| left.manifest.key.cmp(&right.manifest.key));
    let capabilities = grants.into_values().collect::<Vec<_>>();
    let authorized_capabilities = tools
        .iter()
        .map(|tool| tool.manifest.capability_key.clone())
        .collect::<BTreeSet<_>>();
    let skill_catalog = builtin_skill_catalog().expect("Skill catalog");
    let skills = skill_catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Desktop,
            agent_contract: VersionedKey::new("musteragent.runtime-contract@1")
                .expect("Agent contract"),
            authorized_capabilities: authorized_capabilities.clone(),
        })
        .expect("authorize discovery Skills")
        .into_iter()
        .filter(|skill| {
            skill
                .required_capabilities
                .is_subset(&authorized_capabilities)
        })
        .collect::<Vec<_>>();
    let selection = skill_catalog
        .select_initial_disclosures(
            &skills,
            &musteragent_skill::SkillDisclosureRequest {
                explicitly_selected_skill_refs: BTreeSet::new(),
            },
        )
        .expect("select initial Skill disclosures");
    let bootstrap_tool_keys = bootstrap
        .into_iter()
        .map(|key| {
            tools
                .iter()
                .find(|tool| tool.manifest.key.logical_key() == key)
                .expect("required kernel bootstrap Tool")
                .manifest
                .key
                .clone()
        })
        .collect::<Vec<_>>();
    let authorized = authorized_tool_catalog(&tools, &capabilities).expect("authorized Tools");
    snapshot.schema_version = DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("Snapshot schema");
    snapshot.protocol_versions.snapshot_schema = snapshot.schema_version.clone();
    snapshot.skills = skills;
    snapshot.tools = tools;
    snapshot.capabilities = capabilities;
    snapshot.discovery = Some(DiscoverySnapshot {
        authorized_skills: Vec::new(),
        authorized_skill_catalog_hash: hash_skill_catalog(&snapshot.skills),
        authorized_tool_catalog_hash: authorized.catalog_hash(),
        bootstrap_tool_keys: bootstrap_tool_keys.clone(),
        initial_exposed_tool_keys: bootstrap_tool_keys,
        initial_disclosed_skill_refs: selection.initial_disclosed_skill_refs,
        default_disclosed_skill_refs: Vec::new(),
        explicitly_selected_skill_refs: selection.explicitly_selected_skill_refs,
        carry_forward_source_run_id: None,
        carry_forward_decision_ref: None,
        discovery_policy_ref: VersionedKey::new("musteragent.agent-directed-discovery@11")
            .expect("discovery policy"),
        max_exposed_tools: NonZeroU32::new(64).expect("exposure limit"),
        max_search_results: NonZeroU32::new(20).expect("search limit"),
    });
    refresh_catalogs(&mut snapshot);
    snapshot
}

pub fn snapshot_content_with_read_tool(
    run_id: RunId,
    policy_ref: ContentRef,
    wire_name: &str,
) -> RuntimeSnapshotContent {
    let mut snapshot = snapshot_content(run_id, policy_ref);
    let capability = snapshot
        .capabilities
        .iter()
        .find(|grant| grant.key.as_str() == "web.search@2")
        .expect("web-search capability grant");
    let input_schema_json = br#"{"type":"object"}"#.to_vec();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    snapshot.tools.push(ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(VersionedKey::new("web.search@1").expect("Tool key")),
            wire_name: WireName::new(wire_name).expect("wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("web.search.input@1").expect("input schema"),
                schema_hash: Sha256::digest(&input_schema_json),
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("web.search.output@1").expect("output schema"),
                schema_hash: Sha256::digest(&output_schema_json),
            },
            capability_key: VersionedKey::new(capability.key.as_str()).expect("capability key"),
            capability_provider_id: capability.provider_id,
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            execution_policy: musteragent_tool::ToolExecutionPolicy::admitted_activity(
                VersionedKey::new("web.search@1").expect("current fixture"),
                Duration::from_secs(30),
            ),
            max_attempts: NonZeroU32::new(2).expect("attempts"),
            retry_contract: ToolRetryContract::ReadOnly,
            required_scopes: capability.allowed_scopes.clone(),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::Internet,
        },
        execution_target: CapabilityExecutionTarget::parse("Web.Search", None)
            .expect("fixture backend operation"),
        concurrency_class: ConcurrencyClass::ParallelRead,
        idempotency: BuiltinIdempotency::Native,
        file_operation: None,
        title: "Recovery search".into(),
        description: "Recovery fixture Tool".into(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json,
        output_schema_json,
    });
    snapshot
        .discovery
        .as_mut()
        .expect("current fixture")
        .initial_exposed_tool_keys
        .push(ToolKey::from_versioned_key(
            VersionedKey::new("web.search@1").expect("current fixture"),
        ));
    refresh_catalogs(&mut snapshot);
    snapshot
}

#[derive(Clone, Copy)]
pub struct SnapshotRoute {
    route_ref: &'static str,
    resolved_model_key: &'static str,
    api_format: ApiFormat,
    adapter_version: &'static str,
    protocol_version: &'static str,
}

pub const fn openai_responses_route() -> SnapshotRoute {
    SnapshotRoute {
        route_ref: "cloud.chat-primary@1",
        resolved_model_key: "gpt-context-test",
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: "openai.responses-adapter@1",
        protocol_version: "openai.responses@1",
    }
}

pub const fn anthropic_route() -> SnapshotRoute {
    SnapshotRoute {
        route_ref: "cloud.chat-anthropic@1",
        resolved_model_key: "claude-context-test",
        api_format: ApiFormat::AnthropicMessages,
        adapter_version: "anthropic.messages-adapter@1",
        protocol_version: "anthropic.messages@1",
    }
}

pub fn snapshot_content_for_route(
    run_id: RunId,
    policy_ref: ContentRef,
    route: SnapshotRoute,
) -> RuntimeSnapshotContent {
    pin_discovery(raw_snapshot_content_for_route(run_id, policy_ref, route))
}

/// Refresh the exact catalogs after a fixture adds a current Tool definition.
pub fn refresh_catalogs(snapshot: &mut RuntimeSnapshotContent) {
    let catalog =
        authorized_tool_catalog(&snapshot.tools, &snapshot.capabilities).expect("authorized Tools");
    let references = snapshot
        .skills
        .iter()
        .map(|skill| skill.skill_ref.clone())
        .collect::<BTreeSet<_>>();
    let entries = musteragent_skill::builtin_skill_catalog_entries()
        .expect("kernel entries")
        .into_iter()
        .filter(|entry| references.contains(&entry.definition.skill_ref))
        .collect::<Vec<_>>();
    let capabilities = snapshot
        .capabilities
        .iter()
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("current fixture"))
        .collect();
    let skills = musteragent_runtime_skill::authorized_skill_catalog(&entries, &capabilities)
        .expect("authorized Skills");
    let discovery = snapshot.discovery.as_mut().expect("current discovery");
    discovery.authorized_skills = skills.definitions().to_vec();
    discovery.authorized_skill_catalog_hash = skills.catalog_hash();
    discovery.authorized_tool_catalog_hash = catalog.catalog_hash();
    super::source_selection_snapshot::refresh_initial_disclosures(snapshot);
}

fn raw_snapshot_content_for_route(
    run_id: RunId,
    policy_ref: ContentRef,
    route: SnapshotRoute,
) -> RuntimeSnapshotContent {
    let musteragent = system_version(7, "assistant-base@1");
    let core_time = VersionedKey::new("core.time.read@1").expect("Capability");
    let web_fetch = VersionedKey::new("web.fetch@2").expect("Capability");
    let web_search = VersionedKey::new("web.search@2").expect("Capability");
    let authorized_capabilities =
        BTreeSet::from([core_time.clone(), web_fetch.clone(), web_search.clone()]);
    let skills = builtin_skill_catalog()
        .expect("Skill catalog")
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: musteragent.supported_runtime_contract.clone(),
            authorized_capabilities: authorized_capabilities.clone(),
        })
        .expect("resolve base Skill")
        .into_iter()
        .filter(|skill| {
            skill
                .required_capabilities
                .is_subset(&authorized_capabilities)
        })
        .collect();
    let provider_id = CapabilityProviderId::generate();
    let backend_provider_id = CapabilityProviderId::generate();
    let scope = DottedKey::new("time.read").expect("scope");
    let (model_capabilities, model_parameters) = if route.api_format == ApiFormat::AnthropicMessages
    {
        (
            BTreeSet::from([
                DottedKey::new("reasoning_style.anthropic_adaptive").expect("reasoning style")
            ]),
            BTreeMap::from([(
                DottedKey::new("reasoning_effort").expect("reasoning effort"),
                ModelParameter::String("medium".into()),
            )]),
        )
    } else {
        (BTreeSet::new(), BTreeMap::new())
    };
    RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: None,
        schema_version: DottedKey::new(musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION)
            .expect("schema"),
        agent: AgentVersionSnapshot::from_resolved(&system_definition(), &musteragent)
            .expect("Agent Snapshot"),
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("section"),
                    content_ref: policy_ref,
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.assistant").expect("section"),
                    content_ref: musteragent.persona_content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest([FIXTURE_POLICY, FIXTURE_PERSONA].concat()),
        },
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            route: ModelRoute {
                route_ref: VersionedKey::new(route.route_ref).expect("route"),
                resolved_model_key: route.resolved_model_key.into(),
                model_provider_id: "019fa6de-38fd-7000-8000-000000000431"
                    .parse()
                    .expect("model provider"),
                model_entry_id: "019fa6de-38fd-7000-8000-000000000432"
                    .parse()
                    .expect("model entry"),
                api_format: route.api_format,
                adapter_version: VersionedKey::new(route.adapter_version).expect("adapter"),
                protocol_version: VersionedKey::new(route.protocol_version).expect("protocol"),
                context_window: NonZeroU64::new(131_072).expect("window"),
                max_output_tokens: NonZeroU64::new(2_048).expect("output"),
                capabilities: model_capabilities,
                parameters: model_parameters,
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:test.price@1").expect("price"),
                provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
            },
            credential_grant_ref: CredentialGrantRef::new(format!("grant_{run_id}"))
                .expect("grant"),
            fallback_candidates: Vec::new(),
        },
        auxiliary_models: Vec::new(),
        skills,
        tools: Vec::new(),
        capabilities: vec![
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(core_time),
                provider_id,
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([scope]),
                policy_decision: musteragent_policy::Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_fetch),
                provider_id: backend_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("network.public").expect("scope")]),
                policy_decision: musteragent_policy::Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_search),
                provider_id: backend_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("network.search").expect("scope")]),
                policy_decision: musteragent_policy::Decision::Allow,
            },
        ],
        policy: PolicySnapshot {
            policy_ref: musteragent.policy_ref.clone(),
            policy_hash: musteragent.policy_hash,
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: musteragent.memory_policy_ref.clone(),
            selection_policy_hash: musteragent
                .material
                .as_ref()
                .expect("Kernel Agent material")
                .memory_policy_hash,
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: NonZeroU64::new(65_536).expect("input"),
            compaction_trigger_tokens: NonZeroU64::new(52_428).expect("trigger"),
            reserved_output_tokens: NonZeroU64::new(2_048).expect("reserved"),
            compaction_policy_ref: VersionedKey::new("musteragent.context-compaction@5")
                .expect("compaction"),
            resolved_v2: Some(musteragent_runtime_snapshot::ResolvedContextBudgets {
                policy_hash: musteragent_runtime_snapshot::context_compaction_policy_v5_hash(),
                hard_input_budget: NonZeroU64::new(65_536).expect("current fixture"),
                trigger_budget: NonZeroU64::new(52_428).expect("current fixture"),
                reserved_output_budget: NonZeroU64::new(2_048).expect("current fixture"),
                recent_tail_budget: NonZeroU64::new(2_000).expect("current fixture"),
                summary_budget: NonZeroU64::new(4_000).expect("current fixture"),
            }),
            source_selection: Some(
                musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1,
            ),
        },
        output_policy: musteragent_runtime_snapshot::OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy: RunExecutionPolicySnapshot::production_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2").expect("command"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("record"),
            snapshot_schema: DottedKey::new(musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION)
                .expect("snapshot"),
            model_protocol: VersionedKey::new("musteragent.model.canonical@2").expect("model"),
        },
        discovery: None,
    }
}
