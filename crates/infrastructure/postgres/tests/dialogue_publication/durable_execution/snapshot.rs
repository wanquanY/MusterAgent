//! Complete current Snapshot bound to the fixture's published Agent version.
use super::*;
use musteragent_capability::CapabilityGrantSnapshot;
use musteragent_definition::AgentDefinitionStore;
use musteragent_discovery::DiscoverySnapshot;
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_runtime_snapshot::*;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

pub(super) async fn install(f: &Fixture, run: RunContext) {
    let registry = musteragent_postgres::PostgresAgentDefinitionStore::new(f.pool.clone());
    let version = registry
        .load_version(f.activation.agent.published_version_id)
        .await
        .expect("published version")
        .expect("fixture Agent version");
    let agent_key: String =
        sqlx::query_scalar("SELECT agent_key FROM agent.agent_definitions WHERE agent_id=$1")
            .bind(version.agent_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .expect("Agent key");
    let definition = registry
        .load_definition(&AgentKey::new(agent_key).expect("Agent key"))
        .await
        .expect("definition")
        .expect("published Agent");
    let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
    let catalog = musteragent_tool::BuiltinToolCatalog::load_cloud().expect("kernel Tools");
    let bootstrap = dialogue_bootstrap_logical_keys_for_policy(DIALOGUE_DISCOVERY_POLICY, false);
    let provider = CapabilityProviderId::generate();
    let mut grants = BTreeMap::new();
    let tools: Vec<_> = catalog
        .definitions()
        .iter()
        .filter(|tool| bootstrap.contains(&tool.key.logical_key()))
        .map(|tool| {
            grants.insert(
                tool.capability_key.clone(),
                CapabilityGrantSnapshot {
                    key: musteragent_capability::CapabilityKey::from_versioned_key(
                        tool.capability_key.clone(),
                    ),
                    provider_id: provider,
                    location: CapabilityLocation::Cloud,
                    effect: tool.effect,
                    allowed_scopes: tool.required_scopes.clone(),
                    policy_decision: musteragent_policy::Decision::Allow,
                },
            );
            ToolDefinitionSnapshot {
                manifest: tool.pin(provider),
                execution_target: tool.target.clone(),
                concurrency_class: tool.concurrency,
                idempotency: tool.idempotency,
                file_operation: tool.file_operation.clone(),
                title: tool.title.clone(),
                description: tool.description.clone(),
                input_schema_json: tool.input_schema_json.clone(),
                output_schema_json: tool.output_schema_json.clone(),
            }
        })
        .collect();
    let capabilities: Vec<_> = grants.into_values().collect();
    let entries: Vec<_> = musteragent_skill::builtin_skill_catalog_entries()
        .expect("kernel Skills")
        .into_iter()
        .filter(|entry| {
            version
                .base_skill_refs
                .contains(&entry.definition.skill_ref)
        })
        .collect();
    let skills: Vec<_> = entries
        .iter()
        .map(|entry| entry.definition.clone())
        .collect();
    let capability_keys: BTreeSet<_> = capabilities
        .iter()
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("capability key"))
        .collect();
    let authorized_skills =
        musteragent_runtime_skill::authorized_skill_catalog(&entries, &capability_keys)
            .expect("Skill descriptors");
    let authorized_tools =
        authorized_tool_catalog(&tools, &capabilities).expect("Tool descriptors");
    let bootstrap_keys: Vec<_> = bootstrap
        .iter()
        .map(|logical| {
            tools
                .iter()
                .find(|tool| tool.manifest.key.logical_key() == *logical)
                .expect("bootstrap Tool")
                .manifest
                .key
                .clone()
        })
        .collect();
    let model = material::model();
    let budgets = ContextCompactionPolicyV11Spec::resolve(&model.route).expect("context budgets");
    let material = version.material.as_ref().expect("published Agent material");
    let content = RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: Some(release),
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
        agent: AgentVersionSnapshot::from_resolved(&definition, &version).expect("complete Agent"),
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("safety"),
                    content_ref: material.policy_content_ref.clone(),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new(MUSTERAGENT_ASSISTANT_PROMPT_SECTION)
                        .expect("persona"),
                    content_ref: version.persona_content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest(b"fixture policy and persona"),
        },
        model,
        auxiliary_models: vec![],
        skills,
        tools,
        capabilities,
        policy: PolicySnapshot {
            policy_ref: version.policy_ref.clone(),
            policy_hash: version.policy_hash,
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: vec![],
            selection_policy_ref: version.memory_policy_ref.clone(),
            selection_policy_hash: material.memory_policy_hash,
        },
        instincts: vec![],
        context_policy: ContextPolicySnapshot {
            max_input_tokens: budgets.hard_input_budget,
            compaction_trigger_tokens: budgets.trigger_budget,
            reserved_output_tokens: budgets.reserved_output_budget,
            compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V11)
                .expect("context policy"),
            resolved_v2: Some(budgets),
            source_selection: Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1),
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: false,
        },
        execution_policy: RunExecutionPolicySnapshot::production_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2").expect("command"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("record"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snapshot"),
            model_protocol: VersionedKey::new(MODEL_PROTOCOL_V2).expect("model protocol"),
        },
        discovery: Some(DiscoverySnapshot {
            authorized_skills: authorized_skills.definitions().to_vec(),
            authorized_skill_catalog_hash: authorized_skills.catalog_hash(),
            authorized_tool_catalog_hash: authorized_tools.catalog_hash(),
            bootstrap_tool_keys: bootstrap_keys.clone(),
            initial_exposed_tool_keys: bootstrap_keys,
            initial_disclosed_skill_refs: version.base_skill_refs.clone(),
            default_disclosed_skill_refs: vec![],
            explicitly_selected_skill_refs: vec![],
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: VersionedKey::new(DIALOGUE_DISCOVERY_POLICY)
                .expect("discovery policy"),
            max_exposed_tools: NonZeroU32::new(64).expect("exposure limit"),
            max_search_results: NonZeroU32::new(20).expect("search limit"),
        }),
    };
    content.validate().expect("current admission contract");
    let bytes = musteragent_persistence_codec::ProtobufSnapshotCodec
        .encode_content(&content)
        .expect("current snapshot bytes");
    sqlx::query("INSERT INTO capability.runtime_snapshots(runtime_snapshot_id,tenant_id,conversation_id,run_id,schema_version,encoded_snapshot,content_hash,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
        .bind(run.runtime_snapshot_id.into_uuid()).bind(run.tenant_id.into_uuid()).bind(run.conversation_id.into_uuid()).bind(run.run_id.into_uuid())
        .bind(content.schema_version.as_str()).bind(&bytes).bind(Sha256::digest(&bytes).into_bytes().to_vec()).execute(&f.pool).await.expect("persist current snapshot");
}
