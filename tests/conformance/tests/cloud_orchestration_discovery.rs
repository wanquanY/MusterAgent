//! Sealing a cloud orchestration Snapshot: the frozen `@1` ceiling rejects the
//! conversation delivery and work-item tools, the additive `@2` admits them.
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_conformance::fixtures::{agent, context_snapshot};
use musteragent_identity::{CapabilityProviderId, DottedKey, ProductId, RunId, VersionedKey};
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2, CLOUD_ORCHESTRATION_V2_ADDITIONS,
    CONTEXT_COMPACTION_POLICY_V11, ContextCompactionPolicyV11Spec, ContextSourceSelectionPolicy,
    MODEL_PROTOCOL_V2, RunExecutionPolicySnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
    SnapshotContractError, ToolDefinitionSnapshot, authorized_tool_catalog,
    cloud_orchestration_discovery_policy_for,
};
use musteragent_tool::{BuiltinToolCatalog, ProductExecutionTarget, compiled_product_release};

const BOOTSTRAP: [&str; 8] = [
    "tool.catalog.search",
    "tool.catalog.activate",
    "interaction.request",
    "skills.search",
    "skills.list",
    "skills.view",
    "plan.get",
    "plan.update",
];

/// A kernel-bound cloud orchestration Snapshot carrying `extra` tools.
fn orchestration_snapshot(extra: &[&str], location: CapabilityLocation) -> RuntimeSnapshotContent {
    let policy = agent::fixture_runtime_policy_content_ref().expect("policy content");
    let mut value = context_snapshot::snapshot_content_with_discovery(RunId::generate(), policy);
    value.execution_policy = RunExecutionPolicySnapshot::production_defaults();
    let mut version = agent::system_version(7, "assistant-base@1");
    version.agent_version_id = musteragent_identity::AgentVersionId::generate();
    value.agent = AgentVersionSnapshot::from_resolved(&agent::system_definition(), &version)
        .expect("host Agent publication");
    let assistant = value
        .prompt
        .sections
        .iter_mut()
        .find(|section| section.section_key.as_str() == "musteragent.assistant")
        .expect("assistant section");
    assistant.content_ref = version.persona_content_ref.clone();
    value.protocol_versions.model_protocol =
        VersionedKey::new(MODEL_PROTOCOL_V2).expect("model protocol");
    value.schema_version = DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema");
    value.protocol_versions.snapshot_schema = value.schema_version.clone();
    value.context_policy.source_selection = Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1);
    value.context_policy.compaction_policy_ref =
        VersionedKey::new(CONTEXT_COMPACTION_POLICY_V11).expect("policy");
    let budgets =
        ContextCompactionPolicyV11Spec::resolve(&value.model.route).expect("resolved budgets");
    value.context_policy.resolved_v2 = Some(budgets);
    value.context_policy.max_input_tokens = budgets.hard_input_budget;
    value.context_policy.compaction_trigger_tokens = budgets.trigger_budget;
    value.context_policy.reserved_output_tokens = budgets.reserved_output_budget;
    value.product_release = Some(compiled_product_release(ProductId::KERNEL));

    let catalog = BuiltinToolCatalog::for_product_release(
        &compiled_product_release(ProductId::KERNEL),
        ProductExecutionTarget::Cloud,
    )
    .expect("kernel catalog");
    let provider: CapabilityProviderId = musteragent_tool::BUILTIN_RUST_PROVIDER_ID
        .parse()
        .expect("provider");
    value.tools.clear();
    value.capabilities.clear();
    for key in BOOTSTRAP.iter().chain(extra) {
        let definition = catalog
            .definitions()
            .iter()
            .find(|d| d.key.logical_key() == *key || d.key.as_str() == *key)
            .unwrap_or_else(|| panic!("released tool {key}"));
        let location = if BOOTSTRAP.contains(key) {
            CapabilityLocation::Cloud
        } else {
            location
        };
        value.capabilities.push(CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
            provider_id: provider,
            location,
            effect: definition.effect,
            allowed_scopes: definition.required_scopes.clone(),
            policy_decision: musteragent_policy::Decision::Allow,
        });
        value.tools.push(ToolDefinitionSnapshot {
            manifest: definition.pin(provider),
            execution_target: definition.target.clone(),
            concurrency_class: definition.concurrency,
            idempotency: definition.idempotency,
            file_operation: definition.file_operation.clone(),
            title: definition.title.clone(),
            description: definition.description.clone(),
            input_schema_json: definition.input_schema_json.clone(),
            output_schema_json: definition.output_schema_json.clone(),
        });
    }
    value.capabilities.sort_by(|a, b| a.key.cmp(&b.key));
    value
        .tools
        .sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    let hash = authorized_tool_catalog(&value.tools, &value.capabilities)
        .expect("authorized tools")
        .catalog_hash();
    let bootstrap = BOOTSTRAP
        .iter()
        .map(|key| {
            value
                .tools
                .iter()
                .find(|t| t.manifest.key.logical_key() == *key)
                .expect("bootstrap tool")
                .manifest
                .key
                .clone()
        })
        .collect::<Vec<_>>();
    let references = value
        .skills
        .iter()
        .map(|skill| skill.skill_ref.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let entries = musteragent_skill::builtin_skill_catalog_entries()
        .expect("bundled entries")
        .into_iter()
        .filter(|entry| references.contains(&entry.definition.skill_ref))
        .collect::<Vec<_>>();
    let capabilities = value
        .capabilities
        .iter()
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("capability key"))
        .collect();
    let skills = musteragent_runtime_skill::authorized_skill_catalog(&entries, &capabilities)
        .expect("authorized skills");
    let policy = cloud_orchestration_discovery_policy_for(
        value.tools.iter().map(|t| t.manifest.key.as_str()),
    );
    let discovery = value.discovery.as_mut().expect("discovery");
    discovery.authorized_tool_catalog_hash = hash;
    discovery.authorized_skills = skills.definitions().to_vec();
    discovery.authorized_skill_catalog_hash = skills.catalog_hash();
    discovery.bootstrap_tool_keys = bootstrap.clone();
    discovery.initial_exposed_tool_keys = bootstrap;
    discovery.discovery_policy_ref = VersionedKey::new(policy).expect("policy");
    value
}

fn with_policy(mut value: RuntimeSnapshotContent, policy: &str) -> RuntimeSnapshotContent {
    value
        .discovery
        .as_mut()
        .expect("discovery")
        .discovery_policy_ref = VersionedKey::new(policy).expect("policy");
    value
}

#[test]
fn existing_orchestration_catalog_still_seals_under_v1() {
    let value = orchestration_snapshot(&["delegation.spawn@5"], CapabilityLocation::Cloud);
    assert_eq!(
        value
            .discovery
            .as_ref()
            .unwrap()
            .discovery_policy_ref
            .as_str(),
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1
    );
    value.validate().expect("v1 snapshot seals");
    with_policy(value, CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2)
        .validate()
        .expect("v2 keeps the v1 ceiling");
}

#[test]
fn delivery_and_work_item_tools_seal_only_under_v2() {
    let mut extra = vec!["delegation.spawn@5"];
    extra.extend(CLOUD_ORCHESTRATION_V2_ADDITIONS);
    let value = orchestration_snapshot(&extra, CapabilityLocation::Cloud);
    assert_eq!(
        value
            .discovery
            .as_ref()
            .unwrap()
            .discovery_policy_ref
            .as_str(),
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2
    );
    value.validate().expect("v2 snapshot seals");
    for key in CLOUD_ORCHESTRATION_V2_ADDITIONS {
        let single = orchestration_snapshot(&[key], CapabilityLocation::Cloud);
        single
            .validate()
            .unwrap_or_else(|e| panic!("{key} under v2: {e:?}"));
        assert!(
            matches!(
                with_policy(single, CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1).validate(),
                Err(SnapshotContractError::InvalidDiscovery)
            ),
            "{key} must stay outside the frozen v1 ceiling"
        );
    }
}
