//! Pin the complete Skill descriptors required by the kernel's named Skill Tools.
use std::collections::BTreeSet;

use musteragent_definition::AgentVersion;
use musteragent_identity::{DottedKey, VersionedKey};
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
};

pub(super) fn pin_current_snapshot_with_skills(
    content: &mut RuntimeSnapshotContent,
    version: &AgentVersion,
) {
    let definition = musteragent_conformance::fixtures::agent::system_definition();
    content.agent = AgentVersionSnapshot::from_resolved(&definition, version)
        .expect("explicit host Agent publication");
    let resolved = content
        .agent
        .resolved_v5
        .as_ref()
        .expect("complete Agent material");
    content.policy.policy_ref = content.agent.policy_ref.clone();
    content.policy.policy_hash = resolved.policy_hash;
    content.memory.selection_policy_ref = resolved.memory_policy_ref.clone();
    content.memory.selection_policy_hash = resolved.memory_policy_hash;
    let persona = content
        .prompt
        .sections
        .iter_mut()
        .find(|section| {
            matches!(
                section.section_key.as_str(),
                "musteragent.legacy.persona" | "musteragent.assistant"
            )
        })
        .expect("fixture persona section");
    persona.section_key = DottedKey::new("musteragent.assistant").expect("persona section key");
    assert_eq!(persona.content_ref, version.persona_content_ref);
    content.schema_version = DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema");
    content.protocol_versions.snapshot_schema = content.schema_version.clone();
    content.protocol_versions.model_protocol =
        VersionedKey::new(musteragent_runtime_snapshot::MODEL_PROTOCOL_V2)
            .expect("current fixture");
    content.execution_policy =
        musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults();
    for tool in &mut content.tools {
        tool.manifest.execution_policy = musteragent_tool::ToolExecutionPolicy::admitted_activity(
            VersionedKey::new(tool.manifest.key.as_str()).expect("tool key"),
            std::time::Duration::from_secs(30),
        );
    }
    let budgets =
        musteragent_runtime_snapshot::ContextCompactionPolicyV5Spec::resolve(&content.model.route)
            .expect("current fixture");
    content.context_policy.compaction_policy_ref =
        VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V5)
            .expect("current fixture");
    content.context_policy.resolved_v2 = Some(budgets);
    content.context_policy.max_input_tokens = budgets.hard_input_budget;
    content.context_policy.compaction_trigger_tokens = budgets.trigger_budget;
    content.context_policy.reserved_output_tokens = budgets.reserved_output_budget;
    content.context_policy.source_selection =
        Some(musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1);
    let references = content
        .skills
        .iter()
        .map(|skill| skill.skill_ref.clone())
        .collect::<BTreeSet<_>>();
    let entries = musteragent_skill::builtin_skill_catalog_entries()
        .expect("kernel Skill entries")
        .into_iter()
        .filter(|entry| references.contains(&entry.definition.skill_ref))
        .collect::<Vec<_>>();
    assert_eq!(
        entries.len(),
        content.skills.len(),
        "every frozen Skill has its original descriptor"
    );
    let capabilities = content
        .capabilities
        .iter()
        .map(|grant| VersionedKey::new(grant.key.as_str()).expect("capability key"))
        .collect();
    let catalog = musteragent_runtime_skill::authorized_skill_catalog(&entries, &capabilities)
        .expect("authorized kernel Skill catalog");
    let discovery = content.discovery.as_mut().expect("discovery");
    discovery.authorized_skills = catalog.definitions().to_vec();
    discovery.authorized_skill_catalog_hash = catalog.catalog_hash();
    musteragent_conformance::fixtures::context_snapshot::refresh_catalogs(content);
}
