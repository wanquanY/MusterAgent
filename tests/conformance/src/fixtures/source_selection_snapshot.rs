//! Synthetic execution-isolated snapshots. No cloud admission/API is exercised.
use musteragent_content_ref::MediaType;
use musteragent_identity::{DottedKey, RuntimeSnapshotId, UtcTimestamp, VersionedKey};
use musteragent_model::ModelRoute;
use musteragent_run::RunContext;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_snapshot::{
    ContextSourceSelectionPolicy, RuntimeSnapshot, SNAPSHOT_SCHEMA_VERSION,
};
use musteragent_wire::ProtobufSnapshotCodec;
use std::sync::Arc;
pub async fn isolated_snapshot(
    content: Arc<dyn ContentStore>,
    run_context: RunContext,
    route: ModelRoute,
) -> RuntimeSnapshot {
    let persona = content
        .put(PutContent {
            tenant_id: run_context.tenant_id,
            media_type: MediaType::new("text/markdown;charset=utf-8").expect("persona media"),
            bytes: b"synthetic persona".to_vec(),
        })
        .await
        .expect("publish fixture persona");
    let policy = content
        .put(PutContent {
            tenant_id: run_context.tenant_id,
            media_type: MediaType::new("text/plain").expect("media"),
            bytes: b"synthetic runtime policy".to_vec(),
        })
        .await
        .expect("persist fixture policy");
    let mut value =
        super::context_snapshot::snapshot_content_with_discovery(run_context.run_id, policy);
    value.model.route = route;
    value.execution_policy =
        musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults();
    let mut version = super::agent::system_version(7, "assistant-base@1");
    version.agent_version_id = musteragent_identity::AgentVersionId::generate();
    version.persona_content_ref = persona.clone();
    value.agent = musteragent_runtime_snapshot::AgentVersionSnapshot::from_resolved(
        &super::agent::system_definition(),
        &version,
    )
    .expect("host Agent publication");
    let assistant = value
        .prompt
        .sections
        .iter_mut()
        .find(|section| section.section_key.as_str() == "musteragent.assistant")
        .expect("assistant section");
    assistant.section_key = DottedKey::new("musteragent.assistant").expect("section key");
    assistant.content_ref = persona;
    value.protocol_versions.model_protocol =
        VersionedKey::new(musteragent_runtime_snapshot::MODEL_PROTOCOL_V2).expect("model protocol");
    value.schema_version = DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema");
    value.protocol_versions.snapshot_schema = value.schema_version.clone();
    value.context_policy.source_selection = Some(ContextSourceSelectionPolicy::ExecutionIsolatedV1);
    value.context_policy.compaction_policy_ref =
        VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V11)
            .expect("policy");
    value.context_policy.resolved_v2 = Some(
        musteragent_runtime_snapshot::ContextCompactionPolicyV11Spec::resolve(&value.model.route)
            .expect("budgets"),
    );
    let budgets = value.context_policy.resolved_v2.expect("resolved budgets");
    value.context_policy.max_input_tokens = budgets.hard_input_budget;
    value.context_policy.compaction_trigger_tokens = budgets.trigger_budget;
    value.context_policy.reserved_output_tokens = budgets.reserved_output_budget;
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
    let discovery = value.discovery.as_mut().expect("discovery");
    discovery.authorized_skills = skills.definitions().to_vec();
    discovery.authorized_skill_catalog_hash = skills.catalog_hash();
    refresh_initial_disclosures(&mut value);
    RuntimeSnapshot::seal(
        RuntimeSnapshotId::generate(),
        value,
        UtcTimestamp::from_unix_millis(100),
        &ProtobufSnapshotCodec,
    )
    .expect("seal fixture")
}
/// Apply the frozen automatic disclosure rules to an unbound host fixture.
pub fn refresh_initial_disclosures(
    content: &mut musteragent_runtime_snapshot::RuntimeSnapshotContent,
) {
    assert!(content.product_release.is_none());
    let discovery = content.discovery.as_mut().expect("discovery");
    let mut initial = musteragent_skill::SkillCatalog::new(content.skills.clone())
        .expect("current disclosures must succeed")
        .select_initial_disclosures(
            &content.skills,
            &musteragent_skill::SkillDisclosureRequest {
                explicitly_selected_skill_refs: discovery
                    .explicitly_selected_skill_refs
                    .iter()
                    .cloned()
                    .collect(),
            },
        )
        .expect("current disclosures must succeed")
        .initial_disclosed_skill_refs;
    let catalog = musteragent_runtime_snapshot::authorized_tool_catalog(
        &content.tools,
        &content.capabilities,
    )
    .expect("current disclosures must succeed");
    let schema = content.schema_version.as_str();
    for rule in musteragent_skill_release::OFFICIAL_AUTOMATIC_SKILL_DISCLOSURES {
        if rule.snapshot_schema_version != schema {
            continue;
        }
        let applies = match (
            rule.required_route_capability,
            rule.required_tool_capability,
        ) {
            (Some(key), None) => !content
                .model
                .route
                .capabilities
                .iter()
                .any(|item| item.as_str() == key),
            (None, Some(key)) => catalog
                .definitions()
                .iter()
                .any(|tool| tool.capability_ref.as_str() == key),
            _ => false,
        };
        if applies {
            let matching = content
                .skills
                .iter()
                .filter(|skill| skill.skill_ref.logical_key() == rule.skill_key)
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "one current automatic Skill");
            let key = matching[0].skill_ref.clone();
            if !initial.contains(&key) {
                initial.push(key);
            }
        }
    }
    discovery.initial_disclosed_skill_refs = initial;
}
