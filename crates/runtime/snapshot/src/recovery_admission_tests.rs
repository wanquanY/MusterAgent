use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::{
    AgentDefinition, AgentKind, AgentLifecycle, AgentVersion, AgentVersionMaterialV1,
    ReleaseChannel,
};
use musteragent_identity::{ContentId, DottedKey, VersionedKey};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_skill::{SkillAuthorizationRequest, Surface};

use super::{
    AgentVersionSnapshot, AgentVersionSnapshotV5, CONTEXT_COMPACTION_POLICY_V1,
    CONTEXT_COMPACTION_POLICY_V2, ContextPolicySnapshot, MAX_DURABLE_MODEL_COMPLETION_BYTES,
    MODEL_COMPLETION_INTENT_FIXED_BYTES, MODEL_COMPLETION_INTENT_PER_TOOL_BYTES, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ResolvedContextBudgets,
    RunExecutionPolicySnapshot, SnapshotContractError, context_compaction_policy_v2_hash,
    initial_skill_refs_are_valid, validate_agent_v5_projection,
};

const SYSTEM_AGENT_ID: &str = "01982f80-0000-7000-8000-000000000001";

fn content(id: &str, media_type: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::from_str(id).expect("Content ID"),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

/// Synthetic host-published system Agent; the kernel ships no built-in Agent.
fn system_definition() -> AgentDefinition {
    AgentDefinition {
        agent_id: musteragent_identity::AgentId::from_str(SYSTEM_AGENT_ID).expect("Agent ID"),
        agent_key: musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
        system_slot: Some("main".to_owned()),
        display_name: "Assistant".to_owned(),
        kind: AgentKind::SystemBuiltin,
        built_in: true,
        deletable: false,
        replaceable: false,
        default_for_product: true,
        lifecycle: AgentLifecycle::Active,
    }
}

fn system_version(version: u64, base_skill: &str) -> AgentVersion {
    use musteragent_identity::{AgentVersionId, UtcTimestamp, VersionNumber};
    let policy = content(
        "01982f80-0000-7000-8000-000000000010",
        "text/plain;charset=utf-8",
        b"synthetic runtime policy",
    );
    let memory_policy = content(
        "01982f80-0000-7000-8000-00000000000f",
        "text/plain;charset=utf-8",
        b"synthetic memory policy",
    );
    let manifest = content(
        "01982f80-0000-7000-8000-000000000015",
        "application/json",
        format!("synthetic Agent manifest v{version}").as_bytes(),
    );
    AgentVersion {
        agent_version_id: AgentVersionId::from_str(&format!(
            "01982f80-0000-7000-8000-0000000001{version:02x}"
        ))
        .expect("Agent version ID"),
        agent_id: system_definition().agent_id,
        version: VersionNumber::new(version).expect("version"),
        persona_version: VersionNumber::new(1).expect("persona version"),
        persona_content_ref: content(
            "01982f80-0000-7000-8000-000000000003",
            "text/markdown;charset=utf-8",
            b"synthetic persona",
        ),
        base_skill_refs: vec![VersionedKey::new(base_skill).expect("base Skill")],
        policy_ref: VersionedKey::new("musteragent.runtime-policy@1").expect("policy ref"),
        policy_hash: policy.sha256,
        memory_policy_ref: VersionedKey::new(
            "musteragent.memory-selection.scope-confidence-recency@1",
        )
        .expect("memory policy ref"),
        model_requirement_ref: VersionedKey::new("musteragent.chat-model@1")
            .expect("model requirement"),
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1")
            .expect("runtime contract"),
        release_channel: ReleaseChannel::Stable,
        published_at: UtcTimestamp::from_unix_millis(1_790_874_000_000),
        activated_at: UtcTimestamp::from_unix_millis(1_790_874_000_000),
        supersedes: (version > 1).then(|| {
            AgentVersionId::from_str("01982f80-0000-7000-8000-000000000011")
                .expect("parent version ID")
        }),
        material: Some(AgentVersionMaterialV1 {
            manifest_hash: manifest.sha256,
            manifest_ref: manifest,
            policy_content_ref: policy,
            memory_policy_hash: memory_policy.sha256,
            memory_policy_content_ref: memory_policy,
            user_memory_collection_id: None,
            long_term_memory_collection_id: None,
        }),
    }
}

fn valid_current_projection() -> (
    AgentVersionSnapshot,
    AgentVersionSnapshotV5,
    PromptAssemblySnapshot,
    PolicySnapshot,
    MemorySelectionSnapshot,
    Vec<musteragent_skill::SkillDefinition>,
) {
    let version = system_version(7, "assistant-base@1");
    let agent = AgentVersionSnapshot::from_resolved(&system_definition(), &version)
        .expect("Agent Snapshot");
    let policy_hash = Sha256::digest(b"synthetic runtime policy");
    let memory_policy_hash = Sha256::digest(b"synthetic memory policy");
    let manifest_hash = Sha256::digest(b"synthetic Agent manifest");
    let resolved = AgentVersionSnapshotV5 {
        kind: AgentKind::SystemBuiltin,
        version: version.version,
        parent_version_id: version.supersedes,
        manifest_ref: ContentRef {
            content_id: ContentId::from_str("01982f80-0000-7000-8000-000000000120")
                .expect("manifest Content ID"),
            media_type: MediaType::new("application/json").expect("manifest media type"),
            byte_length: 24,
            sha256: manifest_hash,
        },
        manifest_hash,
        persona_hash: version.persona_content_ref.sha256,
        policy_hash,
        memory_policy_ref: version.memory_policy_ref.clone(),
        memory_policy_hash,
        model_requirement_ref: version.model_requirement_ref,
        supported_runtime_contract: version.supported_runtime_contract.clone(),
        user_memory_collection_id: None,
        long_term_memory_collection_id: None,
        release_channel: ReleaseChannel::Stable,
    };
    let policy = PolicySnapshot {
        policy_ref: version.policy_ref,
        policy_hash,
        permission_mode: musteragent_policy::PermissionMode::Supervised,
    };
    let prompt = PromptAssemblySnapshot {
        sections: vec![
            PromptSectionSnapshot {
                section_key: DottedKey::new("platform.safety").expect("section key"),
                content_ref: ContentRef {
                    content_id: ContentId::from_str("01982f80-0000-7000-8000-000000000121")
                        .expect("policy Content ID"),
                    media_type: MediaType::new("text/plain;charset=utf-8")
                        .expect("policy media type"),
                    byte_length: 24,
                    sha256: policy_hash,
                },
            },
            PromptSectionSnapshot {
                section_key: DottedKey::new("musteragent.legacy.persona").expect("section key"),
                content_ref: agent.persona_content_ref.clone(),
            },
        ],
        assembled_hash: Sha256::digest(b"synthetic assembled prompt"),
    };
    let memory = MemorySelectionSnapshot {
        selected: Vec::new(),
        selection_policy_ref: version.memory_policy_ref,
        selection_policy_hash: memory_policy_hash,
    };
    let catalog = crate::current_skill_fixture::work_catalog();
    let authorized_capabilities = catalog
        .definitions()
        .flat_map(|skill| skill.required_capabilities.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut skills = catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: version.supported_runtime_contract,
            authorized_capabilities,
        })
        .expect("authorized Skills");
    // This fixture pins V6, not the process's selected successor Agent. Its
    // Base Skill stays byte-immutable when a release profile selects V7.
    skills.retain(|skill| skill.activation_policy != musteragent_skill::ActivationPolicy::Base);
    let mut base = crate::current_skill_fixture::skill(version.base_skill_refs[0].as_str());
    base.lifecycle = musteragent_skill::SkillLifecycle::Active;
    skills.push(base);
    (agent, resolved, prompt, policy, memory, skills)
}

#[test]
fn resolved_constructor_projects_complete_agent_version_material() {
    let definition = system_definition();
    let version = system_version(5, "assistant-base@1");
    let snapshot =
        AgentVersionSnapshot::from_resolved(&definition, &version).expect("v5 projection");
    let resolved = snapshot.resolved_v5.as_ref().expect("v5 material");

    assert_eq!(snapshot.agent_key, definition.agent_key);
    assert_eq!(resolved.version, version.version);
    assert_eq!(resolved.policy_hash, version.policy_hash);
    assert_eq!(resolved.memory_policy_ref, version.memory_policy_ref);
    assert_eq!(
        resolved.manifest_ref,
        version.material.as_ref().expect("material").manifest_ref
    );
}

#[test]
fn v5_agent_projection_rejects_policy_memory_skill_and_contract_drift() {
    let (agent, resolved, prompt, policy, memory, skills) = valid_current_projection();
    assert!(
        validate_agent_v5_projection(&agent, &resolved, &prompt, &policy, &memory, &skills, false)
            .is_ok()
    );

    let mut drifted_policy = policy.clone();
    drifted_policy.policy_hash = Sha256::digest(b"drifted policy");
    assert!(matches!(
        validate_agent_v5_projection(
            &agent,
            &resolved,
            &prompt,
            &drifted_policy,
            &memory,
            &skills,
            false,
        ),
        Err(SnapshotContractError::InvalidAgentVersionSnapshot)
    ));

    let mut drifted_prompt = prompt.clone();
    drifted_prompt.sections[0].content_ref.sha256 = Sha256::digest(b"drifted prompt policy");
    assert!(matches!(
        validate_agent_v5_projection(
            &agent,
            &resolved,
            &drifted_prompt,
            &policy,
            &memory,
            &skills,
            false,
        ),
        Err(SnapshotContractError::InvalidAgentVersionSnapshot)
    ));

    let mut drifted_memory = memory.clone();
    drifted_memory.selection_policy_ref =
        VersionedKey::new("musteragent.legacy.memory-policy@2").expect("drifted Memory policy");
    assert!(matches!(
        validate_agent_v5_projection(
            &agent,
            &resolved,
            &prompt,
            &policy,
            &drifted_memory,
            &skills,
            false,
        ),
        Err(SnapshotContractError::InvalidAgentVersionSnapshot)
    ));

    let mut drifted_agent = agent.clone();
    drifted_agent.base_skill_refs.clear();
    assert!(matches!(
        validate_agent_v5_projection(
            &drifted_agent,
            &resolved,
            &prompt,
            &policy,
            &memory,
            &skills,
            false,
        ),
        Err(SnapshotContractError::InvalidAgentVersionSnapshot)
    ));

    let mut unknown_contract = resolved;
    unknown_contract.supported_runtime_contract =
        VersionedKey::new("musteragent.legacy.runtime-contract@999").expect("unknown contract");
    assert!(matches!(
        validate_agent_v5_projection(
            &agent,
            &unknown_contract,
            &prompt,
            &policy,
            &memory,
            &skills,
            false,
        ),
        Err(SnapshotContractError::InvalidAgentVersionSnapshot)
    ));
}

#[test]
fn initial_skill_projection_is_base_then_surface_then_explicit_and_closed() {
    let catalog = crate::current_skill_fixture::work_catalog();
    let all_capabilities = catalog
        .definitions()
        .flat_map(|skill| skill.required_capabilities.iter().cloned())
        .collect::<BTreeSet<_>>();
    let contract = VersionedKey::new("musteragent.runtime-contract@1").expect("contract");
    let web = catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: contract,
            authorized_capabilities: all_capabilities,
        })
        .expect("authorized Web Skills");
    let base = web
        .iter()
        .find(|skill| skill.activation_policy == musteragent_skill::ActivationPolicy::Base)
        .expect("selected Base")
        .skill_ref
        .clone();
    let research = web
        .iter()
        .find(|skill| skill.skill_ref.logical_key() == "web-research")
        .expect("selected research")
        .skill_ref
        .clone();
    let web_initial = vec![base.clone()];
    assert!(initial_skill_refs_are_valid(&web, &[], &web_initial));
    let mut implicit_web_research = web_initial;
    implicit_web_research.push(research.clone());
    assert!(!initial_skill_refs_are_valid(
        &web,
        &[],
        &implicit_web_research
    ));

    let explicit = vec![research.clone()];
    let stable = vec![base, research];
    assert!(initial_skill_refs_are_valid(&web, &explicit, &stable));
    let mut reordered = stable;
    reordered.swap(0, 1);
    assert!(!initial_skill_refs_are_valid(&web, &explicit, &reordered));
}

#[test]
fn context_policy_v2_requires_exact_resolved_budgets_without_widening_v1() {
    let legacy = ContextPolicySnapshot {
        source_selection: None,
        max_input_tokens: NonZeroU64::new(100).expect("input"),
        compaction_trigger_tokens: NonZeroU64::new(70).expect("trigger"),
        reserved_output_tokens: NonZeroU64::new(20).expect("reserved"),
        compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V1).expect("v1 policy"),
        resolved_v2: None,
    };
    assert!(legacy.validate().is_ok());

    let resolved = ResolvedContextBudgets {
        policy_hash: context_compaction_policy_v2_hash(),
        hard_input_budget: NonZeroU64::new(100).expect("hard input"),
        trigger_budget: NonZeroU64::new(85).expect("trigger"),
        reserved_output_budget: NonZeroU64::new(20).expect("reserved"),
        recent_tail_budget: NonZeroU64::new(17).expect("recent tail"),
        summary_budget: NonZeroU64::new(33).expect("summary"),
    };
    let modern = ContextPolicySnapshot {
        source_selection: None,
        max_input_tokens: resolved.hard_input_budget,
        compaction_trigger_tokens: resolved.trigger_budget,
        reserved_output_tokens: resolved.reserved_output_budget,
        compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V2).expect("v2 policy"),
        resolved_v2: Some(resolved),
    };
    assert!(modern.validate().is_ok());

    let mut v2_without_resolution = modern.clone();
    v2_without_resolution.resolved_v2 = None;
    assert!(matches!(
        v2_without_resolution.validate(),
        Err(SnapshotContractError::InvalidContextBudget)
    ));
    let mut v2_with_drift = modern;
    v2_with_drift.max_input_tokens = NonZeroU64::new(99).expect("drifted input");
    assert!(matches!(
        v2_with_drift.validate(),
        Err(SnapshotContractError::InvalidContextBudget)
    ));
    let mut v1_with_v2_material = legacy;
    v1_with_v2_material.resolved_v2 = Some(resolved);
    assert!(matches!(
        v1_with_v2_material.validate(),
        Err(SnapshotContractError::InvalidContextBudget)
    ));
}

#[test]
fn stream_larger_than_the_recoverable_completion_bound_is_rejected_at_admission() {
    let mut policy = RunExecutionPolicySnapshot::production_defaults();
    let tool_calls = 10_000_u64;
    policy.model_stream.max_events = NonZeroU32::new(10_002).expect("non-zero");
    policy.model_stream.max_tool_calls =
        NonZeroU32::new(u32::try_from(tool_calls).expect("fits u32")).expect("non-zero");
    policy.model_stream.max_tool_argument_bytes =
        NonZeroUsize::new(16 * 1_048_576).expect("non-zero");
    policy.model_stream.max_reasoning_replay_bytes =
        NonZeroUsize::new(64 * 1_048_576).expect("non-zero");
    let exact_total = MAX_DURABLE_MODEL_COMPLETION_BYTES
        - MODEL_COMPLETION_INTENT_FIXED_BYTES
        - tool_calls * MODEL_COMPLETION_INTENT_PER_TOOL_BYTES;
    policy.model_stream.max_total_bytes = NonZeroU64::new(exact_total).expect("non-zero");
    policy.model_stream.max_event_bytes = NonZeroUsize::new(16 * 1_048_576).expect("non-zero");
    assert_eq!(
        policy.model_stream.completion_intent_worst_case_bytes(),
        Some(MAX_DURABLE_MODEL_COMPLETION_BYTES)
    );
    assert!(policy.validate().is_ok());

    let mut rejected = policy;
    rejected.model_stream.max_total_bytes = NonZeroU64::new(exact_total + 1).expect("non-zero");
    assert!(matches!(
        rejected.validate(),
        Err(SnapshotContractError::InvalidModelStreamBudget)
    ));
}
