#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::publication::{OwnedAgentVersionDraft, verify_dialogue_manifest};
use musteragent_definition::{AgentDefinition, AgentKind, AgentLifecycle, ReleaseChannel};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, ContentId, UtcTimestamp, VersionNumber, VersionedKey,
};

fn custom_definition() -> AgentDefinition {
    let id = AgentId::generate();
    AgentDefinition {
        agent_id: id,
        agent_key: AgentKey::new(format!("agent-{id}")).unwrap(),
        display_name: "Research assistant".into(),
        kind: AgentKind::Custom,
        lifecycle: AgentLifecycle::Active,
        built_in: false,
        deletable: true,
        replaceable: true,
        default_for_product: false,
        system_slot: None,
    }
}

fn builtin_definition() -> AgentDefinition {
    AgentDefinition {
        agent_id: AgentId::generate(),
        agent_key: AgentKey::new("assistant").unwrap(),
        display_name: "Assistant".into(),
        kind: AgentKind::SystemBuiltin,
        lifecycle: AgentLifecycle::Active,
        built_in: true,
        deletable: false,
        replaceable: false,
        default_for_product: true,
        system_slot: Some("main".into()),
    }
}

fn text(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn draft(def: &AgentDefinition) -> OwnedAgentVersionDraft {
    let now = UtcTimestamp::from_unix_millis(1_790_000_000_000);
    OwnedAgentVersionDraft {
        agent_id: def.agent_id,
        agent_version_id: AgentVersionId::generate(),
        version: VersionNumber::new(1).unwrap(),
        supersedes: None,
        persona_version: VersionNumber::new(1).unwrap(),
        persona_content_ref: text(b"You are a careful research assistant."),
        base_skill_refs: vec![VersionedKey::new("assistant-base@1").unwrap()],
        policy_ref: VersionedKey::new("musteragent.runtime-policy@1").unwrap(),
        policy_content_ref: text(b"{}"),
        memory_policy_ref: VersionedKey::new(
            "musteragent.memory-selection.scope-confidence-recency@1",
        )
        .unwrap(),
        memory_policy_content_ref: text(b"select memory"),
        user_memory_collection_id: None,
        long_term_memory_collection_id: None,
        model_requirement_ref: VersionedKey::new("musteragent.chat-model@1").unwrap(),
        supported_runtime_contract: VersionedKey::new("musteragent.runtime-contract@1").unwrap(),
        release_channel: ReleaseChannel::Stable,
        published_at: now,
        activated_at: now,
    }
}

#[test]
fn canonical_manifest_binds_identity_and_every_published_material_reference() {
    let def = custom_definition();
    let prepared = draft(&def).prepare(&def, ContentId::generate()).unwrap();
    verify_dialogue_manifest(&def, &prepared.version, &prepared.manifest_bytes).unwrap();
    let mut changed = prepared.manifest_bytes.clone();
    changed.push(b' ');
    assert!(verify_dialogue_manifest(&def, &prepared.version, &changed).is_err());
    let mut wrong = prepared.version.clone();
    wrong.persona_content_ref.content_id = ContentId::generate();
    assert!(verify_dialogue_manifest(&def, &wrong, &prepared.manifest_bytes).is_err());
    let mut wrong = def.clone();
    wrong.agent_key = AgentKey::new("another-agent").unwrap();
    assert!(verify_dialogue_manifest(&wrong, &prepared.version, &prepared.manifest_bytes).is_err());
    assert!(
        verify_dialogue_manifest(
            &builtin_definition(),
            &prepared.version,
            &prepared.manifest_bytes
        )
        .is_err()
    );
}

#[test]
fn malformed_lineage_and_builtin_identity_cannot_be_published_as_account_agents() {
    let def = custom_definition();
    let mut invalid = draft(&def);
    invalid.supersedes = Some(invalid.agent_version_id);
    assert!(invalid.prepare(&def, ContentId::generate()).is_err());
    let mut invalid = draft(&def);
    invalid.version = VersionNumber::new(2).unwrap();
    assert!(invalid.prepare(&def, ContentId::generate()).is_err());
    let builtin = builtin_definition();
    assert!(
        draft(&builtin)
            .prepare(&builtin, ContentId::generate())
            .is_err()
    );
}
