//! Synthetic host publication used only by conformance tests.
use musteragent_content_ref::*;
use musteragent_definition::*;
use musteragent_identity::*;
use std::str::FromStr;
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
pub fn system_definition() -> AgentDefinition {
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

pub fn system_version(version: u64, base_skill: &str) -> AgentVersion {
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

/// Publish the synthetic persona through the real ContentStore port.
pub async fn publish_persona(
    content: &dyn musteragent_runtime_content::ContentStore,
    tenant_id: TenantId,
) {
    let reference = system_version(7, "assistant-base@1").persona_content_ref;
    let stored = content
        .put_at(
            reference.content_id,
            musteragent_runtime_content::PutContent {
                tenant_id,
                media_type: reference.media_type.clone(),
                bytes: b"synthetic persona".to_vec(),
            },
        )
        .await
        .expect("publish host persona fixture");
    assert_eq!(stored, reference);
}

/// Immutable synthetic host persona used by worker conformance.
pub const FIXTURE_PERSONA_V1: &str = "synthetic persona";
/// Immutable fixture Agent version.
pub fn fixture_version_v1() -> AgentVersion {
    system_version(7, "assistant-base@1")
}

/// Synthetic policy bytes owned by the fixture host.
pub const FIXTURE_RUNTIME_POLICY_V1: &str = "synthetic runtime policy";
pub fn fixture_runtime_policy_content_ref()
-> Result<ContentRef, musteragent_identity::IdentityError> {
    Ok(system_version(7, "assistant-base@1")
        .material
        .expect("fixture material")
        .policy_content_ref)
}
