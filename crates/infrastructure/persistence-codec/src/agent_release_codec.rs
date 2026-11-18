//! Exact published Agent material transport. Decoding does not publish an Agent
//! or authorize its ContentRefs; original publication and Content owners do that.
use crate::{dialogue_codec::*, encode_content_ref, wire};
use musteragent_content_ref::Sha256;
use musteragent_definition::*;
use musteragent_identity::*;

type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
fn versioned(v: &VersionedKey) -> wire::VersionedRef {
    wire::VersionedRef {
        key: v.as_str().into(),
    }
}
fn decode_versioned(v: Option<wire::VersionedRef>) -> Result<VersionedKey, Error> {
    VersionedKey::new(required(v)?.key).map_err(|_| BAD)
}
fn hash(v: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: v.into_bytes().to_vec(),
    }
}
fn decode_hash(v: Option<wire::Sha256>) -> Result<Sha256, Error> {
    Sha256::from_bytes(required(v)?.value).map_err(|_| BAD)
}
pub fn definition(v: &AgentDefinition) -> Result<wire::PublishedAgentDefinitionMaterial, Error> {
    v.validate().map_err(|_| BAD)?;
    Ok(wire::PublishedAgentDefinitionMaterial {
        agent_id: Some(wire::AgentId {
            value: v.agent_id.to_string(),
        }),
        agent_key: v.agent_key.as_str().into(),
        system_slot: v.system_slot.clone(),
        display_name: v.display_name.clone(),
        kind: match v.kind {
            AgentKind::SystemBuiltin => 1,
            AgentKind::Custom => 2,
            AgentKind::Installed => 3,
        },
        built_in: v.built_in,
        deletable: v.deletable,
        replaceable: v.replaceable,
        default_for_product: v.default_for_product,
        lifecycle: match v.lifecycle {
            AgentLifecycle::Active => 1,
            AgentLifecycle::Retired => 2,
            AgentLifecycle::Deleted => 3,
        },
    })
}
pub fn decode_definition(
    v: wire::PublishedAgentDefinitionMaterial,
) -> Result<AgentDefinition, Error> {
    let value = AgentDefinition {
        agent_id: id(v.agent_id.map(|v| v.value))?,
        agent_key: AgentKey::new(v.agent_key).map_err(|_| BAD)?,
        system_slot: v.system_slot,
        display_name: v.display_name,
        kind: match v.kind {
            1 => AgentKind::SystemBuiltin,
            2 => AgentKind::Custom,
            3 => AgentKind::Installed,
            _ => return Err(BAD),
        },
        built_in: v.built_in,
        deletable: v.deletable,
        replaceable: v.replaceable,
        default_for_product: v.default_for_product,
        lifecycle: match v.lifecycle {
            1 => AgentLifecycle::Active,
            2 => AgentLifecycle::Retired,
            3 => AgentLifecycle::Deleted,
            _ => return Err(BAD),
        },
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}
pub fn version(v: &AgentVersion) -> Result<wire::PublishedAgentVersionMaterial, Error> {
    v.validate_complete_for_publication().map_err(|_| BAD)?;
    let m = v.material.as_ref().ok_or(BAD)?;
    Ok(wire::PublishedAgentVersionMaterial {
        agent_version_id: Some(wire::AgentVersionId {
            value: v.agent_version_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: v.agent_id.to_string(),
        }),
        version: v.version.get(),
        persona_version: v.persona_version.get(),
        persona_content_ref: Some(encode_content_ref(&v.persona_content_ref)),
        base_skill_refs: v.base_skill_refs.iter().map(versioned).collect(),
        policy_ref: Some(versioned(&v.policy_ref)),
        policy_hash: Some(hash(v.policy_hash)),
        memory_policy_ref: Some(versioned(&v.memory_policy_ref)),
        model_requirement_ref: Some(versioned(&v.model_requirement_ref)),
        supported_runtime_contract: Some(versioned(&v.supported_runtime_contract)),
        release_channel: match v.release_channel {
            ReleaseChannel::Stable => 1,
            ReleaseChannel::Canary => 2,
        },
        published_at_ms: v.published_at.unix_millis(),
        activated_at_ms: v.activated_at.unix_millis(),
        supersedes: v.supersedes.map(|v| wire::AgentVersionId {
            value: v.to_string(),
        }),
        manifest_ref: Some(encode_content_ref(&m.manifest_ref)),
        manifest_hash: Some(hash(m.manifest_hash)),
        policy_content_ref: Some(encode_content_ref(&m.policy_content_ref)),
        memory_policy_content_ref: Some(encode_content_ref(&m.memory_policy_content_ref)),
        memory_policy_hash: Some(hash(m.memory_policy_hash)),
        user_memory_collection_id: m
            .user_memory_collection_id
            .map(|v| wire::MemoryCollectionId {
                value: v.to_string(),
            }),
        long_term_memory_collection_id: m.long_term_memory_collection_id.map(|v| {
            wire::MemoryCollectionId {
                value: v.to_string(),
            }
        }),
    })
}
pub fn decode_version(v: wire::PublishedAgentVersionMaterial) -> Result<AgentVersion, Error> {
    let value = AgentVersion {
        agent_version_id: id(v.agent_version_id.map(|v| v.value))?,
        agent_id: id(v.agent_id.map(|v| v.value))?,
        version: VersionNumber::new(v.version).map_err(|_| BAD)?,
        persona_version: VersionNumber::new(v.persona_version).map_err(|_| BAD)?,
        persona_content_ref: content(required(v.persona_content_ref)?)?,
        base_skill_refs: v
            .base_skill_refs
            .into_iter()
            .map(|v| decode_versioned(Some(v)))
            .collect::<Result<_, _>>()?,
        policy_ref: decode_versioned(v.policy_ref)?,
        policy_hash: decode_hash(v.policy_hash)?,
        memory_policy_ref: decode_versioned(v.memory_policy_ref)?,
        model_requirement_ref: decode_versioned(v.model_requirement_ref)?,
        supported_runtime_contract: decode_versioned(v.supported_runtime_contract)?,
        release_channel: match v.release_channel {
            1 => ReleaseChannel::Stable,
            2 => ReleaseChannel::Canary,
            _ => return Err(BAD),
        },
        published_at: UtcTimestamp::from_unix_millis(v.published_at_ms),
        activated_at: UtcTimestamp::from_unix_millis(v.activated_at_ms),
        supersedes: v.supersedes.map(|v| id(Some(v.value))).transpose()?,
        material: Some(AgentVersionMaterialV1 {
            manifest_ref: content(required(v.manifest_ref)?)?,
            manifest_hash: decode_hash(v.manifest_hash)?,
            policy_content_ref: content(required(v.policy_content_ref)?)?,
            memory_policy_content_ref: content(required(v.memory_policy_content_ref)?)?,
            memory_policy_hash: decode_hash(v.memory_policy_hash)?,
            user_memory_collection_id: v
                .user_memory_collection_id
                .map(|v| id(Some(v.value)))
                .transpose()?,
            long_term_memory_collection_id: v
                .long_term_memory_collection_id
                .map(|v| id(Some(v.value)))
                .transpose()?,
        }),
    };
    value.validate_complete_for_publication().map_err(|_| BAD)?;
    Ok(value)
}
