use musteragent_identity::{
    AgentKey, MemoryCollectionId as DomainMemoryCollectionId, VersionNumber,
};
use musteragent_runtime_snapshot::{
    AgentKind as DomainAgentKind, AgentVersionSnapshot, AgentVersionSnapshotV5, ReleaseChannel,
};

use crate::wire::{AgentId, AgentVersionRef, MemoryCollectionId};

use super::{
    SnapshotCodecError, SnapshotWireSchema, content_ref, decode_content_ref, decode_sha,
    decode_versioned, decode_versioned_refs, parse_id, sha256, versioned_ref, wire,
};

pub(super) fn encode_agent_snapshot(
    schema: SnapshotWireSchema,
    value: &AgentVersionSnapshot,
) -> Result<wire::AgentVersionSnapshot, SnapshotCodecError> {
    let resolved = value
        .resolved_v5
        .as_ref()
        .ok_or(SnapshotCodecError::InvalidContract)?;
    validate_identity(schema, value, resolved)?;
    let resolved_v5 = Some(encode_agent_snapshot_v5(resolved));
    Ok(wire::AgentVersionSnapshot {
        agent_ref: Some(AgentVersionRef {
            agent_id: Some(AgentId {
                value: value.agent_id.to_string(),
            }),
            agent_version_id: Some(wire::AgentVersionId {
                value: value.agent_version_id.to_string(),
            }),
            agent_key: value.agent_key.as_str().to_owned(),
        }),
        persona_version: value.persona_version.get(),
        persona_content_ref: Some(content_ref(&value.persona_content_ref)),
        base_skill_refs: value.base_skill_refs.iter().map(versioned_ref).collect(),
        policy_ref: Some(versioned_ref(&value.policy_ref)),
        resolved_v5,
    })
}

fn encode_agent_snapshot_v5(value: &AgentVersionSnapshotV5) -> wire::AgentVersionSnapshotV5 {
    wire::AgentVersionSnapshotV5 {
        kind: agent_kind(value.kind) as i32,
        version: value.version.get(),
        parent_version_id: value.parent_version_id.map(|parent| wire::AgentVersionId {
            value: parent.to_string(),
        }),
        manifest_ref: Some(content_ref(&value.manifest_ref)),
        manifest_hash: Some(sha256(value.manifest_hash)),
        persona_hash: Some(sha256(value.persona_hash)),
        policy_hash: Some(sha256(value.policy_hash)),
        memory_policy_ref: Some(versioned_ref(&value.memory_policy_ref)),
        memory_policy_hash: Some(sha256(value.memory_policy_hash)),
        model_requirement_ref: Some(versioned_ref(&value.model_requirement_ref)),
        supported_runtime_contract: Some(versioned_ref(&value.supported_runtime_contract)),
        user_memory_collection_id: value.user_memory_collection_id.map(|collection| {
            MemoryCollectionId {
                value: collection.to_string(),
            }
        }),
        long_term_memory_collection_id: value.long_term_memory_collection_id.map(|collection| {
            MemoryCollectionId {
                value: collection.to_string(),
            }
        }),
        release_channel: agent_release_channel(value.release_channel) as i32,
    }
}

pub(super) fn decode_agent_snapshot(
    schema: SnapshotWireSchema,
    value: wire::AgentVersionSnapshot,
) -> Result<AgentVersionSnapshot, SnapshotCodecError> {
    let resolved_v5 = Some(decode_agent_snapshot_v5(
        value
            .resolved_v5
            .ok_or(SnapshotCodecError::InvalidContract)?,
    )?);
    let agent_ref = value.agent_ref.ok_or(SnapshotCodecError::MissingField)?;
    let decoded = AgentVersionSnapshot {
        agent_id: parse_id(
            agent_ref
                .agent_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        agent_key: AgentKey::new(agent_ref.agent_key)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        agent_version_id: parse_id(
            agent_ref
                .agent_version_id
                .ok_or(SnapshotCodecError::MissingField)?
                .value,
        )?,
        persona_version: VersionNumber::new(value.persona_version)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        persona_content_ref: decode_content_ref(
            value
                .persona_content_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        base_skill_refs: decode_versioned_refs(value.base_skill_refs)?,
        policy_ref: decode_versioned(value.policy_ref.ok_or(SnapshotCodecError::MissingField)?)?,
        resolved_v5,
    };
    if let Some(resolved) = decoded.resolved_v5.as_ref() {
        validate_identity(schema, &decoded, resolved)?;
    }
    Ok(decoded)
}

fn validate_identity(
    _schema: SnapshotWireSchema,
    agent: &AgentVersionSnapshot,
    resolved: &AgentVersionSnapshotV5,
) -> Result<(), SnapshotCodecError> {
    if matches!(
        resolved.kind,
        musteragent_definition::AgentKind::Custom | musteragent_definition::AgentKind::Installed
    ) {
        resolved.validate_owned_agent(agent)
    } else {
        resolved.validate_for_agent(agent)
    }
    .map_err(|_| SnapshotCodecError::InvalidContract)
}

fn decode_agent_snapshot_v5(
    value: wire::AgentVersionSnapshotV5,
) -> Result<AgentVersionSnapshotV5, SnapshotCodecError> {
    Ok(AgentVersionSnapshotV5 {
        kind: decode_agent_kind(value.kind)?,
        version: VersionNumber::new(value.version)
            .map_err(|_| SnapshotCodecError::InvalidScalar)?,
        parent_version_id: value
            .parent_version_id
            .map(|parent| parse_id(parent.value))
            .transpose()?,
        manifest_ref: decode_content_ref(
            value.manifest_ref.ok_or(SnapshotCodecError::MissingField)?,
        )?,
        manifest_hash: decode_sha(
            value
                .manifest_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        persona_hash: decode_sha(value.persona_hash.ok_or(SnapshotCodecError::MissingField)?)?,
        policy_hash: decode_sha(value.policy_hash.ok_or(SnapshotCodecError::MissingField)?)?,
        memory_policy_ref: decode_versioned(
            value
                .memory_policy_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        memory_policy_hash: decode_sha(
            value
                .memory_policy_hash
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        model_requirement_ref: decode_versioned(
            value
                .model_requirement_ref
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        supported_runtime_contract: decode_versioned(
            value
                .supported_runtime_contract
                .ok_or(SnapshotCodecError::MissingField)?,
        )?,
        user_memory_collection_id: value
            .user_memory_collection_id
            .map(|collection| parse_id::<DomainMemoryCollectionId>(collection.value))
            .transpose()?,
        long_term_memory_collection_id: value
            .long_term_memory_collection_id
            .map(|collection| parse_id::<DomainMemoryCollectionId>(collection.value))
            .transpose()?,
        release_channel: decode_agent_release_channel(value.release_channel)?,
    })
}

const fn agent_kind(value: DomainAgentKind) -> wire::AgentKind {
    match value {
        DomainAgentKind::SystemBuiltin => wire::AgentKind::SystemBuiltin,
        DomainAgentKind::Custom => wire::AgentKind::Custom,
        DomainAgentKind::Installed => wire::AgentKind::Installed,
    }
}

fn decode_agent_kind(value: i32) -> Result<DomainAgentKind, SnapshotCodecError> {
    match wire::AgentKind::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::AgentKind::SystemBuiltin => Ok(DomainAgentKind::SystemBuiltin),
        wire::AgentKind::Custom => Ok(DomainAgentKind::Custom),
        wire::AgentKind::Installed => Ok(DomainAgentKind::Installed),
        wire::AgentKind::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}

const fn agent_release_channel(value: ReleaseChannel) -> wire::AgentReleaseChannel {
    match value {
        ReleaseChannel::Stable => wire::AgentReleaseChannel::Stable,
        ReleaseChannel::Canary => wire::AgentReleaseChannel::Canary,
    }
}

fn decode_agent_release_channel(value: i32) -> Result<ReleaseChannel, SnapshotCodecError> {
    match wire::AgentReleaseChannel::try_from(value).map_err(|_| SnapshotCodecError::ClosedEnum)? {
        wire::AgentReleaseChannel::Stable => Ok(ReleaseChannel::Stable),
        wire::AgentReleaseChannel::Canary => Ok(ReleaseChannel::Canary),
        wire::AgentReleaseChannel::Unspecified => Err(SnapshotCodecError::ClosedEnum),
    }
}
