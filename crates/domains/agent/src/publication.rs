//! Canonical material for account-owned Custom/Installed definitions. Builtin
//! manifests keep their frozen codecs; this is a distinct publication schema.
use crate::{
    AgentContractError, AgentDefinition, AgentKind, AgentLifecycle, AgentVersion,
    AgentVersionMaterialV1, ReleaseChannel,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AgentId, AgentVersionId, ContentId, MemoryCollectionId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use serde::Serialize;
use thiserror::Error;

pub const OWNED_AGENT_MANIFEST_SCHEMA: &str = "musteragent.owned-agent-manifest@1";
pub const MAX_OWNED_AGENT_MATERIAL_BYTES: usize = 1024 * 1024;

/// No manifest hash is supplied by the caller: it is derived from the complete
/// immutable identity and references below, then persisted at manifest_id.
#[derive(Debug, Clone)]
pub struct OwnedAgentVersionDraft {
    pub agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub version: VersionNumber,
    pub supersedes: Option<AgentVersionId>,
    pub persona_version: VersionNumber,
    pub persona_content_ref: ContentRef,
    pub base_skill_refs: Vec<VersionedKey>,
    pub policy_ref: VersionedKey,
    pub policy_content_ref: ContentRef,
    pub memory_policy_ref: VersionedKey,
    pub memory_policy_content_ref: ContentRef,
    pub user_memory_collection_id: Option<MemoryCollectionId>,
    pub long_term_memory_collection_id: Option<MemoryCollectionId>,
    pub model_requirement_ref: VersionedKey,
    pub supported_runtime_contract: VersionedKey,
    pub release_channel: ReleaseChannel,
    pub published_at: UtcTimestamp,
    pub activated_at: UtcTimestamp,
}

#[derive(Debug, Clone)]
pub struct PreparedOwnedAgentVersion {
    pub version: AgentVersion,
    pub manifest_bytes: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum OwnedAgentQualificationError {
    #[error(transparent)]
    Contract(#[from] AgentContractError),
    #[error("publication requires an active account-owned Agent identity")]
    InvalidIdentity,
    #[error("Agent material does not match its canonical immutable reference")]
    InvalidMaterial,
    #[error("Agent material is not bounded UTF-8 text")]
    InvalidText,
    #[error("account-owned Agent manifest is not canonical for this version")]
    InvalidManifest,
    #[error("account-owned Agent manifest encoding failed")]
    Encoding,
}

#[derive(Serialize)]
struct Manifest<'a> {
    schema: &'static str,
    agent_id: String,
    agent_key: &'a str,
    agent_version_id: String,
    version: u64,
    parent_version_id: Option<String>,
    kind: &'static str,
    persona_version: u64,
    persona: Content<'a>,
    base_skill_refs: Vec<&'a str>,
    policy_ref: &'a str,
    policy: Content<'a>,
    memory_policy_ref: &'a str,
    memory_policy: Content<'a>,
    user_memory_collection_id: Option<String>,
    long_term_memory_collection_id: Option<String>,
    model_requirement_ref: &'a str,
    supported_runtime_contract: &'a str,
    release_channel: &'static str,
    published_at_ms: i64,
    activated_at_ms: i64,
}
#[derive(Serialize)]
struct Content<'a> {
    content_id: String,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
}
impl<'a> From<&'a ContentRef> for Content<'a> {
    fn from(value: &'a ContentRef) -> Self {
        Self {
            content_id: value.content_id.to_string(),
            media_type: value.media_type.as_str(),
            byte_length: value.byte_length,
            sha256: value.sha256.to_string(),
        }
    }
}

impl OwnedAgentVersionDraft {
    fn bytes(&self, definition: &AgentDefinition) -> Result<Vec<u8>, OwnedAgentQualificationError> {
        validate_identity(definition)?;
        if definition.agent_id != self.agent_id {
            return Err(OwnedAgentQualificationError::InvalidIdentity);
        }
        let value = Manifest {
            schema: OWNED_AGENT_MANIFEST_SCHEMA,
            agent_id: self.agent_id.to_string(),
            agent_key: definition.agent_key.as_str(),
            agent_version_id: self.agent_version_id.to_string(),
            version: self.version.get(),
            parent_version_id: self.supersedes.map(|value| value.to_string()),
            kind: match definition.kind {
                AgentKind::Custom => "custom",
                AgentKind::Installed => "installed",
                AgentKind::SystemBuiltin => {
                    return Err(OwnedAgentQualificationError::InvalidIdentity);
                }
            },
            persona_version: self.persona_version.get(),
            persona: (&self.persona_content_ref).into(),
            base_skill_refs: self
                .base_skill_refs
                .iter()
                .map(VersionedKey::as_str)
                .collect(),
            policy_ref: self.policy_ref.as_str(),
            policy: (&self.policy_content_ref).into(),
            memory_policy_ref: self.memory_policy_ref.as_str(),
            memory_policy: (&self.memory_policy_content_ref).into(),
            user_memory_collection_id: self
                .user_memory_collection_id
                .map(|value| value.to_string()),
            long_term_memory_collection_id: self
                .long_term_memory_collection_id
                .map(|value| value.to_string()),
            model_requirement_ref: self.model_requirement_ref.as_str(),
            supported_runtime_contract: self.supported_runtime_contract.as_str(),
            release_channel: match self.release_channel {
                ReleaseChannel::Stable => "stable",
                ReleaseChannel::Canary => "canary",
            },
            published_at_ms: self.published_at.unix_millis(),
            activated_at_ms: self.activated_at.unix_millis(),
        };
        let mut bytes =
            serde_json::to_vec(&value).map_err(|_| OwnedAgentQualificationError::Encoding)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    pub fn prepare(
        self,
        definition: &AgentDefinition,
        manifest_id: ContentId,
    ) -> Result<PreparedOwnedAgentVersion, OwnedAgentQualificationError> {
        let manifest_bytes = self.bytes(definition)?;
        let manifest_ref = ContentRef {
            content_id: manifest_id,
            media_type: MediaType::new("application/json")
                .map_err(|_| OwnedAgentQualificationError::InvalidMaterial)?,
            byte_length: manifest_bytes.len() as u64,
            sha256: Sha256::digest(&manifest_bytes),
        };
        let version = AgentVersion {
            agent_id: self.agent_id,
            agent_version_id: self.agent_version_id,
            version: self.version,
            supersedes: self.supersedes,
            persona_version: self.persona_version,
            persona_content_ref: self.persona_content_ref,
            base_skill_refs: self.base_skill_refs,
            policy_ref: self.policy_ref,
            policy_hash: self.policy_content_ref.sha256,
            memory_policy_ref: self.memory_policy_ref,
            model_requirement_ref: self.model_requirement_ref,
            supported_runtime_contract: self.supported_runtime_contract,
            release_channel: self.release_channel,
            published_at: self.published_at,
            activated_at: self.activated_at,
            material: Some(AgentVersionMaterialV1 {
                manifest_hash: manifest_ref.sha256,
                manifest_ref,
                policy_content_ref: self.policy_content_ref,
                memory_policy_hash: self.memory_policy_content_ref.sha256,
                memory_policy_content_ref: self.memory_policy_content_ref,
                user_memory_collection_id: self.user_memory_collection_id,
                long_term_memory_collection_id: self.long_term_memory_collection_id,
            }),
        };
        version.validate_for_definition(definition)?;
        version.validate_complete_for_publication()?;
        Ok(PreparedOwnedAgentVersion {
            version,
            manifest_bytes,
        })
    }

    pub fn from_version(version: &AgentVersion) -> Result<Self, OwnedAgentQualificationError> {
        version.validate_complete_for_publication()?;
        let material = version
            .material
            .as_ref()
            .ok_or(OwnedAgentQualificationError::InvalidMaterial)?;
        Ok(Self {
            agent_id: version.agent_id,
            agent_version_id: version.agent_version_id,
            version: version.version,
            supersedes: version.supersedes,
            persona_version: version.persona_version,
            persona_content_ref: version.persona_content_ref.clone(),
            base_skill_refs: version.base_skill_refs.clone(),
            policy_ref: version.policy_ref.clone(),
            policy_content_ref: material.policy_content_ref.clone(),
            memory_policy_ref: version.memory_policy_ref.clone(),
            memory_policy_content_ref: material.memory_policy_content_ref.clone(),
            user_memory_collection_id: material.user_memory_collection_id,
            long_term_memory_collection_id: material.long_term_memory_collection_id,
            model_requirement_ref: version.model_requirement_ref.clone(),
            supported_runtime_contract: version.supported_runtime_contract.clone(),
            release_channel: version.release_channel,
            published_at: version.published_at,
            activated_at: version.activated_at,
        })
    }
}

pub fn validate_identity(definition: &AgentDefinition) -> Result<(), OwnedAgentQualificationError> {
    definition.validate()?;
    if !matches!(definition.kind, AgentKind::Custom | AgentKind::Installed)
        || definition.lifecycle != AgentLifecycle::Active
    {
        return Err(OwnedAgentQualificationError::InvalidIdentity);
    }
    Ok(())
}

/// Reconstructing exact canonical bytes from an authority-owned definition and
/// version rejects unknown fields, alternate encodings and another identity's
/// otherwise valid manifest. Parent existence is separately checked by Registry.
pub fn verify_dialogue_manifest(
    definition: &AgentDefinition,
    version: &AgentVersion,
    bytes: &[u8],
) -> Result<(), OwnedAgentQualificationError> {
    version.validate_for_definition(definition)?;
    let draft = OwnedAgentVersionDraft::from_version(version)?;
    let material = version
        .material
        .as_ref()
        .ok_or(OwnedAgentQualificationError::InvalidMaterial)?;
    verify_bytes(&material.manifest_ref, bytes)?;
    if bytes != draft.bytes(definition)? {
        return Err(OwnedAgentQualificationError::InvalidManifest);
    }
    Ok(())
}

pub fn verify_bytes(
    reference: &ContentRef,
    bytes: &[u8],
) -> Result<(), OwnedAgentQualificationError> {
    if bytes.is_empty()
        || bytes.len() > MAX_OWNED_AGENT_MATERIAL_BYTES
        || bytes.len() as u64 != reference.byte_length
        || Sha256::digest(bytes) != reference.sha256
    {
        return Err(OwnedAgentQualificationError::InvalidMaterial);
    }
    Ok(())
}

pub fn verify_text(
    reference: &ContentRef,
    bytes: &[u8],
) -> Result<(), OwnedAgentQualificationError> {
    verify_bytes(reference, bytes)?;
    if !matches!(
        reference.media_type.as_str(),
        "text/plain" | "text/markdown" | "text/plain;charset=utf-8" | "text/markdown;charset=utf-8"
    ) || std::str::from_utf8(bytes).is_err()
        || bytes.contains(&0)
    {
        return Err(OwnedAgentQualificationError::InvalidText);
    }
    Ok(())
}
