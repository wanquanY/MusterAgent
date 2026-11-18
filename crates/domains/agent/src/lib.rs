//! Stable Agent identity, immutable versions and the Agent registry.
//!
//! The kernel ships no built-in Agent. A host registers its Agent definitions,
//! immutable versions (persona, policies, base Skills, model requirement) and
//! published pointers; admission resolves an Agent only through this registry.

#![forbid(unsafe_code)]

pub mod publication;

mod subagent;
use std::collections::BTreeSet;
use std::str::FromStr;
pub use subagent::*;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, ContentId, MemoryCollectionId, PrincipalId, UtcTimestamp,
    VersionNumber, VersionedKey,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Schema of the canonical, byte-stable Agent Version manifest.
pub const AGENT_VERSION_MANIFEST_SCHEMA: &str = "musteragent.agent-version-manifest@1";

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AgentKind {
    SystemBuiltin,
    Custom,
    Installed,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AgentLifecycle {
    Active,
    Retired,
    Deleted,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReleaseChannel {
    Stable,
    Canary,
}

/// Atomic material introduced by Agent Version manifest schema v1. Historical
/// versions may have no material; new commits and forward publication require
/// this complete group.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentVersionMaterialV1 {
    pub manifest_ref: ContentRef,
    pub manifest_hash: Sha256,
    pub policy_content_ref: ContentRef,
    pub memory_policy_content_ref: ContentRef,
    pub memory_policy_hash: Sha256,
    pub user_memory_collection_id: Option<MemoryCollectionId>,
    pub long_term_memory_collection_id: Option<MemoryCollectionId>,
}

impl AgentVersionMaterialV1 {
    pub fn validate(&self, policy_hash: Sha256) -> Result<(), AgentContractError> {
        let zero = [0_u8; Sha256::LENGTH];
        if self.manifest_ref.byte_length == 0
            || self.policy_content_ref.byte_length == 0
            || self.memory_policy_content_ref.byte_length == 0
            || self.manifest_ref.sha256 != self.manifest_hash
            || self.policy_content_ref.sha256 != policy_hash
            || self.memory_policy_content_ref.sha256 != self.memory_policy_hash
            || self.manifest_ref.content_id == self.policy_content_ref.content_id
            || self.manifest_ref.content_id == self.memory_policy_content_ref.content_id
            || self.policy_content_ref.content_id == self.memory_policy_content_ref.content_id
            || self.manifest_hash.into_bytes() == zero
            || policy_hash.into_bytes() == zero
            || self.memory_policy_hash.into_bytes() == zero
            || (self.user_memory_collection_id.is_some()
                && self.user_memory_collection_id == self.long_term_memory_collection_id)
        {
            return Err(AgentContractError::InvalidVersionMaterial);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalAgentVersionManifestV1 {
    schema: String,
    agent_id: String,
    agent_key: String,
    agent_version_id: String,
    version: u64,
    parent_version_id: String,
    kind: String,
    persona: CanonicalPersonaMaterial,
    base_skill_refs: Vec<String>,
    policy: CanonicalPolicyMaterial,
    memory_policy: CanonicalPolicyMaterial,
    memory_collections: CanonicalMemoryCollections,
    model_requirement_ref: String,
    supported_runtime_contract: String,
    release_channel: String,
    published_at_ms: i64,
    activated_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPersonaMaterial {
    version: u64,
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPolicyMaterial {
    #[serde(rename = "ref")]
    version_ref: String,
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalMemoryCollections {
    user: Option<String>,
    long_term: Option<String>,
}

impl CanonicalAgentVersionManifestV1 {
    /// Re-serializes with the sole canonical encoding: compact ordered JSON and
    /// exactly one trailing LF.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, AgentContractError> {
        let mut bytes = serde_json::to_vec(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Builds the executable version. `manifest_ref` is the content identity
    /// under which the host stored these exact canonical manifest bytes.
    pub fn to_agent_version(
        &self,
        manifest_ref: ContentRef,
    ) -> Result<AgentVersion, AgentContractError> {
        let persona = content_ref(
            &self.persona.content_id,
            &self.persona.media_type,
            self.persona.byte_length,
            &self.persona.sha256,
        )?;
        let policy = content_ref(
            &self.policy.content_id,
            &self.policy.media_type,
            self.policy.byte_length,
            &self.policy.sha256,
        )?;
        let memory_policy = content_ref(
            &self.memory_policy.content_id,
            &self.memory_policy.media_type,
            self.memory_policy.byte_length,
            &self.memory_policy.sha256,
        )?;
        let version = AgentVersion {
            agent_version_id: AgentVersionId::from_str(&self.agent_version_id)?,
            agent_id: AgentId::from_str(&self.agent_id)?,
            version: VersionNumber::new(self.version)?,
            persona_version: VersionNumber::new(self.persona.version)?,
            persona_content_ref: persona,
            base_skill_refs: self
                .base_skill_refs
                .iter()
                .map(VersionedKey::new)
                .collect::<Result<_, _>>()?,
            policy_ref: VersionedKey::new(&self.policy.version_ref)?,
            policy_hash: policy.sha256,
            memory_policy_ref: VersionedKey::new(&self.memory_policy.version_ref)?,
            model_requirement_ref: VersionedKey::new(&self.model_requirement_ref)?,
            supported_runtime_contract: VersionedKey::new(&self.supported_runtime_contract)?,
            release_channel: ReleaseChannel::Stable,
            published_at: UtcTimestamp::from_unix_millis(self.published_at_ms),
            activated_at: UtcTimestamp::from_unix_millis(self.activated_at_ms),
            supersedes: Some(AgentVersionId::from_str(&self.parent_version_id)?),
            material: Some(AgentVersionMaterialV1 {
                manifest_hash: manifest_ref.sha256,
                manifest_ref,
                policy_content_ref: policy,
                memory_policy_hash: memory_policy.sha256,
                memory_policy_content_ref: memory_policy,
                user_memory_collection_id: self
                    .memory_collections
                    .user
                    .as_deref()
                    .map(MemoryCollectionId::from_str)
                    .transpose()?,
                long_term_memory_collection_id: self
                    .memory_collections
                    .long_term
                    .as_deref()
                    .map(MemoryCollectionId::from_str)
                    .transpose()?,
            }),
        };
        version.validate()?;
        Ok(version)
    }
}

/// Parses one canonical Agent Version manifest. The bytes must be exactly the
/// canonical encoding (sorted, compact JSON followed by one newline) so their
/// digest is a stable identity across Cloud and Device distributions.
pub fn parse_agent_version_manifest(
    bytes: &[u8],
    manifest_content_id: ContentId,
) -> Result<(CanonicalAgentVersionManifestV1, AgentVersion), AgentContractError> {
    let payload = bytes
        .strip_suffix(b"\n")
        .ok_or(AgentContractError::NonCanonicalManifestBytes)?;
    let manifest: CanonicalAgentVersionManifestV1 = serde_json::from_slice(payload)?;
    if manifest.canonical_bytes()? != bytes {
        return Err(AgentContractError::NonCanonicalManifestBytes);
    }
    if manifest.schema != AGENT_VERSION_MANIFEST_SCHEMA {
        return Err(AgentContractError::InvalidCanonicalManifest);
    }
    let manifest_ref = ContentRef {
        content_id: manifest_content_id,
        media_type: MediaType::new("application/json")?,
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    };
    let version = manifest.to_agent_version(manifest_ref)?;
    Ok((manifest, version))
}

fn content_ref(
    content_id: &str,
    media_type: &str,
    byte_length: u64,
    sha256: &str,
) -> Result<ContentRef, AgentContractError> {
    Ok(ContentRef {
        content_id: ContentId::from_str(content_id)?,
        media_type: MediaType::new(media_type)?,
        byte_length,
        sha256: sha256_from_hex(sha256)?,
    })
}

fn sha256_from_hex(value: &str) -> Result<Sha256, AgentContractError> {
    if value.len() != Sha256::LENGTH * 2 {
        return Err(AgentContractError::InvalidCanonicalManifest);
    }
    let mut bytes = [0_u8; Sha256::LENGTH];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| AgentContractError::InvalidCanonicalManifest)?;
    }
    Ok(Sha256::from_bytes(bytes).expect("fixed SHA-256 width"))
}

/// Stable logical Agent identity. Executable behavior belongs to a version.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentDefinition {
    pub agent_id: AgentId,
    pub agent_key: AgentKey,
    pub system_slot: Option<String>,
    pub display_name: String,
    pub kind: AgentKind,
    pub built_in: bool,
    pub deletable: bool,
    pub replaceable: bool,
    pub default_for_product: bool,
    pub lifecycle: AgentLifecycle,
}

impl AgentDefinition {
    pub fn validate(&self) -> Result<(), AgentContractError> {
        if self.display_name.trim().is_empty() || self.display_name.len() > 120 {
            return Err(AgentContractError::InvalidDisplayName);
        }
        match self.kind {
            AgentKind::SystemBuiltin => {
                if self.system_slot.as_deref().is_none_or(str::is_empty)
                    || !self.built_in
                    || self.deletable
                {
                    return Err(AgentContractError::InvalidBuiltinDefinition);
                }
            }
            AgentKind::Custom | AgentKind::Installed => {
                if self.built_in || self.system_slot.is_some() || self.default_for_product {
                    return Err(AgentContractError::InvalidCustomDefinition);
                }
            }
        }
        Ok(())
    }
}

/// Immutable executable version shared by Cloud and Device distributions.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentVersion {
    pub agent_version_id: AgentVersionId,
    pub agent_id: AgentId,
    pub version: VersionNumber,
    pub persona_version: VersionNumber,
    pub persona_content_ref: ContentRef,
    pub base_skill_refs: Vec<VersionedKey>,
    pub policy_ref: VersionedKey,
    pub policy_hash: Sha256,
    pub memory_policy_ref: VersionedKey,
    pub model_requirement_ref: VersionedKey,
    pub supported_runtime_contract: VersionedKey,
    pub release_channel: ReleaseChannel,
    pub published_at: UtcTimestamp,
    pub activated_at: UtcTimestamp,
    pub supersedes: Option<AgentVersionId>,
    pub material: Option<AgentVersionMaterialV1>,
}

impl AgentVersion {
    pub fn validate(&self) -> Result<(), AgentContractError> {
        if self.base_skill_refs.is_empty()
            || self
                .base_skill_refs
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(AgentContractError::BaseSkillsNotCanonical);
        }
        if self.activated_at < self.published_at {
            return Err(AgentContractError::ActivationBeforePublication);
        }
        if self.persona_content_ref.byte_length == 0 {
            return Err(AgentContractError::EmptyPersonaContent);
        }
        if self.policy_hash.into_bytes() == [0_u8; Sha256::LENGTH] {
            return Err(AgentContractError::InvalidVersionMaterial);
        }
        if let Some(material) = &self.material {
            material.validate(self.policy_hash)?;
            if self.persona_content_ref.content_id == material.manifest_ref.content_id
                || self.persona_content_ref.content_id == material.policy_content_ref.content_id
                || self.persona_content_ref.content_id
                    == material.memory_policy_content_ref.content_id
            {
                return Err(AgentContractError::InvalidVersionMaterial);
            }
        }
        match self.supersedes {
            None if self.version.get() != 1 => {
                return Err(AgentContractError::ParentVersionRequired);
            }
            Some(parent) if parent == self.agent_version_id => {
                return Err(AgentContractError::SelfReferentialVersion);
            }
            Some(_) if self.version.get() == 1 => {
                return Err(AgentContractError::RootVersionHasParent);
            }
            None | Some(_) => {}
        }
        Ok(())
    }

    pub fn validate_complete_for_publication(&self) -> Result<(), AgentContractError> {
        self.validate()?;
        self.material
            .as_ref()
            .ok_or(AgentContractError::IncompleteVersionMaterial)?
            .validate(self.policy_hash)
    }

    pub fn validate_for_definition(
        &self,
        definition: &AgentDefinition,
    ) -> Result<(), AgentContractError> {
        definition.validate()?;
        self.validate()?;
        if self.agent_id != definition.agent_id {
            return Err(AgentContractError::VersionDefinitionMismatch);
        }
        if definition.kind == AgentKind::SystemBuiltin
            && self.material.as_ref().is_some_and(|material| {
                material.user_memory_collection_id.is_some()
                    || material.long_term_memory_collection_id.is_some()
            })
        {
            return Err(AgentContractError::InvalidBuiltinMemoryCollection);
        }
        Ok(())
    }

    pub fn validate_child_of(&self, parent: &Self) -> Result<(), AgentContractError> {
        self.validate()?;
        parent.validate()?;
        let expected_version = parent
            .version
            .get()
            .checked_add(1)
            .ok_or(AgentContractError::VersionSequenceOverflow)?;
        if self.agent_id != parent.agent_id
            || self.supersedes != Some(parent.agent_version_id)
            || self.version.get() != expected_version
        {
            return Err(AgentContractError::InvalidVersionLineage);
        }
        Ok(())
    }
}

/// The single mutable production fact for an otherwise immutable Agent lineage.
/// `version` is the monotonic CAS guard and is distinct from AgentVersion.version.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PublishedAgentVersionPointer {
    pub agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub version: u64,
    pub published_at: UtcTimestamp,
    pub published_by: PrincipalId,
}

impl PublishedAgentVersionPointer {
    pub fn validate(
        &self,
        definition: &AgentDefinition,
        version: &AgentVersion,
    ) -> Result<(), AgentContractError> {
        definition.validate()?;
        version.validate_for_definition(definition)?;
        if self.version == 0 {
            return Err(AgentContractError::PublishedPointerVersionZero);
        }
        if self.agent_id != definition.agent_id
            || self.agent_version_id != version.agent_version_id
            || self.published_at != version.published_at
        {
            return Err(AgentContractError::PublishedPointerMismatch);
        }
        Ok(())
    }
}

/// Runtime location changes provider availability, never Agent identity.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DistributionTarget {
    Cloud,
    Device,
}

/// Closed in-memory registry view used by admission and bootstrap validation.
#[derive(Debug, Clone)]
pub struct AgentRegistry {
    definitions: Vec<AgentDefinition>,
    versions: Vec<AgentVersion>,
    published: Vec<PublishedAgentVersionPointer>,
}

impl AgentRegistry {
    pub fn new(
        definitions: Vec<AgentDefinition>,
        versions: Vec<AgentVersion>,
        published: Vec<PublishedAgentVersionPointer>,
    ) -> Result<Self, AgentContractError> {
        for definition in &definitions {
            definition.validate()?;
        }
        let ids = definitions
            .iter()
            .map(|definition| definition.agent_id)
            .collect::<BTreeSet<_>>();
        let keys = definitions
            .iter()
            .map(|definition| definition.agent_key.clone())
            .collect::<BTreeSet<_>>();
        if ids.len() != definitions.len() || keys.len() != definitions.len() {
            return Err(AgentContractError::DuplicateDefinition);
        }

        let version_ids = versions
            .iter()
            .map(|version| version.agent_version_id)
            .collect::<BTreeSet<_>>();
        let version_numbers = versions
            .iter()
            .map(|version| (version.agent_id, version.version.get()))
            .collect::<BTreeSet<_>>();
        if version_ids.len() != versions.len() || version_numbers.len() != versions.len() {
            return Err(AgentContractError::DuplicateVersion);
        }
        for version in &versions {
            let definition = definitions
                .iter()
                .find(|definition| definition.agent_id == version.agent_id)
                .ok_or(AgentContractError::OrphanVersion)?;
            version.validate_for_definition(definition)?;
            if let Some(parent_id) = version.supersedes {
                let parent = versions
                    .iter()
                    .find(|candidate| candidate.agent_version_id == parent_id)
                    .ok_or(AgentContractError::ParentVersionUnavailable)?;
                version.validate_child_of(parent)?;
            }
        }

        let pointer_agents = published
            .iter()
            .map(|pointer| pointer.agent_id)
            .collect::<BTreeSet<_>>();
        if pointer_agents.len() != published.len() {
            return Err(AgentContractError::DuplicatePublishedPointer);
        }
        for pointer in &published {
            let definition = definitions
                .iter()
                .find(|definition| definition.agent_id == pointer.agent_id)
                .ok_or(AgentContractError::PublishedPointerMismatch)?;
            if definition.lifecycle != AgentLifecycle::Active {
                return Err(AgentContractError::PublishedPointerForInactiveAgent);
            }
            let version = versions
                .iter()
                .find(|version| version.agent_version_id == pointer.agent_version_id)
                .ok_or(AgentContractError::PublishedVersionUnavailable)?;
            pointer.validate(definition, version)?;
        }
        for definition in &definitions {
            let pointer_count = published
                .iter()
                .filter(|pointer| pointer.agent_id == definition.agent_id)
                .count();
            let expected = usize::from(definition.lifecycle == AgentLifecycle::Active);
            if pointer_count != expected {
                return Err(AgentContractError::MissingPublishedPointer);
            }
        }

        Ok(Self {
            definitions,
            versions,
            published,
        })
    }

    pub fn with_published_pointers(
        definitions: Vec<AgentDefinition>,
        versions: Vec<AgentVersion>,
        published: Vec<PublishedAgentVersionPointer>,
    ) -> Result<Self, AgentContractError> {
        Self::new(definitions, versions, published)
    }

    #[must_use]
    pub fn system_builtin_count(&self) -> usize {
        self.definitions
            .iter()
            .filter(|definition| definition.kind == AgentKind::SystemBuiltin)
            .count()
    }

    /// Resolves only the explicit Published Pointer. Runtime location is not
    /// allowed to select a different Agent identity or version.
    pub fn resolve(
        &self,
        agent_key: &AgentKey,
        _target: DistributionTarget,
    ) -> Result<(&AgentDefinition, &AgentVersion), AgentContractError> {
        let definition = self
            .definitions
            .iter()
            .find(|definition| {
                definition.lifecycle == AgentLifecycle::Active && definition.agent_key == *agent_key
            })
            .ok_or(AgentContractError::AgentUnavailable)?;
        let pointer = self
            .published
            .iter()
            .find(|pointer| pointer.agent_id == definition.agent_id)
            .ok_or(AgentContractError::MissingPublishedPointer)?;
        let version = self
            .versions
            .iter()
            .find(|version| version.agent_version_id == pointer.agent_version_id)
            .ok_or(AgentContractError::PublishedVersionUnavailable)?;
        pointer.validate(definition, version)?;
        Ok((definition, version))
    }

    /// Resolves a fixed immutable version for recovery without consulting the
    /// current Published Pointer.
    pub fn resolve_version(
        &self,
        agent_key: &AgentKey,
        agent_version_id: AgentVersionId,
        _target: DistributionTarget,
    ) -> Result<(&AgentDefinition, &AgentVersion), AgentContractError> {
        let definition = self
            .definitions
            .iter()
            .find(|definition| definition.agent_key == *agent_key)
            .ok_or(AgentContractError::AgentUnavailable)?;
        let version = self
            .versions
            .iter()
            .find(|version| version.agent_version_id == agent_version_id)
            .ok_or(AgentContractError::VersionUnavailable)?;
        version.validate_for_definition(definition)?;
        Ok((definition, version))
    }
}

/// Stable Agent definition reference.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentRef {
    pub agent_id: AgentId,
    pub agent_key: AgentKey,
}

/// Immutable executable Agent version reference.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentVersionRef {
    pub agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub agent_key: AgentKey,
}

#[derive(Debug, Error)]
pub enum AgentContractError {
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    Key(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    Scalar(#[from] musteragent_identity::NonZeroScalarError),
    #[error(transparent)]
    MediaType(#[from] musteragent_content_ref::MediaTypeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Agent display name is empty or too long")]
    InvalidDisplayName,
    #[error("the built-in Agent contract needs a system slot and must not be deletable")]
    InvalidBuiltinDefinition,
    #[error("custom/installed Agents cannot occupy the built-in system slot")]
    InvalidCustomDefinition,
    #[error("base Skill refs must be non-empty, unique and sorted")]
    BaseSkillsNotCanonical,
    #[error("Agent version activation cannot precede publication")]
    ActivationBeforePublication,
    #[error("Agent persona content cannot be empty")]
    EmptyPersonaContent,
    #[error("Agent version material is missing from a newly published version")]
    IncompleteVersionMaterial,
    #[error("Agent version material is partial, inconsistent or non-canonical")]
    InvalidVersionMaterial,
    #[error("a built-in Agent cannot pin a tenant Memory Collection")]
    InvalidBuiltinMemoryCollection,
    #[error("the Agent Version manifest has invalid fields")]
    InvalidCanonicalManifest,
    #[error("Agent Version manifest bytes are not canonical compact JSON plus one LF")]
    NonCanonicalManifestBytes,
    #[error("Agent version numbers above one require an immutable parent")]
    ParentVersionRequired,
    #[error("Agent version one cannot declare a parent")]
    RootVersionHasParent,
    #[error("Agent version cannot reference itself as its parent")]
    SelfReferentialVersion,
    #[error("Agent version does not belong to the referenced definition")]
    VersionDefinitionMismatch,
    #[error("Agent version parent, identity or number is not a linear successor")]
    InvalidVersionLineage,
    #[error("Agent version sequence overflowed")]
    VersionSequenceOverflow,
    #[error("Agent version references an unknown definition")]
    OrphanVersion,
    #[error("Agent version references an unavailable parent")]
    ParentVersionUnavailable,
    #[error("Agent version ID or per-Agent version number is duplicated")]
    DuplicateVersion,
    #[error("Agent definition ID or key is duplicated")]
    DuplicateDefinition,
    #[error("the active built-in Agent cardinality must equal one")]
    BuiltinCardinality,
    #[error("active Agent is missing its explicit Published Pointer")]
    MissingPublishedPointer,
    #[error("Agent has more than one Published Pointer")]
    DuplicatePublishedPointer,
    #[error("inactive Agent cannot retain a Published Pointer")]
    PublishedPointerForInactiveAgent,
    #[error("Published Pointer CAS version must be greater than zero")]
    PublishedPointerVersionZero,
    #[error("Published Pointer does not match its Agent definition and immutable version")]
    PublishedPointerMismatch,
    #[error("Published Pointer references an unavailable Agent version")]
    PublishedVersionUnavailable,
    #[error("Agent definition is unavailable")]
    AgentUnavailable,
    #[error("fixed Agent version is unavailable")]
    VersionUnavailable,
    #[error("AGENT_PROFILE_RETIRED")]
    AgentProfileRetired,
}

mod storage;
pub use storage::*;

/// Account-owned Agent configuration independent of execution placement.
pub mod owned_agent;
