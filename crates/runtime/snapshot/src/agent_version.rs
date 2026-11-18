use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_definition::{AgentDefinition, AgentVersion};
use musteragent_identity::{
    AgentId, AgentKey, AgentVersionId, MemoryCollectionId, VersionNumber, VersionedKey,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_skill::{ActivationPolicy, SkillDefinition};

use crate::{
    AgentKind, LEGACY_PERSONA_PROMPT_SECTION, MUSTERAGENT_ASSISTANT_PROMPT_SECTION, PolicySnapshot,
    PromptAssemblySnapshot, ReleaseChannel, SnapshotContractError,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentVersionSnapshot {
    pub agent_id: AgentId,
    pub agent_key: AgentKey,
    pub agent_version_id: AgentVersionId,
    pub persona_version: VersionNumber,
    pub persona_content_ref: ContentRef,
    pub base_skill_refs: Vec<VersionedKey>,
    pub policy_ref: VersionedKey,
    /// Complete immutable Agent material; required by the current snapshot.
    /// Option represents missing wire material, which validation rejects.
    pub resolved_v5: Option<AgentVersionSnapshotV5>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentVersionSnapshotV5 {
    pub kind: AgentKind,
    pub version: VersionNumber,
    pub parent_version_id: Option<AgentVersionId>,
    pub manifest_ref: ContentRef,
    pub manifest_hash: Sha256,
    pub persona_hash: Sha256,
    pub policy_hash: Sha256,
    pub memory_policy_ref: VersionedKey,
    pub memory_policy_hash: Sha256,
    pub model_requirement_ref: VersionedKey,
    pub supported_runtime_contract: VersionedKey,
    pub user_memory_collection_id: Option<MemoryCollectionId>,
    pub long_term_memory_collection_id: Option<MemoryCollectionId>,
    pub release_channel: ReleaseChannel,
}

impl AgentVersionSnapshotV5 {
    /// Validates the self-contained v5 AgentVersion identity before any
    /// Runtime-wide policy, Memory or Skill projection is consulted.
    pub fn validate_for_agent(
        &self,
        agent: &AgentVersionSnapshot,
    ) -> Result<(), SnapshotContractError> {
        self.validate_identity(agent, false)
    }

    pub fn validate_owned_agent(
        &self,
        agent: &AgentVersionSnapshot,
    ) -> Result<(), SnapshotContractError> {
        self.validate_identity(agent, true)
    }

    fn validate_identity(
        &self,
        agent: &AgentVersionSnapshot,
        owned_agent: bool,
    ) -> Result<(), SnapshotContractError> {
        let valid_kind = if owned_agent {
            matches!(self.kind, AgentKind::Custom | AgentKind::Installed)
        } else {
            self.kind == AgentKind::SystemBuiltin
        };
        let zero = [0_u8; Sha256::LENGTH];
        let invalid_parent = match (self.version.get(), self.parent_version_id) {
            (1, None) => false,
            (1, Some(_)) | (_, None) => true,
            (_, Some(parent)) => parent == agent.agent_version_id,
        };
        let duplicate_collections = self.user_memory_collection_id.is_some()
            && self.user_memory_collection_id == self.long_term_memory_collection_id;
        if !valid_kind
            || invalid_parent
            || duplicate_collections
            || self.manifest_ref.byte_length == 0
            || self.manifest_hash != self.manifest_ref.sha256
            || self.persona_hash != agent.persona_content_ref.sha256
            || agent.persona_content_ref.byte_length == 0
            || self.manifest_hash.into_bytes() == zero
            || self.persona_hash.into_bytes() == zero
            || self.policy_hash.into_bytes() == zero
            || self.memory_policy_hash.into_bytes() == zero
        {
            return Err(SnapshotContractError::InvalidAgentVersionSnapshot);
        }
        Ok(())
    }
}

impl AgentVersionSnapshot {
    /// Projects one fully validated immutable Agent version into the v5
    /// executable Snapshot contract. Content bytes are verified by admission
    /// before this pure projection is called.
    pub fn from_resolved(
        definition: &AgentDefinition,
        version: &AgentVersion,
    ) -> Result<Self, SnapshotContractError> {
        definition.validate()?;
        version.validate_for_definition(definition)?;
        version.validate_complete_for_publication()?;
        let material = version
            .material
            .as_ref()
            .ok_or(SnapshotContractError::InvalidAgentVersionSnapshot)?;

        Ok(Self {
            agent_id: version.agent_id,
            agent_key: definition.agent_key.clone(),
            agent_version_id: version.agent_version_id,
            persona_version: version.persona_version,
            persona_content_ref: version.persona_content_ref.clone(),
            base_skill_refs: version.base_skill_refs.clone(),
            policy_ref: version.policy_ref.clone(),
            resolved_v5: Some(AgentVersionSnapshotV5 {
                kind: definition.kind,
                version: version.version,
                parent_version_id: version.supersedes,
                manifest_ref: material.manifest_ref.clone(),
                manifest_hash: material.manifest_hash,
                persona_hash: version.persona_content_ref.sha256,
                policy_hash: version.policy_hash,
                memory_policy_ref: version.memory_policy_ref.clone(),
                memory_policy_hash: material.memory_policy_hash,
                model_requirement_ref: version.model_requirement_ref.clone(),
                supported_runtime_contract: version.supported_runtime_contract.clone(),
                user_memory_collection_id: material.user_memory_collection_id,
                long_term_memory_collection_id: material.long_term_memory_collection_id,
                release_channel: version.release_channel,
            }),
        })
    }
}

pub(crate) fn validate_agent_v5_projection(
    agent: &AgentVersionSnapshot,
    resolved: &AgentVersionSnapshotV5,
    prompt: &PromptAssemblySnapshot,
    policy: &PolicySnapshot,
    memory: &MemorySelectionSnapshot,
    skills: &[SkillDefinition],
    owned_agent: bool,
) -> Result<(), SnapshotContractError> {
    if owned_agent {
        resolved.validate_owned_agent(agent)?;
    } else {
        resolved.validate_for_agent(agent)?;
    }
    let base_skill_refs = skills
        .iter()
        .filter(|skill| skill.activation_policy == ActivationPolicy::Base)
        .map(|skill| skill.skill_ref.clone())
        .collect::<Vec<_>>();
    let safety_policy_ref = prompt
        .sections
        .iter()
        .find(|section| section.section_key.as_str() == "platform.safety")
        .map(|section| &section.content_ref);
    let assistant_ref = assistant_prompt_ref(prompt)?;

    if agent.policy_ref != policy.policy_ref
        || resolved.policy_hash != policy.policy_hash
        || safety_policy_ref.is_none_or(|reference| reference.sha256 != resolved.policy_hash)
        || assistant_ref != &agent.persona_content_ref
        || resolved.memory_policy_ref != memory.selection_policy_ref
        || resolved.memory_policy_hash != memory.selection_policy_hash
        || agent.base_skill_refs.is_empty()
        || agent
            .base_skill_refs
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || agent.base_skill_refs != base_skill_refs
        || skills.iter().any(|skill| {
            !skill
                .compatible_agent_contracts
                .contains(&resolved.supported_runtime_contract)
        })
    {
        return Err(SnapshotContractError::InvalidAgentVersionSnapshot);
    }
    Ok(())
}

/// Validates the same immutable AgentVersion identity for a private Runtime
/// evaluation without requiring an executable Skill catalog. The Agent's
/// published base Skill references remain part of its version identity, but
/// no Skill definition or instruction is pinned into the Judge Snapshot.
pub(crate) fn validate_private_evaluation_agent_v5_projection(
    agent: &AgentVersionSnapshot,
    resolved: &AgentVersionSnapshotV5,
    prompt: &PromptAssemblySnapshot,
    policy: &PolicySnapshot,
    memory: &MemorySelectionSnapshot,
    skills: &[SkillDefinition],
    owned_agent: bool,
) -> Result<(), SnapshotContractError> {
    if owned_agent {
        resolved.validate_owned_agent(agent)?;
    } else {
        resolved.validate_for_agent(agent)?;
    }
    let safety_policy_ref = prompt
        .sections
        .iter()
        .find(|section| section.section_key.as_str() == "platform.safety")
        .map(|section| &section.content_ref);
    let assistant_ref = assistant_prompt_ref(prompt)?;

    if agent.policy_ref != policy.policy_ref
        || resolved.policy_hash != policy.policy_hash
        || safety_policy_ref.is_none_or(|reference| reference.sha256 != resolved.policy_hash)
        || assistant_ref != &agent.persona_content_ref
        || resolved.memory_policy_ref != memory.selection_policy_ref
        || resolved.memory_policy_hash != memory.selection_policy_hash
        || agent.base_skill_refs.is_empty()
        || agent
            .base_skill_refs
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || !skills.is_empty()
    {
        return Err(SnapshotContractError::InvalidAgentVersionSnapshot);
    }
    Ok(())
}

fn assistant_prompt_ref(
    prompt: &PromptAssemblySnapshot,
) -> Result<&ContentRef, SnapshotContractError> {
    let mut matches = prompt.sections.iter().filter(|section| {
        matches!(
            section.section_key.as_str(),
            MUSTERAGENT_ASSISTANT_PROMPT_SECTION | LEGACY_PERSONA_PROMPT_SECTION
        )
    });
    let reference = matches
        .next()
        .map(|section| &section.content_ref)
        .ok_or(SnapshotContractError::InvalidAgentVersionSnapshot)?;
    if matches.next().is_some() {
        return Err(SnapshotContractError::InvalidAgentVersionSnapshot);
    }
    Ok(reference)
}
