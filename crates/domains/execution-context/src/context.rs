//! Immutable public provenance. This is shared conversation context, not Memory.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::{DialogueContextCheckpointId, InvocationId, VersionedKey};

pub const DIALOGUE_CONTEXT_SOURCE_CONTRACT: &str = "musteragent.dialogue-context-source@1";
pub const DIALOGUE_CONTEXT_PAGE_ENTRIES: usize = 64;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextCut {
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub through: DialogueCursor,
    pub entry_count: u64,
    pub source_chain_sha256: Sha256,
}
impl DialogueContextCut {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.through.validate()?;
        if self.entry_count != self.through.seq || self.through.seq > i64::MAX as u64 {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        if self.through.seq == 0 && self.source_chain_sha256 != self.empty_chain_hash() {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        Ok(())
    }
    pub fn empty_chain_hash(&self) -> Sha256 {
        let mut bytes = DIALOGUE_CONTEXT_SOURCE_CONTRACT.as_bytes().to_vec();
        bytes.extend(self.tenant_id.into_uuid().as_bytes());
        bytes.extend(self.owner_id.into_uuid().as_bytes());
        bytes.extend(self.agent_id.into_uuid().as_bytes());
        bytes.extend(self.dialogue_id.into_uuid().as_bytes());
        bytes.extend(self.through.epoch.to_be_bytes());
        Sha256::digest(bytes)
    }
    pub fn same_dialogue(&self, other: &Self) -> bool {
        (
            self.tenant_id,
            self.owner_id,
            self.agent_id,
            self.dialogue_id,
            self.through.epoch,
        ) == (
            other.tenant_id,
            other.owner_id,
            other.agent_id,
            other.dialogue_id,
            other.through.epoch,
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextCheckpoint {
    pub checkpoint_id: DialogueContextCheckpointId,
    pub coverage: DialogueContextCut,
    pub parent_checkpoint_id: Option<DialogueContextCheckpointId>,
    pub summary_ref: ContentRef,
    pub source_manifest_ref: ContentRef,
    pub generator_invocation_id: InvocationId,
    pub policy_ref: VersionedKey,
}
impl DialogueContextCheckpoint {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.coverage.validate()?;
        if self.coverage.through.seq == 0
            || self.summary_ref.byte_length == 0
            || self.source_manifest_ref.byte_length == 0
            || self.parent_checkpoint_id == Some(self.checkpoint_id)
        {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextSourcePage {
    pub frozen_cut: DialogueContextCut,
    pub from_seq: u64,
    pub through_seq: u64,
    pub entries: Vec<AgentDialogueEntry>,
    pub checkpoint: Option<DialogueContextCheckpoint>,
    pub prefix_chain_sha256: Sha256,
    pub through_chain_sha256: Sha256,
}
impl DialogueContextSourcePage {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.frozen_cut.validate()?;
        if self.from_seq == 0
            || self.from_seq > self.through_seq
            || self.through_seq > self.frozen_cut.through.seq
        {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            if !self.entries.is_empty()
                || self.from_seq != 1
                || !self.frozen_cut.same_dialogue(&checkpoint.coverage)
                || checkpoint.coverage.through.seq != self.through_seq
                || checkpoint.coverage.source_chain_sha256 != self.through_chain_sha256
            {
                return Err(AgentDialogueError::ScopeMismatch);
            }
        } else {
            if self.entries.is_empty()
                || self.entries.len() > DIALOGUE_CONTEXT_PAGE_ENTRIES
                || self.through_seq - self.from_seq + 1 != self.entries.len() as u64
            {
                return Err(AgentDialogueError::ScopeMismatch);
            }
            for (offset, entry) in self.entries.iter().enumerate() {
                if (
                    entry.tenant_id,
                    entry.owner_id,
                    entry.dialogue_id,
                    entry.cursor.epoch,
                    entry.cursor.seq,
                ) != (
                    self.frozen_cut.tenant_id,
                    self.frozen_cut.owner_id,
                    self.frozen_cut.dialogue_id,
                    self.frozen_cut.through.epoch,
                    self.from_seq + offset as u64,
                ) {
                    return Err(AgentDialogueError::ScopeMismatch);
                }
            }
        }
        if (self.from_seq == 1 && self.prefix_chain_sha256 != self.frozen_cut.empty_chain_hash())
            || (self.through_seq == self.frozen_cut.through.seq
                && self.through_chain_sha256 != self.frozen_cut.source_chain_sha256)
        {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueContextBound {
    pub frozen_cut: DialogueContextCut,
    pub page_ordinal: std::num::NonZeroU32,
    pub from_seq: u64,
    pub through_seq: u64,
    pub source_page_ref: ContentRef,
    pub checkpoint_id: Option<DialogueContextCheckpointId>,
    pub completes_cut: bool,
}
impl DialogueContextBound {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.frozen_cut.validate()?;
        if self.from_seq == 0
            || self.through_seq < self.from_seq
            || self.through_seq > self.frozen_cut.through.seq
            || self.source_page_ref.byte_length == 0
            || self.completes_cut != (self.through_seq == self.frozen_cut.through.seq)
            || (self.checkpoint_id.is_some()
                && (self.from_seq != 1 || self.page_ordinal.get() != 1))
        {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        Ok(())
    }
}

/// The reference encoder is injected so the domain has no protobuf dependency.
/// SQL and Rust use this exact append-only source format. UI presentation
/// revisions must be replaced by the source index's insertion-time revision.
pub fn dialogue_context_entry_hash(
    entry: &AgentDialogueEntry,
    encode_ref: impl Fn(&ContentRef) -> Vec<u8>,
) -> Sha256 {
    fn optional(bytes: &mut Vec<u8>, value: Option<&[u8]>) {
        match value {
            None => bytes.push(0),
            Some(v) => {
                bytes.push(1);
                bytes.extend((v.len() as u64).to_be_bytes());
                bytes.extend(v);
            }
        }
    }
    let (kind, content, interaction, artifact, version) = match &entry.content {
        AgentDialogueContent::Erased {
            original_source_payload_sha256,
        } => {
            return *original_source_payload_sha256;
        }
        AgentDialogueContent::PartnerCreated(partner) => (
            5,
            Some(partner.context_bytes(&encode_ref)),
            None,
            None,
            None,
        ),
        AgentDialogueContent::UserMessage {
            content_ref,
            input_attachments,
        } => {
            if input_attachments.is_empty() {
                (1u64, Some(encode_ref(content_ref)), None, None, None)
            } else {
                (
                    6,
                    Some(crate::dialogue_attachment_entry_bytes(
                        content_ref,
                        input_attachments,
                        &encode_ref,
                    )),
                    None,
                    None,
                    None,
                )
            }
        }
        AgentDialogueContent::AgentMessage { content_ref } => {
            (2, Some(encode_ref(content_ref)), None, None, None)
        }
        AgentDialogueContent::Interaction { interaction_id } => {
            (3, None, Some(interaction_id.into_uuid()), None, None)
        }
        AgentDialogueContent::Artifact {
            artifact_id,
            artifact_version_id,
        } => (
            4,
            None,
            None,
            Some(artifact_id.into_uuid()),
            Some(artifact_version_id.into_uuid()),
        ),
    };
    let mut bytes = entry.entry_id.into_uuid().as_bytes().to_vec();
    bytes.extend(entry.cursor.epoch.to_be_bytes());
    bytes.extend(entry.cursor.seq.to_be_bytes());
    bytes.extend(kind.to_be_bytes());
    let intent = entry.source_intent_id.map(|id| id.into_uuid());
    let author = entry.author_agent_id.map(|id| id.into_uuid());
    optional(
        &mut bytes,
        intent.as_ref().map(|id| id.as_bytes().as_slice()),
    );
    optional(
        &mut bytes,
        author.as_ref().map(|id| id.as_bytes().as_slice()),
    );
    optional(&mut bytes, content.as_deref());
    optional(
        &mut bytes,
        interaction.as_ref().map(|v| v.as_bytes().as_slice()),
    );
    optional(
        &mut bytes,
        artifact.as_ref().map(|v| v.as_bytes().as_slice()),
    );
    optional(
        &mut bytes,
        version.as_ref().map(|v| v.as_bytes().as_slice()),
    );
    bytes.extend(entry.visible_revision.get().to_be_bytes());
    bytes.extend(entry.created_at.unix_millis().to_be_bytes());
    Sha256::digest(bytes)
}
