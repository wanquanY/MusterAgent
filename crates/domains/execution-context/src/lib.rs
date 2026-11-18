//! The account-owned public dialogue. Execution branches remain separate owners.
#![forbid(unsafe_code)]

mod reactions;
pub use reactions::*;
mod reply;
pub use reply::*;
mod task_target;
pub use task_target::*;
mod admission_progress;
pub use admission_progress::*;
mod admission;
pub use admission::*;
mod publication;
pub use publication::*;
mod execution_context;
pub use execution_context::*;
mod approval_context;
pub use approval_context::*;
mod scene;
pub use scene::*;
mod context;
pub use context::*;
mod input_attachment;
pub use input_attachment::*;
pub mod device_execution;
pub mod local_execution;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AgentDialogueId, AgentId, ArtifactId, ArtifactVersionId, CommandId, DeviceInstallationId,
    DialogueEntryId, InteractionId, PrincipalId, TenantId, UserIntentId, UtcTimestamp,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum AgentDialogueError {
    #[error("dialogue epoch must be positive")]
    InvalidEpoch,
    #[error("dialogue cursor is ahead of its authoritative head")]
    FutureCursor,
    #[error("dialogue cursor belongs to another epoch")]
    EpochMismatch,
    #[error("dialogue sequence is exhausted")]
    SequenceExhausted,
    #[error("intent belongs to another dialogue or account")]
    ScopeMismatch,
    #[error("native input requires its originating installation")]
    MissingOriginInstallation,
    #[error("scheduled input must not carry a device installation")]
    UnexpectedOriginInstallation,
    #[error("user content is empty")]
    EmptyContent,
    #[error("input attachment identity or provenance is invalid")]
    InvalidAttachment,
    #[error("task target revisions are invalid")]
    InvalidTaskTarget,
    #[error("reply target or its accepted binding is invalid")]
    InvalidReplyTarget,
}

/// Cursor authority is the pair, never a client timestamp or an execution Run.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueCursor {
    pub epoch: u64,
    pub seq: u64,
}

impl DialogueCursor {
    pub fn validate(self) -> Result<(), AgentDialogueError> {
        if self.epoch == 0 {
            return Err(AgentDialogueError::InvalidEpoch);
        }
        Ok(())
    }

    pub fn require_at_or_before(self, head: Self) -> Result<(), AgentDialogueError> {
        self.validate()?;
        head.validate()?;
        if self.epoch != head.epoch {
            return Err(AgentDialogueError::EpochMismatch);
        }
        if self.seq > head.seq {
            return Err(AgentDialogueError::FutureCursor);
        }
        Ok(())
    }

    pub fn next(self) -> Result<Self, AgentDialogueError> {
        self.validate()?;
        Ok(Self {
            epoch: self.epoch,
            seq: self
                .seq
                .checked_add(1)
                .ok_or(AgentDialogueError::SequenceExhausted)?,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentDialogue {
    pub dialogue_id: AgentDialogueId,
    pub agent_id: AgentId,
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub head: DialogueCursor,
    pub created_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInputSurface {
    Desktop,
    Web,
    Mobile,
    Schedule,
    /// Original capability Interaction completed; no public user message is fabricated.
    Capability,
}

impl DialogueInputSurface {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Desktop => "desktop",
            Self::Web => "web",
            Self::Mobile => "mobile",
            Self::Schedule => "schedule",
            Self::Capability => "capability",
        }
    }
}

/// Assigned by the authenticated product adapter; model text cannot select it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInputOrigin {
    pub surface: DialogueInputSurface,
    pub installation_id: Option<DeviceInstallationId>,
}

impl DialogueInputOrigin {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        match &self.surface {
            DialogueInputSurface::Desktop | DialogueInputSurface::Mobile
                if self.installation_id.is_none() =>
            {
                Err(AgentDialogueError::MissingOriginInstallation)
            }
            DialogueInputSurface::Schedule if self.installation_id.is_some() => {
                Err(AgentDialogueError::UnexpectedOriginInstallation)
            }
            _ => Ok(()),
        }
    }
}

/// Reliable input acceptance contains no promised or fabricated execution ID.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct UserIntent {
    pub intent_id: UserIntentId,
    pub command_id: CommandId,
    pub dialogue_id: AgentDialogueId,
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub source_entry_id: DialogueEntryId,
    pub content_ref: ContentRef,
    pub input_attachments: Vec<DialogueInputAttachment>,
    pub origin: DialogueInputOrigin,
    pub observed_cursor: DialogueCursor,
    pub reply_to_intent_id: Option<UserIntentId>,
    pub task_target: Option<AgentTaskTarget>,
    pub reply_binding: Option<DialogueReplyBinding>,
    pub accepted_cursor: DialogueCursor,
    pub accepted_at: UtcTimestamp,
}

impl UserIntent {
    /// The store invokes this inside the transaction holding the dialogue head.
    /// A prior accepted intent never blocks another independent acceptance.
    pub fn validate_for(&self, dialogue: &AgentDialogue) -> Result<(), AgentDialogueError> {
        if (self.dialogue_id, self.tenant_id, self.owner_id)
            != (dialogue.dialogue_id, dialogue.tenant_id, dialogue.owner_id)
        {
            return Err(AgentDialogueError::ScopeMismatch);
        }
        self.origin.validate()?;
        if let Some(binding) = &self.reply_binding {
            binding.validate()?;
            if binding.agent_id != dialogue.agent_id || binding.dialogue_id != self.dialogue_id {
                return Err(AgentDialogueError::ScopeMismatch);
            }
            binding.cursor.require_at_or_before(self.observed_cursor)?;
        }
        validate_dialogue_input_attachments(&self.input_attachments)?;
        if self.content_ref.byte_length == 0 && self.input_attachments.is_empty() {
            return Err(AgentDialogueError::EmptyContent);
        }
        self.observed_cursor.require_at_or_before(dialogue.head)?;
        if self.accepted_cursor != dialogue.head.next()? {
            return Err(AgentDialogueError::FutureCursor);
        }
        Ok(())
    }
}

/// No Tool, reasoning, internal status or notification variant exists here.
/// Publication of non-user content additionally requires its source Owner's
/// authority in the application layer, not merely constructing this value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AgentDialogueContent {
    UserMessage {
        content_ref: ContentRef,
        input_attachments: Vec<musteragent_runtime_contract::AttachmentRef>,
    },
    AgentMessage {
        content_ref: ContentRef,
    },
    Interaction {
        interaction_id: InteractionId,
    },
    Artifact {
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
    },
    PartnerCreated(AgentDialoguePartnerCreated),
    /// Original source position retained after verified Owner erasure. Contains
    /// no message, object name or capability; the digest preserves chain audit.
    Erased {
        original_source_payload_sha256: Sha256,
    },
}

/// Frozen creation receipt, not a second mutable Agent profile or lifecycle.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentDialoguePartnerCreated {
    pub creation_command_id: CommandId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub display_name: String,
    pub description: String,
    pub avatar_ref: Option<ContentRef>,
    pub profile_revision: std::num::NonZeroU64,
}

impl AgentDialoguePartnerCreated {
    /// Matches the SQL Context source index. Length-prefixed UTF-8 fields and
    /// canonical ContentRef bytes preserve the exact original receipt.
    pub fn context_bytes(&self, encode_ref: impl Fn(&ContentRef) -> Vec<u8>) -> Vec<u8> {
        let mut bytes = self.creation_command_id.into_uuid().as_bytes().to_vec();
        bytes.extend(self.agent_id.into_uuid().as_bytes());
        bytes.extend(self.dialogue_id.into_uuid().as_bytes());
        for field in [&self.display_name, &self.description] {
            bytes.extend((field.len() as u64).to_be_bytes());
            bytes.extend(field.as_bytes());
        }
        match &self.avatar_ref {
            None => bytes.push(0),
            Some(reference) => {
                let reference = encode_ref(reference);
                bytes.push(1);
                bytes.extend((reference.len() as u64).to_be_bytes());
                bytes.extend(reference);
            }
        }
        bytes.extend(self.profile_revision.get().to_be_bytes());
        bytes
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentDialogueEntry {
    pub entry_id: DialogueEntryId,
    pub dialogue_id: AgentDialogueId,
    pub tenant_id: TenantId,
    pub owner_id: PrincipalId,
    pub cursor: DialogueCursor,
    pub source_intent_id: Option<UserIntentId>,
    pub author_agent_id: Option<AgentId>,
    pub visible_revision: std::num::NonZeroU64,
    pub content: AgentDialogueContent,
    pub created_at: UtcTimestamp,
    /// How the entry is shown. Fixed when the entry is created.
    pub presentation: AgentDialoguePresentation,
}

/// Progress narration is shown as a light, foldable note under the running
/// task; every other entry is a standard bubble or card.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum AgentDialoguePresentation {
    #[default]
    Standard,
    Progress,
}

impl AgentDialoguePresentation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Progress => "progress",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "standard" => Some(Self::Standard),
            "progress" => Some(Self::Progress),
            _ => None,
        }
    }

    pub fn for_purpose(purpose: DialoguePublicationPurpose) -> Self {
        if purpose == DialoguePublicationPurpose::Progress {
            Self::Progress
        } else {
            Self::Standard
        }
    }
}

/// Stable identity of the product's welcome card. Its text is UI copy, not a
/// model utterance or a user instruction.
pub fn product_welcome_entry_id(dialogue: AgentDialogueId) -> DialogueEntryId {
    DialogueEntryId::from_uuid(uuid::Uuid::new_v5(
        &dialogue.into_uuid(),
        b"product-welcome-entry@1",
    ))
    .expect("a version-five UUID is non-nil")
}

impl AgentDialogueEntry {
    pub fn is_product_welcome(&self) -> bool {
        self.source_intent_id.is_none()
            && self.cursor == (DialogueCursor { epoch: 1, seq: 1 })
            && self.entry_id == product_welcome_entry_id(self.dialogue_id)
            && matches!(self.content, AgentDialogueContent::AgentMessage { .. })
    }
}

impl UserIntent {
    pub fn public_entry(&self) -> AgentDialogueEntry {
        AgentDialogueEntry {
            entry_id: self.source_entry_id,
            dialogue_id: self.dialogue_id,
            tenant_id: self.tenant_id,
            owner_id: self.owner_id,
            cursor: self.accepted_cursor,
            source_intent_id: Some(self.intent_id),
            author_agent_id: None,
            visible_revision: std::num::NonZeroU64::MIN,
            content: AgentDialogueContent::UserMessage {
                content_ref: self.content_ref.clone(),
                input_attachments: self
                    .input_attachments
                    .iter()
                    .map(|v| v.attachment.clone())
                    .collect(),
            },
            created_at: self.accepted_at,
            presentation: Default::default(),
        }
    }
}

mod delivery_contract;
pub use delivery_contract::*;

mod delivery_versions;
pub use delivery_versions::*;
mod output_contract;
pub use output_contract::*;
mod run_output_contract;
pub use run_output_contract::{RunOutputContract, WorkItemResultStep};
mod publication_tools;
pub use publication_tools::*;

mod delivery_correction;
mod delivery_v5;

mod delivery_diagnostic;
pub use delivery_diagnostic::*;
mod delivery_repair;
pub use delivery_repair::*;

mod delivery_diagnostic_decode;
pub use delivery_diagnostic_decode::*;
