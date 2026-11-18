//! Explicit user-facing delivery. Internal model output is not a candidate.
use crate::{AgentDialogueContent, AgentDialoguePartnerCreated};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ArtifactId, ArtifactVersionId, InteractionId};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialoguePublicationPurpose {
    Reply,
    Result,
    ImportantUpdate,
    Clarification,
    RequiredDecision,
    Blocked,
    /// Runtime-issued narration of the step the Agent is taking. It never ends
    /// or settles a request, never notifies, and is only ever a short message.
    Progress,
}

impl DialoguePublicationPurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reply => "reply",
            Self::Result => "result",
            Self::ImportantUpdate => "important_update",
            Self::Clarification => "clarification",
            Self::RequiredDecision => "required_decision",
            Self::Blocked => "blocked",
            Self::Progress => "progress",
        }
    }
}

/// A closed set of deliverable object kinds. It has no generic tool event,
/// execution status, arbitrary action, or user-message variant. The shared Loop
/// must separately authorize message text as an explicit public candidate;
/// a MIME/size check cannot determine whether prose is useful to the user.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PublicDeliveryContent {
    Message(ContentRef),
    Interaction(InteractionId),
    Artifact {
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
    },
    PartnerCreated(AgentDialoguePartnerCreated),
}

impl PublicDeliveryContent {
    pub fn validate(
        &self,
        purpose: DialoguePublicationPurpose,
    ) -> Result<(), DialoguePublicationContractError> {
        match self {
            Self::PartnerCreated(partner) => {
                if purpose != DialoguePublicationPurpose::Result {
                    return Err(DialoguePublicationContractError::PurposeMismatch);
                }
                if partner.display_name.trim().is_empty()
                    || partner.display_name.len() > 120
                    || partner.description.len() > 2048
                {
                    return Err(DialoguePublicationContractError::InvalidPartner);
                }
            }
            Self::Message(content) => {
                if content.byte_length == 0 || content.byte_length > 2 * 1024 * 1024 {
                    return Err(DialoguePublicationContractError::InvalidMessage);
                }
                let media = content.media_type.as_str();
                if !matches!(
                    media,
                    "text/plain"
                        | "text/markdown"
                        | "text/plain;charset=utf-8"
                        | "text/markdown;charset=utf-8"
                ) {
                    return Err(DialoguePublicationContractError::InvalidMessage);
                }
            }
            Self::Interaction(_) => {
                if !matches!(
                    purpose,
                    DialoguePublicationPurpose::Clarification
                        | DialoguePublicationPurpose::RequiredDecision
                        | DialoguePublicationPurpose::Blocked
                ) {
                    return Err(DialoguePublicationContractError::PurposeMismatch);
                }
            }
            Self::Artifact { .. } => {
                if !matches!(
                    purpose,
                    DialoguePublicationPurpose::Reply
                        | DialoguePublicationPurpose::Result
                        | DialoguePublicationPurpose::ImportantUpdate
                ) {
                    return Err(DialoguePublicationContractError::PurposeMismatch);
                }
            }
        }
        Ok(())
    }

    pub fn public_content(&self) -> AgentDialogueContent {
        match self {
            Self::PartnerCreated(partner) => AgentDialogueContent::PartnerCreated(partner.clone()),
            Self::Message(content_ref) => AgentDialogueContent::AgentMessage {
                content_ref: content_ref.clone(),
            },
            Self::Interaction(interaction_id) => AgentDialogueContent::Interaction {
                interaction_id: *interaction_id,
            },
            Self::Artifact {
                artifact_id,
                artifact_version_id,
            } => AgentDialogueContent::Artifact {
                artifact_id: *artifact_id,
                artifact_version_id: *artifact_version_id,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum DialoguePublicationContractError {
    #[error("public messages require bounded, non-empty plain text or Markdown")]
    InvalidMessage,
    #[error("delivery object is not valid for the stated publication purpose")]
    PurposeMismatch,
    #[error("partner delivery requires a bounded original creation profile")]
    InvalidPartner,
}
