//! Reliable public-input acceptance. This path does not wait for a Run or hold
//! a model execution lock; the committed outbox drives subsequent dispatch.
mod execution_target;
use crate::DialogueStoreError;
pub use execution_target::{DialogueIntentExecutionTarget, OriginDeviceExecutionTarget};

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_execution_context::{
    AgentDialogue, AgentDialogueEntry, AgentDialogueError, DialogueCursor, DialogueInputAttachment,
    DialogueInputOrigin, UserIntent, dialogue_input_attachment_bytes,
    validate_dialogue_input_attachments,
};
use musteragent_identity::{
    Actor, AgentDialogueId, CommandId, PrincipalKind, UserIntentId, UtcTimestamp,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

/// Public text custody and scoped reads have the same UTF-8 byte ceiling.
pub const MAX_DIALOGUE_MESSAGE_TEXT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AcceptDialogueIntent {
    pub actor: Actor,
    pub command_id: CommandId,
    pub dialogue_id: AgentDialogueId,
    pub content_ref: ContentRef,
    pub input_attachments: Vec<DialogueInputAttachment>,
    pub origin: DialogueInputOrigin,
    pub observed_cursor: DialogueCursor,
    pub reply_to_intent_id: Option<UserIntentId>,
    pub task_target: Option<musteragent_execution_context::AgentTaskTarget>,
    pub reply_target: Option<musteragent_execution_context::DialogueReplyTarget>,
    pub accepted_at: UtcTimestamp,
}

impl AcceptDialogueIntent {
    /// Construct the same user turn under the transaction holding the authoritative dialogue head.
    pub fn into_accepted_intent(
        self,
        dialogue: &AgentDialogue,
        reply_binding: Option<musteragent_execution_context::DialogueReplyBinding>,
    ) -> Result<UserIntent, DialogueStoreError> {
        if self.dialogue_id != dialogue.dialogue_id
            || self.reply_target != reply_binding.as_ref().map(|binding| binding.target)
        {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let intent = UserIntent {
            intent_id: UserIntentId::generate(),
            command_id: self.command_id,
            dialogue_id: dialogue.dialogue_id,
            tenant_id: self.actor.tenant_id,
            owner_id: self.actor.principal_id,
            source_entry_id: musteragent_identity::DialogueEntryId::generate(),
            content_ref: self.content_ref,
            input_attachments: self.input_attachments,
            origin: self.origin,
            observed_cursor: self.observed_cursor,
            reply_to_intent_id: self.reply_to_intent_id,
            task_target: self.task_target,
            reply_binding,
            accepted_cursor: dialogue
                .head
                .next()
                .map_err(|_| DialogueStoreError::RevisionConflict)?,
            accepted_at: self.accepted_at,
        };
        intent
            .validate_for(dialogue)
            .map_err(|_| DialogueStoreError::RevisionConflict)?;
        Ok(intent)
    }

    pub fn validate(&self) -> Result<(), AgentDialogueApplicationError> {
        if self.actor.principal_kind != PrincipalKind::User {
            return Err(AgentDialogueApplicationError::Forbidden);
        }
        self.origin.validate()?;
        if let Some(target) = self.reply_target {
            target.validate()?;
        }
        self.observed_cursor.validate()?;
        if let Some(target) = self.task_target {
            target.validate()?;
        }
        validate_dialogue_input_attachments(&self.input_attachments)?;
        if !self.input_attachments.is_empty()
            && (self.content_ref.byte_length > MAX_DIALOGUE_MESSAGE_TEXT_BYTES
                || self.input_attachments.iter().any(|v| {
                    v.attachment.content_ref.byte_length
                        > musteragent_runtime_content::MAX_CONTENT_BYTES
                }))
        {
            return Err(AgentDialogueApplicationError::ContentTooLarge);
        }
        if self.content_ref.byte_length == 0 && self.content_ref.sha256 != Sha256::digest([]) {
            return Err(AgentDialogueError::InvalidAttachment.into());
        }
        if self.content_ref.byte_length == 0 && self.input_attachments.is_empty() {
            return Err(AgentDialogueError::EmptyContent.into());
        }
        Ok(())
    }

    /// Length-delimited, versioned identity excludes acceptance time and all
    /// newly allocated server IDs. Retries cannot create another visible input.
    pub fn fingerprint(&self) -> Sha256 {
        let mut material = if self.reply_target.is_some() {
            b"musteragent.dialogue-intent.v4\0"
        } else if self.task_target.is_some() {
            b"musteragent.dialogue-intent.v3\0"
        } else if self.input_attachments.is_empty() {
            b"musteragent.dialogue-intent.v1\0"
        } else {
            b"musteragent.dialogue-intent.v2\0"
        }
        .to_vec();
        for field in [
            self.actor.tenant_id.to_string(),
            self.actor.principal_id.to_string(),
            self.dialogue_id.to_string(),
            self.content_ref.content_id.to_string(),
            self.content_ref.media_type.as_str().to_owned(),
            self.content_ref.byte_length.to_string(),
            self.content_ref.sha256.to_string(),
            self.origin.surface.as_str().to_owned(),
            self.origin
                .installation_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            self.observed_cursor.epoch.to_string(),
            self.observed_cursor.seq.to_string(),
            self.reply_to_intent_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
        ] {
            material.extend_from_slice(&(field.len() as u64).to_be_bytes());
            material.extend_from_slice(field.as_bytes());
        }
        if !self.input_attachments.is_empty() {
            let attachments = dialogue_input_attachment_bytes(&self.input_attachments);
            material.extend((attachments.len() as u64).to_be_bytes());
            material.extend(attachments);
        }
        if let Some(target) = self.task_target {
            material.extend_from_slice(&target.canonical_bytes());
        }
        if let Some(target) = self.reply_target {
            material.extend_from_slice(&target.canonical_bytes());
        }
        Sha256::digest(material)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AcceptedDialogueIntent {
    pub intent: UserIntent,
    pub replayed: bool,
}

/// A read observation only; absence never closes a command against acceptance.
#[derive(Debug, Clone, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum DialogueIntentReceiptObservation {
    Accepted(UserIntent),
    NotObservedAtRead,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueHistoryEntry {
    pub work_creations: Vec<crate::AgentWorkCreation>,
    pub reply_context: Option<musteragent_execution_context::DialogueReplyTarget>,
    pub reply_target: Option<musteragent_execution_context::DialogueReplyTarget>,
    pub entry: AgentDialogueEntry,
    /// Joined from the immutable accepted intent, never inferred from text.
    pub client_command_id: Option<CommandId>,
    pub admission_progress: Option<musteragent_execution_context::DialogueAdmissionProgress>,
    pub reactions: Option<musteragent_execution_context::PublicEntryReactions>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentDialoguePage {
    pub activity: Option<crate::OwnedAgentActivity>,
    pub dialogue: AgentDialogue,
    pub entries: Vec<DialogueHistoryEntry>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueHistoryQuery {
    Before {
        before: Option<DialogueCursor>,
        limit: u16,
    },
    Around {
        entry_id: musteragent_identity::DialogueEntryId,
        radius: u16,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueHistoryPage {
    pub activity: Option<crate::OwnedAgentActivity>,
    pub dialogue: AgentDialogue,
    pub entries: Vec<DialogueHistoryEntry>,
    pub has_before: bool,
    pub has_after: bool,
}

/// Authority implementation must commit the UserIntent, public user entry,
/// command receipt, head advance and outbox in ONE local transaction.
pub trait AgentDialogueStore: Send + Sync {
    /// Serialize against original acceptance on the authoritative primary store.
    /// Implementations cannot return absence when this capability is unavailable.
    fn read_intent_receipt(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        command_id: CommandId,
    ) -> ApplicationFuture<'_, Result<DialogueIntentReceiptObservation, DialogueStoreError>>;
    fn read_history(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        query: DialogueHistoryQuery,
    ) -> ApplicationFuture<'_, Result<DialogueHistoryPage, DialogueStoreError>>;
    /// All entries are bounded by the returned authoritative head in one snapshot.
    fn read_page(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        after: DialogueCursor,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<AgentDialoguePage, DialogueStoreError>>;
    fn accept(
        &self,
        command: AcceptDialogueIntent,
    ) -> ApplicationFuture<'_, Result<AcceptedDialogueIntent, DialogueStoreError>>;
    fn accept_on_target(
        &self,
        command: AcceptDialogueIntent,
        target: DialogueIntentExecutionTarget,
    ) -> ApplicationFuture<'_, Result<AcceptedDialogueIntent, DialogueStoreError>> {
        Box::pin(async move {
            match target {
                DialogueIntentExecutionTarget::Cloud => self.accept(command).await,
                DialogueIntentExecutionTarget::DesktopTools(_)
                | DialogueIntentExecutionTarget::CloudFromDesktop(_)
                | DialogueIntentExecutionTarget::OriginDevice(_) => {
                    Err(DialogueStoreError::Unavailable)
                }
            }
        })
    }
    fn load(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
    ) -> ApplicationFuture<'_, Result<Option<AgentDialogue>, DialogueStoreError>>;
}

/// Adapter to the existing Content Owner; an arbitrary valid digest is never
/// sufficient proof that the caller may publish another account's content.
pub trait OwnerContentAccess: Send + Sync {
    fn require_readable<'a>(
        &'a self,
        actor: Actor,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;
}

#[derive(Debug, Error)]
pub enum AgentDialogueApplicationError {
    #[error(transparent)]
    Domain(#[from] AgentDialogueError),
    #[error(transparent)]
    Store(#[from] DialogueStoreError),
    #[error("dialogue input requires an authenticated user")]
    Forbidden,
    #[error("dialogue input exceeds its byte bound")]
    ContentTooLarge,
}
