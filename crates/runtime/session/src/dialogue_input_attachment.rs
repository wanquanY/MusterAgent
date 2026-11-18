//! Input attachment authority is a public-entry position, never a raw file ID.
use crate::DialogueStoreError;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::{Actor, AgentDialogueId, DialogueEntryId, PrincipalKind};
use musteragent_runtime_contract::{ApplicationFuture, AttachmentRef};

pub const MAX_DIALOGUE_ATTACHMENT_READ_BYTES: u32 = 512 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueInputAttachmentQuery {
    pub actor: Actor,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub cursor: DialogueCursor,
    pub ordinal: u32,
}
impl DialogueInputAttachmentQuery {
    pub fn validate(self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User
            || self.cursor.seq == 0
            || self.ordinal >= 64
        {
            return Err(DialogueStoreError::Unavailable);
        }
        self.cursor
            .validate()
            .map_err(|_| DialogueStoreError::Unavailable)
    }
}

/// Implementations check the original accepted intent, immutable public entry,
/// current account ownership, ordinal and complete reference in one snapshot.
pub trait DialogueInputAttachmentAccess: Send + Sync {
    fn resolve(
        &self,
        query: DialogueInputAttachmentQuery,
    ) -> ApplicationFuture<'_, Result<AttachmentRef, DialogueStoreError>>;
}
