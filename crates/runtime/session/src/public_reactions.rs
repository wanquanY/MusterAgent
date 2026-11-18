//! User feedback belongs to the original Dialogue public-message owner.
use crate::DialogueStoreError;
use musteragent_content_ref::Sha256;
pub use musteragent_execution_context::{PublicEntryReactions, PublicReactionSet};
use musteragent_identity::{Actor, AgentDialogueId, CommandId, DialogueEntryId, PrincipalKind};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetPublicEntryReaction {
    pub actor: Actor,
    pub command_id: CommandId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub emoji_key: String,
    pub present: bool,
    pub expected_revision: u64,
}
impl SetPublicEntryReaction {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User
            || self.expected_revision > i64::MAX as u64
            || PublicReactionSet::index(&self.emoji_key).is_err()
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> Sha256 {
        let mut bytes = b"musteragent.public-entry-reaction.v1\0".to_vec();
        for field in [
            self.actor.tenant_id.to_string(),
            self.actor.principal_id.to_string(),
            self.dialogue_id.to_string(),
            self.entry_id.to_string(),
            self.emoji_key.clone(),
            self.present.to_string(),
            self.expected_revision.to_string(),
        ] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        Sha256::digest(bytes)
    }
}
pub trait PublicEntryReactionsStore: Send + Sync {
    fn read_reactions(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
    ) -> ApplicationFuture<'_, Result<PublicEntryReactions, DialogueStoreError>>;
    fn set_reaction(
        &self,
        command: SetPublicEntryReaction,
    ) -> ApplicationFuture<'_, Result<PublicEntryReactions, DialogueStoreError>>;
}
