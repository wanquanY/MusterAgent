//! Dialogue stop authority is an exact retained command target and original
//! admission. It never grants access to generic private Conversation controls.
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, CommandId, ConversationId, PrincipalKind, RunId, UserIntentId,
};

use crate::{DeviceDeliveryBinding, DialogueStoreError};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueRunControlFence {
    pub actor: Actor,
    /// The original user stop command that froze this target set.
    pub cancellation_id: CommandId,
    /// Deterministic per-Run command, retained by the original stop owner.
    pub command_id: CommandId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub intent_id: UserIntentId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub native: Option<DeviceDeliveryBinding>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DialogueCancelRunCommit {
    pub fence: DialogueRunControlFence,
    pub batch: musteragent_ledger::CancelRunBatch,
}

impl DialogueRunControlFence {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User {
            return Err(DialogueStoreError::Unavailable);
        }
        if let Some(native) = &self.native
            && (native.run_id != self.run_id
                || native.intent_id != self.intent_id
                || native.work_item.is_some()
                || [
                    native.source_digest,
                    native.snapshot_hash,
                    native.grant_proof_hash,
                ]
                .iter()
                .any(|hash| hash.into_bytes() == [0; 32]))
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
}
