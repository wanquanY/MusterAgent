//! Account stop control freezes the observed foreground Intent set. This owner
//! invokes existing Run cancellation; it never admits a replacement execution.
use crate::{DialogueRunControlFence, DialogueStoreError};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AgentDialogueId, AgentId, CommandId, PrincipalKind, UserIntentId,
};
use musteragent_runtime_contract::{ApplicationFuture, ConversationGuard};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionCancellationRequest {
    pub actor: Actor,
    pub command_id: CommandId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub observed_activity_revision: u64,
    /// One task to stop: the direct user request it answers. None stops every
    /// foreground execution observed at this revision.
    pub task_intent_id: Option<UserIntentId>,
}
impl DialogueExecutionCancellationRequest {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User || self.observed_activity_revision == 0
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> Sha256 {
        let mut bytes = b"musteragent.dialogue-execution-cancellation.v1\0".to_vec();
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.actor.principal_id.into_uuid(),
            self.command_id.into_uuid(),
            self.agent_id.into_uuid(),
            self.dialogue_id.into_uuid(),
        ] {
            bytes.extend_from_slice(id.as_bytes());
        }
        bytes.extend_from_slice(&self.observed_activity_revision.to_be_bytes());
        // Appended only when present, so every whole-dialogue fingerprint
        // retained before task-scoped stops existed is unchanged.
        if let Some(task) = self.task_intent_id {
            bytes.extend_from_slice(b"\0task\0");
            bytes.extend_from_slice(task.into_uuid().as_bytes());
        }
        Sha256::digest(bytes)
    }
}
#[derive(Debug, Clone)]
pub struct DialogueExecutionCancellationTarget {
    pub fence: DialogueRunControlFence,
    pub guard: ConversationGuard,
}
#[derive(Debug, Clone)]
pub struct RetainedDialogueExecutionCancellation {
    pub request: DialogueExecutionCancellationRequest,
    /// Immutable owner sequence, replayed unchanged and observed by activity.
    pub cancellation_sequence: u64,
    /// Total immutable targets across Cloud and Native owners.
    pub target_count: u32,
    /// Bounded Cloud preview (at most 64), never the control-set authority.
    /// Delivery always reads persisted target pages; total includes all homes.
    pub targets: Vec<DialogueExecutionCancellationTarget>,
}
pub struct DialogueExecutionCancellationRecoveryPage {
    pub commands: Vec<RetainedDialogueExecutionCancellation>,
    pub next_cursor: Option<CommandId>,
}
pub trait DialogueExecutionCancellationStore: Send + Sync {
    fn request(
        &self,
        request: DialogueExecutionCancellationRequest,
    ) -> ApplicationFuture<'_, Result<RetainedDialogueExecutionCancellation, DialogueStoreError>>;
    /// Select at most limit retained targets, enqueue Native/accepted work at
    /// their original owners, and return only actual Cloud targets for dispatch.
    fn delivery_page(
        &self,
        request: &DialogueExecutionCancellationRequest,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<Vec<DialogueExecutionCancellationTarget>, DialogueStoreError>>;
    /// Called only after the original Runtime accepted this exact cancellation.
    fn target_applied(
        &self,
        request: &DialogueExecutionCancellationRequest,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<(), DialogueStoreError>>;
    fn pending(
        &self,
        after: Option<CommandId>,
        limit: u32,
    ) -> ApplicationFuture<'_, Result<DialogueExecutionCancellationRecoveryPage, DialogueStoreError>>;
}
