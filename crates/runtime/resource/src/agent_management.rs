//! Account file metadata commands reuse the Resource transaction and its receipt.
use crate::{ResourceMutationReceipt, ResourceStoreError};
use musteragent_identity::{
    Actor, AgentDialogueId, ArtifactVersionId, CommandId, DialogueEntryId, ResourceRevision,
    UtcTimestamp,
};
use musteragent_resource::ResourceName;
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Clone, Copy)]
pub struct AgentFileSelector {
    pub actor: Actor,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
}
#[derive(Debug, Clone)]
pub struct AgentFileManagementState {
    pub revision: ResourceRevision,
}
#[derive(Debug, Clone)]
pub enum AgentFileManagementChange {
    Rename(ResourceName),
    Trash,
}
#[derive(Debug, Clone)]
pub struct AgentFileManagementCommand {
    pub selector: AgentFileSelector,
    pub expected_head: ArtifactVersionId,
    pub expected_revision: ResourceRevision,
    pub command_id: CommandId,
    pub change: AgentFileManagementChange,
    pub now: UtcTimestamp,
}
pub trait AgentFileManagementPort: Send + Sync {
    fn inspect(
        &self,
        selector: AgentFileSelector,
    ) -> ApplicationFuture<'_, Result<Option<AgentFileManagementState>, ResourceStoreError>>;
    fn execute(
        &self,
        command: AgentFileManagementCommand,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceStoreError>>;
}
