//! Typed business authority for original Runtime Run cancellation. A terminal
//! WorkItem or privacy fence never substitutes for the Run's own quiescence.
use super::*;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemRunControlReason {
    TaskCommand { command_id: CommandId },
    PrivacyErasure { job_id: ErasureJobId },
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemRunControlFence {
    pub actor: Actor,
    pub task_id: WorkItemId,
    pub control_revision: WorkItemControlRevision,
    pub run_id: RunId,
    pub reason: WorkItemRunControlReason,
}
#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemCancelRunCommit {
    pub fence: WorkItemRunControlFence,
    pub batch: musteragent_ledger::CancelRunBatch,
}
impl WorkItemRunControlFence {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User {
            Err(DialogueStoreError::Unavailable)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemControlOperation {
    Cancel,
    Amend,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemControlRun {
    pub run_id: RunId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
}
#[derive(Debug, Clone)]
pub struct WorkItemControlClaim {
    pub event_seq: u64,
    pub owner: uuid::Uuid,
    pub generation: u64,
    pub command_id: CommandId,
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub work_item_id: WorkItemId,
    pub goal: WorkItemGoalRevision,
    pub control: WorkItemControlRevision,
    pub operation: WorkItemControlOperation,
    pub runs: Vec<WorkItemControlRun>,
    pub obsolete: bool,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemControlProgress {
    Idle,
    Deferred,
    Settled,
    Superseded,
}
pub trait WorkItemControlStore: Send + Sync {
    fn claim(
        &self,
        owner: uuid::Uuid,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemControlClaim>, DialogueStoreError>>;
    /// Rechecks the lease, exact command authority, all Run terminal/lease facts,
    /// and effects before changing the original Task or reserving new execution.
    fn settle<'a>(
        &'a self,
        claim: &'a WorkItemControlClaim,
    ) -> ApplicationFuture<'a, Result<WorkItemControlProgress, DialogueStoreError>>;
    fn defer<'a>(
        &'a self,
        claim: &'a WorkItemControlClaim,
    ) -> ApplicationFuture<'a, Result<(), DialogueStoreError>>;
}
