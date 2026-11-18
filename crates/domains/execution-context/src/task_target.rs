//! An explicit user-selected task and its observed revisions. The input owner
//! validates current visibility; these identifiers never confer authority.
use crate::AgentDialogueError;
use musteragent_identity::WorkItemId;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentTaskTarget {
    pub work_item_id: WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
}

impl AgentTaskTarget {
    pub fn validate(self) -> Result<(), AgentDialogueError> {
        if self.goal_revision == 0
            || self.control_revision == 0
            || self.goal_revision >= i64::MAX as u64
            || self.control_revision >= i64::MAX as u64
        {
            return Err(AgentDialogueError::InvalidTaskTarget);
        }
        Ok(())
    }

    pub fn canonical_bytes(self) -> Vec<u8> {
        let mut bytes = b"musteragent.agent-task-target@1\0".to_vec();
        bytes.extend_from_slice(self.work_item_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&self.goal_revision.to_be_bytes());
        bytes.extend_from_slice(&self.control_revision.to_be_bytes());
        bytes
    }
}
