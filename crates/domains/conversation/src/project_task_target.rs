//! A user-selected Project task is input context, never a grant of control.
//! The original Project Control owner validates it at admission and mutation.
use musteragent_identity::{
    AssignmentEpoch, ConversationId, FencingToken, ProjectId, ProjectRevision, WorkItemId,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ProjectTaskTarget {
    pub project_id: ProjectId,
    pub work_item_id: WorkItemId,
    pub project_revision: ProjectRevision,
    pub work_item_revision: ProjectRevision,
    pub controller_conversation_id: ConversationId,
    pub controller_fencing_token: FencingToken,
    pub assignment_epoch: Option<AssignmentEpoch>,
}

impl ProjectTaskTarget {
    pub fn canonical_bytes(self) -> Vec<u8> {
        let mut bytes = b"musteragent.project-task-target@1\0".to_vec();
        for id in [
            self.project_id.into_uuid(),
            self.work_item_id.into_uuid(),
            self.controller_conversation_id.into_uuid(),
        ] {
            bytes.extend_from_slice(id.as_bytes());
        }
        for value in [
            self.project_revision.get(),
            self.work_item_revision.get(),
            self.controller_fencing_token.get(),
        ] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        match self.assignment_epoch {
            Some(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.get().to_be_bytes());
            }
            None => bytes.push(0),
        }
        bytes
    }
}
