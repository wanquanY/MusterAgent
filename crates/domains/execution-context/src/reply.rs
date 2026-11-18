//! A selected public message is a bounded context reference, never executable
//! instructions or authority. The original publication/input owners derive the
//! subject in the acceptance transaction; callers supply only target identity.
use crate::{AgentDialogueError, AgentTaskTarget, DialogueCursor};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AgentDialogueId, AgentId, CommandId, ConversationId, DialogueEntryId, GoalId, RunId,
    UserIntentId,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueReplyTarget {
    pub entry_id: DialogueEntryId,
    pub visible_revision: u64,
}
impl DialogueReplyTarget {
    pub fn validate(self) -> Result<(), AgentDialogueError> {
        if self.visible_revision == 0 || self.visible_revision >= i64::MAX as u64 {
            return Err(AgentDialogueError::InvalidReplyTarget);
        }
        Ok(())
    }
    pub fn canonical_bytes(self) -> Vec<u8> {
        let mut bytes = b"musteragent.dialogue-reply-target@1\0".to_vec();
        bytes.extend_from_slice(self.entry_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&self.visible_revision.to_be_bytes());
        bytes
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueReplyPublication {
    pub delivery_id: CommandId,
    pub conversation_id: ConversationId,
    pub run_id: RunId,
    pub payload_sha256: Sha256,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentTaskActionTarget {
    pub target: AgentTaskTarget,
    pub assignment_epoch: u64,
    pub chain_root_id: musteragent_identity::WorkItemId,
    pub chain_sequence: u64,
}
impl AgentTaskActionTarget {
    pub fn validate(self) -> Result<(), AgentDialogueError> {
        self.target.validate()?;
        if self.assignment_epoch == 0
            || self.assignment_epoch >= i64::MAX as u64
            || self.chain_sequence == 0
            || self.chain_sequence >= i64::MAX as u64
        {
            return Err(AgentDialogueError::InvalidReplyTarget);
        }
        if self.chain_sequence == 1 && self.target.work_item_id != self.chain_root_id {
            return Err(AgentDialogueError::InvalidReplyTarget);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueReplySubject {
    /// An original accepted user message, with no publication or execution claim.
    AcceptedInput {
        intent_id: UserIntentId,
    },
    IntentOutput {
        intent_id: UserIntentId,
    },
    /// Exact execution result; this does not mean the Task is still amendable.
    WorkItemResult {
        target: AgentTaskTarget,
        assignment_epoch: u64,
        action_target: AgentTaskActionTarget,
        producing_run_id: RunId,
        /// Report delivery Run when the original report owner published the message.
        report_run_id: Option<RunId>,
        report_content_ref: Option<ContentRef>,
    },
    /// Feedback on one delivery, never implicit changes to Goal settings or wakes.
    GoalResult {
        goal_id: GoalId,
        binding_revision: u64,
        rules_version: Sha256,
    },
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueReplyBinding {
    pub target: DialogueReplyTarget,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub cursor: DialogueCursor,
    /// The precise message text version; quoted text from the client is not used.
    pub content_ref: ContentRef,
    pub publication: Option<DialogueReplyPublication>,
    pub subject: DialogueReplySubject,
}
impl DialogueReplyBinding {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.target.validate()?;
        self.cursor.validate()?;
        if self.cursor.seq == 0
            || self.content_ref.byte_length == 0
            || self.content_ref.byte_length > 2 * 1024 * 1024
        {
            return Err(AgentDialogueError::InvalidReplyTarget);
        }
        match &self.subject {
            DialogueReplySubject::AcceptedInput { .. } if self.publication.is_some() => {
                return Err(AgentDialogueError::InvalidReplyTarget);
            }
            DialogueReplySubject::AcceptedInput { .. } => {}
            _ if self.publication.is_none() => {
                return Err(AgentDialogueError::InvalidReplyTarget);
            }
            DialogueReplySubject::WorkItemResult {
                target,
                assignment_epoch,
                action_target,
                producing_run_id,
                report_run_id,
                report_content_ref,
            } => {
                target.validate()?;
                action_target.validate()?;
                if *assignment_epoch == 0
                    || *assignment_epoch >= i64::MAX as u64
                    || report_run_id.is_some() != report_content_ref.is_some()
                    || report_run_id
                        .is_some_and(|run| Some(run) != self.publication.as_ref().map(|p| p.run_id))
                    || report_run_id.is_none()
                        && Some(*producing_run_id) != self.publication.as_ref().map(|p| p.run_id)
                {
                    return Err(AgentDialogueError::InvalidReplyTarget);
                }
            }
            DialogueReplySubject::GoalResult {
                binding_revision, ..
            } if *binding_revision == 0 || *binding_revision >= i64::MAX as u64 => {
                return Err(AgentDialogueError::InvalidReplyTarget);
            }
            _ => {}
        }
        Ok(())
    }
    /// Explicit published context only. This list never includes the producing
    /// Task's private files or a guessed latest artifact version.
    pub fn content_refs(&self) -> Vec<&ContentRef> {
        let mut refs = vec![&self.content_ref];
        if let DialogueReplySubject::WorkItemResult {
            report_content_ref: Some(report),
            ..
        } = &self.subject
            && report != &self.content_ref
        {
            refs.push(report);
        }
        refs
    }
    pub fn canonical_bytes(&self) -> Vec<u8> {
        fn content(bytes: &mut Vec<u8>, value: &ContentRef) {
            bytes.extend(value.content_id.into_uuid().as_bytes());
            bytes.extend((value.media_type.as_str().len() as u64).to_be_bytes());
            bytes.extend(value.media_type.as_str().as_bytes());
            bytes.extend(value.byte_length.to_be_bytes());
            bytes.extend(value.sha256.into_bytes());
        }
        let mut b = b"musteragent.dialogue-reply-binding@1\0".to_vec();
        b.extend(self.target.canonical_bytes());
        b.extend(self.agent_id.into_uuid().as_bytes());
        b.extend(self.dialogue_id.into_uuid().as_bytes());
        b.extend(self.cursor.epoch.to_be_bytes());
        b.extend(self.cursor.seq.to_be_bytes());
        content(&mut b, &self.content_ref);
        b.push(u8::from(self.publication.is_some()));
        if let Some(p) = &self.publication {
            b.extend(p.delivery_id.into_uuid().as_bytes());
            b.extend(p.conversation_id.into_uuid().as_bytes());
            b.extend(p.run_id.into_uuid().as_bytes());
            b.extend(p.payload_sha256.into_bytes());
        }
        match &self.subject {
            DialogueReplySubject::AcceptedInput { intent_id } => {
                b.push(1);
                b.extend(intent_id.into_uuid().as_bytes());
            }
            DialogueReplySubject::IntentOutput { intent_id } => {
                b.push(2);
                b.extend(intent_id.into_uuid().as_bytes());
            }
            DialogueReplySubject::WorkItemResult {
                target,
                assignment_epoch,
                action_target,
                producing_run_id,
                report_run_id,
                report_content_ref,
            } => {
                b.push(3);
                b.extend(target.work_item_id.into_uuid().as_bytes());
                b.extend(target.goal_revision.to_be_bytes());
                b.extend(target.control_revision.to_be_bytes());
                b.extend(assignment_epoch.to_be_bytes());
                b.extend(action_target.target.canonical_bytes());
                b.extend(action_target.assignment_epoch.to_be_bytes());
                b.extend(action_target.chain_root_id.into_uuid().as_bytes());
                b.extend(action_target.chain_sequence.to_be_bytes());
                b.extend(producing_run_id.into_uuid().as_bytes());
                b.push(u8::from(report_run_id.is_some()));
                if let Some(run) = report_run_id {
                    b.extend(run.into_uuid().as_bytes());
                }
                b.push(u8::from(report_content_ref.is_some()));
                if let Some(reference) = report_content_ref {
                    content(&mut b, reference);
                }
            }
            DialogueReplySubject::GoalResult {
                goal_id,
                binding_revision,
                rules_version,
            } => {
                b.push(4);
                b.extend(goal_id.into_uuid().as_bytes());
                b.extend(binding_revision.to_be_bytes());
                b.extend(rules_version.into_bytes());
            }
        }
        b
    }
    pub fn task_action(&self) -> Option<AgentTaskActionTarget> {
        match &self.subject {
            DialogueReplySubject::WorkItemResult { action_target, .. } => Some(*action_target),
            _ => None,
        }
    }
    pub fn task_result(&self) -> Option<AgentTaskTarget> {
        match self.subject {
            DialogueReplySubject::WorkItemResult { target, .. } => Some(target),
            _ => None,
        }
    }
}
