//! Current execution ownership, selected by the original Dialogue execution
//! owners. This is read-only context, never a transfer, control or effect grant.
use super::*;
use musteragent_identity::{GoalId, GoalPursuitId, RunId, WorkItemId};
use std::collections::BTreeSet;

pub const DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT: usize = 16;
pub const DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT: usize = 8;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum DialogueExecutionContextPhase {
    Running = 1,
    Preparing,
    Thinking,
    UsingTool,
    Cancelling,
    Waiting,
    WaitingUser,
    WaitingApproval,
    WaitingDependency,
    Unconfirmed,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}
impl DialogueExecutionContextPhase {
    pub fn terminal(self) -> bool {
        (self as u8) >= Self::Succeeded as u8
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Preparing => "preparing",
            Self::Thinking => "thinking",
            Self::UsingTool => "using_tool",
            Self::Cancelling => "cancelling",
            Self::Waiting => "waiting",
            Self::WaitingUser => "waiting_user",
            Self::WaitingApproval => "waiting_approval",
            Self::WaitingDependency => "waiting_dependency",
            Self::Unconfirmed => "unconfirmed",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionInputReference {
    pub entry_id: DialogueEntryId,
    pub cursor: DialogueCursor,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionObservation {
    pub source: DialogueExecutionSource,
    pub run_id: RunId,
    /// A public user entry in this same dialogue, if any. A delegated Task's
    /// local accepted cursor is not its foreign/private Goal or source entry.
    pub input: Option<DialogueExecutionInputReference>,
    pub accepted: DialogueCursor,
    pub phase: DialogueExecutionContextPhase,
    pub committed_publications: u64,
    pub committed_results: u64,
    pub committed_artifact_cards: u64,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueExecutionSource {
    Intent {
        intent_id: UserIntentId,
        task_id: Option<WorkItemId>,
    },
    Goal {
        goal_id: GoalId,
        pursuit_id: GoalPursuitId,
    },
}
impl DialogueExecutionSource {
    pub fn intent_id(self) -> Option<UserIntentId> {
        match self {
            Self::Intent { intent_id, .. } => Some(intent_id),
            Self::Goal { .. } => None,
        }
    }
    pub fn task_id(self) -> Option<WorkItemId> {
        match self {
            Self::Intent { task_id, .. } => task_id,
            Self::Goal { .. } => None,
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionContext {
    pub reply_binding: Option<crate::DialogueReplyBinding>,
    pub frozen_cut: DialogueContextCut,
    /// Selected independently by the qualified current Run binding. Its own
    /// input may be exactly after the historical cut; other inputs may not.
    pub current: DialogueExecutionObservation,
    pub active: Vec<DialogueExecutionObservation>,
    pub active_truncated: bool,
    pub recent_terminal: Vec<DialogueExecutionObservation>,
    pub recent_terminal_truncated: bool,
}
impl DialogueExecutionContext {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.frozen_cut.validate()?;
        if let Some(binding) = &self.reply_binding {
            binding.validate()?;
            if binding.agent_id != self.frozen_cut.agent_id
                || binding.dialogue_id != self.frozen_cut.dialogue_id
                || self.current.source.task_id().is_some()
                || self.current.source.intent_id().is_none()
            {
                return Err(AgentDialogueError::ScopeMismatch);
            }
            binding
                .cursor
                .require_at_or_before(self.frozen_cut.through)?;
        }
        let bad = AgentDialogueError::ScopeMismatch;
        if self.active.len() > DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT
            || self.recent_terminal.len() > DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT
            || (self.active_truncated
                && self.active.len() != DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT)
            || (self.recent_terminal_truncated
                && self.recent_terminal.len() != DIALOGUE_RECENT_EXECUTION_CONTEXT_LIMIT)
        {
            return Err(bad);
        }
        let mut runs = BTreeSet::new();
        for (o, current) in std::iter::once((&self.current, true)).chain(
            self.active
                .iter()
                .chain(&self.recent_terminal)
                .map(|o| (o, false)),
        ) {
            let upper = self
                .frozen_cut
                .through
                .seq
                .saturating_add(u64::from(current));
            if (matches!(o.source, DialogueExecutionSource::Goal { .. }) && o.input.is_some())
                || !runs.insert(o.run_id)
                || o.accepted.epoch != self.frozen_cut.through.epoch
                || o.accepted.seq > upper
                || o.committed_results > o.committed_publications
                || o.committed_artifact_cards > o.committed_publications
            {
                return Err(bad);
            }
            if let Some(input) = &o.input
                && (input.cursor.epoch != self.frozen_cut.through.epoch
                    || input.cursor.seq == 0
                    || input.cursor.seq > upper)
            {
                return Err(bad);
            }
        }
        if self.active.iter().any(|o| o.phase.terminal())
            || self.recent_terminal.iter().any(|o| !o.phase.terminal())
        {
            return Err(bad);
        }
        Ok(())
    }
    /// Stable identity of semantic facts, excluding heartbeat/query wallclock.
    /// The original owners still reauthorize every read; this is not a permit.
    pub fn fingerprint(&self) -> Sha256 {
        let has_goals = std::iter::once(&self.current)
            .chain(&self.active)
            .chain(&self.recent_terminal)
            .any(|o| matches!(o.source, DialogueExecutionSource::Goal { .. }));
        let mut b = if has_goals {
            b"musteragent.execution-context.v2\0".to_vec()
        } else {
            b"musteragent.execution-context.v1\0".to_vec()
        };
        for id in [
            self.frozen_cut.tenant_id.into_uuid(),
            self.frozen_cut.owner_id.into_uuid(),
            self.frozen_cut.agent_id.into_uuid(),
            self.frozen_cut.dialogue_id.into_uuid(),
        ] {
            b.extend(id.as_bytes());
        }
        b.extend(self.frozen_cut.through.epoch.to_be_bytes());
        b.extend(self.frozen_cut.through.seq.to_be_bytes());
        b.extend(self.frozen_cut.source_chain_sha256.into_bytes());
        for rows in [
            std::slice::from_ref(&self.current),
            self.active.as_slice(),
            self.recent_terminal.as_slice(),
        ] {
            b.extend((rows.len() as u64).to_be_bytes());
            for o in rows {
                match o.source {
                    DialogueExecutionSource::Intent { intent_id, .. } => {
                        if has_goals {
                            b.push(1);
                        }
                        b.extend(intent_id.into_uuid().as_bytes());
                    }
                    DialogueExecutionSource::Goal {
                        goal_id,
                        pursuit_id,
                    } => {
                        b.push(2);
                        b.extend(goal_id.into_uuid().as_bytes());
                        b.extend(pursuit_id.into_uuid().as_bytes());
                    }
                }
                b.extend(o.run_id.into_uuid().as_bytes());
                b.push(u8::from(o.source.task_id().is_some()));
                if let Some(id) = o.source.task_id() {
                    b.extend(id.into_uuid().as_bytes());
                }
                b.extend(o.accepted.epoch.to_be_bytes());
                b.extend(o.accepted.seq.to_be_bytes());
                b.push(o.phase as u8);
                b.push(u8::from(o.input.is_some()));
                if let Some(i) = &o.input {
                    b.extend(i.entry_id.into_uuid().as_bytes());
                    b.extend(i.cursor.epoch.to_be_bytes());
                    b.extend(i.cursor.seq.to_be_bytes());
                }
                for n in [
                    o.committed_publications,
                    o.committed_results,
                    o.committed_artifact_cards,
                ] {
                    b.extend(n.to_be_bytes());
                }
            }
        }
        b.push(u8::from(self.active_truncated));
        b.push(u8::from(self.recent_terminal_truncated));
        if let Some(binding) = &self.reply_binding {
            b.extend(b"musteragent.dialogue-reply-context@1\0");
            let bytes = binding.canonical_bytes();
            b.extend((bytes.len() as u64).to_be_bytes());
            b.extend(bytes);
        }
        Sha256::digest(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> DialogueExecutionContext {
        let cut = DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 2 },
            entry_count: 2,
            source_chain_sha256: Sha256::digest(b"original"),
        };
        let observation = |seq| DialogueExecutionObservation {
            source: DialogueExecutionSource::Intent {
                intent_id: UserIntentId::generate(),
                task_id: None,
            },
            run_id: RunId::generate(),
            input: Some(DialogueExecutionInputReference {
                entry_id: DialogueEntryId::generate(),
                cursor: DialogueCursor { epoch: 1, seq },
            }),
            accepted: DialogueCursor { epoch: 1, seq },
            phase: DialogueExecutionContextPhase::Unconfirmed,
            committed_publications: 0,
            committed_results: 0,
            committed_artifact_cards: 0,
        };
        DialogueExecutionContext {
            reply_binding: None,

            frozen_cut: cut,
            current: observation(3),
            active: vec![observation(1)],
            active_truncated: false,
            recent_terminal: vec![],
            recent_terminal_truncated: false,
        }
    }
    #[test]
    fn execution_context_own_input_may_follow_cut_but_other_inputs_cannot() {
        let mut v = fixture();
        assert!(v.validate().is_ok());
        v.active[0].accepted.seq = 3;
        assert!(v.validate().is_err());
        v.active[0].accepted.seq = 1;
        v.active[0].input.as_mut().unwrap().cursor.seq = 3;
        assert!(v.validate().is_err());
        v.active[0].input.as_mut().unwrap().cursor.seq = 1;
        v.active[0].run_id = v.current.run_id;
        assert!(v.validate().is_err());
    }
    #[test]
    fn execution_context_priority_bounds_are_explicit_and_unknown_not_terminal() {
        let mut v = fixture();
        v.active_truncated = true;
        assert!(v.validate().is_err());
        while v.active.len() < DIALOGUE_ACTIVE_EXECUTION_CONTEXT_LIMIT {
            let mut o = v.active[0].clone();
            o.run_id = RunId::generate();
            v.active.push(o);
        }
        assert!(v.validate().is_ok());
        let mut extra = v.active[0].clone();
        extra.run_id = RunId::generate();
        v.active.push(extra);
        assert!(v.validate().is_err());
        v.active.pop();
        v.recent_terminal.push(v.active.pop().unwrap());
        v.active_truncated = false;
        assert!(v.validate().is_err());
        v.recent_terminal[0].phase = DialogueExecutionContextPhase::Succeeded;
        assert!(v.validate().is_ok());
        assert_eq!(
            v.recent_terminal[0].committed_publications, 0,
            "Run success never invents delivery"
        );
    }
    #[test]
    fn execution_context_semantic_fingerprint_binds_owner_state_and_coverage() {
        let v = fixture();
        assert_eq!(v.fingerprint(), v.clone().fingerprint());
        let mut next = v.clone();
        next.active[0].phase = DialogueExecutionContextPhase::WaitingApproval;
        assert_ne!(v.fingerprint(), next.fingerprint());
        next = v.clone();
        next.current.source = DialogueExecutionSource::Intent {
            intent_id: UserIntentId::generate(),
            task_id: None,
        };
        assert_ne!(v.fingerprint(), next.fingerprint());
        next = v.clone();
        next.active[0].input.as_mut().unwrap().entry_id = DialogueEntryId::generate();
        assert_ne!(v.fingerprint(), next.fingerprint());
        next = v.clone();
        next.frozen_cut.source_chain_sha256 = Sha256::digest(b"different cut");
        assert_ne!(v.fingerprint(), next.fingerprint());
    }
}
