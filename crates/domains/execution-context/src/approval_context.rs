//! Current read-only approval observations, separate from the frozen narrative
//! cut. This projection is not an approval grant or a claim that an effect ran.
use super::*;
use musteragent_identity::RunId;
use std::{collections::BTreeSet, num::NonZeroU64};

pub const DIALOGUE_APPROVAL_CONTEXT_LIMIT: usize = 64;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueApprovalContextKind {
    Approval,
    Confirmation,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueApprovalContextState {
    Pending,
    Approved,
    Denied,
    Expired,
    Cancelled,
    /// The original public projection records resolution without disclosing the
    /// decision (e.g. local session approval). Never infer an approval grant.
    ResolvedDecisionUnavailable,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueApprovalObservation {
    pub entry_id: DialogueEntryId,
    pub sequence: u64,
    pub interaction_id: InteractionId,
    pub run_id: RunId,
    pub visible_revision: NonZeroU64,
    pub control_revision: Sha256,
    pub kind: DialogueApprovalContextKind,
    pub state: DialogueApprovalContextState,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueApprovalContext {
    pub frozen_cut: DialogueContextCut,
    /// Lower bound of the bounded original public reference scan. Older or
    /// inaccessible observations are unknown, not pending or approved.
    pub from_seq: u64,
    pub observations: Vec<DialogueApprovalObservation>,
}
impl DialogueApprovalContext {
    pub fn validate(&self) -> Result<(), AgentDialogueError> {
        self.frozen_cut.validate()?;
        let bad = AgentDialogueError::ScopeMismatch;
        if self.from_seq == 0
            || self.from_seq > self.frozen_cut.through.seq.saturating_add(1)
            || self.observations.len() > DIALOGUE_APPROVAL_CONTEXT_LIMIT
        {
            return Err(bad);
        }
        let mut entries = BTreeSet::new();
        let mut interactions = BTreeSet::new();
        let mut previous = 0;
        for o in &self.observations {
            if o.sequence < self.from_seq
                || o.sequence > self.frozen_cut.through.seq
                || o.sequence <= previous
                || !entries.insert(o.entry_id)
                || !interactions.insert(o.interaction_id)
                || o.visible_revision.get() > i64::MAX as u64
                || o.control_revision.into_bytes() == [0; 32]
            {
                return Err(bad);
            }
            previous = o.sequence;
        }
        Ok(())
    }
}
