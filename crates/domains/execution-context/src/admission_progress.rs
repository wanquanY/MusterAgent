//! Observations of original accepted-input custody and receipt lifetime. No authority.
use musteragent_identity::{AgentDialogueId, AgentId, DialogueEntryId, UserIntentId, UtcTimestamp};
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueAdmissionStage {
    WaitingDevice,
    ConfirmationPending,
    Admitted,
    /// A definitive original pre-Run failure, never an execution receipt.
    Failed,
    /// Original accepted-input or device fence proves no execution can start.
    Cancelled,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueAdmissionBlocker {
    AuthorizationCheckFailed,
    DependencyUnavailable,
    SourceChanged,
    /// The qualified device cannot prepare this material. Not an execution outcome.
    RuntimeContractRejected,
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DialogueAdmissionProgress {
    pub intent_id: UserIntentId,
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub revision: u64,
    pub stage: DialogueAdmissionStage,
    pub blocker: Option<DialogueAdmissionBlocker>,
    pub observed_at: UtcTimestamp,
    pub execution_finished: bool,
}
impl DialogueAdmissionProgress {
    pub fn valid(&self) -> bool {
        self.revision > 0
            && self.revision <= i64::MAX as u64
            && self.observed_at.unix_millis() > 0
            && !(matches!(
                self.stage,
                DialogueAdmissionStage::Admitted
                    | DialogueAdmissionStage::Failed
                    | DialogueAdmissionStage::Cancelled
            ) && self.blocker.is_some())
            && (!self.execution_finished || self.stage == DialogueAdmissionStage::Admitted)
    }
}
