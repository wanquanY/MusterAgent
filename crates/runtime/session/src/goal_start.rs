use crate::AdmissionCommit;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::Sha256;
use musteragent_ledger::AdmissionBatchError;
use musteragent_runtime_automation::StandingGoalStart;
/// The original admission transaction must commit this entire value, or none
/// of it. Implementations must never dispatch an ordinary message as fallback.
#[derive(Debug, Clone, PartialEq)]
pub struct GoalStartAdmissionCommit {
    pub admission: AdmissionCommit,
    pub start: StandingGoalStart,
    pub message_fingerprint: Sha256,
}

impl GoalStartAdmissionCommit {
    pub fn validate(&self) -> Result<(), AdmissionBatchError> {
        self.admission.batch.validate()?;
        let batch = &self.admission.batch;
        if self.start.validate().is_err()
            || batch.activity.kind != ActivityKind::Chat
            || batch.run.context.activity_kind != ActivityKind::Chat
            || batch.runtime_snapshot.execution_grant_proof.is_none()
            || batch.append.command_fingerprint
                != self.start.command_fingerprint(self.message_fingerprint)
        {
            return Err(AdmissionBatchError::InvalidBatchShape);
        }
        Ok(())
    }
}
