//! Explicit resumption of the same durable Run, without another user turn.
use musteragent_identity::{AttemptNumber, ParticipantId, RunId};
use musteragent_runtime_contract::{CommandPayload, CommandType};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RunRecoveryAction {
    /// Re-evaluate the repaired dependency under the original admission.
    RetryDependency,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ResumeRun {
    pub run_id: RunId,
    pub participant_id: ParticipantId,
    /// Prevent a stale button/request from releasing a later recovery wait.
    pub expected_attempt_number: AttemptNumber,
    pub action: RunRecoveryAction,
}

impl CommandPayload for ResumeRun {
    const COMMAND_TYPE: CommandType = CommandType::ResumeRun;
}
