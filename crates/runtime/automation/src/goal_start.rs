//! First-run Goal admission is a cross-context application use case, not a
//! scheduler Skill selection or a post-submit UI callback. An existing Draft
//! and its exact revision are bound into the original message receipt.
use musteragent_content_ref::Sha256;
use musteragent_conversation::SubmitMessage;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_identity::{GoalId, GoalPursuitId};
use musteragent_runtime_contract::{ApplicationFuture, CommandAck, CommandEnvelope, RuntimeError};
use musteragent_scheduler::AdmissionSelection;
use thiserror::Error;

/// Cross-context first-admission boundary shared by Cloud and native transports.
/// Receipt inspection is a distinct, read-only operation with no grant input.
pub trait StandingGoalStartApplication: Send + Sync {
    fn start(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
        start: StandingGoalStart,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn lookup_receipt(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        selection: AdmissionSelection,
        start: StandingGoalStart,
    ) -> ApplicationFuture<'_, Result<Option<CommandAck>, RuntimeError>>;
}

#[cfg(test)]
mod tests;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandingGoalStart {
    pub goal_id: GoalId,
    pub expected_goal_revision: u64,
    pub pursuit_id: GoalPursuitId,
}

impl StandingGoalStart {
    pub fn validate(self) -> Result<(), GoalStartRejection> {
        if self.expected_goal_revision == 0 || self.expected_goal_revision == u64::MAX {
            return Err(GoalStartRejection::Invalid);
        }
        Ok(())
    }

    /// Wrap, never replace or weaken, the complete original message digest.
    /// Ordinary messages keep their historical fingerprint byte-for-byte.
    #[must_use]
    pub fn command_fingerprint(self, message_fingerprint: Sha256) -> Sha256 {
        let mut bytes = b"musteragent.standing-goal-first-admission.v1\0".to_vec();
        bytes.extend_from_slice(&message_fingerprint.into_bytes());
        bytes.extend_from_slice(self.goal_id.into_uuid().as_bytes());
        bytes.extend_from_slice(&self.expected_goal_revision.to_be_bytes());
        bytes.extend_from_slice(self.pursuit_id.into_uuid().as_bytes());
        Sha256::digest(bytes)
    }
}

/// Stable business rejection, distinct from database/corrupt-state failures.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum GoalStartRejection {
    #[error("Goal start intent is invalid")]
    Invalid,
    #[error("Goal draft revision or first Pursuit changed")]
    Conflict,
    #[error("Goal draft is unavailable to this actor and conversation")]
    Unavailable,
}

impl GoalStartRejection {
    #[must_use]
    pub const fn error_code(self) -> musteragent_runtime_contract::ErrorCode {
        use musteragent_runtime_contract::ErrorCode;
        match self {
            Self::Invalid => ErrorCode::InvalidCommand,
            Self::Conflict => ErrorCode::ResourceRevisionConflict,
            Self::Unavailable => ErrorCode::PolicyDenied,
        }
    }
}
