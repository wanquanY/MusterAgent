//! Goal authority captured before an external grant exchange. A valid signed
//! grant does not override a later product control or a transferred claim.
use crate::AppendReceipt;
use musteragent_identity::RunId;

/// Execution provenance is distinct from authority. An owner-scoped Goal starts
/// without a parent Run and obtains current authority for every bounded Run.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalAdmissionSource {
    TerminalRun(RunId),
    OwnerGoal {
        previous_run_id: Option<RunId>,
        binding_revision: u64,
        source_sha256: musteragent_content_ref::Sha256,
    },
}
impl StandingGoalAdmissionSource {
    pub fn parent_run_id(self) -> Option<RunId> {
        match self {
            Self::TerminalRun(run) => Some(run),
            Self::OwnerGoal {
                previous_run_id, ..
            } => previous_run_id,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StandingGoalAdmissionFence {
    pub goal_revision: u64,
    pub pursuit_revision: u64,
    pub claim_token: String,
}

impl StandingGoalAdmissionFence {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.goal_revision > 0
            && self.pursuit_revision > 0
            && !self.claim_token.trim().is_empty()
            && self.claim_token.len() <= 512
    }
}

/// Superseded is a healthy no-op, not a failed execution or a retryable write.
/// Admitted includes duplicate receipts, even after a later Goal control.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StandingGoalCommitOutcome {
    Admitted(AppendReceipt),
    Superseded,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_fence_requires_exact_revisions_and_a_bounded_nonempty_claim() {
        let valid = StandingGoalAdmissionFence {
            goal_revision: 2,
            pursuit_revision: 9,
            claim_token: "worker:claim".into(),
        };
        assert!(valid.is_valid());
        for fence in [
            StandingGoalAdmissionFence {
                goal_revision: 0,
                ..valid.clone()
            },
            StandingGoalAdmissionFence {
                pursuit_revision: 0,
                ..valid.clone()
            },
            StandingGoalAdmissionFence {
                claim_token: String::new(),
                ..valid.clone()
            },
            StandingGoalAdmissionFence {
                claim_token: "   ".into(),
                ..valid.clone()
            },
            StandingGoalAdmissionFence {
                claim_token: "a".repeat(513),
                ..valid
            },
        ] {
            assert!(!fence.is_valid());
        }
    }
}
