//! Shared claim/renewal relationships. No scheduler, signer, lease or effect gate.
use musteragent_identity::{AuthorityRequestId, RunId, RunSeq};

use crate::execution_assignment::ExecutionWorkerBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionLeaseOperation {
    Claim {
        expected_run_sequence: RunSeq,
    },
    Renew {
        worker: ExecutionWorkerBinding,
        previous_request_id: AuthorityRequestId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLeaseRequest<S> {
    pub scope: S,
    pub run_id: RunId,
    pub assignment_digest: [u8; 32],
    pub request_id: AuthorityRequestId,
    pub executor_run_authority_digest: [u8; 32],
    pub operation: ExecutionLeaseOperation,
}
impl<S> ExecutionLeaseRequest<S> {
    pub fn validate(&self) -> Result<(), ExecutionClaimError> {
        if self.assignment_digest == [0; 32] || self.executor_run_authority_digest == [0; 32] {
            return Err(ExecutionClaimError::Binding);
        }
        if let ExecutionLeaseOperation::Renew {
            worker,
            previous_request_id,
        } = self.operation
        {
            worker
                .validate()
                .map_err(|_| ExecutionClaimError::Binding)?;
            if worker.assignment_digest != self.assignment_digest
                || previous_request_id == self.request_id
            {
                return Err(ExecutionClaimError::Binding);
            }
        }
        Ok(())
    }

    /// Original journal claim appends AttemptStarted + WorkerLeaseAcquired.
    /// Structural sequence correspondence is not evidence of a committed claim.
    pub fn check_worker_commit(
        &self,
        worker: ExecutionWorkerBinding,
        observed_run_sequence: RunSeq,
    ) -> Result<(), ExecutionClaimError> {
        self.validate()?;
        worker
            .validate()
            .map_err(|_| ExecutionClaimError::Binding)?;
        if worker.assignment_digest != self.assignment_digest {
            return Err(ExecutionClaimError::Binding);
        }
        match self.operation {
            ExecutionLeaseOperation::Claim {
                expected_run_sequence,
            } if expected_run_sequence.get().checked_add(2)
                != Some(observed_run_sequence.get()) =>
            {
                Err(ExecutionClaimError::Sequence)
            }
            ExecutionLeaseOperation::Renew {
                worker: previous, ..
            } if previous != worker => Err(ExecutionClaimError::Binding),
            _ => Ok(()),
        }
    }
}

/// Extracted Hosted attenuation arithmetic. Never rebase the captured deadline
/// on a retry/signing sample. Callers verify the original clock epoch separately.
pub fn remaining_execution_lease_ms(
    observed_ms: u64,
    deadline_ms: u64,
    signed_ms: u64,
    max_freshness_ms: u64,
) -> Result<u64, ExecutionClaimError> {
    if signed_ms < observed_ms
        || !deadline_ms
            .checked_sub(observed_ms)
            .is_some_and(|v| (1..=max_freshness_ms).contains(&v))
    {
        return Err(ExecutionClaimError::Window);
    }
    deadline_ms
        .checked_sub(signed_ms)
        .and_then(|v| v.checked_sub(1))
        .filter(|v| *v > 0)
        .ok_or(ExecutionClaimError::Window)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionClaimError {
    #[error("execution claim identity or evidence does not match")]
    Binding,
    #[error("execution claim does not match the committed Run sequence")]
    Sequence,
    #[error("execution claim has no valid attenuated time window")]
    Window,
}
