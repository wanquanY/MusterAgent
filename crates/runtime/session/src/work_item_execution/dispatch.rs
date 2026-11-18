//! Recoverable reservation-to-Run handoff. Queue leases are delivery state;
//! only the ordinary Runtime admits, executes, retries and terminates Runs.
use super::*;

use musteragent_work_item::SignedWorkItemAdmissionRejection;
use std::time::Duration;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemExecutionClaim {
    pub request: WorkItemExecutionRequest,
    pub event_seq: u64,
    pub owner: uuid::Uuid,
    pub generation: u64,
    pub attempt_count: u64,
    pub lease_expires_at: UtcTimestamp,
}
pub trait WorkItemExecutionHandoffStore: Send + Sync {
    fn claim(
        &self,
        owner: uuid::Uuid,
        lease: Duration,
    ) -> ApplicationFuture<'_, Result<Option<WorkItemExecutionClaim>, DialogueStoreError>>;
    fn complete(
        &self,
        claim: WorkItemExecutionClaim,
        receipt: WorkItemExecutionReceipt,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>>;
    fn reject(
        &self,
        claim: WorkItemExecutionClaim,
        preparation: WorkItemExecutionPreparation,
        proof: SignedWorkItemAdmissionRejection,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>>;
    fn retry(
        &self,
        claim: WorkItemExecutionClaim,
        failure: WorkItemHandoffFailure,
        delay: Duration,
    ) -> ApplicationFuture<'_, Result<bool, DialogueStoreError>>;
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemDispatchOutcome {
    Idle,
    HandedOff,
    Rejected,
    Deferred,
    LeaseLost,
}
