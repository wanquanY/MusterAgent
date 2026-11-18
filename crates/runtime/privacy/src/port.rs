use std::num::NonZeroU16;

use musteragent_identity::{ErasureJobId, TenantId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;

use crate::{ErasureJob, ErasurePlanSnapshot, ErasureReceipt, ErasureRequest, ErasureTransition};

pub const MAX_ERASURE_QUERY_PAGE_SIZE: u16 = 200;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ErasureAppendDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureMutationReceipt {
    pub disposition: ErasureAppendDisposition,
    pub job: ErasureJob,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ErasureQueryLimit(NonZeroU16);

impl ErasureQueryLimit {
    pub fn new(value: u16) -> Result<Self, ErasureQueryLimitError> {
        let value = NonZeroU16::new(value).ok_or(ErasureQueryLimitError)?;
        if value.get() > MAX_ERASURE_QUERY_PAGE_SIZE {
            return Err(ErasureQueryLimitError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
#[error("privacy-erasure query limit must be between 1 and 200")]
pub struct ErasureQueryLimitError;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ErasureJobCursor {
    pub requested_at: UtcTimestamp,
    pub erasure_job_id: ErasureJobId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureJobQuery {
    pub tenant_id: TenantId,
    pub after: Option<ErasureJobCursor>,
    pub limit: ErasureQueryLimit,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErasureJobPage {
    pub jobs: Vec<ErasureJob>,
    pub next_cursor: Option<ErasureJobCursor>,
}

/// Append-only persistence contract owned by the privacy-erasure consumer.
///
/// Each mutation returns the complete validated aggregate so callers cannot
/// mistake an accepted fact for a later state. Implementations must classify
/// exact replay as `Duplicate` and reject same-identity payload drift.
pub trait ErasureJobStore: Send + Sync {
    type Error: Send;

    /// Consumer-owned preparation may stop a producer and freeze its derived
    /// coverage before any bytes are deleted. Existing consumers are already
    /// fenced at admission and retain their original behavior.
    fn prepare_execution(
        &self,
        job: ErasureJob,
        _now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<ErasureJob, Self::Error>> {
        Box::pin(async move { Ok(job) })
    }

    /// Resolve only the immutable provider coverage proved by this plan. The
    /// request's root resources never change when derived content is included.
    /// Implementations must reject an unproved or stale plan before returning
    /// additional content to the executor.
    fn resolve_execution_roots(
        &self,
        _tenant_id: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> ApplicationFuture<'_, Result<Vec<musteragent_runtime_contract::ResourceRef>, Self::Error>>
    {
        Box::pin(async move { Ok(plan.root_resource_refs) })
    }

    fn create(
        &self,
        request: ErasureRequest,
    ) -> ApplicationFuture<'_, Result<ErasureMutationReceipt, Self::Error>>;

    fn append_plan(
        &self,
        tenant_id: TenantId,
        plan: ErasurePlanSnapshot,
    ) -> ApplicationFuture<'_, Result<ErasureMutationReceipt, Self::Error>>;

    fn append_transition(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
        transition: ErasureTransition,
    ) -> ApplicationFuture<'_, Result<ErasureMutationReceipt, Self::Error>>;

    /// Production execution attestation. Implementations validate the original
    /// request/plan/sequence and every provider in their final write transaction,
    /// then derive the receipt from that transaction's actual clock. A caller
    /// supplies no completion timestamp, outcome, coverage or proof.
    fn complete_execution(
        &self,
        command: crate::CompleteErasureExecution,
    ) -> ApplicationFuture<'_, Result<ErasureMutationReceipt, Self::Error>>;

    /// Exactly replays an already persisted terminal transition and receipt.
    /// Never records a new fact: production completion must use
    /// complete_execution, whose timestamp and proof are owned by persistence.
    /// Historical receipt bytes remain unchanged.
    fn complete(
        &self,
        transition: ErasureTransition,
        receipt: ErasureReceipt,
    ) -> ApplicationFuture<'_, Result<ErasureMutationReceipt, Self::Error>>;

    fn load(
        &self,
        tenant_id: TenantId,
        erasure_job_id: ErasureJobId,
    ) -> ApplicationFuture<'_, Result<Option<ErasureJob>, Self::Error>>;

    fn list(
        &self,
        query: ErasureJobQuery,
    ) -> ApplicationFuture<'_, Result<ErasureJobPage, Self::Error>>;
}

#[cfg(test)]
mod tests {
    use super::{ErasureQueryLimit, MAX_ERASURE_QUERY_PAGE_SIZE};

    #[test]
    fn query_limit_is_closed_and_bounded() {
        assert!(ErasureQueryLimit::new(0).is_err());
        assert_eq!(
            ErasureQueryLimit::new(MAX_ERASURE_QUERY_PAGE_SIZE)
                .expect("maximum")
                .get(),
            MAX_ERASURE_QUERY_PAGE_SIZE
        );
        assert!(ErasureQueryLimit::new(MAX_ERASURE_QUERY_PAGE_SIZE + 1).is_err());
    }
}
