//! Delivery leases over the original erasure aggregate, never another job status.
use musteragent_identity::{ErasureJobId, TenantId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ErasureWorkClaim {
    pub tenant_id: TenantId,
    pub erasure_job_id: ErasureJobId,
    pub worker_id: uuid::Uuid,
    pub generation: u64,
    pub lease_expires_at: UtcTimestamp,
}
pub trait ErasureRecoveryStore: Send + Sync {
    type Error: Send;
    fn claim_erasure_work(
        &self,
        worker_id: uuid::Uuid,
        now: UtcTimestamp,
        lease_ms: u32,
    ) -> ApplicationFuture<'_, Result<Option<ErasureWorkClaim>, Self::Error>>;
    /// A receipt is the sole proof of completion. Without one the claim is
    /// released with bounded backoff; callers cannot mark a job successful.
    fn release_erasure_work(
        &self,
        claim: ErasureWorkClaim,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), Self::Error>>;
}
