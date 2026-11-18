//! Source-owned Journal port. Implementations use the existing Home commit lane
//! and Attempt fence; this port never defines a second lease or state store.
use crate::{
    ClockSample, ExecutionActivitySource, ExecutionCheckpoint, ExecutionLiveness, ExecutionRecord,
    LivenessError, RecordMutation, RecordTransition,
};
use musteragent_identity::{TenantId, UtcTimestamp};
use musteragent_runtime_contract::execution_liveness::{
    BusinessDeadline, ExecutionBinding, ExecutionLivenessPolicy, ExecutionScope,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::num::NonZeroU16;
use std::sync::Arc;

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
#[error("execution liveness journal failed: {code:?}")]
pub struct LivenessJournalError {
    pub code: ErrorCode,
    pub retryable: bool,
    /// Closed reducer rejection, retained for reload/recovery decisions.
    pub rejection: Option<LivenessError>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DueExecutionScope {
    pub binding: ExecutionBinding,
    pub revision: u64,
    pub due_at: UtcTimestamp,
}

/// Original dispatch intent contains immutable policy and identity, no caller
/// timestamps or mutable checkpoint state. Only Home assigns the first clock.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionAdmission {
    pub binding: ExecutionBinding,
    pub policy: Arc<ExecutionLivenessPolicy>,
    pub business_deadline: Option<BusinessDeadline>,
}
impl ExecutionAdmission {
    pub fn into_checkpoint(self, now: ClockSample) -> Result<ExecutionCheckpoint, LivenessError> {
        ExecutionLiveness::new(self.binding, self.policy, self.business_deadline, now)?
            .checkpoint(now)
    }
}

pub trait FencedExecutionLivenessJournal: Send + Sync {
    /// Concrete Home adapters use the original runtime AttemptContext. Keeping
    /// this associated avoids a dependency cycle with Run-owned Journal events.
    type Owner: Send + Sync;

    fn load<'a>(
        &'a self,
        owner: &'a Self::Owner,
        scope: ExecutionScope,
    ) -> ApplicationFuture<'a, Result<Option<ExecutionRecord>, LivenessJournalError>>;

    /// Only an original dispatch intent may enter here. Home validates the
    /// frozen policy/operation, samples its clock after acquiring the lane,
    /// and creates revision 1 / Starting only if the original row is absent.
    /// Production dispatch commits this within the existing invocation-start
    /// transaction; the standalone method also owns the same commit lane.
    fn admit<'a>(
        &'a self,
        owner: &'a Self::Owner,
        admission: ExecutionAdmission,
    ) -> ApplicationFuture<'a, Result<ExecutionRecord, LivenessJournalError>>;

    /// Atomically capture Home ingress and check deadlines after acquiring the
    /// original commit lane. Snapshotting before waiting for that lane can
    /// falsely time out work observed during database contention. The source
    /// is acknowledged only after the checkpoint/phase transaction commits.
    fn observe<'a>(
        &'a self,
        owner: &'a Self::Owner,
        expected_revision: u64,
        source: &'a mut dyn ExecutionActivitySource,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>>;

    /// Reduction time is sampled from the injected Home clock inside the
    /// acquired commit lane. A caller cannot supply its own current time.
    /// Activity retains its first Home observation time; the committed result
    /// is the only action authority.
    fn mutate<'a>(
        &'a self,
        owner: &'a Self::Owner,
        scope: ExecutionScope,
        expected_revision: u64,
        mutation: RecordMutation,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>>;
}

/// Maximum rows materialized by one due-index page, even if a caller asks for more.
pub const MAX_DUE_SCOPES: u16 = 256;

/// Scheduler read port: a tenant-partitioned due index, not a full Run/ledger
/// scan. The returned hint does not authorize a mutation or prove a timeout.
pub trait ExecutionLivenessDueIndex: Send + Sync {
    fn due<'a>(
        &'a self,
        tenant: TenantId,
        before: UtcTimestamp,
        limit: NonZeroU16,
    ) -> ApplicationFuture<'a, Result<Vec<DueExecutionScope>, LivenessJournalError>>;
}
