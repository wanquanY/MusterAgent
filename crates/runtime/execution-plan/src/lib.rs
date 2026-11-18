//! Durable Run-scoped execution-plan ports shared by Cloud and Device homes.

#![forbid(unsafe_code)]

use musteragent_identity::{RunId, TenantId};
use musteragent_run::{AttemptContext, ExecutionPlan, RunContext};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitExecutionPlanUpdate {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub expected_revision: u64,
    /// Exact visible predecessor, revalidated in the commit transaction.
    pub inherit_from: Option<ExecutionPlan>,
    pub plan: ExecutionPlan,
    pub result: CapabilityResult,
    pub committed_at: musteragent_identity::UtcTimestamp,
}

impl CommitExecutionPlanUpdate {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), ExecutionPlanStoreError> {
        if self
            .inherit_from
            .as_ref()
            .is_some_and(|source| self.expected_revision != 0 || source.run_id == attempt.run_id)
            || self.run_context.run_id != attempt.run_id
            || self.plan.run_id != attempt.run_id
            || self.plan.updated_by_invocation_id != self.invocation_id
            || self.plan.updated_at != self.committed_at
            || self.plan.revision.get() != self.expected_revision.saturating_add(1)
            || self.result.provider_request_ref.is_some()
            || self.result.validate().is_err()
        {
            return Err(ExecutionPlanStoreError::invalid());
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionPlanCommitDisposition {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionPlanStoreFailure {
    InvalidCommand,
    ContentIntegrity,
    StaleFence,
    RevisionConflict,
    IdempotencyConflict,
    Corrupt,
    Unavailable,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionPlanStoreError {
    pub failure: ExecutionPlanStoreFailure,
    pub code: ErrorCode,
    pub retryable: bool,
}

impl ExecutionPlanStoreError {
    #[must_use]
    pub const fn invalid() -> Self {
        Self {
            failure: ExecutionPlanStoreFailure::InvalidCommand,
            code: ErrorCode::InvalidCommand,
            retryable: false,
        }
    }

    #[must_use]
    pub const fn revision_conflict() -> Self {
        Self {
            failure: ExecutionPlanStoreFailure::RevisionConflict,
            code: ErrorCode::IdempotencyConflict,
            retryable: false,
        }
    }
}

pub trait ExecutionPlanStore: Send + Sync {
    fn load(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionPlan>, ExecutionPlanStoreError>>;

    /// Returns the latest visible predecessor root plan (or an exact source).
    /// Implementations recheck current membership and active branch reachability.
    fn load_predecessor(
        &self,
        context: RunContext,
        source_run_id: Option<RunId>,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionPlan>, ExecutionPlanStoreError>> {
        let _ = (context, source_run_id);
        Box::pin(async { Err(ExecutionPlanStoreError::invalid()) })
    }

    fn commit_update<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitExecutionPlanUpdate,
    ) -> ApplicationFuture<'a, Result<ExecutionPlanCommitDisposition, ExecutionPlanStoreError>>;
}
