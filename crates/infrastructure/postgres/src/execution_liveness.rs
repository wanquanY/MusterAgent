//! PostgreSQL supervision shares the original Conversation commit lane.
use crate::commit_lane::{CommitLaneError, CommitLaneState};
use crate::ledger::{PostgresLedgerError, PostgresLedgerStore};
use crate::{GovernedPostgresTransaction, PostgresDatabaseRuntime};
use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_execution_liveness::{
    DueExecutionScope, ExecutionAdmission, ExecutionLivenessDueIndex, ExecutionRecord,
    FencedExecutionLivenessJournal, LivenessHomeClock, LivenessJournalError, RecordMutation,
    RecordTransition,
};
use musteragent_identity::{TenantId, UtcTimestamp};
use musteragent_ledger::LedgerPayloadEncoder;
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::execution_liveness::{ExecutionBinding, ExecutionScope};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use sqlx::Row as _;
use std::{num::NonZeroU16, sync::Arc};
mod admission;
mod auxiliary;
mod material;
pub(crate) use auxiliary::auxiliary_stop_confirmed;
mod effect_terminal;
mod lifecycle;
mod model_lifecycle;
mod observations;
pub(crate) use effect_terminal::stage_tool_terminal;
pub(crate) use lifecycle::LifecycleChange;
mod publication;
mod storage;
mod tool_authority;
mod transitions;
pub(crate) use tool_authority::validate_tool_content_access;

#[derive(Clone)]
pub struct PostgresFencedExecutionLivenessJournal {
    database: PostgresDatabaseRuntime,
    ledger: PostgresLedgerStore,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    prepared_tool: Option<Arc<material::PreparedTool>>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    clock: Arc<dyn LivenessHomeClock>,
}
impl PostgresFencedExecutionLivenessJournal {
    /// Exact public dependencies of sealed selections; never live account authority.
    #[must_use]
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    pub(crate) fn supervision(
        &self,
        activities: Arc<dyn musteragent_execution_liveness::ExecutionActivityFactory>,
    ) -> musteragent_loop::ExecutionSupervision {
        musteragent_loop::ExecutionSupervision::new(
            Arc::new(self.clone()),
            self.clock.clone(),
            activities,
        )
    }

    #[must_use]
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        clock: Arc<dyn LivenessHomeClock>,
    ) -> Self {
        let database = database.into();
        Self {
            ledger: PostgresLedgerStore::new(database.clone()),
            database,
            snapshots,
            prepared_tool: None,
            application_catalogs: None,
            encoder,
            decoder,
            clock,
        }
    }
    async fn admit_inner(
        &self,
        owner: &AttemptContext,
        admission: ExecutionAdmission,
    ) -> Result<ExecutionRecord, StoreError> {
        let (_, invocation) = scope_parts(admission.binding.scope);
        let prepared = if matches!(admission.binding.scope, ExecutionScope::Invocation(_)) {
            self.prepare_material_inner(owner, None, Some(invocation))
                .await?
        } else {
            self.clone()
        };
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, owner).await?;
        admission::validate_initial(&state, owner, &admission)?;
        if let Some(existing) = storage::load(
            &mut tx,
            state.tenant_id,
            owner.run_id,
            admission.binding.scope,
        )
        .await?
        {
            if existing.binding() != admission.binding
                || existing.policy() != admission.policy.as_ref()
            {
                return Err(StoreError::Conflict);
            }
            tx.rollback().await?;
            return Ok(existing);
        }
        admission::validate(&prepared, &mut tx, &state, owner, &admission).await?;
        let now = self.clock.sample().map_err(StoreError::Rejected)?;
        let record: ExecutionRecord = admission
            .into_checkpoint(now)
            .map_err(|_| StoreError::Invalid)?
            .into();
        self.commit(tx, &state, owner, None, &record).await?;
        Ok(record)
    }
    async fn mutate_inner(
        &self,
        owner: &AttemptContext,
        scope: ExecutionScope,
        expected_revision: u64,
        mutation: RecordMutation,
    ) -> Result<RecordTransition, StoreError> {
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, owner).await?;
        let current = storage::load(&mut tx, state.tenant_id, owner.run_id, scope)
            .await?
            .ok_or(StoreError::Invalid)?;
        let target = match &mutation {
            RecordMutation::BeginRecovery { rebound, .. }
            | RecordMutation::RebindPassive { rebound }
            | RecordMutation::AbandonRecovery { rebound } => *rebound,
            _ => current.binding(),
        };
        validate_owner(target, &state, owner)?;
        transitions::validate(self, &mut tx, &state, owner, &current, &mutation).await?;
        let observed = self.clock.sample().map_err(StoreError::Rejected)?;
        let result = current
            .reduce(expected_revision, mutation, observed)
            .map_err(StoreError::Rejected)?;
        if current == result.next {
            tx.rollback().await?;
            return Ok(result);
        }
        self.commit(tx, &state, owner, Some(&current), &result.next)
            .await?;
        Ok(result)
    }
}
impl FencedExecutionLivenessJournal for PostgresFencedExecutionLivenessJournal {
    type Owner = AttemptContext;
    fn load<'a>(
        &'a self,
        owner: &'a AttemptContext,
        scope: ExecutionScope,
    ) -> ApplicationFuture<'a, Result<Option<ExecutionRecord>, LivenessJournalError>> {
        Box::pin(async move {
            async {
                let (mut tx, state) = crate::commit_lane::acquire(&self.database, owner).await?;
                let record = storage::load(&mut tx, state.tenant_id, owner.run_id, scope).await?;
                tx.rollback().await?;
                Ok(record)
            }
            .await
            .map_err(StoreError::port)
        })
    }
    fn admit<'a>(
        &'a self,
        owner: &'a AttemptContext,
        admission: ExecutionAdmission,
    ) -> ApplicationFuture<'a, Result<ExecutionRecord, LivenessJournalError>> {
        Box::pin(async move {
            self.admit_inner(owner, admission)
                .await
                .map_err(StoreError::port)
        })
    }
    fn observe<'a>(
        &'a self,
        owner: &'a AttemptContext,
        expected_revision: u64,
        source: &'a mut dyn musteragent_execution_liveness::ExecutionActivitySource,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            self.observe_inner(owner, expected_revision, source)
                .await
                .map_err(StoreError::port)
        })
    }
    fn mutate<'a>(
        &'a self,
        owner: &'a AttemptContext,
        scope: ExecutionScope,
        expected_revision: u64,
        mutation: RecordMutation,
    ) -> ApplicationFuture<'a, Result<RecordTransition, LivenessJournalError>> {
        Box::pin(async move {
            self.mutate_inner(owner, scope, expected_revision, mutation)
                .await
                .map_err(StoreError::port)
        })
    }
}
impl ExecutionLivenessDueIndex for PostgresFencedExecutionLivenessJournal {
    fn due<'a>(
        &'a self,
        tenant: TenantId,
        before: UtcTimestamp,
        limit: NonZeroU16,
    ) -> ApplicationFuture<'a, Result<Vec<DueExecutionScope>, LivenessJournalError>> {
        Box::pin(async move {
            async {
            let mut tx=self.database.read_unit_of_work(musteragent_telemetry::DbCommitKind::LedgerOutbox,crate::PostgresTransactionIsolation::ReadCommitted).await?;
            let rows=sqlx::query("SELECT * FROM runtime.execution_liveness WHERE tenant_id=$1 AND NOT closed AND due_at_ms IS NOT NULL AND due_at_ms<=$2 ORDER BY due_at_ms,run_id,scope_kind,scope_invocation_id LIMIT $3")
                .bind(tenant.into_uuid()).bind(before.unix_millis()).bind(i64::from(limit.get().min(musteragent_execution_liveness::MAX_DUE_SCOPES))).fetch_all(&mut *tx).await?;
            let mut due=Vec::with_capacity(rows.len());
            for row in rows { let record=storage::decode(&row)?; due.push(DueExecutionScope{binding:record.binding(),revision:record.revision(),due_at:record.next_due_utc().map_err(|_|StoreError::Corrupt)?.ok_or(StoreError::Corrupt)?}); }
            tx.rollback().await?; Ok(due)
        }.await.map_err(StoreError::port)
        })
    }
}
fn scope_parts(scope: ExecutionScope) -> (i16, musteragent_identity::InvocationId) {
    match scope {
        ExecutionScope::Invocation(id) => (1, id),
        ExecutionScope::RunHandoff {
            previous_invocation,
        } => (2, previous_invocation),
    }
}
fn validate_owner(
    binding: ExecutionBinding,
    state: &CommitLaneState,
    owner: &AttemptContext,
) -> Result<(), StoreError> {
    if binding.tenant_id != state.tenant_id
        || binding.run_id != owner.run_id
        || binding.owner_attempt_id != owner.attempt_id
        || binding.owner_fence != owner.fencing_token
    {
        return Err(StoreError::Stale);
    }
    Ok(())
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("frozen application source is unavailable")]
    MaterialUnavailable,
    #[error("liveness transition is invalid")]
    Invalid,
    #[error(transparent)]
    Rejected(musteragent_execution_liveness::LivenessError),
    #[error("liveness state is corrupt")]
    Corrupt,
    #[error("liveness revision conflicts")]
    Conflict,
    #[error("liveness owner is stale")]
    Stale,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Lane(#[from] CommitLaneError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Fence(#[from] crate::PostgresExecutionError),
    #[error(transparent)]
    Numeric(#[from] crate::numeric::BigintRangeError),
}
impl StoreError {
    pub(crate) fn port(self) -> LivenessJournalError {
        let rejection = match &self {
            Self::Rejected(error) => Some(*error),
            _ => None,
        };
        let code = match &self {
            Self::Rejected(musteragent_execution_liveness::LivenessError::RevisionMismatch) => {
                ErrorCode::IdempotencyConflict
            }
            Self::Rejected(musteragent_execution_liveness::LivenessError::ClockUnavailable) => {
                ErrorCode::DependencyUnavailable
            }
            Self::Rejected(_) => ErrorCode::InvalidCommand,
            Self::Stale
            | Self::Lane(CommitLaneError::StaleFence)
            | Self::Fence(crate::PostgresExecutionError::StaleFence) => {
                ErrorCode::AttemptLeaseStale
            }
            Self::Invalid => ErrorCode::InvalidCommand,
            Self::MaterialUnavailable => ErrorCode::DependencyUnavailable,
            Self::Conflict => ErrorCode::IdempotencyConflict,
            Self::Database(error)
            | Self::Lane(CommitLaneError::Database(error))
            | Self::Ledger(PostgresLedgerError::Database(error))
            | Self::Fence(crate::PostgresExecutionError::Database(error))
                if crate::database_failure::classify(error)
                    == crate::database_failure::PostgresFailureClass::Unavailable =>
            {
                ErrorCode::DependencyUnavailable
            }
            Self::Transaction(error)
            | Self::Lane(CommitLaneError::Transaction(error))
            | Self::Ledger(PostgresLedgerError::Transaction(error))
            | Self::Fence(crate::PostgresExecutionError::Transaction(error))
                if matches!(
                    error.failure_class(),
                    crate::PostgresFailureClass::SerializationFailure
                        | crate::PostgresFailureClass::Deadlock
                        | crate::PostgresFailureClass::PoolAcquireTimeout
                        | crate::PostgresFailureClass::LockTimeout
                        | crate::PostgresFailureClass::StatementTimeout
                        | crate::PostgresFailureClass::Unavailable
                ) =>
            {
                ErrorCode::DependencyUnavailable
            }
            _ => ErrorCode::InternalInvariantViolated,
        };
        LivenessJournalError {
            code,
            retryable: code == ErrorCode::DependencyUnavailable,
            rejection,
        }
    }
}
