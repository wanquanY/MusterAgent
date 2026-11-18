//! Durable, attempt-fenced Run execution-plan persistence.

pub(crate) mod history;

use std::num::NonZeroU64;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ContentId, CorrelationId, InvocationId, LedgerRecordId, RunId, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_run::{AttemptContext, ExecutionPlan, RunContext, RunEvent};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_execution_plan::{
    CommitExecutionPlanUpdate, ExecutionPlanCommitDisposition, ExecutionPlanStore,
    ExecutionPlanStoreError, ExecutionPlanStoreFailure,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Postgres, Row as _};
use thiserror::Error;

use crate::artifact::{PostgresArtifactError, authorize_result_content, complete_tool};
use crate::commit_lane::{CommitLaneError, CommitLaneState};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresExecutionPlanStore {
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresExecutionPlanStore {
    /// Supply the same Home clock used by the invocation Journal and activity observers.
    #[must_use]
    pub fn with_execution_liveness(
        mut self,
        clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    ) -> Self {
        self.liveness_clock = Some(clock);
        self
    }

    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self {
            liveness_clock: None,
            ledger: PostgresLedgerStore::new(database),
            pool,
            encoder,
            reference_keys,
            audit,
        }
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<Option<ExecutionPlan>, PlanError> {
        load_plan(&self.pool, tenant_id, run_id, false).await
    }

    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        command: CommitExecutionPlanUpdate,
    ) -> Result<ExecutionPlanCommitDisposition, PlanError> {
        command
            .validate(attempt)
            .map_err(|_| PlanError::InvalidCommand)?;
        let (mut transaction, state) =
            crate::commit_lane::acquire(self.ledger.database(), attempt).await?;
        validate_run_context(&state, &command.run_context)?;
        let mut append = self.append(&state, attempt, &command)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut transaction,
            self.liveness_clock.as_deref(),
            self.encoder.as_ref(),
            attempt,
            command.invocation_id,
            &mut append,
        )
        .await?;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return if self
                    .verify_duplicate(state.tenant_id, attempt.run_id, &command)
                    .await?
                {
                    crate::tool_security_audit::replay_succeeded(
                        &self.audit,
                        &self.pool,
                        state.tenant_id,
                        command.invocation_id,
                    )
                    .await?;
                    Ok(ExecutionPlanCommitDisposition::AlreadyApplied)
                } else {
                    Err(PlanError::IdempotencyConflict)
                };
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let current = load_plan(
            &mut *prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            true,
        )
        .await?;
        if let Some(source) = &command.inherit_from {
            let versioned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool.tool_invocations WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3 AND tool_ref='plan.update@2')")
                .bind(state.tenant_id.into_uuid()).bind(attempt.run_id.into_uuid()).bind(command.invocation_id.into_uuid())
                .fetch_one(&mut *prepared.transaction_mut()).await?;
            if !versioned {
                return Err(PlanError::InvalidCommand);
            }
            let authorized = history::predecessor(
                &mut *prepared.transaction_mut(),
                command.run_context,
                Some(source.run_id),
            )
            .await?;
            if authorized.as_ref() != Some(source) {
                return Err(PlanError::InvalidReference);
            }
        }
        if current.as_ref().map_or(0, |plan| plan.revision.get()) != command.expected_revision {
            return Err(PlanError::RevisionConflict);
        }
        let now = database_now_ms(prepared.transaction_mut()).await?;
        lock_and_validate_fence(
            prepared.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_invocation(
            prepared.transaction_mut(),
            state.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await?;
        authorize_content(
            prepared.transaction_mut(),
            state.tenant_id,
            &command.plan.plan_ref,
        )
        .await?;
        authorize_result_content(prepared.transaction_mut(), state.tenant_id, &command.result)
            .await?;
        write_projection(
            prepared.transaction_mut(),
            state.tenant_id,
            &command,
            deterministic_record_id(command.invocation_id, 1)?,
        )
        .await?;
        complete_tool(
            prepared.transaction_mut(),
            &self.reference_keys,
            state.tenant_id,
            command.invocation_id,
            &command.result,
            command.committed_at,
            false,
        )
        .await?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            prepared.transaction_mut(),
            state.tenant_id,
            command.invocation_id,
        )
        .await?;
        if let Some(change) = &liveness {
            change.persist(prepared.transaction_mut()).await?;
        }
        advance_run_sequence(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            state.next_sequence(
                u64::try_from(append.records.len()).map_err(|_| PlanError::Corrupt)?,
            )?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(ExecutionPlanCommitDisposition::Applied)
    }

    fn append(
        &self,
        state: &CommitLaneState,
        attempt: &AttemptContext,
        command: &CommitExecutionPlanUpdate,
    ) -> Result<AppendRequest, PlanError> {
        let events = [
            DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                invocation_id: command.invocation_id,
                result_ref: command.result.result_ref.clone(),
                result_hash: command.result.result_hash,
                provider_request_ref: None,
                authorized_content: Vec::new(),
            }),
            DomainEvent::Run(RunEvent::ExecutionPlanUpdated {
                revision: command.plan.revision,
                plan_ref: command.plan.plan_ref.clone(),
                source_invocation_id: command.invocation_id,
            }),
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let payload = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| PlanError::Corrupt)?,
                    record_id: deterministic_record_id(command.invocation_id, index)?,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(state.activity_id),
                    run_id: Some(attempt.run_id),
                    run_seq: Some(
                        RunSeq::new(state.next_sequence(u64::try_from(index)?.saturating_add(1))?)
                            .map_err(|_| PlanError::Corrupt)?,
                    ),
                    attempt_id: Some(attempt.attempt_id),
                    participant_id: Some(command.run_context.executor_participant_id),
                    producer: Producer::Worker,
                    occurred_at: command.committed_at,
                    causation_id: None,
                    correlation_id: CorrelationId::from_uuid(command.invocation_id.into_uuid())
                        .map_err(|_| PlanError::Corrupt)?,
                    payload_hash: Sha256::digest(&payload),
                    payload,
                })
            })
            .collect::<Result<Vec<_>, PlanError>>()?;
        Ok(state.append(format!("execution-plan:{}", command.invocation_id), records)?)
    }

    async fn verify_duplicate(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        command: &CommitExecutionPlanUpdate,
    ) -> Result<bool, PlanError> {
        let current = self.load_inner(tenant_id, run_id).await?;
        let result_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM tool.tool_invocations AS invocation
                 JOIN tool.tool_results AS result USING (invocation_id)
                 WHERE invocation.tenant_id = $1 AND invocation.run_id = $2
                   AND invocation.invocation_id = $3 AND invocation.status = 4
                   AND invocation.result_content_id = $4 AND result.result_hash = $5
                   AND result.result_schema_key = $6 AND result.result_schema_hash = $7
                   AND result.committed_at_ms = $8
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .bind(command.result.result_ref.content_id.into_uuid())
        .bind(command.result.result_hash.into_bytes().as_slice())
        .bind(command.result.result_schema.schema_key.as_str())
        .bind(
            command
                .result
                .result_schema
                .schema_hash
                .into_bytes()
                .as_slice(),
        )
        .bind(command.committed_at.unix_millis())
        .fetch_one(&self.pool)
        .await?;
        Ok(result_matches && current.as_ref() == Some(&command.plan))
    }
}

impl ExecutionPlanStore for PostgresExecutionPlanStore {
    fn load(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionPlan>, ExecutionPlanStoreError>> {
        Box::pin(async move {
            self.load_inner(tenant_id, run_id)
                .await
                .map_err(PlanError::port)
        })
    }

    fn load_predecessor(
        &self,
        context: RunContext,
        source: Option<RunId>,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionPlan>, ExecutionPlanStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(PlanError::Transaction)
                .map_err(PlanError::port)?;
            let result = history::predecessor(&mut tx, context, source)
                .await
                .map_err(PlanError::port)?;
            tx.commit()
                .await
                .map_err(PlanError::Transaction)
                .map_err(PlanError::port)?;
            Ok(result)
        })
    }

    fn commit_update<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitExecutionPlanUpdate,
    ) -> ApplicationFuture<'a, Result<ExecutionPlanCommitDisposition, ExecutionPlanStoreError>>
    {
        Box::pin(async move {
            self.commit_inner(attempt, command)
                .await
                .map_err(PlanError::port)
        })
    }
}

async fn load_plan<'e, E>(
    executor: E,
    tenant_id: TenantId,
    run_id: RunId,
    for_update: bool,
) -> Result<Option<ExecutionPlan>, PlanError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let suffix = if for_update { " FOR UPDATE" } else { "" };
    let query = format!(
        "SELECT revision::text AS revision, plan_content_id, plan_media_type,
                plan_byte_length::text AS plan_byte_length, plan_hash,
                source_invocation_id, updated_at_ms
         FROM runtime.execution_plans WHERE tenant_id = $1 AND run_id = $2{suffix}"
    );
    let Some(row) = sqlx::query(&query)
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_optional(executor)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(ExecutionPlan {
        run_id,
        revision: NonZeroU64::new(parse_u64(row.try_get("revision")?)?)
            .ok_or(PlanError::Corrupt)?,
        plan_ref: ContentRef {
            content_id: ContentId::from_uuid(row.try_get("plan_content_id")?)
                .map_err(|_| PlanError::Corrupt)?,
            media_type: MediaType::new(row.try_get::<String, _>("plan_media_type")?)
                .map_err(|_| PlanError::Corrupt)?,
            byte_length: parse_u64(row.try_get("plan_byte_length")?)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("plan_hash")?)
                .map_err(|_| PlanError::Corrupt)?,
        },
        updated_by_invocation_id: InvocationId::from_uuid(row.try_get("source_invocation_id")?)
            .map_err(|_| PlanError::Corrupt)?,
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
    }))
}

fn validate_run_context(state: &CommitLaneState, context: &RunContext) -> Result<(), PlanError> {
    if state.tenant_id != context.tenant_id
        || state.conversation_id != context.conversation_id
        || state.activity_id != context.activity_id
        || state.home_epoch != context.home_epoch
    {
        return Err(PlanError::InvalidCommand);
    }
    Ok(())
}

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), PlanError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2 AND run_id = $3
           AND status = 3 AND execution_attempt_id = $4 FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if locked.is_some() {
        Ok(())
    } else {
        Err(PlanError::ToolTransition)
    }
}

async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    content: &ContentRef,
) -> Result<(), PlanError> {
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM artifact.content_refs
         WHERE tenant_id = $1 AND content_id = $2 AND state = 2
           AND media_type = $3 AND byte_length = $4::bigint AND content_hash = $5)",
    )
    .bind(tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().as_slice())
    .fetch_one(&mut *transaction)
    .await?;
    if valid {
        Ok(())
    } else {
        Err(PlanError::InvalidReference)
    }
}

async fn write_projection(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    command: &CommitExecutionPlanUpdate,
    record_id: LedgerRecordId,
) -> Result<(), PlanError> {
    let plan = &command.plan;
    let changed = if command.expected_revision == 0 {
        sqlx::query(
            "INSERT INTO runtime.execution_plans (
                 tenant_id, run_id, revision, plan_content_id, plan_media_type,
                 plan_byte_length, plan_hash, source_invocation_id, updated_at_ms, record_id
             ) VALUES ($1, $2, $3::bigint, $4, $5, $6::bigint, $7, $8, $9, $10)
             ON CONFLICT (tenant_id, run_id) DO NOTHING",
        )
        .bind(tenant_id.into_uuid())
        .bind(plan.run_id.into_uuid())
        .bind(plan.revision.get().to_string())
        .bind(plan.plan_ref.content_id.into_uuid())
        .bind(plan.plan_ref.media_type.as_str())
        .bind(plan.plan_ref.byte_length.to_string())
        .bind(plan.plan_ref.sha256.into_bytes().as_slice())
        .bind(plan.updated_by_invocation_id.into_uuid())
        .bind(plan.updated_at.unix_millis())
        .bind(record_id.into_uuid())
        .execute(&mut *transaction)
        .await?
    } else {
        sqlx::query(
            "UPDATE runtime.execution_plans SET
                 revision = $3::bigint, plan_content_id = $4, plan_media_type = $5,
                 plan_byte_length = $6::bigint, plan_hash = $7,
                 source_invocation_id = $8, updated_at_ms = $9, record_id = $10
             WHERE tenant_id = $1 AND run_id = $2 AND revision = $11::bigint",
        )
        .bind(tenant_id.into_uuid())
        .bind(plan.run_id.into_uuid())
        .bind(plan.revision.get().to_string())
        .bind(plan.plan_ref.content_id.into_uuid())
        .bind(plan.plan_ref.media_type.as_str())
        .bind(plan.plan_ref.byte_length.to_string())
        .bind(plan.plan_ref.sha256.into_bytes().as_slice())
        .bind(plan.updated_by_invocation_id.into_uuid())
        .bind(plan.updated_at.unix_millis())
        .bind(record_id.into_uuid())
        .bind(command.expected_revision.to_string())
        .execute(&mut *transaction)
        .await?
    };
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PlanError::RevisionConflict)
    }
}

async fn advance_run_sequence(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
    sequence: u64,
) -> Result<(), PlanError> {
    let changed = sqlx::query(
        "UPDATE runtime.runs SET last_run_sequence = $3::bigint, run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $2 AND status IN (2, 4)",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PlanError::TransitionConflict)
    }
}

fn attempt_fence(
    attempt: &AttemptContext,
    home_epoch: musteragent_identity::HomeEpoch,
) -> AttemptFence {
    AttemptFence {
        run_id: attempt.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        worker_id: attempt.worker_id.clone(),
        lease_id: attempt.lease_id,
        fencing_token: attempt.fencing_token,
        home_epoch,
    }
}

fn deterministic_record_id(
    invocation_id: InvocationId,
    index: usize,
) -> Result<LedgerRecordId, PlanError> {
    let mut seed = b"musteragent.legacy.execution-plan.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value)).map_err(|_| PlanError::Corrupt)
}

fn parse_u64(value: String) -> Result<u64, PlanError> {
    value.parse().map_err(|_| PlanError::Corrupt)
}

#[derive(Debug, Error)]
pub(crate) enum PlanError {
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error("invalid execution plan command")]
    InvalidCommand,
    #[error("execution plan content reference is invalid")]
    InvalidReference,
    #[error("execution plan revision conflict")]
    RevisionConflict,
    #[error("execution plan idempotency conflict")]
    IdempotencyConflict,
    #[error("attempt or Tool transition is stale")]
    ToolTransition,
    #[error("Run transition conflicted")]
    TransitionConflict,
    #[error("execution plan state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    CommitLane(#[from] CommitLaneError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Artifact(#[from] PostgresArtifactError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
    #[error(transparent)]
    Numeric(#[from] std::num::TryFromIntError),
}

impl PlanError {
    fn port(self) -> ExecutionPlanStoreError {
        let (failure, code, retryable) = match self {
            Self::Liveness(error) => (
                match error.code {
                    ErrorCode::AttemptLeaseStale => ExecutionPlanStoreFailure::StaleFence,
                    ErrorCode::IdempotencyConflict => {
                        ExecutionPlanStoreFailure::IdempotencyConflict
                    }
                    ErrorCode::InvalidCommand => ExecutionPlanStoreFailure::InvalidCommand,
                    ErrorCode::DependencyUnavailable => ExecutionPlanStoreFailure::Unavailable,
                    _ => ExecutionPlanStoreFailure::Corrupt,
                },
                error.code,
                error.retryable,
            ),
            Self::InvalidCommand | Self::InvalidReference => (
                ExecutionPlanStoreFailure::InvalidCommand,
                ErrorCode::InvalidCommand,
                false,
            ),
            Self::RevisionConflict => (
                ExecutionPlanStoreFailure::RevisionConflict,
                ErrorCode::IdempotencyConflict,
                false,
            ),
            Self::IdempotencyConflict => (
                ExecutionPlanStoreFailure::IdempotencyConflict,
                ErrorCode::IdempotencyConflict,
                false,
            ),
            Self::CommitLane(CommitLaneError::StaleFence)
            | Self::Execution(PostgresExecutionError::StaleFence) => (
                ExecutionPlanStoreFailure::StaleFence,
                ErrorCode::AttemptLeaseStale,
                false,
            ),
            Self::Database(_)
            | Self::Transaction(_)
            | Self::Ledger(PostgresLedgerError::Database(_))
            | Self::Artifact(
                PostgresArtifactError::Sql(_) | PostgresArtifactError::Transaction(_),
            )
            | Self::Audit(_) => (
                ExecutionPlanStoreFailure::Unavailable,
                ErrorCode::DependencyUnavailable,
                true,
            ),
            Self::TransitionConflict => (
                ExecutionPlanStoreFailure::Unavailable,
                ErrorCode::ConversationSeqConflict,
                true,
            ),
            Self::ToolTransition => (
                ExecutionPlanStoreFailure::Corrupt,
                ErrorCode::RunTransitionInvalid,
                false,
            ),
            Self::Corrupt
            | Self::Ledger(_)
            | Self::CommitLane(_)
            | Self::Execution(_)
            | Self::Artifact(_)
            | Self::Numeric(_) => (
                ExecutionPlanStoreFailure::Corrupt,
                ErrorCode::InternalInvariantViolated,
                false,
            ),
        };
        ExecutionPlanStoreError {
            failure,
            code,
            retryable,
        }
    }
}
