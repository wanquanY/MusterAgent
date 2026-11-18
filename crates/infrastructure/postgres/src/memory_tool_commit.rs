use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, ConversationId, CorrelationId, HomeEpoch, IdempotencyKey, InvocationId,
    LedgerOffset, LedgerRecordId, RunSeq, TenantId,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_memory::{
    ClassifyMemoryToolCommitError, CommitPreparedMemoryTool, FencedMemoryToolCommitter,
    MemoryToolCommitFailure, MemoryToolMutation, MemoryToolResult,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row as _};
use thiserror::Error;

use crate::artifact::{authorize_result_content, complete_tool};
use crate::database_failure::{
    PostgresFailureClass as DatabaseFailureClass, classify as classify_database_failure,
};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};
use crate::memory::{
    PostgresMemoryEvolutionError, propose_transaction, publish_explicit_user_memory_transaction,
    remember_user_transaction,
};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresFencedMemoryToolCommitter {
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedMemoryToolCommitter {
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
        let audit = Arc::new(crate::PostgresSecurityAuditPipeline::standard(
            database.clone(),
        ));
        Self::with_security_audit(database, encoder, reference_keys, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        encoder: Arc<dyn LedgerPayloadEncoder>,
        reference_keys: Arc<OpaqueRefKeyring>,
        audit: Arc<crate::PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            liveness_clock: None,
            ledger: PostgresLedgerStore::new(database),
            pool,
            encoder,
            reference_keys,
            audit,
        }
    }

    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedMemoryTool,
    ) -> Result<MemoryToolResult, PostgresFencedMemoryToolError> {
        command.validate(attempt)?;
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresFencedMemoryToolError::StaleFence)?;
        let state = self
            .load_state_on(&mut transaction, attempt, &command)
            .await?;
        if let MemoryToolMutation::Publish(publication) = &command.mutation {
            crate::memory_evolution_commit::authorize_tool_publication(
                &mut transaction,
                command.invocation_id,
                publication,
            )
            .await?;
        }
        let record = self.record(&state, attempt, &command)?;
        let mut append = state.append(command.invocation_id, record)?;
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
                self.verify_duplicate(&command).await?;
                crate::tool_security_audit::replay_succeeded(
                    &self.audit,
                    &self.pool,
                    command.run_context.tenant_id,
                    command.invocation_id,
                )
                .await?;
                return Ok(MemoryToolResult::Duplicate);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
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
            command.run_context.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await?;
        authorize_result_content(
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            &command.result,
        )
        .await?;
        match &command.mutation {
            MemoryToolMutation::Remember(value) => {
                remember_user_transaction(prepared.transaction_mut(), value).await?;
                publish_explicit_user_memory_transaction(
                    prepared.transaction_mut(),
                    value,
                    command.committed_at,
                )
                .await?;
            }
            MemoryToolMutation::Propose(value) => {
                propose_transaction(prepared.transaction_mut(), value).await?;
            }
            MemoryToolMutation::Publish(value) => {
                crate::memory_evolution_commit::publish_evolution_transaction(
                    prepared.transaction_mut(),
                    command.invocation_id,
                    value,
                )
                .await?;
            }
        }
        complete_tool(
            prepared.transaction_mut(),
            &self.reference_keys,
            command.run_context.tenant_id,
            command.invocation_id,
            &command.result,
            command.committed_at,
            true,
        )
        .await?;
        crate::dialogue_publication::interaction::memory_tool_changed(
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            command.invocation_id,
        )
        .await?;
        crate::tool_security_audit::append_succeeded_in_transaction(
            &self.audit,
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            command.invocation_id,
        )
        .await?;
        if let Some(change) = &liveness {
            change.persist(prepared.transaction_mut()).await?;
        }
        advance_run(
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            command.run_context.run_id,
            state
                .last_run_sequence
                .get()
                .checked_add(u64::try_from(append.records.len())?)
                .ok_or(PostgresFencedMemoryToolError::Numeric)?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(MemoryToolResult::Committed)
    }

    async fn load_state_on(
        &self,
        connection: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        command: &CommitPreparedMemoryTool,
    ) -> Result<MemoryRunState, PostgresFencedMemoryToolError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id AND attempt.run_id = run.run_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id
              AND lease.run_id = run.run_id
              AND lease.attempt_id = attempt.attempt_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             WHERE run.run_id = $1 AND run.status = 2
               AND attempt.attempt_id = $2 AND attempt.attempt_number = $3
               AND attempt.status = 1
               AND lease.worker_id = $4 AND lease.lease_id = $5
               AND lease.fencing_token = $6::bigint
               AND lease.expires_at_ms >
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get())?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(PostgresFencedMemoryToolError::StaleFence)?;
        let state = MemoryRunState {
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch")?)?)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            last_run_sequence: RunSeq::new(parse_u64(row.try_get("last_run_sequence")?)?)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            conversation_offset: LedgerOffset::new(parse_u64(row.try_get("conversation_offset")?)?),
        };
        if state.tenant_id != command.run_context.tenant_id
            || state.conversation_id != command.run_context.conversation_id
            || state.activity_id != command.run_context.activity_id
            || state.home_epoch != command.run_context.home_epoch
        {
            return Err(PostgresFencedMemoryToolError::IdentityMismatch);
        }
        Ok(state)
    }

    fn record(
        &self,
        state: &MemoryRunState,
        attempt: &AttemptContext,
        command: &CommitPreparedMemoryTool,
    ) -> Result<ProposedRecord, PostgresFencedMemoryToolError> {
        let payload = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Tool(
                ToolEvent::InvocationSucceeded {
                    invocation_id: command.invocation_id,
                    result_ref: command.result.result_ref.clone(),
                    result_hash: command.result.result_hash,
                    provider_request_ref: command.result.provider_request_ref.clone(),
                    authorized_content: Vec::new(),
                },
            )));
        Ok(ProposedRecord {
            schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?,
            record_id: record_id(command.invocation_id)?,
            record_kind: LedgerRecordKind::DomainEvent,
            activity_id: Some(state.activity_id),
            run_id: Some(command.run_context.run_id),
            run_seq: Some(
                RunSeq::new(
                    state
                        .last_run_sequence
                        .get()
                        .checked_add(1)
                        .ok_or(PostgresFencedMemoryToolError::Numeric)?,
                )
                .map_err(|_| PostgresFencedMemoryToolError::Numeric)?,
            ),
            attempt_id: Some(attempt.attempt_id),
            participant_id: Some(command.run_context.executor_participant_id),
            producer: Producer::Worker,
            occurred_at: command.committed_at,
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            payload_hash: Sha256::digest(&payload),
            payload,
        })
    }

    async fn verify_duplicate(
        &self,
        command: &CommitPreparedMemoryTool,
    ) -> Result<(), PostgresFencedMemoryToolError> {
        let tool_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM tool.tool_invocations AS invocation
                 JOIN tool.tool_results AS result USING (invocation_id)
                 WHERE invocation.tenant_id = $1
                   AND invocation.invocation_id = $2
                   AND invocation.status = 4
                   AND invocation.result_content_id = $3
                   AND result.result_hash = $4
             )",
        )
        .bind(command.run_context.tenant_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .bind(command.result.result_ref.content_id.into_uuid())
        .bind(command.result.result_hash.into_bytes().as_slice())
        .fetch_one(&self.pool)
        .await?;
        let mutation_matches: bool = match &command.mutation {
            MemoryToolMutation::Remember(value) => {
                if value.retains_current_version() {
                    // Probe-only publication: the kernel Unit of Work is
                    // always rolled back.
                    let mut transaction = self
                        .ledger
                        .database()
                        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Memory)
                        .await?;
                    publish_explicit_user_memory_transaction(
                        &mut transaction,
                        value,
                        command.committed_at,
                    )
                    .await?;
                    transaction.rollback().await?;
                }
                sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM memory.memory_versions AS version
                         JOIN memory.memories AS memory USING (memory_id)
                         JOIN memory.explicit_publications AS publication
                           ON publication.memory_version_id = version.memory_version_id
                          AND publication.memory_id = memory.memory_id
                          AND publication.tenant_id = memory.tenant_id
                         WHERE memory.tenant_id = $1
                           AND memory.memory_id = $2
                           AND version.memory_version_id = $3
                           AND version.content_id = $4
                           AND version.content_hash = $5
                           AND ($10 OR (publication.tool_invocation_id = $6
                           AND publication.approval_interaction_id = $7
                           AND publication.request_content_id = $8
                           AND publication.source_user_record_id = $9))
                     )",
                )
                .bind(value.record.tenant_id.into_uuid())
                .bind(value.record.memory_id.into_uuid())
                .bind(value.version.memory_version_id.into_uuid())
                .bind(value.version.content_ref.content_id.into_uuid())
                .bind(value.version.content_ref.sha256.into_bytes().as_slice())
                .bind(value.authorization.invocation_id.into_uuid())
                .bind(value.authorization.approval_interaction_id.into_uuid())
                .bind(value.authorization.request_ref.content_id.into_uuid())
                .bind(value.authorization.origin.record_id.into_uuid())
                .bind(value.retains_current_version())
                .fetch_one(&self.pool)
                .await?
            }
            MemoryToolMutation::Propose(value) => {
                sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM evolution.proposals
                         WHERE tenant_id = $1 AND proposal_id = $2
                           AND candidate_content_id = $3
                           AND candidate_content_hash = $4
                     )",
                )
                .bind(value.tenant_id.into_uuid())
                .bind(value.proposal_id.into_uuid())
                .bind(value.claim_ref.content_id.into_uuid())
                .bind(value.claim_ref.sha256.into_bytes().as_slice())
                .fetch_one(&self.pool)
                .await?
            }
            MemoryToolMutation::Publish(value) => {
                sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM evolution.promotions AS promotion
                         JOIN evolution.approvals AS approval
                           ON approval.approval_interaction_id=promotion.approval_interaction_id
                          AND approval.proposal_id=promotion.proposal_id
                          AND approval.evaluation_id=promotion.evaluation_id
                         WHERE promotion.proposal_id=$1
                           AND promotion.published_memory_version_id=$2
                           AND promotion.approval_interaction_id=$3
                     )",
                )
                .bind(value.promotion.proposal_id.into_uuid())
                .bind(value.memory_version.memory_version_id.into_uuid())
                .bind(value.approval.approval_interaction_id.into_uuid())
                .fetch_one(&self.pool)
                .await?
            }
        };
        if tool_matches && mutation_matches {
            Ok(())
        } else {
            Err(PostgresFencedMemoryToolError::IdempotencyConflict)
        }
    }
}

impl FencedMemoryToolCommitter for PostgresFencedMemoryToolCommitter {
    type Error = PostgresFencedMemoryToolError;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedMemoryTool,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<MemoryToolResult, Self::Error>>
    {
        let attempt = attempt.clone();
        Box::pin(async move { self.commit_inner(&attempt, command).await })
    }
}

struct MemoryRunState {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    home_epoch: HomeEpoch,
    last_run_sequence: RunSeq,
    conversation_offset: LedgerOffset,
}

impl MemoryRunState {
    fn append(
        &self,
        invocation_id: InvocationId,
        record: ProposedRecord,
    ) -> Result<AppendRequest, PostgresFencedMemoryToolError> {
        let idempotency_key = IdempotencyKey::new(format!("memory-tool:{invocation_id}"))
            .map_err(|_| PostgresFencedMemoryToolError::Corrupt)?;
        let mut fingerprint = idempotency_key.as_str().as_bytes().to_vec();
        fingerprint.extend_from_slice(&record.payload_hash.into_bytes());
        Ok(AppendRequest {
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            expected_home_epoch: self.home_epoch,
            expected_offset: self.conversation_offset,
            idempotency_key,
            command_fingerprint: Sha256::digest(fingerprint),
            records: vec![record],
            outbox: Vec::new(),
        })
    }
}

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), PostgresFencedMemoryToolError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id
         FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2
           AND run_id = $3
           AND execution_attempt_id = $4
           AND status = 3
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if locked == Some(invocation_id.into_uuid()) {
        Ok(())
    } else {
        Err(PostgresFencedMemoryToolError::ToolTransitionConflict)
    }
}

async fn advance_run(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
    next_sequence: u64,
) -> Result<(), PostgresFencedMemoryToolError> {
    let result = sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $3::bigint
         WHERE tenant_id = $1 AND run_id = $2 AND status = 2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(next_sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresFencedMemoryToolError::StaleFence)
    }
}

fn attempt_fence(attempt: &AttemptContext, home_epoch: HomeEpoch) -> AttemptFence {
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

fn record_id(invocation_id: InvocationId) -> Result<LedgerRecordId, PostgresFencedMemoryToolError> {
    let mut seed = b"musteragent.legacy.memory.tool.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| PostgresFencedMemoryToolError::Corrupt)
}

fn parse_u64(value: String) -> Result<u64, PostgresFencedMemoryToolError> {
    value
        .parse()
        .map_err(|_| PostgresFencedMemoryToolError::Corrupt)
}

#[derive(Debug, Error)]
pub enum PostgresFencedMemoryToolError {
    #[error(transparent)]
    Interaction(#[from] crate::PostgresInteractionError),
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error(transparent)]
    Contract(#[from] musteragent_runtime_memory::MemoryToolError),
    #[error(transparent)]
    Memory(#[from] PostgresMemoryEvolutionError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Artifact(#[from] crate::PostgresArtifactError),
    #[error("Memory Tool Attempt lease is stale")]
    StaleFence,
    #[error("Memory Tool identity differs from its Run")]
    IdentityMismatch,
    #[error("Memory Tool is not Running under the active Attempt")]
    ToolTransitionConflict,
    #[error("Memory Tool idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Memory Tool durable state is corrupt")]
    Corrupt,
    #[error("Memory Tool numeric value is out of range")]
    Numeric,
    #[error("PostgreSQL fenced Memory commit failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Memory Tool Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
}

impl From<crate::commit_lane::CommitLaneError> for PostgresFencedMemoryToolError {
    fn from(error: crate::commit_lane::CommitLaneError) -> Self {
        match error {
            crate::commit_lane::CommitLaneError::StaleFence => Self::StaleFence,
            crate::commit_lane::CommitLaneError::Corrupt => Self::Corrupt,
            crate::commit_lane::CommitLaneError::Numeric
            | crate::commit_lane::CommitLaneError::NumericConversion(_)
            | crate::commit_lane::CommitLaneError::NumericRange(_) => Self::Numeric,
            crate::commit_lane::CommitLaneError::Database(error) => Self::Sql(error),
            crate::commit_lane::CommitLaneError::Transaction(error) => Self::Transaction(error),
        }
    }
}

impl ClassifyMemoryToolCommitError for PostgresFencedMemoryToolError {
    fn failure(&self) -> MemoryToolCommitFailure {
        match self {
            Self::Liveness(error) => {
                use musteragent_runtime_contract::ErrorCode;
                match error.code {
                    ErrorCode::AttemptLeaseStale => MemoryToolCommitFailure::StaleFence,
                    ErrorCode::IdempotencyConflict | ErrorCode::ConversationSeqConflict => {
                        MemoryToolCommitFailure::Conflict
                    }
                    ErrorCode::InvalidCommand => MemoryToolCommitFailure::InvalidCommand,
                    ErrorCode::DependencyUnavailable => MemoryToolCommitFailure::Unavailable,
                    _ => MemoryToolCommitFailure::Corrupt,
                }
            }
            Self::Contract(error) => match error {
                musteragent_runtime_memory::MemoryToolError::InvalidInput
                | musteragent_runtime_memory::MemoryToolError::UnsupportedTool => {
                    MemoryToolCommitFailure::InvalidCommand
                }
                musteragent_runtime_memory::MemoryToolError::IdentityMismatch
                | musteragent_runtime_memory::MemoryToolError::InactiveMemory
                | musteragent_runtime_memory::MemoryToolError::PublicationNotReady => {
                    MemoryToolCommitFailure::Authorization
                }
                musteragent_runtime_memory::MemoryToolError::Content
                | musteragent_runtime_memory::MemoryToolError::Store => {
                    MemoryToolCommitFailure::Unavailable
                }
                musteragent_runtime_memory::MemoryToolError::ProgramUnavailable
                | musteragent_runtime_memory::MemoryToolError::Execution(_)
                | musteragent_runtime_memory::MemoryToolError::InvalidResult
                | musteragent_runtime_memory::MemoryToolError::VersionOverflow
                | musteragent_runtime_memory::MemoryToolError::Application(_) => {
                    MemoryToolCommitFailure::Corrupt
                }
            },
            Self::Memory(error) => match error {
                PostgresMemoryEvolutionError::SourceUnavailable => {
                    MemoryToolCommitFailure::Unavailable
                }
                PostgresMemoryEvolutionError::ApprovalNotSatisfied
                | PostgresMemoryEvolutionError::MemoryManagementUnauthorized => {
                    MemoryToolCommitFailure::Authorization
                }
                PostgresMemoryEvolutionError::ContentNotFound
                | PostgresMemoryEvolutionError::ContentIntegrityMismatch => {
                    MemoryToolCommitFailure::ContentIntegrity
                }
                PostgresMemoryEvolutionError::MemoryConflict
                | PostgresMemoryEvolutionError::MemoryVersionConflict
                | PostgresMemoryEvolutionError::SearchProjectionErased
                | PostgresMemoryEvolutionError::MemoryNotFound
                | PostgresMemoryEvolutionError::DraftNotFound
                | PostgresMemoryEvolutionError::DraftConflict
                | PostgresMemoryEvolutionError::DraftIdempotencyConflict
                | PostgresMemoryEvolutionError::EvolutionConflict
                | PostgresMemoryEvolutionError::EvolutionNotFound
                | PostgresMemoryEvolutionError::ProposalNotVerified
                | PostgresMemoryEvolutionError::EvaluationNotPassing
                | PostgresMemoryEvolutionError::StaleBackfillLease => {
                    MemoryToolCommitFailure::Conflict
                }
                PostgresMemoryEvolutionError::Sql(error) => database_failure(error),
                PostgresMemoryEvolutionError::Transaction(error) => transaction_failure(error),
                PostgresMemoryEvolutionError::Application(_)
                | PostgresMemoryEvolutionError::InvalidBackfillClaim
                | PostgresMemoryEvolutionError::MemoryContract(_)
                | PostgresMemoryEvolutionError::EvolutionContract(_)
                | PostgresMemoryEvolutionError::InstinctContract(_)
                | PostgresMemoryEvolutionError::Json(_)
                | PostgresMemoryEvolutionError::NumericRange
                | PostgresMemoryEvolutionError::CorruptDatabase => MemoryToolCommitFailure::Corrupt,
            },
            Self::Ledger(PostgresLedgerError::Database(error))
            | Self::Execution(PostgresExecutionError::Database(error))
            | Self::Artifact(crate::PostgresArtifactError::Sql(error))
            | Self::Sql(error)
            | Self::Audit(crate::tool_security_audit::AtomicToolSecurityAuditError::Database(
                error,
            )) => database_failure(error),
            Self::Ledger(PostgresLedgerError::Transaction(error))
            | Self::Execution(PostgresExecutionError::Transaction(error))
            | Self::Transaction(error)
            | Self::Audit(crate::tool_security_audit::AtomicToolSecurityAuditError::Audit(
                crate::PostgresSecurityAuditError::Transaction(error),
            )) => transaction_failure(error),
            Self::Audit(crate::tool_security_audit::AtomicToolSecurityAuditError::Audit(
                crate::PostgresSecurityAuditError::Database(error),
            )) => database_failure(error),
            Self::Execution(
                PostgresExecutionError::StaleFence
                | PostgresExecutionError::LeaseExpiredOrNotExtended
                | PostgresExecutionError::LeaseNotExpiredOrStale,
            )
            | Self::StaleFence => MemoryToolCommitFailure::StaleFence,
            Self::IdentityMismatch
            | Self::Artifact(crate::PostgresArtifactError::IdentityMismatch) => {
                MemoryToolCommitFailure::Authorization
            }
            Self::ToolTransitionConflict
            | Self::IdempotencyConflict
            | Self::Artifact(
                crate::PostgresArtifactError::ToolTransitionConflict
                | crate::PostgresArtifactError::ArtifactConflict
                | crate::PostgresArtifactError::VersionConflict
                | crate::PostgresArtifactError::IdempotencyConflict,
            ) => MemoryToolCommitFailure::Conflict,
            Self::Artifact(crate::PostgresArtifactError::ContentUnavailable) => {
                MemoryToolCommitFailure::ContentIntegrity
            }
            Self::Interaction(crate::PostgresInteractionError::Sql(error)) => {
                database_failure(error)
            }
            Self::Interaction(crate::PostgresInteractionError::Transaction(error)) => {
                transaction_failure(error)
            }
            Self::Interaction(_)
            | Self::Ledger(_)
            | Self::Execution(_)
            | Self::Artifact(_)
            | Self::Corrupt
            | Self::Numeric
            | Self::Audit(_) => MemoryToolCommitFailure::Corrupt,
        }
    }
}

fn database_failure(error: &sqlx::Error) -> MemoryToolCommitFailure {
    match classify_database_failure(error) {
        DatabaseFailureClass::Unavailable => MemoryToolCommitFailure::Unavailable,
        DatabaseFailureClass::Invalid => {
            tracing::error!(
                error = ?error,
                "PostgreSQL Memory Tool commit was rejected by a deterministic database error"
            );
            MemoryToolCommitFailure::Corrupt
        }
    }
}

fn transaction_failure(error: &crate::PostgresTransactionError) -> MemoryToolCommitFailure {
    match error.failure_class() {
        crate::PostgresFailureClass::SerializationFailure
        | crate::PostgresFailureClass::Deadlock
        | crate::PostgresFailureClass::PoolAcquireTimeout
        | crate::PostgresFailureClass::LockTimeout
        | crate::PostgresFailureClass::StatementTimeout
        | crate::PostgresFailureClass::Unavailable => MemoryToolCommitFailure::Unavailable,
        crate::PostgresFailureClass::Constraint | crate::PostgresFailureClass::Other => {
            tracing::error!(
                error = ?error,
                "PostgreSQL Memory Tool transaction failed deterministically"
            );
            MemoryToolCommitFailure::Corrupt
        }
    }
}

impl From<std::num::TryFromIntError> for PostgresFencedMemoryToolError {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::Numeric
    }
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_memory::{ClassifyMemoryToolCommitError, MemoryToolCommitFailure};

    use super::PostgresFencedMemoryToolError;

    #[test]
    fn deterministic_database_rejection_is_not_retryable() {
        let invalid = PostgresFencedMemoryToolError::Sql(sqlx::Error::Protocol(
            "deterministic statement rejection".to_owned(),
        ));
        assert_eq!(invalid.failure(), MemoryToolCommitFailure::Corrupt);

        let unavailable = PostgresFencedMemoryToolError::Sql(sqlx::Error::PoolClosed);
        assert_eq!(unavailable.failure(), MemoryToolCommitFailure::Unavailable);
    }
}
