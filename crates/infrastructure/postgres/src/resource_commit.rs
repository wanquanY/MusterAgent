use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, ConversationId, CorrelationId, HomeEpoch, IdempotencyKey, InvocationId,
    LedgerOffset, LedgerRecordId, RunSeq, TenantId, WorkspaceId,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_resource::{
    CommitPreparedResourceTool, FencedResourceToolCommitter, ResourceMutationReceipt,
    ResourceStoreError, ResourceToolError,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row as _};
use thiserror::Error;

use crate::artifact::{authorize_result_content, complete_tool};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresFencedResourceToolCommitter {
    original_tool_receipts: Option<Arc<crate::PostgresFencedToolInvocationJournal>>,
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedResourceToolCommitter {
    /// Reuse the exact original Tool journal, including its sealed Snapshot and Ledger decoder.
    #[must_use]
    pub fn with_original_tool_receipts(
        mut self,
        journal: Arc<crate::PostgresFencedToolInvocationJournal>,
    ) -> Self {
        self.original_tool_receipts = Some(journal);
        self
    }

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
        Self {
            original_tool_receipts: None,
            liveness_clock: None,
            pool: database.pool().clone(),
            ledger: PostgresLedgerStore::new(database),
            encoder,
            reference_keys,
            audit,
        }
    }

    async fn commit_inner(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedResourceTool,
    ) -> Result<ResourceMutationReceipt, PostgresFencedResourceToolError> {
        command.validate(attempt)?;
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresFencedResourceToolError::StaleFence)?;
        let state = Self::load_state_on(&mut transaction, attempt, &command.run_context).await?;
        validate_scope(&state, command.mutation.context())?;
        let receipt = match &command.mutation {
            musteragent_runtime_resource::PreparedResourceMutation::ChangeSet(change_set)
            | musteragent_runtime_resource::PreparedResourceMutation::ChangeSets {
                first: change_set,
                ..
            } => {
                let receipt = crate::resource_change_set::commit_resource_change_set_on(
                    &mut transaction,
                    change_set,
                )
                .await?;
                if let musteragent_runtime_resource::PreparedResourceMutation::ChangeSets {
                    additional,
                    ..
                } = &command.mutation
                {
                    for next in additional {
                        validate_scope(&state, &next.context)?;
                        crate::resource_change_set::commit_resource_change_set_on(
                            &mut transaction,
                            next,
                        )
                        .await?;
                    }
                }
                let first = receipt
                    .mutations
                    .first()
                    .ok_or(PostgresFencedResourceToolError::Corrupt)?;
                ResourceMutationReceipt {
                    scope: receipt.scope,
                    node: first.node.clone(),
                    previous_revision: receipt.previous_revision,
                    revision: receipt.revision,
                    duplicate: receipt.duplicate,
                }
            }
            _ => {
                crate::resource::apply_prepared_resource_mutation(
                    &mut transaction,
                    &command.mutation,
                )
                .await?
            }
        };
        let artifact_events = crate::artifact::invocation_events(
            &mut transaction,
            command.run_context.tenant_id,
            command.invocation_id,
        )
        .await?;
        let records = self.records(&state, attempt, &command, artifact_events)?;
        let mut append = state.append("resource-tool", command.invocation_id, records)?;
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
                let receipt = self.verify_duplicate(&command).await?;
                crate::tool_security_audit::replay_succeeded(
                    &self.audit,
                    &self.pool,
                    command.run_context.tenant_id,
                    command.invocation_id,
                )
                .await?;
                return Ok(receipt);
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
            state.next_run_sequence(append.records.len())?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(receipt)
    }

    pub(crate) async fn load_state_on(
        connection: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        run_context: &RunContext,
    ) -> Result<ResourceRunState, PostgresFencedResourceToolError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset,
                    conversation.workspace_id
               FROM runtime.runs AS run
               JOIN runtime.attempts AS attempt
                 ON attempt.tenant_id = run.tenant_id AND attempt.run_id = run.run_id
               JOIN runtime.leases AS lease
                 ON lease.tenant_id = run.tenant_id AND lease.run_id = run.run_id
                AND lease.attempt_id = attempt.attempt_id
               JOIN event_store.ledger_streams AS stream
                 ON stream.tenant_id = run.tenant_id
                AND stream.conversation_id = run.conversation_id
               JOIN conversation.conversations AS conversation
                 ON conversation.tenant_id = run.tenant_id
                AND conversation.conversation_id = run.conversation_id
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
        .ok_or(PostgresFencedResourceToolError::StaleFence)?;
        let state = ResourceRunState {
            tenant_id: id(row.try_get("tenant_id")?)?,
            conversation_id: id(row.try_get("conversation_id")?)?,
            activity_id: id(row.try_get("activity_id")?)?,
            workspace_id: row
                .try_get::<Option<uuid::Uuid>, _>("workspace_id")?
                .map(WorkspaceId::from_uuid)
                .transpose()
                .map_err(|_| PostgresFencedResourceToolError::Corrupt)?,
            home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch")?)?)
                .map_err(|_| PostgresFencedResourceToolError::Corrupt)?,
            last_run_sequence: RunSeq::new(parse_u64(row.try_get("last_run_sequence")?)?)
                .map_err(|_| PostgresFencedResourceToolError::Corrupt)?,
            conversation_offset: LedgerOffset::new(parse_u64(row.try_get("conversation_offset")?)?),
        };
        if state.tenant_id != run_context.tenant_id
            || state.conversation_id != run_context.conversation_id
            || state.activity_id != run_context.activity_id
            || state.home_epoch != run_context.home_epoch
        {
            return Err(PostgresFencedResourceToolError::IdentityMismatch);
        }
        Ok(state)
    }

    fn records(
        &self,
        state: &ResourceRunState,
        attempt: &AttemptContext,
        command: &CommitPreparedResourceTool,
        artifact_events: Vec<musteragent_artifact::ArtifactEvent>,
    ) -> Result<Vec<ProposedRecord>, PostgresFencedResourceToolError> {
        let event = DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            invocation_id: command.invocation_id,
            result_ref: command.result.result_ref.clone(),
            result_hash: command.result.result_hash,
            provider_request_ref: command.result.provider_request_ref.clone(),
            authorized_content: Vec::new(),
        });
        std::iter::once(event)
            .chain(artifact_events.into_iter().map(DomainEvent::Artifact))
            .enumerate()
            .map(|(index, event)| {
                let encoded = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| PostgresFencedResourceToolError::Corrupt)?,
                    record_id: record_id(command.invocation_id, index)?,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(state.activity_id),
                    run_id: Some(command.run_context.run_id),
                    run_seq: Some(
                        RunSeq::new(
                            state
                                .last_run_sequence
                                .get()
                                .checked_add(u64::try_from(index)?.saturating_add(1))
                                .ok_or(PostgresFencedResourceToolError::Numeric)?,
                        )
                        .map_err(|_| PostgresFencedResourceToolError::Numeric)?,
                    ),
                    attempt_id: Some(attempt.attempt_id),
                    participant_id: Some(command.run_context.executor_participant_id),
                    producer: Producer::Worker,
                    occurred_at: command.committed_at,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&encoded),
                    payload: encoded,
                })
            })
            .collect()
    }

    async fn verify_duplicate(
        &self,
        command: &CommitPreparedResourceTool,
    ) -> Result<ResourceMutationReceipt, PostgresFencedResourceToolError> {
        let context = command.mutation.context();
        let identity = command.mutation.identity();
        let (scope_kind, scope_id) = crate::resource::encode_scope(context.scope());
        let matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                   FROM tool.tool_invocations AS invocation
                   JOIN tool.tool_results AS result USING (invocation_id)
                   JOIN resource.change_sets AS change_set
                     ON change_set.source_invocation_id = invocation.invocation_id
                  WHERE invocation.tenant_id = $1
                    AND invocation.invocation_id = $2
                    AND invocation.status = 4
                    AND invocation.result_content_id = $3
                    AND result.result_hash = $4
                    AND change_set.scope_kind = $5 AND change_set.scope_id = $6
                    AND change_set.change_set_id = $7
                    AND change_set.command_fingerprint = $8
                    AND change_set.status = 2
             )",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .bind(command.result.result_ref.content_id.into_uuid())
        .bind(command.result.result_hash.into_bytes().to_vec())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(identity.change_set_id.into_uuid())
        .bind(identity.command_fingerprint.into_bytes().to_vec())
        .fetch_one(&self.pool)
        .await?;
        if !matches {
            return Err(PostgresFencedResourceToolError::IdempotencyConflict);
        }
        let row = sqlx::query(
            "SELECT expected_revision, committed_revision, result_node_id
               FROM resource.change_sets
              WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3
                AND change_set_id = $4 AND status = 2",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(identity.change_set_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        let node_id =
            musteragent_identity::ResourceNodeId::from_uuid(row.try_get("result_node_id")?)
                .map_err(|_| PostgresFencedResourceToolError::Corrupt)?;
        let node =
            crate::resource::load_node_from_pool(&self.pool, context.tenant_id, node_id).await?;
        Ok(ResourceMutationReceipt {
            scope: context.scope(),
            node,
            previous_revision: musteragent_identity::ResourceRevision::new(u64::try_from(
                row.try_get::<i64, _>("expected_revision")?,
            )?),
            revision: musteragent_identity::ResourceRevision::new(u64::try_from(
                row.try_get::<i64, _>("committed_revision")?,
            )?),
            duplicate: true,
        })
    }
}

impl FencedResourceToolCommitter for PostgresFencedResourceToolCommitter {
    fn lookup_original_receipt(
        &self,
        attempt: &AttemptContext,
        query: musteragent_runtime_capability::OriginalToolReceiptQuery,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_capability::ToolTerminalOutcome>,
            musteragent_runtime_capability::ToolExecutionPortError,
        >,
    > {
        let attempt = attempt.clone();
        Box::pin(async move {
            let journal = self.original_tool_receipts.as_ref().ok_or(
                musteragent_runtime_capability::ToolExecutionPortError {
                    code: musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
                    retryable: false,
                },
            )?;
            journal.lookup_original_receipt(&attempt, query).await
        })
    }

    type Error = PostgresFencedResourceToolError;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedResourceTool,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ResourceMutationReceipt, Self::Error>,
    > {
        let attempt = attempt.clone();
        Box::pin(async move { self.commit_inner(&attempt, command).await })
    }
}

pub(crate) struct ResourceRunState {
    pub(crate) tenant_id: TenantId,
    pub(crate) conversation_id: ConversationId,
    pub(crate) activity_id: ActivityId,
    pub(crate) workspace_id: Option<WorkspaceId>,
    pub(crate) home_epoch: HomeEpoch,
    pub(crate) last_run_sequence: RunSeq,
    pub(crate) conversation_offset: LedgerOffset,
}

impl ResourceRunState {
    pub(crate) fn next_run_sequence(
        &self,
        record_count: usize,
    ) -> Result<u64, PostgresFencedResourceToolError> {
        self.last_run_sequence
            .get()
            .checked_add(u64::try_from(record_count)?)
            .ok_or(PostgresFencedResourceToolError::Numeric)
    }

    pub(crate) fn append(
        &self,
        namespace: &str,
        invocation_id: InvocationId,
        records: Vec<ProposedRecord>,
    ) -> Result<AppendRequest, PostgresFencedResourceToolError> {
        let idempotency_key = IdempotencyKey::new(format!("{namespace}:{invocation_id}"))
            .map_err(|_| PostgresFencedResourceToolError::Corrupt)?;
        let mut fingerprint = idempotency_key.as_str().as_bytes().to_vec();
        for record in &records {
            fingerprint.extend_from_slice(&record.payload_hash.into_bytes());
        }
        Ok(AppendRequest {
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            expected_home_epoch: self.home_epoch,
            expected_offset: self.conversation_offset,
            idempotency_key,
            command_fingerprint: Sha256::digest(fingerprint),
            records,
            outbox: Vec::new(),
        })
    }
}

pub(crate) fn validate_scope(
    state: &ResourceRunState,
    context: &musteragent_runtime_resource::ResourceCommandContext,
) -> Result<(), PostgresFencedResourceToolError> {
    if context.workspace_id() != state.workspace_id {
        Err(PostgresFencedResourceToolError::IdentityMismatch)
    } else {
        Ok(())
    }
}

pub(crate) async fn lock_invocation(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), PostgresFencedResourceToolError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id FROM tool.tool_invocations
          WHERE tenant_id = $1 AND invocation_id = $2 AND run_id = $3
            AND ((status = 3 AND execution_attempt_id = $4)
                 OR (status = 6 AND retry_contract = 3))
          FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    if locked.is_some() {
        Ok(())
    } else {
        Err(PostgresFencedResourceToolError::ToolTransitionConflict)
    }
}

pub(crate) async fn advance_run(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
    next_sequence: u64,
) -> Result<(), PostgresFencedResourceToolError> {
    let updated = sqlx::query(
        "UPDATE runtime.runs SET last_run_sequence = $3::bigint, run_version = run_version + 1
          WHERE tenant_id = $1 AND run_id = $2 AND status = 2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(next_sequence.to_string())
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresFencedResourceToolError::StaleFence)
    }
}

pub(crate) fn attempt_fence(attempt: &AttemptContext, home_epoch: HomeEpoch) -> AttemptFence {
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

fn record_id(
    invocation_id: InvocationId,
    index: usize,
) -> Result<LedgerRecordId, PostgresFencedResourceToolError> {
    let mut seed = b"musteragent.resource.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    if index > 0 {
        seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    }
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| PostgresFencedResourceToolError::Corrupt)
}

fn id<T: FromUuid>(value: uuid::Uuid) -> Result<T, PostgresFencedResourceToolError> {
    T::from_uuid(value).map_err(|_| PostgresFencedResourceToolError::Corrupt)
}

fn parse_u64(value: String) -> Result<u64, PostgresFencedResourceToolError> {
    value
        .parse()
        .map_err(|_| PostgresFencedResourceToolError::Corrupt)
}

trait FromUuid: Sized {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError>;
}

macro_rules! from_uuid {
    ($($identity:ty),+ $(,)?) => {
        $(impl FromUuid for $identity {
            fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError> {
                Self::from_uuid(value)
            }
        })+
    };
}

from_uuid!(TenantId, ConversationId, ActivityId);

#[derive(Debug, Error)]
pub enum PostgresFencedResourceToolError {
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error(transparent)]
    Contract(#[from] ResourceToolError),
    #[error(transparent)]
    Resource(#[from] ResourceStoreError),
    #[error(transparent)]
    ResourceApplication(#[from] musteragent_runtime_resource::ResourceApplicationError),
    #[error(transparent)]
    Artifact(#[from] crate::PostgresArtifactError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error("Resource Tool numeric value is out of range")]
    Numeric,
    #[error("PostgreSQL fenced Resource commit failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Resource Tool Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
    #[error("Resource Tool Attempt lease is stale")]
    StaleFence,
    #[error("Resource Tool identity differs from its Run")]
    IdentityMismatch,
    #[error("Resource Tool is not Running under the active Attempt")]
    ToolTransitionConflict,
    #[error("Resource Tool idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Resource Tool durable state is corrupt")]
    Corrupt,
}

impl From<crate::commit_lane::CommitLaneError> for PostgresFencedResourceToolError {
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

impl From<std::num::TryFromIntError> for PostgresFencedResourceToolError {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::Numeric
    }
}

impl musteragent_runtime_resource::ClassifyResourceToolCommitError
    for PostgresFencedResourceToolError
{
    fn failure(&self) -> musteragent_runtime_resource::ResourceToolCommitFailure {
        use musteragent_runtime_resource::{
            ResourceApplicationError, ResourceStoreError, ResourceToolCommitFailure,
        };

        match self {
            Self::Liveness(error) => {
                use musteragent_runtime_contract::ErrorCode;
                match error.code {
                    ErrorCode::AttemptLeaseStale => ResourceToolCommitFailure::StaleFence,
                    ErrorCode::IdempotencyConflict | ErrorCode::ConversationSeqConflict => {
                        ResourceToolCommitFailure::Conflict
                    }
                    ErrorCode::InvalidCommand => ResourceToolCommitFailure::InvalidCommand,
                    ErrorCode::DependencyUnavailable => ResourceToolCommitFailure::Unavailable,
                    _ => ResourceToolCommitFailure::Corrupt,
                }
            }
            Self::StaleFence => ResourceToolCommitFailure::StaleFence,
            Self::IdentityMismatch
            | Self::Contract(ResourceToolError::IdentityMismatch)
            | Self::ResourceApplication(ResourceApplicationError::IdentityMismatch) => {
                ResourceToolCommitFailure::Authorization
            }
            Self::Contract(
                ResourceToolError::UnsupportedTool
                | ResourceToolError::InvalidInput
                | ResourceToolError::InvalidIdentifier
                | ResourceToolError::InvalidResult,
            )
            | Self::ResourceApplication(
                ResourceApplicationError::Store(ResourceStoreError::BoundsExceeded)
                | ResourceApplicationError::InvalidPage
                | ResourceApplicationError::InvalidSortKey
                | ResourceApplicationError::InvalidPath
                | ResourceApplicationError::InvalidFile
                | ResourceApplicationError::InvalidChangeSet,
            ) => ResourceToolCommitFailure::InvalidCommand,
            Self::Resource(ResourceStoreError::PrivateTaskResult)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::PrivateTaskResult,
            )) => ResourceToolCommitFailure::PrivateTaskResult,
            Self::Resource(ResourceStoreError::NameConflict)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::NameConflict,
            )) => ResourceToolCommitFailure::NameConflict,
            Self::Resource(ResourceStoreError::WriterBusy)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::WriterBusy,
            )) => ResourceToolCommitFailure::WriterBusy,
            Self::Resource(ResourceStoreError::RevisionConflict)
            | Self::Resource(ResourceStoreError::MoveCycle)
            | Self::Resource(ResourceStoreError::FileUnavailable)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::RevisionConflict
                | ResourceStoreError::MoveCycle
                | ResourceStoreError::FileUnavailable,
            ))
            | Self::ToolTransitionConflict
            | Self::IdempotencyConflict => ResourceToolCommitFailure::Conflict,
            Self::Contract(ResourceToolError::ContentUnavailable)
            | Self::Contract(ResourceToolError::SearchUnavailable)
            | Self::Resource(ResourceStoreError::DependencyUnavailable)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::DependencyUnavailable,
            ))
            | Self::Ledger(_)
            | Self::Execution(_)
            | Self::Sql(_)
            | Self::Transaction(_)
            | Self::Audit(_) => ResourceToolCommitFailure::Unavailable,
            Self::Resource(ResourceStoreError::BoundsExceeded)
            | Self::Resource(ResourceStoreError::NodeUnavailable)
            | Self::Resource(ResourceStoreError::ParentUnavailable)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::NodeUnavailable | ResourceStoreError::ParentUnavailable,
            )) => ResourceToolCommitFailure::InvalidCommand,
            Self::Contract(ResourceToolError::Resource(error)) => match error {
                ResourceApplicationError::Store(ResourceStoreError::PrivateTaskResult) => {
                    ResourceToolCommitFailure::PrivateTaskResult
                }
                ResourceApplicationError::Store(ResourceStoreError::WriterBusy) => {
                    ResourceToolCommitFailure::WriterBusy
                }
                ResourceApplicationError::IdentityMismatch => {
                    ResourceToolCommitFailure::Authorization
                }
                ResourceApplicationError::Store(ResourceStoreError::BoundsExceeded)
                | ResourceApplicationError::InvalidPage
                | ResourceApplicationError::InvalidSortKey
                | ResourceApplicationError::InvalidPath
                | ResourceApplicationError::InvalidFile
                | ResourceApplicationError::InvalidChangeSet
                | ResourceApplicationError::Store(ResourceStoreError::NodeUnavailable)
                | ResourceApplicationError::Store(ResourceStoreError::ParentUnavailable) => {
                    ResourceToolCommitFailure::InvalidCommand
                }
                ResourceApplicationError::Store(ResourceStoreError::NameConflict) => {
                    ResourceToolCommitFailure::NameConflict
                }
                ResourceApplicationError::Store(
                    ResourceStoreError::RevisionConflict
                    | ResourceStoreError::MoveCycle
                    | ResourceStoreError::FileUnavailable,
                ) => ResourceToolCommitFailure::Conflict,
                ResourceApplicationError::Store(ResourceStoreError::DependencyUnavailable) => {
                    ResourceToolCommitFailure::Unavailable
                }
                ResourceApplicationError::Store(ResourceStoreError::Corrupt) => {
                    ResourceToolCommitFailure::Corrupt
                }
            },
            Self::Contract(ResourceToolError::Search(error)) => match error {
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::PrivateTaskResult,
                ) => ResourceToolCommitFailure::PrivateTaskResult,
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::WriterBusy,
                ) => ResourceToolCommitFailure::WriterBusy,
                musteragent_runtime_resource::ResourceSearchError::IdentityMismatch => {
                    ResourceToolCommitFailure::Authorization
                }
                musteragent_runtime_resource::ResourceSearchError::InvalidQuery => {
                    ResourceToolCommitFailure::InvalidCommand
                }
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::DependencyUnavailable,
                ) => ResourceToolCommitFailure::Unavailable,
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::RevisionConflict
                    | ResourceStoreError::NameConflict
                    | ResourceStoreError::MoveCycle
                    | ResourceStoreError::FileUnavailable,
                ) => ResourceToolCommitFailure::Conflict,
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::NodeUnavailable
                    | ResourceStoreError::ParentUnavailable
                    | ResourceStoreError::BoundsExceeded,
                ) => ResourceToolCommitFailure::InvalidCommand,
                musteragent_runtime_resource::ResourceSearchError::Store(
                    ResourceStoreError::Corrupt,
                ) => ResourceToolCommitFailure::Corrupt,
            },
            Self::Resource(ResourceStoreError::Corrupt)
            | Self::ResourceApplication(ResourceApplicationError::Store(
                ResourceStoreError::Corrupt,
            ))
            | Self::Artifact(_)
            | Self::Numeric
            | Self::Corrupt => ResourceToolCommitFailure::Corrupt,
        }
    }
}
