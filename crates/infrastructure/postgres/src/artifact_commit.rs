use std::sync::Arc;

use musteragent_artifact::ArtifactEvent;
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
use musteragent_runtime_artifact::{
    ArtifactCommitDisposition, ArtifactCommitReceipt, ArtifactMutation, CommitPreparedArtifactTool,
    FencedArtifactToolCommitter,
};
use musteragent_runtime_contract::DisplayText;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::artifact::{
    PostgresArtifactError, authorize_content, authorize_result_content, complete_tool,
    insert_artifact, insert_version, update_artifact,
};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

#[derive(Clone)]
pub struct PostgresFencedArtifactToolCommitter {
    original_tool_receipts: Option<Arc<crate::PostgresFencedToolInvocationJournal>>,
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedArtifactToolCommitter {
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
        let pool = database.pool().clone();
        Self {
            original_tool_receipts: None,
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
        command: CommitPreparedArtifactTool,
    ) -> Result<ArtifactCommitReceipt, PostgresFencedArtifactError> {
        command.validate(attempt)?;
        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresFencedArtifactError::StaleFence)?;
        let state = self
            .load_state_on(&mut transaction, attempt, &command)
            .await?;
        let task_scope =
            crate::artifact::task_result::verified_scope(&mut transaction, attempt.run_id)
                .await
                .map_err(task_asset_error)?;
        let records = self.records(&state, attempt, &command, task_scope.as_ref())?;
        let mut append = state.append(command.invocation_id, records)?;
        let liveness = crate::execution_liveness::stage_tool_terminal(
            &mut transaction,
            self.liveness_clock.as_deref(),
            self.encoder.as_ref(),
            attempt,
            command.invocation_id,
            &mut append,
        )
        .await?;
        let artifact_id = command.mutation.artifact_id();
        let version = command.mutation.version().version;
        let mut prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(ledger) => {
                if let Some(scope) = task_scope.as_ref() {
                    let mut connection = self.pool.acquire().await?;
                    crate::artifact::task_result::verify_duplicate(
                        &mut connection,
                        scope,
                        &command,
                    )
                    .await
                    .map_err(task_asset_error)?;
                } else {
                    self.verify_duplicate(&command).await?;
                }
                crate::tool_security_audit::replay_succeeded(
                    &self.audit,
                    &self.pool,
                    command.run_context.tenant_id,
                    command.invocation_id,
                )
                .await?;
                return Ok(ArtifactCommitReceipt {
                    disposition: ArtifactCommitDisposition::Duplicate,
                    artifact_id,
                    version,
                    ledger,
                });
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
        authorize_content(
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            command.mutation.version(),
        )
        .await?;
        authorize_result_content(
            prepared.transaction_mut(),
            command.run_context.tenant_id,
            &command.result,
        )
        .await?;
        if let Some(scope) = task_scope.as_ref() {
            crate::artifact::task_result::stage_artifact(
                prepared.transaction_mut(),
                scope,
                &command,
            )
            .await
            .map_err(task_asset_error)?;
        } else {
            match &command.mutation {
                ArtifactMutation::Create { artifact, version } => {
                    insert_artifact(prepared.transaction_mut(), artifact).await?;
                    insert_version(prepared.transaction_mut(), version).await?;
                }
                ArtifactMutation::Update {
                    artifact_id,
                    expected_version,
                    version,
                    title,
                } => {
                    if let Some(target) = &command.resource_replacement {
                        crate::resource_file_replacement::replace_file_on(
                            prepared.transaction_mut(),
                            target,
                            version,
                        )
                        .await?;
                    } else {
                        update_artifact(
                            prepared.transaction_mut(),
                            command.run_context.tenant_id,
                            command.run_context.conversation_id,
                            *artifact_id,
                            *expected_version,
                            version,
                            title.as_deref(),
                        )
                        .await?;
                    }
                    insert_version(prepared.transaction_mut(), version).await?;
                }
            }
            if let Some(placement) = &command.resource_placement {
                crate::resource::place_committed_file(
                    prepared.transaction_mut(),
                    placement,
                    artifact_id,
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
        let ledger = prepared.commit(&append).await?;
        Ok(ArtifactCommitReceipt {
            disposition: ArtifactCommitDisposition::Committed,
            artifact_id,
            version,
            ledger,
        })
    }

    async fn load_state_on(
        &self,
        connection: &mut sqlx::PgConnection,
        attempt: &AttemptContext,
        command: &CommitPreparedArtifactTool,
    ) -> Result<ArtifactRunState, PostgresFencedArtifactError> {
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
        .ok_or(PostgresFencedArtifactError::StaleFence)?;
        let state = ArtifactRunState {
            tenant_id: id(row.try_get("tenant_id")?)?,
            conversation_id: id(row.try_get("conversation_id")?)?,
            activity_id: id(row.try_get("activity_id")?)?,
            home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch")?)?)
                .map_err(|_| PostgresFencedArtifactError::Corrupt)?,
            last_run_sequence: RunSeq::new(parse_u64(row.try_get("last_run_sequence")?)?)
                .map_err(|_| PostgresFencedArtifactError::Corrupt)?,
            conversation_offset: LedgerOffset::new(parse_u64(row.try_get("conversation_offset")?)?),
        };
        if state.tenant_id != command.run_context.tenant_id
            || state.conversation_id != command.run_context.conversation_id
            || state.activity_id != command.run_context.activity_id
            || state.home_epoch != command.run_context.home_epoch
        {
            return Err(PostgresFencedArtifactError::IdentityMismatch);
        }
        Ok(state)
    }

    fn records(
        &self,
        state: &ArtifactRunState,
        attempt: &AttemptContext,
        command: &CommitPreparedArtifactTool,
        task_scope: Option<&musteragent_runtime_session::WorkItemResultScope>,
    ) -> Result<Vec<ProposedRecord>, PostgresFencedArtifactError> {
        let mut events = vec![DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            invocation_id: command.invocation_id,
            result_ref: command.result.result_ref.clone(),
            result_hash: command.result.result_hash,
            provider_request_ref: command.result.provider_request_ref.clone(),
            authorized_content: if task_scope.is_some() {
                Vec::new()
            } else {
                command.authorized_content.clone()
            },
        })];
        if task_scope.is_none() {
            if let ArtifactMutation::Create { artifact, .. } = &command.mutation {
                events.push(DomainEvent::Artifact(ArtifactEvent::Created {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                    title: DisplayText::new(artifact.title.clone())
                        .map_err(|_| PostgresFencedArtifactError::Corrupt)?,
                    source_invocation_id: Some(command.invocation_id),
                }));
            }
            let version = command.mutation.version();
            events.push(DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                artifact_id: version.artifact_id,
                version: version.version,
                content_ref: version.content_ref.clone(),
                dependency_refs: version.dependency_refs.clone(),
                content_hash: version.content_hash,
                source_invocation_id: version.source_invocation_id,
            }));
        }
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let encoded = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| PostgresFencedArtifactError::Corrupt)?,
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
                                .ok_or(PostgresFencedArtifactError::Numeric)?,
                        )
                        .map_err(|_| PostgresFencedArtifactError::Numeric)?,
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
        command: &CommitPreparedArtifactTool,
    ) -> Result<(), PostgresFencedArtifactError> {
        let matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM tool.tool_invocations AS invocation
                 JOIN tool.tool_results AS result USING (invocation_id)
                 JOIN artifact.artifact_versions AS version
                   ON version.source_invocation_id = invocation.invocation_id
                 WHERE invocation.tenant_id = $1
                   AND invocation.invocation_id = $2
                   AND invocation.status = 4
                   AND invocation.result_content_id = $3
                   AND result.result_hash = $4
                   AND version.artifact_id = $5
                   AND version.artifact_version_id = $6
                   AND version.version = $7::bigint
                   AND version.content_id = $8
                   AND version.content_hash = $9
                   AND version.source_invocation_id = $2
                   AND version.committed_at_ms = $10
             )",
        )
        .bind(command.run_context.tenant_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .bind(command.result.result_ref.content_id.into_uuid())
        .bind(command.result.result_hash.into_bytes().as_slice())
        .bind(command.mutation.artifact_id().into_uuid())
        .bind(command.mutation.version().artifact_version_id.into_uuid())
        .bind(command.mutation.version().version.get().to_string())
        .bind(
            command
                .mutation
                .version()
                .content_ref
                .content_id
                .into_uuid(),
        )
        .bind(
            command
                .mutation
                .version()
                .content_hash
                .into_bytes()
                .as_slice(),
        )
        .bind(command.mutation.version().committed_at.unix_millis())
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT dependency.dependency_ordinal, dependency.content_id,
                    dependency.content_media_type,
                    dependency.content_byte_length::text AS content_byte_length,
                    dependency.content_hash
             FROM artifact.artifact_version_dependencies AS dependency
             WHERE dependency.artifact_version_id = $1
             ORDER BY dependency.dependency_ordinal",
        )
        .bind(command.mutation.version().artifact_version_id.into_uuid())
        .fetch_all(&self.pool)
        .await?;
        let dependencies_match = rows.len() == command.mutation.version().dependency_refs.len()
            && rows
                .iter()
                .zip(&command.mutation.version().dependency_refs)
                .enumerate()
                .all(|(ordinal, (row, expected))| {
                    row.try_get::<i16, _>("dependency_ordinal")
                        .ok()
                        .and_then(|value| usize::try_from(value).ok())
                        == Some(ordinal)
                        && row.try_get::<uuid::Uuid, _>("content_id").ok()
                            == Some(expected.content_id.into_uuid())
                        && row
                            .try_get::<String, _>("content_media_type")
                            .ok()
                            .as_deref()
                            == Some(expected.media_type.as_str())
                        && row
                            .try_get::<String, _>("content_byte_length")
                            .ok()
                            .as_deref()
                            == Some(expected.byte_length.to_string().as_str())
                        && row.try_get::<Vec<u8>, _>("content_hash").ok().as_deref()
                            == Some(expected.sha256.into_bytes().as_slice())
                });
        if matches && dependencies_match {
            Ok(())
        } else {
            Err(PostgresFencedArtifactError::IdempotencyConflict)
        }
    }
}

impl FencedArtifactToolCommitter for PostgresFencedArtifactToolCommitter {
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

    type Error = PostgresFencedArtifactError;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedArtifactTool,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ArtifactCommitReceipt, Self::Error>,
    > {
        let attempt = attempt.clone();
        Box::pin(async move { self.commit_inner(&attempt, command).await })
    }
}

struct ArtifactRunState {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    home_epoch: HomeEpoch,
    last_run_sequence: RunSeq,
    conversation_offset: LedgerOffset,
}

impl ArtifactRunState {
    fn next_run_sequence(&self, record_count: usize) -> Result<u64, PostgresFencedArtifactError> {
        self.last_run_sequence
            .get()
            .checked_add(u64::try_from(record_count)?)
            .ok_or(PostgresFencedArtifactError::Numeric)
    }

    fn append(
        &self,
        invocation_id: InvocationId,
        records: Vec<ProposedRecord>,
    ) -> Result<AppendRequest, PostgresFencedArtifactError> {
        let idempotency_key = IdempotencyKey::new(format!("artifact-tool:{invocation_id}"))
            .map_err(|_| PostgresFencedArtifactError::Corrupt)?;
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

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), PostgresFencedArtifactError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id
         FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2
           AND run_id = $3
           AND (
               (status = 3 AND (execution_attempt_id = $4 OR (retry_contract = 3 AND EXISTS (
                   SELECT 1 FROM runtime.execution_liveness live
                   WHERE live.tenant_id=$1 AND live.run_id=$3 AND live.scope_kind=1
                     AND live.scope_invocation_id=$2 AND live.owner_attempt_id=$4
                     AND live.owner_fence=$5 AND live.record_kind=2 AND NOT live.closed
               ))))
               OR (status = 6 AND retry_contract = 3)
           )
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(i64::try_from(attempt.fencing_token.get())?)
    .fetch_optional(&mut *transaction)
    .await?;
    if locked.is_some() {
        Ok(())
    } else {
        Err(PostgresFencedArtifactError::ToolTransitionConflict)
    }
}

async fn advance_run(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
    next_sequence: u64,
) -> Result<(), PostgresFencedArtifactError> {
    let updated = sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $3::bigint, run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $2 AND status = 2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(next_sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresFencedArtifactError::StaleFence)
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

fn record_id(
    invocation_id: InvocationId,
    index: usize,
) -> Result<LedgerRecordId, PostgresFencedArtifactError> {
    let mut seed = b"musteragent.legacy.artifact.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    LedgerRecordId::from_uuid(deterministic_uuid(&seed))
        .map_err(|_| PostgresFencedArtifactError::Corrupt)
}

fn deterministic_uuid(seed: &[u8]) -> uuid::Uuid {
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

fn id<T>(value: uuid::Uuid) -> Result<T, PostgresFencedArtifactError>
where
    T: FromUuid,
{
    T::from_uuid(value).map_err(|_| PostgresFencedArtifactError::Corrupt)
}

fn parse_u64(value: String) -> Result<u64, PostgresFencedArtifactError> {
    value
        .parse()
        .map_err(|_| PostgresFencedArtifactError::Corrupt)
}

trait FromUuid: Sized {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError>;
}

macro_rules! from_uuid {
    ($($identity:ty),+ $(,)?) => {
        $(impl FromUuid for $identity {
            fn from_uuid(
                value: uuid::Uuid,
            ) -> Result<Self, musteragent_identity::IdentityError> {
                Self::from_uuid(value)
            }
        })+
    };
}

from_uuid!(TenantId, ConversationId, ActivityId);

#[derive(Debug, Error)]
pub enum PostgresFencedArtifactError {
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error(transparent)]
    Contract(#[from] musteragent_runtime_artifact::ArtifactRuntimeError),
    #[error(transparent)]
    Artifact(#[from] PostgresArtifactError),
    #[error(transparent)]
    Resource(#[from] musteragent_runtime_resource::ResourceStoreError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error("Artifact Tool Attempt lease is stale")]
    StaleFence,
    #[error("Artifact Tool identity differs from its Run")]
    IdentityMismatch,
    #[error("Artifact Tool is not Running under the active Attempt")]
    ToolTransitionConflict,
    #[error("Artifact Tool idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Artifact Tool durable state is corrupt")]
    Corrupt,
    #[error("Artifact Tool numeric value is out of range")]
    Numeric,
    #[error("PostgreSQL fenced Artifact commit failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL Artifact Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
}

impl From<crate::commit_lane::CommitLaneError> for PostgresFencedArtifactError {
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

impl From<std::num::TryFromIntError> for PostgresFencedArtifactError {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::Numeric
    }
}

impl musteragent_runtime_artifact::ClassifyArtifactToolCommitError for PostgresFencedArtifactError {
    fn failure(&self) -> musteragent_runtime_artifact::ArtifactToolCommitFailure {
        use musteragent_runtime_artifact::ArtifactToolCommitFailure;
        use musteragent_runtime_resource::ResourceStoreError;

        match self {
            Self::Liveness(error) => {
                use musteragent_runtime_contract::ErrorCode;
                match error.code {
                    ErrorCode::AttemptLeaseStale => ArtifactToolCommitFailure::StaleFence,
                    ErrorCode::IdempotencyConflict | ErrorCode::ConversationSeqConflict => {
                        ArtifactToolCommitFailure::Conflict
                    }
                    ErrorCode::InvalidCommand => ArtifactToolCommitFailure::InvalidCommand,
                    ErrorCode::DependencyUnavailable => ArtifactToolCommitFailure::Unavailable,
                    _ => ArtifactToolCommitFailure::Corrupt,
                }
            }
            Self::Resource(ResourceStoreError::WriterBusy) => ArtifactToolCommitFailure::WriterBusy,
            Self::Resource(ResourceStoreError::NameConflict) => {
                ArtifactToolCommitFailure::NameConflict
            }
            Self::StaleFence => ArtifactToolCommitFailure::StaleFence,
            Self::IdentityMismatch
            | Self::Contract(
                musteragent_runtime_artifact::ArtifactRuntimeError::IdentityMismatch,
            )
            | Self::Artifact(PostgresArtifactError::IdentityMismatch)
            | Self::Artifact(PostgresArtifactError::ContentUnavailable) => {
                ArtifactToolCommitFailure::Authorization
            }
            Self::Resource(
                ResourceStoreError::RevisionConflict
                | ResourceStoreError::MoveCycle
                | ResourceStoreError::FileUnavailable,
            )
            | Self::Artifact(
                PostgresArtifactError::ArtifactConflict
                | PostgresArtifactError::VersionConflict
                | PostgresArtifactError::ToolTransitionConflict
                | PostgresArtifactError::IdempotencyConflict,
            )
            | Self::ToolTransitionConflict
            | Self::IdempotencyConflict => ArtifactToolCommitFailure::Conflict,
            Self::Resource(
                ResourceStoreError::NodeUnavailable
                | ResourceStoreError::ParentUnavailable
                | ResourceStoreError::BoundsExceeded
                | ResourceStoreError::PrivateTaskResult,
            )
            | Self::Artifact(
                PostgresArtifactError::InvocationNotFound | PostgresArtifactError::NotFound,
            ) => ArtifactToolCommitFailure::InvalidCommand,
            Self::Resource(ResourceStoreError::DependencyUnavailable)
            | Self::Artifact(
                PostgresArtifactError::Ledger(_)
                | PostgresArtifactError::Protection(_)
                | PostgresArtifactError::Sql(_)
                | PostgresArtifactError::Transaction(_),
            )
            | Self::Ledger(_)
            | Self::Execution(_)
            | Self::Sql(_)
            | Self::Transaction(_)
            | Self::Audit(_) => ArtifactToolCommitFailure::Unavailable,
            Self::Contract(_)
            | Self::Resource(ResourceStoreError::Corrupt)
            | Self::Artifact(
                PostgresArtifactError::Contract(_)
                | PostgresArtifactError::InvalidLedger
                | PostgresArtifactError::Corrupt,
            )
            | Self::Corrupt
            | Self::Numeric => ArtifactToolCommitFailure::Corrupt,
        }
    }
}

fn task_asset_error(
    error: musteragent_runtime_session::DialogueStoreError,
) -> PostgresFencedArtifactError {
    use musteragent_runtime_session::DialogueStoreError as E;
    match error {
        E::Corrupt => PostgresFencedArtifactError::Corrupt,
        E::Infrastructure => PostgresFencedArtifactError::Resource(
            musteragent_runtime_resource::ResourceStoreError::DependencyUnavailable,
        ),
        E::IdempotencyConflict => PostgresFencedArtifactError::IdempotencyConflict,
        E::RevisionConflict => {
            PostgresFencedArtifactError::Artifact(PostgresArtifactError::VersionConflict)
        }
        _ => PostgresFencedArtifactError::IdentityMismatch,
    }
}

#[cfg(test)]
mod classification_tests {
    use musteragent_runtime_artifact::{
        ArtifactToolCommitFailure, ClassifyArtifactToolCommitError,
    };
    use musteragent_runtime_resource::ResourceStoreError;

    use super::PostgresFencedArtifactError;

    #[test]
    fn resource_revision_conflict_is_not_a_retryable_dependency_failure() {
        let error = PostgresFencedArtifactError::Resource(ResourceStoreError::RevisionConflict);

        assert_eq!(error.failure(), ArtifactToolCommitFailure::Conflict);
    }

    #[test]
    fn resource_name_collision_preserves_the_recoverable_cause() {
        let error = PostgresFencedArtifactError::Resource(ResourceStoreError::NameConflict);
        assert_eq!(error.failure(), ArtifactToolCommitFailure::NameConflict);
    }
}
