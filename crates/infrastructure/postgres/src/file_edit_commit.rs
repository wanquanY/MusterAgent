use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{CorrelationId, InvocationId, LedgerRecordId, RunSeq};
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord};
use musteragent_run::AttemptContext;
use musteragent_runtime_resource::{
    ClassifyFileEditToolCommitError, CommitPreparedFileEditTool, FencedFileEditToolCommitter,
    FileEditError, FileEditMutationReceipt, FileEditToolCommitFailure, FileEditToolError,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::PgPool;
use thiserror::Error;

use crate::artifact::{authorize_result_content, complete_tool};
use crate::execution::lock_and_validate_fence;
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};
use crate::resource_commit::{
    PostgresFencedResourceToolCommitter, advance_run, attempt_fence, lock_invocation,
    validate_scope,
};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

/// Commits one prepared File Edit mutation, its Tool result and its Ledger
/// success fact as one fenced PostgreSQL Unit of Work.
#[derive(Clone)]
pub struct PostgresFencedFileEditToolCommitter {
    original_tool_receipts: Option<Arc<crate::PostgresFencedToolInvocationJournal>>,
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedFileEditToolCommitter {
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
        command: CommitPreparedFileEditTool,
    ) -> Result<FileEditMutationReceipt, PostgresFencedFileEditToolError> {
        command.validate(attempt)?;
        if let Some(receipt) = self.try_duplicate(&command).await? {
            crate::tool_security_audit::replay_succeeded(
                &self.audit,
                &self.pool,
                command.run_context.tenant_id,
                command.invocation_id,
            )
            .await?;
            return Ok(receipt);
        }

        let mut transaction =
            crate::commit_lane::acquire_for_run(self.ledger.database(), attempt.run_id)
                .await?
                .ok_or(PostgresFencedFileEditToolError::StaleFence)?;
        let state = PostgresFencedResourceToolCommitter::load_state_on(
            &mut transaction,
            attempt,
            &command.run_context,
        )
        .await?;
        validate_scope(&state, command.mutation.context())?;
        let now = database_now_ms(&mut transaction).await?;
        lock_and_validate_fence(
            &mut transaction,
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            now,
        )
        .await?;
        lock_invocation(
            &mut transaction,
            command.run_context.tenant_id,
            command.invocation_id,
            attempt,
        )
        .await?;

        let receipt = crate::resource_edit::apply_prepared_file_edit_mutation(
            &mut transaction,
            &command.mutation,
        )
        .await?;
        let result = command.results.select(&receipt)?;
        authorize_result_content(&mut transaction, command.run_context.tenant_id, &result).await?;
        let artifact_events = crate::artifact::invocation_events(
            &mut transaction,
            command.run_context.tenant_id,
            command.invocation_id,
        )
        .await?;
        let records = self.records(&state, attempt, &command, &result, artifact_events)?;
        let mut append = state.append("file-edit-tool", command.invocation_id, records)?;
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
                let receipt = self
                    .try_duplicate(&command)
                    .await?
                    .ok_or(PostgresFencedFileEditToolError::IdempotencyConflict)?;
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
        complete_tool(
            prepared.transaction_mut(),
            &self.reference_keys,
            command.run_context.tenant_id,
            command.invocation_id,
            &result,
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

    fn records(
        &self,
        state: &crate::resource_commit::ResourceRunState,
        attempt: &AttemptContext,
        command: &CommitPreparedFileEditTool,
        result: &musteragent_runtime_capability::CapabilityResult,
        artifact_events: Vec<musteragent_artifact::ArtifactEvent>,
    ) -> Result<Vec<ProposedRecord>, PostgresFencedFileEditToolError> {
        let event = DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            invocation_id: command.invocation_id,
            result_ref: result.result_ref.clone(),
            result_hash: result.result_hash,
            provider_request_ref: result.provider_request_ref.clone(),
            authorized_content: Vec::new(),
        });
        std::iter::once(event)
            .chain(artifact_events.into_iter().map(DomainEvent::Artifact))
            .enumerate()
            .map(|(index, event)| {
                let encoded = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| PostgresFencedFileEditToolError::Corrupt)?,
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
                                .ok_or(PostgresFencedFileEditToolError::Numeric)?,
                        )
                        .map_err(|_| PostgresFencedFileEditToolError::Numeric)?,
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

    /// Resolves a completed invocation back to the exact edit receipt and
    /// verifies that the dynamically selected success/conflict result is the
    /// result durably recorded for the Tool.
    async fn try_duplicate(
        &self,
        command: &CommitPreparedFileEditTool,
    ) -> Result<Option<FileEditMutationReceipt>, PostgresFencedFileEditToolError> {
        let completed: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM tool.tool_invocations
                  WHERE tenant_id = $1 AND invocation_id = $2 AND status = 4
             )",
        )
        .bind(command.run_context.tenant_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        if !completed {
            return Ok(None);
        }

        let mut transaction = self
            .ledger
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::ToolTerminal)
            .await?;
        let receipt = crate::resource_edit::apply_prepared_file_edit_mutation(
            &mut transaction,
            &command.mutation,
        )
        .await?;
        let result = command.results.select(&receipt)?;
        let matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                   FROM tool.tool_invocations AS invocation
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
        .bind(result.result_ref.content_id.into_uuid())
        .bind(result.result_hash.into_bytes().to_vec())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        if matches {
            Ok(Some(receipt))
        } else {
            Err(PostgresFencedFileEditToolError::IdempotencyConflict)
        }
    }
}

impl FencedFileEditToolCommitter for PostgresFencedFileEditToolCommitter {
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

    type Error = PostgresFencedFileEditToolError;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedFileEditTool,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<FileEditMutationReceipt, Self::Error>,
    > {
        let attempt = attempt.clone();
        Box::pin(async move { self.commit_inner(&attempt, command).await })
    }
}

fn record_id(
    invocation_id: InvocationId,
    index: usize,
) -> Result<LedgerRecordId, PostgresFencedFileEditToolError> {
    let mut seed = b"musteragent.file-edit.commit.v1\0".to_vec();
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
        .map_err(|_| PostgresFencedFileEditToolError::Corrupt)
}

#[derive(Debug, Error)]
pub enum PostgresFencedFileEditToolError {
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error(transparent)]
    Contract(#[from] FileEditToolError),
    #[error(transparent)]
    Edit(#[from] FileEditError),
    #[error(transparent)]
    CommitKernel(#[from] crate::resource_commit::PostgresFencedResourceToolError),
    #[error(transparent)]
    Artifact(#[from] crate::PostgresArtifactError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] crate::PostgresExecutionError),
    #[error("File Edit Tool numeric value is out of range")]
    Numeric,
    #[error("PostgreSQL fenced File Edit commit failed")]
    Sql(#[from] sqlx::Error),
    #[error("governed PostgreSQL File Edit Tool Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
    #[error("File Edit Tool Attempt lease is stale")]
    StaleFence,
    #[error("File Edit Tool idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("File Edit Tool durable state is corrupt")]
    Corrupt,
}

impl From<crate::commit_lane::CommitLaneError> for PostgresFencedFileEditToolError {
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

impl From<std::num::TryFromIntError> for PostgresFencedFileEditToolError {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::Numeric
    }
}

impl ClassifyFileEditToolCommitError for PostgresFencedFileEditToolError {
    fn failure(&self) -> FileEditToolCommitFailure {
        match self {
            Self::Liveness(error) => {
                use musteragent_runtime_contract::ErrorCode;
                match error.code {
                    ErrorCode::AttemptLeaseStale => FileEditToolCommitFailure::StaleFence,
                    ErrorCode::IdempotencyConflict | ErrorCode::ConversationSeqConflict => {
                        FileEditToolCommitFailure::Conflict
                    }
                    ErrorCode::InvalidCommand => FileEditToolCommitFailure::InvalidCommand,
                    ErrorCode::DependencyUnavailable => FileEditToolCommitFailure::Unavailable,
                    _ => FileEditToolCommitFailure::Corrupt,
                }
            }
            Self::Edit(FileEditError::WriterBusy)
            | Self::Contract(FileEditToolError::Edit(FileEditError::WriterBusy)) => {
                FileEditToolCommitFailure::WriterBusy
            }
            Self::StaleFence => FileEditToolCommitFailure::StaleFence,
            Self::Contract(
                FileEditToolError::IdentityMismatch | FileEditToolError::ResourceTool(_),
            )
            | Self::Edit(FileEditError::IdentityMismatch) => {
                FileEditToolCommitFailure::Authorization
            }
            Self::Contract(
                FileEditToolError::UnsupportedTool
                | FileEditToolError::InvalidInput
                | FileEditToolError::EditTargetNotFound
                | FileEditToolError::AmbiguousEditTarget
                | FileEditToolError::InvalidIdentifier
                | FileEditToolError::InvalidResult,
            )
            | Self::Edit(
                FileEditError::InvalidExpiry
                | FileEditError::InvalidOperation
                | FileEditError::UnsupportedFile
                | FileEditError::FileTooLarge
                | FileEditError::InvalidEncoding
                | FileEditError::SessionUnavailable
                | FileEditError::FileUnavailable,
            ) => FileEditToolCommitFailure::InvalidCommand,
            Self::Edit(FileEditError::SessionConflict | FileEditError::BaseVersionConflict)
            | Self::IdempotencyConflict => FileEditToolCommitFailure::Conflict,
            Self::Contract(FileEditToolError::ContentUnavailable)
            | Self::Edit(
                FileEditError::ContentUnavailable | FileEditError::DependencyUnavailable,
            )
            | Self::Ledger(_)
            | Self::Execution(_)
            | Self::Sql(_)
            | Self::Transaction(_)
            | Self::Audit(_) => FileEditToolCommitFailure::Unavailable,
            Self::Contract(FileEditToolError::Edit(error)) => match error {
                FileEditError::IdentityMismatch => FileEditToolCommitFailure::Authorization,
                FileEditError::SessionConflict | FileEditError::BaseVersionConflict => {
                    FileEditToolCommitFailure::Conflict
                }
                FileEditError::ContentUnavailable | FileEditError::DependencyUnavailable => {
                    FileEditToolCommitFailure::Unavailable
                }
                FileEditError::FileUnavailable => FileEditToolCommitFailure::InvalidCommand,
                FileEditError::Corrupt => FileEditToolCommitFailure::Corrupt,
                _ => FileEditToolCommitFailure::InvalidCommand,
            },
            Self::Contract(FileEditToolError::Capability(_))
            | Self::Contract(FileEditToolError::Read(_))
            | Self::CommitKernel(_)
            | Self::Artifact(_)
            | Self::Corrupt
            | Self::Edit(FileEditError::Corrupt) => FileEditToolCommitFailure::Corrupt,
            Self::Numeric => FileEditToolCommitFailure::InvalidCommand,
        }
    }
}
