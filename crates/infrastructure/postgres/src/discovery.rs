use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_discovery::{DiscoveryEvent, ToolExposureAdded, ToolExposureItem};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    CorrelationId, InvocationId, LedgerRecordId, RunSeq, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord,
};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_discovery::{
    CommitToolExposure, DiscoveryStoreError, ExposureCommitDisposition,
    FencedDiscoveryToolCommitter,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::ToolEvent;
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::artifact::{PostgresArtifactError, authorize_result_content, complete_tool};
use crate::commit_lane::{CommitLaneError, CommitLaneState};
use crate::execution::{PostgresExecutionError, lock_and_validate_fence};
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore, database_now_ms};

const RECORD_SCHEMA: &str = "musteragent.runtime.record.v1";

/// Atomic PostgreSQL writer for Run-scoped Tool exposure facts.
///
/// The canonical Ledger record, query projection, and Run sequence advance in
/// one fenced transaction. A caller retry with the same source invocation is
/// idempotent only when every immutable exposure field matches exactly.
#[derive(Clone)]
pub struct PostgresFencedDiscoveryToolCommitter {
    liveness_clock: Option<Arc<dyn musteragent_execution_liveness::LivenessHomeClock>>,
    pool: PgPool,
    ledger: PostgresLedgerStore,
    encoder: Arc<dyn LedgerPayloadEncoder>,
    reference_keys: Arc<OpaqueRefKeyring>,
    audit: Arc<crate::PostgresSecurityAuditPipeline>,
}

impl PostgresFencedDiscoveryToolCommitter {
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
        command: CommitToolExposure,
    ) -> Result<ExposureCommitDisposition, DiscoveryCommitError> {
        validate_command_shape(attempt, &command)?;
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
                    Ok(ExposureCommitDisposition::AlreadyApplied)
                } else {
                    Err(DiscoveryCommitError::IdempotencyConflict)
                };
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let persisted =
            load_run_exposures(prepared.transaction_mut(), state.tenant_id, attempt.run_id).await?;
        validate_next_exposure(&command.initial_exposures, &persisted, &command.event)?;
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
        validate_reason_content(prepared.transaction_mut(), state.tenant_id, &command.event)
            .await?;
        authorize_result_content(prepared.transaction_mut(), state.tenant_id, &command.result)
            .await?;
        insert_exposures(
            prepared.transaction_mut(),
            state.tenant_id,
            attempt.run_id,
            deterministic_record_id(command.invocation_id, 1)?,
            &command.event,
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
                u64::try_from(append.records.len()).map_err(|_| DiscoveryCommitError::Corrupt)?,
            )?,
        )
        .await?;
        prepared.commit(&append).await?;
        Ok(ExposureCommitDisposition::Applied)
    }

    fn append(
        &self,
        state: &CommitLaneState,
        attempt: &AttemptContext,
        command: &CommitToolExposure,
    ) -> Result<AppendRequest, DiscoveryCommitError> {
        let events = [
            DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                invocation_id: command.invocation_id,
                result_ref: command.result.result_ref.clone(),
                result_hash: command.result.result_hash,
                provider_request_ref: command.result.provider_request_ref.clone(),
                authorized_content: Vec::new(),
            }),
            DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(command.event.clone())),
        ];
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let payload = self.encoder.encode(LedgerPayload::DomainEvent(event));
                Ok(ProposedRecord {
                    schema_version: musteragent_identity::DottedKey::new(RECORD_SCHEMA)
                        .map_err(|_| DiscoveryCommitError::Corrupt)?,
                    record_id: deterministic_record_id(command.invocation_id, index)?,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(state.activity_id),
                    run_id: Some(attempt.run_id),
                    run_seq: Some(
                        RunSeq::new(state.next_sequence(u64::try_from(index)?.saturating_add(1))?)
                            .map_err(|_| DiscoveryCommitError::Corrupt)?,
                    ),
                    attempt_id: Some(attempt.attempt_id),
                    participant_id: Some(command.run_context.executor_participant_id),
                    producer: Producer::Worker,
                    occurred_at: command.committed_at,
                    causation_id: None,
                    correlation_id: CorrelationId::from_uuid(command.invocation_id.into_uuid())
                        .map_err(|_| DiscoveryCommitError::Corrupt)?,
                    payload_hash: Sha256::digest(&payload),
                    payload,
                })
            })
            .collect::<Result<Vec<_>, DiscoveryCommitError>>()?;
        Ok(state.append(format!("discovery-tool:{}", command.invocation_id), records)?)
    }

    async fn verify_duplicate(
        &self,
        tenant_id: TenantId,
        run_id: musteragent_identity::RunId,
        command: &CommitToolExposure,
    ) -> Result<bool, DiscoveryCommitError> {
        let mut connection = self.pool.acquire().await?;
        let persisted = load_run_exposures(&mut connection, tenant_id, run_id).await?;
        let result_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM tool.tool_invocations AS invocation
                 JOIN tool.tool_results AS result USING (invocation_id)
                 WHERE invocation.tenant_id = $1
                   AND invocation.run_id = $2
                   AND invocation.invocation_id = $3
                   AND invocation.status = 4
                   AND invocation.result_content_id = $4
                   AND result.result_hash = $5
                   AND result.result_schema_key = $6
                   AND result.result_schema_hash = $7
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
        .fetch_one(&mut *connection)
        .await?;
        Ok(result_matches && event_is_exact_retry(&persisted, &command.event))
    }
}

impl FencedDiscoveryToolCommitter for PostgresFencedDiscoveryToolCommitter {
    fn commit_tool_exposure<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        command: CommitToolExposure,
    ) -> ApplicationFuture<'a, Result<ExposureCommitDisposition, DiscoveryStoreError>> {
        Box::pin(async move {
            self.commit_inner(attempt, command).await.map_err(|error| {
                tracing::error!(error = ?error, "fenced discovery Tool commit failed");
                error.port()
            })
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PersistedExposure {
    item: ToolExposureItem,
    source_invocation_id: musteragent_identity::InvocationId,
    reason_content_id: musteragent_identity::ContentId,
    exposed_at: UtcTimestamp,
}

async fn load_run_exposures(
    connection: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
) -> Result<Vec<PersistedExposure>, DiscoveryCommitError> {
    let rows = sqlx::query(
        "SELECT tool_key, schema_hash, ordinal, source_invocation_id,
                reason_content_id, exposed_at_ms
         FROM runtime.tool_exposures
         WHERE tenant_id = $1 AND run_id = $2
         ORDER BY ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(connection)
    .await?;
    rows.into_iter()
        .map(|row| {
            let ordinal = u32::try_from(row.try_get::<i32, _>("ordinal")?)
                .ok()
                .and_then(std::num::NonZeroU32::new)
                .ok_or(DiscoveryCommitError::Corrupt)?;
            Ok(PersistedExposure {
                item: ToolExposureItem {
                    tool_ref: musteragent_tool::ToolKey::from_versioned_key(
                        musteragent_identity::VersionedKey::new(
                            row.try_get::<String, _>("tool_key")?,
                        )
                        .map_err(|_| DiscoveryCommitError::Corrupt)?,
                    ),
                    schema_hash: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("schema_hash")?)
                        .map_err(|_| DiscoveryCommitError::Corrupt)?,
                    ordinal,
                },
                source_invocation_id: musteragent_identity::InvocationId::from_uuid(
                    row.try_get("source_invocation_id")?,
                )
                .map_err(|_| DiscoveryCommitError::Corrupt)?,
                reason_content_id: musteragent_identity::ContentId::from_uuid(
                    row.try_get("reason_content_id")?,
                )
                .map_err(|_| DiscoveryCommitError::Corrupt)?,
                exposed_at: UtcTimestamp::from_unix_millis(row.try_get("exposed_at_ms")?),
            })
        })
        .collect()
}

fn validate_command_shape(
    attempt: &AttemptContext,
    command: &CommitToolExposure,
) -> Result<(), DiscoveryCommitError> {
    if command.run_context.run_id != attempt.run_id
        || command.invocation_id != command.event.source_invocation_id
        || command.event.exposures.is_empty()
        || command.event.exposed_at != command.committed_at
        || command.result.provider_request_ref.is_some()
        || command.result.validate().is_err()
    {
        return Err(DiscoveryCommitError::InvalidCommand);
    }
    let mut expected = 1_u32;
    for exposure in &command.initial_exposures {
        if exposure.ordinal.get() != expected {
            return Err(DiscoveryCommitError::InvalidCommand);
        }
        expected = expected
            .checked_add(1)
            .ok_or(DiscoveryCommitError::Corrupt)?;
    }
    Ok(())
}

fn validate_run_context(
    state: &CommitLaneState,
    context: &RunContext,
) -> Result<(), DiscoveryCommitError> {
    if state.tenant_id != context.tenant_id
        || state.conversation_id != context.conversation_id
        || state.activity_id != context.activity_id
        || state.home_epoch != context.home_epoch
    {
        return Err(DiscoveryCommitError::InvalidCommand);
    }
    Ok(())
}

fn validate_next_exposure(
    initial: &[ToolExposureItem],
    persisted: &[PersistedExposure],
    event: &ToolExposureAdded,
) -> Result<(), DiscoveryCommitError> {
    let first = initial
        .len()
        .saturating_add(persisted.len())
        .saturating_add(1);
    for (index, exposure) in event.exposures.iter().enumerate() {
        if usize::try_from(exposure.ordinal.get()).ok() != Some(first.saturating_add(index))
            || initial
                .iter()
                .any(|item| item.tool_ref == exposure.tool_ref)
            || persisted
                .iter()
                .any(|item| item.item.tool_ref == exposure.tool_ref)
            || event.exposures[..index]
                .iter()
                .any(|item| item.tool_ref == exposure.tool_ref)
        {
            return Err(DiscoveryCommitError::TransitionConflict);
        }
    }
    Ok(())
}

fn event_is_exact_retry(persisted: &[PersistedExposure], event: &ToolExposureAdded) -> bool {
    let matching = persisted
        .iter()
        .filter(|item| item.source_invocation_id == event.source_invocation_id)
        .collect::<Vec<_>>();
    matching.len() == event.exposures.len()
        && matching.iter().zip(&event.exposures).all(|(left, right)| {
            left.item == *right
                && left.reason_content_id == event.reason_ref.content_id
                && left.exposed_at == event.exposed_at
        })
}

async fn validate_reason_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    event: &ToolExposureAdded,
) -> Result<(), DiscoveryCommitError> {
    let valid: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM artifact.content_refs AS reason
             WHERE reason.tenant_id = $1 AND reason.content_id = $2
               AND reason.state = 2 AND reason.media_type = $3
               AND reason.byte_length = $4::bigint AND reason.content_hash = $5
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(event.reason_ref.content_id.into_uuid())
    .bind(event.reason_ref.media_type.as_str())
    .bind(event.reason_ref.byte_length.to_string())
    .bind(event.reason_ref.sha256.into_bytes().as_slice())
    .fetch_one(&mut *transaction)
    .await?;
    if valid {
        Ok(())
    } else {
        Err(DiscoveryCommitError::InvalidReference)
    }
}

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    attempt: &AttemptContext,
) -> Result<(), DiscoveryCommitError> {
    let locked: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT invocation_id
         FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2
           AND run_id = $3 AND status = 3 AND execution_attempt_id = $4
         FOR UPDATE",
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
        Err(DiscoveryCommitError::ToolTransitionConflict)
    }
}

async fn insert_exposures(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
    record_id: LedgerRecordId,
    event: &ToolExposureAdded,
) -> Result<(), DiscoveryCommitError> {
    for exposure in &event.exposures {
        sqlx::query(
            "INSERT INTO runtime.tool_exposures (
                 tenant_id, run_id, tool_key, schema_hash, ordinal,
                 source_invocation_id, reason_content_id, exposed_at_ms, record_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(exposure.tool_ref.as_str())
        .bind(exposure.schema_hash.into_bytes().to_vec())
        .bind(i32::try_from(exposure.ordinal.get())?)
        .bind(event.source_invocation_id.into_uuid())
        .bind(event.reason_ref.content_id.into_uuid())
        .bind(event.exposed_at.unix_millis())
        .bind(record_id.into_uuid())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

async fn advance_run_sequence(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: musteragent_identity::RunId,
    sequence: u64,
) -> Result<(), DiscoveryCommitError> {
    let changed = sqlx::query(
        "UPDATE runtime.runs
         SET last_run_sequence = $3::bigint, run_version = run_version + 1
         WHERE tenant_id = $1 AND run_id = $2 AND status = 2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(sequence.to_string())
    .execute(&mut *transaction)
    .await?;
    if changed.rows_affected() == 1 {
        Ok(())
    } else {
        Err(DiscoveryCommitError::TransitionConflict)
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
    invocation_id: musteragent_identity::InvocationId,
    index: usize,
) -> Result<LedgerRecordId, DiscoveryCommitError> {
    let mut seed = b"musteragent.discovery-tool.commit.v1\0".to_vec();
    seed.extend_from_slice(invocation_id.into_uuid().as_bytes());
    seed.extend_from_slice(&u64::try_from(index)?.to_be_bytes());
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    LedgerRecordId::from_uuid(uuid::Uuid::from_bytes(value))
        .map_err(|_| DiscoveryCommitError::Corrupt)
}

#[derive(Debug, Error)]
enum DiscoveryCommitError {
    #[error(transparent)]
    Liveness(#[from] musteragent_execution_liveness::LivenessJournalError),
    #[error("discovery command is invalid")]
    InvalidCommand,
    #[error("discovery transition conflicts with durable state")]
    TransitionConflict,
    #[error("discovery idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("discovery source invocation or reason Content is invalid")]
    InvalidReference,
    #[error("discovery Tool is not Running under the active Attempt")]
    ToolTransitionConflict,
    #[error("discovery durable state is corrupt")]
    Corrupt,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Execution(#[from] PostgresExecutionError),
    #[error(transparent)]
    Artifact(#[from] PostgresArtifactError),
    #[error(transparent)]
    CommitLane(#[from] CommitLaneError),
    #[error(transparent)]
    Numeric(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Audit(#[from] crate::tool_security_audit::AtomicToolSecurityAuditError),
}

impl DiscoveryCommitError {
    fn port(self) -> DiscoveryStoreError {
        match self {
            Self::Liveness(error) => DiscoveryStoreError {
                code: error.code,
                retryable: error.retryable,
            },
            Self::CommitLane(CommitLaneError::StaleFence)
            | Self::Execution(PostgresExecutionError::StaleFence) => DiscoveryStoreError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::TransitionConflict => DiscoveryStoreError {
                code: ErrorCode::ConversationSeqConflict,
                retryable: true,
            },
            Self::IdempotencyConflict => DiscoveryStoreError {
                code: ErrorCode::IdempotencyConflict,
                retryable: false,
            },
            Self::InvalidCommand | Self::InvalidReference => DiscoveryStoreError {
                code: ErrorCode::InvalidCommand,
                retryable: false,
            },
            Self::Database(_) | Self::Ledger(PostgresLedgerError::Database(_)) | Self::Audit(_) => {
                DiscoveryStoreError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                }
            }
            Self::Ledger(PostgresLedgerError::Contract(_)) => DiscoveryStoreError {
                code: ErrorCode::ConversationSeqConflict,
                retryable: true,
            },
            Self::Ledger(_) => DiscoveryStoreError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
            Self::ToolTransitionConflict => DiscoveryStoreError {
                code: ErrorCode::RunTransitionInvalid,
                retryable: false,
            },
            Self::Artifact(PostgresArtifactError::Sql(_)) => DiscoveryStoreError {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
            },
            Self::Artifact(_) => DiscoveryStoreError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
            Self::Corrupt | Self::Numeric(_) | Self::CommitLane(_) | Self::Execution(_) => {
                DiscoveryStoreError {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                }
            }
        }
    }
}
