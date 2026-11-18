use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ActivityId, ConversationId, HomeEpoch, IdempotencyKey, LedgerOffset, RunSeq, TenantId,
};
use musteragent_ledger::{AppendRequest, ProposedRecord};
use musteragent_run::AttemptContext;
use sqlx::Row;
use thiserror::Error;

use crate::numeric::{BigintRangeError, decode_u64, encode_u64};
use crate::{GovernedPostgresTransaction, PostgresDatabaseRuntime};

/// Facts re-read after acquiring the per-Conversation Ledger stream lock.
///
/// Every Runtime-owned writer must derive both its Ledger offset and Run
/// sequence from this state, and must keep the returned transaction alive
/// until its domain projection and Ledger append commit together.
#[derive(Debug, Copy, Clone)]
pub(crate) struct CommitLaneState {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub activity_id: ActivityId,
    pub home_epoch: HomeEpoch,
    pub last_run_sequence: RunSeq,
    pub conversation_offset: LedgerOffset,
}

impl CommitLaneState {
    pub fn next_sequence(self, increment: u64) -> Result<u64, CommitLaneError> {
        self.last_run_sequence
            .get()
            .checked_add(increment)
            .ok_or(CommitLaneError::Numeric)
    }

    pub fn append(
        self,
        idempotency: String,
        records: Vec<ProposedRecord>,
    ) -> Result<AppendRequest, CommitLaneError> {
        let idempotency_key =
            IdempotencyKey::new(idempotency).map_err(|_| CommitLaneError::Corrupt)?;
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

/// Acquires and validates the durable commit lane for the active Attempt.
///
/// The first query finds the Conversation and owns its Ledger stream row
/// lock. The second query deliberately runs after any lock wait so its Run,
/// Attempt, lease, and sequence values belong to the acquired generation.
pub(crate) async fn acquire(
    database: &PostgresDatabaseRuntime,
    attempt: &AttemptContext,
) -> Result<(GovernedPostgresTransaction<'static>, CommitLaneState), CommitLaneError> {
    let mut transaction = acquire_for_run(database, attempt.run_id)
        .await?
        .ok_or(CommitLaneError::StaleFence)?;
    let row = sqlx::query(
        "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                run.home_epoch, run.last_run_sequence,
                stream.last_sequence AS conversation_offset
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
         WHERE run.run_id = $1 AND run.status IN (2, 4)
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
    .bind(encode_u64(
        attempt.fencing_token.get(),
        "attempt.fencing_token",
    )?)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(CommitLaneError::StaleFence)?;
    let state = CommitLaneState {
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| CommitLaneError::Corrupt)?,
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
            .map_err(|_| CommitLaneError::Corrupt)?,
        activity_id: ActivityId::from_uuid(row.try_get("activity_id")?)
            .map_err(|_| CommitLaneError::Corrupt)?,
        home_epoch: HomeEpoch::new(decode_u64(row.try_get("home_epoch")?, "run.home_epoch")?)
            .map_err(|_| CommitLaneError::Corrupt)?,
        last_run_sequence: RunSeq::new(decode_u64(
            row.try_get("last_run_sequence")?,
            "run.last_run_sequence",
        )?)
        .map_err(|_| CommitLaneError::Corrupt)?,
        conversation_offset: LedgerOffset::new(decode_u64(
            row.try_get("conversation_offset")?,
            "ledger_stream.last_sequence",
        )?),
    };
    Ok((transaction, state))
}

/// Acquires only the stream row lock. Callers that need the standardized
/// Runtime state should use [`acquire`].
pub(crate) async fn acquire_for_run(
    database: &PostgresDatabaseRuntime,
    run_id: musteragent_identity::RunId,
) -> Result<Option<GovernedPostgresTransaction<'static>>, CommitLaneError> {
    let mut transaction = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::LedgerOutbox)
        .await?;
    let identity = sqlx::query(
        "SELECT tenant_id, conversation_id
         FROM runtime.runs
         WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(identity) = identity else {
        transaction.rollback().await?;
        return Ok(None);
    };
    let tenant_id: uuid::Uuid = identity.try_get("tenant_id")?;
    let conversation_id: uuid::Uuid = identity.try_get("conversation_id")?;
    let locked = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT conversation_id
         FROM event_store.ledger_streams
         WHERE tenant_id = $1 AND conversation_id = $2
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(conversation_id)
    .fetch_optional(&mut *transaction)
    .await?;
    if locked.is_none() {
        transaction.rollback().await?;
        return Ok(None);
    }
    Ok(Some(transaction))
}

#[derive(Debug, Error)]
pub(crate) enum CommitLaneError {
    #[error("Attempt lease or fencing token is stale")]
    StaleFence,
    #[error("commit lane state is corrupt")]
    Corrupt,
    #[error("commit lane sequence overflowed")]
    Numeric,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    NumericConversion(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    NumericRange(#[from] BigintRangeError),
}

/// Recheck database time, lease and cancellation after preparing an effect.
pub(crate) async fn validate_active_fence(
    tx: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    state: crate::commit_lane::CommitLaneState,
) -> Result<(), CommitLaneError> {
    let now = musteragent_identity::UtcTimestamp::from_unix_millis(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&mut *tx)
            .await?,
    );
    crate::execution::lock_and_validate_fence(
        tx,
        &crate::resource_commit::attempt_fence(attempt, state.home_epoch),
        state.last_run_sequence,
        now,
    )
    .await
    .map_err(|_| CommitLaneError::StaleFence)?;
    let cancelled: bool = sqlx::query_scalar(
        "SELECT status<>2 OR cancel_requested_at_ms IS NOT NULL FROM runtime.runs WHERE run_id=$1",
    )
    .bind(attempt.run_id.into_uuid())
    .fetch_one(tx)
    .await?;
    if cancelled {
        return Err(CommitLaneError::StaleFence);
    }
    Ok(())
}
