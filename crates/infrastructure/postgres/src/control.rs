pub(crate) mod consumed;

use musteragent_identity::{CommandId, MessageId};
use musteragent_ledger::{
    AppendDisposition, AppendReceipt, ConsumeRunControlsBatch, RunControlBatch,
};
use musteragent_run::RunControlKind;
use musteragent_runtime_content::AdmissionContentStage;
use sqlx::Row as _;
use thiserror::Error;

use crate::content::PostgresContentStore;
use crate::ledger::database_now_ms;
use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore};

impl PostgresLedgerStore {
    pub async fn enqueue_run_control(
        &self,
        batch: RunControlBatch,
    ) -> Result<AppendReceipt, PostgresRunControlError> {
        self.enqueue_run_control_inner(batch, None).await
    }

    pub async fn enqueue_run_control_content(
        &self,
        batch: RunControlBatch,
        content: &PostgresContentStore,
        stage: AdmissionContentStage,
    ) -> Result<AppendReceipt, PostgresRunControlError> {
        if batch.append.tenant_id != stage.tenant_id
            || batch.append.command_fingerprint != stage.command_fingerprint
            || batch.message.content != stage.content
        {
            return Err(PostgresRunControlError::ContentNotAuthorized);
        }
        self.enqueue_run_control_inner(batch, Some((content, stage)))
            .await
    }

    async fn enqueue_run_control_inner(
        &self,
        batch: RunControlBatch,
        admission_content: Option<(&PostgresContentStore, AdmissionContentStage)>,
    ) -> Result<AppendReceipt, PostgresRunControlError> {
        batch.validate()?;
        let prepared = match self.begin_append(&batch.append).await? {
            BeginAppend::Duplicate(receipt) => {
                if let Some((content, stage)) = admission_content {
                    content
                        .validate_published_admission_stage(&stage)
                        .await
                        .map_err(|_| PostgresRunControlError::ContentNotAuthorized)?;
                }
                return Ok(receipt);
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let message_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or(PostgresRunControlError::InvalidState)?;
        let mut staged = prepared.stage(&batch.append).await?;
        let row = sqlx::query(
            "SELECT run.status, run.last_run_sequence::text AS last_run_sequence,
                    EXISTS (
                        SELECT 1
                        FROM conversation.participants AS participant
                        WHERE participant.tenant_id = run.tenant_id
                          AND participant.conversation_id = run.conversation_id
                          AND participant.participant_id = $5
                          AND participant.kind = 1
                          AND participant.principal_ref = $6
                          AND participant.left_at_ms IS NULL
                    ) AS participant_authorized
             FROM runtime.runs AS run
             WHERE run.tenant_id = $1
               AND run.conversation_id = $2
               AND run.run_id = $3
               AND run.activity_id = $4
               AND run.home_epoch = $7::bigint
             FOR UPDATE",
        )
        .bind(batch.append.tenant_id.into_uuid())
        .bind(batch.append.conversation_id.into_uuid())
        .bind(batch.run_id.into_uuid())
        .bind(batch.activity_id.into_uuid())
        .bind(batch.message.participant_id.into_uuid())
        .bind(batch.actor.principal_id.into_uuid())
        .bind(batch.append.expected_home_epoch.get().to_string())
        .fetch_optional(&mut *staged.transaction_mut())
        .await?
        .ok_or(PostgresRunControlError::InvalidState)?;
        if !matches!(row.try_get::<i16, _>("status")?, 1..=3)
            || !row.try_get::<bool, _>("participant_authorized")?
            || row.try_get::<String, _>("last_run_sequence")?
                != batch.expected_last_run_sequence.get().to_string()
        {
            return Err(PostgresRunControlError::InvalidState);
        }
        if let Some((content, stage)) = admission_content {
            content
                .publish_admission_stage(staged.transaction_mut(), &stage)
                .await
                .map_err(|_| PostgresRunControlError::ContentNotAuthorized)?;
        }
        let content = &batch.message.content;
        let content_authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM artifact.content_refs
                 WHERE tenant_id = $1 AND content_id = $2
                   AND media_type = $3 AND byte_length = $4::bigint
                   AND content_hash = $5 AND state = 2
             )",
        )
        .bind(batch.append.tenant_id.into_uuid())
        .bind(content.content_id.into_uuid())
        .bind(content.media_type.as_str())
        .bind(content.byte_length.to_string())
        .bind(content.sha256.into_bytes().to_vec())
        .fetch_one(&mut *staged.transaction_mut())
        .await?;
        if !content_authorized {
            return Err(PostgresRunControlError::ContentNotAuthorized);
        }
        let now_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .fetch_one(&mut *staged.transaction_mut())
        .await?;
        sqlx::query(
            "INSERT INTO conversation.messages (
                 message_id, tenant_id, conversation_id, participant_id, run_id,
                 role, content_id, content_hash, content_bytes, lifecycle,
                 committed_sequence, committed_at_ms
             ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8::bigint, 1, $9::bigint, $10)",
        )
        .bind(batch.message.message_id.into_uuid())
        .bind(batch.append.tenant_id.into_uuid())
        .bind(batch.append.conversation_id.into_uuid())
        .bind(batch.message.participant_id.into_uuid())
        .bind(batch.run_id.into_uuid())
        .bind(content.content_id.into_uuid())
        .bind(content.sha256.into_bytes().to_vec())
        .bind(content.byte_length.to_string())
        .bind(message_sequence.get().to_string())
        .bind(now_ms)
        .execute(&mut *staged.transaction_mut())
        .await?;
        sqlx::query(
            "INSERT INTO runtime.loop_controls (
                 control_id, tenant_id, conversation_id, run_id, message_id,
                 source_record_id, source_conversation_seq, kind, status,
                 enqueued_at_ms, consumed_at_ms, consumed_attempt_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::bigint, $8, 1, $9, NULL, NULL)",
        )
        .bind(batch.control_id.into_uuid())
        .bind(batch.append.tenant_id.into_uuid())
        .bind(batch.append.conversation_id.into_uuid())
        .bind(batch.run_id.into_uuid())
        .bind(batch.message.message_id.into_uuid())
        .bind(batch.message_record_id.into_uuid())
        .bind(message_sequence.get().to_string())
        .bind(control_kind_code(batch.kind))
        .bind(now_ms)
        .execute(&mut *staged.transaction_mut())
        .await?;
        let next_run_sequence = batch
            .expected_last_run_sequence
            .get()
            .checked_add(2)
            .ok_or(PostgresRunControlError::InvalidState)?;
        let updated = sqlx::query(
            "UPDATE runtime.runs
             SET last_run_sequence = $2::bigint, run_version = run_version + 1
             WHERE run_id = $1",
        )
        .bind(batch.run_id.into_uuid())
        .bind(next_run_sequence.to_string())
        .execute(&mut *staged.transaction_mut())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresRunControlError::InvalidState);
        }
        let receipt = staged.commit().await?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(receipt)
    }

    pub async fn consume_run_controls(
        &self,
        batch: ConsumeRunControlsBatch,
    ) -> Result<(AppendReceipt, u32), PostgresRunControlError> {
        let transaction = self
            .database()
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await?;
        self.consume_run_controls_in_transaction(transaction, batch)
            .await
    }

    pub(crate) async fn consume_run_controls_in_transaction(
        &self,
        transaction: crate::GovernedPostgresTransaction<'static>,
        batch: ConsumeRunControlsBatch,
    ) -> Result<(AppendReceipt, u32), PostgresRunControlError> {
        batch
            .validate()
            .map_err(PostgresRunControlError::ConsumeContract)?;
        let consumed_count = u32::try_from(batch.control_ids.len())
            .map_err(|_| PostgresRunControlError::InvalidState)?;
        let prepared = match self
            .begin_append_in_transaction(transaction, &batch.fenced.append)
            .await?
        {
            BeginAppend::Duplicate(receipt) => return Ok((receipt, consumed_count)),
            BeginAppend::Ready(prepared) => prepared,
        };
        let mut staged = prepared.stage(&batch.fenced.append).await?;
        let now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &batch.fenced.fence,
            batch.fenced.expected_last_run_sequence,
            now,
        )
        .await?;
        let control_ids = batch
            .control_ids
            .iter()
            .map(|control_id| control_id.into_uuid())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT control_id, message_id
             FROM runtime.loop_controls
             WHERE tenant_id = $1
               AND conversation_id = $2
               AND run_id = $3
               AND kind = $4
               AND status = 1
               AND control_id = ANY($5)
             ORDER BY source_conversation_seq, control_id
             FOR UPDATE",
        )
        .bind(batch.fenced.append.tenant_id.into_uuid())
        .bind(batch.fenced.append.conversation_id.into_uuid())
        .bind(batch.fenced.fence.run_id.into_uuid())
        .bind(control_kind_code(batch.kind))
        .bind(&control_ids)
        .fetch_all(&mut *staged.transaction_mut())
        .await?;
        let persisted = rows
            .into_iter()
            .map(|row| {
                Ok((
                    CommandId::from_uuid(row.try_get("control_id")?)
                        .map_err(|_| PostgresRunControlError::InvalidState)?,
                    MessageId::from_uuid(row.try_get("message_id")?)
                        .map_err(|_| PostgresRunControlError::InvalidState)?,
                ))
            })
            .collect::<Result<Vec<_>, PostgresRunControlError>>()?;
        let expected = batch
            .control_ids
            .iter()
            .copied()
            .zip(batch.message_ids.iter().copied())
            .collect::<Vec<_>>();
        if persisted != expected {
            return Err(PostgresRunControlError::InvalidState);
        }
        let updated = sqlx::query(
            "UPDATE runtime.loop_controls
             SET status = 2,
                 consumed_at_ms = $2,
                 consumed_attempt_id = $3
             WHERE control_id = ANY($1) AND status = 1",
        )
        .bind(&control_ids)
        .bind(now.unix_millis())
        .bind(batch.fenced.fence.attempt_id.into_uuid())
        .execute(&mut *staged.transaction_mut())
        .await?;
        if updated.rows_affected() != u64::from(consumed_count) {
            return Err(PostgresRunControlError::InvalidState);
        }
        let next_run_sequence = batch
            .fenced
            .expected_last_run_sequence
            .get()
            .checked_add(1)
            .ok_or(PostgresRunControlError::InvalidState)?;
        let advanced = sqlx::query(
            "UPDATE runtime.runs
             SET last_run_sequence = $2::bigint, run_version = run_version + 1
             WHERE run_id = $1",
        )
        .bind(batch.fenced.fence.run_id.into_uuid())
        .bind(next_run_sequence.to_string())
        .execute(&mut *staged.transaction_mut())
        .await?;
        if advanced.rows_affected() != 1 {
            return Err(PostgresRunControlError::InvalidState);
        }
        Ok((staged.commit().await?, consumed_count))
    }
}

const fn control_kind_code(kind: RunControlKind) -> i16 {
    match kind {
        RunControlKind::Steering => 1,
        RunControlKind::FollowUp => 2,
    }
}

#[derive(Debug, Error)]
pub enum PostgresRunControlError {
    #[error(transparent)]
    EnqueueContract(#[from] musteragent_ledger::RunControlBatchError),
    #[error(transparent)]
    ConsumeContract(musteragent_ledger::ConsumeRunControlsBatchError),
    #[error(transparent)]
    Execution(#[from] crate::execution::PostgresExecutionError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Run Control Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("Run does not accept this control message")]
    InvalidState,
    #[error("Run control content is not tenant-authorized")]
    ContentNotAuthorized,
}
