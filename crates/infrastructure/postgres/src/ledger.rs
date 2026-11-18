use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerCursor, LedgerMetadata, LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, ConversationId, ConversationSeq, CorrelationId, DottedKey, HomeEpoch,
    IdempotencyKey, LedgerOffset, LedgerRecordId, OutboxMessageId, ParticipantId, RunId, RunSeq,
    TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendDisposition, AppendReceipt, AppendRequest, CommittedOutbox, CommittedRecord, LedgerError,
    LedgerPage, LedgerStore, MAX_PAGE_SIZE, ProposedOutbox,
};
use musteragent_telemetry::DbCommitKind;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::numeric::{decode_u64, encode_u64};
use crate::{GovernedPostgresTransaction, PostgresDatabaseRuntime};

#[derive(Debug, Clone)]
pub struct PostgresLedgerStore {
    pool: PgPool,
    database: PostgresDatabaseRuntime,
}

impl PostgresLedgerStore {
    #[must_use]
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub(crate) const fn database(&self) -> &PostgresDatabaseRuntime {
        &self.database
    }

    pub async fn append(
        &self,
        request: AppendRequest,
    ) -> Result<AppendReceipt, PostgresLedgerError> {
        match self.begin_append(&request).await? {
            BeginAppend::Duplicate(receipt) => Ok(receipt),
            BeginAppend::Ready(prepared) => prepared.commit(&request).await,
        }
    }

    pub(crate) async fn begin_append(
        &self,
        request: &AppendRequest,
    ) -> Result<BeginAppend, PostgresLedgerError> {
        request.validate().map_err(PostgresLedgerError::Contract)?;
        validate_bigint_request(request)?;
        let transaction = self
            .database
            .write_unit_of_work(DbCommitKind::LedgerOutbox)
            .await?;
        self.begin_append_in_transaction(transaction, request).await
    }

    /// Prepares an append inside a caller-owned transaction.
    ///
    /// Runtime-owned writers use this entry point after locking the
    /// Conversation stream and rebuilding their transition from the latest
    /// durable state. Product commands continue to use [`Self::begin_append`]
    /// with an exact caller-supplied offset.
    pub(crate) async fn begin_append_in_transaction(
        &self,
        transaction: GovernedPostgresTransaction<'static>,
        request: &AppendRequest,
    ) -> Result<BeginAppend, PostgresLedgerError> {
        request.validate().map_err(PostgresLedgerError::Contract)?;
        let mut transaction = transaction;
        let lock_identity = format!("{}:{}", request.tenant_id, request.idempotency_key.as_str());
        let locked = async {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(lock_identity)
                .execute(&mut *transaction)
                .await?;

            if let Some(receipt) = self
                .load_idempotency_receipt(&mut transaction, request)
                .await?
            {
                return Ok::<_, PostgresLedgerError>((Some(receipt), None));
            }

            let committed_at = database_now_ms(&mut transaction).await?;
            self.ensure_and_lock_stream(&mut transaction, request, committed_at)
                .await?;
            self.reject_existing_ids(&mut transaction, request).await?;
            Ok((None, Some(committed_at)))
        }
        .await;
        let (duplicate, committed_at) = locked?;
        if let Some(receipt) = duplicate {
            transaction.rollback().await?;
            return Ok(BeginAppend::Duplicate(receipt));
        }
        let committed_at = committed_at
            .ok_or_else(|| PostgresLedgerError::corrupt("ledger_transaction_committed_at"))?;

        let mut next_offset = request.expected_offset;
        let mut allocated = Vec::with_capacity(request.records.len());
        for _record in &request.records {
            let sequence = next_offset
                .next_sequence()
                .map_err(LedgerError::from)
                .map_err(PostgresLedgerError::Contract)?;
            next_offset = LedgerOffset::from(sequence);
            allocated.push(sequence);
        }

        Ok(BeginAppend::Ready(PreparedLedgerAppend {
            transaction,
            allocated,
            committed_at,
        }))
    }

    async fn load_idempotency_receipt(
        &self,
        transaction: &mut sqlx::PgConnection,
        request: &AppendRequest,
    ) -> Result<Option<AppendReceipt>, PostgresLedgerError> {
        let row = sqlx::query(
            "SELECT command_fingerprint,
                    first_sequence,
                    last_sequence,
                    committed_at_ms
             FROM event_store.command_idempotency
             WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.idempotency_key.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fingerprint: Vec<u8> = row.try_get("command_fingerprint")?;
        if Sha256::from_bytes(&fingerprint)
            .map_err(|_| PostgresLedgerError::corrupt("command_fingerprint"))?
            != request.command_fingerprint
        {
            return Err(PostgresLedgerError::Contract(
                LedgerError::IdempotencyConflict,
            ));
        }
        Ok(Some(AppendReceipt {
            disposition: AppendDisposition::Duplicate,
            first_sequence: conversation_seq(&row, "first_sequence")?,
            last_sequence: conversation_seq(&row, "last_sequence")?,
            committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
        }))
    }

    async fn ensure_and_lock_stream(
        &self,
        transaction: &mut sqlx::PgConnection,
        request: &AppendRequest,
        now: UtcTimestamp,
    ) -> Result<(), PostgresLedgerError> {
        sqlx::query(
            "INSERT INTO event_store.ledger_streams (
                 tenant_id, conversation_id, home_epoch, last_sequence,
                 retained_from_sequence, created_at_ms, updated_at_ms
             ) VALUES ($1, $2, $3, 0, 1, $4, $4)
             ON CONFLICT DO NOTHING",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.conversation_id.into_uuid())
        .bind(encode_u64(
            request.expected_home_epoch.get(),
            "ledger_streams.home_epoch",
        )?)
        .bind(now.unix_millis())
        .execute(&mut *transaction)
        .await?;

        let row = sqlx::query(
            "SELECT tenant_id,
                    home_epoch,
                    last_sequence
             FROM event_store.ledger_streams
             WHERE conversation_id = $1
             FOR UPDATE",
        )
        .bind(request.conversation_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if row.try_get::<Uuid, _>("tenant_id")? != request.tenant_id.into_uuid() {
            return Err(PostgresLedgerError::ConversationTenantMismatch);
        }
        let home_epoch = decode_u64(row.try_get("home_epoch")?, "home_epoch")?;
        if home_epoch != request.expected_home_epoch.get() {
            return Err(PostgresLedgerError::Contract(
                LedgerError::HomeEpochMismatch,
            ));
        }
        let current_offset =
            LedgerOffset::new(decode_u64(row.try_get("last_sequence")?, "last_sequence")?);
        if current_offset != request.expected_offset {
            return Err(PostgresLedgerError::Contract(
                LedgerError::SequenceConflict {
                    expected: request.expected_offset,
                    actual: current_offset,
                },
            ));
        }
        Ok(())
    }

    async fn reject_existing_ids(
        &self,
        transaction: &mut sqlx::PgConnection,
        request: &AppendRequest,
    ) -> Result<(), PostgresLedgerError> {
        let record_ids = request
            .records
            .iter()
            .map(|record| record.record_id.into_uuid())
            .collect::<Vec<_>>();
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM event_store.ledger_records
                 WHERE record_id = ANY($1)
             )",
        )
        .bind(record_ids)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(PostgresLedgerError::Contract(
                LedgerError::DuplicateRecordId,
            ));
        }
        let outbox_ids = request
            .outbox
            .iter()
            .map(|message| message.message_id.into_uuid())
            .collect::<Vec<_>>();
        if !outbox_ids.is_empty()
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                     SELECT 1 FROM event_store.outbox
                     WHERE message_id = ANY($1)
                 )",
            )
            .bind(outbox_ids)
            .fetch_one(&mut *transaction)
            .await?
        {
            return Err(PostgresLedgerError::Contract(
                LedgerError::DuplicateOutboxId,
            ));
        }
        Ok(())
    }

    pub async fn load(
        &self,
        cursor: LedgerCursor,
        limit: usize,
    ) -> Result<LedgerPage, PostgresLedgerError> {
        self.load_bounded(cursor, limit, usize::MAX).await
    }

    pub async fn load_bounded(
        &self,
        cursor: LedgerCursor,
        limit: usize,
        max_page_bytes: usize,
    ) -> Result<LedgerPage, PostgresLedgerError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(PostgresLedgerError::Contract(LedgerError::InvalidPageSize));
        }
        let budget = musteragent_ledger::LedgerReadBudget::new(max_page_bytes)
            .map_err(PostgresLedgerError::Contract)?;
        let policy = crate::transaction_policy::PostgresTransactionPolicy::single_attempt(
            &self.database,
            DbCommitKind::Projection,
            crate::transaction_policy::PostgresTransactionAccess::ReadOnly,
        )
        .with_isolation(crate::transaction_policy::PostgresTransactionIsolation::RepeatableRead);
        let mut read = crate::transaction_policy::begin_transaction(&self.database, policy).await?;
        let stream = sqlx::query(
            "SELECT home_epoch,
                    last_sequence,
                    retained_from_sequence
             FROM event_store.ledger_streams
             WHERE conversation_id = $1",
        )
        .bind(cursor.conversation_id.into_uuid())
        .fetch_optional(&mut *read)
        .await?
        .ok_or(PostgresLedgerError::Contract(
            LedgerError::ConversationNotFound,
        ))?;
        let home_epoch = decode_u64(stream.try_get("home_epoch")?, "home_epoch")?;
        if home_epoch != cursor.home_epoch.get() {
            return Err(PostgresLedgerError::Contract(
                LedgerError::HomeEpochMismatch,
            ));
        }
        let current_offset = decode_u64(stream.try_get("last_sequence")?, "last_sequence")?;
        if cursor.after_conversation_seq.get() > current_offset {
            return Err(PostgresLedgerError::Contract(LedgerError::CursorAhead));
        }
        let retained_from = decode_u64(
            stream.try_get("retained_from_sequence")?,
            "retained_from_sequence",
        )?;
        if cursor.after_conversation_seq.get().saturating_add(1) < retained_from {
            return Err(PostgresLedgerError::CursorCompacted {
                retained_from_sequence: retained_from,
            });
        }

        // Read lengths in the same database snapshot before materializing any
        // payload. The look-ahead row never crosses the persistence boundary.
        let sizes: Vec<i64> = sqlx::query_scalar(
            "SELECT octet_length(COALESCE(events.encoded_event, frames.encoded_frame))::bigint
             FROM event_store.ledger_records AS records
             LEFT JOIN event_store.domain_events AS events ON events.record_id = records.record_id
             LEFT JOIN event_store.output_frames AS frames ON frames.record_id = records.record_id
             WHERE records.conversation_id = $1 AND records.home_epoch = $2
               AND records.conversation_seq > $3
             ORDER BY records.conversation_seq LIMIT $4",
        )
        .bind(cursor.conversation_id.into_uuid())
        .bind(encode_u64(cursor.home_epoch.get(), "home_epoch")?)
        .bind(encode_u64(
            cursor.after_conversation_seq.get(),
            "conversation_seq",
        )?)
        .bind(i64::try_from(limit + 1).map_err(|_| PostgresLedgerError::corrupt("limit"))?)
        .fetch_all(&mut *read)
        .await?;
        let sizes = sizes
            .into_iter()
            .map(|size| {
                usize::try_from(size).map_err(|_| PostgresLedgerError::corrupt("payload_length"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let admitted = budget
            .admitted_prefix(sizes.iter().copied(), limit)
            .map_err(PostgresLedgerError::Contract)?;
        let has_more = sizes.len() > admitted;
        let rows = sqlx::query(
            "SELECT records.schema_version,
                    records.record_id,
                    records.record_kind,
                    records.tenant_id,
                    records.conversation_id,
                    records.conversation_seq,
                    records.activity_id,
                    records.run_id,
                    records.run_seq,
                    records.attempt_id,
                    records.participant_id,
                    records.home_epoch,
                    records.producer,
                    records.occurred_at_ms,
                    records.committed_at_ms,
                    records.causation_id,
                    records.correlation_id,
                    records.idempotency_key,
                    records.payload_hash,
                    COALESCE(events.encoded_event, frames.encoded_frame) AS payload
             FROM event_store.ledger_records AS records
             LEFT JOIN event_store.domain_events AS events
               ON events.record_id = records.record_id
             LEFT JOIN event_store.output_frames AS frames
               ON frames.record_id = records.record_id
             WHERE records.conversation_id = $1
               AND records.home_epoch = $2
               AND records.conversation_seq > $3
             ORDER BY records.conversation_seq
             LIMIT $4",
        )
        .bind(cursor.conversation_id.into_uuid())
        .bind(encode_u64(
            cursor.home_epoch.get(),
            "ledger_records.home_epoch",
        )?)
        .bind(encode_u64(
            cursor.after_conversation_seq.get(),
            "ledger_records.conversation_seq",
        )?)
        .bind(i64::try_from(admitted).map_err(|_| PostgresLedgerError::corrupt("limit"))?)
        .fetch_all(&mut *read)
        .await?;
        if rows.len() != admitted {
            return Err(PostgresLedgerError::corrupt("ledger_read_prefix"));
        }
        read.commit().await?;
        let records = rows
            .into_iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        let next_offset = records
            .last()
            .map_or(cursor.after_conversation_seq, |record| {
                LedgerOffset::from(record.metadata.conversation_seq)
            });
        Ok(LedgerPage {
            records,
            next_cursor: LedgerCursor {
                after_conversation_seq: next_offset,
                ..cursor
            },
            has_more,
        })
    }

    pub async fn pending_outbox(&self) -> Result<Vec<CommittedOutbox>, PostgresLedgerError> {
        let rows = sqlx::query(
            "SELECT message_id, tenant_id, conversation_id, topic, payload,
                    payload_hash, committed_at_ms
             FROM event_store.outbox
             WHERE published_at_ms IS NULL AND erasure_job_id IS NULL
             ORDER BY committed_at_ms, message_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(decode_outbox)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn current_offset(
        &self,
        conversation_id: ConversationId,
    ) -> Result<LedgerOffset, PostgresLedgerError> {
        let row = sqlx::query(
            "SELECT last_sequence
             FROM event_store.ledger_streams
             WHERE conversation_id = $1",
        )
        .bind(conversation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresLedgerError::Contract(
            LedgerError::ConversationNotFound,
        ))?;
        Ok(LedgerOffset::new(decode_u64(
            row.try_get("last_sequence")?,
            "last_sequence",
        )?))
    }
}

pub(crate) enum BeginAppend {
    Duplicate(AppendReceipt),
    Ready(PreparedLedgerAppend),
}

pub(crate) struct PreparedLedgerAppend {
    transaction: GovernedPostgresTransaction<'static>,
    allocated: Vec<ConversationSeq>,
    committed_at: UtcTimestamp,
}

impl PreparedLedgerAppend {
    pub(crate) fn transaction_mut(&mut self) -> &mut sqlx::PgConnection {
        &mut self.transaction
    }

    pub(crate) fn sequences(&self) -> &[ConversationSeq] {
        &self.allocated
    }

    pub(crate) async fn commit(
        self,
        request: &AppendRequest,
    ) -> Result<AppendReceipt, PostgresLedgerError> {
        self.stage(request).await?.commit().await
    }

    pub(crate) async fn stage(
        self,
        request: &AppendRequest,
    ) -> Result<StagedLedgerAppend, PostgresLedgerError> {
        self.stage_with_context_branch_from(request, None).await
    }

    /// Stages one atomic append whose suffix belongs to a branch activated
    /// inside the same append. The prefix inherits the durable active branch.
    pub(crate) async fn stage_with_context_branch_from(
        mut self,
        request: &AppendRequest,
        branch_suffix: Option<(usize, musteragent_identity::ContextBranchId)>,
    ) -> Result<StagedLedgerAppend, PostgresLedgerError> {
        if branch_suffix.is_some_and(|(index, _)| index == 0 || index >= request.records.len()) {
            return Err(PostgresLedgerError::Contract(
                LedgerError::InvalidRecordIdentity,
            ));
        }
        let first_sequence = self
            .allocated
            .first()
            .copied()
            .ok_or(PostgresLedgerError::Contract(LedgerError::EmptyAppend))?;
        let last_sequence = self
            .allocated
            .last()
            .copied()
            .ok_or(PostgresLedgerError::Contract(LedgerError::EmptyAppend))?;
        let stage_result = stage_append_rows(
            &mut self.transaction,
            request,
            &self.allocated,
            self.committed_at,
            branch_suffix,
            first_sequence,
            last_sequence,
        )
        .await;
        stage_result?;

        let receipt = AppendReceipt {
            disposition: AppendDisposition::Committed,
            first_sequence,
            last_sequence,
            committed_at: self.committed_at,
        };
        Ok(StagedLedgerAppend {
            transaction: self.transaction,
            receipt,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_append_rows(
    transaction: &mut sqlx::PgConnection,
    request: &AppendRequest,
    allocated: &[ConversationSeq],
    committed_at: UtcTimestamp,
    branch_suffix: Option<(usize, musteragent_identity::ContextBranchId)>,
    first_sequence: ConversationSeq,
    last_sequence: ConversationSeq,
) -> Result<(), PostgresLedgerError> {
    let tenant_id = request.tenant_id.into_uuid();
    let conversation_id = request.conversation_id.into_uuid();
    let mut query = QueryBuilder::<Postgres>::new(
        "WITH active_branch AS (
             SELECT (
                 SELECT active_branch_id
                 FROM conversation.context_branch_state
                 WHERE tenant_id = ",
    );
    query
        .push_bind(tenant_id)
        .push(" AND conversation_id = ")
        .push_bind(conversation_id)
        .push(
            ") AS active_branch_id
         ), record_rows (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash, payload,
             branch_override
         ) AS (",
        );
    query.push_values(
        request.records.iter().zip(allocated).enumerate(),
        |mut row, (index, (record, sequence))| {
            let branch_override = branch_suffix
                .filter(|(start, _)| index >= *start)
                .map(|(_, branch_id)| branch_id.into_uuid());
            row.push_bind(record.record_id.into_uuid())
                .push_bind(record.schema_version.as_str())
                .push_bind(record_kind_code(record.record_kind))
                .push_bind(tenant_id)
                .push_bind(conversation_id)
                .push_bind(
                    encode_u64(sequence.get(), "ledger_records.conversation_seq")
                        .expect("AppendRequest BIGINT bounds were validated before locking"),
                )
                .push_bind(record.activity_id.map(ActivityId::into_uuid))
                .push_bind(record.run_id.map(RunId::into_uuid))
                .push_bind(record.run_seq.map(|value| {
                    encode_u64(value.get(), "ledger_records.run_seq")
                        .expect("AppendRequest BIGINT bounds were validated before locking")
                }))
                .push_bind(record.attempt_id.map(AttemptId::into_uuid))
                .push_bind(record.participant_id.map(ParticipantId::into_uuid))
                .push_bind(
                    encode_u64(
                        request.expected_home_epoch.get(),
                        "ledger_records.home_epoch",
                    )
                    .expect("AppendRequest BIGINT bounds were validated before locking"),
                )
                .push_bind(producer_code(record.producer))
                .push_bind(record.occurred_at.unix_millis())
                .push_bind(committed_at.unix_millis())
                .push_bind(record.causation_id.map(LedgerRecordId::into_uuid))
                .push_bind(record.correlation_id.into_uuid())
                .push_bind(request.idempotency_key.as_str())
                .push_bind(record.payload_hash.into_bytes().to_vec())
                .push_bind(record.payload.as_slice())
                .push_bind(branch_override);
        },
    );
    query.push(
        "), inserted_records AS (
             INSERT INTO event_store.ledger_records (
                 record_id, schema_version, record_kind, tenant_id, conversation_id,
                 conversation_seq, activity_id, run_id, run_seq, attempt_id,
                 participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
                 causation_id, correlation_id, idempotency_key, payload_hash,
                 context_branch_id
             )
             SELECT record_id, schema_version, record_kind, tenant_id, conversation_id,
                    conversation_seq, activity_id, run_id, run_seq,
                    attempt_id, participant_id, home_epoch, producer,
                    occurred_at_ms, committed_at_ms, causation_id, correlation_id,
                    idempotency_key, payload_hash,
                    COALESCE(branch_override, active_branch.active_branch_id)
             FROM record_rows CROSS JOIN active_branch
             RETURNING record_id, record_kind
         ), inserted_domain_events AS (
             INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
             SELECT row.record_id, row.payload, row.payload_hash
             FROM record_rows AS row
             JOIN inserted_records AS inserted USING (record_id)
             WHERE inserted.record_kind = 1
             RETURNING record_id
         ), inserted_output_frames AS (
             INSERT INTO event_store.output_frames (record_id, encoded_frame, payload_hash)
             SELECT row.record_id, row.payload, row.payload_hash
             FROM record_rows AS row
             JOIN inserted_records AS inserted USING (record_id)
             WHERE inserted.record_kind = 2
             RETURNING record_id
         )",
    );
    if !request.outbox.is_empty() {
        query.push(
            ", outbox_rows (
                 message_id, tenant_id, conversation_id, topic, payload, payload_hash,
                 committed_at_ms
             ) AS (",
        );
        query.push_values(&request.outbox, |mut row, message| {
            row.push_bind(message.message_id.into_uuid())
                .push_bind(tenant_id)
                .push_bind(conversation_id)
                .push_bind(message.topic.as_str())
                .push_bind(message.payload.as_slice())
                .push_bind(message.payload_hash.into_bytes().to_vec())
                .push_bind(committed_at.unix_millis());
        });
        query.push(
            "), inserted_outbox AS (
                 INSERT INTO event_store.outbox (
                     message_id, tenant_id, conversation_id, topic, payload, payload_hash,
                     committed_at_ms, available_at_ms
                 )
                 SELECT message_id, tenant_id, conversation_id, topic, payload, payload_hash,
                        committed_at_ms, committed_at_ms
                 FROM outbox_rows
                 RETURNING message_id
             )",
        );
    }
    query
        .push(
            ", advanced_stream AS (
                 UPDATE event_store.ledger_streams
                 SET last_sequence = ",
        )
        .push_bind(
            encode_u64(last_sequence.get(), "ledger_streams.last_sequence")
                .expect("AppendRequest BIGINT bounds were validated before locking"),
        )
        .push(", updated_at_ms = ")
        .push_bind(committed_at.unix_millis())
        .push(" WHERE tenant_id = ")
        .push_bind(tenant_id)
        .push(" AND conversation_id = ")
        .push_bind(conversation_id)
        .push(
            " RETURNING tenant_id
             ), recorded_idempotency AS (
                 INSERT INTO event_store.command_idempotency (
                     tenant_id, idempotency_key, command_fingerprint,
                     conversation_id, home_epoch, first_sequence, last_sequence,
                     committed_at_ms
                 )
                 SELECT ",
        )
        .push_bind(tenant_id)
        .push(", ")
        .push_bind(request.idempotency_key.as_str())
        .push(", ")
        .push_bind(request.command_fingerprint.into_bytes().to_vec())
        .push(", ")
        .push_bind(conversation_id)
        .push(", ")
        .push_bind(
            encode_u64(
                request.expected_home_epoch.get(),
                "command_idempotency.home_epoch",
            )
            .expect("AppendRequest BIGINT bounds were validated before locking"),
        )
        .push(", ")
        .push_bind(
            encode_u64(first_sequence.get(), "command_idempotency.first_sequence")
                .expect("AppendRequest BIGINT bounds were validated before locking"),
        )
        .push(", ")
        .push_bind(
            encode_u64(last_sequence.get(), "command_idempotency.last_sequence")
                .expect("AppendRequest BIGINT bounds were validated before locking"),
        )
        .push(", ")
        .push_bind(committed_at.unix_millis())
        .push(
            " FROM advanced_stream
                 RETURNING tenant_id
             )
             SELECT
                 (SELECT count(*) FROM inserted_records)
               + (SELECT count(*) FROM inserted_domain_events)
               + (SELECT count(*) FROM inserted_output_frames)",
        );
    if request.outbox.is_empty() {
        query.push(" + 0");
    } else {
        query.push(" + (SELECT count(*) FROM inserted_outbox)");
    }
    query.push(" + (SELECT count(*) FROM recorded_idempotency)");

    query
        .build_query_scalar::<i64>()
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_stage_insert_error)?;
    for record in &request.records {
        crate::ledger_content::capture(
            transaction,
            record.record_id.into_uuid(),
            tenant_id,
            record.schema_version.as_str(),
            record.record_kind,
            &record.payload,
        )
        .await?;
    }
    Ok(())
}

pub(crate) struct StagedLedgerAppend {
    transaction: GovernedPostgresTransaction<'static>,
    receipt: AppendReceipt,
}

impl StagedLedgerAppend {
    /// Continue a larger atomic unit (for example child admission plus the
    /// parent's Tool receipt). The caller must commit or drop the transaction.
    pub(crate) fn into_parts(self) -> (GovernedPostgresTransaction<'static>, AppendReceipt) {
        (self.transaction, self.receipt)
    }
    pub(crate) fn transaction_mut(&mut self) -> &mut sqlx::PgConnection {
        &mut self.transaction
    }

    pub(crate) async fn commit(self) -> Result<AppendReceipt, PostgresLedgerError> {
        self.transaction.commit().await?;
        Ok(self.receipt)
    }
}

impl LedgerStore for PostgresLedgerStore {
    type Error = PostgresLedgerError;

    fn append(
        &self,
        request: AppendRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<AppendReceipt, Self::Error>>
    {
        Box::pin(async move { PostgresLedgerStore::append(self, request).await })
    }

    fn load(
        &self,
        cursor: LedgerCursor,
        limit: usize,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<LedgerPage, Self::Error>> {
        Box::pin(async move { PostgresLedgerStore::load(self, cursor, limit).await })
    }

    fn pending_outbox(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<Vec<CommittedOutbox>, Self::Error>,
    > {
        Box::pin(async move { PostgresLedgerStore::pending_outbox(self).await })
    }

    fn current_offset(
        &self,
        conversation_id: ConversationId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<LedgerOffset, Self::Error>>
    {
        Box::pin(async move { PostgresLedgerStore::current_offset(self, conversation_id).await })
    }
}

pub(crate) async fn database_now_ms(
    transaction: &mut sqlx::PgConnection,
) -> Result<UtcTimestamp, PostgresLedgerError> {
    let value: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&mut *transaction)
            .await?;
    Ok(UtcTimestamp::from_unix_millis(value))
}

fn decode_record(row: PgRow) -> Result<CommittedRecord, PostgresLedgerError> {
    let metadata = decode_record_metadata(&row)?;
    let payload: Vec<u8> = row.try_get("payload")?;
    if Sha256::digest(&payload) != metadata.payload_hash {
        return Err(PostgresLedgerError::corrupt("payload"));
    }
    Ok(CommittedRecord { metadata, payload })
}

pub(crate) fn decode_record_metadata(row: &PgRow) -> Result<LedgerMetadata, PostgresLedgerError> {
    let record_kind = decode_record_kind(row.try_get("record_kind")?)?;
    let producer = decode_producer(row.try_get("producer")?)?;
    let run_id = optional_id::<RunId>(row.try_get("run_id")?, "run_id")?;
    let run_seq = row
        .try_get::<Option<i64>, _>("run_seq")?
        .map(|value| {
            RunSeq::new(decode_u64(value, "run_seq")?)
                .map_err(|_| PostgresLedgerError::corrupt("run_seq"))
        })
        .transpose()?;
    let metadata = LedgerMetadata {
        schema_version: DottedKey::new(row.try_get::<String, _>("schema_version")?)
            .map_err(|_| PostgresLedgerError::corrupt("schema_version"))?,
        record_id: required_id(row.try_get("record_id")?, "record_id")?,
        record_kind,
        tenant_id: required_id(row.try_get("tenant_id")?, "tenant_id")?,
        conversation_id: required_id(row.try_get("conversation_id")?, "conversation_id")?,
        conversation_seq: conversation_seq(row, "conversation_seq")?,
        activity_id: optional_id(row.try_get("activity_id")?, "activity_id")?,
        run_id,
        run_seq,
        attempt_id: optional_id(row.try_get("attempt_id")?, "attempt_id")?,
        participant_id: optional_id(row.try_get("participant_id")?, "participant_id")?,
        home_epoch: HomeEpoch::new(decode_u64(row.try_get("home_epoch")?, "home_epoch")?)
            .map_err(|_| PostgresLedgerError::corrupt("home_epoch"))?,
        producer,
        occurred_at: UtcTimestamp::from_unix_millis(row.try_get("occurred_at_ms")?),
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
        causation_id: optional_id(row.try_get("causation_id")?, "causation_id")?,
        correlation_id: required_id(row.try_get("correlation_id")?, "correlation_id")?,
        idempotency_key: IdempotencyKey::new(row.try_get::<String, _>("idempotency_key")?)
            .map_err(|_| PostgresLedgerError::corrupt("idempotency_key"))?,
        payload_hash: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_hash")?)
            .map_err(|_| PostgresLedgerError::corrupt("payload_hash"))?,
    };
    metadata
        .validate()
        .map_err(|_| PostgresLedgerError::corrupt("record_identity"))?;
    Ok(metadata)
}

fn decode_outbox(row: PgRow) -> Result<CommittedOutbox, PostgresLedgerError> {
    let payload: Vec<u8> = row.try_get("payload")?;
    let payload_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("payload_hash")?)
        .map_err(|_| PostgresLedgerError::corrupt("outbox.payload_hash"))?;
    if Sha256::digest(&payload) != payload_hash {
        return Err(PostgresLedgerError::corrupt("outbox.payload"));
    }
    Ok(CommittedOutbox {
        tenant_id: required_id(row.try_get("tenant_id")?, "outbox.tenant_id")?,
        conversation_id: required_id(row.try_get("conversation_id")?, "outbox.conversation_id")?,
        message: ProposedOutbox {
            message_id: required_id(row.try_get("message_id")?, "outbox.message_id")?,
            topic: DottedKey::new(row.try_get::<String, _>("topic")?)
                .map_err(|_| PostgresLedgerError::corrupt("outbox.topic"))?,
            payload,
            payload_hash,
        },
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
    })
}

trait FromDatabaseUuid: Sized {
    fn from_database_uuid(value: Uuid) -> Result<Self, ()>;
}

macro_rules! database_id {
    ($($type:ty),+ $(,)?) => {
        $(
            impl FromDatabaseUuid for $type {
                fn from_database_uuid(value: Uuid) -> Result<Self, ()> {
                    Self::from_uuid(value).map_err(|_| ())
                }
            }
        )+
    };
}

database_id!(
    TenantId,
    ConversationId,
    ActivityId,
    RunId,
    AttemptId,
    ParticipantId,
    LedgerRecordId,
    CorrelationId,
    OutboxMessageId,
);

fn required_id<T: FromDatabaseUuid>(
    value: Uuid,
    field: &'static str,
) -> Result<T, PostgresLedgerError> {
    T::from_database_uuid(value).map_err(|_| PostgresLedgerError::corrupt(field))
}

fn optional_id<T: FromDatabaseUuid>(
    value: Option<Uuid>,
    field: &'static str,
) -> Result<Option<T>, PostgresLedgerError> {
    value.map(|id| required_id(id, field)).transpose()
}

fn validate_bigint_request(request: &AppendRequest) -> Result<(), PostgresLedgerError> {
    encode_u64(
        request.expected_home_epoch.get(),
        "ledger_streams.home_epoch",
    )?;
    encode_u64(
        request.expected_offset.get(),
        "ledger_streams.last_sequence",
    )?;
    let record_count =
        u64::try_from(request.records.len()).map_err(|_| crate::numeric::BigintRangeError {
            field: "ledger_records.count",
        })?;
    let last_sequence = request
        .expected_offset
        .get()
        .checked_add(record_count)
        .ok_or(crate::numeric::BigintRangeError {
            field: "ledger_records.conversation_seq",
        })?;
    encode_u64(last_sequence, "ledger_records.conversation_seq")?;
    for record in &request.records {
        if let Some(run_seq) = record.run_seq {
            encode_u64(run_seq.get(), "ledger_records.run_seq")?;
        }
    }
    Ok(())
}

fn conversation_seq(
    row: &PgRow,
    field: &'static str,
) -> Result<ConversationSeq, PostgresLedgerError> {
    ConversationSeq::new(decode_u64(row.try_get::<i64, _>(field)?, field)?)
        .map_err(|_| PostgresLedgerError::corrupt(field))
}

const fn record_kind_code(value: LedgerRecordKind) -> i16 {
    match value {
        LedgerRecordKind::DomainEvent => 1,
        LedgerRecordKind::OutputFrame => 2,
    }
}

fn decode_record_kind(value: i16) -> Result<LedgerRecordKind, PostgresLedgerError> {
    match value {
        1 => Ok(LedgerRecordKind::DomainEvent),
        2 => Ok(LedgerRecordKind::OutputFrame),
        _ => Err(PostgresLedgerError::corrupt("record_kind")),
    }
}

const fn producer_code(value: Producer) -> i16 {
    match value {
        Producer::RuntimeApi => 1,
        Producer::Worker => 2,
        Producer::Scheduler => 3,
        Producer::Projector => 4,
        Producer::Importer => 5,
    }
}

fn decode_producer(value: i16) -> Result<Producer, PostgresLedgerError> {
    match value {
        1 => Ok(Producer::RuntimeApi),
        2 => Ok(Producer::Worker),
        3 => Ok(Producer::Scheduler),
        4 => Ok(Producer::Projector),
        5 => Ok(Producer::Importer),
        _ => Err(PostgresLedgerError::corrupt("producer")),
    }
}

fn map_stage_insert_error(error: sqlx::Error) -> PostgresLedgerError {
    if constraint_name(&error) == Some("ledger_records_pkey") {
        PostgresLedgerError::Contract(LedgerError::DuplicateRecordId)
    } else if constraint_name(&error) == Some("outbox_pkey") {
        PostgresLedgerError::Contract(LedgerError::DuplicateOutboxId)
    } else {
        PostgresLedgerError::Database(error)
    }
}

fn constraint_name(error: &sqlx::Error) -> Option<&str> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::constraint)
}

#[derive(Debug, Error)]
pub enum PostgresLedgerError {
    #[error(transparent)]
    Contract(#[from] LedgerError),
    #[error("PostgreSQL persistence failed")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Ledger Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("Runtime field {field} is outside PostgreSQL BIGINT range")]
    NumericRange { field: &'static str },
    #[error("persisted Agent Runtime field is corrupt: {field}")]
    CorruptData { field: &'static str },
    #[error("Conversation identity already belongs to another tenant")]
    ConversationTenantMismatch,
    #[error("requested cursor was compacted; retained records start at {retained_from_sequence}")]
    CursorCompacted { retained_from_sequence: u64 },
}

impl PostgresLedgerError {
    const fn corrupt(field: &'static str) -> Self {
        Self::CorruptData { field }
    }
}

impl From<crate::numeric::BigintRangeError> for PostgresLedgerError {
    fn from(error: crate::numeric::BigintRangeError) -> Self {
        Self::NumericRange { field: error.field }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_producer, decode_record_kind, producer_code, record_kind_code};
    use musteragent_event_envelope::{LedgerRecordKind, Producer};

    #[test]
    fn persisted_closed_enums_round_trip_exhaustively() {
        for kind in [LedgerRecordKind::DomainEvent, LedgerRecordKind::OutputFrame] {
            assert_eq!(
                decode_record_kind(record_kind_code(kind)).expect("record kind round trip"),
                kind
            );
        }
        for producer in [
            Producer::RuntimeApi,
            Producer::Worker,
            Producer::Scheduler,
            Producer::Projector,
            Producer::Importer,
        ] {
            assert_eq!(
                decode_producer(producer_code(producer)).expect("producer round trip"),
                producer
            );
        }
    }
}
