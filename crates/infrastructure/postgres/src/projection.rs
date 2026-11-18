use crate::keyed_signal::{KeyedSignalHub, KeyedSignalSubscription};
use std::sync::Arc;
use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerCursor, LedgerRecordKind};
use musteragent_identity::{
    Actor, ConversationId, DottedKey, LedgerOffset, OutboxMessageId, TenantId, UtcTimestamp,
};
use musteragent_ledger::{CanonicalLedgerRecord, LedgerPayload, RecordPage};
use musteragent_projection::{
    CatalogChangeSubscription, CatalogProjectionRecord, CatalogProjectionRecordPage, ChangeFeed,
    ChangeSubscription, ConversationCatalogInvalidationFeed,
    ConversationCatalogInvalidationSubscription, ConversationCatalogRecordSource,
    ConversationCatalogSourceError, CursorRecoveryAuthoritySource, LedgerPayloadDecoder,
    LedgerRecordSource, OutboxLease, ProjectionDisposition, ProjectionError, ProjectionStore,
    ProjectionWork, SubscriptionError,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_telemetry::DbCommitKind;
use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool, Row};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

use crate::PostgresLedgerStore;
use crate::database_runtime::PostgresDatabaseRuntime;
use crate::ledger::decode_record_metadata;
use crate::numeric::{decode_u64, encode_u64};
use crate::supervisor_wake::{self, SupervisorLane, SupervisorWakes};
use crate::transaction_policy::{
    PostgresTransactionAccess, PostgresTransactionPolicy, begin_transaction, execute_transaction,
};

const LEDGER_CHANNEL: &str = "musteragent_runtime_ledger";
pub(crate) const CATALOG_INVALIDATION_CHANNEL: &str = "musteragent_conversation_catalog";

#[derive(Debug, Clone)]
pub struct PostgresProjectionStore {
    pool: PgPool,
    database: PostgresDatabaseRuntime,
}

impl PostgresProjectionStore {
    #[must_use]
    pub fn new(database: impl Into<PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    async fn claim_work(
        &self,
        owner: &str,
        topics: &[DottedKey],
        recover_unsupported_topics: &[DottedKey],
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<ProjectionWork>, ProjectionError> {
        if owner.is_empty()
            || owner.len() > 160
            || owner.chars().any(char::is_control)
            || topics.is_empty()
            || topics.len() > 100
            || recover_unsupported_topics.len() > topics.len()
            || recover_unsupported_topics.iter().any(|topic| {
                !topics.iter().any(|owned| owned == topic)
                    || recover_unsupported_topics
                        .iter()
                        .filter(|candidate| *candidate == topic)
                        .count()
                        > 1
            })
            || limit == 0
            || limit > 1_000
        {
            return Err(ProjectionError::InvalidLease);
        }
        let lease_millis =
            i64::try_from(lease_duration.as_millis()).map_err(|_| ProjectionError::InvalidLease)?;
        let claim_limit = i64::try_from(limit).map_err(|_| ProjectionError::InvalidLease)?;
        let claim_owner = owner.to_owned();
        let claim_topics = topics
            .iter()
            .map(|topic| topic.as_str().to_owned())
            .collect::<Vec<_>>();
        let recover_unsupported_topics = recover_unsupported_topics
            .iter()
            .map(|topic| topic.as_str().to_owned())
            .collect::<Vec<_>>();
        let (rows, now, expires_at) = execute_transaction(
            &self.database,
            PostgresTransactionPolicy::deterministic_idempotent(
                &self.database,
                DbCommitKind::Projection,
            ),
            |connection| {
                let claim_owner = claim_owner.clone();
                let claim_topics = claim_topics.clone();
                let recover_unsupported_topics = recover_unsupported_topics.clone();
                Box::pin(async move {
                    let now = database_now_connection(connection).await?;
                    let expires_at = now.checked_add_millis(lease_millis).ok_or_else(|| {
                        sqlx::Error::InvalidArgument("Projection lease horizon overflow".to_owned())
                    })?;
                    let rows = sqlx::query(
                        "WITH candidates AS (
                 SELECT message_id
                 FROM event_store.outbox
                 WHERE published_at_ms IS NULL AND erasure_job_id IS NULL
                   AND topic = ANY($2::varchar[])
                   AND (
                       quarantined_at_ms IS NULL
                       OR (
                           quarantined_at_ms IS NOT NULL
                           AND last_error_code = 'projection.unsupported_topic'
                           AND topic = ANY($3::varchar[])
                       )
                   )
                   AND available_at_ms <= $1
                   AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= $1)
                 ORDER BY available_at_ms, committed_at_ms, message_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT $4
             )
             UPDATE event_store.outbox AS item
             SET lease_owner = $5,
                 lease_expires_at_ms = $6,
                 lease_generation = item.lease_generation + 1,
                 delivery_attempts = item.delivery_attempts + 1,
                 quarantined_at_ms = NULL,
                 last_error_code = NULL,
                 last_error_ref = NULL
             FROM candidates
             WHERE item.message_id = candidates.message_id
             RETURNING item.message_id, item.tenant_id, item.conversation_id,
                       item.topic, item.payload, item.payload_hash,
                       item.committed_at_ms,
                       item.delivery_attempts,
                       item.lease_generation",
                    )
                    .bind(now.unix_millis())
                    .bind(&claim_topics)
                    .bind(&recover_unsupported_topics)
                    .bind(claim_limit)
                    .bind(&claim_owner)
                    .bind(expires_at.unix_millis())
                    .fetch_all(&mut *connection)
                    .await?;
                    Ok((rows, now, expires_at))
                })
            },
        )
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        rows.into_iter()
            .map(|row| decode_projection_work(row, owner, now, expires_at))
            .collect()
    }

    async fn complete_work(
        &self,
        work: &ProjectionWork,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        work.validate()?;
        let mut transaction = begin_transaction(
            &self.database,
            PostgresTransactionPolicy::single_attempt(
                &self.database,
                DbCommitKind::Projection,
                PostgresTransactionAccess::ReadWrite,
            ),
        )
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        let now = database_now(&mut transaction).await?;
        let result = sqlx::query(
            "UPDATE event_store.outbox
             SET published_at_ms = $7,
                 lease_owner = NULL,
                 lease_expires_at_ms = NULL,
                 last_error_code = NULL,
                 last_error_ref = NULL
             WHERE message_id = $1
               AND tenant_id = $2
               AND conversation_id = $3
               AND payload_hash = $4
               AND lease_owner = $5
               AND lease_generation = $6::bigint
               AND published_at_ms IS NULL AND erasure_job_id IS NULL
               AND quarantined_at_ms IS NULL
               AND lease_expires_at_ms > $7",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .bind(&work.lease.owner)
        .bind(
            encode_u64(work.lease.generation, "outbox.lease_generation")
                .map_err(|_| ProjectionError::InvalidLease)?,
        )
        .bind(now.unix_millis())
        .execute(&mut *transaction)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if result.rows_affected() == 1 {
            transaction
                .commit()
                .await
                .map_err(|_| ProjectionError::DependencyUnavailable)?;
            return Ok(ProjectionDisposition::Applied);
        }
        let duplicate = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM event_store.outbox
                 WHERE message_id = $1
                   AND tenant_id = $2
                   AND conversation_id = $3
                   AND payload_hash = $4
                   AND published_at_ms IS NOT NULL AND erasure_job_id IS NULL
             )",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        transaction
            .rollback()
            .await
            .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if duplicate {
            Ok(ProjectionDisposition::Duplicate)
        } else {
            Err(ProjectionError::InvalidLease)
        }
    }

    async fn renew_work(
        &self,
        work: &ProjectionWork,
        lease: &OutboxLease,
        lease_duration: Duration,
    ) -> Result<OutboxLease, ProjectionError> {
        work.validate()?;
        if lease.owner != work.lease.owner
            || lease.generation != work.lease.generation
            || lease_duration < Duration::from_secs(1)
            || lease_duration > Duration::from_secs(5 * 60)
        {
            return Err(ProjectionError::InvalidLease);
        }
        let lease_millis =
            i64::try_from(lease_duration.as_millis()).map_err(|_| ProjectionError::InvalidLease)?;
        let expires_at_ms = sqlx::query_scalar::<_, i64>(
            "WITH database_time AS (
                 SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
             )
             UPDATE event_store.outbox AS item
             SET lease_expires_at_ms = database_time.now_ms + $7
             FROM database_time
             WHERE item.message_id = $1
               AND item.tenant_id = $2
               AND item.conversation_id = $3
               AND item.payload_hash = $4
               AND item.lease_owner = $5
               AND item.lease_generation = $6::bigint
               AND item.published_at_ms IS NULL AND item.erasure_job_id IS NULL
               AND item.quarantined_at_ms IS NULL
               AND item.lease_expires_at_ms > database_time.now_ms
             RETURNING item.lease_expires_at_ms",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .bind(&lease.owner)
        .bind(
            encode_u64(lease.generation, "outbox.lease_generation")
                .map_err(|_| ProjectionError::InvalidLease)?,
        )
        .bind(lease_millis)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?
        .ok_or(ProjectionError::InvalidLease)?;
        Ok(OutboxLease {
            owner: lease.owner.clone(),
            generation: lease.generation,
            expires_at: UtcTimestamp::from_unix_millis(expires_at_ms),
        })
    }

    async fn retry_work(
        &self,
        work: &ProjectionWork,
        available_at: UtcTimestamp,
        error: ProjectionError,
    ) -> Result<(), ProjectionError> {
        work.validate()?;
        let error_code = match error {
            ProjectionError::CorruptPayload => "projection.corrupt_payload",
            ProjectionError::InvalidLease => "projection.invalid_lease",
            ProjectionError::DependencyUnavailable => "projection.dependency_unavailable",
            ProjectionError::UnsupportedTopic => "projection.unsupported_topic",
        };
        let result = sqlx::query(
            "UPDATE event_store.outbox
             SET available_at_ms = $7,
                 lease_owner = NULL,
                 lease_expires_at_ms = NULL,
                 last_error_code = $8
             WHERE message_id = $1
               AND tenant_id = $2
               AND conversation_id = $3
               AND payload_hash = $4
               AND lease_owner = $5
               AND lease_generation = $6::bigint
               AND published_at_ms IS NULL AND erasure_job_id IS NULL
               AND quarantined_at_ms IS NULL",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .bind(&work.lease.owner)
        .bind(
            encode_u64(work.lease.generation, "outbox.lease_generation")
                .map_err(|_| ProjectionError::InvalidLease)?,
        )
        .bind(available_at.unix_millis())
        .bind(error_code)
        .execute(&self.pool)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(ProjectionError::InvalidLease)
        }
    }

    async fn quarantine_work(
        &self,
        work: &ProjectionWork,
        error: ProjectionError,
    ) -> Result<ProjectionDisposition, ProjectionError> {
        let error_code = match error {
            ProjectionError::CorruptPayload => "projection.corrupt_payload",
            ProjectionError::UnsupportedTopic => "projection.unsupported_topic",
            ProjectionError::InvalidLease | ProjectionError::DependencyUnavailable => {
                return Err(ProjectionError::InvalidLease);
            }
        };
        let mut transaction = begin_transaction(
            &self.database,
            PostgresTransactionPolicy::single_attempt(
                &self.database,
                DbCommitKind::Projection,
                PostgresTransactionAccess::ReadWrite,
            ),
        )
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        let now = database_now(&mut transaction).await?;
        let result = sqlx::query(
            "UPDATE event_store.outbox
             SET quarantined_at_ms = $7,
                 lease_owner = NULL,
                 lease_expires_at_ms = NULL,
                 last_error_code = $8
             WHERE message_id = $1
               AND tenant_id = $2
               AND conversation_id = $3
               AND payload_hash = $4
               AND lease_owner = $5
               AND lease_generation = $6::bigint
               AND published_at_ms IS NULL AND erasure_job_id IS NULL
               AND quarantined_at_ms IS NULL",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .bind(&work.lease.owner)
        .bind(
            encode_u64(work.lease.generation, "outbox.lease_generation")
                .map_err(|_| ProjectionError::InvalidLease)?,
        )
        .bind(now.unix_millis())
        .bind(error_code)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if result.rows_affected() == 1 {
            transaction
                .commit()
                .await
                .map_err(|_| ProjectionError::DependencyUnavailable)?;
            return Ok(ProjectionDisposition::Applied);
        }
        let duplicate = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM event_store.outbox
                 WHERE message_id = $1
                   AND tenant_id = $2
                   AND conversation_id = $3
                   AND payload_hash = $4
                   AND quarantined_at_ms IS NOT NULL AND erasure_job_id IS NULL
             )",
        )
        .bind(work.message_id.into_uuid())
        .bind(work.tenant_id.into_uuid())
        .bind(work.conversation_id.into_uuid())
        .bind(work.payload_hash.into_bytes().to_vec())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ProjectionError::DependencyUnavailable)?;
        transaction
            .rollback()
            .await
            .map_err(|_| ProjectionError::DependencyUnavailable)?;
        if duplicate {
            Ok(ProjectionDisposition::Duplicate)
        } else {
            Err(ProjectionError::InvalidLease)
        }
    }
}

impl ProjectionStore for PostgresProjectionStore {
    fn claim(
        &self,
        owner: &str,
        topics: &[DottedKey],
        recover_unsupported_topics: &[DottedKey],
        limit: usize,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<Vec<ProjectionWork>, ProjectionError>> {
        let owner = owner.to_owned();
        let topics = topics.to_vec();
        let recover_unsupported_topics = recover_unsupported_topics.to_vec();
        Box::pin(async move {
            self.claim_work(
                &owner,
                &topics,
                &recover_unsupported_topics,
                limit,
                lease_duration,
            )
            .await
        })
    }

    fn complete(
        &self,
        work: &ProjectionWork,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        let work = work.clone();
        Box::pin(async move { self.complete_work(&work).await })
    }

    fn renew(
        &self,
        work: &ProjectionWork,
        lease: &OutboxLease,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<OutboxLease, ProjectionError>> {
        let work = work.clone();
        let lease = lease.clone();
        Box::pin(async move { self.renew_work(&work, &lease, lease_duration).await })
    }

    fn retry(
        &self,
        work: &ProjectionWork,
        available_at: UtcTimestamp,
        error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<(), ProjectionError>> {
        let work = work.clone();
        Box::pin(async move { self.retry_work(&work, available_at, error).await })
    }

    fn quarantine(
        &self,
        work: &ProjectionWork,
        error: ProjectionError,
    ) -> ApplicationFuture<'_, Result<ProjectionDisposition, ProjectionError>> {
        let work = work.clone();
        Box::pin(async move { self.quarantine_work(&work, error).await })
    }
}

pub struct PostgresLedgerRecordSource {
    pool: PgPool,
    ledger: PostgresLedgerStore,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    recovery_authority: Arc<dyn CursorRecoveryAuthoritySource>,
}

impl PostgresLedgerRecordSource {
    #[must_use]
    pub fn new(
        database: impl Into<PostgresDatabaseRuntime>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        recovery_authority: Arc<dyn CursorRecoveryAuthoritySource>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database),
            pool,
            decoder,
            recovery_authority,
        }
    }

    async fn list_records(
        &self,
        actor: Actor,
        cursor: musteragent_event_envelope::LedgerCursor,
        limit: usize,
        max_page_bytes: usize,
    ) -> Result<RecordPage, SubscriptionError> {
        let authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM conversation.participants p
                 JOIN conversation.conversations c ON c.tenant_id=p.tenant_id AND c.conversation_id=p.conversation_id AND c.purpose='standard'
                 WHERE p.tenant_id = $1
                   AND p.conversation_id = $2
                   AND p.principal_ref = $3
                   AND p.left_at_ms IS NULL
             )",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(cursor.conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        if !authorized {
            return Err(SubscriptionError::Unauthorized);
        }
        let page = match self
            .ledger
            .load_bounded(cursor, limit, max_page_bytes)
            .await
        {
            Ok(page) => page,
            Err(crate::PostgresLedgerError::Contract(
                musteragent_ledger::LedgerError::HomeEpochMismatch
                | musteragent_ledger::LedgerError::CursorAhead,
            )) => {
                return Err(self
                    .cursor_epoch_recovery(actor, cursor.conversation_id)
                    .await?);
            }
            Err(crate::PostgresLedgerError::CursorCompacted { .. }) => {
                return Err(self
                    .cursor_compaction_recovery(actor, cursor.conversation_id)
                    .await?);
            }
            Err(error) => return Err(map_ledger_error(error)),
        };
        let mut records = Vec::with_capacity(page.records.len());
        for record in page.records {
            let payload = self
                .decoder
                .decode(record.metadata.record_kind, &record.payload)?;
            let canonical = CanonicalLedgerRecord {
                metadata: record.metadata,
                payload,
            };
            canonical
                .validate()
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            records.push(canonical);
        }
        Ok(RecordPage {
            records,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
            projection_checkpoint: page.next_cursor,
        })
    }

    async fn cursor_epoch_recovery(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> Result<SubscriptionError, SubscriptionError> {
        let authority = self
            .recovery_authority
            .current_cursor_recovery_authority(actor, conversation_id)
            .await?;
        Ok(SubscriptionError::CursorEpochStale { authority })
    }

    async fn cursor_compaction_recovery(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> Result<SubscriptionError, SubscriptionError> {
        let authority = self
            .recovery_authority
            .current_cursor_recovery_authority(actor, conversation_id)
            .await?;
        Ok(SubscriptionError::CursorCompacted { authority })
    }
}

impl LedgerRecordSource for PostgresLedgerRecordSource {
    fn list(
        &self,
        actor: Actor,
        cursor: musteragent_event_envelope::LedgerCursor,
        limit: usize,
        max_page_bytes: usize,
    ) -> ApplicationFuture<'_, Result<RecordPage, SubscriptionError>> {
        Box::pin(async move {
            self.list_records(actor, cursor, limit, max_page_bytes)
                .await
        })
    }
}

impl ConversationCatalogRecordSource for PostgresLedgerRecordSource {
    fn list_catalog_projection_records(
        &self,
        actor: Actor,
        cursor: LedgerCursor,
        limit: usize,
    ) -> ApplicationFuture<'_, Result<CatalogProjectionRecordPage, SubscriptionError>> {
        Box::pin(async move {
            if limit == 0 || limit > musteragent_ledger::MAX_PAGE_SIZE {
                return Err(SubscriptionError::InvalidQuery);
            }
            let stream = sqlx::query(
                "SELECT stream.tenant_id, stream.home_epoch, stream.last_sequence,
                        stream.retained_from_sequence,
                        EXISTS (
                            SELECT 1 FROM conversation.participants AS participant
                            WHERE participant.tenant_id = stream.tenant_id
                              AND participant.conversation_id = stream.conversation_id
                              AND participant.principal_ref = $1
                              AND participant.left_at_ms IS NULL
                        ) AS authorized
                 FROM event_store.ledger_streams AS stream
                 JOIN conversation.conversations c ON c.tenant_id=stream.tenant_id AND c.conversation_id=stream.conversation_id AND c.purpose='standard'
                 WHERE stream.conversation_id = $2",
            )
            .bind(actor.principal_id.into_uuid())
            .bind(cursor.conversation_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?
            .ok_or(SubscriptionError::SourceUnavailable)?;
            let stream_tenant = TenantId::from_uuid(
                stream
                    .try_get("tenant_id")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            let stream_epoch = decode_u64(
                stream
                    .try_get("home_epoch")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
                "catalog_stream.home_epoch",
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            let head = decode_u64(
                stream
                    .try_get("last_sequence")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
                "catalog_stream.last_sequence",
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            let retained_from = decode_u64(
                stream
                    .try_get("retained_from_sequence")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
                "catalog_stream.retained_from_sequence",
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            let authorized = stream
                .try_get::<bool, _>("authorized")
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            if stream_tenant != actor.tenant_id || !authorized {
                return Err(SubscriptionError::Unauthorized);
            }
            if stream_epoch != cursor.home_epoch.get() || cursor.after_conversation_seq.get() > head
            {
                return Err(self
                    .cursor_epoch_recovery(actor, cursor.conversation_id)
                    .await?);
            }
            let after = encode_u64(
                cursor.after_conversation_seq.get(),
                "catalog_records.after_conversation_seq",
            )
            .map_err(|_| SubscriptionError::InvalidQuery)?;
            let epoch = encode_u64(cursor.home_epoch.get(), "catalog_records.home_epoch")
                .map_err(|_| SubscriptionError::InvalidQuery)?;
            let query_limit = i64::try_from(limit).map_err(|_| SubscriptionError::InvalidQuery)?;
            let rows = sqlx::query(
                "SELECT record.schema_version, record.record_id, record.record_kind,
                        record.tenant_id, record.conversation_id, record.conversation_seq,
                        record.activity_id, record.run_id, record.run_seq, record.attempt_id,
                        record.participant_id, record.home_epoch, record.producer,
                        record.occurred_at_ms, record.committed_at_ms, record.causation_id,
                        record.correlation_id, record.idempotency_key, record.payload_hash,
                        event.encoded_event AS domain_payload,
                        event.payload_hash AS domain_payload_hash
                 FROM event_store.ledger_records AS record
                 LEFT JOIN event_store.domain_events AS event
                   ON event.record_id = record.record_id
                 WHERE record.conversation_id = $1
                   AND record.home_epoch = $2
                   AND record.conversation_seq > $3
                 ORDER BY record.conversation_seq
                 LIMIT $4",
            )
            .bind(cursor.conversation_id.into_uuid())
            .bind(epoch)
            .bind(after)
            .bind(query_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;
            let mut expected = cursor
                .after_conversation_seq
                .get()
                .checked_add(1)
                .ok_or(SubscriptionError::CorruptRecord)?;
            let mut records = Vec::with_capacity(rows.len());
            for row in rows {
                let metadata = decode_record_metadata(&row).map_err(map_ledger_error)?;
                let sequence = metadata.conversation_seq.get();
                if metadata.tenant_id != actor.tenant_id
                    || metadata.conversation_id != cursor.conversation_id
                    || metadata.home_epoch != cursor.home_epoch
                    || sequence < expected
                    || (sequence > expected && sequence > retained_from)
                {
                    return Err(SubscriptionError::CorruptRecord);
                }
                expected = sequence
                    .checked_add(1)
                    .ok_or(SubscriptionError::CorruptRecord)?;
                let payload = row
                    .try_get::<Option<Vec<u8>>, _>("domain_payload")
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                let payload_hash = row
                    .try_get::<Option<Vec<u8>>, _>("domain_payload_hash")
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                let event = match metadata.record_kind {
                    LedgerRecordKind::DomainEvent => {
                        let payload = payload.ok_or(SubscriptionError::CorruptRecord)?;
                        let payload_hash = Sha256::from_bytes(
                            payload_hash.ok_or(SubscriptionError::CorruptRecord)?,
                        )
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                        if payload_hash != metadata.payload_hash
                            || Sha256::digest(&payload) != metadata.payload_hash
                        {
                            return Err(SubscriptionError::CorruptRecord);
                        }
                        match self
                            .decoder
                            .decode(LedgerRecordKind::DomainEvent, &payload)?
                        {
                            LedgerPayload::DomainEvent(event) => Some(event),
                            LedgerPayload::OutputFrame(_) => {
                                return Err(SubscriptionError::CorruptRecord);
                            }
                        }
                    }
                    LedgerRecordKind::OutputFrame => {
                        if payload.is_some() || payload_hash.is_some() {
                            return Err(SubscriptionError::CorruptRecord);
                        }
                        None
                    }
                };
                records.push(CatalogProjectionRecord { metadata, event });
            }
            let mut through = records
                .last()
                .map_or(cursor.after_conversation_seq, |record| {
                    LedgerOffset::from(record.metadata.conversation_seq)
                });
            if through.get() < head && records.len() < limit {
                if head >= retained_from {
                    return Err(SubscriptionError::CorruptRecord);
                }
                through = LedgerOffset::new(head);
            }
            Ok(CatalogProjectionRecordPage { records, through })
        })
    }
}

/// Shared one-connection LISTEN fan-out. PostgreSQL notifications and every
/// in-process signal remain acceleration hints only.
#[derive(Debug, Clone)]
pub struct PostgresLedgerChangeFeed {
    conversation_signals: KeyedSignalHub<ConversationId>,
    ledger_catalog_sender: broadcast::Sender<Option<ConversationId>>,
    supervisors: SupervisorWakes,
    catalog_signals: KeyedSignalHub<TenantId>,
}

impl PostgresLedgerChangeFeed {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (ledger_catalog_sender, _) = broadcast::channel(capacity);
        Self {
            conversation_signals: KeyedSignalHub::default(),
            ledger_catalog_sender,
            supervisors: SupervisorWakes::default(),
            catalog_signals: KeyedSignalHub::default(),
        }
    }

    pub fn supervisor_changes(&self, lane: SupervisorLane) -> tokio::sync::watch::Receiver<u64> {
        self.supervisors.subscribe(lane)
    }

    /// Establishes LISTEN before the public server is made reachable. NOTIFY
    /// remains only an acceleration hint; subscription polling closes any
    /// commit gap caused by reconnects or listener loss.
    pub async fn connect(&self, pool: &PgPool) -> Result<PgListener, SubscriptionError> {
        let mut listener = PgListener::connect_with(pool)
            .await
            .map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
        listener
            .listen(LEDGER_CHANNEL)
            .await
            .map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
        listener
            .listen_all([CATALOG_INVALIDATION_CHANNEL, supervisor_wake::CHANNEL])
            .await
            .map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
        Ok(listener)
    }

    pub async fn run(
        &self,
        mut listener: PgListener,
        cancellation: CancellationToken,
    ) -> Result<(), SubscriptionError> {
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                notification = listener.try_recv() => {
                    let Some(notification) = notification.map_err(|_| SubscriptionError::ChangeFeedUnavailable)? else {
                        self.conversation_signals.notify_all().map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
                        self.catalog_signals.notify_all().map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
                        let _ = self.ledger_catalog_sender.send(None);
                        self.supervisors.replay();
                        continue;
                    };
                    match notification.channel() {
                        LEDGER_CHANNEL => {
                            let conversation_id = notification
                                .payload()
                                .parse::<ConversationId>()
                                .map_err(|_| SubscriptionError::CorruptRecord)?;
                            self.conversation_signals
                                .notify(conversation_id)
                                .map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
                            self.supervisors.wake(SupervisorLane::Catalog);
                            let _ = self.ledger_catalog_sender.send(Some(conversation_id));
                        }
                        supervisor_wake::CHANNEL => {
                            if let Some(lane) = SupervisorLane::from_hint(notification.payload()) { self.supervisors.wake(lane); }
                        }
                        CATALOG_INVALIDATION_CHANNEL => {
                            let tenant_id = notification
                                .payload()
                                .parse::<TenantId>()
                                .map_err(|_| SubscriptionError::CorruptRecord)?;
                            self.catalog_signals
                                .notify(tenant_id)
                                .map_err(|_| SubscriptionError::ChangeFeedUnavailable)?;
                        }
                        _ => return Err(SubscriptionError::CorruptRecord),
                    }
                }
            }
        }
    }
}

impl ConversationCatalogInvalidationFeed for PostgresLedgerChangeFeed {
    fn subscribe(
        &self,
        tenant_id: TenantId,
    ) -> Result<Arc<dyn ConversationCatalogInvalidationSubscription>, ConversationCatalogSourceError>
    {
        self.catalog_signals
            .subscribe(tenant_id)
            .map(|subscription| {
                Arc::new(subscription) as Arc<dyn ConversationCatalogInvalidationSubscription>
            })
            .map_err(|_| ConversationCatalogSourceError::Unavailable)
    }
}

impl ConversationCatalogInvalidationSubscription for KeyedSignalSubscription<TenantId> {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), ConversationCatalogSourceError>> {
        Box::pin(async move {
            self.wait_for_change()
                .await
                .map_err(|_| ConversationCatalogSourceError::Unavailable)
        })
    }
}

impl ChangeFeed for PostgresLedgerChangeFeed {
    fn subscribe(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Arc<dyn ChangeSubscription>, SubscriptionError> {
        self.conversation_signals
            .subscribe(conversation_id)
            .map(|subscription| Arc::new(subscription) as Arc<dyn ChangeSubscription>)
            .map_err(|_| SubscriptionError::ChangeFeedUnavailable)
    }

    fn subscribe_catalog(&self) -> Result<Arc<dyn CatalogChangeSubscription>, SubscriptionError> {
        Ok(Arc::new(PostgresCatalogChangeSubscription {
            receiver: Mutex::new(self.ledger_catalog_sender.subscribe()),
        }))
    }
}

impl ChangeSubscription for KeyedSignalSubscription<ConversationId> {
    fn wait(&self) -> ApplicationFuture<'_, Result<(), SubscriptionError>> {
        Box::pin(async move {
            self.wait_for_change()
                .await
                .map_err(|_| SubscriptionError::ChangeFeedUnavailable)
        })
    }
}

struct PostgresCatalogChangeSubscription {
    receiver: Mutex<broadcast::Receiver<Option<ConversationId>>>,
}

impl CatalogChangeSubscription for PostgresCatalogChangeSubscription {
    fn wait(&self) -> ApplicationFuture<'_, Result<Option<ConversationId>, SubscriptionError>> {
        Box::pin(async move {
            let mut receiver = self.receiver.lock().await;
            match receiver.recv().await {
                Ok(value) => Ok(value),
                Err(broadcast::error::RecvError::Lagged(_)) => Ok(None),
                Err(broadcast::error::RecvError::Closed) => {
                    Err(SubscriptionError::ChangeFeedUnavailable)
                }
            }
        })
    }
}

fn decode_projection_work(
    row: sqlx::postgres::PgRow,
    owner: &str,
    claimed_at: UtcTimestamp,
    expires_at: UtcTimestamp,
) -> Result<ProjectionWork, ProjectionError> {
    let payload: Vec<u8> = row
        .try_get("payload")
        .map_err(|_| ProjectionError::CorruptPayload)?;
    let payload_hash = Sha256::from_bytes(
        row.try_get::<Vec<u8>, _>("payload_hash")
            .map_err(|_| ProjectionError::CorruptPayload)?,
    )
    .map_err(|_| ProjectionError::CorruptPayload)?;
    let work = ProjectionWork {
        message_id: OutboxMessageId::from_uuid(
            row.try_get("message_id")
                .map_err(|_| ProjectionError::CorruptPayload)?,
        )
        .map_err(|_| ProjectionError::CorruptPayload)?,
        tenant_id: TenantId::from_uuid(
            row.try_get("tenant_id")
                .map_err(|_| ProjectionError::CorruptPayload)?,
        )
        .map_err(|_| ProjectionError::CorruptPayload)?,
        conversation_id: ConversationId::from_uuid(
            row.try_get("conversation_id")
                .map_err(|_| ProjectionError::CorruptPayload)?,
        )
        .map_err(|_| ProjectionError::CorruptPayload)?,
        topic: DottedKey::new(
            row.try_get::<String, _>("topic")
                .map_err(|_| ProjectionError::CorruptPayload)?,
        )
        .map_err(|_| ProjectionError::CorruptPayload)?,
        payload,
        payload_hash,
        committed_at: UtcTimestamp::from_unix_millis(
            row.try_get("committed_at_ms")
                .map_err(|_| ProjectionError::CorruptPayload)?,
        ),
        delivery_attempts: row
            .try_get::<i32, _>("delivery_attempts")
            .map_err(|_| ProjectionError::CorruptPayload)?
            .try_into()
            .map_err(|_| ProjectionError::CorruptPayload)?,
        claimed_at,
        lease: OutboxLease {
            owner: owner.to_owned(),
            generation: decode_u64(
                row.try_get::<i64, _>("lease_generation")
                    .map_err(|_| ProjectionError::CorruptPayload)?,
                "outbox.lease_generation",
            )
            .map_err(|_| ProjectionError::CorruptPayload)?,
            expires_at,
        },
    };
    Ok(work)
}

async fn database_now(transaction: &mut PgConnection) -> Result<UtcTimestamp, ProjectionError> {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(transaction)
            .await
            .map_err(|_| ProjectionError::DependencyUnavailable)?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

async fn database_now_connection(
    connection: &mut PgConnection,
) -> Result<UtcTimestamp, sqlx::Error> {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(connection)
            .await?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

fn map_ledger_error(error: crate::PostgresLedgerError) -> SubscriptionError {
    match error {
        crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::ReadBudgetExceeded,
        ) => SubscriptionError::ReadBudgetExceeded,
        crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::InvalidReadBudget,
        ) => SubscriptionError::InvalidQuery,
        crate::PostgresLedgerError::CursorCompacted { .. }
        | crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::HomeEpochMismatch
            | musteragent_ledger::LedgerError::CursorAhead,
        ) => SubscriptionError::SourceUnavailable,
        crate::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::PayloadHashMismatch
            | musteragent_ledger::LedgerError::InvalidRecordIdentity,
        ) => SubscriptionError::CorruptRecord,
        _ => SubscriptionError::SourceUnavailable,
    }
}

#[cfg(test)]
mod keyed_signal_tests {
    use super::*;

    #[tokio::test]
    async fn signals_are_key_scoped_coalesced_and_reclaimed() {
        let hub = KeyedSignalHub::<TenantId>::default();
        let target_id = TenantId::generate();
        let unrelated_id = TenantId::generate();
        let target = hub.subscribe(target_id).expect("subscribe target");
        let target_peer = hub.subscribe(target_id).expect("subscribe target peer");
        let unrelated = hub.subscribe(unrelated_id).expect("subscribe unrelated");
        assert_eq!(hub.active_key_count(), 2);

        hub.notify(unrelated_id).expect("notify unrelated");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), target.wait_for_change())
                .await
                .is_err(),
            "an unrelated tenant signal must not wake the target"
        );
        tokio::time::timeout(Duration::from_secs(1), unrelated.wait_for_change())
            .await
            .expect("unrelated signal timeout")
            .expect("unrelated signal closed");

        hub.notify(target_id).expect("notify target");
        tokio::time::timeout(Duration::from_secs(1), target.wait_for_change())
            .await
            .expect("target signal timeout")
            .expect("target signal closed");
        tokio::time::timeout(Duration::from_secs(1), target_peer.wait_for_change())
            .await
            .expect("target peer signal timeout")
            .expect("target peer signal closed");

        drop(target);
        drop(target_peer);
        drop(unrelated);
        assert_eq!(hub.active_key_count(), 0);
    }
}
