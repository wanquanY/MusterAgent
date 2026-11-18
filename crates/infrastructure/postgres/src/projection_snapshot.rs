mod failure_backfill;

use std::collections::BTreeMap;
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{
    ConversationId, LedgerOffset, MessageId, OutputId, TenantId, UtcTimestamp,
};
use musteragent_ledger::CanonicalLedgerRecord;
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshot,
    ConversationProjectionSnapshotCodec, ConversationProjectionWindow,
    CursorRecoveryAuthoritySource, GetConversationProjection, GetConversationProjectionWindow,
    LedgerPayloadDecoder, ProductProjectionReducer, ProjectionContentStore, SubscriptionError,
    SubscriptionErrorMapper, projection_window_from_snapshot,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{ApplicationFuture, CursorRecoveryAuthority, RuntimeError};
use sqlx::{PgPool, Row};

use crate::{
    PostgresLedgerStore,
    projection_branch::ProjectionBranchRoute,
    projection_snapshot_authority::{self, CapturedProjectionAuthority, StreamHead},
};

// Reducer v5 projects one product-visible Assistant Message per Run and applies
// the canonical full-run retraction scope to every derived projection item.
// Keeping a distinct storage kind prevents snapshots produced by older reducer
// semantics from surviving a restart as apparently valid product state.
const BUILD_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ProjectionReadMode {
    Reusable,
    Head,
    BoundedHead,
}

/// PostgreSQL-backed Snapshot + bounded Tail application.
///
/// Ledger remains append-only and authoritative. This projection is an
/// immutable, rebuildable bootstrap accelerator and never participates in
/// command admission or execution recovery.
pub struct PostgresConversationProjection {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    ledger: PostgresLedgerStore,
    decoder: Arc<dyn LedgerPayloadDecoder>,
    codec: Arc<dyn ConversationProjectionSnapshotCodec>,
    content: Arc<dyn ProjectionContentStore>,
    errors: Arc<dyn SubscriptionErrorMapper>,
}

impl PostgresConversationProjection {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
        codec: Arc<dyn ConversationProjectionSnapshotCodec>,
        content: Arc<dyn ContentStore>,
        errors: Arc<dyn SubscriptionErrorMapper>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            ledger: PostgresLedgerStore::new(database.clone()),
            pool,
            database,
            decoder,
            codec,
            content: Arc::new(RuntimeProjectionContentStore { inner: content }),
            errors,
        }
    }

    async fn get(
        &self,
        query: GetConversationProjection,
        mode: ProjectionReadMode,
    ) -> Result<ConversationProjectionSnapshot, SubscriptionError> {
        let CapturedProjectionAuthority {
            stream,
            branch_route,
            snapshot_kind,
            selected_snapshot,
            selected_snapshot_is_legacy,
        } = projection_snapshot_authority::capture(&self.database, self.codec.as_ref(), query)
            .await?;
        if let Some(snapshot_id) = query.preferred_snapshot_id {
            let snapshot = selected_snapshot.ok_or(SubscriptionError::CorruptRecord)?;
            if snapshot.snapshot_id != snapshot_id {
                return Err(SubscriptionError::CorruptRecord);
            }
            self.validate_snapshot_tail(&snapshot, &stream, &branch_route)?;
            return Ok(snapshot);
        }
        // A supported historical Snapshot is a valid recovery base after Ledger compaction, but
        // an intact Ledger can recover the exact original reasoning sequence.
        // Prefer that deterministic rebuild when the public history is intact.
        let mut latest = if selected_snapshot_is_legacy && stream.retained_from == 1 {
            None
        } else {
            selected_snapshot
        };

        if let Some(snapshot) = latest.as_ref() {
            let tail = self.validate_snapshot_position(snapshot, &stream)?;
            if mode == ProjectionReadMode::BoundedHead
                && tail > musteragent_projection::MAX_PUBLIC_PROJECTION_TAIL_RECORDS
            {
                return Err(SubscriptionError::CorruptRecord);
            }
            // An unbranched snapshot may be followed by the public bounded
            // tail. Once an Entry Tree exists, the raw tail may contain facts
            // from sibling branches, so the server must first reduce that tail
            // through the active-lineage filter and return a head snapshot.
            if !selected_snapshot_is_legacy
                && (tail == 0
                    || (mode == ProjectionReadMode::Reusable
                        && branch_route.is_unbranched()
                        && tail <= musteragent_projection::MAX_PUBLIC_PROJECTION_TAIL_RECORDS))
            {
                return Ok(snapshot.clone());
            }
        }

        if latest.is_none() && stream.retained_from > 1 {
            return Err(SubscriptionError::CorruptRecord);
        }

        if selected_snapshot_is_legacy && let Some(snapshot) = latest.as_mut() {
            musteragent_projection::backfill_projection_run_failures(
                snapshot,
                &failure_backfill::RetainedRunEvents(&self.pool),
                self.decoder.as_ref(),
                |branch, sequence| branch_route.includes(branch, sequence),
            )
            .await?;
        }

        let output_messages = self
            .load_output_message_bindings(query.actor.tenant_id, query.conversation_id)
            .await?;
        let mut reducer = ProductProjectionReducer::new(
            query.actor.tenant_id,
            query.conversation_id,
            stream.home_epoch,
            latest,
            output_messages,
        )?;
        self.reduce_tail(&mut reducer, stream.head, &branch_route)
            .await?;
        let now = database_now(&self.pool).await?;
        let mut snapshot = reducer
            .finish(Arc::clone(&self.content), stream.head, now)
            .await?;
        crate::PostgresArtifactStore::recover_resource_file_projection(&self.pool, &mut snapshot)
            .await
            .map_err(|error| match error {
                crate::PostgresArtifactError::Sql(_)
                | crate::PostgresArtifactError::Transaction(_) => {
                    SubscriptionError::SourceUnavailable
                }
                _ => SubscriptionError::CorruptRecord,
            })?;
        snapshot.checksum = self.codec.checksum(&snapshot);
        let encoded = self
            .codec
            .encode(&snapshot)
            .map_err(|_| SubscriptionError::CorruptRecord)?;
        self.persist_snapshot(&snapshot, encoded, &snapshot_kind)
            .await
    }

    fn validate_snapshot_tail(
        &self,
        snapshot: &ConversationProjectionSnapshot,
        stream: &StreamHead,
        branch_route: &ProjectionBranchRoute,
    ) -> Result<(), SubscriptionError> {
        projection_snapshot_authority::validate_public_snapshot_tail(
            snapshot,
            stream,
            branch_route,
        )?;
        Ok(())
    }

    fn validate_snapshot_position(
        &self,
        snapshot: &ConversationProjectionSnapshot,
        stream: &StreamHead,
    ) -> Result<u64, SubscriptionError> {
        projection_snapshot_authority::validate_snapshot_position(snapshot, stream)
    }

    async fn load_output_message_bindings(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
    ) -> Result<BTreeMap<OutputId, MessageId>, SubscriptionError> {
        let rows = sqlx::query(
            "SELECT invocation.assistant_message_id,
                    invocation.assistant_output_id,
                    invocation.reasoning_output_id
             FROM runtime.model_invocations AS invocation
             JOIN runtime.runs AS run
               ON run.tenant_id = invocation.tenant_id
              AND run.run_id = invocation.run_id
             WHERE invocation.tenant_id = $1
               AND run.conversation_id = $2
               AND invocation.assistant_message_id IS NOT NULL",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        let mut bindings = BTreeMap::new();
        for row in rows {
            let message_id = MessageId::from_uuid(
                row.try_get("assistant_message_id")
                    .map_err(|_| SubscriptionError::CorruptRecord)?,
            )
            .map_err(|_| SubscriptionError::CorruptRecord)?;
            for column in ["assistant_output_id", "reasoning_output_id"] {
                if let Some(value) = row
                    .try_get::<Option<uuid::Uuid>, _>(column)
                    .map_err(|_| SubscriptionError::CorruptRecord)?
                {
                    let output_id =
                        OutputId::from_uuid(value).map_err(|_| SubscriptionError::CorruptRecord)?;
                    if bindings.insert(output_id, message_id).is_some() {
                        return Err(SubscriptionError::CorruptRecord);
                    }
                }
            }
        }
        Ok(bindings)
    }

    async fn reduce_tail(
        &self,
        reducer: &mut ProductProjectionReducer,
        head: LedgerOffset,
        branch_route: &ProjectionBranchRoute,
    ) -> Result<(), SubscriptionError> {
        let mut cursor = LedgerCursor {
            conversation_id: reducer.conversation_id(),
            home_epoch: reducer.home_epoch(),
            after_conversation_seq: reducer.through(),
        };
        while cursor.after_conversation_seq < head {
            let page = self
                .ledger
                .load(cursor, BUILD_PAGE_SIZE)
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)?;
            if page.records.is_empty() {
                return Err(SubscriptionError::CorruptRecord);
            }
            let page_through = page
                .records
                .last()
                .map(|record| LedgerOffset::from(record.metadata.conversation_seq))
                .ok_or(SubscriptionError::CorruptRecord)?;
            let record_branches = branch_route
                .record_branches(
                    &self.pool,
                    reducer.tenant_id(),
                    reducer.conversation_id(),
                    cursor.after_conversation_seq,
                    page_through,
                )
                .await?;
            let mut advanced = false;
            for record in page.records {
                let offset = LedgerOffset::from(record.metadata.conversation_seq);
                if offset > head {
                    break;
                }
                let record_branch = if branch_route.is_unbranched() {
                    None
                } else {
                    *record_branches
                        .get(&record.metadata.record_id)
                        .ok_or(SubscriptionError::CorruptRecord)?
                };
                if branch_route.includes(record_branch, record.metadata.conversation_seq) {
                    let payload = self
                        .decoder
                        .decode(record.metadata.record_kind, &record.payload)?;
                    let record = CanonicalLedgerRecord {
                        metadata: record.metadata,
                        payload,
                    };
                    record
                        .validate()
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    reducer.apply(record, Arc::clone(&self.content)).await?;
                }
                cursor.after_conversation_seq = offset;
                reducer.advance_through(offset)?;
                advanced = true;
            }
            if !advanced {
                return Err(SubscriptionError::CorruptRecord);
            }
        }
        Ok(())
    }

    async fn persist_snapshot(
        &self,
        snapshot: &ConversationProjectionSnapshot,
        encoded: Vec<u8>,
        snapshot_kind: &str,
    ) -> Result<ConversationProjectionSnapshot, SubscriptionError> {
        let encoded_hash = Sha256::digest(&encoded);
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;
        let inserted = sqlx::query(
            "INSERT INTO event_store.snapshots (
                 snapshot_id, tenant_id, conversation_id, home_epoch,
                 source_sequence_from, source_sequence_to, snapshot_kind,
                 encoded_snapshot, content_hash, created_at_ms
             ) VALUES (
                 $1, $2, $3, $4::bigint, 1, $5::bigint, $6, $7, $8, $9
             )
             ON CONFLICT (
                 tenant_id, conversation_id, home_epoch,
                 snapshot_kind, source_sequence_to
             ) DO NOTHING",
        )
        .bind(snapshot.snapshot_id.into_uuid())
        .bind(snapshot.tenant_id.into_uuid())
        .bind(snapshot.conversation_id.into_uuid())
        .bind(snapshot.home_epoch.get().to_string())
        .bind(snapshot.through_conversation_seq.get().to_string())
        .bind(snapshot_kind)
        .bind(&encoded)
        .bind(encoded_hash.into_bytes().to_vec())
        .bind(snapshot.created_at.unix_millis())
        .execute(&mut *transaction)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        let reusable_from = snapshot
            .through_conversation_seq
            .get()
            .saturating_sub(musteragent_projection::MAX_PUBLIC_PROJECTION_TAIL_RECORDS);
        sqlx::query(
            "DELETE FROM event_store.snapshots
             WHERE tenant_id = $1 AND conversation_id = $2
               AND home_epoch = $3::bigint AND snapshot_kind = $4
               AND source_sequence_to < $5::bigint",
        )
        .bind(snapshot.tenant_id.into_uuid())
        .bind(snapshot.conversation_id.into_uuid())
        .bind(snapshot.home_epoch.get().to_string())
        .bind(snapshot_kind)
        .bind(reusable_from.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;
        if inserted.rows_affected() == 1 {
            return Ok(snapshot.clone());
        }
        projection_snapshot_authority::load_at(
            &self.pool,
            self.codec.as_ref(),
            snapshot.tenant_id,
            snapshot.conversation_id,
            snapshot.home_epoch,
            snapshot.through_conversation_seq,
            snapshot_kind,
        )
        .await
    }
}

impl ConversationProjectionApplication for PostgresConversationProjection {
    fn get_product_read_projection_window(
        &self,
        query: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>> {
        Box::pin(async move {
            let mode = if query.before.is_some() {
                ProjectionReadMode::Reusable
            } else {
                ProjectionReadMode::Head
            };
            let snapshot = self
                .get(
                    GetConversationProjection {
                        actor: query.actor,
                        conversation_id: query.conversation_id,
                        preferred_snapshot_id: query
                            .before
                            .as_ref()
                            .map(|cursor| cursor.snapshot_id),
                    },
                    mode,
                )
                .await
                .map_err(|error| self.errors.map(error))?;
            projection_window_from_snapshot(
                &snapshot,
                query.before.as_ref(),
                query.limit,
                self.codec.as_ref(),
            )
            .map_err(|_| self.errors.map(SubscriptionError::CorruptRecord))
        })
    }

    fn get_conversation_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            self.get(query, ProjectionReadMode::Reusable)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }

    fn get_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            self.get(query, ProjectionReadMode::Head)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }

    fn get_bounded_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            self.get(query, ProjectionReadMode::BoundedHead)
                .await
                .map_err(|error| self.errors.map(error))
        })
    }
}

impl CursorRecoveryAuthoritySource for PostgresConversationProjection {
    fn current_cursor_recovery_authority(
        &self,
        actor: musteragent_identity::Actor,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<CursorRecoveryAuthority, SubscriptionError>> {
        Box::pin(async move {
            let snapshot = self
                .get(
                    GetConversationProjection {
                        actor,
                        conversation_id,
                        preferred_snapshot_id: None,
                    },
                    ProjectionReadMode::Head,
                )
                .await?;
            Ok(CursorRecoveryAuthority {
                snapshot_id: snapshot.snapshot_id,
                resume_cursor: snapshot.cursor(),
            })
        })
    }
}

struct RuntimeProjectionContentStore {
    inner: Arc<dyn ContentStore>,
}

impl ProjectionContentStore for RuntimeProjectionContentStore {
    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, SubscriptionError>> {
        Box::pin(async move {
            self.inner
                .read(tenant_id, content)
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)
        })
    }

    fn put(
        &self,
        tenant_id: TenantId,
        media_type: MediaType,
        bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, SubscriptionError>> {
        Box::pin(async move {
            self.inner
                .put(PutContent {
                    tenant_id,
                    media_type,
                    bytes,
                })
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)
        })
    }
}

async fn database_now(pool: &PgPool) -> Result<UtcTimestamp, SubscriptionError> {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}
