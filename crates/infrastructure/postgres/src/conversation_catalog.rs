//! Cloud-owned transactional Conversation Catalog projection.

use musteragent_conversation::{ConversationKind, ConversationLifecycle};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    Actor, ConversationId, DottedKey, HomeEpoch, LedgerOffset, PrincipalId, PrincipalKind,
    TenantId, UtcTimestamp,
};
use musteragent_projection::{
    AcknowledgeConversationSeenThrough, AttentionRevision, CatalogActionIdentity,
    CatalogActionKind, CatalogActivityPosition, CatalogAttentionAudience, CatalogAttentionKind,
    CatalogAttentionRecord, CatalogExecutionIdentity, CatalogExecutionStatus, CatalogFactDraft,
    CatalogGoalEvaluationProjectionSnapshot, CatalogGoalProjectionSnapshot,
    CatalogGoalPursuitStatus, CatalogProjectionApplyDisposition, CatalogProjectionAuthority,
    CatalogProjectionError, CatalogRecoverability, CatalogRunAuthority, CatalogRunWaitReason,
    CatalogSourceClaim, CatalogSourceIdentity, CatalogSourceKey, CatalogSourceKind,
    CatalogStandingGoalStatus, ConversationActionSummaryV2, ConversationCatalogCursorV2,
    ConversationCatalogInvalidationBatchV2, ConversationCatalogInvalidationV2,
    ConversationCatalogPageV2, ConversationCatalogProjection, ConversationCatalogProjectionPort,
    ConversationCatalogProjectorError, ConversationCatalogSourceError,
    ConversationCatalogSummaryV2, ConversationCatalogV2Source, ConversationExecutionSummaryV2,
    ConversationOwnedOutcomeSummaryV2, ConversationReadReceiptAck, ConversationUnreadSummaryV2,
    ExecutionOutcome, ExecutionOutcomeKind, ListConversationCatalogV2,
};
use musteragent_runtime_automation::StandingGoalProductStore;
use musteragent_runtime_contract::{ApplicationFuture, DisplayText};
use sqlx::{PgConnection, PgPool, Postgres, Row as _};
use thiserror::Error;
use uuid::Uuid;

use crate::PostgresStandingGoalProductStore;

#[path = "conversation_catalog/codec.rs"]
mod catalog_codec;
pub(crate) mod fact;
mod workspace_activity;
use catalog_codec::{
    count_u32, decode_counts, decode_owned_outcome, decode_title, optional_dotted,
    optional_strong_id, parse_u64, required_strong_id, storage_action_uuid, storage_uuid,
};

#[derive(Clone)]
pub struct PostgresConversationCatalogStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresConversationCatalogStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self { pool, database }
    }

    async fn write_unit_of_work(
        &self,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, PostgresConversationCatalogError> {
        self.database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Projection)
            .await
            .map_err(PostgresConversationCatalogError::Transaction)
    }

    pub async fn apply_fact(
        &self,
        draft: CatalogFactDraft,
    ) -> Result<CatalogProjectionApplyDisposition, PostgresConversationCatalogError> {
        let mut transaction = self.write_unit_of_work().await?;
        let disposition = fact::apply_fact_on(transaction.as_mut(), draft).await?;
        transaction.commit().await?;
        Ok(disposition)
    }

    /// Indexed keyset query over the durable Cloud projection. The query plan
    /// is page-bounded and never reads the Ledger.
    pub async fn list_catalog(
        &self,
        query: ListConversationCatalogV2,
    ) -> Result<ConversationCatalogPageV2, PostgresConversationCatalogError> {
        let query = query
            .validate()
            .map_err(|_| PostgresConversationCatalogError::InvalidQuery)?;
        let mut transaction = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(PostgresConversationCatalogError::Transaction)?;
        let snapshot: String = sqlx::query_scalar(
            "SELECT COALESCE((SELECT current_position
               FROM projection.conversation_catalog_clocks WHERE tenant_id = $1), 0)::text",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .fetch_one(transaction.as_mut())
        .await?;
        let snapshot_position = CatalogActivityPosition::new(parse_u64(snapshot)?);
        let after_at = query
            .after
            .map(|value| value.after_activity_at.unix_millis());
        let after_id = query
            .after
            .map(|value| value.after_conversation_id.into_uuid());
        let rows = sqlx::query(
            "WITH page AS (
                 SELECT catalog.*, conversation.workspace_id, conversation.kind,
                        conversation.title
                 FROM projection.conversation_catalog AS catalog
                 JOIN conversation.conversations AS conversation
                   ON conversation.tenant_id = catalog.tenant_id
                  AND conversation.conversation_id = catalog.conversation_id
                 WHERE conversation.purpose='standard' AND catalog.tenant_id = $1 AND catalog.lifecycle = $3
                   AND EXISTS(SELECT 1 FROM conversation.participants AS viewer
                       WHERE viewer.tenant_id = catalog.tenant_id
                         AND viewer.conversation_id = catalog.conversation_id
                         AND viewer.kind = 1 AND viewer.principal_ref = $2
                         AND viewer.left_at_ms IS NULL)
                   AND NOT EXISTS(
                       SELECT 1
                       FROM projection.conversation_catalog_bootstrap_receipts AS bootstrap
                       WHERE bootstrap.tenant_id = catalog.tenant_id
                         AND bootstrap.conversation_id = catalog.conversation_id
                   )
                   AND ($4::uuid IS NULL OR catalog.conversation_id = $4)
                   AND ($5::uuid IS NULL OR conversation.workspace_id = $5)
                   AND ($6::bigint IS NULL OR catalog.last_user_visible_activity_at_ms < $6
                        OR (catalog.last_user_visible_activity_at_ms = $6
                            AND catalog.conversation_id < $7))
                 ORDER BY catalog.last_user_visible_activity_at_ms DESC,
                          catalog.conversation_id DESC
                 LIMIT $8
             )
             SELECT page.*,
                    page.home_epoch::text AS home_epoch_text,
                    page.catalog_revision::text AS catalog_revision_text,
                    page.last_activity_position::text AS last_activity_position_text,
                    COALESCE(receipt.seen_through_revision, 0)::text AS seen_through_revision,
                    COALESCE(visible.latest_visible_revision, 0)::text AS latest_visible_revision,
                    COALESCE(execution.root_queued, 0) AS root_queued,
                    COALESCE(execution.root_running, 0) AS root_running,
                    COALESCE(execution.root_waiting, 0) AS root_waiting,
                    COALESCE(execution.root_cancelling, 0) AS root_cancelling,
                    COALESCE(execution.child_queued, 0) AS child_queued,
                    COALESCE(execution.child_running, 0) AS child_running,
                    COALESCE(execution.child_waiting, 0) AS child_waiting,
                    COALESCE(execution.child_cancelling, 0) AS child_cancelling,
                    COALESCE(execution.pursuit_queued, 0) AS pursuit_queued,
                    COALESCE(execution.pursuit_running, 0) AS pursuit_running,
                    COALESCE(execution.pursuit_waiting, 0) AS pursuit_waiting,
                    COALESCE(execution.pursuit_cancelling, 0) AS pursuit_cancelling,
                    COALESCE(execution.wait_reasons, ARRAY[]::smallint[]) AS wait_reasons,
                    COALESCE(action.action_count, 0) AS action_count,
                    COALESCE(action.action_kinds, ARRAY[]::smallint[]) AS action_kinds,
                    COALESCE(unread.unread_count, 0) AS unread_count,
                    latest_unread.attention_kind AS latest_unread_kind,
                    latest_unread.outcome_kind AS latest_unread_outcome_kind,
                    latest_unread.error_class AS latest_unread_error_class,
                    latest_unread.recoverability AS latest_unread_recoverability
             FROM page
             LEFT JOIN projection.conversation_read_receipts AS receipt
               ON receipt.tenant_id = page.tenant_id
              AND receipt.conversation_id = page.conversation_id
              AND receipt.principal_id = $2
             LEFT JOIN LATERAL (
                 SELECT MAX(attention_revision) AS latest_visible_revision
                 FROM projection.conversation_catalog_attention_deliveries
                 WHERE tenant_id = page.tenant_id
                   AND conversation_id = page.conversation_id AND principal_id = $2
             ) AS visible ON TRUE
             LEFT JOIN LATERAL (
                 SELECT
                   COUNT(*) FILTER(WHERE execution_kind=1 AND status=1) AS root_queued,
                   COUNT(*) FILTER(WHERE execution_kind=1 AND status=2) AS root_running,
                   COUNT(*) FILTER(WHERE execution_kind=1 AND status=3) AS root_waiting,
                   COUNT(*) FILTER(WHERE execution_kind=1 AND status=4) AS root_cancelling,
                   COUNT(*) FILTER(WHERE execution_kind=2 AND status=1) AS child_queued,
                   COUNT(*) FILTER(WHERE execution_kind=2 AND status=2) AS child_running,
                   COUNT(*) FILTER(WHERE execution_kind=2 AND status=3) AS child_waiting,
                   COUNT(*) FILTER(WHERE execution_kind=2 AND status=4) AS child_cancelling,
                   COUNT(*) FILTER(WHERE execution_kind=3 AND status=1) AS pursuit_queued,
                   COUNT(*) FILTER(WHERE execution_kind=3 AND status=2) AS pursuit_running,
                   COUNT(*) FILTER(WHERE execution_kind=3 AND status=3) AS pursuit_waiting,
                   COUNT(*) FILTER(WHERE execution_kind=3 AND status=4) AS pursuit_cancelling,
                   ARRAY_AGG(DISTINCT wait_reason ORDER BY wait_reason)
                     FILTER(WHERE wait_reason IS NOT NULL) AS wait_reasons
                 FROM projection.conversation_catalog_executions
                 WHERE tenant_id = page.tenant_id AND conversation_id = page.conversation_id
             ) AS execution ON TRUE
             LEFT JOIN LATERAL (
                 SELECT COUNT(*) AS action_count,
                        ARRAY_AGG(DISTINCT action_kind ORDER BY action_kind) AS action_kinds
                 FROM projection.conversation_catalog_actions
                 WHERE tenant_id = page.tenant_id AND conversation_id = page.conversation_id
             ) AS action ON TRUE
             LEFT JOIN LATERAL (
                 SELECT COUNT(*) AS unread_count
                 FROM projection.conversation_catalog_attention_deliveries AS delivery
                 WHERE delivery.tenant_id = page.tenant_id
                   AND delivery.conversation_id = page.conversation_id
                   AND delivery.principal_id = $2
                   AND delivery.attention_revision > COALESCE(receipt.seen_through_revision, 0)
             ) AS unread ON TRUE
             LEFT JOIN LATERAL (
                 SELECT attention.attention_kind, attention.outcome_kind,
                        attention.error_class, attention.recoverability
                 FROM projection.conversation_catalog_attention_deliveries AS delivery
                 JOIN projection.conversation_catalog_attention AS attention
                   ON attention.tenant_id = delivery.tenant_id
                  AND attention.conversation_id = delivery.conversation_id
                  AND attention.attention_revision = delivery.attention_revision
                 WHERE delivery.tenant_id = page.tenant_id
                   AND delivery.conversation_id = page.conversation_id
                   AND delivery.principal_id = $2
                   AND delivery.attention_revision > COALESCE(receipt.seen_through_revision, 0)
                 ORDER BY delivery.attention_revision DESC LIMIT 1
             ) AS latest_unread ON TRUE
             ORDER BY page.last_user_visible_activity_at_ms DESC, page.conversation_id DESC",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(encode_lifecycle(query.lifecycle))
        .bind(query.conversation_id.map(|value| value.into_uuid()))
        .bind(query.workspace_id.map(|value| value.into_uuid()))
        .bind(after_at)
        .bind(after_id)
        .bind(i64::from(query.page_size) + 1)
        .fetch_all(transaction.as_mut())
        .await?;
        let has_more = rows.len() > usize::from(query.page_size);
        let mut conversations = Vec::with_capacity(usize::from(query.page_size));
        for row in rows.into_iter().take(usize::from(query.page_size)) {
            let conversation_id = required_strong_id(row.try_get("conversation_id")?)?;
            let wait_reasons = row
                .try_get::<Vec<i16>, _>("wait_reasons")?
                .into_iter()
                .map(|value| {
                    CatalogRunWaitReason::from_storage_code(value)
                        .ok_or(PostgresConversationCatalogError::CorruptProjection)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let action_kinds = row
                .try_get::<Vec<i16>, _>("action_kinds")?
                .into_iter()
                .map(|value| {
                    CatalogActionKind::from_storage_code(value)
                        .ok_or(PostgresConversationCatalogError::CorruptProjection)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let highest_priority = action_kinds
                .iter()
                .copied()
                .min_by_key(|kind| kind.display_priority());
            conversations.push(ConversationCatalogSummaryV2 {
                tenant_id: query.actor.tenant_id,
                conversation_id,
                workspace_id: optional_strong_id(row.try_get("workspace_id")?)?,
                kind: decode_conversation_kind(row.try_get("kind")?)?,
                title: decode_title(row.try_get("title")?)?,
                lifecycle: decode_lifecycle(row.try_get("lifecycle")?)?,
                control_home: decode_home(
                    row.try_get("control_home_kind")?,
                    row.try_get("control_home_device_id")?,
                )?,
                home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch_text")?)?)
                    .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?,
                catalog_revision: parse_u64(row.try_get("catalog_revision_text")?)?,
                last_activity_position: CatalogActivityPosition::new(parse_u64(
                    row.try_get("last_activity_position_text")?,
                )?),
                last_user_visible_activity_at: UtcTimestamp::from_unix_millis(
                    row.try_get("last_user_visible_activity_at_ms")?,
                ),
                metadata_updated_at: UtcTimestamp::from_unix_millis(
                    row.try_get("metadata_updated_at_ms")?,
                ),
                execution: ConversationExecutionSummaryV2 {
                    root: decode_counts(&row, "root")?,
                    child: decode_counts(&row, "child")?,
                    pursuit: decode_counts(&row, "pursuit")?,
                    wait_reasons,
                },
                action_required: ConversationActionSummaryV2 {
                    count: count_u32(row.try_get("action_count")?)?,
                    highest_priority,
                    kinds: action_kinds,
                },
                standing_goal_status: row
                    .try_get::<Option<i16>, _>("standing_goal_status")?
                    .map(decode_standing_goal_status)
                    .transpose()?,
                pursuit_status: decode_optional_pursuit_status(
                    row.try_get("pursuit_status")?,
                    row.try_get("pursuit_wait_reason")?,
                )?,
                latest_outcome: decode_owned_outcome(
                    row.try_get("latest_outcome_kind")?,
                    row.try_get("latest_error_class")?,
                    row.try_get("latest_recoverability")?,
                )?,
                unread: ConversationUnreadSummaryV2 {
                    count: count_u32(row.try_get("unread_count")?)?,
                    seen_through: AttentionRevision::new(parse_u64(
                        row.try_get("seen_through_revision")?,
                    )?),
                    latest_visible_revision: AttentionRevision::new(parse_u64(
                        row.try_get("latest_visible_revision")?,
                    )?),
                    latest_unread_kind: row
                        .try_get::<Option<i16>, _>("latest_unread_kind")?
                        .map(|value| {
                            CatalogAttentionKind::from_storage_code(value)
                                .ok_or(PostgresConversationCatalogError::CorruptProjection)
                        })
                        .transpose()?,
                    latest_unread_outcome: decode_owned_outcome(
                        row.try_get("latest_unread_outcome_kind")?,
                        row.try_get("latest_unread_error_class")?,
                        row.try_get("latest_unread_recoverability")?,
                    )?,
                },
            });
        }
        let next_cursor = if has_more {
            conversations
                .last()
                .map(|value| ConversationCatalogCursorV2 {
                    snapshot_position,
                    after_activity_at: value.last_user_visible_activity_at,
                    after_conversation_id: value.conversation_id,
                })
        } else {
            None
        };
        transaction.commit().await?;
        Ok(ConversationCatalogPageV2 {
            snapshot_position,
            conversations,
            next_cursor,
        })
    }

    async fn finalize_bootstrap_receipts_batch(
        &self,
        limit: u16,
        finalized_at: UtcTimestamp,
    ) -> Result<usize, PostgresConversationCatalogError> {
        if limit == 0 {
            return Err(PostgresConversationCatalogError::InvalidQuery);
        }
        let mut transaction = self.write_unit_of_work().await?;
        let rows = sqlx::query(
            "SELECT bootstrap.tenant_id, bootstrap.conversation_id
             FROM projection.conversation_catalog_bootstrap_receipts AS bootstrap
             JOIN projection.conversation_catalog AS catalog
               ON catalog.tenant_id = bootstrap.tenant_id
              AND catalog.conversation_id = bootstrap.conversation_id
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = bootstrap.tenant_id
              AND conversation.conversation_id = bootstrap.conversation_id
              AND conversation.home_epoch = catalog.home_epoch
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
              AND stream.home_epoch = conversation.home_epoch
             JOIN projection.conversation_catalog_projector_checkpoints AS checkpoint
               ON checkpoint.tenant_id = conversation.tenant_id
              AND checkpoint.conversation_id = conversation.conversation_id
              AND checkpoint.home_epoch = conversation.home_epoch
              AND checkpoint.through_conversation_seq >= stream.last_sequence
             GROUP BY bootstrap.tenant_id, bootstrap.conversation_id
             ORDER BY bootstrap.tenant_id, bootstrap.conversation_id
             LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(transaction.as_mut())
        .await?;
        let mut finalized = 0;
        for row in rows {
            let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
                .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?;
            let conversation_id = ConversationId::from_uuid(row.try_get("conversation_id")?)
                .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?;
            lock_conversation(&mut transaction, tenant_id, conversation_id).await?;
            sqlx::query(
                "INSERT INTO projection.conversation_read_receipts(
                     tenant_id, conversation_id, principal_id, control_home_kind,
                     control_home_device_id, home_epoch, seen_through_revision,
                     acknowledged_at_ms
                 )
                 SELECT bootstrap.tenant_id, bootstrap.conversation_id,
                        bootstrap.principal_id, catalog.control_home_kind,
                        catalog.control_home_device_id, catalog.home_epoch,
                        COALESCE(MAX(delivery.attention_revision), 0), $3
                 FROM projection.conversation_catalog_bootstrap_receipts AS bootstrap
                 JOIN projection.conversation_catalog AS catalog
                   ON catalog.tenant_id = bootstrap.tenant_id
                  AND catalog.conversation_id = bootstrap.conversation_id
                 LEFT JOIN projection.conversation_catalog_attention_deliveries AS delivery
                   ON delivery.tenant_id = bootstrap.tenant_id
                  AND delivery.conversation_id = bootstrap.conversation_id
                  AND delivery.principal_id = bootstrap.principal_id
                 WHERE bootstrap.tenant_id = $1 AND bootstrap.conversation_id = $2
                 GROUP BY bootstrap.tenant_id, bootstrap.conversation_id,
                          bootstrap.principal_id, catalog.control_home_kind,
                          catalog.control_home_device_id, catalog.home_epoch
                 ON CONFLICT(tenant_id, conversation_id, principal_id) DO UPDATE SET
                     control_home_kind = excluded.control_home_kind,
                     control_home_device_id = excluded.control_home_device_id,
                     home_epoch = excluded.home_epoch,
                     seen_through_revision = CASE
                         WHEN excluded.home_epoch
                              = projection.conversation_read_receipts.home_epoch
                         THEN GREATEST(
                             projection.conversation_read_receipts.seen_through_revision,
                             excluded.seen_through_revision
                         )
                         ELSE excluded.seen_through_revision
                     END,
                     acknowledged_at_ms = CASE
                         WHEN excluded.home_epoch
                                  <> projection.conversation_read_receipts.home_epoch
                           OR excluded.seen_through_revision
                                  > projection.conversation_read_receipts.seen_through_revision
                         THEN excluded.acknowledged_at_ms
                         ELSE projection.conversation_read_receipts.acknowledged_at_ms
                     END",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(finalized_at.unix_millis())
            .execute(transaction.as_mut())
            .await?;
            sqlx::query(
                "DELETE FROM projection.conversation_catalog_bootstrap_receipts
                 WHERE tenant_id = $1 AND conversation_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .execute(transaction.as_mut())
            .await?;

            let position = allocate_position(transaction.as_mut(), tenant_id).await?;
            let catalog_revision: String = sqlx::query_scalar(
                "SELECT catalog_revision::text FROM projection.conversation_catalog
                 WHERE tenant_id = $1 AND conversation_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .fetch_one(transaction.as_mut())
            .await?;
            sqlx::query(
                "INSERT INTO projection.conversation_catalog_changes(
                     tenant_id, activity_position, conversation_id, principal_id,
                     change_kind, catalog_revision, committed_at_ms
                 ) VALUES($1, $2::numeric, $3, NULL, 1, $4::numeric, $5)",
            )
            .bind(tenant_id.into_uuid())
            .bind(position.get().to_string())
            .bind(conversation_id.into_uuid())
            .bind(catalog_revision)
            .bind(finalized_at.unix_millis())
            .execute(transaction.as_mut())
            .await?;
            finalized += 1;
        }
        transaction.commit().await?;
        Ok(finalized)
    }

    pub async fn acknowledge_seen_through(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        principal_id: PrincipalId,
        expected_home_epoch: HomeEpoch,
        seen_through: AttentionRevision,
        acknowledged_at: UtcTimestamp,
    ) -> Result<bool, PostgresConversationCatalogError> {
        let mut transaction = self.write_unit_of_work().await?;
        lock_conversation(&mut transaction, tenant_id, conversation_id).await?;
        let epoch: String = sqlx::query_scalar(
            "SELECT home_epoch::text FROM projection.conversation_catalog
             WHERE tenant_id = $1 AND conversation_id = $2 AND control_home_kind = 1
             FOR UPDATE",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_optional(transaction.as_mut())
        .await?
        .ok_or(PostgresConversationCatalogError::ConversationNotProjected)?;
        if parse_u64(epoch)? != expected_home_epoch.get() {
            return Err(PostgresConversationCatalogError::AuthorityScopeMismatch);
        }
        let authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM conversation.participants
                 WHERE tenant_id = $1 AND conversation_id = $2 AND kind = 1
                   AND principal_ref = $3 AND left_at_ms IS NULL
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .fetch_one(transaction.as_mut())
        .await?;
        if !authorized {
            return Err(PostgresConversationCatalogError::Unauthorized);
        }
        let latest_visible: String = sqlx::query_scalar(
            "SELECT COALESCE(MAX(attention_revision), 0)::text
             FROM projection.conversation_catalog_attention_deliveries
             WHERE tenant_id = $1 AND conversation_id = $2 AND principal_id = $3",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .fetch_one(transaction.as_mut())
        .await?;
        if seen_through.get() > parse_u64(latest_visible)? {
            return Err(PostgresConversationCatalogError::ReceiptBeyondVisibleRevision);
        }
        let result = sqlx::query(
            "INSERT INTO projection.conversation_read_receipts(
                 tenant_id, conversation_id, principal_id, control_home_kind,
                 control_home_device_id, home_epoch, seen_through_revision, acknowledged_at_ms
             ) SELECT $1, $2, $3, control_home_kind, control_home_device_id,
                      home_epoch, $4::numeric, $5
               FROM projection.conversation_catalog
              WHERE tenant_id = $1 AND conversation_id = $2
             ON CONFLICT(tenant_id, conversation_id, principal_id) DO UPDATE SET
                 seen_through_revision = excluded.seen_through_revision,
                 acknowledged_at_ms = excluded.acknowledged_at_ms
             WHERE excluded.home_epoch = projection.conversation_read_receipts.home_epoch
               AND excluded.seen_through_revision
                   > projection.conversation_read_receipts.seen_through_revision",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(seen_through.get().to_string())
        .bind(acknowledged_at.unix_millis())
        .execute(transaction.as_mut())
        .await?;
        let changed = result.rows_affected() > 0;
        if changed {
            let position = allocate_position(transaction.as_mut(), tenant_id).await?;
            let catalog_revision: String = sqlx::query_scalar(
                "SELECT catalog_revision::text FROM projection.conversation_catalog
                 WHERE tenant_id = $1 AND conversation_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .fetch_one(transaction.as_mut())
            .await?;
            sqlx::query(
                "INSERT INTO projection.conversation_catalog_changes(
                     tenant_id, activity_position, conversation_id, principal_id,
                     change_kind, catalog_revision, committed_at_ms
                 ) VALUES($1, $2::numeric, $3, $4, 2, $5::numeric, $6)",
            )
            .bind(tenant_id.into_uuid())
            .bind(position.get().to_string())
            .bind(conversation_id.into_uuid())
            .bind(principal_id.into_uuid())
            .bind(catalog_revision)
            .bind(acknowledged_at.unix_millis())
            .execute(transaction.as_mut())
            .await?;
        }
        transaction.commit().await?;
        Ok(changed)
    }

    pub async fn read_invalidations(
        &self,
        actor: Actor,
        after: CatalogActivityPosition,
        limit: u16,
    ) -> Result<ConversationCatalogInvalidationBatchV2, PostgresConversationCatalogError> {
        if limit == 0 || limit > 4_096 {
            return Err(PostgresConversationCatalogError::InvalidQuery);
        }
        let latest: String = sqlx::query_scalar(
            "SELECT COALESCE((SELECT current_position
               FROM projection.conversation_catalog_clocks WHERE tenant_id = $1), 0)::text",
        )
        .bind(actor.tenant_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT change.activity_position::text AS activity_position,
                    change.conversation_id
             FROM projection.conversation_catalog_changes AS change
             JOIN conversation.conversations AS c ON c.tenant_id=change.tenant_id AND c.conversation_id=change.conversation_id AND c.purpose='standard'
             WHERE change.tenant_id = $1 AND change.activity_position > $2::numeric
               AND (change.principal_id IS NULL OR change.principal_id = $3)
               AND EXISTS(SELECT 1 FROM conversation.participants AS viewer
                   WHERE viewer.tenant_id = change.tenant_id
                     AND viewer.conversation_id = change.conversation_id
                     AND viewer.kind = 1 AND viewer.principal_ref = $3
                     AND viewer.left_at_ms IS NULL)
             ORDER BY change.activity_position ASC
             LIMIT $4",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(after.get().to_string())
        .bind(actor.principal_id.into_uuid())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let invalidations = rows
            .into_iter()
            .map(|row| {
                Ok(ConversationCatalogInvalidationV2 {
                    position: CatalogActivityPosition::new(parse_u64(
                        row.try_get("activity_position")?,
                    )?),
                    conversation_id: ConversationId::from_uuid(
                        row.try_get::<Uuid, _>("conversation_id")?,
                    )
                    .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?,
                })
            })
            .collect::<Result<Vec<_>, PostgresConversationCatalogError>>()?;
        Ok(ConversationCatalogInvalidationBatchV2 {
            invalidations,
            latest_position: CatalogActivityPosition::new(parse_u64(latest)?),
        })
    }

    async fn goal_projection_snapshots(
        &self,
        limit: u16,
    ) -> Result<Vec<CatalogGoalProjectionSnapshot>, ConversationCatalogProjectorError> {
        if limit == 0 {
            return Err(ConversationCatalogProjectorError::Corrupt);
        }
        let rows = sqlx::query(
            "SELECT goal.tenant_id, goal.goal_id,
                    conversation.home_epoch::text AS home_epoch
             FROM runtime.standing_goals AS goal
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = goal.tenant_id
              AND conversation.conversation_id = goal.conversation_id
              AND conversation.purpose='standard' AND conversation.control_home_kind = 1
             WHERE NOT EXISTS (
                 SELECT 1 FROM projection.conversation_catalog_sources AS source
                 WHERE source.tenant_id = goal.tenant_id
                   AND source.conversation_id = goal.conversation_id
                   AND source.source_kind = 18 AND source.source_identity_kind = 6
                   AND source.source_identity = goal.goal_id
                   AND source.semantic_revision = goal.revision
             ) OR EXISTS (
                 SELECT 1 FROM runtime.goal_pursuits AS pursuit
                 WHERE pursuit.tenant_id = goal.tenant_id AND pursuit.goal_id = goal.goal_id
                   AND pursuit.pursuit_id = (SELECT latest.pursuit_id FROM runtime.goal_pursuits latest
                       WHERE latest.tenant_id=goal.tenant_id AND latest.goal_id=goal.goal_id
                       ORDER BY latest.started_at_ms DESC, latest.pursuit_id DESC LIMIT 1)
                   AND NOT EXISTS (
                       SELECT 1 FROM projection.conversation_catalog_sources AS source
                       WHERE source.tenant_id = goal.tenant_id
                         AND source.conversation_id = goal.conversation_id
                         AND source.source_kind = 19 AND source.source_identity_kind = 7
                         AND source.source_identity = pursuit.pursuit_id
                         AND source.semantic_revision = pursuit.revision
                   )
             )
             ORDER BY goal.updated_at_ms, goal.goal_id LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ConversationCatalogProjectorError::Unavailable)?;
        let goals = PostgresStandingGoalProductStore::new(self.pool.clone());
        let mut snapshots = Vec::with_capacity(rows.len());
        for row in rows {
            let tenant_id = TenantId::from_uuid(projector_pg_column(&row, "tenant_id")?)
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
            let goal_id =
                musteragent_identity::GoalId::from_uuid(projector_pg_column(&row, "goal_id")?)
                    .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
            let (goal, pursuit) = goals
                .load(tenant_id, goal_id)
                .await
                .map_err(|_| ConversationCatalogProjectorError::Unavailable)?
                .ok_or(ConversationCatalogProjectorError::Corrupt)?;
            snapshots.push(CatalogGoalProjectionSnapshot {
                control_home: RuntimeHome::Cloud,
                home_epoch: HomeEpoch::new(
                    parse_u64(projector_pg_column(&row, "home_epoch")?)
                        .map_err(map_projector_error)?,
                )
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?,
                goal,
                pursuit,
            });
        }
        Ok(snapshots)
    }

    async fn goal_evaluation_projection_snapshots(
        &self,
        limit: u16,
    ) -> Result<Vec<CatalogGoalEvaluationProjectionSnapshot>, ConversationCatalogProjectorError>
    {
        if limit == 0 {
            return Err(ConversationCatalogProjectorError::Corrupt);
        }
        let rows = sqlx::query(&format!(
            "SELECT evaluation.*, goal.conversation_id,
                    conversation.home_epoch::text AS home_epoch
             FROM ({}) AS evaluation
             JOIN runtime.standing_goals AS goal
               ON goal.tenant_id = evaluation.tenant_id AND goal.goal_id = evaluation.goal_id
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = goal.tenant_id
              AND conversation.conversation_id = goal.conversation_id
              AND conversation.purpose='standard' AND conversation.control_home_kind = 1
             WHERE NOT EXISTS (
                 SELECT 1 FROM projection.conversation_catalog_sources AS source
                 WHERE source.tenant_id = goal.tenant_id
                   AND source.conversation_id = goal.conversation_id
                   AND source.source_kind = 9 AND source.source_identity_kind = 8
                   AND source.source_identity = evaluation.evaluation_id
                   AND source.semantic_revision = 1
             ) ORDER BY evaluation.occurred_at_ms, evaluation.evaluation_id LIMIT $1",
            crate::standing_goal::EVALUATION_SELECT
        ))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ConversationCatalogProjectorError::Unavailable)?;
        let mut snapshots = Vec::with_capacity(rows.len());
        for row in rows {
            let conversation_id =
                ConversationId::from_uuid(projector_pg_column(&row, "conversation_id")?)
                    .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
            let evaluation = crate::standing_goal::decode_evaluation(&self.pool, &row)
                .await
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
            snapshots.push(CatalogGoalEvaluationProjectionSnapshot {
                control_home: RuntimeHome::Cloud,
                home_epoch: HomeEpoch::new(
                    parse_u64(projector_pg_column(&row, "home_epoch")?)
                        .map_err(map_projector_error)?,
                )
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?,
                conversation_id,
                evaluation,
            });
        }
        Ok(snapshots)
    }
}

impl ConversationCatalogV2Source for PostgresConversationCatalogStore {
    fn list(
        &self,
        query: ListConversationCatalogV2,
    ) -> ApplicationFuture<'_, Result<ConversationCatalogPageV2, ConversationCatalogSourceError>>
    {
        Box::pin(async move { self.list_catalog(query).await.map_err(map_source_error) })
    }

    fn acknowledge(
        &self,
        command: AcknowledgeConversationSeenThrough,
    ) -> ApplicationFuture<'_, Result<ConversationReadReceiptAck, ConversationCatalogSourceError>>
    {
        Box::pin(async move {
            let changed = self
                .acknowledge_seen_through(
                    command.actor.tenant_id,
                    command.conversation_id,
                    command.actor.principal_id,
                    command.expected_home_epoch,
                    command.seen_through,
                    command.rendered_at,
                )
                .await
                .map_err(map_source_error)?;
            let persisted: String = sqlx::query_scalar(
                "SELECT seen_through_revision::text
                 FROM projection.conversation_read_receipts
                 WHERE tenant_id = $1 AND conversation_id = $2 AND principal_id = $3",
            )
            .bind(command.actor.tenant_id.into_uuid())
            .bind(command.conversation_id.into_uuid())
            .bind(command.actor.principal_id.into_uuid())
            .fetch_one(&self.pool)
            .await
            .map_err(|_| ConversationCatalogSourceError::Unavailable)?;
            Ok(ConversationReadReceiptAck {
                principal_id: command.actor.principal_id,
                conversation_id: command.conversation_id,
                seen_through: AttentionRevision::new(
                    parse_u64(persisted).map_err(map_source_error)?,
                ),
                changed,
            })
        })
    }

    fn read_invalidations(
        &self,
        actor: Actor,
        after: CatalogActivityPosition,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<ConversationCatalogInvalidationBatchV2, ConversationCatalogSourceError>,
    > {
        Box::pin(async move {
            self.read_invalidations(actor, after, limit)
                .await
                .map_err(map_source_error)
        })
    }
}

impl ConversationCatalogProjectionPort for PostgresConversationCatalogStore {
    fn list_projection_authorities(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogProjectionAuthority>, ConversationCatalogProjectorError>,
    > {
        Box::pin(async move {
            if limit == 0 {
                return Err(ConversationCatalogProjectorError::Corrupt);
            }
            let rows = sqlx::query(
                "SELECT conversation.tenant_id, conversation.conversation_id,
                        conversation.home_epoch::text AS home_epoch,
                        MIN(viewer.principal_ref::text) AS principal_ref,
                        checkpoint.through_conversation_seq::text AS through_conversation_seq
                 FROM conversation.conversations AS conversation
                 JOIN event_store.ledger_streams AS stream
                   ON stream.tenant_id = conversation.tenant_id
                  AND stream.conversation_id = conversation.conversation_id
                  AND stream.home_epoch = conversation.home_epoch
                 JOIN conversation.participants AS viewer
                   ON viewer.tenant_id = conversation.tenant_id
                  AND viewer.conversation_id = conversation.conversation_id
                  AND viewer.kind = 1 AND viewer.left_at_ms IS NULL
                 LEFT JOIN projection.conversation_catalog_projector_checkpoints AS checkpoint
                   ON checkpoint.tenant_id = conversation.tenant_id
                  AND checkpoint.conversation_id = conversation.conversation_id
                  AND checkpoint.home_epoch = conversation.home_epoch
                 WHERE conversation.purpose='standard' AND conversation.control_home_kind = 1
                   AND (checkpoint.through_conversation_seq IS NULL
                     OR checkpoint.through_conversation_seq < stream.last_sequence)
                 GROUP BY conversation.tenant_id, conversation.conversation_id,
                          conversation.home_epoch, checkpoint.through_conversation_seq
                 ORDER BY conversation.tenant_id, conversation.conversation_id
                 LIMIT $1",
            )
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| ConversationCatalogProjectorError::Unavailable)?;
            rows.into_iter()
                .map(|row| {
                    let tenant_id = TenantId::from_uuid(projector_pg_column(&row, "tenant_id")?)
                        .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
                    let conversation_id =
                        ConversationId::from_uuid(projector_pg_column(&row, "conversation_id")?)
                            .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
                    let principal = projector_pg_column::<String>(&row, "principal_ref")?
                        .parse()
                        .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
                    let home_epoch = HomeEpoch::new(
                        parse_u64(projector_pg_column(&row, "home_epoch")?)
                            .map_err(map_projector_error)?,
                    )
                    .map_err(|_| ConversationCatalogProjectorError::Corrupt)?;
                    let after =
                        projector_pg_column::<Option<String>>(&row, "through_conversation_seq")?
                            .map_or(Ok(LedgerOffset::new(0)), |value| {
                                parse_u64(value)
                                    .map(LedgerOffset::new)
                                    .map_err(map_projector_error)
                            })?;
                    Ok(CatalogProjectionAuthority {
                        actor: Actor {
                            tenant_id,
                            principal_id: principal,
                            principal_kind: PrincipalKind::User,
                        },
                        conversation_id,
                        control_home: RuntimeHome::Cloud,
                        home_epoch,
                        after,
                    })
                })
                .collect()
        })
    }

    fn resolve_run_authority(
        &self,
        tenant_id: TenantId,
        run_id: musteragent_identity::RunId,
    ) -> ApplicationFuture<'_, Result<CatalogRunAuthority, ConversationCatalogProjectorError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT run.parent_run_id, run.delegation_id,
                        run.requested_by_participant_id, binding.pursuit_id
                 FROM runtime.runs AS run
                 LEFT JOIN projection.conversation_catalog_pursuit_runs AS binding
                   ON binding.tenant_id = run.tenant_id AND binding.run_id = run.run_id
                 WHERE run.tenant_id = $1 AND run.run_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(run_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| ConversationCatalogProjectorError::Unavailable)?
            .ok_or(ConversationCatalogProjectorError::Corrupt)?;
            Ok(CatalogRunAuthority {
                parent_run_id: projector_pg_optional_id(&row, "parent_run_id")?,
                delegation_id: projector_pg_optional_id(&row, "delegation_id")?,
                pursuit_id: projector_pg_optional_id(&row, "pursuit_id")?,
                requested_by_participant_id: musteragent_identity::ParticipantId::from_uuid(
                    projector_pg_column(&row, "requested_by_participant_id")?,
                )
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?,
            })
        })
    }

    fn list_goal_projection_snapshots(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogGoalProjectionSnapshot>, ConversationCatalogProjectorError>,
    > {
        Box::pin(async move { self.goal_projection_snapshots(limit).await })
    }

    fn list_goal_evaluation_projection_snapshots(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogGoalEvaluationProjectionSnapshot>, ConversationCatalogProjectorError>,
    > {
        Box::pin(async move { self.goal_evaluation_projection_snapshots(limit).await })
    }

    fn apply_catalog_fact(
        &self,
        draft: CatalogFactDraft,
    ) -> ApplicationFuture<
        '_,
        Result<CatalogProjectionApplyDisposition, ConversationCatalogProjectorError>,
    > {
        Box::pin(async move { self.apply_fact(draft).await.map_err(map_projector_error) })
    }

    fn advance_projection_checkpoint(
        &self,
        authority: CatalogProjectionAuthority,
        through: LedgerOffset,
        projected_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ConversationCatalogProjectorError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO projection.conversation_catalog_projector_checkpoints(
                     tenant_id, conversation_id, home_epoch, through_conversation_seq,
                     projected_at_ms
                 ) VALUES($1, $2, $3::numeric, $4::numeric, $5)
                 ON CONFLICT(tenant_id, conversation_id, home_epoch) DO UPDATE SET
                     through_conversation_seq = excluded.through_conversation_seq,
                     projected_at_ms = excluded.projected_at_ms
                 WHERE excluded.through_conversation_seq
                       > projection.conversation_catalog_projector_checkpoints.through_conversation_seq",
            )
            .bind(authority.actor.tenant_id.into_uuid())
            .bind(authority.conversation_id.into_uuid())
            .bind(authority.home_epoch.get().to_string())
            .bind(through.get().to_string())
            .bind(projected_at.unix_millis())
            .execute(&self.pool)
            .await
            .map_err(|_| ConversationCatalogProjectorError::Unavailable)?;
            Ok(())
        })
    }

    fn finalize_bootstrap_receipts(
        &self,
        limit: u16,
        finalized_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<usize, ConversationCatalogProjectorError>> {
        Box::pin(async move {
            self.finalize_bootstrap_receipts_batch(limit, finalized_at)
                .await
                .map_err(map_projector_error)
        })
    }
}

fn projector_pg_column<'row, T>(
    row: &'row sqlx::postgres::PgRow,
    name: &str,
) -> Result<T, ConversationCatalogProjectorError>
where
    T: sqlx::Decode<'row, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get(name)
        .map_err(|_| ConversationCatalogProjectorError::Corrupt)
}

fn projector_pg_optional_id<T>(
    row: &sqlx::postgres::PgRow,
    name: &str,
) -> Result<Option<T>, ConversationCatalogProjectorError>
where
    T: std::str::FromStr,
{
    projector_pg_column::<Option<Uuid>>(row, name)?
        .map(|value| {
            value
                .to_string()
                .parse()
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)
        })
        .transpose()
}

fn map_projector_error(
    error: PostgresConversationCatalogError,
) -> ConversationCatalogProjectorError {
    match error {
        PostgresConversationCatalogError::Storage(_)
        | PostgresConversationCatalogError::Transaction(_) => {
            ConversationCatalogProjectorError::Unavailable
        }
        PostgresConversationCatalogError::AuthorityScopeMismatch => {
            ConversationCatalogProjectorError::AuthorityStale
        }
        _ => ConversationCatalogProjectorError::Corrupt,
    }
}

fn map_source_error(error: PostgresConversationCatalogError) -> ConversationCatalogSourceError {
    match error {
        PostgresConversationCatalogError::InvalidQuery
        | PostgresConversationCatalogError::ReceiptBeyondVisibleRevision
        | PostgresConversationCatalogError::AuthorityScopeMismatch => {
            ConversationCatalogSourceError::InvalidQuery
        }
        PostgresConversationCatalogError::Unauthorized => {
            ConversationCatalogSourceError::Unauthorized
        }
        PostgresConversationCatalogError::ConversationNotProjected => {
            ConversationCatalogSourceError::NotFound
        }
        PostgresConversationCatalogError::CorruptProjection
        | PostgresConversationCatalogError::Projection(_) => {
            ConversationCatalogSourceError::Corrupt
        }
        PostgresConversationCatalogError::Storage(_)
        | PostgresConversationCatalogError::Transaction(_) => {
            ConversationCatalogSourceError::Unavailable
        }
    }
}

#[derive(Debug, Error)]
pub enum PostgresConversationCatalogError {
    #[error(transparent)]
    Storage(#[from] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error(transparent)]
    Projection(#[from] CatalogProjectionError),
    #[error("Conversation has not been projected into the catalog")]
    ConversationNotProjected,
    #[error("Conversation Catalog projection is corrupt")]
    CorruptProjection,
    #[error("Conversation Catalog authority scope changed")]
    AuthorityScopeMismatch,
    #[error("Principal is not authorized for this Conversation Catalog row")]
    Unauthorized,
    #[error("Conversation Catalog query is invalid")]
    InvalidQuery,
    #[error("read receipt exceeds the latest Principal-visible revision")]
    ReceiptBeyondVisibleRevision,
}

async fn lock_conversation(
    transaction: &mut PgConnection,
    tenant_id: TenantId,
    conversation_id: ConversationId,
) -> Result<(), PostgresConversationCatalogError> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM conversation.conversations
         WHERE tenant_id = $1 AND conversation_id = $2 AND control_home_kind = 1 AND purpose='standard'
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    if found.is_none() {
        return Err(PostgresConversationCatalogError::ConversationNotProjected);
    }
    Ok(())
}

async fn claim_source(
    transaction: &mut PgConnection,
    draft: &CatalogFactDraft,
) -> Result<bool, PostgresConversationCatalogError> {
    let source = draft.source_key.identity;
    let result = sqlx::query(
        "INSERT INTO projection.conversation_catalog_sources(
             tenant_id, conversation_id, source_kind, source_identity_kind,
             source_identity, semantic_revision, committed_at_ms
         ) VALUES($1, $2, $3, $4, $5, $6::numeric, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.conversation_id.into_uuid())
    .bind(draft.source_key.kind.storage_code())
    .bind(source.storage_kind_code())
    .bind(storage_uuid(source)?)
    .bind(draft.source_key.semantic_revision.to_string())
    .bind(draft.committed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn allocate_position(
    connection: &mut PgConnection,
    tenant_id: TenantId,
) -> Result<CatalogActivityPosition, PostgresConversationCatalogError> {
    let value: String = sqlx::query_scalar(
        "INSERT INTO projection.conversation_catalog_clocks(tenant_id, current_position)
         VALUES($1, 1)
         ON CONFLICT(tenant_id) DO UPDATE
             SET current_position = projection.conversation_catalog_clocks.current_position + 1
         RETURNING current_position::text",
    )
    .bind(tenant_id.into_uuid())
    .fetch_one(connection)
    .await?;
    Ok(CatalogActivityPosition::new(parse_u64(value)?))
}

async fn persist_change(
    connection: &mut PgConnection,
    projection: &ConversationCatalogProjection,
    position: CatalogActivityPosition,
    principal_id: Option<PrincipalId>,
    change_kind: i16,
    committed_at: UtcTimestamp,
) -> Result<(), PostgresConversationCatalogError> {
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_changes(
             tenant_id, activity_position, conversation_id, principal_id,
             change_kind, catalog_revision, committed_at_ms
         ) VALUES($1, $2::numeric, $3, $4, $5, $6::numeric, $7)",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(position.get().to_string())
    .bind(projection.conversation_id.into_uuid())
    .bind(principal_id.map(|value| value.into_uuid()))
    .bind(change_kind)
    .bind(projection.catalog_revision.to_string())
    .bind(committed_at.unix_millis())
    .execute(connection)
    .await?;
    Ok(())
}

async fn load_projection(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    conversation_id: ConversationId,
) -> Result<ConversationCatalogProjection, PostgresConversationCatalogError> {
    let row = sqlx::query(
        "SELECT conversation.workspace_id, conversation.kind, conversation.lifecycle,
                conversation.control_home_kind, conversation.control_device_id,
                conversation.home_epoch::text AS conversation_home_epoch,
                conversation.created_at_ms,
                catalog.catalog_revision::text AS catalog_revision,
                catalog.attention_revision::text AS attention_revision,
                catalog.latest_commit_position::text AS latest_commit_position,
                catalog.last_activity_position::text AS last_activity_position,
                catalog.last_user_visible_activity_at_ms, catalog.metadata_updated_at_ms,
                catalog.latest_outcome_kind, catalog.latest_error_class,
                catalog.latest_recoverability, catalog.standing_goal_status,
                catalog.pursuit_status, catalog.pursuit_wait_reason
         FROM conversation.conversations AS conversation
         LEFT JOIN projection.conversation_catalog AS catalog
           ON catalog.tenant_id = conversation.tenant_id
          AND catalog.conversation_id = conversation.conversation_id
         WHERE conversation.purpose='standard' AND conversation.tenant_id = $1 AND conversation.conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(PostgresConversationCatalogError::ConversationNotProjected)?;
    let created_at = UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?);
    let mut projection = ConversationCatalogProjection::new(
        decode_home(
            row.try_get("control_home_kind")?,
            row.try_get("control_device_id")?,
        )?,
        HomeEpoch::new(parse_u64(row.try_get("conversation_home_epoch")?)?)
            .map_err(|_| PostgresConversationCatalogError::CorruptProjection)?,
        tenant_id,
        conversation_id,
        optional_strong_id(row.try_get::<Option<Uuid>, _>("workspace_id")?)?,
        decode_conversation_kind(row.try_get("kind")?)?,
        decode_lifecycle(row.try_get("lifecycle")?)?,
        created_at,
    );
    let Some(catalog_revision) = row.try_get::<Option<String>, _>("catalog_revision")? else {
        return Ok(projection);
    };
    projection.catalog_revision = parse_u64(catalog_revision)?;
    projection.attention_revision =
        AttentionRevision::new(parse_u64(row.try_get("attention_revision")?)?);
    projection.latest_commit_position =
        CatalogActivityPosition::new(parse_u64(row.try_get("latest_commit_position")?)?);
    projection.last_activity_position =
        CatalogActivityPosition::new(parse_u64(row.try_get("last_activity_position")?)?);
    projection.last_user_visible_activity_at =
        UtcTimestamp::from_unix_millis(row.try_get("last_user_visible_activity_at_ms")?);
    projection.metadata_updated_at =
        UtcTimestamp::from_unix_millis(row.try_get("metadata_updated_at_ms")?);
    projection.latest_outcome = decode_optional_outcome(
        row.try_get("latest_outcome_kind")?,
        row.try_get("latest_error_class")?,
        row.try_get("latest_recoverability")?,
    )?;
    projection.standing_goal_status = row
        .try_get::<Option<i16>, _>("standing_goal_status")?
        .map(decode_standing_goal_status)
        .transpose()?;
    projection.pursuit_status = decode_optional_pursuit_status(
        row.try_get("pursuit_status")?,
        row.try_get("pursuit_wait_reason")?,
    )?;
    restore_latest_attention(connection, &mut projection).await?;
    restore_executions(connection, &mut projection).await?;
    restore_actions(connection, &mut projection).await?;
    Ok(projection)
}

async fn restore_latest_attention(
    connection: &mut PgConnection,
    projection: &mut ConversationCatalogProjection,
) -> Result<(), PostgresConversationCatalogError> {
    let row = sqlx::query(
        "SELECT attention_revision::text AS attention_revision,
                activity_position::text AS activity_position, attention_kind,
                source_kind, source_identity_kind, source_identity,
                semantic_revision::text AS semantic_revision, outcome_kind,
                error_class, recoverability, occurred_at_ms, committed_at_ms
         FROM projection.conversation_catalog_attention
         WHERE tenant_id = $1 AND conversation_id = $2
         ORDER BY attention_revision DESC LIMIT 1",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else { return Ok(()) };
    projection.latest_attention = Some(CatalogAttentionRecord {
        revision: AttentionRevision::new(parse_u64(row.try_get("attention_revision")?)?),
        position: CatalogActivityPosition::new(parse_u64(row.try_get("activity_position")?)?),
        kind: CatalogAttentionKind::from_storage_code(row.try_get("attention_kind")?)
            .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
        source_key: CatalogSourceKey::new(
            CatalogSourceKind::from_storage_code(row.try_get("source_kind")?)
                .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            decode_source_identity(
                row.try_get("source_identity_kind")?,
                row.try_get("source_identity")?,
            )?,
            parse_u64(row.try_get("semantic_revision")?)?,
        )?,
        audience: CatalogAttentionAudience::AllParticipants,
        outcome: decode_optional_outcome(
            row.try_get("outcome_kind")?,
            row.try_get("error_class")?,
            row.try_get("recoverability")?,
        )?,
        occurred_at: UtcTimestamp::from_unix_millis(row.try_get("occurred_at_ms")?),
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
    });
    Ok(())
}

async fn restore_executions(
    connection: &mut PgConnection,
    projection: &mut ConversationCatalogProjection,
) -> Result<(), PostgresConversationCatalogError> {
    let rows = sqlx::query(
        "SELECT execution_kind, run_id, parent_run_id, delegation_id, pursuit_id,
                status, wait_reason, error_class, recoverability
         FROM projection.conversation_catalog_executions
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .fetch_all(&mut *connection)
    .await?;
    for row in rows {
        let run_id = optional_strong_id(row.try_get::<Option<Uuid>, _>("run_id")?)?;
        let identity = match row.try_get::<i16, _>("execution_kind")? {
            1 => CatalogExecutionIdentity::Root {
                run_id: run_id.ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            },
            2 => CatalogExecutionIdentity::Child {
                run_id: run_id.ok_or(PostgresConversationCatalogError::CorruptProjection)?,
                parent_run_id: required_strong_id(row.try_get("parent_run_id")?)?,
                delegation_id: required_strong_id(row.try_get("delegation_id")?)?,
            },
            3 => CatalogExecutionIdentity::Pursuit {
                pursuit_id: required_strong_id(row.try_get("pursuit_id")?)?,
                run_id,
            },
            _ => return Err(PostgresConversationCatalogError::CorruptProjection),
        };
        projection.restore_active_execution(
            identity,
            decode_execution_status(
                row.try_get("status")?,
                row.try_get("wait_reason")?,
                row.try_get("error_class")?,
                row.try_get("recoverability")?,
            )?,
        )?;
    }
    Ok(())
}

async fn restore_actions(
    connection: &mut PgConnection,
    projection: &mut ConversationCatalogProjection,
) -> Result<(), PostgresConversationCatalogError> {
    let rows = sqlx::query(
        "SELECT action_identity_kind, action_identity, action_kind
         FROM projection.conversation_catalog_actions
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .fetch_all(connection)
    .await?;
    for row in rows {
        let value: Uuid = row.try_get("action_identity")?;
        let identity = match row.try_get::<i16, _>("action_identity_kind")? {
            1 => CatalogActionIdentity::Interaction(required_strong_id(value)?),
            2 => CatalogActionIdentity::Run(required_strong_id(value)?),
            3 => CatalogActionIdentity::GoalPursuit(required_strong_id(value)?),
            4 => CatalogActionIdentity::Artifact(required_strong_id(value)?),
            5 => CatalogActionIdentity::ArtifactProposal(required_strong_id(value)?),
            _ => return Err(PostgresConversationCatalogError::CorruptProjection),
        };
        let kind = CatalogActionKind::from_storage_code(row.try_get("action_kind")?)
            .ok_or(PostgresConversationCatalogError::CorruptProjection)?;
        projection.restore_open_action(identity, kind);
    }
    Ok(())
}

async fn persist_projection(
    connection: &mut PgConnection,
    projection: &ConversationCatalogProjection,
) -> Result<(), PostgresConversationCatalogError> {
    if projection.control_home != RuntimeHome::Cloud {
        return Err(PostgresConversationCatalogError::AuthorityScopeMismatch);
    }
    let (latest_attention_kind, latest_outcome_kind, latest_error, latest_recoverability) = (
        projection
            .latest_attention
            .as_ref()
            .map(|value| value.kind.storage_code()),
        projection
            .latest_outcome
            .as_ref()
            .map(|value| value.kind.storage_code()),
        projection
            .latest_outcome
            .as_ref()
            .and_then(|value| value.error_class.as_ref())
            .map(DottedKey::as_str),
        projection
            .latest_outcome
            .as_ref()
            .and_then(|value| value.recoverability)
            .map(CatalogRecoverability::storage_code),
    );
    let (pursuit_status, pursuit_wait) = encode_pursuit_status(projection.pursuit_status);
    sqlx::query(
        "INSERT INTO projection.conversation_catalog(
             conversation_id, tenant_id, control_home_kind, control_home_device_id,
             home_epoch, lifecycle, catalog_revision, attention_revision,
             latest_commit_position, last_activity_position,
             last_user_visible_activity_at_ms, metadata_updated_at_ms,
             latest_attention_kind, latest_outcome_kind, latest_error_class,
             latest_recoverability, standing_goal_status, pursuit_status,
             pursuit_wait_reason
         ) VALUES($1, $2, 1, NULL, $3::numeric, $4, $5::numeric, $6::numeric,
                  $7::numeric, $8::numeric, $9, $10, $11, $12, $13, $14, $15, $16, $17)
         ON CONFLICT(conversation_id) DO UPDATE SET
             tenant_id = excluded.tenant_id, home_epoch = excluded.home_epoch,
             lifecycle = excluded.lifecycle, catalog_revision = excluded.catalog_revision,
             attention_revision = excluded.attention_revision,
             latest_commit_position = excluded.latest_commit_position,
             last_activity_position = excluded.last_activity_position,
             last_user_visible_activity_at_ms = excluded.last_user_visible_activity_at_ms,
             metadata_updated_at_ms = excluded.metadata_updated_at_ms,
             latest_attention_kind = excluded.latest_attention_kind,
             latest_outcome_kind = excluded.latest_outcome_kind,
             latest_error_class = excluded.latest_error_class,
             latest_recoverability = excluded.latest_recoverability,
             standing_goal_status = excluded.standing_goal_status,
             pursuit_status = excluded.pursuit_status,
             pursuit_wait_reason = excluded.pursuit_wait_reason",
    )
    .bind(projection.conversation_id.into_uuid())
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.home_epoch.get().to_string())
    .bind(encode_lifecycle(projection.lifecycle))
    .bind(projection.catalog_revision.to_string())
    .bind(projection.attention_revision.get().to_string())
    .bind(projection.latest_commit_position.get().to_string())
    .bind(projection.last_activity_position.get().to_string())
    .bind(projection.last_user_visible_activity_at.unix_millis())
    .bind(projection.metadata_updated_at.unix_millis())
    .bind(latest_attention_kind)
    .bind(latest_outcome_kind)
    .bind(latest_error)
    .bind(latest_recoverability)
    .bind(
        projection
            .standing_goal_status
            .map(CatalogStandingGoalStatus::storage_code),
    )
    .bind(pursuit_status)
    .bind(pursuit_wait)
    .execute(&mut *connection)
    .await?;

    sqlx::query(
        "DELETE FROM projection.conversation_catalog_executions
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .execute(&mut *connection)
    .await?;
    for (identity, status) in projection.active_executions() {
        persist_execution(connection, projection, *identity, status).await?;
    }
    sqlx::query(
        "DELETE FROM projection.conversation_catalog_actions
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .execute(&mut *connection)
    .await?;
    for (identity, kind) in projection.open_actions() {
        sqlx::query(
            "INSERT INTO projection.conversation_catalog_actions(
                 tenant_id, conversation_id, action_identity_kind,
                 action_identity, action_kind, opened_at_ms
             ) VALUES($1, $2, $3, $4, $5, $6)",
        )
        .bind(projection.tenant_id.into_uuid())
        .bind(projection.conversation_id.into_uuid())
        .bind(identity.storage_kind_code())
        .bind(storage_action_uuid(*identity)?)
        .bind(kind.storage_code())
        .bind(projection.metadata_updated_at.unix_millis())
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

async fn persist_execution(
    connection: &mut PgConnection,
    projection: &ConversationCatalogProjection,
    identity: CatalogExecutionIdentity,
    status: &CatalogExecutionStatus,
) -> Result<(), PostgresConversationCatalogError> {
    let (execution_kind, execution_id, run_id, parent_run_id, delegation_id, pursuit_id) =
        match identity {
            CatalogExecutionIdentity::Root { run_id } => (
                1_i16,
                run_id.into_uuid(),
                Some(run_id.into_uuid()),
                None,
                None,
                None,
            ),
            CatalogExecutionIdentity::Child {
                run_id,
                parent_run_id,
                delegation_id,
            } => (
                2,
                run_id.into_uuid(),
                Some(run_id.into_uuid()),
                Some(parent_run_id.into_uuid()),
                Some(delegation_id.into_uuid()),
                None,
            ),
            CatalogExecutionIdentity::Pursuit { pursuit_id, run_id } => (
                3,
                pursuit_id.into_uuid(),
                run_id.map(|value| value.into_uuid()),
                None,
                None,
                Some(pursuit_id.into_uuid()),
            ),
        };
    let (status_code, wait_reason, error_class, recoverability) = encode_execution_status(status);
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_executions(
             tenant_id, conversation_id, execution_kind, execution_id,
             run_id, parent_run_id, delegation_id, pursuit_id, status,
             wait_reason, error_class, recoverability
         ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .bind(execution_kind)
    .bind(execution_id)
    .bind(run_id)
    .bind(parent_run_id)
    .bind(delegation_id)
    .bind(pursuit_id)
    .bind(status_code)
    .bind(wait_reason)
    .bind(error_class)
    .bind(recoverability)
    .execute(connection)
    .await?;
    Ok(())
}

async fn persist_latest_attention(
    connection: &mut PgConnection,
    projection: &ConversationCatalogProjection,
) -> Result<(), PostgresConversationCatalogError> {
    let attention = projection
        .latest_attention
        .as_ref()
        .ok_or(PostgresConversationCatalogError::CorruptProjection)?;
    let source = attention.source_key.identity;
    let (outcome_kind, error_class, recoverability) = encode_optional_outcome(&attention.outcome);
    sqlx::query(
        "INSERT INTO projection.conversation_catalog_attention(
             tenant_id, conversation_id, attention_revision, activity_position,
             attention_kind, source_kind, source_identity_kind, source_identity,
             semantic_revision, outcome_kind, error_class, recoverability,
             occurred_at_ms, committed_at_ms
         ) VALUES($1, $2, $3::numeric, $4::numeric, $5, $6, $7, $8,
                  $9::numeric, $10, $11, $12, $13, $14)",
    )
    .bind(projection.tenant_id.into_uuid())
    .bind(projection.conversation_id.into_uuid())
    .bind(attention.revision.get().to_string())
    .bind(attention.position.get().to_string())
    .bind(attention.kind.storage_code())
    .bind(attention.source_key.kind.storage_code())
    .bind(source.storage_kind_code())
    .bind(storage_uuid(source)?)
    .bind(attention.source_key.semantic_revision.to_string())
    .bind(outcome_kind)
    .bind(error_class)
    .bind(recoverability)
    .bind(attention.occurred_at.unix_millis())
    .bind(attention.committed_at.unix_millis())
    .execute(&mut *connection)
    .await?;
    for principal_id in
        resolve_attention_recipients(connection, projection, attention.audience).await?
    {
        sqlx::query(
            "INSERT INTO projection.conversation_catalog_attention_deliveries(
                 tenant_id, conversation_id, principal_id, attention_revision
             ) VALUES($1, $2, $3, $4::numeric) ON CONFLICT DO NOTHING",
        )
        .bind(projection.tenant_id.into_uuid())
        .bind(projection.conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(attention.revision.get().to_string())
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

async fn resolve_attention_recipients(
    connection: &mut PgConnection,
    projection: &ConversationCatalogProjection,
    audience: CatalogAttentionAudience,
) -> Result<Vec<PrincipalId>, PostgresConversationCatalogError> {
    match audience {
        CatalogAttentionAudience::Principal(principal_id) => Ok(vec![principal_id]),
        CatalogAttentionAudience::Participant(participant_id) => {
            let value: Option<Uuid> = sqlx::query_scalar(
                "SELECT principal_ref FROM conversation.participants
                 WHERE tenant_id = $1 AND conversation_id = $2 AND participant_id = $3
                   AND kind = 1 AND left_at_ms IS NULL",
            )
            .bind(projection.tenant_id.into_uuid())
            .bind(projection.conversation_id.into_uuid())
            .bind(participant_id.into_uuid())
            .fetch_optional(connection)
            .await?
            .flatten();
            value
                .map(required_strong_id)
                .transpose()
                .map(Option::into_iter)
                .map(Iterator::collect)
        }
        CatalogAttentionAudience::AllParticipants
        | CatalogAttentionAudience::AllExceptPrincipal(_)
        | CatalogAttentionAudience::AllExceptParticipant(_) => {
            let excluded = match audience {
                CatalogAttentionAudience::AllExceptPrincipal(value) => Some(value.into_uuid()),
                CatalogAttentionAudience::AllExceptParticipant(participant_id) => {
                    sqlx::query_scalar(
                        "SELECT principal_ref FROM conversation.participants
                         WHERE tenant_id = $1 AND conversation_id = $2 AND participant_id = $3
                           AND kind = 1 AND left_at_ms IS NULL",
                    )
                    .bind(projection.tenant_id.into_uuid())
                    .bind(projection.conversation_id.into_uuid())
                    .bind(participant_id.into_uuid())
                    .fetch_optional(&mut *connection)
                    .await?
                    .flatten()
                }
                _ => None,
            };
            let values: Vec<Uuid> = sqlx::query_scalar(
                "SELECT DISTINCT principal_ref FROM conversation.participants
                 WHERE tenant_id = $1 AND conversation_id = $2 AND kind = 1
                   AND principal_ref IS NOT NULL AND left_at_ms IS NULL
                   AND ($3::uuid IS NULL OR principal_ref <> $3)",
            )
            .bind(projection.tenant_id.into_uuid())
            .bind(projection.conversation_id.into_uuid())
            .bind(excluded)
            .fetch_all(connection)
            .await?;
            values.into_iter().map(required_strong_id).collect()
        }
    }
}

fn decode_home(
    kind: i16,
    device_id: Option<Uuid>,
) -> Result<RuntimeHome, PostgresConversationCatalogError> {
    match (kind, device_id) {
        (1, None) => Ok(RuntimeHome::Cloud),
        _ => Err(PostgresConversationCatalogError::AuthorityScopeMismatch),
    }
}

fn decode_conversation_kind(
    value: i16,
) -> Result<ConversationKind, PostgresConversationCatalogError> {
    match value {
        1 => Ok(ConversationKind::Direct),
        2 => Ok(ConversationKind::Team),
        3 => Ok(ConversationKind::Automation),
        4 => Ok(ConversationKind::System),
        _ => Err(PostgresConversationCatalogError::CorruptProjection),
    }
}

const fn encode_lifecycle(value: ConversationLifecycle) -> i16 {
    match value {
        ConversationLifecycle::Active => 1,
        ConversationLifecycle::Archived => 2,
    }
}

fn decode_lifecycle(value: i16) -> Result<ConversationLifecycle, PostgresConversationCatalogError> {
    match value {
        1 => Ok(ConversationLifecycle::Active),
        2 => Ok(ConversationLifecycle::Archived),
        _ => Err(PostgresConversationCatalogError::CorruptProjection),
    }
}

fn encode_execution_status(
    status: &CatalogExecutionStatus,
) -> (i16, Option<i16>, Option<&str>, Option<i16>) {
    match status {
        CatalogExecutionStatus::Queued => (1, None, None, None),
        CatalogExecutionStatus::Running => (2, None, None, None),
        CatalogExecutionStatus::Waiting(reason) => (3, Some(reason.storage_code()), None, None),
        CatalogExecutionStatus::Cancelling => (4, None, None, None),
        CatalogExecutionStatus::Completed => (5, None, None, None),
        CatalogExecutionStatus::Failed {
            error_class,
            recoverability,
        } => (
            6,
            None,
            error_class.as_ref().map(DottedKey::as_str),
            Some(recoverability.storage_code()),
        ),
        CatalogExecutionStatus::Cancelled => (7, None, None, None),
        CatalogExecutionStatus::Interrupted {
            error_class,
            recoverability,
        } => (
            8,
            None,
            error_class.as_ref().map(DottedKey::as_str),
            Some(recoverability.storage_code()),
        ),
    }
}

fn decode_execution_status(
    status: i16,
    wait_reason: Option<i16>,
    error_class: Option<String>,
    recoverability: Option<i16>,
) -> Result<CatalogExecutionStatus, PostgresConversationCatalogError> {
    match status {
        1 => Ok(CatalogExecutionStatus::Queued),
        2 => Ok(CatalogExecutionStatus::Running),
        3 => Ok(CatalogExecutionStatus::Waiting(
            CatalogRunWaitReason::from_storage_code(
                wait_reason.ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            )
            .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
        )),
        4 => Ok(CatalogExecutionStatus::Cancelling),
        5 => Ok(CatalogExecutionStatus::Completed),
        6 => Ok(CatalogExecutionStatus::Failed {
            error_class: optional_dotted(error_class)?,
            recoverability: decode_recoverability(recoverability)?,
        }),
        7 => Ok(CatalogExecutionStatus::Cancelled),
        8 => Ok(CatalogExecutionStatus::Interrupted {
            error_class: optional_dotted(error_class)?,
            recoverability: decode_recoverability(recoverability)?,
        }),
        _ => Err(PostgresConversationCatalogError::CorruptProjection),
    }
}

fn encode_pursuit_status(status: Option<CatalogGoalPursuitStatus>) -> (Option<i16>, Option<i16>) {
    match status {
        None => (None, None),
        Some(CatalogGoalPursuitStatus::Pending) => (Some(1), None),
        Some(CatalogGoalPursuitStatus::Running) => (Some(2), None),
        Some(CatalogGoalPursuitStatus::Waiting(reason)) => (Some(3), Some(reason.storage_code())),
        Some(CatalogGoalPursuitStatus::Paused) => (Some(4), None),
        Some(CatalogGoalPursuitStatus::Achieved) => (Some(5), None),
        Some(CatalogGoalPursuitStatus::Failed) => (Some(6), None),
        Some(CatalogGoalPursuitStatus::Cancelled) => (Some(7), None),
    }
}

fn decode_optional_pursuit_status(
    status: Option<i16>,
    wait_reason: Option<i16>,
) -> Result<Option<CatalogGoalPursuitStatus>, PostgresConversationCatalogError> {
    status
        .map(|value| match value {
            1 => Ok(CatalogGoalPursuitStatus::Pending),
            2 => Ok(CatalogGoalPursuitStatus::Running),
            3 => Ok(CatalogGoalPursuitStatus::Waiting(
                CatalogRunWaitReason::from_storage_code(
                    wait_reason.ok_or(PostgresConversationCatalogError::CorruptProjection)?,
                )
                .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            )),
            4 => Ok(CatalogGoalPursuitStatus::Paused),
            5 => Ok(CatalogGoalPursuitStatus::Achieved),
            6 => Ok(CatalogGoalPursuitStatus::Failed),
            7 => Ok(CatalogGoalPursuitStatus::Cancelled),
            _ => Err(PostgresConversationCatalogError::CorruptProjection),
        })
        .transpose()
}

fn decode_standing_goal_status(
    value: i16,
) -> Result<CatalogStandingGoalStatus, PostgresConversationCatalogError> {
    CatalogStandingGoalStatus::from_storage_code(value)
        .ok_or(PostgresConversationCatalogError::CorruptProjection)
}

fn encode_optional_outcome(
    outcome: &Option<ExecutionOutcome>,
) -> (Option<i16>, Option<&str>, Option<i16>) {
    outcome.as_ref().map_or((None, None, None), |value| {
        (
            Some(value.kind.storage_code()),
            value.error_class.as_ref().map(DottedKey::as_str),
            value
                .recoverability
                .map(CatalogRecoverability::storage_code),
        )
    })
}

fn decode_optional_outcome(
    kind: Option<i16>,
    error_class: Option<String>,
    recoverability: Option<i16>,
) -> Result<Option<ExecutionOutcome>, PostgresConversationCatalogError> {
    kind.map(|value| {
        Ok(ExecutionOutcome {
            kind: ExecutionOutcomeKind::from_storage_code(value)
                .ok_or(PostgresConversationCatalogError::CorruptProjection)?,
            error_class: optional_dotted(error_class)?,
            recoverability: recoverability
                .map(|item| {
                    CatalogRecoverability::from_storage_code(item)
                        .ok_or(PostgresConversationCatalogError::CorruptProjection)
                })
                .transpose()?,
        })
    })
    .transpose()
}

fn decode_recoverability(
    value: Option<i16>,
) -> Result<CatalogRecoverability, PostgresConversationCatalogError> {
    CatalogRecoverability::from_storage_code(
        value.ok_or(PostgresConversationCatalogError::CorruptProjection)?,
    )
    .ok_or(PostgresConversationCatalogError::CorruptProjection)
}

fn decode_source_identity(
    kind: i16,
    value: Uuid,
) -> Result<CatalogSourceIdentity, PostgresConversationCatalogError> {
    match kind {
        1 => Ok(CatalogSourceIdentity::Conversation(required_strong_id(
            value,
        )?)),
        2 => Ok(CatalogSourceIdentity::Message(required_strong_id(value)?)),
        3 => Ok(CatalogSourceIdentity::Run(required_strong_id(value)?)),
        4 => Ok(CatalogSourceIdentity::Interaction(required_strong_id(
            value,
        )?)),
        5 => Ok(CatalogSourceIdentity::Delegation(required_strong_id(
            value,
        )?)),
        6 => Ok(CatalogSourceIdentity::Goal(required_strong_id(value)?)),
        7 => Ok(CatalogSourceIdentity::GoalPursuit(required_strong_id(
            value,
        )?)),
        8 => Ok(CatalogSourceIdentity::GoalEvaluation(required_strong_id(
            value,
        )?)),
        9 => Ok(CatalogSourceIdentity::Artifact(required_strong_id(value)?)),
        10 => Ok(CatalogSourceIdentity::ArtifactProposal(required_strong_id(
            value,
        )?)),
        11 => Ok(CatalogSourceIdentity::ArtifactVersion(required_strong_id(
            value,
        )?)),
        12 => Ok(CatalogSourceIdentity::InterSessionMessage(
            required_strong_id(value)?,
        )),
        14 => Ok(CatalogSourceIdentity::LedgerRecord(required_strong_id(
            value,
        )?)),
        _ => Err(PostgresConversationCatalogError::CorruptProjection),
    }
}

#[cfg(test)]
#[path = "conversation_catalog/tests.rs"]
mod tests;
