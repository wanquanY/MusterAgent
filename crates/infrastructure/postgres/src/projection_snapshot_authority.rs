use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, ContextBranchId, ConversationId, HomeEpoch, LedgerOffset, ProjectionSnapshotId, TenantId,
};
use musteragent_projection::{
    ConversationProjectionSnapshot, ConversationProjectionSnapshotCodec, GetConversationProjection,
    MAX_PUBLIC_PROJECTION_TAIL_RECORDS, SubscriptionError,
};
use sqlx::{PgConnection, PgPool, Row};

use crate::projection_branch::ProjectionBranchRoute;

pub(super) struct CapturedProjectionAuthority {
    pub(super) stream: StreamHead,
    pub(super) branch_route: ProjectionBranchRoute,
    pub(super) snapshot_kind: String,
    pub(super) selected_snapshot: Option<ConversationProjectionSnapshot>,
    pub(super) selected_snapshot_is_legacy: bool,
}

pub(super) struct StreamHead {
    pub(super) home_epoch: HomeEpoch,
    pub(super) head: LedgerOffset,
    pub(super) retained_from: u64,
    active_branch_id: Option<ContextBranchId>,
}

/// Captures every mutable authority input under one PostgreSQL MVCC snapshot.
/// A Snapshot committed after this transaction starts cannot be compared
/// against an older captured Ledger head and misclassified as a future
/// Snapshot.
pub(super) async fn capture(
    database: &crate::PostgresDatabaseRuntime,
    codec: &dyn ConversationProjectionSnapshotCodec,
    query: GetConversationProjection,
) -> Result<CapturedProjectionAuthority, SubscriptionError> {
    let mut transaction = database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Projection,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
    let stream = authorized_stream(&mut transaction, query.actor, query.conversation_id).await?;
    let branch_route = ProjectionBranchRoute::load(
        &mut transaction,
        query.actor.tenant_id,
        query.conversation_id,
        stream.head,
        stream.active_branch_id,
    )
    .await?;
    let snapshot_kind = branch_route.snapshot_kind();
    let supported_snapshot_kinds = branch_route.supported_snapshot_kinds();
    let (selected_snapshot, selected_snapshot_is_legacy) =
        if let Some(snapshot_id) = query.preferred_snapshot_id {
            let (snapshot, is_legacy) = load_snapshot_by_id(
                &mut transaction,
                codec,
                SnapshotLookup {
                    tenant_id: query.actor.tenant_id,
                    conversation_id: query.conversation_id,
                    home_epoch: stream.home_epoch,
                    snapshot_id,
                    snapshot_kind: &snapshot_kind,
                    supported_snapshot_kinds: &supported_snapshot_kinds,
                },
            )
            .await?;
            (Some(snapshot), is_legacy)
        } else {
            let current = load_latest_snapshot(
                &mut transaction,
                codec,
                query.actor.tenant_id,
                query.conversation_id,
                stream.home_epoch,
                &snapshot_kind,
            )
            .await?;
            if current.is_some() {
                (current, false)
            } else {
                let legacy = load_latest_supported_snapshot(
                    &mut transaction,
                    codec,
                    query.actor.tenant_id,
                    query.conversation_id,
                    stream.home_epoch,
                    &supported_snapshot_kinds[1..],
                )
                .await?;
                let is_legacy = legacy.is_some();
                (legacy, is_legacy)
            }
        };
    transaction
        .commit()
        .await
        .map_err(|_| SubscriptionError::SourceUnavailable)?;
    Ok(CapturedProjectionAuthority {
        stream,
        branch_route,
        snapshot_kind,
        selected_snapshot,
        selected_snapshot_is_legacy,
    })
}

/// Applies the public Projection GET invariant to a selected Snapshot.
///
/// An unbranched Conversation may expose a bounded canonical Ledger tail. A
/// branched Conversation may not: its raw tail can include sibling facts, so
/// only a Snapshot reduced through the active lineage at the captured head is
/// safe to advertise as recovery authority.
pub(super) fn validate_public_snapshot_tail(
    snapshot: &ConversationProjectionSnapshot,
    stream: &StreamHead,
    branch_route: &ProjectionBranchRoute,
) -> Result<u64, SubscriptionError> {
    let tail = validate_snapshot_position(snapshot, stream)?;
    if tail > MAX_PUBLIC_PROJECTION_TAIL_RECORDS || (!branch_route.is_unbranched() && tail != 0) {
        return Err(SubscriptionError::CorruptRecord);
    }
    Ok(tail)
}

pub(super) fn validate_snapshot_position(
    snapshot: &ConversationProjectionSnapshot,
    stream: &StreamHead,
) -> Result<u64, SubscriptionError> {
    let through = snapshot.through_conversation_seq.get();
    let tail = stream
        .head
        .get()
        .checked_sub(through)
        .ok_or(SubscriptionError::CorruptRecord)?;
    if through
        .checked_add(1)
        .ok_or(SubscriptionError::CorruptRecord)?
        < stream.retained_from
    {
        return Err(SubscriptionError::CorruptRecord);
    }
    Ok(tail)
}

async fn authorized_stream(
    connection: &mut PgConnection,
    actor: Actor,
    conversation_id: ConversationId,
) -> Result<StreamHead, SubscriptionError> {
    let row = sqlx::query(
        "SELECT stream.tenant_id,
                stream.home_epoch::text AS home_epoch,
                stream.last_sequence::text AS last_sequence,
                stream.retained_from_sequence::text AS retained_from_sequence,
                branch_state.active_branch_id,
                EXISTS (
                    SELECT 1
                    FROM conversation.participants AS participant
                    WHERE stream.tenant_id = $1
                      AND participant.tenant_id = stream.tenant_id
                      AND participant.conversation_id = stream.conversation_id
                      AND participant.principal_ref = $3
                      AND participant.left_at_ms IS NULL
                ) AS authorized
         FROM event_store.ledger_streams AS stream
         JOIN conversation.conversations AS c ON c.tenant_id=stream.tenant_id AND c.conversation_id=stream.conversation_id AND c.purpose='standard'
         LEFT JOIN conversation.context_branch_state AS branch_state
           ON branch_state.tenant_id = stream.tenant_id
          AND branch_state.conversation_id = stream.conversation_id
         WHERE stream.conversation_id = $2",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(connection)
    .await
    .map_err(|_| SubscriptionError::SourceUnavailable)?
    .ok_or(SubscriptionError::SourceUnavailable)?;
    let row_tenant_id = TenantId::from_uuid(
        row.try_get("tenant_id")
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    )
    .map_err(|_| SubscriptionError::CorruptRecord)?;
    if row_tenant_id != actor.tenant_id
        || !row
            .try_get::<bool, _>("authorized")
            .map_err(|_| SubscriptionError::CorruptRecord)?
    {
        return Err(SubscriptionError::Unauthorized);
    }
    Ok(StreamHead {
        home_epoch: HomeEpoch::new(parse_u64(&row, "home_epoch")?)
            .map_err(|_| SubscriptionError::CorruptRecord)?,
        head: LedgerOffset::new(parse_u64(&row, "last_sequence")?),
        retained_from: parse_u64(&row, "retained_from_sequence")?,
        active_branch_id: row
            .try_get::<Option<uuid::Uuid>, _>("active_branch_id")
            .map_err(|_| SubscriptionError::CorruptRecord)?
            .map(ContextBranchId::from_uuid)
            .transpose()
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    })
}

async fn load_latest_snapshot(
    connection: &mut PgConnection,
    codec: &dyn ConversationProjectionSnapshotCodec,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    snapshot_kind: &str,
) -> Result<Option<ConversationProjectionSnapshot>, SubscriptionError> {
    let row = sqlx::query(
        "SELECT snapshot.snapshot_id, snapshot.tenant_id, snapshot.conversation_id,
                snapshot.home_epoch::text AS row_home_epoch,
                snapshot.source_sequence_from::text AS source_sequence_from,
                snapshot.source_sequence_to::text AS source_sequence_to,
                snapshot.snapshot_kind, snapshot.encoded_snapshot, snapshot.content_hash
         FROM event_store.snapshots AS snapshot
         WHERE snapshot.tenant_id = $1
           AND snapshot.conversation_id = $2
           AND snapshot.home_epoch = $3::bigint
           AND snapshot.snapshot_kind = $4
         ORDER BY snapshot.source_sequence_to DESC, snapshot.created_at_ms DESC
         LIMIT 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(home_epoch.get().to_string())
    .bind(snapshot_kind)
    .fetch_optional(connection)
    .await
    .map_err(|_| SubscriptionError::SourceUnavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    decode_snapshot_row(
        codec,
        row,
        tenant_id,
        conversation_id,
        home_epoch,
        snapshot_kind,
        None,
    )
    .map(Some)
}

async fn load_latest_supported_snapshot(
    connection: &mut PgConnection,
    codec: &dyn ConversationProjectionSnapshotCodec,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    snapshot_kinds: &[String],
) -> Result<Option<ConversationProjectionSnapshot>, SubscriptionError> {
    if snapshot_kinds.is_empty() {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT snapshot.snapshot_id, snapshot.tenant_id, snapshot.conversation_id,
                snapshot.home_epoch::text AS row_home_epoch,
                snapshot.source_sequence_from::text AS source_sequence_from,
                snapshot.source_sequence_to::text AS source_sequence_to,
                snapshot.snapshot_kind, snapshot.encoded_snapshot, snapshot.content_hash
         FROM event_store.snapshots AS snapshot
         WHERE snapshot.tenant_id = $1
           AND snapshot.conversation_id = $2
           AND snapshot.home_epoch = $3::bigint
           AND snapshot.snapshot_kind = ANY($4::text[])
         ORDER BY snapshot.source_sequence_to DESC, snapshot.created_at_ms DESC
         LIMIT 1",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(home_epoch.get().to_string())
    .bind(snapshot_kinds)
    .fetch_optional(connection)
    .await
    .map_err(|_| SubscriptionError::SourceUnavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let expected_snapshot_kind = row
        .try_get::<String, _>("snapshot_kind")
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    decode_snapshot_row(
        codec,
        row,
        tenant_id,
        conversation_id,
        home_epoch,
        &expected_snapshot_kind,
        None,
    )
    .map(Some)
}

struct SnapshotLookup<'a> {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    snapshot_id: ProjectionSnapshotId,
    snapshot_kind: &'a str,
    supported_snapshot_kinds: &'a [String],
}

async fn load_snapshot_by_id(
    connection: &mut PgConnection,
    codec: &dyn ConversationProjectionSnapshotCodec,
    lookup: SnapshotLookup<'_>,
) -> Result<(ConversationProjectionSnapshot, bool), SubscriptionError> {
    let row = sqlx::query(
        "SELECT snapshot_id, tenant_id, conversation_id,
                home_epoch::text AS row_home_epoch,
                source_sequence_from::text AS source_sequence_from,
                source_sequence_to::text AS source_sequence_to,
                snapshot_kind, encoded_snapshot, content_hash
         FROM event_store.snapshots
         WHERE tenant_id = $1
           AND conversation_id = $2
           AND home_epoch = $3::bigint
           AND snapshot_id = $4",
    )
    .bind(lookup.tenant_id.into_uuid())
    .bind(lookup.conversation_id.into_uuid())
    .bind(lookup.home_epoch.get().to_string())
    .bind(lookup.snapshot_id.into_uuid())
    .fetch_optional(connection)
    .await
    .map_err(|_| SubscriptionError::SourceUnavailable)?
    .ok_or(SubscriptionError::CorruptRecord)?;
    let expected_snapshot_kind = row
        .try_get::<String, _>("snapshot_kind")
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    if !lookup
        .supported_snapshot_kinds
        .contains(&expected_snapshot_kind)
    {
        return Err(SubscriptionError::CorruptRecord);
    }
    let snapshot = decode_snapshot_row(
        codec,
        row,
        lookup.tenant_id,
        lookup.conversation_id,
        lookup.home_epoch,
        &expected_snapshot_kind,
        Some(lookup.snapshot_id),
    )?;
    Ok((snapshot, expected_snapshot_kind != lookup.snapshot_kind))
}

pub(super) async fn load_at(
    pool: &PgPool,
    codec: &dyn ConversationProjectionSnapshotCodec,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    through: LedgerOffset,
    snapshot_kind: &str,
) -> Result<ConversationProjectionSnapshot, SubscriptionError> {
    let row = sqlx::query(
        "SELECT snapshot_id, tenant_id, conversation_id,
                home_epoch::text AS row_home_epoch,
                source_sequence_from::text AS source_sequence_from,
                source_sequence_to::text AS source_sequence_to,
                snapshot_kind, encoded_snapshot, content_hash
         FROM event_store.snapshots
         WHERE tenant_id = $1
           AND conversation_id = $2
           AND home_epoch = $3::bigint
           AND snapshot_kind = $4
           AND source_sequence_to = $5::bigint",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(home_epoch.get().to_string())
    .bind(snapshot_kind)
    .bind(through.get().to_string())
    .fetch_one(pool)
    .await
    .map_err(|_| SubscriptionError::SourceUnavailable)?;
    decode_snapshot_row(
        codec,
        row,
        tenant_id,
        conversation_id,
        home_epoch,
        snapshot_kind,
        None,
    )
}

fn decode_snapshot_row(
    codec: &dyn ConversationProjectionSnapshotCodec,
    row: sqlx::postgres::PgRow,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    expected_snapshot_kind: &str,
    expected_snapshot_id: Option<ProjectionSnapshotId>,
) -> Result<ConversationProjectionSnapshot, SubscriptionError> {
    let row_snapshot_id = ProjectionSnapshotId::from_uuid(
        row.try_get("snapshot_id")
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    )
    .map_err(|_| SubscriptionError::CorruptRecord)?;
    let row_tenant_id = TenantId::from_uuid(
        row.try_get("tenant_id")
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    )
    .map_err(|_| SubscriptionError::CorruptRecord)?;
    let row_conversation_id = ConversationId::from_uuid(
        row.try_get("conversation_id")
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    )
    .map_err(|_| SubscriptionError::CorruptRecord)?;
    let row_home_epoch = HomeEpoch::new(parse_u64(&row, "row_home_epoch")?)
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    let source_sequence_from = parse_u64(&row, "source_sequence_from")?;
    let source_sequence_to = parse_u64(&row, "source_sequence_to")?;
    let row_snapshot_kind = row
        .try_get::<String, _>("snapshot_kind")
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    let encoded: Vec<u8> = row
        .try_get("encoded_snapshot")
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    let stored_hash = Sha256::from_bytes(
        row.try_get::<Vec<u8>, _>("content_hash")
            .map_err(|_| SubscriptionError::CorruptRecord)?,
    )
    .map_err(|_| SubscriptionError::CorruptRecord)?;
    if Sha256::digest(&encoded) != stored_hash {
        return Err(SubscriptionError::CorruptRecord);
    }
    let snapshot = codec
        .decode(&encoded)
        .map_err(|_| SubscriptionError::CorruptRecord)?;
    if row_snapshot_id != snapshot.snapshot_id
        || row_tenant_id != tenant_id
        || row_conversation_id != conversation_id
        || row_home_epoch != home_epoch
        || source_sequence_from != 1
        || row_snapshot_kind != expected_snapshot_kind
        || snapshot.tenant_id != tenant_id
        || snapshot.conversation_id != conversation_id
        || snapshot.home_epoch != home_epoch
        || snapshot.through_conversation_seq.get() != source_sequence_to
        || expected_snapshot_id.is_some_and(|expected| expected != row_snapshot_id)
    {
        return Err(SubscriptionError::CorruptRecord);
    }
    Ok(snapshot)
}

fn parse_u64(row: &sqlx::postgres::PgRow, field: &str) -> Result<u64, SubscriptionError> {
    row.try_get::<String, _>(field)
        .map_err(|_| SubscriptionError::CorruptRecord)?
        .parse()
        .map_err(|_| SubscriptionError::CorruptRecord)
}
