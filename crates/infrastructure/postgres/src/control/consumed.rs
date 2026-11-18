//! Exact source proof from the original consumed-control transaction.
use musteragent_content_ref::Sha256;
use musteragent_context::ContextPlanControlCut;
use musteragent_identity::{ConversationSeq, LedgerRecordId, RunId, TenantId};
use musteragent_persistence_codec::wire;
use musteragent_run::RunControlKind;
use prost::Message;
use sqlx::{PgConnection, Row};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConsumedControlError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("invalid consumed control source")]
    Corrupt,
}
pub(crate) async fn latest(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    cutoff: ConversationSeq,
) -> Result<Option<ContextPlanControlCut>, ConsumedControlError> {
    // This immutable append key is authored only by the original control
    // consumption transaction. The partial index avoids scanning Run history;
    // the hash and actual event still establish authority, not the key string.
    let row = sqlx::query(
        "SELECT r.record_id,r.conversation_seq::bigint AS sequence,r.payload_hash,e.encoded_event,
                r.attempt_id,r.conversation_id,r.idempotency_key
         FROM event_store.ledger_records r JOIN event_store.domain_events e USING(record_id)
         WHERE r.tenant_id=$1 AND r.run_id=$2 AND r.record_kind=1
           AND r.idempotency_key LIKE 'loop:control-consume:%' AND r.conversation_seq<=$3
         ORDER BY r.conversation_seq DESC LIMIT 1",
    )
    .bind(tenant.into_uuid())
    .bind(run.into_uuid())
    .bind(i64::try_from(cutoff.get()).map_err(|_| ConsumedControlError::Corrupt)?)
    .fetch_optional(&mut *conn)
    .await?;
    let control = if let Some(row) = row {
        let bytes: Vec<u8> = row.try_get("encoded_event")?;
        if row.try_get::<Vec<u8>, _>("payload_hash")? != Sha256::digest(&bytes).into_bytes() {
            return Err(ConsumedControlError::Corrupt);
        }
        let event = wire::DomainEvent::decode(bytes.as_slice())
            .map_err(|_| ConsumedControlError::Corrupt)?;
        match event.event {
            Some(wire::domain_event::Event::RunControlConsumed(value))
                if matches!(value.kind, 1 | 2) && !value.message_ids.is_empty() =>
            {
                verify_consumed_control(conn, tenant, run, &row, &value).await?;
                Some(ContextPlanControlCut {
                    sequence: sequence(row.try_get("sequence")?)?,
                    record_id: LedgerRecordId::from_uuid(row.try_get("record_id")?)
                        .map_err(|_| ConsumedControlError::Corrupt)?,
                    kind: match value.kind {
                        1 => RunControlKind::Steering,
                        2 => RunControlKind::FollowUp,
                        _ => return Err(ConsumedControlError::Corrupt),
                    },
                })
            }
            _ => return Err(ConsumedControlError::Corrupt),
        }
    } else {
        None
    };
    Ok(control)
}
// The indexed record is only a locator. Prove its exact batch against the
// original consumed controls and their canonical source records, bounded to
// the original maximum batch size. No caller-provided LIKE pattern is used.
async fn verify_consumed_control(
    conn: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
    row: &sqlx::postgres::PgRow,
    event: &wire::RunControlConsumed,
) -> Result<(), ConsumedControlError> {
    if event.message_ids.len() > 256 {
        return Err(ConsumedControlError::Corrupt);
    }
    let attempt: uuid::Uuid = row.try_get("attempt_id")?;
    let conversation: uuid::Uuid = row.try_get("conversation_id")?;
    let key: String = row.try_get("idempotency_key")?;
    let prefix = format!("loop:control-consume:{run}:{attempt}:{}:", event.kind);
    let range = key
        .strip_prefix(&prefix)
        .ok_or(ConsumedControlError::Corrupt)?;
    let (first, last) = range.split_once(':').ok_or(ConsumedControlError::Corrupt)?;
    let first = first
        .parse::<i64>()
        .map_err(|_| ConsumedControlError::Corrupt)?;
    let last = last
        .parse::<i64>()
        .map_err(|_| ConsumedControlError::Corrupt)?;
    let consumed: i64 = row.try_get("sequence")?;
    if first < 1 || last < first || last >= consumed || key != format!("{prefix}{first}:{last}") {
        return Err(ConsumedControlError::Corrupt);
    }
    let message_ids = event
        .message_ids
        .iter()
        .map(|id| uuid::Uuid::parse_str(&id.value).map_err(|_| ConsumedControlError::Corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    let sources = sqlx::query(
        "SELECT c.message_id,c.source_conversation_seq::bigint AS sequence
         FROM runtime.loop_controls c
         JOIN runtime.attempts a ON a.attempt_id=c.consumed_attempt_id
           AND a.tenant_id=c.tenant_id AND a.run_id=c.run_id
         JOIN event_store.ledger_records source ON source.record_id=c.source_record_id
           AND source.tenant_id=c.tenant_id AND source.conversation_id=c.conversation_id
           AND source.conversation_seq=c.source_conversation_seq
         WHERE c.tenant_id=$1 AND c.run_id=$2 AND c.conversation_id=$3
           AND c.consumed_attempt_id=$4 AND c.kind=$5 AND c.status=2
           AND c.source_conversation_seq BETWEEN $6 AND $7 AND c.message_id=ANY($8)
         ORDER BY c.source_conversation_seq,c.control_id LIMIT 257",
    )
    .bind(tenant.into_uuid())
    .bind(run.into_uuid())
    .bind(conversation)
    .bind(attempt)
    .bind(i16::try_from(event.kind).map_err(|_| ConsumedControlError::Corrupt)?)
    .bind(first)
    .bind(last)
    .bind(message_ids)
    .fetch_all(&mut *conn)
    .await?;
    if sources.len() != event.message_ids.len()
        || sources.is_empty()
        || sources
            .first()
            .ok_or(ConsumedControlError::Corrupt)?
            .try_get::<i64, _>("sequence")?
            != first
        || sources
            .last()
            .ok_or(ConsumedControlError::Corrupt)?
            .try_get::<i64, _>("sequence")?
            != last
    {
        return Err(ConsumedControlError::Corrupt);
    }
    for (source, message) in sources.iter().zip(&event.message_ids) {
        if source.try_get::<uuid::Uuid, _>("message_id")?.to_string() != message.value {
            return Err(ConsumedControlError::Corrupt);
        }
    }
    Ok(())
}

fn sequence(value: i64) -> Result<ConversationSeq, ConsumedControlError> {
    ConversationSeq::new(u64::try_from(value).map_err(|_| ConsumedControlError::Corrupt)?)
        .map_err(|_| ConsumedControlError::Corrupt)
}
