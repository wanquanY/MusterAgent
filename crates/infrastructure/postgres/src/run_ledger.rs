use musteragent_content_ref::Sha256;
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{LedgerRecordId, RunId, TenantId};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use sqlx::{PgConnection, PgPool, Row};

const DOMAIN_EVENT_RECORD_KIND: i16 = 1;

/// One hash-verified, typed domain event from the canonical Run Ledger.
pub(crate) struct CanonicalRunDomainEvent {
    pub(crate) record_id: LedgerRecordId,
    pub(crate) event: DomainEvent,
}

/// Loads the canonical domain-event stream for one Run in Conversation order.
///
/// Both the parent Ledger hash and the domain-event child hash are verified
/// before transport decoding. Persistence projections must derive authority
/// from this reader instead of independently trusting denormalized rows.
pub(crate) async fn load_run_domain_events(
    pool: &PgPool,
    decoder: &dyn LedgerPayloadDecoder,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<Vec<CanonicalRunDomainEvent>, RunLedgerReadError> {
    let mut connection = pool.acquire().await?;
    load_run_domain_events_in(&mut connection, decoder, tenant_id, run_id).await
}

pub(crate) async fn load_run_domain_events_in(
    connection: &mut PgConnection,
    decoder: &dyn LedgerPayloadDecoder,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<Vec<CanonicalRunDomainEvent>, RunLedgerReadError> {
    let rows = sqlx::query(
        "SELECT record.record_id, record.record_kind,
                record.payload_hash AS record_payload_hash,
                event.payload_hash AS event_payload_hash,
                event.encoded_event
         FROM event_store.ledger_records AS record
         LEFT JOIN event_store.domain_events AS event
           ON event.record_id = record.record_id
         WHERE record.tenant_id = $1 AND record.run_id = $2
           AND record.record_kind = $3
         ORDER BY record.conversation_seq",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .bind(DOMAIN_EVENT_RECORD_KIND)
    .fetch_all(connection)
    .await?;

    rows.into_iter()
        .map(|row| {
            if row.try_get::<i16, _>("record_kind")? != DOMAIN_EVENT_RECORD_KIND {
                return Err(RunLedgerReadError::Corrupt);
            }
            let encoded = row
                .try_get::<Option<Vec<u8>>, _>("encoded_event")?
                .ok_or(RunLedgerReadError::Corrupt)?;
            let record_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("record_payload_hash")?)
                .map_err(|_| RunLedgerReadError::Corrupt)?;
            let event_hash = Sha256::from_bytes(
                row.try_get::<Option<Vec<u8>>, _>("event_payload_hash")?
                    .ok_or(RunLedgerReadError::Corrupt)?,
            )
            .map_err(|_| RunLedgerReadError::Corrupt)?;
            if record_hash != event_hash || Sha256::digest(&encoded) != record_hash {
                return Err(RunLedgerReadError::Corrupt);
            }
            let record_id = LedgerRecordId::from_uuid(row.try_get("record_id")?)
                .map_err(|_| RunLedgerReadError::Corrupt)?;
            let event = match decoder
                .decode(LedgerRecordKind::DomainEvent, &encoded)
                .map_err(|_| RunLedgerReadError::Corrupt)?
            {
                LedgerPayload::DomainEvent(event) => event,
                LedgerPayload::OutputFrame(_) => return Err(RunLedgerReadError::Corrupt),
            };
            Ok(CanonicalRunDomainEvent { record_id, event })
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RunLedgerReadError {
    #[error("Run Ledger persistence is corrupt")]
    Corrupt,
    #[error("Run Ledger query failed")]
    Database(#[from] sqlx::Error),
}
