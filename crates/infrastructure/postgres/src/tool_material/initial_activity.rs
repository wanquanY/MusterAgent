//! Verify canonical admission events once; consumers separately select template
//! presentation or derive source authority from the authenticated input owner.
use super::{
    ActivityId, ConversationId, DomainEvent, MaterialError, PostgresToolExecutionMaterialSource,
    Sha256, TenantId,
};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::LedgerRecordId;
use musteragent_ledger::LedgerPayload;
use sqlx::Row;

impl PostgresToolExecutionMaterialSource {
    pub(super) async fn load_initial_activity_events(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        activity_id: ActivityId,
    ) -> Result<Vec<(LedgerRecordId, DomainEvent)>, MaterialError> {
        const DOMAIN_EVENT_RECORD_KIND: i16 = 1;
        let rows = sqlx::query(
            "SELECT record.record_id, record.record_kind,
                    record.payload_hash AS record_payload_hash,
                    event.payload_hash AS event_payload_hash,
                    event.encoded_event
             FROM event_store.ledger_records AS record
             LEFT JOIN event_store.domain_events AS event
               ON event.record_id = record.record_id
             WHERE record.tenant_id = $1
               AND record.activity_id = $2
               AND record.run_id IS NULL
               AND record.record_kind = $3
               AND record.conversation_id = $4
             ORDER BY record.conversation_seq",
        )
        .bind(tenant_id.into_uuid())
        .bind(activity_id.into_uuid())
        .bind(DOMAIN_EVENT_RECORD_KIND)
        .bind(conversation_id.into_uuid())
        .fetch_all(&self.pool)
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if row.try_get::<i16, _>("record_kind")? != DOMAIN_EVENT_RECORD_KIND {
                return Err(MaterialError::Corrupt);
            }
            let encoded = row
                .try_get::<Option<Vec<u8>>, _>("encoded_event")?
                .ok_or(MaterialError::Corrupt)?;
            let record_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("record_payload_hash")?)
                .map_err(|_| MaterialError::Corrupt)?;
            let event_hash = Sha256::from_bytes(
                row.try_get::<Option<Vec<u8>>, _>("event_payload_hash")?
                    .ok_or(MaterialError::Corrupt)?,
            )
            .map_err(|_| MaterialError::Corrupt)?;
            if record_hash != event_hash || Sha256::digest(&encoded) != record_hash {
                return Err(MaterialError::Corrupt);
            }
            let event = match self
                .ledger_decoder
                .decode(LedgerRecordKind::DomainEvent, &encoded)
                .map_err(|_| MaterialError::Corrupt)?
            {
                LedgerPayload::DomainEvent(event) => event,
                LedgerPayload::OutputFrame(_) => return Err(MaterialError::Corrupt),
            };
            let record_id =
                musteragent_identity::LedgerRecordId::from_uuid(row.try_get("record_id")?)
                    .map_err(|_| MaterialError::Corrupt)?;
            events.push((record_id, event));
        }
        Ok(events)
    }
}
