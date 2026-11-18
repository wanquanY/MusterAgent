use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, CorrelationId, LedgerRecordId, ParticipantId, RunId, RunSeq, UtcTimestamp,
};
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder, ProposedRecord};
use musteragent_runtime_contract::ErrorCode;

pub(crate) struct RecordIdentity {
    pub record_id: LedgerRecordId,
    pub activity_id: Option<ActivityId>,
    pub run_id: Option<RunId>,
    pub run_seq: Option<RunSeq>,
    pub participant_id: Option<ParticipantId>,
}

pub(crate) fn domain_record(
    encoder: &dyn LedgerPayloadEncoder,
    identity: RecordIdentity,
    event: DomainEvent,
    occurred_at: UtcTimestamp,
    correlation_id: CorrelationId,
) -> Result<ProposedRecord, ErrorCode> {
    let payload = encoder.encode(LedgerPayload::DomainEvent(event));
    let schema_version = musteragent_identity::DottedKey::new("musteragent.runtime.record.v1")
        .map_err(|_| ErrorCode::InternalInvariantViolated)?;
    Ok(ProposedRecord {
        schema_version,
        record_id: identity.record_id,
        record_kind: LedgerRecordKind::DomainEvent,
        activity_id: identity.activity_id,
        run_id: identity.run_id,
        run_seq: identity.run_seq,
        attempt_id: None,
        participant_id: identity.participant_id,
        producer: Producer::RuntimeApi,
        occurred_at,
        causation_id: None,
        correlation_id,
        payload_hash: Sha256::digest(&payload),
        payload,
    })
}
