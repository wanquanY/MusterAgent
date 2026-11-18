//! Real application commits used by both storage conformance suites.
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::*;
use musteragent_ledger::{
    AppendRequest, DomainEvent, LedgerPayload, ProposedOutbox, ProposedRecord,
};
use musteragent_run::{ResumeRun, RunEvent, RunRecoveryAction};
use musteragent_runtime_session::{RunControlAuthority, RunResumeCommit};
use musteragent_wire::ProtobufLedgerPayloadCodec;

pub fn resume_commit(
    authority: RunControlAuthority,
    actor: Actor,
    attempt: AttemptNumber,
    action: RunRecoveryAction,
) -> RunResumeCommit {
    let payload = ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::RecoveryRequested {
            expected_attempt_number: attempt,
            action,
            requested_by: actor.principal_id,
        },
    )));
    let record_id = LedgerRecordId::generate();
    let dispatch = format!("run:{}", authority.run_id).into_bytes();
    RunResumeCommit {
        actor,
        activity_id: authority.activity_id,
        expected_last_run_sequence: authority.last_run_sequence,
        request: ResumeRun {
            run_id: authority.run_id,
            participant_id: authority.requested_by_participant_id,
            expected_attempt_number: attempt,
            action,
        },
        append: AppendRequest {
            tenant_id: authority.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: authority.current_offset,
            idempotency_key: IdempotencyKey::new(format!("repair:{record_id}")).expect("key"),
            command_fingerprint: Sha256::digest(&payload),
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(authority.activity_id),
                run_id: Some(authority.run_id),
                run_seq: Some(RunSeq::new(authority.last_run_sequence.get() + 1).expect("seq")),
                attempt_id: None,
                participant_id: Some(authority.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: UtcTimestamp::from_unix_millis(1),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new("run.dispatch").expect("topic"),
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        },
    }
}
