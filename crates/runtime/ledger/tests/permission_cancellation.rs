#![cfg(test)]

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::*;
use musteragent_ledger::{AppendRequest, CancelMode, CancelRunBatch, ProposedRecord};
use musteragent_runtime_contract::CancelReason;

fn revoked_wait() -> CancelRunBatch {
    let run_id = RunId::generate();
    let activity_id = ActivityId::generate();
    let payload = b"cancellation contract fixture".to_vec();
    CancelRunBatch {
        append: AppendRequest {
            tenant_id: TenantId::generate(),
            conversation_id: ConversationId::generate(),
            expected_home_epoch: HomeEpoch::new(1).unwrap(),
            expected_offset: LedgerOffset::new(10),
            command_fingerprint: Sha256::digest(b"permission-revoked"),
            idempotency_key: IdempotencyKey::new("permission-revoked").unwrap(),
            records: (0..5)
                .map(|index| ProposedRecord {
                    schema_version: DottedKey::new("musteragent.runtime.record.v1").unwrap(),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(activity_id),
                    run_id: Some(run_id),
                    run_seq: Some(RunSeq::new(11 + index).unwrap()),
                    attempt_id: None,
                    participant_id: None,
                    producer: Producer::Scheduler,
                    occurred_at: UtcTimestamp::from_unix_millis(1_790_000_000_000),
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload: payload.clone(),
                })
                .collect(),
            outbox: Vec::new(),
        },
        run_id,
        activity_id,
        expected_last_run_sequence: RunSeq::new(10).unwrap(),
        mode: CancelMode::CancelWithoutAttempt,
        reason: CancelReason::PolicyRevoked,
        pending_interaction_id: Some(InteractionId::generate()),
        pending_tool_invocation_id: Some(InvocationId::generate()),
        unresolved_tool_invocation_id: None,
        descendants: Vec::new(),
    }
}

#[test]
fn scheduler_can_close_a_revoked_wait_without_impersonating_the_api() {
    revoked_wait().validate().unwrap();
}

#[test]
fn scheduler_cannot_claim_user_cancellation_or_take_over_an_active_attempt() {
    let mut command = revoked_wait();
    command.reason = CancelReason::UserRequested;
    assert!(command.validate().is_err());
    command.reason = CancelReason::PolicyRevoked;
    command.mode = CancelMode::RequestActiveAttempt;
    command.pending_interaction_id = None;
    command.pending_tool_invocation_id = None;
    command.append.records.truncate(1);
    assert!(command.validate().is_err());
}

#[test]
fn cancellation_keeps_one_producer_and_preserves_user_api_cancellation() {
    let mut command = revoked_wait();
    command.append.records[1].producer = Producer::RuntimeApi;
    assert!(command.validate().is_err());
    for record in &mut command.append.records {
        record.producer = Producer::RuntimeApi;
    }
    command.reason = CancelReason::UserRequested;
    command.validate().unwrap();
}
