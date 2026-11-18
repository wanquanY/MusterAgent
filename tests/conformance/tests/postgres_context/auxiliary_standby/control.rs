use super::*;
use musteragent_conversation::AdmissionMessage;
use musteragent_identity::{Actor, CommandId, PrincipalKind};
use musteragent_ledger::RunControlBatch;
use musteragent_loop::LoopControlQueue;

pub(super) async fn consume(
    pool: &PgPool,
    content: &Arc<dyn ContentStore>,
    tenant: TenantId,
    f: &RunningContextFixture,
    codec: &Arc<ProtobufLedgerPayloadCodec>,
) {
    let content_ref = put_text(content, tenant, "Use the revised source for this turn.").await;
    let principal: uuid::Uuid = sqlx::query_scalar(
        "SELECT principal_ref FROM conversation.participants WHERE participant_id=$1",
    )
    .bind(f.requester_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let principal_id = PrincipalId::from_uuid(principal).unwrap();
    let last: i64 =
        sqlx::query_scalar("SELECT last_run_sequence::bigint FROM runtime.runs WHERE run_id=$1")
            .bind(f.attempt.run_id.into_uuid())
            .fetch_one(pool)
            .await
            .unwrap();
    let last = u64::try_from(last).unwrap();
    let control_id = CommandId::generate();
    let message_id = MessageId::generate();
    let message_record_id = LedgerRecordId::generate();
    let control_record_id = LedgerRecordId::generate();
    let now = database_now(pool).await;
    let message_payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
        ConversationEvent::MessageAccepted {
            project_task_target: None,

            message_id,
            role: MessageRole::User,
            content_ref: content_ref.clone(),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        },
    )));
    let control_payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::ControlQueued {
            kind: RunControlKind::Steering,
            message_id,
        },
    )));
    let append = AppendRequest {
        tenant_id: tenant,
        conversation_id: f.conversation_id,
        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
        expected_offset: current_ledger_offset(pool, tenant, f.conversation_id).await,
        idempotency_key: IdempotencyKey::new(format!("loop-control:enqueue:{control_id}"))
            .expect("idempotency"),
        command_fingerprint: Sha256::digest(control_id.to_string()),
        records: vec![
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: message_record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(f.activity_id),
                run_id: Some(f.attempt.run_id),
                run_seq: Some(RunSeq::new(last + 1).expect("run sequence")),
                attempt_id: None,
                participant_id: Some(f.requester_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&message_payload),
                payload: message_payload,
            },
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: control_record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(f.activity_id),
                run_id: Some(f.attempt.run_id),
                run_seq: Some(RunSeq::new(last + 2).expect("run sequence")),
                attempt_id: None,
                participant_id: Some(f.requester_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&control_payload),
                payload: control_payload,
            },
        ],
        outbox: Vec::new(),
    };
    PostgresLedgerStore::new(pool.clone())
        .enqueue_run_control(RunControlBatch {
            actor: Actor {
                tenant_id: tenant,
                principal_id,
                principal_kind: PrincipalKind::User,
            },
            append,
            control_id,
            kind: RunControlKind::Steering,
            activity_id: f.activity_id,
            run_id: f.attempt.run_id,
            expected_last_run_sequence: RunSeq::new(last).expect("run sequence"),
            message_record_id,
            control_record_id,
            message: AdmissionMessage {
                project_task_target: None,
                message_id,
                participant_id: f.requester_id,
                role: MessageRole::User,
                source_run_id: f.attempt.run_id,
                content: content_ref,
            },
        })
        .await
        .expect("enqueue steering control");

    let queue = musteragent_postgres::PostgresLoopControlQueue::new(pool.clone(), codec.clone());
    assert_eq!(queue.drain_steering(&f.attempt).await.unwrap(), 1);
}
