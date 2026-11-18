use std::{sync::Arc, time::Duration};

#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    CorrelationId, DottedKey, HomeEpoch, IdempotencyKey, LedgerOffset, LedgerRecordId, RunSeq,
};
use musteragent_ledger::{
    AppendRequest, CancelMode, CancelRunBatch, DomainEvent, LedgerPayload, ProposedRecord,
};
use musteragent_loop::{AgentLoopJournal, LoopPhase, RunCompletionDisposition};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore, PostgresLedgerStore,
    migrate,
};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{RunEvent, WorkerId};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::CancelReason;
use musteragent_scheduler::{AttemptCancellationStore, DispatchStore};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufMeteringPublicationEncoder;
use musteragent_wire::ProtobufSchedulerEventEncoder;
use running_loop::{database_now, keyring, seed_running_loop};

#[tokio::test]
async fn cancellation_winning_terminal_commit_lane_cannot_complete() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresAgentLoopJournal::new(
        pool.clone(),
        content,
        codec.clone(),
        codec.clone(),
        Arc::new(ProtobufMeteringPublicationEncoder),
        keys,
        running_loop::recovery_keyring(),
    );
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let now = database_now(&pool).await;
    let payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::CancelRequested {
            reason: CancelReason::UserRequested,
            requested_by: fixture.principal_id,
        },
    )));
    PostgresLedgerStore::new(pool.clone())
        .cancel_run(CancelRunBatch {
            append: AppendRequest {
                tenant_id: fixture.tenant_id,
                conversation_id: fixture.conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
                expected_offset: LedgerOffset::new(1),
                idempotency_key: IdempotencyKey::new("cancel-wins-terminal-lane")
                    .expect("idempotency"),
                command_fingerprint: Sha256::digest(&payload),
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new("musteragent.runtime.record.v1")
                        .expect("schema"),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(fixture.activity_id),
                    run_id: Some(fixture.run_id),
                    run_seq: Some(RunSeq::new(2).expect("Run sequence")),
                    attempt_id: None,
                    participant_id: Some(fixture.requester_id),
                    producer: Producer::RuntimeApi,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }],
                outbox: Vec::new(),
            },
            run_id: fixture.run_id,
            activity_id: fixture.activity_id,
            expected_last_run_sequence: RunSeq::new(1).expect("Run sequence"),
            mode: CancelMode::RequestActiveAttempt,
            reason: musteragent_runtime_contract::CancelReason::UserRequested,
            pending_interaction_id: None,
            pending_tool_invocation_id: None,
            unresolved_tool_invocation_id: None,
            descendants: Vec::new(),
        })
        .await
        .expect("commit cancellation intent first");

    let disposition = journal
        .complete(&attempt)
        .await
        .expect("linearize terminal after cancellation");
    let RunCompletionDisposition::Cancelled(checkpoint) = disposition else {
        panic!("cancellation that wins the commit lane must return Cancelled");
    };
    assert_eq!(checkpoint.phase, LoopPhase::Cancelled);
    let state: (i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, attempt.status, activity.status,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.attempt_id = $2
         JOIN activity.activities AS activity ON activity.activity_id = run.activity_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancellation terminal state");
    assert_eq!(state, (7, 5, 6, 0));

    let payloads: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event USING (record_id)
         WHERE record.run_id = $1 ORDER BY record.run_seq",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read terminal Ledger");
    let events = payloads
        .iter()
        .filter_map(|payload| codec.decode(LedgerRecordKind::DomainEvent, payload).ok())
        .collect::<Vec<_>>();
    assert!(events.iter().any(|payload| matches!(
        payload,
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Cancelled { .. }))
    )));
    assert!(!events.iter().any(|payload| matches!(
        payload,
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Completed { .. }))
    )));
}

#[tokio::test]
async fn expired_cancelling_attempt_converges_without_the_execution_queue() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = Arc::new(musteragent_conformance::fixtures::model_journal::supervise(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content,
            codec.clone(),
            codec.clone(),
            Arc::new(ProtobufMeteringPublicationEncoder),
            keys.clone(),
            running_loop::recovery_keyring(),
        ),
    ));
    let (fixture, original_attempt) = seed_running_loop(&pool).await;
    let now = database_now(&pool).await;
    let payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::CancelRequested {
            reason: CancelReason::UserRequested,
            requested_by: fixture.principal_id,
        },
    )));
    PostgresLedgerStore::new(pool.clone())
        .cancel_run(CancelRunBatch {
            append: AppendRequest {
                tenant_id: fixture.tenant_id,
                conversation_id: fixture.conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
                expected_offset: LedgerOffset::new(1),
                idempotency_key: IdempotencyKey::new("cancel-before-control-recovery")
                    .expect("idempotency"),
                command_fingerprint: Sha256::digest(&payload),
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new("musteragent.runtime.record.v1")
                        .expect("schema"),
                    record_id: LedgerRecordId::generate(),
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: Some(fixture.activity_id),
                    run_id: Some(fixture.run_id),
                    run_seq: Some(RunSeq::new(2).expect("Run sequence")),
                    attempt_id: None,
                    participant_id: Some(fixture.requester_id),
                    producer: Producer::RuntimeApi,
                    occurred_at: now,
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload_hash: Sha256::digest(&payload),
                    payload,
                }],
                outbox: Vec::new(),
            },
            run_id: fixture.run_id,
            activity_id: fixture.activity_id,
            expected_last_run_sequence: RunSeq::new(1).expect("Run sequence"),
            mode: CancelMode::RequestActiveAttempt,
            reason: CancelReason::UserRequested,
            pending_interaction_id: None,
            pending_tool_invocation_id: None,
            unresolved_tool_invocation_id: None,
            descendants: Vec::new(),
        })
        .await
        .expect("commit cancellation intent");
    sqlx::query("UPDATE runtime.leases SET expires_at_ms = $2 WHERE run_id = $1")
        .bind(fixture.run_id.into_uuid())
        .bind(now.unix_millis() - 1)
        .execute(&pool)
        .await
        .expect("expire cancelled Attempt lease");

    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let recovered = dispatch
        .claim_one_cancellation(
            &WorkerId::new("cancellation-control-recovery").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim cancellation recovery")
        .expect("expired cancelling Attempt must be recoverable");
    assert_ne!(recovered.fence.attempt_id, original_attempt.attempt_id);
    let recovered_attempt = musteragent_run::AttemptContext {
        attempt_id: recovered.fence.attempt_id,
        run_id: recovered.fence.run_id,
        attempt_number: recovered.fence.attempt_number,
        worker_id: recovered.fence.worker_id,
        lease_id: recovered.fence.lease_id,
        fencing_token: recovered.fence.fencing_token,
        lease_expires_at: recovered.lease_expires_at,
    };
    journal
        .acknowledge_user_cancellation(&recovered_attempt)
        .await
        .expect("control recovery terminalizes cancellation");

    let state: (i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, attempt.status, activity.status,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.attempt_id = $2
         JOIN activity.activities AS activity ON activity.activity_id = run.activity_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(recovered_attempt.attempt_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered cancellation state");
    assert_eq!(state, (7, 5, 6, 0));
}
