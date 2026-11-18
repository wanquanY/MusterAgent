#[path = "postgres_execution/liveness.rs"]
mod liveness;
#[path = "support/retained_content.rs"]
mod retained_content;

use std::time::Duration;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, CapabilityProviderId, CommandId, ContentId,
    ConversationId, CorrelationId, DottedKey, FencingToken, HomeEpoch, IdempotencyKey,
    InvocationId, LeaseId, LedgerOffset, LedgerRecordId, MessageId, ParticipantId, RunId, RunSeq,
    RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp,
};
use musteragent_ledger::{
    AcknowledgeCancellationBatch, AppendRequest, CancelMode, CancelRunBatch, ClaimAttemptBatch,
    DomainEvent, ExecutionStore, FencedAppendBatch, LedgerPayload, ProposedRecord,
    RecoverAttemptBatch, RenewLeaseRequest,
};
use musteragent_postgres::{PostgresDispatchStore, PostgresExecutionError, PostgresLedgerStore};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptTrigger, RunEvent, WorkerId};
use musteragent_runtime_contract::{
    InterruptionReason, RetryDisposition, TraceContext, TraceParent,
};
use musteragent_scheduler::{AttemptControlStore, ClaimOutcome, DispatchErrorClass, DispatchStore};
use musteragent_telemetry::RunTelemetryContext;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::ToolRetryContract;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use sqlx::{PgPool, Row as _, postgres::PgListener};

#[derive(Debug, Copy, Clone)]
struct SeededRun {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    run_id: RunId,
    home_epoch: HomeEpoch,
}

async fn runtime_pool() -> (IsolatedPostgres, PgPool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    (postgres, pool)
}

async fn database_now(pool: &PgPool) -> UtcTimestamp {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .expect("database time");
    UtcTimestamp::from_unix_millis(millis)
}

#[tokio::test]
async fn attempt_and_run_trigger_constraints_share_the_complete_domain_enum() {
    let (_postgres, pool) = runtime_pool().await;
    let constraints: Vec<(String, String)> = sqlx::query_as(
        "SELECT conname, pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conrelid IN ('runtime.runs'::regclass, 'runtime.attempts'::regclass)
           AND conname IN ('run_next_attempt_trigger_closed', 'attempt_trigger_closed')
         ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("read scheduler trigger constraints");
    assert_eq!(
        constraints,
        vec![
            (
                "attempt_trigger_closed".to_owned(),
                "CHECK (((trigger >= 1) AND (trigger <= 7)))".to_owned(),
            ),
            (
                "run_next_attempt_trigger_closed".to_owned(),
                "CHECK (((next_attempt_trigger >= 1) AND (next_attempt_trigger <= 7)))".to_owned(),
            ),
        ],
        "Run scheduling intent and claimed Attempt facts must encode the same AttemptTrigger enum",
    );
}

#[tokio::test]
async fn scheduler_is_notified_when_a_durable_wait_deadline_is_installed() {
    let (postgres, pool) = runtime_pool().await;
    let run = seed_queued_run(&pool).await;
    let mut listener = PgListener::connect(postgres.connection_url())
        .await
        .expect("connect dispatch listener");
    listener
        .listen("musteragent_runtime_dispatch")
        .await
        .expect("listen for dispatch hints");
    let now = database_now(&pool).await;

    for (wait_reason, next_trigger, wait_invocation_id) in [
        (4_i16, 2_i16, None),
        (7_i16, 7_i16, Some(uuid::Uuid::now_v7())),
    ] {
        sqlx::query(
            "UPDATE runtime.runs
             SET status = 3, wait_reason = $2, wait_invocation_id = $3,
                 not_before_ms = $4, next_attempt_trigger = $5
             WHERE run_id = $1",
        )
        .bind(run.run_id.into_uuid())
        .bind(wait_reason)
        .bind(wait_invocation_id)
        .bind(now.unix_millis() + 60_000)
        .bind(next_trigger)
        .execute(&pool)
        .await
        .expect("install durable Scheduler wait deadline");
        let notification = tokio::time::timeout(Duration::from_secs(1), listener.recv())
            .await
            .expect("waiting Run must wake Scheduler")
            .expect("receive dispatch notification");
        assert_eq!(notification.payload(), run.run_id.to_string());

        sqlx::query(
            "UPDATE runtime.runs
             SET status = 1, wait_reason = NULL, wait_invocation_id = NULL,
                 not_before_ms = NULL, next_attempt_trigger = 1
             WHERE run_id = $1",
        )
        .bind(run.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("restore queued Run between wait cases");
        tokio::time::timeout(Duration::from_secs(1), listener.recv())
            .await
            .expect("queued Run must wake Scheduler")
            .expect("drain queued Run dispatch notification");
    }
}

#[tokio::test]
async fn scheduler_reports_persistence_constraint_drift_as_contract_violation() {
    let (_postgres, pool) = runtime_pool().await;
    let run = seed_queued_run(&pool).await;
    let now = database_now(&pool).await;
    sqlx::query(
        "UPDATE runtime.runs
         SET status = 3, wait_reason = 7, wait_invocation_id = $2,
             not_before_ms = $3, next_attempt_trigger = 7
         WHERE run_id = $1",
    )
    .bind(run.run_id.into_uuid())
    .bind(uuid::Uuid::now_v7())
    .bind(now.unix_millis())
    .execute(&pool)
    .await
    .expect("seed due external-operation reconciliation");
    sqlx::raw_sql(
        "ALTER TABLE runtime.attempts DROP CONSTRAINT attempt_trigger_closed;
         ALTER TABLE runtime.attempts ADD CONSTRAINT attempt_trigger_closed
             CHECK (trigger BETWEEN 1 AND 6);",
    )
    .execute(&pool)
    .await
    .expect("simulate a stale AttemptTrigger schema");

    let scheduler = PostgresDispatchStore::new(
        pool.clone(),
        std::sync::Arc::new(ProtobufSchedulerEventEncoder),
    );
    let error = scheduler
        .claim_one(
            &WorkerId::new("constraint-drift-worker").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect_err("stale trigger constraint must reject reconciliation claim");
    assert_eq!(error.class, DispatchErrorClass::ContractViolation);
}

async fn seed_pending_control(pool: &PgPool, run: SeededRun) -> CommandId {
    let control_id = CommandId::generate();
    sqlx::query(
        "INSERT INTO runtime.loop_controls (
             control_id, tenant_id, conversation_id, run_id, message_id,
             source_record_id, source_conversation_seq, kind, status,
             enqueued_at_ms, consumed_at_ms, consumed_attempt_id
         ) SELECT $1, $2, $3, $4, $5, record_id, conversation_seq,
                  1, 1, 1, NULL, NULL
           FROM event_store.ledger_records
          WHERE tenant_id = $2 AND conversation_id = $3 AND conversation_seq = 1",
    )
    .bind(control_id.into_uuid())
    .bind(run.tenant_id.into_uuid())
    .bind(run.conversation_id.into_uuid())
    .bind(run.run_id.into_uuid())
    .bind(MessageId::generate().into_uuid())
    .execute(pool)
    .await
    .expect("seed pending control");
    control_id
}

async fn assert_control_discarded_by_cancellation(pool: &PgPool, control_id: CommandId) {
    let resolution: (i16, Option<i64>, Option<i16>) = sqlx::query_as(
        "SELECT status, discarded_at_ms, discard_reason
         FROM runtime.loop_controls WHERE control_id = $1",
    )
    .bind(control_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read control resolution");
    assert_eq!(resolution.0, 3);
    assert!(resolution.1.is_some());
    assert_eq!(resolution.2, Some(1));
}

async fn seed_queued_run(pool: &PgPool) -> SeededRun {
    let seeded = SeededRun {
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        run_id: RunId::generate(),
        home_epoch: HomeEpoch::new(1).expect("epoch"),
    };
    let principal_id = musteragent_identity::PrincipalId::generate();
    let snapshot_id = RuntimeSnapshotId::generate();
    let executor_id = ParticipantId::generate();
    let requester_id = ParticipantId::generate();
    let record_id = LedgerRecordId::generate();
    let payload = b"run.admitted";
    let payload_hash = Sha256::digest(payload);
    let mut participant_transaction = pool.begin().await.expect("begin participant fixture");
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, 1, NULL, 1, NULL, 1, 1, $3, 1, 1, NULL, $4)",
    )
    .bind(seeded.conversation_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(executor_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES
             ($1, $3, $4, 1, $5, NULL, NULL, 'Execution Tester', NULL, 1, NULL),
             ($2, $3, $4, 2, NULL,
              (SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'),
              NULL, 'Kernel', NULL, 1, NULL)",
    )
    .bind(requester_id.into_uuid())
    .bind(executor_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed participants");
    participant_transaction
        .commit()
        .await
        .expect("commit participant fixture");
    sqlx::query(
        "INSERT INTO activity.activities (
             activity_id, tenant_id, conversation_id, root_activity_id,
             parent_activity_id, kind, status, policy_snapshot_ref,
             created_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $1, NULL, 1, 2, 'musteragent.legacy.chat@1', 1, NULL)",
    )
    .bind(seeded.activity_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Activity");
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms
         ) VALUES ($1, $2, $3, $4, 'musteragent.runtime.snapshot.v17', $5, $6, 1)",
    )
    .bind(snapshot_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .bind(seeded.run_id.into_uuid())
    .bind(b"snapshot".as_slice())
    .bind(Sha256::digest(b"snapshot").into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("seed Snapshot");
    sqlx::query(
        "INSERT INTO runtime.runs (
             run_id, tenant_id, conversation_id, activity_id, activity_kind,
             executor_participant_id, requested_by_participant_id, parent_run_id,
             delegation_id, execution_scope_kind, workspace_id, device_id,
             control_home_kind, control_device_id, execution_home_kind,
             execution_device_id, home_epoch, runtime_snapshot_id,
             admitted_sequence, last_run_sequence, next_fencing_token,
             status, wait_reason, wait_interaction_id, wait_invocation_id,
             next_attempt_number, not_before_ms, cancel_requested_at_ms,
             admitted_at_ms, started_at_ms, finished_at_ms, terminal_error, run_version,
             traceparent, tracestate
         ) VALUES (
             $1, $2, $3, $4, 1, $5, $6, NULL, NULL, 1, NULL, NULL,
             1, NULL, 1, NULL, 1, $7, 1, 1, 1,
             1, NULL, NULL, NULL, 1, NULL, NULL, 1, NULL, NULL, NULL, 1,
             '00-00000000000000000000000000000001-0000000000000001-00', NULL
         )",
    )
    .bind(seeded.run_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .bind(seeded.activity_id.into_uuid())
    .bind(executor_id.into_uuid())
    .bind(requester_id.into_uuid())
    .bind(snapshot_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Run");
    sqlx::query(
        "INSERT INTO event_store.ledger_streams (
             tenant_id, conversation_id, home_epoch, last_sequence,
             retained_from_sequence, created_at_ms, updated_at_ms
         ) VALUES ($1, $2, 1, 1, 1, 1, 1)",
    )
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed Ledger stream");
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.runtime.record.v1', 1, $2, $3, 1, $4, $5, 1,
             NULL, NULL, 1, 1, 1, 1, NULL, $6, 'seed:run', $7
         )",
    )
    .bind(record_id.into_uuid())
    .bind(seeded.tenant_id.into_uuid())
    .bind(seeded.conversation_id.into_uuid())
    .bind(seeded.activity_id.into_uuid())
    .bind(seeded.run_id.into_uuid())
    .bind(CorrelationId::generate().into_uuid())
    .bind(payload_hash.into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("seed Ledger record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(record_id.into_uuid())
    .bind(payload.as_slice())
    .bind(payload_hash.into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("seed Domain Event");
    seeded
}

fn record(
    seeded: SeededRun,
    attempt_id: AttemptId,
    run_sequence: u64,
    producer: Producer,
    name: &str,
) -> ProposedRecord {
    let payload = name.as_bytes().to_vec();
    ProposedRecord {
        schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
        record_id: LedgerRecordId::generate(),
        record_kind: LedgerRecordKind::DomainEvent,
        activity_id: Some(seeded.activity_id),
        run_id: Some(seeded.run_id),
        run_seq: Some(RunSeq::new(run_sequence).expect("run sequence")),
        attempt_id: Some(attempt_id),
        participant_id: None,
        producer,
        occurred_at: UtcTimestamp::from_unix_millis(1),
        causation_id: None,
        correlation_id: CorrelationId::generate(),
        payload_hash: Sha256::digest(&payload),
        payload,
    }
}

fn append(
    seeded: SeededRun,
    offset: u64,
    idempotency: &str,
    records: Vec<ProposedRecord>,
) -> AppendRequest {
    AppendRequest {
        tenant_id: seeded.tenant_id,
        conversation_id: seeded.conversation_id,
        expected_home_epoch: seeded.home_epoch,
        expected_offset: LedgerOffset::new(offset),
        idempotency_key: IdempotencyKey::new(idempotency).expect("idempotency"),
        command_fingerprint: Sha256::digest(format!("fingerprint:{idempotency}")),
        records,
        outbox: Vec::new(),
    }
}

async fn claim_batch(pool: &PgPool, seeded: SeededRun) -> ClaimAttemptBatch {
    let attempt_id = AttemptId::generate();
    let now = database_now(pool).await;
    ClaimAttemptBatch {
        append: append(
            seeded,
            1,
            "claim:initial",
            vec![
                record(
                    seeded,
                    attempt_id,
                    2,
                    Producer::Scheduler,
                    "attempt.claimed",
                ),
                record(seeded, attempt_id, 3, Producer::Scheduler, "run.started"),
            ],
        ),
        run_id: seeded.run_id,
        activity_id: seeded.activity_id,
        activity_kind: ActivityKind::Chat,
        expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
        attempt_id,
        attempt_number: AttemptNumber::new(1).expect("attempt number"),
        trigger: AttemptTrigger::Initial,
        worker_id: WorkerId::new("worker-a").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("fencing"),
        lease_expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 30_000),
        telemetry: run_telemetry(seeded),
    }
}

fn assert_execution_store_port<T: ExecutionStore>(_store: &T) {}

#[tokio::test]
async fn lease_renewal_recovery_and_fenced_commit_reject_the_old_worker() {
    let (_postgres, pool) = runtime_pool().await;
    let seeded = seed_queued_run(&pool).await;
    let store = PostgresLedgerStore::new(pool.clone());
    assert_execution_store_port(&store);
    let claim = claim_batch(&pool, seeded).await;
    let claimed = store
        .claim_attempt(claim)
        .await
        .expect("claim initial Attempt");
    assert_eq!(claimed.fence.fencing_token.get(), 1);

    let renewed_expiry =
        UtcTimestamp::from_unix_millis(database_now(&pool).await.unix_millis() + 60_000);
    assert_eq!(
        store
            .renew_lease(RenewLeaseRequest {
                fence: claimed.fence.clone(),
                new_expires_at: renewed_expiry,
            })
            .await
            .expect("renew"),
        renewed_expiry
    );

    let worker_append = FencedAppendBatch {
        fence: claimed.fence.clone(),
        expected_last_run_sequence: RunSeq::new(3).expect("run sequence"),
        append: append(
            seeded,
            3,
            "worker:frame:1",
            vec![record(
                seeded,
                claimed.fence.attempt_id,
                4,
                Producer::Worker,
                "output.committed",
            )],
        ),
    };
    store
        .append_fenced(worker_append)
        .await
        .expect("fenced Worker commit");

    let expired_at = database_now(&pool).await.unix_millis() - 500;
    sqlx::query("UPDATE runtime.leases SET expires_at_ms = $2 WHERE run_id = $1")
        .bind(seeded.run_id.into_uuid())
        .bind(expired_at)
        .execute(&pool)
        .await
        .expect("expire lease");
    sqlx::query("UPDATE runtime.attempts SET lease_expires_at_ms = $2 WHERE attempt_id = $1")
        .bind(claimed.fence.attempt_id.into_uuid())
        .bind(expired_at)
        .execute(&pool)
        .await
        .expect("expire Attempt lease");

    let recovery_now = database_now(&pool).await;
    let new_attempt_id = AttemptId::generate();
    let recovery = RecoverAttemptBatch {
        append: append(
            seeded,
            4,
            "claim:recovery",
            vec![
                record(
                    seeded,
                    claimed.fence.attempt_id,
                    5,
                    Producer::Scheduler,
                    "attempt.abandoned",
                ),
                record(
                    seeded,
                    new_attempt_id,
                    6,
                    Producer::Scheduler,
                    "attempt.claimed",
                ),
                record(
                    seeded,
                    new_attempt_id,
                    7,
                    Producer::Scheduler,
                    "run.resumed",
                ),
            ],
        ),
        run_id: seeded.run_id,
        activity_id: seeded.activity_id,
        activity_kind: ActivityKind::Chat,
        expected_last_run_sequence: RunSeq::new(4).expect("run sequence"),
        stale_fence: claimed.fence.clone(),
        new_attempt_id,
        new_attempt_number: AttemptNumber::new(2).expect("attempt number"),
        worker_id: WorkerId::new("worker-b").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(2).expect("fencing"),
        lease_expires_at: UtcTimestamp::from_unix_millis(recovery_now.unix_millis() + 30_000),
        telemetry: run_telemetry(seeded),
    };
    let recovered = store.recover_attempt(recovery).await.expect("recover");
    assert_eq!(recovered.fence.fencing_token.get(), 2);
    let recovery_claimed_at: i64 =
        sqlx::query_scalar("SELECT claimed_at_ms FROM runtime.attempts WHERE attempt_id = $1")
            .bind(recovered.fence.attempt_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("recovery timestamp");
    assert!(
        recovery_claimed_at - expired_at <= 2_000,
        "recovery exceeded lease TTL + 2 seconds"
    );

    let stale_attempt_id = claimed.fence.attempt_id;
    let stale_append = FencedAppendBatch {
        fence: claimed.fence,
        expected_last_run_sequence: RunSeq::new(7).expect("run sequence"),
        append: append(
            seeded,
            7,
            "worker:stale",
            vec![record(
                seeded,
                stale_attempt_id,
                8,
                Producer::Worker,
                "late.output",
            )],
        ),
    };
    assert!(matches!(
        store.append_fenced(stale_append).await,
        Err(PostgresExecutionError::StaleFence)
    ));
    let ledger_head: String = sqlx::query_scalar(
        "SELECT last_sequence::text
         FROM event_store.ledger_streams
         WHERE conversation_id = $1",
    )
    .bind(seeded.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("ledger head");
    assert_eq!(ledger_head, "7");
    pool.close().await;
}

fn run_telemetry(seeded: SeededRun) -> RunTelemetryContext {
    RunTelemetryContext {
        tenant_id: seeded.tenant_id,
        conversation_id: seeded.conversation_id,
        activity_id: seeded.activity_id,
        run_id: seeded.run_id,
        correlation_id: CorrelationId::generate(),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
    }
}

#[tokio::test]
async fn cancellation_has_one_terminal_commit_for_active_and_queued_runs() {
    let (_postgres, pool) = runtime_pool().await;
    let store = PostgresLedgerStore::new(pool.clone());
    let active = seed_queued_run(&pool).await;
    let claimed = store
        .claim_attempt(claim_batch(&pool, active).await)
        .await
        .expect("claim");
    let active_control_id = seed_pending_control(&pool, active).await;
    let request_cancel = CancelRunBatch {
        append: append(
            active,
            3,
            "cancel:active",
            vec![record(
                active,
                claimed.fence.attempt_id,
                4,
                Producer::RuntimeApi,
                "run.cancel_requested",
            )],
        ),
        run_id: active.run_id,
        activity_id: active.activity_id,
        expected_last_run_sequence: RunSeq::new(3).expect("run sequence"),
        mode: CancelMode::RequestActiveAttempt,
        reason: musteragent_runtime_contract::CancelReason::UserRequested,
        pending_interaction_id: None,
        pending_tool_invocation_id: None,
        unresolved_tool_invocation_id: None,
        descendants: Vec::new(),
    };
    let mut request_cancel = request_cancel;
    request_cancel.append.records[0].attempt_id = None;
    let pending = store
        .cancel_run(request_cancel)
        .await
        .expect("request cancel");
    assert!(!pending.terminal);

    let acknowledge = AcknowledgeCancellationBatch {
        append: append(
            active,
            4,
            "cancel:ack",
            vec![
                record(
                    active,
                    claimed.fence.attempt_id,
                    5,
                    Producer::Worker,
                    "attempt.cancelled",
                ),
                record(
                    active,
                    claimed.fence.attempt_id,
                    6,
                    Producer::Worker,
                    "run.cancelled",
                ),
                record(
                    active,
                    claimed.fence.attempt_id,
                    7,
                    Producer::Worker,
                    "activity.cancelled",
                ),
            ],
        ),
        fence: claimed.fence,
        activity_id: active.activity_id,
        expected_last_run_sequence: RunSeq::new(4).expect("run sequence"),
    };
    assert!(
        store
            .acknowledge_cancellation(acknowledge)
            .await
            .expect("acknowledge cancellation")
            .terminal
    );
    assert_control_discarded_by_cancellation(&pool, active_control_id).await;

    let queued = seed_queued_run(&pool).await;
    let queued_control_id = seed_pending_control(&pool, queued).await;
    let placeholder_attempt = AttemptId::generate();
    let mut records = vec![
        record(
            queued,
            placeholder_attempt,
            2,
            Producer::RuntimeApi,
            "run.cancel_requested",
        ),
        record(
            queued,
            placeholder_attempt,
            3,
            Producer::RuntimeApi,
            "run.cancelled",
        ),
        record(
            queued,
            placeholder_attempt,
            4,
            Producer::RuntimeApi,
            "activity.cancelled",
        ),
    ];
    for record in &mut records {
        record.attempt_id = None;
    }
    let direct = store
        .cancel_run(CancelRunBatch {
            append: append(queued, 1, "cancel:queued", records),
            run_id: queued.run_id,
            activity_id: queued.activity_id,
            expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
            mode: CancelMode::CancelWithoutAttempt,
            reason: musteragent_runtime_contract::CancelReason::UserRequested,
            pending_interaction_id: None,
            pending_tool_invocation_id: None,
            unresolved_tool_invocation_id: None,
            descendants: Vec::new(),
        })
        .await
        .expect("cancel queued Run");
    assert!(direct.terminal);
    let statuses: (i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, activity.status,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN activity.activities AS activity ON activity.activity_id = run.activity_id
         WHERE run.run_id = $1",
    )
    .bind(queued.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("cancelled status");
    assert_eq!(statuses, (7, 6, 0));
    assert_control_discarded_by_cancellation(&pool, queued_control_id).await;
    pool.close().await;
}

#[tokio::test]
async fn cancellation_closes_postgres_external_wait_and_preserves_unknown_tool() {
    let _postgres = IsolatedPostgres::start_with_pgvector().await.expect(
        "cancellation closes postgres external wait and preserves unknown tool must succeed",
    );
    let pool = _postgres.pool().await.expect(
        "cancellation closes postgres external wait and preserves unknown tool must succeed",
    );
    musteragent_conformance::migrate(&pool).await.expect(
        "cancellation closes postgres external wait and preserves unknown tool must succeed",
    );
    let store = PostgresLedgerStore::new(pool.clone());
    let run = seed_queued_run(&pool).await;
    let invocation_id = InvocationId::generate();
    let now = database_now(&pool).await;
    let request_ref = musteragent_content_ref::ContentRef {
        content_id: ContentId::generate(),
        media_type: musteragent_content_ref::MediaType::new("application/json")
            .expect("request media type"),
        byte_length: 2,
        sha256: Sha256::digest(b"{}"),
    };
    retained_content::retain(&pool, run.tenant_id, &request_ref).await;
    sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, execution_attempt_id, tool_call_id,
             tool_ref, capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id, request_hash,
             deadline_ms, started_at_ms, terminal_reason,
             reconciliation_ref_protected, indeterminate_observed_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $5, $6,
             'artifact.image.create@1', $7, 6, 3, 1,
             3, $8, $9, $10, $11, $12, $13, $14, $12
         )",
    )
    .bind(invocation_id.into_uuid())
    .bind(run.tenant_id.into_uuid())
    .bind(run.conversation_id.into_uuid())
    .bind(run.run_id.into_uuid())
    .bind(AttemptId::generate().into_uuid())
    .bind(
        ToolCallId::new("postgres-external-image-cancel")
            .expect("Tool call")
            .as_str(),
    )
    .bind(CapabilityProviderId::generate().into_uuid())
    .bind(
        IdempotencyKey::new(format!("external-cancel:{invocation_id}"))
            .expect("idempotency")
            .as_str(),
    )
    .bind(request_ref.content_id.into_uuid())
    .bind(Sha256::digest(b"{}").into_bytes().to_vec())
    .bind(now.unix_millis() + 20 * 60 * 1_000)
    .bind(now.unix_millis())
    .bind(b"Image generation is processing".as_slice())
    .bind(b"protected-reconciliation-reference".as_slice())
    .execute(&pool)
    .await
    .expect("seed indeterminate Backend media Tool");
    sqlx::query(
        "UPDATE runtime.runs
         SET status = 3, wait_reason = 7, wait_invocation_id = $2,
             not_before_ms = $3, next_attempt_trigger = 7
         WHERE run_id = $1",
    )
    .bind(run.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(now.unix_millis() + 30_000)
    .execute(&pool)
    .await
    .expect("seed ExternalOperation wait");

    // Archive uses the same canonical mutation. Its authority must preserve
    // the unknown outcome rather than enqueue an invented ToolCancelled event.
    let owner: uuid::Uuid = sqlx::query_scalar(
        "SELECT created_by FROM conversation.conversations WHERE conversation_id=$1",
    )
    .bind(run.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("cancellation closes postgres external wait and preserves unknown tool must succeed");
    let application = musteragent_postgres::PostgresRuntimeApplicationStore::new(
        pool.clone(),
        std::sync::Arc::new(musteragent_wire::ProtobufSnapshotCodec),
    );
    let authority =
        musteragent_runtime_session::RuntimeApplicationStore::authorize_archive_conversation(
            &application,
            musteragent_identity::Actor {
                tenant_id: run.tenant_id,
                principal_id: musteragent_identity::PrincipalId::from_uuid(owner).expect("cancellation closes postgres external wait and preserves unknown tool must succeed"),
                principal_kind: musteragent_identity::PrincipalKind::User,
            },
            run.conversation_id,
            CorrelationId::generate(),
        )
        .await
        .expect("cancellation closes postgres external wait and preserves unknown tool must succeed");
    assert_eq!(authority.active_runs.len(), 1);
    assert_eq!(authority.active_runs[0].pending_tool_invocation_id, None);
    assert_eq!(
        authority.active_runs[0].unresolved_tool_invocation_id,
        Some(invocation_id)
    );

    let placeholder_attempt = AttemptId::generate();
    let mut records = [
        "run.cancel_requested",
        "run.cancelled",
        "activity.cancelled",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| {
        record(
            run,
            placeholder_attempt,
            u64::try_from(index + 2).expect("run sequence"),
            Producer::RuntimeApi,
            name,
        )
    })
    .collect::<Vec<_>>();
    for record in &mut records {
        record.attempt_id = None;
    }
    let receipt = store
        .cancel_run(CancelRunBatch {
            append: append(run, 1, "cancel:external-operation", records),
            run_id: run.run_id,
            activity_id: run.activity_id,
            expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
            mode: CancelMode::CancelWithoutAttempt,
            reason: musteragent_runtime_contract::CancelReason::UserRequested,
            pending_interaction_id: None,
            pending_tool_invocation_id: None,
            unresolved_tool_invocation_id: Some(invocation_id),
            descendants: Vec::new(),
        })
        .await
        .expect("cancel ExternalOperation wait");
    assert!(receipt.terminal);

    let cancelled = sqlx::query(
        "SELECT run.status AS run_status,
                run.wait_reason,
                run.wait_invocation_id,
                run.not_before_ms,
                tool.status AS tool_status,
                tool.terminal_reason,
                tool.reconciliation_ref_protected
         FROM runtime.runs AS run
         JOIN tool.tool_invocations AS tool
           ON tool.run_id = run.run_id AND tool.invocation_id = $2
         WHERE run.run_id = $1",
    )
    .bind(run.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancelled ExternalOperation wait");
    assert_eq!(cancelled.get::<i16, _>("run_status"), 7);
    assert_eq!(cancelled.get::<Option<i16>, _>("wait_reason"), None);
    assert_eq!(
        cancelled.get::<Option<uuid::Uuid>, _>("wait_invocation_id"),
        None
    );
    assert_eq!(cancelled.get::<Option<i64>, _>("not_before_ms"), None);
    assert_eq!(cancelled.get::<i16, _>("tool_status"), 6);
    assert_eq!(
        cancelled.get::<Option<Vec<u8>>, _>("terminal_reason"),
        Some(b"Image generation is processing".to_vec())
    );
    assert_eq!(
        cancelled.get::<Option<Vec<u8>>, _>("reconciliation_ref_protected"),
        Some(b"protected-reconciliation-reference".to_vec())
    );
    pool.close().await;
}

#[tokio::test]
async fn durable_scheduler_claims_queued_work_and_recovers_an_expired_lease() {
    let (_postgres, pool) = runtime_pool().await;
    let seeded = seed_queued_run(&pool).await;
    let scheduler = PostgresDispatchStore::new(
        pool.clone(),
        std::sync::Arc::new(ProtobufSchedulerEventEncoder),
    );
    let first = scheduler
        .claim_one(
            &WorkerId::new("scheduler-worker-a").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("claim queued Run");
    let ClaimOutcome::Claimed(first) = first else {
        panic!("queued Run must be claimed");
    };
    assert_eq!(first.fence.run_id, seeded.run_id);
    assert_eq!(first.fence.attempt_number.get(), 1);

    let expired_at = database_now(&pool).await.unix_millis() - 1;
    sqlx::query("UPDATE runtime.leases SET expires_at_ms = $2 WHERE run_id = $1")
        .bind(seeded.run_id.into_uuid())
        .bind(expired_at)
        .execute(&pool)
        .await
        .expect("expire Lease");
    sqlx::query(
        "UPDATE runtime.attempts
         SET lease_expires_at_ms = $2
         WHERE attempt_id = $1",
    )
    .bind(first.fence.attempt_id.into_uuid())
    .bind(expired_at)
    .execute(&pool)
    .await
    .expect("expire Attempt");

    let recovered = scheduler
        .claim_one(
            &WorkerId::new("scheduler-worker-b").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("recover expired Run");
    let ClaimOutcome::Claimed(recovered) = recovered else {
        panic!("expired Run must be recovered");
    };
    assert_eq!(recovered.fence.attempt_number.get(), 2);
    assert!(
        recovered.fence.fencing_token.get() > first.fence.fencing_token.get(),
        "recovery must advance fencing"
    );
    pool.close().await;
}

#[tokio::test]
async fn graceful_surrender_is_immediately_recoverable_with_its_durable_reason() {
    let (_postgres, pool) = runtime_pool().await;
    let seeded = seed_queued_run(&pool).await;
    let scheduler = PostgresDispatchStore::new(
        pool.clone(),
        std::sync::Arc::new(ProtobufSchedulerEventEncoder),
    );
    let first = scheduler
        .claim_one(
            &WorkerId::new("scheduler-worker-shutdown").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("claim queued Run");
    let ClaimOutcome::Claimed(first) = first else {
        panic!("queued Run must be claimed");
    };

    scheduler
        .surrender(&first.fence, InterruptionReason::SystemShutdown)
        .await
        .expect("surrender owned Attempt");
    let surrendered: (i16, i64) = sqlx::query_as(
        "SELECT surrender_reason, expires_at_ms
         FROM runtime.leases
         WHERE run_id = $1",
    )
    .bind(seeded.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read surrendered Lease");
    assert_eq!(surrendered.0, 4);
    assert!(surrendered.1 <= database_now(&pool).await.unix_millis());

    let recovered = scheduler
        .claim_one(
            &WorkerId::new("scheduler-worker-recovery").expect("worker"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("recover surrendered Run");
    let ClaimOutcome::Claimed(recovered) = recovered else {
        panic!("surrendered Run must be recovered without waiting for its old TTL");
    };
    assert_eq!(recovered.fence.attempt_number.get(), 2);
    assert!(recovered.fence.fencing_token.get() > first.fence.fencing_token.get());

    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event USING (record_id)
         WHERE record.run_id = $1 AND record.attempt_id = $2
         ORDER BY record.run_seq DESC
         LIMIT 1",
    )
    .bind(seeded.run_id.into_uuid())
    .bind(first.fence.attempt_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read Attempt abandoned event");
    let decoded = ProtobufLedgerPayloadCodec
        .decode(LedgerRecordKind::DomainEvent, &encoded)
        .expect("decode Attempt abandoned event");
    assert!(matches!(
        decoded,
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptAbandoned {
            reason: InterruptionReason::SystemShutdown,
            ..
        }))
    ));
    pool.close().await;
}

#[tokio::test]
async fn side_effect_retry_policy_covers_all_four_recovery_classes() {
    assert_eq!(
        ToolRetryContract::ReadOnly.after_unknown_outcome(),
        RetryDisposition::RetryNewAttempt
    );
    assert_eq!(
        ToolRetryContract::IdempotentByKey.after_unknown_outcome(),
        RetryDisposition::RetryNewAttempt
    );
    assert_eq!(
        ToolRetryContract::QueryBeforeRetry.after_unknown_outcome(),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert_eq!(
        ToolRetryContract::NotReplayable.after_unknown_outcome(),
        RetryDisposition::DoNotRetry
    );
}

#[tokio::test]
async fn postgres_liveness_concurrent_cas_has_one_committed_phase_and_bounded_activity() {
    liveness::postgres_liveness_concurrent_cas_has_one_committed_phase_and_bounded_activity().await;
}

#[tokio::test]
async fn postgres_liveness_ledger_failure_rolls_back_stop_and_rejects_expired_owner() {
    liveness::postgres_liveness_ledger_failure_rolls_back_stop_and_rejects_expired_owner().await;
}

#[tokio::test]
async fn postgres_liveness_rejects_activity_delayed_in_the_home_commit_lane() {
    liveness::postgres_liveness_rejects_activity_delayed_in_the_home_commit_lane().await;
}

#[tokio::test]
async fn postgres_liveness_observes_work_during_commit_contention_and_retains_failed_batches() {
    liveness::postgres_liveness_observes_work_during_commit_contention_and_retains_failed_batches()
        .await;
}
