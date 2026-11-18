#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

use std::sync::Arc;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, ConversationSeq, CorrelationId,
    DottedKey, FencingToken, HomeEpoch, LeaseId, LedgerRecordId, ParticipantId, PrincipalId, RunId,
    RuntimeSnapshotId, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendDisposition, AppendReceipt, AttemptFence, DomainEvent, ExecutionReceipt, LedgerPayload,
};
use musteragent_loop::{AgentLoopExit, DurableAgentLoop, InvocationSupervisor};
use musteragent_run::{AttemptContext, RunContext, RunEvent, WorkerId};
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_storage_protection::{OpaqueRefKeyring, ProtectedContentKeyring};
use musteragent_telemetry::AttemptTelemetryContext;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use sqlx::PgPool;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Fixture {
    pub(crate) tenant_id: TenantId,
    pub(crate) conversation_id: ConversationId,
    pub(crate) activity_id: ActivityId,
    pub(crate) run_id: RunId,
    pub(crate) attempt_id: AttemptId,
    pub(crate) executor_id: ParticipantId,
    pub(crate) executor_principal_id: PrincipalId,
    pub(crate) requester_id: ParticipantId,
    pub(crate) runtime_snapshot_id: RuntimeSnapshotId,
    pub(crate) principal_id: PrincipalId,
}

pub(crate) fn keyring() -> Arc<OpaqueRefKeyring> {
    let key_id = DottedKey::new("runtime.loop.test").expect("key id");
    Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [31_u8; 32])]).expect("keyring"))
}

pub(crate) fn recovery_keyring() -> Arc<ProtectedContentKeyring> {
    let key_id = DottedKey::new("runtime.loop.test").expect("key id");
    Arc::new(
        ProtectedContentKeyring::new(key_id.clone(), [(key_id, [31_u8; 32])])
            .expect("recovery keyring"),
    )
}

pub(crate) async fn database_now(pool: &PgPool) -> UtcTimestamp {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .expect("database time");
    UtcTimestamp::from_unix_millis(millis)
}

pub(crate) fn run_context(fixture: Fixture) -> RunContext {
    RunContext {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        activity_kind: ActivityKind::Chat,
        run_id: fixture.run_id,
        executor_participant_id: fixture.executor_id,
        requested_by_participant_id: fixture.requester_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant {
            tenant_id: fixture.tenant_id,
        },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        runtime_snapshot_id: fixture.runtime_snapshot_id,
    }
}

fn run_started_payload(fixture: Fixture) -> Vec<u8> {
    ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::Started {
            attempt_id: fixture.attempt_id,
            attempt_number: AttemptNumber::new(1).expect("attempt number"),
        },
    )))
}

pub(crate) fn attempt_telemetry(
    fixture: Fixture,
    attempt: &AttemptContext,
) -> AttemptTelemetryContext {
    AttemptTelemetryContext {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        run_id: fixture.run_id,
        attempt_id: attempt.attempt_id,
        attempt_number: attempt.attempt_number,
        fencing_token: attempt.fencing_token,
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

pub(crate) async fn execute_loop(
    runtime: &DurableAgentLoop,
    fixture: Fixture,
    attempt: AttemptContext,
) -> AgentLoopExit {
    let telemetry = attempt_telemetry(fixture, &attempt);
    runtime
        .execute(attempt, telemetry, InvocationSupervisor::new())
        .await
}

pub(crate) fn execution_receipt(fixture: Fixture, attempt: &AttemptContext) -> ExecutionReceipt {
    ExecutionReceipt {
        ledger: AppendReceipt {
            disposition: AppendDisposition::Committed,
            first_sequence: ConversationSeq::new(1).expect("first sequence"),
            last_sequence: ConversationSeq::new(1).expect("last sequence"),
            committed_at: UtcTimestamp::from_unix_millis(1),
        },
        fence: AttemptFence {
            run_id: fixture.run_id,
            attempt_id: attempt.attempt_id,
            attempt_number: attempt.attempt_number,
            worker_id: attempt.worker_id.clone(),
            lease_id: attempt.lease_id,
            fencing_token: attempt.fencing_token,
            home_epoch: HomeEpoch::new(1).expect("home epoch"),
        },
        activity_kind: musteragent_activity::ActivityKind::Chat,
        lease_expires_at: attempt.lease_expires_at,
        telemetry: Box::new(attempt_telemetry(fixture, attempt)),
    }
}

pub(crate) async fn seed_running_loop(pool: &PgPool) -> (Fixture, AttemptContext) {
    seed_running_loop_inner(pool, None, None).await
}

pub(crate) async fn seed_running_loop_with_input(
    pool: &PgPool,
    content: &dyn musteragent_runtime_content::ContentStore,
    text: &[u8],
) -> (Fixture, AttemptContext) {
    seed_running_loop_inner(pool, Some((content, text)), None).await
}

type SnapshotFixture = (
    Arc<dyn musteragent_runtime_content::ContentStore>,
    musteragent_model::ModelRoute,
    fn(
        musteragent_runtime_snapshot::RuntimeSnapshotContent,
    ) -> musteragent_runtime_snapshot::RuntimeSnapshotContent,
);

pub(crate) async fn seed_running_loop_with_snapshot(
    pool: &PgPool,
    fixture: SnapshotFixture,
) -> (Fixture, AttemptContext) {
    seed_running_loop_inner(pool, None, Some(fixture)).await
}

async fn seed_running_loop_inner(
    pool: &PgPool,
    input: Option<(&dyn musteragent_runtime_content::ContentStore, &[u8])>,
    snapshot: Option<SnapshotFixture>,
) -> (Fixture, AttemptContext) {
    let principal_id = PrincipalId::generate();
    let fixture = Fixture {
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        run_id: RunId::generate(),
        attempt_id: AttemptId::generate(),
        executor_id: ParticipantId::generate(),
        executor_principal_id: PrincipalId::generate(),
        requester_id: ParticipantId::generate(),
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
        principal_id,
    };
    let lease_id = LeaseId::generate();
    let now = database_now(pool).await;
    let lease_expires_at =
        UtcTimestamp::from_unix_millis(now.unix_millis().saturating_add(120_000));
    let mut participant_transaction = pool.begin().await.expect("begin participant fixture");
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, 1, NULL, 1, NULL, 1, 1, $3, $4, $4, NULL, $5)",
    )
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(now.unix_millis())
    .bind(fixture.executor_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed Conversation");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES ($1, $2, $3, 1, $4, NULL, NULL, 'Loop Tester', NULL, $5, NULL)",
    )
    .bind(fixture.requester_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed requester participant");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES ($1, $2, $3, 2, NULL, $4, NULL, 'Loop Executor', NULL, $5, NULL)",
    )
    .bind(fixture.executor_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.executor_principal_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed executor participant");
    participant_transaction
        .commit()
        .await
        .expect("commit participant fixture");
    sqlx::query(
        "INSERT INTO activity.activities (
             activity_id, tenant_id, conversation_id, root_activity_id,
             parent_activity_id, kind, status, policy_snapshot_ref,
             created_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $1, NULL, 1, 2, 'musteragent.legacy.chat@1', $4, NULL)",
    )
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Activity");
    let (encoded, schema) = match snapshot {
        Some((content, route, configure)) => {
            use musteragent_runtime_snapshot::SnapshotCodec;
            let snapshot =
                musteragent_conformance::fixtures::source_selection_snapshot::isolated_snapshot(
                    content,
                    run_context(fixture),
                    route,
                )
                .await;
            let configured = configure(snapshot.content);
            (
                musteragent_wire::ProtobufSnapshotCodec
                    .encode_content(&configured)
                    .expect("seed running loop inner must succeed"),
                configured.schema_version.as_str().to_owned(),
            )
        }
        None => (
            musteragent_conformance::fixtures::model_journal::snapshot_bytes(fixture.run_id),
            musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION.to_owned(),
        ),
    };
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms
         ) VALUES ($1, $2, $3, $4, $8, $5, $6, $7)",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(&encoded)
    .bind(Sha256::digest(&encoded).into_bytes().to_vec())
    .bind(now.unix_millis())
    .bind(schema)
    .execute(pool)
    .await
    .expect("seed RuntimeSnapshot");
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
             1, NULL, 1, NULL, 1, $7, 1, 1, 2,
             2, NULL, NULL, NULL, 2, NULL, NULL, $8, $8, NULL, NULL, 2,
             '00-00000000000000000000000000000001-0000000000000001-00', NULL
         )",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.executor_id.into_uuid())
    .bind(fixture.requester_id.into_uuid())
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Run");
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, 1, 1, 'worker-loop', $4, 1, $5, 1, $6, NULL)",
    )
    .bind(fixture.attempt_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Attempt");
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, 'worker-loop', $4, 1, $5, $6)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Lease");
    sqlx::query(
        "INSERT INTO event_store.ledger_streams (
             tenant_id, conversation_id, home_epoch, last_sequence,
             retained_from_sequence, created_at_ms, updated_at_ms
         ) VALUES ($1, $2, 1, 1, 1, $3, $3)",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Ledger stream");
    let seed_record = LedgerRecordId::generate();
    let seed_payload = if let Some((content, text)) = input {
        let reference = content
            .put(musteragent_runtime_content::PutContent {
                tenant_id: fixture.tenant_id,
                media_type: musteragent_content_ref::MediaType::new("text/plain;charset=utf-8")
                    .expect("seed running loop inner must succeed"),
                bytes: text.to_vec(),
            })
            .await
            .expect("persist admission input");
        ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
            musteragent_conversation::ConversationEvent::MessageAccepted {
                project_task_target: None,
                message_id: musteragent_identity::MessageId::generate(),
                role: musteragent_conversation::MessageRole::User,
                content_ref: reference,
                attachment_refs: vec![],
                requested_skill_refs: vec![],
            },
        )))
    } else {
        run_started_payload(fixture)
    };
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.runtime.record.v1', 1, $2, $3, 1, $4, $5, 1, $6,
             $10, 1, 2, $7, $7, NULL, $8, 'seed:loop', $9
         )",
    )
    .bind(seed_record.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .bind(CorrelationId::generate().into_uuid())
    .bind(Sha256::digest(&seed_payload).into_bytes().to_vec())
    .bind(fixture.requester_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(seed_record.into_uuid())
    .bind(seed_payload.as_slice())
    .bind(Sha256::digest(&seed_payload).into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("seed event");
    sqlx::query(
        "INSERT INTO runtime.loop_checkpoints (
             run_id, tenant_id, conversation_id, attempt_id, iteration, phase,
             model_invocation_id, remaining_tool_call_ids,
             waiting_interaction_id, completed_tool_calls, total_tokens,
             continuation_ref, continuation_invocation_id,
             checkpoint_version, updated_at_ms
         ) VALUES ($1, $2, $3, $4, 0, 1, NULL, '{}', NULL, 0, 0,
                   NULL, NULL, 1, $5)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed checkpoint");
    (
        fixture,
        AttemptContext {
            attempt_id: fixture.attempt_id,
            run_id: fixture.run_id,
            attempt_number: AttemptNumber::new(1).expect("attempt number"),
            worker_id: WorkerId::new("worker-loop").expect("worker"),
            lease_id,
            fencing_token: FencingToken::new(1).expect("fencing"),
            lease_expires_at,
        },
    )
}

pub(crate) async fn seed_parallel_run(pool: &PgPool, base: Fixture) -> (Fixture, AttemptContext) {
    let fixture = Fixture {
        tenant_id: base.tenant_id,
        conversation_id: base.conversation_id,
        activity_id: ActivityId::generate(),
        run_id: RunId::generate(),
        attempt_id: AttemptId::generate(),
        executor_id: base.executor_id,
        executor_principal_id: base.executor_principal_id,
        requester_id: base.requester_id,
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
        principal_id: base.principal_id,
    };
    let lease_id = LeaseId::generate();
    let now = database_now(pool).await;
    let lease_expires_at =
        UtcTimestamp::from_unix_millis(now.unix_millis().saturating_add(120_000));
    sqlx::query(
        "INSERT INTO activity.activities (
             activity_id, tenant_id, conversation_id, root_activity_id,
             parent_activity_id, kind, status, policy_snapshot_ref,
             created_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, $1, NULL, 3, 2, 'musteragent.legacy.team-member@1', $4, NULL)",
    )
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel Activity");
    let encoded = musteragent_conformance::fixtures::model_journal::snapshot_bytes(fixture.run_id);
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms
         ) VALUES ($1, $2, $3, $4, 'musteragent.runtime.snapshot.v17', $5, $6, $7)",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(&encoded)
    .bind(Sha256::digest(&encoded).into_bytes().to_vec())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel RuntimeSnapshot");
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
             $1, $2, $3, $4, 3, $5, $6, $7, NULL, 1, NULL, NULL,
             1, NULL, 1, NULL, 1, $8, 2, 1, 2,
             2, NULL, NULL, NULL, 2, NULL, NULL, $9, $9, NULL, NULL, 2,
             '00-00000000000000000000000000000002-0000000000000002-00', NULL
         )",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.executor_id.into_uuid())
    .bind(fixture.requester_id.into_uuid())
    .bind(base.run_id.into_uuid())
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel Run");
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, 1, 1, 'worker-parallel', $4, 1, $5, 1, $6, NULL)",
    )
    .bind(fixture.attempt_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel Attempt");
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, 'worker-parallel', $4, 1, $5, $6)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(lease_expires_at.unix_millis())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel Lease");
    sqlx::query(
        "INSERT INTO runtime.loop_checkpoints (
             run_id, tenant_id, conversation_id, attempt_id, iteration, phase,
             model_invocation_id, remaining_tool_call_ids,
             waiting_interaction_id, completed_tool_calls, total_tokens,
             continuation_ref, continuation_invocation_id,
             checkpoint_version, updated_at_ms
         ) VALUES ($1, $2, $3, $4, 0, 1, NULL, '{}', NULL, 0, 0,
                   NULL, NULL, 1, $5)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed parallel checkpoint");
    let seed_record = LedgerRecordId::generate();
    let seed_payload = run_started_payload(fixture);
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.runtime.record.v1', 1, $2, $3, 2, $4, $5, 1, $6,
             $10, 1, 2, $7, $7, NULL, $8, 'seed:parallel-loop', $9
         )",
    )
    .bind(seed_record.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(now.unix_millis())
    .bind(CorrelationId::generate().into_uuid())
    .bind(Sha256::digest(&seed_payload).into_bytes().to_vec())
    .bind(fixture.requester_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed parallel Ledger record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(seed_record.into_uuid())
    .bind(seed_payload.as_slice())
    .bind(Sha256::digest(&seed_payload).into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("seed parallel domain event");
    sqlx::query(
        "UPDATE event_store.ledger_streams
         SET last_sequence = 2, updated_at_ms = $3
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("advance parallel Ledger stream");
    (
        fixture,
        AttemptContext {
            attempt_id: fixture.attempt_id,
            run_id: fixture.run_id,
            attempt_number: AttemptNumber::new(1).expect("attempt number"),
            worker_id: WorkerId::new("worker-parallel").expect("worker"),
            lease_id,
            fencing_token: FencingToken::new(1).expect("fencing"),
            lease_expires_at,
        },
    )
}
