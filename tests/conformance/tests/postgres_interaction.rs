use std::sync::Arc;

use musteragent_approval::{
    ApprovalDecision, Interaction, InteractionClosure, InteractionEvent, InteractionEventResponse,
    InteractionKind, InteractionResponseRef, InteractionStatus, SecureResponseRef,
};
use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    ActivityId, Actor, AttemptId, AttemptNumber, CapabilityProviderId, ConversationId,
    CorrelationId, DottedKey, FencingToken, HomeEpoch, IdempotencyKey, InteractionId, InvocationId,
    LeaseId, LedgerOffset, LedgerRecordId, OutboxMessageId, ParticipantId, PrincipalId,
    PrincipalKind, RunId, RunSeq, RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp,
    VersionedKey,
};
use musteragent_ledger::{
    AppendRequest, AttemptFence, CancelMode, CancelRunBatch, DomainEvent, LedgerPayload,
    ProposedOutbox, ProposedRecord,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{
    PostgresContentStore, PostgresInteractionError, PostgresInteractionStore, PostgresLedgerStore,
    PostgresRuntimeApplicationStore, PostgresSecurityAuditError, PostgresSecurityAuditPipeline,
    PostgresSecurityAuditTestStore, PostgresToolInvocationStore, PostgresTransactionError,
};
use musteragent_run::WorkerId;
use musteragent_runtime_audit::{
    ApprovedRetentionPolicies, SecurityAuditFact, SecurityAuditProducer,
    SecurityAuditProducerContext, SecurityAuditTraceId,
};
use musteragent_runtime_capability::{ToolInvocationStore, ToolPersistenceDisposition};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{CancelReason, DataRetentionPolicyRef};
use musteragent_runtime_interaction::{
    CloseInteractionCommit, InteractionCommitDisposition, InteractionRuntimeError,
    InteractionSemanticRecords, InteractionStore, RequestInteractionCommit,
    RespondInteractionCommit, response_hash,
};
use musteragent_runtime_session::RuntimeApplicationStore;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufSnapshotCodec;
use sqlx::PgPool;

type CancelledInteractionRow = (
    i16,
    i16,
    i16,
    i16,
    Option<i16>,
    Option<uuid::Uuid>,
    Option<uuid::Uuid>,
    bool,
    bool,
    i16,
    i64,
    String,
);

#[derive(Debug, Copy, Clone)]
struct Fixture {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    activity_id: ActivityId,
    run_id: RunId,
    principal_id: PrincipalId,
    participant_id: ParticipantId,
    attempt_id: AttemptId,
    invocation_id: InvocationId,
    interaction_id: InteractionId,
}

fn keyring() -> Arc<OpaqueRefKeyring> {
    let key_id = DottedKey::new("runtime.interaction.test").expect("key ID");
    Arc::new(
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [73_u8; 32])])
            .expect("opaque reference keyring"),
    )
}

async fn database_now(pool: &PgPool) -> UtcTimestamp {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .expect("database time");
    UtcTimestamp::from_unix_millis(millis)
}

async fn stored_content(
    pool: &PgPool,
    fixture: Fixture,
    keys: Arc<OpaqueRefKeyring>,
    bytes: &[u8],
) -> ContentRef {
    PostgresContentStore::new(pool.clone(), Arc::new(MemoryBlobs::in_memory()), keys)
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: bytes.to_vec(),
        })
        .await
        .expect("store content")
}

async fn seed_running_interaction(
    pool: &PgPool,
    keys: Arc<OpaqueRefKeyring>,
) -> (Fixture, ContentRef, AttemptFence) {
    let fixture = Fixture {
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        run_id: RunId::generate(),
        principal_id: PrincipalId::generate(),
        participant_id: ParticipantId::generate(),
        attempt_id: AttemptId::generate(),
        invocation_id: InvocationId::generate(),
        interaction_id: InteractionId::generate(),
    };
    let snapshot_id = RuntimeSnapshotId::generate();
    let executor_id = ParticipantId::generate();
    let now = database_now(pool).await;
    let lease_id = LeaseId::generate();
    let prompt = stored_content(
        pool,
        fixture,
        keys.clone(),
        br#"{"prompt":"Connect your provider account"}"#,
    )
    .await;
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
    .bind(fixture.principal_id.into_uuid())
    .bind(now.unix_millis())
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
             ($1, $2, $3, 1, $4, NULL, NULL, 'Tester', NULL, $5, NULL),
             ($6, $2, $3, 2, NULL,
              (SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'),
              NULL, 'Kernel', NULL, $5, NULL)",
    )
    .bind(fixture.participant_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .bind(now.unix_millis())
    .bind(executor_id.into_uuid())
    .execute(&mut *participant_transaction)
    .await
    .expect("seed participant");
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
    sqlx::query(
        "INSERT INTO capability.runtime_snapshots (
             runtime_snapshot_id, tenant_id, conversation_id, run_id,
             schema_version, encoded_snapshot, content_hash, created_at_ms
         ) VALUES ($1, $2, $3, $4, 'musteragent.runtime.snapshot.v17', $5, $6, $7)",
    )
    .bind(snapshot_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(b"snapshot".as_slice())
    .bind(Sha256::digest(b"snapshot").into_bytes().as_slice())
    .bind(now.unix_millis())
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
    .bind(executor_id.into_uuid())
    .bind(fixture.participant_id.into_uuid())
    .bind(snapshot_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed running Run");
    sqlx::query(
        "INSERT INTO runtime.attempts (
             attempt_id, tenant_id, run_id, attempt_number, trigger, worker_id,
             lease_id, fencing_token, lease_expires_at_ms, status,
             claimed_at_ms, finished_at_ms
         ) VALUES ($1, $2, $3, 1, 1, 'worker-interaction', $4, 1, $5, 1, $6, NULL)",
    )
    .bind(fixture.attempt_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(now.unix_millis() + 60_000)
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Attempt");
    sqlx::query(
        "INSERT INTO runtime.leases (
             run_id, tenant_id, attempt_id, worker_id, lease_id,
             fencing_token, expires_at_ms, renewed_at_ms
         ) VALUES ($1, $2, $3, 'worker-interaction', $4, 1, $5, $6)",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(lease_id.into_uuid())
    .bind(now.unix_millis() + 60_000)
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed lease");
    sqlx::query(
        "INSERT INTO runtime.loop_checkpoints (
             run_id, tenant_id, conversation_id, attempt_id, iteration, phase,
             model_invocation_id, remaining_tool_call_ids,
             waiting_interaction_id, completed_tool_calls, total_tokens,
             continuation_ref, continuation_invocation_id,
             checkpoint_version, updated_at_ms
         ) VALUES (
             $1, $2, $3, $4, 1, 3, $5,
             ARRAY['call-request-credential']::text[],
             NULL, 0, 0, NULL, NULL, 1, $6
         )",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.attempt_id.into_uuid())
    .bind(fixture.invocation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Agent Loop checkpoint");
    sqlx::query(
        "INSERT INTO runtime.pending_tool_calls (
             run_id, invocation_id, tool_call_id, wire_name,
             arguments_json, ordinal, completed_at_ms
         ) VALUES (
             $1, $2, 'call-request-credential',
             'interaction_request', '{}'::jsonb, 0, NULL
         )",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.invocation_id.into_uuid())
    .execute(pool)
    .await
    .expect("seed pending Tool call");
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
    let seed_payload = b"run.started";
    let seed_record = LedgerRecordId::generate();
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.legacy.event.v1', 1, $2, $3, 1, $4, $5, 1, $6,
             NULL, 1, 4, $7, $7, NULL, $8, 'seed:interaction', $9
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
    .bind(Sha256::digest(seed_payload).into_bytes().as_slice())
    .execute(pool)
    .await
    .expect("seed Ledger record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(seed_record.into_uuid())
    .bind(seed_payload.as_slice())
    .bind(Sha256::digest(seed_payload).into_bytes().as_slice())
    .execute(pool)
    .await
    .expect("seed DomainEvent");

    let tool_store = PostgresToolInvocationStore::new(pool.clone(), keys);
    let tool = ToolInvocation {
        invocation_id: fixture.invocation_id,
        run_id: fixture.run_id,
        requested_by_attempt_id: fixture.attempt_id,
        execution_attempt_id: None,
        tool_call_id: ToolCallId::new("call-request-credential").expect("Tool call ID"),
        tool_key: ToolKey::from_versioned_key(
            VersionedKey::new("interaction.request@1").expect("Tool key"),
        ),
        capability_provider_id: CapabilityProviderId::generate(),
        status: ToolInvocationStatus::Requested,
        effect: Effect::WriteReversible,
        approval: ApprovalRequirement::Never,
        idempotency_key: IdempotencyKey::new("tool:interaction:request").expect("idempotency"),
        request_ref: prompt.clone(),
        result_ref: None,
        interaction_id: None,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(now.unix_millis() + 50_000) },
    };
    assert_eq!(
        tool_store
            .begin(
                fixture.tenant_id,
                fixture.conversation_id,
                tool,
                ToolRetryContract::NotReplayable,
            )
            .await
            .expect("begin Interaction Tool"),
        ToolPersistenceDisposition::Applied
    );

    let fence = AttemptFence {
        run_id: fixture.run_id,
        attempt_id: fixture.attempt_id,
        attempt_number: AttemptNumber::new(1).expect("attempt number"),
        worker_id: WorkerId::new("worker-interaction").expect("worker"),
        lease_id,
        fencing_token: FencingToken::new(1).expect("fencing token"),
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
    };
    (fixture, prompt, fence)
}

fn records(
    fixture: Fixture,
    ids: &[LedgerRecordId],
    first_run_sequence: u64,
    producer: Producer,
    payload_prefix: &str,
    participant_id: Option<ParticipantId>,
    occurred_at: UtcTimestamp,
) -> Vec<ProposedRecord> {
    ids.iter()
        .enumerate()
        .map(|(index, record_id)| {
            let payload = format!("{payload_prefix}:{index}").into_bytes();
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.legacy.event.v1").expect("schema"),
                record_id: *record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(fixture.activity_id),
                run_id: Some(fixture.run_id),
                run_seq: Some(
                    RunSeq::new(
                        first_run_sequence
                            .checked_add(u64::try_from(index).expect("bounded index"))
                            .expect("bounded sequence"),
                    )
                    .expect("run sequence"),
                ),
                attempt_id: None,
                participant_id,
                producer,
                occurred_at,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&payload),
                payload,
            }
        })
        .collect()
}

fn append(
    fixture: Fixture,
    offset: u64,
    key: &str,
    records: Vec<ProposedRecord>,
    dispatch: bool,
) -> AppendRequest {
    let outbox = if dispatch {
        let payload = b"dispatch-after-interaction".to_vec();
        vec![ProposedOutbox {
            message_id: OutboxMessageId::generate(),
            topic: DottedKey::new("run.dispatch").expect("topic"),
            payload_hash: Sha256::digest(&payload),
            payload,
        }]
    } else {
        Vec::new()
    };
    AppendRequest {
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
        expected_offset: LedgerOffset::new(offset),
        idempotency_key: IdempotencyKey::new(key).expect("idempotency"),
        command_fingerprint: Sha256::digest(format!("fingerprint:{key}")),
        records,
        outbox,
    }
}

#[tokio::test]
async fn secure_response_is_atomic_secret_safe_retryable_and_tenant_isolated() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = keyring();
    let (fixture, prompt, fence) = seed_running_interaction(&pool, keys.clone()).await;
    let now = database_now(&pool).await;
    let interaction = Interaction {
        interaction_id: fixture.interaction_id,
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        run_id: fixture.run_id,
        requested_by_invocation_id: fixture.invocation_id,
        kind: InteractionKind::Credential,
        status: InteractionStatus::Pending,
        prompt_ref: prompt,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new("interaction.secure-response@1").expect("schema"),
            schema_hash: Sha256::digest(b"interaction-secure-response-schema"),
        },
        response_ref: None,
        expires_at: Some(UtcTimestamp::from_unix_millis(now.unix_millis() + 30_000)),
        requested_at: now,
        responded_by: None,
        responded_at: None,
    };
    let request_ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    let request = RequestInteractionCommit {
        interaction: interaction.clone(),
        fence,
        expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
        append: append(
            fixture,
            1,
            "interaction:request",
            records(
                fixture,
                &request_ids,
                2,
                Producer::Worker,
                "interaction.requested",
                None,
                now,
            ),
            false,
        ),
        semantic_records: InteractionSemanticRecords {
            interaction_record_id: request_ids[0],
            run_record_id: Some(request_ids[1]),
            tool_record_id: Some(request_ids[2]),
        },
    };
    let security_audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
    let ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let store = PostgresInteractionStore::with_security_audit(
        pool.clone(),
        keys.clone(),
        ledger_codec.clone(),
        Arc::clone(&security_audit),
    );
    let receipt = store
        .request(request.clone())
        .await
        .expect("request Interaction");
    assert_eq!(receipt.disposition, InteractionCommitDisposition::Committed);
    let state: (i16, i16, i16, i64, String) = sqlx::query_as(
        "SELECT run.status, attempt.status, tool.status,
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id),
                run.last_run_sequence::text
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read suspended state");
    assert_eq!(state, (3, 2, 2, 0, "4".into()));

    let restarted = PostgresInteractionStore::with_security_audit(
        pool.clone(),
        keys.clone(),
        ledger_codec.clone(),
        security_audit,
    );
    let secure = SecureResponseRef::new("grant_ref_01H_SECRET_7").expect("secure ref");
    let response = InteractionResponseRef::Secure(secure.clone());
    let response_ids = [LedgerRecordId::generate()];
    let response_correlation_id = CorrelationId::generate();
    let responded_at = UtcTimestamp::from_unix_millis(now.unix_millis() + 1);
    let mut response_records = records(
        fixture,
        &response_ids,
        5,
        Producer::RuntimeApi,
        "interaction.responded.secure-accepted",
        Some(fixture.participant_id),
        responded_at,
    );
    for record in &mut response_records {
        record.correlation_id = response_correlation_id;
    }
    let response_event = InteractionEvent::Responded {
        interaction_id: fixture.interaction_id,
        responded_by: fixture.principal_id,
        response: InteractionEventResponse::SecureAccepted,
        responded_at,
    };
    response_records[0].payload = ledger_codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(response_event.clone()),
    ));
    response_records[0].payload_hash = Sha256::digest(&response_records[0].payload);
    let respond = RespondInteractionCommit {
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        interaction_id: fixture.interaction_id,
        response_hash: response_hash(&response),
        response,
        responded_at,
        expected_last_run_sequence: RunSeq::new(4).expect("run sequence"),
        append: append(fixture, 4, "interaction:respond", response_records, true),
    };

    let mut leaked = respond.clone();
    leaked.append.records[0].payload = secure.as_str().as_bytes().to_vec();
    leaked.append.records[0].payload_hash = Sha256::digest(&leaked.append.records[0].payload);
    assert!(matches!(
        restarted.respond(leaked).await,
        Err(PostgresInteractionError::Contract(
            InteractionRuntimeError::SecretLeak
        ))
    ));

    let mut wrong_event = respond.clone();
    wrong_event.append.records[0].payload = ledger_codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Expired {
            interaction_id: fixture.interaction_id,
            expired_at: responded_at,
        }),
    ));
    wrong_event.append.records[0].payload_hash =
        Sha256::digest(&wrong_event.append.records[0].payload);
    assert!(matches!(
        restarted.respond(wrong_event).await,
        Err(PostgresInteractionError::SemanticLedgerMismatch)
    ));

    let mut wrong_interaction = respond.clone();
    wrong_interaction.append.records[0].payload = ledger_codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: InteractionId::generate(),
            responded_by: fixture.principal_id,
            response: InteractionEventResponse::SecureAccepted,
            responded_at,
        }),
    ));
    wrong_interaction.append.records[0].payload_hash =
        Sha256::digest(&wrong_interaction.append.records[0].payload);
    assert!(matches!(
        restarted.respond(wrong_interaction).await,
        Err(PostgresInteractionError::SemanticLedgerMismatch)
    ));

    let mut wrong_decision = respond.clone();
    wrong_decision.append.records[0].payload = ledger_codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: fixture.interaction_id,
            responded_by: fixture.principal_id,
            response: InteractionEventResponse::Approval(ApprovalDecision::Deny),
            responded_at,
        }),
    ));
    wrong_decision.append.records[0].payload_hash =
        Sha256::digest(&wrong_decision.append.records[0].payload);
    assert!(matches!(
        restarted.respond(wrong_decision).await,
        Err(PostgresInteractionError::SemanticLedgerMismatch)
    ));

    let mut wrong_hash = respond.clone();
    wrong_hash.response_hash = Sha256::digest(b"wrong canonical response hash");
    assert!(matches!(
        restarted.respond(wrong_hash).await,
        Err(PostgresInteractionError::Contract(
            InteractionRuntimeError::IdentityMismatch
        ))
    ));

    let mut forbidden = respond.clone();
    let forbidden_principal = PrincipalId::generate();
    forbidden.actor.principal_id = forbidden_principal;
    forbidden.append.records[0].payload = ledger_codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: fixture.interaction_id,
            responded_by: forbidden_principal,
            response: InteractionEventResponse::SecureAccepted,
            responded_at,
        }),
    ));
    forbidden.append.records[0].payload_hash = Sha256::digest(&forbidden.append.records[0].payload);
    assert!(matches!(
        restarted.respond(forbidden).await,
        Err(PostgresInteractionError::ActorForbidden)
    ));

    sqlx::raw_sql(
        "CREATE FUNCTION approval.reject_interaction_response()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected interaction response failure'; END $$;
         CREATE TRIGGER reject_interaction_response
         BEFORE INSERT ON approval.interaction_responses
         FOR EACH ROW EXECUTE FUNCTION approval.reject_interaction_response();",
    )
    .execute(&pool)
    .await
    .expect("install failure trigger");
    assert!(restarted.respond(respond.clone()).await.is_err());
    let rolled_back: (i16, i16, String) = sqlx::query_as(
        "SELECT interaction.status, run.status, stream.last_sequence::text
         FROM approval.interactions AS interaction
         JOIN runtime.runs AS run ON run.run_id = interaction.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.conversation_id = interaction.conversation_id
         WHERE interaction.interaction_id = $1",
    )
    .bind(fixture.interaction_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back state");
    assert_eq!(rolled_back, (1, 3, "4".into()));
    let ghost_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND action = 11
           AND source_ledger_record_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(response_ids[0].into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count rolled-back Interaction Audit ghosts");
    assert_eq!(ghost_audits, 0, "failed response must roll Audit back");
    sqlx::raw_sql(
        "DROP TRIGGER reject_interaction_response ON approval.interaction_responses;
         DROP FUNCTION approval.reject_interaction_response();",
    )
    .execute(&pool)
    .await
    .expect("remove failure trigger");

    let committed = restarted
        .respond(respond.clone())
        .await
        .expect("respond Interaction");
    assert_eq!(
        committed.disposition,
        InteractionCommitDisposition::Committed
    );
    let loaded = restarted
        .load(fixture.tenant_id, fixture.interaction_id)
        .await
        .expect("load response");
    assert_eq!(loaded.status, InteractionStatus::Responded);
    assert_eq!(
        loaded.response_ref,
        Some(InteractionResponseRef::Secure(secure.clone()))
    );
    assert!(matches!(
        restarted
            .load(TenantId::generate(), fixture.interaction_id)
            .await,
        Err(PostgresInteractionError::NotFound)
    ));

    let stored_response: (Option<String>, Vec<u8>) = sqlx::query_as(
        "SELECT secure_response_ref, secure_response_ref_protected
         FROM approval.interaction_responses WHERE interaction_id = $1",
    )
    .bind(fixture.interaction_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read encrypted response");
    assert!(stored_response.0.is_none());
    assert!(
        !stored_response
            .1
            .windows(secure.as_str().len())
            .any(|window| window == secure.as_str().as_bytes())
    );
    let persisted_payloads: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT event.encoded_event
         FROM event_store.domain_events AS event
         JOIN event_store.ledger_records AS record ON record.record_id = event.record_id
         WHERE record.conversation_id = $1",
    )
    .bind(fixture.conversation_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read Ledger payloads");
    assert!(persisted_payloads.iter().all(|payload| {
        !payload
            .windows(secure.as_str().len())
            .any(|window| window == secure.as_str().as_bytes())
    }));
    let resumed: (i16, String, i64) = sqlx::query_as(
        "SELECT run.status, run.last_run_sequence::text,
                (SELECT count(*) FROM event_store.outbox
                 WHERE conversation_id = run.conversation_id AND topic = 'run.dispatch')
         FROM runtime.runs AS run WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read resumed state");
    assert_eq!(resumed, (1, "5".into(), 1));
    let mut replay_drift = respond.clone();
    replay_drift.append.records[0].correlation_id = CorrelationId::generate();
    assert!(matches!(
        restarted.respond(replay_drift).await,
        Err(PostgresInteractionError::IdempotencyConflict)
    ));
    assert_eq!(
        restarted
            .respond(respond)
            .await
            .expect("replay response")
            .disposition,
        InteractionCommitDisposition::Duplicate
    );
    let (audit_rows, source_rows, content_rows, trace): (i64, i64, i64, String) = sqlx::query_as(
        "SELECT count(DISTINCT audit.audit_record_id),
                    count(DISTINCT source.record_id),
                    count(*) FILTER (
                        WHERE audit.detail_content_id IS NOT NULL
                           OR resource.content_id IS NOT NULL
                    ),
                    min(encode(audit.trace_id, 'hex'))
             FROM security_audit.records AS audit
             JOIN event_store.ledger_records AS source
               ON source.tenant_id = audit.tenant_id
              AND source.record_id = audit.source_ledger_record_id
             JOIN runtime.runs AS run
               ON run.tenant_id = source.tenant_id
              AND run.run_id = source.run_id
             LEFT JOIN security_audit.resource_refs AS resource
               ON resource.tenant_id = audit.tenant_id
              AND resource.audit_record_id = audit.audit_record_id
             WHERE audit.tenant_id = $1
               AND audit.action = 11
               AND audit.outcome = 1
               AND audit.source_ledger_record_id = $2
               AND audit.actor_principal_id = $3",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(response_ids[0].into_uuid())
    .bind(fixture.principal_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("join Interaction Audit to Ledger and Run");
    assert_eq!(audit_rows, 1, "response replay is audited exactly once");
    assert_eq!(source_rows, 1);
    assert_eq!(content_rows, 0, "secure response must not enter Audit");
    assert_eq!(trace, "00000000000000000000000000000001");
    let mut trace_bytes = vec![0_u8; 16];
    trace_bytes[15] = 1;
    let wrong_proposal = SecurityAuditProducer::new(
        DataRetentionPolicyRef::new("security.audit.standard@1").expect("retention policy"),
    )
    .propose(
        SecurityAuditProducerContext {
            actor: Actor {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
                principal_kind: PrincipalKind::User,
            },
            correlation_id: response_correlation_id,
            trace_id: Some(SecurityAuditTraceId::from_bytes(trace_bytes).expect("trace identity")),
            occurred_at: responded_at,
        },
        SecurityAuditFact::InteractionResponded {
            run_id: fixture.run_id,
            interaction_id: fixture.interaction_id,
            source_ledger_record_id: request_ids[0],
        },
    )
    .expect("construct same-correlation Interaction cross-record proposal");
    let raw = PostgresSecurityAuditTestStore::new(
        pool.clone(),
        Arc::new(
            ApprovedRetentionPolicies::new([DataRetentionPolicyRef::new(
                "security.audit.standard@1",
            )
            .expect("retention policy")])
            .expect("approved policies"),
        ),
    );
    let cross_record_result = raw.append(wrong_proposal).await;
    assert!(
        matches!(
            cross_record_result,
            Err(PostgresSecurityAuditError::Transaction(
                PostgresTransactionError::Database(_)
            ))
        ),
        "unexpected cross-record result: {cross_record_result:?}"
    );
    let cross_record_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM security_audit.records
         WHERE tenant_id = $1 AND action = 11 AND source_ledger_record_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(request_ids[0].into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count rejected Interaction cross-record");
    assert_eq!(
        cross_record_rows, 0,
        "same-Run same-correlation record cannot replace exact response source"
    );
    assert_eq!(
        restarted
            .request(request)
            .await
            .expect("replay request after response")
            .disposition,
        InteractionCommitDisposition::Duplicate
    );
    pool.close().await;
}

#[tokio::test]
async fn pending_interaction_cancellation_closes_the_bound_tool_and_run_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = keyring();
    let (fixture, prompt, fence) = seed_running_interaction(&pool, keys.clone()).await;
    let now = database_now(&pool).await;
    let interaction = Interaction {
        interaction_id: fixture.interaction_id,
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        run_id: fixture.run_id,
        requested_by_invocation_id: fixture.invocation_id,
        kind: InteractionKind::Selection,
        status: InteractionStatus::Pending,
        prompt_ref: prompt,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new("interaction.selection@1").expect("schema"),
            schema_hash: Sha256::digest(b"interaction-selection-schema"),
        },
        response_ref: None,
        expires_at: Some(UtcTimestamp::from_unix_millis(now.unix_millis() + 30_000)),
        requested_at: now,
        responded_by: None,
        responded_at: None,
    };
    let request_ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    PostgresInteractionStore::with_security_audit(
        pool.clone(),
        keys,
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone())),
    )
    .request(RequestInteractionCommit {
        interaction,
        fence,
        expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
        append: append(
            fixture,
            1,
            "interaction:cancel:request",
            records(
                fixture,
                &request_ids,
                2,
                Producer::Worker,
                "interaction.cancel.requested",
                None,
                now,
            ),
            false,
        ),
        semantic_records: InteractionSemanticRecords {
            interaction_record_id: request_ids[0],
            run_record_id: Some(request_ids[1]),
            tool_record_id: Some(request_ids[2]),
        },
    })
    .await
    .expect("request pending Interaction");

    let waiting_binding: (Option<uuid::Uuid>, Option<uuid::Uuid>, uuid::Uuid) = sqlx::query_as(
        "SELECT run.wait_interaction_id, run.wait_invocation_id,
                interaction.requested_by_invocation_id
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction
           ON interaction.tenant_id = run.tenant_id
          AND interaction.conversation_id = run.conversation_id
          AND interaction.run_id = run.run_id
          AND interaction.interaction_id = run.wait_interaction_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read pending Interaction wait binding");
    assert_eq!(waiting_binding.0, Some(fixture.interaction_id.into_uuid()));
    assert_eq!(waiting_binding.1, Some(waiting_binding.2));

    let authority =
        PostgresRuntimeApplicationStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .authorize_cancel_run(
                Actor {
                    tenant_id: fixture.tenant_id,
                    principal_id: fixture.principal_id,
                    principal_kind: PrincipalKind::User,
                },
                fixture.run_id,
                CorrelationId::generate(),
            )
            .await
            .expect("authorize pending Interaction cancellation");
    assert_eq!(authority.mode, CancelMode::CancelWithoutAttempt);
    assert_eq!(
        authority.pending_interaction_id,
        Some(fixture.interaction_id)
    );
    assert_eq!(
        authority.pending_tool_invocation_id,
        Some(fixture.invocation_id)
    );

    let cancellation_ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    let receipt = PostgresLedgerStore::new(pool.clone())
        .cancel_run(CancelRunBatch {
            append: append(
                fixture,
                authority.current_offset.get(),
                "interaction:cancel",
                records(
                    fixture,
                    &cancellation_ids,
                    authority
                        .last_run_sequence
                        .get()
                        .checked_add(1)
                        .expect("bounded run sequence"),
                    Producer::RuntimeApi,
                    "interaction.cancelled",
                    Some(authority.requested_by_participant_id),
                    now,
                ),
                false,
            ),
            run_id: authority.run_id,
            activity_id: authority.activity_id,
            expected_last_run_sequence: authority.last_run_sequence,
            mode: authority.mode,
            reason: CancelReason::UserRequested,
            pending_interaction_id: authority.pending_interaction_id,
            pending_tool_invocation_id: authority.pending_tool_invocation_id,
            unresolved_tool_invocation_id: None,
            descendants: Vec::new(),
        })
        .await
        .expect("cancel pending Interaction");
    assert!(receipt.terminal);

    let cancelled: CancelledInteractionRow = sqlx::query_as(
        "SELECT run.status, activity.status, interaction.status, tool.status,
                run.wait_reason, run.wait_interaction_id, run.wait_invocation_id,
                interaction.closed_at_ms IS NOT NULL,
                tool.finished_at_ms IS NOT NULL,
                checkpoint.phase,
                (SELECT count(*) FROM runtime.pending_tool_calls
                 WHERE run_id = run.run_id AND completed_at_ms IS NULL),
                stream.last_sequence::text
         FROM runtime.runs AS run
         JOIN activity.activities AS activity
           ON activity.activity_id = run.activity_id
         JOIN approval.interactions AS interaction
           ON interaction.run_id = run.run_id
          AND interaction.interaction_id = $2
         JOIN tool.tool_invocations AS tool
           ON tool.run_id = run.run_id
          AND tool.invocation_id = $3
         JOIN runtime.loop_checkpoints AS checkpoint
           ON checkpoint.run_id = run.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.interaction_id.into_uuid())
    .bind(fixture.invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read atomically cancelled Interaction wait");
    assert_eq!(
        cancelled,
        (7, 6, 4, 7, None, None, None, true, true, 7, 0, "9".into())
    );
    pool.close().await;
}

#[tokio::test]
async fn approval_response_binds_exact_typed_ledger_decision_and_rolls_back_on_audit_failure() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = keyring();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, prompt, fence) = seed_running_interaction(&pool, keys.clone()).await;
    let requested_at = database_now(&pool).await;
    let interaction = Interaction {
        interaction_id: fixture.interaction_id,
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        run_id: fixture.run_id,
        requested_by_invocation_id: fixture.invocation_id,
        kind: InteractionKind::Approval,
        status: InteractionStatus::Pending,
        prompt_ref: prompt,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new("interaction.approval@1").expect("schema"),
            schema_hash: Sha256::digest(b"interaction-approval-schema"),
        },
        response_ref: None,
        expires_at: Some(UtcTimestamp::from_unix_millis(
            requested_at.unix_millis() + 30_000,
        )),
        requested_at,
        responded_by: None,
        responded_at: None,
    };
    let request_ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    let request = RequestInteractionCommit {
        interaction,
        fence,
        expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
        append: append(
            fixture,
            1,
            "interaction:approval:request",
            records(
                fixture,
                &request_ids,
                2,
                Producer::Worker,
                "interaction.approval.requested",
                None,
                requested_at,
            ),
            false,
        ),
        semantic_records: InteractionSemanticRecords {
            interaction_record_id: request_ids[0],
            run_record_id: Some(request_ids[1]),
            tool_record_id: Some(request_ids[2]),
        },
    };
    let store = PostgresInteractionStore::with_security_audit(
        pool.clone(),
        keys,
        codec.clone(),
        Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone())),
    );
    store.request(request).await.expect("request Approval");

    let responded_at = UtcTimestamp::from_unix_millis(requested_at.unix_millis() + 1);
    let response = InteractionResponseRef::Approval(ApprovalDecision::Approve);
    let response_id = LedgerRecordId::generate();
    let mut response_record = records(
        fixture,
        &[response_id],
        5,
        Producer::RuntimeApi,
        "interaction.approval.responded",
        Some(fixture.participant_id),
        responded_at,
    )
    .remove(0);
    response_record.payload = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Interaction(
        InteractionEvent::Responded {
            interaction_id: fixture.interaction_id,
            responded_by: fixture.principal_id,
            response: InteractionEventResponse::Approval(ApprovalDecision::Approve),
            responded_at,
        },
    )));
    response_record.payload_hash = Sha256::digest(&response_record.payload);
    let respond = RespondInteractionCommit {
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        interaction_id: fixture.interaction_id,
        response_hash: response_hash(&response),
        response,
        responded_at,
        expected_last_run_sequence: RunSeq::new(4).expect("run sequence"),
        append: append(
            fixture,
            4,
            "interaction:approval:respond",
            vec![response_record],
            true,
        ),
    };

    let mut wrong_decision = respond.clone();
    wrong_decision.append.records[0].payload = codec.encode(LedgerPayload::DomainEvent(
        DomainEvent::Interaction(InteractionEvent::Responded {
            interaction_id: fixture.interaction_id,
            responded_by: fixture.principal_id,
            response: InteractionEventResponse::Approval(ApprovalDecision::Deny),
            responded_at,
        }),
    ));
    wrong_decision.append.records[0].payload_hash =
        Sha256::digest(&wrong_decision.append.records[0].payload);
    assert!(matches!(
        store.respond(wrong_decision).await,
        Err(PostgresInteractionError::SemanticLedgerMismatch)
    ));

    sqlx::raw_sql(
        "CREATE FUNCTION security_audit.reject_approval_audit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 1 THEN
                 RAISE EXCEPTION 'injected Approval Audit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_approval_audit
         BEFORE INSERT ON security_audit.records
         FOR EACH ROW EXECUTE FUNCTION security_audit.reject_approval_audit();",
    )
    .execute(&pool)
    .await
    .expect("install Audit failure trigger");
    assert!(store.respond(respond.clone()).await.is_err());
    let rolled_back: (i16, i16, String, i64, i64) = sqlx::query_as(
        "SELECT interaction.status, run.status, stream.last_sequence::text,
                (SELECT count(*) FROM approval.interaction_responses
                 WHERE interaction_id = interaction.interaction_id),
                (SELECT count(*) FROM security_audit.records
                 WHERE tenant_id = interaction.tenant_id AND action = 1)
         FROM approval.interactions AS interaction
         JOIN runtime.runs AS run ON run.run_id = interaction.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.conversation_id = interaction.conversation_id
         WHERE interaction.interaction_id = $1",
    )
    .bind(fixture.interaction_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read Approval rollback state");
    assert_eq!(rolled_back, (1, 3, "4".into(), 0, 0));
    sqlx::raw_sql(
        "DROP TRIGGER reject_approval_audit ON security_audit.records;
         DROP FUNCTION security_audit.reject_approval_audit();",
    )
    .execute(&pool)
    .await
    .expect("remove Audit failure trigger");

    assert_eq!(
        store
            .respond(respond.clone())
            .await
            .expect("commit Approval response")
            .disposition,
        InteractionCommitDisposition::Committed
    );
    assert_eq!(
        store
            .respond(respond)
            .await
            .expect("replay Approval response")
            .disposition,
        InteractionCommitDisposition::Duplicate
    );
    let binding: (i64, i16, i16, i16, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT count(*) OVER (), audit.action, audit.outcome,
                response.response_ledger_event_kind,
                response.response_ledger_payload_hash, event.payload_hash
         FROM security_audit.records AS audit
         JOIN approval.interaction_responses AS response
           ON response.response_ledger_record_id = audit.source_ledger_record_id
         JOIN event_store.domain_events AS event
           ON event.record_id = response.response_ledger_record_id
         WHERE audit.tenant_id = $1 AND audit.action = 1",
    )
    .bind(fixture.tenant_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read Approval semantic binding");
    assert_eq!(binding.0, 1);
    assert_eq!((binding.1, binding.2, binding.3), (1, 1, 1));
    assert_eq!(binding.4, binding.5);
    assert!(
        sqlx::query(
            "UPDATE approval.interaction_responses
             SET response_ledger_payload_hash = $2
             WHERE interaction_id = $1",
        )
        .bind(fixture.interaction_id.into_uuid())
        .bind(vec![99_u8; 32])
        .execute(&pool)
        .await
        .is_err(),
        "typed response receipt must remain immutable"
    );
    pool.close().await;
}

#[tokio::test]
async fn expiry_closes_the_interaction_and_requeues_the_run_exactly_once() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = keyring();
    let (fixture, prompt, fence) = seed_running_interaction(&pool, keys.clone()).await;
    let now = database_now(&pool).await;
    let expires_at = UtcTimestamp::from_unix_millis(now.unix_millis() + 1_000);
    let interaction = Interaction {
        interaction_id: fixture.interaction_id,
        tenant_id: fixture.tenant_id,
        conversation_id: fixture.conversation_id,
        activity_id: fixture.activity_id,
        run_id: fixture.run_id,
        requested_by_invocation_id: fixture.invocation_id,
        kind: InteractionKind::Credential,
        status: InteractionStatus::Pending,
        prompt_ref: prompt,
        response_schema: SchemaRef {
            schema_key: VersionedKey::new("interaction.secure-response@1").expect("schema"),
            schema_hash: Sha256::digest(b"interaction-secure-response-schema"),
        },
        response_ref: None,
        expires_at: Some(expires_at),
        requested_at: now,
        responded_by: None,
        responded_at: None,
    };
    let request_ids = [
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
        LedgerRecordId::generate(),
    ];
    let store =
        PostgresInteractionStore::new(pool.clone(), keys, Arc::new(ProtobufLedgerPayloadCodec));
    store
        .request(RequestInteractionCommit {
            interaction,
            fence,
            expected_last_run_sequence: RunSeq::new(1).expect("run sequence"),
            append: append(
                fixture,
                1,
                "interaction:expiry:request",
                records(
                    fixture,
                    &request_ids,
                    2,
                    Producer::Worker,
                    "interaction.requested",
                    None,
                    now,
                ),
                false,
            ),
            semantic_records: InteractionSemanticRecords {
                interaction_record_id: request_ids[0],
                run_record_id: Some(request_ids[1]),
                tool_record_id: Some(request_ids[2]),
            },
        })
        .await
        .expect("request expiring Interaction");

    let close_ids = [LedgerRecordId::generate(), LedgerRecordId::generate()];
    let close = CloseInteractionCommit {
        tenant_id: fixture.tenant_id,
        interaction_id: fixture.interaction_id,
        closure: InteractionClosure::Expire,
        closed_at: expires_at,
        expected_last_run_sequence: RunSeq::new(4).expect("run sequence"),
        append: append(
            fixture,
            4,
            "interaction:expiry:close",
            records(
                fixture,
                &close_ids,
                5,
                Producer::Scheduler,
                "interaction.expired",
                None,
                expires_at,
            ),
            true,
        ),
        semantic_records: InteractionSemanticRecords {
            interaction_record_id: close_ids[0],
            run_record_id: Some(close_ids[1]),
            tool_record_id: None,
        },
    };
    let mut premature = close.clone();
    premature.closed_at = UtcTimestamp::from_unix_millis(expires_at.unix_millis() - 1);
    assert!(matches!(
        store.close(premature).await,
        Err(PostgresInteractionError::NotExpired)
    ));
    assert_eq!(
        store
            .close(close.clone())
            .await
            .expect("expire Interaction")
            .disposition,
        InteractionCommitDisposition::Committed
    );
    assert_eq!(
        store
            .load(fixture.tenant_id, fixture.interaction_id)
            .await
            .expect("load expired Interaction")
            .status,
        InteractionStatus::Expired
    );
    let run: (i16, String) = sqlx::query_as(
        "SELECT status, last_run_sequence::text
         FROM runtime.runs WHERE run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read resumed Run");
    assert_eq!(run, (1, "6".into()));
    assert_eq!(
        store.close(close).await.expect("replay expiry").disposition,
        InteractionCommitDisposition::Duplicate
    );
    pool.close().await;
}
