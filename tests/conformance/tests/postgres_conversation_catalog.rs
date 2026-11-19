use std::sync::Arc;

use musteragent_approval::{InteractionEvent, InteractionKind};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_conversation::{
    ConversationEvent, ConversationKind, ConversationLifecycle, MessageRole, PartialMessageReason,
    ParticipantDisplaySnapshot, ParticipantKind,
};
use musteragent_event_envelope::{LedgerCursor, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_identity::{
    ActivityId, Actor, AgentId, AttemptId, AttemptNumber, ConversationId, CorrelationId, DottedKey,
    HomeEpoch, IdempotencyKey, InteractionId, InvocationId, LedgerOffset, LedgerRecordId,
    MessageId, ParticipantId, PrincipalId, PrincipalKind, ProjectionSnapshotId, RunId, RunSeq,
    RuntimeSnapshotId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{AppendRequest, DomainEvent, LedgerPayload, ProposedRecord};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    PostgresContentStore, PostgresConversationCatalogStore, PostgresLedgerRecordSource,
    PostgresLedgerStore,
};
use musteragent_projection::{
    CatalogAttentionKind, ConversationCatalogLedgerProjector, ConversationCatalogSummaryV2,
    CursorRecoveryAuthoritySource, ExecutionOutcomeKind, ListConversationCatalogV2,
    SubscriptionError,
};
use musteragent_run::RunEvent;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, CursorRecoveryAuthority, DisplayText,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use sqlx::PgPool;

struct UnusedRecoveryAuthority;

impl CursorRecoveryAuthoritySource for UnusedRecoveryAuthority {
    fn current_cursor_recovery_authority(
        &self,
        _actor: Actor,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<CursorRecoveryAuthority, SubscriptionError>> {
        Box::pin(async move {
            Ok(CursorRecoveryAuthority {
                snapshot_id: ProjectionSnapshotId::generate(),
                resume_cursor: LedgerCursor {
                    conversation_id,
                    home_epoch: HomeEpoch::new(1).expect("home epoch"),
                    after_conversation_seq: LedgerOffset::ZERO,
                },
            })
        })
    }
}

fn reference_keys() -> Arc<OpaqueRefKeyring> {
    Arc::new(
        OpaqueRefKeyring::new(
            DottedKey::new("runtime.content.active").expect("key id"),
            [(
                DottedKey::new("runtime.content.active").expect("key id"),
                [29_u8; 32],
            )],
        )
        .expect("reference keyring"),
    )
}

async fn store_text_content(
    content: &PostgresContentStore,
    tenant_id: TenantId,
    text: &[u8],
) -> ContentRef {
    content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/plain").expect("media type"),
            bytes: text.to_vec(),
        })
        .await
        .expect("store content")
}

#[allow(clippy::too_many_arguments)]
fn domain_record(
    codec: &ProtobufLedgerPayloadCodec,
    activity_id: Option<ActivityId>,
    run_id: Option<RunId>,
    run_seq: Option<u64>,
    attempt_id: Option<AttemptId>,
    participant_id: Option<ParticipantId>,
    producer: Producer,
    occurred_at_ms: i64,
    event: DomainEvent,
) -> ProposedRecord {
    let payload = codec.encode(LedgerPayload::DomainEvent(event));
    ProposedRecord {
        schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("record schema"),
        record_id: LedgerRecordId::generate(),
        record_kind: LedgerRecordKind::DomainEvent,
        activity_id,
        run_id,
        run_seq: run_seq.map(|value| RunSeq::new(value).expect("run seq")),
        attempt_id,
        participant_id,
        producer,
        occurred_at: UtcTimestamp::from_unix_millis(occurred_at_ms),
        causation_id: None,
        correlation_id: CorrelationId::generate(),
        payload_hash: Sha256::digest(&payload),
        payload,
    }
}

async fn append_events(
    ledger: &PostgresLedgerStore,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    expected_offset: u64,
    key: &str,
    records: Vec<ProposedRecord>,
) -> u64 {
    let receipt = ledger
        .append(AppendRequest {
            tenant_id,
            conversation_id,
            expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
            expected_offset: LedgerOffset::new(expected_offset),
            idempotency_key: IdempotencyKey::new(key).expect("idempotency key"),
            command_fingerprint: Sha256::digest(key.as_bytes()),
            records,
            outbox: Vec::new(),
        })
        .await
        .expect("append ledger records");
    receipt.last_sequence.get()
}

#[allow(clippy::too_many_arguments)]
async fn insert_run_row(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    run_id: RunId,
    activity_id: ActivityId,
    executor_participant_id: ParticipantId,
    requested_by_participant_id: ParticipantId,
    admitted_sequence: u64,
    admitted_at_ms: i64,
) {
    let mut tx = pool.begin().await.expect("begin run fixture");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *tx)
        .await
        .expect("defer constraints");
    sqlx::query(
        "INSERT INTO runtime.runs(
             run_id, tenant_id, conversation_id, activity_id, activity_kind,
             executor_participant_id, requested_by_participant_id,
             execution_scope_kind, control_home_kind, execution_home_kind,
             home_epoch, runtime_snapshot_id, admitted_sequence,
             last_run_sequence, next_fencing_token, status,
             next_attempt_number, admitted_at_ms, traceparent
         ) VALUES(
             $1, $2, $3, $4, 1,
             $5, $6,
             1, 1, 1,
             1, $7, $8,
             1, 1, 2,
             1, $9, '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
         )",
    )
    .bind(run_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(activity_id.into_uuid())
    .bind(executor_participant_id.into_uuid())
    .bind(requested_by_participant_id.into_uuid())
    .bind(RuntimeSnapshotId::generate().into_uuid())
    .bind(i64::try_from(admitted_sequence).expect("admitted sequence"))
    .bind(admitted_at_ms)
    .execute(&mut *tx)
    .await
    .expect("insert run row");
    tx.commit().await.expect("commit run row");
}

async fn read_row(
    catalog: &PostgresConversationCatalogStore,
    actor: Actor,
    conversation_id: ConversationId,
    lifecycle: ConversationLifecycle,
) -> ConversationCatalogSummaryV2 {
    let mut page = catalog
        .list_catalog(
            ListConversationCatalogV2::new(actor, lifecycle, 10)
                .with_conversation_id(Some(conversation_id)),
        )
        .await
        .expect("list catalog");
    assert_eq!(page.conversations.len(), 1);
    page.conversations.pop().expect("single row")
}

async fn ack_all_visible(
    catalog: &PostgresConversationCatalogStore,
    actor: Actor,
    conversation_id: ConversationId,
    lifecycle: ConversationLifecycle,
    at_ms: i64,
) {
    let summary = read_row(catalog, actor, conversation_id, lifecycle).await;
    if summary.unread.latest_visible_revision > summary.unread.seen_through {
        catalog
            .acknowledge_seen_through(
                actor.tenant_id,
                conversation_id,
                actor.principal_id,
                HomeEpoch::new(1).expect("home epoch"),
                summary.unread.latest_visible_revision,
                UtcTimestamp::from_unix_millis(at_ms),
            )
            .await
            .expect("acknowledge visible attention");
    }
}

#[tokio::test]
async fn catalog_projector_attributes_unread_across_create_complete_cancel_archive_and_replay() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");

    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();

    // Ensure `other_principal` sorts lexicographically BEFORE `creator_principal`
    // so `list_projection_authorities` must prefer `conversation.created_by` over
    // an arbitrary `MIN(viewer.principal_ref)`.
    let mut pair = [PrincipalId::generate(), PrincipalId::generate()];
    pair.sort_by_key(ToString::to_string);
    let other_principal = pair[0];
    let creator_principal = pair[1];
    assert!(other_principal.to_string() < creator_principal.to_string());

    let creator_actor = Actor {
        tenant_id,
        principal_id: creator_principal,
        principal_kind: PrincipalKind::User,
    };
    let other_actor = Actor {
        tenant_id,
        principal_id: other_principal,
        principal_kind: PrincipalKind::User,
    };

    let creator_participant = ParticipantId::generate();
    let other_participant = ParticipantId::generate();
    let agent_participant = ParticipantId::generate();
    let agent_id = AgentId::generate();

    let mut tx = pool.begin().await.expect("begin conversation seed");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *tx)
        .await
        .expect("defer constraints");
    sqlx::query(
        "INSERT INTO conversation.conversations(
             conversation_id, tenant_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms, workspace_id,
             execution_owner_participant_id
         ) VALUES($1, $2, 2, 'Unread Attribution Check', 1, NULL, 1, 1, $3,
                  1000, 1000, NULL, NULL, $4)",
    )
    .bind(conversation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(creator_principal.into_uuid())
    .bind(agent_participant.into_uuid())
    .execute(&mut *tx)
    .await
    .expect("insert conversation");
    for (participant_id, principal_id, name) in [
        (creator_participant, creator_principal, "Creator"),
        (other_participant, other_principal, "Collaborator"),
    ] {
        sqlx::query(
            "INSERT INTO conversation.participants(
                 participant_id, tenant_id, conversation_id, kind, principal_ref,
                 agent_ref, service_ref, display_name, avatar_content_id,
                 joined_at_ms, left_at_ms
             ) VALUES($1, $2, $3, 1, $4, NULL, NULL, $5, NULL, 1000, NULL)",
        )
        .bind(participant_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(name)
        .execute(&mut *tx)
        .await
        .expect("insert user participant");
    }
    sqlx::query(
        "INSERT INTO conversation.participants(
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES($1, $2, $3, 2, NULL, $4, NULL, 'Agent', NULL, 1000, NULL)",
    )
    .bind(agent_participant.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(agent_id.into_uuid())
    .execute(&mut *tx)
    .await
    .expect("insert agent participant");
    tx.commit().await.expect("commit conversation seed");

    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        reference_keys(),
    );
    let ledger = PostgresLedgerStore::new(pool.clone());
    let records = Arc::new(PostgresLedgerRecordSource::new(
        pool.clone(),
        codec.clone(),
        Arc::new(UnusedRecoveryAuthority),
    ));
    let catalog = Arc::new(PostgresConversationCatalogStore::new(pool.clone()));
    let projector = ConversationCatalogLedgerProjector::new(20, 64, records, catalog.clone())
        .expect("projector");

    // 1. Creation: envelope `participant_id` is None because ParticipantJoined follows.
    let mut offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        0,
        "phase-1-create",
        vec![
            domain_record(
                &codec,
                None,
                None,
                None,
                None,
                None,
                Producer::RuntimeApi,
                1_000,
                DomainEvent::Conversation(ConversationEvent::Created {
                    kind: ConversationKind::Team,
                    control_home: RuntimeHome::Cloud,
                    workspace_id: None,
                }),
            ),
            domain_record(
                &codec,
                None,
                None,
                None,
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                1_001,
                DomainEvent::Conversation(ConversationEvent::ParticipantJoined {
                    kind: ParticipantKind::User,
                    principal_ref: Some(creator_principal),
                    agent_ref: None,
                    service_ref: None,
                    display_snapshot: ParticipantDisplaySnapshot {
                        display_name: DisplayText::new("Creator").expect("name"),
                        avatar_ref: None,
                    },
                }),
            ),
            domain_record(
                &codec,
                None,
                None,
                None,
                None,
                Some(other_participant),
                Producer::RuntimeApi,
                1_002,
                DomainEvent::Conversation(ConversationEvent::ParticipantJoined {
                    kind: ParticipantKind::User,
                    principal_ref: Some(other_principal),
                    agent_ref: None,
                    service_ref: None,
                    display_snapshot: ParticipantDisplaySnapshot {
                        display_name: DisplayText::new("Collaborator").expect("name"),
                        avatar_ref: None,
                    },
                }),
            ),
        ],
    )
    .await;

    projector
        .run_pass(UtcTimestamp::from_unix_millis(1_100))
        .await
        .expect("project create");
    let creator_after_create = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    let other_after_create = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    assert_eq!(
        creator_after_create.unread.count, 0,
        "conversation creator must not receive unread attention on creation"
    );
    assert_eq!(creator_after_create.unread.latest_unread_kind, None);
    assert_eq!(other_after_create.unread.count, 1);
    assert_eq!(
        other_after_create.unread.latest_unread_kind,
        Some(CatalogAttentionKind::ConversationCreated)
    );
    ack_all_visible(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
        1_200,
    )
    .await;

    // 2. Normal Run completion: both participants receive unread output/outcome.
    let run_complete = RunId::generate();
    let act_complete = ActivityId::generate();
    insert_run_row(
        &pool,
        tenant_id,
        conversation_id,
        run_complete,
        act_complete,
        agent_participant,
        creator_participant,
        offset + 2,
        2_000,
    )
    .await;
    let user_msg_ref = store_text_content(&content, tenant_id, b"hello").await;
    let assistant_msg_ref = store_text_content(&content, tenant_id, b"completed reply").await;
    let assistant_msg_id = MessageId::generate();
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-2-complete",
        vec![
            domain_record(
                &codec,
                Some(act_complete),
                None,
                None,
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                2_000,
                DomainEvent::Conversation(ConversationEvent::MessageCommitted {
                    message_id: MessageId::generate(),
                    role: MessageRole::User,
                    content_ref: user_msg_ref.clone(),
                    content_hash: user_msg_ref.sha256,
                    content_bytes: user_msg_ref.byte_length,
                }),
            ),
            domain_record(
                &codec,
                Some(act_complete),
                Some(run_complete),
                Some(1),
                None,
                Some(agent_participant),
                Producer::RuntimeApi,
                2_001,
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: RuntimeSnapshotId::generate(),
                    execution_home: RuntimeHome::Cloud,
                    parent_run_id: None,
                    delegation_id: None,
                    retry_source_message_id: None,
                    retry_source_attempt_number: None,
                }),
            ),
            domain_record(
                &codec,
                Some(act_complete),
                Some(run_complete),
                Some(2),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                2_010,
                DomainEvent::Run(RunEvent::Started {
                    attempt_id: AttemptId::generate(),
                    attempt_number: AttemptNumber::new(1).expect("attempt"),
                }),
            ),
            domain_record(
                &codec,
                Some(act_complete),
                Some(run_complete),
                Some(3),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                2_020,
                DomainEvent::Conversation(ConversationEvent::MessageCommitted {
                    message_id: assistant_msg_id,
                    role: MessageRole::Assistant,
                    content_ref: assistant_msg_ref.clone(),
                    content_hash: assistant_msg_ref.sha256,
                    content_bytes: assistant_msg_ref.byte_length,
                }),
            ),
            domain_record(
                &codec,
                Some(act_complete),
                Some(run_complete),
                Some(4),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                2_030,
                DomainEvent::Run(RunEvent::Completed {
                    assistant_message_id: None,
                    result_refs: Vec::new(),
                }),
            ),
        ],
    )
    .await;
    sqlx::query("UPDATE runtime.runs SET status = 5 WHERE run_id = $1")
        .bind(run_complete.into_uuid())
        .execute(&pool)
        .await
        .expect("mark run completed");

    projector
        .run_pass(UtcTimestamp::from_unix_millis(2_100))
        .await
        .expect("project completion");
    let creator_after_complete = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    let other_after_complete = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    assert_eq!(creator_after_complete.unread.count, 2);
    assert_eq!(other_after_complete.unread.count, 3);
    assert_eq!(
        creator_after_complete
            .latest_outcome
            .as_ref()
            .map(|o| o.kind),
        Some(ExecutionOutcomeKind::Completed)
    );
    ack_all_visible(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
        2_200,
    )
    .await;
    ack_all_visible(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
        2_200,
    )
    .await;

    // 3. User-requested cancellation: active execution + open interaction + cancelled
    //    partial message across separate projector passes.
    let run_user_cancel = RunId::generate();
    let act_user_cancel = ActivityId::generate();
    let interaction_id = InteractionId::generate();
    insert_run_row(
        &pool,
        tenant_id,
        conversation_id,
        run_user_cancel,
        act_user_cancel,
        agent_participant,
        creator_participant,
        offset + 1,
        3_000,
    )
    .await;
    let prompt_ref = store_text_content(&content, tenant_id, b"approve?").await;
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-3a-active-and-pending",
        vec![
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(1),
                None,
                Some(agent_participant),
                Producer::RuntimeApi,
                3_000,
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: RuntimeSnapshotId::generate(),
                    execution_home: RuntimeHome::Cloud,
                    parent_run_id: None,
                    delegation_id: None,
                    retry_source_message_id: None,
                    retry_source_attempt_number: None,
                }),
            ),
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(2),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                3_010,
                DomainEvent::Run(RunEvent::Started {
                    attempt_id: AttemptId::generate(),
                    attempt_number: AttemptNumber::new(1).expect("attempt"),
                }),
            ),
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(3),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                3_020,
                DomainEvent::Interaction(InteractionEvent::Requested {
                    interaction_id,
                    invocation_id: InvocationId::generate(),
                    kind: InteractionKind::Approval,
                    prompt_ref,
                    response_schema: SchemaRef {
                        schema_key: VersionedKey::new("interaction.approval@1").expect("schema"),
                        schema_hash: Sha256::digest(b"{}"),
                    },
                    expires_at: None,
                }),
            ),
        ],
    )
    .await;

    projector
        .run_pass(UtcTimestamp::from_unix_millis(3_050))
        .await
        .expect("project active run and open action");
    let before_cancel = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    assert!(before_cancel.execution.has_active_execution());
    assert_eq!(before_cancel.action_required.count, 1);
    ack_all_visible(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
        3_060,
    )
    .await;

    // Pass A: creator requests cancellation.
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-3b-cancel-request",
        vec![
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(4),
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                3_100,
                DomainEvent::Interaction(InteractionEvent::Cancelled {
                    interaction_id,
                    reason: CancelReason::UserRequested,
                }),
            ),
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(5),
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                3_101,
                DomainEvent::Run(RunEvent::CancelRequested {
                    reason: CancelReason::UserRequested,
                    requested_by: creator_principal,
                }),
            ),
        ],
    )
    .await;
    sqlx::query(
        "UPDATE runtime.runs SET status = 4, cancel_requested_at_ms = 3101 WHERE run_id = $1",
    )
    .bind(run_user_cancel.into_uuid())
    .execute(&pool)
    .await
    .expect("mark cancelling");
    projector
        .run_pass(UtcTimestamp::from_unix_millis(3_150))
        .await
        .expect("project cancel request");

    // Pass B: worker commits partial cancelled output + terminal RunEvent::Cancelled
    // with empty envelope participant_id.
    let partial_ref = store_text_content(&content, tenant_id, b"partial before stop").await;
    let cancelled_attempt = AttemptId::generate();
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-3c-worker-cancelled",
        vec![
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(6),
                Some(cancelled_attempt),
                None,
                Producer::Worker,
                3_200,
                DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                    message_id: MessageId::generate(),
                    content_ref: partial_ref.clone(),
                    content_hash: partial_ref.sha256,
                    content_bytes: partial_ref.byte_length,
                    reason: PartialMessageReason::Cancelled,
                }),
            ),
            domain_record(
                &codec,
                Some(act_user_cancel),
                Some(run_user_cancel),
                Some(7),
                Some(cancelled_attempt),
                None,
                Producer::Worker,
                3_201,
                DomainEvent::Run(RunEvent::Cancelled {
                    reason: CancelReason::UserRequested,
                    last_attempt_id: Some(cancelled_attempt),
                }),
            ),
        ],
    )
    .await;
    sqlx::query("UPDATE runtime.runs SET status = 7 WHERE run_id = $1")
        .bind(run_user_cancel.into_uuid())
        .execute(&pool)
        .await
        .expect("mark cancelled");

    projector
        .run_pass(UtcTimestamp::from_unix_millis(3_300))
        .await
        .expect("project worker cancelled terminal");
    let creator_after_user_cancel = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;
    let other_after_user_cancel = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
    )
    .await;

    assert!(!creator_after_user_cancel.execution.has_active_execution());
    assert_eq!(creator_after_user_cancel.action_required.count, 0);
    assert_eq!(
        creator_after_user_cancel
            .latest_outcome
            .as_ref()
            .map(|o| o.kind),
        Some(ExecutionOutcomeKind::Cancelled)
    );
    assert_eq!(
        creator_after_user_cancel.unread.count, 0,
        "cancelling participant must receive zero unread from partial output and cancelled outcome"
    );
    assert_eq!(creator_after_user_cancel.unread.latest_unread_kind, None);
    assert_eq!(creator_after_user_cancel.unread.latest_unread_outcome, None);

    assert_eq!(
        other_after_user_cancel.unread.count, 2,
        "other participant keeps today's unread behavior for partial output and cancelled outcome"
    );
    assert_eq!(
        other_after_user_cancel.unread.latest_unread_kind,
        Some(CatalogAttentionKind::ExecutionOutcome)
    );
    assert_eq!(
        other_after_user_cancel
            .unread
            .latest_unread_outcome
            .as_ref()
            .map(|o| o.kind),
        Some(ExecutionOutcomeKind::Cancelled)
    );
    ack_all_visible(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
        3_400,
    )
    .await;

    // 4. Non-user cancellation (`PolicyRevoked`) still broadcasts unread to all participants.
    let run_policy_cancel = RunId::generate();
    let act_policy_cancel = ActivityId::generate();
    insert_run_row(
        &pool,
        tenant_id,
        conversation_id,
        run_policy_cancel,
        act_policy_cancel,
        agent_participant,
        creator_participant,
        offset + 1,
        4_000,
    )
    .await;
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-4-policy-cancel",
        vec![
            domain_record(
                &codec,
                Some(act_policy_cancel),
                Some(run_policy_cancel),
                Some(1),
                None,
                Some(agent_participant),
                Producer::RuntimeApi,
                4_000,
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: RuntimeSnapshotId::generate(),
                    execution_home: RuntimeHome::Cloud,
                    parent_run_id: None,
                    delegation_id: None,
                    retry_source_message_id: None,
                    retry_source_attempt_number: None,
                }),
            ),
            domain_record(
                &codec,
                Some(act_policy_cancel),
                Some(run_policy_cancel),
                Some(2),
                None,
                None,
                Producer::Scheduler,
                4_010,
                DomainEvent::Run(RunEvent::CancelRequested {
                    reason: CancelReason::PolicyRevoked,
                    requested_by: creator_principal,
                }),
            ),
            domain_record(
                &codec,
                Some(act_policy_cancel),
                Some(run_policy_cancel),
                Some(3),
                None,
                None,
                Producer::Scheduler,
                4_011,
                DomainEvent::Run(RunEvent::Cancelled {
                    reason: CancelReason::PolicyRevoked,
                    last_attempt_id: None,
                }),
            ),
        ],
    )
    .await;
    sqlx::query(
        "UPDATE runtime.runs SET status = 7, cancel_requested_at_ms = 4010 WHERE run_id = $1",
    )
    .bind(run_policy_cancel.into_uuid())
    .execute(&pool)
    .await
    .expect("mark policy cancelled");
    projector
        .run_pass(UtcTimestamp::from_unix_millis(4_100))
        .await
        .expect("project policy cancel");
    assert_eq!(
        read_row(
            &catalog,
            creator_actor,
            conversation_id,
            ConversationLifecycle::Active
        )
        .await
        .unread
        .count,
        1
    );
    assert_eq!(
        read_row(
            &catalog,
            other_actor,
            conversation_id,
            ConversationLifecycle::Active
        )
        .await
        .unread
        .count,
        1
    );
    ack_all_visible(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
        4_200,
    )
    .await;
    ack_all_visible(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Active,
        4_200,
    )
    .await;

    // 5. Archive-driven cancellation (`ConversationArchived`): closes active execution
    //    and pending items, sets latest_outcome to Cancelled, archives row, and adds
    //    zero unread for the archiving participant while notifying the other participant.
    let run_archive_cancel = RunId::generate();
    let act_archive_cancel = ActivityId::generate();
    let archive_interaction_id = InteractionId::generate();
    insert_run_row(
        &pool,
        tenant_id,
        conversation_id,
        run_archive_cancel,
        act_archive_cancel,
        agent_participant,
        creator_participant,
        offset + 1,
        5_000,
    )
    .await;
    let archive_prompt_ref =
        store_text_content(&content, tenant_id, b"confirm before archive").await;
    let archive_partial_ref =
        store_text_content(&content, tenant_id, b"partial before archive").await;
    offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-5a-archive-setup",
        vec![
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(1),
                None,
                Some(agent_participant),
                Producer::RuntimeApi,
                5_000,
                DomainEvent::Run(RunEvent::Admitted {
                    runtime_snapshot_id: RuntimeSnapshotId::generate(),
                    execution_home: RuntimeHome::Cloud,
                    parent_run_id: None,
                    delegation_id: None,
                    retry_source_message_id: None,
                    retry_source_attempt_number: None,
                }),
            ),
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(2),
                None,
                None,
                Producer::Worker,
                5_005,
                DomainEvent::Interaction(InteractionEvent::Requested {
                    interaction_id: archive_interaction_id,
                    invocation_id: InvocationId::generate(),
                    kind: InteractionKind::Confirmation,
                    prompt_ref: archive_prompt_ref,
                    response_schema: SchemaRef {
                        schema_key: VersionedKey::new("interaction.confirm@1").expect("schema"),
                        schema_hash: Sha256::digest(b"{}"),
                    },
                    expires_at: None,
                }),
            ),
        ],
    )
    .await;
    projector
        .run_pass(UtcTimestamp::from_unix_millis(5_050))
        .await
        .expect("project pre-archive pending action");
    ack_all_visible(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Active,
        5_060,
    )
    .await;

    let _final_offset = append_events(
        &ledger,
        tenant_id,
        conversation_id,
        offset,
        "phase-5b-archive-cancel",
        vec![
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(3),
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                5_100,
                DomainEvent::Interaction(InteractionEvent::Cancelled {
                    interaction_id: archive_interaction_id,
                    reason: CancelReason::ConversationArchived,
                }),
            ),
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(4),
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                5_101,
                DomainEvent::Run(RunEvent::CancelRequested {
                    reason: CancelReason::ConversationArchived,
                    requested_by: creator_principal,
                }),
            ),
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(5),
                Some(AttemptId::generate()),
                None,
                Producer::Worker,
                5_102,
                DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                    message_id: MessageId::generate(),
                    content_ref: archive_partial_ref.clone(),
                    content_hash: archive_partial_ref.sha256,
                    content_bytes: archive_partial_ref.byte_length,
                    reason: PartialMessageReason::Cancelled,
                }),
            ),
            domain_record(
                &codec,
                Some(act_archive_cancel),
                Some(run_archive_cancel),
                Some(6),
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                5_103,
                DomainEvent::Run(RunEvent::Cancelled {
                    reason: CancelReason::ConversationArchived,
                    last_attempt_id: None,
                }),
            ),
            domain_record(
                &codec,
                None,
                None,
                None,
                None,
                Some(creator_participant),
                Producer::RuntimeApi,
                5_104,
                DomainEvent::Conversation(ConversationEvent::Archived),
            ),
        ],
    )
    .await;
    sqlx::query(
        "UPDATE runtime.runs SET status = 7, cancel_requested_at_ms = 5101 WHERE run_id = $1",
    )
    .bind(run_archive_cancel.into_uuid())
    .execute(&pool)
    .await
    .expect("mark archive cancelled");
    sqlx::query(
        "UPDATE conversation.conversations
         SET lifecycle = 2, archived_at_ms = 5104, updated_at_ms = 5104
         WHERE conversation_id = $1",
    )
    .bind(conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("mark archived");

    projector
        .run_pass(UtcTimestamp::from_unix_millis(5_200))
        .await
        .expect("project archive-driven cancel");
    let creator_archived = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;
    let other_archived = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;

    assert_eq!(creator_archived.lifecycle, ConversationLifecycle::Archived);
    assert!(!creator_archived.execution.has_active_execution());
    assert_eq!(creator_archived.action_required.count, 0);
    assert_eq!(
        creator_archived.latest_outcome.as_ref().map(|o| o.kind),
        Some(ExecutionOutcomeKind::Cancelled)
    );
    assert_eq!(
        creator_archived.unread.count, 0,
        "archive-driven cancellation must not add unread for archiving participant"
    );
    assert_eq!(other_archived.unread.count, 2);
    assert_eq!(
        other_archived
            .unread
            .latest_unread_outcome
            .as_ref()
            .map(|o| o.kind),
        Some(ExecutionOutcomeKind::Cancelled)
    );

    // 6. Projector replay idempotence:
    // 6a. Immediate steady-state pass does nothing.
    let baseline_creator = catalog
        .list_catalog(ListConversationCatalogV2::new(
            creator_actor,
            ConversationLifecycle::Archived,
            10,
        ))
        .await
        .expect("baseline creator page");
    let baseline_other = catalog
        .list_catalog(ListConversationCatalogV2::new(
            other_actor,
            ConversationLifecycle::Archived,
            10,
        ))
        .await
        .expect("baseline other page");

    let noop_pass = projector
        .run_pass(UtcTimestamp::from_unix_millis(5_300))
        .await
        .expect("steady-state pass");
    assert_eq!(noop_pass.projected_facts, 0);
    assert_eq!(noop_pass.advanced_authorities, 0);

    // 6b. Deleting projector checkpoints replays the full Ledger from seq 0
    //     with zero duplicate mutations or unread inflation.
    sqlx::query("DELETE FROM projection.conversation_catalog_projector_checkpoints")
        .execute(&pool)
        .await
        .expect("clear checkpoints");
    let replay_pass = projector
        .run_pass(UtcTimestamp::from_unix_millis(5_400))
        .await
        .expect("full checkpoint replay");
    assert_eq!(replay_pass.projected_facts, 0);
    assert_eq!(replay_pass.advanced_authorities, 1);

    let replayed_creator = catalog
        .list_catalog(ListConversationCatalogV2::new(
            creator_actor,
            ConversationLifecycle::Archived,
            10,
        ))
        .await
        .expect("replayed creator page");
    let replayed_other = catalog
        .list_catalog(ListConversationCatalogV2::new(
            other_actor,
            ConversationLifecycle::Archived,
            10,
        ))
        .await
        .expect("replayed other page");
    assert_eq!(replayed_creator, baseline_creator);
    assert_eq!(replayed_other, baseline_other);

    // 6c. Full projection rebuild from empty catalog tables reconstructs the
    //     exact delivery totals and row state for both participants (deleting
    //     conversation_catalog cascades conversation_read_receipts).
    let creator_seen_before_wipe = baseline_creator.conversations[0].unread.seen_through;
    let other_seen_before_wipe = baseline_other.conversations[0].unread.seen_through;
    for statement in [
        "DELETE FROM projection.conversation_catalog_projector_checkpoints",
        "DELETE FROM projection.conversation_catalog_attention_deliveries",
        "DELETE FROM projection.conversation_catalog_attention",
        "DELETE FROM projection.conversation_catalog_actions",
        "DELETE FROM projection.conversation_catalog_executions",
        "DELETE FROM projection.conversation_catalog_sources",
        "DELETE FROM projection.conversation_catalog_changes",
        "DELETE FROM projection.conversation_catalog",
    ] {
        sqlx::query(statement)
            .execute(&pool)
            .await
            .expect("wipe catalog projection table");
    }
    let rebuild_pass = projector
        .run_pass(UtcTimestamp::from_unix_millis(5_500))
        .await
        .expect("rebuild catalog from ledger");
    assert!(rebuild_pass.projected_facts > 0);
    assert_eq!(rebuild_pass.advanced_authorities, 1);

    let unacked_creator = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;
    let unacked_other = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;
    // Creator received only: assistant output (1) + completed outcome (1) +
    // two interaction prompts (2) + PolicyRevoked cancel (1) = 5 total (zero from
    // create, user-cancel partial/outcome, or archive-cancel partial/outcome).
    assert_eq!(unacked_creator.unread.count, 5);
    assert_eq!(
        unacked_creator.unread.latest_visible_revision,
        baseline_creator.conversations[0]
            .unread
            .latest_visible_revision
    );
    // Collaborator received: create (1) + user msg (1) + assistant output (1) +
    // completed outcome (1) + user-cancel partial & outcome (2) + PolicyRevoked (1)
    // + archive-cancel partial & outcome (2) = 9 total.
    assert_eq!(unacked_other.unread.count, 9);
    assert_eq!(
        unacked_other.unread.latest_visible_revision,
        baseline_other.conversations[0]
            .unread
            .latest_visible_revision
    );

    catalog
        .acknowledge_seen_through(
            tenant_id,
            conversation_id,
            creator_principal,
            HomeEpoch::new(1).expect("home epoch"),
            creator_seen_before_wipe,
            UtcTimestamp::from_unix_millis(5_600),
        )
        .await
        .expect("restore creator receipt");
    catalog
        .acknowledge_seen_through(
            tenant_id,
            conversation_id,
            other_principal,
            HomeEpoch::new(1).expect("home epoch"),
            other_seen_before_wipe,
            UtcTimestamp::from_unix_millis(5_600),
        )
        .await
        .expect("restore collaborator receipt");

    let rebuilt_creator = read_row(
        &catalog,
        creator_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;
    let rebuilt_other = read_row(
        &catalog,
        other_actor,
        conversation_id,
        ConversationLifecycle::Archived,
    )
    .await;
    assert_eq!(
        rebuilt_creator.unread,
        baseline_creator.conversations[0].unread
    );
    assert_eq!(rebuilt_other.unread, baseline_other.conversations[0].unread);
    assert_eq!(
        rebuilt_creator.latest_outcome,
        baseline_creator.conversations[0].latest_outcome
    );
    assert_eq!(
        rebuilt_creator.execution,
        baseline_creator.conversations[0].execution
    );
    assert_eq!(
        rebuilt_creator.action_required,
        baseline_creator.conversations[0].action_required
    );
}
