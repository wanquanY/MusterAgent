use super::*;
use musteragent_policy::RequiredContext;
use musteragent_postgres::PostgresToolExecutionMaterialSource;
use musteragent_runtime_capability::ToolExecutionMaterialSource;
use musteragent_runtime_contract::{AttachmentRef, DisplayText};

#[tokio::test]
async fn follow_up_inherits_visible_attachments_but_retraction_revokes_them() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key_id = DottedKey::new("runtime.attachment.test").expect("key");
    let keys = Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [58; 32])]).expect("keys"));
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys,
    ));
    let tenant_id = TenantId::generate();
    let first_id = RunId::generate();
    let policy = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let user = put_text(&content, tenant_id, "read attachment").await;
    let first = seed_running_context(&pool, tenant_id, first_id, &policy, &user, None, false).await;
    let attachment = put_text(&content, tenant_id, "private document body").await;
    let message_id = MessageId::generate();
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: first.conversation_id,
            run_id: first_id,
            expected_offset: current_ledger_offset(&pool, tenant_id, first.conversation_id).await,
            run_seq_start: 2,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id,
                    role: MessageRole::User,
                    content_ref: user,
                    attachment_refs: vec![AttachmentRef {
                        content_ref: attachment.clone(),
                        display_name: DisplayText::new("document.txt").expect("name"),
                    }],
                    requested_skill_refs: Vec::new(),
                },
            )],
            participant_id: Some(first.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "attachments:first",
        },
    )
    .await;
    sqlx::query(
        "UPDATE runtime.runs SET status=6, finished_at_ms=$2, last_run_sequence=2 WHERE run_id=$1",
    )
    .bind(first_id.into_uuid())
    .bind(database_now(&pool).await.unix_millis())
    .execute(&pool)
    .await
    .expect("failed first run");
    let next_id = RunId::generate();
    let next = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: first.conversation_id,
        run_id: next_id,
        policy_ref: &policy,
        route: openai_responses_route(),
        context_policy: None,
        device_tools: false,
        parallel_history_tools: false,
    })
    .await;
    let wire = WireName::new("read_attachment").expect("wire");
    sqlx::query("INSERT INTO runtime.loop_checkpoints (
        run_id, tenant_id, attempt_id, phase, iteration, updated_at_ms,
        conversation_id, remaining_tool_call_ids, completed_tool_calls, total_tokens, checkpoint_version
    ) VALUES ($1, $2, $3, 1, 0, $4, $5, '{}', 0, 0, 1)")
        .bind(next_id.into_uuid()).bind(tenant_id.into_uuid())
        .bind(next.attempt.attempt_id.into_uuid()).bind(database_now(&pool).await.unix_millis())
        .bind(next.conversation_id.into_uuid())
        .execute(&pool).await.expect("active tool execution checkpoint");
    let snapshot = RuntimeSnapshot::seal(
        next.runtime_snapshot_id,
        context_snapshot::snapshot_content_with_read_tool(next_id, policy, wire.as_str()),
        database_now(&pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("snapshot");
    sqlx::query("UPDATE capability.runtime_snapshots SET encoded_snapshot=$2, content_hash=$3 WHERE runtime_snapshot_id=$1")
        .bind(next.runtime_snapshot_id.into_uuid()).bind(&snapshot.encoded_content)
        .bind(snapshot.content_hash.into_bytes().to_vec()).execute(&pool).await.expect("snapshot");
    let source = PostgresToolExecutionMaterialSource::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let before = source
        .load(&next.attempt, &wire)
        .await
        .expect("follow-up material");
    assert_eq!(before.authorized_content, vec![attachment]);
    assert!(
        before
            .authorization
            .available_contexts
            .contains(&RequiredContext::ConversationAttachments)
    );
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: first.conversation_id,
            run_id: first_id,
            expected_offset: current_ledger_offset(&pool, tenant_id, first.conversation_id).await,
            run_seq_start: 3,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageRetracted {
                    message_id,
                    reason: SafeDisplayText::new("withdraw attachment").expect("reason"),
                    source_run_id: Some(first_id),
                },
            )],
            participant_id: Some(first.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "attachments:retract",
        },
    )
    .await;
    let after = source
        .load(&next.attempt, &wire)
        .await
        .expect("material after retraction");
    assert!(after.authorized_content.is_empty());
    assert!(
        !after
            .authorization
            .available_contexts
            .contains(&RequiredContext::ConversationAttachments)
    );
}
