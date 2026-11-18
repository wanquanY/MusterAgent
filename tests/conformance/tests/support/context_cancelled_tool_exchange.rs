use super::*;

use super::context_persistence::{
    DomainEventAppend, append_domain_events, current_ledger_offset, database_now,
    seed_cancelled_incomplete_tool_exchange,
};

pub(super) async fn run_interrupted_tool_exchange_recovery(succeeded: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key_id = DottedKey::new("runtime.context.cancelled-tool").expect("key id");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [59_u8; 32])]).expect("keyring"));
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys,
    ));
    let tenant_id = TenantId::generate();
    let first_run_id = RunId::generate();
    let policy_ref = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let first_user_ref = put_text(&content, tenant_id, "Start a delegated task.").await;
    let first = seed_running_context(
        &pool,
        tenant_id,
        first_run_id,
        &policy_ref,
        &first_user_ref,
        None,
        false,
    )
    .await;
    let invocation_id = InvocationId::generate();
    let request_ref = put_text(&content, tenant_id, "canonical Tool-call request").await;
    let assistant_ref = put_text(&content, tenant_id, "").await;
    let placeholder_ref =
        put_text(&content, tenant_id, "verified document parsing succeeded").await;
    let exchange = SeedToolExchange {
        tool_invocation_id: InvocationId::generate(),
        call_id: ToolCallId::new("call_cancelled_delegate_join").expect("call id"),
        wire_name: WireName::new("delegate_join").expect("wire name"),
        arguments: serde_json::json!({"handles": ["cancelled-child"]}),
        tool_ref: "delegation.join@1",
        capability_result_ref: placeholder_ref.clone(),
        tool_message_ref: placeholder_ref,
    };
    let terminal = if succeeded {
        seed_completed_tool_exchange(
            &pool,
            tenant_id,
            &first,
            invocation_id,
            &request_ref,
            &assistant_ref,
            std::slice::from_ref(&exchange),
        )
        .await;
        sqlx::query(
            "UPDATE runtime.pending_tool_calls SET completed_at_ms = NULL,
            result_content_id = NULL, result_content_hash = NULL, result_content_bytes = NULL
            WHERE invocation_id = $1",
        )
        .bind(invocation_id.into_uuid())
        .execute(&pool)
        .await
        .expect("simulate interrupted delivery");
        DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            invocation_id: exchange.tool_invocation_id,
            result_ref: exchange.capability_result_ref.clone(),
            result_hash: exchange.capability_result_ref.sha256,
            provider_request_ref: None,
            authorized_content: Vec::new(),
        })
    } else {
        seed_cancelled_incomplete_tool_exchange(
            &pool,
            tenant_id,
            &first,
            invocation_id,
            &request_ref,
            &assistant_ref,
            &exchange,
        )
        .await;
        DomainEvent::Tool(ToolEvent::InvocationCancelled {
            invocation_id: exchange.tool_invocation_id,
            reason: musteragent_runtime_contract::CancelReason::UserRequested,
        })
    };
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: first.conversation_id,
            run_id: first_run_id,
            expected_offset: LedgerOffset::new(1),
            run_seq_start: 2,
            events: vec![
                terminal,
                DomainEvent::Model(musteragent_runtime_model::ModelEvent::InvocationCompleted {
                    invocation_id,
                    response_ref: assistant_ref,
                    continuation_ref: None,
                }),
            ],
            participant_id: Some(first.executor_id),
            producer: Producer::Worker,
            idempotency_key: "cancelled-tool:model-completed",
        },
    )
    .await;
    let cancelled_at = database_now(&pool).await.unix_millis();
    sqlx::query(
        "UPDATE runtime.runs
         SET status = $3, finished_at_ms = $2, last_run_sequence = 3
         WHERE run_id = $1",
    )
    .bind(first_run_id.into_uuid())
    .bind(cancelled_at)
    .bind(if succeeded { 6_i16 } else { 7_i16 })
    .execute(&pool)
    .await
    .expect("cancel prior Run");

    let next_run_id = RunId::generate();
    let next = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: first.conversation_id,
        run_id: next_run_id,
        policy_ref: &policy_ref,
        route: openai_responses_route(),
        context_policy: None,
        device_tools: false,
        parallel_history_tools: false,
    })
    .await;
    replace_snapshot_skill_with_fixture_content(
        &pool,
        &content,
        tenant_id,
        next_run_id,
        &next,
        &policy_ref,
    )
    .await;
    let follow_up_ref = put_text(
        &content,
        tenant_id,
        "Continue after the cancelled Tool call.",
    )
    .await;
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: first.conversation_id,
            run_id: next_run_id,
            expected_offset: current_ledger_offset(&pool, tenant_id, first.conversation_id).await,
            run_seq_start: 1,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id: MessageId::generate(),
                    role: MessageRole::User,
                    content_ref: follow_up_ref,
                    attachment_refs: Vec::new(),
                    requested_skill_refs: Vec::new(),
                },
            )],
            participant_id: Some(next.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "cancelled-tool:follow-up",
        },
    )
    .await;

    let material = PostgresContextMaterialSource::new(
        pool,
        content,
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
    )
    .load_inner(&ContextPlanInput {
        mode: musteragent_context::ContextPlanMode::ToolCapable,
        active_model_route_ordinal: 0,
        attempt: next.attempt,
        next_iteration: 1,
        invocation_timing:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                .expect("current timing"),
        continuation_ref: None,
        continuation_source_invocation_id: None,
        consecutive_empty_responses: 0,
    })
    .await
    .expect("cancelled incomplete Tool exchange must not poison the next Context");

    assert_eq!(material.entries.len(), 4);
    let expected = if succeeded {
        "verified document parsing succeeded"
    } else {
        "cancelled"
    };
    assert!(
        material.entries.iter().any(|entry| {
            entry.message.role == CanonicalRole::Tool && entry.message.content.iter().any(|part|
                matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains(expected)))
        }),
        "the interrupted ToolCall must retain its verified outcome"
    );
    assert_eq!(
        material.entries.last().expect("follow-up").message.content,
        vec![CanonicalContentPart::Text(
            ModelText::new("Continue after the cancelled Tool call.").expect("follow-up text")
        )]
    );
}

pub(super) async fn replace_snapshot_skill_with_fixture_content(
    pool: &PgPool,
    content_store: &Arc<dyn ContentStore>,
    tenant_id: TenantId,
    run_id: RunId,
    fixture: &RunningContextFixture,
    policy_ref: &musteragent_content_ref::ContentRef,
) {
    let instructions_ref = put_text(
        content_store,
        tenant_id,
        "Advance this context-materialization regression fixture.",
    )
    .await;
    let mut snapshot_content = snapshot_content(run_id, policy_ref.clone());
    let mut skill = snapshot_content
        .skills
        .first()
        .cloned()
        .expect("fixture Snapshot has a base Skill");
    skill.instructions_ref = instructions_ref.clone();
    skill.instructions_hash = instructions_ref.sha256;
    snapshot_content.skills = vec![skill.clone()];
    let discovery = snapshot_content
        .discovery
        .as_mut()
        .expect("current discovery");
    discovery
        .authorized_skills
        .retain(|entry| entry.skill_ref == skill.skill_ref);
    let descriptor = discovery
        .authorized_skills
        .first_mut()
        .expect("base descriptor");
    descriptor.instructions_ref = skill.instructions_ref.clone();
    descriptor.instructions_hash = skill.instructions_hash;
    for reference in &mut descriptor.references {
        if reference.kind == musteragent_discovery::SkillCatalogReferenceKind::Instructions {
            reference.content_ref = skill.instructions_ref.clone();
        }
    }
    discovery.authorized_skill_catalog_hash =
        musteragent_discovery::AuthorizedSkillCatalog::new(discovery.authorized_skills.clone())
            .expect("fixture catalog")
            .catalog_hash();
    musteragent_conformance::fixtures::source_selection_snapshot::refresh_initial_disclosures(
        &mut snapshot_content,
    );
    snapshot_content
        .validate()
        .expect("fixture-owned Skill keeps the Snapshot valid");
    let now = database_now(pool).await;
    let snapshot = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot_content,
        now,
        &ProtobufSnapshotCodec,
    )
    .expect("seal fixture-owned Skill Snapshot");
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET schema_version = $2, encoded_snapshot = $3,
             content_hash = $4, created_at_ms = $5
         WHERE runtime_snapshot_id = $1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(snapshot.content.schema_version.as_str())
    .bind(&snapshot.encoded_content)
    .bind(snapshot.content_hash.into_bytes().to_vec())
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("replace Snapshot with fixture-owned Skill content");
}
