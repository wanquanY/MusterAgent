use super::*;

use musteragent_conformance::fixtures::source_selection_snapshot;

async fn append_message(
    pool: &PgPool,
    tenant_id: TenantId,
    fixture: &RunningContextFixture,
    run_seq: u64,
    content_ref: ContentRef,
) {
    append_domain_events(
        pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: fixture.conversation_id,
            run_id: fixture.attempt.run_id,
            expected_offset: current_ledger_offset(pool, tenant_id, fixture.conversation_id).await,
            run_seq_start: run_seq,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id: MessageId::generate(),
                    role: MessageRole::User,
                    content_ref,
                    attachment_refs: Vec::new(),
                    requested_skill_refs: Vec::new(),
                },
            )],
            participant_id: Some(fixture.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: &format!("isolation:{}:{run_seq}", fixture.attempt.run_id),
        },
    )
    .await;
}

#[tokio::test]
async fn frozen_isolation_excludes_pre_admission_private_sources_and_retains_late_roots() {
    verify_source_selection(true).await;
}

#[tokio::test]
async fn legacy_admission_retains_pre_admission_private_sources_and_late_roots() {
    verify_source_selection(false).await;
}

async fn verify_source_selection(isolated: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key = DottedKey::new("isolation.test").expect("key");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [41; 32])]).expect("keys")),
    ));
    let tenant_id = TenantId::generate();
    let policy = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let predecessor = seed_running_context(
        &pool,
        tenant_id,
        RunId::generate(),
        &policy,
        &put_text(&content, tenant_id, "predecessor root").await,
        None,
        false,
    )
    .await;
    sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
        .bind(predecessor.attempt.run_id.into_uuid())
        .bind(database_now(&pool).await.unix_millis())
        .execute(&pool)
        .await
        .expect("complete predecessor");
    let private = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: predecessor.conversation_id,
        run_id: RunId::generate(),
        policy_ref: &policy,
        route: openai_responses_route(),
        context_policy: None,
        device_tools: false,
        parallel_history_tools: false,
    })
    .await;
    let private_ref = put_text(&content, tenant_id, "private automation").await;
    append_message(&pool, tenant_id, &private, 1, private_ref.clone()).await;
    sqlx::query("UPDATE runtime.runs SET activity_kind = 3 WHERE run_id = $1")
        .bind(private.attempt.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("private Run");
    sqlx::query("UPDATE activity.activities SET kind = 3 WHERE activity_id = $1")
        .bind(private.activity_id.into_uuid())
        .execute(&pool)
        .await
        .expect("private Activity");
    let target = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: predecessor.conversation_id,
        run_id: RunId::generate(),
        policy_ref: &policy,
        route: openai_responses_route(),
        context_policy: None,
        device_tools: false,
        parallel_history_tools: false,
    })
    .await;
    append_message(
        &pool,
        tenant_id,
        &target,
        1,
        put_text(&content, tenant_id, "own target").await,
    )
    .await;
    append_message(
        &pool,
        tenant_id,
        &predecessor,
        2,
        put_text(&content, tenant_id, "late predecessor root").await,
    )
    .await;
    if isolated {
        let run_context = RunContext {
            tenant_id,
            conversation_id: target.conversation_id,
            activity_id: target.activity_id,
            activity_kind: ActivityKind::Chat,
            run_id: target.attempt.run_id,
            executor_participant_id: target.executor_id,
            requested_by_participant_id: target.requester_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::Tenant { tenant_id },
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).expect("epoch"),
            runtime_snapshot_id: target.runtime_snapshot_id,
        };
        let snapshot = source_selection_snapshot::isolated_snapshot(
            content.clone(),
            run_context,
            snapshot_content(target.attempt.run_id, policy).model.route,
        )
        .await;
        seed_builtin_skill_instruction_content(
            &pool,
            &snapshot.content.skills,
            database_now(&pool).await,
        )
        .await;
        sqlx::query("UPDATE capability.runtime_snapshots SET schema_version = $2, encoded_snapshot = $3, content_hash = $4 WHERE runtime_snapshot_id = $1")
            .bind(target.runtime_snapshot_id.into_uuid()).bind(snapshot.content.schema_version.as_str()).bind(snapshot.encoded_content).bind(snapshot.content_hash.into_bytes().to_vec()).execute(&pool).await.expect("freeze isolated Snapshot");
    }
    let source = Arc::new(PostgresContextMaterialSource::new(
        pool.clone(),
        content.clone(),
        Arc::new(ProtobufSnapshotCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
    ));
    let planner = DurableContextPlanner::new(
        source.clone(),
        content,
        Arc::new(PostgresContextPlanStore::new(
            pool,
            Arc::new(ProtobufLedgerPayloadCodec),
        )),
        Arc::new(CanonicalJsonContextCodec),
        Arc::new(FixtureContextCompactor),
    );
    let input = ContextPlanInput {
        mode: musteragent_context::ContextPlanMode::ToolCapable,
        active_model_route_ordinal: 0,
        attempt: target.attempt,
        next_iteration: 1,
        invocation_timing:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                .expect("current timing"),
        continuation_ref: isolated.then(|| {
            ContinuationRef::from_opaque(
                OpaqueExternalRef::new("provider:excluded-private-history").expect("continuation"),
            )
        }),
        continuation_source_invocation_id: isolated.then(InvocationId::generate),
        consecutive_empty_responses: 0,
    };
    let material = source.load_inner(&input).await.expect("load material");
    assert_eq!(
        material.source_content_refs.contains(&private_ref),
        !isolated
    );
    let ledger_texts = material
        .entries
        .iter()
        .flat_map(|entry| &entry.message.content)
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_ledger = if isolated {
        vec!["predecessor root", "own target", "late predecessor root"]
    } else {
        vec![
            "predecessor root",
            "private automation",
            "own target",
            "late predecessor root",
        ]
    };
    assert_eq!(ledger_texts, expected_ledger);
    let plan = planner
        .prepare_model_iteration(input.clone())
        .await
        .expect("prepare request");
    assert!(plan.request.continuation_ref.is_none());
    assert_eq!(
        plan.request.cache_policy,
        CachePolicy::ExplicitEphemeralPrefix
    );
    let texts = plan
        .request
        .input
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_input = if isolated {
        vec!["predecessor root", "late predecessor root", "own target"]
    } else {
        vec![
            "predecessor root",
            "private automation",
            "late predecessor root",
            "own target",
        ]
    };
    assert_eq!(texts, expected_input);
    assert_eq!(
        planner
            .prepare_model_iteration(input)
            .await
            .expect("replay request"),
        plan
    );
}
