use super::*;

#[tokio::test]
async fn postgres_planner_continues_from_committed_standby_summary_after_http_primary_failure() {
    let super::complex_scenarios::CompactionHistory {
        _postgres,
        pool,
        keys,
        content,
        tenant_id,
        old,
        policy_ref,
        invocation_id,
        ..
    } = super::complex_scenarios::compaction_history(192_000).await;
    let current_run_id = RunId::generate();
    let current = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: old.conversation_id,
        run_id: current_run_id,
        policy_ref: &policy_ref,
        route: openai_responses_route(),
        context_policy: None,
        device_tools: false,
        parallel_history_tools: false,
    })
    .await;
    let current_user_ref =
        put_text(&content, tenant_id, "continue from all three tool results").await;
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: old.conversation_id,
            run_id: current_run_id,
            expected_offset: LedgerOffset::new(7),
            run_seq_start: 1,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id: MessageId::generate(),
                    role: MessageRole::User,
                    content_ref: current_user_ref.clone(),
                    attachment_refs: Vec::new(),
                    requested_skill_refs: Vec::new(),
                },
            )],
            participant_id: Some(current.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "context-compaction:current-user",
        },
    )
    .await;

    let ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let run_context = RunContext {
        tenant_id,
        conversation_id: current.conversation_id,
        activity_id: current.activity_id,
        activity_kind: ActivityKind::Chat,
        run_id: current_run_id,
        executor_participant_id: current.executor_id,
        requested_by_participant_id: current.requester_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).unwrap(),
        runtime_snapshot_id: current.runtime_snapshot_id,
    };
    let policy = Arc::new(
        musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default().model_policy(),
    );
    let (mut snapshot, _, _) = musteragent_conformance::fixtures::liveness_admission::fixture(
        content.clone(),
        run_context,
        &current.attempt,
        policy,
    )
    .await;
    // Exercise admission's v14 liveness contract with the original conversation-history reader.
    // The two are independent in v14. The prior user's ledger and tools belong to this tenant.
    snapshot.content.model.route.context_window = NonZeroU64::new(48_000).unwrap();
    snapshot.content.model.route.max_output_tokens = NonZeroU64::new(1_024).unwrap();
    let resolved = musteragent_runtime_snapshot::ContextCompactionPolicyV6Spec::resolve(
        &snapshot.content.model.route,
    )
    .unwrap();
    snapshot.content.context_policy = ContextPolicySnapshot {
        max_input_tokens: resolved.hard_input_budget,
        compaction_trigger_tokens: resolved.trigger_budget,
        reserved_output_tokens: resolved.reserved_output_budget,
        compaction_policy_ref: VersionedKey::new(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V6,
        )
        .unwrap(),
        source_selection: Some(
            musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1,
        ),
        resolved_v2: Some(resolved),
    };
    context_compaction::configure_compression_route(&mut snapshot.content);
    let role = musteragent_model::AuxiliaryModelRole::Compression;
    let template = snapshot
        .content
        .auxiliary_models
        .iter()
        .find(|p| p.role == role)
        .unwrap()
        .model
        .clone();
    let mut routes = auxiliary_wire::routes(template, role);
    for route in &mut routes {
        route.route.context_window = NonZeroU64::new(512_000).unwrap();
    }
    snapshot.content.auxiliary_models.retain(|p| p.role != role);
    snapshot.content.auxiliary_models.extend(
        routes
            .iter()
            .cloned()
            .map(|model| PinnedAuxiliaryModelRoute { role, model }),
    );
    snapshot.content.auxiliary_models.sort_by_key(|p| p.role);
    let snapshot = RuntimeSnapshot::seal(
        current.runtime_snapshot_id,
        snapshot.content,
        database_now(&pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("seal modern role-planner fixture");
    seed_builtin_skill_instruction_content(
        &pool,
        &snapshot.content.skills,
        database_now(&pool).await,
    )
    .await;
    let timing = snapshot
        .content
        .execution_policy
        .model_invocation_timing(database_now(&pool).await)
        .unwrap();
    sqlx::query("UPDATE capability.runtime_snapshots SET encoded_snapshot=$1,content_hash=$2,schema_version=$4 WHERE runtime_snapshot_id=$3")
        .bind(snapshot.encoded_content).bind(snapshot.content_hash.into_bytes().to_vec()).bind(current.runtime_snapshot_id.into_uuid())
        .bind(snapshot.content.schema_version.as_str()).execute(&pool).await.unwrap();
    let wire = auxiliary_wire::Providers::start(role, routes, content.clone()).await;
    let observer = Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default());
    let journal = Arc::new(
        musteragent_postgres::PostgresFencedExecutionLivenessJournal::new(
            pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
            ledger_codec.clone(),
            ledger_codec.clone(),
            observer.clone(),
        ),
    );
    let store = PostgresContextCompactionSlotStore::new(pool.clone(), keys)
        .with_execution_liveness(journal.clone());
    let compactor = Arc::new(
        PersistedContextCompactor::new(
            wire.adapter.clone(),
            store,
            content.clone(),
            Arc::new(ProtobufMeteringPublicationEncoder),
            Arc::new(FixedRuntimeClock(database_now(&pool).await)),
        )
        .with_execution_supervision(musteragent_loop::ExecutionSupervision::new(
            journal,
            observer,
            Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
        )),
    );
    let new_planner = || {
        DurableContextPlanner::new(
            Arc::new(PostgresContextMaterialSource::new(
                pool.clone(),
                content.clone(),
                Arc::new(ProtobufSnapshotCodec),
                ledger_codec.clone(),
            )),
            content.clone(),
            Arc::new(PostgresContextPlanStore::new(
                pool.clone(),
                ledger_codec.clone(),
            )),
            Arc::new(CanonicalJsonContextCodec),
            compactor.clone(),
        )
    };
    let input = ContextPlanInput {
        mode: musteragent_context::ContextPlanMode::ToolCapable,
        active_model_route_ordinal: 0,
        attempt: current.attempt.clone(),
        next_iteration: 1,
        invocation_timing: timing,
        continuation_ref: Some(ContinuationRef::from_opaque(
            OpaqueExternalRef::new("provider:old-chat-scope").expect("continuation"),
        )),
        continuation_source_invocation_id: Some(invocation_id),
        consecutive_empty_responses: 0,
    };
    let planned = new_planner()
        .prepare_model_iteration(input.clone())
        .await
        .expect("plan compacted Context");
    let summary_row = sqlx::query(
        "SELECT plan.summary_content_id, plan.summary_content_hash,
                plan.summary_content_bytes::text AS summary_content_bytes,
                content.media_type
         FROM runtime.context_plans AS plan
         JOIN artifact.content_refs AS content
           ON content.content_id = plan.summary_content_id
         WHERE plan.run_id = $1 AND plan.iteration = 1",
    )
    .bind(current_run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load compacted summary reference");
    let summary_ref = ContentRef {
        content_id: ContentId::from_uuid(summary_row.get("summary_content_id"))
            .expect("summary content ID"),
        media_type: MediaType::new(summary_row.get::<String, _>("media_type"))
            .expect("summary media type"),
        byte_length: summary_row
            .get::<String, _>("summary_content_bytes")
            .parse()
            .expect("summary byte length"),
        sha256: Sha256::from_bytes(summary_row.get::<Vec<u8>, _>("summary_content_hash"))
            .expect("summary hash"),
    };
    let summary_bytes = content
        .read(tenant_id, &summary_ref)
        .await
        .expect("read compacted summary");
    let summary_json = serde_json::from_slice::<serde_json::Value>(&summary_bytes)
        .expect("decode compacted summary JSON");
    assert!(
        summary_json["summary"]
            .as_str()
            .unwrap()
            .contains("original task")
    );
    let generated = summary_json["generator_invocation_id"].as_str().unwrap();
    let row: (String, i16) = sqlx::query_as("SELECT requested_model_key,status FROM runtime.model_invocations WHERE invocation_id=$1 AND run_id=$2")
        .bind(uuid::Uuid::parse_str(generated).unwrap()).bind(current_run_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(row, ("local-aux-1".into(), 2));
    assert!(planned.request.input.iter().any(|message| message.content.iter().any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("continue from all three tool results")))));
    let physical = || async {
        sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT count(DISTINCT m.invocation_id), count(DISTINCT usage.invocation_id), count(DISTINCT plan.snapshot_id)
             FROM runtime.model_invocations m
             LEFT JOIN runtime.model_usage_facts usage ON usage.invocation_id=m.invocation_id
             LEFT JOIN runtime.context_plans plan ON plan.run_id=m.run_id AND plan.iteration=1
             WHERE m.run_id=$1",
        )
        .bind(current_run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    assert_eq!(physical().await, (2, 1, 1));
    let restored = new_planner()
        .prepare_model_iteration(input.clone())
        .await
        .unwrap();
    assert_eq!(restored.request.input, planned.request.input);
    assert_eq!(physical().await, (2, 1, 1));
    // Revoke the exact source in this disposable database, then restore its
    // fixture state to exercise the independent real steering transaction.
    sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE tenant_id=$1 AND content_id=$2")
        .bind(tenant_id.into_uuid())
        .bind(current_user_ref.content_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        new_planner()
            .prepare_model_iteration(input.clone())
            .await
            .is_err()
    );
    assert_eq!(physical().await, (2, 1, 1));
    sqlx::query("UPDATE artifact.content_refs SET state=2 WHERE tenant_id=$1 AND content_id=$2")
        .bind(tenant_id.into_uuid())
        .bind(current_user_ref.content_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    control::consume(&pool, &content, tenant_id, &current, &ledger_codec).await;
    let legacy_claim = sqlx::query("INSERT INTO runtime.attempts SELECT (jsonb_populate_record(NULL::runtime.attempts, to_jsonb(a) || jsonb_build_object('supports_control_compaction_v1', false))).* FROM runtime.attempts a WHERE run_id=$1")
        .bind(current_run_id.into_uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        legacy_claim.as_database_error().unwrap().code().as_deref(),
        Some("55000"),
        "the previous worker cannot claim the dynamically changed source"
    );
    let mut old_attempt = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE runtime.attempts SET supports_control_compaction_v1=false WHERE attempt_id=$1",
    )
    .bind(current.attempt.attempt_id.into_uuid())
    .execute(&mut *old_attempt)
    .await
    .unwrap();
    let guarded_start = sqlx::query("SELECT runtime.require_control_compaction_reader($1,$2,$3)")
        .bind(tenant_id.into_uuid())
        .bind(current_run_id.into_uuid())
        .bind(current.attempt.attempt_id.into_uuid())
        .execute(&mut *old_attempt)
        .await
        .unwrap_err();
    assert_eq!(
        guarded_start.as_database_error().unwrap().code().as_deref(),
        Some("55000")
    );
    old_attempt.rollback().await.unwrap();
    let revised = new_planner()
        .prepare_model_iteration(input.clone())
        .await
        .expect("replan the same iteration with the newly consumed source");
    assert_ne!(planned.request.input, revised.request.input);
    assert!(revised.request.continuation_ref.is_none());
    assert!(revised.request.input.iter().any(|message| message.content.iter().any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("Use the revised source")))));
    let reopened = new_planner()
        .prepare_model_iteration(input.clone())
        .await
        .expect("recover the revised sealed plan");
    assert_eq!(revised.request.input, reopened.request.input);
    assert_eq!(physical().await, (2, 1, 2));
    control::consume(&pool, &content, tenant_id, &current, &ledger_codec).await;
    let third = new_planner()
        .prepare_model_iteration(input)
        .await
        .expect("a further correction reuses the still-authorized ancestor");
    assert_ne!(third.invocation_id, revised.invocation_id);
    assert_eq!(physical().await, (2, 1, 3));
    let body = wire.assert_same_input().await;
    assert!(body["input"].to_string().contains("old requirement"));
}

#[path = "auxiliary_standby/control.rs"]
mod control;
