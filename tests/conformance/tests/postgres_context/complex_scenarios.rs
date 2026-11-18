use super::*;

#[tokio::test]
async fn cancelled_incomplete_tool_exchange_recovers_explicit_outcome() {
    context_cancelled_tool_exchange::run_interrupted_tool_exchange_recovery(false).await;
}

#[tokio::test]
async fn failed_run_recovers_successful_tool_without_canonical_delivery() {
    context_cancelled_tool_exchange::run_interrupted_tool_exchange_recovery(true).await;
}

pub(super) struct CompactionHistory {
    pub(super) _postgres: IsolatedPostgres,
    pub(super) pool: PgPool,
    pub(super) keys: Arc<OpaqueRefKeyring>,
    pub(super) content: Arc<dyn ContentStore>,
    pub(super) tenant_id: TenantId,
    pub(super) old: RunningContextFixture,
    pub(super) policy_ref: ContentRef,
    pub(super) invocation_id: InvocationId,
    exchanges: Vec<SeedToolExchange>,
}

pub(super) async fn compaction_history(original_bytes: usize) -> CompactionHistory {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key_id = DottedKey::new("runtime.context.compaction").expect("key id");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [53_u8; 32])]).expect("keyring"));
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let tenant_id = TenantId::generate();
    let policy_ref = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let old_run_id = RunId::generate();
    let old_user_ref = put_text(
        &content,
        tenant_id,
        &format!("old requirement {}", "x".repeat(original_bytes)),
    )
    .await;
    let old = seed_running_context(
        &pool,
        tenant_id,
        old_run_id,
        &policy_ref,
        &old_user_ref,
        None,
        false,
    )
    .await;

    // This is a valid legacy root-scoped file search: the pinned Tool schema
    // declares `path` optional with a default of `.`. Keep it in a separate,
    // older atomic exchange so the next compaction projects it into the
    // structured summary instead of retaining it in the protected recent
    // Tool-result tail.
    let pathless_invocation_id = InvocationId::generate();
    let pathless_request_ref = put_text(&content, tenant_id, "pathless find request").await;
    let pathless_assistant_ref = put_text(&content, tenant_id, "").await;
    let pathless_call_id = ToolCallId::new("pathless-find").expect("pathless call id");
    let pathless_wire_name = WireName::new("find_files").expect("find wire name");
    let pathless_capability_result_ref = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json").expect("media"),
            bytes: serde_json::to_vec(&serde_json::json!({
                "path": ".",
                "files": [],
                "scanned_entries": 0,
                "skipped_entries": 0,
                "truncated": false
            }))
            .expect("pathless capability result"),
        })
        .await
        .expect("store pathless capability result");
    let pathless_result_text = format!(
        "workspace-root search completed {}",
        "historical-result ".repeat(1_024),
    );
    let pathless_tool_message_ref = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/vnd.musteragent.legacy.tool-result+json")
                .expect("media"),
            bytes: serde_json::to_vec(&serde_json::json!({
                "schema": "musteragent.legacy.runtime.tool-result.v2",
                "role": "tool",
                "tool_call_id": pathless_call_id.as_str(),
                "tool_name": pathless_wire_name.as_str(),
                "content": [{"type": "text", "text": pathless_result_text}]
            }))
            .expect("pathless Tool message"),
        })
        .await
        .expect("store pathless Tool message");
    let pathless_exchange = SeedToolExchange {
        tool_invocation_id: InvocationId::generate(),
        arguments: serde_json::json!({"glob": "**/*.rs", "limit": 10}),
        call_id: pathless_call_id,
        wire_name: pathless_wire_name,
        tool_ref: "device.workspace.file.find@1",
        capability_result_ref: pathless_capability_result_ref,
        tool_message_ref: pathless_tool_message_ref,
    };
    seed_completed_tool_exchange(
        &pool,
        tenant_id,
        &old,
        pathless_invocation_id,
        &pathless_request_ref,
        &pathless_assistant_ref,
        std::slice::from_ref(&pathless_exchange),
    )
    .await;
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: old.conversation_id,
            run_id: old_run_id,
            expected_offset: LedgerOffset::new(1),
            run_seq_start: 2,
            events: vec![
                DomainEvent::Model(musteragent_runtime_model::ModelEvent::InvocationCompleted {
                    invocation_id: pathless_invocation_id,
                    response_ref: pathless_assistant_ref,
                    continuation_ref: None,
                }),
                DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                    invocation_id: pathless_exchange.tool_invocation_id,
                    result_ref: pathless_exchange.capability_result_ref.clone(),
                    result_hash: pathless_exchange.capability_result_ref.sha256,
                    provider_request_ref: None,
                    authorized_content: Vec::new(),
                }),
            ],
            participant_id: Some(old.executor_id),
            producer: Producer::Worker,
            idempotency_key: "context-compaction:pathless-find",
        },
    )
    .await;

    let invocation_id = InvocationId::generate();
    let request_ref = put_text(&content, tenant_id, "old canonical request").await;
    let assistant_ref = put_text(&content, tenant_id, "").await;
    let mut exchanges = Vec::new();
    for (ordinal, call_id) in ["z-first", "m-second", "a-third"].into_iter().enumerate() {
        let call_id = ToolCallId::new(call_id).expect("call id");
        let wire_name = WireName::new("web_search").expect("wire name");
        let capability_result_ref = content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("application/json").expect("media"),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "ordinal": ordinal,
                    "ok": true
                }))
                .expect("capability result"),
            })
            .await
            .expect("store capability result");
        let tool_message_ref = content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("application/vnd.musteragent.legacy.tool-result+json")
                    .expect("media"),
                bytes: serde_json::to_vec(&serde_json::json!({
                    "schema": "musteragent.legacy.runtime.tool-result.v2",
                    "role": "tool",
                    "tool_call_id": call_id.as_str(),
                    "tool_name": wire_name.as_str(),
                    "content": [{"type": "text", "text": call_id.as_str()}]
                }))
                .expect("tool message"),
            })
            .await
            .expect("store Tool message");
        exchanges.push(SeedToolExchange {
            tool_invocation_id: InvocationId::generate(),
            call_id,
            wire_name,
            arguments: serde_json::json!({"ordinal": ordinal}),
            tool_ref: "web.search@2",
            capability_result_ref,
            tool_message_ref,
        });
    }
    seed_completed_tool_exchange(
        &pool,
        tenant_id,
        &old,
        invocation_id,
        &request_ref,
        &assistant_ref,
        &exchanges,
    )
    .await;
    let mut completed = vec![DomainEvent::Model(
        musteragent_runtime_model::ModelEvent::InvocationCompleted {
            invocation_id,
            response_ref: assistant_ref,
            continuation_ref: None,
        },
    )];
    completed.extend(exchanges.iter().map(|exchange| {
        DomainEvent::Tool(ToolEvent::InvocationSucceeded {
            invocation_id: exchange.tool_invocation_id,
            result_ref: exchange.capability_result_ref.clone(),
            result_hash: exchange.capability_result_ref.sha256,
            provider_request_ref: None,
            authorized_content: Vec::new(),
        })
    }));
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: old.conversation_id,
            run_id: old_run_id,
            expected_offset: LedgerOffset::new(3),
            run_seq_start: 4,
            events: completed,
            participant_id: Some(old.executor_id),
            producer: Producer::Worker,
            idempotency_key: "context-compaction:tool-exchange",
        },
    )
    .await;
    sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
        .bind(old_run_id.into_uuid())
        .bind(database_now(&pool).await.unix_millis())
        .execute(&pool)
        .await
        .expect("complete old Run");

    CompactionHistory {
        _postgres: postgres,
        pool,
        keys,
        content,
        tenant_id,
        old,
        policy_ref,
        invocation_id,
        exchanges,
    }
}

pub(super) async fn run_context_compaction_restarts_deterministically_with_fileops_and_complete_tool_units()
 {
    let CompactionHistory {
        _postgres,
        pool,
        keys,
        content,
        tenant_id,
        old,
        policy_ref,
        invocation_id,
        exchanges,
    } = compaction_history(24_576).await;
    let current_run_id = RunId::generate();
    let resolved_v2 = ResolvedContextBudgets {
        policy_hash: musteragent_runtime_snapshot::context_compaction_policy_v5_hash(),
        // Include the current file-tool bootstrap while retaining the same
        // bounded summary, exact tail and mandatory compaction assertions.
        hard_input_budget: NonZeroU64::new(32_768).expect("hard input"),
        trigger_budget: NonZeroU64::new(26_214).expect("trigger"),
        reserved_output_budget: NonZeroU64::new(1_024).expect("reserved"),
        recent_tail_budget: NonZeroU64::new(2_048).expect("recent tail"),
        summary_budget: NonZeroU64::new(1_024).expect("summary"),
    };
    let context_policy = ContextPolicySnapshot {
        max_input_tokens: resolved_v2.hard_input_budget,
        compaction_trigger_tokens: resolved_v2.trigger_budget,
        reserved_output_tokens: resolved_v2.reserved_output_budget,
        compaction_policy_ref: VersionedKey::new(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V5,
        )
        .expect("compaction"),
        resolved_v2: Some(resolved_v2),
        source_selection: Some(
            musteragent_runtime_snapshot::ContextSourceSelectionPolicy::LegacyAdmissionV1,
        ),
    };
    let current = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: old.conversation_id,
        run_id: current_run_id,
        policy_ref: &policy_ref,
        route: openai_responses_route(),
        context_policy: Some(context_policy.clone()),
        device_tools: true,
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
                    content_ref: current_user_ref,
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
    let summary_model = Arc::new(StructuredSummaryModel {
        calls: AtomicUsize::new(0),
        rolling_calls: AtomicUsize::new(0),
        response_model_key: None,
    });
    let compactor = Arc::new(context_compaction::supervised_compactor(
        &pool,
        &keys,
        content.clone(),
        summary_model.clone(),
        database_now(&pool).await,
    ));
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
        invocation_timing: context_compaction::supervised_model_timing(),
        continuation_ref: Some(ContinuationRef::from_opaque(
            OpaqueExternalRef::new("provider:old-chat-scope").expect("continuation"),
        )),
        continuation_source_invocation_id: Some(invocation_id),
        consecutive_empty_responses: 0,
    };
    let material = PostgresContextMaterialSource::new(
        pool.clone(),
        content.clone(),
        Arc::new(ProtobufSnapshotCodec),
        ledger_codec.clone(),
    )
    .load(&input)
    .await
    .expect("materialize pathless historical Tool call");
    let pathless_call = material
        .entries
        .iter()
        .flat_map(|entry| entry.message.tool_calls.iter())
        .find(|call| call.call_id.as_str() == "pathless-find")
        .expect("pathless historical Tool call");
    assert_eq!(pathless_call.name.as_str(), "find_files");
    assert!(
        matches!(
            &pathless_call.arguments,
            musteragent_runtime_model::JsonValue::Object(arguments)
                if !arguments.contains_key("path")
        ),
        "fixture must preserve a valid omitted optional path",
    );
    let pinned_find = material
        .runtime_snapshot
        .tools
        .iter()
        .find(|tool| tool.manifest.wire_name.as_str() == "find_files")
        .expect("pinned find_files definition");
    assert_eq!(
        pinned_find
            .file_operation
            .as_ref()
            .expect("pinned file operation")
            .identity_argument,
        "/path",
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&pinned_find.input_schema_json)
            .expect("pinned find_files schema")
            .pointer("/properties/path/default"),
        Some(&serde_json::json!(".")),
    );
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
    assert_eq!(
        summary_json.pointer("/file_operations/read"),
        Some(&serde_json::json!(["."])),
        "an omitted optional path must project to its pinned workspace-root default",
    );
    let compaction_audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM security_audit.records
         WHERE tenant_id = $1
           AND action = 9
           AND source_model_invocation_id IS NOT NULL",
    )
    .bind(tenant_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read compaction Model security audit evidence");
    assert_eq!(
        compaction_audit_count, 1,
        "Context compaction must commit its terminal Model audit evidence"
    );
    assert_eq!(
        planned
            .request
            .input
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [
            CanonicalRole::Assistant,
            CanonicalRole::Tool,
            CanonicalRole::Tool,
            CanonicalRole::Tool,
            CanonicalRole::User,
        ],
        "compaction must retain the complete latest parallel Tool exchange in the canonical history contract"
    );
    assert!(
        planned.request.continuation_ref.is_none(),
        "logical-history compaction must create a fresh Provider scope"
    );
    assert!(
        planned.request.input.iter().any(|message| {
            message.role == CanonicalRole::User
                && message.content.iter().any(|part| {
                    matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains("continue from all three"))
                })
        }),
        "compaction must retain logical messages outside the opaque continuation delta"
    );
    // The current handoff contract preserves canonical call/result pairs as
    // historical data. Current execution authority comes only from request.tools.
    assert!(
        planned
            .request
            .tools
            .iter()
            .all(|tool| tool.key.as_str() != "web.search@2"),
        "historical calls must not expose a Tool in the current Run"
    );
    assert_eq!(
        planned.request.input[0]
            .tool_calls
            .iter()
            .map(|call| call.call_id.as_str())
            .collect::<Vec<_>>(),
        exchanges
            .iter()
            .map(|exchange| exchange.call_id.as_str())
            .collect::<Vec<_>>(),
        "retain every parallel call in its original order"
    );
    for (message, exchange) in planned.request.input[1..4].iter().zip(&exchanges) {
        assert_eq!(message.tool_call_id.as_ref(), Some(&exchange.call_id));
        assert!(
            message.content.iter().any(|part| matches!(part,
            CanonicalContentPart::Text(text) if text.as_str() == exchange.call_id.as_str())),
            "retain the exact result paired to each historical call"
        );
    }
    assert!(planned.request.input.iter().any(|message| {
        message.role == CanonicalRole::User && message.content.iter().any(|part|
            matches!(part, CanonicalContentPart::Text(text) if text.as_str().starts_with("Historical reference (summary):")))
    }));
    assert!(
        planned
            .request
            .instructions
            .iter()
            .all(|text| !text.as_str().contains("Historical reference (summary):"))
    );
    let replay = new_planner()
        .prepare_model_iteration(input)
        .await
        .expect("restart replay of compacted Context");
    assert_eq!(
        replay, planned,
        "restart must reproduce the exact Context plan"
    );
    assert_eq!(
        summary_model.calls.load(Ordering::SeqCst),
        1,
        "deterministic replay must recover the persisted auxiliary response instead of invoking the Provider twice"
    );
    let plan_row = sqlx::query(
        "SELECT cardinality(retained_record_ids)::bigint AS retained_count,
                input_tokens_before, input_tokens_after
         FROM runtime.context_plans
         WHERE run_id = $1 AND iteration = 1",
    )
    .bind(current_run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("query retained provenance");
    assert_eq!(plan_row.get::<i64, _>("retained_count"), 5);
    assert!(
        plan_row.get::<i64, _>("input_tokens_before")
            > plan_row.get::<i64, _>("input_tokens_after")
    );
    assert!(
        u64::try_from(plan_row.get::<i64, _>("input_tokens_after")).expect("nonnegative tokens")
            < resolved_v2.trigger_budget.get()
    );
    let invocation = sqlx::query(
        "SELECT invocation.purpose, invocation.status,
                usage.purpose AS usage_purpose,
                usage.input_tokens_uncached, usage.output_tokens
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_usage_facts AS usage
           ON usage.invocation_id = invocation.invocation_id
         WHERE invocation.run_id = $1 AND invocation.purpose = 2",
    )
    .bind(current_run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("query persisted context-compaction invocation");
    assert_eq!(invocation.get::<i16, _>("purpose"), 2);
    assert_eq!(invocation.get::<i16, _>("status"), 2);
    assert_eq!(invocation.get::<i16, _>("usage_purpose"), 2);
    assert_eq!(invocation.get::<i64, _>("input_tokens_uncached"), 200);
    assert_eq!(invocation.get::<i64, _>("output_tokens"), 100);
    let records = sqlx::query(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event
           ON event.record_id = record.record_id
         WHERE record.run_id = $1
         ORDER BY record.conversation_seq",
    )
    .bind(current_run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("load compaction ledger records");
    let events = records
        .into_iter()
        .map(|row| {
            ledger_codec
                .decode(
                    LedgerRecordKind::DomainEvent,
                    &row.get::<Vec<u8>, _>("encoded_event"),
                )
                .expect("decode compaction event")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|payload| matches!(
                payload,
                LedgerPayload::DomainEvent(DomainEvent::Context(
                    musteragent_context::ContextEvent::CompactionStarted { .. }
                ))
            ))
            .count(),
        1,
        "replay must not publish a second CompactionStarted fact"
    );
    assert!(!events.iter().any(|payload| matches!(
        payload,
        LedgerPayload::DomainEvent(DomainEvent::Context(
            musteragent_context::ContextEvent::CompactionFailed { .. }
        ))
    )));

    sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
        .bind(current_run_id.into_uuid())
        .bind(database_now(&pool).await.unix_millis())
        .execute(&pool)
        .await
        .expect("complete first compacted Run");
    let continuation_run_id = RunId::generate();
    let continuation = seed_additional_running_context(AdditionalRunningContextSeed {
        pool: &pool,
        tenant_id,
        conversation_id: old.conversation_id,
        run_id: continuation_run_id,
        policy_ref: &policy_ref,
        route: openai_responses_route(),
        context_policy: Some(context_policy),
        device_tools: true,
        parallel_history_tools: false,
    })
    .await;
    let continuation_user_ref = put_text(
        &content,
        tenant_id,
        &format!("continue after the durable checkpoint {}", "y".repeat(512)),
    )
    .await;
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: old.conversation_id,
            run_id: continuation_run_id,
            expected_offset: current_ledger_offset(&pool, tenant_id, old.conversation_id).await,
            run_seq_start: 1,
            events: vec![DomainEvent::Conversation(
                ConversationEvent::MessageAccepted {
                    project_task_target: None,

                    message_id: MessageId::generate(),
                    role: MessageRole::User,
                    content_ref: continuation_user_ref,
                    attachment_refs: Vec::new(),
                    requested_skill_refs: Vec::new(),
                },
            )],
            participant_id: Some(continuation.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "context-compaction:rolling-user",
        },
    )
    .await;
    new_planner()
        .prepare_model_iteration(ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: continuation.attempt.clone(),
            next_iteration: 1,
            invocation_timing: context_compaction::supervised_model_timing(),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        })
        .await
        .expect("plan next Run from the Conversation-level checkpoint");
    let parent_snapshot_id = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
        "SELECT parent_snapshot_id FROM runtime.context_plans WHERE run_id = $1",
    )
    .bind(continuation_run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load rolling checkpoint parent");
    let first_snapshot_id = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT snapshot_id FROM runtime.context_plans WHERE run_id = $1",
    )
    .bind(current_run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load prior checkpoint");
    assert_eq!(parent_snapshot_id, Some(first_snapshot_id));
    assert_eq!(
        summary_model.calls.load(Ordering::SeqCst),
        1,
        "an unchanged compacted prefix must reuse the Conversation checkpoint without another Provider call"
    );
    assert_eq!(
        summary_model.rolling_calls.load(Ordering::SeqCst),
        0,
        "no delta means the prior summary is reused byte-for-byte rather than regenerated"
    );

    let crash_snapshot = enable_compression(snapshot_content_for_route(
        continuation_run_id,
        policy_ref.clone(),
        openai_responses_route(),
    ));
    let crash_model = Arc::new(CrashAfterDispatchModel {
        calls: AtomicUsize::new(0),
    });
    let crash_compactor = Arc::new(context_compaction::supervised_compactor(
        &pool,
        &keys,
        content.clone(),
        crash_model.clone(),
        database_now(&pool).await,
    ));
    let crash_record_id = LedgerRecordId::generate();
    let crash_source_seq = ConversationSeq::new(1).expect("source sequence");
    let mut crash_request = ContextCompactionRequest {
        capture_provider_failure: false,
        continuation: None,
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot::legacy_default(),
        slot: ContextCompactionSlotV1 {
            source: Default::default(),
            loop_iteration: 1,
            generation: NonZeroU32::new(1).expect("generation"),
            active_branch_id: None,
            ledger_cutoff: ConversationSeq::new(
                current_ledger_offset(&pool, tenant_id, continuation.conversation_id)
                    .await
                    .get(),
            )
            .expect("ledger cutoff"),
            source_seq_from: crash_source_seq,
            source_seq_to: crash_source_seq,
            retained_record_ids: vec![crash_record_id],
            previous_summary_ref: None,
            input_budget: 8_192,
            input_tokens_before: 1_024,
            summary_budget: 512,
            compaction_policy_ref: crash_snapshot.context_policy.compaction_policy_ref.clone(),
            plan_fingerprint: Sha256::digest(b"controlled-crash-compaction-plan-v1"),
        },
        invocation: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: RunContext {
                tenant_id,
                conversation_id: continuation.conversation_id,
                activity_id: continuation.activity_id,
                activity_kind: ActivityKind::Chat,
                run_id: continuation_run_id,
                executor_participant_id: continuation.executor_id,
                requested_by_participant_id: continuation.requester_id,
                parent_run_id: None,
                delegation_id: None,
                execution_scope: ExecutionScope::Tenant { tenant_id },
                control_home: RuntimeHome::Cloud,
                execution_home: RuntimeHome::Cloud,
                home_epoch: HomeEpoch::new(1).expect("home epoch"),
                runtime_snapshot_id: continuation.runtime_snapshot_id,
            },
            attempt_context: continuation.attempt.clone(),
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: context_compaction::supervised_model_timing(),
        },
        model: crash_snapshot.auxiliary_models[0].model.clone(),
        stream_budget: crash_snapshot.execution_policy.model_stream,
        serialized_history: serde_json::to_vec(&serde_json::json!({
            "schema_version": "musteragent.context.compaction-input.v4",
            "messages": [{"role": "user", "text": ["crash boundary"]}],
        }))
        .expect("canonical compaction history"),
        previous_summary: None,
        summarized_record_ids: vec![crash_record_id],
        max_output_tokens: NonZeroU64::new(512).expect("output budget"),
    };
    crash_request.slot.plan_fingerprint = crash_request
        .expected_plan_fingerprint()
        .expect("crash compaction plan fingerprint");
    let drift_model = Arc::new(StructuredSummaryModel {
        calls: AtomicUsize::new(0),
        rolling_calls: AtomicUsize::new(0),
        response_model_key: Some("model-drifted-by-provider"),
    });
    let drift_compactor = context_compaction::supervised_compactor(
        &pool,
        &keys,
        content.clone(),
        drift_model.clone(),
        database_now(&pool).await,
    );
    let mut drift_request = crash_request.clone();
    drift_request.invocation.invocation_id = InvocationId::generate();
    drift_request.slot.generation = NonZeroU32::new(2).expect("drift generation");
    drift_request.slot.plan_fingerprint = drift_request
        .expected_plan_fingerprint()
        .expect("model drift plan fingerprint");
    let drift_invocation_id = drift_request.invocation.invocation_id;
    let drift = drift_compactor
        .compact(drift_request)
        .await
        .expect_err("Provider response model drift must fail closed");
    assert_eq!(drift.code, ErrorCode::InternalInvariantViolated);
    assert_eq!(drift_model.calls.load(Ordering::SeqCst), 1);
    let drift_row = sqlx::query(
        "SELECT invocation.status, invocation.requested_model_key,
                invocation.response_model_key, count(usage.invocation_id)::bigint AS usage_count
         FROM runtime.model_invocations AS invocation
         LEFT JOIN runtime.model_usage_facts AS usage
           ON usage.invocation_id = invocation.invocation_id
         WHERE invocation.invocation_id = $1
         GROUP BY invocation.status, invocation.requested_model_key,
                  invocation.response_model_key",
    )
    .bind(drift_invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("query failed response-model drift invocation");
    assert_eq!(drift_row.get::<i16, _>("status"), 3);
    assert_eq!(
        drift_row.get::<String, _>("requested_model_key"),
        "context-compression"
    );
    assert_eq!(
        drift_row.get::<String, _>("response_model_key"),
        "model-drifted-by-provider"
    );
    assert_eq!(drift_row.get::<i64, _>("usage_count"), 1);

    let crashed = tokio::spawn({
        let compactor = Arc::clone(&crash_compactor);
        let request = crash_request.clone();
        async move { compactor.compact(request).await }
    })
    .await;
    assert!(
        crashed.is_err(),
        "fixture must crash after physical dispatch"
    );
    let indeterminate = crash_compactor
        .compact(crash_request.clone())
        .await
        .expect_err("Started without terminal proof must not dispatch again");
    assert_eq!(indeterminate.code, ErrorCode::DependencyUnavailable);
    assert!(!indeterminate.retryable, "Indeterminate is not replayable");
    assert_eq!(
        crash_model.calls.load(Ordering::SeqCst),
        1,
        "recovery without terminal proof must never repeat Provider dispatch"
    );
    let mut cross_wired = crash_request;
    cross_wired.serialized_history = serde_json::to_vec(&serde_json::json!({
        "schema_version": "musteragent.context.compaction-input.v4",
        "messages": [{"role": "user", "text": ["different"]}],
    }))
    .expect("different canonical history");
    let conflict = crash_compactor
        .compact(cross_wired.clone())
        .await
        .expect_err("same invocation with another request hash must fail closed");
    assert_eq!(conflict.code, ErrorCode::IdempotencyConflict);
    assert_eq!(crash_model.calls.load(Ordering::SeqCst), 1);

    let mut raced_request = cross_wired;
    raced_request.invocation.invocation_id = InvocationId::generate();
    raced_request.slot.generation = NonZeroU32::new(3).expect("race generation");
    raced_request.slot.plan_fingerprint = raced_request
        .expected_plan_fingerprint()
        .expect("raced compaction plan fingerprint");
    assert_slot_dispatch_race(&pool, &keys, &content, raced_request.clone()).await;
    durable_compression::assert_persistent_recovery(&pool, &keys, &content, raced_request).await;
}
