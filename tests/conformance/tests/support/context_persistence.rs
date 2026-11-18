use super::*;

pub(super) async fn database_now(pool: &PgPool) -> UtcTimestamp {
    let millis: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(pool)
            .await
            .expect("database time");
    UtcTimestamp::from_unix_millis(millis)
}

pub(super) async fn current_ledger_offset(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
) -> LedgerOffset {
    let value = sqlx::query_scalar::<_, String>(
        "SELECT last_sequence::text
         FROM event_store.ledger_streams
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read current Conversation offset")
    .parse::<u64>()
    .expect("parse current Conversation offset");
    LedgerOffset::new(value)
}

pub(super) async fn next_run_sequence(pool: &PgPool, run_id: RunId) -> u64 {
    sqlx::query_scalar::<_, String>(
        "SELECT (last_run_sequence + 1)::text
         FROM runtime.runs
         WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read next Run sequence")
    .parse::<u64>()
    .expect("parse next Run sequence")
}

/// The production cloud policy (@6) on a 48,000-token route resolves a
/// 46,976-token hard input budget and a 39,929-token compaction trigger.
const COMPACTION_ROUTE_WINDOW: u64 = 48_000;

pub(super) async fn run_hard_window_failure_fixture() {
    // Far beyond the hard input budget: only a successful compaction could fit.
    run_compaction_failure_fixture(280_000, false, true).await;
}

pub(super) async fn run_lossless_compaction_failure_fixture() {
    // Keep the rebuilt request above the compaction trigger but below the
    // hard input budget under the conservative ASCII byte/3 estimator.
    run_compaction_failure_fixture(LOSSLESS_HISTORY_BYTES, true, false).await;
}

// Current discovery includes file schemas; 30k history tokens plus that
// prefix remains between the production trigger and hard input budget.
const LOSSLESS_HISTORY_BYTES: usize = 90_000;

async fn run_compaction_failure_fixture(
    history_bytes: usize,
    expect_uncompressed_fit: bool,
    indeterminate: bool,
) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key_id = DottedKey::new("runtime.context.hard-window").expect("key id");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [61_u8; 32])]).expect("keyring"));
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
        &format!("logical history {}", "x".repeat(history_bytes)),
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
    let invocation_id = InvocationId::generate();
    let request_ref = put_text(&content, tenant_id, "old request").await;
    let assistant_ref = put_text(&content, tenant_id, "").await;
    let call_id = ToolCallId::new("hard-window-result").expect("call id");
    let wire_name = WireName::new("web_search").expect("wire name");
    let capability_result_ref = put_text(&content, tenant_id, "result").await;
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
                "content": [{"type": "text", "text": "small continuation delta"}]
            }))
            .expect("Tool message"),
        })
        .await
        .expect("store Tool message");
    let exchange = SeedToolExchange {
        tool_invocation_id: InvocationId::generate(),
        call_id,
        wire_name,
        arguments: serde_json::json!({"ordinal": 0}),
        tool_ref: "web.search@2",
        capability_result_ref,
        tool_message_ref,
    };
    seed_completed_tool_exchange(
        &pool,
        tenant_id,
        &old,
        invocation_id,
        &request_ref,
        &assistant_ref,
        std::slice::from_ref(&exchange),
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
                    invocation_id,
                    response_ref: assistant_ref,
                    continuation_ref: None,
                }),
                DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                    invocation_id: exchange.tool_invocation_id,
                    result_ref: exchange.capability_result_ref.clone(),
                    result_hash: exchange.capability_result_ref.sha256,
                    provider_request_ref: None,
                    authorized_content: Vec::new(),
                }),
            ],
            participant_id: Some(old.executor_id),
            producer: Producer::Worker,
            idempotency_key: "hard-window:old-completion",
        },
    )
    .await;
    sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
        .bind(old_run_id.into_uuid())
        .bind(database_now(&pool).await.unix_millis())
        .execute(&pool)
        .await
        .expect("complete old Run");

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
    let mut compression_routes = None;
    let (_, invocation_timing) = admit_production_compaction_snapshot(
        &pool,
        content.clone(),
        tenant_id,
        &current,
        current_run_id,
        COMPACTION_ROUTE_WINDOW,
        |snapshot| {
            // A lossless continuation is recorded against the durable
            // compression call, so it runs the production persisted
            // compactor against role providers that both reject the call.
            if !expect_uncompressed_fit {
                return;
            }
            let role = AuxiliaryModelRole::Compression;
            let template = snapshot
                .auxiliary_models
                .iter()
                .find(|pinned| pinned.role == role)
                .expect("compression route")
                .model
                .clone();
            let mut routes = super::auxiliary_wire::routes(template, role);
            for route in &mut routes {
                route.route.context_window = NonZeroU64::new(512_000).expect("window");
            }
            snapshot
                .auxiliary_models
                .retain(|pinned| pinned.role != role);
            snapshot.auxiliary_models.extend(
                routes
                    .iter()
                    .cloned()
                    .map(|model| PinnedAuxiliaryModelRoute { role, model }),
            );
            snapshot.auxiliary_models.sort_by_key(|pinned| pinned.role);
            compression_routes = Some(routes);
        },
    )
    .await;
    let current_user_ref = put_text(&content, tenant_id, "continue").await;
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id,
            conversation_id: old.conversation_id,
            run_id: current_run_id,
            expected_offset: current_ledger_offset(&pool, tenant_id, old.conversation_id).await,
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
            idempotency_key: "hard-window:current-user",
        },
    )
    .await;
    let failing = Arc::new(FailingContextCompactor {
        calls: AtomicUsize::new(0),
        indeterminate,
    });
    let ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let wire = match compression_routes {
        Some(routes) => Some(
            super::auxiliary_wire::Providers::start_with_standby(
                AuxiliaryModelRole::Compression,
                routes,
                content.clone(),
                false,
            )
            .await,
        ),
        None => None,
    };
    let compactor: Arc<dyn ContextCompactor> = match &wire {
        Some(wire) => {
            let clock =
                Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default());
            let journal = Arc::new(
                musteragent_postgres::PostgresFencedExecutionLivenessJournal::new(
                    pool.clone(),
                    Arc::new(ProtobufSnapshotCodec),
                    ledger_codec.clone(),
                    ledger_codec.clone(),
                    clock.clone(),
                ),
            );
            Arc::new(
                PersistedContextCompactor::new(
                    wire.adapter.clone(),
                    PostgresContextCompactionSlotStore::new(pool.clone(), keys.clone())
                        .with_execution_liveness(journal.clone()),
                    content.clone(),
                    Arc::new(ProtobufMeteringPublicationEncoder),
                    Arc::new(FixedRuntimeClock(database_now(&pool).await)),
                )
                .with_execution_supervision(
                    musteragent_loop::ExecutionSupervision::new(
                        journal,
                        clock,
                        Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
                    ),
                ),
            )
        }
        None => failing.clone(),
    };
    let planner = DurableContextPlanner::new(
        Arc::new(PostgresContextMaterialSource::new(
            pool.clone(),
            content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            ledger_codec.clone(),
        )),
        content,
        Arc::new(PostgresContextPlanStore::new(
            pool.clone(),
            ledger_codec.clone(),
        )),
        Arc::new(CanonicalJsonContextCodec),
        compactor,
    );
    let result = planner
        .prepare_model_iteration(ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: current.attempt,
            next_iteration: 1,
            invocation_timing,
            continuation_ref: Some(ContinuationRef::from_opaque(
                OpaqueExternalRef::new("provider:small-delta").expect("continuation"),
            )),
            continuation_source_invocation_id: Some(invocation_id),
            consecutive_empty_responses: 0,
        })
        .await;
    let compaction_calls = match &wire {
        Some(wire) => {
            wire.primary.captured_requests().await.len()
                + wire.standby.captured_requests().await.len()
        }
        None => failing.calls.load(Ordering::SeqCst),
    };
    assert!(
        compaction_calls >= 1,
        "Context planning must reach compaction: {:?}",
        result.as_ref().map(|plan| plan.iteration)
    );
    let plans: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.context_plans WHERE run_id = $1")
            .bind(current_run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("count Context plans");
    if expect_uncompressed_fit {
        let planned = result.expect("full uncompressed request still fits");
        assert_eq!(
            planned
                .request
                .continuation_ref
                .as_ref()
                .map(ContinuationRef::as_str),
            Some("provider:small-delta"),
            "failed compaction must losslessly retain the existing Provider continuation"
        );
        assert_eq!(plans, 1, "lossless uncompressed plan must be committed");
    } else {
        let error = result.expect_err("full logical request exceeds the hard window");
        assert_eq!(error.code, ErrorCode::DependencyUnavailable);
        // The production Persistent recovery policy schedules a durable retry
        // after an ambiguous compression instead of stopping the Run
        // (e97eff7217); the ambiguous paid call itself is never replayed.
        assert!(error.retryable, "the Run waits for a durable retry");
        assert_eq!(
            plans, 0,
            "hard-window failure must not publish a model request"
        );
    }
    let records = sqlx::query(
        "SELECT event.encoded_event
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event
           ON event.record_id = record.record_id
         WHERE record.run_id = $1",
    )
    .bind(current_run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("load hard-window Context events");
    let failed = records
        .into_iter()
        .filter(|row| {
            matches!(
                ledger_codec
                    .decode(
                        LedgerRecordKind::DomainEvent,
                        &row.get::<Vec<u8>, _>("encoded_event"),
                    )
                    .expect("decode Context event"),
                LedgerPayload::DomainEvent(DomainEvent::Context(
                    musteragent_context::ContextEvent::CompactionFailed { .. }
                ))
            )
        })
        .count();
    // Each rejected physical compression call (primary, then role standby,
    // since e97eff7217) records its own terminal failure. Indeterminate never
    // becomes a false terminal failure.
    let expected_failures = if indeterminate { 0 } else { compaction_calls };
    assert_eq!(
        failed, expected_failures,
        "Indeterminate must not be persisted as a false terminal compaction failure"
    );
}

pub(super) async fn seed_completed_tool_exchange(
    pool: &PgPool,
    tenant_id: TenantId,
    first: &RunningContextFixture,
    invocation_id: InvocationId,
    request_ref: &musteragent_content_ref::ContentRef,
    assistant_ref: &musteragent_content_ref::ContentRef,
    exchanges: &[SeedToolExchange],
) {
    let started_conversation_sequence =
        current_ledger_offset(pool, tenant_id, first.conversation_id)
            .await
            .next_sequence()
            .expect("next Tool-call Model sequence");
    seed_tool_call_model_invocation(
        pool,
        tenant_id,
        first,
        invocation_id,
        request_ref,
        assistant_ref,
        started_conversation_sequence,
    )
    .await;
    let now = database_now(pool).await;
    for (ordinal, exchange) in exchanges.iter().enumerate() {
        sqlx::query(
            "INSERT INTO runtime.pending_tool_calls (
                 run_id, invocation_id, tool_call_id, wire_name, arguments_json,
                 ordinal, completed_at_ms, result_content_id, result_content_hash,
                 result_content_bytes
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::numeric)",
        )
        .bind(first.attempt.run_id.into_uuid())
        .bind(invocation_id.into_uuid())
        .bind(exchange.call_id.as_str())
        .bind(exchange.wire_name.as_str())
        .bind(exchange.arguments.clone())
        .bind(i32::try_from(ordinal).expect("Tool ordinal"))
        .bind(now.unix_millis())
        .bind(exchange.tool_message_ref.content_id.into_uuid())
        .bind(exchange.tool_message_ref.sha256.into_bytes().to_vec())
        .bind(exchange.tool_message_ref.byte_length.to_string())
        .execute(pool)
        .await
        .expect("seed completed pending Tool call");
        sqlx::query(
            "INSERT INTO tool.tool_invocations (
                 invocation_id, tenant_id, conversation_id, run_id,
                 requested_by_attempt_id, execution_attempt_id, tool_call_id,
                 tool_ref, capability_provider_id, status, effect, approval_policy,
                 retry_contract, idempotency_key, request_content_id, request_hash,
                 result_content_id, deadline_ms, started_at_ms, finished_at_ms
             ) VALUES (
                 $1, $2, $3, $4, $5, $5, $6, $7, $8,
                 4, 1, 1, 1, $9, $10, $11, $12, $13, $14, $14
             )",
        )
        .bind(exchange.tool_invocation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(first.conversation_id.into_uuid())
        .bind(first.attempt.run_id.into_uuid())
        .bind(first.attempt.attempt_id.into_uuid())
        .bind(exchange.call_id.as_str())
        .bind(exchange.tool_ref)
        .bind(CapabilityProviderId::generate().into_uuid())
        .bind(format!("cross-run:tool:{}", exchange.tool_invocation_id))
        .bind(request_ref.content_id.into_uuid())
        .bind(request_ref.sha256.into_bytes().to_vec())
        .bind(exchange.capability_result_ref.content_id.into_uuid())
        .bind(now.unix_millis().saturating_add(60_000))
        .bind(now.unix_millis())
        .execute(pool)
        .await
        .expect("seed successful Tool invocation");
        sqlx::query(
            "INSERT INTO tool.tool_results (
                 invocation_id, result_content_id, result_hash,
                 result_schema_key, result_schema_hash, committed_at_ms
             ) VALUES ($1, $2, $3, 'schema.web-search-result@1', $4, $5)",
        )
        .bind(exchange.tool_invocation_id.into_uuid())
        .bind(exchange.capability_result_ref.content_id.into_uuid())
        .bind(exchange.capability_result_ref.sha256.into_bytes().to_vec())
        .bind(
            Sha256::digest(b"schema.web-search-result@1")
                .into_bytes()
                .to_vec(),
        )
        .bind(now.unix_millis())
        .execute(pool)
        .await
        .expect("seed Tool result");
    }
}

pub(super) async fn seed_cancelled_incomplete_tool_exchange(
    pool: &PgPool,
    tenant_id: TenantId,
    first: &RunningContextFixture,
    invocation_id: InvocationId,
    request_ref: &musteragent_content_ref::ContentRef,
    assistant_ref: &musteragent_content_ref::ContentRef,
    exchange: &SeedToolExchange,
) {
    let started_conversation_sequence =
        current_ledger_offset(pool, tenant_id, first.conversation_id)
            .await
            .next_sequence()
            .expect("next cancelled Tool-call Model sequence");
    seed_tool_call_model_invocation(
        pool,
        tenant_id,
        first,
        invocation_id,
        request_ref,
        assistant_ref,
        started_conversation_sequence,
    )
    .await;
    let now = database_now(pool).await;
    sqlx::query(
        "INSERT INTO runtime.pending_tool_calls (
             run_id, invocation_id, tool_call_id, wire_name, arguments_json,
             ordinal, completed_at_ms, result_content_id, result_content_hash,
             result_content_bytes
         ) VALUES ($1, $2, $3, $4, $5, 0, NULL, NULL, NULL, NULL)",
    )
    .bind(first.attempt.run_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(exchange.call_id.as_str())
    .bind(exchange.wire_name.as_str())
    .bind(serde_json::json!({"cancelled": true}))
    .execute(pool)
    .await
    .expect("seed incomplete pending Tool call");
    sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, execution_attempt_id, tool_call_id,
             tool_ref, capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id, request_hash,
             result_content_id, deadline_ms, started_at_ms, finished_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $5, $6, 'delegation.join@1', $7,
             7, 1, 1, 1, $8, $9, $10, NULL, $11, $12, $12
         )",
    )
    .bind(exchange.tool_invocation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(first.conversation_id.into_uuid())
    .bind(first.attempt.run_id.into_uuid())
    .bind(first.attempt.attempt_id.into_uuid())
    .bind(exchange.call_id.as_str())
    .bind(CapabilityProviderId::generate().into_uuid())
    .bind(format!(
        "cancelled-incomplete:tool:{}",
        exchange.tool_invocation_id
    ))
    .bind(request_ref.content_id.into_uuid())
    .bind(request_ref.sha256.into_bytes().to_vec())
    .bind(now.unix_millis().saturating_add(60_000))
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed cancelled Tool invocation");
}

async fn seed_tool_call_model_invocation(
    pool: &PgPool,
    tenant_id: TenantId,
    first: &RunningContextFixture,
    invocation_id: InvocationId,
    request_ref: &musteragent_content_ref::ContentRef,
    assistant_ref: &musteragent_content_ref::ContentRef,
    started_conversation_sequence: ConversationSeq,
) {
    let now = database_now(pool).await;
    sqlx::query(
        "INSERT INTO runtime.model_invocations (
             invocation_id, tenant_id, conversation_id, run_id, attempt_id,
             purpose, route_role, status, model_route_ref, credential_grant_ref,
             requested_model_key, response_model_key,
             pricing_version_ref, idempotency_key, request_content_id,
             request_hash, response_content_id, started_conversation_seq,
             started_at_ms, finished_at_ms, terminal_stop_reason
         ) VALUES (
             $1, $2, $3, $4, $5, 1, 0, 2, 'cloud.chat-primary@1',
             'grant_context_fixture', 'gpt-context-test', 'gpt-context-test',
             'catalog:test.price@1', $6, $7, $8, $9,
             $10, $11, $11, 2
         )",
    )
    .bind(invocation_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(first.conversation_id.into_uuid())
    .bind(first.attempt.run_id.into_uuid())
    .bind(first.attempt.attempt_id.into_uuid())
    .bind(format!("cross-run:model:{invocation_id}"))
    .bind(request_ref.content_id.into_uuid())
    .bind(request_ref.sha256.into_bytes().to_vec())
    .bind(assistant_ref.content_id.into_uuid())
    .bind(
        i64::try_from(started_conversation_sequence.get())
            .expect("Tool-call Model sequence fits PostgreSQL numeric fixture"),
    )
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed Tool-call model invocation");
}

pub(super) struct DomainEventAppend<'a> {
    pub(super) tenant_id: TenantId,
    pub(super) conversation_id: ConversationId,
    pub(super) run_id: RunId,
    pub(super) expected_offset: LedgerOffset,
    pub(super) run_seq_start: u64,
    pub(super) events: Vec<DomainEvent>,
    pub(super) participant_id: Option<ParticipantId>,
    pub(super) producer: Producer,
    pub(super) idempotency_key: &'a str,
}

pub(super) async fn append_domain_events(pool: &PgPool, request: DomainEventAppend<'_>) {
    let DomainEventAppend {
        tenant_id,
        conversation_id,
        run_id,
        expected_offset,
        run_seq_start,
        events,
        participant_id,
        producer,
        idempotency_key,
    } = request;
    let codec = ProtobufLedgerPayloadCodec;
    let now = database_now(pool).await;
    let mut fingerprint_bytes = Vec::new();
    let records = events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let bytes = codec.encode(LedgerPayload::DomainEvent(event));
            fingerprint_bytes.extend_from_slice(&bytes);
            ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: Some(run_id),
                run_seq: Some(
                    RunSeq::new(
                        run_seq_start
                            .checked_add(u64::try_from(index).expect("index"))
                            .expect("run sequence"),
                    )
                    .expect("run seq"),
                ),
                attempt_id: None,
                participant_id,
                producer,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload_hash: Sha256::digest(&bytes),
                payload: bytes,
            }
        })
        .collect();
    PostgresLedgerStore::new(pool.clone())
        .append(AppendRequest {
            tenant_id,
            conversation_id,
            expected_home_epoch: HomeEpoch::new(1).expect("home epoch"),
            expected_offset,
            idempotency_key: IdempotencyKey::new(idempotency_key).expect("idempotency"),
            command_fingerprint: Sha256::digest(&fingerprint_bytes),
            records,
            outbox: Vec::new(),
        })
        .await
        .expect("append cross-Run events");
}

pub(super) async fn seed_context_participants(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    principal_id: PrincipalId,
    requester_id: ParticipantId,
    executor_id: ParticipantId,
    now: UtcTimestamp,
) {
    let musteragent_id = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT agent_id
         FROM agent.agent_definitions
         WHERE agent_key = 'assistant' AND built_in AND lifecycle = 1",
    )
    .fetch_one(&mut **transaction)
    .await
    .expect("load built-in Kernel agent");
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind,
             principal_ref, agent_ref, service_ref, display_name,
             avatar_content_id, joined_at_ms, left_at_ms
         ) VALUES
             ($1, $3, $4, 1, $5, NULL, NULL, 'Context Tester', NULL, $6, NULL),
             ($2, $3, $4, 2, NULL, $7, NULL, 'Kernel', NULL, $6, NULL)",
    )
    .bind(requester_id.into_uuid())
    .bind(executor_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(principal_id.into_uuid())
    .bind(now.unix_millis())
    .bind(musteragent_id)
    .execute(&mut **transaction)
    .await
    .expect("seed canonical Context participants");
}

pub(super) async fn load_context_participants(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
) -> (ParticipantId, ParticipantId) {
    let row = sqlx::query_as::<_, (uuid::Uuid, uuid::Uuid)>(
        "SELECT requester.participant_id, executor.participant_id
         FROM conversation.conversations AS conversation
         JOIN conversation.participants AS requester
           ON requester.tenant_id = conversation.tenant_id
          AND requester.conversation_id = conversation.conversation_id
          AND requester.kind = 1
          AND requester.principal_ref = conversation.created_by
          AND requester.left_at_ms IS NULL
         JOIN agent.agent_definitions AS musteragent
           ON musteragent.agent_key = 'assistant'
          AND musteragent.built_in
          AND musteragent.lifecycle = 1
         JOIN conversation.participants AS executor
           ON executor.tenant_id = conversation.tenant_id
          AND executor.conversation_id = conversation.conversation_id
          AND executor.kind = 2
          AND executor.agent_ref = musteragent.agent_id
          AND executor.left_at_ms IS NULL
         WHERE conversation.tenant_id = $1 AND conversation.conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("load canonical Context participants");
    (
        ParticipantId::from_uuid(row.0).expect("valid requester participant"),
        ParticipantId::from_uuid(row.1).expect("valid executor participant"),
    )
}
