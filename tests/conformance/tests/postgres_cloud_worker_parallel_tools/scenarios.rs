use super::*;

#[tokio::test]
async fn cloud_worker_parallel_tools_execute_physically_and_commit_durably() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(pool.clone(), codec.clone(), codec.clone(), keys)
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
    );
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    install_loop_context(&pool, content.clone(), fixture).await;
    let provider = Arc::new(ConcurrentProvider {
        content: content.clone(),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        parallel_entry: Barrier::new(3),
    });
    let executor = DurableToolExecutor::new(
        production_tool_material_source(&pool, codec),
        journal,
        content,
        provider.clone(),
        Arc::new(FixedClock(database_now(&pool).await)),
        Arc::new(FixedTrace),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let before_tools: i64 = sqlx::query_scalar(
        "SELECT stream.last_sequence::bigint
         FROM runtime.runs AS run
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read Ledger sequence before Tools");
    let telemetry = attempt_telemetry(fixture, &attempt);
    let (first, second, third) = tokio::join!(
        executor.execute_with_telemetry(&attempt, &telemetry, planned_call("parallel-a")),
        executor.execute_with_telemetry(&attempt, &telemetry, planned_call("parallel-b")),
        executor.execute_with_telemetry(&attempt, &telemetry, planned_call("parallel-c")),
    );
    for result in [first, second, third] {
        assert!(matches!(
            result.expect("parallel Tool execution"),
            ToolExecutionOutcome::Completed {
                terminal: musteragent_loop::DurableToolCompletion::Succeeded,
                ..
            }
        ));
    }
    assert_eq!(provider.peak.load(Ordering::SeqCst), 3);
    let durable: (i64, String, String) = sqlx::query_as(
        "SELECT count(*), run.last_run_sequence::text, stream.last_sequence::text
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE invocation.run_id = $1 AND invocation.status = 4
         GROUP BY run.last_run_sequence, stream.last_sequence",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable Tool state");
    // Every Tool and liveness fact is appended to the Run's own Ledger stream
    // and the Run sequence tracks that stream exactly.
    assert_eq!(durable.0, 3);
    assert_eq!(durable.1, durable.2);
    assert!(
        durable.2.parse::<i64>().expect("Ledger sequence") > before_tools,
        "{durable:?} after {before_tools}"
    );
}

#[tokio::test]
async fn cloud_worker_loop_preserves_parallel_barriers_and_multi_turn_completion() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let tool_journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let (fixture, attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Run the mixed tool batch and report the result.",
    )
    .await;
    let read_provider_id = CapabilityProviderId::generate();
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        live_runtime::upgrade_snapshot(
            content.clone(),
            run_context(fixture),
            loop_runtime_snapshot(read_provider_id),
        )
        .await,
        b"Run the mixed tool batch and report the result.",
    )
    .await;
    let provider = Arc::new(BarrierProvider {
        content: content.clone(),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        parallel_entry: Barrier::new(2),
        first_pair_completed: Notify::new(),
        second_pair_completed: Notify::new(),
    });
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            production_tool_material_source(&pool, codec.clone()),
            tool_journal,
            content.clone(),
            provider.clone(),
            Arc::new(FixedClock(database_now(&pool).await)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool)),
    );
    let now = database_now(&pool).await;
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy { admitted_at: now }),
        context,
        tool_executor,
        Arc::new(EmptyControls),
    ));
    let journal = Arc::new(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec,
            Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
            keys,
            running_loop::recovery_keyring(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let model = Arc::new(TwoTurnModel::new());
    let loop_runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        policy,
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        journal,
    )
    .with_execution_supervision(live_runtime::supervision(&pool));

    assert!(matches!(
        execute_loop(&loop_runtime, fixture, attempt).await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(model.invocation.load(Ordering::SeqCst), 2);
    let model_requests = model.requests();
    assert_eq!(model_requests.len(), 2);
    assert!(
        model_requests[0]
            .instructions
            .iter()
            .any(|instruction| instruction.as_str() == FIXTURE_RUNTIME_POLICY_V1),
        "the full Loop fixture must use the production Snapshot-backed Context planner"
    );
    // The admitted Snapshot also exposes the production bootstrap Tools; the
    // five controlled fixture Tools must all be offered to the Model.
    assert_eq!(
        model_requests[0]
            .tools
            .iter()
            .filter(|tool| tool.key.as_str().starts_with("fixture."))
            .count(),
        5
    );
    let second_tool_calls = model_requests[1]
        .input
        .iter()
        .filter(|message| message.role == CanonicalRole::Assistant)
        .flat_map(|message| message.tool_calls.iter())
        .map(|call| call.call_id.as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        second_tool_calls,
        [
            "parallel-a",
            "parallel-b",
            "write-barrier",
            "parallel-c",
            "parallel-d",
        ],
        "the second physical Model call must receive the first call's ToolCall exchange in Provider source order"
    );
    let second_tool_results = model_requests[1]
        .input
        .iter()
        .filter(|message| message.role == CanonicalRole::Tool)
        .map(|message| {
            message
                .tool_call_id
                .as_ref()
                .expect("Tool Result call ID")
                .as_str()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(second_tool_results, second_tool_calls);
    let events = lock(&provider.events).clone();
    assert_eq!(
        provider.peak.load(Ordering::SeqCst),
        2,
        "mixed-batch provider events: {events:?}"
    );
    parallel_tool_events::assert_mixed_batch_barriers(&events);

    let source_order: Vec<String> = sqlx::query_scalar(
        "SELECT tool_call_id
         FROM runtime.pending_tool_calls
         WHERE run_id = $1 AND completed_at_ms IS NOT NULL
         ORDER BY ordinal",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("read source-ordered Tool results");
    assert_eq!(
        source_order,
        [
            "parallel-a",
            "parallel-b",
            "write-barrier",
            "parallel-c",
            "parallel-d",
        ]
    );
    let durable: (i64, i64, i16, String, String) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM tool.tool_invocations
               WHERE run_id = $1 AND status = 4),
             (SELECT count(*) FROM runtime.model_invocations
               WHERE run_id = $1 AND status = 2),
             run.status,
             run.last_run_sequence::text,
             stream.last_sequence::text
         FROM runtime.runs AS run
         JOIN event_store.ledger_streams AS stream
           ON stream.tenant_id = run.tenant_id
          AND stream.conversation_id = run.conversation_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read completed Loop state");
    assert_eq!(durable.0, 5);
    assert_eq!(durable.1, 2);
    assert_eq!(durable.2, 5);
    assert_eq!(durable.3, durable.4);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM conversation.messages
             WHERE run_id = $1 AND role = 2 AND lifecycle IN (1, 2)",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("count user-visible Assistant messages"),
        1,
        "one multi-turn Run must project as one user-visible Assistant message"
    );
}

#[tokio::test]
async fn mixed_tool_batch_waits_and_resumes_before_the_next_model_turn() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, first_attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Run the mixed Tool batch, waiting for the write decision when required.",
    )
    .await;
    let mut snapshot = loop_runtime_snapshot(CapabilityProviderId::generate());
    snapshot
        .tools
        .iter_mut()
        .find(|tool| tool.manifest.wire_name.as_str() == "write_barrier")
        .expect("write barrier Tool")
        .manifest
        .approval = ApprovalRequirement::ByPolicy;
    // Admission resolves an unattended ServiceManaged posture; this scenario
    // is an interactive chat Run whose ByPolicy write must ask the user.
    let mut supervised =
        live_runtime::upgrade_snapshot(content.clone(), run_context(fixture), snapshot).await;
    supervised.policy.permission_mode = musteragent_policy::PermissionMode::Supervised;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        supervised,
        b"Run the mixed Tool batch, waiting for the write decision when required.",
    )
    .await;
    let provider = Arc::new(BarrierProvider {
        content: content.clone(),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        parallel_entry: Barrier::new(2),
        first_pair_completed: Notify::new(),
        second_pair_completed: Notify::new(),
    });
    let model = Arc::new(TwoTurnModel::new());
    let now = database_now(&pool).await;
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy { admitted_at: now }),
            context,
            Arc::new(
                DurableToolExecutor::new(
                    production_tool_material_source(&pool, codec.clone()),
                    Arc::new(
                        PostgresFencedToolInvocationJournal::new(
                            pool.clone(),
                            codec.clone(),
                            codec.clone(),
                            keys.clone(),
                        )
                        .with_execution_liveness(
                            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                            live_runtime::clock(),
                        ),
                    ),
                    content.clone(),
                    provider.clone(),
                    Arc::new(FixedClock(now)),
                    Arc::new(FixedTrace),
                )
                .with_execution_supervision(live_runtime::supervision(&pool)),
            ),
            Arc::new(EmptyControls),
        )),
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content,
                codec.clone(),
                codec.clone(),
                Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
                keys.clone(),
                running_loop::recovery_keyring(),
            )
            .with_execution_liveness(
                Arc::new(musteragent_wire::ProtobufSnapshotCodec),
                live_runtime::clock(),
            ),
        ),
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("write barrier must suspend the first Attempt, got {other:?}"),
    };
    {
        let events = lock(&provider.events);
        parallel_tool_events::assert_pre_wait_parallel_events(&events);
    }
    let before_resume: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM runtime.pending_tool_calls
             WHERE run_id = $1 AND completed_at_ms IS NOT NULL),
            (SELECT count(*) FROM tool.tool_invocations
             WHERE run_id = $1 AND status = 2),
            (SELECT count(*) FROM tool.tool_invocations WHERE run_id = $1)",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read suspended mixed batch");
    assert_eq!(before_resume, (2, 1, 3));
    sqlx::query(
        "UPDATE approval.interactions
         SET expires_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1
         WHERE interaction_id = $1 AND status = 1",
    )
    .bind(interaction_id.into_uuid())
    .execute(&pool)
    .await
    .expect("make the barrier Interaction due");
    let scheduler = PostgresDispatchStore::with_interaction_expiry(
        pool.clone(),
        Arc::new(ProtobufSchedulerEventEncoder),
        codec,
        keys,
    );
    assert_eq!(
        scheduler.advance_due_work().await.expect("expire barrier"),
        1
    );
    let receipt = match scheduler
        .claim_one(
            &WorkerId::new("worker-mixed-batch-resume").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim mixed-batch resume")
    {
        ClaimOutcome::Claimed(receipt) => receipt,
        ClaimOutcome::Idle { .. } => panic!("expired barrier must requeue the Run"),
    };
    let second_attempt = AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id,
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    };
    assert!(matches!(
        execute_loop(&runtime, fixture, second_attempt).await,
        AgentLoopExit::Completed { .. }
    ));
    let terminal: (i64, i64, i64, i16) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM runtime.pending_tool_calls
             WHERE run_id = $1 AND completed_at_ms IS NOT NULL),
            (SELECT count(*) FROM tool.tool_invocations WHERE run_id = $1 AND status = 4),
            (SELECT count(*) FROM tool.tool_invocations WHERE run_id = $1 AND status = 5),
            (SELECT status FROM runtime.runs WHERE run_id = $1)",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read resumed mixed batch");
    assert_eq!(terminal, (5, 4, 1, 5));
    let events = lock(&provider.events).clone();
    assert!(!events.iter().any(|event| event.contains("write-barrier")));
    assert!(events.iter().any(|event| event == "end:parallel-c"));
    assert!(events.iter().any(|event| event == "end:parallel-d"));
    let requests = model.requests();
    let resumed_results = requests[1]
        .input
        .iter()
        .filter(|message| message.role == CanonicalRole::Tool)
        .map(|message| {
            (
                message
                    .tool_call_id
                    .as_ref()
                    .expect("Tool call ID")
                    .as_str(),
                &message.content,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        resumed_results
            .iter()
            .map(|(call_id, _)| *call_id)
            .collect::<Vec<_>>(),
        [
            "parallel-a",
            "parallel-b",
            "write-barrier",
            "parallel-c",
            "parallel-d"
        ]
    );
    assert!(matches!(
        &resumed_results[2].1[0],
        CanonicalContentPart::Text(text) if text.as_str().contains("interaction_expired")
    ));
    pool.close().await;
}

#[tokio::test]
async fn cancellation_flushes_uncommitted_model_output_before_the_unique_terminal() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    let loop_context = install_loop_context(&pool, content.clone(), fixture).await;
    let waiting = Arc::new(Notify::new());
    let model = Arc::new(PartialModel {
        waiting: Arc::clone(&waiting),
    });
    let cancellation_requested = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(PendingProvider {
        cancellation_requested,
        cancel_after: 1,
        started: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
        all_started: Notify::new(),
        all_dropped: Notify::new(),
    });
    let (runtime, _) =
        build_production_loop(&pool, keys, content, loop_context.clone(), model, provider).await;
    let supervisor = InvocationSupervisor::new();
    let cancelling = supervisor.clone();
    let telemetry = attempt_telemetry(fixture, &attempt);
    let task = tokio::spawn(async move { runtime.execute(attempt, telemetry, supervisor).await });
    tokio::time::timeout(Duration::from_secs(2), waiting.notified())
        .await
        .expect("model reached the pending stream boundary");
    cancelling.cancel();
    assert!(matches!(
        task.await.expect("join cancelled Loop"),
        AgentLoopExit::Cancelled { .. }
    ));

    let durable: (i64, i16, String, i16, i16, i16) = sqlx::query_as(
        "SELECT
             (SELECT count(*)
                FROM event_store.output_frames AS frame
                JOIN event_store.ledger_records AS record USING (record_id)
               WHERE record.run_id = $1),
             message.lifecycle,
             message.content_bytes::text,
             run.status,
             attempt.status,
             invocation.status
         FROM conversation.messages AS message
         JOIN runtime.runs AS run ON run.run_id = message.run_id
         JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
         JOIN runtime.model_invocations AS invocation ON invocation.run_id = run.run_id
         WHERE message.run_id = $1 AND message.role = 2",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancelled partial model state");
    assert_eq!(
        durable.0, 2,
        "data and terminal Frames must both be durable"
    );
    assert_eq!(
        durable.1, 2,
        "cancelled Assistant output is explicitly partial"
    );
    assert_eq!(durable.2, "durable before cancellation".len().to_string());
    assert_eq!(durable.3, 7, "Run is cancelled");
    assert_eq!(durable.4, 5, "Attempt is cancelled");
    assert_eq!(durable.5, 4, "physical Model invocation is cancelled");
}

#[tokio::test]
async fn process_shutdown_with_visible_partial_fails_closed_under_the_replacement_fence() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    let loop_context = install_loop_context(&pool, content.clone(), fixture).await;
    let waiting = Arc::new(Notify::new());
    let (runtime, cancellations) = build_production_loop(
        &pool,
        keys.clone(),
        content.clone(),
        loop_context.clone(),
        Arc::new(PartialModel {
            waiting: Arc::clone(&waiting),
        }),
        Arc::new(ConcurrentProvider {
            content: content.clone(),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            parallel_entry: Barrier::new(1),
        }),
    )
    .await;
    let dispatch = Arc::new(PostgresDispatchStore::new(
        pool.clone(),
        Arc::new(ProtobufSchedulerEventEncoder),
    ));
    let runner = Arc::new(
        CloudWorkerRunner::new(
            WorkerRunnerPolicy {
                max_inflight_runs: std::num::NonZeroUsize::new(1).expect("inflight Run"),
                lease_duration: Duration::from_secs(180),
                heartbeat_interval: Duration::from_millis(100),
                shutdown_drain_timeout: Duration::from_secs(2),
            },
            dispatch.clone(),
            cancellations,
            runtime,
        )
        .expect("Cloud Worker runner"),
    );
    let (dispatch_tx, dispatch_rx) = mpsc::channel(1);
    dispatch_tx
        .send(execution_receipt(fixture, &attempt))
        .await
        .expect("dispatch Attempt");
    let process_cancellation = CancellationToken::new();
    let runner_cancellation = process_cancellation.clone();
    let runner_task =
        tokio::spawn(async move { runner.run(dispatch_rx, runner_cancellation).await });

    tokio::time::timeout(Duration::from_secs(2), waiting.notified())
        .await
        .expect("model reached the pending stream boundary");
    process_cancellation.cancel();
    drop(dispatch_tx);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), runner_task)
            .await
            .expect("Worker shutdown is bounded")
            .expect("join Worker runner"),
        Ok(())
    );

    let save_point: (i64, i16, i16, i16, i16, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM event_store.output_frames AS frame
                JOIN event_store.ledger_records AS record USING (record_id)
               WHERE record.run_id = $1),
             (SELECT status FROM runtime.model_invocations WHERE run_id = $1),
             run.status,
             attempt.status,
             lease.surrender_reason,
             lease.expires_at_ms
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.attempt_id = $2
         JOIN runtime.leases AS lease ON lease.attempt_id = attempt.attempt_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read graceful shutdown save point");
    assert_eq!(save_point.0, 1, "the received delta is durable before exit");
    assert_eq!(
        save_point.1, 1,
        "the abandoned physical Model is still in flight"
    );
    assert_eq!(save_point.2, 2, "the Run remains recoverable, not terminal");
    assert_eq!(save_point.3, 1, "the old Attempt awaits Scheduler recovery");
    assert_eq!(save_point.4, 4, "the Lease records SystemShutdown");
    assert!(save_point.5 <= database_now(&pool).await.unix_millis());
    let abandoned_invocation_id: uuid::Uuid =
        sqlx::query_scalar("SELECT invocation_id FROM runtime.model_invocations WHERE run_id = $1")
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("read abandoned Model invocation identity");

    let recovered = dispatch
        .claim_one(
            &WorkerId::new("worker-after-shutdown").expect("recovery Worker"),
            Duration::from_secs(180),
        )
        .await
        .expect("claim surrendered Attempt");
    let ClaimOutcome::Claimed(recovered) = recovered else {
        panic!("gracefully surrendered work must be immediately recoverable");
    };
    assert_eq!(recovered.fence.attempt_number.get(), 2);
    assert!(recovered.fence.fencing_token.get() > attempt.fencing_token.get());
    assert_eq!(
        dispatch
            .heartbeat(
                &execution_receipt(fixture, &attempt).fence,
                Duration::from_secs(180)
            )
            .await,
        Err(AttemptControlError::LeaseLost),
        "the old Worker is permanently fenced after recovery"
    );

    let recovered_attempt = AttemptContext {
        attempt_id: recovered.fence.attempt_id,
        run_id: recovered.fence.run_id,
        attempt_number: recovered.fence.attempt_number,
        worker_id: recovered.fence.worker_id.clone(),
        lease_id: recovered.fence.lease_id,
        fencing_token: recovered.fence.fencing_token,
        lease_expires_at: recovered.lease_expires_at,
    };
    let (recovery_runtime, _) = build_production_loop(
        &pool,
        keys,
        content.clone(),
        loop_context.clone(),
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            RecoveryCompletionModel,
        ))),
        Arc::new(ConcurrentProvider {
            content,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            parallel_entry: Barrier::new(1),
        }),
    )
    .await;
    // Production admission seals the Persistent recovery policy (8ffd8a38f3):
    // a NonResumable call whose partial is already sealed is closed as failed
    // and the Run continues in a new physical invocation under a new Attempt.
    let recovery_exit = recovery_runtime
        .execute(
            recovered_attempt.clone(),
            attempt_telemetry(fixture, &recovered_attempt),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(
            recovery_exit,
            AgentLoopExit::Failed {
                stage: musteragent_loop::AgentLoopFailureStage::RecoverCheckpoint,
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                receipt: None,
                ..
            }
        ),
        "the replacement fence schedules a new Attempt instead of resuming the call: {recovery_exit:?}"
    );

    let durable: (i64, i64, i16, i64, i16, i16) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM conversation.messages
               WHERE run_id = $1 AND role = 2 AND lifecycle = 2),
             (SELECT count(*) FROM conversation.messages
               WHERE run_id = $1 AND role = 2 AND lifecycle = 1),
             (SELECT status FROM runtime.model_invocations WHERE invocation_id = $3),
             (SELECT count(*) FROM runtime.model_invocations WHERE attempt_id = $2),
             run.status,
             (SELECT status FROM runtime.attempts WHERE attempt_id = $2)
         FROM runtime.runs AS run
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(recovered.fence.attempt_id.into_uuid())
    .bind(abandoned_invocation_id)
    .fetch_one(&pool)
    .await
    .expect("read recovered Run");
    assert_eq!(
        durable.0, 1,
        "the old public output remains an interrupted partial"
    );
    assert_eq!(durable.1, 0, "recovery creates no stitched active output");
    assert_eq!(durable.2, 3, "the interrupted physical Model fails closed");
    assert_eq!(
        durable.3, 0,
        "the replacement fence itself never dispatches a second physical Model"
    );
    assert_eq!(
        durable.4, 3,
        "the Run waits for its durable retry instead of terminating"
    );
    assert_eq!(
        durable.5, 4,
        "the replacement Attempt ends without a Model call"
    );
}

#[tokio::test]
async fn worker_cancellation_fans_out_to_parallel_tools_and_closes_durable_invocations() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    let loop_context = install_loop_context(&pool, content.clone(), fixture).await;
    let cancellation_requested = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(PendingProvider {
        cancellation_requested: Arc::clone(&cancellation_requested),
        cancel_after: 2,
        started: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
        all_started: Notify::new(),
        all_dropped: Notify::new(),
    });
    let (runtime, cancellations) = build_production_loop(
        &pool,
        keys,
        content,
        loop_context.clone(),
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            TwoTurnModel::new(),
        ))),
        provider.clone(),
    )
    .await;
    let runner = Arc::new(
        CloudWorkerRunner::new(
            WorkerRunnerPolicy {
                max_inflight_runs: std::num::NonZeroUsize::new(1).expect("inflight Run"),
                lease_duration: Duration::from_secs(1),
                heartbeat_interval: Duration::from_millis(100),
                shutdown_drain_timeout: Duration::from_secs(2),
            },
            Arc::new(SignalControls {
                cancellation_requested,
                lease_expires_at: attempt.lease_expires_at,
            }),
            cancellations,
            runtime,
        )
        .expect("Cloud Worker runner"),
    );
    let (dispatch_tx, dispatch_rx) = mpsc::channel(1);
    dispatch_tx
        .send(execution_receipt(fixture, &attempt))
        .await
        .expect("dispatch Attempt");
    let process_cancellation = CancellationToken::new();
    let runner_cancellation = process_cancellation.clone();
    let runner_task =
        tokio::spawn(async move { runner.run(dispatch_rx, runner_cancellation).await });

    if tokio::time::timeout(
        musteragent_testkit::integration_timeout(),
        provider.all_started.notified(),
    )
    .await
    .is_err()
    {
        let state: (i16, i16, i64, i64, Vec<i16>) = sqlx::query_as(
            "SELECT run.status, attempt.status,
                    (SELECT count(*) FROM runtime.model_invocations WHERE run_id = run.run_id),
                    (SELECT count(*) FROM runtime.pending_tool_calls WHERE run_id = run.run_id),
                    ARRAY(SELECT status FROM tool.tool_invocations WHERE run_id = run.run_id)
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
             WHERE run.run_id = $1",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("inspect stalled parallel Run");
        process_cancellation.cancel();
        runner_task.abort();
        panic!(
            "parallel provider calls did not start before the conformance deadline; \
             started={}, dropped={}, durable state={state:?}",
            provider.started.load(Ordering::SeqCst),
            provider.dropped.load(Ordering::SeqCst),
        );
    }
    tokio::time::timeout(
        musteragent_testkit::integration_timeout(),
        provider.all_dropped.notified(),
    )
    .await
    .expect("parallel provider futures were dropped by cancellation");
    tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        loop {
            let status: i16 =
                sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id = $1")
                    .bind(fixture.run_id.into_uuid())
                    .fetch_one(&pool)
                    .await
                    .expect("read Run status");
            if status == 7 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Worker committed cancellation");
    process_cancellation.cancel();
    drop(dispatch_tx);
    assert_eq!(runner_task.await.expect("join Worker runner"), Ok(()));

    let durable: (i64, i64, i16, i16, i64) = sqlx::query_as(
        "SELECT
             (SELECT count(*) FROM tool.tool_invocations
               WHERE run_id = $1 AND status = 7),
             (SELECT count(*) FROM tool.tool_invocations
               WHERE run_id = $1 AND status IN (1, 2, 3)),
             run.status,
             attempt.status,
             (SELECT count(DISTINCT idempotency_key)
                FROM event_store.ledger_records
               WHERE run_id = $1
                 AND idempotency_key = $2)
         FROM runtime.runs AS run
         JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .bind(format!("loop:terminal:{}:cancelled", fixture.run_id))
    .fetch_one(&pool)
    .await
    .expect("read durable Tool cancellation state");
    assert_eq!(provider.started.load(Ordering::SeqCst), 2);
    assert_eq!(provider.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(
        durable.0, 2,
        "both physically started read Tools are cancelled"
    );
    assert_eq!(durable.1, 0, "no durable Tool invocation remains active");
    assert_eq!(durable.2, 7);
    assert_eq!(durable.3, 5);
    assert_eq!(durable.4, 1, "Run terminal idempotency key is unique");
}

#[tokio::test]
async fn cancellation_marks_started_external_writes_indeterminate_instead_of_faking_cancelled() {
    let _suite_guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let keys: Arc<OpaqueRefKeyring> = keyring();
    let proof_keys = Arc::clone(&keys);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    let loop_context = install_loop_context(&pool, content.clone(), fixture).await;
    let cancellation_requested = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(PendingProvider {
        cancellation_requested,
        cancel_after: 1,
        started: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
        all_started: Notify::new(),
        all_dropped: Notify::new(),
    });
    let (runtime, _) = build_production_loop(
        &pool,
        keys,
        content,
        loop_context.clone(),
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(SingleWriteModel))),
        provider.clone(),
    )
    .await;
    let supervisor = InvocationSupervisor::new();
    let cancelling = supervisor.clone();
    let telemetry = attempt_telemetry(fixture, &attempt);
    let mut task =
        tokio::spawn(async move { runtime.execute(attempt, telemetry, supervisor).await });
    tokio::select! {
        () = provider.all_started.notified() => {}
        result = &mut task => panic!("Run exited before the external write started: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(2)) => {
            let state: (i16, i16, i64, i64, i64) = sqlx::query_as(
                "SELECT run.status, attempt.status,
                        (SELECT count(*) FROM runtime.model_invocations WHERE run_id = run.run_id),
                        (SELECT count(*) FROM runtime.pending_tool_calls WHERE run_id = run.run_id),
                        (SELECT count(*) FROM tool.tool_invocations WHERE run_id = run.run_id)
                 FROM runtime.runs AS run
                 JOIN runtime.attempts AS attempt ON attempt.run_id = run.run_id
                 WHERE run.run_id = $1",
            )
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("inspect stalled external write Run");
            task.abort();
            panic!("external write did not start before the conformance deadline; durable state={state:?}")
        }
    }
    cancelling.cancel();
    // An unconfirmed stop keeps the physical future until its sealed cleanup
    // and post-exit drain windows have elapsed.
    let windows = live_runtime::execution_policy()
        .liveness
        .expect("v14 liveness")
        .model_policy()
        .windows();
    tokio::time::timeout(
        windows.cleanup + windows.post_exit_drain + Duration::from_secs(2),
        provider.all_dropped.notified(),
    )
    .await
    .expect("external write future dropped");
    assert!(matches!(
        task.await.expect("join cancelled write Run"),
        AgentLoopExit::Cancelled { .. }
    ));

    let durable: (uuid::Uuid, i16, Option<i64>, bool, Vec<u8>, i16) = sqlx::query_as(
        "SELECT invocation.invocation_id, invocation.status, invocation.finished_at_ms,
                invocation.terminal_reason IS NOT NULL,
                invocation.reconciliation_ref_protected,
                run.status
         FROM tool.tool_invocations AS invocation
         JOIN runtime.runs AS run ON run.run_id = invocation.run_id
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read indeterminate external write");
    assert_eq!(
        durable.1, 6,
        "started write result remains explicitly unknown"
    );
    assert_eq!(
        durable.2, None,
        "indeterminate is not falsely marked finished"
    );
    assert!(durable.3, "indeterminate reason is durable");
    let invocation_id = InvocationId::from_uuid(durable.0).expect("invocation identity");
    let protected = musteragent_storage_protection::ProtectedOpaqueRef::from_bytes(durable.4)
        .expect("protected reconciliation identity");
    let context = format!("tool-invocation:{invocation_id}:reconciliation");
    assert_eq!(
        proof_keys
            .open(context.as_bytes(), &protected)
            .expect("decrypt reconciliation identity"),
        format!("test-provider:{invocation_id}"),
        "Run cancellation must preserve the Provider-owned identity verbatim"
    );
    assert_eq!(durable.5, 7, "the user-facing Run is still cancelled");
}
