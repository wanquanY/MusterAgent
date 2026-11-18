use super::*;

#[tokio::test]
async fn waiting_interaction_cancel_is_terminal_and_cannot_be_responded() {
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
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot.clone(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            Arc::new(
                DurableToolExecutor::new(
                    Arc::new(FixedMaterial(contracts.material)),
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
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            ApprovalThenCompletionModel {
                invocations: AtomicUsize::new(0),
                observed_tool_result: AtomicBool::new(false),
                attempts: Mutex::new(Vec::new()),
                expected_tool_result: "unused-after-cancellation",
            },
        ))),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
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
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt.clone()).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("Run must wait before cancellation, got {other:?}"),
    };
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);

    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(now)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    );
    let sequence = current_sequence(&pool, fixture).await;
    let cancel_id = CommandId::generate();
    let cancel = CommandEnvelope::new(
        metadata(cancel_id, fixture, sequence, now),
        CancelRun {
            run_id: fixture.run_id,
            reason: CancelReason::UserRequested,
        },
    )
    .expect("cancel command");
    assert_eq!(
        application
            .cancel_run(cancel.clone())
            .await
            .expect("cancel waiting Run")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .cancel_run(cancel)
            .await
            .expect("deduplicate waiting cancellation")
            .disposition,
        CommandDisposition::Duplicate
    );

    let response_sequence = current_sequence(&pool, fixture).await;
    let response = CommandEnvelope::new(
        metadata(CommandId::generate(), fixture, response_sequence, now),
        RespondInteraction {
            interaction_id,
            response_authority: InteractionResponseAuthority::DeviceUserPresence,
            response: InteractionResponseRef::Approval(ApprovalDecision::Approve),
        },
    )
    .expect("late response command");
    let late = application
        .respond_interaction(response)
        .await
        .expect_err("cancelled Interaction must reject late response");
    assert_eq!(late.code, ErrorCode::InteractionNotPending);
    assert!(
        matches!(
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder))
                .claim_one(
                    &WorkerId::new("worker-after-wait-cancel").expect("worker"),
                    Duration::from_secs(30),
                )
                .await
                .expect("query Scheduler"),
            ClaimOutcome::Idle { .. }
        ),
        "cancelling a waiting Run must not fabricate a resumed Attempt"
    );
    let terminal: (i16, i16, i16, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status, tool.status,
                (SELECT count(*) FROM runtime.attempts WHERE run_id = run.run_id),
                (SELECT count(*) FROM runtime.leases WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read cancelled waiting Run");
    assert_eq!(terminal, (7, 4, 7, 1, 0));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
}

#[tokio::test]
async fn interaction_multichannel_race_commits_one_response_and_one_resume() {
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
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = approval_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot.clone(),
        b"Perform the requested write only if the user approves it.",
    )
    .await;
    let provider = Arc::new(ApprovedWriteProvider {
        content: content.clone(),
        physical_calls: AtomicUsize::new(0),
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            Arc::new(
                DurableToolExecutor::new(
                    Arc::new(FixedMaterial(contracts.material)),
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
        Arc::new(owned_model::OwnedScriptedModel(Arc::new(
            ApprovalThenCompletionModel {
                invocations: AtomicUsize::new(0),
                observed_tool_result: AtomicBool::new(false),
                attempts: Mutex::new(Vec::new()),
                expected_tool_result: "unused-before-resume",
            },
        ))),
        Arc::new(
            PostgresAgentLoopJournal::new(
                pool.clone(),
                content.clone(),
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
    let interaction_id = match execute_loop(&runtime, fixture, first_attempt.clone()).await {
        AgentLoopExit::Waiting { interaction_id, .. } => interaction_id,
        other => panic!("Run must wait before the response race, got {other:?}"),
    };
    let sequence = current_sequence(&pool, fixture).await;
    let application = Arc::new(InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content,
        codec.clone(),
        Arc::new(FixedClock(now)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    ));
    let response = |decision| {
        CommandEnvelope::new(
            metadata(CommandId::generate(), fixture, sequence, now),
            RespondInteraction {
                interaction_id,
                response_authority: InteractionResponseAuthority::DeviceUserPresence,
                response: InteractionResponseRef::Approval(decision),
            },
        )
        .expect("competing Interaction response")
    };
    let desktop = application.clone();
    let channel = application.clone();
    let (desktop_result, channel_result) = tokio::join!(
        async move {
            desktop
                .respond_interaction(response(ApprovalDecision::Approve))
                .await
        },
        async move {
            channel
                .respond_interaction(response(ApprovalDecision::Deny))
                .await
        },
    );
    let outcomes = [desktop_result, channel_result];
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Ok(ack) if ack.disposition == CommandDisposition::Committed))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::InteractionNotPending | ErrorCode::ConversationSeqConflict
                    )
            ))
            .count(),
        1
    );
    let committed: (i16, i16, i64, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status,
                (SELECT count(*) FROM approval.interaction_responses
                 WHERE interaction_id = interaction.interaction_id),
                (SELECT count(*) FROM event_store.outbox
                 WHERE conversation_id = run.conversation_id AND topic = 'run.dispatch')
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read response race outcome");
    assert_eq!(committed, (1, 2, 1, 1));
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    assert!(matches!(
        scheduler
            .claim_one(
                &WorkerId::new("worker-response-race").expect("worker"),
                Duration::from_secs(30),
            )
            .await
            .expect("claim unique resumed Attempt"),
        ClaimOutcome::Claimed(_)
    ));
    assert!(matches!(
        scheduler
            .claim_one(
                &WorkerId::new("worker-response-race-extra").expect("worker"),
                Duration::from_secs(30),
            )
            .await
            .expect("query extra resumed Attempt"),
        ClaimOutcome::Idle { .. }
    ));
    assert_eq!(provider.physical_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
}
