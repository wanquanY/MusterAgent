use super::*;

pub(super) async fn run_selection_response_is_validated_before_resume_and_enters_production_context()
 {
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
        b"Ask me to choose a release channel, then continue with the selected value.",
    )
    .await;
    let now = database_now(&pool).await;
    let provider_id = CapabilityProviderId::generate();
    let contracts = interaction_contracts(fixture, provider_id, content.clone()).await;
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        contracts.snapshot,
        b"Ask me to choose a release channel, then continue with the selected value.",
    )
    .await;
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
    let mut dispatcher = CompositeCapabilityDispatcher::new(Arc::new(CapabilityRegistry::new()));
    dispatcher
        .register_atomic(Arc::new(InteractionCapabilityHandler::new(
            contracts.manifest,
            tool_journal.clone(),
            content.clone(),
            Arc::new(FixedClock(now)),
        )))
        .expect("register Interaction handler");
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            Arc::new(FixedMaterial(contracts.material)),
            tool_journal,
            content.clone(),
            Arc::new(dispatcher),
            Arc::new(FixedClock(now)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool)),
    );
    let model = Arc::new(StructuredInteractionThenCompletionModel {
        invocations: AtomicUsize::new(0),
        observed_response: AtomicBool::new(false),
        attempts: Mutex::new(Vec::new()),
        tool_call_id: "release-channel-selection",
        // The admitted bootstrap Tool is `interaction.request@2`, whose
        // arguments are the simplified question/choices contract.
        request_arguments: JsonValue::Object(BTreeMap::from([
            ("kind".into(), JsonValue::String("single_select".into())),
            (
                "question".into(),
                JsonValue::String("Select a release channel".into()),
            ),
            (
                "choices".into(),
                JsonValue::Array(vec![
                    JsonValue::String("stable".into()),
                    JsonValue::String("beta".into()),
                ]),
            ),
            ("allow_other".into(), JsonValue::Bool(false)),
        ])),
        expected_response_fragments: vec!["\"value\":\"beta\"", "\"status\":\"submitted\""],
    });
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixedExecutionPolicy {
                admitted_at: now,
                snapshot: contracts.execution_policy,
            }),
            context,
            tool_executor,
            Arc::new(EmptyControls),
        )),
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
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
        other => panic!("Selection must suspend the first Attempt, got {other:?}"),
    };
    let invalid_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"value":""}"#.to_vec(),
        })
        .await
        .expect("store invalid Selection response");
    let response_time = UtcTimestamp::from_unix_millis(now.unix_millis() + 1);
    let application = InteractionResponseApplication::new(
        runtime_application_store(&pool),
        content.clone(),
        codec.clone(),
        Arc::new(FixedClock(response_time)),
        Arc::new(PostgresInteractionApplicationStore::new(
            pool.clone(),
            keys,
            codec,
        )),
    );
    let invalid = application
        .respond_interaction(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    fixture,
                    current_sequence(&pool, fixture).await,
                    response_time,
                ),
                RespondInteraction {
                    interaction_id,
                    response_authority: InteractionResponseAuthority::DeviceUserPresence,
                    response: InteractionResponseRef::Content(invalid_ref),
                },
            )
            .expect("invalid Selection response command"),
        )
        .await
        .expect_err("empty required Selection must fail at the command boundary");
    assert_eq!(invalid.code, ErrorCode::InteractionResponseInvalid);
    let still_waiting: (i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, interaction.status,
                (SELECT count(*) FROM runtime.attempts WHERE run_id = run.run_id)
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rejected Selection state");
    assert_eq!(still_waiting, (3, 1, 1));

    let valid_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"value":"beta"}"#.to_vec(),
        })
        .await
        .expect("store valid Selection response");
    application
        .respond_interaction(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    fixture,
                    current_sequence(&pool, fixture).await,
                    response_time,
                ),
                RespondInteraction {
                    interaction_id,
                    response_authority: InteractionResponseAuthority::DeviceUserPresence,
                    response: InteractionResponseRef::Content(valid_ref),
                },
            )
            .expect("valid Selection response command"),
        )
        .await
        .expect("commit valid Selection response");
    let scheduler =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    let receipt = match scheduler
        .claim_one(
            &WorkerId::new("worker-selection-resume").expect("worker"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim Selection-resumed Attempt")
    {
        ClaimOutcome::Claimed(receipt) => receipt,
        ClaimOutcome::Idle { .. } => panic!("valid Selection response must requeue the Run"),
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
        execute_loop(&runtime, fixture, second_attempt.clone()).await,
        AgentLoopExit::Completed { .. }
    ));
    assert!(model.observed_response.load(Ordering::SeqCst));
    assert_eq!(
        lock(&model.attempts).as_slice(),
        &[first_attempt.attempt_id, second_attempt.attempt_id]
    );
    let terminal: (i16, i16, i16, i16, i64) = sqlx::query_as(
        "SELECT run.status, interaction.kind, interaction.status, tool.status,
                (SELECT count(*) FROM conversation.messages
                 WHERE run_id = run.run_id AND role = 2)
         FROM runtime.runs AS run
         JOIN approval.interactions AS interaction ON interaction.run_id = run.run_id
         JOIN tool.tool_invocations AS tool ON tool.run_id = run.run_id
         WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read completed Selection Run");
    assert_eq!(terminal, (5, 4, 2, 4, 1));
    pool.close().await;
}
