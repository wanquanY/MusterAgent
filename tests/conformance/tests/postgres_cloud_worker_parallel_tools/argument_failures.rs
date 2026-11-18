//! Production Loop, schema validator, Tool journal and Context reconstruction.
use super::*;
use musteragent_runtime_contract::ToolArgumentValidationIssueKind;

#[derive(Default)]
struct ArgumentModel {
    requests: Mutex<Vec<CanonicalModelRequest>>,
}
impl ModelProvider for ArgumentModel {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        let n = self.requests.lock().expect("valid runtime fixture").len();
        let model_key = musteragent_model::ModelKey::new(request.route.resolved_model_key.clone())
            .expect("valid runtime fixture");
        self.requests
            .lock()
            .expect("valid runtime fixture")
            .push(request);
        assert!(
            n < 4,
            "argument failures must not create invisible model retries"
        );
        let mut events = vec![Ok(ModelStreamEvent::ResponseStarted {
            response_ref: ProviderRequestRef::from_opaque(
                OpaqueExternalRef::new(format!("args-response-{n}"))
                    .expect("valid runtime fixture"),
            ),
            response_model_key: model_key,
        })];
        let calls: &[(&str, bool)] = match n {
            0 => &[
                ("missing-fields", false),
                ("malformed-json", false),
                ("sibling-good", true),
            ],
            1 => &[("length-limit", false)],
            2 => &[("repaired-small-call", true)],
            _ => &[],
        };
        for (i, (id, valid)) in calls.iter().enumerate() {
            let call_id = ToolCallId::new(*id).expect("valid runtime fixture");
            events.push(Ok(ModelStreamEvent::ToolCallStarted {
                output_index: i as u32,
                call_id: call_id.clone(),
                name: WireName::new("read_a").expect("valid runtime fixture"),
                provider_item_ref: None,
                provider_replay: None,
            }));
            events.push(Ok(if *id == "malformed-json" {
                ModelStreamEvent::ToolCallRejected {
                    diagnostic: None,
                    output_index: i as u32,
                    call_id,
                    kind: ToolArgumentValidationIssueKind::MalformedJson,
                }
            } else {
                ModelStreamEvent::ToolCallCompleted {
                    output_index: i as u32,
                    call_id,
                    arguments: JsonValue::Object(if *valid {
                        BTreeMap::from([("text".to_owned(), JsonValue::String("small".to_owned()))])
                    } else {
                        BTreeMap::new()
                    }),
                }
            }));
        }
        if n == 3 {
            events.push(Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("Finished after correcting the tool calls.")
                    .expect("valid runtime fixture"),
            }));
        }
        events.push(Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 10,
            output_tokens: 10,
            total_tokens: Some(20),
            ..Default::default()
        })));
        events.push(Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: match n {
                1 => StopReason::MaxOutputTokens,
                3 => StopReason::Stop,
                _ => StopReason::ToolCalls,
            },
        }));
        Box::pin(stream::iter(events))
    }
}

#[tokio::test]
async fn invalid_and_truncated_calls_are_tools_and_the_full_run_completes() {
    let _guard = POSTGRES_PARALLEL_TOOLS_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("valid runtime fixture");
    let pool = postgres.pool().await.expect("valid runtime fixture");
    migrate(&pool).await.expect("valid runtime fixture");
    let keys = keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let (fixture, attempt) = running_loop::seed_running_loop_with_input(
        &pool,
        content.as_ref(),
        b"Make a document; correct individual tool errors and finish.",
    )
    .await;
    let mut snapshot = loop_runtime_snapshot(CapabilityProviderId::generate());
    for tool in &mut snapshot.tools {
        tool.input_schema_json = br#"{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}"#.to_vec();
        tool.manifest.input_schema.schema_hash = Sha256::digest(&tool.input_schema_json);
    }
    let context = install_production_context(
        &pool,
        content.clone(),
        fixture,
        codec.clone(),
        live_runtime::upgrade_snapshot(content.clone(), run_context(fixture), snapshot).await,
        b"Make a document; correct individual tool errors and finish.",
    )
    .await;
    let dispatcher = Arc::new(ConcurrentProvider {
        content: content.clone(),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        parallel_entry: Barrier::new(1),
    });
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
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
            dispatcher,
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
            content,
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
    let model = Arc::new(ArgumentModel::default());
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock(now)),
        policy,
        Arc::new(owned_model::OwnedScriptedModel(model.clone())),
        journal,
    )
    .with_execution_supervision(live_runtime::supervision(&pool));
    let exit = execute_loop(&runtime, fixture, attempt).await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    {
        let requests = model.requests.lock().expect("valid runtime fixture");
        assert_eq!(requests.len(), 4);
        let feedback = |n: usize, id: &str| -> serde_json::Value {
            let message = requests[n]
                .input
                .iter()
                .find(|m| {
                    m.role == CanonicalRole::Tool
                        && m.tool_call_id.as_ref().is_some_and(|c| c.as_str() == id)
                })
                .expect("durable tool feedback in next model request");
            let CanonicalContentPart::Text(t) = &message.content[0] else {
                panic!("feedback")
            };
            serde_json::from_str(t.as_str()).expect("valid runtime fixture")
        };
        // The model-facing message is generic; the exact failing paths are
        // carried by the structured repair diagnostic.
        let missing = feedback(1, "missing-fields");
        assert_eq!(missing["ok"], false, "{missing}");
        assert_eq!(
            missing["diagnostic"]["repair"]["groups"][0]["paths"][0], "/text",
            "{missing}"
        );
        assert!(
            feedback(1, "malformed-json")["error"]["message"]
                .as_str()
                .expect("valid runtime fixture")
                .contains("valid JSON")
        );
        assert_eq!(feedback(1, "sibling-good")["ok"], true);
        let truncated = feedback(2, "length-limit");
        assert_eq!(
            truncated["diagnostic"]["error"]["kind"], "truncated",
            "{truncated}"
        );
        assert!(
            truncated["error"]["message"]
                .as_str()
                .expect("valid runtime fixture")
                .contains("token limit"),
            "{truncated}"
        );
        assert_eq!(feedback(3, "repaired-small-call")["ok"], true);
    }
    let rejected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.model_action_rejections AS rejection JOIN runtime.model_invocations AS invocation USING(invocation_id) WHERE invocation.run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("valid runtime fixture");
    assert_eq!(rejected, 0);
    let completed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime.model_invocations WHERE run_id=$1 AND status=2",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("valid runtime fixture");
    assert_eq!(
        completed, 4,
        "all physical responses, including usage, are finalized normally"
    );
    pool.close().await;
}
