use super::*;
use musteragent_postgres::{
    PostgresContextCompactionSlotStore, PostgresFencedExecutionLivenessJournal,
};
use musteragent_runtime_snapshot::{CONTEXT_COMPACTION_POLICY_V8, ContextCompactionPolicyV8Spec};
use musteragent_wire::ProtobufSnapshotCodec;

struct LimitThenFinal {
    stop: StopReason,
    input_tokens: u64,
    calls: AtomicUsize,
    compressions: AtomicUsize,
}
impl ModelProvider for LimitThenFinal {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        if request.route.route_ref.as_str() == "cloud.aux-compression@1" {
            self.compressions.fetch_add(1, Ordering::SeqCst);
            let mut events =
                final_no_usage_events_for(&ModelKey::new("compression-model").unwrap());
            events.insert(
                events.len() - 1,
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 5,
                    output_tokens: 2,
                    total_tokens: Some(7),
                    ..Default::default()
                }),
            );
            for event in &mut events {
                if let ModelStreamEvent::TextDelta { delta, .. } = event {
                    *delta = ModelText::new("## Goal\nFinish the test task.\n\n## Constraints & Preferences\n- Preserve tool results.\n\n## Progress\n### Done\n- [x] Earlier tool operations completed.\n\n### In Progress\n- [ ] Produce the answer.\n\n### Blocked\n- (none)\n\n## Key Decisions\n- **Recovery**: Continue the same Run.\n\n## Next Steps\n1. Answer the user.\n\n## Critical Context\n- The completed tools returned success.").unwrap();
                }
            }
            return Box::pin(tokio_stream::iter(events.into_iter().map(Ok)));
        }
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let model = ModelKey::new("gpt-context-test").unwrap();
        let events = if n < 4 {
            let mut events = tool_completion_events(&model);
            let id = ToolCallId::new(format!("history-{n}")).unwrap();
            for event in &mut events {
                match event {
                    ModelStreamEvent::ToolCallStarted { call_id, .. } => *call_id = id.clone(),
                    ModelStreamEvent::ToolCallArgumentsDelta { call_id, delta, .. } => {
                        *call_id = id.clone();
                        *delta = serde_json::json!({"notes": "historical context ".repeat(2_000)})
                            .to_string();
                    }
                    ModelStreamEvent::ToolCallCompleted {
                        call_id, arguments, ..
                    } => {
                        *call_id = id.clone();
                        *arguments = JsonValue::Object(BTreeMap::from([(
                            "notes".into(),
                            JsonValue::String("historical context ".repeat(2_000)),
                        )]));
                    }
                    _ => {}
                }
            }
            events.retain(|event| !matches!(event, ModelStreamEvent::Usage(_)));
            events
        } else if n == 4 {
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("empty-limit"),
                    response_model_key: model,
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: self.input_tokens,
                    output_tokens: 4096,
                    reasoning_tokens: 4096,
                    total_tokens: Some(self.input_tokens + 4096),
                    ..Default::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: self.stop,
                },
            ]
        } else {
            assert_eq!(n, 5, "no repeated primary dispatch");
            final_no_usage_events_for(&model)
        };
        Box::pin(tokio_stream::iter(events.into_iter().map(Ok)))
    }
}

#[tokio::test]
async fn empty_generation_recovery_runs_production_compaction_only_at_measured_threshold() {
    for (stop, high) in [
        (StopReason::MaxOutputTokens, false),
        (StopReason::MaxOutputTokens, true),
        (StopReason::TokenLimit, false),
        (StopReason::TokenLimit, true),
    ] {
        let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = pg.pool().await.unwrap();
        migrate(&pool).await.unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobs::in_memory());
        let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
            pool.clone(),
            blobs.clone(),
            keyring(),
        ));
        let input = b"Use the tools, then finish the task after any recovery.";
        let (fixture, first) =
            running_loop::seed_running_loop_with_input(&pool, content.as_ref(), input).await;
        let mut controlled = context_snapshot::snapshot_content_with_read_tool(
            fixture.run_id,
            musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref().unwrap(),
            "exact_recovery_tool",
        );
        controlled.model.route.context_window = std::num::NonZeroU64::new(32768).unwrap();
        controlled.model.route.max_output_tokens = std::num::NonZeroU64::new(4096).unwrap();
        let mut snapshot =
            runtime_fixture::upgrade_snapshot(content.clone(), fixture, controlled).await;
        let mut compression = snapshot.model.route.clone();
        compression.route_ref = VersionedKey::new("cloud.aux-compression@1").unwrap();
        compression.resolved_model_key = "compression-model".into();
        compression.capabilities.clear();
        compression.fallback_route_refs.clear();
        compression.provider_state_policy = musteragent_model::ProviderStatePolicy::Stateless;
        snapshot
            .auxiliary_models
            .push(musteragent_runtime_snapshot::PinnedAuxiliaryModelRoute {
                role: musteragent_model::AuxiliaryModelRole::Compression,
                model: musteragent_runtime_snapshot::PinnedModelRoute {
                    route: compression,
                    credential_grant_ref: snapshot.model.credential_grant_ref.clone(),
                },
            });
        let budgets = ContextCompactionPolicyV8Spec::resolve(&snapshot.model.route).unwrap();
        snapshot.context_policy.compaction_policy_ref =
            VersionedKey::new(CONTEXT_COMPACTION_POLICY_V8).unwrap();
        snapshot.context_policy.resolved_v2 = Some(budgets);
        snapshot.context_policy.max_input_tokens = budgets.hard_input_budget;
        snapshot.context_policy.compaction_trigger_tokens = budgets.trigger_budget;
        snapshot.context_policy.reserved_output_tokens = budgets.reserved_output_budget;
        let provider = Arc::new(LimitThenFinal {
            stop,
            input_tokens: if high {
                budgets.trigger_budget.get()
            } else {
                10
            },
            calls: AtomicUsize::new(0),
            compressions: AtomicUsize::new(0),
        });
        let model: Arc<dyn ModelProvider> =
            Arc::new(runtime_fixture::OwnedScriptedModel(provider.clone()));
        let owner =
            runtime_fixture::journal(&pool, content.clone(), running_loop::recovery_keyring());
        let supervision = runtime_fixture::supervision(&owner);
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let liveness = Arc::new(PostgresFencedExecutionLivenessJournal::new(
            pool.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
            codec.clone(),
            runtime_fixture::clock(),
        ));
        let compactor = Arc::new(
            musteragent_cloud_worker::PersistedContextCompactor::new(
                model.clone(),
                PostgresContextCompactionSlotStore::new(pool.clone(), keyring())
                    .with_execution_liveness(liveness),
                content.clone(),
                Arc::new(ProtobufMeteringPublicationEncoder),
                Arc::new(FixedClock(database_now(&pool).await)),
            )
            .with_execution_supervision(supervision.clone()),
        );
        let planner = production_context::install_production_context_with_compactor(
            &pool,
            content.clone(),
            fixture,
            codec,
            snapshot,
            input,
            compactor,
        )
        .await;
        let tools = runtime_fixture::recording_tools(&pool, blobs).await;
        let now = database_now(&pool).await;
        let runtime = || {
            DurableAgentLoop::new(
                Arc::new(FixedClock(now)),
                Arc::new(DefaultLoopPolicy::new(
                    Arc::new(FixedExecutionPolicy { now }),
                    planner.clone(),
                    tools.clone(),
                    Arc::new(EmptyControls),
                )),
                model.clone(),
                Arc::new(runtime_fixture::journal(
                    &pool,
                    content.clone(),
                    running_loop::recovery_keyring(),
                )),
            )
            .with_execution_supervision(supervision.clone())
        };
        let exit = runtime()
            .execute(
                first.clone(),
                attempt_telemetry(fixture, &first),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(
            matches!(
                exit,
                AgentLoopExit::Failed {
                    code: ErrorCode::ModelResponseInvalid,
                    retryable: true,
                    failure_commit_error: None,
                    ..
                }
            ),
            "{exit:?}, primary={}, tools={}",
            provider.calls.load(Ordering::SeqCst),
            tools.calls.lock().unwrap().len()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
        assert_eq!(provider.compressions.load(Ordering::SeqCst), 0);
        let scheduler =
            PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        assert!(matches!(
            scheduler
                .claim_one(
                    &WorkerId::new("recovery").unwrap(),
                    Duration::from_secs(120)
                )
                .await
                .unwrap(),
            ClaimOutcome::Idle { .. }
        ));
        sqlx::query("UPDATE runtime.runs SET not_before_ms=0 WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .execute(&pool)
            .await
            .unwrap();
        let ClaimOutcome::Claimed(claimed) = scheduler
            .claim_one(
                &WorkerId::new("recovery").unwrap(),
                Duration::from_secs(120),
            )
            .await
            .unwrap()
        else {
            panic!("due recovery");
        };
        let next = AttemptContext {
            attempt_id: claimed.fence.attempt_id,
            run_id: claimed.fence.run_id,
            attempt_number: claimed.fence.attempt_number,
            worker_id: claimed.fence.worker_id,
            lease_id: claimed.fence.lease_id,
            fencing_token: claimed.fence.fencing_token,
            lease_expires_at: claimed.lease_expires_at,
        };
        let exit = runtime()
            .execute(
                next.clone(),
                attempt_telemetry(fixture, &next),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(
            matches!(exit, AgentLoopExit::Completed { .. }),
            "{exit:?}, high={high}, compression={}",
            provider.compressions.load(Ordering::SeqCst)
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 6);
        assert_eq!(
            provider.compressions.load(Ordering::SeqCst),
            usize::from(high)
        );
        let facts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id=$1), (SELECT count(*) FROM event_store.outbox WHERE topic='musteragent.model-usage.v1'), (SELECT count(*) FROM runtime.context_compaction_slots WHERE run_id=$1)")
            .bind(fixture.run_id.into_uuid()).fetch_one(&pool).await.unwrap();
        assert_eq!(
            facts,
            (1 + i64::from(high), 1 + i64::from(high), i64::from(high))
        );
        pool.close().await;
    }
}
