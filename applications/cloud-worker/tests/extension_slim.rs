//! P10s real PostgreSQL journal/content boundary; the tool is a recording fixture.
use musteragent_content_ref::MediaType;
use musteragent_extension::*;
use musteragent_identity::*;
use musteragent_loop::*;
use musteragent_postgres::*;
use musteragent_run::AttemptContext;
use musteragent_runtime_content::{ContentStore, PutContent, RecordingContentStore};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use musteragent_runtime_model::*;
use musteragent_testkit::IsolatedPostgres;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
#[path = "../../../tests/conformance/tests/support/running_loop.rs"]
mod running_loop;

#[derive(Default)]
struct RecordingPolicy(Mutex<Vec<JsonValue>>);
fn unavailable<T>() -> ApplicationFuture<'static, Result<T, LoopPortError>>
where
    T: Send + 'static,
{
    Box::pin(async {
        Err(LoopPortError {
            code: ErrorCode::CapabilityUnavailable,
            retryable: false,
        })
    })
}
impl LoopPolicy for RecordingPolicy {
    fn execute_tool(
        &self,
        _: &AttemptContext,
        _: &musteragent_telemetry::AttemptTelemetryContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move {
            self.0.lock().expect("record").push(call.arguments);
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(
                        ModelText::new("pending").expect("text"),
                    )],
                    tool_calls: vec![],
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
    fn transform_context(
        &self,
        _: musteragent_context::ContextPlanInput,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_context::ModelIteration, musteragent_context::ContextPlanError>,
    > {
        Box::pin(async { Err(musteragent_context::ContextPlanError::invariant()) })
    }
    fn load_execution_policy(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        unavailable()
    }
    fn gate(&self, _: &StepContext) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>> {
        unavailable()
    }
    fn before_tool(
        &self,
        _: &PlannedToolCall,
        _: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>> {
        unavailable()
    }
    fn after_tool(
        &self,
        _: &PlannedToolCall,
        _: ToolExecutionOutcome,
        _: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        unavailable()
    }
    fn should_stop(
        &self,
        _: TurnSummary,
        _: &StepContext,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>> {
        unavailable()
    }
    fn prepare_next_turn(
        &self,
        _: TurnSummary,
        _: &StepContext,
    ) -> ApplicationFuture<'_, Result<NextTurnDecision, LoopPortError>> {
        unavailable()
    }
    fn fallback_after_model_failure(
        &self,
        _: ModelFailure,
        _: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        unavailable()
    }
    fn drain_steering(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        unavailable()
    }
    fn drain_follow_up(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        unavailable()
    }
}
struct Extension {
    mode: u8,
    visits: Arc<AtomicUsize>,
}
impl KernelExtension for Extension {
    fn id(&self) -> &str {
        if self.mode == 2 { "deny" } else { "fixture" }
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        self.visits.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            0 => Ok(ToolDecision::Allow),
            2 => Ok(ToolDecision::Deny(HookError::new(
                "denied",
                "fixture denial",
            ))),
            _ => Ok(ToolDecision::Rewrite(JsonValue::Object(
                std::collections::BTreeMap::from([(
                    "private".into(),
                    JsonValue::String(format!("replacement-{}", self.mode)),
                )]),
            ))),
        }
    }
}
fn registry(mode: u8, deny: bool, visits: Arc<AtomicUsize>) -> ExtensionRegistry {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Extension {
                mode,
                visits: visits.clone(),
            }),
            serde_json::json!({}),
        )
        .expect("extension");
    if deny {
        registry
            .register_extension(
                Arc::new(Extension { mode: 2, visits }),
                serde_json::json!({}),
            )
            .expect("deny");
    }
    registry
}
fn host(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    registry: ExtensionRegistry,
    detection: LoopDetectionConfig,
) -> Arc<ExtensionHost> {
    ExtensionHost::new(
        registry,
        Arc::new(PostgresExtensionJournal::new(pool.clone())),
        RuntimePorts {
            state_root: PathBuf::from("/tmp/extension-slim"),
            subagents: None,
            history: Arc::new(PostgresExtensionToolHistory::new(
                pool.clone(),
                content.clone(),
            )),
            content,
        },
        detection,
    )
    .expect("host")
}
fn call(id: &str, name: &str) -> PlannedToolCall {
    PlannedToolCall {
        call_id: ToolCallId::new(id).expect("id"),
        name: musteragent_tool::WireName::new(name).expect("tool"),
        arguments: JsonValue::Object(Default::default()),
        provider_replay: None,
        planning_failure: None,
        argument_diagnostic: None,
    }
}
async fn counts(pool: &sqlx::PgPool, scope: &ExtensionContext) -> (i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM runtime.extension_facts WHERE run_id=$1), (SELECT count(*) FROM event_store.outbox WHERE conversation_id=$2 AND topic='extension.fact')")
        .bind(scope.run.run_id.into_uuid()).bind(scope.run.conversation_id.into_uuid()).fetch_one(pool).await.expect("counts")
}
async fn scope(pool: &sqlx::PgPool) -> (running_loop::Fixture, ExtensionContext) {
    let (fixture, attempt) = running_loop::seed_running_loop(pool).await;
    (
        fixture,
        ExtensionContext {
            run: running_loop::run_context(fixture),
            attempt,
            workspace_dir: PathBuf::from("/tmp/extension-slim"),
            session_key: "fixture".into(),
            channel: None,
            sandboxed: false,
            ingress: None,
        },
    )
}
#[tokio::test]
async fn postgres_untouched_rewrite_deny_restart_and_indexed_reads() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated pg");
    let pool = pg.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let blobs = Arc::new(musteragent_conformance::MemoryBlobs::default());
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        running_loop::keyring(),
    ));
    let (fixture, scope) = scope(&pool).await;
    let visits = Arc::new(AtomicUsize::new(0));
    let inner = Arc::new(RecordingPolicy::default());
    let policy = ExtensionLoopPolicy::new(
        inner.clone(),
        host(
            &pool,
            content.clone(),
            registry(0, false, visits.clone()),
            Default::default(),
        ),
        scope.clone(),
    )
    .expect("policy");
    let telemetry = running_loop::attempt_telemetry(fixture, &scope.attempt);
    let untouched = call("untouched", "probe");
    let result = policy
        .execute_tool(&scope.attempt, &telemetry, untouched.clone())
        .await
        .expect("tool");
    let context = StepContext {
        attempt: scope.attempt.clone(),
        checkpoint: DurableLoopCheckpoint {
            run_id: scope.run.run_id,
            attempt_id: scope.attempt.attempt_id,
            iteration: 0,
            active_model_route_ordinal: 0,
            phase: LoopPhase::ReadyForModel,
            completed_tool_calls: 0,
            consecutive_empty_responses: 0,
            total_tokens: 0,
            total_cost_microusd: 0,
            continuation_ref: None,
            continuation_source_invocation_id: None,
        },
        execution: PinnedRunExecutionPolicy {
            snapshot: musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults(
            ),
            admitted_at: running_loop::database_now(&pool).await,
            model_route_count: std::num::NonZeroU32::MIN,
        },
        now: running_loop::database_now(&pool).await,
    };
    policy
        .tool_result_committed(&untouched, &result, &context)
        .await
        .expect("post-result");
    assert_eq!(visits.load(Ordering::SeqCst), 1);
    assert_eq!(counts(&pool, &scope).await, (0, 0));
    let journal = PostgresExtensionJournal::new(pool.clone());
    for i in 0..500 {
        journal
            .append(
                &scope,
                &format!("prior:{i}"),
                ExtensionFact::Service {
                    service: "fixture".into(),
                    hook: Hook::ServiceStart,
                },
            )
            .await
            .expect("seed");
    }
    assert_eq!(
        journal
            .select(&scope, FactQuery::Key("prior:499".into()))
            .await
            .expect("key")
            .len(),
        1
    );
    policy
        .execute_tool(&scope.attempt, &telemetry, call("after-500", "probe"))
        .await
        .expect("tool");
    assert_eq!(counts(&pool, &scope).await, (500, 500));
    for deny in [false, true] {
        let id = if deny { "denied" } else { "rewritten" };
        let policy = ExtensionLoopPolicy::new(
            inner.clone(),
            host(
                &pool,
                content.clone(),
                registry(1, deny, visits.clone()),
                Default::default(),
            ),
            scope.clone(),
        )
        .expect("policy");
        assert_eq!(
            policy
                .execute_tool(&scope.attempt, &telemetry, call(id, "probe"))
                .await
                .is_err(),
            deny
        );
        let before = counts(&pool, &scope).await;
        let visits_before = visits.load(Ordering::SeqCst);
        let reopened_content =
            PostgresContentStore::new(pool.clone(), blobs.clone(), running_loop::keyring());
        let orphan = reopened_content
            .put_reclaimable_at(
                ContentId::generate(),
                PutContent {
                    tenant_id: scope.run.tenant_id,
                    media_type: MediaType::new("application/json").expect("type"),
                    bytes: format!("{{\"unpublished\":\"{id}\"}}").into_bytes(),
                },
            )
            .await
            .expect("unpublished prewrite");
        let gc = reopened_content
            .sweep_unreferenced_content(
                UtcTimestamp::from_unix_millis(4_000_000_000_000),
                ContentGcPolicy::default(),
            )
            .await
            .expect("content-owner GC");
        assert_eq!(
            gc.barrier_tenants, 0,
            "GC must not pass behind a coverage barrier"
        );
        assert!(
            gc.still_referenced >= 1,
            "canonical rewrite reference must retain the replacement"
        );
        assert_eq!(
            gc.reclaimed, 1,
            "only the unpublished prewrite is collectible"
        );
        assert!(
            reopened_content
                .read(scope.run.tenant_id, &orphan)
                .await
                .is_err()
        );
        // Fresh journal, host, policy AND content adapter, same content owner.
        let fresh_content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
            pool.clone(),
            blobs.clone(),
            running_loop::keyring(),
        ));
        let fresh = ExtensionLoopPolicy::new(
            inner.clone(),
            host(
                &pool,
                fresh_content,
                registry(3, deny, visits.clone()),
                Default::default(),
            ),
            scope.clone(),
        )
        .expect("fresh");
        assert_eq!(
            fresh
                .execute_tool(&scope.attempt, &telemetry, call(id, "probe"))
                .await
                .is_err(),
            deny
        );
        assert_eq!(visits.load(Ordering::SeqCst), visits_before);
        assert_eq!(counts(&pool, &scope).await, before);
    }
    {
        let executions = inner.0.lock().expect("record");
        assert_eq!(
            canonical_arguments(&executions[2]),
            serde_json::json!({"private":"replacement-1"})
        );
        assert_eq!(executions[2], executions[3]);
    }
    let facts = journal
        .select(&scope, FactQuery::Call("rewritten".into()))
        .await
        .expect("call");
    let encoded = serde_json::to_string(&facts).expect("serialize");
    assert!(!encoded.contains("replacement-1"));
    assert_eq!(facts.len(), 1);
    sqlx::query("ANALYZE runtime.extension_facts")
        .execute(&pool)
        .await
        .expect("analyze");
    for (column, value) in [("fact_key", "prior:499"), ("call_id", "rewritten")] {
        // Match the production selector, including scope verification and sort.
        let sql = format!(
            "EXPLAIN (ANALYZE, FORMAT JSON) SELECT f.payload,f.payload_hash FROM (SELECT * FROM runtime.extension_facts WHERE tenant_id=$1 AND run_id=$3 AND attempt_id=$4 AND {column}=($6::text[])[1]) f JOIN runtime.runs r ON r.run_id=f.run_id AND r.tenant_id=f.tenant_id AND r.conversation_id=f.conversation_id WHERE f.conversation_id=$2 AND r.home_epoch=$5 ORDER BY f.record_id"
        );
        let plan: serde_json::Value = sqlx::query_scalar(&sql)
            .bind(scope.run.tenant_id.into_uuid())
            .bind(scope.run.conversation_id.into_uuid())
            .bind(scope.run.run_id.into_uuid())
            .bind(scope.attempt.attempt_id.into_uuid())
            .bind(scope.run.home_epoch.get() as i64)
            .bind(vec![value])
            .fetch_one(&pool)
            .await
            .expect("plan");
        let encoded = plan.to_string();
        let mut nodes = vec![&plan[0]["Plan"]];
        let mut inspected = false;
        while let Some(node) = nodes.pop() {
            if node["Relation Name"] == "extension_facts" {
                inspected = true;
                assert_ne!(node["Node Type"], "Seq Scan", "{encoded}");
                assert_eq!(node["Actual Rows"], 1, "{encoded}");
                assert_eq!(
                    node["Rows Removed by Filter"].as_u64().unwrap_or(0),
                    0,
                    "{encoded}"
                );
            }
            if let Some(children) = node["Plans"].as_array() {
                nodes.extend(children);
            }
        }
        assert!(inspected && encoded.contains("Index"), "{encoded}");
        assert_eq!(plan[0]["Plan"]["Actual Rows"], 1);
    }
}

async fn seed_history(
    pool: &sqlx::PgPool,
    content: &dyn ContentStore,
    scope: &ExtensionContext,
    pattern: LoopPattern,
) {
    let request = content
        .put(PutContent {
            tenant_id: scope.run.tenant_id,
            media_type: MediaType::new("application/json").expect("type"),
            bytes: b"{}".to_vec(),
        })
        .await
        .expect("request");
    let invocation = InvocationId::generate();
    sqlx::query("INSERT INTO runtime.model_invocations(invocation_id,tenant_id,conversation_id,run_id,attempt_id,purpose,route_role,status,model_route_ref,credential_grant_ref,idempotency_key,request_content_id,request_hash,started_at_ms,started_conversation_seq) VALUES($1,$2,$3,$4,$5,1,0,2,'fixture.route@1','fixture-grant',$6,$7,$8,1000,1)")
        .bind(invocation.into_uuid()).bind(scope.run.tenant_id.into_uuid()).bind(scope.run.conversation_id.into_uuid()).bind(scope.run.run_id.into_uuid()).bind(scope.attempt.attempt_id.into_uuid()).bind(format!("fixture:{invocation}")).bind(request.content_id.into_uuid()).bind(request.sha256.into_bytes().as_slice()).execute(pool).await.expect("model record");
    for i in 0..4 {
        let name = if pattern == LoopPattern::KnownPollNoProgress {
            "poll"
        } else if pattern == LoopPattern::PingPong && i % 2 == 1 {
            "other"
        } else {
            "probe"
        };
        // Results deliberately contain different call IDs; history compares only
        // the original canonical content, as the real loop does.
        let result = content.put(PutContent { tenant_id: scope.run.tenant_id, media_type: MediaType::new("application/vnd.musteragent.legacy.tool-result+json").expect("type"), bytes: serde_json::to_vec(&serde_json::json!({"tool_call_id":format!("c-{i}"),"content":[{"type":"text","text":"pending"}]})).expect("bytes") }).await.expect("result");
        sqlx::query("INSERT INTO runtime.pending_tool_calls(run_id,invocation_id,tool_call_id,wire_name,arguments_json,ordinal,completed_at_ms,result_content_id,result_content_hash,result_content_bytes) VALUES($1,$2,$3,$4,'{}'::jsonb,$5,1000,$6,$7,$8)")
            .bind(scope.run.run_id.into_uuid()).bind(invocation.into_uuid()).bind(format!("c-{i}")).bind(name).bind(i).bind(result.content_id.into_uuid()).bind(result.sha256.into_bytes().as_slice()).bind(result.byte_length as i64).execute(pool).await.expect("original tool record");
    }
}
#[tokio::test]
async fn postgres_fresh_history_owner_recovers_three_patterns_without_shadow_facts() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated pg");
    let pool = pg.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(musteragent_conformance::MemoryBlobs::default()),
        running_loop::keyring(),
    ));
    for pattern in [
        LoopPattern::GenericRepeat,
        LoopPattern::PingPong,
        LoopPattern::KnownPollNoProgress,
    ] {
        let (fixture, scope) = scope(&pool).await;
        seed_history(&pool, content.as_ref(), &scope, pattern).await;
        let mut config = LoopDetectionConfig {
            generic_repeat: LoopThresholds {
                warning: None,
                critical: None,
                circuit_breaker: None,
            },
            known_poll_no_progress: LoopThresholds {
                warning: None,
                critical: None,
                circuit_breaker: None,
            },
            ping_pong: LoopThresholds {
                warning: None,
                critical: None,
                circuit_breaker: None,
            },
            ..Default::default()
        };
        let thresholds = LoopThresholds {
            warning: None,
            critical: Some(4),
            circuit_breaker: None,
        };
        match pattern {
            LoopPattern::GenericRepeat => config.generic_repeat = thresholds,
            LoopPattern::PingPong => config.ping_pong = thresholds,
            LoopPattern::KnownPollNoProgress => {
                config.known_poll_no_progress = thresholds;
                config.poll_tools.insert("poll".into());
            }
        }
        let recorded = Arc::new(RecordingContentStore::new(content.clone()));
        let history = PostgresExtensionToolHistory::new(pool.clone(), recorded.clone());
        let result_tools = config.poll_tools.clone();
        let entries = history
            .recent(&scope, Some("c-3"), 4, &result_tools)
            .await
            .expect("history");
        assert_eq!(entries.len(), 4);
        assert_eq!(
            recorded.references().expect("read evidence").len(),
            if pattern == LoopPattern::KnownPollNoProgress {
                4
            } else {
                0
            },
            "argument-only detection must perform zero result content reads"
        );
        sqlx::query("INSERT INTO runtime.pending_tool_calls(run_id,invocation_id,tool_call_id,wire_name,arguments_json,ordinal) SELECT run_id,invocation_id,'future','probe','{}'::jsonb,10 FROM runtime.pending_tool_calls WHERE run_id=$1 LIMIT 1").bind(scope.run.run_id.into_uuid()).execute(&pool).await.expect("unprepared future call");
        let admitted = history
            .recent(&scope, None, 30, &result_tools)
            .await
            .expect("exclude future");
        assert_eq!(admitted.len(), 4);
        assert!(admitted.iter().all(|e| e.call.call_id != "future"));
        assert_eq!(config.detect(&entries)[0].pattern, pattern);
        assert_eq!(counts(&pool, &scope).await, (0, 0));
        let inner = Arc::new(RecordingPolicy::default());
        let policy = ExtensionLoopPolicy::new(
            inner.clone(),
            host(&pool, content.clone(), ExtensionRegistry::default(), config),
            scope.clone(),
        )
        .expect("fresh policy");
        assert!(
            policy
                .execute_tool(
                    &scope.attempt,
                    &running_loop::attempt_telemetry(fixture, &scope.attempt),
                    call("c-3", &entries[3].call.tool)
                )
                .await
                .is_err()
        );
        assert!(inner.0.lock().expect("executions").is_empty());
        assert_eq!(counts(&pool, &scope).await, (1, 1));
        let mut other_attempt = scope.clone();
        other_attempt.attempt.attempt_id = AttemptId::generate();
        assert!(
            history
                .recent(&other_attempt, None, 4, &result_tools)
                .await
                .expect("isolation")
                .is_empty()
        );
        let bounded = history
            .recent(&scope, Some("c-2"), 2, &result_tools)
            .await
            .expect("bounded history");
        assert_eq!(
            bounded
                .iter()
                .map(|e| e.call.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["c-1", "c-2"]
        );
    }
}
