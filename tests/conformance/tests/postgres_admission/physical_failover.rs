//! Exercise two physical provider protocols through the Cloud planner, journal and usage receipts.
use super::*;
use musteragent_activity::ActivityEvent;
use musteragent_cloud_worker::RuntimeModelContentResolver;
use musteragent_context::{
    ContextCompactionRequest, ContextCompactionResult, ContextCompactor, ContextPlanError,
    DurableContextPlanner,
};
use musteragent_conversation::ConversationEvent;
use musteragent_identity::InvocationId;
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_loop::{
    AgentLoopClock, AgentLoopExit, DefaultLoopPolicy, DurableAgentLoop, InvocationSupervisor,
    LoopPortError, PlannedToolCall, ToolExecutionOutcome, ToolExecutor,
};
use musteragent_model::{ApiFormat, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef};
use musteragent_model_providers::{
    CredentialMaterial, CredentialResolver, CredentialResolverError, DirectModelProvider,
    ProviderAdapterConfig, ProviderEndpoint, ProviderEndpointOrigin, StaticEndpointRegistry,
};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContextMaterialSource, PostgresContextPlanStore,
    PostgresDispatchStore, PostgresLoopControlQueue,
};
use musteragent_run::{AttemptContext, RunEvent, WorkerId};
use musteragent_runtime_content::PutContent;
use musteragent_runtime_model::ModelInvocationRouteRole;
use musteragent_scheduler::{ClaimOutcome, DispatchStore};
use musteragent_storage_protection::ProtectedContentKeyring;
use musteragent_wire::CanonicalJsonContextCodec;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};

use musteragent_testkit::{MockProviderWire, ProviderWireScenario};
use std::time::Instant;

struct FixtureAccess;
impl ContextCompactor for FixtureAccess {
    fn compact(
        &self,
        _: ContextCompactionRequest,
    ) -> ApplicationFuture<'_, Result<ContextCompactionResult, ContextPlanError>> {
        panic!("a bounded single-message model turn requested compaction")
    }
}
struct SystemClock;
impl AgentLoopClock for SystemClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        )
    }
}
impl CredentialResolver for FixtureAccess {
    fn resolve<'a>(
        &'a self,
        _: &'a CredentialGrantRef,
        _: &'a ModelRoute,
        _: ModelInvocationRouteRole,
        _: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async { Ok(CredentialMaterial::Bearer("loopback-fixture".into())) })
    }
}
impl ToolExecutor for FixtureAccess {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("a pure model turn dispatched a tool")
    }
}

fn endpoint(route: &ModelRoute, server: &MockProviderWire) -> ProviderEndpoint {
    ProviderEndpoint {
        model_provider_id: route.model_provider_id,
        model_entry_id: route.model_entry_id,
        api_format: route.api_format,
        invocation_url: server.endpoint().parse().unwrap(),
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(3),
        total_timeout: Duration::from_secs(5),
        anthropic_version: None,
        aws_region: None,
        origin: ProviderEndpointOrigin::Direct,
    }
}
fn completed() -> ProviderWireScenario {
    let frames = [
        (
            "response.created",
            r#"{"type":"response.created","response":{"id":"local-response","model":"local-alternate"}}"#,
        ),
        (
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","output_index":0,"delta":"Recovered in the same run."}"#,
        ),
        (
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"local-response","model":"local-alternate","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}"#,
        ),
    ];
    ProviderWireScenario::successful(
        "text/event-stream",
        frames
            .map(|(event, data)| format!("event: {event}\ndata: {data}\n\n").into_bytes())
            .to_vec(),
    )
}

#[tokio::test]
async fn cloud_original_run_switches_between_physical_providers_and_meters_the_actual_invocations()
{
    let (_guard, _database, pool) = runtime_pool().await;
    let pg_content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        reference_keys(),
    ));
    for status in [Some(402), Some(429), Some(408), Some(500), Some(503), None] {
        exercise_switch(&pool, pg_content.clone(), status).await;
    }
}

async fn exercise_switch(
    pool: &sqlx::PgPool,
    pg_content: Arc<PostgresContentStore>,
    failure: Option<u16>,
) {
    let fault = failure.map_or_else(|| "network-before-headers".to_owned(), |s| s.to_string());
    let denied = if let Some(status) = failure {
        MockProviderWire::start_with_retry_after(
            ProviderWireScenario {
                status,
                content_type: "application/json".into(),
                chunks: vec![if status == 402 {
                    br#"{"error":{"code":"insufficient_quota"}}"#.to_vec()
                } else {
                    br#"{"error":{"message":"temporarily unavailable"}}"#.to_vec()
                }],
                hang_after_chunks: false,
            },
            (status == 429).then_some(600),
        )
        .await
        .unwrap()
    } else {
        MockProviderWire::start_closing_before_headers()
            .await
            .unwrap()
    };
    let success = MockProviderWire::start(completed()).await.unwrap();
    let keys = reference_keys();
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = pg_content.clone();
    let journal = Arc::new(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content.clone(),
            codec.clone(),
            codec.clone(),
            Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
            keys,
            Arc::new(
                ProtectedContentKeyring::new(
                    DottedKey::new("failover.recovery").unwrap(),
                    [(DottedKey::new("failover.recovery").unwrap(), [31_u8; 32])],
                )
                .unwrap(),
            ),
        )
        .with_execution_liveness(
            Arc::new(ProtobufSnapshotCodec),
            Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default()),
        ),
    );
    let supervision = journal
        .execution_supervision(Arc::new(
            musteragent_execution_liveness_io::HomeExecutionActivityFactory,
        ))
        .expect("model liveness owner");

    let policy_owner = Arc::new(musteragent_postgres::PostgresExecutionPolicySource::new(
        pool.clone(),
        Arc::new(musteragent_wire::ProtobufSnapshotCodec),
        codec.clone(),
    ));
    let mut primary = model_route();
    primary.api_format = ApiFormat::OpenAiChatCompletions;
    primary.adapter_version = VersionedKey::new("openai.chat-adapter@1").unwrap();
    primary.protocol_version = VersionedKey::new("openai.chat@1").unwrap();
    primary.context_window = NonZeroU64::new(131072).unwrap();
    let mut backup = model_route();
    backup.route_ref = VersionedKey::new("test.other-provider@1").unwrap();
    backup.model_entry_id = ModelEntryId::from_uuid(uuid::Uuid::now_v7()).unwrap();
    backup.model_provider_id = ModelProviderId::from_uuid(uuid::Uuid::now_v7()).unwrap();
    backup.resolved_model_key = "local-alternate".into();
    backup.context_window = primary.context_window;
    backup.pricing_version_ref = PricingVersionRef::new("catalog:local-alternate@1").unwrap();
    primary.fallback_route_refs = vec![backup.route_ref.clone()];
    let provider = Arc::new(
        DirectModelProvider::new(
            Arc::new(
                StaticEndpointRegistry::new(
                    [endpoint(&primary, &denied), endpoint(&backup, &success)],
                    true,
                )
                .unwrap(),
            ),
            Arc::new(FixtureAccess),
            Arc::new(RuntimeModelContentResolver::new(content.clone())),
            ProviderAdapterConfig::default(),
        )
        .unwrap()
        .with_dependency_recovery(Arc::new(
            musteragent_postgres::PostgresModelDependencyRecoveryStore::new(pool.clone()),
        )),
    );
    let sample: usize = std::env::var("MUSTERAGENT_TEST_FAILOVER_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .clamp(1, 100);
    let mut durations = Vec::new();
    let planner = Arc::new(DurableContextPlanner::new(
        Arc::new(PostgresContextMaterialSource::new(
            pool.clone(),
            content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            codec.clone(),
        )),
        content.clone(),
        Arc::new(PostgresContextPlanStore::new(pool.clone(), codec.clone())),
        Arc::new(CanonicalJsonContextCodec),
        Arc::new(FixtureAccess),
    ));
    let policy = Arc::new(DefaultLoopPolicy::new(
        policy_owner,
        planner,
        Arc::new(FixtureAccess),
        Arc::new(PostgresLoopControlQueue::new(pool.clone(), codec.clone())),
    ));
    let runner = DurableAgentLoop::new(Arc::new(SystemClock), policy, provider, journal)
        .with_execution_supervision(supervision);
    let dispatch =
        PostgresDispatchStore::new(pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
    for ordinal in 0..sample {
        let fixture = seed_fixture(pool, None).await;
        let mut execution_policy = RunExecutionPolicySnapshot::production_defaults();
        execution_policy.provider_failover_v1 = true;
        let mut batch = admission_batch_with_model(
            pool,
            &fixture,
            &format!("physical:{fault}:{ordinal}"),
            content.clone(),
            execution_policy,
            |grant| {
                grant.model_route = primary.clone();
                grant.fallback_model_routes = vec![backup.clone()];
            },
        )
        .await;
        if ordinal == 0 {
            let snapshot = ProtobufSnapshotCodec
                .decode_content(&batch.runtime_snapshot.encoded_snapshot)
                .unwrap();
            assert!(snapshot.execution_policy.liveness.is_some());
            assert!(snapshot.execution_policy.provider_failover_v1);
            for skill in &snapshot.skills {
                let reference = &skill.instructions_ref;
                let bytes = musteragent_skill::builtin_skill_instructions(reference)
                    .expect("exact immutable release skill")
                    .as_bytes()
                    .to_vec();
                pg_content
                    .retain_runtime_release_at(reference, bytes)
                    .await
                    .expect("retain exact Runtime Release material");
            }
        }
        let input = b"Answer the question using the authorized text. Local test only.";
        let run_id = batch.run.context.run_id;
        batch.message.content = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
                bytes: input.to_vec(),
            })
            .await
            .unwrap();
        let events = [
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                project_task_target: None,
                message_id: batch.message.message_id,
                role: MessageRole::User,
                content_ref: batch.message.content.clone(),
                attachment_refs: vec![],
                requested_skill_refs: vec![],
            }),
            DomainEvent::Activity(ActivityEvent::Started {
                kind: ActivityKind::Chat,
                root_activity_id: batch.activity.activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: batch.activity.policy_snapshot_ref.clone(),
            }),
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: batch.run.context.runtime_snapshot_id,
                execution_home: RuntimeHome::Cloud,
                parent_run_id: None,
                delegation_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        ];
        for (record, event) in batch.append.records.iter_mut().zip(events) {
            record.payload = codec.encode(LedgerPayload::DomainEvent(event));
            record.payload_hash = Sha256::digest(&record.payload);
        }
        admission_store(pool)
            .admit(batch)
            .await
            .expect("production admission accepts both frozen routes");
        let ClaimOutcome::Claimed(receipt) = dispatch
            .claim_one(
                &WorkerId::new("failover-worker").unwrap(),
                Duration::from_secs(120),
            )
            .await
            .expect("claim")
        else {
            panic!("admitted Run did not dispatch")
        };
        assert_eq!(receipt.fence.run_id, run_id);
        let f = receipt.fence;
        let attempt = AttemptContext {
            attempt_id: f.attempt_id,
            run_id: f.run_id,
            attempt_number: f.attempt_number,
            worker_id: f.worker_id,
            lease_id: f.lease_id,
            fencing_token: f.fencing_token,
            lease_expires_at: receipt.lease_expires_at,
        };
        let start = Instant::now();
        let result = runner
            .execute(attempt, *receipt.telemetry, InvocationSupervisor::new())
            .await;
        assert!(
            matches!(result, AgentLoopExit::Completed { .. }),
            "physical failover: {result:?}"
        );
        durations.push(start.elapsed());
        let rows: Vec<(String,i16,Option<String>,bool)> = sqlx::query_as(
            "SELECT model_route_ref,status,pricing_version_ref,usage_fact IS NOT NULL FROM runtime.model_invocations WHERE run_id=$1 ORDER BY started_at_ms,invocation_id"
        ).bind(run_id.into_uuid()).fetch_all(pool).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, primary.route_ref.as_str());
        assert_eq!(rows[1].0, backup.route_ref.as_str());
        assert_eq!(
            rows[1].2.as_deref(),
            Some(backup.pricing_version_ref.as_str())
        );
        assert_eq!((rows[0].1, rows[0].3), (3, false));
        assert_eq!((rows[1].1, rows[1].3), (2, true));
        let facts: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
            "SELECT model_route_ref,pricing_version_ref,response_model_key,input_tokens_uncached,output_tokens
             FROM runtime.model_usage_facts WHERE run_id=$1"
        ).bind(run_id.into_uuid()).fetch_all(pool).await.unwrap();
        assert_eq!(
            facts,
            vec![(
                backup.route_ref.as_str().to_owned(),
                backup.pricing_version_ref.as_str().to_owned(),
                "local-alternate".to_owned(),
                7,
                3
            )]
        );
    }
    let primary = denied.received_at().await;
    let alternate = success.received_at().await;
    assert_eq!((primary.len(), alternate.len()), (sample, sample));
    let mut switch: Vec<_> = primary
        .iter()
        .zip(alternate)
        .map(|(a, b)| b.duration_since(*a))
        .collect();
    switch.sort();
    durations.sort();
    let percentile = (sample * 95).div_ceil(100) - 1;
    for body in success.captured_requests().await {
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["model"], "local-alternate");
        assert!(json.get("previous_response_id").is_none());
        assert!(json["input"].to_string().contains("authorized text"));
    }
    println!(
        "physical failover fault={fault} n={sample} first HTTP to alternate HTTP p95={:?}, worker loop to terminal p95={:?}",
        switch[percentile], durations[percentile]
    );
    if sample >= 100 {
        assert!(switch[percentile] <= Duration::from_secs(1));
    }
    denied.shutdown().await.unwrap();
    success.shutdown().await.unwrap();
}
