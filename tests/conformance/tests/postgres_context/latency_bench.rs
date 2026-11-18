use super::*;
#[path = "../support/context_latency_bench.rs"]
mod bench;

#[tokio::test]
async fn warmed_cloud_planner_144_complete_messages() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("warmed cloud planner 144 complete messages fixture");
    let pool = postgres
        .pool()
        .await
        .expect("warmed cloud planner 144 complete messages fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("warmed cloud planner 144 complete messages fixture");
    let key_id = DottedKey::new("test.latency.bench")
        .expect("warmed cloud planner 144 complete messages fixture");
    let keys = Arc::new(
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [43; 32])])
            .expect("warmed cloud planner 144 complete messages fixture"),
    );
    let store = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys,
    ));
    let _watch = store
        .connect_cache_watch()
        .await
        .expect("warmed cloud planner 144 complete messages fixture");
    let content: Arc<dyn ContentStore> = store;
    let tenant = TenantId::generate();
    let run = RunId::generate();
    let policy = put_text(&content, tenant, FIXTURE_RUNTIME_POLICY_V1).await;
    let first = put_text(&content, tenant, &bench::history_text(0)).await;
    let fixture = seed_running_context(&pool, tenant, run, &policy, &first, None, false).await;
    let mut events = Vec::new();
    for index in 1..bench::MESSAGES {
        let body = put_text(&content, tenant, &bench::history_text(index)).await;
        events.push(DomainEvent::Conversation(
            ConversationEvent::MessageAccepted {
                project_task_target: None,
                message_id: MessageId::generate(),
                role: MessageRole::User,
                content_ref: body,
                attachment_refs: vec![],
                requested_skill_refs: vec![],
            },
        ));
    }
    append_domain_events(
        &pool,
        DomainEventAppend {
            tenant_id: tenant,
            conversation_id: fixture.conversation_id,
            run_id: run,
            expected_offset: current_ledger_offset(&pool, tenant, fixture.conversation_id).await,
            run_seq_start: 2,
            events,
            participant_id: Some(fixture.requester_id),
            producer: Producer::RuntimeApi,
            idempotency_key: "latency-bench-history",
        },
    )
    .await;
    sqlx::query(
        "UPDATE runtime.runs SET admitted_sequence=$2,last_run_sequence=$2 WHERE run_id=$1",
    )
    .bind(run.into_uuid())
    .bind(bench::MESSAGES as i64)
    .execute(&pool)
    .await
    .expect("warmed cloud planner 144 complete messages fixture");
    let snapshot = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        bench::sized_snapshot(context_snapshot::snapshot_content(run, policy)),
        database_now(&pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("warmed cloud planner 144 complete messages fixture");
    sqlx::query("UPDATE capability.runtime_snapshots SET encoded_snapshot=$2,content_hash=$3 WHERE runtime_snapshot_id=$1")
        .bind(fixture.runtime_snapshot_id.into_uuid()).bind(snapshot.encoded_content).bind(snapshot.content_hash.into_bytes().to_vec())
        .execute(&pool).await.expect("warmed cloud planner 144 complete messages fixture");
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let source = Arc::new(PostgresContextMaterialSource::new(
        pool.clone(),
        content.clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec.clone(),
    ));
    let planner = DurableContextPlanner::new(
        source,
        content.clone(),
        Arc::new(PostgresContextPlanStore::new(pool, codec)),
        Arc::new(CanonicalJsonContextCodec),
        Arc::new(bench::NeverCompact),
    );
    bench::measure(
        "Cloud/PostgreSQL",
        &planner,
        ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: fixture.attempt,
            next_iteration: 1,
            invocation_timing:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                    .expect("current timing"),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        },
        content.as_ref(),
    )
    .await;
}
