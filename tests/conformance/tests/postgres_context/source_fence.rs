use super::*;
#[path = "../support/context_source_deletion.rs"]
mod mutation;

pub(super) async fn cloud_deletion_after_materialization_prevents_context_plan_commit() {
    source_mutation_prevents_commit(mutation::SourceMutation::DeleteContent).await;
}

pub(super) async fn cloud_branch_activation_after_materialization_prevents_context_plan_commit() {
    source_mutation_prevents_commit(mutation::SourceMutation::ActivateBranch).await;
}

pub(super) async fn cloud_retraction_after_materialization_prevents_context_plan_commit() {
    source_mutation_prevents_commit(mutation::SourceMutation::RetractMessage).await;
}

async fn source_mutation_prevents_commit(change: mutation::SourceMutation) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("source mutation prevents commit fixture");
    let pool = postgres
        .pool()
        .await
        .expect("source mutation prevents commit fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("source mutation prevents commit fixture");
    let key_id =
        DottedKey::new("test.source.fence").expect("source mutation prevents commit fixture");
    let keys = Arc::new(
        OpaqueRefKeyring::new(key_id.clone(), [(key_id, [41; 32])])
            .expect("source mutation prevents commit fixture"),
    );
    let cached_content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys,
    ));
    let _watch = cached_content.connect_cache_watch().await.expect("watch");
    let content: Arc<dyn ContentStore> = cached_content.clone();
    let tenant = TenantId::generate();
    let run = RunId::generate();
    let policy = put_text(&content, tenant, FIXTURE_RUNTIME_POLICY_V1).await;
    let user = put_text(&content, tenant, "keep the complete input").await;
    let fixture = seed_running_context(&pool, tenant, run, &policy, &user, None, false).await;
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let original = Arc::new(PostgresContextMaterialSource::new(
        pool.clone(),
        content.clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec.clone(),
    ));
    let source = Arc::new(mutation::DeleteAfterLoad {
        source: original,
        target: policy,
        mutation: change,
        database: mutation::Database::Postgres(pool.clone()),
    });
    let planner = DurableContextPlanner::new(
        source,
        content,
        Arc::new(PostgresContextPlanStore::new(pool.clone(), codec)),
        Arc::new(CanonicalJsonContextCodec),
        Arc::new(mutation::NeverCompact),
    );
    let result = planner
        .prepare_model_iteration(ContextPlanInput {
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
        })
        .await;
    assert!(
        result
            .expect_err("source deletion fences planning before dispatch")
            .retryable,
        "safe pre-dispatch rebuild"
    );
    assert_eq!(
        cached_content.cache_stats().expect("stats").entries,
        0,
        "failed source authorization must discard derived material even before notification delivery"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.context_plans WHERE run_id=$1")
            .bind(run.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("source mutation prevents commit fixture"),
        0
    );
}
