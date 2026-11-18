use super::*;

pub(super) async fn run_context_plan_attempt_takeover_fixture() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let key_id = DottedKey::new("runtime.context.attempt-takeover").expect("key id");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [73_u8; 32])]).expect("keyring"));
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys,
    ));
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let policy_ref = put_text(&content, tenant_id, FIXTURE_RUNTIME_POLICY_V1).await;
    let user_ref = put_text(
        &content,
        tenant_id,
        "preserve one logical model turn across Attempt takeover",
    )
    .await;
    let fixture = seed_running_context(
        &pool,
        tenant_id,
        run_id,
        &policy_ref,
        &user_ref,
        None,
        false,
    )
    .await;
    let ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let planner = DurableContextPlanner::new(
        Arc::new(PostgresContextMaterialSource::new(
            pool.clone(),
            content.clone(),
            Arc::new(ProtobufSnapshotCodec),
            ledger_codec.clone(),
        )),
        content.clone(),
        Arc::new(PostgresContextPlanStore::new(pool.clone(), ledger_codec)),
        Arc::new(CanonicalJsonContextCodec),
        Arc::new(FixtureContextCompactor),
    );
    let first = planner
        .prepare_model_iteration(ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: fixture.attempt.clone(),
            next_iteration: 1,
            invocation_timing:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                    .expect("current timing"),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        })
        .await
        .expect("commit Context plan before main begin");

    let replacement = replace_attempt_before_main_begin(&pool, tenant_id, &fixture.attempt).await;
    let recovered = planner
        .prepare_model_iteration(ContextPlanInput {
            mode: musteragent_context::ContextPlanMode::ToolCapable,
            active_model_route_ordinal: 0,
            attempt: replacement.clone(),
            next_iteration: 1,
            invocation_timing:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_invocation_timing(UtcTimestamp::from_unix_millis(0))
                    .expect("current timing"),
            continuation_ref: None,
            continuation_source_invocation_id: None,
            consecutive_empty_responses: 0,
        })
        .await
        .expect("resolve committed plan under replacement Attempt");

    assert_eq!(recovered.iteration, first.iteration);
    assert_eq!(recovered.invocation_id, first.invocation_id);
    assert_eq!(recovered.assistant_message_id, first.assistant_message_id);
    assert_eq!(recovered.assistant_output_id, first.assistant_output_id);
    assert_eq!(recovered.reasoning_output_id, first.reasoning_output_id);
    assert_eq!(
        recovered.request.invocation.attempt_context, replacement,
        "physical execution provenance must use the live Attempt fence",
    );
    assert_ne!(
        recovered.request_ref, first.request_ref,
        "Attempt-bound canonical envelopes remain separately content addressed",
    );
    assert_eq!(
        map_request(&recovered.request, &ResolvedContents::default())
            .expect("map recovered request")
            .body,
        map_request(&first.request, &ResolvedContents::default())
            .expect("map original request")
            .body,
        "Attempt takeover must not change the provider-visible request",
    );

    let row = sqlx::query(
        "SELECT count(*) OVER () AS plan_count, attempt_id, invocation_id,
                request_content_id, checksum
         FROM runtime.context_plans
         WHERE run_id = $1 AND iteration = 1",
    )
    .bind(run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("load stable Context plan");
    assert_eq!(row.get::<i64, _>("plan_count"), 1);
    assert_eq!(
        row.get::<uuid::Uuid, _>("attempt_id"),
        fixture.attempt.attempt_id.into_uuid(),
        "the immutable plan retains its original commit provenance",
    );
    assert_eq!(
        row.get::<uuid::Uuid, _>("invocation_id"),
        recovered.invocation_id.into_uuid(),
    );
    assert_eq!(
        row.get::<uuid::Uuid, _>("request_content_id"),
        first.request_ref.content_id.into_uuid(),
        "logical plan evidence remains immutable",
    );
    assert_eq!(row.get::<Vec<u8>, _>("checksum").len(), Sha256::LENGTH);
    let main_invocations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime.model_invocations
         WHERE run_id = $1 AND purpose = 1",
    )
    .bind(run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("count pre-begin invocations");
    assert_eq!(
        main_invocations, 0,
        "fixture must stay in the exact plan-committed/main-not-begun window",
    );
}

#[path = "attempt_takeover.rs"]
mod attempt_takeover;
pub(super) use attempt_takeover::replace_attempt_before_main_begin;
