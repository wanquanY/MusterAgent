use super::*;
use musteragent_identity::{ContentId, UtcTimestamp};
use musteragent_postgres::ContentGcPolicy;
use musteragent_runtime_content::ContentStoreError;
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;

#[tokio::test]
async fn waiting_run_keeps_its_request_through_ninety_days_of_content_gc() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let blobs = Arc::new(MemoryBlobs::in_memory());
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        blobs.clone(),
        keyring(),
    ));
    let put = |bytes: &[u8]| PutContent {
        tenant_id: fixture.tenant_id,
        media_type: MediaType::new("application/x-musteragent-model-request").expect("media"),
        bytes: bytes.to_vec(),
    };
    // Exercise the strictest path: even an explicitly reclaimable prewrite
    // must become protected by the real Model Invocation foreign key.
    let orphan = content
        .put_reclaimable_at(ContentId::generate(), put(b"unreferenced prewrite"))
        .await
        .expect("orphan");
    let active = reopen_journal(&pool, content.clone());
    let mut iteration = model_iteration(fixture, attempt.clone(), orphan.clone(), 1);
    let request_bytes = musteragent_context::ContextCodec::encode_model_request(
        &musteragent_wire::CanonicalJsonContextCodec,
        &iteration.request,
    )
    .expect("canonical request");
    let request = content
        .put_reclaimable_at(ContentId::generate(), put(&request_bytes))
        .await
        .expect("reclaimable canonical request");
    iteration.request_ref = request.clone();
    recovery_budget_cases::exercise_failed_start_with_policy(
        &pool,
        &active,
        &iteration,
        0,
        recovery_budget_cases::FailedStart::EmptyResponse,
        RecoveryPolicySnapshot::production_default(),
        false,
    )
    .await;
    let now = database_now(&pool).await.unix_millis();
    let reopened = Arc::new(PostgresContentStore::new(pool.clone(), blobs, keyring()));
    let report = reopened
        .sweep_unreferenced_content(
            UtcTimestamp::from_unix_millis(now + 90 * 24 * 60 * 60 * 1_000),
            ContentGcPolicy::default(),
        )
        .await
        .expect("ninety-day GC");
    assert_eq!(report.scanned, 2);
    assert_eq!(report.still_referenced, 1);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(
        reopened
            .read(fixture.tenant_id, &request)
            .await
            .expect("retained request"),
        request_bytes
    );
    assert_eq!(
        reopened.read(fixture.tenant_id, &orphan).await,
        Err(ContentStoreError::NotFound)
    );
    let resumed = claim_budget_retry(&pool, fixture).await;
    let active = reopen_journal(&pool, reopened.clone());
    assert_eq!(
        active
            .recovery_budget(&resumed)
            .await
            .expect("durable episode")
            .provider_failures,
        1
    );
    let mut next = model_iteration(fixture, resumed.clone(), request, 2);
    musteragent_conformance::fixtures::model_journal::freeze_model_request(
        reopened.as_ref(),
        &mut next,
    )
    .await;
    active
        .begin_model(&resumed, &next, ProviderRecoveryCapability::NonResumable)
        .await
        .expect("original request remains usable");
    active
        .acknowledge_cancellation(&resumed, None)
        .await
        .expect("cancel");
}
