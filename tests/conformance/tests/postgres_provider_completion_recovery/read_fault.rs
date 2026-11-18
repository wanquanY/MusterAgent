use super::*;

pub(super) async fn assert_unavailable_intent_read_recovers_without_redispatch() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let provider = Arc::new(SingleCompletionProvider::new());
    let gated = Arc::new(DeleteGateBlobStore::staging_fault());
    let blobs: Arc<dyn BlobStore> = gated.clone();
    let (first, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::FinishUnavailable,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert_eq!(
        first
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    gated.read_unavailable.store(true, Ordering::SeqCst);
    let (unavailable, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert_eq!(
        unavailable
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::SystemShutdown,
        }
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let preserved: (i16, bool, bool, bool, i16) = sqlx::query_as(
        "SELECT invocation.status,
                invocation.provider_dispatched_at_ms IS NOT NULL,
                invocation.provider_request_ref IS NOT NULL,
                invocation.response_model_key IS NOT NULL,
                intent.lifecycle
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_completion_intents AS intent USING (invocation_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read unavailable recovery evidence");
    assert_eq!(preserved, (1, true, true, true, 3));

    gated.read_unavailable.store(false, Ordering::SeqCst);
    let (recovered, _) = build_loop_with_storage(
        &pool,
        fixture,
        provider.clone(),
        FaultMode::None,
        Arc::new(NoTools),
        blobs.clone(),
    )
    .await;
    assert!(matches!(
        recovered
            .execute(
                attempt.clone(),
                attempt_telemetry(fixture, &attempt),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let terminal: (i16, i16, bool) = sqlx::query_as(
        "SELECT invocation.status, intent.lifecycle,
                invocation.completion_intent_content_id IS NULL
         FROM runtime.model_invocations AS invocation
         JOIN runtime.model_completion_intents AS intent USING (invocation_id)
         WHERE invocation.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered terminal");
    assert_eq!(terminal, (2, 5, true));
    pool.close().await;
}
