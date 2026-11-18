//! Journal proof tests use a provider whose lookup is unavailable. Issuing a
//! ticket must not perform I/O, renew a former ticket, or require a repair plan.
use super::*;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, ExecutionBinding, InvocationExecutionTiming,
};
use musteragent_runtime_model::{
    ModelExecution, ProviderError, ProviderErrorCode, ProviderErrorKind,
};

struct UnavailableLookup;
impl ModelProvider for UnavailableLookup {
    fn supports_invocation_recovery(&self) -> bool {
        true
    }
    fn recovery_capability(&self, _: &CanonicalModelRequest) -> ProviderRecoveryCapability {
        ProviderRecoveryCapability::IdempotentRequest
    }
    fn recover_execution(
        &self,
        _: CanonicalModelRequest,
        _: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'_> + '_>, ProviderError> {
        Err(unavailable())
    }
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        Box::pin(tokio_stream::iter([Err(unavailable())]))
    }
}
fn unavailable() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Transport,
        safe_code: ProviderErrorCode::ExistingInvocationLookupFailed,
        retryable: false,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}

#[tokio::test]
async fn embedded_request_without_repair_plan_recovers_once_under_original_identity() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, original) = seed_running_loop(&pool).await;
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let snapshot = runtime_fixture::install_snapshot(
        &pool,
        content.clone(),
        fixture,
        support::model_route(),
        &[],
    )
    .await;
    let journal =
        runtime_fixture::journal(&pool, content.clone(), running_loop::recovery_keyring());
    let placeholder = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/octet-stream").expect("media"),
            bytes: b"fixture construction".to_vec(),
        })
        .await
        .expect("placeholder");
    let mut iteration = model_iteration(fixture, original.clone(), placeholder, 1);
    iteration.request.route = snapshot.model.route;
    iteration.request.credential_grant_ref = snapshot.model.credential_grant_ref;
    iteration.request.invocation.timing =
        InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: snapshot
                .execution_policy
                .liveness
                .expect("liveness")
                .model_policy()
                .reference()
                .clone(),
            business_deadline: None,
        });
    runtime_fixture::freeze(content.as_ref(), &mut iteration).await;
    journal
        .begin_model(
            &original,
            &iteration,
            ProviderRecoveryCapability::IdempotentRequest,
        )
        .await
        .expect("begin original invocation");
    journal
        .mark_model_dispatched(&original, iteration.invocation_id)
        .await
        .expect("dispatch marker");
    let plans: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.context_plans WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("plans");
    assert_eq!(plans, 0, "embedded frozen request has no repair plan");

    let successor = claim_replacement_attempt(&pool, &original).await;
    let constraints = || ModelRecoveryConstraints {
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
    };
    let checkpoint = journal
        .load_checkpoint(&successor)
        .await
        .expect("checkpoint");
    let recovered = journal
        .recover_checkpoint_with_provider(&successor, checkpoint, constraints(), &UnavailableLookup)
        .await
        .expect("recover original frozen request");
    let ticket = recovered
        .pending_model_recovery
        .expect("one recovery ticket");
    assert_eq!(ticket.iteration.request, iteration.request);
    assert_eq!(ticket.iteration.request_ref, iteration.request_ref);
    assert_eq!(
        ticket.iteration.assistant_output_id,
        iteration.assistant_output_id
    );
    assert_eq!(ticket.binding.owner_attempt_id, successor.attempt_id);
    assert_eq!(ticket.binding.owner_fence, successor.fencing_token);
    assert!(ticket.iteration.delivery_repair.is_none());
    assert!(!recovered.replacement_attempt_scheduled);

    // Another crash must not renew the fixed observation challenge or create
    // another billable model invocation, even before a lookup was completed.
    let third = claim_replacement_attempt(&pool, &successor).await;
    let checkpoint = journal
        .load_checkpoint(&third)
        .await
        .expect("third checkpoint");
    let recovered = journal
        .recover_checkpoint_with_provider(&third, checkpoint, constraints(), &UnavailableLookup)
        .await
        .expect("close interrupted recovery");
    assert!(recovered.pending_model_recovery.is_none());
    assert!(!recovered.replacement_attempt_scheduled);
    assert_eq!(
        recovered.checkpoint.phase,
        musteragent_loop::LoopPhase::Failed
    );
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runtime.model_invocations WHERE run_id=$1),
                (SELECT count(*) FROM runtime.model_usage_facts WHERE run_id=$1)",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("facts");
    assert_eq!(counts, (1, 0));
}
