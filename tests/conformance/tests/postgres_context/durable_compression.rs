use super::*;
use musteragent_runtime_model::{ProviderError, ProviderErrorCode, ProviderErrorKind};
use musteragent_runtime_snapshot::RecoveryPolicySnapshot;
use std::sync::atomic::AtomicBool;

struct RenewableCompressionModel {
    ready: AtomicBool,
    inner: StructuredSummaryModel,
}
impl ModelProvider for RenewableCompressionModel {
    fn preflight<'a>(
        &'a self,
        _request: &'a CanonicalModelRequest,
    ) -> ApplicationFuture<'a, Result<(), ProviderError>> {
        Box::pin(async move {
            if self.ready.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(ProviderError {
                    kind: ProviderErrorKind::Transport,
                    safe_code: ProviderErrorCode::CredentialExpired,
                    retryable: true,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                })
            }
        })
    }
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        self.inner.stream(request)
    }
}

pub(super) async fn assert_persistent_recovery(
    pool: &PgPool,
    keys: &Arc<OpaqueRefKeyring>,
    content: &Arc<dyn ContentStore>,
    mut request: ContextCompactionRequest,
) {
    request.slot.loop_iteration = 101;
    request.slot.generation = NonZeroU32::MIN;
    request.invocation.invocation_id = InvocationId::generate();
    request.invocation.correlation_id = CorrelationId::generate();
    request.invocation.timing = context_compaction::supervised_model_timing();
    request.recovery_policy = RecoveryPolicySnapshot::production_default();
    request.slot.plan_fingerprint = request
        .expected_plan_fingerprint()
        .expect("persistent fingerprint");
    let model = Arc::new(RenewableCompressionModel {
        ready: AtomicBool::new(false),
        inner: StructuredSummaryModel {
            calls: AtomicUsize::new(0),
            rolling_calls: AtomicUsize::new(0),
            response_model_key: None,
        },
    });
    let now = database_now(pool).await;
    let make_compactor = || {
        context_compaction::supervised_compactor(pool, keys, content.clone(), model.clone(), now)
    };
    let compactor = make_compactor();
    let original = compactor
        .prepare_invocation(request.clone())
        .await
        .expect("first generation");
    let error = compactor
        .compact(original.clone())
        .await
        .expect_err("expired preflight");
    assert_eq!(error.code, ErrorCode::CredentialRefreshRequired);
    assert!(error.retryable);
    assert_eq!(model.inner.calls.load(Ordering::SeqCst), 0);
    drop(compactor);
    let compactor = make_compactor();
    let current = compactor
        .prepare_invocation(original.clone())
        .await
        .expect("same owner recovers sealed failure");
    assert_eq!(
        current.invocation.invocation_id,
        original.invocation.invocation_id
    );
    assert!(
        compactor
            .compact(current)
            .await
            .expect_err("failure retains retryability")
            .retryable
    );
    let replacement = context_plan_takeover::replace_attempt_before_main_begin(
        pool,
        request.invocation.run_context.tenant_id,
        &request.invocation.attempt_context,
    )
    .await;
    request.invocation.attempt_context = replacement;
    request.slot.ledger_cutoff =
        ConversationSeq::new(request.slot.ledger_cutoff.get() + 5).expect("bookkeeping cutoff");
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().expect("current plan");
    let renewed = compactor
        .prepare_invocation(request.clone())
        .await
        .expect("new physical generation");
    assert_eq!(renewed.slot.generation.get(), 2);
    assert_ne!(
        renewed.invocation.invocation_id,
        original.invocation.invocation_id
    );
    assert!(
        compactor.prepare_invocation(original).await.is_err(),
        "stale owner cannot select or dispatch"
    );
    model.ready.store(true, Ordering::SeqCst);
    let summary = compactor
        .compact(renewed.clone())
        .await
        .expect("renewed model succeeds");
    assert_eq!(model.inner.calls.load(Ordering::SeqCst), 1);
    drop(compactor);
    // A further process takeover must reuse the successful summary exactly.
    request.invocation.attempt_context = context_plan_takeover::replace_attempt_before_main_begin(
        pool,
        request.invocation.run_context.tenant_id,
        &request.invocation.attempt_context,
    )
    .await;
    request.slot.ledger_cutoff =
        ConversationSeq::new(request.slot.ledger_cutoff.get() + 5).expect("later cutoff");
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().expect("later plan");
    let compactor = make_compactor();
    let cached = compactor
        .prepare_invocation(request.clone())
        .await
        .expect("successful generation reuse");
    assert_eq!(
        cached.invocation.invocation_id,
        renewed.invocation.invocation_id
    );
    assert_eq!(cached.slot.plan_fingerprint, renewed.slot.plan_fingerprint);
    assert_eq!(
        compactor.compact(cached).await.expect("cached summary"),
        summary
    );
    assert_eq!(
        model.inner.calls.load(Ordering::SeqCst),
        1,
        "no duplicate paid call"
    );
    // Distinct @7 nodes share one loop iteration. Reopening must select the
    // matching successful node, while allocation uses the global high watermark.
    request.slot.loop_iteration += 1;
    request.slot.generation = NonZeroU32::MIN;
    request.invocation.invocation_id = InvocationId::generate();
    request.invocation.correlation_id = CorrelationId::generate();
    request.slot.compaction_policy_ref =
        VersionedKey::new(musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V7).unwrap();
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
    let first_node = compactor.prepare_invocation(request.clone()).await.unwrap();
    let first_result = compactor.compact(first_node.clone()).await.unwrap();
    let mut second_node = request.clone();
    second_node.serialized_history.extend_from_slice(b" ");
    second_node.slot.plan_fingerprint = second_node.expected_plan_fingerprint().unwrap();
    let second_node = compactor.prepare_invocation(second_node).await.unwrap();
    assert_eq!(second_node.slot.generation.get(), 2);
    compactor.compact(second_node).await.unwrap();
    drop(compactor);
    let compactor = make_compactor();
    let cached_node = compactor.prepare_invocation(request.clone()).await.unwrap();
    assert_eq!(cached_node.slot.generation.get(), 1);
    assert_eq!(compactor.compact(cached_node).await.unwrap(), first_result);
    assert_eq!(model.inner.calls.load(Ordering::SeqCst), 3);
    let observed = PostgresContextCompactionSlotStore::new(pool.clone(), keys.clone())
        .latest_generation(&request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.generation.get(), 1);
    assert_eq!(observed.generation_high_watermark.get(), 2);
    // Replacing an unknown physical call never overwrites its dispatch marker.
    request.slot.loop_iteration += 1;
    request.invocation.invocation_id = InvocationId::generate();
    request.slot.generation = NonZeroU32::MIN;
    request.slot.plan_fingerprint = request.expected_plan_fingerprint().expect("crash plan");
    let crash_model = Arc::new(CrashAfterDispatchModel {
        calls: AtomicUsize::new(0),
    });
    let (crash_compactor, crash_journal) = context_compaction::supervised_compactor_with_journal(
        pool,
        keys,
        content.clone(),
        crash_model.clone(),
        database_now(pool).await,
    );
    let crash_compactor = Arc::new(crash_compactor);
    let crashed = tokio::spawn({
        let compactor = crash_compactor.clone();
        let request = request.clone();
        async move { compactor.compact(request).await }
    })
    .await;
    assert!(crashed.is_err());
    assert_eq!(crash_model.calls.load(Ordering::SeqCst), 1);
    assert!(
        compactor
            .prepare_invocation(request.clone())
            .await
            .expect_err("unconfirmed physical call cannot be replaced")
            .is_indeterminate()
    );
    context_compaction::confirm_joined_provider_stop(pool, keys, &crash_journal, &request).await;
    request.invocation.attempt_context = context_plan_takeover::replace_attempt_before_main_begin(
        pool,
        request.invocation.run_context.tenant_id,
        &request.invocation.attempt_context,
    )
    .await;
    let recovered = compactor
        .prepare_invocation(request)
        .await
        .expect("replace unknown call");
    assert_eq!(recovered.slot.generation.get(), 2);
    compactor
        .compact(recovered)
        .await
        .expect("fresh model request finishes logical work");
    assert_eq!(model.inner.calls.load(Ordering::SeqCst), 4);
    assert_eq!(crash_model.calls.load(Ordering::SeqCst), 1);
}
