use std::sync::Arc;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_identity::{CorrelationId, DottedKey, InvocationId, LedgerRecordId, UtcTimestamp};
use musteragent_model::ModelKey;
use musteragent_postgres::{PostgresContentStore, PostgresModelInvocationStore, migrate};
use musteragent_run::InvocationContext;
use musteragent_runtime_capability::{
    PersistedVisionModelInvoker, RuntimeClock, VisionAnalysisMode, VisionDetail, VisionModelImage,
    VisionModelInvocation, VisionModelInvoker,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CanonicalModelRequest, ModelEventStream, ModelProvider, ModelStreamEvent, ModelText,
    ModelUsage, ProviderRequestRef, StopReason,
};
use musteragent_runtime_snapshot::{PinnedModelRoute, RunExecutionPolicySnapshot};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufMeteringPublicationEncoder;

#[path = "support/running_loop.rs"]
mod running_loop;
#[allow(dead_code)]
#[path = "support/tool_material.rs"]
mod tool_material;

struct FixedClock(UtcTimestamp);

impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        self.0
    }
}

struct VisionProvider(Option<musteragent_runtime_model::ProviderError>);

impl ModelProvider for VisionProvider {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        if let Some(error) = &self.0 {
            return Box::pin(tokio_stream::iter([Err(error.clone())]));
        }
        Box::pin(tokio_stream::iter([
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: ProviderRequestRef::from_opaque(
                    OpaqueExternalRef::new("vision-response").expect("provider ref"),
                ),
                response_model_key: ModelKey::new(request.route.resolved_model_key).expect("model"),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: ModelText::new("灰色立方体，含红色和青绿色色块。".repeat(32))
                    .expect("analysis"),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 500,
                output_tokens: 350,
                total_tokens: Some(850),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]))
    }
}

struct PartialVisionProvider;
impl ModelProvider for PartialVisionProvider {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        use tokio_stream::StreamExt;
        static OBSERVED: VisionProvider = VisionProvider(None);
        Box::pin(OBSERVED.stream(request).map(|event| match event {
            Ok(ModelStreamEvent::ResponseCompleted { .. }) => Ok(ModelStreamEvent::ResponseError(
                musteragent_runtime_model::ProviderError {
                    kind: musteragent_runtime_model::ProviderErrorKind::Server,
                    safe_code: musteragent_runtime_model::ProviderErrorCode::ProviderServer,
                    retryable: true,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                },
            )),
            other => other,
        }))
    }
}

#[tokio::test]
async fn successful_vision_commits_response_usage_and_audit_before_returning() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    // Advance a synthetic interval ending at database time; security audit
    // correctly rejects observations dated after their database commit.
    let now = UtcTimestamp::from_unix_millis(
        running_loop::database_now(&pool).await.unix_millis() - 180_000,
    );
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    ));
    let image_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("image/png").expect("image type"),
            bytes: b"\x89PNG\r\n\x1a\nfixture".to_vec(),
        })
        .await
        .expect("image");
    let mut snapshot = tool_material::fixture_model_route();
    snapshot
        .route
        .capabilities
        .insert(DottedKey::new("vision").expect("vision capability"));
    let invocation_id = InvocationId::generate();
    let runner = PersistedVisionModelInvoker::new(
        Arc::new(VisionProvider(None)),
        PostgresModelInvocationStore::new(pool.clone(), keys.clone()),
        content.clone(),
        Arc::new(ProtobufMeteringPublicationEncoder),
        Arc::new(FixedClock(UtcTimestamp::from_unix_millis(
            now.unix_millis() + 180_000,
        ))),
    );
    let request = VisionModelInvocation {
        capture_provider_failure: true,
        invocation_context: InvocationContext {
            invocation_id,
            run_context: running_loop::run_context(fixture),
            attempt_context: attempt,
            parent_invocation_id: Some(InvocationId::generate()),
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(now.unix_millis() + 300_000) },
        },
        model: PinnedModelRoute {
            route: snapshot.route,
            credential_grant_ref: snapshot.credential_grant_ref,
        },
        stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        images: vec![VisionModelImage {
            image_ref,
            label: Some("test_image".into()),
        }],
        question: "描述图片".into(),
        detail: VisionDetail::Auto,
        analysis_mode: VisionAnalysisMode::Independent,
        task_context: vec![],
    };
    let partial = PersistedVisionModelInvoker::new(
        Arc::new(PartialVisionProvider),
        PostgresModelInvocationStore::new(pool.clone(), keys.clone()),
        content.clone(),
        Arc::new(ProtobufMeteringPublicationEncoder),
        Arc::new(FixedClock(UtcTimestamp::from_unix_millis(
            now.unix_millis() + 180_000,
        ))),
    );
    let mut partial_request = request.clone();
    let partial_id = InvocationId::generate();
    partial_request.invocation_context.invocation_id = partial_id;
    let error = partial.analyze(partial_request).await.unwrap_err();
    assert!(matches!(
        error,
        musteragent_runtime_capability::VisionCapabilityError::ModelFailure {
            early_provider_cause: None,
            ..
        }
    ));
    let partial_fact:(i16,Option<String>,i64,i64) = sqlx::query_as(
        "SELECT model.status,model.early_provider_cause,usage.input_tokens_uncached,usage.output_tokens::bigint FROM runtime.model_invocations model JOIN runtime.model_usage_facts usage USING(invocation_id) WHERE model.invocation_id=$1"
    ).bind(partial_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        partial_fact,
        (3, None, 500, 350),
        "a provider error after visible text preserves the reported usage and cannot authorize a second physical route"
    );
    for (code, retryable, expected_code) in [
        (
            musteragent_runtime_model::ProviderErrorCode::RateLimited,
            true,
            musteragent_runtime_contract::ErrorCode::DependencyUnavailable,
        ),
        (
            musteragent_runtime_model::ProviderErrorCode::CredentialExpired,
            true,
            musteragent_runtime_contract::ErrorCode::CredentialRefreshRequired,
        ),
        (
            musteragent_runtime_model::ProviderErrorCode::AuthorizationFailed,
            false,
            musteragent_runtime_contract::ErrorCode::PolicyDenied,
        ),
    ] {
        let mut failed_request = request.clone();
        failed_request.invocation_context.invocation_id = InvocationId::generate();
        failed_request.invocation_context.parent_invocation_id = Some(InvocationId::generate());
        let failure_id = failed_request.invocation_context.invocation_id;
        let failed_runner = PersistedVisionModelInvoker::new(
            Arc::new(VisionProvider(Some(
                musteragent_runtime_model::ProviderError {
                    kind: musteragent_runtime_model::ProviderErrorKind::Authorization,
                    safe_code: code,
                    retryable: true,
                    retry_after_ms: Some(180_000),
                    diagnostic_ref: None,
                },
            ))),
            PostgresModelInvocationStore::new(pool.clone(), keys.clone()),
            content.clone(),
            Arc::new(ProtobufMeteringPublicationEncoder),
            Arc::new(FixedClock(now)),
        );
        assert_eq!(
            failed_runner
                .analyze(failed_request.clone())
                .await
                .expect_err("provider failure"),
            musteragent_runtime_capability::VisionCapabilityError::ModelFailure {
                early_provider_cause: code.permits_alternate_provider().then_some(code),
                code: expected_code,
                retryable,
                retry_after_ms: Some(180_000),
            }
        );
        let persisted: (i16, Option<bool>, i64) = sqlx::query_as(
            "SELECT status, terminal_error_retryable, (SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id = $1) FROM runtime.model_invocations WHERE invocation_id = $1")
            .bind(failure_id.into_uuid()).fetch_one(&pool).await.expect("read physical failure");
        assert_eq!(persisted, (3, Some(retryable), 0));
        if retryable {
            // Simulate restart between auxiliary completion and the enclosing
            // Tool/Run commit. A fresh invoker must honor the original cooldown
            // without admitting a replacement or dispatching the provider.
            let deferred = PersistedVisionModelInvoker::new(
                Arc::new(VisionProvider(None)),
                PostgresModelInvocationStore::new(pool.clone(), keys.clone()),
                content.clone(),
                Arc::new(ProtobufMeteringPublicationEncoder),
                Arc::new(FixedClock(UtcTimestamp::from_unix_millis(
                    now.unix_millis() + 60_000,
                ))),
            );
            failed_request.invocation_context.invocation_id = InvocationId::generate();
            let deferred_id = failed_request.invocation_context.invocation_id;
            assert_eq!(
                deferred
                    .analyze(failed_request.clone())
                    .await
                    .expect_err("durable cooldown"),
                musteragent_runtime_capability::VisionCapabilityError::ModelFailure {
                    early_provider_cause: code.permits_alternate_provider().then_some(code),
                    code: expected_code,
                    retryable: true,
                    retry_after_ms: Some(120_000),
                }
            );
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runtime.model_invocations WHERE invocation_id = $1",
            )
            .bind(deferred_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("no early admission");
            assert_eq!(count, 0);
            runner
                .analyze(failed_request)
                .await
                .expect("same logical Tool recovers after original cooldown");
        }
    }
    let result = runner
        .analyze(request)
        .await
        .expect("same model can complete after failure");
    assert!(result.analysis.contains("立方体"));
    let row: (i16, bool, i64, i64) = sqlx::query_as(
        "SELECT status, response_content_id IS NOT NULL,
             (SELECT count(*) FROM runtime.model_usage_facts WHERE invocation_id = $1),
             (SELECT count(*) FROM security_audit.records WHERE source_model_invocation_id = $1)
         FROM runtime.model_invocations WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("durable result");
    assert_eq!(row, (2, true, 1, 1));
}

#[path = "postgres_vision_model/activity.rs"]
mod activity;
#[tokio::test]
async fn progressing_auxiliary_vision_outlives_idle_windows_and_commits_usage_with_liveness() {
    activity::vision_uses_shared_supervision(false).await;
}
#[tokio::test]
async fn silent_auxiliary_vision_returns_timeout_and_retains_unknown_stop_without_ending_run() {
    activity::vision_uses_shared_supervision(true).await;
}
