#[path = "activity_mode/keepalive.rs"]
mod keepalive;
mod support;
use futures_util::StreamExt as _;
use musteragent_content_ref::Sha256;
use musteragent_execution_liveness::StopReason;
use musteragent_identity::VersionedKey;
use musteragent_model::ApiFormat;
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, InvocationExecutionTiming, LivenessPolicyRef,
};
use musteragent_runtime_contract::execution_liveness::{ExecutionBinding, ExecutionScope};
use musteragent_runtime_model::{CanonicalModelRequest, ModelStreamEvent};
use musteragent_runtime_model::{ModelProvider as _, ProviderErrorCode};
use musteragent_testkit::{MockProviderSse, ProviderSseFrame, ProviderSseScenario};
use std::{num::NonZeroU64, time::Duration};

#[tokio::test]
async fn cancelling_preflight_releases_preparation_and_permanently_prevents_dispatch() {
    use musteragent_model_providers::{
        DirectModelProvider, EndpointFuture, EndpointRegistry, ProviderAdapterConfig,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Endpoint {
        entered: tokio::sync::Notify,
        released: Arc<AtomicBool>,
    }
    struct Release(Arc<AtomicBool>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl EndpointRegistry for Endpoint {
        fn resolve<'a>(&'a self, _: &'a musteragent_model::ModelRoute) -> EndpointFuture<'a> {
            Box::pin(async move {
                let _release = Release(self.released.clone());
                self.entered.notify_one();
                std::future::pending().await
            })
        }
    }
    let endpoint = Arc::new(Endpoint {
        entered: tokio::sync::Notify::new(),
        released: Arc::new(AtomicBool::new(false)),
    });
    let provider = DirectModelProvider::new(
        endpoint.clone(),
        Arc::new(support::FixtureCredentials),
        Arc::new(support::NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("provider");
    let (request, binding) = activity_request();
    let mut operation = provider.execution(request, binding).expect("operation");
    let control = operation.stop_control();
    let (preparation, stopped) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(operation.preflight(), async {
            endpoint.entered.notified().await;
            control.stop(StopReason::UserCancelled).await
        })
    })
    .await
    .expect("bounded cleanup");
    assert_eq!(
        preparation.expect_err("cancelled preparation").safe_code,
        ProviderErrorCode::Cancelled
    );
    assert_eq!(stopped.expect("not dispatched").binding, binding);
    assert!(endpoint.released.load(Ordering::SeqCst));
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(
        matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::Cancelled)
    );
}

fn activity_request() -> (CanonicalModelRequest, ExecutionBinding) {
    let mut request = support::request(ApiFormat::OpenAiResponses);
    request.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("musteragent.model-liveness@1").expect("key"),
            digest: Sha256::digest(b"Home-sealed fixture policy"),
        },
        business_deadline: None,
    });
    let binding = ExecutionBinding {
        tenant_id: request.invocation.run_context.tenant_id,
        run_id: request.invocation.run_context.run_id,
        scope: ExecutionScope::Invocation(request.invocation.invocation_id),
        owner_attempt_id: request.invocation.attempt_context.attempt_id,
        owner_fence: request.invocation.attempt_context.fencing_token,
        producer_generation: NonZeroU64::MIN,
        operation_identity: Sha256::digest(b"Home-verified original request"),
    };
    (request, binding)
}

fn started() -> ProviderSseFrame {
    ProviderSseFrame::new(
        "response.created",
        r#"{"type":"response.created","response":{"id":"resp_activity"}}"#,
    )
}

#[tokio::test]
async fn legacy_model_port_rejects_activity_before_dispatch_even_after_preflight() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let url = format!(
        "http://{}/responses",
        listener.local_addr().expect("address")
    );
    let provider = support::provider(&url, ApiFormat::OpenAiResponses);
    let mut request = support::request(ApiFormat::OpenAiResponses);
    provider
        .preflight(&request)
        .await
        .expect("local legacy preparation");
    request.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("musteragent.model-liveness@1").expect("key"),
            digest: Sha256::digest(b"policy"),
        },
        business_deadline: None,
    });
    assert_eq!(
        provider
            .preflight(&request)
            .await
            .expect_err("requires supervision")
            .safe_code,
        ProviderErrorCode::RouteConfigurationInvalid
    );
    let mut stream = provider.stream(request);
    assert_eq!(
        stream
            .next()
            .await
            .expect("failure")
            .expect_err("requires observer and control")
            .safe_code,
        ProviderErrorCode::RouteConfigurationInvalid
    );
    assert!(stream.next().await.is_none());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "rejecting an unsupported mode cannot open a provider connection"
    );
}

#[tokio::test]
async fn activity_operation_preparation_and_stop_before_dispatch_never_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let endpoint = format!(
        "http://{}/responses",
        listener.local_addr().expect("address")
    );
    let provider = support::provider(&endpoint, ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut operation = provider
        .execution(request, binding)
        .expect("bound operation");
    assert_eq!(operation.binding(), binding);
    let control = operation.stop_control();
    operation.preflight().await.expect("local preparation");
    assert_eq!(
        operation
            .preflight()
            .await
            .expect_err("prepare once")
            .safe_code,
        ProviderErrorCode::ProtocolViolation
    );
    assert_eq!(
        control
            .stop(StopReason::UserCancelled)
            .await
            .expect("no dispatch")
            .binding,
        binding
    );
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(
        matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::Cancelled)
    );
    assert_eq!(
        control
            .stop(StopReason::UserCancelled)
            .await
            .expect("idempotent stop")
            .binding,
        binding
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn activity_operation_requires_preflight_and_rejects_cross_scope_bindings() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let endpoint = format!(
        "http://{}/responses",
        listener.local_addr().expect("address")
    );
    let provider = support::provider(&endpoint, ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut other = binding;
    other.scope = ExecutionScope::Invocation(musteragent_identity::InvocationId::generate());
    assert!(provider.execution(request.clone(), other).is_err());
    other = binding;
    other.owner_attempt_id = musteragent_identity::AttemptId::generate();
    assert!(provider.execution(request.clone(), other).is_err());
    other = binding;
    other.tenant_id = musteragent_identity::TenantId::generate();
    assert!(provider.execution(request.clone(), other).is_err());
    other = binding;
    other.owner_fence = musteragent_identity::FencingToken::new(2).expect("fence");
    assert!(provider.execution(request.clone(), other).is_err());
    let operation = provider
        .execution(request, binding)
        .expect("bound operation");
    let control = operation.stop_control();
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(
        matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::ProtocolViolation)
    );
    assert_eq!(
        control
            .stop(StopReason::SystemShutdown)
            .await
            .expect("no effect")
            .binding,
        binding
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn activity_operation_terminal_response_supplies_exact_stop_proof() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        started(),
        ProviderSseFrame::new("response.completed", r#"{"type":"response.completed","response":{"id":"resp_activity","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#),
    ])).await.expect("provider");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut operation = provider
        .execution(request, binding)
        .expect("bound operation");
    let control = operation.stop_control();
    operation.preflight().await.expect("prepare");
    assert!(server.captured_requests().await.is_empty());
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert_eq!(
        control
            .stop(StopReason::SystemShutdown)
            .await
            .expect("provider terminal")
            .binding,
        binding
    );
    assert_eq!(server.captured_requests().await.len(), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn activity_operation_local_cancellation_closes_transport_without_remote_stop_claim() {
    let server = MockProviderSse::start_hanging(ProviderSseScenario::successful(vec![started()]))
        .await
        .expect("provider");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut operation = provider
        .execution(request, binding)
        .expect("bound operation");
    let control = operation.stop_control();
    operation.preflight().await.expect("prepare");
    let mut stream = operation.into_stream();
    assert!(matches!(
        stream.next().await,
        Some(Ok(ModelStreamEvent::ResponseStarted { .. }))
    ));
    let (proof, remaining) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            control.stop(StopReason::IdleExpired),
            stream.collect::<Vec<_>>()
        )
    })
    .await
    .expect("bounded local cleanup");
    assert!(
        proof.is_err(),
        "closing HTTP does not prove remote model termination"
    );
    assert!(
        matches!(remaining.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::Cancelled)
    );
    assert!(control.stop(StopReason::IdleExpired).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.closed_stream_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual transport released");
    assert_eq!(server.captured_requests().await.len(), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn activity_operation_early_eof_and_future_drop_retain_unknown_remote_outcome() {
    for drop_stream in [false, true] {
        let server = MockProviderSse::start(ProviderSseScenario::successful(vec![started()]))
            .await
            .expect("provider");
        let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
        let (request, binding) = activity_request();
        let mut operation = provider.execution(request, binding).expect("operation");
        let control = operation.stop_control();
        operation.preflight().await.expect("prepare");
        let mut stream = operation.into_stream();
        assert!(matches!(
            stream.next().await,
            Some(Ok(ModelStreamEvent::ResponseStarted { .. }))
        ));
        if drop_stream {
            drop(stream);
        } else {
            let events = stream.collect::<Vec<_>>().await;
            assert!(
                matches!(events.last(), Some(Err(error)) if error.safe_code == ProviderErrorCode::TransportFailed)
            );
        }
        assert!(control.stop(StopReason::SystemShutdown).await.is_err());
        assert_eq!(server.captured_requests().await.len(), 1);
        server.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn extension_annotations_reach_native_http_without_changing_credentials() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let url = format!(
        "http://{}/responses",
        listener.local_addr().expect("address")
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0u8; 4096];
            let count = socket.read(&mut chunk).await.expect("request bytes");
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .expect("length")
                    .trim()
                    .parse()
                    .expect("number");
                if bytes.len() >= end + 4 + length {
                    assert!(headers.contains("x-extension-report: ready"));
                    assert!(headers.contains("idempotency-key: extension-invocation"));
                    assert!(headers.contains("authorization: bearer "));
                    break;
                }
            }
        }
        socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.expect("response");
    });
    let provider = support::provider(&url, ApiFormat::OpenAiResponses);
    let (request, binding) = activity_request();
    let mut operation = provider.execution(request, binding).expect("operation");
    let mut annotations = musteragent_runtime_model::ModelRequestAnnotations::default();
    annotations
        .insert_header("X-Extension-Report", "ready")
        .expect("header");
    annotations
        .insert_header("Idempotency-Key", "extension-invocation")
        .expect("key");
    assert!(
        annotations
            .insert_header("Authorization", "replacement")
            .is_err()
    );
    operation
        .annotate(annotations)
        .expect("annotations supported");
    operation.preflight().await.expect("prepare");
    assert!(
        operation.annotate(Default::default()).is_err(),
        "prepared requests are immutable"
    );
    let events = operation.into_stream().collect::<Vec<_>>().await;
    assert!(
        events.iter().any(Result::is_err),
        "server returned 400 after capturing headers"
    );
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server bounded")
        .expect("server assertions");
}
