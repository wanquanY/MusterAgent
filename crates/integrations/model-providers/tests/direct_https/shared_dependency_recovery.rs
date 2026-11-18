#[path = "shared_dependency_recovery/revisions.rs"]
mod revisions;
use super::*;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_model::dependency_recovery::*;
use musteragent_testkit::{MockProviderWire, ProviderWireScenario};
use std::sync::Mutex;

#[derive(Default)]
struct Store {
    state: Mutex<DependencyRecoveryState>,
    credential_state: Mutex<DependencyRecoveryState>,
    publications: AtomicUsize,
}
impl Store {
    fn scope(&self, key: &ModelDependencyKey) -> &Mutex<DependencyRecoveryState> {
        if key.model_entry_id.is_some() {
            &self.state
        } else {
            &self.credential_state
        }
    }
}
fn now() -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis() as i64,
    )
}
impl ModelDependencyRecoveryStore for Store {
    fn acquire<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        invocation_id: InvocationId,
        lifetime: Duration,
    ) -> DependencyFuture<'a, DependencyAdmission> {
        Box::pin(async move {
            self.scope(key)
                .lock()
                .expect("state")
                .acquire(invocation_id, now(), lifetime)
        })
    }
    fn observe<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        permit: DependencyPermit,
        observation: DependencyObservation,
    ) -> DependencyFuture<'a, ()> {
        Box::pin(async move {
            self.publications.fetch_add(1, Ordering::SeqCst);
            self.scope(key)
                .lock()
                .expect("state")
                .observe(permit, observation, now())?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn stalled_http_error_body_is_bounded_and_shared_wait_makes_no_second_http_request() {
    let server = MockProviderWire::start(ProviderWireScenario {
        status: 503,
        content_type: "application/json".into(),
        chunks: vec![],
        hang_after_chunks: true,
    })
    .await
    .expect("HTTP");
    let store = Arc::new(Store::default());
    let provider = provider_for(server.endpoint(), Duration::from_secs(1))
        .with_dependency_recovery(store.clone());
    let request = request(fixture_model_provider_id(), fixture_model_entry_id());
    let events = tokio::time::timeout(
        Duration::from_secs(4),
        provider.stream(request.clone()).collect::<Vec<_>>(),
    )
    .await
    .expect("status reaches recovery even when error body hangs");
    assert!(
        matches!(events.as_slice(),[Err(error)] if error.safe_code==ProviderErrorCode::ProviderServer && error.permits_dependency_recovery())
    );
    let mut next = request;
    next.invocation.invocation_id = InvocationId::generate();
    let events = provider.stream(next).collect::<Vec<_>>().await;
    assert!(
        matches!(events.as_slice(),[Err(error)] if error.retry_after_ms.is_some_and(|delay| delay>0) && error.permits_dependency_recovery())
    );
    assert_eq!(
        server.captured_requests().await.len(),
        1,
        "coalesced Run must not send a probe prematurely"
    );
    assert_eq!(
        store.state.lock().expect("state").failures,
        1,
        "deferral is not another endpoint failure"
    );
    assert_eq!(store.publications.load(Ordering::SeqCst), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn one_successful_real_probe_releases_subsequent_requests_without_health_writes_on_the_fast_path()
 {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![ProviderSseFrame::new(
        "response.completed", r#"{"type":"response.completed","response":{"id":"resp_probe","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
    )])).await.expect("HTTP");
    let store = Arc::new(Store::default());
    store.state.lock().expect("state").failures = 1;
    let provider = provider_for(server.endpoint(), Duration::from_secs(1))
        .with_dependency_recovery(store.clone());
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    for _ in 0..2 {
        request.invocation.invocation_id = InvocationId::generate();
        let events = provider.stream(request.clone()).collect::<Vec<_>>().await;
        assert!(events.iter().all(Result::is_ok));
    }
    assert_eq!(server.captured_requests().await.len(), 2);
    assert_eq!(store.state.lock().expect("state").failures, 0);
    assert_eq!(
        store.publications.load(Ordering::SeqCst),
        1,
        "healthy non-probes do not contend on shared state"
    );
    server.shutdown().await.expect("shutdown");
}

fn sse_rate_limited_scenario() -> ProviderSseScenario {
    ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_sse_429"}}"#,
        ),
        ProviderSseFrame::new(
            "response.failed",
            r#"{"type":"response.failed","response":{"id":"resp_sse_429","status":"failed","error":{"code":"upstream_rate_limited","upstream_status":429}}}"#,
        ),
    ])
}

#[tokio::test]
async fn http_200_then_sse_429_publishes_one_fenced_unavailable_and_defers_the_next_request() {
    let server = MockProviderSse::start(sse_rate_limited_scenario())
        .await
        .expect("HTTP");
    let store = Arc::new(Store::default());
    let provider = provider_for(server.endpoint(), Duration::from_secs(1))
        .with_dependency_recovery(store.clone());

    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(
        matches!(
            events.last(),
            Some(Ok(ModelStreamEvent::ResponseError(error)))
                if error.kind == ProviderErrorKind::RateLimited
                    && error.safe_code == ProviderErrorCode::RateLimited
                    && error.retryable
        ),
        "the caller still receives the real terminal SSE failure: {events:?}"
    );
    {
        let state = store.state.lock().expect("state");
        assert_eq!(
            state.failures, 1,
            "HTTP 200 admission must not count as health; the SSE 429 is the outcome"
        );
        assert!(state.retry_not_before > now(), "backoff is armed");
    }
    assert_eq!(
        store.publications.load(Ordering::SeqCst),
        1,
        "one invocation publishes its terminal outcome at most once"
    );

    let mut queued = request(fixture_model_provider_id(), fixture_model_entry_id());
    queued.invocation.invocation_id = InvocationId::generate();
    let events = provider.stream(queued).collect::<Vec<_>>().await;
    assert!(
        matches!(events.as_slice(), [Err(error)] if error.retry_after_ms.is_some_and(|delay| delay > 0) && error.permits_dependency_recovery()),
        "the coalesced request waits on the shared backoff: {events:?}"
    );
    assert_eq!(
        server.captured_requests().await.len(),
        1,
        "a deferred request never reaches the provider"
    );
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn http_200_does_not_clear_an_active_outage_before_the_terminal_sse_outcome() {
    let server = MockProviderSse::start(sse_rate_limited_scenario())
        .await
        .expect("HTTP");
    let store = Arc::new(Store::default());
    store.state.lock().expect("state").failures = 1;
    let provider = provider_for(server.endpoint(), Duration::from_secs(1))
        .with_dependency_recovery(store.clone());

    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseError(error)))
            if error.kind == ProviderErrorKind::RateLimited
    ));

    {
        let state = store.state.lock().expect("state");
        assert_eq!(
            state.failures, 2,
            "the probe's HTTP 200 must not reset the outage before the SSE 429 arrives"
        );
        assert!(state.retry_not_before > now());
    }
    assert_eq!(
        store.publications.load(Ordering::SeqCst),
        1,
        "no premature Healthy publication followed by the real outcome"
    );
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn eof_before_terminal_after_http_200_is_an_unavailable_outcome() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_half_open"}}"#,
        ),
    ]))
    .await
    .expect("HTTP");
    let store = Arc::new(Store::default());
    let provider = provider_for(server.endpoint(), Duration::from_secs(1))
        .with_dependency_recovery(store.clone());

    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        events.last(),
        Some(Err(error))
            if error.kind == ProviderErrorKind::Transport
                && error.safe_code == ProviderErrorCode::TransportFailed
    ));
    assert_eq!(store.state.lock().expect("state").failures, 1);
    assert_eq!(store.publications.load(Ordering::SeqCst), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn idle_timeout_after_http_200_is_an_unavailable_outcome() {
    let server = MockProviderSse::start_hanging(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_idle"}}"#,
        ),
    ]))
    .await
    .expect("HTTP");
    let store = Arc::new(Store::default());
    let provider = provider_for(server.endpoint(), Duration::from_millis(50))
        .with_dependency_recovery(store.clone());

    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        events.last(),
        Some(Err(error))
            if error.kind == ProviderErrorKind::Timeout
                && error.safe_code == ProviderErrorCode::DeadlineExceeded
    ));
    assert_eq!(store.state.lock().expect("state").failures, 1);
    assert_eq!(store.publications.load(Ordering::SeqCst), 1);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dropping_the_stream_after_http_200_publishes_no_health_claim() {
    let server = MockProviderSse::start_hanging(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_dropped"}}"#,
        ),
    ]))
    .await
    .expect("HTTP");
    let store = Arc::new(Store::default());
    store.state.lock().expect("state").failures = 1;
    let provider = provider_for(server.endpoint(), Duration::from_secs(5))
        .with_dependency_recovery(store.clone());

    let mut stream = provider.stream(request(
        fixture_model_provider_id(),
        fixture_model_entry_id(),
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(ModelStreamEvent::ResponseStarted { .. }))
    ));
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        store.publications.load(Ordering::SeqCst),
        0,
        "cancellation must not fabricate a healthy outcome; the bounded probe lease expires on its own"
    );
    {
        let state = store.state.lock().expect("state");
        assert_eq!(
            state.failures, 1,
            "the outage is not cleared by a dropped stream"
        );
    }
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn http_context_overflow_is_classified_before_generic_bad_request() {
    for (status, body, expected) in [
        (
            400,
            r#"{"error":{"code":"context_length_exceeded"}}"#,
            ProviderErrorCode::ContextWindowExceeded,
        ),
        (
            400,
            r#"{"error":{"code":"invalid_request_error"}}"#,
            ProviderErrorCode::InvalidRequest,
        ),
        (
            413,
            r#"{"error":{"code":"request_too_large"}}"#,
            ProviderErrorCode::InvalidRequest,
        ),
        (
            429,
            r#"{"error":{"code":"context_length_exceeded"}}"#,
            ProviderErrorCode::RateLimited,
        ),
    ] {
        let server = MockProviderWire::start(ProviderWireScenario {
            status,
            content_type: "application/json".into(),
            chunks: vec![body.as_bytes().to_vec()],
            hang_after_chunks: false,
        })
        .await
        .expect("HTTP");
        let provider = provider_for(server.endpoint(), Duration::from_secs(1));
        let events = provider
            .stream(request(
                fixture_model_provider_id(),
                fixture_model_entry_id(),
            ))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(events.as_slice(), [Err(error)] if error.safe_code == expected),
            "{events:?}"
        );
        server.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn upstream_repair_is_shared_without_poisoning_endpoint_or_product_authority() {
    for (status, body, expected, repair) in [
        (
            401,
            "{}",
            ProviderErrorCode::AuthenticationFailed,
            Some(DependencyRepair::Credential),
        ),
        (
            402,
            "{}",
            ProviderErrorCode::PaymentRequired,
            Some(DependencyRepair::Payment),
        ),
        (
            429,
            r#"{"error":{"code":"insufficient_quota"}}"#,
            ProviderErrorCode::QuotaExceeded,
            Some(DependencyRepair::Quota),
        ),
        (403, "{}", ProviderErrorCode::AuthorizationFailed, None),
        (
            402,
            r#"{"detail":{"error_code":"billing_credits_exhausted"}}"#,
            ProviderErrorCode::CreditsExhausted,
            None,
        ),
        (
            403,
            r#"{"detail":{"error_code":"billing_model_not_entitled"}}"#,
            ProviderErrorCode::ModelNotEntitled,
            None,
        ),
    ] {
        let server = MockProviderWire::start(ProviderWireScenario {
            status,
            content_type: "application/json".into(),
            chunks: vec![body.as_bytes().to_vec()],
            hang_after_chunks: false,
        })
        .await
        .expect("HTTP");
        let store = Arc::new(Store::default());
        let provider = provider_for(server.endpoint(), Duration::from_secs(1))
            .with_dependency_recovery(store.clone());
        for _ in 0..2 {
            let events = provider
                .stream(request(
                    fixture_model_provider_id(),
                    fixture_model_entry_id(),
                ))
                .collect::<Vec<_>>()
                .await;
            assert!(
                matches!(events.as_slice(), [Err(error)] if error.safe_code == expected && !error.retryable
                && (repair.is_none() || error.retry_after_ms.is_some_and(|ms| ms > 290_000))),
                "{events:?}"
            );
        }
        assert_eq!(
            server.captured_requests().await.len(),
            if repair.is_some() { 1 } else { 2 }
        );
        assert_eq!(store.credential_state.lock().expect("state").repair, repair);
        assert_eq!(store.state.lock().expect("state").failures, 0);
        assert_eq!(
            store.publications.load(Ordering::SeqCst),
            usize::from(repair.is_some())
        );
        server.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn interrupted_gateway_stream_does_not_open_any_provider_circuit() {
    let server = MockProviderWire::start(ProviderWireScenario {
        status: 200,
        content_type: "text/event-stream".into(),
        chunks: vec![],
        hang_after_chunks: true,
    })
    .await
    .expect("HTTP");
    let store = Arc::new(Store::default());
    let sends = Arc::new(AtomicUsize::new(0));
    let provider = super::first_party_recovery::provider(server.endpoint(), sends.clone(), false)
        .with_dependency_recovery(store.clone());
    for _ in 0..2 {
        let events = provider
            .stream(request(
                fixture_model_provider_id(),
                fixture_model_entry_id(),
            ))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(events.last(), Some(Err(error)) if error.safe_code == ProviderErrorCode::GatewayUnavailable),
            "{events:?}"
        );
    }
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    assert_eq!(store.publications.load(Ordering::SeqCst), 0);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn explicit_hard_quota_429_keeps_a_longer_http_retry_after() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let endpoint = format!("http://{}/", listener.local_addr().expect("address"));
    let router = axum::Router::new().route(
        "/",
        axum::routing::post(|| async {
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "600")],
                axum::Json(serde_json::json!({"error":{"code":"insufficient_quota"}})),
            )
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let store = Arc::new(Store::default());
    let provider =
        provider_for(&endpoint, Duration::from_secs(1)).with_dependency_recovery(store.clone());
    let invocation = request(fixture_model_provider_id(), fixture_model_entry_id());
    for _ in 0..2 {
        let events = provider
            .stream(invocation.clone())
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::QuotaExceeded && error.retry_after_ms.is_some_and(|ms| ms > 590_000)),
            "{events:?}"
        );
    }
    assert_eq!(store.publications.load(Ordering::SeqCst), 1);
    server.abort();
}
