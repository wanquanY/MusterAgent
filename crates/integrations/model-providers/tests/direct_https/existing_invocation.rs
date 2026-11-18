//! Adapter boundary tests. Process/fence takeover requires separate PostgreSQL proof.
use super::*;
use axum::{
    Router,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use musteragent_model_providers::{
    AlreadyAcceptedResponse, ExistingInvocationLookupPolicy, ExistingInvocationRecovery,
    HttpExistingInvocationResolver,
};
use reqwest::header::HeaderMap;
use std::sync::Mutex;

const COMPLETION: &str = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"original-result\",\"model\":\"fixture-model\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"original completion\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"original-result\",\"model\":\"fixture-model\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n";

struct GatewayState {
    posts: AtomicUsize,
    dispatches: AtomicUsize,
    lookups: AtomicUsize,
    keys: Mutex<Vec<String>>,
    pending: usize,
    lookup_status: u16,
    conflict_code: &'static str,
    stall_first: bool,
    stall_lookup: bool,
}
struct Gateway {
    base: String,
    state: Arc<GatewayState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Gateway {
    async fn start(
        pending: usize,
        lookup_status: u16,
        stall_first: bool,
        stall_lookup: bool,
        conflict_code: &'static str,
    ) -> Self {
        let state = Arc::new(GatewayState {
            posts: AtomicUsize::new(0),
            dispatches: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            keys: Mutex::new(Vec::new()),
            pending,
            lookup_status,
            conflict_code,
            stall_first,
            stall_lookup,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let router = Router::new()
            .route("/invoke", post(invoke))
            .route("/result", get(result))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("gateway");
        });
        Self { base, state, task }
    }
    fn provider(&self, resolver: bool) -> DirectModelProvider {
        let recovery = ExistingInvocationRecovery {
            already_accepted: AlreadyAcceptedResponse {
                status: 409,
                code_pointer: "/error/code".into(),
                code: "accepted-fixture".into(),
            },
            resolver: resolver.then(|| {
                Arc::new(
                    HttpExistingInvocationResolver::new(
                        format!("{}/result", self.base).parse().expect("URL"),
                    )
                    .expect("resolver"),
                ) as Arc<_>
            }),
            policy: ExistingInvocationLookupPolicy {
                timeout_ms: 200,
                max_lookups: 4,
                initial_backoff_ms: 5,
                max_backoff_ms: 20,
            },
        };
        provider_for(&format!("{}/invoke", self.base), Duration::from_secs(1))
            .with_existing_invocation_recovery(BTreeMap::from([(
                (fixture_model_provider_id(), fixture_model_entry_id()),
                recovery,
            )]))
            .expect("recovery config")
    }
}
async fn invoke(State(state): State<Arc<GatewayState>>, headers: HeaderMap) -> Response {
    let key = headers["idempotency-key"].to_str().expect("key").to_owned();
    state.keys.lock().expect("keys").push(key);
    if state.posts.fetch_add(1, Ordering::SeqCst) == 0 {
        state.dispatches.fetch_add(1, Ordering::SeqCst);
        if state.stall_first {
            std::future::pending::<()>().await;
        }
    }
    (
        axum::http::StatusCode::CONFLICT,
        axum::Json(serde_json::json!({"error":{"code":state.conflict_code}})),
    )
        .into_response()
}
async fn result(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let index = state.lookups.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        query["key"],
        *state
            .keys
            .lock()
            .expect("keys")
            .last()
            .expect("dispatch first")
    );
    assert_eq!(headers["authorization"], "Bearer fixture-token");
    assert_eq!(headers["idempotency-key"], query["key"]);
    if state.stall_lookup {
        std::future::pending::<()>().await;
    }
    if index < state.pending {
        return axum::http::StatusCode::ACCEPTED.into_response();
    }
    if state.lookup_status != 200 {
        return axum::http::StatusCode::from_u16(state.lookup_status)
            .expect("status")
            .into_response();
    }
    ([("content-type", "text/event-stream")], COMPLETION).into_response()
}
fn call() -> CanonicalModelRequest {
    request(fixture_model_provider_id(), fixture_model_entry_id())
}

#[tokio::test]
async fn replacement_adapter_uses_original_key_and_completion_after_lost_response() {
    let gateway = Gateway::start(0, 200, true, false, "accepted-fixture").await;
    let call = call();
    let original = gateway.provider(true);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            original.stream(call.clone()).collect::<Vec<_>>()
        )
        .await
        .is_err()
    );
    drop(original);
    let replacement = gateway.provider(true);
    let events = replacement.stream(call.clone()).collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(events.iter().any(|e| matches!(e, Ok(ModelStreamEvent::TextDelta { delta, .. }) if delta.as_str() == "original completion")));
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert_eq!(gateway.state.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 2);
    assert_eq!(gateway.state.lookups.load(Ordering::SeqCst), 1);
    let usage = events
        .iter()
        .filter_map(|event| match event {
            Ok(ModelStreamEvent::Usage(usage)) => Some(*usage),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].input_tokens_uncached, 5);
    assert_eq!(usage[0].output_tokens, 2);
    assert_eq!(
        *gateway.state.keys.lock().expect("keys"),
        vec![format!("model-{}", call.invocation.invocation_id); 2]
    );
}

#[tokio::test]
async fn pending_then_completed_uses_only_bounded_get_lookups() {
    let gateway = Gateway::start(2, 200, false, false, "accepted-fixture").await;
    let events = gateway
        .provider(true)
        .stream(call())
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert_eq!(gateway.state.lookups.load(Ordering::SeqCst), 3);
    assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_forever_and_stalled_lookup_are_typed_nonretryable_timeouts() {
    for stall in [false, true] {
        let gateway = Gateway::start(usize::MAX, 200, false, stall, "accepted-fixture").await;
        let start = Instant::now();
        let events = gateway
            .provider(true)
            .stream(call())
            .collect::<Vec<_>>()
            .await;
        assert_failure(&events, ProviderErrorCode::ExistingInvocationTimeout);
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(
            gateway.state.lookups.load(Ordering::SeqCst),
            if stall { 1 } else { 4 }
        );
        assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn terminal_failure_and_lookup_failure_never_authorize_redispatch() {
    for (status, code) in [
        (410, ProviderErrorCode::ExistingInvocationFailed),
        (503, ProviderErrorCode::ExistingInvocationLookupFailed),
    ] {
        let gateway = Gateway::start(0, status, false, false, "accepted-fixture").await;
        let events = gateway
            .provider(true)
            .stream(call())
            .collect::<Vec<_>>()
            .await;
        assert_failure(&events, code);
        assert_eq!(gateway.state.lookups.load(Ordering::SeqCst), 1);
        assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn no_resolver_returns_explicit_error_without_another_dispatch() {
    let gateway = Gateway::start(0, 200, false, false, "accepted-fixture").await;
    let events = gateway
        .provider(false)
        .stream(call())
        .collect::<Vec<_>>()
        .await;
    assert_failure(
        &events,
        ProviderErrorCode::ExistingInvocationResolverUnavailable,
    );
    assert_eq!(gateway.state.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unrelated_conflict_does_not_invoke_the_resolver() {
    let gateway = Gateway::start(0, 200, false, false, "unrelated-conflict").await;
    let events = gateway
        .provider(true)
        .stream(call())
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(events.as_slice(), [Err(_)]));
    assert_eq!(gateway.state.lookups.load(Ordering::SeqCst), 0);
}

fn assert_failure(
    events: &[Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>],
    code: ProviderErrorCode,
) {
    assert!(
        matches!(events, [Err(e)] if e.safe_code == code && !e.retryable && !e.permits_dependency_recovery() && !e.safe_code.permits_alternate_provider()),
        "{events:?}"
    );
}

#[tokio::test]
async fn malformed_recovered_stream_cannot_enable_new_inference() {
    use musteragent_model_providers::{
        ExistingInvocationRequest, ExistingInvocationResolution, ExistingInvocationResolver,
    };
    struct Truncated;
    impl ExistingInvocationResolver for Truncated {
        fn resolve<'a>(
            &'a self,
            _: ExistingInvocationRequest<'a>,
        ) -> musteragent_runtime_contract::ApplicationFuture<
            'a,
            Result<ExistingInvocationResolution, musteragent_runtime_model::ProviderError>,
        > {
            Box::pin(async {
                let response = http::Response::builder().status(200)
                    .body("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"truncated\"}}\n\n")
                    .expect("response").into();
                Ok(ExistingInvocationResolution::Completed(response))
            })
        }
    }
    let gateway = Gateway::start(0, 200, false, false, "accepted-fixture").await;
    let provider = gateway
        .provider(false)
        .with_existing_invocation_recovery(BTreeMap::from([(
            (fixture_model_provider_id(), fixture_model_entry_id()),
            ExistingInvocationRecovery {
                already_accepted: AlreadyAcceptedResponse {
                    status: 409,
                    code_pointer: "/error/code".into(),
                    code: "accepted-fixture".into(),
                },
                resolver: Some(Arc::new(Truncated)),
                policy: Default::default(),
            },
        )]))
        .expect("configuration");
    let events = provider.stream(call()).collect::<Vec<_>>().await;
    assert_failure(
        &events[events.len() - 1..],
        ProviderErrorCode::ExistingInvocationLookupFailed,
    );
    assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn result_endpoint_on_another_origin_receives_no_credentials() {
    let gateway = Gateway::start(0, 200, false, false, "accepted-fixture").await;
    let other = Gateway::start(0, 200, false, false, "accepted-fixture").await;
    let provider = gateway
        .provider(false)
        .with_existing_invocation_recovery(BTreeMap::from([(
            (fixture_model_provider_id(), fixture_model_entry_id()),
            ExistingInvocationRecovery {
                already_accepted: AlreadyAcceptedResponse {
                    status: 409,
                    code_pointer: "/error/code".into(),
                    code: "accepted-fixture".into(),
                },
                resolver: Some(Arc::new(
                    HttpExistingInvocationResolver::new(
                        format!("{}/result", other.base).parse().expect("URL"),
                    )
                    .expect("resolver"),
                )),
                policy: Default::default(),
            },
        )]))
        .expect("configuration");
    let events = provider.stream(call()).collect::<Vec<_>>().await;
    assert_failure(&events, ProviderErrorCode::ExistingInvocationLookupFailed);
    assert_eq!(other.state.lookups.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_lookup_budgets_fail_configuration() {
    for policy in [
        ExistingInvocationLookupPolicy {
            max_lookups: 0,
            ..Default::default()
        },
        ExistingInvocationLookupPolicy {
            timeout_ms: u64::MAX,
            ..Default::default()
        },
        ExistingInvocationLookupPolicy {
            initial_backoff_ms: 0,
            ..Default::default()
        },
        ExistingInvocationLookupPolicy {
            max_backoff_ms: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            policy.validate().expect_err("invalid budget").safe_code,
            ProviderErrorCode::RouteConfigurationInvalid
        );
    }
}

#[tokio::test]
async fn annotated_key_survives_lookup_and_only_verified_results_confirm_remote_stop() {
    use musteragent_runtime_contract::execution_liveness::{
        ActivityExecutionTiming, ExecutionBinding, ExecutionScope, InvocationExecutionTiming,
        LivenessPolicyRef,
    };
    for (status, resolver, pending, confirmed) in [
        (200, true, 0, true),
        (410, true, 0, true),
        (200, true, usize::MAX, false),
        (200, false, 0, false),
    ] {
        let gateway = Gateway::start(pending, status, false, false, "accepted-fixture").await;
        let provider = gateway.provider(resolver);
        let mut call = call();
        call.invocation.timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: LivenessPolicyRef {
                key: versioned("musteragent.model-liveness@1"),
                digest: Sha256::digest(b"Home-sealed fixture policy"),
            },
            business_deadline: None,
        });
        let binding = ExecutionBinding {
            tenant_id: call.invocation.run_context.tenant_id,
            run_id: call.invocation.run_context.run_id,
            scope: ExecutionScope::Invocation(call.invocation.invocation_id),
            owner_attempt_id: call.invocation.attempt_context.attempt_id,
            owner_fence: call.invocation.attempt_context.fencing_token,
            producer_generation: NonZeroU64::MIN,
            operation_identity: Sha256::digest(b"Home-verified original request"),
        };
        let key = format!("host:{}?part=one&part=two", call.invocation.invocation_id);
        let mut annotations = musteragent_runtime_model::ModelRequestAnnotations::default();
        annotations
            .insert_header("idempotency-key", &key)
            .expect("host key");
        let mut operation = provider.execution(call, binding).expect("owned operation");
        operation.annotate(annotations).expect("annotations");
        let control = operation.stop_control();
        operation.preflight().await.expect("preflight");
        let events = operation.into_stream().collect::<Vec<_>>().await;
        if resolver && pending == 0 && status == 200 {
            assert!(events.iter().all(Result::is_ok), "{events:?}");
        } else {
            assert!(events.iter().any(Result::is_err));
        }
        let stop = control
            .stop(musteragent_execution_liveness::StopReason::SystemShutdown)
            .await;
        assert_eq!(
            stop.is_ok(),
            confirmed,
            "pending/missing lookup cannot prove remote stop"
        );
        assert_eq!(*gateway.state.keys.lock().expect("keys"), vec![key]);
        assert_eq!(gateway.state.posts.load(Ordering::SeqCst), 1);
    }
}
