use super::*;
use axum::{
    Json, Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct Gateway {
    base: Url,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Gateway {
    async fn start(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let base = Url::parse(&format!(
            "http://{}/v1",
            listener.local_addr().expect("address")
        ))
        .expect("URL");
        Self {
            base,
            server: tokio::spawn(async move {
                axum::serve(listener, router).await.expect("test gateway");
            }),
        }
    }
    fn provider(
        &self,
        credentials: Arc<NativeModelCredentialAuthority>,
    ) -> NativeMemoryEmbeddingProvider {
        NativeMemoryEmbeddingProvider::new(
            self.base.clone(),
            credentials,
            NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp,
        )
        .expect("provider")
    }
}
fn credentials() -> Arc<NativeModelCredentialAuthority> {
    Arc::new(NativeModelCredentialAuthority::new(
        secrecy::SecretString::from("fixture-first"),
        UtcTimestamp::from_unix_millis(60_000),
        || UtcTimestamp::from_unix_millis(0),
    ))
}
fn budget() -> MemoryEmbeddingDeadline {
    MemoryEmbeddingDeadline::after(Duration::from_secs(5))
}
fn output() -> Json<serde_json::Value> {
    Json(json!({
        "data": [{"index": 0, "embedding": [0.25, 0.75]}],
        "musteragent_model_key": "test-model", "musteragent_model_revision": "test-revision",
    }))
}
fn catalog() -> Json<serde_json::Value> {
    Json(json!({
        "ready": true, "defaultModel": "test-model",
        "models": [{"id": "test-model", "musteragentModelRevision": "test-revision", "maxInputTokens": 8192}],
    }))
}

#[tokio::test]
async fn current_credential_is_used_for_both_routes_and_revocation_is_terminal() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let embed_seen = seen.clone();
    let catalog_seen = seen.clone();
    let gateway = Gateway::start(
        Router::new()
            .route(
                "/v1/embeddings",
                post(
                    move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                        let seen = embed_seen.clone();
                        async move {
                            assert_eq!(body, json!({"input": ["query"]}));
                            let remaining: u64 = headers[MemoryEmbeddingDeadline::timeout_header()]
                                .to_str()
                                .expect("budget")
                                .parse()
                                .expect("millis");
                            assert!((1..=5000).contains(&remaining));
                            seen.lock().expect("seen").push(
                                headers["authorization"]
                                    .to_str()
                                    .expect("bearer")
                                    .to_owned(),
                            );
                            output()
                        }
                    },
                ),
            )
            .route(
                "/v1/embedding-models",
                get(move |headers: HeaderMap| {
                    let seen = catalog_seen.clone();
                    async move {
                        seen.lock().expect("seen").push(
                            headers["authorization"]
                                .to_str()
                                .expect("bearer")
                                .to_owned(),
                        );
                        catalog()
                    }
                }),
            ),
    )
    .await;
    let credentials = credentials();
    let provider = gateway.provider(credentials.clone());
    assert_eq!(
        provider
            .configured_model(budget())
            .await
            .expect("model")
            .max_input_tokens
            .get(),
        8192
    );
    provider
        .embed(vec!["query".into()], budget())
        .await
        .expect("first request");
    credentials
        .rotate(
            secrecy::SecretString::from("fixture-second"),
            UtcTimestamp::from_unix_millis(120_000),
        )
        .expect("renewal");
    provider
        .configured_model(budget())
        .await
        .expect("renewed catalog");
    provider
        .embed(vec!["query".into()], budget())
        .await
        .expect("renewed request");
    credentials.revoke();
    assert_eq!(
        provider.embed(vec!["query".into()], budget()).await,
        Err(MemoryEmbeddingError::Unavailable)
    );
    assert_eq!(
        provider.configured_model(budget()).await,
        Err(MemoryEmbeddingError::Unavailable)
    );
    assert_eq!(
        *seen.lock().expect("seen"),
        [
            "Bearer fixture-first",
            "Bearer fixture-first",
            "Bearer fixture-second",
            "Bearer fixture-second",
        ]
    );
}

#[tokio::test]
async fn invalid_input_expired_credential_and_expired_deadline_never_dispatch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let gateway = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(move || {
            let calls = observed.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                output()
            }
        }),
    ))
    .await;
    let provider = gateway.provider(credentials());
    for input in [
        vec![],
        vec![" ".into()],
        vec![String::new()],
        vec!["x".into(); 33],
        vec!["x".repeat(MAX_INPUT_BYTES + 1)],
    ] {
        assert_eq!(
            provider.embed(input, budget()).await,
            Err(MemoryEmbeddingError::InvalidInput)
        );
    }
    assert_eq!(
        provider
            .embed(
                vec!["query".into()],
                MemoryEmbeddingDeadline::after(Duration::ZERO)
            )
            .await,
        Err(MemoryEmbeddingError::Timeout)
    );
    let expired = gateway.provider(Arc::new(NativeModelCredentialAuthority::new(
        secrecy::SecretString::from("fixture-expired"),
        UtcTimestamp::from_unix_millis(0),
        || UtcTimestamp::from_unix_millis(0),
    )));
    assert_eq!(
        expired.embed(vec!["query".into()], budget()).await,
        Err(MemoryEmbeddingError::Unavailable)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn revocation_during_network_io_discards_the_stale_response() {
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let entered_server = entered.clone();
    let finish_server = finish.clone();
    let gateway = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(move || {
            let entered = entered_server.clone();
            let finish = finish_server.clone();
            async move {
                entered.notify_one();
                finish.notified().await;
                output()
            }
        }),
    ))
    .await;
    let credentials = credentials();
    let provider = gateway.provider(credentials.clone());
    let pending = tokio::spawn(async move { provider.embed(vec!["query".into()], budget()).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("request started");
    credentials.revoke();
    finish.notify_one();
    assert_eq!(
        pending.await.expect("request joined"),
        Err(MemoryEmbeddingError::Unavailable)
    );
}

#[tokio::test]
async fn redirects_are_not_followed_and_transient_errors_are_not_retried() {
    let target_calls = Arc::new(AtomicUsize::new(0));
    let observed = target_calls.clone();
    let target = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(move || {
            let calls = observed.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                output()
            }
        }),
    ))
    .await;
    let location = format!("{}/embeddings", target.base);
    let source_calls = Arc::new(AtomicUsize::new(0));
    let observed = source_calls.clone();
    let source = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(move || {
            let location = location.clone();
            let calls = observed.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    (StatusCode::TEMPORARY_REDIRECT, [("location", location)]).into_response()
                } else {
                    StatusCode::SERVICE_UNAVAILABLE.into_response()
                }
            }
        }),
    ))
    .await;
    let provider = source.provider(credentials());
    for _ in 0..2 {
        assert_eq!(
            provider.embed(vec!["query".into()], budget()).await,
            Err(MemoryEmbeddingError::Unavailable)
        );
    }
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
    assert_eq!(target_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wrong_content_type_and_oversized_announced_response_are_rejected() {
    for (content_type, content_length, body) in [
        ("text/html", 2, "{}".to_owned()),
        (
            "application/json",
            (MAX_EMBEDDING_BYTES + 1) as u64,
            " ".repeat(MAX_EMBEDDING_BYTES + 1),
        ),
        ("application/json", 2, "{}".to_owned()),
    ] {
        let gateway = Gateway::start(Router::new().route(
            "/v1/embeddings",
            post(move || {
                let body = body.clone();
                async move {
                    Response::builder()
                        .header("content-type", content_type)
                        .header("content-length", content_length)
                        .body(Body::from(body))
                        .expect("response")
                }
            }),
        ))
        .await;
        assert_eq!(
            gateway
                .provider(credentials())
                .embed(vec!["query".into()], budget())
                .await,
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
}

#[tokio::test]
async fn streaming_limit_is_enforced_without_waiting_for_the_body_to_finish() {
    let gateway = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(|| async {
            let stream = async_stream::stream! {
                yield Ok::<_, std::convert::Infallible>(vec![b' '; MAX_EMBEDDING_BYTES + 1]);
                std::future::pending::<()>().await;
            };
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from_stream(stream))
                .expect("streaming response")
        }),
    ))
    .await;
    assert_eq!(
        gateway
            .provider(credentials())
            .embed(vec!["query".into()], budget())
            .await,
        Err(MemoryEmbeddingError::InvalidResponse)
    );
}

#[tokio::test]
async fn catalog_has_an_independent_small_response_budget() {
    let gateway = Gateway::start(Router::new().route(
        "/v1/embedding-models",
        get(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                " ".repeat(MAX_CATALOG_BYTES + 1),
            )
        }),
    ))
    .await;
    assert_eq!(
        gateway
            .provider(credentials())
            .configured_model(budget())
            .await,
        Err(MemoryEmbeddingError::InvalidResponse)
    );
}

#[tokio::test]
async fn body_stall_shares_the_original_request_deadline() {
    let gateway = Gateway::start(Router::new().route(
        "/v1/embeddings",
        post(|| async {
            let stream = async_stream::stream! {
                yield Ok::<_, std::convert::Infallible>(b"{".to_vec());
                tokio::time::sleep(Duration::from_secs(2)).await;
                yield Ok(b"}".to_vec());
            };
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from_stream(stream))
                .expect("delayed body")
        }),
    ))
    .await;
    assert_eq!(
        gateway
            .provider(credentials())
            .embed(
                vec!["query".into()],
                MemoryEmbeddingDeadline::after(Duration::from_millis(100))
            )
            .await,
        Err(MemoryEmbeddingError::Timeout)
    );
}
