use super::*;
use musteragent_identity::UtcTimestamp;
use std::time::Duration;

mod http;

#[cfg(target_os = "android")]
#[test]
fn absent_android_host_rejects_embedding_before_tls_pool_construction() {
    let credentials = Arc::new(NativeModelCredentialAuthority::new(
        secrecy::SecretString::from("fixture"),
        UtcTimestamp::from_unix_millis(60_000),
        || UtcTimestamp::from_unix_millis(0),
    ));
    assert_eq!(
        NativeMemoryEmbeddingProvider::new(
            Url::parse("https://gateway.example/v1").expect("HTTPS"),
            credentials,
            NativeEmbeddingHttpPolicy::HttpsDirect,
        )
        .err(),
        Some(MemoryEmbeddingError::Unavailable)
    );
}

async fn delayed_provider() -> (NativeMemoryEmbeddingProvider, tokio::task::JoinHandle<()>) {
    let app = axum::Router::new().route(
        "/v1/embeddings",
        axum::routing::post(|headers: axum::http::HeaderMap| async move {
            let budget: u64 = headers[MemoryEmbeddingDeadline::timeout_header()]
                .to_str()
                .expect("ASCII timeout header")
                .parse()
                .expect("numeric timeout header");
            assert!((1..=5000).contains(&budget));
            tokio::time::sleep(Duration::from_secs(2)).await;
            axum::Json(serde_json::json!({
                "data": [{"index": 0, "embedding": [0.1, 0.2]}],
                "musteragent_model_key": "test-embedding",
                "musteragent_model_revision": "revision-1",
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test gateway");
    let base = Url::parse(&format!(
        "http://{}/v1",
        listener.local_addr().expect("test gateway address")
    ))
    .expect("test gateway URL");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve test gateway");
    });
    let credentials = Arc::new(NativeModelCredentialAuthority::new(
        secrecy::SecretString::from("test-token"),
        UtcTimestamp::from_unix_millis(60_000),
        || UtcTimestamp::from_unix_millis(0),
    ));
    let mut provider = NativeMemoryEmbeddingProvider::new(
        base,
        credentials,
        NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp,
    )
    .expect("native provider");
    provider.client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .expect("test HTTP client");
    (provider, server)
}

#[test]
fn native_gateway_policy_keeps_mobile_strict_and_desktop_loopback_explicit() {
    for url in [
        "http://gateway.example/v1",
        "file:///v1",
        "https://user:secret@gateway.example/v1",
        "https://gateway.example/v1?token=x",
        "https://gateway.example/v1#fragment",
    ] {
        for policy in [
            NativeEmbeddingHttpPolicy::HttpsDirect,
            NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp,
        ] {
            assert_eq!(
                validate_gateway(&Url::parse(url).expect("URL"), policy),
                Err(MemoryEmbeddingError::InvalidInput),
                "{url}"
            );
        }
    }
    for url in [
        "http://127.0.0.1/v1",
        "http://localhost/v1",
        "http://[::1]/v1",
    ] {
        let url = Url::parse(url).expect("loopback");
        assert_eq!(
            validate_gateway(&url, NativeEmbeddingHttpPolicy::HttpsDirect),
            Err(MemoryEmbeddingError::InvalidInput)
        );
        assert!(
            validate_gateway(&url, NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp).is_ok()
        );
    }
    assert!(
        validate_gateway(
            &Url::parse("https://gateway.example/v1").expect("HTTPS"),
            NativeEmbeddingHttpPolicy::HttpsDirect
        )
        .is_ok()
    );
}

fn embedding_response(data: Vec<EmbeddingItem>) -> EmbeddingResponse {
    EmbeddingResponse {
        data,
        musteragent_model_key: "embedding-default".into(),
        musteragent_model_revision: "revision-1".into(),
    }
}

#[test]
fn embedding_indices_must_be_a_permutation_of_exact_input_positions() {
    let row = |index| EmbeddingItem {
        index,
        embedding: vec![index as f32, 0.25],
    };
    let good = embedding_response(vec![row(1), row(0)])
        .into_batch(2)
        .expect("out-of-order response");
    assert_eq!(good.vectors, vec![vec![0., 0.25], vec![1., 0.25]]);
    for indices in [vec![0, 0], vec![1, 1], vec![0, 2], vec![0], vec![0, 1, 2]] {
        assert_eq!(
            embedding_response(indices.into_iter().map(row).collect()).into_batch(2),
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
}

#[test]
fn embedding_vectors_reject_invalid_dimensions_values_and_model_identity() {
    for vectors in [
        vec![],
        vec![vec![], vec![]],
        vec![vec![1.], vec![1., 2.]],
        vec![vec![f32::NAN], vec![1.]],
        vec![vec![1.], vec![f32::INFINITY]],
    ] {
        let response = embedding_response(
            vectors
                .into_iter()
                .enumerate()
                .map(|(index, embedding)| EmbeddingItem { index, embedding })
                .collect(),
        );
        assert_eq!(
            response.into_batch(2),
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
    for (key, revision) in [(" ", "revision"), ("model", " ")] {
        let mut response = embedding_response(vec![EmbeddingItem {
            index: 0,
            embedding: vec![1.],
        }]);
        response.musteragent_model_key = key.into();
        response.musteragent_model_revision = revision.into();
        assert_eq!(
            response.into_batch(1),
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
}

#[test]
fn configured_catalog_rejects_ambiguous_missing_and_invalid_model_contracts() {
    let model = serde_json::json!({
        "id": "embedding-default", "musteragentModelRevision": "revision-1", "maxInputTokens": 8192,
    });
    for models in [vec![], vec![model.clone(), model.clone()]] {
        let catalog: EmbeddingCatalog = serde_json::from_value(serde_json::json!({
            "ready": true, "defaultModel": "embedding-default", "models": models,
        }))
        .expect("catalog");
        assert_eq!(
            catalog.into_configured_model(),
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
    for (ready, degradation, default_model, max_tokens) in [
        (false, "unavailable", None, 8192),
        (false, "fts_only", Some("embedding-default"), 8192),
        (true, "", Some("embedding-default"), 0),
        (true, "", None, 8192),
    ] {
        let catalog: EmbeddingCatalog = serde_json::from_value(serde_json::json!({
            "ready": ready, "degradation": degradation, "defaultModel": default_model,
            "models": [{ "id": "embedding-default", "musteragentModelRevision": "revision-1",
                "maxInputTokens": max_tokens }],
        }))
        .expect("catalog");
        assert_eq!(
            catalog.into_configured_model(),
            Err(MemoryEmbeddingError::InvalidResponse)
        );
    }
}

#[tokio::test]
async fn managed_embedding_accepts_response_beyond_the_old_network_caps() {
    let (provider, server) = delayed_provider().await;
    let deadline = MemoryEmbeddingDeadline::after(Duration::from_secs(5));
    let result = deadline
        .run(provider.embed(vec!["query".to_owned()], deadline))
        .await;
    server.abort();
    assert_eq!(
        result.expect("successful delayed embedding").vectors,
        vec![vec![0.1, 0.2]]
    );
}

#[tokio::test]
async fn managed_embedding_network_timeout_keeps_its_category() {
    let (provider, server) = delayed_provider().await;
    let deadline = MemoryEmbeddingDeadline::after(Duration::from_millis(50));
    // Exercise reqwest's timeout mapping, independently of the caller guard.
    let result = provider.embed(vec!["query".to_owned()], deadline).await;
    server.abort();
    assert_eq!(result, Err(MemoryEmbeddingError::Timeout));
    assert_eq!(
        memory_embedding_http_error(reqwest::StatusCode::GATEWAY_TIMEOUT),
        MemoryEmbeddingError::Timeout
    );
}

#[test]
fn explicit_fts_only_catalog_is_not_a_provider_failure() {
    let catalog: EmbeddingCatalog = serde_json::from_value(serde_json::json!({
        "ready": false,
        "degradation": "fts_only",
        "defaultModel": null,
        "models": [],
    }))
    .expect("catalog");

    assert!(matches!(
        catalog.into_configured_model(),
        Err(MemoryEmbeddingError::NotConfigured)
    ));
}

#[test]
fn configured_catalog_requires_and_preserves_the_input_token_ceiling() {
    let catalog: EmbeddingCatalog = serde_json::from_value(serde_json::json!({
        "ready": true,
        "degradation": null,
        "defaultModel": "embedding-default",
        "models": [{
            "id": "embedding-default",
            "musteragentModelRevision": "revision-1",
            "maxInputTokens": 8192,
        }],
    }))
    .expect("catalog");

    let configured = catalog.into_configured_model().expect("configured model");

    assert_eq!(configured.model_key, "embedding-default");
    assert_eq!(configured.model_revision, "revision-1");
    assert_eq!(configured.max_input_tokens.get(), 8_192);
}

#[test]
fn provider_contract_rejections_are_non_retryable_embedding_inputs() {
    for status in [
        reqwest::StatusCode::BAD_REQUEST,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
    ] {
        assert_eq!(
            memory_embedding_http_error(status),
            MemoryEmbeddingError::InvalidInput
        );
    }
    assert_eq!(
        memory_embedding_http_error(reqwest::StatusCode::BAD_GATEWAY),
        MemoryEmbeddingError::Unavailable
    );
}
