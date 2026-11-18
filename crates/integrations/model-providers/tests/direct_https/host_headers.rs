use super::*;
use musteragent_model_providers::{HostRequestHeaders, ModelRequestHeaderHook};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_model::{ModelRequestAuthority, ProviderError};
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct SignedContext;
impl ModelRequestHeaderHook for SignedContext {
    fn headers<'a>(
        &'a self,
        authority: &'a ModelRequestAuthority,
    ) -> ApplicationFuture<'a, Result<HeaderMap, ProviderError>> {
        Box::pin(async move {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-host-invocation",
                HeaderValue::from_str(&authority.invocation.invocation_id.to_string())
                    .expect("invocation header"),
            );
            Ok(headers)
        })
    }
}

struct RenewingCredential(AtomicUsize);
impl CredentialResolver for RenewingCredential {
    fn resolve<'a>(
        &'a self,
        _: &'a CredentialGrantRef,
        _: &'a ModelRoute,
        _: musteragent_runtime_model::ModelInvocationRouteRole,
        _: InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis() as i64;
            Ok(CredentialMaterial::ScopedBearer {
                secret: SecretString::from(if first {
                    "expired-fixture"
                } else {
                    "renewed-fixture"
                }),
                expires_at: UtcTimestamp::from_unix_millis(now + if first { 300 } else { 60_000 }),
            })
        })
    }
}

#[tokio::test]
async fn host_headers_and_idempotency_survive_retry_and_new_provider_with_renewal() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let capture = tokio::spawn(async move {
            let mut captured = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.expect("read");
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                        let length = headers
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .expect("length")
                            .parse::<usize>()
                            .expect("length number");
                        captured.push(headers);
                        break (end + 4, length);
                    }
                };
                while bytes.len() < end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.expect("body");
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                socket.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.expect("response");
            }
            captured
        });
        let credentials = Arc::new(RenewingCredential(AtomicUsize::new(0)));
        let mut call = request(fixture_model_provider_id(), fixture_model_entry_id());
        call.route.api_format = format;
        call.reasoning_policy = ReasoningPolicy::Disabled;
        call.cache_policy = CachePolicy::Disabled;
        for generation in 0..2 {
            let endpoints = StaticEndpointRegistry::new(
                [ProviderEndpoint {
                    model_provider_id: fixture_model_provider_id(),
                    model_entry_id: fixture_model_entry_id(),
                    api_format: format,
                    invocation_url: format!("http://{address}/invoke").parse().expect("URL"),
                    connect_timeout: Duration::from_secs(1),
                    read_timeout: Duration::from_secs(1),
                    total_timeout: Duration::from_secs(2),
                    anthropic_version: Some("2023-06-01".into()),
                    aws_region: None,
                    origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
                }],
                true,
            )
            .expect("registry");
            let mut static_headers = HeaderMap::new();
            static_headers.insert("x-host-static", HeaderValue::from_static("fixture-host"));
            let provider = DirectModelProvider::new(
                Arc::new(endpoints),
                credentials.clone(),
                Arc::new(NoContent),
                ProviderAdapterConfig::default(),
            )
            .expect("provider")
            .with_host_headers(HostRequestHeaders {
                static_headers,
                hook: Some(Arc::new(SignedContext)),
            });
            provider.preflight(&call).await.expect("prepare");
            if generation == 0 {
                tokio::time::sleep(Duration::from_millis(350)).await;
            }
            let events = provider.stream(call.clone()).collect::<Vec<_>>().await;
            assert!(events.iter().any(Result::is_err), "503 must surface");
        }
        let requests = tokio::time::timeout(Duration::from_secs(5), capture)
            .await
            .expect("server completed")
            .expect("capture task");
        assert_eq!(
            credentials.0.load(Ordering::SeqCst),
            3,
            "prepare, expiry renewal, resumed prepare"
        );
        for headers in requests {
            assert!(headers.contains("x-host-static: fixture-host"));
            assert!(headers.contains(&format!(
                "x-host-invocation: {}",
                call.invocation.invocation_id
            )));
            assert!(headers.contains(&format!(
                "idempotency-key: model-{}",
                call.invocation.invocation_id
            )));
            assert!(headers.contains("authorization: bearer renewed-fixture"));
        }
    }
}
