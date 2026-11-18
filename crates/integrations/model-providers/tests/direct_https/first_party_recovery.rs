//! First-party transport shares the same durable dependency classification as
//! native HTTP, without performing hidden sends inside an invocation.
use super::*;
use musteragent_model_providers::{
    FirstPartyPrepareFuture, FirstPartyRequestTransport, FirstPartySendFuture, NativeWireBody,
    PreparedFirstPartyRequest, PreparedModelHttpRequest,
};

struct Transport {
    sends: Arc<AtomicUsize>,
    stall: bool,
}
struct Prepared {
    native: reqwest::Request,
    sends: Arc<AtomicUsize>,
    stall: bool,
}
impl FirstPartyRequestTransport for Transport {
    fn prepare<'a>(
        &'a self,
        _: &'a musteragent_runtime_model::ModelRequestAuthority,
        _: &'a ProviderEndpoint,
        _: NativeWireBody,
        native: reqwest::Request,
    ) -> FirstPartyPrepareFuture<'a> {
        Box::pin(async move {
            Ok(PreparedModelHttpRequest::FirstParty(Box::new(Prepared {
                native,
                sends: self.sends.clone(),
                stall: self.stall,
            })))
        })
    }
}
impl PreparedFirstPartyRequest for Prepared {
    fn replace_bearer_credential(&mut self, value: reqwest::header::HeaderValue) {
        self.native
            .headers_mut()
            .insert(reqwest::header::AUTHORIZATION, value);
    }
    fn send(self: Box<Self>, client: reqwest::Client) -> FirstPartySendFuture {
        Box::pin(async move {
            self.sends.fetch_add(1, Ordering::SeqCst);
            if self.stall {
                std::future::pending::<()>().await;
            }
            Ok(client.execute(self.native).await.expect("fixture HTTP"))
        })
    }
}
pub(super) fn provider(
    endpoint: &str,
    sends: Arc<AtomicUsize>,
    stall: bool,
) -> DirectModelProvider {
    let registry = StaticEndpointRegistry::new(
        [ProviderEndpoint {
            model_provider_id: fixture_model_provider_id(),
            model_entry_id: fixture_model_entry_id(),
            api_format: ApiFormat::OpenAiResponses,
            invocation_url: Url::parse(endpoint).expect("URL"),
            connect_timeout: Duration::from_millis(10),
            read_timeout: Duration::from_millis(20),
            total_timeout: Duration::from_millis(if stall { 30 } else { 3000 }),
            anthropic_version: None,
            aws_region: None,
            origin: musteragent_model_providers::ProviderEndpointOrigin::RouteGateway,
        }],
        true,
    )
    .expect("registry");
    DirectModelProvider::new(
        Arc::new(registry),
        Arc::new(FixtureCredentials),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("provider")
    .with_first_party_transport(Arc::new(Transport { sends, stall }))
}

#[tokio::test]
async fn first_party_http_failures_keep_typed_recovery_without_in_place_retries() {
    for (status, body, expected, recoverable) in [
        (503, "{}", ProviderErrorCode::GatewayUnavailable, true),
        (429, "{}", ProviderErrorCode::GatewayUnavailable, true),
        (401, "{}", ProviderErrorCode::AuthorizationFailed, false),
        (400, "{}", ProviderErrorCode::InvalidRequest, false),
        (
            502,
            r#"{"detail":{"details":{"upstream_status":503}}}"#,
            ProviderErrorCode::ProviderServer,
            true,
        ),
        (
            502,
            r#"{"detail":{"details":{"upstream_status":402}}}"#,
            ProviderErrorCode::PaymentRequired,
            false,
        ),
        (
            429,
            r#"{"error":{"upstream_status":429}}"#,
            ProviderErrorCode::RateLimited,
            true,
        ),
    ] {
        let server = MockProviderSse::start(ProviderSseScenario::failure(status, body))
            .await
            .expect("HTTP");
        let sends = Arc::new(AtomicUsize::new(0));
        let provider = provider(server.endpoint(), sends.clone(), false);
        let request = request(fixture_model_provider_id(), fixture_model_entry_id());
        provider.preflight(&request).await.expect("local prepare");
        assert_eq!(sends.load(Ordering::SeqCst), 0);
        let events = provider.stream(request).collect::<Vec<_>>().await;
        assert!(
            matches!(events.as_slice(), [Err(error)] if error.safe_code == expected
            && error.permits_dependency_recovery() == recoverable),
            "{events:?}"
        );
        assert_eq!(sends.load(Ordering::SeqCst), 1);
        assert_eq!(server.captured_requests().await.len(), 1);
        server.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn first_party_response_start_timeout_returns_recoverable_failure_and_consumes_send_once() {
    let sends = Arc::new(AtomicUsize::new(0));
    let provider = provider("http://127.0.0.1:9/", sends.clone(), true);
    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(
        matches!(events.as_slice(), [Err(error)] if error.safe_code == ProviderErrorCode::GatewayUnavailable
        && error.permits_dependency_recovery())
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
}
