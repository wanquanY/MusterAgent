#[path = "direct_https/shared_dependency_recovery.rs"]
mod shared_dependency_recovery;

#[path = "direct_https/first_party_recovery.rs"]
mod first_party_recovery;

#[path = "direct_https/credential_recovery.rs"]
mod credential_recovery;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use eventsource_stream::Eventsource as _;
use futures_util::StreamExt as _;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ContentId, ConversationId, CorrelationId, DottedKey,
    FencingToken, HomeEpoch, InvocationId, LeaseId, LedgerRecordId, ParticipantId, RunId,
    RuntimeSnapshotId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelParameter,
    ModelProviderId, ModelRoute, PricingVersionRef, ProviderStatePolicy,
};
use musteragent_model_providers::{
    ContentResolver, ContentResolverError, CredentialMaterial, CredentialResolver,
    CredentialResolverError, DirectModelProvider, EndpointFuture, EndpointRegistry,
    ProviderAdapterConfig, ProviderEndpoint, ResolvedContent, StaticEndpointRegistry,
};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalToolDefinition, ContinuationRef, JsonValue, ModelProvider, ModelStreamEvent,
    ModelText, ProviderErrorCode, ProviderErrorKind, ReasoningPolicy, StopReason,
};
use musteragent_testkit::{MockProviderSse, ProviderSseFrame, ProviderSseScenario};
use musteragent_tool::{ToolKey, WireName};
use secrecy::{SecretSlice, SecretString};
use url::Url;

// Installing an optional private codec must not change a native/BYOK route.
struct RejectPrivateTransport;
impl musteragent_model_providers::FirstPartyRequestTransport for RejectPrivateTransport {
    fn prepare<'a>(
        &'a self,
        _: &'a musteragent_runtime_model::ModelRequestAuthority,
        _: &'a ProviderEndpoint,
        _: musteragent_model_providers::NativeWireBody,
        _: reqwest::Request,
    ) -> musteragent_model_providers::FirstPartyPrepareFuture<'a> {
        panic!("native/BYOK request entered the private transport")
    }
}

struct FixtureCredentials;

impl CredentialResolver for FixtureCredentials {
    fn resolve<'a>(
        &'a self,
        _grant_ref: &'a CredentialGrantRef,
        _route: &'a ModelRoute,
        _route_role: musteragent_runtime_model::ModelInvocationRouteRole,
        _invocation_id: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        Box::pin(async {
            Ok(CredentialMaterial::Bearer(SecretString::new(
                "fixture-token".to_owned().into_boxed_str(),
            )))
        })
    }
}

struct NoContent;

impl ContentResolver for NoContent {
    fn resolve<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content_ref: &'a musteragent_content_ref::ContentRef,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>
    {
        Box::pin(async { Err(ContentResolverError::NotFound) })
    }
}

#[tokio::test]
async fn successful_preflight_prepares_once_and_dispatch_consumes_the_exact_request() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_prepared","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let provider = provider_for(server.endpoint(), Duration::from_secs(1));
    let request = request(fixture_model_provider_id(), fixture_model_entry_id());

    provider.preflight(&request).await.expect("preflight");
    let events = provider.stream(request).collect::<Vec<_>>().await;

    assert!(events.iter().all(Result::is_ok));
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    server.shutdown().await.expect("mock Provider shutdown");
}

// Run the real adapter in a child so proxy configuration cannot leak into parallel tests.
#[cfg(feature = "system-proxy")]
#[test]
fn loopback_inference_bypasses_a_configured_system_proxy() {
    let proxy = std::net::TcpListener::bind("127.0.0.1:0").expect("isolated proxy address");
    let address = format!("http://{}", proxy.local_addr().unwrap());
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    for name in [
        "http_proxy",
        "HTTP_PROXY",
        "https_proxy",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ] {
        child.env(name, &address);
    }
    let result = child
        .env("no_proxy", "")
        .env("NO_PROXY", "")
        .args([
            "--exact",
            "successful_preflight_prepares_once_and_dispatch_consumes_the_exact_request",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "loopback inference failed: {}",
        String::from_utf8_lossy(&result.stdout)
    );
    proxy.set_nonblocking(true).unwrap();
    assert_eq!(
        proxy.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[derive(Default)]
struct PreflightEffects {
    endpoint_resolutions: AtomicUsize,
    credential_resolutions: AtomicUsize,
    budget_reservations: AtomicUsize,
    secret_decryptions: AtomicUsize,
    content_resolutions: AtomicUsize,
}

struct RecordingEndpoint {
    effects: Arc<PreflightEffects>,
    endpoint: ProviderEndpoint,
}

impl EndpointRegistry for RecordingEndpoint {
    fn resolve<'a>(&'a self, _route: &'a ModelRoute) -> EndpointFuture<'a> {
        self.effects
            .endpoint_resolutions
            .fetch_add(1, Ordering::SeqCst);
        let endpoint = self.endpoint.clone();
        Box::pin(async move { Ok(endpoint) })
    }
}

struct RecordingCredentials(Arc<PreflightEffects>);

impl CredentialResolver for RecordingCredentials {
    fn resolve<'a>(
        &'a self,
        _grant_ref: &'a CredentialGrantRef,
        _route: &'a ModelRoute,
        _route_role: musteragent_runtime_model::ModelInvocationRouteRole,
        _invocation_id: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        self.0.credential_resolutions.fetch_add(1, Ordering::SeqCst);
        self.0.budget_reservations.fetch_add(1, Ordering::SeqCst);
        self.0.secret_decryptions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(CredentialMaterial::Bearer(SecretString::new(
                "must-not-be-decrypted".to_owned().into_boxed_str(),
            )))
        })
    }
}

struct RecordingContent(Arc<PreflightEffects>);

struct ScopedCredentials(Arc<AtomicUsize>, InvocationId);
impl CredentialResolver for ScopedCredentials {
    fn resolve<'a>(
        &'a self,
        _grant: &'a CredentialGrantRef,
        _route: &'a ModelRoute,
        _role: musteragent_runtime_model::ModelInvocationRouteRole,
        _invocation: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        Box::pin(async move {
            assert_eq!(_invocation, self.1, "prepared Invocation must not change");
            let count = self.0.fetch_add(1, Ordering::SeqCst);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_millis();
            Ok(CredentialMaterial::ScopedBearer {
                secret: SecretString::from(format!("fixture-scoped-{count}")),
                expires_at: UtcTimestamp::from_unix_millis(
                    now as i64 + if count == 0 { 500 } else { 4000 },
                ),
            })
        })
    }
}

#[tokio::test]
async fn scoped_credential_renews_after_preflight_without_replanning_or_changing_invocation() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new("response.created", r#"{"type":"response.created","response":{"id":"scoped"}}"#),
        ProviderSseFrame::new("response.completed", r#"{"type":"response.completed","response":{"id":"scoped","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#),
    ])).await.expect("fixture server");
    let count = Arc::new(AtomicUsize::new(0));
    let effects = Arc::new(PreflightEffects::default());
    let endpoint = ProviderEndpoint {
        model_provider_id: fixture_model_provider_id(),
        model_entry_id: fixture_model_entry_id(),
        api_format: ApiFormat::OpenAiResponses,
        invocation_url: Url::parse(server.endpoint()).expect("fixture endpoint"),
        connect_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_secs(2),
        total_timeout: Duration::from_secs(5),
        anthropic_version: None,
        aws_region: None,
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    };
    let request = request(fixture_model_provider_id(), fixture_model_entry_id());
    let provider = DirectModelProvider::new(
        Arc::new(RecordingEndpoint {
            effects: effects.clone(),
            endpoint,
        }),
        Arc::new(ScopedCredentials(
            count.clone(),
            request.invocation.invocation_id,
        )),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("provider");
    provider.preflight(&request).await.expect("preflight");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    let events = provider.stream(request).collect::<Vec<_>>().await;
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(effects.endpoint_resolutions.load(Ordering::SeqCst), 1);
    server.shutdown().await.expect("fixture shutdown");
}

#[tokio::test]
async fn missing_input_content_cannot_consume_a_credential_or_dispatch_http() {
    let effects = Arc::new(PreflightEffects::default());
    let endpoint = ProviderEndpoint {
        model_provider_id: fixture_model_provider_id(),
        model_entry_id: fixture_model_entry_id(),
        api_format: ApiFormat::OpenAiResponses,
        invocation_url: Url::parse("https://content-must-fail.invalid/invoke")
            .expect("fixture URL"),
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        total_timeout: Duration::from_secs(2),
        anthropic_version: None,
        aws_region: None,
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    };
    let provider = DirectModelProvider::new(
        Arc::new(RecordingEndpoint {
            effects: effects.clone(),
            endpoint,
        }),
        Arc::new(RecordingCredentials(effects.clone())),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("provider");
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route.capabilities.insert(
        DottedKey::new("input.image").expect("the fixture route authorizes its image input"),
    );
    request.input[0]
        .content
        .push(CanonicalContentPart::Image(ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("media"),
            byte_length: 5,
            sha256: Sha256::digest(b"image"),
        }));
    let error = provider
        .preflight(&request)
        .await
        .expect_err("input must be authorized first");
    assert_eq!(error.safe_code, ProviderErrorCode::ContentNotFound);
    assert_eq!(effects.credential_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.budget_reservations.load(Ordering::SeqCst), 0);
    assert_eq!(effects.secret_decryptions.load(Ordering::SeqCst), 0);
}

impl ContentResolver for RecordingContent {
    fn resolve<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content_ref: &'a ContentRef,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>
    {
        self.0.content_resolutions.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ResolvedContent::Inline {
                media_type: MediaType::new("image/png").expect("media type"),
                bytes: SecretSlice::new(b"image".to_vec().into_boxed_slice()),
            })
        })
    }
}

#[tokio::test]
async fn direct_reqwest_sse_path_produces_only_canonical_events() {
    let server = match MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_direct"}}"#,
        ),
        ProviderSseFrame::new(
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","output_index":0,"delta":"hello"}"#,
        ),
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_direct","usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6}}}"#,
        ),
    ]))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("mock Provider failed: {error}"),
    };
    let endpoint_url = match Url::parse(server.endpoint()) {
        Ok(value) => value,
        Err(error) => panic!("mock URL failed: {error}"),
    };
    let model_provider_id = fixture_model_provider_id();
    let model_entry_id = fixture_model_entry_id();
    let endpoints = match StaticEndpointRegistry::new(
        [ProviderEndpoint {
            model_provider_id,
            model_entry_id,
            api_format: ApiFormat::OpenAiResponses,
            invocation_url: endpoint_url,
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(5),
            anthropic_version: None,
            aws_region: None,
            origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
        }],
        true,
    ) {
        Ok(value) => value,
        Err(error) => panic!("endpoint registry failed: {error}"),
    };
    let provider = match DirectModelProvider::new(
        Arc::new(endpoints),
        Arc::new(FixtureCredentials),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    ) {
        Ok(value) => value,
        Err(error) => panic!("direct Provider failed: {error:?}"),
    };

    let provider = provider.with_first_party_transport(Arc::new(RejectPrivateTransport));

    let results = provider
        .stream(request(model_provider_id, model_entry_id))
        .collect::<Vec<_>>()
        .await;
    let mut events = Vec::new();
    for result in results {
        match result {
            Ok(event) => events.push(event),
            Err(error) => panic!("direct stream failed: {error:?}"),
        }
    }
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::TextDelta { delta, .. } if delta.as_str() == "hello")
    ));
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted { .. })
    ));

    if let Err(error) = server.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
async fn direct_compression_request_emits_only_owner_controlled_openai_responses_wire() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_compression","usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6}}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let provider = provider_for(server.endpoint(), Duration::from_secs(2));
    let mut compression = request(fixture_model_provider_id(), fixture_model_entry_id());
    compression.route_role = musteragent_runtime_model::ModelInvocationRouteRole::Auxiliary(
        AuxiliaryModelRole::Compression,
    );
    compression.credential_grant_ref = credential_grant("grant_compression_direct");
    compression.reasoning_policy = ReasoningPolicy::Disabled;
    compression.cache_policy = CachePolicy::Disabled;
    compression.route.parameters.insert(
        DottedKey::new("top_p").expect("sampling key"),
        ModelParameter::Double(0.7),
    );

    let results = provider.stream(compression).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok));
    let captured = server.captured_requests().await;
    assert_eq!(captured.len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(&captured[0]).expect("captured Provider request JSON");
    assert_eq!(body["store"], false);
    assert_eq!(body["temperature"], 0.2);
    assert_eq!(body["top_p"], 0.7);
    for forbidden in [
        "tools",
        "reasoning",
        "include",
        "previous_response_id",
        "cache",
        "cache_control",
    ] {
        assert!(
            body.get(forbidden).is_none(),
            "wire leaked {forbidden}: {body}"
        );
    }
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_preflight_rejects_unsafe_compression_before_all_external_side_effects() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"must_not_dispatch"}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let effects = Arc::new(PreflightEffects::default());
    let endpoint = ProviderEndpoint {
        model_provider_id: fixture_model_provider_id(),
        model_entry_id: fixture_model_entry_id(),
        api_format: ApiFormat::OpenAiResponses,
        invocation_url: Url::parse(server.endpoint()).expect("mock URL"),
        connect_timeout: Duration::from_millis(20),
        read_timeout: Duration::from_secs(1),
        total_timeout: Duration::from_secs(3),
        anthropic_version: None,
        aws_region: None,
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    };
    let provider = DirectModelProvider::new(
        Arc::new(RecordingEndpoint {
            effects: Arc::clone(&effects),
            endpoint,
        }),
        Arc::new(RecordingCredentials(Arc::clone(&effects))),
        Arc::new(RecordingContent(Arc::clone(&effects))),
        ProviderAdapterConfig::default(),
    )
    .expect("Direct Provider");
    let content_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("media type"),
        byte_length: 5,
        sha256: Sha256::digest(b"image"),
    };

    let mut tools = compression_request(content_ref.clone());
    tools.tools.push(tool_definition());
    let mut response_schema = compression_request(content_ref.clone());
    let definition = tool_definition();
    response_schema.response_schema = Some((
        definition.input_schema_ref.clone(),
        definition.input_schema.clone(),
    ));
    let mut reasoning = compression_request(content_ref.clone());
    reasoning.reasoning_policy = ReasoningPolicy::Summary;
    let mut cache = compression_request(content_ref.clone());
    cache.cache_policy = CachePolicy::ProviderDefault;
    let mut opaque_state = compression_request(content_ref.clone());
    opaque_state.route.provider_state_policy = ProviderStatePolicy::OpaqueContinuation;
    let mut continuation = opaque_state.clone();
    continuation.continuation_ref = Some(ContinuationRef::from_opaque(
        OpaqueExternalRef::new("previous-compression-response").expect("continuation"),
    ));
    let mut unknown_parameter = compression_request(content_ref.clone());
    unknown_parameter.route.parameters.insert(
        DottedKey::new("experimental_payload").expect("unknown parameter"),
        ModelParameter::Bool(true),
    );
    let mut reserved_parameter = compression_request(content_ref.clone());
    reserved_parameter.route.parameters.insert(
        DottedKey::new("previous_response_id").expect("reserved parameter"),
        ModelParameter::String("cross-wire".to_owned()),
    );
    let mut invalid_semantic_field = compression_request(content_ref);
    invalid_semantic_field.max_output_tokens = non_zero(8_192);

    for (label, request) in [
        ("tools", tools),
        ("response_schema", response_schema),
        ("reasoning", reasoning),
        ("cache", cache),
        ("opaque_state", opaque_state),
        ("continuation", continuation),
        ("unknown_parameter", unknown_parameter),
        ("reserved_parameter", reserved_parameter),
        ("invalid_semantic_field", invalid_semantic_field),
    ] {
        let expected_safe_code = if matches!(label, "unknown_parameter" | "reserved_parameter") {
            ProviderErrorCode::RouteConfigurationInvalid
        } else {
            ProviderErrorCode::InvalidRequest
        };
        let result = provider.stream(request).collect::<Vec<_>>().await;
        assert!(
            matches!(
                result.as_slice(),
                [Err(error)]
                    if error.kind == ProviderErrorKind::InvalidRequest
                        && error.safe_code == expected_safe_code
                        && !error.retryable
            ),
            "unsafe Compression case escaped preflight: {label}: {result:?}"
        );
    }

    assert_eq!(effects.endpoint_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.credential_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.budget_reservations.load(Ordering::SeqCst), 0);
    assert_eq!(effects.secret_decryptions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.content_resolutions.load(Ordering::SeqCst), 0);
    assert!(
        server.captured_requests().await.is_empty(),
        "unsafe Compression request reached controlled HTTP"
    );
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_route_parameter_preflight_rejects_contract_violations_without_side_effects() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"must_not_dispatch"}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let effects = Arc::new(PreflightEffects::default());
    let endpoint = ProviderEndpoint {
        model_provider_id: fixture_model_provider_id(),
        model_entry_id: fixture_model_entry_id(),
        api_format: ApiFormat::OpenAiResponses,
        invocation_url: Url::parse(server.endpoint()).expect("mock URL"),
        connect_timeout: Duration::from_millis(20),
        read_timeout: Duration::from_secs(1),
        total_timeout: Duration::from_secs(3),
        anthropic_version: None,
        aws_region: None,
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    };
    let provider = DirectModelProvider::new(
        Arc::new(RecordingEndpoint {
            effects: Arc::clone(&effects),
            endpoint,
        }),
        Arc::new(RecordingCredentials(Arc::clone(&effects))),
        Arc::new(RecordingContent(Arc::clone(&effects))),
        ProviderAdapterConfig::default(),
    )
    .expect("Direct Provider");

    let mut incompatible = request(fixture_model_provider_id(), fixture_model_entry_id());
    incompatible
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.openrouter").expect("style"));
    let mut duplicate = request(fixture_model_provider_id(), fixture_model_entry_id());
    duplicate
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.provider_default").expect("style"));
    duplicate
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.openai_effort").expect("style"));
    let mut unknown = request(fixture_model_provider_id(), fixture_model_entry_id());
    unknown
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.guess_from_model").expect("style"));
    let mut raw_reasoning = request(fixture_model_provider_id(), fixture_model_entry_id());
    raw_reasoning.route.parameters.insert(
        DottedKey::new("reasoning").expect("raw reasoning"),
        ModelParameter::String("caller-owned".into()),
    );
    let mut wrong_effort = request(fixture_model_provider_id(), fixture_model_entry_id());
    wrong_effort.route.parameters.insert(
        DottedKey::new("reasoning_effort").expect("effort"),
        ModelParameter::Int(3),
    );
    let mut unsupported_budget = request(fixture_model_provider_id(), fixture_model_entry_id());
    unsupported_budget.route.parameters.insert(
        DottedKey::new("reasoning_budget_tokens").expect("budget"),
        ModelParameter::Int(2_048),
    );
    let mut reasoning_content = request(fixture_model_provider_id(), fixture_model_entry_id());
    reasoning_content.route.parameters.insert(
        DottedKey::new("reasoning_content").expect("reasoning content"),
        ModelParameter::String("caller-owned".into()),
    );
    let mut include_reasoning = request(fixture_model_provider_id(), fixture_model_entry_id());
    include_reasoning.route.parameters.insert(
        DottedKey::new("include_reasoning").expect("include reasoning"),
        ModelParameter::Bool(true),
    );
    let mut reasoning_format = request(fixture_model_provider_id(), fixture_model_entry_id());
    reasoning_format.route.parameters.insert(
        DottedKey::new("reasoning_format").expect("reasoning format"),
        ModelParameter::String("raw".into()),
    );
    let mut max_tokens = request(fixture_model_provider_id(), fixture_model_entry_id());
    max_tokens.route.parameters.insert(
        DottedKey::new("max_tokens").expect("token bound"),
        ModelParameter::Int(999_999),
    );
    let mut max_completion_tokens = request(fixture_model_provider_id(), fixture_model_entry_id());
    max_completion_tokens.route.parameters.insert(
        DottedKey::new("max_completion_tokens").expect("completion token bound"),
        ModelParameter::Int(999_999),
    );
    let mut max_output_tokens = request(fixture_model_provider_id(), fixture_model_entry_id());
    max_output_tokens.route.parameters.insert(
        DottedKey::new("max_output_tokens").expect("output token bound"),
        ModelParameter::Int(999_999),
    );
    let mut cache_control = request(fixture_model_provider_id(), fixture_model_entry_id());
    cache_control.route.parameters.insert(
        DottedKey::new("cache_control").expect("cache control"),
        ModelParameter::String("caller-owned".into()),
    );
    let mut store = request(fixture_model_provider_id(), fixture_model_entry_id());
    store.route.parameters.insert(
        DottedKey::new("store").expect("store"),
        ModelParameter::Bool(true),
    );
    let mut cot_config = request(fixture_model_provider_id(), fixture_model_entry_id());
    cot_config.route.parameters.insert(
        DottedKey::new("cotconfig").expect("COT config"),
        ModelParameter::Bool(true),
    );

    for (label, request) in [
        ("incompatible_style", incompatible),
        ("duplicate_style", duplicate),
        ("unknown_style", unknown),
        ("raw_reasoning", raw_reasoning),
        ("wrong_effort", wrong_effort),
        ("unsupported_budget", unsupported_budget),
        ("reasoning_content", reasoning_content),
        ("include_reasoning", include_reasoning),
        ("reasoning_format", reasoning_format),
        ("max_tokens", max_tokens),
        ("max_completion_tokens", max_completion_tokens),
        ("max_output_tokens", max_output_tokens),
        ("cache_control", cache_control),
        ("store", store),
        ("cotconfig", cot_config),
    ] {
        let result = provider.stream(request).collect::<Vec<_>>().await;
        assert!(
            matches!(
                result.as_slice(),
                [Err(error)]
                    if error.kind == ProviderErrorKind::InvalidRequest
                        && error.safe_code == ProviderErrorCode::RouteConfigurationInvalid
                        && !error.retryable
            ),
            "invalid signed route parameter escaped preflight: {label}: {result:?}"
        );
    }

    assert_eq!(effects.endpoint_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.credential_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.budget_reservations.load(Ordering::SeqCst), 0);
    assert_eq!(effects.secret_decryptions.load(Ordering::SeqCst), 0);
    assert_eq!(effects.content_resolutions.load(Ordering::SeqCst), 0);
    assert!(server.captured_requests().await.is_empty());
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_openai_chat_wire_uses_typed_registry_reasoning_and_tool_controls() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "message",
            r#"{"id":"chatcmpl_registry","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let provider = provider_for_format(
        server.endpoint(),
        Duration::from_secs(2),
        ApiFormat::OpenAiChatCompletions,
    );
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route.api_format = ApiFormat::OpenAiChatCompletions;
    request.reasoning_policy = ReasoningPolicy::Disabled;
    request
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.openai_effort").expect("style"));
    request.route.parameters.insert(
        DottedKey::new("reasoning_effort").expect("effort"),
        ModelParameter::String("medium".into()),
    );
    request.route.parameters.insert(
        DottedKey::new("parallel_tool_calls").expect("parallel tool calls"),
        ModelParameter::Bool(false),
    );
    request.route.parameters.insert(
        DottedKey::new("tool_choice").expect("tool choice"),
        ModelParameter::String("auto".into()),
    );
    request.tools.push(tool_definition());

    let results = provider.stream(request).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let captured = server.captured_requests().await;
    assert_eq!(captured.len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(&captured[0]).expect("captured Provider request JSON");
    assert_eq!(body["reasoning_effort"], "medium");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["tool_choice"], "auto");
    assert_eq!(body["max_tokens"], 256);
    assert!(body.get("max_completion_tokens").is_none());
    assert!(body.get("reasoning").is_none());
    assert!(body.get("thinking").is_none());
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_anthropic_messages_wire_maps_signed_adaptive_effort() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_effort","model":"provider-reported-model","usage":{"input_tokens":10,"output_tokens":0}}}"#,
        ),
        ProviderSseFrame::new(
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}"#,
        ),
        ProviderSseFrame::new(
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
        ),
        ProviderSseFrame::new("message_stop", r#"{"type":"message_stop"}"#),
    ]))
    .await
    .expect("mock Provider");
    let provider = provider_for_format(
        server.endpoint(),
        Duration::from_secs(2),
        ApiFormat::AnthropicMessages,
    );
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route.api_format = ApiFormat::AnthropicMessages;
    request.reasoning_policy = ReasoningPolicy::Disabled;
    request
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_style.anthropic_adaptive").expect("style"));
    request.route.parameters.insert(
        DottedKey::new("reasoning_effort").expect("effort"),
        ModelParameter::String("high".into()),
    );

    let results = provider.stream(request).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let captured = server.captured_requests().await;
    assert_eq!(captured.len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(&captured[0]).expect("captured Provider request JSON");
    assert_eq!(body["thinking"]["type"], "adaptive");
    assert_eq!(body["output_config"]["effort"], "high");
    assert!(body.get("temperature").is_none());
    assert!(body.get("reasoning_effort").is_none());
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_openai_chat_disclosed_reasoning_follows_the_pinned_summary_capability() {
    let reasoning_frames = || {
        ProviderSseScenario::successful(vec![
            ProviderSseFrame::new(
                "message",
                r#"{"id":"chatcmpl_reasoning","choices":[{"index":0,"delta":{"reasoning_content":"thinking "}}]}"#,
            ),
            ProviderSseFrame::new(
                "message",
                r#"{"id":"chatcmpl_reasoning","choices":[{"index":0,"delta":{"reasoning_content":"about it"}}]}"#,
            ),
            ProviderSseFrame::new(
                "message",
                r#"{"id":"chatcmpl_reasoning","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}"#,
            ),
        ])
    };
    let summary_of = |events: &[ModelStreamEvent]| {
        events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::ReasoningSummaryDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>()
    };
    let private_of = |events: &[ModelStreamEvent]| {
        events
            .iter()
            .filter_map(|event| match event {
                ModelStreamEvent::ReasoningDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>()
    };

    // A route whose signed Grant pins `reasoning_summary` (the BYOK fix) plus
    // a Summary policy publishes provider-disclosed reasoning to the public
    // summary lane the Desktop renders as the thinking trace.
    let server = MockProviderSse::start(reasoning_frames())
        .await
        .expect("mock Provider");
    let provider = provider_for_format(
        server.endpoint(),
        Duration::from_secs(2),
        ApiFormat::OpenAiChatCompletions,
    );
    let mut gated = request(fixture_model_provider_id(), fixture_model_entry_id());
    gated.route.api_format = ApiFormat::OpenAiChatCompletions;
    gated.reasoning_policy = ReasoningPolicy::Summary;
    gated
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_summary").expect("capability"));
    let results = provider.stream(gated).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let events = results
        .into_iter()
        .map(|result| result.expect("stream event"))
        .collect::<Vec<_>>();
    assert_eq!(summary_of(&events), "thinking about it");
    assert_eq!(private_of(&events), "thinking about it");
    server.shutdown().await.expect("shutdown mock Provider");

    // Without the pinned capability the same stream stays on the private
    // replay lane: no public summary delta may escape.
    let server = MockProviderSse::start(reasoning_frames())
        .await
        .expect("mock Provider");
    let provider = provider_for_format(
        server.endpoint(),
        Duration::from_secs(2),
        ApiFormat::OpenAiChatCompletions,
    );
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route.api_format = ApiFormat::OpenAiChatCompletions;
    request.reasoning_policy = ReasoningPolicy::Summary;
    let results = provider.stream(request).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let events = results
        .into_iter()
        .map(|result| result.expect("stream event"))
        .collect::<Vec<_>>();
    assert_eq!(summary_of(&events), "");
    assert_eq!(private_of(&events), "thinking about it");
    server.shutdown().await.expect("shutdown mock Provider");
}

#[tokio::test]
async fn direct_openai_responses_tool_only_stream_finishes_as_tool_calls() {
    let server = match MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_tool"}}"#,
        ),
        ProviderSseFrame::new(
            "response.output_item.added",
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_tool","call_id":"call_tool","name":"get_current_time"}}"#,
        ),
        ProviderSseFrame::new(
            "response.function_call_arguments.delta",
            r#"{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}"#,
        ),
        ProviderSseFrame::new(
            "response.function_call_arguments.done",
            r#"{"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"}"#,
        ),
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_tool","status":"completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}"#,
        ),
    ]))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("mock Provider failed: {error}"),
    };
    let provider = provider_for(server.endpoint(), Duration::from_secs(2));
    let results = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    let events = results
        .into_iter()
        .map(|result| result.unwrap_or_else(|error| panic!("direct stream failed: {error:?}")))
        .collect::<Vec<_>>();

    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
    );
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls
        })
    ));

    if let Err(error) = server.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
async fn provider_stream_budget_is_cumulative() {
    let delta = "x".repeat(1_600_000);
    let first = serde_json::json!({
        "type": "response.output_text.delta",
        "output_index": 0,
        "delta": delta,
    })
    .to_string();
    let second = serde_json::json!({
        "type": "response.output_text.delta",
        "output_index": 0,
        "delta": "y".repeat(1_600_000),
    })
    .to_string();
    assert!(first.len() < 2 * 1_048_576);
    assert!(second.len() < 2 * 1_048_576);
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_budget"}}"#,
        ),
        ProviderSseFrame::new("response.output_text.delta", first),
        ProviderSseFrame::new("response.output_text.delta", second),
        ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_budget"}}"#,
        ),
    ]))
    .await
    .unwrap_or_else(|error| panic!("mock Provider failed: {error}"));
    let provider = provider_for(server.endpoint(), Duration::from_secs(2));
    let mut bounded = request(fixture_model_provider_id(), fixture_model_entry_id());
    bounded.stream_budget.max_event_bytes = NonZeroUsize::new(2 * 1_048_576).expect("event budget");
    bounded.stream_budget.max_total_bytes =
        NonZeroU64::new(3 * 1_048_576).expect("cumulative budget");
    bounded.stream_budget.max_events = NonZeroU32::new(16).expect("event count");
    bounded.stream_budget.max_tool_argument_bytes =
        NonZeroUsize::new(1_048_576).expect("argument budget");
    bounded.stream_budget.max_reasoning_replay_bytes =
        NonZeroUsize::new(1_048_576).expect("reasoning budget");

    let events = provider.stream(bounded).collect::<Vec<_>>().await;
    // The oversized stream never completes; whether the bounded attempt is
    // retried is the Run retry budget's decision.
    assert!(matches!(
        events.last(),
        Some(Err(error))
            if error.kind == ProviderErrorKind::Protocol
                && error.safe_code == ProviderErrorCode::ProtocolViolation
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::ResponseCompleted { .. })))
    );

    if let Err(error) = server.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
async fn dropping_the_canonical_stream_terminates_the_upstream_body() {
    let server = match MockProviderSse::start_hanging(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_cancel"}}"#,
        ),
    ]))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("mock Provider failed: {error}"),
    };
    let provider = provider_for(server.endpoint(), Duration::from_secs(2));
    let mut stream = provider.stream(request(
        fixture_model_provider_id(),
        fixture_model_entry_id(),
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(ModelStreamEvent::ResponseStarted { .. }))
    ));
    drop(stream);
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if server.closed_stream_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "upstream body remained open after cancellation"
    );
    if let Err(error) = server.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
async fn provider_failure_modes_are_safe_bounded_and_retry_typed() {
    let throttled = match MockProviderSse::start(ProviderSseScenario {
        status: 429,
        frames: vec![],
        disconnect_after: None,
        error_body: None,
    })
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("throttled Provider failed: {error}"),
    };
    let provider = provider_for(throttled.endpoint(), Duration::from_secs(1));
    let result = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        result.as_slice(),
        [Err(error)]
            if error.kind == ProviderErrorKind::RateLimited
                && error.safe_code == ProviderErrorCode::RateLimited
                && error.retryable
    ));
    if let Err(error) = throttled.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }

    let exhausted = match MockProviderSse::start(ProviderSseScenario::failure(
        400,
        r#"{"error":{"type":"Arrearage","message":"must not escape the adapter"}}"#,
    ))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("exhausted Provider failed: {error}"),
    };
    let provider = provider_for(exhausted.endpoint(), Duration::from_secs(1));
    let result = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        result.as_slice(),
        [Err(error)]
            if error.kind == ProviderErrorKind::Quota
                && error.safe_code == ProviderErrorCode::QuotaExceeded
                && !error.retryable
                && error.retry_after_ms.is_none()
                && error.diagnostic_ref.is_none()
    ));
    if let Err(error) = exhausted.shutdown().await {
        panic!("exhausted Provider shutdown failed: {error}");
    }

    let hanging = match MockProviderSse::start_hanging(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_timeout"}}"#,
        ),
    ]))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("hanging Provider failed: {error}"),
    };
    let provider = provider_for(hanging.endpoint(), Duration::from_millis(50));
    let result = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        result.last(),
        Some(Err(error))
            if error.kind == ProviderErrorKind::Timeout
                && error.safe_code == ProviderErrorCode::DeadlineExceeded
                && error.retryable
    ));
    if let Err(error) = hanging.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }

    let half_closed = match MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_half_closed"}}"#,
        ),
    ]))
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("half-closed Provider failed: {error}"),
    };
    let provider = provider_for(half_closed.endpoint(), Duration::from_secs(1));
    let result = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(
        result.last(),
        Some(Err(error))
            if error.kind == ProviderErrorKind::Transport
                && error.safe_code == ProviderErrorCode::TransportFailed
                && error.retryable
    ));
    if let Err(error) = half_closed.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
#[ignore = "timing gate; run explicitly for P5 performance evidence"]
async fn canonical_adapter_ttft_overhead_is_within_five_percent() {
    let server = match MockProviderSse::start_delayed(
        ProviderSseScenario::successful(vec![
            ProviderSseFrame::new(
                "response.created",
                r#"{"type":"response.created","response":{"id":"resp_perf"}}"#,
            ),
            ProviderSseFrame::new(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"x"}"#,
            ),
            ProviderSseFrame::new(
                "response.completed",
                r#"{"type":"response.completed","response":{"id":"resp_perf","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
            ),
        ]),
        Duration::from_millis(100),
    )
    .await
    {
        Ok(value) => value,
        Err(error) => panic!("performance Provider failed: {error}"),
    };
    let provider = provider_for(server.endpoint(), Duration::from_secs(2));
    let raw_client = reqwest::Client::new();
    let mut raw_samples = Vec::new();
    let mut adapter_samples = Vec::new();
    for _ in 0..12 {
        raw_samples.push(raw_ttft(&raw_client, server.endpoint()).await);
        adapter_samples.push(adapter_ttft(&provider).await);
    }
    raw_samples.sort_unstable();
    adapter_samples.sort_unstable();
    let raw = raw_samples[raw_samples.len() / 2];
    let adapter = adapter_samples[adapter_samples.len() / 2];
    let allowed = raw.mul_f64(1.05);
    eprintln!(
        "P5 TTFT gate: raw_median_us={} adapter_median_us={} overhead_bps={}",
        raw.as_micros(),
        adapter.as_micros(),
        adapter.as_micros().saturating_mul(10_000) / raw.as_micros()
    );
    assert!(
        adapter <= allowed,
        "canonical adapter TTFT {adapter:?} exceeds raw transport {raw:?} by more than 5%"
    );
    if let Err(error) = server.shutdown().await {
        panic!("mock Provider shutdown failed: {error}");
    }
}

#[tokio::test]
async fn response_start_timeout_does_not_cap_an_active_stream_lifetime() {
    let server = MockProviderSse::start_delayed(
        ProviderSseScenario::successful(vec![
            ProviderSseFrame::new(
                "response.created",
                r#"{"type":"response.created","response":{"id":"resp_slow_body"}}"#,
            ),
            ProviderSseFrame::new(
                "response.output_text.delta",
                r#"{"type":"response.output_text.delta","output_index":0,"delta":"ok"}"#,
            ),
            ProviderSseFrame::new(
                "response.completed",
                r#"{"type":"response.completed","response":{"id":"resp_slow_body","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
            ),
        ]),
        Duration::from_millis(150),
    )
    .await
    .expect("delayed Provider");
    let provider = provider_for_timeouts(
        server.endpoint(),
        Duration::from_millis(500),
        Duration::from_millis(50),
        ApiFormat::OpenAiResponses,
    );

    let started = Instant::now();
    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok));
    assert!(started.elapsed() > Duration::from_millis(100));
    server.shutdown().await.expect("shutdown delayed Provider");
}

async fn raw_ttft(client: &reqwest::Client, endpoint: &str) -> Duration {
    let started = Instant::now();
    let response = match client
        .post(endpoint)
        .json(&serde_json::json!({}))
        .send()
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("raw baseline request failed: {error}"),
    };
    let mut frames = response.bytes_stream().eventsource();
    while let Some(frame) = frames.next().await {
        match frame {
            Ok(frame) if frame.event == "response.output_text.delta" => return started.elapsed(),
            Ok(_) => {}
            Err(error) => panic!("raw baseline SSE failed: {error}"),
        }
    }
    panic!("raw baseline closed before text");
}

async fn adapter_ttft(provider: &DirectModelProvider) -> Duration {
    let started = Instant::now();
    let mut events = provider.stream(request(
        fixture_model_provider_id(),
        fixture_model_entry_id(),
    ));
    while let Some(event) = events.next().await {
        match event {
            Ok(ModelStreamEvent::TextDelta { .. }) => return started.elapsed(),
            Ok(_) => {}
            Err(error) => panic!("adapter TTFT request failed: {error:?}"),
        }
    }
    panic!("adapter stream closed before text");
}

fn provider_for(endpoint: &str, read_timeout: Duration) -> DirectModelProvider {
    provider_for_format(endpoint, read_timeout, ApiFormat::OpenAiResponses)
}

fn provider_for_format(
    endpoint: &str,
    read_timeout: Duration,
    api_format: ApiFormat,
) -> DirectModelProvider {
    provider_for_timeouts(endpoint, read_timeout, Duration::from_secs(3), api_format)
}

fn provider_for_timeouts(
    endpoint: &str,
    read_timeout: Duration,
    response_start_timeout: Duration,
    api_format: ApiFormat,
) -> DirectModelProvider {
    let endpoint_url = match Url::parse(endpoint) {
        Ok(value) => value,
        Err(error) => panic!("mock URL failed: {error}"),
    };
    let endpoints = match StaticEndpointRegistry::new(
        [ProviderEndpoint {
            model_provider_id: fixture_model_provider_id(),
            model_entry_id: fixture_model_entry_id(),
            api_format,
            invocation_url: endpoint_url,
            connect_timeout: Duration::from_millis(20),
            read_timeout,
            total_timeout: response_start_timeout,
            anthropic_version: None,
            aws_region: None,
            origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
        }],
        true,
    ) {
        Ok(value) => value,
        Err(error) => panic!("endpoint registry failed: {error}"),
    };
    match DirectModelProvider::new(
        Arc::new(endpoints),
        Arc::new(FixtureCredentials),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    ) {
        Ok(value) => value,
        Err(error) => panic!("direct Provider failed: {error:?}"),
    }
}

fn request(
    model_provider_id: ModelProviderId,
    model_entry_id: ModelEntryId,
) -> CanonicalModelRequest {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let attempt = AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id,
        attempt_number: attempt_number(1),
        worker_id: worker_id("fixture-worker"),
        lease_id: LeaseId::generate(),
        fencing_token: fencing_token(1),
        lease_expires_at: UtcTimestamp::from_unix_millis(2_000),
    };
    let run = RunContext {
        tenant_id,
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        activity_kind: ActivityKind::Chat,
        run_id,
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: ParticipantId::generate(),
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: home_epoch(1),
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    };
    CanonicalModelRequest {
        invocation: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: run,
            attempt_context: attempt,
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(2_000) },
        },
        route: ModelRoute {
            route_ref: versioned("model.route.openai@1"),
            resolved_model_key: "gpt-fixture".into(),
            model_provider_id,
            model_entry_id,
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: versioned("adapter.openai@1"),
            protocol_version: versioned("protocol.openai.responses@1"),
            context_window: non_zero(32_768),
            max_output_tokens: non_zero(4_096),
            capabilities: BTreeSet::new(),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:pricing.fixture@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
        credential_grant_ref: credential_grant("grant_fixture"),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![model_text("You are Musteragent.")],
        input: vec![CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(model_text("Hello"))],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        }],
        tools: Vec::new(),
        response_schema: None,
        reasoning_policy: ReasoningPolicy::Summary,
        cache_policy: CachePolicy::ProviderDefault,
        stream_budget:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_stream,
        max_output_tokens: non_zero(256),
        temperature: Some(0.2),
    }
}

fn compression_request(content_ref: ContentRef) -> CanonicalModelRequest {
    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route_role = musteragent_runtime_model::ModelInvocationRouteRole::Auxiliary(
        AuxiliaryModelRole::Compression,
    );
    request.credential_grant_ref = credential_grant("grant_compression_preflight");
    request.reasoning_policy = ReasoningPolicy::Disabled;
    request.cache_policy = CachePolicy::Disabled;
    request.input[0]
        .content
        .push(CanonicalContentPart::Image(content_ref));
    request
}

fn tool_definition() -> CanonicalToolDefinition {
    let input_schema = JsonValue::Object(BTreeMap::from([(
        "type".to_owned(),
        JsonValue::String("object".to_owned()),
    )]));
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(versioned("fixture.compression-tool@1")),
        model_name: WireName::new("compression_tool").expect("wire name"),
        name: WireName::new("compression_tool").expect("wire name"),
        description: model_text("must never reach Compression"),
        input_schema_ref: SchemaRef {
            schema_key: versioned("schema.compression-tool@1"),
            schema_hash: Sha256::digest(b"compression-tool-schema"),
        },
        input_schema,
        strict: true,
    }
}

fn fixture_model_provider_id() -> ModelProviderId {
    "019fa6de-38fd-7000-8000-000000000301"
        .parse()
        .expect("model provider")
}

fn fixture_model_entry_id() -> ModelEntryId {
    "019fa6de-38fd-7000-8000-000000000302"
        .parse()
        .expect("model entry")
}

fn versioned(value: &str) -> VersionedKey {
    match VersionedKey::new(value) {
        Ok(value) => value,
        Err(error) => panic!("VersionedKey fixture failed: {error}"),
    }
}

fn non_zero(value: u64) -> NonZeroU64 {
    match NonZeroU64::new(value) {
        Some(value) => value,
        None => panic!("non-zero fixture failed"),
    }
}

fn model_text(value: &str) -> ModelText {
    match ModelText::new(value) {
        Ok(value) => value,
        Err(error) => panic!("ModelText fixture failed: {error}"),
    }
}

fn credential_grant(value: &str) -> CredentialGrantRef {
    match CredentialGrantRef::new(value) {
        Ok(value) => value,
        Err(error) => panic!("CredentialGrant fixture failed: {error}"),
    }
}

fn attempt_number(value: u32) -> AttemptNumber {
    match AttemptNumber::new(value) {
        Ok(value) => value,
        Err(error) => panic!("AttemptNumber fixture failed: {error}"),
    }
}

fn fencing_token(value: u64) -> FencingToken {
    match FencingToken::new(value) {
        Ok(value) => value,
        Err(error) => panic!("FencingToken fixture failed: {error}"),
    }
}

fn home_epoch(value: u64) -> HomeEpoch {
    match HomeEpoch::new(value) {
        Ok(value) => value,
        Err(error) => panic!("HomeEpoch fixture failed: {error}"),
    }
}

fn worker_id(value: &str) -> WorkerId {
    match WorkerId::new(value) {
        Ok(value) => value,
        Err(error) => panic!("WorkerId fixture failed: {error}"),
    }
}

#[tokio::test]
async fn comment_heartbeats_refresh_transport_activity_without_becoming_model_events() {
    let server = MockProviderSse::start_delayed_with_heartbeats(
        ProviderSseScenario::successful(vec![ProviderSseFrame::new(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_heartbeat","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
        )]),
        Duration::from_millis(500), Duration::from_millis(30),
    ).await.expect("heartbeat Provider");
    let provider = provider_for_timeouts(
        server.endpoint(),
        Duration::from_millis(150),
        Duration::from_millis(50),
        ApiFormat::OpenAiResponses,
    );
    let started = Instant::now();
    let events = provider
        .stream(request(
            fixture_model_provider_id(),
            fixture_model_entry_id(),
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
    assert!(started.elapsed() >= Duration::from_millis(400));
    server
        .shutdown()
        .await
        .expect("shutdown heartbeat Provider");
}

#[tokio::test]
async fn billing_detail_is_terminal_and_not_confused_with_provider_rate_limit() {
    for (status, body, code, retryable) in [
        (
            402,
            r#"{"id":"Payment Required","message":"This account has been blocked"}"#,
            ProviderErrorCode::PaymentRequired,
            false,
        ),
        (
            402,
            "Payment Required",
            ProviderErrorCode::PaymentRequired,
            false,
        ),
        (
            400,
            r#"{"message":"Payment Required"}"#,
            ProviderErrorCode::InvalidRequest,
            false,
        ),
        (
            402,
            r#"{"detail":{"error_code":"BILLING_CREDITS_EXHAUSTED","message":"private"}}"#,
            ProviderErrorCode::CreditsExhausted,
            false,
        ),
        (
            402,
            r#"{"detail":{"error_code":"BILLING_MODEL_NOT_ENTITLED"}}"#,
            ProviderErrorCode::ModelNotEntitled,
            false,
        ),
        (
            402,
            r#"{"detail":{"error_code":"BILLING_MODEL_ROUTE_UNAVAILABLE"}}"#,
            ProviderErrorCode::RouteConfigurationInvalid,
            false,
        ),
        (
            503,
            r#"{"detail":{"error_code":"BILLING_UNAVAILABLE"}}"#,
            ProviderErrorCode::ProviderServer,
            true,
        ),
        (
            429,
            r#"{"error":{"code":"insufficient_quota"}}"#,
            ProviderErrorCode::QuotaExceeded,
            false,
        ),
        (
            429,
            r#"{"error":{"code":"rate_limit_exceeded"}}"#,
            ProviderErrorCode::RateLimited,
            true,
        ),
    ] {
        let server = MockProviderSse::start(ProviderSseScenario::failure(status, body))
            .await
            .expect("server");
        let provider = provider_for(server.endpoint(), Duration::from_secs(1));
        let result = provider
            .stream(request(
                fixture_model_provider_id(),
                fixture_model_entry_id(),
            ))
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(result.as_slice(), [Err(error)] if error.safe_code == code && error.retryable == retryable),
            "{result:?}"
        );
        server.shutdown().await.expect("shutdown");
    }
}

#[path = "direct_https/host_headers.rs"]
mod host_headers;

#[path = "direct_https/existing_invocation.rs"]
mod existing_invocation;
