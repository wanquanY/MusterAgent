use super::*;
use musteragent_runtime_model::{CanonicalRuntimeEvent, ModelRouteRef, ReasoningReplay};

const STORED_REASONING: &[u8] = b"stored provider reasoning";

/// Serves the stored reasoning content for exactly one reference.
struct StoredReasoning(ContentRef);

impl ContentResolver for StoredReasoning {
    fn resolve<'a>(
        &'a self,
        _tenant_id: TenantId,
        content_ref: &'a ContentRef,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>
    {
        Box::pin(async move {
            if content_ref != &self.0 {
                return Err(ContentResolverError::NotFound);
            }
            Ok(ResolvedContent::Inline {
                media_type: content_ref.media_type.clone(),
                bytes: SecretSlice::new(STORED_REASONING.to_vec().into_boxed_slice()),
            })
        })
    }
}

/// An output-limit continuation replays a reasoning-only assistant message.
/// A route that requires reasoning echo must receive the stored reasoning,
/// not fail locally because the content was never resolved.
#[tokio::test]
async fn reasoning_only_continuation_dispatches_with_stored_reasoning() {
    let server = MockProviderSse::start(ProviderSseScenario::successful(vec![
        ProviderSseFrame::new(
            "message",
            r#"{"id":"chatcmpl_continued","choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}"#,
        ),
    ]))
    .await
    .expect("mock Provider");
    let reasoning_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(STORED_REASONING.len()).expect("length"),
        sha256: Sha256::digest(STORED_REASONING),
    };
    let endpoints = StaticEndpointRegistry::new(
        [ProviderEndpoint {
            model_provider_id: fixture_model_provider_id(),
            model_entry_id: fixture_model_entry_id(),
            api_format: ApiFormat::OpenAiChatCompletions,
            invocation_url: Url::parse(server.endpoint()).expect("mock URL"),
            connect_timeout: Duration::from_millis(20),
            read_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(3),
            anthropic_version: None,
            aws_region: None,
            origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
        }],
        true,
    )
    .expect("endpoint registry");
    let provider = DirectModelProvider::new(
        Arc::new(endpoints),
        Arc::new(FixtureCredentials),
        Arc::new(StoredReasoning(reasoning_ref.clone())),
        ProviderAdapterConfig::default(),
    )
    .expect("direct Provider");

    let mut request = request(fixture_model_provider_id(), fixture_model_entry_id());
    request.route.api_format = ApiFormat::OpenAiChatCompletions;
    request
        .route
        .capabilities
        .insert(DottedKey::new("reasoning_echo.required").expect("capability"));
    request.input.push(CanonicalMessage {
        role: CanonicalRole::Assistant,
        content: Vec::new(),
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: Some(ReasoningReplay {
            content_ref: reasoning_ref,
            source_route_ref: ModelRouteRef::from_key(request.route.route_ref.clone()),
            continuation_ref: None,
        }),
    });
    request.runtime_events = vec![CanonicalRuntimeEvent::OutputLimitContinuation {
        source_invocation_id: InvocationId::generate(),
    }];

    let results = provider.stream(request).collect::<Vec<_>>().await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let captured = server.captured_requests().await;
    assert_eq!(captured.len(), 1, "the continuation must be dispatched");
    let body: serde_json::Value =
        serde_json::from_slice(&captured[0]).expect("captured Provider request JSON");
    let assistant = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|message| message["role"] == "assistant")
        .expect("assistant message");
    assert_eq!(assistant["reasoning_content"], "stored provider reasoning");
    server.shutdown().await.expect("shutdown mock Provider");
}
