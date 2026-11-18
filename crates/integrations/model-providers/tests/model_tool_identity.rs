//! Regression for the real 66-character canvas_get_state failure.
mod support;

use futures_util::StreamExt as _;
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_discovery::ModelToolNames;
use musteragent_identity::{ToolCallId, VersionedKey};
use musteragent_model::ApiFormat;
use musteragent_model_providers::{ProviderNormalizer, ResolvedContents, map_request};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall,
    CanonicalToolDefinition, JsonValue, ModelProvider, ModelRouteRef, ModelStreamEvent,
};
use musteragent_testkit::{MockProviderWire, ProviderWireScenario};
use musteragent_tool::{ToolKey, WireName};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const FORMATS: [ApiFormat; 7] = [
    ApiFormat::OpenAiResponses,
    ApiFormat::OpenAiChatCompletions,
    ApiFormat::AnthropicMessages,
    ApiFormat::GoogleGenerateContent,
    ApiFormat::GoogleInteractions,
    ApiFormat::BedrockConverse,
    ApiFormat::BedrockInvokeModel,
];
const DIGEST: &str = "442d989c8c7b92b828cbfa3bde7ffcbd089c93519e218aff748198475a2f5727";

fn definition(raw: &str) -> CanonicalToolDefinition {
    let key = ToolKey::from_versioned_key(
        VersionedKey::new(format!("mcp.s_test.t_{raw}.h_{DIGEST}@1")).expect("key"),
    );
    let name = WireName::new(format!("mcp_test_{raw}_{}", &DIGEST[..40])).expect("wire");
    let names = ModelToolNames::new([(&key, &name)]).expect("name catalog");
    CanonicalToolDefinition {
        model_name: names.name(&key).expect("model name").clone(),
        key,
        name,
        description: text("Read or update the map scene"),
        input_schema_ref: SchemaRef {
            schema_key: VersionedKey::new("schema.canvas@1").expect("schema"),
            schema_hash: Sha256::digest(b"canvas"),
        },
        input_schema: JsonValue::Object(BTreeMap::from([
            ("type".into(), JsonValue::String("object".into())),
            ("properties".into(), JsonValue::Object(BTreeMap::new())),
            ("additionalProperties".into(), JsonValue::Bool(false)),
        ])),
        strict: true,
    }
}

fn start(format: ApiFormat, name: &str) -> (&'static str, Value) {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => (
            "response.output_item.added",
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_canvas","call_id":"call_canvas","name":name,"arguments":""}}),
        ),
        ApiFormat::OpenAiChatCompletions => (
            "message",
            json!({"id":"response_canvas","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_canvas","function":{"name":name,"arguments":"{}"}}]},"finish_reason":null}]}),
        ),
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => (
            "content_block_start",
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_canvas","name":name,"input":{}}}),
        ),
        ApiFormat::GoogleGenerateContent => (
            "message",
            json!({"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":name,"args":{}}}]}}]}),
        ),
        ApiFormat::GoogleInteractions => (
            "step.start",
            json!({"event_type":"step.start","index":0,"step":{"type":"function_call","id":"call_canvas","name":name,"arguments":{}}}),
        ),
        ApiFormat::BedrockConverse => (
            "contentBlockStart",
            json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call_canvas","name":name}}}),
        ),
    }
}

fn normalizer(format: ApiFormat, tools: &[CanonicalToolDefinition]) -> ProviderNormalizer {
    ProviderNormalizer::new(
        format,
        ModelRouteRef::from_key(VersionedKey::new("model.route.cap016@1").expect("route")),
        support::REQUESTED_MODEL,
        "identity-regression",
    )
    .expect("normalizer")
    .with_tool_definitions(tools)
    .expect("precise reverse map")
}

#[test]
fn every_protocol_roundtrips_the_real_long_canvas_names() {
    for raw in ["routes_get", "canvas_get_state", "canvas_apply_scene"] {
        let tool = definition(raw);
        for format in FORMATS {
            let mut request = support::request(format);
            request.tools = vec![tool.clone()];
            let body = map_request(&request, &ResolvedContents::default())
                .expect("map")
                .body;
            let encoded = body.to_string();
            assert!(
                encoded.contains(tool.model_name.as_str()),
                "{format:?}: {encoded}"
            );
            assert!(!encoded.contains(DIGEST));
            assert!(!encoded.contains(tool.name.as_str()));
            let (event, payload) = start(format, tool.model_name.as_str());
            let output = normalizer(format, &request.tools)
                .observe(Some(event), payload)
                .expect("roundtrip");
            assert!(output.iter().any(|event| matches!(event, ModelStreamEvent::ToolCallStarted { name, .. } if name == &tool.name)), "{format:?}: {output:?}");
        }
    }
}

#[test]
fn history_and_results_use_the_same_model_interface() {
    for format in FORMATS {
        let tool = definition("canvas_get_state");
        let mut request = support::request(format);
        request.tools = vec![tool.clone()];
        let call_id = ToolCallId::new("call_canvas").expect("call id");
        request.input.extend([
            CanonicalMessage {
                role: CanonicalRole::Assistant,
                content: vec![],
                tool_calls: vec![CanonicalToolCall {
                    call_id: call_id.clone(),
                    name: tool.name.clone(),
                    arguments: JsonValue::Object(BTreeMap::new()),
                    provider_item_ref: None,
                    provider_replay_ref: None,
                }],
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            CanonicalMessage {
                role: CanonicalRole::Tool,
                content: vec![CanonicalContentPart::Text(text("{\"revision\":1}"))],
                tool_calls: vec![],
                tool_call_id: Some(call_id),
                tool_name: Some(tool.name.clone()),
                reasoning: None,
            },
        ]);
        if format == ApiFormat::GoogleInteractions {
            request.route.provider_state_policy =
                musteragent_model::ProviderStatePolicy::OpaqueContinuation;
            request.continuation_ref =
                Some(musteragent_runtime_model::ContinuationRef::from_opaque(
                    musteragent_runtime_contract::OpaqueExternalRef::new(
                        "previous_canvas_response",
                    )
                    .expect("opaque reference"),
                ));
            request.input = request.input.split_off(2);
        }
        let encoded = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?}: {error}"))
            .body
            .to_string();
        assert!(!encoded.contains(tool.name.as_str()), "{format:?}");
        assert!(
            encoded.matches(tool.model_name.as_str()).count() >= 2,
            "{format:?}"
        );
    }
}

#[test]
fn unknown_names_remain_rejected_actions_and_canonical_names_cannot_bypass_interface() {
    for format in FORMATS {
        let tool = definition("canvas_get_state");
        let (event, payload) = start(format, tool.name.as_str());
        let rejected = normalizer(format, std::slice::from_ref(&tool))
            .observe(Some(event), payload)
            .expect("wrong name is a correctable model action");
        assert!(rejected.iter().any(|event| matches!(event, ModelStreamEvent::ToolCallUnexposed {name} if name == &tool.name)));
        assert!(
            !rejected
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::ToolCallStarted { .. }))
        );
        let (event, payload) = start(format, "mcp_unapproved_tool");
        let output = normalizer(format, &[tool])
            .observe(Some(event), payload)
            .expect("unknown action is classified by Loop");
        assert!(output.iter().any(|event| matches!(event, ModelStreamEvent::ToolCallStarted {name, ..} if name.as_str() == "mcp_unapproved_tool")));
    }
}

#[tokio::test]
async fn direct_responses_adapter_applies_reverse_mapping_to_real_http_stream() {
    let tool = definition("canvas_get_state");
    let (event, data) = start(ApiFormat::OpenAiResponses, tool.model_name.as_str());
    let chunks = vec![format!("event: {event}\ndata: {data}\n\n").into_bytes(),
        b"event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"arguments\":\"{}\"}\n\n".to_vec(),
        b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"response_canvas\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n".to_vec()];
    let server = MockProviderWire::start(ProviderWireScenario::successful(
        "text/event-stream",
        chunks,
    ))
    .await
    .expect("mock server");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let mut request = support::request(ApiFormat::OpenAiResponses);
    request.tools = vec![tool.clone()];
    let events = provider.stream(request).collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(events.iter().any(|event| matches!(event, Ok(ModelStreamEvent::ToolCallStarted {name, ..}) if name == &tool.name)));
    assert!(
        matches!(
            events.last(),
            Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
        ),
        "{events:?}"
    );
    server.shutdown().await.expect("shutdown");
}

fn text(value: &str) -> musteragent_runtime_model::ModelText {
    musteragent_runtime_model::ModelText::new(value).expect("text")
}

#[test]
fn ambiguous_physical_tool_maps_are_rejected_before_dispatch() {
    let mut request = support::request(ApiFormat::OpenAiResponses);
    let first = definition("canvas_get_state");
    let mut second = definition("canvas_apply_scene");
    second.model_name = first.model_name.clone();
    request.tools = vec![first, second];
    assert!(request.validate().is_err());
    assert!(
        ProviderNormalizer::new(
            ApiFormat::OpenAiResponses,
            ModelRouteRef::from_key(VersionedKey::new("route.test@1").expect("route")),
            "model",
            "invocation"
        )
        .expect("normalizer")
        .with_tool_definitions(&request.tools)
        .is_err()
    );
}

#[tokio::test]
async fn local_name_rejection_preserves_real_provider_usage_and_terminal() {
    let tool = definition("canvas_get_state");
    let (event, data) = start(ApiFormat::OpenAiResponses, tool.name.as_str());
    let chunks = vec![format!("event: {event}\ndata: {data}\n\n").into_bytes(),
        b"event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"arguments\":\"{}\"}\n\n".to_vec(),
        b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"response_canvas\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n".to_vec()];
    let server = MockProviderWire::start(ProviderWireScenario::successful(
        "text/event-stream",
        chunks,
    ))
    .await
    .expect("server");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let mut request = support::request(ApiFormat::OpenAiResponses);
    request.tools = vec![tool.clone()];
    let events = provider.stream(request).collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(events.iter().any(|event| matches!(event, Ok(ModelStreamEvent::ToolCallUnexposed { name }) if name == &tool.name)));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::ToolCallStarted { .. })))
    );
    assert!(events.iter().any(
        |event| matches!(event, Ok(ModelStreamEvent::Usage(usage)) if usage.total_tokens == Some(2))
    ));
    assert!(
        matches!(
            events.last(),
            Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
        ),
        "{events:?}"
    );
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn local_name_rejection_without_provider_terminal_stays_unconfirmed() {
    let tool = definition("canvas_get_state");
    let (event, data) = start(ApiFormat::OpenAiResponses, tool.name.as_str());
    let server = MockProviderWire::start(ProviderWireScenario::successful(
        "text/event-stream",
        vec![format!("event: {event}\ndata: {data}\n\n").into_bytes()],
    ))
    .await
    .expect("server");
    let provider = support::provider(server.endpoint(), ApiFormat::OpenAiResponses);
    let mut request = support::request(ApiFormat::OpenAiResponses);
    request.tools = vec![tool];
    let events = provider.stream(request).collect::<Vec<_>>().await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::ToolCallUnexposed { .. })))
    );
    assert!(matches!(events.last(), Some(Err(_))), "{events:?}");
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::ResponseCompleted { .. })))
    );
    server.shutdown().await.expect("shutdown");
}
