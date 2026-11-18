use musteragent_identity::VersionedKey;
use musteragent_model::ApiFormat;
use musteragent_model_providers::{NormalizeError, ProviderNormalizer};
use musteragent_runtime_model::{
    JsonValue as CanonicalJsonValue, ModelRouteRef, ModelStreamEvent, ModelStreamState,
    ProviderErrorCode, ProviderErrorKind, StopReason,
};
use serde_json::Value;

struct Frame {
    event: Option<String>,
    data: Value,
}

fn fixture(source: &str) -> Vec<Frame> {
    source
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .map(|value| Frame {
            event: value
                .get("event")
                .and_then(Value::as_str)
                .map(str::to_owned),
            data: value.get("data").cloned().unwrap_or(Value::Null),
        })
        .collect()
}

fn replay(format: ApiFormat, source: &str) -> Vec<ModelStreamEvent> {
    replay_with_reasoning_disclosure(format, source, false)
}

fn replay_with_reasoning_disclosure(
    format: ApiFormat,
    source: &str,
    publish_disclosed_reasoning: bool,
) -> Vec<ModelStreamEvent> {
    let mut normalizer = match ProviderNormalizer::new(
        format,
        fixture_route(),
        "fixture-model",
        "fixture-invocation",
    ) {
        Ok(value) => value.with_disclosed_reasoning_output(publish_disclosed_reasoning),
        Err(error) => panic!("normalizer creation failed: {error}"),
    };
    let mut state = ModelStreamState::default();
    let mut output = Vec::new();
    for frame in fixture(source) {
        let events = match normalizer.observe(frame.event.as_deref(), frame.data) {
            Ok(value) => value,
            Err(error) => panic!("fixture normalization failed: {error}"),
        };
        for event in events {
            if let Err(error) = state.accept(&event) {
                panic!("canonical state rejected fixture: {error}");
            }
            output.push(event);
        }
    }
    if !state.is_terminal() {
        let events = match normalizer.finish() {
            Ok(value) => value,
            Err(error) => panic!("fixture finish failed: {error}"),
        };
        for event in events {
            if let Err(error) = state.accept(&event) {
                panic!("canonical state rejected terminal fixture: {error}");
            }
            output.push(event);
        }
    }
    assert!(state.is_terminal());
    output
}

fn first_tool_call_id(invocation_ref: &str, source: &str) -> String {
    let mut normalizer = ProviderNormalizer::new(
        ApiFormat::OpenAiChatCompletions,
        fixture_route(),
        "fixture-model",
        invocation_ref,
    )
    .expect("normalizer creation");
    fixture(source)
        .into_iter()
        .flat_map(|frame| {
            normalizer
                .observe(frame.event.as_deref(), frame.data)
                .expect("fixture normalization")
        })
        .find_map(|event| match event {
            ModelStreamEvent::ToolCallStarted { call_id, .. } => Some(call_id.as_str().to_owned()),
            _ => None,
        })
        .expect("tool call started")
}

#[test]
fn openai_chat_namespaces_reused_provider_tool_ids_by_physical_invocation() {
    let source = "{\"data\":{\"id\":\"chatcmpl_tools\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"browser_observe_0\",\"function\":{\"name\":\"browser_observe\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}}\n";
    let first = first_tool_call_id("invocation-first", source);
    let repeated = first_tool_call_id("invocation-first", source);
    let second = first_tool_call_id("invocation-second", source);

    assert_eq!(first, repeated, "normalization must be deterministic");
    assert_ne!(first, second, "ToolCallId is run-global across invocations");
    assert!(first.len() <= 40, "must remain provider-wire compatible");
    assert_ne!(first, "browser_observe_0");
}

#[test]
fn disclosed_openai_reasoning_requires_explicit_public_authorization() {
    let source = concat!(
        "{\"data\":{\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"先核对时间。\"},\"finish_reason\":null}]}}\n",
        "{\"data\":{\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"现在是北京时间。\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":8,\"total_tokens\":18}}}\n",
    );

    let private_only =
        replay_with_reasoning_disclosure(ApiFormat::OpenAiChatCompletions, source, false);
    assert!(private_only.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningDelta { delta, .. }
            if delta.as_str() == "先核对时间。")
    ));
    assert!(
        !private_only
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ReasoningSummaryDelta { .. }))
    );

    let disclosed =
        replay_with_reasoning_disclosure(ApiFormat::OpenAiChatCompletions, source, true);
    assert!(disclosed.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningSummaryDelta { delta, .. }
            if delta.as_str() == "先核对时间。")
    ));
}

#[test]
fn all_production_api_families_replay_to_a_valid_terminal_stream() {
    let cases = [
        (
            ApiFormat::OpenAiChatCompletions,
            include_str!("fixtures/openai_chat.jsonl"),
        ),
        (
            ApiFormat::OpenAiResponses,
            include_str!("fixtures/openai_responses.jsonl"),
        ),
        (
            ApiFormat::AnthropicMessages,
            include_str!("fixtures/anthropic.jsonl"),
        ),
        (
            ApiFormat::GoogleGenerateContent,
            include_str!("fixtures/google_generate.jsonl"),
        ),
        (
            ApiFormat::GoogleInteractions,
            include_str!("fixtures/google_interactions.jsonl"),
        ),
        (
            ApiFormat::BedrockConverse,
            include_str!("fixtures/bedrock_converse.jsonl"),
        ),
        (
            ApiFormat::BedrockInvokeModel,
            include_str!("fixtures/bedrock_invoke.jsonl"),
        ),
    ];
    for (format, source) in cases {
        let events = replay(format, source);
        assert!(
            matches!(
                events.last(),
                Some(ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop
                })
            ),
            "{format:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::Usage(_))),
            "{format:?}"
        );
    }
}

#[test]
fn streamed_gateway_bad_request_is_non_retryable_for_every_agent_loop() {
    let events = replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad_request\"}}}\n",
            "{\"event\":\"error\",\"data\":{\"type\":\"error\",\"error\":{\"type\":\"upstream_request_failed\",\"message\":\"provider detail must remain private\",\"code\":\"UPSTREAM_REQUEST_FAILED\",\"upstream_status\":400}}}\n",
        ),
    );

    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseError(error))
            if error.kind == ProviderErrorKind::InvalidRequest
                && error.safe_code == ProviderErrorCode::InvalidRequest
                && !error.retryable
    ));
}

#[test]
fn streamed_gateway_transient_errors_preserve_retry_semantics() {
    let rate_limited = replay(
        ApiFormat::OpenAiResponses,
        "{\"event\":\"error\",\"data\":{\"type\":\"error\",\"error\":{\"code\":\"UPSTREAM_RATE_LIMITED\",\"upstream_status\":429}}}\n",
    );
    assert!(matches!(
        rate_limited.last(),
        Some(ModelStreamEvent::ResponseError(error))
            if error.kind == ProviderErrorKind::RateLimited
                && error.safe_code == ProviderErrorCode::RateLimited
                && error.retryable
    ));

    let unavailable = replay(
        ApiFormat::OpenAiResponses,
        "{\"event\":\"error\",\"data\":{\"type\":\"error\",\"error\":{\"code\":\"UPSTREAM_UNAVAILABLE\",\"upstream_status\":503}}}\n",
    );
    assert!(matches!(
        unavailable.last(),
        Some(ModelStreamEvent::ResponseError(error))
            if error.kind == ProviderErrorKind::Server
                && error.safe_code == ProviderErrorCode::ProviderServer
                && error.retryable
    ));
}

#[test]
fn response_model_identity_uses_provider_value_with_explicit_requested_fallback() {
    let provider_reported = replay(
        ApiFormat::OpenAiChatCompletions,
        concat!(
            "{\"data\":{\"id\":\"\",\"model\":\"\",\"choices\":[]}}\n",
            "{\"data\":{\"id\":\"chatcmpl_model_identity\",\"model\":\"gpt-provider-reported\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}}\n",
        ),
    );
    assert!(matches!(
        provider_reported.first(),
        Some(ModelStreamEvent::ResponseStarted {
            response_model_key,
            ..
        }) if response_model_key.as_str() == "gpt-provider-reported"
    ));

    let requested_fallback = replay(
        ApiFormat::OpenAiChatCompletions,
        "{\"data\":{\"id\":\"chatcmpl_model_fallback\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}}\n",
    );
    assert!(matches!(
        requested_fallback.first(),
        Some(ModelStreamEvent::ResponseStarted {
            response_model_key,
            ..
        }) if response_model_key.as_str() == "fixture-model"
    ));
}

#[test]
fn anthropic_preserves_one_hour_cache_creation_as_a_distinct_dimension() {
    let events = replay(
        ApiFormat::AnthropicMessages,
        concat!(
            "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_cache_dimensions\",\"model\":\"claude-provider-reported\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0,\"cache_creation_input_tokens\":10,\"cache_creation\":{\"ephemeral_1h_input_tokens\":7}}}}}\n",
            "{\"event\":\"message_delta\",\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}}\n",
            "{\"event\":\"message_stop\",\"data\":{\"type\":\"message_stop\"}}\n",
        ),
    );
    let usage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            ModelStreamEvent::Usage(usage) => Some(usage),
            _ => None,
        })
        .expect("terminal usage");
    assert_eq!(usage.provider_cache_write_tokens, 10);
    assert_eq!(usage.provider_cache_write_1h_tokens, 7);
}

#[test]
fn unknown_or_malformed_provider_frames_fail_closed() {
    let mut responses = match ProviderNormalizer::new(
        ApiFormat::OpenAiResponses,
        fixture_route(),
        "fixture-model",
        "fixture-unknown",
    ) {
        Ok(value) => value,
        Err(error) => panic!("normalizer creation failed: {error}"),
    };
    assert_eq!(
        responses.observe(
            Some("response.future_unrecognized"),
            serde_json::json!({"type": "response.future_unrecognized"})
        ),
        Err(NormalizeError::UnknownEventType)
    );

    let mut anthropic = match ProviderNormalizer::new(
        ApiFormat::AnthropicMessages,
        fixture_route(),
        "fixture-model",
        "fixture-malformed",
    ) {
        Ok(value) => value,
        Err(error) => panic!("normalizer creation failed: {error}"),
    };
    assert!(matches!(
        anthropic.observe(
            None,
            serde_json::json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "unknown_delta"}
            })
        ),
        Err(NormalizeError::UnknownDeltaType)
    ));
}

fn fixture_route() -> ModelRouteRef {
    ModelRouteRef::from_key(VersionedKey::new("model.route.fixture@1").expect("fixture route"))
}

#[test]
fn openai_chat_accepts_metadata_and_null_content_while_streaming_a_tool_call() {
    let source = concat!(
        "{\"data\":{\"choices\":[],\"id\":\"\",\"model\":\"\"}}\n",
        "{\"data\":{\"id\":\"chatcmpl_live_shape\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_live\",\"function\":{\"name\":\"current_time\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}}\n",
        "{\"data\":{\"id\":\"chatcmpl_live_shape\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}}\n",
        "{\"data\":{\"id\":\"chatcmpl_live_shape\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}}\n",
    );
    let events = replay(ApiFormat::OpenAiChatCompletions, source);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
    );
}

#[test]
fn openai_responses_infers_tool_stop_from_completed_function_calls() {
    let source = concat!(
        "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_tool\"}}}\n",
        "{\"event\":\"response.output_item.added\",\"data\":{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_tool\",\"call_id\":\"call_tool\",\"name\":\"get_current_time\"}}}\n",
        "{\"event\":\"response.function_call_arguments.delta\",\"data\":{\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}}\n",
        "{\"event\":\"response.function_call_arguments.done\",\"data\":{\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"arguments\":\"{}\"}}\n",
        "{\"event\":\"response.output_item.done\",\"data\":{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_tool\",\"call_id\":\"call_tool\",\"name\":\"get_current_time\",\"arguments\":\"{}\"}}}\n",
        "{\"event\":\"response.completed\",\"data\":{\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tool\",\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"id\":\"fc_tool\",\"call_id\":\"call_tool\",\"name\":\"get_current_time\",\"arguments\":\"{}\"}],\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"total_tokens\":12}}}}\n",
    );

    let events = replay(ApiFormat::OpenAiResponses, source);

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
}

#[test]
fn openai_responses_accepts_explicit_incomplete_terminal_events() {
    let source = concat!(
        "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_incomplete\"}}}\n",
        "{\"event\":\"response.output_text.delta\",\"data\":{\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}}\n",
        "{\"event\":\"response.incomplete\",\"data\":{\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_incomplete\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"total_tokens\":12}}}}\n",
    );
    let events = replay(ApiFormat::OpenAiResponses, source);
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::TokenLimit
        })
    ));
}

#[test]
fn openai_responses_distinguishes_context_overflow_from_output_exhaustion() {
    let source = concat!(
        "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_context_overflow\"}}}\n",
        "{\"event\":\"response.incomplete\",\"data\":{\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_context_overflow\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"model_context_window_exceeded\"}}}}\n",
    );

    let events = replay(ApiFormat::OpenAiResponses, source);

    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ContextWindowExceeded
        })
    ));
}

#[test]
fn length_truncated_tool_json_is_closed_without_parsing_or_execution_arguments() {
    let source = concat!(
        "{\"data\":{\"id\":\"chatcmpl_truncated\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_truncated\",\"function\":{\"name\":\"dangerous_write\",\"arguments\":\"{\\\"path\\\":\"}}]},\"finish_reason\":null}]}}\n",
        "{\"data\":{\"id\":\"chatcmpl_truncated\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}}\n",
    );

    let events = replay(ApiFormat::OpenAiChatCompletions, source);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            ModelStreamEvent::ToolCallRejected {
                kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind::GenerationTokenLimit,
                diagnostic: Some(_),
                ..
            }
        )
    }));
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::TokenLimit
        })
    ));
}

#[test]
fn responses_item_done_waits_for_terminal_truncation_before_parsing_arguments() {
    for (terminal, should_reject) in [("response.incomplete", false), ("response.completed", true)]
    {
        let mut normalizer = ProviderNormalizer::new(
            ApiFormat::OpenAiResponses,
            fixture_route(),
            "fixture-model",
            "item-truncation",
        )
        .expect("normalizer");
        normalizer.observe(Some("response.output_item.added"), serde_json::json!({
            "output_index": 0, "item": { "type": "function_call", "id": "fc_1",
                "call_id": "call_1", "name": "artifact_presentation_build", "arguments": "" }
        })).expect("start");
        let item = normalizer.observe(Some("response.output_item.done"), serde_json::json!({
            "output_index": 0, "item": { "type": "function_call", "arguments": "{\"pages\":[" }
        })).expect("item completion alone cannot determine truncation");
        assert!(
            !item
                .iter()
                .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
        );
        let result = normalizer.observe(Some(terminal), if terminal == "response.incomplete" {
            serde_json::json!({"response":{"incomplete_details":{"reason":"max_output_tokens"}}})
        } else { serde_json::json!({}) });
        if should_reject {
            let events = result.expect("identified invalid arguments are correctable");
            assert!(events.iter().any(|event| matches!(event,
                ModelStreamEvent::ToolCallRejected { kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind::MalformedJson, .. })));
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
            );
        } else {
            let events = result.expect("truncated response");
            assert!(events.iter().any(|event| matches!(event,
                ModelStreamEvent::ToolCallRejected { kind: musteragent_runtime_contract::ToolArgumentValidationIssueKind::GenerationTokenLimit, diagnostic: Some(_), .. })));
            assert!(matches!(
                events.last(),
                Some(ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::TokenLimit
                })
            ));
        }
    }
}

#[test]
fn google_generate_parallel_function_calls_are_unique_and_stop_for_tools() {
    let source = concat!(
        "{\"data\":{\"responseId\":\"google_tools\",\"candidates\":[{\"content\":{\"parts\":[",
        "{\"functionCall\":{\"id\":\"call_a\",\"name\":\"lookup\",\"args\":{\"value\":\"a\"}},\"thoughtSignature\":\"c2ln\"},",
        "{\"functionCall\":{\"id\":\"call_b\",\"name\":\"lookup\",\"args\":{\"value\":\"b\"}}}",
        "]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":2,\"totalTokenCount\":12}}}\n",
    );
    let events = replay(ApiFormat::GoogleGenerateContent, source);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. }))
            .count(),
        2
    );
    assert!(events.iter().any(|event| {
        matches!(
            event,
            ModelStreamEvent::ToolCallStarted {
                call_id,
                provider_replay: Some(replay),
                ..
            } if call_id.as_str() == "call_a"
                && replay.source_model_key() == "fixture-model"
                && replay.google_signature() == Some("c2ln")
        )
    }));
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls
        })
    ));
}

#[test]
fn google_generate_thought_text_is_a_public_summary_not_raw_reasoning() {
    let source = concat!(
        "{\"data\":{\"responseId\":\"google_summary\",\"candidates\":[{\"content\":{\"parts\":[",
        "{\"thought\":true,\"text\":\"checked the available context\"},",
        "{\"text\":\"answer\"}",
        "]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":2,\"totalTokenCount\":12}}}\n",
    );
    let events = replay(ApiFormat::GoogleGenerateContent, source);
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningSummaryDelta { delta, .. }
            if delta.as_str() == "checked the available context")
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ReasoningDelta { .. }))
    );
}

#[test]
fn google_interactions_accepts_official_tool_pause_and_resume_event_shapes() {
    let tool_stream = concat!(
        "{\"event\":\"interaction.created\",\"data\":{\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"int_tool\",\"status\":\"in_progress\"}}}\n",
        "{\"event\":\"step.start\",\"data\":{\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"thought\",\"signature\":\"opaque\",\"summary\":[{\"type\":\"text\",\"text\":\"use tool\"}]}}}\n",
        "{\"event\":\"step.delta\",\"data\":{\"event_type\":\"step.delta\",\"index\":0,\"delta\":{\"type\":\"thought_signature\",\"signature\":\"opaque-more\"}}}\n",
        "{\"event\":\"step.delta\",\"data\":{\"event_type\":\"step.delta\",\"index\":0,\"delta\":{\"type\":\"thought_summary\",\"content\":{\"type\":\"text\",\"text\":\" after checking\"}}}}\n",
        "{\"event\":\"step.stop\",\"data\":{\"event_type\":\"step.stop\",\"index\":0}}\n",
        "{\"event\":\"step.start\",\"data\":{\"event_type\":\"step.start\",\"index\":1,\"step\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"lookup\",\"arguments\":{}}}}\n",
        "{\"event\":\"step.delta\",\"data\":{\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"arguments_delta\",\"arguments\":\"{\\\"value\\\":\"}}}\n",
        "{\"event\":\"step.delta\",\"data\":{\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"arguments_delta\",\"arguments\":\"\\\"a\\\"}\"}}}\n",
        "{\"event\":\"step.stop\",\"data\":{\"event_type\":\"step.stop\",\"index\":1}}\n",
        "{\"event\":\"interaction.status_update\",\"data\":{\"event_type\":\"interaction.status_update\",\"interaction_id\":\"int_tool\",\"status\":\"requires_action\",\"metadata\":{\"total_usage\":{\"total_input_tokens\":10,\"total_output_tokens\":2,\"total_thought_tokens\":1,\"total_tokens\":13}}}}\n",
        "{\"event\":\"interaction.completed\",\"data\":{\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_tool\",\"status\":\"requires_action\"}}}\n",
    );
    let tool_events = replay(ApiFormat::GoogleInteractions, tool_stream);
    assert!(matches!(
        tool_events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls
        })
    ));
    assert!(tool_events.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningSummaryDelta { delta, .. }
            if delta.as_str() == "use tool")
    ));
    assert!(
        !tool_events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ReasoningDelta { .. }))
    );
    assert_eq!(
        tool_events
            .iter()
            .filter(|event| matches!(event, ModelStreamEvent::Usage(_)))
            .count(),
        1
    );

    let resumed_stream = concat!(
        "{\"event\":\"interaction.created\",\"data\":{\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"int_resume\",\"status\":\"in_progress\"}}}\n",
        "{\"event\":\"step.start\",\"data\":{\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"function_result\",\"call_id\":\"fc_1\",\"name\":\"lookup\",\"result\":[{\"type\":\"text\",\"text\":\"ok\"}]}}}\n",
        "{\"event\":\"step.stop\",\"data\":{\"event_type\":\"step.stop\",\"index\":0}}\n",
        "{\"event\":\"step.start\",\"data\":{\"event_type\":\"step.start\",\"index\":1,\"step\":{\"type\":\"model_output\"}}}\n",
        "{\"event\":\"step.delta\",\"data\":{\"event_type\":\"step.delta\",\"index\":1,\"delta\":{\"type\":\"text\",\"text\":\"done\"}}}\n",
        "{\"event\":\"step.stop\",\"data\":{\"event_type\":\"step.stop\",\"index\":1}}\n",
        "{\"event\":\"interaction.completed\",\"data\":{\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_resume\",\"status\":\"completed\"},\"metadata\":{\"total_usage\":{\"total_input_tokens\":12,\"total_output_tokens\":3,\"total_tokens\":15}}}}\n",
    );
    let resumed_events = replay(ApiFormat::GoogleInteractions, resumed_stream);
    assert!(matches!(
        resumed_events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop
        })
    ));
    assert!(resumed_events.iter().any(
        |event| matches!(event, ModelStreamEvent::TextDelta { delta, .. } if delta.as_str() == "done")
    ));
}

#[test]
fn google_interactions_accepts_complete_function_arguments_on_step_start() {
    let source = concat!(
        "{\"event\":\"interaction.created\",\"data\":{\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"int_snapshot\",\"status\":\"in_progress\"}}}\n",
        "{\"event\":\"step.start\",\"data\":{\"event_type\":\"step.start\",\"index\":0,\"step\":{\"type\":\"function_call\",\"id\":\"fc_snapshot\",\"name\":\"lookup\",\"arguments\":{\"value\":\"a\"}}}}\n",
        "{\"event\":\"step.stop\",\"data\":{\"event_type\":\"step.stop\",\"index\":0}}\n",
        "{\"event\":\"interaction.completed\",\"data\":{\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_snapshot\",\"status\":\"requires_action\"},\"metadata\":{\"total_usage\":{\"total_input_tokens\":5,\"total_output_tokens\":1,\"total_tokens\":6}}}}\n",
    );
    let events = replay(ApiFormat::GoogleInteractions, source);
    let arguments = events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ToolCallCompleted { arguments, .. } => Some(arguments),
            _ => None,
        })
        .expect("function call should complete");
    assert_eq!(
        arguments
            .as_object()
            .expect("tool arguments should be an object")
            .get("value"),
        Some(&CanonicalJsonValue::String("a".to_owned()))
    );
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls
        })
    ));
}

#[test]
fn bedrock_converse_reads_nested_reasoning_content() {
    let source = concat!(
        "{\"event\":\"messageStart\",\"data\":{\"role\":\"assistant\"}}\n",
        "{\"event\":\"contentBlockDelta\",\"data\":{\"contentBlockIndex\":0,\"delta\":{\"reasoningContent\":{\"text\":\"thinking\",\"signature\":\"opaque\"}}}}\n",
        "{\"event\":\"metadata\",\"data\":{\"usage\":{\"inputTokens\":10,\"outputTokens\":2,\"totalTokens\":12}}}\n",
        "{\"event\":\"messageStop\",\"data\":{\"stopReason\":\"end_turn\"}}\n",
    );
    let events = replay(ApiFormat::BedrockConverse, source);
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningDelta { delta, .. } if delta.as_str() == "thinking")
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::ProviderReasoningReplay { item, .. }
            if item.source_api_format == ApiFormat::BedrockConverse
                && item.source_model_key == "fixture-model"
    )));
}

#[test]
fn anthropic_preserves_the_complete_signed_thinking_block_for_tool_replay() {
    let source = concat!(
        "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_reasoning\",\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}}\n",
        "{\"event\":\"content_block_start\",\"data\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"private reasoning\"}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"opaque-signature\"}}}\n",
        "{\"event\":\"content_block_stop\",\"data\":{\"type\":\"content_block_stop\",\"index\":0}}\n",
        "{\"event\":\"content_block_start\",\"data\":{\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\"}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}}\n",
        "{\"event\":\"content_block_stop\",\"data\":{\"type\":\"content_block_stop\",\"index\":1}}\n",
        "{\"event\":\"message_delta\",\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":8}}}\n",
        "{\"event\":\"message_stop\",\"data\":{\"type\":\"message_stop\"}}\n",
    );
    let events = replay(ApiFormat::AnthropicMessages, source);
    let replay = events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ProviderReasoningReplay { item, .. } => Some(item),
            _ => None,
        })
        .expect("signed reasoning replay");
    let payload = replay.payload.as_object().expect("replay object");
    assert_eq!(
        payload.get("thinking"),
        Some(&CanonicalJsonValue::String("private reasoning".into()))
    );
    assert_eq!(
        payload.get("signature"),
        Some(&CanonicalJsonValue::String("opaque-signature".into()))
    );
}

#[test]
fn anthropic_accepts_empty_argument_delta_for_a_no_argument_tool() {
    let source = concat!(
        "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_empty_args\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}}\n",
        "{\"event\":\"content_block_start\",\"data\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_empty\",\"name\":\"current_time\",\"input\":{}}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\"}}}\n",
        "{\"event\":\"content_block_stop\",\"data\":{\"type\":\"content_block_stop\",\"index\":0}}\n",
        "{\"event\":\"message_delta\",\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":2}}}\n",
        "{\"event\":\"message_stop\",\"data\":{\"type\":\"message_stop\"}}\n",
    );
    let events = replay(ApiFormat::AnthropicMessages, source);
    let arguments = events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ToolCallCompleted { arguments, .. } => Some(arguments),
            _ => None,
        })
        .expect("no-argument tool should complete");
    assert!(arguments.as_object().expect("arguments object").is_empty());
}

#[test]
fn anthropic_empty_text_fragments_preserve_content_and_signed_replay() {
    let source = concat!(
        "{\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_fragments\"}}}\n",
        "{\"data\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"retained\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"signed\"}}}\n",
        "{\"data\":{\"type\":\"content_block_stop\",\"index\":0}}\n",
        "{\"data\":{\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}}\n",
        "{\"data\":{\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}}\n",
        "{\"data\":{\"type\":\"content_block_stop\",\"index\":1}}\n",
        "{\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}}\n",
        "{\"data\":{\"type\":\"message_stop\"}}\n",
    );
    for format in [ApiFormat::AnthropicMessages, ApiFormat::BedrockInvokeModel] {
        let events = replay_with_reasoning_disclosure(format, source, true);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ModelStreamEvent::TextDelta { .. }))
                .count(),
            1
        );
        let payload = events
            .iter()
            .find_map(|event| match event {
                ModelStreamEvent::ProviderReasoningReplay { item, .. } => {
                    Some(item.payload.as_object().expect("replay object"))
                }
                _ => None,
            })
            .expect("signed replay survived empty fragments");
        assert_eq!(
            payload.get("thinking"),
            Some(&CanonicalJsonValue::String("retained".into()))
        );
    }
}

#[test]
fn anthropic_text_fragment_requires_a_string_and_thinking_requires_a_block() {
    for field in [
        None,
        Some(Value::Null),
        Some(Value::Bool(false)),
        Some(serde_json::json!({})),
    ] {
        let mut normalizer = ProviderNormalizer::new(
            ApiFormat::AnthropicMessages,
            fixture_route(),
            "fixture-model",
            "fixture-invocation",
        )
        .expect("normalizer");
        let mut delta = serde_json::json!({"type":"text_delta"});
        if let Some(field) = field {
            delta["text"] = field;
        }
        assert!(
            normalizer
                .observe(
                    None,
                    serde_json::json!({"type":"content_block_delta","index":0,"delta":delta})
                )
                .is_err()
        );
    }
    let mut normalizer = ProviderNormalizer::new(
        ApiFormat::AnthropicMessages,
        fixture_route(),
        "fixture-model",
        "fixture-invocation",
    )
    .expect("normalizer");
    assert!(matches!(normalizer.observe(None, serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":""}})), Err(NormalizeError::UnknownReasoningBlock)));
}

#[test]
fn anthropic_accepts_complete_arguments_on_content_block_start() {
    let source = concat!(
        "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_snapshot_args\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}}\n",
        "{\"event\":\"content_block_start\",\"data\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_snapshot\",\"name\":\"current_time\",\"input\":{\"timezone\":\"Asia/Shanghai\"}}}}\n",
        "{\"event\":\"content_block_stop\",\"data\":{\"type\":\"content_block_stop\",\"index\":0}}\n",
        "{\"event\":\"message_delta\",\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":2}}}\n",
        "{\"event\":\"message_stop\",\"data\":{\"type\":\"message_stop\"}}\n",
    );
    let events = replay(ApiFormat::AnthropicMessages, source);
    let arguments = events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ToolCallCompleted { arguments, .. } => Some(arguments),
            _ => None,
        })
        .expect("snapshot tool arguments should complete");
    assert_eq!(
        arguments
            .as_object()
            .expect("arguments object")
            .get("timezone"),
        Some(&CanonicalJsonValue::String("Asia/Shanghai".to_owned()))
    );
}

#[test]
fn anthropic_keeps_unsigned_thinking_public_but_does_not_mark_it_replayable() {
    let source = concat!(
        "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_unsigned\",\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}}\n",
        "{\"event\":\"content_block_start\",\"data\":{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}}\n",
        "{\"event\":\"content_block_delta\",\"data\":{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"visible but unsigned\"}}}\n",
        "{\"event\":\"content_block_stop\",\"data\":{\"type\":\"content_block_stop\",\"index\":0}}\n",
        "{\"event\":\"message_delta\",\"data\":{\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}}\n",
        "{\"event\":\"message_stop\",\"data\":{\"type\":\"message_stop\"}}\n",
    );
    let events = replay(ApiFormat::AnthropicMessages, source);
    assert!(events.iter().any(
        |event| matches!(event, ModelStreamEvent::ReasoningDelta { delta, .. } if delta.as_str() == "visible but unsigned")
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ProviderReasoningReplay { .. }))
    );
}

#[test]
fn openai_responses_backfills_encrypted_reasoning_from_terminal_response() {
    let source = concat!(
        "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_reasoning\"}}}\n",
        "{\"event\":\"response.output_item.done\",\"data\":{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[]}}}\n",
        "{\"event\":\"response.completed\",\"data\":{\"type\":\"response.completed\",\"response\":{\"id\":\"resp_reasoning\",\"output\":[{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"opaque-encrypted\"}],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}}\n",
    );
    let events = replay(ApiFormat::OpenAiResponses, source);
    let replay = events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ProviderReasoningReplay { item, .. } => Some(item),
            _ => None,
        })
        .expect("terminal encrypted reasoning replay");
    let payload = replay.payload.as_object().expect("replay object");
    assert_eq!(
        payload.get("encrypted_content"),
        Some(&CanonicalJsonValue::String("opaque-encrypted".into()))
    );
}

#[test]
fn malformed_tool_arguments_across_streaming_protocols_are_correctable_not_executable() {
    use musteragent_runtime_contract::ToolArgumentValidationIssueKind as Issue;
    use serde_json::json;
    for (arguments, expected) in [
        ("{broken", Issue::MalformedJson),
        ("[]", Issue::ObjectRequired),
        ("null", Issue::ObjectRequired),
    ] {
        let chat = vec![json!({"data":{"id":"response","choices":[{"index":0,
            "delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"lookup","arguments":arguments}}]},"finish_reason":"tool_calls"}]}})];
        let anthropic = vec![
            json!({"data":{"type":"message_start","message":{"id":"response"}}}),
            json!({"data":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":{}}}}),
            json!({"data":{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":arguments}}}),
            json!({"data":{"type":"content_block_stop","index":0}}),
            json!({"data":{"type":"message_delta","delta":{"stop_reason":"tool_use"}}}),
            json!({"data":{"type":"message_stop"}}),
        ];
        let bedrock = vec![
            json!({"event":"messageStart","data":{"role":"assistant"}}),
            json!({"event":"contentBlockStart","data":{"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call","name":"lookup"}}}}),
            json!({"event":"contentBlockDelta","data":{"contentBlockIndex":0,"delta":{"toolUse":{"input":arguments}}}}),
            json!({"event":"contentBlockStop","data":{"contentBlockIndex":0}}),
            json!({"event":"messageStop","data":{"stopReason":"tool_use"}}),
        ];
        for (format, frames) in [
            (ApiFormat::OpenAiChatCompletions, &chat),
            (ApiFormat::AnthropicMessages, &anthropic),
            (ApiFormat::BedrockInvokeModel, &anthropic),
            (ApiFormat::BedrockConverse, &bedrock),
        ] {
            let source = frames
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            let events = replay(format, &source);
            assert!(events.iter().any(|event| matches!(event, ModelStreamEvent::ToolCallRejected {kind,..} if *kind == expected)), "{format:?}");
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. })),
                "{format:?}"
            );
        }
    }
}

#[test]
fn structured_provider_tool_inputs_use_the_same_object_rejection_boundary() {
    use musteragent_runtime_contract::ToolArgumentValidationIssueKind as Issue;
    use serde_json::json;
    for input in [json!([]), json!(null), json!("invalid")] {
        let anthropic = vec![
            json!({"data":{"type":"message_start","message":{"id":"response"}}}),
            json!({"data":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"lookup","input":input}}}),
            json!({"data":{"type":"content_block_stop","index":0}}),
            json!({"data":{"type":"message_delta","delta":{"stop_reason":"tool_use"}}}),
            json!({"data":{"type":"message_stop"}}),
        ];
        let google = vec![
            json!({"data":{"responseId":"response","candidates":[{"content":{"parts":[{"functionCall":{"id":"call","name":"lookup","args":input}}]},"finishReason":"STOP"}]}}),
        ];
        let interactions = vec![
            json!({"data":{"event_type":"interaction.created","interaction":{"id":"response","status":"in_progress"}}}),
            json!({"data":{"event_type":"step.start","index":0,"step":{"type":"function_call","id":"call","name":"lookup","arguments":input}}}),
            json!({"data":{"event_type":"step.stop","index":0}}),
            json!({"data":{"event_type":"interaction.completed","interaction":{"id":"response","status":"requires_action"}}}),
        ];
        for (format, frames) in [
            (ApiFormat::AnthropicMessages, &anthropic),
            (ApiFormat::BedrockInvokeModel, &anthropic),
            (ApiFormat::GoogleGenerateContent, &google),
            (ApiFormat::GoogleInteractions, &interactions),
        ] {
            let source = frames
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            let events = replay(format, &source);
            assert!(
                events.iter().any(|event| matches!(
                    event,
                    ModelStreamEvent::ToolCallRejected {
                        kind: Issue::ObjectRequired,
                        ..
                    }
                )),
                "{format:?}"
            );
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, ModelStreamEvent::ToolCallCompleted { .. })),
                "{format:?}"
            );
        }
    }
}
#[path = "normalization/argument_protocol.rs"]
mod argument_protocol;

#[test]
fn google_long_native_text_keeps_exact_signature_boundaries_in_bounded_private_chunks() {
    let mut normalizer = ProviderNormalizer::new(
        ApiFormat::GoogleGenerateContent,
        fixture_route(),
        "fixture-model",
        "long-private",
    )
    .expect("valid test fixture");
    let piece = "x".repeat(16_384);
    for _ in 0..80 {
        normalizer.observe(None,serde_json::json!({"responseId":"long-response","candidates":[{"content":{"role":"model","parts":[{"text":piece}]}}]})).expect("valid test fixture");
    }
    let events=normalizer.observe(None,serde_json::json!({"responseId":"long-response","candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"exact-private-signature"}]},"finishReason":"STOP"}]})).expect("valid test fixture");
    let item = events
        .iter()
        .find_map(|event| {
            if let ModelStreamEvent::ProviderReasoningReplay { item, .. } = event {
                Some(item)
            } else {
                None
            }
        })
        .expect("valid test fixture");
    item.validate().expect("valid test fixture");
    let object = item.payload.as_object().expect("valid test fixture");
    let CanonicalJsonValue::Array(chunks) = object.get("wire_chunks").expect("valid test fixture")
    else {
        panic!("bounded native chunks required");
    };
    assert!(chunks.len() > 1);
    let encoded = chunks
        .iter()
        .map(|chunk| {
            if let CanonicalJsonValue::String(text) = chunk {
                assert!(text.len() <= 1_048_576);
                text.as_str()
            } else {
                panic!("string chunks only")
            }
        })
        .collect::<String>();
    let native: Value = serde_json::from_str(&encoded).expect("valid test fixture");
    assert_eq!(
        native["parts"][0]["text"]
            .as_str()
            .expect("valid test fixture")
            .len(),
        16_384 * 80
    );
    assert_eq!(
        native["parts"][1]["thoughtSignature"],
        "exact-private-signature"
    );
}

#[test]
fn google_interactions_terminal_step_snapshot_supplies_arguments_without_duplicate_text() {
    let mut normalizer = ProviderNormalizer::new(
        ApiFormat::GoogleInteractions,
        fixture_route(),
        "fixture-model",
        "snapshot-tool",
    )
    .expect("valid test fixture");
    let frames = serde_json::json!([
        {"event_type":"interaction.created","interaction":{"id":"snapshot-response","status":"in_progress"}},
        {"event_type":"step.start","index":0,"step":{"type":"function_call","id":"native-call","name":"lookup","arguments":{}}},
        {"event_type":"step.stop","index":0,"step":{"type":"function_call","id":"native-call","name":"lookup","arguments":{"value":"snapshot-result"}}},
        {"event_type":"interaction.completed","interaction":{"id":"snapshot-response","status":"requires_action"}}
    ]);
    let mut state = ModelStreamState::default();
    let mut output = Vec::new();
    for frame in frames.as_array().expect("valid test fixture") {
        for event in normalizer
            .observe(None, frame.clone())
            .expect("valid test fixture")
        {
            state.accept(&event).expect("valid test fixture");
            output.push(event);
        }
    }
    assert!(state.is_terminal());
    assert!(output.iter().any(|event| matches!(event,ModelStreamEvent::ToolCallCompleted {arguments:CanonicalJsonValue::Object(value),..} if value.get("value")==Some(&CanonicalJsonValue::String("snapshot-result".to_owned())))));
}

#[test]
fn google_generate_accepts_empty_terminal_text_after_a_signed_tool_call() {
    let mut normalizer = ProviderNormalizer::new(
        ApiFormat::GoogleGenerateContent,
        fixture_route(),
        "fixture-model",
        "empty-final",
    )
    .expect("valid test fixture");
    let frames = serde_json::json!([
        {"responseId":"response","candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"lookup","args":{"value":"a"}},"thoughtSignature":"private-signature"}]}}]},
        {"responseId":"response","candidates":[{"content":{"role":"model","parts":[{"text":""}]},"finishReason":"STOP"}]}
    ]);
    let mut state = ModelStreamState::default();
    let mut count = 0;
    for frame in frames.as_array().expect("valid test fixture") {
        for event in normalizer
            .observe(None, frame.clone())
            .expect("valid test fixture")
        {
            state.accept(&event).expect("valid test fixture");
            if matches!(event, ModelStreamEvent::ToolCallCompleted { .. }) {
                count += 1;
            }
        }
    }
    assert!(state.is_terminal());
    assert_eq!(count, 1);
}

#[test]
fn google_generate_settles_cached_usage_at_the_end_of_the_hop() {
    let events = replay(
        ApiFormat::GoogleGenerateContent,
        concat!(
            r#"{"data":{"responseId":"cache","candidates":[{"content":{"parts":[{"text":"hi"}]}}],"usageMetadata":{"promptTokenCount":34918,"candidatesTokenCount":22,"totalTokenCount":34940}}}"#,
            "\n",
            r#"{"data":{"responseId":"cache","candidates":[{"content":{"parts":[{"text":""}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":39213,"candidatesTokenCount":28,"totalTokenCount":39241,"cachedContentTokenCount":37087}}}"#
        ),
    );
    let usage: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ModelStreamEvent::Usage(usage) => Some(usage),
            _ => None,
        })
        .collect();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].input_tokens_uncached, 2126);
    assert_eq!(usage[0].provider_cache_read_tokens, 37087);
    assert_eq!(usage[0].total_tokens, Some(39241));
}

#[test]
fn google_reports_a_rejected_generation_without_dispatching_an_empty_answer() {
    let events = replay(
        ApiFormat::GoogleGenerateContent,
        r#"{"data":{"responseId":"invalid","candidates":[{"content":{"parts":[{"text":""}]},"finishReason":"MALFORMED_RESPONSE"}],"usageMetadata":{"promptTokenCount":18,"totalTokenCount":18}}}"#,
    );
    assert!(
        matches!(events.last(),Some(ModelStreamEvent::ResponseError(e)) if e.safe_code==ProviderErrorCode::ProtocolViolation)
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e,ModelStreamEvent::Usage(u) if u.total_tokens==Some(18)))
    );
}
