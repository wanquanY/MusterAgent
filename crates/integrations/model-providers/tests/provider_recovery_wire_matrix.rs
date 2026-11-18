mod support;

use std::time::Duration;

use futures_util::StreamExt as _;
use musteragent_model::ApiFormat;
use musteragent_model_providers::{
    DirectProviderRecoveryProfile, DirectProviderWireProtocol, ProviderRecoveryCapability,
};
use musteragent_runtime_model::{
    CanonicalModelRequest, ModelProvider, ModelStreamEvent, ProviderErrorCode, ProviderErrorKind,
};
use musteragent_testkit::{MockProviderWire, ProviderWireScenario};

use support::{
    FIXTURE_SECRET, REQUESTED_MODEL, RESPONSE_MODEL, SourceFrame, compression_request,
    content_type, provider, request, wire_chunks,
};

const APPROVED_FORMATS: [ApiFormat; 7] = [
    ApiFormat::OpenAiResponses,
    ApiFormat::OpenAiChatCompletions,
    ApiFormat::AnthropicMessages,
    ApiFormat::GoogleGenerateContent,
    ApiFormat::GoogleInteractions,
    ApiFormat::BedrockConverse,
    ApiFormat::BedrockInvokeModel,
];

#[test]
fn approved_api_formats_have_one_closed_production_wire_recovery_profile() {
    for format in APPROVED_FORMATS {
        let profile = DirectProviderRecoveryProfile::for_api_format(format);
        assert_eq!(profile.api_format, format);
        assert_eq!(profile.recovery, ProviderRecoveryCapability::NonResumable);
        assert_eq!(
            profile.wire_protocol,
            if matches!(
                format,
                ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
            ) {
                DirectProviderWireProtocol::AwsEventStream
            } else {
                DirectProviderWireProtocol::ServerSentEvents
            }
        );
    }
}

#[tokio::test]
async fn every_production_wire_path_reports_partial_eof_as_retryable_transport_failure() {
    assert_every_wire_path_reports_partial_eof_as_retryable_transport_failure(request).await;
}

#[tokio::test]
async fn every_compression_wire_path_reports_partial_eof_as_retryable_transport_failure() {
    assert_every_wire_path_reports_partial_eof_as_retryable_transport_failure(compression_request)
        .await;
}

async fn assert_every_wire_path_reports_partial_eof_as_retryable_transport_failure(
    build_request: fn(ApiFormat) -> CanonicalModelRequest,
) {
    for format in APPROVED_FORMATS {
        let frames = interrupted_frames(format);
        let server = MockProviderWire::start(ProviderWireScenario::successful(
            content_type(format),
            wire_chunks(format, &frames),
        ))
        .await
        .expect("controlled Provider wire");
        let direct = provider(server.endpoint(), format);
        let results = direct
            .stream(build_request(format))
            .collect::<Vec<_>>()
            .await;

        assert!(
            results.iter().any(|result| matches!(
                result,
                Ok(ModelStreamEvent::TextDelta { delta, .. }) if delta.as_str() == "partial"
            )),
            "{format:?}: {results:?}"
        );
        assert!(
            !results
                .iter()
                .any(|result| matches!(result, Ok(ModelStreamEvent::ResponseCompleted { .. }))),
            "{format:?}: transport EOF became success"
        );
        assert!(
            matches!(
                results.last(),
                Some(Err(error))
                    if error.kind == ProviderErrorKind::Transport
                        && error.safe_code == ProviderErrorCode::TransportFailed
                        && error.retryable
                        && error.retry_after_ms.is_none()
                        && error.diagnostic_ref.is_none()
            ),
            "{format:?}: {results:?}"
        );
        let rendered = format!("{:?}", results.last());
        assert!(
            !rendered.contains(FIXTURE_SECRET),
            "{format:?}: secret leaked"
        );
        server.shutdown().await.expect("wire shutdown");
    }
}

#[tokio::test]
async fn every_production_wire_path_emits_empty_completion_with_or_without_usage_and_identity() {
    assert_every_wire_path_emits_empty_completion_with_or_without_usage_and_identity(request).await;
}

#[tokio::test]
async fn every_compression_wire_path_emits_empty_completion_with_or_without_usage_and_identity() {
    assert_every_wire_path_emits_empty_completion_with_or_without_usage_and_identity(
        compression_request,
    )
    .await;
}

async fn assert_every_wire_path_emits_empty_completion_with_or_without_usage_and_identity(
    build_request: fn(ApiFormat) -> CanonicalModelRequest,
) {
    for format in APPROVED_FORMATS {
        for include_usage in [true, false] {
            let frames = if include_usage {
                empty_completion_frames(format)
            } else {
                empty_completion_without_usage_frames(format)
            };
            let server = MockProviderWire::start(ProviderWireScenario::successful(
                content_type(format),
                wire_chunks(format, &frames),
            ))
            .await
            .expect("controlled Provider wire");
            let direct = provider(server.endpoint(), format);
            let events = direct
                .stream(build_request(format))
                .map(|result| result.expect("empty completion must be valid"))
                .collect::<Vec<_>>()
                .await;

            assert!(
                !events.iter().any(ModelStreamEvent::is_public_output),
                "{format:?}/{include_usage}: {events:?}"
            );
            let usages = events
                .iter()
                .filter_map(|event| match event {
                    ModelStreamEvent::Usage(usage) => Some(usage),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if include_usage {
                assert!(
                    !usages.is_empty()
                        && usages.iter().all(|usage| {
                            usage.input_tokens_uncached == 10 && usage.output_tokens == 0
                        }),
                    "{format:?}: {events:?}"
                );
            } else {
                assert!(
                    usages.is_empty(),
                    "{format:?}: adapter synthesized Usage: {events:?}"
                );
            }
            assert!(
                matches!(
                    events.last(),
                    Some(ModelStreamEvent::ResponseCompleted { .. })
                ),
                "{format:?}/{include_usage}: {events:?}"
            );
            let response_model = events.iter().find_map(|event| match event {
                ModelStreamEvent::ResponseStarted {
                    response_model_key, ..
                } => Some(response_model_key.as_str()),
                _ => None,
            });
            let expected_model = if format == ApiFormat::BedrockConverse {
                REQUESTED_MODEL
            } else {
                RESPONSE_MODEL
            };
            assert_eq!(
                response_model,
                Some(expected_model),
                "{format:?}/{include_usage}"
            );
            server.shutdown().await.expect("wire shutdown");
        }
    }
}

#[tokio::test]
async fn dropping_each_partial_production_stream_cancels_the_upstream_body() {
    assert_dropping_each_partial_stream_cancels_the_upstream_body(request).await;
}

#[tokio::test]
async fn dropping_each_partial_compression_stream_cancels_the_upstream_body() {
    assert_dropping_each_partial_stream_cancels_the_upstream_body(compression_request).await;
}

async fn assert_dropping_each_partial_stream_cancels_the_upstream_body(
    build_request: fn(ApiFormat) -> CanonicalModelRequest,
) {
    for format in APPROVED_FORMATS {
        let frames = interrupted_frames(format);
        let server = MockProviderWire::start(ProviderWireScenario::hanging(
            content_type(format),
            wire_chunks(format, &frames),
        ))
        .await
        .expect("controlled Provider wire");
        let direct = provider(server.endpoint(), format);
        let mut stream = direct.stream(build_request(format));
        let mut saw_partial = false;
        while let Some(event) = stream.next().await {
            match event.expect("hanging stream prefix") {
                ModelStreamEvent::TextDelta { delta, .. } if delta.as_str() == "partial" => {
                    saw_partial = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_partial, "{format:?}");
        drop(stream);
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.closed_stream_count() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{format:?}: upstream body remained open"));
        server.shutdown().await.expect("wire shutdown");
    }
}

fn interrupted_frames(format: ApiFormat) -> Vec<SourceFrame> {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => vec![
            SourceFrame {
                event: "response.created",
                data: r#"{"type":"response.created","response":{"id":"resp_partial","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "response.output_text.delta",
                data: r#"{"type":"response.output_text.delta","output_index":0,"delta":"partial"}"#,
            },
        ],
        ApiFormat::OpenAiChatCompletions => vec![SourceFrame {
            event: "message",
            data: r#"{"id":"chat_partial","model":"provider-reported-model","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}"#,
        }],
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => vec![
            SourceFrame {
                event: "message_start",
                data: r#"{"type":"message_start","message":{"id":"msg_partial","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "content_block_delta",
                data: r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}"#,
            },
        ],
        ApiFormat::GoogleGenerateContent => vec![SourceFrame {
            event: "message",
            data: r#"{"responseId":"google_partial","modelVersion":"provider-reported-model","candidates":[{"content":{"parts":[{"text":"partial"}]}}]}"#,
        }],
        ApiFormat::GoogleInteractions => vec![
            SourceFrame {
                event: "interaction.created",
                data: r#"{"event_type":"interaction.created","interaction":{"id":"interaction_partial","model":"provider-reported-model","status":"in_progress"}}"#,
            },
            SourceFrame {
                event: "step.start",
                data: r#"{"event_type":"step.start","index":0,"step":{"type":"model_output"}}"#,
            },
            SourceFrame {
                event: "step.delta",
                data: r#"{"event_type":"step.delta","index":0,"delta":{"type":"text","text":"partial"}}"#,
            },
        ],
        ApiFormat::BedrockConverse => vec![
            SourceFrame {
                event: "messageStart",
                data: r#"{"role":"assistant"}"#,
            },
            SourceFrame {
                event: "contentBlockDelta",
                data: r#"{"contentBlockIndex":0,"delta":{"text":"partial"}}"#,
            },
        ],
    }
}

fn empty_completion_frames(format: ApiFormat) -> Vec<SourceFrame> {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => vec![
            SourceFrame {
                event: "response.created",
                data: r#"{"type":"response.created","response":{"id":"resp_empty","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "response.completed",
                data: r#"{"type":"response.completed","response":{"id":"resp_empty","model":"provider-reported-model","usage":{"input_tokens":10,"output_tokens":0,"total_tokens":10}}}"#,
            },
        ],
        ApiFormat::OpenAiChatCompletions => vec![SourceFrame {
            event: "message",
            data: r#"{"id":"chat_empty","model":"provider-reported-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":0,"total_tokens":10}}"#,
        }],
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => vec![
            SourceFrame {
                event: "message_start",
                data: r#"{"type":"message_start","message":{"id":"msg_empty","model":"provider-reported-model","usage":{"input_tokens":10,"output_tokens":0}}}"#,
            },
            SourceFrame {
                event: "message_delta",
                data: r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}"#,
            },
            SourceFrame {
                event: "message_stop",
                data: r#"{"type":"message_stop"}"#,
            },
        ],
        ApiFormat::GoogleGenerateContent => vec![SourceFrame {
            event: "message",
            data: r#"{"responseId":"google_empty","modelVersion":"provider-reported-model","candidates":[{"content":{"parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":0,"totalTokenCount":10}}"#,
        }],
        ApiFormat::GoogleInteractions => vec![
            SourceFrame {
                event: "interaction.created",
                data: r#"{"event_type":"interaction.created","interaction":{"id":"interaction_empty","model":"provider-reported-model","status":"in_progress"}}"#,
            },
            SourceFrame {
                event: "interaction.completed",
                data: r#"{"event_type":"interaction.completed","interaction":{"id":"interaction_empty","model":"provider-reported-model","status":"completed"},"metadata":{"total_usage":{"total_input_tokens":10,"total_output_tokens":0,"total_tokens":10}}}"#,
            },
        ],
        ApiFormat::BedrockConverse => vec![
            SourceFrame {
                event: "messageStart",
                data: r#"{"role":"assistant"}"#,
            },
            SourceFrame {
                event: "metadata",
                data: r#"{"usage":{"inputTokens":10,"outputTokens":0,"totalTokens":10}}"#,
            },
            SourceFrame {
                event: "messageStop",
                data: r#"{"stopReason":"end_turn"}"#,
            },
        ],
    }
}

fn empty_completion_without_usage_frames(format: ApiFormat) -> Vec<SourceFrame> {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiResponses => vec![
            SourceFrame {
                event: "response.created",
                data: r#"{"type":"response.created","response":{"id":"resp_empty","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "response.completed",
                data: r#"{"type":"response.completed","response":{"id":"resp_empty","model":"provider-reported-model"}}"#,
            },
        ],
        ApiFormat::OpenAiChatCompletions => vec![SourceFrame {
            event: "message",
            data: r#"{"id":"chat_empty","model":"provider-reported-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        }],
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => vec![
            SourceFrame {
                event: "message_start",
                data: r#"{"type":"message_start","message":{"id":"msg_empty","model":"provider-reported-model"}}"#,
            },
            SourceFrame {
                event: "message_delta",
                data: r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"}}"#,
            },
            SourceFrame {
                event: "message_stop",
                data: r#"{"type":"message_stop"}"#,
            },
        ],
        ApiFormat::GoogleGenerateContent => vec![SourceFrame {
            event: "message",
            data: r#"{"responseId":"google_empty","modelVersion":"provider-reported-model","candidates":[{"content":{"parts":[]},"finishReason":"STOP"}]}"#,
        }],
        ApiFormat::GoogleInteractions => vec![
            SourceFrame {
                event: "interaction.created",
                data: r#"{"event_type":"interaction.created","interaction":{"id":"interaction_empty","model":"provider-reported-model","status":"in_progress"}}"#,
            },
            SourceFrame {
                event: "interaction.completed",
                data: r#"{"event_type":"interaction.completed","interaction":{"id":"interaction_empty","model":"provider-reported-model","status":"completed"}}"#,
            },
        ],
        ApiFormat::BedrockConverse => vec![
            SourceFrame {
                event: "messageStart",
                data: r#"{"role":"assistant"}"#,
            },
            SourceFrame {
                event: "messageStop",
                data: r#"{"stopReason":"end_turn"}"#,
            },
        ],
    }
}
