#[path = "request_mapping/dialogue_input.rs"]
mod dialogue_input;
#[path = "request_mapping/fixtures.rs"]
mod fixtures;
#[path = "request_mapping/google_native.rs"]
mod google_native;
#[path = "request_mapping/native_publication.rs"]
mod native_publication;
#[path = "request_mapping/public_delivery_repair.rs"]
mod public_delivery_repair;
#[path = "request_mapping/structured_output.rs"]
mod structured_output;
use fixtures::*;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ContentId, ConversationId, CorrelationId,
    DelegationDeliveryId, DelegationId, DottedKey, FencingToken, HomeEpoch, InvocationId, LeaseId,
    LedgerRecordId, ParticipantId, RunId, RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp,
    VersionedKey,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelParameter, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_model_providers::{MappingError, ResolvedContent, ResolvedContents, map_request};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelError,
    CanonicalModelRequest, CanonicalRole, CanonicalRuntimeEvent, CanonicalToolCall,
    CanonicalToolDefinition, ContinuationRef, JsonValue, ModelRouteRef, ModelText, ReasoningPolicy,
    ReasoningReplay,
};
use musteragent_tool::{ToolKey, WireName};
use secrecy::SecretSlice;

#[test]
fn provider_requests_preserve_one_atomic_parallel_tool_exchange() {
    let cases = [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ];
    for format in cases {
        let mapped = map_request(&request(format), &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} mapping failed: {error}"));
        assert_mapping(format, &mapped.body);
        if matches!(
            format,
            ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
        ) {
            assert_eq!(mapped.accept, "application/vnd.amazon.eventstream");
        }
    }
}

#[test]
fn runtime_event_is_a_private_canonical_stimulus_and_a_legal_final_provider_input() {
    let formats = [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ];
    for (event, marker) in [
        (
            CanonicalRuntimeEvent::ModelRecovery {
                source_invocation_id: InvocationId::generate(),
            },
            "Runtime event: model recovery",
        ),
        (
            delegation_completion_event(),
            "private-runtime-event-report",
        ),
        (
            CanonicalRuntimeEvent::OutputLimitContinuation {
                source_invocation_id: InvocationId::generate(),
            },
            "Runtime event: output limit continuation",
        ),
    ] {
        for format in formats {
            let mut request = if format == ApiFormat::GoogleInteractions {
                interactions_continuation_request()
            } else {
                let mut request = request(format);
                request.input = vec![
                    message(CanonicalRole::User, "Start the task"),
                    message(CanonicalRole::Assistant, "Previous assistant output"),
                ];
                request
            };
            request.runtime_events = vec![event.clone()];
            request
                .validate()
                .expect("Runtime event is a turn stimulus");

            let body = map_request(&request, &ResolvedContents::default())
                .unwrap_or_else(|error| panic!("{format:?} Runtime-event mapping failed: {error}"))
                .body;
            match format {
                ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                    unreachable!("decision protocols do not use chat fixtures")
                }
                ApiFormat::OpenAiChatCompletions => {
                    let final_input = body["messages"]
                        .as_array()
                        .expect("messages")
                        .last()
                        .expect("final OpenAI Chat input");
                    assert_eq!(final_input["role"], "user");
                    assert!(
                        final_input["content"]
                            .as_str()
                            .expect("OpenAI Chat content")
                            .contains(marker)
                    );
                    assert!(
                        !body["messages"][0]["content"]
                            .as_str()
                            .expect("OpenAI Chat system content")
                            .contains(marker)
                    );
                }
                ApiFormat::OpenAiResponses => {
                    let final_input = body["input"]
                        .as_array()
                        .expect("input")
                        .last()
                        .expect("final OpenAI Responses input");
                    assert_eq!(final_input["role"], "user");
                    assert_eq!(final_input["content"][0]["type"], "input_text");
                    assert!(
                        final_input["content"][0]["text"]
                            .as_str()
                            .expect("OpenAI Responses input text")
                            .contains(marker)
                    );
                    assert!(
                        !body["instructions"]
                            .as_str()
                            .expect("OpenAI Responses instructions")
                            .contains(marker)
                    );
                }
                ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                    let final_input = body["messages"]
                        .as_array()
                        .expect("messages")
                        .last()
                        .expect("final Anthropic or Bedrock input");
                    assert_eq!(final_input["role"], "user");
                    assert!(
                        final_input["content"][0]["text"]
                            .as_str()
                            .expect("Anthropic or Bedrock input text")
                            .contains(marker)
                    );
                    assert!(
                        !serde_json::to_string(&body["system"])
                            .expect("serialize Anthropic or Bedrock system prompt")
                            .contains(marker)
                    );
                }
                ApiFormat::GoogleGenerateContent => {
                    let final_input = body["contents"]
                        .as_array()
                        .expect("contents")
                        .last()
                        .expect("final Google Generate Content input");
                    assert_eq!(final_input["role"], "user");
                    assert!(
                        final_input["parts"][0]["text"]
                            .as_str()
                            .expect("Google Generate Content input text")
                            .contains(marker)
                    );
                    assert!(
                        !serde_json::to_string(&body["systemInstruction"])
                            .expect("serialize Google system instruction")
                            .contains(marker)
                    );
                }
                ApiFormat::GoogleInteractions => {
                    let final_input = body["input"]
                        .as_array()
                        .expect("input")
                        .last()
                        .expect("final Google Interactions input");
                    assert_eq!(final_input["type"], "user_input");
                    assert!(
                        final_input["content"][0]["text"]
                            .as_str()
                            .expect("Google Interactions input text")
                            .contains(marker)
                    );
                    assert!(
                        !body["system_instruction"]
                            .as_str()
                            .expect("Google Interactions system instruction")
                            .contains(marker)
                    );
                }
                ApiFormat::BedrockConverse => {
                    let final_input = body["messages"]
                        .as_array()
                        .expect("messages")
                        .last()
                        .expect("final Bedrock Converse input");
                    assert_eq!(final_input["role"], "user");
                    assert!(
                        final_input["content"][0]["text"]
                            .as_str()
                            .expect("Bedrock Converse input text")
                            .contains(marker)
                    );
                    assert!(
                        !serde_json::to_string(&body["system"])
                            .expect("serialize Bedrock Converse system prompt")
                            .contains(marker)
                    );
                }
            }
        }
    }
}

#[test]
fn assistant_tail_without_a_new_turn_stimulus_is_rejected_before_provider_dispatch() {
    let mut request = request(ApiFormat::OpenAiResponses);
    request.input = vec![
        message(CanonicalRole::User, "Start the task"),
        message(CanonicalRole::Assistant, "Previous assistant output"),
    ];

    assert_eq!(
        request.validate(),
        Err(CanonicalModelError::MissingTurnStimulus)
    );
    assert_eq!(
        mapping_error(&request),
        MappingError::InvalidCanonicalRequest
    );
}

#[test]
fn openai_chat_joins_adjacent_canonical_text_parts_without_changing_message_bytes() {
    let mut request = request(ApiFormat::OpenAiChatCompletions);
    request.input = vec![CanonicalMessage {
        role: CanonicalRole::User,
        content: ["large ", "logical ", "message"]
            .into_iter()
            .map(|value| {
                CanonicalContentPart::Text(ModelText::new(value).expect("bounded text part"))
            })
            .collect(),
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    }];

    let body = map_request(&request, &ResolvedContents::default())
        .expect("map adjacent text parts")
        .body;
    let user = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|message| message["role"] == "user")
        .expect("user message");
    assert_eq!(user["content"], "large logical message");
}

#[test]
fn portable_tool_history_projects_across_protocol_matrix_and_private_replay_stays_route_bound() {
    let formats = [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ];
    for source_format in formats {
        for target_format in formats {
            let mut request = request(target_format);
            if source_format == target_format {
                request.route.route_ref = source_route(source_format);
                request.route.resolved_model_key = source_model(source_format).into();
            } else {
                request.route.route_ref = target_route(target_format);
                request.route.resolved_model_key = target_model(target_format).into();
            }
            let mut contents = ResolvedContents::default();
            let marker = attach_source_private_replay(source_format, &mut request, &mut contents);

            let body = map_request(&request, &contents)
                .unwrap_or_else(|error| {
                    panic!("{source_format:?} -> {target_format:?} mapping failed: {error}")
                })
                .body;
            assert_mapping(target_format, &body);
            let encoded = serde_json::to_string(&body).expect("mapped body JSON");
            assert_eq!(
                encoded.contains(marker),
                source_format == target_format,
                "{source_format:?} -> {target_format:?} private replay decision"
            );
        }
    }
}

#[test]
fn openai_responses_accepts_portable_tool_history_when_reasoning_is_enabled() {
    let mut request = request(ApiFormat::OpenAiResponses);
    request.reasoning_policy = ReasoningPolicy::Summary;

    let mapped = map_request(&request, &ResolvedContents::default())
        .expect("portable tool history must not require an opaque response continuation");

    assert_mapping(ApiFormat::OpenAiResponses, &mapped.body);
    assert_eq!(mapped.body["reasoning"]["summary"], "auto");
    assert!(mapped.body.get("previous_response_id").is_none());
}

#[test]
fn google_interactions_stateful_continuation_sends_only_exact_function_results() {
    let request = interactions_continuation_request();
    let mapped =
        map_request(&request, &ResolvedContents::default()).expect("map Interaction continuation");
    assert_mapping(ApiFormat::GoogleInteractions, &mapped.body);
    assert_eq!(
        mapped.body["previous_interaction_id"],
        serde_json::json!("interaction-previous")
    );
}

#[test]
fn google_generate_replays_a_same_model_thought_signature_on_the_exact_tool_part() {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "schema": "musteragent.legacy.runtime.provider-tool-replay.v2",
        "kind": "google_thought_signature",
        "source_route_ref": "model.route.mapping@1",
        "source_api_format": "google.generate-content",
        "source_model_key": "model-fixture",
        "signature": "opaque-google-signature",
    }))
    .expect("replay JSON");
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.legacy.provider-tool-replay+json")
            .expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(&bytes),
    };
    let mut request = request(ApiFormat::GoogleGenerateContent);
    request.input[1].tool_calls[0].provider_replay_ref = Some(reference.clone());
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("resolved replay");

    let mapped = map_request(&request, &contents).expect("map Google request");
    let tool_part = &mapped.body["contents"][1]["parts"][0];
    assert_eq!(
        tool_part["thoughtSignature"],
        serde_json::json!("opaque-google-signature")
    );
    assert!(tool_part["functionCall"].get("thoughtSignature").is_none());
}

#[test]
fn legacy_unbound_google_tool_replay_degrades_without_cross_route_replay() {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "schema": "musteragent.legacy.runtime.provider-tool-replay.v1",
        "kind": "google_thought_signature",
        "source_model_key": "model-fixture",
        "signature": "legacy-unbound-signature",
    }))
    .expect("legacy replay JSON");
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.legacy.provider-tool-replay+json")
            .expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(&bytes),
    };
    let mut request = request(ApiFormat::GoogleGenerateContent);
    request.input[1].tool_calls[0].provider_replay_ref = Some(reference.clone());
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("resolved legacy replay");

    let body = map_request(&request, &contents)
        .expect("legacy unbound replay must degrade to portable tool history")
        .body;

    assert_mapping(ApiFormat::GoogleGenerateContent, &body);
    assert!(
        !serde_json::to_string(&body)
            .expect("mapped request JSON")
            .contains("legacy-unbound-signature")
    );
}

#[test]
fn provider_requests_use_native_multimodal_tool_result_shapes() {
    let bytes = b"image-fixture".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(&bytes),
    };
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.input[2]
            .content
            .push(CanonicalContentPart::Image(reference.clone()));
        if format == ApiFormat::GoogleInteractions {
            request = interactions_continuation(request);
        }
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.clone().into_boxed_slice()),
                },
            )
            .expect("resolved image");
        let body = map_request(&request, &contents)
            .unwrap_or_else(|error| panic!("{format:?} mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiChatCompletions => {
                let messages = body["messages"].as_array().expect("messages");
                let tool_positions = messages
                    .iter()
                    .enumerate()
                    .filter_map(|(index, message)| (message["role"] == "tool").then_some(index))
                    .collect::<Vec<_>>();
                assert_eq!(tool_positions.len(), 2);
                assert_eq!(tool_positions[1], tool_positions[0] + 1);
                assert_eq!(messages.last().expect("media message")["role"], "user");
                assert_eq!(
                    messages.last().expect("media message")["content"][1]["type"],
                    "image_url"
                );
            }
            ApiFormat::OpenAiResponses => {
                let output = body["input"]
                    .as_array()
                    .expect("input")
                    .iter()
                    .find(|item| {
                        item["type"] == "function_call_output" && item["call_id"] == "call_one"
                    })
                    .expect("tool output");
                assert_eq!(output["output"][1]["type"], "input_image");
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                let result = &body["messages"]
                    .as_array()
                    .expect("messages")
                    .last()
                    .expect("result turn")["content"][0];
                assert_eq!(result["content"][1]["type"], "image");
            }
            ApiFormat::GoogleGenerateContent => {
                let result = &body["contents"]
                    .as_array()
                    .expect("contents")
                    .last()
                    .expect("result turn")["parts"][0]["functionResponse"];
                assert!(result["parts"][0].get("inlineData").is_some());
            }
            ApiFormat::GoogleInteractions => {
                let result = body["input"]
                    .as_array()
                    .expect("input")
                    .iter()
                    .find(|step| step["type"] == "function_result" && step["call_id"] == "call_one")
                    .expect("function result");
                assert_eq!(result["result"][1]["type"], "image");
            }
            ApiFormat::BedrockConverse => {
                let result = &body["messages"]
                    .as_array()
                    .expect("messages")
                    .last()
                    .expect("result turn")["content"][0]["toolResult"];
                assert!(result["content"][1].get("image").is_some());
            }
        }
    }
}

#[test]
fn google_generate_maps_audio_and_video_as_native_parts() {
    let audio_bytes = b"audio-fixture".to_vec();
    let video_bytes = b"video-fixture".to_vec();
    let audio = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("audio/mpeg").expect("media type"),
        byte_length: audio_bytes.len() as u64,
        sha256: Sha256::digest(&audio_bytes),
    };
    let video = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("video/mp4").expect("media type"),
        byte_length: video_bytes.len() as u64,
        sha256: Sha256::digest(&video_bytes),
    };
    let mut request = request(ApiFormat::GoogleGenerateContent);
    request.route.capabilities.insert(capability("input.audio"));
    request.route.capabilities.insert(capability("input.video"));
    request.input[0].content.extend([
        CanonicalContentPart::Audio(audio.clone()),
        CanonicalContentPart::Video(video.clone()),
    ]);
    let mut contents = ResolvedContents::default();
    for (reference, bytes) in [(&audio, audio_bytes), (&video, video_bytes)] {
        contents
            .insert(
                reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.into_boxed_slice()),
                },
            )
            .expect("resolved media");
    }

    let body = map_request(&request, &contents)
        .expect("native media mapping")
        .body;
    let parts = body["contents"][0]["parts"].as_array().expect("parts");
    assert!(
        parts
            .iter()
            .any(|part| part["inlineData"]["mimeType"] == "audio/mpeg")
    );
    assert!(
        parts
            .iter()
            .any(|part| part["inlineData"]["mimeType"] == "video/mp4")
    );
}

#[test]
fn multimodal_admission_requires_signed_modality_and_wire_dialect() {
    let bytes = b"audio-fixture".to_vec();
    let audio = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("audio/mpeg").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let mut request = request(ApiFormat::OpenAiChatCompletions);
    request.input[0]
        .content
        .push(CanonicalContentPart::Audio(audio.clone()));
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &audio,
            ResolvedContent::Inline {
                media_type: audio.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("resolved audio");
    assert_eq!(
        map_request(&request, &contents).expect_err("unsigned modality must fail"),
        MappingError::UnauthorizedInputModality,
    );
    request.route.capabilities.insert(capability("input.audio"));
    assert_eq!(
        map_request(&request, &contents).expect_err("missing dialect must fail"),
        MappingError::UnsupportedInputModality,
    );
    request
        .route
        .capabilities
        .insert(capability("multimodal_wire.openai_chat_qwen"));
    let body = map_request(&request, &contents)
        .expect("qwen audio mapping")
        .body;
    assert_eq!(body["messages"][1]["content"][1]["type"], "input_audio");
}

#[test]
fn provider_file_results_use_native_payloads_and_bounded_formats() {
    let bytes = b"%PDF-1.7 fixture".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/pdf").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(&bytes),
    };
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.input[2]
            .content
            .push(CanonicalContentPart::File(reference.clone()));
        if format == ApiFormat::GoogleInteractions {
            request = interactions_continuation(request);
        }
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.clone().into_boxed_slice()),
                },
            )
            .expect("resolved PDF");
        let body = map_request(&request, &contents)
            .unwrap_or_else(|error| panic!("{format:?} file result mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiChatCompletions => {
                let file = &body["messages"]
                    .as_array()
                    .expect("messages")
                    .last()
                    .expect("file")["content"][1]["file"];
                assert_eq!(file["filename"], "musteragent_attachment.pdf");
                // OpenAI documents file_data as a data URL, like image_url.
                assert!(
                    file["file_data"]
                        .as_str()
                        .is_some_and(|data| data.starts_with("data:application/pdf;base64,"))
                );
            }
            ApiFormat::OpenAiResponses => {
                let file = body["input"]
                    .as_array()
                    .expect("input")
                    .iter()
                    .find_map(|item| item["output"].as_array())
                    .and_then(|output| output.iter().find(|part| part["type"] == "input_file"))
                    .expect("input file");
                assert_eq!(file["filename"], "musteragent_attachment.pdf");
                assert!(
                    file["file_data"]
                        .as_str()
                        .is_some_and(|data| data.starts_with("data:application/pdf;base64,"))
                );
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                let source = &body["messages"]
                    .as_array()
                    .expect("messages")
                    .last()
                    .expect("result")["content"][0]["content"][1]["source"];
                assert_eq!(source["media_type"], "application/pdf");
                assert_eq!(source["type"], "base64");
            }
            ApiFormat::GoogleGenerateContent => {
                let file = &body["contents"]
                    .as_array()
                    .expect("contents")
                    .last()
                    .expect("result")["parts"][0]["functionResponse"]["parts"][0]["inlineData"];
                assert_eq!(file["mimeType"], "application/pdf");
            }
            ApiFormat::GoogleInteractions => {
                let result = body["input"]
                    .as_array()
                    .expect("input")
                    .iter()
                    .find(|step| step["call_id"] == "call_one")
                    .expect("result");
                assert_eq!(result["result"][1]["type"], "document");
                assert_eq!(result["result"][1]["mime_type"], "application/pdf");
                assert!(result["result"][1]["data"].is_string());
            }
            ApiFormat::BedrockConverse => {
                let document = &body["messages"]
                    .as_array()
                    .expect("messages")
                    .last()
                    .expect("result")["content"][0]["toolResult"]["content"][1]["document"];
                assert_eq!(document["format"], "pdf");
            }
        }
    }
}

#[test]
fn unsupported_reasoning_request_shapes_fail_before_network_io() {
    for format in [
        ApiFormat::AnthropicMessages,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.reasoning_policy = ReasoningPolicy::Summary;
        assert!(
            map_request(&request, &ResolvedContents::default()).is_err(),
            "{format:?} must not silently discard reasoning policy"
        );
    }
}

#[test]
fn anthropic_and_bedrock_reasoning_is_explicitly_registry_owned() {
    for format in [
        ApiFormat::AnthropicMessages,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.reasoning_policy = ReasoningPolicy::Summary;
        request.max_output_tokens = non_zero(4_096);
        request
            .route
            .capabilities
            .insert(capability("reasoning_style.anthropic_adaptive"));
        request.route.parameters.insert(
            capability("reasoning_effort"),
            ModelParameter::String("medium".into()),
        );
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} reasoning mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                assert_eq!(body["thinking"]["type"], "adaptive");
                assert_eq!(body["thinking"]["display"], "summarized");
                assert_eq!(body["output_config"]["effort"], "medium");
            }
            ApiFormat::BedrockConverse => {
                assert_eq!(
                    body["additionalModelRequestFields"]["thinking"]["type"],
                    "adaptive"
                );
                assert_eq!(
                    body["additionalModelRequestFields"]["output_config"]["effort"],
                    "medium"
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn exact_source_reasoning_replay_uses_each_protocol_native_item() {
    let cases = [
        (
            ApiFormat::OpenAiResponses,
            serde_json::json!({
                "type": "reasoning",
                "id": "rs_fixture",
                "summary": [],
                "encrypted_content": "opaque-openai",
            }),
        ),
        (
            ApiFormat::AnthropicMessages,
            serde_json::json!({
                "type": "thinking",
                "thinking": "provider-private",
                "signature": "opaque-anthropic",
            }),
        ),
        (
            ApiFormat::BedrockInvokeModel,
            serde_json::json!({
                "type": "thinking",
                "thinking": "provider-private",
                "signature": "opaque-bedrock-invoke",
            }),
        ),
        (
            ApiFormat::BedrockConverse,
            serde_json::json!({
                "reasoningContent": {
                    "reasoningText": {
                        "text": "provider-private",
                        "signature": "opaque-bedrock"
                    }
                }
            }),
        ),
    ];
    for (format, item) in cases {
        let mut request = request(format);
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema": "musteragent.legacy.runtime.provider-reasoning-replay.v2",
            "source_api_format": format.as_str(),
            "source_model_key": request.route.resolved_model_key,
            "items": [item.clone()],
        }))
        .expect("replay JSON");
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(
                "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            )
            .expect("media"),
            byte_length: u64::try_from(bytes.len()).expect("length"),
            sha256: Sha256::digest(&bytes),
        };
        request.input[1].reasoning = Some(ReasoningReplay {
            content_ref: reference.clone(),
            source_route_ref: ModelRouteRef::from_key(request.route.route_ref.clone()),
            continuation_ref: None,
        });
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.into_boxed_slice()),
                },
            )
            .expect("resolved reasoning replay");
        let body = map_request(&request, &contents)
            .unwrap_or_else(|error| panic!("{format:?} replay mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::OpenAiResponses => assert_eq!(body["input"][1], item),
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                assert_eq!(body["messages"][1]["content"][0], item);
            }
            ApiFormat::BedrockConverse => {
                assert_eq!(body["messages"][1]["content"][0], item);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn reasoning_output_policy_and_provider_effort_are_mapped_independently() {
    let mut chat = request(ApiFormat::OpenAiChatCompletions);
    chat.route
        .capabilities
        .insert(capability("max_completion_tokens"));
    chat.route
        .capabilities
        .insert(capability("reasoning_style.openai_effort"));
    chat.route.parameters.insert(
        capability("reasoning_effort"),
        ModelParameter::String("high".into()),
    );
    chat.reasoning_policy = ReasoningPolicy::Summary;
    let chat_body = map_request(&chat, &ResolvedContents::default())
        .expect("map OpenAI Chat reasoning")
        .body;
    assert_eq!(chat_body["max_completion_tokens"], 256);
    assert!(chat_body.get("max_tokens").is_none());
    assert_eq!(chat_body["reasoning_effort"], "high");

    let mut responses = request(ApiFormat::OpenAiResponses);
    responses.input.truncate(1);
    responses.route.parameters.insert(
        capability("reasoning_effort"),
        ModelParameter::String("medium".into()),
    );
    responses.reasoning_policy = ReasoningPolicy::Summary;
    let responses_body = map_request(&responses, &ResolvedContents::default())
        .expect("map Responses reasoning")
        .body;
    assert!(responses_body.get("reasoning_effort").is_none());
    assert_eq!(responses_body["reasoning"]["effort"], "medium");
    assert_eq!(responses_body["reasoning"]["summary"], "auto");

    for format in [
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
    ] {
        let mut request = request(format);
        request.route.parameters.insert(
            capability("reasoning_effort"),
            ModelParameter::String("low".into()),
        );
        request.reasoning_policy = ReasoningPolicy::Summary;
        if format == ApiFormat::GoogleInteractions {
            request.input.truncate(1);
        }
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} reasoning mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::GoogleGenerateContent => {
                assert_eq!(
                    body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
                    "low"
                );
                assert_eq!(
                    body["generationConfig"]["thinkingConfig"]["includeThoughts"],
                    true
                );
            }
            ApiFormat::GoogleInteractions => {
                assert_eq!(body["generation_config"]["thinking_level"], "low");
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn openai_chat_replays_private_reasoning_only_under_the_signed_same_route_contract() {
    let bytes = b"provider-private reasoning".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(
            "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
        )
        .expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(&bytes),
    };
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("resolved reasoning replay");

    let mut same_route = request(ApiFormat::OpenAiChatCompletions);
    same_route
        .route
        .capabilities
        .insert(capability("reasoning_echo.required"));
    same_route.input[1].reasoning = Some(ReasoningReplay {
        content_ref: reference.clone(),
        source_route_ref: ModelRouteRef::from_key(same_route.route.route_ref.clone()),
        continuation_ref: None,
    });
    let mapped = map_request(&same_route, &contents).expect("map same-route replay");
    assert_eq!(
        mapped.body["messages"][2]["reasoning_content"],
        "provider-private reasoning"
    );

    let mut continuation = same_route.clone();
    continuation.input.truncate(2);
    continuation.input[1].content.clear();
    continuation.input[1].tool_calls.clear();
    continuation.runtime_events = vec![CanonicalRuntimeEvent::OutputLimitContinuation {
        source_invocation_id: InvocationId::generate(),
    }];
    let continued = map_request(&continuation, &contents).expect("private-only continuation");
    assert_eq!(
        continued.body["messages"][2]["reasoning_content"],
        "provider-private reasoning"
    );
    assert!(
        continued.body["messages"][3]["content"]
            .as_str()
            .expect("runtime input")
            .contains("output limit continuation")
    );

    let mut incompatible = same_route.clone();
    incompatible.input[1]
        .reasoning
        .as_mut()
        .expect("reasoning")
        .source_route_ref = ModelRouteRef::from_key(versioned("model.route.other@1"));
    let required = map_request(&incompatible, &contents).expect("map required incompatible replay");
    assert_eq!(required.body["messages"][2]["reasoning_content"], " ");

    incompatible.route.capabilities.clear();
    incompatible
        .route
        .capabilities
        .insert(capability("reasoning_echo.allowed"));
    let allowed = map_request(&incompatible, &contents).expect("map allowed replay");
    assert!(
        allowed.body["messages"][2]
            .get("reasoning_content")
            .is_none()
    );

    incompatible.route.capabilities.clear();
    incompatible
        .route
        .capabilities
        .insert(capability("reasoning_echo.forbidden"));
    incompatible.input[1]
        .reasoning
        .as_mut()
        .expect("reasoning")
        .source_route_ref = ModelRouteRef::from_key(incompatible.route.route_ref.clone());
    let forbidden = map_request(&incompatible, &contents).expect("map forbidden replay");
    assert!(
        forbidden.body["messages"][2]
            .get("reasoning_content")
            .is_none()
    );
}

#[test]
fn openai_compatible_reasoning_styles_are_registry_owned() {
    let cases = [
        ("reasoning_style.provider_default", "medium", None, None),
        (
            "reasoning_style.openai_effort",
            "medium",
            Some("medium"),
            None,
        ),
        (
            "reasoning_style.openrouter",
            "high",
            None,
            Some(serde_json::json!({"enabled": true, "effort": "high"})),
        ),
        (
            "reasoning_style.kimi",
            "none",
            None,
            Some(serde_json::json!({"type": "disabled"})),
        ),
    ];
    for (style, effort, top_level_effort, structured) in cases {
        let mut request = request(ApiFormat::OpenAiChatCompletions);
        request.route.capabilities.insert(capability(style));
        request.route.parameters.insert(
            capability("reasoning_effort"),
            ModelParameter::String(effort.into()),
        );
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{style} mapping failed: {error}"))
            .body;
        assert_eq!(
            body.get("reasoning_effort")
                .and_then(serde_json::Value::as_str),
            top_level_effort
        );
        if style.ends_with("openrouter") {
            assert_eq!(body.get("reasoning"), structured.as_ref());
        } else if style.ends_with("kimi") {
            assert_eq!(body.get("thinking"), structured.as_ref());
        }
    }
}

#[test]
fn signed_reasoning_effort_is_independent_of_output_policy_across_api_formats() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.reasoning_policy = ReasoningPolicy::Disabled;
        request.route.parameters.insert(
            capability("reasoning_effort"),
            ModelParameter::String("medium".into()),
        );
        if matches!(
            format,
            ApiFormat::AnthropicMessages
                | ApiFormat::BedrockConverse
                | ApiFormat::BedrockInvokeModel
        ) {
            request
                .route
                .capabilities
                .insert(capability("reasoning_style.anthropic_adaptive"));
            request.max_output_tokens = non_zero(4_096);
        }
        if format == ApiFormat::OpenAiChatCompletions {
            request
                .route
                .capabilities
                .insert(capability("reasoning_style.openai_effort"));
        }
        if format == ApiFormat::GoogleInteractions {
            request.input.truncate(1);
        }

        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} effort mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiChatCompletions => {
                assert_eq!(body["reasoning_effort"], "medium");
            }
            ApiFormat::OpenAiResponses => {
                assert_eq!(body["reasoning"]["effort"], "medium");
                assert!(body["reasoning"].get("summary").is_none());
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                assert_eq!(body["thinking"]["type"], "adaptive");
                assert_eq!(body["output_config"]["effort"], "medium");
            }
            ApiFormat::GoogleGenerateContent => {
                assert_eq!(
                    body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
                    "medium"
                );
                assert!(
                    body["generationConfig"]["thinkingConfig"]
                        .get("includeThoughts")
                        .is_none()
                );
            }
            ApiFormat::GoogleInteractions => {
                assert_eq!(body["generation_config"]["thinking_level"], "medium");
            }
            ApiFormat::BedrockConverse => {
                assert_eq!(
                    body["additionalModelRequestFields"]["output_config"]["effort"],
                    "medium"
                );
            }
        }
    }
}

#[test]
fn typed_reasoning_budget_is_limited_to_the_matching_native_style() {
    for format in [
        ApiFormat::AnthropicMessages,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request.reasoning_policy = ReasoningPolicy::Summary;
        request.max_output_tokens = non_zero(4_096);
        request
            .route
            .capabilities
            .insert(capability("reasoning_style.anthropic_budget"));
        request.route.parameters.insert(
            capability("reasoning_budget_tokens"),
            ModelParameter::Int(2_048),
        );
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} budget mapping failed: {error}"))
            .body;
        let budget = match format {
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                &body["thinking"]["budget_tokens"]
            }
            ApiFormat::BedrockConverse => {
                &body["additionalModelRequestFields"]["thinking"]["budget_tokens"]
            }
            _ => unreachable!(),
        };
        assert_eq!(budget, 2_048, "{format:?}");
    }

    let mut wrong_style = request(ApiFormat::AnthropicMessages);
    wrong_style
        .route
        .capabilities
        .insert(capability("reasoning_style.anthropic_adaptive"));
    wrong_style.route.parameters.insert(
        capability("reasoning_budget_tokens"),
        ModelParameter::Int(2_048),
    );
    assert_eq!(
        mapping_error(&wrong_style),
        MappingError::UnsupportedReasoningPolicy
    );
}

#[test]
fn reasoning_preflight_rejects_cross_format_styles_and_untyped_parameter_bypasses() {
    let incompatible_styles = [
        (
            ApiFormat::OpenAiChatCompletions,
            "reasoning_style.anthropic_adaptive",
        ),
        (ApiFormat::OpenAiResponses, "reasoning_style.openai_effort"),
        (ApiFormat::AnthropicMessages, "reasoning_style.openrouter"),
        (ApiFormat::GoogleGenerateContent, "reasoning_style.kimi"),
        (ApiFormat::GoogleInteractions, "reasoning_style.openrouter"),
        (ApiFormat::BedrockConverse, "reasoning_style.openai_effort"),
        (ApiFormat::BedrockInvokeModel, "reasoning_style.kimi"),
    ];
    for (format, style) in incompatible_styles {
        let mut request = request(format);
        request.route.capabilities.insert(capability(style));
        assert_eq!(
            mapping_error(&request),
            MappingError::UnsupportedReasoningPolicy,
            "{format:?} accepted {style}"
        );
    }

    let mut duplicate = request(ApiFormat::OpenAiChatCompletions);
    duplicate
        .route
        .capabilities
        .insert(capability("reasoning_style.openai_effort"));
    duplicate
        .route
        .capabilities
        .insert(capability("reasoning_style.openrouter"));
    assert_eq!(
        mapping_error(&duplicate),
        MappingError::InvalidRouteConfiguration
    );

    let mut unknown = request(ApiFormat::OpenAiChatCompletions);
    unknown
        .route
        .capabilities
        .insert(capability("reasoning_style.guess_from_model"));
    assert_eq!(
        mapping_error(&unknown),
        MappingError::UnsupportedReasoningPolicy
    );

    for parameter in [
        "reasoning",
        "thinking",
        "reasoning_request_style",
        "reasoning_summary",
        "include",
    ] {
        let mut request = request(ApiFormat::OpenAiChatCompletions);
        request.route.parameters.insert(
            capability(parameter),
            ModelParameter::String("caller-owned".into()),
        );
        assert_eq!(
            mapping_error(&request),
            MappingError::UnsupportedReasoningPolicy,
            "raw route parameter bypassed reasoning owner: {parameter}"
        );
    }

    let mut wrong_effort_type = request(ApiFormat::OpenAiResponses);
    wrong_effort_type
        .route
        .parameters
        .insert(capability("reasoning_effort"), ModelParameter::Bool(true));
    assert_eq!(
        mapping_error(&wrong_effort_type),
        MappingError::InvalidRouteConfiguration
    );

    let mut chat_budget = request(ApiFormat::OpenAiChatCompletions);
    chat_budget.route.parameters.insert(
        capability("reasoning_budget_tokens"),
        ModelParameter::Int(2_048),
    );
    assert_eq!(
        mapping_error(&chat_budget),
        MappingError::UnsupportedReasoningPolicy
    );
}

#[test]
fn reasoning_owner_parameter_families_fail_closed_across_every_api_format() {
    let aliases = [
        "reasoning_content",
        "reasoning_format",
        "reasoning_enabled",
        "include_reasoning",
        "reasoning.details",
        "thinking-budget",
        "include_thoughts",
        "reasoningcontent",
        "thinkingconfig",
        "includereasoning",
        "cotconfig",
        "enablecot",
    ];
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        for alias in aliases {
            let mut request = request(format);
            if format == ApiFormat::GoogleInteractions {
                request.input.truncate(1);
            }
            request.route.parameters.insert(
                capability(alias),
                ModelParameter::String("caller-owned".into()),
            );
            assert_eq!(
                mapping_error(&request),
                MappingError::UnsupportedReasoningPolicy,
                "{format:?} accepted reasoning owner alias {alias}"
            );
        }
    }
}

#[test]
fn signed_route_tuning_allowlist_maps_approved_parameters_across_every_api_format() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        if format == ApiFormat::GoogleInteractions {
            request.input.truncate(1);
        }
        request
            .route
            .parameters
            .insert(capability("temperature"), ModelParameter::Double(0.4));
        request
            .route
            .parameters
            .insert(capability("top_p"), ModelParameter::Double(0.8));
        if matches!(
            format,
            ApiFormat::OpenAiChatCompletions | ApiFormat::OpenAiResponses
        ) {
            request
                .route
                .parameters
                .insert(capability("frequency_penalty"), ModelParameter::Double(0.2));
            request
                .route
                .parameters
                .insert(capability("presence_penalty"), ModelParameter::Double(0.1));
            request
                .route
                .parameters
                .insert(capability("seed"), ModelParameter::Int(7));
            request.route.parameters.insert(
                capability("parallel_tool_calls"),
                ModelParameter::Bool(false),
            );
            request.route.parameters.insert(
                capability("tool_choice"),
                ModelParameter::String("auto".into()),
            );
        }
        if matches!(
            format,
            ApiFormat::AnthropicMessages
                | ApiFormat::GoogleGenerateContent
                | ApiFormat::GoogleInteractions
                | ApiFormat::BedrockInvokeModel
        ) {
            request
                .route
                .parameters
                .insert(capability("top_k"), ModelParameter::Int(40));
        }

        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} tuning mapping failed: {error}"))
            .body;
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiChatCompletions | ApiFormat::OpenAiResponses => {
                assert_eq!(body["temperature"], 0.4);
                assert_eq!(body["top_p"], 0.8);
                assert_eq!(body["frequency_penalty"], 0.2);
                assert_eq!(body["presence_penalty"], 0.1);
                assert_eq!(body["seed"], 7);
                assert_eq!(body["parallel_tool_calls"], false);
                assert_eq!(body["tool_choice"], "auto");
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                assert_eq!(body["temperature"], 0.4);
                assert_eq!(body["top_p"], 0.8);
                assert_eq!(body["top_k"], 40);
            }
            ApiFormat::GoogleGenerateContent => {
                assert_eq!(body["generationConfig"]["temperature"], 0.4);
                assert_eq!(body["generationConfig"]["topP"], 0.8);
                assert_eq!(body["generationConfig"]["topK"], 40);
            }
            ApiFormat::GoogleInteractions => {
                assert_eq!(body["generation_config"]["temperature"], 0.4);
                assert_eq!(body["generation_config"]["top_p"], 0.8);
                assert_eq!(body["generation_config"]["top_k"], 40);
            }
            ApiFormat::BedrockConverse => {
                assert_eq!(body["inferenceConfig"]["temperature"], 0.4);
                assert_eq!(body["inferenceConfig"]["topP"], 0.8);
            }
        }
    }
}

#[test]
fn signed_route_rejects_every_non_tuning_canonical_owner_parameter() {
    let forbidden = [
        "model",
        "messages",
        "input",
        "contents",
        "instructions",
        "system",
        "system_instruction",
        "max_completion_tokens",
        "max_tokens",
        "max_output_tokens",
        "tools",
        "tool_config",
        "response_schema",
        "response_format",
        "json_schema",
        "text",
        "output_config",
        "previous_response_id",
        "previous_interaction_id",
        "continuation",
        "continuation_ref",
        "cache",
        "cache_control",
        "cached_content",
        "prompt_cache_key",
        "prompt_cache_retention",
        "store",
        "stream",
        "stream_options",
        "extra_body",
        "generation_config",
        "additional_model_request_fields",
        "anthropic_beta",
        "anthropic_version",
        "transport",
        "timeout",
        "headers",
        "unknown_vendor_flag",
    ];
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        for parameter in forbidden {
            let mut request = request(format);
            if format == ApiFormat::GoogleInteractions {
                request.input.truncate(1);
            }
            request.route.parameters.insert(
                capability(parameter),
                ModelParameter::String("caller-owned".into()),
            );
            assert_eq!(
                mapping_error(&request),
                MappingError::InvalidRouteConfiguration,
                "{format:?} accepted canonical owner parameter {parameter}"
            );
        }
    }
}

#[test]
fn registry_tool_controls_are_closed_typed_and_openai_format_owned() {
    let mut wrong_format = request(ApiFormat::AnthropicMessages);
    wrong_format.route.parameters.insert(
        capability("tool_choice"),
        ModelParameter::String("auto".into()),
    );
    assert_eq!(
        mapping_error(&wrong_format),
        MappingError::InvalidRouteConfiguration
    );

    let mut wrong_choice = request(ApiFormat::OpenAiResponses);
    wrong_choice.route.parameters.insert(
        capability("tool_choice"),
        ModelParameter::String("caller_json".into()),
    );
    assert_eq!(
        mapping_error(&wrong_choice),
        MappingError::InvalidRouteConfiguration
    );

    let mut wrong_parallel_type = request(ApiFormat::OpenAiChatCompletions);
    wrong_parallel_type.route.parameters.insert(
        capability("parallel_tool_calls"),
        ModelParameter::String("false".into()),
    );
    assert_eq!(
        mapping_error(&wrong_parallel_type),
        MappingError::InvalidRouteConfiguration
    );
}

#[test]
fn conflicting_openai_token_bound_aliases_fail_closed_instead_of_emitting_dual_fields() {
    let mut legacy_primary = request(ApiFormat::OpenAiChatCompletions);
    legacy_primary.route.parameters.insert(
        capability("max_completion_tokens"),
        ModelParameter::Int(999_999),
    );
    assert_eq!(
        mapping_error(&legacy_primary),
        MappingError::InvalidRouteConfiguration
    );

    let mut modern_primary = request(ApiFormat::OpenAiChatCompletions);
    modern_primary
        .route
        .capabilities
        .insert(capability("max_completion_tokens"));
    modern_primary
        .route
        .parameters
        .insert(capability("max_tokens"), ModelParameter::Int(999_999));
    assert_eq!(
        mapping_error(&modern_primary),
        MappingError::InvalidRouteConfiguration
    );
}

#[test]
fn strict_tools_are_omitted_unless_the_signed_route_authorizes_them() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let without_capability = map_request(&request(format), &ResolvedContents::default())
            .expect("map route without strict capability")
            .body;
        assert!(
            strict_value(format, &without_capability).is_none(),
            "{format:?} must omit unsupported strict fields"
        );

        let mut with_capability = request(format);
        with_capability
            .route
            .capabilities
            .insert(capability("strict_tool_schema"));
        let with_capability = map_request(&with_capability, &ResolvedContents::default())
            .expect("map strict route")
            .body;
        assert_eq!(
            strict_value(format, &with_capability),
            Some(true),
            "{format:?}"
        );
    }
}

#[test]
fn json_schema_output_uses_each_protocols_native_shape() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let mut request = request(format);
        request
            .route
            .capabilities
            .insert(capability("json_schema_output"));
        let definition = tool_definition();
        request.response_schema = Some((
            definition.input_schema_ref.clone(),
            definition.input_schema.clone(),
        ));
        if format == ApiFormat::GoogleInteractions {
            request.input.truncate(1);
        }
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|error| panic!("{format:?} structured output failed: {error}"))
            .body;
        match format {
            ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
                unreachable!("decision protocols do not use chat fixtures")
            }
            ApiFormat::OpenAiChatCompletions => {
                assert_eq!(body["response_format"]["type"], "json_schema");
            }
            ApiFormat::OpenAiResponses => {
                assert_eq!(body["text"]["format"]["type"], "json_schema");
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
                assert_eq!(body["output_config"]["format"]["type"], "json_schema");
            }
            ApiFormat::GoogleGenerateContent => {
                assert_eq!(
                    body["generationConfig"]["responseMimeType"],
                    "application/json"
                );
            }
            ApiFormat::GoogleInteractions => {
                assert_eq!(body["response_format"]["type"], "text");
                assert_eq!(body["response_format"]["mime_type"], "application/json");
            }
            ApiFormat::BedrockConverse => {
                assert_eq!(body["outputConfig"]["textFormat"]["type"], "json_schema");
                assert!(
                    body["outputConfig"]["textFormat"]["structure"]["jsonSchema"]["schema"]
                        .as_str()
                        .is_some_and(|schema| schema.contains("\"type\":\"object\""))
                );
            }
        }
    }
}

#[test]
fn json_mode_cannot_authorize_a_schema_constrained_request() {
    let mut request = request(ApiFormat::OpenAiResponses);
    request.route.capabilities.insert(capability("json_mode"));
    let definition = tool_definition();
    request.response_schema = Some((
        definition.input_schema_ref.clone(),
        definition.input_schema.clone(),
    ));

    assert!(matches!(
        map_request(&request, &ResolvedContents::default()),
        Err(MappingError::UnsupportedJsonSchemaOutput)
    ));
}

#[test]
fn provider_default_output_limit_omits_the_field_except_where_required() {
    for format in [ApiFormat::OpenAiChatCompletions, ApiFormat::OpenAiResponses] {
        let mut sized = request(format);
        if format == ApiFormat::OpenAiResponses {
            sized.input.truncate(1);
        }
        let body = map_request(&sized, &ResolvedContents::default())
            .expect("map sized route")
            .body;
        assert!(body.get("max_tokens").is_some() || body.get("max_output_tokens").is_some());

        let mut open = sized.clone();
        open.route
            .capabilities
            .insert(capability("max_output.provider_default"));
        let body = map_request(&open, &ResolvedContents::default())
            .expect("map provider-default route")
            .body;
        assert!(body.get("max_tokens").is_none());
        assert!(body.get("max_completion_tokens").is_none());
        assert!(body.get("max_output_tokens").is_none());
    }

    // Anthropic Messages requires max_tokens, so it is always sent.
    let mut anthropic = request(ApiFormat::AnthropicMessages);
    anthropic
        .route
        .capabilities
        .insert(capability("max_output.provider_default"));
    let body = map_request(&anthropic, &ResolvedContents::default())
        .expect("map Anthropic route")
        .body;
    assert_eq!(body["max_tokens"], 256);
}
