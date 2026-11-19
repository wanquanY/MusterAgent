mod google;
use google::{google_generate, google_interactions};
mod runtime_events;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use musteragent_content_ref::ContentRef;
use musteragent_model::{ApiFormat, AuxiliaryModelRole, ProviderStatePolicy};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalToolDefinition, JsonValue, ReasoningPolicy,
};
use secrecy::ExposeSecret as _;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::content::{ContentResolverError, ResolvedContent, ResolvedContents};
use crate::reasoning::{
    apply_anthropic_reasoning, apply_bedrock_reasoning, apply_google_generate_reasoning,
    apply_google_interactions_reasoning, apply_openai_chat_reasoning,
    apply_openai_responses_reasoning, preflight_reasoning, reasoning_requested,
};
use crate::route_parameters::{
    apply_flat_signed_route_parameters, apply_nested_signed_route_parameters,
    is_approved_compression_tuning, preflight_signed_route_parameters,
};
use crate::wire::MappedRequest;

const EMPTY_TOOL_OUTPUT: &str = "(no tool output)";
const EMPTY_BEDROCK_TEXT: &str = "<empty>";

pub fn map_request(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<MappedRequest, MappingError> {
    preflight_request(request)?;
    if request.response_schema.is_some() && !has_json_schema_output_capability(request) {
        return Err(MappingError::UnsupportedJsonSchemaOutput);
    }
    if request.route.api_format == ApiFormat::OpenAiChatCompletions
        && reasoning_requested(request)?
        && !request.tools.is_empty()
        && request.route.capabilities.iter().any(|capability| {
            capability.as_str() == "responses_api_required_for_reasoning_with_tools"
        })
    {
        return Err(MappingError::ExactProviderReplayRequired);
    }
    let body = match request.route.api_format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            return Err(MappingError::InvalidCanonicalRequest);
        }
        ApiFormat::OpenAiChatCompletions => openai_chat(request, contents)?,
        ApiFormat::OpenAiResponses => openai_responses(request, contents)?,
        ApiFormat::AnthropicMessages => anthropic(request, contents, false)?,
        ApiFormat::GoogleGenerateContent => google_generate(request, contents)?,
        ApiFormat::GoogleInteractions => google_interactions(request, contents)?,
        ApiFormat::BedrockConverse => {
            return Ok(MappedRequest::aws_event_stream(bedrock_converse(
                request, contents,
            )?));
        }
        ApiFormat::BedrockInvokeModel => {
            return Ok(MappedRequest::aws_event_stream(anthropic(
                request, contents, true,
            )?));
        }
    };
    Ok(MappedRequest::json_sse(body))
}

/// Content-independent validation that must run before endpoint lookup,
/// credential/budget consumption, Secret decryption, or Content resolution.
/// `map_request` repeats it defensively for callers outside the Direct
/// Provider orchestration path.
pub(crate) fn preflight_request(request: &CanonicalModelRequest) -> Result<(), MappingError> {
    request
        .validate()
        .map_err(|_| MappingError::InvalidCanonicalRequest)?;
    if request.runtime_events.iter().any(|event| {
        matches!(
            event,
            musteragent_runtime_model::CanonicalRuntimeEvent::AgentDialogueHistory { .. }
        )
    }) {
        // Stored requests from before the native-turn cutover require replanning.
        return Err(MappingError::InvalidCanonicalRequest);
    }
    validate_auxiliary_request_ownership(request)?;
    validate_input_modalities(request)?;
    preflight_reasoning(request)?;
    if matches!(
        request.route.api_format,
        ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
    ) && request.max_output_tokens.get() > 65_536
        && !has_capability(request, "usage_aggregates_requests")
    {
        return Err(MappingError::InvalidRouteConfiguration);
    }

    preflight_signed_route_parameters(request)?;
    Ok(())
}

fn validate_input_modalities(request: &CanonicalModelRequest) -> Result<(), MappingError> {
    // The route owns one native-input rule shared with the context planner;
    // the planner already turned anything it cannot carry into reader notices.
    let route = &request.route;
    for part in request.input.iter().flat_map(|message| &message.content) {
        let (authorized, carried) = match part {
            CanonicalContentPart::Text(_) => continue,
            CanonicalContentPart::Image(_) => (
                has_capability(request, "input.image") || has_capability(request, "vision"),
                route.accepts_native_image(),
            ),
            CanonicalContentPart::Audio(_) => (
                has_capability(request, "input.audio"),
                route.accepts_native_audio(),
            ),
            CanonicalContentPart::Video(_) => (
                has_capability(request, "input.video"),
                route.accepts_native_video(),
            ),
            CanonicalContentPart::File(reference) => (
                has_capability(request, "input.document")
                    || has_capability(request, "document_input"),
                route.accepts_native_document(reference.media_type.as_str()),
            ),
        };
        if !authorized {
            return Err(MappingError::UnauthorizedInputModality);
        }
        if !carried {
            return Err(MappingError::UnsupportedInputModality);
        }
    }
    Ok(())
}

fn openai_chat(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut messages = Vec::new();
    messages.push(json!({
        "role": "system",
        "content": join_instructions(request),
    }));
    let mut deferred_tool_media = Vec::new();
    for message in &request.input {
        let content_parts = message.content.as_slice();
        if message.role != CanonicalRole::Tool {
            flush_openai_tool_media(request, &mut messages, &mut deferred_tool_media, contents)?;
        }
        let mut value = Map::new();
        value.insert(
            "role".into(),
            Value::String(
                match message.role {
                    CanonicalRole::User => "user",
                    CanonicalRole::Assistant => "assistant",
                    CanonicalRole::Tool => "tool",
                }
                .into(),
            ),
        );
        value.insert(
            "content".into(),
            if message.role == CanonicalRole::Assistant && content_parts.is_empty() {
                Value::Null
            } else if message.role == CanonicalRole::Tool {
                let text = tool_text(content_parts);
                Value::String(if text.trim().is_empty() {
                    if content_parts.iter().any(|part| {
                        matches!(
                            part,
                            CanonicalContentPart::Image(_)
                                | CanonicalContentPart::Audio(_)
                                | CanonicalContentPart::Video(_)
                                | CanonicalContentPart::File(_)
                        )
                    }) {
                        "(see attached media)".to_owned()
                    } else {
                        EMPTY_TOOL_OUTPUT.to_owned()
                    }
                } else {
                    text
                })
            } else {
                openai_chat_content(request, content_parts, contents)?
            },
        );
        if message.role == CanonicalRole::Assistant && !message.tool_calls.is_empty() {
            value.insert(
                "tool_calls".into(),
                Value::Array(
                    message
                        .tool_calls
                        .iter()
                        .map(|call| {
                            Ok(json!({
                                "id": provider_tool_call_id(
                                    ApiFormat::OpenAiChatCompletions,
                                    call.call_id.as_str(),
                                ),
                                "type": "function",
                                "function": {
                                    "name": provider_tool_name(request, call.name.as_str())?,
                                    "arguments": serde_json::to_string(&to_serde(&call.arguments)?)
                                        .map_err(|_| MappingError::JsonEncoding)?,
                                },
                            }))
                        })
                        .collect::<Result<_, MappingError>>()?,
                ),
            );
        }
        if message.role == CanonicalRole::Assistant
            && (message.reasoning.is_some() || !message.tool_calls.is_empty())
            && let Some(reasoning_content) =
                openai_chat_reasoning_content(request, message, contents)?
        {
            value.insert("reasoning_content".into(), Value::String(reasoning_content));
        }
        if message.role == CanonicalRole::Tool {
            value.insert(
                "tool_call_id".into(),
                Value::String(provider_tool_call_id(
                    ApiFormat::OpenAiChatCompletions,
                    message
                        .tool_call_id
                        .as_ref()
                        .ok_or(MappingError::InvalidCanonicalRequest)?
                        .as_str(),
                )),
            );
            deferred_tool_media.extend(content_parts.iter().filter_map(|part| match part {
                CanonicalContentPart::Image(_)
                | CanonicalContentPart::Audio(_)
                | CanonicalContentPart::Video(_)
                | CanonicalContentPart::File(_) => Some(part.clone()),
                CanonicalContentPart::Text(_) => None,
            }));
        }
        messages.push(Value::Object(value));
    }
    flush_openai_tool_media(request, &mut messages, &mut deferred_tool_media, contents)?;
    for event in request.runtime_events.iter() {
        messages.push(
            json!({"role":"user", "content":runtime_events::chat(event, request, contents)?}),
        );
    }

    let mut body = json!({
        "model": request.route.resolved_model_key,
        "messages": messages,
        "stream": true,
        "stream_options": {"include_usage": true},
    });
    if !provider_default_output_limit(request) {
        body[if has_capability(request, "max_completion_tokens") {
            "max_completion_tokens"
        } else {
            "max_tokens"
        }] = json!(request.max_output_tokens.get());
    }
    apply_flat_signed_route_parameters(&mut body, request)?;
    apply_openai_chat_reasoning(&mut body, request)?;
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| openai_tool(tool, supports_strict_tool_schema(request)))
                .collect::<Result<_, _>>()?,
        );
    }
    if let Some((_, schema)) = &request.response_schema {
        body["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "musteragent_response",
                "strict": true,
                "schema": openai_schema(schema)?,
            }
        });
    }
    Ok(body)
}

fn flush_openai_tool_media(
    request: &CanonicalModelRequest,
    messages: &mut Vec<Value>,
    deferred: &mut Vec<CanonicalContentPart>,
    contents: &ResolvedContents,
) -> Result<(), MappingError> {
    if deferred.is_empty() {
        return Ok(());
    }
    let mut content = vec![json!({
        "type": "text",
        "text": "Attached media from the preceding Tool results:",
    })];
    for part in deferred.drain(..) {
        match part {
            CanonicalContentPart::Image(reference) => content.push(json!({
                "type": "image_url",
                "image_url": {"url": data_or_url(&reference, contents)?},
            })),
            CanonicalContentPart::File(reference) => content.push(json!({
                "type": "file",
                "file": openai_file_payload(&reference, contents)?,
            })),
            CanonicalContentPart::Audio(reference) => {
                content.push(openai_chat_audio(request, &reference, contents)?)
            }
            CanonicalContentPart::Video(reference) => {
                content.push(openai_chat_video(request, &reference, contents)?)
            }
            CanonicalContentPart::Text(_) => return Err(MappingError::InvalidCanonicalRequest),
        }
    }
    messages.push(json!({"role": "user", "content": content}));
    Ok(())
}

fn openai_responses(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut input = Vec::new();
    for message in &request.input {
        let content_parts = message.content.as_slice();
        if message.role == CanonicalRole::Tool {
            input.push(json!({
                "type": "function_call_output",
                "call_id": tool_result_call_id(
                    request,
                    message.tool_call_id.as_ref()
                        .ok_or(MappingError::InvalidCanonicalRequest)?.as_str(),
                ),
                "output": responses_tool_output(request, content_parts, contents)?,
            }));
            continue;
        }
        if message.role == CanonicalRole::Assistant {
            input.extend(provider_reasoning_replay_items(
                request,
                message,
                contents,
                ApiFormat::OpenAiResponses,
            )?);
        }
        let role = if message.role == CanonicalRole::Assistant {
            "assistant"
        } else {
            "user"
        };
        if !content_parts.is_empty() {
            let content = content_parts
                .iter()
                .map(|part| responses_part(request, part, contents, role == "assistant"))
                .collect::<Result<Vec<_>, _>>()?;
            input.push(json!({"type": "message", "role": role, "content": content}));
        }
        for call in &message.tool_calls {
            input.push(json!({
                "type": "function_call",
                "call_id": provider_tool_call_id(
                    ApiFormat::OpenAiResponses,
                    call.call_id.as_str(),
                ),
                "name": provider_tool_name(request, call.name.as_str())?,
                "arguments": serde_json::to_string(&to_serde(&call.arguments)?)
                    .map_err(|_| MappingError::JsonEncoding)?,
            }));
        }
    }
    for event in request.runtime_events.iter() {
        input.push(json!({"type":"message", "role":"user", "content":runtime_events::parts(event, request, contents, runtime_events::Wire::Responses)?}));
    }

    let mut body = json!({
        "model": request.route.resolved_model_key,
        "instructions": join_instructions(request),
        "input": input,
        "stream": true,
        "store": provider_state_persistence_enabled(request),
        "max_output_tokens": request.max_output_tokens.get(),
    });
    if provider_default_output_limit(request) {
        body.as_object_mut()
            .ok_or(MappingError::JsonEncoding)?
            .remove("max_output_tokens");
    }
    if let Some(continuation) = &request.continuation_ref {
        body["previous_response_id"] = json!(continuation.as_str());
    }
    apply_flat_signed_route_parameters(&mut body, request)?;
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    let mut definition = json!({
                        "type": "function",
                        "name": tool.model_name.as_str(),
                        "description": tool.description.as_str(),
                        "parameters": openai_tool_parameters(tool)?,
                    });
                    if supports_strict_tool_schema(request) {
                        definition["strict"] = Value::Bool(tool.strict);
                    }
                    Ok(definition)
                })
                .collect::<Result<_, MappingError>>()?,
        );
    }
    apply_openai_responses_reasoning(&mut body, request)?;
    if let Some((_, schema)) = &request.response_schema {
        body["text"] = json!({"format": {
            "type": "json_schema",
            "name": "musteragent_response",
            "strict": true,
            "schema": openai_schema(schema)?,
        }});
    }
    Ok(body)
}

fn anthropic(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
    bedrock_invoke: bool,
) -> Result<Value, MappingError> {
    let system = request
        .instructions
        .iter()
        .map(|text| json!({"type": "text", "text": text.as_str()}))
        .collect::<Vec<_>>();
    let mut messages = Vec::new();
    for message in &request.input {
        let content_parts = message.content.as_slice();
        match message.role {
            CanonicalRole::User | CanonicalRole::Assistant => {
                let mut content = if message.role == CanonicalRole::Assistant {
                    provider_reasoning_replay_items(
                        request,
                        message,
                        contents,
                        if bedrock_invoke {
                            ApiFormat::BedrockInvokeModel
                        } else {
                            ApiFormat::AnthropicMessages
                        },
                    )?
                } else {
                    Vec::new()
                };
                content.extend(
                    content_parts
                        .iter()
                        .map(|part| anthropic_part(part, contents))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                for call in &message.tool_calls {
                    content.push(json!({
                        "type": "tool_use",
                        "id": provider_tool_call_id(
                            if bedrock_invoke {
                                ApiFormat::BedrockInvokeModel
                            } else {
                                ApiFormat::AnthropicMessages
                            },
                            call.call_id.as_str(),
                        ),
                        "name": provider_tool_name(request, call.name.as_str())?,
                        "input": to_serde(&call.arguments)?,
                    }));
                }
                push_message_content(
                    &mut messages,
                    if message.role == CanonicalRole::Assistant {
                        "assistant"
                    } else {
                        "user"
                    },
                    content,
                )?;
            }
            CanonicalRole::Tool => push_message_content(
                &mut messages,
                "user",
                vec![json!({
                    "type": "tool_result",
                    "tool_use_id": provider_tool_call_id(
                        if bedrock_invoke {
                            ApiFormat::BedrockInvokeModel
                        } else {
                            ApiFormat::AnthropicMessages
                        },
                        message.tool_call_id.as_ref()
                            .ok_or(MappingError::InvalidCanonicalRequest)?.as_str(),
                    ),
                    "content": anthropic_tool_output(content_parts, contents)?,
                })],
            )?,
        }
    }
    for event in request.runtime_events.iter() {
        push_message_content(
            &mut messages,
            "user",
            runtime_events::parts(event, request, contents, runtime_events::Wire::Anthropic)?,
        )?;
    }
    let mut body = json!({
        "model": request.route.resolved_model_key,
        "system": system,
        "messages": messages,
        "max_tokens": request.max_output_tokens.get(),
        "stream": true,
    });
    apply_flat_signed_route_parameters(&mut body, request)?;
    if bedrock_invoke {
        body.as_object_mut()
            .ok_or(MappingError::JsonEncoding)?
            .remove("model");
        body["anthropic_version"] = Value::String("bedrock-2023-05-31".into());
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    let mut definition = json!({
                        "name": tool.model_name.as_str(),
                        "description": tool.description.as_str(),
                        "input_schema": crate::tool_schema::provider_tool_schema(&tool.input_schema)?,
                    });
                    if tool.strict && supports_strict_tool_schema(request) {
                        definition["strict"] = Value::Bool(true);
                    }
                    Ok(definition)
                })
                .collect::<Result<_, MappingError>>()?,
        );
    }
    apply_anthropic_reasoning(&mut body, request)?;
    if let Some((_, schema)) = &request.response_schema {
        body["output_config"] = json!({"format": {
            "type": "json_schema",
            "schema": crate::structured_output::anthropic_schema(to_serde(schema)?),
        }});
    }
    if request.cache_policy == CachePolicy::ExplicitEphemeralPrefix {
        apply_anthropic_cache_breakpoints(&mut body);
    }
    Ok(body)
}

/// `CachePolicy::ExplicitEphemeralPrefix` on Anthropic. The provider caches
/// only prefixes ending at an explicit `cache_control` breakpoint (no automatic
/// caching), so without these every agent step pays full input price for the
/// whole history. `Disabled` requests (one-shot compaction, vision) stay
/// unmarked. Prefix order is tools → system →
/// messages; the provider allows at most four breakpoints. Mark the last tool,
/// the last system block, and the last two messages: each step writes the
/// conversation so far and the next step reads it back, even when one step
/// appended more than the provider's 20-block lookback.
fn apply_anthropic_cache_breakpoints(body: &mut Value) {
    const BREAKPOINT: &str = "cache_control";
    let ephemeral = || json!({"type": "ephemeral"});
    if let Some(tool) = body
        .get_mut("tools")
        .and_then(Value::as_array_mut)
        .and_then(|tools| tools.last_mut())
        .and_then(Value::as_object_mut)
    {
        tool.insert(BREAKPOINT.into(), ephemeral());
    }
    if let Some(block) = body
        .get_mut("system")
        .and_then(Value::as_array_mut)
        .and_then(|blocks| blocks.last_mut())
        .and_then(Value::as_object_mut)
    {
        block.insert(BREAKPOINT.into(), ephemeral());
    }
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let count = messages.len();
    for message in messages.iter_mut().skip(count.saturating_sub(2)) {
        // Thinking blocks and empty text cannot carry a breakpoint.
        if let Some(block) = message
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .and_then(|blocks| {
                blocks.iter_mut().rev().find(|block| {
                    let kind = block
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let empty_text = kind == "text"
                        && block
                            .get("text")
                            .and_then(Value::as_str)
                            .is_none_or(str::is_empty);
                    !matches!(kind, "thinking" | "redacted_thinking") && !empty_text
                })
            })
            .and_then(Value::as_object_mut)
        {
            block.insert(BREAKPOINT.into(), ephemeral());
        }
    }
}

fn bedrock_converse(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut messages = Vec::new();
    for message in &request.input {
        let content_parts = message.content.as_slice();
        let content = if message.role == CanonicalRole::Tool {
            vec![json!({"toolResult": {
                "toolUseId": provider_tool_call_id(
                    ApiFormat::BedrockConverse,
                    message.tool_call_id.as_ref()
                        .ok_or(MappingError::InvalidCanonicalRequest)?.as_str(),
                ),
                "content": bedrock_tool_result_content(content_parts, contents)?,
            }})]
        } else {
            let mut content = if message.role == CanonicalRole::Assistant {
                provider_reasoning_replay_items(
                    request,
                    message,
                    contents,
                    ApiFormat::BedrockConverse,
                )?
            } else {
                Vec::new()
            };
            content.extend(
                content_parts
                    .iter()
                    .filter_map(|part| match part {
                        CanonicalContentPart::Text(text) if text.as_str().trim().is_empty() => None,
                        _ => Some(bedrock_part(part, contents)),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
            for call in &message.tool_calls {
                content.push(json!({"toolUse": {
                    "toolUseId": provider_tool_call_id(
                        ApiFormat::BedrockConverse,
                        call.call_id.as_str(),
                    ),
                    "name": provider_tool_name(request, call.name.as_str())?,
                    "input": to_serde(&call.arguments)?,
                }}));
            }
            if content.is_empty() && message.role == CanonicalRole::User {
                content.push(json!({"text": EMPTY_BEDROCK_TEXT}));
            }
            content
        };
        if !content.is_empty() {
            push_message_content(
                &mut messages,
                if message.role == CanonicalRole::Assistant {
                    "assistant"
                } else {
                    "user"
                },
                content,
            )?;
        }
    }
    for event in request.runtime_events.iter() {
        push_message_content(
            &mut messages,
            "user",
            runtime_events::parts(event, request, contents, runtime_events::Wire::Bedrock)?,
        )?;
    }
    let mut body = json!({
        "system": request.instructions.iter()
            .map(|text| json!({"text": text.as_str()})).collect::<Vec<_>>(),
        "messages": messages,
        "inferenceConfig": {
            "maxTokens": request.max_output_tokens.get(),
        },
    });
    if let Some(temperature) = request.temperature {
        body["inferenceConfig"]["temperature"] = json!(temperature);
    }
    apply_nested_signed_route_parameters(&mut body["inferenceConfig"], request)?;
    if !request.tools.is_empty() {
        body["toolConfig"] = json!({"tools": request.tools.iter().map(|tool| {
            let mut specification = json!({
                "name": tool.model_name.as_str(),
                "description": tool.description.as_str(),
                "inputSchema": {"json": crate::tool_schema::provider_tool_schema(&tool.input_schema)?},
            });
            if tool.strict && supports_strict_tool_schema(request) {
                specification["strict"] = Value::Bool(true);
            }
            Ok(json!({"toolSpec": specification}))
        }).collect::<Result<Vec<_>, MappingError>>()?});
    }
    apply_bedrock_reasoning(&mut body, request)?;
    if let Some((_, schema)) = &request.response_schema {
        body["outputConfig"] = json!({"textFormat": {
            "type": "json_schema",
            "structure": {"jsonSchema": {
                "schema": serde_json::to_string(&to_serde(schema)?)
                    .map_err(|_| MappingError::JsonEncoding)?,
                "name": "musteragent_response",
            }},
        }});
    }
    Ok(body)
}

fn validate_auxiliary_request_ownership(
    request: &CanonicalModelRequest,
) -> Result<(), MappingError> {
    if request.route_role
        != musteragent_runtime_model::ModelInvocationRouteRole::Auxiliary(
            AuxiliaryModelRole::Compression,
        )
    {
        return Ok(());
    }
    if !request.tools.is_empty()
        || request.response_schema.is_some()
        || request.reasoning_policy != ReasoningPolicy::Disabled
        || request.cache_policy != CachePolicy::Disabled
        || request.continuation_ref.is_some()
        || request.route.provider_state_policy != ProviderStatePolicy::Stateless
    {
        return Err(MappingError::UnsafeAuxiliaryRequest);
    }
    for (key, value) in &request.route.parameters {
        if key.as_str() == "reasoning_effort"
            && crate::reasoning::has_explicit_reasoning_disable(request)
        {
            continue;
        }
        if !is_approved_compression_tuning(request.route.api_format, key.as_str(), value) {
            return Err(MappingError::UnsafeAuxiliaryParameter);
        }
    }
    Ok(())
}

fn provider_state_persistence_enabled(request: &CanonicalModelRequest) -> bool {
    request.cache_policy != CachePolicy::Disabled
        && request.route.provider_state_policy == ProviderStatePolicy::OpaqueContinuation
}

/// A route signed without an explicit output limit (a BYOK model the user
/// did not size) lets the provider apply its own default instead of an
/// arbitrary ceiling; a turn cut at that limit continues in a new segment.
/// The route's max_output_tokens still reserves context headroom. Anthropic
/// Messages requires the field, so it is always sent there.
fn provider_default_output_limit(request: &CanonicalModelRequest) -> bool {
    has_capability(request, "max_output.provider_default")
}

fn has_capability(request: &CanonicalModelRequest, expected: &str) -> bool {
    request
        .route
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == expected)
}

fn has_json_schema_output_capability(request: &CanonicalModelRequest) -> bool {
    has_capability(request, "json_schema_output")
        // Immutable routes signed before Backend migration 337 used this
        // ambiguous key. New admissions must use json_schema_output.
        || has_capability(request, "structured_output")
}

fn reasoning_echo_mode(request: &CanonicalModelRequest) -> Result<Option<&str>, MappingError> {
    let mut modes = request
        .route
        .capabilities
        .iter()
        .filter_map(|capability| capability.as_str().strip_prefix("reasoning_echo."));
    let mode = modes.next();
    if modes.next().is_some()
        || mode.is_some_and(|value| !matches!(value, "forbidden" | "allowed" | "required"))
    {
        return Err(MappingError::InvalidRouteConfiguration);
    }
    Ok(mode.filter(|value| *value != "forbidden"))
}

fn provider_reasoning_replay_items(
    request: &CanonicalModelRequest,
    message: &CanonicalMessage,
    contents: &ResolvedContents,
    expected_format: ApiFormat,
) -> Result<Vec<Value>, MappingError> {
    let Some(reasoning) = message.reasoning.as_ref().filter(|reasoning| {
        reasoning.source_route_ref.as_str() == request.route.route_ref.as_str()
    }) else {
        return Ok(Vec::new());
    };
    let bytes = match contents.get(&reasoning.content_ref)? {
        ResolvedContent::Inline { media_type, bytes }
            if media_type.as_str()
                == "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8" =>
        {
            bytes.expose_secret()
        }
        _ => return Err(MappingError::InvalidReasoningReplay),
    };
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| MappingError::InvalidReasoningReplay)?;
    let Some(object) = value.as_object() else {
        return Err(MappingError::InvalidReasoningReplay);
    };
    if object.get("schema").and_then(Value::as_str)
        != Some("musteragent.legacy.runtime.provider-reasoning-replay.v2")
    {
        return Ok(Vec::new());
    }
    if object.get("source_api_format").and_then(Value::as_str) != Some(expected_format.as_str())
        || object.get("source_model_key").and_then(Value::as_str)
            != Some(request.route.resolved_model_key.as_str())
    {
        return Ok(Vec::new());
    }
    let items = object
        .get("items")
        .and_then(Value::as_array)
        .ok_or(MappingError::InvalidReasoningReplay)?;
    let mut resolved = Vec::with_capacity(items.len());
    for item in items {
        let decoded = google::decode_private_payload(
            item,
            expected_format,
            request.stream_budget.max_reasoning_replay_bytes.get(),
        )?;
        let item = decoded
            .as_object()
            .ok_or(MappingError::InvalidReasoningReplay)?;
        let valid = match expected_format {
            ApiFormat::OpenAiResponses => {
                item.get("type").and_then(Value::as_str) == Some("reasoning")
            }
            ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => matches!(
                item.get("type").and_then(Value::as_str),
                Some("thinking" | "redacted_thinking")
            ),
            ApiFormat::BedrockConverse => item
                .get("reasoningContent")
                .and_then(Value::as_object)
                .is_some(),
            ApiFormat::GoogleGenerateContent => {
                item.get("type").and_then(Value::as_str) == Some("google_content")
                    && item.get("parts").is_some_and(Value::is_array)
            }
            ApiFormat::GoogleInteractions => matches!(
                item.get("type").and_then(Value::as_str),
                Some("thought" | "model_output" | "function_call")
            ),
            ApiFormat::OpenAiChatCompletions
            | ApiFormat::TypeSafeSystemOne
            | ApiFormat::OpenRouterDecisions => false,
        };
        if !valid {
            return Err(MappingError::InvalidReasoningReplay);
        }
        resolved.push(Value::Object(item.clone()));
    }
    Ok(resolved)
}

fn openai_chat_reasoning_content(
    request: &CanonicalModelRequest,
    message: &CanonicalMessage,
    contents: &ResolvedContents,
) -> Result<Option<String>, MappingError> {
    let Some(mode) = reasoning_echo_mode(request)? else {
        return Ok(None);
    };
    let compatible = message.reasoning.as_ref().filter(|reasoning| {
        reasoning.source_route_ref.as_str() == request.route.route_ref.as_str()
    });
    if let Some(reasoning) = compatible {
        return resolved_utf8(&reasoning.content_ref, contents).map(Some);
    }
    Ok((mode == "required").then(|| " ".to_owned()))
}

fn openai_tool(
    tool: &CanonicalToolDefinition,
    supports_strict: bool,
) -> Result<Value, MappingError> {
    let mut function = json!({
        "name": tool.model_name.as_str(),
        "description": tool.description.as_str(),
        "parameters": openai_tool_parameters(tool)?,
    });
    if supports_strict {
        function["strict"] = Value::Bool(tool.strict);
    }
    Ok(json!({
        "type": "function",
        "function": function,
    }))
}

fn supports_strict_tool_schema(request: &CanonicalModelRequest) -> bool {
    request
        .route
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == "strict_tool_schema")
}

fn google_tool(tool: &CanonicalToolDefinition) -> Result<Value, MappingError> {
    Ok(json!({
        "name": tool.model_name.as_str(),
        "description": tool.description.as_str(),
        "parametersJsonSchema": crate::tool_schema::provider_tool_schema(&tool.input_schema)?,
    }))
}

fn push_message_content(
    messages: &mut Vec<Value>,
    role: &str,
    content: Vec<Value>,
) -> Result<(), MappingError> {
    if content.is_empty() {
        return Ok(());
    }
    if let Some(previous) = messages.last_mut()
        && previous.get("role").and_then(Value::as_str) == Some(role)
    {
        previous
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .ok_or(MappingError::JsonEncoding)?
            .extend(content);
        return Ok(());
    }
    messages.push(json!({"role": role, "content": content}));
    Ok(())
}

fn push_message_parts(
    messages: &mut Vec<Value>,
    role: &str,
    parts: Vec<Value>,
    parallel_results: bool,
) -> Result<(), MappingError> {
    if parts.is_empty() {
        return Ok(());
    }
    if parallel_results && let Some(previous) = messages.last_mut() {
        previous
            .get_mut("parts")
            .and_then(Value::as_array_mut)
            .ok_or(MappingError::JsonEncoding)?
            .extend(parts);
        return Ok(());
    }
    messages.push(json!({"role": role, "parts": parts}));
    Ok(())
}

fn openai_chat_content(
    request: &CanonicalModelRequest,
    parts: &[CanonicalContentPart],
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    if parts
        .iter()
        .all(|part| matches!(part, CanonicalContentPart::Text(_)))
    {
        let mut text = String::new();
        for part in parts {
            let CanonicalContentPart::Text(part) = part else {
                unreachable!("all parts were classified as text");
            };
            text.push_str(part.as_str());
        }
        return Ok(Value::String(text));
    }
    Ok(Value::Array(
        parts
            .iter()
            .map(|part| match part {
                CanonicalContentPart::Text(text) => {
                    Ok(json!({"type": "text", "text": text.as_str()}))
                }
                CanonicalContentPart::Image(reference) => Ok(json!({
                    "type": "image_url",
                    "image_url": {"url": data_or_url(reference, contents)?}
                })),
                CanonicalContentPart::File(reference) => Ok(json!({
                    "type": "file",
                    "file": openai_file_payload(reference, contents)?
                })),
                CanonicalContentPart::Audio(reference) => {
                    openai_chat_audio(request, reference, contents)
                }
                CanonicalContentPart::Video(reference) => {
                    openai_chat_video(request, reference, contents)
                }
            })
            .collect::<Result<Vec<_>, MappingError>>()?,
    ))
}

fn responses_part(
    request: &CanonicalModelRequest,
    part: &CanonicalContentPart,
    contents: &ResolvedContents,
    assistant: bool,
) -> Result<Value, MappingError> {
    match part {
        CanonicalContentPart::Text(text) => Ok(json!({
            "type": if assistant {"output_text"} else {"input_text"},
            "text": text.as_str()
        })),
        CanonicalContentPart::Image(reference) => {
            Ok(json!({"type": "input_image", "image_url": data_or_url(reference, contents)?}))
        }
        CanonicalContentPart::File(reference) => {
            let mut payload = openai_file_payload(reference, contents)?;
            payload["type"] = Value::String("input_file".into());
            Ok(payload)
        }
        CanonicalContentPart::Audio(reference) => {
            if !has_capability(request, "multimodal_wire.openai_responses_qwen") {
                return Err(MappingError::UnsupportedInputModality);
            }
            openai_responses_audio(reference, contents)
        }
        CanonicalContentPart::Video(reference) => {
            if !has_capability(request, "multimodal_wire.openai_responses_qwen") {
                return Err(MappingError::UnsupportedInputModality);
            }
            // Our content resolvers deliver bytes inline; Qwen accepts the
            // same base64 data URL it takes for chat `video_url`.
            Ok(json!({
                "type": "input_video",
                "video_url": data_or_url(reference, contents)?,
            }))
        }
    }
}

fn anthropic_part(
    part: &CanonicalContentPart,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    match part {
        CanonicalContentPart::Text(text) => Ok(json!({"type": "text", "text": text.as_str()})),
        CanonicalContentPart::Image(reference) => {
            let (media_type, data) = inline_base64(reference, contents)?;
            Ok(json!({"type": "image", "source": {
                "type": "base64", "media_type": media_type, "data": data
            }}))
        }
        CanonicalContentPart::File(reference) => match contents.get(reference)? {
            ResolvedContent::Inline { media_type, bytes }
                if media_type.as_str() == "application/pdf" =>
            {
                Ok(json!({"type": "document", "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": BASE64_STANDARD.encode(bytes.expose_secret()),
                }}))
            }
            ResolvedContent::Inline { media_type, bytes }
                if media_type.as_str() == "text/plain" =>
            {
                let text = std::str::from_utf8(bytes.expose_secret())
                    .map_err(|_| MappingError::InvalidUtf8)?;
                Ok(json!({"type": "document", "source": {
                    "type": "text",
                    "media_type": "text/plain",
                    "data": text,
                }}))
            }
            ResolvedContent::Inline { .. } => Err(MappingError::UnsupportedMedia),
            ResolvedContent::PublicUrl { .. } => Err(MappingError::InlineContentRequired),
        },
        CanonicalContentPart::Audio(_) | CanonicalContentPart::Video(_) => {
            Err(MappingError::UnsupportedInputModality)
        }
    }
}

fn google_part(
    part: &CanonicalContentPart,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    match part {
        CanonicalContentPart::Text(text) => Ok(json!({"text": text.as_str()})),
        CanonicalContentPart::Image(reference)
        | CanonicalContentPart::Audio(reference)
        | CanonicalContentPart::Video(reference)
        | CanonicalContentPart::File(reference) => match contents.get(reference)? {
            ResolvedContent::PublicUrl { media_type, url } => Ok(json!({"fileData": {
                "mimeType": media_type.as_str(), "fileUri": url.as_str()
            }})),
            ResolvedContent::Inline { media_type, bytes } => Ok(json!({"inlineData": {
                "mimeType": media_type.as_str(),
                "data": BASE64_STANDARD.encode(bytes.expose_secret())
            }})),
        },
    }
}

fn interactions_part(
    part: &CanonicalContentPart,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    match part {
        CanonicalContentPart::Text(text) => Ok(json!({"type": "text", "text": text.as_str()})),
        CanonicalContentPart::Image(reference)
        | CanonicalContentPart::File(reference)
        | CanonicalContentPart::Audio(reference)
        | CanonicalContentPart::Video(reference) => {
            let kind = match part {
                CanonicalContentPart::Image(_) => "image",
                CanonicalContentPart::File(_) => "document",
                CanonicalContentPart::Audio(_) => "audio",
                CanonicalContentPart::Video(_) => "video",
                CanonicalContentPart::Text(_) => unreachable!(),
            };
            match contents.get(reference)? {
                ResolvedContent::PublicUrl { media_type, url } => {
                    Ok(json!({"type":kind, "uri":url.as_str(), "mime_type":media_type.as_str()}))
                }
                ResolvedContent::Inline { media_type, bytes } => Ok(
                    json!({"type":kind, "data":BASE64_STANDARD.encode(bytes.expose_secret()), "mime_type":media_type.as_str()}),
                ),
            }
        }
    }
}

fn bedrock_part(
    part: &CanonicalContentPart,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    match part {
        CanonicalContentPart::Text(text) => Ok(json!({"text": text.as_str()})),
        CanonicalContentPart::Image(reference) => {
            let (media_type, data) = inline_base64(reference, contents)?;
            let format = bedrock_image_format(&media_type)?;
            Ok(json!({"image": {"format": format, "source": {"bytes": data}}}))
        }
        CanonicalContentPart::File(reference) => {
            let (media_type, data) = inline_base64(reference, contents)?;
            Ok(json!({"document": {
                "format": bedrock_document_format(&media_type)?,
                "name": "musteragent_attachment",
                "source": {"bytes": data}
            }}))
        }
        CanonicalContentPart::Audio(_) | CanonicalContentPart::Video(_) => {
            Err(MappingError::UnsupportedInputModality)
        }
    }
}

fn tool_text(parts: &[CanonicalContentPart]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            CanonicalContentPart::Image(_)
            | CanonicalContentPart::Audio(_)
            | CanonicalContentPart::Video(_)
            | CanonicalContentPart::File(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn responses_tool_output(
    request: &CanonicalModelRequest,
    parts: &[CanonicalContentPart],
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    if parts
        .iter()
        .all(|part| matches!(part, CanonicalContentPart::Text(_)))
    {
        let text = tool_text(parts);
        return Ok(Value::String(if text.trim().is_empty() {
            EMPTY_TOOL_OUTPUT.to_owned()
        } else {
            text
        }));
    }
    let mut output = parts
        .iter()
        .map(|part| responses_part(request, part, contents, false))
        .collect::<Result<Vec<_>, _>>()?;
    if output.is_empty() {
        output.push(json!({"type": "input_text", "text": EMPTY_TOOL_OUTPUT}));
    }
    Ok(Value::Array(output))
}

fn anthropic_tool_output(
    parts: &[CanonicalContentPart],
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    if parts
        .iter()
        .all(|part| matches!(part, CanonicalContentPart::Text(_)))
    {
        let text = tool_text(parts);
        return Ok(Value::String(if text.trim().is_empty() {
            EMPTY_TOOL_OUTPUT.to_owned()
        } else {
            text
        }));
    }
    let mut output = parts
        .iter()
        .map(|part| anthropic_part(part, contents))
        .collect::<Result<Vec<_>, _>>()?;
    if output.is_empty() {
        output.push(json!({"type": "text", "text": EMPTY_TOOL_OUTPUT}));
    }
    Ok(Value::Array(output))
}

fn google_function_response(
    request: &CanonicalModelRequest,
    message: &CanonicalMessage,
    contents: &ResolvedContents,
    native_id: Option<&str>,
) -> Result<Value, MappingError> {
    let text = tool_text(&message.content);
    let media = message
        .content
        .iter()
        .filter(|part| !matches!(part, CanonicalContentPart::Text(_)))
        .map(|part| google_part(part, contents))
        .collect::<Result<Vec<_>, _>>()?;
    let mut response = json!({
        "name": provider_tool_name(
            request,
            message.tool_name.as_ref()
                .ok_or(MappingError::InvalidCanonicalRequest)?.as_str(),
        )?,
        "response": {
            "output": if text.trim().is_empty() {
                if media.is_empty() { EMPTY_TOOL_OUTPUT } else { "(see attached media)" }
            } else {
                text.as_str()
            },
        },
    });
    if let Some(id) = native_id {
        response["id"] = Value::String(id.to_owned());
    } else if uses_provider_tool_call_ids(request) {
        response["id"] = Value::String(provider_tool_call_id(
            ApiFormat::GoogleGenerateContent,
            message
                .tool_call_id
                .as_ref()
                .ok_or(MappingError::InvalidCanonicalRequest)?
                .as_str(),
        ));
    }
    if !media.is_empty() {
        response["parts"] = Value::Array(media);
    }
    Ok(json!({"functionResponse": response}))
}

fn uses_provider_tool_call_ids(request: &CanonicalModelRequest) -> bool {
    request
        .route
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == "tool_call_id")
}

fn interactions_tool_result(
    parts: &[CanonicalContentPart],
    contents: &ResolvedContents,
) -> Result<Vec<Value>, MappingError> {
    let mut result = parts
        .iter()
        .map(|part| interactions_part(part, contents))
        .collect::<Result<Vec<_>, _>>()?;
    if result.is_empty() {
        result.push(json!({"type": "text", "text": EMPTY_TOOL_OUTPUT}));
    }
    Ok(result)
}

fn bedrock_tool_result_content(
    parts: &[CanonicalContentPart],
    contents: &ResolvedContents,
) -> Result<Vec<Value>, MappingError> {
    let mut result = parts
        .iter()
        .filter(|part| {
            !matches!(part, CanonicalContentPart::Text(text) if text.as_str().trim().is_empty())
        })
        .map(|part| bedrock_part(part, contents))
        .collect::<Result<Vec<_>, _>>()?;
    if result.is_empty() {
        result.push(json!({"text": EMPTY_BEDROCK_TEXT}));
    }
    Ok(result)
}

fn provider_tool_call_id(format: ApiFormat, value: &str) -> String {
    let max_len = if format == ApiFormat::OpenAiChatCompletions {
        40
    } else {
        64
    };
    bounded_wire_identity(value, max_len, "call")
}

fn tool_result_call_id(request: &CanonicalModelRequest, value: &str) -> String {
    if request.continuation_ref.is_some()
        && matches!(
            request.route.api_format,
            ApiFormat::OpenAiResponses | ApiFormat::GoogleInteractions
        )
    {
        value.to_owned()
    } else {
        provider_tool_call_id(request.route.api_format, value)
    }
}

fn provider_tool_name(
    request: &CanonicalModelRequest,
    value: &str,
) -> Result<String, MappingError> {
    // Historical calls remain representable after removal from the active catalog.
    // Only request.tools grants a callable function on this invocation.
    Ok(request
        .tools
        .iter()
        .find(|tool| tool.name.as_str() == value)
        .map_or(value, |tool| tool.model_name.as_str())
        .to_owned())
}

fn bounded_wire_identity(value: &str, max_len: usize, fallback: &str) -> String {
    let sanitized = value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
                char::from(byte)
            } else {
                '_'
            }
        })
        .collect::<String>();
    if !sanitized.is_empty() && sanitized.len() <= max_len && sanitized == value {
        return sanitized;
    }
    let digest = Sha256::digest(value.as_bytes());
    let suffix = digest[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let prefix_budget = max_len.saturating_sub(suffix.len() + 1);
    let base = if sanitized.is_empty() {
        fallback
    } else {
        sanitized.as_str()
    };
    let prefix = &base[..base.len().min(prefix_budget)];
    format!("{prefix}_{suffix}")
}

#[cfg(test)]
mod tests {
    use musteragent_model::ApiFormat;

    use super::provider_tool_call_id;

    #[test]
    fn provider_tool_identity_is_bounded_stable_and_collision_resistant() {
        let first = format!("foreign|{}A", "+/=".repeat(100));
        let second = format!("foreign|{}B", "+/=".repeat(100));
        let normalized_first = provider_tool_call_id(ApiFormat::OpenAiChatCompletions, &first);
        let normalized_second = provider_tool_call_id(ApiFormat::OpenAiChatCompletions, &second);
        assert!(normalized_first.len() <= 40);
        assert!(normalized_second.len() <= 40);
        assert_ne!(normalized_first, normalized_second);
        assert!(
            normalized_first
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        );
    }
}

fn resolved_utf8(
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<String, MappingError> {
    match contents.get(reference)? {
        ResolvedContent::Inline { bytes, .. } => {
            String::from_utf8(bytes.expose_secret().to_vec()).map_err(|_| MappingError::InvalidUtf8)
        }
        ResolvedContent::PublicUrl { .. } => Err(MappingError::InlineContentRequired),
    }
}

fn google_thought_signature(
    request: &CanonicalModelRequest,
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<Option<String>, MappingError> {
    let ResolvedContent::Inline { media_type, bytes } = contents.get(reference)? else {
        return Err(MappingError::InlineContentRequired);
    };
    if media_type.as_str() != "application/vnd.musteragent.legacy.provider-tool-replay+json" {
        return Err(MappingError::InvalidProviderReplay);
    }
    let value: Value = serde_json::from_slice(bytes.expose_secret())
        .map_err(|_| MappingError::InvalidProviderReplay)?;
    let object = value
        .as_object()
        .ok_or(MappingError::InvalidProviderReplay)?;
    if object.get("kind").and_then(Value::as_str) != Some("google_thought_signature") {
        return Err(MappingError::InvalidProviderReplay);
    }
    match object.get("schema").and_then(Value::as_str) {
        // v1 predates route/api binding. It remains readable for crash
        // recovery, but cannot be proven safe for Provider-private replay.
        Some("musteragent.legacy.runtime.provider-tool-replay.v1") => return Ok(None),
        Some("musteragent.legacy.runtime.provider-tool-replay.v2") => {}
        _ => return Err(MappingError::InvalidProviderReplay),
    }
    let source_route_ref = object
        .get("source_route_ref")
        .and_then(Value::as_str)
        .ok_or(MappingError::InvalidProviderReplay)?;
    let source_api_format = object
        .get("source_api_format")
        .and_then(Value::as_str)
        .ok_or(MappingError::InvalidProviderReplay)?;
    let source_model_key = object
        .get("source_model_key")
        .and_then(Value::as_str)
        .ok_or(MappingError::InvalidProviderReplay)?;
    let signature = object
        .get("signature")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 4_096)
        .ok_or(MappingError::InvalidProviderReplay)?;
    if source_route_ref == request.route.route_ref.as_str()
        && source_api_format == request.route.api_format.as_str()
        && source_model_key == request.route.resolved_model_key
    {
        Ok(Some(signature.to_owned()))
    } else {
        Ok(None)
    }
}

fn openai_file_payload(
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let filename = provider_filename(reference.media_type.as_str());
    Ok(match contents.get(reference)? {
        ResolvedContent::PublicUrl { url, .. } => {
            json!({"file_url": url.as_str(), "filename": filename})
        }
        // OpenAI documents `file_data` as a data URL, like image_url.
        ResolvedContent::Inline { media_type, bytes } => json!({
            "file_data": format!(
                "data:{};base64,{}",
                media_type.as_str(),
                BASE64_STANDARD.encode(bytes.expose_secret())
            ),
            "filename": filename,
        }),
    })
}

fn openai_chat_audio(
    request: &CanonicalModelRequest,
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    if !has_capability(request, "multimodal_wire.openai_chat_qwen")
        && !has_capability(request, "multimodal_wire.openai_chat_volcano")
    {
        return Err(MappingError::UnsupportedInputModality);
    }
    Ok(json!({
        "type": "input_audio",
        "input_audio": {
            "data": data_or_url(reference, contents)?,
            "format": media_format(reference.media_type.as_str())?,
        }
    }))
}

fn openai_chat_video(
    request: &CanonicalModelRequest,
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    if !has_capability(request, "multimodal_wire.openai_chat_qwen")
        && !has_capability(request, "multimodal_wire.openai_chat_volcano")
    {
        return Err(MappingError::UnsupportedInputModality);
    }
    Ok(json!({
        "type": "video_url",
        "video_url": {"url": data_or_url(reference, contents)?},
    }))
}

fn openai_responses_audio(
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut result = json!({
        "type": "input_audio",
        "format": media_format(reference.media_type.as_str())?,
    });
    match contents.get(reference)? {
        ResolvedContent::PublicUrl { url, .. } => {
            result["audio_url"] = Value::String(url.as_str().to_owned());
        }
        ResolvedContent::Inline { media_type, bytes } => {
            result["data"] = Value::String(format!(
                "data:{};base64,{}",
                media_type.as_str(),
                BASE64_STANDARD.encode(bytes.expose_secret())
            ));
        }
    }
    Ok(result)
}

fn media_format(media_type: &str) -> Result<String, MappingError> {
    let known = match media_type {
        "audio/mpeg" => Some("mp3"),
        "audio/wav" | "audio/x-wav" => Some("wav"),
        "audio/mp4" => Some("m4a"),
        "audio/aac" => Some("aac"),
        "audio/ogg" => Some("ogg"),
        "video/mp4" => Some("mp4"),
        "video/quicktime" => Some("mov"),
        "video/x-msvideo" => Some("avi"),
        _ => None,
    };
    if let Some(value) = known {
        return Ok(value.to_owned());
    }
    media_type
        .split_once('/')
        .map(|(_, subtype)| subtype)
        .filter(|subtype| {
            !subtype.is_empty()
                && subtype
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'+' | b'.'))
        })
        .map(str::to_owned)
        .ok_or(MappingError::UnsupportedMedia)
}

fn provider_filename(media_type: &str) -> String {
    let extension = match media_type {
        "application/pdf" => "pdf",
        "application/json" => "json",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.ms-excel" => "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
        "text/csv" => "csv",
        "text/html" => "html",
        "text/markdown" => "md",
        "text/plain" => "txt",
        _ => "bin",
    };
    format!("musteragent_attachment.{extension}")
}

fn bedrock_image_format(media_type: &str) -> Result<&'static str, MappingError> {
    match media_type {
        "image/jpeg" | "image/jpg" => Ok("jpeg"),
        "image/png" => Ok("png"),
        "image/gif" => Ok("gif"),
        "image/webp" => Ok("webp"),
        _ => Err(MappingError::UnsupportedMedia),
    }
}

fn bedrock_document_format(media_type: &str) -> Result<&'static str, MappingError> {
    match media_type {
        "application/pdf" => Ok("pdf"),
        "application/msword" => Ok("doc"),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => Ok("docx"),
        "application/vnd.ms-excel" => Ok("xls"),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => Ok("xlsx"),
        "text/csv" => Ok("csv"),
        "text/html" => Ok("html"),
        "text/markdown" => Ok("md"),
        "text/plain" => Ok("txt"),
        _ => Err(MappingError::UnsupportedMedia),
    }
}

fn data_or_url(
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<String, MappingError> {
    match contents.get(reference)? {
        ResolvedContent::PublicUrl { url, .. } => Ok(url.as_str().into()),
        ResolvedContent::Inline { media_type, bytes } => Ok(format!(
            "data:{};base64,{}",
            media_type.as_str(),
            BASE64_STANDARD.encode(bytes.expose_secret())
        )),
    }
}

fn inline_base64(
    reference: &ContentRef,
    contents: &ResolvedContents,
) -> Result<(String, String), MappingError> {
    match contents.get(reference)? {
        ResolvedContent::Inline { media_type, bytes } => Ok((
            media_type.as_str().to_owned(),
            BASE64_STANDARD.encode(bytes.expose_secret()),
        )),
        ResolvedContent::PublicUrl { .. } => Err(MappingError::InlineContentRequired),
    }
}

fn join_instructions(request: &CanonicalModelRequest) -> String {
    request
        .instructions
        .iter()
        .map(|text| text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// OpenAI strict-mode projection of the portable tool parameters.
fn openai_tool_parameters(tool: &CanonicalToolDefinition) -> Result<Value, MappingError> {
    crate::structured_output::openai_schema(crate::tool_schema::provider_tool_schema(
        &tool.input_schema,
    )?)
    .map_err(|_| MappingError::UnsupportedJsonSchemaOutput)
}

fn openai_schema(value: &JsonValue) -> Result<Value, MappingError> {
    crate::structured_output::openai_schema(to_serde(value)?)
        .map_err(|_| MappingError::UnsupportedJsonSchemaOutput)
}

pub(crate) fn to_serde(value: &JsonValue) -> Result<Value, MappingError> {
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(*value),
        JsonValue::Integer(value) => json!(value),
        JsonValue::Number(value) => {
            Value::Number(serde_json::Number::from_f64(*value).ok_or(MappingError::JsonEncoding)?)
        }
        JsonValue::String(value) => Value::String(value.clone()),
        JsonValue::Array(values) => {
            Value::Array(values.iter().map(to_serde).collect::<Result<_, _>>()?)
        }
        JsonValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), to_serde(value)?)))
                .collect::<Result<Map<_, _>, MappingError>>()?,
        ),
    })
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum MappingError {
    #[error("canonical model request is invalid")]
    InvalidCanonicalRequest,
    #[error("the signed model route configuration is invalid")]
    InvalidRouteConfiguration,
    #[error("Provider request JSON encoding failed")]
    JsonEncoding,
    #[error("Provider requires inline content for this API format")]
    InlineContentRequired,
    #[error("resolved text content is not UTF-8")]
    InvalidUtf8,
    #[error("Provider replay metadata is malformed or incompatible")]
    InvalidProviderReplay,
    #[error("Provider reasoning replay metadata is malformed or incompatible")]
    InvalidReasoningReplay,
    #[error("the selected protocol has no lossless mapping for the requested reasoning policy")]
    UnsupportedReasoningPolicy,
    #[error("JSON Schema output is not authorized by the signed model route")]
    UnsupportedJsonSchemaOutput,
    #[error("the requested input modality is not authorized by the signed model route")]
    UnauthorizedInputModality,
    #[error("the selected protocol dialect has no lossless mapping for the input modality")]
    UnsupportedInputModality,
    #[error("the selected protocol requires exact provider-native history that is not available")]
    ExactProviderReplayRequired,
    #[error("content media type is not supported by the Provider API")]
    UnsupportedMedia,
    #[error("auxiliary model invocation contains owner-controlled physical request fields")]
    UnsafeAuxiliaryRequest,
    #[error("auxiliary model route contains a parameter outside its closed format allowlist")]
    UnsafeAuxiliaryParameter,
    #[error(transparent)]
    Content(#[from] ContentResolverError),
}
