//! Google native codecs. Shared history and Memory remain Runtime-owned.
use super::*;
use std::collections::BTreeSet;

pub(super) fn google_generate(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut messages = Vec::new();
    let mut native_call_ids = BTreeSet::<String>::new();
    let mut previous_tool_result = false;
    for message in &request.input {
        let content_parts = message.content.as_slice();
        let role = if message.role == CanonicalRole::Assistant {
            "model"
        } else {
            "user"
        };
        let replay = if message.role == CanonicalRole::Assistant {
            provider_reasoning_replay_items(
                request,
                message,
                contents,
                ApiFormat::GoogleGenerateContent,
            )?
        } else {
            Vec::new()
        };
        let parts = if !replay.is_empty() {
            let parts: Vec<Value> = replay
                .into_iter()
                .flat_map(|item| item["parts"].as_array().cloned().unwrap_or_default())
                .collect();
            native_call_ids.extend(
                parts
                    .iter()
                    .filter_map(|part| {
                        part.get("functionCall")
                            .or_else(|| part.get("function_call"))
                    })
                    .filter_map(|call| call.get("id").and_then(Value::as_str))
                    .map(str::to_owned),
            );
            parts
        } else if message.role == CanonicalRole::Tool {
            let native_id = message
                .tool_call_id
                .as_ref()
                .and_then(|id| native_call_ids.get(id.as_str()))
                .map(String::as_str);
            vec![google_function_response(
                request, message, contents, native_id,
            )?]
        } else {
            let mut parts = content_parts
                .iter()
                .map(|part| google_part(part, contents))
                .collect::<Result<Vec<_>, _>>()?;
            for (index, call) in message.tool_calls.iter().enumerate() {
                let mut part = json!({"functionCall": {
                    "name": provider_tool_name(request, call.name.as_str())?,
                    "args": to_serde(&call.arguments)?,
                }});
                if uses_provider_tool_call_ids(request) {
                    part["functionCall"]["id"] = Value::String(provider_tool_call_id(
                        ApiFormat::GoogleGenerateContent,
                        call.call_id.as_str(),
                    ));
                }
                if let Some(reference) = &call.provider_replay_ref
                    && let Some(signature) = google_thought_signature(request, reference, contents)?
                {
                    part["thoughtSignature"] = Value::String(signature);
                } else if index == 0 && call.provider_replay_ref.is_none() {
                    // GenerateContent accepts this documented value when importing a
                    // tool exchange from another protocol. Native Parts and original
                    // opaque signatures above always retain their exact value.
                    part["thoughtSignature"] = json!("skip_thought_signature_validator");
                }
                parts.push(part);
            }
            parts
        };
        let tool_result = message.role == CanonicalRole::Tool;
        push_message_parts(
            &mut messages,
            role,
            parts,
            tool_result && previous_tool_result,
        )?;
        previous_tool_result = tool_result;
    }
    for event in request.runtime_events.iter() {
        push_message_parts(
            &mut messages,
            "user",
            runtime_events::parts(event, request, contents, runtime_events::Wire::Google)?,
            false,
        )?;
    }
    let mut body = json!({
        "systemInstruction": {"parts": [{"text": join_instructions(request)}]},
        "contents": messages,
        "generationConfig": {
            "maxOutputTokens": request.max_output_tokens.get(),
        },
    });
    if let Some(temperature) = request.temperature {
        body["generationConfig"]["temperature"] = json!(temperature);
    }
    apply_nested_signed_route_parameters(&mut body["generationConfig"], request)?;
    if let Some((_, schema)) = &request.response_schema {
        body["generationConfig"]["responseMimeType"] = json!("application/json");
        body["generationConfig"]["responseJsonSchema"] = to_serde(schema)?;
    }
    apply_google_generate_reasoning(&mut body, request)?;
    if !request.tools.is_empty() {
        body["tools"] = json!([{"functionDeclarations": request
            .tools.iter().map(google_tool).collect::<Result<Vec<_>, _>>()?}]);
    }
    Ok(body)
}

pub(super) fn google_interactions(
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let mut input = Vec::new();
    let mut native_call_ids = BTreeSet::<String>::new();
    for message in &request.input {
        let content_parts = message.content.as_slice();
        match message.role {
            CanonicalRole::User => input.push(json!({
                "type": "user_input",
                "content": content_parts.iter()
                    .map(|part| interactions_part(part, contents))
                    .collect::<Result<Vec<_>, MappingError>>()?,
            })),
            CanonicalRole::Assistant => {
                let replay = provider_reasoning_replay_items(
                    request,
                    message,
                    contents,
                    ApiFormat::GoogleInteractions,
                )?;
                if !replay.is_empty() {
                    native_call_ids.extend(
                        replay
                            .iter()
                            .filter(|step| {
                                step.get("type").and_then(Value::as_str) == Some("function_call")
                            })
                            .filter_map(|step| step.get("id").and_then(Value::as_str))
                            .map(str::to_owned),
                    );
                    input.extend(replay);
                    continue;
                }
                if !content_parts.is_empty() {
                    input.push(json!({
                        "type": "model_output",
                        "content": content_parts.iter()
                            .map(|part| interactions_part(part, contents))
                            .collect::<Result<Vec<_>, MappingError>>()?,
                    }));
                }
                for call in &message.tool_calls {
                    input.push(json!({
                        "type": "function_call",
                        "id": provider_tool_call_id(
                            ApiFormat::GoogleInteractions,
                            call.call_id.as_str(),
                        ),
                        "name": provider_tool_name(request, call.name.as_str())?,
                        "arguments": to_serde(&call.arguments)?,
                    }));
                }
            }
            CanonicalRole::Tool => {
                let id = message
                    .tool_call_id
                    .as_ref()
                    .ok_or(MappingError::InvalidCanonicalRequest)?
                    .as_str();
                let call_id = native_call_ids
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| tool_result_call_id(request, id));
                let name = provider_tool_name(
                    request,
                    message
                        .tool_name
                        .as_ref()
                        .ok_or(MappingError::InvalidCanonicalRequest)?
                        .as_str(),
                )?;
                input.push(json!({
                    "type": "function_result",
                    "call_id": call_id,
                    "name": name,
                    "result": interactions_tool_result(content_parts, contents)?,
                }));
            }
        }
    }
    for event in request.runtime_events.iter() {
        input.push(json!({"type":"user_input", "content":runtime_events::parts(event, request, contents, runtime_events::Wire::Interactions)?}));
    }

    let mut body = json!({
        "model": request.route.resolved_model_key,
        "input": input,
        "system_instruction": join_instructions(request),
        "stream": true,
        "store": provider_state_persistence_enabled(request),
        "generation_config": {
            "max_output_tokens": request.max_output_tokens.get(),
        },
    });
    if let Some(continuation) = &request.continuation_ref {
        body["previous_interaction_id"] = json!(continuation.as_str());
    }
    // Background long decode keeps one server interaction identity. Stateless
    // requests use explicit foreground continuation without opting into storage.
    if provider_state_persistence_enabled(request) && request.max_output_tokens.get() > 65_536 {
        body["background"] = json!(true);
    }
    if let Some(temperature) = request.temperature {
        body["generation_config"]["temperature"] = json!(temperature);
    }
    apply_nested_signed_route_parameters(&mut body["generation_config"], request)?;
    apply_google_interactions_reasoning(&mut body, request)?;
    if let Some((_, schema)) = &request.response_schema {
        body["response_format"] = json!({
            "type": "text",
            "mime_type": "application/json",
            "schema": to_serde(schema)?,
        });
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    Ok(json!({
                        "type": "function",
                        "name": tool.model_name.as_str(),
                        "description": tool.description.as_str(),
                        "parameters": crate::tool_schema::provider_tool_schema(&tool.input_schema)?,
                    }))
                })
                .collect::<Result<_, MappingError>>()?,
        );
    }
    Ok(body)
}

/// Decode only the bounded, route-authorized native representation. This is not
/// an arbitrary request-parameter or transport envelope.
pub(super) fn decode_private_payload<'a>(
    item: &'a Value,
    format: ApiFormat,
    maximum: usize,
) -> Result<std::borrow::Cow<'a, Value>, MappingError> {
    if item.get("type").and_then(Value::as_str) != Some("google_native_chunks") {
        return Ok(std::borrow::Cow::Borrowed(item));
    }
    if !matches!(
        format,
        ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
    ) {
        return Err(MappingError::InvalidReasoningReplay);
    }
    let chunks = item
        .get("wire_chunks")
        .and_then(Value::as_array)
        .ok_or(MappingError::InvalidReasoningReplay)?;
    let mut encoded = String::new();
    for chunk in chunks {
        let text = chunk.as_str().ok_or(MappingError::InvalidReasoningReplay)?;
        if text.len() > 1_048_576
            || encoded
                .len()
                .checked_add(text.len())
                .is_none_or(|size| size > maximum)
        {
            return Err(MappingError::InvalidReasoningReplay);
        }
        encoded.push_str(text);
    }
    serde_json::from_str(&encoded)
        .map(std::borrow::Cow::Owned)
        .map_err(|_| MappingError::InvalidReasoningReplay)
}
