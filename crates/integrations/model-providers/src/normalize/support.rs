use musteragent_identity::ToolCallId;
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    JsonValue, ModelStreamEvent, ModelText, ModelUsage, ProviderErrorKind, ProviderRequestRef,
    ProviderToolCallReplay, StopReason,
};
use musteragent_tool::WireName;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::{NormalizeError, ToolState};

pub(super) fn tool_state(
    call_id: Option<&str>,
    name: Option<&str>,
    index: u32,
    provider_item_ref: Option<ProviderRequestRef>,
    provider_replay: Option<ProviderToolCallReplay>,
    invocation_scope: Option<&str>,
) -> Result<ToolState, NormalizeError> {
    let provider_call_id = call_id
        .map(str::to_owned)
        .unwrap_or_else(|| format!("provider-call-{index}"));
    let call_id = invocation_scope.map_or_else(
        || provider_call_id.clone(),
        |scope| scoped_tool_call_id(scope, &provider_call_id),
    );
    let name = name.ok_or(NormalizeError::MissingToolIdentity)?;
    Ok(ToolState {
        provider_call_id,
        call_id: ToolCallId::new(call_id).map_err(|_| NormalizeError::InvalidToolIdentity)?,
        name: WireName::new(name).map_err(|_| NormalizeError::InvalidToolIdentity)?,
        arguments: String::new(),
        argument_source: musteragent_runtime_model::ArgumentSource::Stream,
        argument_snapshot_seen: false,
        argument_stream_started: false,
        arguments_from_step: false,
        response_item_done: false,
        provider_item_ref,
        provider_replay,
        started: false,
        completed: false,
    })
}

/// OpenAI Chat is stateless between physical requests, and several compatible
/// providers reuse short call IDs (for example `browser_observe_0`) on every
/// response. Runtime ToolCallId is run-global, so namespace those provider IDs
/// by the physical invocation. Keep the result beneath the strictest common
/// OpenAI-compatible 40-byte correlation-ID limit.
fn scoped_tool_call_id(invocation_scope: &str, provider_call_id: &str) -> String {
    let scope_hash = Sha256::digest(invocation_scope.as_bytes());
    let provider_hash = Sha256::digest(provider_call_id.as_bytes());
    format!(
        "dvc_{:016x}_{:016x}",
        u64::from_be_bytes(scope_hash[..8].try_into().expect("SHA-256 prefix")),
        u64::from_be_bytes(
            provider_hash[..8]
                .try_into()
                .expect("SHA-256 provider prefix")
        )
    )
}

pub(super) fn provider_ref(value: impl Into<String>) -> Result<ProviderRequestRef, NormalizeError> {
    let value = OpaqueExternalRef::new(value.into())
        .map_err(|_| NormalizeError::InvalidProviderReference)?;
    Ok(ProviderRequestRef::from_opaque(value))
}

/// Models echo terminal output (ANSI escapes, NUL, backspace). Canonical text
/// excludes control characters, so drop them here instead of failing the
/// stream; a delta made only of them keeps one visible replacement mark.
fn provider_text(value: &str) -> Result<ModelText, NormalizeError> {
    let text = if value
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        let kept: String = value
            .chars()
            .filter(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'))
            .collect();
        if kept.is_empty() {
            "\u{FFFD}".to_owned()
        } else {
            kept
        }
    } else {
        value.to_owned()
    };
    ModelText::new(text).map_err(|_| NormalizeError::InvalidText)
}

pub(super) fn text_delta(index: u32, value: &str) -> Result<ModelStreamEvent, NormalizeError> {
    Ok(ModelStreamEvent::TextDelta {
        output_index: index,
        delta: provider_text(value)?,
    })
}

pub(super) fn reasoning(index: u32, value: &str) -> Result<ModelStreamEvent, NormalizeError> {
    Ok(ModelStreamEvent::ReasoningDelta {
        output_index: index,
        delta: provider_text(value)?,
    })
}

pub(super) fn reasoning_summary(
    index: u32,
    value: &str,
) -> Result<ModelStreamEvent, NormalizeError> {
    Ok(ModelStreamEvent::ReasoningSummaryDelta {
        output_index: index,
        delta: provider_text(value)?,
    })
}

pub(super) fn text_content(
    index: u32,
    value: &Value,
) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
    // OpenAI-compatible gateways commonly emit `content: null` while the same
    // delta carries Tool-call fields. This is an explicit absence of text, not
    // an unknown content shape.
    if value.is_null() {
        return Ok(Vec::new());
    }
    if let Some(text) = value.as_str() {
        if text.is_empty() {
            return Ok(Vec::new());
        }
        return Ok(vec![text_delta(index, text)?]);
    }
    array(value)?
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(|text| text_delta(index, text))
        .collect()
}

pub(super) fn openai_usage(value: &Value) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let prompt = u64_field_default(usage, "prompt_tokens", 0)?;
    let completion = u64_field_default(usage, "completion_tokens", 0)?;
    let cached = usage
        .get("prompt_tokens_details")
        .and_then(Value::as_object)
        .map(|details| u64_field_default(details, "cached_tokens", 0))
        .transpose()?
        .unwrap_or(0);
    let reasoning_tokens = usage
        .get("completion_tokens_details")
        .and_then(Value::as_object)
        .map(|details| u64_field_default(details, "reasoning_tokens", 0))
        .transpose()?
        .unwrap_or(0);
    usage_value(
        prompt.saturating_sub(cached),
        completion,
        reasoning_tokens,
        cached,
        0,
        usage.get("total_tokens").map(u64_value).transpose()?,
    )
}

pub(super) fn openai_responses_usage(value: &Value) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let input = u64_field_default(usage, "input_tokens", 0)?;
    let output = u64_field_default(usage, "output_tokens", 0)?;
    let cached = usage
        .get("input_tokens_details")
        .and_then(Value::as_object)
        .map(|details| u64_field_default(details, "cached_tokens", 0))
        .transpose()?
        .unwrap_or(0);
    let reasoning_tokens = usage
        .get("output_tokens_details")
        .and_then(Value::as_object)
        .map(|details| u64_field_default(details, "reasoning_tokens", 0))
        .transpose()?
        .unwrap_or(0);
    usage_value(
        input.saturating_sub(cached),
        output,
        reasoning_tokens,
        cached,
        0,
        usage.get("total_tokens").map(u64_value).transpose()?,
    )
}

pub(super) fn anthropic_usage(
    value: &Value,
    prior: Option<ModelUsage>,
) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let prior = prior.unwrap_or_default();
    let cache_read = u64_field_default(
        usage,
        "cache_read_input_tokens",
        prior.provider_cache_read_tokens,
    )?;
    let cache_write = u64_field_default(
        usage,
        "cache_creation_input_tokens",
        prior.provider_cache_write_tokens,
    )?;
    let cache_write_1h = usage
        .get("cache_creation")
        .and_then(Value::as_object)
        .map(|details| {
            u64_field_default(
                details,
                "ephemeral_1h_input_tokens",
                prior.provider_cache_write_1h_tokens,
            )
        })
        .transpose()?
        .unwrap_or(prior.provider_cache_write_1h_tokens);
    let mut normalized = usage_value(
        u64_field_default(usage, "input_tokens", prior.input_tokens_uncached)?,
        u64_field_default(usage, "output_tokens", prior.output_tokens)?,
        prior.reasoning_tokens,
        cache_read,
        cache_write,
        None,
    )?;
    normalized.provider_cache_write_1h_tokens = cache_write_1h;
    Ok(normalized.reconciled())
}

pub(super) fn google_usage(value: &Value) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let input = u64_field_default(usage, "promptTokenCount", 0)?;
    let cached = u64_field_default(usage, "cachedContentTokenCount", 0)?;
    usage_value(
        input.saturating_sub(cached),
        u64_field_default(usage, "candidatesTokenCount", 0)?,
        u64_field_default(usage, "thoughtsTokenCount", 0)?,
        cached,
        0,
        usage.get("totalTokenCount").map(u64_value).transpose()?,
    )
}

pub(super) fn interaction_thought_summary(
    index: u32,
    value: &Value,
) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
    array(value)?
        .iter()
        .map(|content| interaction_thought_summary_content(index, content))
        .collect::<Result<Vec<_>, _>>()
        .map(|events| events.into_iter().flatten().collect())
}

pub(super) fn interaction_thought_summary_content(
    index: u32,
    value: &Value,
) -> Result<Vec<ModelStreamEvent>, NormalizeError> {
    let content = json_object(value)?;
    match required_text_field(content, "type")? {
        "text" => Ok(non_empty_text(content.get("text"))?
            .map(|text| reasoning_summary(index, text))
            .transpose()?
            .into_iter()
            .collect()),
        _ => Err(NormalizeError::UnknownContentBlock),
    }
}

pub(super) fn interaction_usage(value: &Value) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let input = u64_field_default_any(
        usage,
        &[
            "total_input_tokens",
            "input_tokens",
            "prompt_tokens",
            "prompt_token_count",
        ],
        0,
    )?;
    let cached = u64_field_default_any(
        usage,
        &[
            "total_cached_tokens",
            "cached_tokens",
            "cached_input_tokens",
            "cached_content_token_count",
        ],
        0,
    )?;
    usage_value(
        input.saturating_sub(cached),
        u64_field_default_any(
            usage,
            &[
                "total_output_tokens",
                "output_tokens",
                "completion_tokens",
                "candidates_token_count",
            ],
            0,
        )?,
        u64_field_default_any(
            usage,
            &[
                "total_thought_tokens",
                "reasoning_tokens",
                "thoughts_token_count",
            ],
            0,
        )?,
        cached,
        0,
        usage
            .get("total_tokens")
            .or(usage.get("total_token_count"))
            .map(u64_value)
            .transpose()?,
    )
}

pub(super) fn bedrock_usage(value: &Value) -> Result<ModelUsage, NormalizeError> {
    let usage = json_object(value)?;
    let input = u64_field_default_any(usage, &["inputTokens", "input_tokens"], 0)?;
    let cache_read = u64_field_default_any(
        usage,
        &["cacheReadInputTokens", "cache_read_input_tokens"],
        0,
    )?;
    let cache_write = u64_field_default_any(
        usage,
        &["cacheWriteInputTokens", "cache_write_input_tokens"],
        0,
    )?;
    usage_value(
        input.saturating_sub(cache_read),
        u64_field_default_any(usage, &["outputTokens", "output_tokens"], 0)?,
        0,
        cache_read,
        cache_write,
        usage
            .get("totalTokens")
            .or(usage.get("total_tokens"))
            .map(u64_value)
            .transpose()?,
    )
}

pub(super) fn usage_value(
    input_tokens_uncached: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    provider_cache_read_tokens: u64,
    provider_cache_write_tokens: u64,
    total_tokens: Option<u64>,
) -> Result<ModelUsage, NormalizeError> {
    let usage = ModelUsage {
        input_tokens_uncached,
        output_tokens,
        reasoning_tokens,
        provider_cache_read_tokens,
        provider_cache_write_tokens,
        provider_cache_write_1h_tokens: 0,
        total_tokens,
    };
    Ok(usage.reconciled())
}

pub(super) fn serde_to_canonical(value: &Value) -> Result<JsonValue, NormalizeError> {
    Ok(match value {
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::Bool(*value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                JsonValue::Integer(value)
            } else {
                JsonValue::Number(value.as_f64().ok_or(NormalizeError::InvalidJson)?)
            }
        }
        Value::String(value) => JsonValue::String(value.clone()),
        Value::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(serde_to_canonical)
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(values) => JsonValue::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), serde_to_canonical(value)?)))
                .collect::<Result<_, NormalizeError>>()?,
        ),
    })
}

pub(super) fn stop_reason(
    value: &str,
    format: musteragent_model::ApiFormat,
) -> Result<StopReason, NormalizeError> {
    match value.to_ascii_lowercase().as_str() {
        "stop" | "end_turn" | "stop_sequence" | "completed" | "finish_reason_stop"
        | "pause_turn" => Ok(StopReason::Stop),
        "tool_calls" | "tool_use" | "requires_action" => Ok(StopReason::ToolCalls),
        "length" | "max_tokens" | "max_output_tokens" | "max_tokens_exceeded" => {
            // OpenAI-compatible APIs do not distinguish output exhaustion from
            // reaching the context window during generation. Preserve that
            // ambiguity; only APIs with an explicit output-only contract may
            // assert MaxOutputTokens.
            use musteragent_model::ApiFormat;
            Ok(match format {
                ApiFormat::AnthropicMessages
                | ApiFormat::GoogleGenerateContent
                | ApiFormat::GoogleInteractions
                | ApiFormat::BedrockConverse => StopReason::MaxOutputTokens,
                ApiFormat::OpenAiResponses
                | ApiFormat::OpenAiChatCompletions
                | ApiFormat::OpenRouterDecisions
                | ApiFormat::TypeSafeSystemOne
                | ApiFormat::BedrockInvokeModel => StopReason::TokenLimit,
            })
        }
        "model_context_window_exceeded" | "context_window_exceeded" => {
            Ok(StopReason::ContextWindowExceeded)
        }
        "content_filter"
        | "safety"
        | "blocked"
        | "blocklist"
        | "prohibited_content"
        | "spii"
        | "recitation"
        | "refusal"
        | "sensitive"
        | "image_safety"
        | "image_prohibited_content"
        | "language" => Ok(StopReason::ContentFilter),
        "cancelled" | "canceled" => Ok(StopReason::Cancelled),
        // Transient generation failures: the attempt is retried, not the Run failed.
        "malformed_function_call"
        | "unexpected_tool_call"
        | "malformed_response"
        | "missing_thought_signature"
        | "insufficient_system_resource"
        | "error"
        | "abort" => Err(NormalizeError::UnknownStopReason),
        other => {
            // Unlisted reasons (`other`, `finish_reason_unspecified`,
            // `function_call`, `too_many_tool_calls`, ...) end the turn; the
            // emitted Tool calls, not this label, decide whether tools run.
            tracing::info!(
                provider.stop_reason = other,
                "Unlisted Provider stop reason treated as stop"
            );
            Ok(StopReason::Stop)
        }
    }
}

pub(super) fn google_output_index(
    candidate_index: usize,
    part_index: usize,
) -> Result<u32, NormalizeError> {
    let candidate = u32::try_from(candidate_index).map_err(|_| NormalizeError::IndexOverflow)?;
    let part = u32::try_from(part_index).map_err(|_| NormalizeError::IndexOverflow)?;
    candidate
        .checked_mul(65_536)
        .and_then(|base| base.checked_add(part))
        .ok_or(NormalizeError::IndexOverflow)
}

pub(super) fn bedrock_reasoning_text(
    delta: &serde_json::Map<String, Value>,
) -> Result<Option<&str>, NormalizeError> {
    let Some(value) = delta
        .get("reasoningContent")
        .or(delta.get("reasoning_content"))
        .or(delta.get("thinking"))
    else {
        return Ok(None);
    };
    if let Some(text) = value.as_str() {
        return Ok((!text.is_empty()).then_some(text));
    }
    let object = json_object(value)?;
    first_non_empty_text(object, &["text", "thinking"])
}

pub(super) fn bedrock_error_kind(event_type: &str) -> ProviderErrorKind {
    match event_type {
        "throttlingException" => ProviderErrorKind::RateLimited,
        "modelTimeoutException" => ProviderErrorKind::Timeout,
        "validationException" => ProviderErrorKind::InvalidRequest,
        _ => ProviderErrorKind::Server,
    }
}

pub(super) fn json_object(
    value: &Value,
) -> Result<&serde_json::Map<String, Value>, NormalizeError> {
    value.as_object().ok_or(NormalizeError::ObjectRequired)
}

pub(super) fn object_required<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<&'a serde_json::Map<String, Value>, NormalizeError> {
    object
        .get(field)
        .ok_or(NormalizeError::MissingField(field))
        .and_then(json_object)
}

pub(super) fn array(value: &Value) -> Result<&Vec<Value>, NormalizeError> {
    value.as_array().ok_or(NormalizeError::ArrayRequired)
}

pub(super) fn required_text(value: &Value) -> Result<&str, NormalizeError> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or(NormalizeError::TextRequired)
}

pub(super) fn string_value(value: &Value) -> Result<&str, NormalizeError> {
    value.as_str().ok_or(NormalizeError::TextRequired)
}

pub(super) fn required_text_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, NormalizeError> {
    object
        .get(field)
        .ok_or(NormalizeError::MissingField(field))
        .and_then(required_text)
}

// Transport fragments are strings, but may contain no new bytes. Identity and
// discriminator fields continue to use required_text_field's non-empty rule.
pub(super) fn string_field<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, NormalizeError> {
    object
        .get(field)
        .ok_or(NormalizeError::MissingField(field))
        .and_then(string_value)
}

pub(super) fn optional_text(value: Option<&Value>) -> Result<Option<&str>, NormalizeError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(NormalizeError::TextRequired),
    }
}

pub(super) fn non_empty_text(value: Option<&Value>) -> Result<Option<&str>, NormalizeError> {
    optional_text(value)
}

pub(super) fn first_non_empty_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    fields: &[&str],
) -> Result<Option<&'a str>, NormalizeError> {
    for field in fields {
        if let Some(value) = non_empty_text(object.get(*field))? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

pub(super) fn u64_value(value: &Value) -> Result<u64, NormalizeError> {
    value
        .as_u64()
        .ok_or(NormalizeError::UnsignedIntegerRequired)
}

pub(super) fn u64_field_default(
    object: &serde_json::Map<String, Value>,
    field: &str,
    default: u64,
) -> Result<u64, NormalizeError> {
    object
        .get(field)
        .map(u64_value)
        .transpose()
        .map(|value| value.unwrap_or(default))
}

pub(super) fn u64_field_default_any(
    object: &serde_json::Map<String, Value>,
    fields: &[&str],
    default: u64,
) -> Result<u64, NormalizeError> {
    fields
        .iter()
        .find_map(|field| object.get(*field))
        .map(u64_value)
        .transpose()
        .map(|value| value.unwrap_or(default))
}

pub(super) fn u32_field_default(
    object: &serde_json::Map<String, Value>,
    field: &str,
    default: u32,
) -> Result<u32, NormalizeError> {
    let value = u64_field_default(object, field, u64::from(default))?;
    u32::try_from(value).map_err(|_| NormalizeError::IndexOverflow)
}

pub(super) fn u32_field_default_any(
    object: &serde_json::Map<String, Value>,
    fields: &[&str],
    default: u32,
) -> Result<u32, NormalizeError> {
    let value = u64_field_default_any(object, fields, u64::from(default))?;
    u32::try_from(value).map_err(|_| NormalizeError::IndexOverflow)
}

#[cfg(test)]
mod termination_tests {
    use super::*;
    use musteragent_model::ApiFormat;
    #[test]
    fn token_stop_semantics_belong_to_each_provider_protocol() {
        for (format, reason, expected) in [
            (
                ApiFormat::OpenAiResponses,
                "max_output_tokens",
                StopReason::TokenLimit,
            ),
            (
                ApiFormat::OpenAiChatCompletions,
                "length",
                StopReason::TokenLimit,
            ),
            (
                ApiFormat::AnthropicMessages,
                "max_tokens",
                StopReason::MaxOutputTokens,
            ),
            (
                ApiFormat::GoogleGenerateContent,
                "MAX_TOKENS",
                StopReason::MaxOutputTokens,
            ),
            (
                ApiFormat::BedrockConverse,
                "max_tokens",
                StopReason::MaxOutputTokens,
            ),
        ] {
            assert_eq!(
                stop_reason(reason, format).expect("valid test fixture"),
                expected
            );
            assert_eq!(
                stop_reason("context_window_exceeded", format).expect("valid test fixture"),
                StopReason::ContextWindowExceeded
            );
        }
    }
}
