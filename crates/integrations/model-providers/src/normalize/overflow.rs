//! Typed, deterministic classification of explicit Provider context-capacity
//! overflow signals.
//!
//! Only protocol/vendor error codes that are verified to mean "the request
//! input exceeded the model context window" classify as overflow. Generic
//! 400/413/429 responses, authentication or authorization failures, output
//! token limits, malformed tool schemas and unknown error codes deliberately
//! keep their original classification: an unknown failure must never be
//! recovered as if the context window were exceeded.

use musteragent_model::ApiFormat;
use musteragent_runtime_model::StopReason;
use serde_json::Value;

/// Which physical signal carried the explicit overflow fact. The recovery
/// path treats every source identically; the distinction is retained for
/// audit and diagnostics.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextOverflowSource {
    /// A non-success HTTP response whose structured error body carries a
    /// verified context-capacity code.
    HttpErrorResponse,
    /// An HTTP 200 stream whose in-band error event carries a verified
    /// context-capacity code (the transport status alone proved nothing).
    StreamErrorEvent,
    /// A trusted terminal stop reason reported by the Provider protocol.
    TerminalStopReason,
}

/// Closed, log-safe classification result. `provider_code` is the normalized
/// verified code from the closed matrix, never a raw Provider body.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExplicitContextOverflow {
    pub source: ContextOverflowSource,
    pub provider_code: &'static str,
}

/// Verified `(ApiFormat, HTTP status, normalized error code)` triples. Codes
/// are matched after ASCII-lowercasing, exactly as the shared error-kind
/// classifier normalizes them.
const HTTP_OVERFLOW_MATRIX: &[(ApiFormat, u16, &str)] = &[
    // OpenAI Responses / Chat Completions reject oversized input with HTTP 400
    // and `error.code = "context_length_exceeded"`.
    (ApiFormat::OpenAiResponses, 400, "context_length_exceeded"),
    (
        ApiFormat::OpenAiChatCompletions,
        400,
        "context_length_exceeded",
    ),
    // Anthropic request_too_large is an HTTP byte limit, NOT a token limit.
    // Its context-window error needs the typed message check below.
];

/// Verified streamed error codes per protocol. An HTTP 200 SSE stream carries
/// no status, so admission rests on the structured code alone.
const STREAM_OVERFLOW_MATRIX: &[(ApiFormat, &str)] = &[
    (ApiFormat::OpenAiResponses, "context_length_exceeded"),
    (ApiFormat::OpenAiChatCompletions, "context_length_exceeded"),
];

/// Classifies a non-success HTTP response. `provider_code` is the bounded,
/// normalized code already extracted from the structured error body; bodies
/// without a verified code classify nothing.
#[must_use]
pub fn classify_http_error(
    format: ApiFormat,
    status: u16,
    provider_code: Option<&str>,
) -> Option<ExplicitContextOverflow> {
    let code = provider_code?;
    HTTP_OVERFLOW_MATRIX
        .iter()
        .find(|(candidate, expected_status, expected_code)| {
            *candidate == format && *expected_status == status && *expected_code == code
        })
        .map(|(_, _, code)| ExplicitContextOverflow {
            source: ContextOverflowSource::HttpErrorResponse,
            provider_code: code,
        })
}

/// Classifies an in-band stream error event (`error`, `response.failed` and
/// protocol equivalents). Only the stable `code`/`type` metadata fields are
/// read; message text never crosses the canonical boundary.
#[must_use]
pub fn classify_stream_error_event(
    format: ApiFormat,
    value: &Value,
) -> Option<ExplicitContextOverflow> {
    if format == ApiFormat::AnthropicMessages && anthropic_prompt_too_long(value) {
        return Some(ExplicitContextOverflow {
            source: ContextOverflowSource::StreamErrorEvent,
            provider_code: "prompt_too_long",
        });
    }
    let code = stream_error_code(value, 0)?;
    STREAM_OVERFLOW_MATRIX
        .iter()
        .find(|(candidate, expected_code)| *candidate == format && *expected_code == code)
        .map(|(_, code)| ExplicitContextOverflow {
            source: ContextOverflowSource::StreamErrorEvent,
            provider_code: code,
        })
}

/// Claude documents input overflow as HTTP 400 invalid_request_error with
/// the exact "prompt is too long" diagnostic. HTTP 413 is a request-byte
/// limit and is deliberately excluded. No raw message escapes this module.
/// https://platform.claude.com/docs/en/build-with-claude/context-windows
pub fn classify_http_body(
    format: ApiFormat,
    status: u16,
    value: &Value,
) -> Option<ExplicitContextOverflow> {
    if format == ApiFormat::AnthropicMessages && status == 400 && anthropic_prompt_too_long(value) {
        return Some(ExplicitContextOverflow {
            source: ContextOverflowSource::HttpErrorResponse,
            provider_code: "prompt_too_long",
        });
    }
    classify_http_error(format, status, stream_error_code(value, 0))
}

fn anthropic_prompt_too_long(value: &Value) -> bool {
    let error = value.get("error").unwrap_or(value);
    error.get("type").and_then(Value::as_str) == Some("invalid_request_error")
        && error
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| {
                message == "prompt is too long" || message.starts_with("prompt is too long:")
            })
}

/// Classifies a trusted terminal stop reason.
#[must_use]
pub const fn classify_terminal_stop_reason(stop: StopReason) -> Option<ExplicitContextOverflow> {
    match stop {
        StopReason::ContextWindowExceeded => Some(ExplicitContextOverflow {
            source: ContextOverflowSource::TerminalStopReason,
            provider_code: "context_window_exceeded",
        }),
        _ => None,
    }
}

fn stream_error_code(value: &Value, depth: u8) -> Option<&str> {
    if depth > 4 {
        return None;
    }
    let object = value.as_object()?;
    for key in ["code", "type"] {
        if let Some(code) = object.get(key).and_then(Value::as_str) {
            let normalized: String = code.trim().to_ascii_lowercase();
            // Only the closed matrix is truth; return the canonical static
            // form so callers never handle raw Provider strings.
            if let Some(canonical) = STREAM_OVERFLOW_MATRIX
                .iter()
                .map(|(_, code)| *code)
                .find(|candidate| *candidate == normalized)
            {
                return Some(canonical);
            }
        }
    }
    for key in ["error", "response", "details", "cause"] {
        if let Some(code) = object
            .get(key)
            .and_then(|nested| stream_error_code(nested, depth.saturating_add(1)))
        {
            return Some(code);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_classification_requires_the_verified_status_and_code_pair() {
        assert_eq!(
            classify_http_error(
                ApiFormat::OpenAiResponses,
                400,
                Some("context_length_exceeded")
            ),
            Some(ExplicitContextOverflow {
                source: ContextOverflowSource::HttpErrorResponse,
                provider_code: "context_length_exceeded",
            })
        );
        assert_eq!(
            classify_http_error(ApiFormat::AnthropicMessages, 413, Some("request_too_large")),
            None
        );
    }

    #[test]
    fn generic_http_failures_never_classify_as_overflow() {
        for (format, status, code) in [
            // Unknown 400 keeps its original invalid-request classification.
            (
                ApiFormat::OpenAiResponses,
                400,
                Some("invalid_request_error"),
            ),
            // Neither bare nor typed request-byte limits are token overflow.
            (ApiFormat::AnthropicMessages, 413, Some("request_too_large")),
            (ApiFormat::AnthropicMessages, 413, None),
            (ApiFormat::OpenAiResponses, 413, Some("request_too_large")),
            // Rate limiting and authentication are separate recovery classes.
            (ApiFormat::OpenAiResponses, 429, Some("rate_limit_exceeded")),
            (ApiFormat::OpenAiResponses, 401, None),
            (ApiFormat::AnthropicMessages, 403, Some("permission_error")),
            // Verified codes on unverified routes stay unknown.
            (
                ApiFormat::GoogleGenerateContent,
                400,
                Some("context_length_exceeded"),
            ),
            (ApiFormat::BedrockConverse, 400, Some("validationexception")),
        ] {
            assert_eq!(
                classify_http_error(format, status, code),
                None,
                "{format:?} {status} {code:?}"
            );
        }
    }

    #[test]
    fn stream_error_classification_reads_nested_codes_without_message_text() {
        let openai_failed = serde_json::json!({
            "type": "response.failed",
            "response": {"status": "failed", "error": {"code": "context_length_exceeded"}}
        });
        assert_eq!(
            classify_stream_error_event(ApiFormat::OpenAiResponses, &openai_failed),
            Some(ExplicitContextOverflow {
                source: ContextOverflowSource::StreamErrorEvent,
                provider_code: "context_length_exceeded",
            })
        );
        let anthropic_error = serde_json::json!({
            "type": "error",
            "error": {"type": "invalid_request_error", "message": "prompt is too long: 210000 tokens > 200000 maximum"}
        });
        assert_eq!(
            classify_stream_error_event(ApiFormat::AnthropicMessages, &anthropic_error),
            Some(ExplicitContextOverflow {
                source: ContextOverflowSource::StreamErrorEvent,
                provider_code: "prompt_too_long",
            })
        );
    }

    #[test]
    fn unknown_stream_errors_and_other_protocols_do_not_classify() {
        for (format, value) in [
            (
                ApiFormat::OpenAiResponses,
                serde_json::json!({"type": "error", "error": {"code": "server_error"}}),
            ),
            (
                ApiFormat::AnthropicMessages,
                serde_json::json!({"type": "error", "error": {"type": "overloaded_error"}}),
            ),
            (
                ApiFormat::OpenAiResponses,
                serde_json::json!({"type": "error", "error": {"code": "rate_limit_exceeded"}}),
            ),
            (
                ApiFormat::GoogleGenerateContent,
                serde_json::json!({"error": {"code": "context_length_exceeded"}}),
            ),
            (
                ApiFormat::OpenAiResponses,
                serde_json::json!({"type": "error"}),
            ),
        ] {
            assert_eq!(
                classify_stream_error_event(format, &value),
                None,
                "{format:?} {value}"
            );
        }
    }

    #[test]
    fn only_the_context_window_stop_reason_classifies() {
        assert_eq!(
            classify_terminal_stop_reason(StopReason::ContextWindowExceeded),
            Some(ExplicitContextOverflow {
                source: ContextOverflowSource::TerminalStopReason,
                provider_code: "context_window_exceeded",
            })
        );
        for stop in [
            StopReason::Stop,
            StopReason::ToolCalls,
            // Neither an output-only nor an ambiguous signal proves overflow.
            StopReason::MaxOutputTokens,
            StopReason::TokenLimit,
            StopReason::ContentFilter,
            StopReason::Cancelled,
        ] {
            assert_eq!(classify_terminal_stop_reason(stop), None, "{stop:?}");
        }
    }
}
