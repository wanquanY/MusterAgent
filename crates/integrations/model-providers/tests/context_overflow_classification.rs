//! Integration coverage for the typed explicit context-capacity overflow
//! classification: streamed error events and terminal stop reasons across the
//! approved wire protocols. Unknown or generic failures must keep their
//! original classification.

use musteragent_identity::VersionedKey;
use musteragent_model::ApiFormat;
use musteragent_model_providers::ProviderNormalizer;
use musteragent_runtime_model::{
    ModelRouteRef, ModelStreamEvent, ModelStreamState, ProviderErrorCode, ProviderErrorKind,
    StopReason,
};
use serde_json::Value;

fn fixture_route() -> ModelRouteRef {
    ModelRouteRef::from_key(VersionedKey::new("model.route.fixture@1").expect("fixture route"))
}

fn replay(format: ApiFormat, source: &str) -> Vec<ModelStreamEvent> {
    let mut normalizer = ProviderNormalizer::new(
        format,
        fixture_route(),
        "fixture-model",
        "fixture-invocation",
    )
    .unwrap_or_else(|error| panic!("normalizer creation failed: {error}"));
    let mut state = ModelStreamState::default();
    let mut output = Vec::new();
    for line in source.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).expect("fixture JSON");
        let event = value
            .get("event")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        let events = normalizer
            .observe(event.as_deref(), data)
            .unwrap_or_else(|error| panic!("fixture normalization failed: {error}"));
        for event in events {
            state
                .accept(&event)
                .unwrap_or_else(|error| panic!("canonical state rejected fixture: {error}"));
            output.push(event);
        }
    }
    if !state.is_terminal() {
        for event in normalizer
            .finish()
            .unwrap_or_else(|error| panic!("fixture finish failed: {error}"))
        {
            state
                .accept(&event)
                .unwrap_or_else(|error| panic!("canonical state rejected terminal: {error}"));
            output.push(event);
        }
    }
    output
}

fn response_error(events: &[ModelStreamEvent]) -> (ProviderErrorKind, ProviderErrorCode, bool) {
    events
        .iter()
        .find_map(|event| match event {
            ModelStreamEvent::ResponseError(error) => {
                Some((error.kind, error.safe_code, error.retryable))
            }
            _ => None,
        })
        .expect("fixture must end in a ResponseError")
}

#[test]
fn openai_streamed_context_overflow_is_a_typed_capacity_failure() {
    let events = replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_overflow\",\"status\":\"in_progress\"}}}\n",
            "{\"event\":\"response.failed\",\"data\":{\"type\":\"response.failed\",\"response\":{\"id\":\"resp_overflow\",\"status\":\"failed\",\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"too many tokens\"}}}}\n",
        ),
    );
    assert_eq!(
        response_error(&events),
        (
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::ContextWindowExceeded,
            false
        )
    );
}

#[test]
fn anthropic_typed_prompt_too_long_is_a_capacity_failure() {
    let events = replay(
        ApiFormat::AnthropicMessages,
        concat!(
            "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_overflow\",\"model\":\"fixture-model\"}}}\n",
            "{\"event\":\"error\",\"data\":{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"prompt is too long: 210000 tokens > 200000 maximum\"}}}\n",
        ),
    );
    assert_eq!(
        response_error(&events),
        (
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::ContextWindowExceeded,
            false
        )
    );
}

#[test]
fn unknown_or_generic_streamed_errors_keep_their_original_classification() {
    let (kind, code, retryable) = response_error(&replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_unknown\",\"status\":\"in_progress\"}}}\n",
            "{\"event\":\"response.failed\",\"data\":{\"type\":\"response.failed\",\"response\":{\"id\":\"resp_unknown\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"oops\"}}}}\n",
        ),
    ));
    assert_eq!(kind, ProviderErrorKind::Server);
    assert_eq!(code, ProviderErrorCode::ProviderServer);
    assert!(retryable);

    let (kind, code, retryable) = response_error(&replay(
        ApiFormat::AnthropicMessages,
        concat!(
            "{\"event\":\"message_start\",\"data\":{\"type\":\"message_start\",\"message\":{\"id\":\"msg_busy\",\"model\":\"fixture-model\"}}}\n",
            "{\"event\":\"error\",\"data\":{\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}}\n",
        ),
    ));
    assert_eq!(kind, ProviderErrorKind::Server);
    assert_eq!(code, ProviderErrorCode::ProviderServer);
    assert!(retryable);

    // A streamed upstream 429 remains rate limiting, never context overflow.
    let (kind, code, retryable) = response_error(&replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_limited\",\"status\":\"in_progress\"}}}\n",
            "{\"event\":\"response.failed\",\"data\":{\"type\":\"response.failed\",\"response\":{\"id\":\"resp_limited\",\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"upstream_status\":429}}}}\n",
        ),
    ));
    assert_eq!(kind, ProviderErrorKind::RateLimited);
    assert_eq!(code, ProviderErrorCode::RateLimited);
    assert!(retryable);

    // An unknown 400 stays an ordinary invalid request.
    let (kind, code, retryable) = response_error(&replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\",\"status\":\"in_progress\"}}}\n",
            "{\"event\":\"response.failed\",\"data\":{\"type\":\"response.failed\",\"response\":{\"id\":\"resp_bad\",\"status\":\"failed\",\"error\":{\"code\":\"invalid_request_error\",\"upstream_status\":400}}}}\n",
        ),
    ));
    assert_eq!(kind, ProviderErrorKind::InvalidRequest);
    assert_eq!(code, ProviderErrorCode::InvalidRequest);
    assert!(!retryable);
}

#[test]
fn terminal_stop_reason_classification_stays_unchanged() {
    // response.incomplete with reason model_context_window_exceeded already
    // reaches the loop as StopReason::ContextWindowExceeded; the typed
    // classifier maps exactly that terminal reason and nothing else.
    let events = replay(
        ApiFormat::OpenAiResponses,
        concat!(
            "{\"event\":\"response.created\",\"data\":{\"type\":\"response.created\",\"response\":{\"id\":\"resp_incomplete\",\"status\":\"in_progress\"}}}\n",
            "{\"event\":\"response.incomplete\",\"data\":{\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_incomplete\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"model_context_window_exceeded\"}}}}\n",
        ),
    );
    assert!(events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ContextWindowExceeded
        }
    )));
    assert_eq!(
        musteragent_model_providers::classify_terminal_stop_reason(
            StopReason::ContextWindowExceeded
        )
        .map(|overflow| overflow.source),
        Some(musteragent_model_providers::ContextOverflowSource::TerminalStopReason)
    );
    assert_eq!(
        musteragent_model_providers::classify_terminal_stop_reason(StopReason::TokenLimit),
        None
    );
}
