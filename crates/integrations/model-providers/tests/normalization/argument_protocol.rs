use super::*;
use musteragent_runtime_model::{ArgumentSource, JsonParseCategory};
use serde_json::json;

fn responses() -> ProviderNormalizer {
    let mut value = ProviderNormalizer::new(
        ApiFormat::OpenAiResponses,
        fixture_route(),
        "fixture-model",
        "argument-protocol",
    )
    .expect("normalizer");
    value.observe(Some("response.output_item.added"), json!({"output_index":0,"item":{"type":"function_call","id":"fc","call_id":"call","name":"presentation_author","arguments":""}})).expect("start");
    value
}

#[test]
fn conflicting_snapshots_never_replace_observed_arguments() {
    for stage in [
        "response.function_call_arguments.done",
        "response.output_item.done",
        "response.completed",
    ] {
        let mut value = responses();
        value
            .observe(
                Some("response.function_call_arguments.delta"),
                json!({"output_index":0,"delta":"{\"page\":1}"}),
            )
            .expect("delta");
        let body = match stage {
            "response.function_call_arguments.done" => {
                json!({"output_index":0,"arguments":"{\"page\":2}"})
            }
            "response.output_item.done" => {
                json!({"output_index":0,"item":{"type":"function_call","arguments":"{\"page\":2}"}})
            }
            _ => {
                json!({"response":{"output":[{"type":"function_call","call_id":"call","name":"presentation_author","arguments":"{\"page\":2}"}]}})
            }
        };
        assert!(
            matches!(
                value.observe(Some(stage), body),
                Err(NormalizeError::ConflictingToolArguments)
            ),
            "{stage}"
        );
    }
    let mut value = responses();
    value
        .observe(
            Some("response.function_call_arguments.done"),
            json!({"arguments":"{}"}),
        )
        .expect("snapshot only");
    assert!(
        matches!(
            value.observe(
                Some("response.output_item.done"),
                json!({"item":{"arguments":""}})
            ),
            Err(NormalizeError::ConflictingToolArguments)
        ),
        "empty terminal snapshots cannot erase arguments"
    );
}

#[test]
fn matching_and_snapshot_only_arguments_complete_once() {
    for streamed in [false, true] {
        let mut value = responses();
        if streamed {
            value
                .observe(
                    Some("response.function_call_arguments.delta"),
                    json!({"delta":"{\"page\":1}"}),
                )
                .expect("delta");
        }
        value
            .observe(
                Some("response.function_call_arguments.done"),
                json!({"arguments":"{\"page\":1}"}),
            )
            .expect("done");
        value
            .observe(
                Some("response.output_item.done"),
                json!({"item":{"arguments":"{\"page\":1}"}}),
            )
            .expect("item done");
        let events=value.observe(Some("response.completed"),json!({"response":{"output":[{"type":"function_call","call_id":"call","name":"presentation_author","arguments":"{\"page\":1}"}]}})).expect("terminal");
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, ModelStreamEvent::ToolCallCompleted { .. }))
                .count(),
            1
        );
    }
}

#[test]
fn parse_evidence_preserves_position_and_fingerprint_without_argument_values() {
    for (arguments, category) in [
        ("{\n\"private-value\":", JsonParseCategory::Eof),
        ("{\n\"private-value\": ?}", JsonParseCategory::Syntax),
    ] {
        let mut value = responses();
        value
            .observe(
                Some("response.function_call_arguments.done"),
                json!({"arguments":arguments}),
            )
            .expect("snapshot");
        let events = value
            .observe(Some("response.completed"), json!({}))
            .expect("normal response, bad arguments");
        let diagnostic = events
            .iter()
            .find_map(|e| match e {
                ModelStreamEvent::ToolCallRejected { diagnostic, .. } => diagnostic.as_ref(),
                _ => None,
            })
            .expect("evidence");
        assert_eq!(diagnostic.source, ArgumentSource::ArgumentsDone);
        assert_eq!(diagnostic.received_bytes, arguments.len() as u64);
        assert_eq!(diagnostic.parse.as_ref().expect("parse").category, category);
        assert_eq!(diagnostic.parse.as_ref().expect("parse").line, 2);
        assert_eq!(
            diagnostic.generation_limit, None,
            "only the Loop pins the actual terminal"
        );
        assert!(
            !String::from_utf8(diagnostic.encode().expect("bounded diagnostic"))
                .expect("UTF-8")
                .contains("private-value")
        );
    }
}

#[test]
fn incomplete_without_a_reason_is_not_output_token_exhaustion() {
    for body in [
        json!({}),
        json!({"response":{"status":"incomplete"}}),
        json!({"response":{"incomplete_details":{"reason":"unknown"}}}),
    ] {
        assert!(
            responses()
                .observe(Some("response.incomplete"), body)
                .is_err()
        );
    }
}

#[test]
fn authoritative_snapshot_has_the_same_capacity_guard_as_stream_deltas() {
    let mut value = responses();
    let mut budget =
        musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
            .model_stream;
    budget.max_tool_argument_bytes = std::num::NonZeroUsize::new(8).expect("limit");
    value = value.with_stream_budget(budget).expect("budget");
    assert!(matches!(
        value.observe(
            Some("response.function_call_arguments.done"),
            json!({"arguments":"{\"long\":1}"})
        ),
        Err(NormalizeError::StreamBudgetExceeded)
    ));
}

#[test]
fn google_tool_presence_cannot_hide_output_exhaustion() {
    let mut value = ProviderNormalizer::new(
        ApiFormat::GoogleGenerateContent,
        fixture_route(),
        "fixture-model",
        "google-limit",
    )
    .expect("normalizer");
    let events=value.observe(None,json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"presentation_author","args":{"page":1}}}]},"finishReason":"MAX_TOKENS"}]})).expect("explicit terminal");
    assert!(matches!(
        events.last(),
        Some(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::MaxOutputTokens
        })
    ));
}

#[test]
fn duplicate_start_or_late_delta_cannot_reset_an_authoritative_snapshot() {
    let mut value = responses();
    value
        .observe(
            Some("response.function_call_arguments.done"),
            json!({"arguments":"{}"}),
        )
        .expect("snapshot");
    assert!(value.observe(Some("response.output_item.added"),json!({"item":{"type":"function_call","call_id":"call","name":"presentation_author","arguments":"{}"}})).is_err());
    assert!(
        value
            .observe(
                Some("response.function_call_arguments.delta"),
                json!({"delta":"{}"})
            )
            .is_err()
    );
}

#[test]
fn final_snapshot_cannot_change_tool_identity() {
    for item in [
        json!({"type":"function_call","call_id":"other","arguments":"{}"}),
        json!({"type":"function_call","name":"other","arguments":"{}"}),
        json!({"type":"function_call","id":"other","arguments":"{}"}),
        json!({"type":"function_call","name":false,"arguments":"{}"}),
    ] {
        assert!(
            responses()
                .observe(Some("response.output_item.done"), json!({"item":item}))
                .is_err()
        );
        assert!(
            responses()
                .observe(
                    Some("response.completed"),
                    json!({"response":{"output":[item]}})
                )
                .is_err()
        );
    }
}
