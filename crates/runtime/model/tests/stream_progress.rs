use musteragent_identity::ToolCallId;
use musteragent_model::ModelKey;
use musteragent_runtime_model::{
    JsonValue, ModelStreamEvent as Event, ModelStreamProgress as Progress, ModelStreamState,
    ModelStreamStateError as Error, ModelUsage, ProviderRequestRef, StopReason,
};
use musteragent_tool::WireName;

fn start(state: &mut ModelStreamState) {
    assert_eq!(
        state
            .accept_with_progress(&Event::ResponseStarted {
                response_ref: ProviderRequestRef::from_opaque(
                    musteragent_runtime_contract::OpaqueExternalRef::new("response-1")
                        .expect("response")
                ),
                response_model_key: ModelKey::new("test-model@1").expect("model"),
            })
            .expect("start"),
        Progress::Accepted
    );
}

#[test]
fn local_tool_name_rejection_cannot_claim_native_model_end() {
    let mut state = ModelStreamState::default();
    start(&mut state);
    assert_eq!(
        state
            .accept_with_progress(&Event::ToolCallUnexposed {
                name: WireName::new("unexposed_tool").expect("name"),
            })
            .expect("local rejection"),
        Progress::Unchanged
    );
    assert!(
        !state.is_terminal(),
        "local rejection cannot close a physical response"
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::Usage(usage(1)))
            .expect("usage"),
        Progress::Advanced
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::ResponseCompleted {
                stop_reason: StopReason::ToolCalls
            })
            .expect("physical end"),
        Progress::Terminal
    );
    assert!(state.is_terminal());
}
fn usage(output: u64) -> ModelUsage {
    ModelUsage {
        input_tokens_uncached: 10,
        output_tokens: output,
        reasoning_tokens: 0,
        provider_cache_read_tokens: 0,
        provider_cache_write_tokens: 0,
        provider_cache_write_1h_tokens: 0,
        total_tokens: Some(10 + output),
    }
}
#[test]
fn unchanged_usage_and_empty_tool_fragments_cannot_renew_model_execution() {
    let mut state = ModelStreamState::default();
    start(&mut state);
    assert_eq!(
        state
            .accept_with_progress(&Event::Usage(usage(1)))
            .expect("usage"),
        Progress::Advanced
    );
    for _ in 0..1_000 {
        assert_eq!(
            state
                .accept_with_progress(&Event::Usage(usage(1)))
                .expect("repeated accounting"),
            Progress::Unchanged
        );
    }
    let mut metadata = usage(1);
    metadata.total_tokens = Some(12);
    assert_eq!(
        state
            .accept_with_progress(&Event::Usage(metadata))
            .expect("aggregate detail"),
        Progress::Unchanged
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::Usage(usage(2)))
            .expect("new token"),
        Progress::Advanced
    );
    assert_eq!(
        state.accept_with_progress(&Event::Usage(usage(1))),
        Err(Error::UsageRegressed)
    );
    let call_id = ToolCallId::new("call-1").expect("call");
    assert_eq!(
        state
            .accept_with_progress(&Event::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("read_file").expect("wire name"),
                provider_item_ref: None,
                provider_replay: None
            })
            .expect("new call"),
        Progress::Advanced
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::ToolCallArgumentsDelta {
                output_index: 0,
                call_id: call_id.clone(),
                delta: String::new()
            })
            .expect("empty fragment"),
        Progress::Unchanged
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::ToolCallArgumentsDelta {
                output_index: 0,
                call_id,
                delta: "{}".into()
            })
            .expect("new fragment"),
        Progress::Advanced
    );
}
#[test]
fn rejected_duplicate_tool_identity_does_not_reopen_the_completed_call() {
    let mut state = ModelStreamState::default();
    start(&mut state);
    let call_id = ToolCallId::new("call-1").expect("call");
    let started = Event::ToolCallStarted {
        output_index: 0,
        call_id: call_id.clone(),
        name: WireName::new("read_file").expect("wire name"),
        provider_item_ref: None,
        provider_replay: None,
    };
    state.accept_with_progress(&started).expect("call");
    state
        .accept_with_progress(&Event::ToolCallCompleted {
            output_index: 0,
            call_id: call_id.clone(),
            arguments: JsonValue::Object(Default::default()),
        })
        .expect("complete");
    assert_eq!(
        state.accept_with_progress(&started),
        Err(Error::DuplicateToolCall)
    );
    assert_eq!(
        state.accept_with_progress(&Event::ToolCallArgumentsDelta {
            output_index: 0,
            call_id,
            delta: "more".into()
        }),
        Err(Error::UnknownOrCompletedToolCall)
    );
    assert_eq!(
        state
            .accept_with_progress(&Event::ResponseCompleted {
                stop_reason: StopReason::ToolCalls
            })
            .expect("unchanged completed state"),
        Progress::Terminal
    );
    assert!(state.is_terminal());
    assert_eq!(
        state.accept_with_progress(&Event::Usage(usage(5))),
        Err(Error::EventAfterTerminal)
    );
}
