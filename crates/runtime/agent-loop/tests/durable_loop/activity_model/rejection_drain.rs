//! Full original Loop/reactor/ordered-Journal journeys; no illegal Tool host.
use super::*;

fn rejected_prefix() -> Vec<Result<ModelStreamEvent, ProviderError>> {
    vec![
        started(),
        Ok(ModelStreamEvent::ToolCallUnexposed {
            name: wire_name("internal_only_tool"),
        }),
    ]
}
fn terminal_usage() -> ModelUsage {
    ModelUsage {
        input_tokens_uncached: 10,
        output_tokens: 5,
        total_tokens: Some(15),
        ..Default::default()
    }
}

#[tokio::test]
async fn rejected_action_drains_real_terminal_without_admitting_any_sibling() {
    let mut events = vec![
        started(),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: tool_call_id("before-rejection"),
            name: wire_name("visible_tool"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id: tool_call_id("before-rejection"),
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::ToolCallUnexposed {
            name: wire_name("internal_only_tool"),
        }),
    ];
    // Even a normally exposed, fully formed sibling remains private after the
    // response's first rejection. Its body must not appear in public output.
    events.extend([
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 1,
            call_id: tool_call_id("sibling"),
            name: wire_name("visible_tool"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 1,
            call_id: tool_call_id("sibling"),
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 2,
            delta: model_text("discarded after rejected action"),
        }),
        Ok(ModelStreamEvent::Usage(terminal_usage())),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        }),
    ]);
    let (f, runtime, journal, provider) = runtime_with_scripts(
        vec![
            Script::EventsThenPending(events),
            Script::Events(assistant_events("corrected", "Recovered.")),
        ],
        Duration::ZERO,
        false,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
    );
    let exit = runtime
        .execute(
            f.attempt.clone(),
            f.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    let rejected = journal.inner.action_rejections();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].observation.usage, Some(terminal_usage()));
    assert!(!rejected[0].observation.public_output_observed);
    assert!(journal.inner.committed_tool_calls().is_empty());
    let batches = journal.inner.batches();
    let first = provider.scripts.requests()[0].invocation.invocation_id;
    let events = batches
        .iter()
        .filter(|batch| batch.invocation_id == first)
        .flat_map(|batch| &batch.events)
        .collect::<Vec<_>>();
    assert!(events.iter().all(|event| matches!(
        event,
        ModelStreamEvent::ResponseStarted { .. }
            | ModelStreamEvent::Usage(_)
            | ModelStreamEvent::ResponseCompleted { .. }
    )));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
    );
    assert!(
        lock(&lock(&provider.controls)[0].calls).is_empty(),
        "real terminal commits without inventing a requested stop"
    );
    assert_eq!(provider.scripts.requests().len(), 2);
}

#[tokio::test]
async fn rejected_action_eof_and_idle_preserve_unknown_stop_and_correction() {
    for pending in [false, true] {
        let prefix = rejected_prefix();
        let first = if pending {
            Script::EventsThenPending(prefix)
        } else {
            Script::Events(prefix)
        };
        let (f, runtime, journal, provider) = runtime_with_scripts(
            vec![
                first,
                Script::Events(assistant_events("corrected", "Recovered.")),
            ],
            Duration::ZERO,
            false,
            Arc::new(CompletingTools),
            ToolExecutionMode::Parallel,
        );
        let exit = runtime
            .execute(
                f.attempt.clone(),
                f.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
        assert_eq!(
            journal.inner.action_rejections().len(),
            1,
            "reactor cancellation of the drain must preserve the first rejection"
        );
        let controls = lock(&provider.controls);
        assert!(!controls[0].terminal.load(Ordering::SeqCst));
        assert!(!lock(&controls[0].calls).is_empty());
        assert!(journal.inner.committed_tool_calls().is_empty());
        let first = provider.scripts.requests()[0].invocation.invocation_id;
        assert!(
            !journal
                .inner
                .batches()
                .iter()
                .filter(|batch| batch.invocation_id == first)
                .flat_map(|batch| &batch.events)
                .any(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
        );
    }
}

#[tokio::test]
async fn rejected_action_records_the_latest_provider_usage_report() {
    let corrected = ModelUsage {
        input_tokens_uncached: 1,
        total_tokens: Some(1),
        ..Default::default()
    };
    let mut events = rejected_prefix();
    events.extend([
        Ok(ModelStreamEvent::Usage(terminal_usage())),
        Ok(ModelStreamEvent::Usage(corrected)),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        }),
    ]);
    let (f, runtime, journal, _provider) = runtime_with_scripts(
        vec![
            Script::Events(events),
            Script::Events(assistant_events("corrected", "Recovered.")),
        ],
        Duration::ZERO,
        false,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
    );
    let exit = runtime
        .execute(
            f.attempt.clone(),
            f.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    // A Provider correction downwards is its latest report, not a protocol failure.
    assert_eq!(
        journal.inner.action_rejections()[0].observation.usage,
        Some(corrected)
    );
    assert!(journal.inner.committed_tool_calls().is_empty());
}

#[tokio::test]
async fn user_stop_wins_over_rejected_action_drain() {
    let (f, runtime, journal, provider) = runtime_with_scripts(
        vec![Script::EventsThenPending(rejected_prefix())],
        Duration::ZERO,
        false,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
    );
    let supervisor = InvocationSupervisor::new();
    let execution = runtime.execute(f.attempt.clone(), f.telemetry(), supervisor.clone());
    let stop = async {
        while provider.reads.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        supervisor.cancel();
    };
    let (exit, ()) = tokio::join!(execution, stop);
    assert!(matches!(exit, AgentLoopExit::Cancelled { .. }), "{exit:?}");
    assert!(journal.inner.action_rejections().is_empty());
    assert_eq!(provider.scripts.requests().len(), 1);
}
