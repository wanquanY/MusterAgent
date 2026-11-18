use super::*;

#[tokio::test]
async fn max_output_segment_continues_before_product_stop_policy() {
    for stop_reason in [StopReason::MaxOutputTokens, StopReason::TokenLimit] {
        let fixture = Fixture::new();
        let mut first = assistant_events("response-max-output", "first segment ");
        *first.last_mut().expect("terminal event") =
            Ok(ModelStreamEvent::ResponseCompleted { stop_reason });
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(first),
            Script::Events(assistant_events("response-final", "final segment")),
        ]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            journal.clone(),
            Duration::from_secs(1),
        );

        assert!(matches!(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopExit::Completed { .. }
        ));
        assert_eq!(journal.snapshot().iteration, 2);
        assert_eq!(journal.snapshot().total_tokens, 4);
        let requests = provider.requests();
        assert_eq!(requests.len(), 2, "max output must continue automatically");
        assert_eq!(
            requests[1]
                .continuation_ref
                .as_ref()
                .map(musteragent_runtime_model::ContinuationRef::as_str),
            Some("response-max-output"),
            "the durable Provider continuation anchor must feed the next segment",
        );
    }
}

#[tokio::test]
async fn reasoning_only_max_output_segment_continues_instead_of_becoming_quota() {
    for stop_reason in [StopReason::MaxOutputTokens, StopReason::TokenLimit] {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(vec![
                Ok(ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("response-reasoning-segment"),
                    response_model_key: model_key(),
                }),
                Ok(ModelStreamEvent::ReasoningDelta {
                    output_index: 0,
                    delta: model_text("unfinished reasoning"),
                }),
                Ok(ModelStreamEvent::ResponseCompleted { stop_reason }),
            ]),
            Script::Events(assistant_events("response-final", "final segment")),
        ]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            journal.clone(),
            Duration::from_secs(1),
        );

        assert!(matches!(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopExit::Completed { .. }
        ));
        assert_eq!(provider.requests().len(), 2);
        assert_eq!(
            journal.completion_kinds(),
            vec![
                ModelCompletionKind::ReasoningContinuation,
                ModelCompletionKind::AssistantResponse,
            ],
            "private reasoning must never be reclassified as public Assistant text",
        );
    }
}

pub(super) async fn run_loop_progress_beyond_legacy_counters() {
    let fixture = Fixture::new();
    let max_output_segment = |response_ref: &'static str, text: &'static str| {
        let mut events = assistant_events(response_ref, text);
        *events.last_mut().expect("terminal event") = Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::MaxOutputTokens,
        });
        events
    };
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(max_output_segment("response-segment-1", "segment one")),
        Script::Events(max_output_segment("response-segment-2", "segment two")),
        Script::Events(max_output_segment("response-segment-3", "segment three")),
        Script::Events(assistant_events("response-final", "complete")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    execution.max_model_events_per_commit = non_zero_usize(2);
    execution.output_frame_flush_interval = Duration::from_millis(25);
    execution.max_output_frame_bytes = non_zero_usize(4 * 1_024);
    execution.model_inactivity_timeout = Duration::from_secs(1);
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        journal.clone(),
    );

    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(journal.snapshot().iteration, 4);
    assert_eq!(
        provider.requests().len(),
        4,
        "three consecutive output ceilings must continue invisibly before the final response",
    );
    assert_eq!(
        provider.requests()[3]
            .continuation_ref
            .as_ref()
            .map(musteragent_runtime_model::ContinuationRef::as_str),
        Some("response-segment-3"),
        "the final continuation must use the latest durable provider anchor",
    );
}
