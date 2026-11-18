use super::*;

fn empty_events(index: u32) -> Vec<Result<ModelStreamEvent, ProviderError>> {
    vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref(&format!("empty-{index}")),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 1,
            total_tokens: Some(1),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ]
}

fn persistent_loop(
    fixture: &Fixture,
    provider: Arc<ScriptedProvider>,
    journal: Arc<FixtureJournal>,
) -> DurableAgentLoop {
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}

#[tokio::test]
async fn empty_responses_release_each_attempt_preserve_usage_and_eventually_complete() {
    check_empty_recovery(StopReason::Stop).await;
}

#[tokio::test]
async fn token_limited_hidden_reasoning_recovers_without_quota_failure_or_hot_retries() {
    for stop in [StopReason::MaxOutputTokens, StopReason::TokenLimit] {
        check_empty_recovery(stop).await;
    }
}

async fn check_empty_recovery(stop: StopReason) {
    let mut fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let mut scripts = (0..24)
        .map(|index| {
            let mut events = empty_events(index);
            if stop.is_token_limited() {
                events[1] = Ok(ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 1,
                    output_tokens: 16384,
                    reasoning_tokens: 16384,
                    total_tokens: Some(16385),
                    ..ModelUsage::default()
                }));
            }
            *events.last_mut().expect("valid test fixture") =
                Ok(ModelStreamEvent::ResponseCompleted { stop_reason: stop });
            Script::Events(events)
        })
        .collect::<Vec<_>>();
    scripts.push(Script::Events(assistant_events("recovered", "Finished.")));
    let provider = Arc::new(ScriptedProvider::new(scripts));
    for index in 0..24 {
        let runtime = persistent_loop(&fixture, provider.clone(), journal.clone());
        assert_durable_failure(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopFailureStage::ModelExecution,
            ErrorCode::ModelResponseInvalid,
            true,
        );
        assert_eq!(
            provider.requests().len(),
            index + 1,
            "no hot in-Attempt retry"
        );
        assert_eq!(lock(&journal.recovery).provider_failures, index as u32 + 1);
        assert_eq!(journal.snapshot().phase, LoopPhase::ReadyForModel);
        assert!(
            lock(&journal.completion_kinds).is_empty(),
            "empty is not success"
        );
        assert_eq!(
            journal
                .batches()
                .iter()
                .flat_map(|batch| &batch.events)
                .filter(|event| matches!(event, ModelStreamEvent::Usage(_)))
                .count(),
            index + 1,
            "provider usage observations survive every retry",
        );
        fixture.attempt.attempt_id = AttemptId::generate();
        fixture.attempt.attempt_number = AttemptNumber::new(index as u32 + 2).expect("attempt");
        fixture.attempt.fencing_token = FencingToken::new(index as u64 + 2).expect("fence");
        lock(&journal.checkpoint).attempt_id = fixture.attempt.attempt_id;
    }
    let runtime = persistent_loop(&fixture, provider.clone(), journal.clone());
    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.requests().len(), 25);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
}

#[tokio::test]
async fn old_persistent_empty_count_does_not_block_dispatch_or_cancellation() {
    for cancelled in [false, true] {
        let fixture = Fixture::new();
        let mut checkpoint = fixture.checkpoint();
        checkpoint.consecutive_empty_responses = RunExecutionPolicySnapshot::production_defaults()
            .max_consecutive_empty_responses
            .get();
        let journal = Arc::new(FixtureJournal::new(checkpoint));
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(
            assistant_events("resumed", "Finished after updating Runtime."),
        )]));
        let runtime = persistent_loop(&fixture, provider.clone(), journal.clone());
        let supervisor = InvocationSupervisor::new();
        if cancelled {
            supervisor.cancel();
        }
        let exit = runtime
            .execute(fixture.attempt.clone(), fixture.telemetry(), supervisor)
            .await;
        if cancelled {
            assert!(matches!(exit, AgentLoopExit::Cancelled { .. }));
            assert!(provider.requests().is_empty());
        } else {
            assert!(matches!(exit, AgentLoopExit::Completed { .. }));
            assert_eq!(provider.requests().len(), 1);
            assert_eq!(journal.snapshot().consecutive_empty_responses, 0);
        }
    }
}
