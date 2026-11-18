#[tokio::test]
async fn loop_50_concurrent() {
    let executions = (0..50).map(|index| async move {
        let fixture = Fixture::new();
        let response_ref = format!("response-concurrent-{index}");
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(
            assistant_events(&response_ref, "completed"),
        )]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            Arc::clone(&journal),
            Duration::from_secs(1),
        );
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        (
            fixture.run.run_id,
            exit,
            journal.snapshot(),
            provider.requests(),
        )
    });

    let results = join_all(executions).await;
    let run_ids = results
        .iter()
        .map(|(run_id, ..)| *run_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(run_ids.len(), 50, "every concurrent Run must stay isolated");
    for (run_id, exit, checkpoint, requests) in results {
        assert!(matches!(exit, AgentLoopExit::Completed { .. }));
        assert_eq!(checkpoint.run_id, run_id);
        assert_eq!(checkpoint.phase, LoopPhase::Completed);
        assert_eq!(checkpoint.iteration, 1);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].invocation.run_context.run_id, run_id);
    }
}

struct FatalFirstTool;

impl ToolExecutor for FatalFirstTool {
    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        if call.call_id.as_str() == "fatal-first" {
            Box::pin(async {
                Err(LoopPortError {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                })
            })
        } else {
            CompletingTools.execute(attempt, call)
        }
    }
}

#[tokio::test]
async fn tool_port_failure_preserves_siblings_without_fabricating_a_terminal_result() {
    let fixture = Fixture::new();
    let first = tool_call_id("fatal-first");
    let second = tool_call_id("successful-second");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(tool_batch_events(&first, &second)),
        Script::Events(assistant_events(
            "recover-after-tool-error",
            "Use the retained sibling result and continue.",
        )),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(FatalFirstTool),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(
        exit,
        AgentLoopExit::Failed {
            stage: AgentLoopFailureStage::ToolExecution,
            code: ErrorCode::InternalInvariantViolated,
            ..
        }
    ));
    assert_eq!(journal.committed_tool_calls(), vec![second]);
}

#[tokio::test]
async fn parallel_tool_batches_execute_concurrently_and_commit_in_source_order() {
    let fixture = Fixture::new();
    let first = tool_call_id("z-source-first");
    let second = tool_call_id("a-source-second");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(tool_batch_events(&first, &second)),
        Script::Events(assistant_events("response-after-parallel", "done")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(BarrierTools {
        barrier: Arc::new(Barrier::new(2)),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let runtime = loop_runtime(
        &fixture,
        provider,
        tools.clone(),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );

    let exit = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        ),
    )
    .await
    .expect("parallel tools must reach the shared barrier");

    assert!(matches!(exit, AgentLoopExit::Completed { .. }));
    assert_eq!(tools.peak.load(Ordering::SeqCst), 2);
    assert_eq!(journal.committed_tool_calls(), vec![first, second]);
}

#[tokio::test]
async fn deployment_tool_capacity_serializes_a_parallel_policy_without_losing_calls() {
    let fixture = Fixture::new();
    let first = tool_call_id("capacity-first");
    let second = tool_call_id("capacity-second");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(tool_batch_events(&first, &second)),
        Script::Events(assistant_events("capacity-finished", "done")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(BarrierTools {
        barrier: Arc::new(Barrier::new(1)),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let runtime = loop_runtime(
        &fixture,
        provider,
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    )
    .with_tool_concurrency_limit(std::num::NonZeroUsize::MIN);
    let exit = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        ),
    )
    .await
    .expect("bounded batch");
    assert!(matches!(exit, AgentLoopExit::Completed { .. }));
    assert_eq!(tools.peak.load(Ordering::SeqCst), 1);
    assert_eq!(journal.committed_tool_calls(), vec![first, second]);
}

#[tokio::test]
async fn loop_truncated_toolcall() {
    let fixture = Fixture::new();
    let first = tool_call_id("truncated-first");
    let second = tool_call_id("truncated-second");
    let mut truncated = tool_batch_events(&first, &second);
    *truncated.last_mut().expect("terminal event") = Ok(ModelStreamEvent::ResponseCompleted {
        stop_reason: StopReason::MaxOutputTokens,
    });
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(truncated),
        Script::Events(assistant_events("response-after-truncation", "recovered")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(CountingTools::default());
    let runtime = loop_runtime(
        &fixture,
        provider,
        tools.clone(),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );

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
    assert_eq!(
        tools.executions.load(Ordering::SeqCst),
        0,
        "a length-truncated batch must never reach the capability executor"
    );
    assert_eq!(
        journal.committed_tool_calls(),
        vec![first, second],
        "every truncated call must receive one durable error result in source order"
    );
    assert_eq!(journal.snapshot().iteration, 2);
}

#[tokio::test]
async fn loop_steer_and_followup() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(assistant_events("response-before-follow-up", "first")),
        Script::Events(assistant_events("response-after-follow-up", "second")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let controls = Arc::new(OneFollowUpControls::default());
    let runtime = loop_runtime_with_components(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        controls.clone(),
        Arc::new(AtomicUsize::new(0)),
    );

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
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(controls.steering.load(Ordering::SeqCst), 4);
    assert_eq!(controls.follow_up.load(Ordering::SeqCst), 4);
    assert_eq!(journal.snapshot().iteration, 2);
}

#[tokio::test]
async fn control_committed_at_terminal_boundary_prevents_run_completion() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(assistant_events("response-before-racing-control", "first")),
        Script::Events(assistant_events("response-after-racing-control", "second")),
    ]));
    let race = Arc::new(AtomicUsize::new(1));
    let journal = Arc::new(FixtureJournal::with_completion_control_race(
        fixture.checkpoint(),
        race.clone(),
    ));
    let controls = Arc::new(TerminalBoundaryRaceControls { race: race.clone() });
    let runtime = loop_runtime_with_components(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        controls,
        Arc::new(AtomicUsize::new(0)),
    );

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
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(race.load(Ordering::SeqCst), 3);
    assert_eq!(journal.snapshot().iteration, 2);
}

#[tokio::test]
async fn loop_policy_is_injected_and_composable() {
    let fixture = Fixture::new();
    let first = tool_call_id("policy-first");
    let second = tool_call_id("policy-second");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(tool_batch_events(&first, &second)),
        Script::Events(assistant_events("policy-complete", "done")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(CountingTools::default());
    let controls = Arc::new(ObservedEmptyControls::default());
    let policy_loads = Arc::new(AtomicUsize::new(0));
    let runtime = loop_runtime_with_components(
        &fixture,
        provider.clone(),
        tools.clone(),
        Arc::clone(&journal),
        controls.clone(),
        policy_loads.clone(),
    );

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
    assert_eq!(policy_loads.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(tools.executions.load(Ordering::SeqCst), 2);
    assert_eq!(controls.steering.load(Ordering::SeqCst), 3);
    assert_eq!(controls.follow_up.load(Ordering::SeqCst), 3);
}

fn loop_runtime_with_components(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    controls: Arc<dyn LoopControlQueue>,
    loads: Arc<AtomicUsize>,
) -> DurableAgentLoop {
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    execution.max_model_events_per_commit = non_zero_usize(2);
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(ObservedExecutionPolicy { execution, loads }),
        Arc::new(FixtureContext { run: fixture.run }),
        tools,
        controls,
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}

struct ObservedExecutionPolicy {
    execution: RunExecutionPolicySnapshot,
    loads: Arc<AtomicUsize>,
}

impl ExecutionPolicySource for ObservedExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let execution = PinnedRunExecutionPolicy {
            snapshot: self.execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        };
        Box::pin(async move { Ok(execution) })
    }
}

#[derive(Default)]
struct ObservedEmptyControls {
    steering: AtomicUsize,
    follow_up: AtomicUsize,
}

impl LoopControlQueue for ObservedEmptyControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        self.steering.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        self.follow_up.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(0) })
    }
}

#[derive(Default)]
struct OneFollowUpControls {
    steering: AtomicUsize,
    follow_up: AtomicUsize,
}

struct TerminalBoundaryRaceControls {
    race: Arc<AtomicUsize>,
}

impl LoopControlQueue for TerminalBoundaryRaceControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let drained = self
            .race
            .compare_exchange(2, 3, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        Box::pin(async move { Ok(u32::from(drained)) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}

impl LoopControlQueue for OneFollowUpControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let prior = self.steering.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(u32::from(prior == 0)) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        let prior = self.follow_up.fetch_add(1, Ordering::SeqCst);
        // The FollowUp arrives while the first physical invocation is in
        // flight: it is absent at the initial pre-plan drain and visible at
        // the first terminal control boundary.
        Box::pin(async move { Ok(u32::from(prior == 1)) })
    }
}
