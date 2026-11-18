use super::*;
use musteragent_loop::CONTEXT_COMPACTION_POLICY_V7;

/// Admission-sealed compaction policy identity for these tests. Everything
/// else delegates to the shared fixture snapshot so the only behavioral
/// difference under test is the sealed compaction policy ref.
struct CompactionGatePolicy {
    execution: RunExecutionPolicySnapshot,
    admitted_at: UtcTimestamp,
    compaction_policy_ref: Option<VersionedKey>,
}

impl ExecutionPolicySource for CompactionGatePolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let policy = PinnedRunExecutionPolicy {
            snapshot: activity_fixture::snapshot(self.execution),
            admitted_at: self.admitted_at,
            model_route_count: NonZeroU32::MIN,
        };
        Box::pin(async move { Ok(policy) })
    }

    fn context_compaction_policy_ref(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<Option<VersionedKey>, LoopPortError>> {
        let policy_ref = self.compaction_policy_ref.clone();
        Box::pin(async move { Ok(policy_ref) })
    }
}

fn overflow_loop_runtime(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    compaction_policy_ref: Option<VersionedKey>,
) -> DurableAgentLoop {
    overflow_loop_runtime_with_recovery(
        fixture,
        provider,
        tools,
        journal,
        compaction_policy_ref,
        false,
    )
}

fn overflow_loop_runtime_with_recovery(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
    compaction_policy_ref: Option<VersionedKey>,
    persistent: bool,
) -> DurableAgentLoop {
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    if !persistent {
        execution.recovery_policy = non_zero_u32(8).into();
    }
    execution.max_consecutive_empty_responses = non_zero_u32(3);
    execution.max_model_events_per_commit = non_zero_usize(2);
    execution.output_frame_flush_interval = Duration::from_millis(25);
    execution.max_output_frame_bytes = non_zero_usize(4 * 1_024);
    execution.model_inactivity_timeout = Duration::from_secs(1);
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(CompactionGatePolicy {
            execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            compaction_policy_ref,
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        tools,
        Arc::new(EmptyControls),
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}

/// The scheduler contract after a retryable durable failure: a replacement
/// Attempt with a fresh fence resumes the same Run journal.
fn replace_attempt(fixture: &mut Fixture, journal: &FixtureJournal, attempt_number: u32) {
    fixture.attempt.attempt_id = AttemptId::generate();
    fixture.attempt.attempt_number = result_or_panic(AttemptNumber::new(attempt_number));
    fixture.attempt.fencing_token = result_or_panic(FencingToken::new(u64::from(attempt_number)));
    lock(&journal.checkpoint).attempt_id = fixture.attempt.attempt_id;
}

fn overflow_stop_events(response_ref: &str) -> Vec<Result<ModelStreamEvent, ProviderError>> {
    vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref(response_ref),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 1,
            total_tokens: Some(1),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ContextWindowExceeded,
        }),
    ]
}

fn overflow_stream_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::InvalidRequest,
        safe_code: ProviderErrorCode::ContextWindowExceeded,
        retryable: false,
        retry_after_ms: None,
        diagnostic_ref: None,
    }
}

#[tokio::test]
async fn explicit_overflow_stop_recovers_through_same_route_backoff_under_v7() {
    let mut fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(overflow_stop_events("overflow-stop")),
        Script::Events(assistant_events(
            "recovered-after-overflow",
            "Recovered after compaction.",
        )),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        true,
    );
    let checkpoint = journal.snapshot();
    assert_eq!(checkpoint.phase, LoopPhase::ReadyForModel);
    assert_eq!(
        checkpoint.active_model_route_ordinal, 0,
        "overflow recovery must keep the frozen route"
    );
    assert_eq!(provider.requests().len(), 1, "no hot in-Attempt retry");
    assert_eq!(
        journal.model_retry_after_hints().len(),
        1,
        "the physical failure stays a durable audit fact"
    );
    assert!(
        journal.failures().is_empty(),
        "no terminal Run failure may be recorded"
    );
    assert_eq!(lock(&journal.recovery).provider_failures, 1);

    replace_attempt(&mut fixture, &journal, 2);
    let resumed = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );
    assert!(matches!(
        resumed
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].route.route_ref, requests[1].route.route_ref,
        "the replacement Attempt resumes the same frozen route"
    );
    assert_ne!(
        requests[0].invocation.invocation_id, requests[1].invocation.invocation_id,
        "the replacement Attempt is a fresh physical invocation"
    );
    let checkpoint = journal.snapshot();
    assert_eq!(checkpoint.phase, LoopPhase::Completed);
    assert_eq!(checkpoint.active_model_route_ordinal, 0);
}

#[tokio::test]
async fn explicit_overflow_stream_error_recovers_under_v7() {
    let mut fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![Err(overflow_stream_error())]),
        Script::Events(assistant_events(
            "recovered-after-stream-overflow",
            "Recovered after compaction.",
        )),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        true,
    );
    assert_eq!(journal.snapshot().phase, LoopPhase::ReadyForModel);
    assert_eq!(journal.snapshot().active_model_route_ordinal, 0);
    assert_eq!(provider.requests().len(), 1);

    replace_attempt(&mut fixture, &journal, 2);
    let resumed = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );
    assert!(matches!(
        resumed
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].route.route_ref, requests[1].route.route_ref);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
}

#[tokio::test]
async fn legacy_policies_keep_explicit_overflow_terminal() {
    for policy_ref in [None, Some(versioned("musteragent.context-compaction@6"))] {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(
            overflow_stop_events("legacy-overflow-stop"),
        )]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = overflow_loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            journal.clone(),
            policy_ref.clone(),
        );

        assert_durable_failure(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopFailureStage::ModelExecution,
            ErrorCode::ModelContextExceeded,
            false,
        );
        assert_eq!(provider.requests().len(), 1, "{policy_ref:?}");
        assert_eq!(
            journal.snapshot().phase,
            LoopPhase::Failed,
            "{policy_ref:?}"
        );
        assert_eq!(
            journal.failures(),
            vec![(ErrorCode::ModelContextExceeded, false)],
            "{policy_ref:?}"
        );
    }
}

#[tokio::test]
async fn public_output_before_overflow_keeps_terminal_failure_under_v7() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("partial-before-overflow"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("partial answer before the overflow"),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 3,
            output_tokens: 1,
            total_tokens: Some(4),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ContextWindowExceeded,
        }),
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );

    // A sealed continuation over already delivered public output is a separate
    // contract (proposal C09); this slice must keep the terminal stop.
    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        false,
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
    assert_eq!(
        journal.failures(),
        vec![(ErrorCode::ModelContextExceeded, false)]
    );
}

#[tokio::test]
async fn unclassified_failures_stay_terminal_under_v7() {
    for (safe_code, expected) in [
        (
            ProviderErrorCode::InvalidRequest,
            ErrorCode::ModelRequestRejected,
        ),
        (
            ProviderErrorCode::AuthenticationFailed,
            ErrorCode::Unauthenticated,
        ),
    ] {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
            ProviderError {
                kind: ProviderErrorKind::InvalidRequest,
                safe_code,
                retryable: false,
                retry_after_ms: None,
                diagnostic_ref: None,
            },
        )])]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = overflow_loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            journal.clone(),
            Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
        );

        assert_durable_failure(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await,
            AgentLoopFailureStage::ModelExecution,
            expected,
            false,
        );
        assert_eq!(provider.requests().len(), 1, "{safe_code:?}");
        assert_eq!(journal.snapshot().phase, LoopPhase::Failed, "{safe_code:?}");
    }
}

#[tokio::test]
async fn overflow_recovery_never_replays_committed_tools() {
    let mut fixture = Fixture::new();
    let call_id = tool_call_id("committed-before-overflow");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("tool-turn"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("current_time"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id: call_id.clone(),
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 4,
                output_tokens: 1,
                total_tokens: Some(5),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(overflow_stop_events("overflow-after-tool")),
        Script::Events(assistant_events("final-after-overflow", "Done.")),
    ]));
    let tools = Arc::new(CountingTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        true,
    );
    assert_eq!(tools.executions.load(Ordering::SeqCst), 1);
    assert_eq!(journal.committed_tool_calls(), vec![call_id.clone()]);
    assert_eq!(journal.snapshot().completed_tool_calls, 1);

    replace_attempt(&mut fixture, &journal, 2);
    let resumed = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );
    assert!(matches!(
        resumed
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(
        tools.executions.load(Ordering::SeqCst),
        1,
        "the committed Tool result must never be replayed"
    );
    assert_eq!(journal.committed_tool_calls(), vec![call_id]);
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
}

#[tokio::test]
async fn crash_before_overflow_failure_commit_rewinds_and_recovers() {
    let mut fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(overflow_stop_events("overflow-before-crash")),
        Script::Events(assistant_events(
            "after-crash",
            "Recovered after the worker crash.",
        )),
    ]));
    // The Journal fails the first failure-commit, simulating a worker crash
    // between observing the overflow and sealing the durable failure fact.
    let journal = Arc::new(FixtureJournal::with_failure_error(
        fixture.checkpoint(),
        port_error(ErrorCode::DependencyUnavailable, true),
    ));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    let AgentLoopExit::Failed {
        failure_commit_error: Some(_),
        retryable: false,
        ..
    } = exit
    else {
        panic!("an uncommitted failure commit must surface the commit error: {exit:?}")
    };
    assert!(
        matches!(journal.snapshot().phase, LoopPhase::ModelInFlight { .. }),
        "the crashed Attempt leaves its physical invocation in flight"
    );
    assert_eq!(provider.requests().len(), 1);
    assert!(journal.failures().is_empty());

    replace_attempt(&mut fixture, &journal, 2);
    let resumed = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );
    assert!(matches!(
        resumed
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(
        provider.requests().len(),
        2,
        "the replacement Attempt re-dispatches the sealed invocation"
    );
    assert!(journal.failures().is_empty());
    assert_eq!(journal.snapshot().phase, LoopPhase::Completed);
}

#[tokio::test]
async fn cancellation_preempts_overflow_recovery() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::DelayedEvents(vec![(
        Duration::from_millis(200),
        Err(overflow_stream_error()),
    )])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
    );
    let supervisor = InvocationSupervisor::new();
    let cancellation = supervisor.clone();
    let cancel_in_flight = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
    };
    let (exit, ()) = tokio::join!(
        runtime.execute(fixture.attempt.clone(), fixture.telemetry(), supervisor,),
        cancel_in_flight
    );

    assert!(matches!(exit, AgentLoopExit::Cancelled { .. }));
    assert_eq!(journal.snapshot().phase, LoopPhase::Cancelled);
    assert!(
        journal.model_retry_after_hints().is_empty(),
        "cancellation must win before any overflow failure is committed"
    );
    assert!(journal.failures().is_empty());
}

#[tokio::test]
async fn persistent_overflow_seals_partial_output_and_resumes_without_executing_incomplete_tool() {
    let mut fixture = Fixture::new();
    let mut events = vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("partial-overflow"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("already delivered"),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 1,
            call_id: tool_call_id("unfinished"),
            name: wire_name("current_time"),
            provider_item_ref: None,
            provider_replay: None,
        }),
    ];
    events.push(Err(overflow_stream_error()));
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(events),
        Script::Events(assistant_events("continued", "continued after compaction")),
    ]));
    let tools = Arc::new(CountingTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime_with_recovery(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
        true,
    );
    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        true,
    );
    assert_eq!(journal.snapshot().phase, LoopPhase::ReadyForModel);
    assert!(journal.failures().is_empty());
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert_eq!(journal.batches().iter().flat_map(|batch| &batch.events).filter(|event|
        matches!(event, ModelStreamEvent::TextDelta { delta, .. } if delta.as_str() == "already delivered")
    ).count(), 1);
    replace_attempt(&mut fixture, &journal, 2);
    let resumed = overflow_loop_runtime_with_recovery(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Some(versioned(CONTEXT_COMPACTION_POLICY_V7)),
        true,
    );
    assert!(matches!(
        resumed
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 2);
}

#[tokio::test]
async fn explicit_context_limit_after_tool_arguments_recovers_without_dispatch() {
    let fixture = Fixture::new();
    let mut events =
        tool_batch_events(&tool_call_id("unexecuted-a"), &tool_call_id("unexecuted-b"));
    *events.last_mut().expect("valid test fixture") = Ok(ModelStreamEvent::ResponseCompleted {
        stop_reason: StopReason::ContextWindowExceeded,
    });
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(events)]));
    let tools = Arc::new(RejectingArgumentsTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = overflow_loop_runtime_with_recovery(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Some(VersionedKey::new(CONTEXT_COMPACTION_POLICY_V7).expect("valid test fixture")),
        true,
    );
    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelContextExceeded,
        true,
    );
    assert_eq!(tools.validations.load(Ordering::SeqCst), 0);
    assert!(lock(&journal.tool_results).is_empty());
    assert_eq!(provider.requests().len(), 1);
}
