use super::*;

#[tokio::test]
async fn consecutive_empty_responses_exhaust_a_bounded_dependency_budget() {
    let fixture = Fixture::new();
    let empty = || {
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-empty"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 1,
                output_tokens: 0,
                total_tokens: Some(1),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ])
    };
    let provider = Arc::new(ScriptedProvider::new(vec![empty(), empty(), empty()]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
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
        ErrorCode::DependencyUnavailable,
        false,
    );
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(journal.snapshot().consecutive_empty_responses, 3);
}

#[tokio::test]
async fn low_rate_output_is_committed_by_the_frame_deadline() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::EventsThenPending(
        vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("response-low-rate"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("first durable bytes"),
            }),
        ],
    )]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(5),
    );
    let attempt = fixture.attempt.clone();
    let telemetry = fixture.telemetry();
    let execution = tokio::spawn(async move {
        runtime
            .execute(attempt, telemetry, InvocationSupervisor::new())
            .await
    });

    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if journal.batches().iter().any(|batch| {
                batch
                    .events
                    .iter()
                    .any(|event| matches!(event, ModelStreamEvent::TextDelta { .. }))
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the first public bytes must be committed by the 25ms frame deadline");

    execution.abort();
    let _ = execution.await;
}

#[tokio::test]
async fn waiting_tool_suspends_without_a_permanent_run_task() {
    let fixture = Fixture::new();
    let interaction_id = InteractionId::generate();
    let call_id = tool_call_id("call-wait");
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("response-wait"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: call_id.clone(),
            name: wire_name("request_approval"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id,
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
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(WaitingTools {
            interaction_id,
            journal: Arc::clone(&journal),
        }),
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
        AgentLoopExit::Waiting {
            interaction_id: observed,
            receipt: Some(_),
        } if observed == interaction_id
    ));
    assert_eq!(
        journal.snapshot().phase,
        LoopPhase::Waiting { interaction_id }
    );
}

#[tokio::test]
async fn parallel_reconciliation_wait_is_deferred_until_sibling_results_commit() {
    let fixture = Fixture::new();
    let media_call = tool_call_id("call-media");
    let read_call = tool_call_id("call-read");
    let wait_invocation_id = InvocationId::generate();
    let observed_at = UtcTimestamp::from_unix_millis(2_000);
    let resume_not_before = UtcTimestamp::from_unix_millis(7_000);
    // The asynchronous Tool is planned first: without deferral its suspension
    // would strand the sibling result commit behind the Attempt fence.
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("response-mixed-reconciliation"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: media_call.clone(),
            name: wire_name("external_write"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id: media_call.clone(),
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 1,
            call_id: read_call.clone(),
            name: wire_name("web_search"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 1,
            call_id: read_call.clone(),
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
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(MixedReconciliationTools {
            wait_call: media_call,
            wait_invocation_id,
            observed_at,
            resume_not_before,
        }),
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
        AgentLoopExit::ReconciliationWaiting {
            invocation_id,
            ..
        } if invocation_id == wait_invocation_id
    ));
    assert_eq!(
        journal.transitions(),
        vec![
            FixtureTransition::ToolCommitted(read_call),
            FixtureTransition::ReconciliationWait {
                invocation_id: wait_invocation_id,
                observed_at,
                resume_not_before,
            },
        ],
        "every sibling Tool result must commit before the reconciliation suspension"
    );
}

#[tokio::test]
async fn cancellation_and_deadline_produce_durable_terminal_outcomes() {
    let cancelled_fixture = Fixture::new();
    let cancelled_journal = Arc::new(FixtureJournal::new(cancelled_fixture.checkpoint()));
    let cancelled_runtime = loop_runtime(
        &cancelled_fixture,
        Arc::new(ScriptedProvider::new(vec![Script::Pending])),
        Arc::new(CompletingTools),
        Arc::clone(&cancelled_journal),
        Duration::from_secs(1),
    );
    let supervisor = InvocationSupervisor::new();
    supervisor.cancel();
    let cancelled = cancelled_runtime
        .execute(
            cancelled_fixture.attempt.clone(),
            cancelled_fixture.telemetry(),
            supervisor,
        )
        .await;
    let AgentLoopExit::Cancelled { receipt } = cancelled else {
        panic!("cancellation must return its durable receipt")
    };
    assert_eq!(receipt.run_id(), cancelled_fixture.attempt.run_id);
    assert_eq!(receipt.attempt_id(), cancelled_fixture.attempt.attempt_id);
    assert_eq!(
        receipt.fencing_token(),
        cancelled_fixture.attempt.fencing_token
    );
    assert_eq!(receipt.attempt_outcome(), DurableAttemptOutcome::Cancelled);
    assert_eq!(receipt.run_outcome(), DurableRunOutcome::Cancelled);
    assert_eq!(cancelled_journal.snapshot().phase, LoopPhase::Cancelled);

    let deadline_fixture = Fixture::new();
    let deadline_journal = Arc::new(FixtureJournal::new(deadline_fixture.checkpoint()));
    let deadline_runtime = loop_runtime(
        &deadline_fixture,
        Arc::new(ScriptedProvider::new(vec![Script::Pending])),
        Arc::new(CompletingTools),
        Arc::clone(&deadline_journal),
        Duration::from_millis(5),
    );
    assert_durable_failure(
        deadline_runtime
            .execute(
                deadline_fixture.attempt.clone(),
                deadline_fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::DependencyTimeout,
        true,
    );
    assert_eq!(deadline_journal.snapshot().phase, LoopPhase::ReadyForModel);
}

#[tokio::test]
async fn canonical_progress_refreshes_model_inactivity_timeout() {
    let fixture = Fixture::new();
    let per_event_delay = Duration::from_millis(20);
    let provider = Arc::new(ScriptedProvider::new(vec![Script::DelayedEvents(
        assistant_events("response-active-stream", "still working")
            .into_iter()
            .enumerate()
            // Accepting the response header is not semantic progress. Deliver
            // it immediately so each subsequent progress gap is 20ms, below
            // the 30ms idle window being tested.
            .map(|(index, event)| {
                (
                    if index == 0 {
                        Duration::ZERO
                    } else {
                        per_event_delay
                    },
                    event,
                )
            })
            .collect(),
    )]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        journal,
        Duration::from_millis(30),
    );

    let started = Instant::now();
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
    assert!(
        started.elapsed() > Duration::from_millis(60),
        "the invocation must outlive one idle window while making progress"
    );
}

#[tokio::test]
async fn coordination_interrupt_produces_a_distinct_durable_terminal_outcome() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        Arc::new(ScriptedProvider::new(vec![Script::Pending])),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );
    let supervisor = InvocationSupervisor::new();
    supervisor.cancel_with(musteragent_loop::CancellationCause::CoordinationRequested);

    let exit = runtime
        .execute(fixture.attempt.clone(), fixture.telemetry(), supervisor)
        .await;
    let AgentLoopExit::CoordinationInterrupted { receipt } = exit else {
        panic!("coordination interrupt must return its distinct durable receipt")
    };
    assert_eq!(receipt.run_id(), fixture.attempt.run_id);
    assert_eq!(receipt.attempt_id(), fixture.attempt.attempt_id);
    assert_eq!(
        receipt.attempt_outcome(),
        DurableAttemptOutcome::Interrupted
    );
    assert_eq!(receipt.run_outcome(), DurableRunOutcome::Interrupted);
    assert_eq!(journal.snapshot().phase, LoopPhase::Interrupted);
    assert_eq!(journal.cancellation_observations(), vec![None]);
}

#[tokio::test]
async fn cancellation_interrupts_context_planning_and_durably_stops_the_run() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(PendingContext {
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
        }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        Arc::new(ScriptedProvider::new(vec![Script::Pending])),
        journal.clone(),
    );
    let supervisor = InvocationSupervisor::new();
    let cancellation = supervisor.clone();
    let telemetry = fixture.telemetry();
    let attempt = fixture.attempt;
    let executing =
        tokio::spawn(async move { runtime.execute(attempt, telemetry, supervisor).await });
    entered.notified().await;
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), executing)
            .await
            .expect("context planning cancellation must not wait for invocation timeout")
            .expect("join Run"),
        AgentLoopExit::Cancelled { .. }
    ));
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(journal.snapshot().phase, LoopPhase::Cancelled);
}

#[tokio::test]
async fn lease_loss_interrupts_tool_policy_before_physical_execution() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("call-policy-stall");
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("response-policy-stall"),
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
            call_id,
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 4,
            output_tokens: 2,
            total_tokens: Some(6),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        }),
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let tools = Arc::new(CountingTools::default());
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(PendingToolExecutionPolicy {
            execution: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            entered: Arc::clone(&entered),
            dropped: Arc::clone(&dropped),
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        tools.clone(),
        Arc::new(EmptyControls),
    ));
    let runtime =
        activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal.clone());
    let supervisor = InvocationSupervisor::new();
    let cancellation = supervisor.clone();
    let telemetry = fixture.telemetry();
    let attempt = fixture.attempt;
    let executing =
        tokio::spawn(async move { runtime.execute(attempt, telemetry, supervisor).await });

    entered.notified().await;
    cancellation.cancel_with(musteragent_loop::CancellationCause::LeaseLost);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), executing)
            .await
            .expect("Tool policy cancellation must release the Attempt")
            .expect("join Run"),
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::LeaseLost,
        }
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert!(matches!(
        journal.snapshot().phase,
        LoopPhase::ToolsPending { .. }
    ));
}

#[tokio::test]
async fn recovery_replays_durable_pending_tools_before_the_next_model() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("call-recovered");
    let invocation_id = InvocationId::generate();
    let mut checkpoint = fixture.checkpoint();
    checkpoint.iteration = 1;
    checkpoint.phase = LoopPhase::ToolsPending {
        invocation_id,
        remaining_tool_call_ids: vec![call_id.clone()],
    };
    checkpoint.continuation_ref = Some(musteragent_runtime_model::ContinuationRef::from_opaque(
        result_or_panic(OpaqueExternalRef::new("response-before-crash")),
    ));
    checkpoint.continuation_source_invocation_id = Some(invocation_id);
    let batch = ModelEventBatch {
        invocation_id,
        iteration: 1,
        batch_index: 0,
        events: vec![
            ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("current_time"),
                provider_item_ref: None,
                provider_replay: None,
            },
            ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            },
        ],
    };
    let journal = Arc::new(FixtureJournal::with_batches(checkpoint, vec![batch]));
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("response-after-recovery"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("Recovered."),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 4,
            output_tokens: 1,
            total_tokens: Some(5),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ])]));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
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
    let state = journal.snapshot();
    assert_eq!(state.completed_tool_calls, 1);
    assert_eq!(state.iteration, 2);
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn provider_retry_after_is_preserved_in_the_durable_failure_commit() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: Some(45_000),
            diagnostic_ref: None,
        },
    )])]));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
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
        ErrorCode::DependencyUnavailable,
        true,
    );
    assert_eq!(journal.model_retry_after_hints(), vec![Some(45_000)]);
}

#[tokio::test]
async fn failure_commit_errors_are_explicit_and_stale_leases_interrupt_the_attempt() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        },
    )])]));
    let unavailable_journal = Arc::new(FixtureJournal::with_failure_error(
        fixture.checkpoint(),
        port_error(ErrorCode::DependencyUnavailable, true),
    ));
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(CompletingTools),
        Arc::clone(&unavailable_journal),
        Duration::from_secs(1),
    );

    assert_eq!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Failed {
            stage: AgentLoopFailureStage::FallbackCommit,
            code: ErrorCode::DependencyUnavailable,
            retryable: false,
            failure_commit_error: Some(ErrorCode::DependencyUnavailable),
            receipt: None,
        }
    );
    assert!(unavailable_journal.failures().is_empty());

    let stale_fixture = Fixture::new();
    let stale_provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        },
    )])]));
    let stale_journal = Arc::new(FixtureJournal::with_failure_error(
        stale_fixture.checkpoint(),
        port_error(ErrorCode::AttemptLeaseStale, false),
    ));
    let stale_runtime = loop_runtime(
        &stale_fixture,
        stale_provider,
        Arc::new(CompletingTools),
        stale_journal,
        Duration::from_secs(1),
    );

    let stale_telemetry = stale_fixture.telemetry();
    assert_eq!(
        stale_runtime
            .execute(
                stale_fixture.attempt,
                stale_telemetry,
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopExit::Interrupted {
            cause: musteragent_loop::CancellationCause::LeaseLost,
        }
    );
}

#[tokio::test]
async fn follow_up_is_consumed_before_the_next_model_turn_without_a_redundant_request() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("follow-up-turn-boundary-call");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("follow-up-tool-response"),
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
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            }),
        ]),
        Script::Events(assistant_events(
            "follow-up-final-response",
            "Answered the follow-up in the next logical turn.",
        )),
        Script::Events(assistant_events(
            "redundant-follow-up-response",
            "This physical request must never be dispatched.",
        )),
    ]));
    let controls = Arc::new(TurnBoundaryFollowUpControls::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_controls(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        journal,
        Duration::from_secs(1),
        controls.clone(),
        NonZeroU32::MIN,
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
    assert_eq!(
        provider.requests().len(),
        2,
        "the FollowUp must be fenced before the second Context plan, not rediscovered at completion",
    );
    assert!(!controls.pending.load(Ordering::SeqCst));
}
