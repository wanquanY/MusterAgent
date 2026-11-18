use super::*;
use musteragent_loop::{
    FallbackDecision, GateVerdict, LoopPolicy, NextTurnDecision, StepContext, ToolGate, TurnSummary,
};
use musteragent_runtime_snapshot::ToolExecutionMode;

#[tokio::test]
async fn provider_retryability_is_preserved_at_the_attempt_boundary() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Quota,
            safe_code: ProviderErrorCode::QuotaExceeded,
            retryable: false,
            retry_after_ms: None,
            diagnostic_ref: None,
        },
    )])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
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
        ErrorCode::QuotaExceeded,
        false,
    );
    assert_eq!(journal.failures(), vec![(ErrorCode::QuotaExceeded, false)]);
}

pub(super) async fn loop_model_route_immutable() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![Err(ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        })]),
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("fallback-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("Recovered through the sealed fallback."),
            }),
            Ok(ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 4,
                output_tokens: 2,
                total_tokens: Some(6),
                ..ModelUsage::default()
            })),
            Ok(ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::Stop,
            }),
        ]),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime_with_route_count(
        &fixture,
        provider.clone(),
        Arc::new(CompletingTools),
        Arc::clone(&journal),
        Duration::from_secs(1),
        non_zero_u32(2),
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
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].route.route_ref.as_str(),
        "model.route.fixture@1"
    );
    assert_eq!(
        requests[1].route.route_ref.as_str(),
        "model.route.fallback@1"
    );
    assert!(requests[1].continuation_ref.is_none());
    assert_eq!(journal.snapshot().active_model_route_ordinal, 1);
    assert!(journal.failures().is_empty());
}

#[tokio::test]
async fn eighth_failed_provider_start_is_terminal_despite_arbitrary_ownership_number() {
    let mut fixture = Fixture::new();
    fixture.attempt.attempt_number = result_or_panic(AttemptNumber::new(50));
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        },
    )])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    lock(&journal.recovery).provider_failures = 7;
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
        false,
    );
    assert_eq!(
        journal.failures(),
        vec![(ErrorCode::DependencyUnavailable, false)]
    );
}

#[tokio::test]
async fn normal_resumption_beyond_eight_ownership_attempts_can_complete() {
    let mut fixture = Fixture::new();
    fixture.attempt.attempt_number = result_or_panic(AttemptNumber::new(9));
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(
        assistant_events("resumed", "Completed after child report."),
    )]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
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
    assert_eq!(provider.requests().len(), 1);
    assert!(journal.failures().is_empty());
}

#[tokio::test]
async fn malicious_policy_cannot_fallback_for_a_disallowed_failure_code() {
    let fixture = Fixture::new();
    let (events, response_ref, usage) = provider_evidence_then_error(ProviderError {
        kind: ProviderErrorKind::Quota,
        safe_code: ProviderErrorCode::QuotaExceeded,
        retryable: true,
        retry_after_ms: None,
        diagnostic_ref: None,
    });
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(events)]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let policy = Arc::new(AdversarialFallbackPolicy::new(
        &fixture,
        AdversarialFallbackBehavior::UseRoute(1),
    ));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        Arc::clone(&journal),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::FallbackPolicy,
        ErrorCode::InternalInvariantViolated,
        false,
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(journal.snapshot().active_model_route_ordinal, 0);
    assert_provider_evidence_sealed(&journal, &response_ref, usage);
}

#[tokio::test]
async fn invalid_fallback_ordinal_still_seals_provider_evidence_before_terminal() {
    assert_adversarial_terminal_after_provider_evidence(
        AdversarialFallbackBehavior::UseRoute(2),
        ErrorCode::InternalInvariantViolated,
    )
    .await;
}

#[tokio::test]
async fn fallback_policy_error_still_seals_provider_evidence_before_terminal() {
    assert_adversarial_terminal_after_provider_evidence(
        AdversarialFallbackBehavior::Error,
        ErrorCode::PolicyDenied,
    )
    .await;
}

async fn assert_adversarial_terminal_after_provider_evidence(
    behavior: AdversarialFallbackBehavior,
    expected_code: ErrorCode,
) {
    let fixture = Fixture::new();
    let (events, response_ref, usage) = provider_evidence_then_error(ProviderError {
        kind: ProviderErrorKind::Transport,
        safe_code: ProviderErrorCode::TransportFailed,
        retryable: true,
        retry_after_ms: None,
        diagnostic_ref: None,
    });
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(events)]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let policy = Arc::new(AdversarialFallbackPolicy::new(&fixture, behavior));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        Arc::clone(&journal),
    );

    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::FallbackPolicy,
        expected_code,
        false,
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
    assert_provider_evidence_sealed(&journal, &response_ref, usage);
}

fn provider_evidence_then_error(
    error: ProviderError,
) -> (
    Vec<Result<ModelStreamEvent, ProviderError>>,
    ProviderRequestRef,
    ModelUsage,
) {
    let response_ref = provider_ref("provider-evidence-before-policy-boundary");
    let usage = ModelUsage {
        input_tokens_uncached: 11,
        output_tokens: 3,
        total_tokens: Some(14),
        ..ModelUsage::default()
    };
    (
        vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: response_ref.clone(),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::Usage(usage)),
            Err(error),
        ],
        response_ref,
        usage,
    )
}

fn assert_provider_evidence_sealed(
    journal: &FixtureJournal,
    expected_response_ref: &ProviderRequestRef,
    expected_usage: ModelUsage,
) {
    let events = journal
        .batches()
        .into_iter()
        .flat_map(|batch| batch.events)
        .collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::ResponseStarted { response_ref, response_model_key }
            if response_ref == expected_response_ref && response_model_key == &model_key()
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ModelStreamEvent::Usage(usage) if usage == &expected_usage
    )));
}

#[derive(Debug, Copy, Clone)]
enum AdversarialFallbackBehavior {
    UseRoute(u32),
    Error,
}

struct AdversarialFallbackPolicy {
    run: RunContext,
    execution: PinnedRunExecutionPolicy,
    behavior: AdversarialFallbackBehavior,
}

impl AdversarialFallbackPolicy {
    fn new(fixture: &Fixture, behavior: AdversarialFallbackBehavior) -> Self {
        let mut snapshot = RunExecutionPolicySnapshot::production_defaults();
        // This adversarial fixture verifies the original, admission-frozen fallback contract.
        snapshot.provider_failover_v1 = false;
        snapshot.recovery_policy = non_zero_u32(8).into();
        Self {
            run: fixture.run,
            execution: PinnedRunExecutionPolicy {
                snapshot,
                admitted_at: UtcTimestamp::from_unix_millis(1_000),
                model_route_count: non_zero_u32(2),
            },
            behavior,
        }
    }
}

impl LoopPolicy for AdversarialFallbackPolicy {
    fn load_execution_policy(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let execution = self.execution;
        Box::pin(async move { Ok(execution) })
    }

    fn gate(
        &self,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>> {
        Box::pin(async { Ok(GateVerdict::Allow) })
    }

    fn transform_context(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        let request = model_request(
            self.run,
            input.attempt,
            input.next_iteration,
            input.active_model_route_ordinal,
            input.continuation_ref,
            input.consecutive_empty_responses,
        );
        Box::pin(async move { Ok(request) })
    }

    fn before_tool(
        &self,
        _call: &PlannedToolCall,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>> {
        Box::pin(async {
            Ok(ToolGate::Allow {
                execution_mode: ToolExecutionMode::Sequential,
            })
        })
    }

    fn execute_tool(
        &self,
        _attempt: &AttemptContext,
        _telemetry: &AttemptTelemetryContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async { Err(port_error(ErrorCode::InternalInvariantViolated, false)) })
    }

    fn after_tool(
        &self,
        _call: &PlannedToolCall,
        outcome: ToolExecutionOutcome,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move { Ok(outcome) })
    }

    fn should_stop(
        &self,
        _turn: TurnSummary,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>> {
        Box::pin(async { Ok(true) })
    }

    fn prepare_next_turn(
        &self,
        _turn: TurnSummary,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<NextTurnDecision, LoopPortError>> {
        Box::pin(async { Ok(NextTurnDecision::Stop) })
    }

    fn fallback_after_model_failure(
        &self,
        _failure: musteragent_loop::ModelFailure,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        let behavior = self.behavior;
        Box::pin(async move {
            match behavior {
                AdversarialFallbackBehavior::UseRoute(next_route_ordinal) => {
                    Ok(FallbackDecision::UseSealedRoute { next_route_ordinal })
                }
                AdversarialFallbackBehavior::Error => {
                    Err(port_error(ErrorCode::PolicyDenied, false))
                }
            }
        })
    }

    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}

fn persistent_runtime(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    journal: Arc<FixtureJournal>,
    tools: Arc<dyn ToolExecutor>,
) -> DurableAgentLoop {
    persistent_runtime_policy(fixture, provider, journal, tools, false)
}

fn persistent_runtime_policy(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    journal: Arc<FixtureJournal>,
    tools: Arc<dyn ToolExecutor>,
    failover: bool,
) -> DurableAgentLoop {
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution: RunExecutionPolicySnapshot {
                provider_failover_v1: failover,
                ..RunExecutionPolicySnapshot::production_defaults()
            },
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: non_zero_u32(2),
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        tools,
        Arc::new(EmptyControls),
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}

#[tokio::test]
async fn persistent_outages_keep_original_route_beyond_fast_retries_then_resume() {
    for (kind, safe_code, code) in [
        (
            ProviderErrorKind::RateLimited,
            ProviderErrorCode::RateLimited,
            ErrorCode::DependencyUnavailable,
        ),
        (
            ProviderErrorKind::Transport,
            ProviderErrorCode::TransportFailed,
            ErrorCode::DependencyUnavailable,
        ),
        (
            ProviderErrorKind::Server,
            ProviderErrorCode::ProviderServer,
            ErrorCode::DependencyUnavailable,
        ),
        (
            ProviderErrorKind::Timeout,
            ProviderErrorCode::DeadlineExceeded,
            ErrorCode::DependencyTimeout,
        ),
    ] {
        let fixture = Fixture::new();
        let mut scripts = (0..12)
            .map(|_| {
                Script::Events(vec![Err(ProviderError {
                    kind,
                    safe_code,
                    retryable: true,
                    retry_after_ms: Some(90_000),
                    diagnostic_ref: None,
                })])
            })
            .collect::<Vec<_>>();
        scripts.push(Script::Events(assistant_events(
            "primary-recovered",
            "Done after recovery.",
        )));
        let provider = Arc::new(ScriptedProvider::new(scripts));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = persistent_runtime(
            &fixture,
            provider.clone(),
            journal.clone(),
            Arc::new(CompletingTools),
        );
        for attempt in 0..12 {
            assert_durable_failure(
                runtime
                    .execute(
                        fixture.attempt.clone(),
                        fixture.telemetry(),
                        InvocationSupervisor::new(),
                    )
                    .await,
                AgentLoopFailureStage::ModelExecution,
                code,
                true,
            );
            assert_eq!(
                provider.requests().len(),
                attempt + 1,
                "one invocation per durable retry"
            );
            assert_eq!(journal.snapshot().active_model_route_ordinal, 0);
            assert_eq!(journal.snapshot().phase, LoopPhase::ReadyForModel);
        }
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
        assert!(
            provider
                .requests()
                .iter()
                .all(|request| request.route.route_ref.as_str() == "model.route.fixture@1")
        );
        assert!(journal.failures().is_empty());
        assert_eq!(
            *lock(&journal.model_retry_after_hints),
            vec![Some(90_000); 12]
        );
    }
}

#[tokio::test]
async fn persistent_rate_limit_after_tool_completion_preserves_results_without_reexecution() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("read-before-rate-limit");
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("tool-response"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: wire_name("visible_tool"),
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
        Script::Events(vec![Err(ProviderError {
            kind: ProviderErrorKind::RateLimited,
            safe_code: ProviderErrorCode::RateLimited,
            retryable: true,
            retry_after_ms: Some(10_000),
            diagnostic_ref: None,
        })]),
        Script::Events(assistant_events(
            "resumed-after-tool",
            "Completed with the existing result.",
        )),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(CountingTools::default());
    let runtime = persistent_runtime(&fixture, provider.clone(), journal.clone(), tools.clone());
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
    assert_eq!(tools.executions.load(Ordering::SeqCst), 1);
    assert_eq!(journal.snapshot().completed_tool_calls, 1);
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
    assert_eq!(tools.executions.load(Ordering::SeqCst), 1);
    assert_eq!(journal.snapshot().active_model_route_ordinal, 0);
    assert_eq!(provider.requests().len(), 3);
}

#[tokio::test]
async fn persistent_runs_terminate_hard_blockers_without_fallback_or_repair_wait() {
    for (safe_code, code) in [
        (
            ProviderErrorCode::CreditsExhausted,
            ErrorCode::CreditsExhausted,
        ),
        (
            ProviderErrorCode::ModelNotEntitled,
            ErrorCode::ModelNotEntitled,
        ),
        (ProviderErrorCode::QuotaExceeded, ErrorCode::QuotaExceeded),
        (
            ProviderErrorCode::AuthenticationFailed,
            ErrorCode::Unauthenticated,
        ),
        (
            ProviderErrorCode::InvalidRequest,
            ErrorCode::ModelRequestRejected,
        ),
        (
            ProviderErrorCode::ProtocolViolation,
            ErrorCode::ModelResponseInvalid,
        ),
    ] {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
            ProviderError {
                kind: ProviderErrorKind::Quota,
                safe_code,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            },
        )])]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = persistent_runtime(
            &fixture,
            provider.clone(),
            journal.clone(),
            Arc::new(CompletingTools),
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
            code,
            false,
        );
        assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
        assert_eq!(provider.requests().len(), 1);
    }
}

#[tokio::test]
async fn new_persistent_admissions_try_a_sealed_alternate_only_for_upstream_failures() {
    for (kind, safe_code, retryable, allow) in [
        (
            ProviderErrorKind::Quota,
            ProviderErrorCode::PaymentRequired,
            false,
            true,
        ),
        (
            ProviderErrorKind::Authentication,
            ProviderErrorCode::AuthenticationFailed,
            false,
            true,
        ),
        (
            ProviderErrorKind::Quota,
            ProviderErrorCode::QuotaExceeded,
            false,
            true,
        ),
        (
            ProviderErrorKind::RateLimited,
            ProviderErrorCode::RateLimited,
            true,
            true,
        ),
        (
            ProviderErrorKind::Transport,
            ProviderErrorCode::TransportFailed,
            true,
            true,
        ),
        (
            ProviderErrorKind::Server,
            ProviderErrorCode::ProviderServer,
            true,
            true,
        ),
        (
            ProviderErrorKind::Timeout,
            ProviderErrorCode::DeadlineExceeded,
            true,
            true,
        ),
        (
            ProviderErrorKind::Quota,
            ProviderErrorCode::CreditsExhausted,
            false,
            false,
        ),
        (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::AuthorizationFailed,
            false,
            false,
        ),
        (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::ModelNotEntitled,
            false,
            false,
        ),
        (
            ProviderErrorKind::Server,
            ProviderErrorCode::DependencyStoreUnavailable,
            true,
            false,
        ),
    ] {
        let fixture = Fixture::new();
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(vec![Err(ProviderError {
                kind,
                safe_code,
                retryable,
                retry_after_ms: Some(9000),
                diagnostic_ref: None,
            })]),
            Script::Events(vec![
                Ok(ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("backup"),
                    response_model_key: model_key(),
                }),
                Ok(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: model_text("ready"),
                }),
                Ok(ModelStreamEvent::Usage(ModelUsage {
                    output_tokens: 1,
                    ..ModelUsage::default()
                })),
                Ok(ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                }),
            ]),
        ]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = persistent_runtime_policy(
            &fixture,
            provider.clone(),
            journal.clone(),
            Arc::new(CompletingTools),
            true,
        );
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        let requests = provider.requests();
        assert_eq!(
            requests.len(),
            if allow { 2 } else { 1 },
            "{safe_code:?}: {exit:?}"
        );
        if allow {
            assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
            assert_ne!(
                requests[0].invocation.invocation_id,
                requests[1].invocation.invocation_id
            );
            assert_eq!(journal.snapshot().active_model_route_ordinal, 1);
        }
    }
}

#[tokio::test]
async fn persistent_round_remembers_a_temporary_primary_when_the_last_backup_needs_repair() {
    for (kind, cause) in [
        (
            ProviderErrorKind::RateLimited,
            ProviderErrorCode::RateLimited,
        ),
        (
            ProviderErrorKind::Authentication,
            ProviderErrorCode::AuthenticationFailed,
        ),
    ] {
        let fixture = Fixture::new();
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let provider = Arc::new(ScriptedProvider::new(
            [
                (kind, cause),
                (ProviderErrorKind::Quota, ProviderErrorCode::PaymentRequired),
            ]
            .into_iter()
            .map(|(kind, safe_code)| {
                Script::Events(vec![Err(ProviderError {
                    kind,
                    safe_code,
                    retryable: safe_code.permits_dependency_recovery(),
                    retry_after_ms: None,
                    diagnostic_ref: None,
                })])
            })
            .collect(),
        ));
        let runtime = persistent_runtime_policy(
            &fixture,
            provider.clone(),
            journal.clone(),
            Arc::new(CompletingTools),
            true,
        );
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert_eq!(provider.requests().len(), 2);
        if cause == ProviderErrorCode::AuthenticationFailed {
            assert!(
                matches!(exit, AgentLoopExit::NeedsAction { .. }),
                "{exit:?}"
            );
        } else {
            assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
        }
        assert_eq!(journal.snapshot().active_model_route_ordinal, 0);
    }
}
