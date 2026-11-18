//! Dependency failures are model input, never a lifetime limit on the Agent.
use super::*;

struct UncommittedPortFailure(ErrorCode);
impl ToolExecutor for UncommittedPortFailure {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("activity path required")
    }
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a AttemptTelemetryContext,
        _: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(async move {
            if call.call_id.as_str().ends_with("-0") {
                Ok(Err(LoopPortError {
                    code: self.0,
                    retryable: false,
                }))
            } else {
                Ok(Ok(ToolExecutionOutcome::Completed {
                    message: Box::new(CanonicalMessage {
                        role: CanonicalRole::Tool,
                        content: vec![CanonicalContentPart::Text(model_text("committed sibling"))],
                        tool_call_id: Some(call.call_id),
                        tool_name: Some(call.name),
                        tool_calls: vec![],
                        reasoning: None,
                    }),
                    terminal: DurableToolCompletion::Succeeded,
                }))
            }
        })
    }
}

#[tokio::test]
async fn tool_port_errors_preserve_the_original_failure_without_fabricating_a_completion() {
    for mode in [ToolExecutionMode::Sequential, ToolExecutionMode::Parallel] {
        for code in [
            ErrorCode::RuntimeSnapshotInvalid,
            ErrorCode::InternalInvariantViolated,
            ErrorCode::DependencyUnavailable,
        ] {
            let (fixture, runtime, journal, provider) = runtime_with_scripts(
                vec![named_tool_script("uncommitted", 2)],
                Duration::ZERO,
                true,
                Arc::new(UncommittedPortFailure(code)),
                mode,
            );
            let exit = runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new(),
                )
                .await;
            assert!(
                matches!(exit, AgentLoopExit::Failed { code: actual, .. } if actual == code),
                "{exit:?}"
            );
            let committed = journal.inner.committed_tool_calls();
            assert!(
                !committed.contains(&tool_call_id("uncommitted-0")),
                "a port error has no terminal receipt"
            );
            assert_eq!(
                committed.len(),
                usize::from(mode == ToolExecutionMode::Parallel),
                "completed parallel siblings are retained"
            );
            assert_eq!(
                provider.scripts.requests().len(),
                1,
                "no Context is assembled from a fabricated result"
            );
        }
    }
}

struct FeedbackTools {
    calls: AtomicUsize,
}
impl ToolExecutor for FeedbackTools {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("activity path required")
    }
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a AttemptTelemetryContext,
        _: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (code, terminal) = if call.call_id.as_str().ends_with("-0") {
                (
                    "dependency_timeout",
                    DurableToolCompletion::Failed {
                        code: ErrorCode::DependencyTimeout,
                    },
                )
            } else if call.call_id.as_str().ends_with("-1") {
                (
                    "dependency_unavailable",
                    DurableToolCompletion::Failed {
                        code: ErrorCode::DependencyUnavailable,
                    },
                )
            } else if call.call_id.as_str().ends_with("-2") {
                ("dependency_timeout", DurableToolCompletion::Indeterminate)
            } else {
                ("ok", DurableToolCompletion::Succeeded)
            };
            Ok(Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text(&serde_json::json!({
                        "ok": terminal == DurableToolCompletion::Succeeded,
                        "error": { "code": code },
                        "outcome": if terminal == DurableToolCompletion::Indeterminate { "unknown" } else { "confirmed" },
                    }).to_string()))],
                    tool_call_id: Some(call.call_id), tool_name: Some(call.name),
                    tool_calls: vec![], reasoning: None,
                }), terminal,
            }))
        })
    }
}

pub(crate) async fn tool_failures_timeouts_and_unknown_effects_return_to_model_in_the_same_run() {
    for mode in [ToolExecutionMode::Sequential, ToolExecutionMode::Parallel] {
        verify_continuation(mode, 1).await;
    }
}

pub(crate) async fn repeated_tool_failures_do_not_exhaust_agent_iterations_or_tool_count() {
    // Exceeds the retired 32-iteration / 128-tool defaults. No retry is made
    // by the timeout machinery; every call is a fresh scripted model decision.
    verify_continuation(ToolExecutionMode::Parallel, 40).await;
}

async fn verify_continuation(mode: ToolExecutionMode, rounds: usize) {
    let tools = Arc::new(FeedbackTools {
        calls: AtomicUsize::new(0),
    });
    let mut scripts: Vec<_> = (0..rounds)
        .map(|round| named_tool_script(&format!("round-{round}"), 4))
        .collect();
    scripts.push(Script::Events(assistant_events(
        "delivered",
        "Task completed using the available results.",
    )));
    let (fixture, runtime, journal, provider) =
        runtime_with_scripts(scripts, Duration::ZERO, true, tools.clone(), mode);
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert!(journal.inner.failures().is_empty());
    assert_eq!(tools.calls.load(Ordering::SeqCst), rounds * 4);
    assert_eq!(journal.inner.committed_tool_calls().len(), rounds * 4);
    let requests = provider.scripts.requests();
    assert_eq!(requests.len(), rounds + 1);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(
            request.invocation.run_context.run_id,
            fixture.attempt.run_id
        );
        let feedback: Vec<_> = request
            .input
            .iter()
            .filter(|message| message.role == CanonicalRole::Tool)
            .collect();
        assert_eq!(
            feedback.len(),
            index * 4,
            "all prior Tool results reach the next model request"
        );
        if index > 0 {
            let encoded = format!("{feedback:?}");
            assert!(encoded.contains("dependency_timeout"));
            assert!(encoded.contains("dependency_unavailable"));
            assert!(encoded.contains("unknown"));
        }
    }
}

pub(crate) async fn unknown_model_stop_preserves_call_evidence_and_recovers_the_same_agent_run() {
    let (fixture, runtime, journal, provider) = runtime_with_recovery(
        vec![
            Script::Pending,
            Script::Events(assistant_events("recovered", "Task delivered.")),
        ],
        Duration::ZERO,
        false,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
        musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default(),
    );
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(
            exit,
            AgentLoopExit::Failed {
                code: ErrorCode::DependencyTimeout,
                retryable: true,
                ..
            }
        ),
        "{exit:?}"
    );
    assert!(matches!(
        journal.home.phase(),
        Phase::Closed(Closure::StopUnconfirmed(
            ExecutionStopReason::StartupExpired
        ))
    ));
    assert_eq!(journal.inner.snapshot().phase, LoopPhase::ReadyForModel);
    assert!(
        journal.inner.failures().is_empty(),
        "the Agent Run was not failed"
    );
    // A durable dependency retry is a replacement attempt in the same Run,
    // with a new model invocation. It cannot reopen the old physical call.
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    let requests = provider.scripts.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].invocation.run_context.run_id,
        requests[1].invocation.run_context.run_id
    );
    assert_ne!(
        requests[0].invocation.invocation_id,
        requests[1].invocation.invocation_id
    );
}
