use super::*;
use musteragent_execution_liveness::{
    Closure, ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    LivenessHomeClock, Phase, StopReason as ExecutionStopReason,
};
use musteragent_execution_liveness_io::HomeExecutionActivityFactory;
use musteragent_loop::CancellationCause;
use musteragent_runtime_contract::execution_liveness::{
    ExecutionBinding, ExecutionLivenessWindows,
};
use musteragent_runtime_model::ModelExecution;
use musteragent_runtime_snapshot::RunLivenessPolicyV1;
use musteragent_telemetry::TelemetryOutcome;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;
#[path = "activity_model/home.rs"]
pub(super) mod home;
#[path = "activity_model/supervision_failure.rs"]
mod supervision_failure;
#[path = "activity_model/tool_recovery_gate.rs"]
mod tool_recovery_gate;

#[path = "activity_model/rejection_drain.rs"]
mod rejection_drain;

struct ContextSource {
    run: RunContext,
    journal: Arc<FixtureJournal>,
}
impl ContextPlanner for ContextSource {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let mut iteration = model_request(
            self.run,
            input.attempt,
            input.next_iteration,
            input.active_model_route_ordinal,
            input.continuation_ref,
            input.consecutive_empty_responses,
        );
        iteration.request.invocation.timing = input.invocation_timing;
        iteration.request.input.extend(
            lock(&self.journal.tool_results)
                .iter()
                .map(|result| result.message.clone()),
        );
        Box::pin(async move { Ok(iteration) })
    }
}
struct Provider {
    scripts: ScriptedProvider,
    home: Arc<home::Home>,
    reads: Arc<AtomicUsize>,
    provable_stop: bool,
    controls: Mutex<Vec<Arc<Control>>>,
}
struct Control {
    binding: ExecutionBinding,
    home: Arc<home::Home>,
    cancel: CancellationToken,
    active: AtomicBool,
    finished: Notify,
    terminal: AtomicBool,
    provable: bool,
    calls: Mutex<Vec<ExecutionStopReason>>,
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        reason: ExecutionStopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            assert!(
                matches!(self.home.phase(), Phase::Stopping { .. } | Phase::Closed(_))
                    || reason == ExecutionStopReason::SystemShutdown,
                "stop intent must commit first"
            );
            lock(&self.calls).push(reason);
            self.cancel.cancel();
            loop {
                let notified = self.finished.notified();
                if !self.active.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
            if self.provable || self.terminal.load(Ordering::SeqCst) {
                Ok(ExecutionStopConfirmation {
                    binding: self.binding,
                })
            } else {
                Err(ExecutionStopError {
                    code: ErrorCode::DependencyUnavailable,
                    rejection: None,
                })
            }
        })
    }
}
struct Operation<'a> {
    provider: &'a Provider,
    request: CanonicalModelRequest,
    control: Arc<Control>,
}
impl<'a> ModelExecution<'a> for Operation<'a> {
    fn binding(&self) -> ExecutionBinding {
        self.control.binding
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async { Ok(()) })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        self.control.active.store(true, Ordering::SeqCst);
        let stream = self
            .provider
            .scripts
            .stream(self.request)
            .take_until(self.control.cancel.clone().cancelled_owned());
        Box::pin(OwnedStream {
            stream: Box::pin(stream),
            control: self.control,
            reads: self.provider.reads.clone(),
        })
    }
}
struct OwnedStream<'a> {
    stream: ModelEventStream<'a>,
    control: Arc<Control>,
    reads: Arc<AtomicUsize>,
}
impl futures_util::Stream for OwnedStream<'_> {
    type Item = Result<ModelStreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.stream.as_mut().poll_next(cx);
        if let Poll::Ready(Some(event)) = &result {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if matches!(event, Ok(ModelStreamEvent::ResponseCompleted { .. })) {
                self.control.terminal.store(true, Ordering::SeqCst);
            }
        }
        result
    }
}
impl Drop for OwnedStream<'_> {
    fn drop(&mut self) {
        self.control.active.store(false, Ordering::SeqCst);
        self.control.finished.notify_waiters();
    }
}
impl ModelProvider for Provider {
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("activity mode must consume the owned physical operation")
    }
    fn execution<'a>(
        &'a self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'a> + 'a>, ProviderError> {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        let control = Arc::new(Control {
            binding,
            home: self.home.clone(),
            cancel: CancellationToken::new(),
            active: AtomicBool::new(false),
            finished: Notify::new(),
            terminal: AtomicBool::new(false),
            provable: self.provable_stop,
            calls: Mutex::new(vec![]),
        });
        lock(&self.controls).push(control.clone());
        Ok(Box::new(Operation {
            provider: self,
            request,
            control,
        }))
    }
}
fn runtime(
    script: Script,
    delay: Duration,
    provable_stop: bool,
) -> (Fixture, DurableAgentLoop, Arc<home::Journal>, Arc<Provider>) {
    runtime_with_tools(
        script,
        delay,
        provable_stop,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
    )
}
fn runtime_with_tools(
    script: Script,
    delay: Duration,
    provable_stop: bool,
    tools: Arc<dyn ToolExecutor>,
    mode: ToolExecutionMode,
) -> (Fixture, DurableAgentLoop, Arc<home::Journal>, Arc<Provider>) {
    runtime_with_scripts(vec![script], delay, provable_stop, tools, mode)
}
fn runtime_with_scripts(
    scripts: Vec<Script>,
    delay: Duration,
    provable_stop: bool,
    tools: Arc<dyn ToolExecutor>,
    mode: ToolExecutionMode,
) -> (Fixture, DurableAgentLoop, Arc<home::Journal>, Arc<Provider>) {
    runtime_with_recovery(
        scripts,
        delay,
        provable_stop,
        tools,
        mode,
        NonZeroU32::MIN.into(),
    )
}
fn runtime_with_recovery(
    scripts: Vec<Script>,
    delay: Duration,
    provable_stop: bool,
    tools: Arc<dyn ToolExecutor>,
    mode: ToolExecutionMode,
    recovery: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
) -> (Fixture, DurableAgentLoop, Arc<home::Journal>, Arc<Provider>) {
    let fixture = Fixture::new();
    let windows = ExecutionLivenessWindows {
        startup: Duration::from_millis(300),
        idle: Duration::from_millis(200),
        max_delivery_lag: Duration::from_millis(100),
        checkpoint_interval: Duration::from_millis(10),
        observation_request: Duration::from_millis(80),
        cleanup: Duration::from_millis(150),
        post_exit_drain: Duration::from_secs(1),
    };
    let profile = RunLivenessPolicyV1::new(windows, windows).expect("profile");
    let home = Arc::new(home::Home {
        record: Mutex::new(None),
        clock: Arc::new(home::Clock::new()),
        policy: Mutex::new(Arc::new(profile.model_policy())),
    });
    let reads = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(Provider {
        scripts: ScriptedProvider::new(scripts),
        home: home.clone(),
        reads: reads.clone(),
        provable_stop,
        controls: Mutex::new(vec![]),
    });
    let journal = Arc::new(home::Journal {
        inner: Arc::new(FixtureJournal::new(fixture.checkpoint())),
        home: home.clone(),
        delay,
        reads,
        first_commit_reads: AtomicUsize::new(0),
        tool_commit_failure: Mutex::new(None),
        tool_recovery: Mutex::new(None),
    });
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    execution.recovery_policy = recovery;
    execution.liveness = Some(profile);
    execution.default_tool_execution_mode = mode;
    execution.model_inactivity_timeout = windows.idle;
    execution.max_model_events_per_commit = non_zero_usize(2);
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(ContextSource {
            run: fixture.run,
            journal: journal.inner.clone(),
        }),
        tools,
        Arc::new(EmptyControls),
    ));
    let supervision = musteragent_loop::ExecutionSupervision::new(
        home.clone(),
        home.clock.clone(),
        Arc::new(HomeExecutionActivityFactory),
    );
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        journal.clone(),
    )
    .with_execution_supervision(supervision);
    (fixture, runtime, journal, provider)
}
fn started() -> Result<ModelStreamEvent, ProviderError> {
    Ok(ModelStreamEvent::ResponseStarted {
        response_ref: provider_ref("activity-model"),
        response_model_key: model_key(),
    })
}

pub(super) async fn sustained_model_work_is_read_during_original_journal_commits_and_outlives_idle_window()
 {
    let mut events = vec![(Duration::ZERO, started())];
    for _ in 0..30 {
        events.push((
            Duration::from_millis(50),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("progress "),
            }),
        ));
    }
    events.push((
        Duration::ZERO,
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ));
    let (fixture, runtime, journal, provider) = runtime(
        Script::DelayedEvents(events),
        Duration::from_millis(60),
        true,
    );
    let start = tokio::time::Instant::now();
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert!(start.elapsed() > Duration::from_secs(1));
    assert_eq!(provider.reads.load(Ordering::SeqCst), 32);
    assert!(
        journal.first_commit_reads.load(Ordering::SeqCst) >= 2,
        "the second event must be consumed while the first write is pending"
    );
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
    assert!(lock(&lock(&provider.controls)[0].calls).is_empty());
}

pub(super) async fn repeated_model_usage_cannot_renew_idle_and_unknown_stop_remains_recorded() {
    let mut events = vec![(Duration::ZERO, started())];
    for _ in 0..20 {
        events.push((
            Duration::from_millis(40),
            Ok(ModelStreamEvent::Usage(ModelUsage::default())),
        ));
    }
    let (fixture, runtime, journal, provider) =
        runtime(Script::DelayedEvents(events), Duration::ZERO, false);
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::StopUnconfirmed(ExecutionStopReason::IdleExpired))
    );
    assert_eq!(provider.scripts.requests().len(), 1);
    assert!(!lock(&provider.controls)[0].active.load(Ordering::SeqCst));
}

pub(super) async fn model_cancellation_drains_owned_stream_before_original_terminal_receipt() {
    for (cause, provable_stop) in [
        (CancellationCause::UserRequested, true),
        (CancellationCause::CoordinationRequested, true),
        (CancellationCause::UserRequested, false),
        (CancellationCause::CoordinationRequested, false),
    ] {
        let (fixture, runtime, journal, provider) = runtime(
            Script::EventsThenPending(vec![started()]),
            Duration::ZERO,
            provable_stop,
        );
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observer = observed.clone();
        let resource = provider.clone();
        let runtime = runtime.with_invocation_completion_observer(Arc::new(
            move |_, _, outcome, error, _, _, _| {
                assert!(!lock(&resource.controls)[0].active.load(Ordering::SeqCst));
                lock(&observer).push((outcome, error));
            },
        ));
        let supervisor = InvocationSupervisor::new();
        let trigger = supervisor.clone();
        let execution = runtime.execute(fixture.attempt.clone(), fixture.telemetry(), supervisor);
        let cancel = async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel_with(cause);
        };
        let (exit, ()) = tokio::join!(execution, cancel);
        assert!(
            matches!(
                exit,
                AgentLoopExit::Cancelled { .. } | AgentLoopExit::CoordinationInterrupted { .. }
            ),
            "{exit:?}"
        );
        let reason = if cause == CancellationCause::UserRequested {
            ExecutionStopReason::UserCancelled
        } else {
            ExecutionStopReason::CoordinationRequested
        };
        assert_eq!(
            journal.home.phase(),
            Phase::Closed(if provable_stop {
                Closure::StopConfirmed(reason)
            } else {
                Closure::StopUnconfirmed(reason)
            })
        );
        assert!(!lock(&provider.controls)[0].active.load(Ordering::SeqCst));
        let expected = if cause == CancellationCause::UserRequested {
            TelemetryOutcome::Cancelled
        } else {
            TelemetryOutcome::Interrupted
        };
        assert_eq!(*lock(&observed), vec![(expected, None)]);
    }
}

pub(super) async fn native_model_end_uses_original_drain_budget_instead_of_idle_budget() {
    let events = vec![
        started(),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("answer"),
        }),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ];
    let (fixture, runtime, journal, provider) =
        runtime(Script::EventsThenPending(events), Duration::ZERO, false);
    let started = tokio::time::Instant::now();
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(started.elapsed() < Duration::from_millis(1200));
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::StopConfirmed(
            ExecutionStopReason::PostExitDrainExpired
        ))
    );
    assert_eq!(provider.scripts.requests().len(), 1);
    assert!(!lock(&provider.controls)[0].active.load(Ordering::SeqCst));
}

pub(super) async fn native_model_end_can_finish_after_idle_due_within_original_drain_budget() {
    let events = vec![
        started(),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text("answer"),
        }),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ];
    let (fixture, runtime, journal, provider) = runtime(
        Script::EventsThenDelayedEof(events, Duration::from_millis(500)),
        Duration::ZERO,
        false,
    );
    let before = tokio::time::Instant::now();
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert!(before.elapsed() >= Duration::from_millis(500));
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
    assert!(lock(&lock(&provider.controls)[0].calls).is_empty());
}
pub(super) async fn model_startup_and_transport_failure_need_independent_stop_proof() {
    for (script, reason) in [
        (Script::Pending, ExecutionStopReason::StartupExpired),
        (
            Script::Events(vec![
                started(),
                Err(ProviderError {
                    kind: ProviderErrorKind::Transport,
                    safe_code: ProviderErrorCode::TransportFailed,
                    retryable: true,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                }),
            ]),
            ExecutionStopReason::ExecutionFailed,
        ),
    ] {
        let (fixture, runtime, journal, provider) = runtime(script, Duration::ZERO, false);
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
        assert_eq!(
            journal.home.phase(),
            Phase::Closed(Closure::StopUnconfirmed(reason))
        );
        assert_eq!(provider.scripts.requests().len(), 1);
        assert!(!lock(&provider.controls)[0].active.load(Ordering::SeqCst));
    }
}
pub(super) async fn stalled_model_journal_is_an_infrastructure_interruption_without_false_idle_stop()
 {
    let (fixture, runtime, journal, provider) = runtime(
        Script::EventsThenPending(vec![
            started(),
            Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text("answer"),
            }),
        ]),
        Duration::from_millis(500),
        true,
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
            AgentLoopExit::Interrupted {
                cause: CancellationCause::SystemShutdown
            }
        ),
        "{exit:?}"
    );
    // Only the business event append is stalled in this fixture. The original
    // Home Journal is healthy and records its exact emergency physical stop.
    // That cleanup receipt must not become a model/business failure receipt.
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::StopConfirmed(ExecutionStopReason::SystemShutdown))
    );
    assert!(journal.inner.failures().is_empty());
    assert_eq!(
        *lock(&lock(&provider.controls)[0].calls),
        vec![ExecutionStopReason::SystemShutdown]
    );
    assert!(!lock(&provider.controls)[0].active.load(Ordering::SeqCst));
}

fn tool_script(count: u32) -> Script {
    named_tool_script("owned-tool", count)
}
fn named_tool_script(prefix: &str, count: u32) -> Script {
    let mut events = vec![started()];
    for index in 0..count {
        let call_id = tool_call_id(&format!("{prefix}-{index}"));
        events.extend([
            Ok(ModelStreamEvent::ToolCallStarted {
                output_index: index,
                call_id: call_id.clone(),
                name: wire_name("visible_tool"),
                provider_item_ref: None,
                provider_replay: None,
            }),
            Ok(ModelStreamEvent::ToolCallCompleted {
                output_index: index,
                call_id,
                arguments: JsonValue::Object(BTreeMap::new()),
            }),
        ]);
    }
    events.push(Ok(ModelStreamEvent::ResponseCompleted {
        stop_reason: StopReason::ToolCalls,
    }));
    Script::Events(events)
}
struct OwnedTools {
    started: AtomicUsize,
    cleaned: AtomicUsize,
    premature_drop: AtomicBool,
    entered: Notify,
}
impl ToolExecutor for OwnedTools {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("an activity Tool must never use its legacy execution boundary")
    }
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a AttemptTelemetryContext,
        supervisor: &'a InvocationSupervisor,
        _: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(async move {
            struct Drain<'a> {
                finished: bool,
                early: &'a AtomicBool,
            }
            impl Drop for Drain<'_> {
                fn drop(&mut self) {
                    if !self.finished {
                        self.early.store(true, Ordering::SeqCst);
                    }
                }
            }
            let mut drain = Drain {
                finished: false,
                early: &self.premature_drop,
            };
            let index = self.started.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let aborted = supervisor.cancelled(CancellationCause::UserRequested).await;
            tokio::time::sleep(Duration::from_millis(
                50 + u64::try_from(index).expect("index") * 50,
            ))
            .await;
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            drain.finished = true;
            Err(aborted)
        })
    }
}
pub(super) async fn activity_tool_cancellation_keeps_every_owned_cleanup_future_polled() {
    for (mode, count) in [
        (ToolExecutionMode::Sequential, 1),
        (ToolExecutionMode::Parallel, 2),
    ] {
        let tools = Arc::new(OwnedTools {
            started: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            premature_drop: AtomicBool::new(false),
            entered: Notify::new(),
        });
        let (fixture, runtime, _, _) = runtime_with_tools(
            tool_script(count),
            Duration::ZERO,
            true,
            tools.clone(),
            mode,
        );
        let supervisor = InvocationSupervisor::new();
        let trigger = supervisor.clone();
        let execute = runtime.execute(fixture.attempt.clone(), fixture.telemetry(), supervisor);
        let cancel = async {
            loop {
                let entered = tools.entered.notified();
                if tools.started.load(Ordering::SeqCst) == count as usize {
                    break;
                }
                entered.await;
            }
            trigger.cancel_with(CancellationCause::UserRequested);
        };
        let (exit, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(execute, cancel)
        })
        .await
        .expect("owned cleanup finishes without dropped futures");
        assert!(matches!(exit, AgentLoopExit::Cancelled { .. }), "{exit:?}");
        assert_eq!(tools.cleaned.load(Ordering::SeqCst), count as usize);
        assert!(!tools.premature_drop.load(Ordering::SeqCst));
    }
}
pub(super) async fn activity_tool_cannot_fall_through_to_an_unsupported_legacy_executor() {
    let (fixture, runtime, journal, provider) = runtime(tool_script(1), Duration::ZERO, true);
    lock(&provider.scripts.scripts).push_back(Script::Events(assistant_events(
        "unsupported-tool-recovery",
        "Continue using another tool.",
    )));
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
                stage: AgentLoopFailureStage::ToolExecution,
                code: ErrorCode::CapabilityUnavailable,
                ..
            }
        ),
        "{exit:?}"
    );
    assert!(journal.inner.committed_tool_calls().is_empty());
    assert_eq!(provider.scripts.requests().len(), 1);
}

struct MixedTools {
    unknown: bool,
    cancel_first: bool,
}
impl ToolExecutor for MixedTools {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("activity only")
    }
    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a AttemptTelemetryContext,
        supervisor: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(async move {
            if self.cancel_first && call.call_id.as_str() == "owned-tool-0" {
                supervisor.cancel();
                return Err(supervisor.cancelled(CancellationCause::UserRequested).await);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            let message = Box::new(CanonicalMessage {
                role: CanonicalRole::Tool,
                tool_name: Some(call.name),
                content: vec![CanonicalContentPart::Text(model_text(
                    "original tool result",
                ))],
                tool_call_id: Some(call.call_id),
                tool_calls: vec![],
                reasoning: None,
            });
            Ok(Ok(ToolExecutionOutcome::Completed {
                message,
                terminal: if self.unknown {
                    DurableToolCompletion::Indeterminate
                } else {
                    DurableToolCompletion::Succeeded
                },
            }))
        })
    }
}
pub(super) async fn activity_parallel_batch_commits_siblings_before_cancellation_and_preserves_unknown()
 {
    for unknown in [false, true] {
        let (fixture, runtime, journal, provider) = runtime_with_tools(
            tool_script(2),
            Duration::ZERO,
            true,
            Arc::new(MixedTools {
                unknown,
                cancel_first: true,
            }),
            ToolExecutionMode::Parallel,
        );
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert_eq!(
            lock(&journal.inner.committed_tool_calls).as_slice(),
            &[tool_call_id("owned-tool-1")]
        );
        assert_eq!(
            provider.scripts.requests().len(),
            1,
            "explicit user cancellation prevents the next model iteration"
        );
        assert!(matches!(exit, AgentLoopExit::Cancelled { .. }), "{exit:?}");
    }
}

pub(super) async fn activity_parallel_commit_failure_preserves_sibling_results_and_unknown() {
    for unknown in [false, true] {
        let (fixture, runtime, journal, provider) = runtime_with_tools(
            tool_script(2),
            Duration::ZERO,
            true,
            Arc::new(MixedTools {
                unknown,
                cancel_first: false,
            }),
            ToolExecutionMode::Parallel,
        );
        *lock(&journal.tool_commit_failure) = Some(tool_call_id("owned-tool-0"));
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert_eq!(
            lock(&journal.inner.committed_tool_calls).as_slice(),
            &[tool_call_id("owned-tool-1")]
        );
        assert_eq!(provider.scripts.requests().len(), 1);
        assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
    }
}

#[path = "activity_model/tool_feedback.rs"]
pub(super) mod tool_feedback;
