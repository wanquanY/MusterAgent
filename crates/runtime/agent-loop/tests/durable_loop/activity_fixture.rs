//! Explicit physical ownership for the deterministic Loop fixtures. Production
//! adapters never inherit this in-memory stop proof or stream-only adapter.
use super::activity_model::home;
use super::*;
use musteragent_execution_liveness::{
    ExecutionStopConfirmation, ExecutionStopControl, ExecutionStopError,
    StopReason as ExecutionStopReason,
};
use musteragent_runtime_contract::execution_liveness::{
    ExecutionBinding, ExecutionLivenessWindows,
};
use musteragent_runtime_model::{ModelExecution, ProviderRecoveryCapability};
use musteragent_runtime_snapshot::RunLivenessPolicyV1;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

pub(super) fn runtime(
    clock: Arc<dyn musteragent_loop::AgentLoopClock>,
    policy: Arc<dyn LoopPolicy>,
    provider: Arc<dyn ModelProvider>,
    journal: Arc<FixtureJournal>,
) -> DurableAgentLoop {
    runtime_with_hook_fault(clock, policy, provider, journal, None)
}

pub(super) fn runtime_with_hook_fault(
    clock: Arc<dyn musteragent_loop::AgentLoopClock>,
    policy: Arc<dyn LoopPolicy>,
    provider: Arc<dyn ModelProvider>,
    journal: Arc<FixtureJournal>,
    hook_fault: Option<(ToolCallId, bool)>,
) -> DurableAgentLoop {
    let home = Arc::new(home::Home {
        record: Mutex::new(None),
        clock: Arc::new(home::Clock::new()),
        policy: Mutex::new(Arc::new(
            RunLivenessPolicyV1::production_default().model_policy(),
        )),
    });
    let supervision = musteragent_loop::ExecutionSupervision::new(
        home.clone(),
        home.clock.clone(),
        Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
    );
    let journal = Arc::new(home::Journal {
        inner: journal,
        home: home.clone(),
        delay: Duration::ZERO,
        reads: Arc::new(AtomicUsize::new(0)),
        first_commit_reads: AtomicUsize::new(0),
        tool_commit_failure: Mutex::new(None),
        tool_recovery: Mutex::new(None),
    });
    DurableAgentLoop::new(
        clock,
        Arc::new(Policy {
            inner: policy,
            home,
            hook_fault,
        }),
        Arc::new(Provider(provider)),
        journal,
    )
    .with_execution_supervision(supervision)
}
struct Policy {
    hook_fault: Option<(ToolCallId, bool)>,
    inner: Arc<dyn LoopPolicy>,
    home: Arc<home::Home>,
}
macro_rules! forward {
    ($name:ident($($arg:ident: $ty:ty),*) -> $result:ty) => {
        fn $name(&self, $($arg: $ty),*) -> ApplicationFuture<'_, $result> {
            self.inner.$name($($arg),*)
        }
    }
}
impl LoopPolicy for Policy {
    fn load_execution_policy(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let loading = self.inner.load_execution_policy(attempt);
        Box::pin(async move {
            let mut policy = loading.await?;
            policy.snapshot = snapshot(policy.snapshot);
            *lock(&self.home.policy) =
                Arc::new(policy.snapshot.liveness.expect("profile").model_policy());
            Ok(policy)
        })
    }
    fn transform_context(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let timing = input.invocation_timing.clone();
        let result = self.inner.transform_context(input);
        Box::pin(async move {
            let mut iteration = result.await?;
            iteration.request.invocation.timing = timing;
            Ok(iteration)
        })
    }
    fn execute_activity_tool<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        telemetry: &'a AttemptTelemetryContext,
        supervisor: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        // All resources in these deterministic Tool fixtures belong to this
        // future. Real process/remote stop evidence has dedicated integration tests.
        Box::pin(supervisor.run(
            musteragent_loop::CancellationCause::UserRequested,
            self.inner.execute_tool(attempt, telemetry, call),
        ))
    }
    forward!(context_compaction_policy_ref(attempt: &AttemptContext) -> Result<Option<VersionedKey>, LoopPortError>);
    forward!(gate(context: &StepContext) -> Result<GateVerdict, LoopPortError>);
    fn before_tool(
        &self,
        call: &PlannedToolCall,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>> {
        if self
            .hook_fault
            .as_ref()
            .is_some_and(|(id, after)| id == &call.call_id && !after)
        {
            return Box::pin(async {
                Err(LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                })
            });
        }
        self.inner.before_tool(call, context)
    }
    forward!(execute_tool(attempt: &AttemptContext, telemetry: &AttemptTelemetryContext, call: PlannedToolCall) -> Result<ToolExecutionOutcome, LoopPortError>);
    forward!(request_tool_approval(attempt: &AttemptContext, request: musteragent_approval::ApprovalRequest) -> Result<InteractionId, LoopPortError>);
    fn after_tool(
        &self,
        call: &PlannedToolCall,
        outcome: ToolExecutionOutcome,
        context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        if self
            .hook_fault
            .as_ref()
            .is_some_and(|(id, after)| id == &call.call_id && *after)
        {
            return Box::pin(async {
                Err(LoopPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                })
            });
        }
        self.inner.after_tool(call, outcome, context)
    }
    forward!(should_stop(turn: TurnSummary, context: &StepContext) -> Result<bool, LoopPortError>);
    forward!(prepare_next_turn(turn: TurnSummary, context: &StepContext) -> Result<NextTurnDecision, LoopPortError>);
    forward!(fallback_after_upstream_failure(context: &StepContext) -> Result<FallbackDecision, LoopPortError>);
    forward!(fallback_after_model_failure(failure: ModelFailure, context: &StepContext) -> Result<FallbackDecision, LoopPortError>);
    forward!(drain_steering(attempt: &AttemptContext) -> Result<u32, LoopPortError>);
    forward!(drain_follow_up(attempt: &AttemptContext) -> Result<u32, LoopPortError>);
    fn preview_tool_arguments(
        &self,
        attempt: &AttemptContext,
        call: &ToolCallId,
        name: &WireName,
        json: &str,
    ) -> Option<musteragent_loop::ToolDocumentPreview> {
        self.inner.preview_tool_arguments(attempt, call, name, json)
    }
}
struct Provider(Arc<dyn ModelProvider>);
impl ModelProvider for Provider {
    fn recovery_capability(&self, request: &CanonicalModelRequest) -> ProviderRecoveryCapability {
        self.0.recovery_capability(request)
    }
    fn execution<'a>(
        &'a self,
        request: CanonicalModelRequest,
        binding: ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'a> + 'a>, ProviderError> {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(Operation {
            provider: self.0.as_ref(),
            request,
            control: Arc::new(Control {
                binding,
                cancelled: CancellationToken::new(),
                active: AtomicBool::new(false),
                finished: Notify::new(),
            }),
        }))
    }
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("supervised entry required")
    }
}
struct Control {
    binding: ExecutionBinding,
    cancelled: CancellationToken,
    active: AtomicBool,
    finished: Notify,
}
impl ExecutionStopControl for Control {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn stop(
        &self,
        _: ExecutionStopReason,
    ) -> ApplicationFuture<'_, Result<ExecutionStopConfirmation, ExecutionStopError>> {
        Box::pin(async move {
            self.cancelled.cancel();
            loop {
                let changed = self.finished.notified();
                if !self.active.load(Ordering::SeqCst) {
                    break;
                }
                changed.await;
            }
            Ok(ExecutionStopConfirmation {
                binding: self.binding,
            })
        })
    }
}
struct Operation<'a> {
    provider: &'a dyn ModelProvider,
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
        self.provider.preflight(&self.request)
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        self.control.active.store(true, Ordering::SeqCst);
        Box::pin(OwnedStream {
            stream: Box::pin(
                self.provider
                    .stream(self.request)
                    .take_until(self.control.cancelled.clone().cancelled_owned()),
            ),
            control: self.control,
        })
    }
}
struct OwnedStream<'a> {
    stream: ModelEventStream<'a>,
    control: Arc<Control>,
}
impl futures_util::Stream for OwnedStream<'_> {
    type Item = Result<ModelStreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}
impl Drop for OwnedStream<'_> {
    fn drop(&mut self) {
        self.control.active.store(false, Ordering::SeqCst);
        self.control.finished.notify_waiters();
    }
}

pub(super) fn snapshot(mut snapshot: RunExecutionPolicySnapshot) -> RunExecutionPolicySnapshot {
    if snapshot.liveness.is_some_and(|profile| {
        profile.model_policy().windows().idle == snapshot.model_inactivity_timeout
    }) {
        return snapshot;
    }
    let mut windows = ExecutionLivenessWindows::for_idle_window(snapshot.model_inactivity_timeout);
    windows.cleanup = Duration::from_millis(100);
    windows.post_exit_drain = Duration::from_secs(1);
    snapshot.liveness = Some(RunLivenessPolicyV1::new(windows, windows).expect("fixture windows"));
    snapshot
}
