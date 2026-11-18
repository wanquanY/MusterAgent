//! Real Loop begin -> reactor HostFence -> original model failure commit.
//! The fence is the test double; the original loop/supervision paths execute.
use super::*;
use musteragent_execution_liveness::LivenessJournalError;
use musteragent_execution_supervision::{HostExecutionFence, HostExecutionPermit};

struct RejectedHost {
    error: LivenessJournalError,
    calls: AtomicUsize,
    cancel: Option<(InvocationSupervisor, CancellationCause)>,
}
impl HostExecutionFence for RejectedHost {
    fn acquire<'a>(
        &'a self,
        owner: &'a AttemptContext,
        control: Arc<dyn ExecutionStopControl>,
        _: Duration,
    ) -> ApplicationFuture<'a, Result<Box<dyn HostExecutionPermit>, LivenessJournalError>> {
        Box::pin(async move {
            assert_eq!(control.binding().run_id, owner.run_id);
            assert_eq!(control.binding().owner_attempt_id, owner.attempt_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some((supervisor, cause)) = &self.cancel {
                supervisor.cancel_with(*cause);
            }
            Err(self.error)
        })
    }
}
fn with_host(
    runtime: DurableAgentLoop,
    journal: &home::Journal,
    host: Arc<RejectedHost>,
) -> DurableAgentLoop {
    runtime.with_execution_supervision(
        musteragent_loop::ExecutionSupervision::new(
            journal.home.clone(),
            journal.home.clock.clone(),
            Arc::new(HomeExecutionActivityFactory),
        )
        .with_host_fence(host),
    )
}
fn host(code: ErrorCode, retryable: bool) -> Arc<RejectedHost> {
    Arc::new(RejectedHost {
        error: LivenessJournalError {
            code,
            retryable,
            rejection: None,
        },
        calls: AtomicUsize::new(0),
        cancel: None,
    })
}
fn require_never_dispatched(provider: &Provider, host: &RejectedHost, journal: &home::Journal) {
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert!(provider.scripts.requests().is_empty());
    assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
    assert!(
        lock(&lock(&provider.controls)[0].calls).is_empty(),
        "no physical stop is invented before the first poll"
    );
    assert_eq!(journal.inner.snapshot().total_tokens, 0);
    assert!(journal.inner.batches().is_empty());
}
#[tokio::test(start_paused = true)]
async fn host_invariant_before_dispatch_commits_one_nonretryable_failure() {
    let (fixture, runtime, journal, provider) = runtime(Script::Pending, Duration::ZERO, true);
    let host = host(ErrorCode::InternalInvariantViolated, false);
    let runtime = with_host(runtime, &journal, host.clone());
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
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
                receipt: Some(_),
                failure_commit_error: None,
                ..
            }
        ),
        "{exit:?}"
    );
    assert_eq!(
        journal.inner.failures(),
        vec![(ErrorCode::InternalInvariantViolated, false)]
    );
    assert_eq!(journal.inner.snapshot().phase, LoopPhase::Failed);
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
    require_never_dispatched(&provider, &host, &journal);
}
#[tokio::test(start_paused = true)]
async fn host_transient_before_dispatch_uses_original_durable_recovery_budget() {
    let (fixture, runtime, journal, provider) = runtime_with_recovery(
        vec![Script::Pending],
        Duration::ZERO,
        true,
        Arc::new(CompletingTools),
        ToolExecutionMode::Parallel,
        NonZeroU32::new(2).expect("valid test fixture").into(),
    );
    let host = host(ErrorCode::DependencyUnavailable, true);
    let runtime = with_host(runtime, &journal, host.clone());
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
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                receipt: Some(_),
                failure_commit_error: None,
                ..
            }
        ),
        "{exit:?}"
    );
    assert!(journal.inner.failures().is_empty());
    assert_eq!(journal.inner.snapshot().phase, LoopPhase::ReadyForModel);
    assert_eq!(lock(&journal.inner.recovery).provider_failures, 1);
    assert_eq!(
        journal.home.phase(),
        Phase::Closed(Closure::TerminalObserved)
    );
    require_never_dispatched(&provider, &host, &journal);
}
#[tokio::test(start_paused = true)]
async fn host_fencing_and_shutdown_do_not_become_business_failures() {
    for (code, cause) in [
        (ErrorCode::AttemptLeaseStale, CancellationCause::LeaseLost),
        (ErrorCode::PolicyDenied, CancellationCause::SystemShutdown),
    ] {
        let (fixture, runtime, journal, provider) = runtime(Script::Pending, Duration::ZERO, true);
        let host = host(code, false);
        let runtime = with_host(runtime, &journal, host.clone());
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(
            matches!(exit,AgentLoopExit::Interrupted{cause:c} if c==cause),
            "{exit:?}"
        );
        assert!(journal.inner.failures().is_empty());
        require_never_dispatched(&provider, &host, &journal);
    }
    let (fixture, runtime, journal, provider) = runtime(Script::Pending, Duration::ZERO, true);
    let supervisor = InvocationSupervisor::new();
    let host = Arc::new(RejectedHost {
        error: LivenessJournalError {
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
            rejection: None,
        },
        calls: AtomicUsize::new(0),
        cancel: Some((supervisor.clone(), CancellationCause::SystemShutdown)),
    });
    let runtime = with_host(runtime, &journal, host.clone());
    let exit = runtime
        .execute(fixture.attempt.clone(), fixture.telemetry(), supervisor)
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
    assert!(journal.inner.failures().is_empty());
    require_never_dispatched(&provider, &host, &journal);
}
#[tokio::test(start_paused = true)]
async fn user_cancellation_wins_over_a_racing_undispatched_host_failure() {
    let (fixture, runtime, journal, provider) = runtime(Script::Pending, Duration::ZERO, true);
    let supervisor = InvocationSupervisor::new();
    let host = Arc::new(RejectedHost {
        error: LivenessJournalError {
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
            rejection: None,
        },
        calls: AtomicUsize::new(0),
        cancel: Some((supervisor.clone(), CancellationCause::UserRequested)),
    });
    let runtime = with_host(runtime, &journal, host.clone());
    let exit = runtime
        .execute(fixture.attempt.clone(), fixture.telemetry(), supervisor)
        .await;
    assert!(matches!(exit, AgentLoopExit::Cancelled { .. }), "{exit:?}");
    assert!(journal.inner.failures().is_empty());
    require_never_dispatched(&provider, &host, &journal);
}
