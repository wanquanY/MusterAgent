//! Shared, bounded worker process supervision for Cloud and Device runtimes.

#![forbid(unsafe_code)]

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use musteragent_activity::ActivityKind;
use musteragent_ledger::ExecutionReceipt;
use musteragent_loop::{
    AgentLoopExit, AgentLoopFailureStage, CancellationCause, DurableAgentLoop,
    DurableAttemptOutcome, DurableExecutionReceipt, DurableRunOutcome, InvocationSupervisor,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::{ErrorCode, InterruptionReason};
use musteragent_scheduler::{
    AttemptCancellationStore, AttemptControlError, AttemptControlStore, AttemptStopIntent,
    CoordinationForcedAbortRecovery, CoordinationForcedAbortRecoveryStore,
    CoordinationForcedAbortStore, ForcedAbortTermination, LeaseHeartbeat,
};
use musteragent_telemetry::{AttemptTelemetryContext, ErrorClass, RuntimeSpan, TelemetryOutcome};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

mod heartbeat;
use heartbeat::ConfirmedLease;

pub type AttemptFuture<'a> = Pin<Box<dyn Future<Output = AgentLoopExit> + Send + 'a>>;

/// One scheduler-claimed Attempt plus its immutable Activity execution class.
/// The class is reconstructed from the durable Run row, never supplied by an
/// executor, so Maintenance work cannot silently enter the interactive loop.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DispatchedAttempt {
    pub context: AttemptContext,
    pub activity_kind: ActivityKind,
}

/// Closed execution boundary used by the process supervisor.
pub trait AttemptExecutor: Send + Sync {
    fn execute(
        &self,
        attempt: DispatchedAttempt,
        telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AttemptFuture<'_>;
}

impl AttemptExecutor for DurableAgentLoop {
    fn execute(
        &self,
        attempt: DispatchedAttempt,
        telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AttemptFuture<'_> {
        Box::pin(DurableAgentLoop::execute(
            self,
            attempt.context,
            telemetry,
            supervisor,
        ))
    }
}

/// Closed worker router. Interactive, coordination, automation and delegation
/// Attempts retain the ordinary Agent Loop; Maintenance and Evaluation
/// Attempts are handed to separately constructed system-Agent executors with
/// no fallback to interactive behavior.
pub struct ActivityKindAttemptExecutor {
    regular: Arc<dyn AttemptExecutor>,
    maintenance: Arc<dyn AttemptExecutor>,
    evaluation: Arc<dyn AttemptExecutor>,
}

impl ActivityKindAttemptExecutor {
    #[must_use]
    pub fn new(
        regular: Arc<dyn AttemptExecutor>,
        maintenance: Arc<dyn AttemptExecutor>,
        evaluation: Arc<dyn AttemptExecutor>,
    ) -> Self {
        Self {
            regular,
            maintenance,
            evaluation,
        }
    }
}

impl AttemptExecutor for ActivityKindAttemptExecutor {
    fn execute(
        &self,
        attempt: DispatchedAttempt,
        telemetry: AttemptTelemetryContext,
        supervisor: InvocationSupervisor,
    ) -> AttemptFuture<'_> {
        match attempt.activity_kind {
            ActivityKind::Maintenance => self.maintenance.execute(attempt, telemetry, supervisor),
            ActivityKind::Evaluation => self.evaluation.execute(attempt, telemetry, supervisor),
            ActivityKind::Chat
            | ActivityKind::Coordination
            | ActivityKind::Automation
            | ActivityKind::Delegation => self.regular.execute(attempt, telemetry, supervisor),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkerRunnerPolicy {
    pub max_inflight_runs: NonZeroUsize,
    pub lease_duration: Duration,
    pub heartbeat_interval: Duration,
    pub shutdown_drain_timeout: Duration,
}

impl WorkerRunnerPolicy {
    pub fn validate(self) -> Result<Self, WorkerRunnerPolicyError> {
        if self.max_inflight_runs.get() > 10_000 {
            return Err(WorkerRunnerPolicyError::InvalidConcurrency);
        }
        if self.lease_duration < Duration::from_secs(1)
            || self.lease_duration > Duration::from_secs(5 * 60)
        {
            return Err(WorkerRunnerPolicyError::InvalidLeaseDuration);
        }
        if self.heartbeat_interval < Duration::from_millis(100)
            || self
                .heartbeat_interval
                .checked_mul(3)
                .is_none_or(|window| window > self.lease_duration)
        {
            return Err(WorkerRunnerPolicyError::InvalidHeartbeatInterval);
        }
        if self.shutdown_drain_timeout < Duration::from_millis(100)
            || self.shutdown_drain_timeout > Duration::from_secs(5 * 60)
        {
            return Err(WorkerRunnerPolicyError::InvalidShutdownDrainTimeout);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkerRunnerPolicyError {
    #[error("Worker concurrency exceeds the supported ceiling")]
    InvalidConcurrency,
    #[error("Worker lease duration must be within 1 second..=5 minutes")]
    InvalidLeaseDuration,
    #[error("heartbeat must run at least three times within each lease")]
    InvalidHeartbeatInterval,
    #[error("shutdown drain timeout must be within 100 milliseconds..=5 minutes")]
    InvalidShutdownDrainTimeout,
}

/// Closed observation emitted after the durable Agent Loop exits.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkerAttemptObservation {
    pub attempt_outcome: TelemetryOutcome,
    pub attempt_error: Option<ErrorCode>,
    /// Log-safe Agent Loop boundary that failed. Present only for failed
    /// exits, including failures whose terminal Journal commit also failed.
    pub failure_stage: Option<AgentLoopFailureStage>,
    pub failure_commit_error: Option<ErrorCode>,
    pub attempt_terminal: bool,
    pub run_terminal_outcome: Option<TelemetryOutcome>,
    pub interaction_outcome: Option<TelemetryOutcome>,
    pub retry_attempt: bool,
    pub retry_run: bool,
    pub duration: Duration,
}

/// Attempt-scoped observer state. The shared runner keeps this value alive for
/// the complete supervised future, so composition adapters can use `Drop` to
/// close gauges even when the bounded shutdown deadline aborts an Attempt.
pub type WorkerAttemptGuard = Box<dyn Send>;

/// Composition-owned observability adapter. The shared supervisor never
/// depends on OTLP, a Cloud process, or a Device process.
pub trait WorkerRunnerObserver: Send + Sync {
    fn dispatch_dequeued(&self) {}

    fn span(&self, _context: &AttemptTelemetryContext) -> RuntimeSpan {
        RuntimeSpan::none()
    }

    fn attempt_started(
        &self,
        _context: &AttemptTelemetryContext,
        _retry: bool,
    ) -> WorkerAttemptGuard {
        Box::new(())
    }

    fn attempt_finished(
        &self,
        _context: &AttemptTelemetryContext,
        _observation: WorkerAttemptObservation,
    ) {
    }
}

struct NoopWorkerRunnerObserver;

impl WorkerRunnerObserver for NoopWorkerRunnerObserver {}

/// Supervised, bounded worker execution runner shared by every Runtime home.
///
/// Scheduler stores own durable claims. This runner owns process lifetime and
/// never detaches an Attempt task. Lease uncertainty always cancels execution
/// before another provider or Tool side effect can be committed.
pub struct RuntimeWorkerRunner {
    policy: WorkerRunnerPolicy,
    controls: Arc<dyn AttemptControlStore>,
    cancellations: Arc<dyn AttemptCancellationStore>,
    coordination_aborts: Arc<dyn CoordinationForcedAbortStore>,
    executor: Arc<dyn AttemptExecutor>,
    observer: Arc<dyn WorkerRunnerObserver>,
}

struct RejectingCoordinationForcedAbortStore;

impl CoordinationForcedAbortStore for RejectingCoordinationForcedAbortStore {
    fn force_abort_coordination(
        &self,
        _fence: &musteragent_ledger::AttemptFence,
        _interrupt_id: musteragent_identity::InterruptRequestId,
        _termination: ForcedAbortTermination,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<(), AttemptControlError>> {
        Box::pin(async { Err(AttemptControlError::ContractViolation) })
    }
}

impl RuntimeWorkerRunner {
    pub fn new(
        policy: WorkerRunnerPolicy,
        controls: Arc<dyn AttemptControlStore>,
        cancellations: Arc<dyn AttemptCancellationStore>,
        executor: Arc<dyn AttemptExecutor>,
    ) -> Result<Self, WorkerRunnerPolicyError> {
        Ok(Self {
            policy: policy.validate()?,
            controls,
            cancellations,
            coordination_aborts: Arc::new(RejectingCoordinationForcedAbortStore),
            executor,
            observer: Arc::new(NoopWorkerRunnerObserver),
        })
    }

    /// Installs the Runtime-home persistence boundary that atomically fences
    /// and terminalizes a non-cooperative coordination interrupt. Device and
    /// Cloud compositions must supply their own authoritative implementation
    /// before they can advertise forced-abort support.
    #[must_use]
    pub fn with_coordination_forced_abort_store(
        mut self,
        store: Arc<dyn CoordinationForcedAbortStore>,
    ) -> Self {
        self.coordination_aborts = store;
        self
    }

    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn WorkerRunnerObserver>) -> Self {
        self.observer = observer;
        self
    }

    pub async fn run(
        &self,
        mut dispatch_rx: mpsc::Receiver<ExecutionReceipt>,
        cancellation: CancellationToken,
    ) -> Result<(), WorkerRunnerError> {
        let permits = Arc::new(Semaphore::new(self.policy.max_inflight_runs.get()));
        let attempts_cancel = cancellation.child_token();
        let mut attempts = JoinSet::new();
        let mut queue_open = true;

        let mut outcome = 'supervision: loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => break Ok(()),
                joined = attempts.join_next(), if !attempts.is_empty() => {
                    match joined {
                        Some(Ok(Ok(()))) => {},
                        Some(Ok(Err(error))) => break Err(error),
                        _ => break Err(WorkerRunnerError::AttemptTaskLost),
                    }
                }
                receipt = dispatch_rx.recv(), if queue_open => {
                    let Some(receipt) = receipt else {
                        queue_open = false;
                        if cancellation.is_cancelled() {
                            break Ok(());
                        }
                        if attempts.is_empty() {
                            break Err(WorkerRunnerError::DispatchQueueClosed);
                        }
                        continue;
                    };
                    self.observer.dispatch_dequeued();
                    let permit = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => break 'supervision Ok(()),
                        permit = Arc::clone(&permits).acquire_owned() => {
                            match permit {
                                Ok(permit) => permit,
                                Err(_) => break 'supervision Err(WorkerRunnerError::SupervisorClosed),
                            }
                        }
                    };
                    let executor = Arc::clone(&self.executor);
                    let controls = Arc::clone(&self.controls);
                    let cancellations = Arc::clone(&self.cancellations);
                    let coordination_aborts = Arc::clone(&self.coordination_aborts);
                    let observer = Arc::clone(&self.observer);
                    let child_cancel = attempts_cancel.child_token();
                    let policy = self.policy;
                    let telemetry = (*receipt.telemetry).clone();
                    let retry = receipt.fence.attempt_number.get() > 1;
                    let span = observer.span(&telemetry);
                    let observed_span = span.clone();
                    let future = async move {
                        let _permit = permit;
                        let _attempt_guard = observer.attempt_started(&telemetry, retry);
                        let started_at = Instant::now();
                        let exit = run_attempt(
                            executor,
                            controls,
                            cancellations,
                            coordination_aborts,
                            policy,
                            receipt,
                            child_cancel,
                        ).await;
                        let observation = observation(&exit, retry, started_at.elapsed());
                        if observation.attempt_outcome == TelemetryOutcome::Error {
                            tracing::error!(
                                tenant_id = %telemetry.tenant_id,
                                conversation_id = %telemetry.conversation_id,
                                run_id = %telemetry.run_id,
                                attempt_id = %telemetry.attempt_id,
                                attempt_number = telemetry.attempt_number.get(),
                                correlation_id = %telemetry.correlation_id,
                                failure.stage = observation
                                    .failure_stage
                                    .map(failure_stage_name)
                                    .unwrap_or("none"),
                                error_code = observation
                                    .attempt_error
                                    .map(ErrorCode::as_str)
                                    .unwrap_or("unknown"),
                                failure_commit_error = observation
                                    .failure_commit_error
                                    .map(ErrorCode::as_str),
                                retry_run = observation.retry_run,
                                attempt_terminal = observation.attempt_terminal,
                                duration_ms = u64::try_from(observation.duration.as_millis())
                                    .unwrap_or(u64::MAX),
                                "Agent Runtime Attempt failed"
                            );
                        }
                        observed_span.record_outcome(observation.attempt_outcome);
                        if let Some(error) = observation.attempt_error {
                            observed_span.record_error(ErrorClass::from_error_code(error));
                        }
                        observed_span.in_scope(|| {
                            observer.attempt_finished(&telemetry, observation);
                        });
                        Ok::<(), WorkerRunnerError>(())
                    };
                    attempts.spawn(span.instrument(future));
                }
            }
            if !queue_open && attempts.is_empty() {
                if cancellation.is_cancelled() {
                    break Ok(());
                }
                break Err(WorkerRunnerError::DispatchQueueClosed);
            }
        };

        // Failure is also process shutdown. Preserve its cause, but always
        // signal and join sibling Attempts before the host may close storage
        // or revoke keys. Dropping JoinSet only aborts; it does not join.
        attempts_cancel.cancel();
        let deadline = Instant::now() + self.policy.shutdown_drain_timeout;
        loop {
            match tokio::time::timeout_at(deadline, attempts.join_next()).await {
                Ok(Some(joined)) => {
                    let result = joined
                        .map_err(WorkerRunnerError::from)
                        .and_then(|result| result);
                    outcome = outcome.and(result);
                }
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!(
                        remaining_attempts = attempts.len(),
                        "Worker shutdown drain deadline elapsed; dropping tasks at durable save points"
                    );
                    attempts.abort_all();
                    while let Some(joined) = attempts.join_next().await {
                        match joined {
                            Ok(result) => outcome = outcome.and(result),
                            Err(error) if !error.is_cancelled() => {
                                outcome = outcome.and(Err(WorkerRunnerError::AttemptTaskLost));
                            }
                            Err(_) => {}
                        }
                    }
                    break;
                }
            }
        }
        outcome
    }
}

const fn failure_stage_name(stage: AgentLoopFailureStage) -> &'static str {
    match stage {
        AgentLoopFailureStage::LoadPolicy => "load_policy",
        AgentLoopFailureStage::PolicyGate => "policy_gate",
        AgentLoopFailureStage::PolicyHook => "policy_hook",
        AgentLoopFailureStage::LoadCheckpoint => "load_checkpoint",
        AgentLoopFailureStage::ValidateCheckpoint => "validate_checkpoint",
        AgentLoopFailureStage::RecoverCheckpoint => "recover_checkpoint",
        AgentLoopFailureStage::ContextPlanning => "context_planning",
        AgentLoopFailureStage::BeginModel => "begin_model",
        AgentLoopFailureStage::ModelExecution => "model_execution",
        AgentLoopFailureStage::FallbackPolicy => "fallback_policy",
        AgentLoopFailureStage::FallbackCommit => "fallback_commit",
        AgentLoopFailureStage::FinishModel => "finish_model",
        AgentLoopFailureStage::ToolExecution => "tool_execution",
        AgentLoopFailureStage::CommitToolResult => "commit_tool_result",
        AgentLoopFailureStage::CompleteRun => "complete_run",
        AgentLoopFailureStage::CancellationCommit => "cancellation_commit",
    }
}

/// Repairs a forced coordination interrupt when its owning Worker disappeared
/// after receiving the request but before committing the terminal outcome.
///
/// Eligibility remains entirely database-owned. This worker only schedules
/// bounded repair passes and therefore cannot use its local clock to shorten a
/// Run lease or a forced-abort deadline.
pub struct CoordinationForcedAbortRecoveryWorker {
    store: Arc<dyn CoordinationForcedAbortRecoveryStore>,
    idle_poll_interval: Duration,
}

impl CoordinationForcedAbortRecoveryWorker {
    pub fn new(
        store: Arc<dyn CoordinationForcedAbortRecoveryStore>,
        idle_poll_interval: Duration,
    ) -> Result<Self, CoordinationForcedAbortRecoveryWorkerError> {
        if !(Duration::from_millis(100)..=Duration::from_secs(60)).contains(&idle_poll_interval) {
            return Err(CoordinationForcedAbortRecoveryWorkerError::InvalidPollInterval);
        }
        Ok(Self {
            store,
            idle_poll_interval,
        })
    }

    pub async fn run(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), CoordinationForcedAbortRecoveryWorkerError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            match self.store.recover_one_due_coordination_abort().await {
                Ok(CoordinationForcedAbortRecovery::Recovered {
                    interrupt_id,
                    run_id,
                    attempt_id,
                }) => {
                    tracing::warn!(
                        %interrupt_id,
                        %run_id,
                        %attempt_id,
                        "recovered a coordination forced abort after Worker lease expiry"
                    );
                    continue;
                }
                Ok(CoordinationForcedAbortRecovery::Idle) | Err(AttemptControlError::LeaseLost) => {
                }
                Err(AttemptControlError::DatabaseUnavailable) => {
                    tracing::warn!(
                        "coordination forced-abort recovery store is temporarily unavailable"
                    );
                }
                Err(AttemptControlError::ContractViolation) => {
                    return Err(CoordinationForcedAbortRecoveryWorkerError::ContractViolation);
                }
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.idle_poll_interval) => {}
            }
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum CoordinationForcedAbortRecoveryWorkerError {
    #[error("coordination forced-abort recovery poll interval is invalid")]
    InvalidPollInterval,
    #[error("coordination forced-abort recovery store violated its contract")]
    ContractViolation,
}

fn observation(
    exit: &AgentLoopExit,
    retry_attempt: bool,
    duration: Duration,
) -> WorkerAttemptObservation {
    let (attempt_outcome, attempt_error, attempt_terminal) = attempt_observation(exit);
    let (failure_stage, failure_commit_error) = match exit {
        AgentLoopExit::Failed {
            stage,
            failure_commit_error,
            ..
        } => (Some(*stage), *failure_commit_error),
        _ => (None, None),
    };
    WorkerAttemptObservation {
        attempt_outcome,
        attempt_error,
        failure_stage,
        failure_commit_error,
        attempt_terminal,
        run_terminal_outcome: durable_receipt(exit)
            .and_then(|receipt| run_outcome(receipt.run_outcome())),
        interaction_outcome: match exit {
            AgentLoopExit::Waiting {
                receipt: Some(receipt),
                ..
            } => interaction_outcome(receipt.attempt_outcome(), receipt.run_outcome()),
            _ => None,
        },
        retry_attempt,
        retry_run: durable_receipt(exit)
            .is_some_and(|receipt| receipt.run_outcome() == DurableRunOutcome::RetryBackoff),
        duration,
    }
}

fn durable_receipt(exit: &AgentLoopExit) -> Option<&DurableExecutionReceipt> {
    match exit {
        AgentLoopExit::Completed { receipt }
        | AgentLoopExit::Cancelled { receipt }
        | AgentLoopExit::CoordinationInterrupted { receipt }
        | AgentLoopExit::CoordinationForcedAbort { receipt }
        | AgentLoopExit::DelegationWaiting { receipt, .. }
        | AgentLoopExit::DelegationGroupWaiting { receipt, .. }
        | AgentLoopExit::DelegationCompletionWaiting { receipt }
        | AgentLoopExit::NeedsAction { receipt, .. }
        | AgentLoopExit::ReconciliationWaiting { receipt, .. }
        | AgentLoopExit::ExternalOutcomeUnknown { receipt, .. } => Some(receipt),
        AgentLoopExit::Waiting { receipt, .. } | AgentLoopExit::Failed { receipt, .. } => {
            receipt.as_ref()
        }
        AgentLoopExit::AlreadyTerminal | AgentLoopExit::Interrupted { .. } => None,
    }
}

fn attempt_observation(exit: &AgentLoopExit) -> (TelemetryOutcome, Option<ErrorCode>, bool) {
    if let Some(receipt) = durable_receipt(exit) {
        let error = match exit {
            AgentLoopExit::Failed { code, .. } => Some(*code),
            _ => None,
        };
        return (attempt_outcome(receipt.attempt_outcome()), error, true);
    }
    let outcome = match exit {
        AgentLoopExit::Waiting { .. }
        | AgentLoopExit::DelegationWaiting { .. }
        | AgentLoopExit::DelegationGroupWaiting { .. }
        | AgentLoopExit::DelegationCompletionWaiting { .. }
        | AgentLoopExit::NeedsAction { .. }
        | AgentLoopExit::ReconciliationWaiting { .. } => TelemetryOutcome::Waiting,
        AgentLoopExit::AlreadyTerminal => TelemetryOutcome::Duplicate,
        AgentLoopExit::Failed { .. } => TelemetryOutcome::Error,
        AgentLoopExit::Interrupted { .. } => TelemetryOutcome::Interrupted,
        AgentLoopExit::Completed { .. }
        | AgentLoopExit::Cancelled { .. }
        | AgentLoopExit::CoordinationInterrupted { .. }
        | AgentLoopExit::CoordinationForcedAbort { .. }
        | AgentLoopExit::ExternalOutcomeUnknown { .. } => {
            unreachable!("receipt-bearing exit handled above")
        }
    };
    let error = match exit {
        AgentLoopExit::Failed { code, .. } => Some(*code),
        _ => None,
    };
    (outcome, error, false)
}

const fn attempt_outcome(outcome: DurableAttemptOutcome) -> TelemetryOutcome {
    match outcome {
        DurableAttemptOutcome::Succeeded => TelemetryOutcome::Success,
        DurableAttemptOutcome::Failed => TelemetryOutcome::Error,
        DurableAttemptOutcome::Cancelled => TelemetryOutcome::Cancelled,
        DurableAttemptOutcome::Waiting => TelemetryOutcome::Waiting,
        DurableAttemptOutcome::Interrupted => TelemetryOutcome::Interrupted,
        DurableAttemptOutcome::ExternalOutcomeUnknown => TelemetryOutcome::ExternalOutcomeUnknown,
    }
}

const fn run_outcome(outcome: DurableRunOutcome) -> Option<TelemetryOutcome> {
    match outcome {
        DurableRunOutcome::Completed => Some(TelemetryOutcome::Success),
        DurableRunOutcome::Failed => Some(TelemetryOutcome::Error),
        DurableRunOutcome::Cancelled => Some(TelemetryOutcome::Cancelled),
        DurableRunOutcome::Interrupted => Some(TelemetryOutcome::Interrupted),
        DurableRunOutcome::ExternalOutcomeUnknown => Some(TelemetryOutcome::ExternalOutcomeUnknown),
        DurableRunOutcome::Waiting | DurableRunOutcome::RetryBackoff => None,
    }
}

const fn interaction_outcome(
    attempt: DurableAttemptOutcome,
    run: DurableRunOutcome,
) -> Option<TelemetryOutcome> {
    if matches!(attempt, DurableAttemptOutcome::Waiting)
        && matches!(run, DurableRunOutcome::Waiting)
    {
        Some(TelemetryOutcome::Waiting)
    } else {
        None
    }
}

async fn run_attempt(
    executor: Arc<dyn AttemptExecutor>,
    controls: Arc<dyn AttemptControlStore>,
    cancellations: Arc<dyn AttemptCancellationStore>,
    coordination_aborts: Arc<dyn CoordinationForcedAbortStore>,
    policy: WorkerRunnerPolicy,
    receipt: ExecutionReceipt,
    process_cancellation: CancellationToken,
) -> AgentLoopExit {
    let mut attempt = attempt_context(&receipt);
    let telemetry = (*receipt.telemetry).clone();
    let fence = receipt.fence;
    let supervisor = InvocationSupervisor::new();
    let initial_heartbeat_started = Instant::now();
    let initial_heartbeat = tokio::select! {
        biased;
        () = process_cancellation.cancelled() => {
            return surrender_after_save_point(&controls, &fence).await;
        }
        heartbeat = controls.heartbeat(&fence, policy.lease_duration) => heartbeat,
    };
    let (initial_stop_intent, mut confirmed_lease) = match initial_heartbeat {
        Ok(heartbeat) => {
            let Ok(lease) = ConfirmedLease::observe(
                &heartbeat,
                initial_heartbeat_started,
                policy.heartbeat_interval,
            ) else {
                return AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                };
            };
            let stop_intent = heartbeat.stop_intent;
            attempt.lease_expires_at = heartbeat.lease_expires_at;
            if let Some(intent) = stop_intent {
                supervisor.cancel_with(stop_intent_cause(intent));
            }
            (stop_intent, lease)
        }
        Err(
            AttemptControlError::LeaseLost
            | AttemptControlError::DatabaseUnavailable
            | AttemptControlError::ContractViolation,
        ) => {
            return AgentLoopExit::Interrupted {
                cause: CancellationCause::LeaseLost,
            };
        }
    };
    if matches!(
        initial_stop_intent,
        Some(AttemptStopIntent::UserCancellation)
    ) {
        return finalize_initial_user_cancellation(&cancellations, &attempt).await;
    }
    let mut execution = executor.execute(
        DispatchedAttempt {
            context: attempt.clone(),
            activity_kind: receipt.activity_kind,
        },
        telemetry,
        supervisor.clone(),
    );
    let mut heartbeat = tokio::time::interval(policy.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;

    if let Some(intent @ AttemptStopIntent::CoordinationInterrupt { .. }) = initial_stop_intent {
        return finish_coordination_interrupt(
            &mut execution,
            &controls,
            &coordination_aborts,
            &fence,
            policy,
            intent,
            confirmed_lease,
        )
        .await;
    }

    loop {
        tokio::select! {
            biased;
            () = process_cancellation.cancelled() => {
                supervisor.cancel_with(CancellationCause::SystemShutdown);
                let exit = drain_cancelled_execution(
                    &mut execution,
                    policy.shutdown_drain_timeout,
                    CancellationCause::SystemShutdown,
                ).await;
                return if matches!(
                    exit,
                    AgentLoopExit::Interrupted {
                        cause: CancellationCause::SystemShutdown,
                    }
                ) {
                    surrender_after_save_point(&controls, &fence).await
                } else {
                    exit
                };
            }
            exit = &mut execution => {
                if matches!(
                    exit,
                    AgentLoopExit::Interrupted {
                        cause: CancellationCause::LeaseLost,
                    }
                ) {
                    tracing::warn!(
                        run_id = %fence.run_id,
                        attempt_id = %fence.attempt_id,
                        lease_expires_at_ms = confirmed_lease.expires_at().unix_millis(),
                        "Agent Loop reported a stale Attempt fence before the heartbeat supervisor observed lease loss"
                    );
                }
                return exit;
            },
            heartbeat_result = confirmed_lease.renew_at_interval(
                &mut heartbeat, controls.as_ref(), &fence, policy.lease_duration
            ) => {
                match heartbeat_result {
                    Ok(LeaseHeartbeat {
                        stop_intent: Some(intent),
                        ..
                    }) => {
                        let cause = stop_intent_cause(intent);
                        supervisor.cancel_with(cause);
                        return match intent {
                            AttemptStopIntent::UserCancellation => {
                                drain_cancelled_execution(
                                    &mut execution,
                                    policy.shutdown_drain_timeout,
                                    cause,
                                ).await
                            }
                            intent @ AttemptStopIntent::CoordinationInterrupt { .. } => {
                                finish_coordination_interrupt(
                                    &mut execution,
                                    &controls,
                                    &coordination_aborts,
                                    &fence,
                                    policy,
                                    intent,
                                    confirmed_lease,
                                ).await
                            }
                        };
                    }
                    Ok(LeaseHeartbeat {
                        stop_intent: None,
                        ..
                    }) => {}
                    Err(error @ (
                        AttemptControlError::LeaseLost
                        | AttemptControlError::DatabaseUnavailable
                        | AttemptControlError::ContractViolation
                    )) => {
                        tracing::warn!(run_id=%fence.run_id, attempt_id=%fence.attempt_id,
                            ?error, confirmed_lease_expires_at_ms=confirmed_lease.expires_at().unix_millis(),
                            "Worker stopped after heartbeat authority could not be confirmed");
                        supervisor.cancel_with(CancellationCause::LeaseLost);
                        return drain_cancelled_execution(
                            &mut execution,
                            policy.shutdown_drain_timeout,
                            CancellationCause::LeaseLost,
                        ).await;
                    }
                }
            }
        }
    }
}

async fn finalize_initial_user_cancellation(
    cancellations: &Arc<dyn AttemptCancellationStore>,
    attempt: &AttemptContext,
) -> AgentLoopExit {
    match cancellations.acknowledge_user_cancellation(attempt).await {
        Ok(()) => AgentLoopExit::Cancelled {
            receipt: DurableExecutionReceipt::worker_cancelled(attempt),
        },
        Err(AttemptControlError::LeaseLost) => AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        },
        Err(AttemptControlError::DatabaseUnavailable) => AgentLoopExit::Failed {
            stage: AgentLoopFailureStage::CancellationCommit,
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
            failure_commit_error: Some(ErrorCode::DependencyUnavailable),
            receipt: None,
        },
        Err(AttemptControlError::ContractViolation) => AgentLoopExit::Failed {
            stage: AgentLoopFailureStage::CancellationCommit,
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
            failure_commit_error: Some(ErrorCode::InternalInvariantViolated),
            receipt: None,
        },
    }
}

const fn stop_intent_cause(intent: AttemptStopIntent) -> CancellationCause {
    match intent {
        AttemptStopIntent::UserCancellation => CancellationCause::UserRequested,
        AttemptStopIntent::CoordinationInterrupt { .. } => CancellationCause::CoordinationRequested,
    }
}

async fn drain_cancelled_execution(
    execution: &mut AttemptFuture<'_>,
    drain_timeout: Duration,
    cause: CancellationCause,
) -> AgentLoopExit {
    match tokio::time::timeout(drain_timeout, execution.as_mut()).await {
        Ok(exit) => exit,
        Err(_) => {
            tracing::warn!(
                ?cause,
                drain_timeout_ms = drain_timeout.as_millis(),
                "Attempt ignored cancellation drain deadline; dropping the fenced execution future"
            );
            AgentLoopExit::Interrupted { cause }
        }
    }
}

enum CoordinationDrainOutcome {
    Exited(AgentLoopExit),
    ForcedAbort {
        interrupt_id: musteragent_identity::InterruptRequestId,
    },
}

async fn finish_coordination_interrupt(
    execution: &mut AttemptFuture<'_>,
    controls: &Arc<dyn AttemptControlStore>,
    coordination_aborts: &Arc<dyn CoordinationForcedAbortStore>,
    fence: &musteragent_ledger::AttemptFence,
    policy: WorkerRunnerPolicy,
    intent: AttemptStopIntent,
    confirmed_lease: ConfirmedLease,
) -> AgentLoopExit {
    let outcome =
        drain_coordination_execution(execution, controls, fence, policy, intent, confirmed_lease)
            .await;
    let CoordinationDrainOutcome::ForcedAbort { interrupt_id } = outcome else {
        let CoordinationDrainOutcome::Exited(exit) = outcome else {
            unreachable!("closed coordination drain outcome")
        };
        return exit;
    };

    // Stop polling and drop the complete supervised execution graph before
    // the durable forced-abort commit fences the Attempt. Dropping this future
    // is not proof that Provider or child-process effects did not occur; the
    // store records those effects as reconciliation obligations.
    let replacement = Box::pin(async {
        AgentLoopExit::Interrupted {
            cause: CancellationCause::CoordinationRequested,
        }
    });
    let execution = std::mem::replace(execution, replacement);
    drop(execution);

    match coordination_aborts
        .force_abort_coordination(
            fence,
            interrupt_id,
            ForcedAbortTermination::ExecutionFutureDropped,
        )
        .await
    {
        Ok(()) => AgentLoopExit::CoordinationForcedAbort {
            receipt: DurableExecutionReceipt::coordination_forced_abort(
                &attempt_context_from_fence(fence),
            ),
        },
        Err(AttemptControlError::LeaseLost) => AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        },
        Err(AttemptControlError::DatabaseUnavailable | AttemptControlError::ContractViolation) => {
            tracing::error!(
                run_id = %fence.run_id,
                attempt_id = %fence.attempt_id,
                %interrupt_id,
                "forced-abort persistence failed after the execution future was dropped"
            );
            AgentLoopExit::Interrupted {
                cause: CancellationCause::CoordinationRequested,
            }
        }
    }
}

async fn drain_coordination_execution(
    execution: &mut AttemptFuture<'_>,
    controls: &Arc<dyn AttemptControlStore>,
    fence: &musteragent_ledger::AttemptFence,
    policy: WorkerRunnerPolicy,
    intent: AttemptStopIntent,
    mut confirmed_lease: ConfirmedLease,
) -> CoordinationDrainOutcome {
    let AttemptStopIntent::CoordinationInterrupt {
        interrupt_id,
        cooperative_deadline_at,
        forced_abort_deadline_at,
    } = intent
    else {
        return CoordinationDrainOutcome::Exited(AgentLoopExit::Interrupted {
            cause: CancellationCause::CoordinationRequested,
        });
    };
    let mut heartbeat = tokio::time::interval(policy.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut remaining = policy.heartbeat_interval;

    loop {
        let deadline = tokio::time::sleep(remaining);
        tokio::pin!(deadline);
        tokio::select! {
            biased;
            exit = execution.as_mut() => return CoordinationDrainOutcome::Exited(exit),
            heartbeat_result = confirmed_lease.renew_at_interval(
                &mut heartbeat, controls.as_ref(), fence, policy.lease_duration
            ) => {
                let Ok(LeaseHeartbeat {
                    database_now,
                    stop_intent: Some(current),
                    ..
                }) = heartbeat_result else {
                    return CoordinationDrainOutcome::Exited(AgentLoopExit::Interrupted {
                        cause: CancellationCause::LeaseLost,
                    });
                };
                if current != intent {
                    return CoordinationDrainOutcome::Exited(AgentLoopExit::Interrupted {
                        cause: CancellationCause::LeaseLost,
                    });
                }
                if database_now >= forced_abort_deadline_at {
                    return CoordinationDrainOutcome::ForcedAbort { interrupt_id };
                }
                if database_now < cooperative_deadline_at {
                    tracing::debug!(
                        run_id = %fence.run_id,
                        attempt_id = %fence.attempt_id,
                        %interrupt_id,
                        "coordination interrupt is draining before its cooperative deadline"
                    );
                }
                remaining = duration_until(database_now, forced_abort_deadline_at);
            }
            () = &mut deadline => {
                // The local monotonic timer only schedules a database-time
                // recheck. It never grants forced-abort authority by itself.
                match confirmed_lease.renew(controls.as_ref(), fence, policy.lease_duration).await {
                    Ok(LeaseHeartbeat {
                        database_now,
                        stop_intent: Some(current),
                        ..
                    }) if current == intent => {
                        if database_now >= forced_abort_deadline_at {
                            return CoordinationDrainOutcome::ForcedAbort { interrupt_id };
                        }
                        remaining = duration_until(database_now, forced_abort_deadline_at);
                    }
                    _ => {
                        return CoordinationDrainOutcome::Exited(AgentLoopExit::Interrupted {
                            cause: CancellationCause::LeaseLost,
                        });
                    }
                }
            }
        }
    }
}

fn duration_until(
    now: musteragent_identity::UtcTimestamp,
    deadline: musteragent_identity::UtcTimestamp,
) -> Duration {
    let millis = deadline.unix_millis().saturating_sub(now.unix_millis());
    Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

fn attempt_context_from_fence(fence: &musteragent_ledger::AttemptFence) -> AttemptContext {
    AttemptContext {
        attempt_id: fence.attempt_id,
        run_id: fence.run_id,
        attempt_number: fence.attempt_number,
        worker_id: fence.worker_id.clone(),
        lease_id: fence.lease_id,
        fencing_token: fence.fencing_token,
        lease_expires_at: musteragent_identity::UtcTimestamp::from_unix_millis(i64::MAX),
    }
}

async fn surrender_after_save_point(
    controls: &Arc<dyn AttemptControlStore>,
    fence: &musteragent_ledger::AttemptFence,
) -> AgentLoopExit {
    match controls
        .surrender(fence, InterruptionReason::SystemShutdown)
        .await
    {
        Ok(()) => AgentLoopExit::Interrupted {
            cause: CancellationCause::SystemShutdown,
        },
        Err(AttemptControlError::LeaseLost) => AgentLoopExit::Interrupted {
            cause: CancellationCause::LeaseLost,
        },
        Err(AttemptControlError::DatabaseUnavailable | AttemptControlError::ContractViolation) => {
            tracing::warn!(
                run_id = %fence.run_id,
                attempt_id = %fence.attempt_id,
                "could not surrender shutdown Attempt; lease expiry remains recovery authority"
            );
            AgentLoopExit::Interrupted {
                cause: CancellationCause::SystemShutdown,
            }
        }
    }
}

fn attempt_context(receipt: &ExecutionReceipt) -> AttemptContext {
    AttemptContext {
        attempt_id: receipt.fence.attempt_id,
        run_id: receipt.fence.run_id,
        attempt_number: receipt.fence.attempt_number,
        worker_id: receipt.fence.worker_id.clone(),
        lease_id: receipt.fence.lease_id,
        fencing_token: receipt.fence.fencing_token,
        lease_expires_at: receipt.lease_expires_at,
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkerRunnerError {
    #[error("Scheduler dispatch queue closed before process shutdown")]
    DispatchQueueClosed,
    #[error("Worker task supervisor closed unexpectedly")]
    SupervisorClosed,
    #[error("an Attempt task panicked or was cancelled outside supervision")]
    AttemptTaskLost,
}

impl From<tokio::task::JoinError> for WorkerRunnerError {
    fn from(_value: tokio::task::JoinError) -> Self {
        Self::AttemptTaskLost
    }
}

#[cfg(test)]
mod tests {
    mod heartbeat_regression;
    mod shutdown_regression;
    use std::future::pending;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use musteragent_identity::{
        ActivityId, AttemptId, AttemptNumber, ConversationId, ConversationSeq, CorrelationId,
        FencingToken, HomeEpoch, InterruptRequestId, LeaseId, RunId, TenantId, UtcTimestamp,
    };
    use musteragent_ledger::{AppendDisposition, AppendReceipt, AttemptFence};
    use musteragent_run::WorkerId;
    use musteragent_runtime_contract::{ApplicationFuture, TraceContext, TraceParent};

    use super::*;

    struct RecordingExecutor {
        causes: Arc<Mutex<Vec<CancellationCause>>>,
    }

    impl AttemptExecutor for RecordingExecutor {
        fn execute(
            &self,
            _attempt: DispatchedAttempt,
            _telemetry: AttemptTelemetryContext,
            supervisor: InvocationSupervisor,
        ) -> AttemptFuture<'_> {
            let causes = Arc::clone(&self.causes);
            Box::pin(async move {
                let cancelled = supervisor
                    .run(CancellationCause::SystemShutdown, pending::<()>())
                    .await
                    .expect_err("fixture is cancelled");
                causes.lock().expect("causes").push(cancelled.cause);
                AgentLoopExit::Interrupted {
                    cause: cancelled.cause,
                }
            })
        }
    }

    struct NonCooperativeExecutor;

    impl AttemptExecutor for NonCooperativeExecutor {
        fn execute(
            &self,
            _attempt: DispatchedAttempt,
            _telemetry: AttemptTelemetryContext,
            _supervisor: InvocationSupervisor,
        ) -> AttemptFuture<'_> {
            Box::pin(pending())
        }
    }

    struct RouteRecordingExecutor {
        calls: Arc<Mutex<Vec<ActivityKind>>>,
    }

    impl AttemptExecutor for RouteRecordingExecutor {
        fn execute(
            &self,
            attempt: DispatchedAttempt,
            _telemetry: AttemptTelemetryContext,
            _supervisor: InvocationSupervisor,
        ) -> AttemptFuture<'_> {
            self.calls
                .lock()
                .expect("route calls")
                .push(attempt.activity_kind);
            Box::pin(async { AgentLoopExit::AlreadyTerminal })
        }
    }

    struct FixedControls {
        stop_intent: Option<AttemptStopIntent>,
    }

    #[derive(Default)]
    struct LeaseLosingControls {
        heartbeats: AtomicUsize,
    }

    struct ForcedAbortControls {
        heartbeats: AtomicUsize,
        intent: AttemptStopIntent,
    }

    #[derive(Default)]
    struct RecordingForcedAbortStore {
        calls: AtomicUsize,
    }

    #[derive(Default)]
    struct RecordingCancellationStore {
        calls: AtomicUsize,
    }

    impl AttemptCancellationStore for RecordingCancellationStore {
        fn acknowledge_user_cancellation(
            &self,
            _attempt: &AttemptContext,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    struct OneShotForcedAbortRecoveryStore {
        calls: AtomicUsize,
        interrupt_id: InterruptRequestId,
        run_id: RunId,
        attempt_id: AttemptId,
    }

    impl CoordinationForcedAbortRecoveryStore for OneShotForcedAbortRecoveryStore {
        fn recover_one_due_coordination_abort(
            &self,
        ) -> ApplicationFuture<'_, Result<CoordinationForcedAbortRecovery, AttemptControlError>>
        {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let recovered = CoordinationForcedAbortRecovery::Recovered {
                interrupt_id: self.interrupt_id,
                run_id: self.run_id,
                attempt_id: self.attempt_id,
            };
            Box::pin(async move {
                Ok(if call == 0 {
                    recovered
                } else {
                    CoordinationForcedAbortRecovery::Idle
                })
            })
        }
    }

    impl AttemptControlStore for FixedControls {
        fn heartbeat(
            &self,
            _fence: &AttemptFence,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
            let stop_intent = self.stop_intent;
            Box::pin(async move {
                Ok(LeaseHeartbeat {
                    database_now: UtcTimestamp::from_unix_millis(1_000),
                    lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
                    stop_intent,
                })
            })
        }

        fn surrender(
            &self,
            _fence: &AttemptFence,
            _reason: InterruptionReason,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl AttemptControlStore for LeaseLosingControls {
        fn heartbeat(
            &self,
            _fence: &AttemptFence,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
            let heartbeat = self.heartbeats.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if heartbeat == 0 {
                    Ok(LeaseHeartbeat {
                        database_now: UtcTimestamp::from_unix_millis(1_000),
                        lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
                        stop_intent: None,
                    })
                } else {
                    Err(AttemptControlError::LeaseLost)
                }
            })
        }

        fn surrender(
            &self,
            _fence: &AttemptFence,
            _reason: InterruptionReason,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl AttemptControlStore for ForcedAbortControls {
        fn heartbeat(
            &self,
            _fence: &AttemptFence,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
            let call = self.heartbeats.fetch_add(1, Ordering::SeqCst);
            let intent = self.intent;
            Box::pin(async move {
                Ok(LeaseHeartbeat {
                    database_now: UtcTimestamp::from_unix_millis(if call == 0 {
                        1_000
                    } else {
                        3_000
                    }),
                    lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
                    stop_intent: Some(intent),
                })
            })
        }

        fn surrender(
            &self,
            _fence: &AttemptFence,
            _reason: InterruptionReason,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl CoordinationForcedAbortStore for RecordingForcedAbortStore {
        fn force_abort_coordination(
            &self,
            _fence: &AttemptFence,
            _interrupt_id: InterruptRequestId,
            termination: ForcedAbortTermination,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            assert_eq!(termination, ForcedAbortTermination::ExecutionFutureDropped);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn initial_user_cancellation_is_committed_without_constructing_execution() {
        let causes = Arc::new(Mutex::new(Vec::new()));
        let cancellations = Arc::new(RecordingCancellationStore::default());
        let runner = RuntimeWorkerRunner::new(
            policy(),
            Arc::new(FixedControls {
                stop_intent: Some(AttemptStopIntent::UserCancellation),
            }),
            cancellations.clone(),
            Arc::new(RecordingExecutor {
                causes: Arc::clone(&causes),
            }),
        )
        .expect("runner");
        let (tx, rx) = mpsc::channel(1);
        tx.send(receipt()).await.expect("dispatch");
        drop(tx);
        assert_eq!(
            runner.run(rx, CancellationToken::new()).await,
            Err(WorkerRunnerError::DispatchQueueClosed)
        );
        assert!(causes.lock().expect("causes").is_empty());
        assert_eq!(cancellations.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coordination_interrupt_is_distinct_from_user_cancellation() {
        let causes = Arc::new(Mutex::new(Vec::new()));
        let runner = RuntimeWorkerRunner::new(
            policy(),
            Arc::new(FixedControls {
                stop_intent: Some(AttemptStopIntent::CoordinationInterrupt {
                    interrupt_id: InterruptRequestId::generate(),
                    cooperative_deadline_at: UtcTimestamp::from_unix_millis(10_000),
                    forced_abort_deadline_at: UtcTimestamp::from_unix_millis(20_000),
                }),
            }),
            Arc::new(RecordingCancellationStore::default()),
            Arc::new(RecordingExecutor {
                causes: Arc::clone(&causes),
            }),
        )
        .expect("runner");
        let (tx, rx) = mpsc::channel(1);
        tx.send(receipt()).await.expect("dispatch");
        drop(tx);
        assert_eq!(
            runner.run(rx, CancellationToken::new()).await,
            Err(WorkerRunnerError::DispatchQueueClosed)
        );
        assert_eq!(
            *causes.lock().expect("causes"),
            vec![CancellationCause::CoordinationRequested]
        );
    }

    #[tokio::test]
    async fn unexpected_dispatch_sender_drop_remains_fatal_without_cancellation() {
        let runner = RuntimeWorkerRunner::new(
            policy(),
            Arc::new(FixedControls { stop_intent: None }),
            Arc::new(RecordingCancellationStore::default()),
            Arc::new(NonCooperativeExecutor),
        )
        .expect("runner");
        let (tx, rx) = mpsc::channel(1);
        drop(tx);

        assert_eq!(
            runner.run(rx, CancellationToken::new()).await,
            Err(WorkerRunnerError::DispatchQueueClosed)
        );
    }

    #[tokio::test]
    async fn cancellation_then_concurrent_dispatch_sender_release_is_clean() {
        for _ in 0..64 {
            let runner = Arc::new(
                RuntimeWorkerRunner::new(
                    policy(),
                    Arc::new(FixedControls { stop_intent: None }),
                    Arc::new(RecordingCancellationStore::default()),
                    Arc::new(NonCooperativeExecutor),
                )
                .expect("runner"),
            );
            let (tx, rx) = mpsc::channel(1);
            let cancellation = CancellationToken::new();
            let runner_cancellation = cancellation.clone();
            let task = tokio::spawn(async move { runner.run(rx, runner_cancellation).await });

            tokio::task::yield_now().await;
            cancellation.cancel();
            drop(tx);

            assert_eq!(task.await.expect("join runner"), Ok(()));
        }
    }

    #[test]
    fn every_non_receipt_exit_has_an_explicit_nonterminal_observation() {
        let cases = [
            (
                AgentLoopExit::Waiting {
                    interaction_id: musteragent_identity::InteractionId::generate(),
                    receipt: None,
                },
                (TelemetryOutcome::Waiting, None, false),
            ),
            (
                AgentLoopExit::AlreadyTerminal,
                (TelemetryOutcome::Duplicate, None, false),
            ),
            (
                AgentLoopExit::Failed {
                    stage: musteragent_loop::AgentLoopFailureStage::ModelExecution,
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                    failure_commit_error: None,
                    receipt: None,
                },
                (
                    TelemetryOutcome::Error,
                    Some(ErrorCode::DependencyUnavailable),
                    false,
                ),
            ),
            (
                AgentLoopExit::Interrupted {
                    cause: CancellationCause::LeaseLost,
                },
                (TelemetryOutcome::Interrupted, None, false),
            ),
        ];
        for (exit, expected) in cases {
            assert_eq!(attempt_observation(&exit), expected);
        }
    }

    #[test]
    fn durable_attempt_run_and_interaction_outcomes_have_closed_mappings() {
        for (outcome, expected) in [
            (DurableAttemptOutcome::Succeeded, TelemetryOutcome::Success),
            (DurableAttemptOutcome::Failed, TelemetryOutcome::Error),
            (
                DurableAttemptOutcome::Cancelled,
                TelemetryOutcome::Cancelled,
            ),
            (DurableAttemptOutcome::Waiting, TelemetryOutcome::Waiting),
            (
                DurableAttemptOutcome::ExternalOutcomeUnknown,
                TelemetryOutcome::ExternalOutcomeUnknown,
            ),
        ] {
            assert_eq!(attempt_outcome(outcome), expected);
        }
        for (outcome, expected) in [
            (
                DurableRunOutcome::Completed,
                Some(TelemetryOutcome::Success),
            ),
            (DurableRunOutcome::Failed, Some(TelemetryOutcome::Error)),
            (
                DurableRunOutcome::Cancelled,
                Some(TelemetryOutcome::Cancelled),
            ),
            (DurableRunOutcome::Waiting, None),
            (DurableRunOutcome::RetryBackoff, None),
            (
                DurableRunOutcome::ExternalOutcomeUnknown,
                Some(TelemetryOutcome::ExternalOutcomeUnknown),
            ),
        ] {
            assert_eq!(run_outcome(outcome), expected);
        }
        for attempt in [
            DurableAttemptOutcome::Succeeded,
            DurableAttemptOutcome::Failed,
            DurableAttemptOutcome::Cancelled,
            DurableAttemptOutcome::Waiting,
            DurableAttemptOutcome::ExternalOutcomeUnknown,
        ] {
            for run in [
                DurableRunOutcome::Completed,
                DurableRunOutcome::Failed,
                DurableRunOutcome::Cancelled,
                DurableRunOutcome::Waiting,
                DurableRunOutcome::RetryBackoff,
                DurableRunOutcome::ExternalOutcomeUnknown,
            ] {
                assert_eq!(
                    interaction_outcome(attempt, run),
                    (attempt == DurableAttemptOutcome::Waiting
                        && run == DurableRunOutcome::Waiting)
                        .then_some(TelemetryOutcome::Waiting)
                );
            }
        }
    }

    #[tokio::test]
    async fn process_shutdown_interrupts_without_user_cancellation() {
        let causes = Arc::new(Mutex::new(Vec::new()));
        let runner = Arc::new(
            RuntimeWorkerRunner::new(
                policy(),
                Arc::new(FixedControls { stop_intent: None }),
                Arc::new(RecordingCancellationStore::default()),
                Arc::new(RecordingExecutor {
                    causes: Arc::clone(&causes),
                }),
            )
            .expect("runner"),
        );
        let (tx, rx) = mpsc::channel(1);
        tx.send(receipt()).await.expect("dispatch");
        let cancellation = CancellationToken::new();
        let runner_cancellation = cancellation.clone();
        let task = tokio::spawn(async move { runner.run(rx, runner_cancellation).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();

        assert_eq!(task.await.expect("join runner"), Ok(()));
        assert_eq!(
            *causes.lock().expect("causes"),
            vec![CancellationCause::SystemShutdown]
        );
    }

    #[tokio::test]
    async fn lease_loss_drops_a_non_cooperative_attempt_after_the_drain_deadline() {
        let controls = Arc::new(LeaseLosingControls::default());
        let exit = tokio::time::timeout(
            Duration::from_secs(1),
            run_attempt(
                Arc::new(NonCooperativeExecutor),
                controls.clone(),
                Arc::new(RecordingCancellationStore::default()),
                Arc::new(RejectingCoordinationForcedAbortStore),
                WorkerRunnerPolicy {
                    heartbeat_interval: Duration::from_millis(100),
                    shutdown_drain_timeout: Duration::from_millis(100),
                    ..policy()
                },
                receipt(),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("lease-loss cleanup must be bounded");

        assert_eq!(
            exit,
            AgentLoopExit::Interrupted {
                cause: CancellationCause::LeaseLost,
            }
        );
        assert!(controls.heartbeats.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn non_cooperative_coordination_interrupt_uses_database_deadline_and_forced_store() {
        let interrupt_id = InterruptRequestId::generate();
        let intent = AttemptStopIntent::CoordinationInterrupt {
            interrupt_id,
            cooperative_deadline_at: UtcTimestamp::from_unix_millis(1_500),
            forced_abort_deadline_at: UtcTimestamp::from_unix_millis(2_000),
        };
        let controls = Arc::new(ForcedAbortControls {
            heartbeats: AtomicUsize::new(0),
            intent,
        });
        let forced_aborts = Arc::new(RecordingForcedAbortStore::default());
        let claimed = receipt();
        let expected_run_id = claimed.fence.run_id;
        let expected_attempt_id = claimed.fence.attempt_id;
        let exit = tokio::time::timeout(
            Duration::from_secs(1),
            run_attempt(
                Arc::new(NonCooperativeExecutor),
                controls.clone(),
                Arc::new(RecordingCancellationStore::default()),
                forced_aborts.clone(),
                WorkerRunnerPolicy {
                    heartbeat_interval: Duration::from_millis(10),
                    ..policy()
                },
                claimed,
                CancellationToken::new(),
            ),
        )
        .await
        .expect("database deadline must bound the forced-abort path");

        let AgentLoopExit::CoordinationForcedAbort { receipt } = exit else {
            panic!("expected a durable coordination forced abort")
        };
        assert_eq!(receipt.run_id(), expected_run_id);
        assert_eq!(receipt.attempt_id(), expected_attempt_id);
        assert_eq!(
            receipt.attempt_outcome(),
            DurableAttemptOutcome::Interrupted
        );
        assert_eq!(receipt.run_outcome(), DurableRunOutcome::Interrupted);
        assert!(controls.heartbeats.load(Ordering::SeqCst) >= 2);
        assert_eq!(forced_aborts.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn forced_abort_recovery_drains_due_work_then_stops_with_the_runtime() {
        let store = Arc::new(OneShotForcedAbortRecoveryStore {
            calls: AtomicUsize::new(0),
            interrupt_id: InterruptRequestId::generate(),
            run_id: RunId::generate(),
            attempt_id: AttemptId::generate(),
        });
        let worker =
            CoordinationForcedAbortRecoveryWorker::new(store.clone(), Duration::from_millis(100))
                .expect("recovery worker");
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let task = tokio::spawn(async move { worker.run(child).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovered work must be drained without an idle delay");
        cancellation.cancel();
        assert_eq!(task.await.expect("join recovery worker"), Ok(()));
    }

    #[tokio::test]
    async fn shutdown_drain_deadline_is_bounded() {
        let runner = Arc::new(
            RuntimeWorkerRunner::new(
                WorkerRunnerPolicy {
                    shutdown_drain_timeout: Duration::from_millis(100),
                    ..policy()
                },
                Arc::new(FixedControls { stop_intent: None }),
                Arc::new(RecordingCancellationStore::default()),
                Arc::new(NonCooperativeExecutor),
            )
            .expect("runner"),
        );
        let (tx, rx) = mpsc::channel(1);
        tx.send(receipt()).await.expect("dispatch");
        let cancellation = CancellationToken::new();
        let child = cancellation.clone();
        let task = tokio::spawn(async move { runner.run(rx, child).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), task)
                .await
                .expect("bounded")
                .expect("join"),
            Ok(())
        );
    }

    #[test]
    fn invalid_policy_fails_closed() {
        assert_eq!(
            WorkerRunnerPolicy {
                heartbeat_interval: Duration::from_millis(400),
                lease_duration: Duration::from_secs(1),
                ..policy()
            }
            .validate(),
            Err(WorkerRunnerPolicyError::InvalidHeartbeatInterval)
        );
    }

    #[tokio::test]
    async fn maintenance_attempts_never_fall_back_to_the_regular_executor() {
        let regular_calls = Arc::new(Mutex::new(Vec::new()));
        let maintenance_calls = Arc::new(Mutex::new(Vec::new()));
        let evaluation_calls = Arc::new(Mutex::new(Vec::new()));
        let router = ActivityKindAttemptExecutor::new(
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&regular_calls),
            }),
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&maintenance_calls),
            }),
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&evaluation_calls),
            }),
        );
        let claimed = receipt();
        let telemetry = (*claimed.telemetry).clone();
        let context = attempt_context(&claimed);

        let exit = router
            .execute(
                DispatchedAttempt {
                    context,
                    activity_kind: ActivityKind::Maintenance,
                },
                telemetry,
                InvocationSupervisor::new(),
            )
            .await;

        assert_eq!(exit, AgentLoopExit::AlreadyTerminal);
        assert!(regular_calls.lock().expect("regular calls").is_empty());
        assert_eq!(
            *maintenance_calls.lock().expect("maintenance calls"),
            vec![ActivityKind::Maintenance]
        );
        assert!(
            evaluation_calls
                .lock()
                .expect("evaluation calls")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn evaluation_attempts_always_use_the_dedicated_system_agent_executor() {
        let regular_calls = Arc::new(Mutex::new(Vec::new()));
        let maintenance_calls = Arc::new(Mutex::new(Vec::new()));
        let evaluation_calls = Arc::new(Mutex::new(Vec::new()));
        let router = ActivityKindAttemptExecutor::new(
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&regular_calls),
            }),
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&maintenance_calls),
            }),
            Arc::new(RouteRecordingExecutor {
                calls: Arc::clone(&evaluation_calls),
            }),
        );
        let claimed = receipt();
        let telemetry = (*claimed.telemetry).clone();
        let context = attempt_context(&claimed);

        let exit = router
            .execute(
                DispatchedAttempt {
                    context,
                    activity_kind: ActivityKind::Evaluation,
                },
                telemetry,
                InvocationSupervisor::new(),
            )
            .await;

        assert_eq!(exit, AgentLoopExit::AlreadyTerminal);
        assert!(regular_calls.lock().expect("regular calls").is_empty());
        assert!(
            maintenance_calls
                .lock()
                .expect("maintenance calls")
                .is_empty()
        );
        assert_eq!(
            *evaluation_calls.lock().expect("evaluation calls"),
            vec![ActivityKind::Evaluation]
        );
    }

    fn policy() -> WorkerRunnerPolicy {
        WorkerRunnerPolicy {
            max_inflight_runs: NonZeroUsize::new(2).expect("nonzero"),
            lease_duration: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(100),
            shutdown_drain_timeout: Duration::from_secs(1),
        }
    }

    fn receipt() -> ExecutionReceipt {
        let run_id = RunId::generate();
        let attempt_id = AttemptId::generate();
        let attempt_number = AttemptNumber::new(1).expect("attempt");
        let fencing_token = FencingToken::new(1).expect("fence");
        ExecutionReceipt {
            ledger: AppendReceipt {
                disposition: AppendDisposition::Committed,
                first_sequence: ConversationSeq::new(1).expect("sequence"),
                last_sequence: ConversationSeq::new(2).expect("sequence"),
                committed_at: UtcTimestamp::from_unix_millis(1),
            },
            fence: AttemptFence {
                run_id,
                attempt_id,
                attempt_number,
                worker_id: WorkerId::new("worker-test").expect("worker"),
                lease_id: LeaseId::generate(),
                fencing_token,
                home_epoch: HomeEpoch::new(1).expect("epoch"),
            },
            activity_kind: ActivityKind::Chat,
            lease_expires_at: UtcTimestamp::from_unix_millis(30_000),
            telemetry: Box::new(AttemptTelemetryContext {
                tenant_id: TenantId::generate(),
                conversation_id: ConversationId::generate(),
                activity_id: ActivityId::generate(),
                run_id,
                attempt_id,
                attempt_number,
                fencing_token,
                correlation_id: CorrelationId::generate(),
                trace_context: TraceContext {
                    traceparent: TraceParent::new(
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    )
                    .expect("traceparent"),
                    tracestate: None,
                },
            }),
        }
    }
}
