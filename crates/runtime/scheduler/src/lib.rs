//! Durable dispatch scheduling driven by database truth and wake-up hints.
//!
//! A signal is only a latency optimization. [`DispatchStore::claim_one`] is
//! the authority for queued, resumed, retryable and expired-Lease work.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use musteragent_conversation::{EditAndResubmitMessage, SubmitMessage};
use musteragent_execution_grant::{ExecutionGrant, SignedExecutionGrant};
use musteragent_identity::{InterruptRequestId, UtcTimestamp, VersionedKey};
use musteragent_ledger::{AttemptFence, DomainEvent, ExecutionReceipt};
use musteragent_run::{AttemptContext, RetryRun, RunEvent, WorkerId};
use musteragent_runtime_contract::{ApplicationFuture, InterruptionReason};
use musteragent_runtime_contract::{CommandAck, CommandEnvelope, ErrorCode, RuntimeError};
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use musteragent_skill::Surface as AdmissionSurface;

/// Trusted product intent used only while sealing a Run Snapshot.
///
/// It selects Musteragent Skills, never an Agent identity or mutable Runtime
/// configuration. The canonical set is included in the admission
/// idempotency fingerprint.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AdmissionSelection {
    pub surface: AdmissionSurface,
    pub requested_skill_refs: BTreeSet<VersionedKey>,
    /// Trusted host sandbox routing context for the admitted Run. When set,
    /// it is persisted in the admission transaction, so a worker can never
    /// observe the Run without its route authority. Part of the admission
    /// idempotency fingerprint.
    pub sandbox_routing: Option<musteragent_sandbox::SandboxRoutingContext>,
}

impl AdmissionSelection {
    /// Bind the host's sandbox routing context atomically with admission.
    #[must_use]
    pub fn with_sandbox_routing(
        mut self,
        routing: musteragent_sandbox::SandboxRoutingContext,
    ) -> Self {
        self.sandbox_routing = Some(routing);
        self
    }

    #[must_use]
    pub fn web(requested_skill_refs: BTreeSet<VersionedKey>) -> Self {
        Self {
            surface: AdmissionSurface::Web,
            requested_skill_refs,
            sandbox_routing: None,
        }
    }

    #[must_use]
    pub fn studio(requested_skill_refs: BTreeSet<VersionedKey>) -> Self {
        Self {
            surface: AdmissionSurface::Studio,
            requested_skill_refs,
            sandbox_routing: None,
        }
    }

    #[must_use]
    pub fn channel(requested_skill_refs: BTreeSet<VersionedKey>) -> Self {
        Self {
            surface: AdmissionSurface::Channel,
            requested_skill_refs,
            sandbox_routing: None,
        }
    }
}

/// Resource ceilings and Lease policy for one Scheduler process.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct SchedulerPolicy {
    pub claim_batch_size: usize,
    pub lease_duration: Duration,
    pub idle_wakeup_ceiling: Duration,
}

impl SchedulerPolicy {
    pub fn validate(self) -> Result<Self, SchedulerPolicyError> {
        if self.claim_batch_size == 0 || self.claim_batch_size > 1_000 {
            return Err(SchedulerPolicyError::InvalidClaimBatch);
        }
        if self.lease_duration < Duration::from_secs(1)
            || self.lease_duration > Duration::from_secs(5 * 60)
        {
            return Err(SchedulerPolicyError::InvalidLeaseDuration);
        }
        if self.idle_wakeup_ceiling < Duration::from_secs(1)
            || self.idle_wakeup_ceiling > Duration::from_secs(24 * 60 * 60)
        {
            return Err(SchedulerPolicyError::InvalidIdleWakeup);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum SchedulerPolicyError {
    #[error("Scheduler claim batch must be within 1..=1,000")]
    InvalidClaimBatch,
    #[error("Lease duration must be within 1 second..=5 minutes")]
    InvalidLeaseDuration,
    #[error("idle wake-up ceiling must be within 1 second..=24 hours")]
    InvalidIdleWakeup,
}

/// Result of one authoritative claim transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ClaimOutcome {
    Claimed(ExecutionReceipt),
    Idle {
        /// Database-derived next due time, if durable work has a future deadline.
        next_due_at: Option<UtcTimestamp>,
        database_now: UtcTimestamp,
    },
}

/// Application-owned durable claim Port.
///
/// The implementation atomically chooses queued/resumable work or an expired
/// Lease, creates the Attempt, advances fencing, and appends its Ledger facts.
pub trait DispatchStore: Send + Sync {
    /// Advances durable scheduler-owned timers before attempting a claim.
    ///
    /// Implementations must use their authoritative database clock and make
    /// each transition idempotent. The default keeps non-timer stores and test
    /// doubles source-compatible while production stores opt in explicitly.
    fn advance_due_work(&self) -> ApplicationFuture<'_, Result<u64, DispatchError>> {
        Box::pin(async { Ok(0) })
    }

    fn claim_one(
        &self,
        worker_id: &WorkerId,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<ClaimOutcome, DispatchError>>;

    /// Claims one expired Attempt whose Run has already entered the durable
    /// cancellation lane.
    ///
    /// Cancellation recovery is deliberately separate from ordinary dispatch:
    /// it must be able to converge while the execution queue is saturated by
    /// long-running or non-cooperative work. Implementations atomically fence
    /// the expired Attempt and return a fresh fence, but do not execute model or
    /// Tool work.
    fn claim_one_cancellation(
        &self,
        _worker_id: &WorkerId,
        _lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<Option<ExecutionReceipt>, DispatchError>> {
        Box::pin(async { Ok(None) })
    }
}

/// Authoritative result of one database-clock lease heartbeat.
///
/// Cancellation is observed through the same locked row as lease ownership so
/// a Worker never races a stale read of Run state against a successful renew.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LeaseHeartbeat {
    /// Authoritative database time used to interpret any attached deadline
    /// without trusting the Worker's wall clock.
    pub database_now: UtcTimestamp,
    pub lease_expires_at: UtcTimestamp,
    pub stop_intent: Option<AttemptStopIntent>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AttemptStopIntent {
    UserCancellation,
    CoordinationInterrupt {
        interrupt_id: InterruptRequestId,
        cooperative_deadline_at: UtcTimestamp,
        forced_abort_deadline_at: UtcTimestamp,
    },
}

/// What the Runtime process could prove after a cooperative interrupt failed
/// to stop its execution future before the forced-abort deadline.
///
/// Dropping the supervised future is a process-local fact only. It must never
/// be interpreted as proof that a child process, remote Provider request, or
/// already-dispatched Tool effect did not occur.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ForcedAbortTermination {
    ExecutionFutureDropped,
    /// The owning Worker stopped heartbeating and its authoritative lease
    /// expired before it could persist the forced-abort outcome. A Runtime
    /// recovery worker fenced the same Attempt after both database deadlines.
    /// This is not evidence that an unmanaged external process stopped.
    WorkerLeaseExpired,
}

/// Result of one bounded forced-abort recovery pass.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CoordinationForcedAbortRecovery {
    Recovered {
        interrupt_id: InterruptRequestId,
        run_id: musteragent_identity::RunId,
        attempt_id: musteragent_identity::AttemptId,
    },
    Idle,
}

/// Durable Scheduler-owned forced-abort boundary.
///
/// Implementations must revalidate the complete Attempt fence, the exact
/// interrupt request, and the authoritative database deadline in one commit.
/// The commit must fence the old Attempt before returning success and preserve
/// every unproven external effect as an indeterminate reconciliation obligation.
pub trait CoordinationForcedAbortStore: Send + Sync {
    fn force_abort_coordination(
        &self,
        fence: &AttemptFence,
        interrupt_id: InterruptRequestId,
        termination: ForcedAbortTermination,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>>;
}

/// Runtime-home repair boundary for an interrupt whose owning Worker died.
///
/// Implementations may recover an Attempt only when the authoritative forced
/// deadline and the Attempt lease have both expired. They must preserve the
/// original Attempt identity, atomically terminalize the Run, and retain all
/// unproven effects as reconciliation obligations. Ordinary Scheduler recovery
/// must remain blocked while the interrupt is pending.
pub trait CoordinationForcedAbortRecoveryStore: Send + Sync {
    fn recover_one_due_coordination_abort(
        &self,
    ) -> ApplicationFuture<'_, Result<CoordinationForcedAbortRecovery, AttemptControlError>>;
}

/// Worker-side lease and cancellation control Port.
///
/// Implementations extend from database time rather than accepting an
/// application-computed absolute timestamp. This prevents clock skew from
/// shortening or over-extending a distributed lease.
pub trait AttemptControlStore: Send + Sync {
    fn heartbeat(
        &self,
        fence: &AttemptFence,
        lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>>;

    /// Relinquishes a still-owned Attempt at a durable process save point.
    ///
    /// Implementations must validate the complete fence and use authoritative
    /// database time. Surrender does not invent a terminal Run outcome: it
    /// makes the lease immediately recoverable so the Scheduler can append the
    /// authoritative `attempt.abandoned` and replacement Attempt facts.
    fn surrender(
        &self,
        fence: &AttemptFence,
        reason: InterruptionReason,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>>;
}

/// Worker-owned durable boundary for a cancellation discovered before the
/// Agent Loop is constructed.
///
/// This port deliberately excludes model, Tool and policy dependencies. The
/// implementation must validate the complete Attempt fence and atomically
/// terminalize both the Attempt and Run before returning success.
pub trait AttemptCancellationStore: Send + Sync {
    fn acknowledge_user_cancellation(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum AttemptControlError {
    #[error("Attempt ownership or lease was lost")]
    LeaseLost,
    #[error("Attempt control persistence is unavailable")]
    DatabaseUnavailable,
    #[error("Attempt control request violates the runtime contract")]
    ContractViolation,
}

/// Authenticates a control-plane grant signature without resolving Secrets or
/// model routes. Request-specific authorization remains an admission concern.
pub trait ExecutionGrantAuthenticator: Send + Sync {
    fn authenticate(&self, signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode>;
}

/// Authorized message admission boundary used by Cloud transport.
///
/// `ConversationApplication::submit_message` intentionally carries no runtime
/// configuration. A Cloud call must cross this port with an authenticated
/// control-plane grant envelope.
pub trait AdmissionApplication: Send + Sync {
    fn submit_message_authorized(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn edit_and_resubmit_message_authorized(
        &self,
        command: CommandEnvelope<EditAndResubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;

    fn retry_run_authorized(
        &self,
        command: CommandEnvelope<RetryRun>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>>;
}

/// Canonical protocol encoding is injected by composition; Scheduler Core does
/// not depend on Protobuf or a transport crate.
pub trait SchedulerEventEncoder: Send + Sync {
    fn encode_domain_event(&self, event: DomainEvent) -> Result<Vec<u8>, DispatchError>;

    fn encode_run_event(&self, event: RunEvent) -> Result<Vec<u8>, DispatchError> {
        self.encode_domain_event(DomainEvent::Run(event))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("durable dispatch failed: {class:?}")]
pub struct DispatchError {
    pub class: DispatchErrorClass,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DispatchErrorClass {
    DatabaseUnavailable,
    SerializationConflict,
    ContractViolation,
}

/// Wake-up hints from admission, Interaction response, retry scheduling or DB notification.
pub trait ScheduleSignal: Send + Sync {
    fn wait(&self) -> ApplicationFuture<'_, ()>;
}

#[derive(Debug, Default)]
pub struct LocalScheduleSignal {
    notify: Notify,
}

impl LocalScheduleSignal {
    pub fn notify(&self) {
        self.notify.notify_one();
    }
}

impl ScheduleSignal for LocalScheduleSignal {
    fn wait(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(self.notify.notified())
    }
}

/// Supervised Scheduler. It has no durable in-memory timer registry.
pub struct DurableScheduler {
    policy: SchedulerPolicy,
    worker_id: WorkerId,
    store: Arc<dyn DispatchStore>,
    signal: Arc<dyn ScheduleSignal>,
    dispatch_tx: mpsc::Sender<ExecutionReceipt>,
    dispatch_queue_observer: Arc<dyn Fn(i64) + Send + Sync>,
    cancellation_store: Option<Arc<dyn AttemptCancellationStore>>,
}

impl DurableScheduler {
    pub fn new(
        policy: SchedulerPolicy,
        worker_id: WorkerId,
        store: Arc<dyn DispatchStore>,
        signal: Arc<dyn ScheduleSignal>,
        dispatch_tx: mpsc::Sender<ExecutionReceipt>,
    ) -> Result<Self, SchedulerPolicyError> {
        Ok(Self {
            policy: policy.validate()?,
            worker_id,
            store,
            signal,
            dispatch_tx,
            dispatch_queue_observer: Arc::new(|_| {}),
            cancellation_store: None,
        })
    }

    /// Installs the control-plane recovery lane for expired cancelling Runs.
    ///
    /// Product compositions must install this with their authoritative Agent
    /// Loop journal. Keeping it outside the normal dispatch queue guarantees
    /// that cancellation can reach a terminal state even when every execution
    /// slot is occupied.
    #[must_use]
    pub fn with_cancellation_recovery(mut self, store: Arc<dyn AttemptCancellationStore>) -> Self {
        self.cancellation_store = Some(store);
        self
    }

    /// Observes the exact in-process dispatch queue admission point. Database
    /// rows remain authoritative and observer work cannot influence claiming.
    #[must_use]
    pub fn with_dispatch_queue_observer(
        mut self,
        observer: Arc<dyn Fn(i64) + Send + Sync>,
    ) -> Self {
        self.dispatch_queue_observer = observer;
        self
    }

    /// Runs until cancellation or a fatal contract violation.
    pub async fn run(self, cancellation: CancellationToken) -> Result<(), SchedulerRunError> {
        let mut claims_in_turn = 0_usize;
        let mut consecutive_dispatch_failures = 0_u32;
        loop {
            if let Err(error) = self.store.advance_due_work().await {
                if wait_after_transient_dispatch_failure(
                    error,
                    &mut consecutive_dispatch_failures,
                    &cancellation,
                )
                .await?
                {
                    continue;
                }
                return Ok(());
            }
            if let Some(cancellation_store) = &self.cancellation_store {
                let recovery = self
                    .store
                    .claim_one_cancellation(&self.worker_id, self.policy.lease_duration)
                    .await;
                match recovery {
                    Ok(Some(receipt)) => {
                        let attempt = attempt_context(&receipt);
                        match cancellation_store
                            .acknowledge_user_cancellation(&attempt)
                            .await
                        {
                            Ok(()) | Err(AttemptControlError::LeaseLost) => {
                                consecutive_dispatch_failures = 0;
                                continue;
                            }
                            Err(AttemptControlError::DatabaseUnavailable) => {
                                if wait_after_transient_dispatch_failure(
                                    DispatchError {
                                        class: DispatchErrorClass::DatabaseUnavailable,
                                    },
                                    &mut consecutive_dispatch_failures,
                                    &cancellation,
                                )
                                .await?
                                {
                                    continue;
                                }
                                return Ok(());
                            }
                            Err(AttemptControlError::ContractViolation) => {
                                return Err(SchedulerRunError::Dispatch(DispatchError {
                                    class: DispatchErrorClass::ContractViolation,
                                }));
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if wait_after_transient_dispatch_failure(
                            error,
                            &mut consecutive_dispatch_failures,
                            &cancellation,
                        )
                        .await?
                        {
                            continue;
                        }
                        return Ok(());
                    }
                }
            }
            let permit = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                permit = self.dispatch_tx.reserve() => {
                    permit.map_err(|_| SchedulerRunError::WorkerQueueClosed)?
                }
            };
            let outcome = match self
                .store
                .claim_one(&self.worker_id, self.policy.lease_duration)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    drop(permit);
                    if wait_after_transient_dispatch_failure(
                        error,
                        &mut consecutive_dispatch_failures,
                        &cancellation,
                    )
                    .await?
                    {
                        continue;
                    }
                    return Ok(());
                }
            };
            consecutive_dispatch_failures = 0;
            match outcome {
                ClaimOutcome::Claimed(receipt) => {
                    (self.dispatch_queue_observer)(1);
                    permit.send(receipt);
                    claims_in_turn += 1;
                    if claims_in_turn == self.policy.claim_batch_size {
                        // Bound one scheduler turn so lease renewal, LISTEN and
                        // other supervised tasks cannot be starved by a deep queue.
                        claims_in_turn = 0;
                        tokio::task::yield_now().await;
                    }
                }
                ClaimOutcome::Idle {
                    next_due_at,
                    database_now,
                } => {
                    claims_in_turn = 0;
                    drop(permit);
                    let wait =
                        next_wait(next_due_at, database_now, self.policy.idle_wakeup_ceiling);
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Ok(()),
                        () = self.signal.wait() => {}
                        () = tokio::time::sleep(wait) => {}
                    }
                }
            }
        }
    }

    /// Starts tracked Scheduler work; shutdown waits for the task to finish.
    pub fn spawn(
        self,
        tracker: &TaskTracker,
        cancellation: CancellationToken,
    ) -> tokio::task::JoinHandle<Result<(), SchedulerRunError>> {
        tracker.spawn(self.run(cancellation))
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

async fn wait_after_transient_dispatch_failure(
    error: DispatchError,
    consecutive_failures: &mut u32,
    cancellation: &CancellationToken,
) -> Result<bool, SchedulerRunError> {
    let base_delay_ms = match error.class {
        DispatchErrorClass::DatabaseUnavailable => 100_u64,
        DispatchErrorClass::SerializationConflict => 10_u64,
        DispatchErrorClass::ContractViolation => {
            return Err(SchedulerRunError::Dispatch(error));
        }
    };
    *consecutive_failures = (*consecutive_failures).saturating_add(1);
    let exponent = (*consecutive_failures).saturating_sub(1).min(6);
    let delay_ms = base_delay_ms.saturating_mul(1_u64 << exponent).min(5_000);
    tracing::warn!(
        failure.class = ?error.class,
        consecutive_failures = *consecutive_failures,
        retry_after_ms = delay_ms,
        "Scheduler durable dispatch is temporarily unavailable; retrying without terminating the Runtime"
    );
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(false),
        () = tokio::time::sleep(Duration::from_millis(delay_ms)) => Ok(true),
    }
}

fn next_wait(
    next_due_at: Option<UtcTimestamp>,
    database_now: UtcTimestamp,
    ceiling: Duration,
) -> Duration {
    let Some(next_due_at) = next_due_at else {
        return ceiling;
    };
    let millis = next_due_at
        .unix_millis()
        .saturating_sub(database_now.unix_millis());
    let due = Duration::from_millis(u64::try_from(millis).unwrap_or(0));
    due.min(ceiling)
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SchedulerRunError {
    #[error("Worker dispatch queue closed")]
    WorkerQueueClosed,
    #[error(transparent)]
    Dispatch(DispatchError),
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use musteragent_identity::{
        ActivityId, AttemptId, AttemptNumber, ConversationId, ConversationSeq, CorrelationId,
        FencingToken, HomeEpoch, LeaseId, RunId, TenantId, UtcTimestamp,
    };
    use musteragent_ledger::{AppendDisposition, AppendReceipt, AttemptFence, ExecutionReceipt};
    use musteragent_runtime_contract::{ApplicationFuture, TraceContext, TraceParent};
    use musteragent_telemetry::AttemptTelemetryContext;
    use tokio::sync::{Notify, mpsc};
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct OneClaimStore(Mutex<Option<ExecutionReceipt>>);

    impl DispatchStore for OneClaimStore {
        fn claim_one(
            &self,
            _worker_id: &WorkerId,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<ClaimOutcome, DispatchError>> {
            Box::pin(async move {
                Ok(self.0.lock().expect("claim store lock").take().map_or(
                    ClaimOutcome::Idle {
                        next_due_at: None,
                        database_now: UtcTimestamp::from_unix_millis(1),
                    },
                    ClaimOutcome::Claimed,
                ))
            })
        }
    }

    struct RecoveringClaimStore(Mutex<VecDeque<Result<ClaimOutcome, DispatchError>>>);

    impl DispatchStore for RecoveringClaimStore {
        fn claim_one(
            &self,
            _worker_id: &WorkerId,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<ClaimOutcome, DispatchError>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .expect("recovering claim store lock")
                    .pop_front()
                    .expect("scripted claim")
            })
        }
    }

    struct CancellationClaimStore(Mutex<Option<ExecutionReceipt>>);

    impl DispatchStore for CancellationClaimStore {
        fn claim_one(
            &self,
            _worker_id: &WorkerId,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<ClaimOutcome, DispatchError>> {
            Box::pin(async {
                Ok(ClaimOutcome::Idle {
                    next_due_at: None,
                    database_now: UtcTimestamp::from_unix_millis(1),
                })
            })
        }

        fn claim_one_cancellation(
            &self,
            _worker_id: &WorkerId,
            _lease_duration: Duration,
        ) -> ApplicationFuture<'_, Result<Option<ExecutionReceipt>, DispatchError>> {
            Box::pin(
                async move { Ok(self.0.lock().expect("cancellation claim store lock").take()) },
            )
        }
    }

    #[derive(Default)]
    struct RecordingCancellationStore {
        attempts: Mutex<Vec<AttemptContext>>,
        observed: Notify,
    }

    impl AttemptCancellationStore for RecordingCancellationStore {
        fn acknowledge_user_cancellation(
            &self,
            attempt: &AttemptContext,
        ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
            let attempt = attempt.clone();
            Box::pin(async move {
                self.attempts
                    .lock()
                    .expect("cancellation attempts lock")
                    .push(attempt);
                self.observed.notify_one();
                Ok(())
            })
        }
    }

    #[test]
    fn next_wakeup_uses_database_time_and_a_bounded_ceiling() {
        let now = UtcTimestamp::from_unix_millis(10_000);
        assert_eq!(
            next_wait(
                Some(UtcTimestamp::from_unix_millis(10_125)),
                now,
                Duration::from_secs(60)
            ),
            Duration::from_millis(125)
        );
        assert_eq!(
            next_wait(
                Some(UtcTimestamp::from_unix_millis(100_000)),
                now,
                Duration::from_secs(10)
            ),
            Duration::from_secs(10)
        );
        assert_eq!(
            next_wait(
                Some(UtcTimestamp::from_unix_millis(9_000)),
                now,
                Duration::from_secs(10)
            ),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn scheduler_dispatch_observer_brackets_queue_publication() {
        let (tx, mut rx) = mpsc::channel(1);
        let deltas = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&deltas);
        let scheduler = DurableScheduler::new(
            SchedulerPolicy {
                claim_batch_size: 1,
                lease_duration: Duration::from_secs(30),
                idle_wakeup_ceiling: Duration::from_secs(1),
            },
            WorkerId::new("scheduler-metric-test").expect("Worker ID"),
            Arc::new(OneClaimStore(Mutex::new(Some(execution_receipt())))),
            Arc::new(LocalScheduleSignal::default()),
            tx,
        )
        .expect("scheduler")
        .with_dispatch_queue_observer(Arc::new(move |delta| {
            observed.lock().expect("observer lock").push(delta);
        }));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(scheduler.run(cancellation.clone()));
        let _receipt = rx.recv().await.expect("published receipt");
        assert_eq!(*deltas.lock().expect("observer lock"), vec![1]);
        cancellation.cancel();
        assert_eq!(task.await.expect("join scheduler"), Ok(()));
    }

    #[tokio::test]
    async fn cancellation_recovery_bypasses_a_saturated_execution_queue() {
        let recovered = execution_receipt();
        let recovered_attempt_id = recovered.fence.attempt_id;
        let store = Arc::new(CancellationClaimStore(Mutex::new(Some(recovered))));
        let cancellations = Arc::new(RecordingCancellationStore::default());
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(execution_receipt())
            .await
            .expect("saturate execution queue");
        let scheduler = DurableScheduler::new(
            SchedulerPolicy {
                claim_batch_size: 1,
                lease_duration: Duration::from_secs(30),
                idle_wakeup_ceiling: Duration::from_secs(1),
            },
            WorkerId::new("scheduler-cancellation-recovery-test").expect("Worker ID"),
            store,
            Arc::new(LocalScheduleSignal::default()),
            tx,
        )
        .expect("scheduler")
        .with_cancellation_recovery(cancellations.clone());
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(scheduler.run(cancellation.clone()));

        tokio::time::timeout(Duration::from_secs(1), cancellations.observed.notified())
            .await
            .expect("cancellation recovery must not wait for queue capacity");
        cancellation.cancel();
        assert_eq!(task.await.expect("join scheduler"), Ok(()));
        assert_eq!(
            cancellations
                .attempts
                .lock()
                .expect("cancellation attempts lock")[0]
                .attempt_id,
            recovered_attempt_id
        );
        let _queued = rx.recv().await.expect("original queued receipt");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn transient_dispatch_failures_back_off_without_terminating_the_runtime() {
        for class in [
            DispatchErrorClass::DatabaseUnavailable,
            DispatchErrorClass::SerializationConflict,
        ] {
            let (tx, mut rx) = mpsc::channel(1);
            let scheduler = DurableScheduler::new(
                SchedulerPolicy {
                    claim_batch_size: 1,
                    lease_duration: Duration::from_secs(30),
                    idle_wakeup_ceiling: Duration::from_secs(1),
                },
                WorkerId::new("scheduler-recovery-test").expect("Worker ID"),
                Arc::new(RecoveringClaimStore(Mutex::new(VecDeque::from([
                    Err(DispatchError { class }),
                    Ok(ClaimOutcome::Claimed(execution_receipt())),
                ])))),
                Arc::new(LocalScheduleSignal::default()),
                tx,
            )
            .expect("scheduler");
            let cancellation = CancellationToken::new();
            let task = tokio::spawn(scheduler.run(cancellation.clone()));

            let _receipt = tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("transient recovery timeout")
                .expect("recovered dispatch receipt");
            cancellation.cancel();
            assert_eq!(task.await.expect("join scheduler"), Ok(()));
        }
    }

    #[tokio::test]
    async fn contract_dispatch_failure_remains_fatal() {
        let (tx, _rx) = mpsc::channel(1);
        let scheduler = DurableScheduler::new(
            SchedulerPolicy {
                claim_batch_size: 1,
                lease_duration: Duration::from_secs(30),
                idle_wakeup_ceiling: Duration::from_secs(1),
            },
            WorkerId::new("scheduler-contract-test").expect("Worker ID"),
            Arc::new(RecoveringClaimStore(Mutex::new(VecDeque::from([Err(
                DispatchError {
                    class: DispatchErrorClass::ContractViolation,
                },
            )])))),
            Arc::new(LocalScheduleSignal::default()),
            tx,
        )
        .expect("scheduler");

        assert_eq!(
            scheduler.run(CancellationToken::new()).await,
            Err(SchedulerRunError::Dispatch(DispatchError {
                class: DispatchErrorClass::ContractViolation,
            }))
        );
    }

    fn execution_receipt() -> ExecutionReceipt {
        let run_id = RunId::generate();
        let attempt_id = AttemptId::generate();
        let attempt_number = AttemptNumber::new(1).expect("attempt number");
        let fencing_token = FencingToken::new(1).expect("fencing token");
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
                worker_id: WorkerId::new("worker-metric-test").expect("Worker ID"),
                lease_id: LeaseId::generate(),
                fencing_token,
                home_epoch: HomeEpoch::new(1).expect("home epoch"),
            },
            activity_kind: musteragent_activity::ActivityKind::Chat,
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
