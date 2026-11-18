//! Drives the shared reducer inside an existing tracked invocation task. This
//! crate creates no task, scheduler, lease, public status, or replacement Run.
#![forbid(unsafe_code)]
use futures_util::FutureExt;
use musteragent_execution_liveness::{
    Closure, ControlStop, Event, ExecutionActivitySource, ExecutionRecord,
    ExecutionStopConfirmation, ExecutionStopControl, FencedExecutionLivenessJournal, LivenessError,
    LivenessHomeClock, LivenessJournalError, LocalInstant, Phase, RecordMutation, RecordTransition,
    StopReason,
};
use musteragent_run::AttemptContext;
use musteragent_runtime_contract::ErrorCode;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

mod boundary;
mod cleanup;
mod emergency;
mod home;
mod host_fence;
pub use host_fence::{HostExecutionFence, HostExecutionPermit};
mod recovery;
use boundary::BoundaryOwner;
pub use home::ExecutionSupervision;
pub use recovery::close_unobserved_execution;

/// Explicit original-owner boundary. A provider return value alone must never
/// use Committed; failures without a durable receipt retain cleanup obligations.
pub enum ExecutionBoundary<T> {
    Committed(T),
    Interrupted(ExecutionSupervisionError),
}

pub enum ExecutionSupervisionOutcome<T> {
    /// The original owner's future returned. T retains its original business
    /// result/receipt semantics; this wrapper does not manufacture success.
    Boundary(T),
    Stopped {
        closure: Closure,
        record: ExecutionRecord,
    },
}
#[derive(Debug, thiserror::Error)]
pub enum ExecutionSupervisionError {
    #[error(transparent)]
    Journal(#[from] LivenessJournalError),
    #[error(transparent)]
    Invalid(#[from] LivenessError),
    #[error("the Home Journal observation request exceeded its request budget")]
    JournalRequestExpired,
    #[error("the original execution owner did not finish its durable boundary drain")]
    BoundaryDrainExpired,
}
impl ExecutionSupervisionError {
    /// Home adapters expose observation recovery through their existing fenced
    /// Journal port without losing the original infrastructure/fencing code.
    pub fn into_journal_error(self) -> LivenessJournalError {
        match self {
            ExecutionSupervisionError::Journal(error) => error,
            ExecutionSupervisionError::Invalid(rejection) => LivenessJournalError {
                code: if rejection == LivenessError::ClockUnavailable {
                    ErrorCode::DependencyUnavailable
                } else {
                    ErrorCode::InvalidCommand
                },
                retryable: rejection == LivenessError::ClockUnavailable,
                rejection: Some(rejection),
            },
            ExecutionSupervisionError::JournalRequestExpired
            | ExecutionSupervisionError::BoundaryDrainExpired => LivenessJournalError {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                rejection: None,
            },
        }
    }
}
/// The original owner that rejected this execution. Error codes alone cannot
/// distinguish an explicit host admission failure from a failed Journal/drain.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionSupervisionFailureOrigin {
    /// Original host acquire/check failed before the boundary's first poll.
    HostFence,
    /// Original Journal, clock, physical observation or durable boundary failed.
    /// Local emergency cleanup does not replace this owner's recovery evidence.
    OriginalOwner,
}

#[derive(Debug)]
pub struct ExecutionSupervisionFailure {
    pub origin: ExecutionSupervisionFailureOrigin,
    pub cause: ExecutionSupervisionError,
    /// Local positive evidence only. Failure to commit this evidence leaves the
    /// durable recovery/reconciliation obligation intact for the next owner.
    pub physical_stop: Option<ExecutionStopConfirmation>,
    /// Original Journal closure, when emergency resource cleanup was also
    /// durably recorded. This never changes the original business failure.
    pub durable_closure: Option<Closure>,
}

pub struct ExecutionReactor {
    journal: Arc<dyn FencedExecutionLivenessJournal<Owner = AttemptContext>>,
    clock: Arc<dyn LivenessHomeClock>,
    owner: AttemptContext,
    record: ExecutionRecord,
    source: Box<dyn ExecutionActivitySource>,
    control: Arc<dyn ExecutionStopControl>,
    host_fence: Option<Arc<dyn HostExecutionFence>>,
}
impl ExecutionReactor {
    /// The record must come from the original dispatch transaction or fenced
    /// recovery. This does not admit or redispatch a physical operation.
    pub fn new(
        journal: Arc<dyn FencedExecutionLivenessJournal<Owner = AttemptContext>>,
        clock: Arc<dyn LivenessHomeClock>,
        owner: AttemptContext,
        record: ExecutionRecord,
        source: Box<dyn ExecutionActivitySource>,
        control: Arc<dyn ExecutionStopControl>,
    ) -> Result<Self, LivenessError> {
        let binding = record.binding();
        if binding != source.binding()
            || binding != control.binding()
            || binding.run_id != owner.run_id
            || binding.owner_attempt_id != owner.attempt_id
            || binding.owner_fence != owner.fencing_token
        {
            return Err(LivenessError::BindingMismatch);
        }
        if !matches!(&record, ExecutionRecord::Checkpoint(_)) {
            return Err(LivenessError::InvalidPhase);
        }
        Ok(Self {
            journal,
            clock,
            owner,
            record,
            source,
            control,
            host_fence: None,
        })
    }

    pub(crate) fn with_host_fence(mut self, fence: Option<Arc<dyn HostExecutionFence>>) -> Self {
        self.host_fence = fence;
        self
    }

    /// The boundary future includes the original owner's effect/terminal/wait
    /// commit, not merely socket acceptance or construction of a provider stream.
    /// It remains polled while observation commits or physical cleanup wait.
    /// The containing RuntimeTaskSupervisor still owns this entire future.
    pub async fn run<F, C, T>(
        mut self,
        boundary: F,
        cancellation: C,
    ) -> Result<ExecutionSupervisionOutcome<T>, ExecutionSupervisionFailure>
    where
        F: Future<Output = ExecutionBoundary<T>>,
        C: Future<Output = ControlStop>,
    {
        // This is before even the first poll of physical work. Failure keeps
        // the original never-dispatched operation, and cannot start cleanup I/O.
        let permit = match &self.host_fence {
            Some(fence) => match fence
                .acquire(
                    &self.owner,
                    self.control.clone(),
                    self.record.policy().cleanup_window(),
                )
                .await
            {
                Ok(permit) => Some(permit),
                Err(error) => {
                    return Err(ExecutionSupervisionFailure {
                        origin: ExecutionSupervisionFailureOrigin::HostFence,
                        cause: error.into(),
                        physical_stop: None,
                        durable_closure: None,
                    });
                }
            },
            None => None,
        };
        if let Some(permit) = permit.as_ref() {
            permit
                .check()
                .map_err(|error| ExecutionSupervisionFailure {
                    origin: ExecutionSupervisionFailureOrigin::HostFence,
                    cause: error.into(),
                    physical_stop: None,
                    durable_closure: None,
                })?;
        }
        let cancellation = async {
            match permit.as_ref() {
                Some(permit) => tokio::select! { biased;
                    () = permit.revoked() => ControlStop::AuthorizationLost,
                    reason = cancellation => reason,
                },
                None => cancellation.await,
            }
        };
        let boundary = boundary.fuse();
        tokio::pin!(boundary);
        let mut boundary = BoundaryOwner::new(boundary.as_mut());
        tokio::pin!(cancellation);
        match self.drive(&mut boundary, &mut cancellation).await {
            Ok(outcome) => {
                let quiescent = matches!(
                    &outcome,
                    ExecutionSupervisionOutcome::Boundary(_)
                        | ExecutionSupervisionOutcome::Stopped {
                            closure: Closure::StopConfirmed(_),
                            ..
                        }
                );
                if quiescent && let Some(permit) = permit.as_ref() {
                    permit.settled();
                }
                Ok(outcome)
            }
            Err(cause) => {
                // Ownership/infrastructure failure revokes local execution.
                // Cleanup evidence may be committed, but must never replace
                // the original observation error with an Agent cancellation.
                let reason = match &cause {
                    ExecutionSupervisionError::Journal(error)
                        if error.code == ErrorCode::AttemptLeaseStale =>
                    {
                        StopReason::OwnershipLost
                    }
                    _ => StopReason::SystemShutdown,
                };
                let physical_stop = self.emergency_cleanup(&mut boundary, reason).await;
                let durable_closure = match physical_stop.as_ref() {
                    Some(proof) => self.record_emergency_stop(proof, reason).await,
                    None => self.record_unconfirmed_cleanup().await,
                };
                if physical_stop.is_some()
                    && let Some(permit) = permit.as_ref()
                {
                    permit.settled();
                }
                Err(ExecutionSupervisionFailure {
                    origin: ExecutionSupervisionFailureOrigin::OriginalOwner,
                    cause,
                    physical_stop,
                    durable_closure,
                })
            }
        }
    }
    async fn drive<F, C, T>(
        &mut self,
        boundary: &mut BoundaryOwner<'_, F>,
        cancellation: &mut Pin<&mut C>,
    ) -> Result<ExecutionSupervisionOutcome<T>, ExecutionSupervisionError>
    where
        F: Future<Output = ExecutionBoundary<T>>,
        C: Future<Output = ControlStop>,
    {
        let mut flush_due: Option<LocalInstant> = None;
        let mut requested_stop = None;
        loop {
            let checkpoint = match &self.record {
                ExecutionRecord::Checkpoint(value) => value,
                ExecutionRecord::Recovering(_) => return Err(LivenessError::InvalidPhase.into()),
            };
            match checkpoint.state().phase() {
                Phase::Stopping { .. } => return self.cleanup(boundary).await,
                Phase::Closed(
                    closure @ (Closure::StopConfirmed(_) | Closure::StopUnconfirmed(_)),
                ) => {
                    return Ok(ExecutionSupervisionOutcome::Stopped {
                        closure,
                        record: self.record.clone(),
                    });
                }
                Phase::Waiting { .. } | Phase::Closed(Closure::TerminalObserved) => {
                    return tokio::time::timeout(
                        self.record.policy().windows().post_exit_drain,
                        boundary.drain(),
                    )
                    .await
                    .map_err(|_| ExecutionSupervisionError::BoundaryDrainExpired)
                    .and_then(boundary_result);
                }
                Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. } => {}
            }
            if let Some(reason) = requested_stop {
                let request = self.journal.mutate(
                    &self.owner,
                    self.record.binding().scope,
                    self.record.revision(),
                    RecordMutation::Event(Event::Stop(reason)),
                );
                match race_boundary(boundary, request, self.request_budget()).await? {
                    Step::Boundary(output) => {
                        return boundary_result(output);
                    }
                    Step::Result(result) => {
                        if let Some(outcome) = self.accept_transition(result, boundary).await? {
                            return Ok(outcome);
                        }
                    }
                }
                continue;
            }
            let now = self.clock.sample()?.monotonic;
            now.since(checkpoint.saved_at().monotonic)?;
            let execution_due = checkpoint
                .state()
                .next_due()
                .ok_or(LivenessError::InvalidPhase)?;
            let due = flush_due.map_or(execution_due, |flush| {
                if flush.elapsed < execution_due.elapsed {
                    flush
                } else {
                    execution_due
                }
            });
            if now.elapsed >= due.elapsed {
                let budget = self.request_budget();
                let request =
                    self.journal
                        .observe(&self.owner, self.record.revision(), self.source.as_mut());
                match race_boundary(boundary, request, budget).await? {
                    Step::Boundary(output) => {
                        return boundary_result(output);
                    }
                    Step::Result(result) => {
                        if let Some(outcome) = self.accept_transition(result, boundary).await? {
                            return Ok(outcome);
                        }
                    }
                }
                flush_due = None;
                continue;
            }
            // Tokio is only a wakeup mechanism. The Home clock decides expiry,
            // including elapsed suspend time, at every bounded wakeup.
            let sleep =
                (due.elapsed - now.elapsed).min(self.record.policy().windows().observation_request);
            tokio::select! {
                biased;
                // A ready stop must win before the first physical dispatch.
                // An already-started boundary still drains during its Journal
                // transition, preserving an original committed result.
                reason = cancellation.as_mut() => requested_stop = Some(reason),
                output = boundary.start_or_continue() => return boundary_result(output),
                () = self.source.changed(), if flush_due.is_none() => {
                    flush_due = self.source.oldest_uncommitted()?.map(|first| {
                        if matches!(checkpoint.state().phase(), Phase::Starting { .. }) { Ok(first) }
                        else { first.checked_add(self.record.policy().windows().checkpoint_interval) }
                    }).transpose()?;
                }
                () = tokio::time::sleep(sleep) => {}
            }
        }
    }
    fn request_budget(&self) -> Duration {
        (self.record.policy().max_delivery_lag()
            - self.record.policy().windows().checkpoint_interval)
            .min(self.record.policy().windows().observation_request)
    }
    async fn accept_transition<F, T>(
        &mut self,
        result: Result<RecordTransition, LivenessJournalError>,
        boundary: &mut BoundaryOwner<'_, F>,
    ) -> Result<Option<ExecutionSupervisionOutcome<T>>, ExecutionSupervisionError>
    where
        F: Future<Output = ExecutionBoundary<T>>,
    {
        match result {
            Ok(result) => self.record = result.next,
            Err(error) if error.code == ErrorCode::IdempotencyConflict => {
                match race_boundary(
                    boundary,
                    self.journal.load(&self.owner, self.record.binding().scope),
                    self.request_budget(),
                )
                .await?
                {
                    Step::Boundary(output) => {
                        return boundary_result(output).map(Some);
                    }
                    Step::Result(result) => {
                        let loaded = result?.ok_or(LivenessError::InvalidPhase)?;
                        if loaded.binding() != self.record.binding() {
                            return Err(LivenessError::BindingMismatch.into());
                        }
                        self.record = loaded;
                    }
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(None)
    }
}
enum Step<T, R> {
    Boundary(ExecutionBoundary<T>),
    Result(Result<R, LivenessJournalError>),
}
async fn race_boundary<F, T, R>(
    boundary: &mut BoundaryOwner<'_, F>,
    request: impl Future<Output = Result<R, LivenessJournalError>>,
    budget: Duration,
) -> Result<Step<T, R>, ExecutionSupervisionError>
where
    F: Future<Output = ExecutionBoundary<T>>,
{
    tokio::select! {
        biased;
        output = boundary.drain() => Ok(Step::Boundary(output)),
        result = tokio::time::timeout(budget, request) => result.map(Step::Result).map_err(|_| ExecutionSupervisionError::JournalRequestExpired),
    }
}

fn boundary_result<T>(
    boundary: ExecutionBoundary<T>,
) -> Result<ExecutionSupervisionOutcome<T>, ExecutionSupervisionError> {
    match boundary {
        ExecutionBoundary::Committed(value) => Ok(ExecutionSupervisionOutcome::Boundary(value)),
        ExecutionBoundary::Interrupted(error) => Err(error),
    }
}
