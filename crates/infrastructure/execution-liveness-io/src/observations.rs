use musteragent_execution_liveness::{
    ActivityCursor, ActivityObservation, ActivitySpan, ExecutionAcceptance,
    ExecutionActivityChannel, ExecutionActivityFactory, ExecutionActivitySink,
    ExecutionActivitySource, ExecutionCheckpoint, ExecutionRecord, LivenessError,
    LivenessHomeClock, LocalInstant, ObservationBatch, ObservationOrigin, Phase,
    SampledObservations,
};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, ExecutionBinding, ExecutionLivenessPolicy,
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

struct Pending {
    accepted: bool,
    acceptance_committed: Option<Result<(), LivenessError>>,
    pending_acceptance: Option<LocalInstant>,
    ended_at: Option<LocalInstant>,
    pending_end: Option<LocalInstant>,
    cursors: [ActivityCursor; ActivitySource::COUNT],
    activities: [Option<ActivitySpan>; ActivitySource::COUNT],
    last_observed: LocalInstant,
    closed: bool,
}
struct Channel {
    binding: ExecutionBinding,
    policy: Arc<ExecutionLivenessPolicy>,
    clock: Arc<dyn LivenessHomeClock>,
    pending: Mutex<Pending>,
    changed: tokio::sync::Notify,
    acceptance_changed: tokio::sync::Notify,
}
/// The unique consumer belongs to the existing execution owner. Dropping it
/// invalidates all producer handles, including callbacks that arrive late.
pub struct ExecutionActivityInbox {
    channel: Arc<Channel>,
    outstanding: Option<ObservationBatch>,
}

#[derive(Default)]
pub struct HomeExecutionActivityFactory;
impl ExecutionActivityFactory for HomeExecutionActivityFactory {
    fn open(
        &self,
        checkpoint: &ExecutionCheckpoint,
        clock: Arc<dyn LivenessHomeClock>,
    ) -> Result<ExecutionActivityChannel, LivenessError> {
        // A new process may only observe an execution after fenced recovery has
        // rebound its checkpoint. Creating an inbox cannot reset that boundary.
        clock
            .sample()?
            .monotonic
            .since(checkpoint.saved_at().monotonic)?;
        let (sink, source) = activity_channel(checkpoint, clock)?;
        Ok(ExecutionActivityChannel {
            sink,
            source: Box::new(source),
        })
    }
}

pub fn activity_channel(
    checkpoint: &ExecutionCheckpoint,
    clock: Arc<dyn LivenessHomeClock>,
) -> Result<(Arc<dyn ExecutionActivitySink>, ExecutionActivityInbox), LivenessError> {
    let parts = checkpoint.parts();
    if !matches!(
        parts.phase,
        Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. }
    ) {
        return Err(LivenessError::InvalidPhase);
    }
    let channel = Arc::new(Channel {
        binding: parts.binding,
        policy: parts.policy,
        clock,
        changed: tokio::sync::Notify::new(),
        acceptance_changed: tokio::sync::Notify::new(),
        pending: Mutex::new(Pending {
            accepted: parts.accepted_at.is_some(),
            acceptance_committed: parts.accepted_at.map(|_| Ok(())),
            pending_acceptance: None,
            ended_at: match parts.phase {
                Phase::Draining { ended_at, .. } => Some(ended_at),
                _ => None,
            },
            pending_end: None,
            cursors: parts.cursors,
            activities: [None; ActivitySource::COUNT],
            last_observed: parts.saved_at.monotonic,
            closed: false,
        }),
    });
    Ok((
        channel.clone(),
        ExecutionActivityInbox {
            channel,
            outstanding: None,
        },
    ))
}
impl Channel {
    fn observe(&self, pending: &mut Pending) -> Result<LocalInstant, LivenessError> {
        if pending.closed {
            return Err(LivenessError::ScopeClosed);
        }
        let observed = self.clock.sample()?.monotonic;
        if observed.epoch != pending.last_observed.epoch {
            return Err(LivenessError::ClockEpochChanged);
        }
        if observed.elapsed < pending.last_observed.elapsed {
            return Err(LivenessError::ClockMovedBackwards);
        }
        pending.last_observed = observed;
        Ok(observed)
    }
}
impl ExecutionActivitySink for Channel {
    fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    fn accepted(&self) -> Result<ExecutionAcceptance, LivenessError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        if pending.closed {
            return Err(LivenessError::ScopeClosed);
        }
        if pending.ended_at.is_some() {
            return Err(LivenessError::InvalidPhase);
        }
        let observed = self.observe(&mut pending)?;
        if pending.accepted {
            // The checkpoint may already contain acceptance when a fenced
            // owner resumes a user wait. It is the same admitted operation:
            // retain the original fact without emitting progress or buying time.
            return Ok(ExecutionAcceptance::AlreadyObserved);
        }
        pending.accepted = true;
        pending.pending_acceptance = Some(observed);
        self.changed.notify_one();
        Ok(ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(
        &self,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<(), LivenessError>> {
        Box::pin(async move {
            loop {
                let changed = self.acceptance_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let pending = self
                        .pending
                        .lock()
                        .map_err(|_| LivenessError::ClockUnavailable)?;
                    if pending.closed {
                        return Err(LivenessError::ScopeClosed);
                    }
                    if let Some(result) = pending.acceptance_committed {
                        return result;
                    }
                }
                changed.await;
            }
        })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        if !pending.accepted || pending.ended_at.is_some() {
            return Err(LivenessError::InvalidPhase);
        }
        let observed = self.observe(&mut pending)?;
        pending.ended_at = Some(observed);
        pending.pending_end = Some(observed);
        self.changed.notify_one();
        Ok(())
    }
    fn advanced(
        &self,
        source: ActivitySource,
        work_cursor: NonZeroU64,
    ) -> Result<(), LivenessError> {
        if !self.policy.accepts(source) {
            return Err(LivenessError::SourceNotAdmitted);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        if !pending.accepted || pending.ended_at.is_some() {
            return Err(LivenessError::InvalidPhase);
        }
        let prior = pending.cursors[source.index()];
        if work_cursor.get() <= prior.work {
            return Err(LivenessError::NonAdvancingEvidence);
        }
        let sequence = prior
            .sequence
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(LivenessError::RevisionOverflow)?;
        let observed_at = self.observe(&mut pending)?;
        pending.cursors[source.index()] = ActivityCursor {
            sequence: sequence.get(),
            work: work_cursor.get(),
        };
        let observation = ActivityObservation {
            source,
            sequence,
            work_cursor,
            observed_at,
            origin: ObservationOrigin::Live,
        };
        let entry = &mut pending.activities[source.index()];
        match entry {
            Some(span) => span.last = observation,
            None => {
                self.changed.notify_one();
                *entry = Some(ActivitySpan {
                    first: observation,
                    last: observation,
                })
            }
        }
        Ok(())
    }
}
impl ExecutionActivityInbox {
    /// Diagnostic/retry snapshot. Deadline decisions must use Journal::observe,
    /// which samples ingress only after acquiring the original commit lane.
    pub fn snapshot(&mut self) -> Result<ObservationBatch, LivenessError> {
        let mut pending = self
            .channel
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        Ok(capture(&mut self.outstanding, &mut pending))
    }
    pub fn acknowledge(
        &mut self,
        batch: &ObservationBatch,
        committed: &ExecutionRecord,
    ) -> Result<(), LivenessError> {
        if committed.binding() != self.channel.binding {
            return Err(LivenessError::BindingMismatch);
        }
        if self.outstanding.as_ref() != Some(batch) {
            return Err(LivenessError::RevisionMismatch);
        }
        if batch.accepted_at.is_some() {
            let accepted = matches!(committed, ExecutionRecord::Checkpoint(checkpoint)
                if checkpoint.parts().accepted_at.is_some()
                    && matches!(checkpoint.state().phase(), Phase::Running { .. } | Phase::Draining { .. }));
            let mut pending = self
                .channel
                .pending
                .lock()
                .map_err(|_| LivenessError::ClockUnavailable)?;
            pending.acceptance_committed = Some(if accepted {
                Ok(())
            } else {
                Err(LivenessError::InvalidPhase)
            });
            self.channel.acceptance_changed.notify_waiters();
        }
        // Arrivals during the commit live in pending, never in outstanding.
        self.outstanding = None;
        Ok(())
    }
}
impl ExecutionActivitySource for ExecutionActivityInbox {
    fn binding(&self) -> ExecutionBinding {
        self.channel.binding
    }
    fn oldest_uncommitted(&self) -> Result<Option<LocalInstant>, LivenessError> {
        let pending = self
            .channel
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        let pending_times = pending
            .pending_acceptance
            .into_iter()
            .chain(pending.pending_end)
            .chain(
                pending
                    .activities
                    .iter()
                    .flatten()
                    .map(|span| span.first.observed_at),
            );
        let outstanding_times = self.outstanding.iter().flat_map(|batch| {
            batch
                .accepted_at
                .into_iter()
                .chain(batch.ended_at)
                .chain(batch.activities.iter().map(|span| span.first.observed_at))
        });
        Ok(pending_times
            .chain(outstanding_times)
            .min_by_key(|value| value.elapsed))
    }
    fn changed(&self) -> musteragent_runtime_contract::ApplicationFuture<'_, ()> {
        Box::pin(self.channel.changed.notified())
    }
    fn sample(
        &mut self,
        checkpoint: &ExecutionCheckpoint,
        clock: &dyn LivenessHomeClock,
    ) -> Result<SampledObservations, LivenessError> {
        if checkpoint.state().binding() != self.channel.binding {
            return Err(LivenessError::BindingMismatch);
        }
        let mut pending = self
            .channel
            .pending
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        if pending.closed {
            return Err(LivenessError::ScopeClosed);
        }
        let cutoff = clock.sample()?;
        cutoff.monotonic.since(pending.last_observed)?;
        cutoff.monotonic.since(checkpoint.saved_at().monotonic)?;
        pending.last_observed = cutoff.monotonic;
        // Commit succeeded but the caller lost its reply: remove only facts
        // covered by the durable row, retaining first arrivals after capture.
        if let Some(batch) = &mut self.outstanding {
            let parts = checkpoint.parts();
            if batch.accepted_at == parts.accepted_at {
                if batch.accepted_at.is_some() {
                    pending.acceptance_committed = Some(
                        if matches!(parts.phase, Phase::Running { .. } | Phase::Draining { .. }) {
                            Ok(())
                        } else {
                            Err(LivenessError::InvalidPhase)
                        },
                    );
                    self.channel.acceptance_changed.notify_waiters();
                }
                batch.accepted_at = None;
            }
            if matches!(parts.phase, Phase::Draining { ended_at, .. } if batch.ended_at == Some(ended_at))
            {
                batch.ended_at = None;
            }
            batch.activities.retain(|span| {
                let cursor = parts.cursors[span.last.source.index()];
                span.last.sequence.get() > cursor.sequence
                    || span.last.work_cursor.get() > cursor.work
            });
        }
        let batch = capture(&mut self.outstanding, &mut pending);
        Ok(SampledObservations { cutoff, batch })
    }
    fn acknowledge(
        &mut self,
        batch: &ObservationBatch,
        committed: &ExecutionRecord,
    ) -> Result<(), LivenessError> {
        self.acknowledge(batch, committed)
    }
}
fn capture(outstanding: &mut Option<ObservationBatch>, pending: &mut Pending) -> ObservationBatch {
    let mut batch = outstanding.take().unwrap_or_default();
    if batch.accepted_at.is_none() {
        batch.accepted_at = pending.pending_acceptance.take();
    }
    if batch.ended_at.is_none() {
        batch.ended_at = pending.pending_end.take();
    }
    for next in &mut pending.activities {
        if let Some(next) = next.take() {
            if let Some(prior) = batch
                .activities
                .iter_mut()
                .find(|prior| prior.last.source == next.last.source)
            {
                prior.last = next.last;
            } else {
                batch.activities.push(next);
            }
        }
    }
    batch
        .activities
        .sort_by_key(|span| span.first.source.index());
    *outstanding = Some(batch.clone());
    batch
}
impl Drop for ExecutionActivityInbox {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.channel.pending.lock() {
            pending.closed = true;
            self.channel.acceptance_changed.notify_waiters();
        }
    }
}
