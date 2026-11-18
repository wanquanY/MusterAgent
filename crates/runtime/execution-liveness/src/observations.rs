//! Bounded Home ingress snapshots. These are local trusted ports, not wire
//! payloads, remote timestamps, or a second authority for execution state.
use crate::{
    Action, ActivityObservation, ClockSample, Command, Event, ExecutionCheckpoint, ExecutionRecord,
    LivenessError, LivenessHomeClock, LocalInstant, ObservationOrigin, Phase, RecordTransition,
};
use musteragent_runtime_contract::execution_liveness::{ActivitySource, ExecutionBinding};
use std::sync::Arc;

/// Consumer-owned ports created from an original committed checkpoint. The
/// unique inbox stays with its existing owner; clones of the sink only report
/// evidence for that exact binding. Neither port creates an execution task.
pub struct ExecutionActivityChannel {
    pub sink: Arc<dyn crate::ExecutionActivitySink>,
    pub source: Box<dyn ExecutionActivitySource>,
}

/// Process composition supplies a Home ingress mechanism without making the
/// Runtime depend on its concrete I/O adapter. The supplied clock must be the
/// same instance used by the original Home Journal.
pub trait ExecutionActivityFactory: Send + Sync {
    fn open(
        &self,
        checkpoint: &ExecutionCheckpoint,
        clock: Arc<dyn LivenessHomeClock>,
    ) -> Result<ExecutionActivityChannel, LivenessError>;
}

/// Keep the first and last uncommitted observation of each source. Since the
/// maximum delivery lag is strictly smaller than idle, validating the first
/// timestamp also bounds every omitted gap. Keeping only the last would erase
/// the evidence that beat an old deadline while the Journal was queued.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ActivitySpan {
    pub first: ActivityObservation,
    pub last: ActivityObservation,
}
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ObservationBatch {
    pub accepted_at: Option<LocalInstant>,
    pub ended_at: Option<LocalInstant>,
    pub activities: Vec<ActivitySpan>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SampledObservations {
    pub cutoff: ClockSample,
    pub batch: ObservationBatch,
}

/// The original owner holds a unique consumer. Journal borrows it mutably
/// through commit, so two reductions cannot capture/acknowledge it concurrently.
/// Implementations sample the supplied Home clock while holding the same short
/// ingress lock used by producers. They must include every observation before
/// that cutoff, and must not await or do I/O while holding this lock.
///
/// The checkpoint permits removal of a previously committed batch whose reply
/// was lost. No uncommitted evidence may be cleared before a successful commit.
/// Only composition may provide this port; never implement it from remote
/// caller-supplied current times or self-declared progress.
pub trait ExecutionActivitySource: Send + Sync {
    fn binding(&self) -> ExecutionBinding;
    /// First Home timestamp still awaiting durable acknowledgement. Scheduling
    /// coalescing from wakeup time instead would add queue delay to the budget.
    fn oldest_uncommitted(&self) -> Result<Option<LocalInstant>, LivenessError>;
    /// Edge notification only; wakeups never count as execution progress.
    fn changed(&self) -> musteragent_runtime_contract::ApplicationFuture<'_, ()>;
    fn sample(
        &mut self,
        checkpoint: &ExecutionCheckpoint,
        clock: &dyn LivenessHomeClock,
    ) -> Result<SampledObservations, LivenessError>;
    fn acknowledge(
        &mut self,
        batch: &ObservationBatch,
        committed: &ExecutionRecord,
    ) -> Result<(), LivenessError>;
}

impl ExecutionRecord {
    /// Apply a coherent ingress snapshot and then evaluate deadlines at its
    /// cutoff. The caller must acquire the original commit lane before sampling.
    /// Delivery failure rejects the whole proposal; it cannot become idle stop.
    pub fn observe(
        &self,
        expected_revision: u64,
        batch: &ObservationBatch,
        now: ClockSample,
    ) -> Result<RecordTransition, LivenessError> {
        if expected_revision != self.revision() {
            return Err(LivenessError::RevisionMismatch);
        }
        let Self::Checkpoint(checkpoint) = self else {
            return Err(LivenessError::InvalidPhase);
        };
        let mut state = checkpoint.state().clone();
        now.monotonic.since(checkpoint.saved_at().monotonic)?;
        let mut observations = batch.validate(checkpoint, now)?;
        if let Some(observed_at) = batch.accepted_at {
            match checkpoint.parts().accepted_at {
                Some(original) if original == observed_at => {}
                Some(_) => return Err(LivenessError::InvalidPhase),
                None => {
                    let started = state.reduce(Command {
                        binding: state.binding(),
                        expected_revision: state.revision(),
                        now: now.monotonic,
                        event: Event::Started { observed_at },
                    })?;
                    state = started.next;
                    if started.action != Action::None {
                        return Ok(RecordTransition {
                            next: state.checkpoint(now)?.into(),
                            action: started.action,
                        });
                    }
                }
            }
        }
        observations.sort_by_key(|value| {
            (
                value.observed_at.elapsed,
                value.source.index(),
                value.sequence,
            )
        });
        let mut action = Action::None;
        for observation in observations {
            let cursor = state.cursors[observation.source.index()];
            if observation.sequence.get() <= cursor.sequence
                && observation.work_cursor.get() <= cursor.work
            {
                continue;
            }
            let transition = state.reduce(Command {
                binding: state.binding(),
                expected_revision: state.revision(),
                now: now.monotonic,
                event: Event::Activity(observation),
            })?;
            state = transition.next;
            action = transition.action;
            if action != Action::None {
                break;
            }
        }
        if action == Action::None
            && let Some(observed_at) = batch.ended_at
        {
            let ended = state.reduce(Command {
                binding: state.binding(),
                expected_revision: state.revision(),
                now: now.monotonic,
                event: Event::ExecutionEnded { observed_at },
            })?;
            state = ended.next;
            action = ended.action;
        }
        if action == Action::None {
            let checked = state.reduce(Command {
                binding: state.binding(),
                expected_revision: state.revision(),
                now: now.monotonic,
                event: Event::CheckDue,
            })?;
            state = checked.next;
            action = checked.action;
        }
        Ok(RecordTransition {
            next: if state.revision() == self.revision() {
                self.clone()
            } else {
                state.checkpoint(now)?.into()
            },
            action,
        })
    }
}
impl ObservationBatch {
    fn validate(
        &self,
        checkpoint: &ExecutionCheckpoint,
        now: ClockSample,
    ) -> Result<Vec<ActivityObservation>, LivenessError> {
        if self.activities.len() > ActivitySource::COUNT {
            return Err(LivenessError::InvalidActivityBatch);
        }
        if !matches!(
            checkpoint.state().phase(),
            Phase::Starting { .. } | Phase::Running { .. } | Phase::Draining { .. }
        ) && (self.accepted_at.is_some()
            || self.ended_at.is_some()
            || !self.activities.is_empty())
        {
            return Err(LivenessError::InvalidPhase);
        }
        let parts = checkpoint.parts();
        if let Some(accepted) = self.accepted_at
            && parts.accepted_at != Some(accepted)
            && now.monotonic.since(accepted)? > parts.policy.max_delivery_lag()
        {
            return Err(LivenessError::StaleObservation);
        }
        if let Some(ended) = self.ended_at
            && !matches!(parts.phase, Phase::Draining { ended_at, .. } if ended_at == ended)
            && now.monotonic.since(ended)? > parts.policy.max_delivery_lag()
        {
            return Err(LivenessError::StaleObservation);
        }
        let mut seen = [false; ActivitySource::COUNT];
        let mut result = Vec::with_capacity(self.activities.len() * 2);
        for span in &self.activities {
            let first = span.first;
            let last = span.last;
            if seen[first.source.index()]
                || first.source != last.source
                || last.observed_at.since(first.observed_at).is_err()
                || (first != last
                    && (first.sequence >= last.sequence || first.work_cursor >= last.work_cursor))
            {
                return Err(LivenessError::InvalidActivityBatch);
            }
            seen[first.source.index()] = true;
            for observation in [first, last] {
                if !parts.policy.accepts(observation.source) {
                    return Err(LivenessError::SourceNotAdmitted);
                }
                if observation.origin != ObservationOrigin::Live {
                    return Err(LivenessError::ReplayCannotRenew);
                }
                let cursor = parts.cursors[observation.source.index()];
                if observation.sequence.get() <= cursor.sequence
                    && observation.work_cursor.get() <= cursor.work
                {
                    continue;
                }
                // Do not discard an old first observation merely because the
                // last is fresh: failure to persist is an infrastructure fault.
                if now.monotonic.since(observation.observed_at)? > parts.policy.max_delivery_lag() {
                    return Err(LivenessError::StaleObservation);
                }
                result.push(observation);
                if first == last {
                    break;
                }
            }
        }
        Ok(result)
    }
}
