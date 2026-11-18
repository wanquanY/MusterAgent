//! Portable committed facts. Monotonic offsets are meaningful only with their
//! Home clock epoch; decoding a checkpoint never authorizes a fresh dispatch.
use std::sync::Arc;
use std::time::Duration;

use musteragent_identity::{RunSeq, UtcTimestamp};
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, ExecutionBinding, ExecutionLivenessPolicy,
};

use crate::state::{AnchoredDeadline, Cursor};
use crate::{
    Action, ClockSample, Command, Event, ExecutionLiveness, LivenessError, LocalInstant, Phase,
    Transition,
};

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct ActivityCursor {
    pub sequence: u64,
    pub work: u64,
}

/// Codec input only. `from_parts` checks invariants before returning a usable
/// checkpoint. Original dispatch/acceptance clocks may predate a recovery;
/// active timers always belong to saved_at's current clock epoch.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointParts {
    pub binding: ExecutionBinding,
    pub policy: Arc<ExecutionLivenessPolicy>,
    pub business: Option<(BusinessDeadline, LocalInstant)>,
    pub revision: u64,
    pub phase: Phase,
    pub dispatched_at: LocalInstant,
    pub accepted_at: Option<LocalInstant>,
    pub active_since: Option<LocalInstant>,
    pub last_wait_sequence: Option<RunSeq>,
    pub last_reduced_at: LocalInstant,
    pub last_activity_at: Option<LocalInstant>,
    pub cursors: [ActivityCursor; ActivitySource::COUNT],
    pub saved_at: ClockSample,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionCheckpoint {
    pub(crate) state: ExecutionLiveness,
    pub(crate) saved_at: ClockSample,
}

impl ExecutionLiveness {
    pub fn checkpoint(&self, saved_at: ClockSample) -> Result<ExecutionCheckpoint, LivenessError> {
        saved_at.monotonic.since(self.last_reduced_at)?;
        let checkpoint = ExecutionCheckpoint {
            state: self.clone(),
            saved_at,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }
}

impl ExecutionCheckpoint {
    pub fn from_parts(parts: CheckpointParts) -> Result<Self, LivenessError> {
        let result = Self {
            saved_at: parts.saved_at,
            state: ExecutionLiveness {
                binding: parts.binding,
                policy: parts.policy,
                business: parts
                    .business
                    .map(|(contract, due)| Arc::new(AnchoredDeadline { contract, due })),
                revision: parts.revision,
                phase: parts.phase,
                dispatched_at: parts.dispatched_at,
                accepted_at: parts.accepted_at,
                active_since: parts.active_since,
                last_wait_sequence: parts.last_wait_sequence,
                last_reduced_at: parts.last_reduced_at,
                last_activity_at: parts.last_activity_at,
                cursors: parts.cursors.map(|cursor| Cursor {
                    sequence: cursor.sequence,
                    work: cursor.work,
                }),
            },
        };
        result.validate()?;
        Ok(result)
    }

    #[must_use]
    pub fn parts(&self) -> CheckpointParts {
        let state = &self.state;
        CheckpointParts {
            binding: state.binding,
            policy: Arc::clone(&state.policy),
            business: state
                .business
                .as_ref()
                .map(|value| (value.contract.clone(), value.due)),
            revision: state.revision,
            phase: state.phase,
            dispatched_at: state.dispatched_at,
            accepted_at: state.accepted_at,
            active_since: state.active_since,
            last_wait_sequence: state.last_wait_sequence,
            last_reduced_at: state.last_reduced_at,
            last_activity_at: state.last_activity_at,
            cursors: state.cursors.map(|cursor| ActivityCursor {
                sequence: cursor.sequence,
                work: cursor.work,
            }),
            saved_at: self.saved_at,
        }
    }

    #[must_use]
    pub fn state(&self) -> &ExecutionLiveness {
        &self.state
    }

    #[must_use]
    pub const fn saved_at(&self) -> ClockSample {
        self.saved_at
    }

    /// Index hint for the Home scheduler. It is not an execution decision:
    /// workers must load, fence and reduce the current revision before stopping.
    pub fn next_due_utc(&self) -> Result<Option<UtcTimestamp>, LivenessError> {
        self.state
            .next_due()
            .map(|due| self.utc_at(due))
            .transpose()
    }

    /// Same live Home clock and owner only. Across epochs use the single-use
    /// recovery challenge, not this path or ExecutionLiveness::new.
    pub fn restore_local(&self, now: LocalInstant) -> Result<Transition, LivenessError> {
        now.since(self.saved_at.monotonic)?;
        if matches!(self.state.phase, Phase::Closed(_)) {
            return Ok(Transition {
                next: self.state.clone(),
                action: Action::None,
            });
        }
        self.state.reduce(Command {
            binding: self.state.binding,
            expected_revision: self.state.revision,
            now,
            event: Event::CheckDue,
        })
    }

    pub(crate) fn utc_at(&self, instant: LocalInstant) -> Result<UtcTimestamp, LivenessError> {
        self.saved_at.index_time(instant)
    }

    fn validate(&self) -> Result<(), LivenessError> {
        let state = &self.state;
        self.saved_at.monotonic.since(state.last_reduced_at)?;
        if state.revision == 0 {
            return Err(LivenessError::InvalidCheckpoint);
        }
        let same_epoch_not_future = |instant: LocalInstant| {
            instant.epoch != state.last_reduced_at.epoch
                || instant.elapsed <= state.last_reduced_at.elapsed
        };
        if !same_epoch_not_future(state.dispatched_at)
            || state
                .accepted_at
                .is_some_and(|value| !same_epoch_not_future(value))
            || state
                .last_activity_at
                .is_some_and(|value| !same_epoch_not_future(value))
        {
            return Err(LivenessError::InvalidCheckpoint);
        }
        if let Some(active) = state.active_since {
            state.last_reduced_at.since(active)?;
        }
        if let Some(business) = &state.business
            && business.due.epoch != state.last_reduced_at.epoch
        {
            return Err(LivenessError::InvalidCheckpoint);
        }
        let due_within = |due: LocalInstant, window: Duration| -> Result<(), LivenessError> {
            if due.epoch != state.last_reduced_at.epoch
                || due.elapsed <= state.last_reduced_at.elapsed
                || due.elapsed > state.last_reduced_at.checked_add(window)?.elapsed
            {
                return Err(LivenessError::InvalidCheckpoint);
            }
            Ok(())
        };
        match state.phase {
            Phase::Starting { due } => {
                if state.accepted_at.is_some()
                    || state.active_since.is_some()
                    || due
                        != state
                            .dispatched_at
                            .checked_add(state.policy.startup_window())?
                {
                    return Err(LivenessError::InvalidCheckpoint);
                }
                due_within(due, state.policy.startup_window())?;
            }
            Phase::Running { due } => {
                if state.accepted_at.is_none() || state.active_since.is_none() {
                    return Err(LivenessError::InvalidCheckpoint);
                }
                due_within(due, state.policy.idle_window())?;
            }
            Phase::Draining { ended_at, due } => {
                if state.accepted_at.is_none()
                    || state.active_since.is_some()
                    || !same_epoch_not_future(ended_at)
                    || [
                        Some(state.dispatched_at),
                        state.accepted_at,
                        state.last_activity_at,
                    ]
                    .into_iter()
                    .flatten()
                    .any(|prior| prior.epoch == ended_at.epoch && prior.elapsed > ended_at.elapsed)
                    || (ended_at.epoch == due.epoch
                        && due != ended_at.checked_add(state.policy.windows().post_exit_drain)?)
                {
                    return Err(LivenessError::InvalidCheckpoint);
                }
                due_within(due, state.policy.windows().post_exit_drain)?;
            }
            Phase::Waiting {
                binding,
                idle_remaining,
            } => {
                if state.accepted_at.is_none()
                    || state.active_since.is_some()
                    || idle_remaining.is_zero()
                    || idle_remaining > state.policy.idle_window()
                    || state.last_wait_sequence != Some(binding.checkpoint_sequence)
                {
                    return Err(LivenessError::InvalidCheckpoint);
                }
            }
            Phase::Stopping { cleanup_due, .. } => {
                due_within(cleanup_due, state.policy.cleanup_window())?
            }
            Phase::Closed(_) => {}
        }
        for source in ActivitySource::ALL {
            let cursor = state.cursors[source.index()];
            if (cursor.sequence == 0) != (cursor.work == 0)
                || (cursor.sequence != 0
                    && (!state.policy.accepts(source) || state.last_activity_at.is_none()))
            {
                return Err(LivenessError::InvalidCheckpoint);
            }
        }
        self.next_due_utc()?;
        Ok(())
    }
}
