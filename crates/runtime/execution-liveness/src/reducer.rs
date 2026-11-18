use musteragent_runtime_contract::execution_liveness::{DurableWaitBinding, ExecutionBinding};
use thiserror::Error;

use crate::state::Cursor;
use crate::{
    Action, ActivityObservation, Closure, ExecutionLiveness, LocalInstant, ObservationOrigin,
    Phase, StopReason,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ControlStop {
    UserCancelled,
    ParentCancelled,
    OwnershipLost,
    AuthorizationLost,
    SystemShutdown,
    ExecutionFailed,
    /// A finite dependency request timed out; distinct from execution inactivity.
    RequestTimedOut,
    CoordinationRequested,
}

impl From<ControlStop> for StopReason {
    fn from(value: ControlStop) -> Self {
        match value {
            ControlStop::UserCancelled => Self::UserCancelled,
            ControlStop::ParentCancelled => Self::ParentCancelled,
            ControlStop::OwnershipLost => Self::OwnershipLost,
            ControlStop::AuthorizationLost => Self::AuthorizationLost,
            ControlStop::SystemShutdown => Self::SystemShutdown,
            ControlStop::ExecutionFailed => Self::ExecutionFailed,
            ControlStop::RequestTimedOut => Self::RequestTimedOut,
            ControlStop::CoordinationRequested => Self::CoordinationRequested,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Event {
    /// The trusted adapter has established acceptance of this exact operation.
    Started {
        observed_at: LocalInstant,
    },
    Activity(ActivityObservation),
    /// Validated native end, distinct from the original durable result commit.
    ExecutionEnded {
        observed_at: LocalInstant,
    },
    EnterWait(DurableWaitBinding),
    Resume(DurableWaitBinding),
    Stop(ControlStop),
    /// A terminal fact closes supervision, not necessarily with business success.
    /// The existing result/reconciliation contract owns eligibility and outcome.
    TerminalObserved,
    /// Positive evidence that owned resources/the remote operation have stopped.
    StopConfirmed,
    CheckDue,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Command {
    pub binding: ExecutionBinding,
    pub expected_revision: u64,
    pub now: LocalInstant,
    pub event: Event,
}

/// A proposal, not a committed decision. Journal adapters must compare the
/// original owner fence and revision, atomically persist, and only then perform
/// its action. Pure reduction does not claim to implement durable CAS.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Transition {
    pub next: ExecutionLiveness,
    pub action: Action,
}

impl ExecutionLiveness {
    /// Serializes timer, terminal, control and activity decisions under the
    /// caller's revision. Rejected input never mutates state or advances cursors.
    pub fn reduce(&self, command: Command) -> Result<Transition, LivenessError> {
        if command.binding != self.binding {
            return Err(LivenessError::BindingMismatch);
        }
        if command.expected_revision != self.revision {
            return Err(LivenessError::RevisionMismatch);
        }
        command.now.since(self.last_reduced_at)?;
        if matches!(self.phase, Phase::Closed(_)) {
            return Err(LivenessError::ScopeClosed);
        }
        let mut next = self.clone();
        let action = match command.event {
            Event::CheckDue => match next.expire(command.now)? {
                Some(action) => action,
                None => return Ok(self.unchanged()),
            },
            Event::Stop(reason) => {
                if matches!(self.phase, Phase::Stopping { .. }) {
                    return Ok(self.unchanged());
                }
                next.request_stop(reason.into(), command.now)?
            }
            Event::StopConfirmed => {
                let Phase::Stopping { reason, .. } = self.phase else {
                    return Err(LivenessError::InvalidPhase);
                };
                next.phase = Phase::Closed(Closure::StopConfirmed(reason));
                Action::None
            }
            Event::TerminalObserved => {
                if matches!(self.phase, Phase::Stopping { .. }) {
                    // A late result goes to the existing reconciliation path;
                    // it cannot overwrite a committed stop or model observation.
                    return Err(LivenessError::ReconciliationRequired);
                }
                next.phase = Phase::Closed(Closure::TerminalObserved);
                Action::None
            }
            Event::Activity(observation) => next.observe(observation, command.now)?,
            Event::ExecutionEnded { observed_at } => {
                if let Phase::Draining { ended_at, .. } = self.phase {
                    if ended_at == observed_at {
                        return Ok(self.unchanged());
                    }
                    return Err(LivenessError::NonAdvancingEvidence);
                }
                let Phase::Running { due } = self.phase else {
                    return Err(LivenessError::InvalidPhase);
                };
                if command.now.since(observed_at)? > self.policy.max_delivery_lag() {
                    return Err(LivenessError::StaleObservation);
                }
                observed_at.since(self.active_since.ok_or(LivenessError::InvalidPhase)?)?;
                if self.last_activity_at.is_some_and(|last| {
                    last.epoch == observed_at.epoch && last.elapsed > observed_at.elapsed
                }) {
                    return Err(LivenessError::PreviousPhaseObservation);
                }
                if observed_at.elapsed >= due.elapsed {
                    next.expire(command.now)?
                        .ok_or(LivenessError::InvalidPhase)?
                } else {
                    next.phase = Phase::Draining {
                        ended_at: observed_at,
                        due: observed_at.checked_add(self.policy.windows().post_exit_drain)?,
                    };
                    next.active_since = None;
                    next.expire(command.now)?.unwrap_or(Action::None)
                }
            }
            Event::Started { observed_at } => {
                let Phase::Starting { due } = self.phase else {
                    return Err(LivenessError::InvalidPhase);
                };
                if command.now.since(observed_at)? > self.policy.max_delivery_lag() {
                    return Err(LivenessError::StaleObservation);
                }
                observed_at.since(self.dispatched_at)?;
                if observed_at.elapsed >= due.elapsed {
                    next.expire(command.now)?
                        .ok_or(LivenessError::InvalidPhase)?
                } else if self
                    .business
                    .as_ref()
                    .is_some_and(|business| command.now.elapsed >= business.due.elapsed)
                {
                    next.request_stop(StopReason::BusinessDeadlineExpired, command.now)?
                } else {
                    next.phase = Phase::Running {
                        due: observed_at.checked_add(self.policy.idle_window())?,
                    };
                    next.active_since = Some(observed_at);
                    next.accepted_at = Some(observed_at);
                    Action::None
                }
            }
            Event::EnterWait(binding) => {
                let Phase::Running { due } = self.phase else {
                    return Err(LivenessError::InvalidPhase);
                };
                if self
                    .last_wait_sequence
                    .is_some_and(|previous| binding.checkpoint_sequence <= previous)
                {
                    return Err(LivenessError::StaleWait);
                }
                if let Some(action) = next.expire(command.now)? {
                    action
                } else {
                    next.phase = Phase::Waiting {
                        binding,
                        idle_remaining: due.since(command.now)?,
                    };
                    next.active_since = None;
                    next.last_wait_sequence = Some(binding.checkpoint_sequence);
                    Action::None
                }
            }
            Event::Resume(binding) => {
                let Phase::Waiting {
                    binding: expected,
                    idle_remaining,
                } = self.phase
                else {
                    return Err(LivenessError::InvalidPhase);
                };
                if expected != binding {
                    return Err(LivenessError::WaitMismatch);
                }
                if let Some(action) = next.expire(command.now)? {
                    action
                } else {
                    next.phase = Phase::Running {
                        due: command.now.checked_add(idle_remaining)?,
                    };
                    next.active_since = Some(command.now);
                    Action::None
                }
            }
        };
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or(LivenessError::RevisionOverflow)?;
        next.last_reduced_at = command.now;
        Ok(Transition { next, action })
    }

    fn observe(
        &mut self,
        observation: ActivityObservation,
        now: LocalInstant,
    ) -> Result<Action, LivenessError> {
        let Phase::Running { due } = self.phase else {
            return Err(LivenessError::InvalidPhase);
        };
        if !self.policy.accepts(observation.source) {
            return Err(LivenessError::SourceNotAdmitted);
        }
        if observation.origin != ObservationOrigin::Live {
            return Err(LivenessError::ReplayCannotRenew);
        }
        let age = now.since(observation.observed_at)?;
        if age > self.policy.max_delivery_lag() {
            return Err(LivenessError::StaleObservation);
        }
        // Prior phase output cannot renew a newly resumed execution.
        let active_since = self.active_since.ok_or(LivenessError::InvalidPhase)?;
        if observation.observed_at.elapsed < active_since.elapsed {
            return Err(LivenessError::PreviousPhaseObservation);
        }
        let cursor = self.cursors[observation.source.index()];
        if observation.sequence.get() <= cursor.sequence
            || observation.work_cursor.get() <= cursor.work
        {
            return Err(LivenessError::NonAdvancingEvidence);
        }
        if observation.observed_at.elapsed >= due.elapsed {
            // This activity began after an entire silent window. Only fresh
            // evidence observed BEFORE due may beat an uncommitted timer.
            // Select the same earliest causal deadline as a late timer.
            return self.expire(now)?.ok_or(LivenessError::InvalidPhase);
        }
        // Business control cannot be extended by a timely activity observation.
        if self
            .business
            .as_ref()
            .is_some_and(|value| now.elapsed >= value.due.elapsed)
        {
            return self.request_stop(StopReason::BusinessDeadlineExpired, now);
        }
        let last_activity = self
            .last_activity_at
            .map_or(observation.observed_at, |prior| {
                if prior.epoch == observation.observed_at.epoch
                    && prior.elapsed > observation.observed_at.elapsed
                {
                    prior
                } else {
                    observation.observed_at
                }
            });
        self.last_activity_at = Some(last_activity);
        self.cursors[observation.source.index()] = Cursor {
            sequence: observation.sequence.get(),
            work: observation.work_cursor.get(),
        };
        // Queueing and multiple readers must neither manufacture a new window
        // from receipt time nor move an already accepted due time backwards.
        let renewed = last_activity.checked_add(self.policy.idle_window())?;
        self.phase = Phase::Running {
            due: if renewed.elapsed > due.elapsed {
                renewed
            } else {
                due
            },
        };
        Ok(Action::None)
    }

    fn expire(&mut self, now: LocalInstant) -> Result<Option<Action>, LivenessError> {
        if let Phase::Stopping {
            reason,
            cleanup_due,
        } = self.phase
        {
            if now.elapsed >= cleanup_due.elapsed {
                self.phase = Phase::Closed(Closure::StopUnconfirmed(reason));
                return Ok(Some(Action::RecordUnconfirmedStop(reason)));
            }
            return Ok(None);
        }
        // Select the earliest causal deadline even when the scheduler is late.
        // An exact tie belongs to explicit business control.
        let phase_due = match self.phase {
            Phase::Starting { due } => Some((due, StopReason::StartupExpired)),
            Phase::Running { due } => Some((due, StopReason::IdleExpired)),
            Phase::Draining { due, .. } => Some((due, StopReason::PostExitDrainExpired)),
            Phase::Waiting { .. } | Phase::Closed(_) => None,
            Phase::Stopping { .. } => unreachable!("stopping handled above"),
        };
        let due = match (phase_due, self.business.as_ref()) {
            (Some((at, reason)), Some(business)) if at.elapsed < business.due.elapsed => {
                Some((at, reason))
            }
            (_, Some(business)) => Some((business.due, StopReason::BusinessDeadlineExpired)),
            (phase, None) => phase,
        };
        match due {
            Some((at, reason)) if now.elapsed >= at.elapsed => {
                self.request_stop(reason, now).map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn request_stop(
        &mut self,
        reason: StopReason,
        now: LocalInstant,
    ) -> Result<Action, LivenessError> {
        self.phase = Phase::Stopping {
            reason,
            cleanup_due: now.checked_add(self.policy.cleanup_window())?,
        };
        Ok(Action::RequestStop(reason))
    }

    fn unchanged(&self) -> Transition {
        Transition {
            next: self.clone(),
            action: Action::None,
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum LivenessError {
    #[error("the authoritative Home clock is unavailable")]
    ClockUnavailable,
    #[error("activity coalescing batch is empty, unordered or contains duplicate sources")]
    InvalidActivityBatch,
    #[error("active execution recovery requires a fresh operation-bound proof")]
    FreshRecoveryProofRequired,
    #[error("recovery proof does not match the committed challenge")]
    RecoveryProofMismatch,
    #[error("the committed recovery verification window expired")]
    RecoveryWindowExpired,
    #[error("recovery verification is still pending in the same clock epoch")]
    RecoveryStillPending,
    #[error("persisted liveness checkpoint violates the closed contract")]
    InvalidCheckpoint,
    #[error("execution scope, operation, producer or owner does not match")]
    BindingMismatch,
    #[error("supervision revision changed; reload before deciding")]
    RevisionMismatch,
    #[error("clock epoch changed; durable recovery is required")]
    ClockEpochChanged,
    #[error("monotonic time moved backwards or an observation is from the future")]
    ClockMovedBackwards,
    #[error("timer cannot be represented without overflow")]
    TimeOverflow,
    #[error("supervision revision exhausted")]
    RevisionOverflow,
    #[error("execution supervision is closed")]
    ScopeClosed,
    #[error("event is not valid in the current execution phase")]
    InvalidPhase,
    #[error("terminal evidence after a stop decision requires reconciliation")]
    ReconciliationRequired,
    #[error("durable wait identity changed")]
    WaitMismatch,
    #[error("durable wait checkpoint was already consumed or superseded")]
    StaleWait,
    #[error("activity source is not admitted by the pinned policy")]
    SourceNotAdmitted,
    #[error("replayed evidence cannot renew current execution")]
    ReplayCannotRenew,
    #[error("observation exceeded its admitted delivery lag")]
    StaleObservation,
    #[error("observation belongs to a previous execution phase")]
    PreviousPhaseObservation,
    #[error("source sequence and work cursor must both advance")]
    NonAdvancingEvidence,
}
