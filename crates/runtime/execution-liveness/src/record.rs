//! One durable supervision row. Recovery is an explicit mutually exclusive
//! state, so a committed ticket cannot be mistaken for a new checkpoint.
use crate::{
    Action, ActivityObservation, ClockSample, Command, Event, ExecutionCheckpoint, LivenessError,
    Phase, RecoveryActivityProof, RecoveryTicket, Transition,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_contract::execution_liveness::{ActivitySource, ExecutionBinding};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExecutionRecord {
    Checkpoint(Box<ExecutionCheckpoint>),
    Recovering(Box<RecoveryTicket>),
}
impl From<ExecutionCheckpoint> for ExecutionRecord {
    fn from(value: ExecutionCheckpoint) -> Self {
        Self::Checkpoint(Box::new(value))
    }
}
impl From<RecoveryTicket> for ExecutionRecord {
    fn from(value: RecoveryTicket) -> Self {
        Self::Recovering(Box::new(value))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RecordMutation {
    Event(Event),
    /// At most the last observation per admitted source, in observation order.
    /// This is an owner-side coalescing bound, never an unbounded event log.
    ActivityBatch(Vec<ActivityObservation>),
    BeginRecovery {
        rebound: ExecutionBinding,
        nonce: Sha256,
    },
    RebindPassive {
        rebound: ExecutionBinding,
    },
    ConfirmRecoveryActivity(RecoveryActivityProof),
    ConfirmRecoveryTerminal {
        binding: ExecutionBinding,
        nonce: Sha256,
    },
    AbandonRecovery {
        rebound: ExecutionBinding,
    },
}

/// Commit `next` iff current kind, binding and revision still match the loaded
/// record. Only after that atomic commit may the caller perform `action`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordTransition {
    pub next: ExecutionRecord,
    pub action: Action,
}

impl ExecutionRecord {
    #[must_use]
    pub fn binding(&self) -> ExecutionBinding {
        match self {
            Self::Checkpoint(value) => value.state().binding(),
            Self::Recovering(value) => value.binding(),
        }
    }
    #[must_use]
    pub fn policy(
        &self,
    ) -> &musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy {
        match self {
            Self::Checkpoint(value) => value.state().policy(),
            Self::Recovering(value) => value.checkpoint().state().policy(),
        }
    }
    #[must_use]
    pub fn revision(&self) -> u64 {
        match self {
            Self::Checkpoint(value) => value.state().revision(),
            Self::Recovering(value) => value.revision(),
        }
    }
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Checkpoint(value) if matches!(value.state().phase(), Phase::Closed(_)))
    }
    pub fn next_due_utc(&self) -> Result<Option<UtcTimestamp>, LivenessError> {
        match self {
            Self::Checkpoint(value) => value.next_due_utc(),
            Self::Recovering(value) => value.next_due_utc().map(Some),
        }
    }

    /// Phase/ownership changes join the canonical Run Journal. Coalesced
    /// activity updates only its bounded checkpoint row and creates no unread.
    #[must_use]
    pub fn publication_changed(&self, next: &Self) -> bool {
        if self.binding() != next.binding() {
            return true;
        }
        match (self, next) {
            (Self::Checkpoint(a), Self::Checkpoint(b)) => {
                std::mem::discriminant(&a.state().phase())
                    != std::mem::discriminant(&b.state().phase())
                    || a.saved_at().monotonic.epoch != b.saved_at().monotonic.epoch
            }
            (Self::Recovering(_), Self::Recovering(_)) => false,
            _ => true,
        }
    }

    pub fn reduce(
        &self,
        expected_revision: u64,
        mutation: RecordMutation,
        now: ClockSample,
    ) -> Result<RecordTransition, LivenessError> {
        if expected_revision != self.revision() {
            return Err(LivenessError::RevisionMismatch);
        }
        let transition = match (self, mutation) {
            (Self::Checkpoint(value), RecordMutation::Event(event)) => {
                value.state().reduce(Command {
                    binding: value.state().binding(),
                    expected_revision,
                    now: now.monotonic,
                    event,
                })?
            }
            (Self::Checkpoint(value), RecordMutation::ActivityBatch(observations)) => {
                if observations.is_empty() || observations.len() > ActivitySource::COUNT {
                    return Err(LivenessError::InvalidActivityBatch);
                }
                let mut seen = [false; ActivitySource::COUNT];
                let mut prior_time = None;
                let mut state = value.state().clone();
                let mut action = Action::None;
                for observation in observations {
                    if seen[observation.source.index()]
                        || prior_time.is_some_and(|prior| prior > observation.observed_at.elapsed)
                    {
                        return Err(LivenessError::InvalidActivityBatch);
                    }
                    seen[observation.source.index()] = true;
                    prior_time = Some(observation.observed_at.elapsed);
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
                Transition {
                    next: state,
                    action,
                }
            }
            (Self::Checkpoint(value), RecordMutation::BeginRecovery { rebound, nonce }) => {
                return Ok(RecordTransition {
                    next: RecoveryTicket::begin(value.as_ref().clone(), rebound, nonce, now)?
                        .into(),
                    action: Action::None,
                });
            }
            (Self::Checkpoint(value), RecordMutation::RebindPassive { rebound }) => {
                value.rebind_passive(rebound, now)?
            }
            (Self::Recovering(value), RecordMutation::ConfirmRecoveryActivity(proof)) => {
                value.confirm_activity(proof, now)?
            }
            (
                Self::Recovering(value),
                RecordMutation::ConfirmRecoveryTerminal { binding, nonce },
            ) => value.confirm_terminal(binding, nonce, now)?,
            (Self::Recovering(value), RecordMutation::AbandonRecovery { rebound }) => {
                value.abandon_for(rebound, now)?
            }
            _ => return Err(LivenessError::InvalidPhase),
        };
        if transition.next.revision() == self.revision() {
            return Ok(RecordTransition {
                next: self.clone(),
                action: transition.action,
            });
        }
        Ok(RecordTransition {
            next: transition.next.checkpoint(now)?.into(),
            action: transition.action,
        })
    }
}
