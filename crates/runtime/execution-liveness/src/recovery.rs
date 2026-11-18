//! Recovery observes the SAME operation. A ticket must be CAS-committed before
//! any challenge is sent. Loading an issued ticket never creates another grace
//! window, and no method here dispatches or retries an effect.
use std::sync::Arc;
use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;

use crate::state::{AnchoredDeadline, Cursor};
use crate::{
    Action, ActivityObservation, ClockSample, Closure, ExecutionCheckpoint, ExecutionLiveness,
    LivenessError, LocalInstant, ObservationOrigin, Phase, StopReason, Transition,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecoveryTicket {
    checkpoint: ExecutionCheckpoint,
    binding: ExecutionBinding,
    nonce: Sha256,
    started_at: ClockSample,
    due: LocalInstant,
    business_due: Option<LocalInstant>,
    revision: u64,
}

/// Only an authenticated adapter can establish this proof. An echoed nonce,
/// generic heartbeat or repeated processing status is not work evidence.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RecoveryActivityProof {
    pub binding: ExecutionBinding,
    pub nonce: Sha256,
    pub observation: ActivityObservation,
}

impl ExecutionCheckpoint {
    /// Passive waits keep their unused idle budget. A stopping scope keeps its
    /// original cleanup deadline, including through a second process restart.
    /// Active scopes require a fresh task-bound challenge instead.
    pub fn rebind_passive(
        &self,
        binding: ExecutionBinding,
        now: ClockSample,
    ) -> Result<Transition, LivenessError> {
        if matches!(self.state.phase, Phase::Closed(_)) {
            return Err(LivenessError::ScopeClosed);
        }
        validate_rebind(self, binding)?;
        if !matches!(
            self.state.phase,
            Phase::Waiting { .. } | Phase::Draining { .. } | Phase::Stopping { .. }
        ) {
            return Err(LivenessError::FreshRecoveryProofRequired);
        }
        let mut next = rebase(self, binding, now)?;
        let action = match self.state.phase {
            Phase::Waiting { .. } => expire_business(&mut next, now.monotonic)?,
            Phase::Draining { ended_at, due } => {
                if now.utc < self.saved_at.utc {
                    // A regressed clock establishes uncertainty, not a business
                    // deadline expiry. It cannot grant another drain window.
                    next.request_stop(StopReason::RecoveryUnverified, now.monotonic)?
                } else {
                    let remaining = remaining_until(self, due, now)?;
                    next.phase = Phase::Draining {
                        ended_at,
                        due: now.monotonic.checked_add(remaining)?,
                    };
                    let reduced = next.reduce(crate::Command {
                        binding,
                        expected_revision: next.revision,
                        now: now.monotonic,
                        event: crate::Event::CheckDue,
                    })?;
                    next = reduced.next;
                    reduced.action
                }
            }
            Phase::Stopping {
                reason,
                cleanup_due,
            } => {
                // A clock regression cannot buy additional cleanup time.
                let remaining = if now.utc < self.saved_at.utc {
                    Duration::ZERO
                } else {
                    remaining_until(self, cleanup_due, now)?
                };
                if remaining.is_zero() {
                    next.phase = Phase::Closed(Closure::StopUnconfirmed(reason));
                    Action::RecordUnconfirmedStop(reason)
                } else {
                    next.phase = Phase::Stopping {
                        reason,
                        cleanup_due: now.monotonic.checked_add(remaining)?,
                    };
                    Action::RequestStop(reason)
                }
            }
            _ => return Err(LivenessError::InvalidPhase),
        };
        next.checkpoint(now)?;
        Ok(Transition { next, action })
    }
}

impl RecoveryTicket {
    /// The input must be the current committed CHECKPOINT row, not an earlier
    /// checkpoint retained inside an already issued RECOVERY row. The Journal
    /// enforces kind + owner + revision in the same CAS transaction.
    pub fn begin(
        checkpoint: ExecutionCheckpoint,
        binding: ExecutionBinding,
        nonce: Sha256,
        now: ClockSample,
    ) -> Result<Self, LivenessError> {
        validate_rebind(&checkpoint, binding)?;
        if !matches!(
            checkpoint.state.phase,
            Phase::Starting { .. } | Phase::Running { .. }
        ) {
            return Err(LivenessError::InvalidPhase);
        }
        validate_recovery_clock(&checkpoint, now)?;
        let revision = checkpoint
            .state
            .revision
            .checked_add(1)
            .ok_or(LivenessError::RevisionOverflow)?;
        let mut due = now
            .monotonic
            .checked_add(checkpoint.state.policy.windows().observation_request)?;
        let mut business_due = None;
        if checkpoint.state.business.is_some() {
            let remaining = business_remaining(&checkpoint, now)?;
            let anchored = now.monotonic.checked_add(remaining)?;
            if anchored.elapsed < due.elapsed {
                due = anchored;
            }
            business_due = Some(anchored);
        }
        Ok(Self {
            checkpoint,
            binding,
            nonce,
            started_at: now,
            due,
            business_due,
            revision,
        })
    }

    #[must_use]
    pub fn checkpoint(&self) -> &ExecutionCheckpoint {
        &self.checkpoint
    }
    #[must_use]
    pub const fn binding(&self) -> ExecutionBinding {
        self.binding
    }
    #[must_use]
    pub const fn nonce(&self) -> Sha256 {
        self.nonce
    }
    #[must_use]
    pub const fn started_at(&self) -> ClockSample {
        self.started_at
    }
    #[must_use]
    pub const fn due(&self) -> LocalInstant {
        self.due
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn next_due_utc(&self) -> Result<musteragent_identity::UtcTimestamp, LivenessError> {
        self.started_at.index_time(self.due)
    }

    pub fn confirm_activity(
        &self,
        proof: RecoveryActivityProof,
        now: ClockSample,
    ) -> Result<Transition, LivenessError> {
        self.validate_response(proof.binding, proof.nonce, now)?;
        let observation = proof.observation;
        if observation.origin != ObservationOrigin::Live {
            return Err(LivenessError::ReplayCannotRenew);
        }
        if !self.checkpoint.state.policy.accepts(observation.source) {
            return Err(LivenessError::SourceNotAdmitted);
        }
        observation.observed_at.since(self.started_at.monotonic)?;
        if now.monotonic.since(observation.observed_at)?
            > self.checkpoint.state.policy.max_delivery_lag()
        {
            return Err(LivenessError::StaleObservation);
        }
        let old = self.checkpoint.state.cursors[observation.source.index()];
        if observation.sequence.get() <= old.sequence || observation.work_cursor.get() <= old.work {
            return Err(LivenessError::NonAdvancingEvidence);
        }
        let mut next = self.recovered(now)?;
        next.accepted_at = next.accepted_at.or(Some(observation.observed_at));
        next.active_since = Some(observation.observed_at);
        next.last_activity_at = Some(observation.observed_at);
        next.cursors[observation.source.index()] = Cursor {
            sequence: observation.sequence.get(),
            work: observation.work_cursor.get(),
        };
        next.phase = Phase::Running {
            due: observation
                .observed_at
                .checked_add(next.policy.idle_window())?,
        };
        let action = expire_business(&mut next, now.monotonic)?;
        next.checkpoint(now)?;
        Ok(Transition { next, action })
    }

    /// Existing reconciliation validates the terminal outcome and commits it
    /// together with this closure. Closing supervision alone is not success.
    pub fn confirm_terminal(
        &self,
        binding: ExecutionBinding,
        nonce: Sha256,
        now: ClockSample,
    ) -> Result<Transition, LivenessError> {
        self.validate_response(binding, nonce, now)?;
        let mut next = self.recovered(now)?;
        next.phase = Phase::Closed(Closure::TerminalObserved);
        next.checkpoint(now)?;
        Ok(Transition {
            next,
            action: Action::None,
        })
    }

    /// Verification expired, or the process carrying an issued ticket was
    /// interrupted. A second epoch cannot restart verification. Stop intent
    /// still needs its normal atomic commit before cancellation is sent.
    pub fn abandon(&self, now: ClockSample) -> Result<Transition, LivenessError> {
        self.abandon_for(self.binding, now)
    }

    /// A fenced successor may close an interrupted recovery, but never obtains
    /// another verification window. Validate against the issued ticket owner,
    /// not only its older checkpoint owner.
    pub fn abandon_for(
        &self,
        binding: ExecutionBinding,
        now: ClockSample,
    ) -> Result<Transition, LivenessError> {
        validate_rebind(&self.checkpoint, binding)?;
        if binding != self.binding
            && (binding.owner_fence <= self.binding.owner_fence
                || binding.owner_attempt_id == self.binding.owner_attempt_id)
        {
            return Err(LivenessError::BindingMismatch);
        }
        if now.monotonic.epoch == self.due.epoch && now.monotonic.elapsed < self.due.elapsed {
            return Err(LivenessError::RecoveryStillPending);
        }
        let mut next = self.recovered(now)?;
        next.binding = binding;
        let action = if now.utc < self.started_at.utc {
            // A broken wall clock proves uncertainty, not business expiry.
            next.request_stop(StopReason::RecoveryUnverified, now.monotonic)?
        } else {
            match expire_business(&mut next, now.monotonic)? {
                Action::None => next.request_stop(StopReason::RecoveryUnverified, now.monotonic)?,
                action => action,
            }
        };
        next.checkpoint(now)?;
        Ok(Transition { next, action })
    }

    fn recovered(&self, now: ClockSample) -> Result<ExecutionLiveness, LivenessError> {
        let mut next = rebase(&self.checkpoint, self.binding, now)?;
        if let (Some(business), Some(due)) = (&mut next.business, self.business_due) {
            let remaining = if now.utc < self.started_at.utc {
                Duration::ZERO
            } else {
                remaining_between(self.started_at, due, now)?
            };
            Arc::make_mut(business).due = now.monotonic.checked_add(remaining)?;
        }
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or(LivenessError::RevisionOverflow)?;
        Ok(next)
    }

    fn validate_response(
        &self,
        binding: ExecutionBinding,
        nonce: Sha256,
        now: ClockSample,
    ) -> Result<(), LivenessError> {
        if binding != self.binding || nonce != self.nonce {
            return Err(LivenessError::RecoveryProofMismatch);
        }
        validate_recovery_clock(&self.checkpoint, now)?;
        if now.utc < self.started_at.utc {
            return Err(LivenessError::ClockMovedBackwards);
        }
        now.monotonic.since(self.started_at.monotonic)?;
        if now.monotonic.elapsed >= self.due.elapsed {
            return Err(LivenessError::RecoveryWindowExpired);
        }
        Ok(())
    }
}

fn validate_rebind(
    checkpoint: &ExecutionCheckpoint,
    binding: ExecutionBinding,
) -> Result<(), LivenessError> {
    let old = checkpoint.state.binding;
    if old.tenant_id != binding.tenant_id
        || old.run_id != binding.run_id
        || old.scope != binding.scope
        || old.operation_identity != binding.operation_identity
        || old.producer_generation != binding.producer_generation
    {
        return Err(LivenessError::BindingMismatch);
    }
    if binding != old
        && (binding.owner_fence <= old.owner_fence
            || binding.owner_attempt_id == old.owner_attempt_id)
    {
        return Err(LivenessError::BindingMismatch);
    }
    Ok(())
}

fn validate_recovery_clock(
    checkpoint: &ExecutionCheckpoint,
    now: ClockSample,
) -> Result<(), LivenessError> {
    if now.utc < checkpoint.saved_at.utc {
        return Err(LivenessError::ClockMovedBackwards);
    }
    if now.monotonic.epoch == checkpoint.saved_at.monotonic.epoch {
        now.monotonic.since(checkpoint.saved_at.monotonic)?;
    }
    Ok(())
}

fn rebase(
    checkpoint: &ExecutionCheckpoint,
    binding: ExecutionBinding,
    now: ClockSample,
) -> Result<ExecutionLiveness, LivenessError> {
    // Rebase changes observation/control ownership, never the original physical
    // dispatch/acceptance facts, operation identity or producer cursors.
    let mut next = checkpoint.state.clone();
    next.binding = binding;
    next.revision = next
        .revision
        .checked_add(1)
        .ok_or(LivenessError::RevisionOverflow)?;
    next.last_reduced_at = now.monotonic;
    next.active_since = None;
    next.business = checkpoint
        .state
        .business
        .as_ref()
        .map(|value| {
            let remaining = if now.utc < checkpoint.saved_at.utc {
                Duration::ZERO
            } else {
                business_remaining(checkpoint, now)?
            };
            Ok::<_, LivenessError>(Arc::new(AnchoredDeadline {
                contract: value.contract.clone(),
                due: now.monotonic.checked_add(remaining)?,
            }))
        })
        .transpose()?;
    if matches!(next.phase, Phase::Waiting { .. }) {
        validate_recovery_clock(checkpoint, now)?;
    }
    Ok(next)
}

fn remaining_until(
    checkpoint: &ExecutionCheckpoint,
    due: LocalInstant,
    now: ClockSample,
) -> Result<Duration, LivenessError> {
    remaining_between(checkpoint.saved_at, due, now)
}

fn business_remaining(
    checkpoint: &ExecutionCheckpoint,
    now: ClockSample,
) -> Result<Duration, LivenessError> {
    let business = checkpoint
        .state
        .business
        .as_ref()
        .ok_or(LivenessError::InvalidCheckpoint)?;
    let anchored = remaining_until(checkpoint, business.due, now)?;
    if business.due.epoch == now.monotonic.epoch {
        return Ok(anchored);
    }
    let source_remaining =
        (i128::from(business.contract.at.unix_millis()) - i128::from(now.utc.unix_millis())).max(0);
    let source_remaining = Duration::from_millis(
        u64::try_from(source_remaining).map_err(|_| LivenessError::TimeOverflow)?,
    );
    Ok(anchored.min(source_remaining))
}

fn remaining_between(
    anchor: ClockSample,
    due: LocalInstant,
    now: ClockSample,
) -> Result<Duration, LivenessError> {
    if due.epoch == now.monotonic.epoch {
        return Ok(due.elapsed.saturating_sub(now.monotonic.elapsed));
    }
    if due.epoch != anchor.monotonic.epoch {
        return Err(LivenessError::InvalidCheckpoint);
    }
    let remaining_at_save = due.elapsed.saturating_sub(anchor.monotonic.elapsed);
    let elapsed_wall =
        (i128::from(now.utc.unix_millis()) - i128::from(anchor.utc.unix_millis())).max(0);
    let elapsed_wall = Duration::from_millis(
        u64::try_from(elapsed_wall).map_err(|_| LivenessError::TimeOverflow)?,
    );
    Ok(remaining_at_save.saturating_sub(elapsed_wall))
}

fn expire_business(
    state: &mut ExecutionLiveness,
    now: LocalInstant,
) -> Result<Action, LivenessError> {
    if state
        .business
        .as_ref()
        .is_some_and(|value| value.due.elapsed <= now.elapsed)
    {
        state.request_stop(StopReason::BusinessDeadlineExpired, now)
    } else {
        Ok(Action::None)
    }
}
