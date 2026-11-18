use std::sync::Arc;
use std::time::Duration;

use musteragent_identity::RunSeq;
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, BusinessDeadline, DurableWaitBinding, ExecutionBinding, ExecutionLivenessPolicy,
};

use crate::{ClockSample, LivenessError, LocalInstant};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StopReason {
    StartupExpired,
    IdleExpired,
    BusinessDeadlineExpired,
    UserCancelled,
    ParentCancelled,
    OwnershipLost,
    AuthorizationLost,
    SystemShutdown,
    RecoveryUnverified,
    /// Execution failed before a complete physical outcome was established.
    /// Preserve the original failure separately; cleanup still requires proof.
    ExecutionFailed,
    /// A finite dependency request timed out; distinct from execution inactivity.
    RequestTimedOut,
    CoordinationRequested,
    PostExitDrainExpired,
}

/// Internal supervision closure, not a business result or a public Run status.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Closure {
    TerminalObserved,
    StopConfirmed(StopReason),
    /// Local cleanup ended without proof of remote/process-tree termination.
    StopUnconfirmed(StopReason),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Phase {
    Starting {
        due: LocalInstant,
    },
    Running {
        due: LocalInstant,
    },
    /// Native execution ended; only the original ordered result/transport drain
    /// remains. Neither new output nor a restart can extend this fixed budget.
    Draining {
        ended_at: LocalInstant,
        due: LocalInstant,
    },
    /// A committed dependency/approval/backoff wait consumes no execution idle
    /// budget. Resuming preserves the unused budget instead of granting a reset.
    Waiting {
        binding: DurableWaitBinding,
        idle_remaining: Duration,
    },
    Stopping {
        reason: StopReason,
        cleanup_due: LocalInstant,
    },
    Closed(Closure),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Action {
    None,
    RequestStop(StopReason),
    /// Reconcile/quarantine using the original operation. Never retry a side
    /// effect or claim the external operation is stopped from this action alone.
    RecordUnconfirmedStop(StopReason),
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub(crate) struct Cursor {
    pub sequence: u64,
    pub work: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct AnchoredDeadline {
    pub contract: BusinessDeadline,
    pub due: LocalInstant,
}

/// Constant-space state of one execution. High-frequency activity reduction
/// does not allocate log entries or duplicate immutable policy strings.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecutionLiveness {
    pub(crate) binding: ExecutionBinding,
    pub(crate) policy: Arc<ExecutionLivenessPolicy>,
    pub(crate) business: Option<Arc<AnchoredDeadline>>,
    pub(crate) revision: u64,
    pub(crate) phase: Phase,
    pub(crate) dispatched_at: LocalInstant,
    pub(crate) accepted_at: Option<LocalInstant>,
    pub(crate) active_since: Option<LocalInstant>,
    pub(crate) last_wait_sequence: Option<RunSeq>,
    pub(crate) last_reduced_at: LocalInstant,
    pub(crate) last_activity_at: Option<LocalInstant>,
    pub(crate) cursors: [Cursor; ActivitySource::COUNT],
}

impl ExecutionLiveness {
    /// For an original, newly admitted dispatch only. Recovery/rebinding must
    /// restore committed facts; calling this constructor on restart is invalid.
    pub fn new(
        binding: ExecutionBinding,
        policy: Arc<ExecutionLivenessPolicy>,
        business: Option<BusinessDeadline>,
        now: ClockSample,
    ) -> Result<Self, LivenessError> {
        let business = business
            .map(|contract| {
                // The i128 subtraction handles the entire UtcTimestamp range.
                let remaining = (i128::from(contract.at.unix_millis())
                    - i128::from(now.utc.unix_millis()))
                .max(0);
                let millis = u64::try_from(remaining).map_err(|_| LivenessError::TimeOverflow)?;
                Ok::<_, LivenessError>(Arc::new(AnchoredDeadline {
                    due: now.monotonic.checked_add(Duration::from_millis(millis))?,
                    contract,
                }))
            })
            .transpose()?;
        Ok(Self {
            binding,
            phase: Phase::Starting {
                due: now.monotonic.checked_add(policy.startup_window())?,
            },
            policy,
            business,
            revision: 1,
            dispatched_at: now.monotonic,
            accepted_at: None,
            active_since: None,
            last_wait_sequence: None,
            last_reduced_at: now.monotonic,
            last_activity_at: None,
            cursors: [Cursor::default(); ActivitySource::COUNT],
        })
    }

    #[must_use]
    pub const fn binding(&self) -> ExecutionBinding {
        self.binding
    }

    #[must_use]
    pub fn policy(&self) -> &ExecutionLivenessPolicy {
        &self.policy
    }

    #[must_use]
    pub fn business_deadline(&self) -> Option<&BusinessDeadline> {
        self.business.as_ref().map(|value| &value.contract)
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    #[must_use]
    pub const fn last_activity_at(&self) -> Option<LocalInstant> {
        self.last_activity_at
    }

    #[must_use]
    pub const fn dispatched_at(&self) -> LocalInstant {
        self.dispatched_at
    }

    #[must_use]
    pub const fn accepted_at(&self) -> Option<LocalInstant> {
        self.accepted_at
    }

    /// Waiting has no idle due; its wake conditions remain owned by the Journal.
    /// Business control continues during a wait. Cleanup is independent of both.
    #[must_use]
    pub fn next_due(&self) -> Option<LocalInstant> {
        let phase_due = match self.phase {
            Phase::Starting { due } | Phase::Running { due } | Phase::Draining { due, .. } => {
                Some(due)
            }
            Phase::Waiting { .. } => None,
            Phase::Stopping { cleanup_due, .. } => return Some(cleanup_due),
            Phase::Closed(_) => return None,
        };
        let business_due = self.business.as_ref().map(|value| value.due);
        match (phase_due, business_due) {
            (Some(a), Some(b)) => Some(if a.elapsed <= b.elapsed { a } else { b }),
            (a, b) => a.or(b),
        }
    }
}
