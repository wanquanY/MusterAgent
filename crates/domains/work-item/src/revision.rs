//! Goal, control and observed-state revisions are intentionally not convertible
//! from ProjectRevision. Legacy Project revisions also count assignments, so
//! they cannot establish historical goal or control revisions.

use std::num::NonZeroU64;
use thiserror::Error;

macro_rules! revision {
    ($name:ident) => {
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub const fn new(value: u64) -> Result<Self, WorkItemRevisionError> {
                match NonZeroU64::new(value) {
                    Some(value) => Ok(Self(value)),
                    None => Err(WorkItemRevisionError::Zero),
                }
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }

            fn next(self) -> Result<Self, WorkItemRevisionError> {
                self.get()
                    .checked_add(1)
                    .ok_or(WorkItemRevisionError::Exhausted)
                    .and_then(Self::new)
            }
        }
    };
}

revision!(WorkItemGoalRevision);
revision!(WorkItemControlRevision);
revision!(WorkItemStateRevision);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct WorkItemRevisions {
    pub goal: WorkItemGoalRevision,
    pub control: WorkItemControlRevision,
    pub state: WorkItemStateRevision,
}

impl WorkItemRevisions {
    /// An explicit baseline for a new task or the owner's migration boundary.
    /// This cannot reconstruct revision history for a legacy Project record.
    #[must_use]
    pub const fn initial() -> Self {
        Self {
            goal: WorkItemGoalRevision(NonZeroU64::MIN),
            control: WorkItemControlRevision(NonZeroU64::MIN),
            state: WorkItemStateRevision(NonZeroU64::MIN),
        }
    }

    pub fn advance_goal(
        self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
    ) -> Result<Self, WorkItemRevisionError> {
        self.check_command(expected_goal, expected_control)?;
        Ok(Self {
            goal: self.goal.next()?,
            state: self.state.next()?,
            ..self
        })
    }

    /// Records an accepted control revision, not that execution has stopped.
    /// The command owner persists the actual control intent in the same commit.
    pub fn advance_control(
        self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
    ) -> Result<Self, WorkItemRevisionError> {
        self.check_command(expected_goal, expected_control)?;
        Ok(Self {
            control: self.control.next()?,
            state: self.state.next()?,
            ..self
        })
    }

    pub fn advance_state(
        self,
        expected_state: WorkItemStateRevision,
    ) -> Result<Self, WorkItemRevisionError> {
        if expected_state != self.state {
            return Err(WorkItemRevisionError::StateConflict);
        }
        Ok(Self {
            state: self.state.next()?,
            ..self
        })
    }

    pub(super) fn check_command(
        self,
        expected_goal: WorkItemGoalRevision,
        expected_control: WorkItemControlRevision,
    ) -> Result<(), WorkItemRevisionError> {
        if expected_goal != self.goal {
            return Err(WorkItemRevisionError::GoalConflict);
        }
        if expected_control != self.control {
            return Err(WorkItemRevisionError::ControlConflict);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum WorkItemRevisionError {
    #[error("Work Item revision must be non-zero")]
    Zero,
    #[error("Work Item revision exhausted")]
    Exhausted,
    #[error("Work Item goal revision is stale")]
    GoalConflict,
    #[error("Work Item control revision is stale")]
    ControlConflict,
    #[error("Work Item state revision is stale")]
    StateConflict,
}
