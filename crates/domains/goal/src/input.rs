//! Business input progress is independent of provider timestamps and wake
//! hints. Persistence serializes this state with the original Goal/Pursuit.
use crate::StandingGoalContractError;

pub const MAX_GOAL_INPUT_BATCH: u16 = 100;

/// Read projection of the original acceptance/settlement cursor. These positions
/// are never writable through Goal definition or status commands.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct GoalInputProgress {
    pub accepted: u64,
    pub consumed: u64,
}

impl GoalInputProgress {
    pub fn pending(self) -> u64 {
        self.accepted - self.consumed
    }

    pub fn validate(self) -> Result<(), StandingGoalContractError> {
        GoalInputCursor {
            accepted: self.accepted,
            consumed: self.consumed,
            batch: None,
        }
        .validate()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GoalInputBatchWindow {
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct GoalInputCursor {
    pub accepted: u64,
    pub consumed: u64,
    /// Frozen across claim expiry and worker restart. New arrivals belong to
    /// the next batch, even if they concern the same provider resource.
    pub batch: Option<GoalInputBatchWindow>,
}

impl GoalInputCursor {
    pub fn validate(self) -> Result<(), StandingGoalContractError> {
        if self.accepted > i64::MAX as u64
            || self.consumed > self.accepted
            || self.batch.is_some_and(|b| {
                b.first != self.consumed + 1
                    || b.last < b.first
                    || b.last > self.accepted
                    || b.last - b.first >= u64::from(MAX_GOAL_INPUT_BATCH)
            })
        {
            return Err(StandingGoalContractError::InvalidInputCursor);
        }
        Ok(())
    }

    /// Called after the source-input uniqueness check in the same transaction.
    /// A redelivery returns its original acceptance sequence.
    pub fn accept(&mut self) -> Result<u64, StandingGoalContractError> {
        self.validate()?;
        let next = self
            .accepted
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or(StandingGoalContractError::InvalidInputCursor)?;
        self.accepted = next;
        Ok(next)
    }

    pub fn freeze(
        &mut self,
        limit: u16,
    ) -> Result<Option<GoalInputBatchWindow>, StandingGoalContractError> {
        self.validate()?;
        if limit == 0 || limit > MAX_GOAL_INPUT_BATCH {
            return Err(StandingGoalContractError::InvalidInputCursor);
        }
        if self.batch.is_none() && self.consumed < self.accepted {
            self.batch = Some(GoalInputBatchWindow {
                first: self.consumed + 1,
                last: self.accepted.min(self.consumed + u64::from(limit)),
            });
        }
        Ok(self.batch)
    }

    /// Belongs to the original fenced evaluation transaction, after output
    /// effects and terminal Run state have become authoritative.
    pub fn settle(
        &mut self,
        expected: GoalInputBatchWindow,
    ) -> Result<(), StandingGoalContractError> {
        self.validate()?;
        if self.batch != Some(expected) {
            return Err(StandingGoalContractError::InvalidInputCursor);
        }
        self.consumed = expected.last;
        self.batch = None;
        Ok(())
    }

    #[must_use]
    pub const fn pending(self) -> bool {
        self.accepted > self.consumed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrivals_before_wait_and_during_a_run_remain_pending() {
        let mut cursor = GoalInputCursor::default();
        assert!(!cursor.pending());
        cursor.accept().unwrap();
        assert!(cursor.pending());
        let original = cursor.freeze(100).unwrap().unwrap();
        cursor.accept().unwrap();
        assert_eq!(cursor.freeze(1).unwrap(), Some(original));
        cursor.settle(original).unwrap();
        assert!(cursor.pending());
        let next = cursor.freeze(100).unwrap().unwrap();
        assert_eq!(next, GoalInputBatchWindow { first: 2, last: 2 });
        assert!(cursor.settle(original).is_err());
        cursor.settle(next).unwrap();
        assert!(!cursor.pending());
    }

    #[test]
    fn batches_are_bounded_and_invalid_progress_cannot_skip_inputs() {
        let mut cursor = GoalInputCursor {
            accepted: 201,
            ..Default::default()
        };
        let batch = cursor.freeze(100).unwrap().unwrap();
        assert_eq!(batch.last, 100);
        assert!(
            cursor
                .settle(GoalInputBatchWindow {
                    first: 1,
                    last: 201
                })
                .is_err()
        );
        assert!(cursor.freeze(0).is_err());
        cursor.consumed = 202;
        assert!(cursor.validate().is_err());
        let mut exhausted = GoalInputCursor {
            accepted: i64::MAX as u64,
            consumed: i64::MAX as u64,
            batch: None,
        };
        assert!(exhausted.accept().is_err());
        assert_eq!(exhausted.accepted, i64::MAX as u64);
    }
}
