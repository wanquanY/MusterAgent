//! Both persistence adapters observe the same canonical Run lifecycle. A Run
//! can be cancelled or fail in Queued, without ever acquiring an Attempt.
use super::GoalRunTerminalStatus;

impl GoalRunTerminalStatus {
    /// Returns execution duration, not the Goal/Pursuit's active-time budget.
    ///
    /// Missing start time is valid only for Failed/Cancelled: the original Run
    /// state machine permits those outcomes before the first Claim. It means no
    /// execution interval, not a fabricated start at admission or completion.
    /// A missing finish, impossible start, reversed interval or overflow remains
    /// corrupt and must not be clamped into a successful observation.
    #[must_use]
    pub fn execution_elapsed_millis(
        self,
        started_at_millis: Option<i64>,
        finished_at_millis: Option<i64>,
    ) -> Option<u64> {
        let finished_at = finished_at_millis?;
        match started_at_millis {
            Some(started_at) => u64::try_from(finished_at.checked_sub(started_at)?).ok(),
            None if matches!(self, Self::Cancelled | Self::Failed) => Some(0),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GoalRunTerminalStatus as Status;

    const STATUSES: [Status; 4] = [
        Status::Completed,
        Status::Failed,
        Status::Cancelled,
        Status::Interrupted,
    ];

    #[test]
    fn cancelled_before_first_claim_has_no_execution_interval() {
        assert_eq!(
            Status::Cancelled.execution_elapsed_millis(None, Some(350)),
            Some(0)
        );
    }

    #[test]
    fn failed_before_first_claim_has_no_execution_interval() {
        assert_eq!(
            Status::Failed.execution_elapsed_millis(None, Some(350)),
            Some(0)
        );
    }

    #[test]
    fn completion_and_interruption_still_require_a_started_attempt() {
        assert_eq!(
            Status::Completed.execution_elapsed_millis(None, Some(350)),
            None
        );
        assert_eq!(
            Status::Interrupted.execution_elapsed_millis(None, Some(350)),
            None
        );
    }

    #[test]
    fn every_terminal_observation_requires_its_original_finish_time() {
        for status in STATUSES {
            assert_eq!(status.execution_elapsed_millis(None, None), None);
            assert_eq!(status.execution_elapsed_millis(Some(100), None), None);
        }
    }

    #[test]
    fn started_terminal_intervals_preserve_the_exact_duration() {
        for status in STATUSES {
            assert_eq!(
                status.execution_elapsed_millis(Some(100), Some(117)),
                Some(17)
            );
            assert_eq!(
                status.execution_elapsed_millis(Some(100), Some(100)),
                Some(0)
            );
        }
    }

    #[test]
    fn invalid_intervals_are_not_saturated_or_clamped() {
        for status in STATUSES {
            assert_eq!(status.execution_elapsed_millis(Some(100), Some(99)), None);
            assert_eq!(
                status.execution_elapsed_millis(Some(i64::MIN), Some(i64::MAX)),
                None
            );
        }
    }
}
