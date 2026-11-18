//! Pure decisions. Stores persist these outcomes atomically with cursor changes.
use crate::{ConcurrencyPolicy, LatenessPolicy, RetrySpec};

/// Decision for a materialized occurrence. A replace must finish cancellation
/// before admission; it is never permission for overlapping execution.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum OccurrenceDecision {
    Run,
    ReplaceActive,
    SkipOverlap,
    SkipLate,
    FailLate,
}

/// Lateness is measured against the original due instant, including retries.
/// The boundary is inclusive; a manual occurrence still respects concurrency.
pub fn occurrence_decision(
    concurrency: ConcurrencyPolicy,
    lateness: LatenessPolicy,
    lateness_window_millis: u64,
    due_unix_millis: i64,
    now_unix_millis: i64,
    active: bool,
    manual: bool,
) -> OccurrenceDecision {
    let late = i128::from(now_unix_millis) - i128::from(due_unix_millis)
        > i128::from(lateness_window_millis);
    if late && !manual {
        match lateness {
            LatenessPolicy::Skip => return OccurrenceDecision::SkipLate,
            LatenessPolicy::Fail => return OccurrenceDecision::FailLate,
            LatenessPolicy::CatchUp => {}
        }
    }
    match (active, concurrency) {
        (true, ConcurrencyPolicy::Skip) => OccurrenceDecision::SkipOverlap,
        (true, ConcurrencyPolicy::Replace) => OccurrenceDecision::ReplaceActive,
        _ => OccurrenceDecision::Run,
    }
}

/// Invalid policies are rejected before storing a schedule or dispatching it.
#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum SchedulePolicyError {
    #[error("invalid retry policy")]
    InvalidRetry,
    #[error("retry timestamp overflow")]
    TimestampOverflow,
}

impl RetrySpec {
    /// The first execution counts as attempt one. Backoff is deterministic,
    /// capped and bounded; no jitter is introduced into persisted scheduling.
    pub fn validate(&self) -> Result<(), SchedulePolicyError> {
        if self.max_attempts == 0
            || self.max_attempts > 100
            || self.base_backoff_millis == 0
            || self.factor == 0
            || self.max_backoff_millis < self.base_backoff_millis
            || self.max_backoff_millis > 86_400_000
        {
            return Err(SchedulePolicyError::InvalidRetry);
        }
        Ok(())
    }

    /// Returns `None` once the configured attempt budget has been consumed.
    pub fn retry_at(
        &self,
        completed_attempts: u16,
        now_unix_millis: i64,
    ) -> Result<Option<i64>, SchedulePolicyError> {
        self.validate()?;
        if completed_attempts == 0 {
            return Err(SchedulePolicyError::InvalidRetry);
        }
        if completed_attempts >= self.max_attempts {
            return Ok(None);
        }
        let mut delay = self.base_backoff_millis;
        for _ in 1..completed_attempts {
            delay = delay
                .saturating_mul(u64::from(self.factor))
                .min(self.max_backoff_millis);
        }
        let delay = i64::try_from(delay).map_err(|_| SchedulePolicyError::TimestampOverflow)?;
        now_unix_millis
            .checked_add(delay)
            .map(Some)
            .ok_or(SchedulePolicyError::TimestampOverflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_matrix_respects_lateness_then_concurrency() {
        for concurrency in [
            ConcurrencyPolicy::Skip,
            ConcurrencyPolicy::Allow,
            ConcurrencyPolicy::Replace,
        ] {
            for lateness in [
                LatenessPolicy::CatchUp,
                LatenessPolicy::Skip,
                LatenessPolicy::Fail,
            ] {
                for active in [false, true] {
                    let timely = occurrence_decision(
                        concurrency,
                        lateness,
                        100,
                        1_000,
                        1_100,
                        active,
                        false,
                    );
                    let expected = match (active, concurrency) {
                        (true, ConcurrencyPolicy::Skip) => OccurrenceDecision::SkipOverlap,
                        (true, ConcurrencyPolicy::Replace) => OccurrenceDecision::ReplaceActive,
                        _ => OccurrenceDecision::Run,
                    };
                    assert_eq!(timely, expected);
                    assert_eq!(
                        occurrence_decision(concurrency, lateness, 100, 1_000, 1_101, active, true),
                        expected
                    );
                    assert_eq!(
                        occurrence_decision(
                            concurrency,
                            lateness,
                            100,
                            1_000,
                            1_101,
                            active,
                            false
                        ),
                        match lateness {
                            LatenessPolicy::CatchUp => expected,
                            LatenessPolicy::Skip => OccurrenceDecision::SkipLate,
                            LatenessPolicy::Fail => OccurrenceDecision::FailLate,
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn retry_budget_backoff_cap_and_overflow_are_explicit() {
        let retry = RetrySpec {
            max_attempts: 6,
            base_backoff_millis: 100,
            factor: 3,
            max_backoff_millis: 800,
        };
        assert_eq!(
            (1..=6)
                .map(|attempt| retry.retry_at(attempt, 1_000).expect("valid retry"))
                .collect::<Vec<_>>(),
            vec![
                Some(1_100),
                Some(1_300),
                Some(1_800),
                Some(1_800),
                Some(1_800),
                None
            ]
        );
        assert_eq!(
            retry.retry_at(1, i64::MAX),
            Err(SchedulePolicyError::TimestampOverflow)
        );
        assert_eq!(retry.retry_at(0, 0), Err(SchedulePolicyError::InvalidRetry));
    }
}
