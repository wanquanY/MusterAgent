//! Immutable recovery semantics. A persistent task never spends its lifetime
//! by waiting for an unavailable dependency; only its probe frequency changes.

use std::num::NonZeroU32;
use std::time::Duration;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RecoveryPolicySnapshot {
    /// Historical snapshots retain their exact finite recovery contract.
    Bounded { max_attempts: NonZeroU32 },
    /// A scheduled occurrence resumes the same durable Run using its exact
    /// configured retry budget and deterministic capped backoff.
    BoundedBackoff {
        spec: musteragent_schedule::RetrySpec,
    },
    /// Fast retries transition to durable dependency probes without ending
    /// the Run. The scheduler releases its lease between every probe.
    Persistent {
        fast_attempts: NonZeroU32,
        min_probe_interval: Duration,
        max_probe_interval: Duration,
    },
}

impl RecoveryPolicySnapshot {
    /// A new physical invocation may continue from a durably sealed partial
    /// response. This never permits replaying a tool or appending to its output.
    #[must_use]
    pub const fn permits_sealed_continuation(self, replay_barrier_observed: bool) -> bool {
        !replay_barrier_observed || matches!(self, Self::Persistent { .. })
    }

    #[must_use]
    pub fn production_default() -> Self {
        Self::Persistent {
            fast_attempts: NonZeroU32::new(8).expect("non-zero constant"),
            min_probe_interval: Duration::from_secs(120),
            max_probe_interval: Duration::from_secs(15 * 60),
        }
    }

    #[must_use]
    pub const fn legacy_default() -> Self {
        Self::Bounded {
            max_attempts: NonZeroU32::new(8).expect("non-zero constant"),
        }
    }

    #[must_use]
    pub fn is_valid(self) -> bool {
        match self {
            Self::BoundedBackoff { spec } => spec.validate().is_ok(),
            Self::Bounded { max_attempts } => max_attempts.get() <= 100,
            Self::Persistent {
                fast_attempts,
                min_probe_interval,
                max_probe_interval,
            } => {
                fast_attempts.get() <= 100
                    && min_probe_interval >= Duration::from_secs(60)
                    && min_probe_interval <= max_probe_interval
                    && max_probe_interval <= Duration::from_secs(24 * 60 * 60)
                    && min_probe_interval.subsec_nanos() == 0
                    && max_probe_interval.subsec_nanos() == 0
            }
        }
    }

    /// `previous_failures` excludes the physical operation currently failing.
    #[must_use]
    pub const fn permits_retry(self, previous_failures: u32) -> bool {
        match self {
            Self::BoundedBackoff { spec } => {
                previous_failures.saturating_add(1) < spec.max_attempts as u32
            }
            Self::Bounded { max_attempts } => {
                previous_failures.saturating_add(1) < max_attempts.get()
            }
            Self::Persistent { .. } => true,
        }
    }

    #[must_use]
    pub const fn terminal_limit(self) -> Option<NonZeroU32> {
        match self {
            Self::BoundedBackoff { spec } => NonZeroU32::new(spec.max_attempts as u32),
            Self::Bounded { max_attempts } => Some(max_attempts),
            Self::Persistent { .. } => None,
        }
    }

    /// Inclusive stable-jitter window. Long-outage probes have a positive
    /// minimum, so even the smallest random sample cannot recreate hot loops.
    #[must_use]
    pub fn delay_window_ms(self, failure_number: NonZeroU32) -> (u64, u64) {
        if let Self::BoundedBackoff { spec } = self {
            let mut delay = spec.base_backoff_millis;
            for _ in 1..failure_number.get().min(100) {
                delay = delay
                    .saturating_mul(u64::from(spec.factor))
                    .min(spec.max_backoff_millis);
            }
            return (delay, delay);
        }
        if let Self::Persistent {
            fast_attempts,
            min_probe_interval,
            max_probe_interval,
        } = self
            && failure_number.get() >= fast_attempts.get()
        {
            let exponent = failure_number
                .get()
                .saturating_sub(fast_attempts.get())
                .min(16);
            let floor = min_probe_interval.as_secs().saturating_mul(1_000);
            let cap = floor
                .saturating_mul(1_u64 << exponent)
                .min(max_probe_interval.as_secs().saturating_mul(1_000));
            return (floor, cap);
        }
        let exponent = failure_number.get().saturating_sub(1).min(6);
        (250, 1_000_u64.saturating_mul(1_u64 << exponent))
    }
}

impl From<NonZeroU32> for RecoveryPolicySnapshot {
    fn from(max_attempts: NonZeroU32) -> Self {
        Self::Bounded { max_attempts }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduled_backoff_and_budget_apply_to_one_durable_run() {
        let policy = RecoveryPolicySnapshot::BoundedBackoff {
            spec: musteragent_schedule::RetrySpec {
                max_attempts: 4,
                base_backoff_millis: 100,
                factor: 3,
                max_backoff_millis: 500,
            },
        };
        assert!(policy.is_valid());
        assert_eq!(
            (1..=4)
                .map(|n| policy.delay_window_ms(NonZeroU32::new(n).unwrap()))
                .collect::<Vec<_>>(),
            vec![(100, 100), (300, 300), (500, 500), (500, 500)]
        );
        assert!(policy.permits_retry(2));
        assert!(!policy.permits_retry(3));
        assert!(!policy.permits_sealed_continuation(true));
    }

    #[test]
    fn prolonged_outage_never_exhausts_a_persistent_run() {
        let policy = RecoveryPolicySnapshot::production_default();
        assert!(policy.is_valid());
        for previous in [0, 7, 8, 100, 10_000, u32::MAX] {
            assert!(policy.permits_retry(previous));
        }
        assert_eq!(policy.terminal_limit(), None);
    }

    #[test]
    fn legacy_failure_limit_remains_exact() {
        let policy = RecoveryPolicySnapshot::legacy_default();
        assert!(policy.permits_retry(6));
        assert!(!policy.permits_retry(7));
        assert!(!policy.permits_retry(u32::MAX));
        assert_eq!(
            policy
                .terminal_limit()
                .expect("valid recovery test fixture")
                .get(),
            8
        );
    }

    #[test]
    fn sustained_outage_releases_fast_retries_for_bounded_slow_probes() {
        let policy = RecoveryPolicySnapshot::production_default();
        assert_eq!(policy.delay_window_ms(NonZeroU32::MIN), (250, 1_000));
        assert_eq!(
            policy.delay_window_ms(NonZeroU32::new(8).expect("valid recovery test fixture")),
            (120_000, 120_000),
        );
        assert_eq!(
            policy.delay_window_ms(NonZeroU32::new(u32::MAX).expect("valid recovery test fixture")),
            (120_000, 900_000),
        );
    }

    #[test]
    fn invalid_probe_intervals_cannot_enter_a_signed_policy() {
        let policy = |min, max| RecoveryPolicySnapshot::Persistent {
            fast_attempts: NonZeroU32::new(8).expect("valid recovery test fixture"),
            min_probe_interval: Duration::from_secs(min),
            max_probe_interval: Duration::from_secs(max),
        };
        assert!(!policy(0, 900).is_valid());
        assert!(!policy(900, 120).is_valid());
        assert!(!policy(120, 86_401).is_valid());
        assert!(policy(120, 900).is_valid());
    }
}
