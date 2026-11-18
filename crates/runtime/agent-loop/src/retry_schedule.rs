use musteragent_identity::UtcTimestamp;
use musteragent_run::AttemptContext;

use musteragent_runtime_snapshot::RecoveryPolicySnapshot;
// Last instant representable by the cross-language google.protobuf.Timestamp
// contract. Internal i64 milliseconds have a wider domain than wire readers.
const MAX_SCHEDULABLE_TIMESTAMP_MILLIS: i64 = 253_402_300_799_999;
const MAX_PROVIDER_RETRY_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

/// A provider-controlled duration cannot overflow a durable transaction. A
/// wait beyond the timestamp domain saturates at its last representable instant.
#[must_use]
pub fn replacement_attempt_not_before(
    now: UtcTimestamp,
    attempt: &AttemptContext,
    failure_number: std::num::NonZeroU32,
    provider_retry_after_ms: Option<u64>,
    policy: RecoveryPolicySnapshot,
) -> UtcTimestamp {
    let delay =
        replacement_attempt_delay_ms(attempt, failure_number, provider_retry_after_ms, policy);
    let timestamp = i128::from(now.unix_millis()) + i128::from(delay);
    UtcTimestamp::from_unix_millis(
        i64::try_from(timestamp.min(i128::from(MAX_SCHEDULABLE_TIMESTAMP_MILLIS)))
            .unwrap_or(MAX_SCHEDULABLE_TIMESTAMP_MILLIS),
    )
}

/// Resolves the one durable replacement-Attempt delay used by every Runtime
/// persistence implementation.
///
/// The exponential cap is intentionally deterministic for a fenced Attempt:
/// retrying a conflicted transaction must reproduce the same schedule. A
/// stable full-jitter sample spreads independent Runs without introducing a
/// second random authority, while a Provider `Retry-After` remains a strict
/// lower bound.
#[must_use]
pub fn replacement_attempt_delay_ms(
    attempt: &AttemptContext,
    failure_number: std::num::NonZeroU32,
    provider_retry_after_ms: Option<u64>,
    policy: RecoveryPolicySnapshot,
) -> u64 {
    let (floor, cap) = policy.delay_window_ms(failure_number);
    let jitter_span = cap.saturating_sub(floor).saturating_add(1);
    let policy_delay = floor.saturating_add(stable_attempt_hash(attempt) % jitter_span.max(1));
    let provider_delay = match policy {
        RecoveryPolicySnapshot::Bounded { .. } | RecoveryPolicySnapshot::BoundedBackoff { .. } => {
            provider_retry_after_ms
                .unwrap_or_default()
                .min(MAX_PROVIDER_RETRY_AFTER_MS)
        }
        RecoveryPolicySnapshot::Persistent { .. } => provider_retry_after_ms.unwrap_or_default(),
    };
    policy_delay.max(provider_delay)
}

fn stable_attempt_hash(attempt: &AttemptContext) -> u64 {
    // FNV-1a is sufficient here: this is scheduling dispersion, not a security
    // boundary. Run and Attempt identity keep the sample stable and distinct.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in attempt
        .run_id
        .into_uuid()
        .as_bytes()
        .iter()
        .chain(attempt.attempt_id.into_uuid().as_bytes())
        .chain(attempt.attempt_number.get().to_le_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use musteragent_identity::{
        AttemptId, AttemptNumber, FencingToken, LeaseId, RunId, UtcTimestamp,
    };
    use musteragent_run::WorkerId;

    use super::*;

    const MIN_JITTER_DELAY_MS: u64 = 250;
    const BASE_DELAY_MS: u64 = 1_000;
    const MAX_POLICY_DELAY_MS: u64 = 64_000;

    fn replacement_attempt_delay_ms(
        attempt: &AttemptContext,
        failure_number: std::num::NonZeroU32,
        provider_retry_after_ms: Option<u64>,
    ) -> u64 {
        super::replacement_attempt_delay_ms(
            attempt,
            failure_number,
            provider_retry_after_ms,
            RecoveryPolicySnapshot::legacy_default(),
        )
    }

    #[test]
    fn persistent_probes_honor_provider_waits_longer_than_a_day() {
        let delay = super::replacement_attempt_delay_ms(
            &attempt(400),
            std::num::NonZeroU32::new(100).expect("valid recovery test fixture"),
            Some(3 * 86_400_000),
            RecoveryPolicySnapshot::production_default(),
        );
        assert_eq!(delay, 3 * 86_400_000);
    }

    fn attempt(number: u32) -> AttemptContext {
        AttemptContext {
            run_id: RunId::generate(),
            attempt_id: AttemptId::generate(),
            attempt_number: AttemptNumber::new(number).expect("number"),
            worker_id: WorkerId::new("retry-schedule-test").expect("worker"),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(1).expect("fence"),
            lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
        }
    }

    #[test]
    fn first_failure_after_many_normal_wakeups_uses_the_initial_delay_cap() {
        let delay = replacement_attempt_delay_ms(&attempt(100), std::num::NonZeroU32::MIN, None);
        assert!((250..=1_000).contains(&delay));
    }

    #[test]
    fn delay_is_stable_bounded_and_honors_provider_floor() {
        let first_attempt = attempt(1);
        let first = replacement_attempt_delay_ms(&first_attempt, std::num::NonZeroU32::MIN, None);
        assert!((MIN_JITTER_DELAY_MS..=BASE_DELAY_MS).contains(&first));
        assert_eq!(
            first,
            replacement_attempt_delay_ms(&first_attempt, std::num::NonZeroU32::MIN, None)
        );

        let seventh = replacement_attempt_delay_ms(
            &attempt(7),
            std::num::NonZeroU32::new(7).expect("failures"),
            None,
        );
        assert!((MIN_JITTER_DELAY_MS..=MAX_POLICY_DELAY_MS).contains(&seventh));
        assert_eq!(
            replacement_attempt_delay_ms(
                &attempt(2),
                std::num::NonZeroU32::new(2).expect("failures"),
                Some(45_000)
            ),
            45_000
        );
        assert_eq!(
            replacement_attempt_delay_ms(
                &attempt(2),
                std::num::NonZeroU32::new(2).expect("failures"),
                Some(u64::MAX)
            ),
            MAX_PROVIDER_RETRY_AFTER_MS
        );
    }
    #[test]
    fn thirty_virtual_days_keep_the_task_recoverable_without_a_retry_storm() {
        let policy = RecoveryPolicySnapshot::production_default();
        let mut now = UtcTimestamp::from_unix_millis(0);
        let mut previous_failures = 0;
        while now.unix_millis() < 30 * 86_400_000 {
            assert!(policy.permits_retry(previous_failures));
            let number = std::num::NonZeroU32::new(previous_failures + 1).expect("failure number");
            let next = super::replacement_attempt_not_before(
                now,
                &attempt(number.get()),
                number,
                None,
                policy,
            );
            let delay = next.unix_millis() - now.unix_millis();
            assert!(delay > 0 && delay <= 900_000);
            if previous_failures >= 7 {
                assert!(delay >= 120_000);
            }
            now = next;
            previous_failures += 1;
        }
        assert!(previous_failures > 100);
        assert_eq!(policy.terminal_limit(), None);
    }

    #[test]
    fn unrepresentable_provider_wait_does_not_abort_the_failure_transaction() {
        assert_eq!(
            super::replacement_attempt_not_before(
                UtcTimestamp::from_unix_millis(1_000),
                &attempt(1),
                std::num::NonZeroU32::MIN,
                Some(u64::MAX),
                RecoveryPolicySnapshot::production_default(),
            ),
            UtcTimestamp::from_unix_millis(MAX_SCHEDULABLE_TIMESTAMP_MILLIS)
        );
    }
}
