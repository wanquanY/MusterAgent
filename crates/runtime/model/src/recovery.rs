//! Persist provider cooldowns as absolute time across physical replacements.
use musteragent_identity::UtcTimestamp;

/// Saturates at the common database timestamp range; never shortens a delay.
#[must_use]
pub fn model_retry_not_before(
    finished: UtcTimestamp,
    delay_ms: Option<u64>,
) -> Option<UtcTimestamp> {
    delay_ms.filter(|delay| *delay > 0).map(|delay| {
        const LAST_MILLISECOND: i64 = 253_402_300_799_999;
        UtcTimestamp::from_unix_millis(
            finished
                .unix_millis()
                .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
                .min(LAST_MILLISECOND)
                .max(finished.unix_millis()),
        )
    })
}

#[must_use]
pub fn remaining_model_retry_delay(now: UtcTimestamp, due: Option<UtcTimestamp>) -> Option<u64> {
    due.and_then(|due| u64::try_from(due.unix_millis().saturating_sub(now.unix_millis())).ok())
        .filter(|delay| *delay > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cooldown_survives_elapsed_time_without_extending_the_original_deadline() {
        let start = UtcTimestamp::from_unix_millis(1_000);
        let due = model_retry_not_before(start, Some(180_000));
        assert_eq!(
            remaining_model_retry_delay(UtcTimestamp::from_unix_millis(61_000), due),
            Some(120_000)
        );
        assert_eq!(
            remaining_model_retry_delay(UtcTimestamp::from_unix_millis(181_000), due),
            None
        );
        assert_eq!(
            model_retry_not_before(start, Some(u64::MAX)).map(UtcTimestamp::unix_millis),
            Some(253_402_300_799_999)
        );
        assert_eq!(model_retry_not_before(start, Some(0)), None);
    }
}
