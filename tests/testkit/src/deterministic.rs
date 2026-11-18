use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use musteragent_identity::UtcTimestamp;
use musteragent_ledger::{Clock, LedgerError};
use uuid::Uuid;

/// Consumer-owned deterministic wall clock.
#[derive(Debug)]
pub struct ManualClock {
    unix_millis: AtomicI64,
}

impl ManualClock {
    #[must_use]
    pub const fn new(initial: UtcTimestamp) -> Self {
        Self {
            unix_millis: AtomicI64::new(initial.unix_millis()),
        }
    }

    pub fn set(&self, timestamp: UtcTimestamp) {
        self.unix_millis
            .store(timestamp.unix_millis(), Ordering::SeqCst);
    }

    pub fn advance_millis(&self, delta: i64) -> Result<UtcTimestamp, LedgerError> {
        if delta < 0 {
            return Err(LedgerError::ClockCannotMoveBackward);
        }
        let result = self
            .unix_millis
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(delta)
            })
            .map_err(|_| LedgerError::ClockOverflow)?
            .checked_add(delta)
            .ok_or(LedgerError::ClockOverflow)?;
        Ok(UtcTimestamp::from_unix_millis(result))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(self.unix_millis.load(Ordering::SeqCst))
    }
}

/// Stable UUID source for replay and failure tests.
#[derive(Debug)]
pub struct DeterministicIdFactory {
    namespace: u64,
    next: AtomicU64,
}

impl DeterministicIdFactory {
    #[must_use]
    pub const fn new(namespace: u64, first_sequence: u64) -> Self {
        Self {
            namespace,
            next: AtomicU64::new(first_sequence),
        }
    }

    #[must_use]
    pub fn next_uuid(&self) -> Uuid {
        let sequence = self.next.fetch_add(1, Ordering::SeqCst);
        Uuid::from_u128((u128::from(self.namespace) << 64) | u128::from(sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::DeterministicIdFactory;

    #[test]
    fn identical_seeds_produce_identical_sequences() {
        let left = DeterministicIdFactory::new(7, 1);
        let right = DeterministicIdFactory::new(7, 1);
        assert_eq!(left.next_uuid(), right.next_uuid());
        assert_eq!(left.next_uuid(), right.next_uuid());
    }
}
