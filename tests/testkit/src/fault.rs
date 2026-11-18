use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use thiserror::Error;

/// Named failure locations shared by persistence, scheduling and provider tests.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum FaultPoint {
    BeforeCommit,
    AfterCommitBeforeAck,
    LeaseAcquire,
    OutboxPublish,
    ProviderConnect,
    ProviderStream,
    SubscriptionDelivery,
}

/// Deterministic occurrence-based fault injector.
#[derive(Debug)]
pub struct FaultInjector {
    schedule: BTreeMap<FaultPoint, BTreeSet<u32>>,
    observations: Mutex<BTreeMap<FaultPoint, u32>>,
}

impl FaultInjector {
    #[must_use]
    pub fn new(schedule: impl IntoIterator<Item = (FaultPoint, u32)>) -> Self {
        let mut points = BTreeMap::<FaultPoint, BTreeSet<u32>>::new();
        for (point, occurrence) in schedule {
            assert!(occurrence > 0, "fault occurrence must be one-based");
            points.entry(point).or_default().insert(occurrence);
        }
        Self {
            schedule: points,
            observations: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn probe(&self, point: FaultPoint) -> Result<(), InjectedFault> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| InjectedFault::StatePoisoned)?;
        let occurrence = observations.entry(point).or_default();
        *occurrence = occurrence.saturating_add(1);
        if self
            .schedule
            .get(&point)
            .is_some_and(|scheduled| scheduled.contains(occurrence))
        {
            return Err(InjectedFault::Triggered {
                point,
                occurrence: *occurrence,
            });
        }
        Ok(())
    }

    pub fn observation_count(&self, point: FaultPoint) -> Result<u32, InjectedFault> {
        self.observations
            .lock()
            .map(|observations| observations.get(&point).copied().unwrap_or_default())
            .map_err(|_| InjectedFault::StatePoisoned)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum InjectedFault {
    #[error("injected fault at {point:?}, occurrence {occurrence}")]
    Triggered { point: FaultPoint, occurrence: u32 },
    #[error("fault injector state lock was poisoned")]
    StatePoisoned,
}

#[cfg(test)]
mod tests {
    use super::{FaultInjector, FaultPoint, InjectedFault};

    #[test]
    fn only_scheduled_occurrences_fail() {
        let injector = FaultInjector::new([(FaultPoint::ProviderStream, 2)]);
        assert_eq!(injector.probe(FaultPoint::ProviderStream), Ok(()));
        assert_eq!(
            injector.probe(FaultPoint::ProviderStream),
            Err(InjectedFault::Triggered {
                point: FaultPoint::ProviderStream,
                occurrence: 2,
            })
        );
        assert_eq!(injector.probe(FaultPoint::ProviderStream), Ok(()));
    }
}
