use musteragent_continuous_clock::{ContinuousClock, ContinuousTimeSource};
use musteragent_execution_liveness::{ClockSample, LivenessError, LivenessHomeClock, LocalInstant};
use musteragent_identity::{HostClockEpochId, UtcTimestamp};
use std::{sync::Mutex, time::Duration};

struct EpochState {
    epoch: HostClockEpochId,
    last: Option<Duration>,
}

/// Construct once per authoritative Home process and share the same Arc with
/// first-receipt observers and Journals. A restart creates a new epoch. Native
/// rollback rotates the epoch, forcing the existing durable recovery protocol;
/// it never resets a checkpoint or grants an execution a new idle window.
pub struct SystemLivenessHomeClock<S = ContinuousClock> {
    source: S,
    state: Mutex<EpochState>,
}
impl Default for SystemLivenessHomeClock {
    fn default() -> Self {
        Self::with_source(ContinuousClock::default())
    }
}
impl<S: ContinuousTimeSource> SystemLivenessHomeClock<S> {
    pub fn with_source(source: S) -> Self {
        Self {
            source,
            state: Mutex::new(EpochState {
                epoch: HostClockEpochId::generate(),
                last: None,
            }),
        }
    }
}
impl<S: ContinuousTimeSource> LivenessHomeClock for SystemLivenessHomeClock<S> {
    fn sample(&self) -> Result<ClockSample, LivenessError> {
        // Serialize raw sampling as well as comparison: concurrent samples
        // may otherwise acquire this lock in the reverse observation order.
        let mut state = self
            .state
            .lock()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        let raw = self
            .source
            .sample()
            .map_err(|_| LivenessError::ClockUnavailable)?;
        if state.last.is_some_and(|last| raw.elapsed < last) {
            state.epoch = HostClockEpochId::generate();
        }
        state.last = Some(raw.elapsed);
        Ok(ClockSample {
            monotonic: LocalInstant {
                epoch: state.epoch,
                elapsed: raw.elapsed,
            },
            utc: UtcTimestamp::from_unix_millis(raw.unix_millis),
        })
    }
}
