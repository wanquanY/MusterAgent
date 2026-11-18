use std::time::Duration;

use musteragent_identity::{HostClockEpochId, UtcTimestamp};

use crate::LivenessError;

/// Trusted Home monotonic time within a single clock epoch. This is not a
/// remote timestamp or a serialized std/Tokio Instant. Epoch changes require
/// the durable recovery protocol, never a fresh call to `ExecutionLiveness::new`.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LocalInstant {
    pub epoch: HostClockEpochId,
    pub elapsed: Duration,
}

impl LocalInstant {
    pub fn checked_add(self, window: Duration) -> Result<Self, LivenessError> {
        Ok(Self {
            epoch: self.epoch,
            elapsed: self
                .elapsed
                .checked_add(window)
                .ok_or(LivenessError::TimeOverflow)?,
        })
    }

    pub fn since(self, earlier: Self) -> Result<Duration, LivenessError> {
        if self.epoch != earlier.epoch {
            return Err(LivenessError::ClockEpochChanged);
        }
        self.elapsed
            .checked_sub(earlier.elapsed)
            .ok_or(LivenessError::ClockMovedBackwards)
    }
}

/// UTC is sampled once to anchor an explicit business deadline; subsequent
/// in-process timer decisions only use the Home monotonic clock.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ClockSample {
    pub monotonic: LocalInstant,
    pub utc: UtcTimestamp,
}

impl ClockSample {
    /// Millisecond database index hint, rounded upward. The actual stop decision
    /// always reduces the current state under its Home monotonic clock/fence.
    pub fn index_time(self, instant: LocalInstant) -> Result<UtcTimestamp, LivenessError> {
        if instant.epoch != self.monotonic.epoch {
            return Err(LivenessError::ClockEpochChanged);
        }
        let nanos = i128::try_from(instant.elapsed.as_nanos())
            .map_err(|_| LivenessError::TimeOverflow)?
            - i128::try_from(self.monotonic.elapsed.as_nanos())
                .map_err(|_| LivenessError::TimeOverflow)?;
        let difference = nanos.div_euclid(1_000_000) + i128::from(nanos.rem_euclid(1_000_000) != 0);
        let millis = i128::from(self.utc.unix_millis())
            .checked_add(difference)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(LivenessError::TimeOverflow)?;
        Ok(UtcTimestamp::from_unix_millis(millis))
    }
}

/// Owned by the authoritative Home process. Journal adapters sample after
/// acquiring their commit lane, never accept a caller/worker's reduction time.
/// An observer samples this same clock at first receipt of task-bound work.
pub trait LivenessHomeClock: Send + Sync {
    fn sample(&self) -> Result<ClockSample, LivenessError>;
}
