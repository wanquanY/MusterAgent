//! Suspend-inclusive OS time. No execution, lease, epoch or timeout policy.
#![forbid(unsafe_code)]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContinuousSample {
    pub elapsed: Duration,
    pub unix_millis: i64,
}
#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
#[error("continuous clock is unavailable or outside its representable range")]
pub struct ClockError;
pub trait ContinuousTimeSource: Send + Sync {
    fn sample(&self) -> Result<ContinuousSample, ClockError>;
}

/// Owners supply a process epoch and check ordering. Unsupported platforms fail.
#[derive(Default)]
pub struct ContinuousClock {
    #[cfg(target_os = "windows")]
    origin: WindowsOrigin,
}
#[cfg(target_os = "windows")]
struct WindowsOrigin(std::time::Instant);
#[cfg(target_os = "windows")]
impl Default for WindowsOrigin {
    fn default() -> Self {
        Self(std::time::Instant::now())
    }
}
impl ContinuousTimeSource for ContinuousClock {
    fn sample(&self) -> Result<ContinuousSample, ClockError> {
        let elapsed = self.raw()?;
        let utc = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ClockError)?;
        let unix_millis = i64::try_from(utc.as_millis()).map_err(|_| ClockError)?;
        Ok(ContinuousSample {
            elapsed,
            unix_millis,
        })
    }
}
impl ContinuousClock {
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    fn raw(&self) -> Result<Duration, ClockError> {
        use nix::time::{ClockId, clock_gettime};
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let source = ClockId::CLOCK_BOOTTIME;
        // Darwin MONOTONIC_RAW includes sleep; UPTIME_RAW/Instant do not.
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        let source = ClockId::from_raw(nix::libc::CLOCK_MONOTONIC_RAW);
        let time = clock_gettime(source).map_err(|_| ClockError)?;
        let seconds = u64::try_from(time.tv_sec()).map_err(|_| ClockError)?;
        let nanos = u32::try_from(time.tv_nsec()).map_err(|_| ClockError)?;
        if nanos >= 1_000_000_000 {
            return Err(ClockError);
        }
        Ok(Duration::new(seconds, nanos))
    }
    #[cfg(target_os = "windows")]
    fn raw(&self) -> Result<Duration, ClockError> {
        // Windows Instant uses QueryPerformanceCounter, including suspend.
        std::time::Instant::now()
            .checked_duration_since(self.origin.0)
            .ok_or(ClockError)
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "windows"
    )))]
    fn raw(&self) -> Result<Duration, ClockError> {
        Err(ClockError)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_continuous_clock_is_ordered_and_has_utc() {
        let clock = ContinuousClock::default();
        let first = clock.sample().expect("supported native clock");
        let second = clock.sample().expect("supported native clock");
        assert!(second.elapsed >= first.elapsed);
        assert!(first.unix_millis > 0);
    }
}
