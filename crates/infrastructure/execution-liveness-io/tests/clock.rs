use musteragent_continuous_clock::{ClockError, ContinuousSample, ContinuousTimeSource};
use musteragent_execution_liveness::{LivenessError, LivenessHomeClock};
use musteragent_execution_liveness_io::SystemLivenessHomeClock;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone)]
struct Source(Arc<Mutex<Result<ContinuousSample, ClockError>>>);
impl Source {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Ok(ContinuousSample {
            elapsed: Duration::ZERO,
            unix_millis: 1000,
        }))))
    }
    fn set(&self, ms: u64, utc: i64) {
        *self.0.lock().expect("source lock") = Ok(ContinuousSample {
            elapsed: Duration::from_millis(ms),
            unix_millis: utc,
        });
    }
}
impl ContinuousTimeSource for Source {
    fn sample(&self) -> Result<ContinuousSample, ClockError> {
        *self.0.lock().expect("source lock")
    }
}
#[test]
fn home_clock_counts_suspend_and_ignores_wall_clock_adjustments() {
    let source = Source::new();
    let clock = SystemLivenessHomeClock::with_source(source.clone());
    let initial = clock.sample().expect("clock");
    source.set(3_600_000, 900);
    let woke = clock.sample().expect("wake");
    assert_eq!(woke.monotonic.epoch, initial.monotonic.epoch);
    assert_eq!(
        woke.monotonic.elapsed - initial.monotonic.elapsed,
        Duration::from_secs(3600)
    );
    assert_eq!(woke.utc.unix_millis(), 900);
}
#[test]
fn home_clock_rollback_changes_epoch_and_native_failure_never_falls_back() {
    let source = Source::new();
    let clock = SystemLivenessHomeClock::with_source(source.clone());
    source.set(10, 1000);
    let initial = clock.sample().expect("clock");
    source.set(9, 1001);
    let rollback = clock.sample().expect("new epoch");
    assert_ne!(rollback.monotonic.epoch, initial.monotonic.epoch);
    *source.0.lock().expect("source lock") = Err(ClockError);
    assert_eq!(clock.sample(), Err(LivenessError::ClockUnavailable));
    source.set(11, 1002);
    assert_eq!(
        clock.sample().expect("recovered clock").monotonic.epoch,
        rollback.monotonic.epoch
    );
    let restarted = SystemLivenessHomeClock::with_source(source);
    assert_ne!(
        restarted.sample().expect("new process").monotonic.epoch,
        rollback.monotonic.epoch
    );
}
