//! Test harness deadlines are configurable independently of runtime policy.
use std::time::Duration;

/// Read a positive millisecond budget. Invalid configuration fails loudly.
/// Defaults must allow scheduling delays on shared development/CI machines;
/// dedicated performance runs can supply their stricter acceptance budget.
#[must_use]
pub fn test_budget(name: &str, default: Duration) -> Duration {
    match std::env::var(name) {
        Ok(value) => {
            let millis: u64 = value.parse().expect("test budget must be milliseconds");
            assert!(millis > 0, "test budget must be positive: {name}");
            Duration::from_millis(millis)
        }
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid test budget {name}: {error}"),
    }
}

/// Completion/cancellation waits observe durable outcomes, not performance.
#[must_use]
pub fn integration_timeout() -> Duration {
    test_budget(
        "MUSTERAGENT_TEST_INTEGRATION_TIMEOUT_MS",
        Duration::from_secs(120),
    )
}
