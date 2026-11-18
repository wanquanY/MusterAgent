use super::*;

fn now(ms: i64) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(ms)
}
fn ready(state: &mut DependencyRecoveryState, at: UtcTimestamp) -> DependencyPermit {
    let DependencyAdmission::Ready(permit) = state
        .acquire(InvocationId::generate(), at, Duration::from_secs(20))
        .expect("valid state")
    else {
        panic!("eligible request");
    };
    permit
}

#[test]
fn one_outage_wave_counts_once_and_stale_success_cannot_reopen_the_endpoint() {
    let mut state = DependencyRecoveryState::default();
    let permits = (0..64)
        .map(|_| ready(&mut state, now(1_000)))
        .collect::<Vec<_>>();
    assert!(
        state
            .observe(
                permits[0],
                DependencyObservation::Unavailable {
                    retry_after_ms: Some(30_000)
                },
                now(1_000)
            )
            .expect("outage")
    );
    let opened = state.clone();
    for permit in &permits[1..] {
        for observation in [
            DependencyObservation::Healthy,
            DependencyObservation::Unavailable {
                retry_after_ms: None,
            },
        ] {
            assert!(
                !state
                    .observe(*permit, observation, now(1_001))
                    .expect("late outcome")
            );
        }
    }
    assert_eq!(state, opened);
    assert_eq!(state.failures, 1);
    assert_eq!(
        state
            .acquire(
                InvocationId::generate(),
                now(30_000),
                Duration::from_secs(20)
            )
            .expect("wait"),
        DependencyAdmission::Deferred {
            retry_after_ms: 1_000
        }
    );
    let probe = ready(&mut state, now(31_000));
    assert!(probe.probe);
    assert!(matches!(
        state
            .acquire(
                InvocationId::generate(),
                now(31_000),
                Duration::from_secs(20)
            )
            .expect("coalesced"),
        DependencyAdmission::Deferred { .. }
    ));
    assert!(
        state
            .observe(probe, DependencyObservation::Healthy, now(31_001))
            .expect("healthy")
    );
    assert_eq!(state.failures, 0);
    assert!(!ready(&mut state, now(31_002)).probe);
    assert!(
        !state
            .observe(
                probe,
                DependencyObservation::Unavailable {
                    retry_after_ms: None
                },
                now(31_002)
            )
            .expect("late duplicate")
    );
}

#[test]
fn process_loss_is_recovered_by_an_expiring_probe_and_old_owner_cannot_release_its_replacement() {
    let mut state = DependencyRecoveryState::default();
    let original = ready(&mut state, now(1_000));
    state
        .observe(
            original,
            DependencyObservation::Unavailable {
                retry_after_ms: None,
            },
            now(1_000),
        )
        .expect("failed");
    let due = state.retry_not_before;
    let lost = ready(&mut state, due);
    let expires = state.probe_expires_at.expect("bounded probe");
    // A new process reloads these same scalar fields; no local timer is authority.
    let mut reopened = state.clone();
    let replacement = ready(&mut reopened, expires);
    assert_ne!(replacement.invocation_id, lost.invocation_id);
    assert!(
        !reopened
            .observe(lost, DependencyObservation::Healthy, expires)
            .expect("fenced")
    );
    assert_eq!(reopened.probe_invocation, Some(replacement.invocation_id));
    assert!(
        reopened
            .observe(replacement, DependencyObservation::Healthy, expires)
            .expect("recovered")
    );
}

#[test]
fn long_outages_slow_the_shared_probe_without_exhausting_it() {
    let mut state = DependencyRecoveryState::default();
    let mut at = now(1_000);
    for failure in 1..=10_000 {
        let permit = ready(&mut state, at);
        state
            .observe(
                permit,
                DependencyObservation::Unavailable {
                    retry_after_ms: None,
                },
                at,
            )
            .expect("persist outage");
        let delay = state.retry_not_before.unix_millis() - at.unix_millis();
        if failure >= 8 {
            assert!((120_000..=900_000).contains(&delay));
        }
        at = state.retry_not_before;
    }
    assert_eq!(state.failures, 10_000);
    let probe = ready(
        &mut state,
        at.checked_add_millis(90 * 24 * 60 * 60 * 1_000)
            .expect("90 days"),
    );
    assert!(probe.probe);
}

#[test]
fn invalid_state_never_creates_a_dispatch_permit() {
    let mut state = DependencyRecoveryState {
        probe_invocation: Some(InvocationId::generate()),
        ..Default::default()
    };
    assert_eq!(
        state.acquire(InvocationId::generate(), now(1_000), Duration::from_secs(1)),
        Err(DependencyRecoveryError::InvalidState)
    );
}

#[test]
fn request_local_failure_releases_a_probe_without_asserting_endpoint_health() {
    let mut state = DependencyRecoveryState::default();
    let initial = ready(&mut state, now(1_000));
    state
        .observe(
            initial,
            DependencyObservation::Unavailable {
                retry_after_ms: None,
            },
            now(1_000),
        )
        .expect("outage");
    let due = state.retry_not_before;
    let probe = ready(&mut state, due);
    assert!(
        state
            .observe(probe, DependencyObservation::Abandoned, due)
            .expect("request-local failure")
    );
    assert_eq!(state.failures, 1);
    assert!(ready(&mut state, due).probe);
}

#[test]
fn oversized_retry_after_keeps_a_durable_cooldown() {
    let mut state = DependencyRecoveryState::default();
    let permit = ready(&mut state, now(1000));
    assert!(
        state
            .observe(
                permit,
                DependencyObservation::Unavailable {
                    retry_after_ms: Some(u64::MAX)
                },
                now(1000)
            )
            .expect("outage")
    );
    assert!(
        matches!(state.acquire(InvocationId::generate(), now(1010), Duration::from_secs(1)), Ok(DependencyAdmission::Deferred { retry_after_ms }) if retry_after_ms > 300_000)
    );
}
