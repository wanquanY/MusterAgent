use std::time::Duration;

use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::execution_liveness::{
    ActivitySource, ExecutionLivenessPolicy, ExecutionLivenessWindows, LivenessPolicyError,
};

fn key() -> VersionedKey {
    VersionedKey::new("test.liveness@1").expect("versioned policy")
}

fn windows(values: [u64; 7]) -> ExecutionLivenessWindows {
    let [
        startup,
        idle,
        max_delivery_lag,
        checkpoint_interval,
        observation_request,
        cleanup,
        post_exit_drain,
    ] = values.map(Duration::from_millis);
    ExecutionLivenessWindows {
        startup,
        idle,
        max_delivery_lag,
        checkpoint_interval,
        observation_request,
        cleanup,
        post_exit_drain,
    }
}

fn valid_windows() -> ExecutionLivenessWindows {
    windows([20, 100, 10, 5, 5, 10, 10])
}

#[test]
fn policy_rejects_zero_windows_and_delivery_lag_that_could_renew_already_stale_work() {
    for index in 0..7 {
        let mut values = [20, 100, 10, 5, 5, 10, 10];
        values[index] = 0;
        assert_eq!(
            ExecutionLivenessPolicy::new(key(), windows(values), []),
            Err(LivenessPolicyError::ZeroWindow)
        );
    }
    for lag in [100, 101] {
        let mut windows = valid_windows();
        windows.max_delivery_lag = Duration::from_millis(lag);
        assert_eq!(
            ExecutionLivenessPolicy::new(key(), windows, []),
            Err(LivenessPolicyError::DeliveryLagExceedsIdleWindow)
        );
    }
}

#[test]
fn evidence_profile_is_explicit_and_allows_finite_silent_only_capabilities() {
    let build = |sources: Vec<ActivitySource>| {
        ExecutionLivenessPolicy::new(key(), valid_windows(), sources).expect("valid policy")
    };
    let silent = build(vec![]);
    let model = build(vec![ActivitySource::CanonicalModelEvent]);
    for source in ActivitySource::ALL {
        assert!(!silent.accepts(source));
        assert_eq!(
            model.accepts(source),
            source == ActivitySource::CanonicalModelEvent
        );
    }
    assert_eq!(silent.idle_window(), Duration::from_millis(100));
    assert_eq!(model.reference().key, key());
    assert_ne!(silent.reference().digest, model.reference().digest);
}

#[test]
fn policy_seal_has_a_cross_language_vector_and_is_independent_of_input_order() {
    use ActivitySource::{Stderr, Stdout};
    let policy =
        ExecutionLivenessPolicy::new(key(), valid_windows(), [Stdout, Stderr]).expect("policy");
    // Independently constructed with Python struct.pack(">Q") + hashlib.sha256.
    assert_eq!(
        policy.reference().digest.to_string(),
        "7f13abf1ca096947c0b85d564012c02a58c6047316ce6b59658b965592efb3a2"
    );
    assert_eq!(
        policy,
        ExecutionLivenessPolicy::new(key(), valid_windows(), [Stderr, Stdout, Stderr])
            .expect("same set")
    );
    let mut changed = valid_windows();
    changed.post_exit_drain += Duration::from_millis(1);
    assert_ne!(
        policy.reference().digest,
        ExecutionLivenessPolicy::new(key(), changed, [Stdout, Stderr])
            .expect("changed")
            .reference()
            .digest
    );
    assert_ne!(
        policy.reference().digest,
        ExecutionLivenessPolicy::new(
            VersionedKey::new("test.liveness@2").expect("key"),
            valid_windows(),
            [Stdout, Stderr]
        )
        .expect("new version")
        .reference()
        .digest
    );
}

#[test]
fn policy_rejects_lossy_milliseconds_overflow_and_missing_checkpoint_margin() {
    for startup in [Duration::from_nanos(1), Duration::from_secs(u64::MAX)] {
        let mut windows = valid_windows();
        windows.startup = startup;
        assert_eq!(
            ExecutionLivenessPolicy::new(key(), windows, []),
            Err(LivenessPolicyError::NonPortableWindow)
        );
    }
    let mut windows = valid_windows();
    windows.checkpoint_interval = Duration::from_millis(85);
    assert_eq!(
        ExecutionLivenessPolicy::new(key(), windows, []),
        Err(LivenessPolicyError::InsufficientObservationMargin)
    );
    windows.checkpoint_interval = Duration::from_millis(84);
    assert_eq!(
        ExecutionLivenessPolicy::new(key(), windows, []),
        Err(LivenessPolicyError::InsufficientDeliveryMargin)
    );
    windows.checkpoint_interval = Duration::from_millis(10);
    assert_eq!(
        ExecutionLivenessPolicy::new(key(), windows, []),
        Err(LivenessPolicyError::InsufficientDeliveryMargin)
    );
    windows.checkpoint_interval = Duration::from_millis(9);
    assert!(ExecutionLivenessPolicy::new(key(), windows, []).is_ok());
}

#[test]
fn original_operation_identity_has_a_cross_language_vector_and_separates_phases() {
    use musteragent_content_ref::Sha256;
    use musteragent_identity::{InvocationId, RunId, TenantId};
    use musteragent_runtime_contract::execution_liveness::{
        ExecutionOperationKind, ExecutionScope, execution_operation_identity,
    };
    let tenant: TenantId = "00000000-0000-0000-0000-000000000001"
        .parse()
        .expect("valid operation identity fixture");
    let run: RunId = "00000000-0000-0000-0000-000000000002"
        .parse()
        .expect("valid operation identity fixture");
    let invocation: InvocationId = "00000000-0000-0000-0000-000000000003"
        .parse()
        .expect("valid operation identity fixture");
    let snapshot = Sha256::digest(b"snapshot");
    let request = Sha256::digest(b"request");
    let scope = ExecutionScope::Invocation(invocation);
    let original = execution_operation_identity(
        tenant,
        run,
        scope,
        ExecutionOperationKind::Tool,
        snapshot,
        request,
    );
    // Python uuid.UUID.bytes + hashlib.sha256, independent of the Rust codec.
    assert_eq!(
        original.to_string(),
        "fe582a6d4aa97bccb58d67c735e82592e594df900ef04aac68d97838d829239b"
    );
    for changed in [
        execution_operation_identity(
            tenant,
            run,
            ExecutionScope::RunHandoff {
                previous_invocation: invocation,
            },
            ExecutionOperationKind::Tool,
            snapshot,
            request,
        ),
        execution_operation_identity(
            tenant,
            run,
            scope,
            ExecutionOperationKind::Model,
            snapshot,
            request,
        ),
        execution_operation_identity(
            tenant,
            run,
            scope,
            ExecutionOperationKind::AuxiliaryModel,
            snapshot,
            request,
        ),
        execution_operation_identity(
            tenant,
            run,
            scope,
            ExecutionOperationKind::Tool,
            Sha256::digest(b"other snapshot"),
            request,
        ),
        execution_operation_identity(
            tenant,
            run,
            scope,
            ExecutionOperationKind::Tool,
            snapshot,
            Sha256::digest(b"other request"),
        ),
        execution_operation_identity(
            TenantId::generate(),
            run,
            scope,
            ExecutionOperationKind::Tool,
            snapshot,
            request,
        ),
        execution_operation_identity(
            tenant,
            RunId::generate(),
            scope,
            ExecutionOperationKind::Tool,
            snapshot,
            request,
        ),
    ] {
        assert_ne!(original, changed);
    }
}
