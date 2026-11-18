use super::*;
use musteragent_identity::{AgentId, PrincipalId, TenantId};

macro_rules! id {
    ($kind:ident, $value:expr) => {
        format!("{:032x}", $value).parse::<$kind>().unwrap()
    };
}
fn counter(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}
fn time(value: i64) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(value)
}
fn key(value: &str) -> VersionedKey {
    VersionedKey::new(value).unwrap()
}

fn fixture() -> (OwnerEventSubscription, GoalSourceBinding) {
    let application = GoalSourceBinding {
        binding_id: id!(GoalSourceBindingId, 4),
        subject: GoalSourceSubject {
            tenant_id: id!(TenantId, 1),
            owner_id: id!(PrincipalId, 2),
            agent_id: id!(AgentId, 3),
        },
        status: GoalSourceBindingStatus::Enabled,
        guard: GoalSourceBindingGuard {
            revision: counter(1),
            authority_epoch: counter(1),
        },
    };
    let config = EventSubscriptionConfiguration {
        application_binding_id: application.binding_id,
        application_guard: application.guard,
        capability_ref: key("calendar.changes@1"),
        capability_digest: Sha256::digest(b"event contract"),
        selection: EventSourceSelection::from_json(br#"{"calendar_id":"private-calendar"}"#)
            .unwrap(),
        event_kinds: [key("calendar.event.changed@1")].into(),
    };
    let subscription = OwnerEventSubscription::prepare(
        id!(OwnerEventSubscriptionId, 5),
        id!(GoalId, 6),
        application.subject,
        config,
        true,
        &application,
        time(100),
    )
    .unwrap();
    (subscription, application)
}

fn boundary(subscription: &OwnerEventSubscription, after: u64) -> SourceActivationBoundary {
    SourceActivationBoundary {
        subscription_id: subscription.subscription_id,
        boundary_id: id!(SourceBoundaryId, 7),
        activation_epoch: subscription.guard.activation_epoch,
        configuration_digest: subscription.configuration.fingerprint(),
        capture_id: id!(SourceCaptureId, 8),
        capture_generation: counter(1),
        after_source_sequence: after,
        source_status_revision: counter(1),
        evidence_digest: Sha256::digest(b"source baseline receipt"),
    }
}

fn observation(sequence: u64) -> SourceObservationIdentity {
    SourceObservationIdentity {
        source_object: None,
        capture_id: id!(SourceCaptureId, 8),
        capture_generation: counter(1),
        event_id: id!(SourceEventId, 9),
        source_sequence: counter(sequence),
        event_kind: key("calendar.event.changed@1"),
        evidence_digest: Sha256::digest(b"durable observation"),
    }
}

#[test]
fn baseline_and_current_application_authority_are_both_required() {
    let (pending, mut app) = fixture();
    assert_eq!(
        pending.check_observation(pending.guard.activation_epoch, &observation(41), &app),
        Err(EventSubscriptionError::NotReady)
    );
    let active = pending
        .activate(boundary(&pending, 40), &app, time(101))
        .unwrap();
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &observation(40), &app)
            .unwrap(),
        SourceInputEligibility::BeforeActivationBoundary
    );
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &observation(41), &app)
            .unwrap(),
        SourceInputEligibility::Eligible
    );
    app.guard.authority_epoch = counter(2);
    assert_eq!(
        active.check_observation(active.guard.activation_epoch, &observation(41), &app),
        Err(EventSubscriptionError::AuthorityChanged)
    );
    app.guard.authority_epoch = counter(1);
    app.subject.agent_id = id!(AgentId, 99);
    assert_eq!(
        active.check_observation(active.guard.activation_epoch, &observation(41), &app),
        Err(EventSubscriptionError::SubjectMismatch)
    );
}

#[test]
fn pause_closes_input_and_resume_requires_a_new_boundary_without_backfill() {
    let (pending, app) = fixture();
    let old_boundary = boundary(&pending, 40);
    let active = pending
        .activate(old_boundary.clone(), &app, time(101))
        .unwrap();
    let paused = active
        .change(
            active.guard,
            EventSubscriptionIntent::Paused,
            None,
            &app,
            time(102),
        )
        .unwrap();
    assert!(paused.source_acknowledged_at.is_none());
    assert_eq!(
        paused
            .check_observation(paused.guard.activation_epoch, &observation(41), &app)
            .unwrap(),
        SourceInputEligibility::NotWatching
    );
    assert_eq!(
        paused.activate(old_boundary.clone(), &app, time(103)),
        Err(EventSubscriptionError::Conflict)
    );
    let resumed = paused
        .change(
            paused.guard,
            EventSubscriptionIntent::Watching,
            None,
            &app,
            time(103),
        )
        .unwrap();
    assert_eq!(
        resumed.activate(old_boundary, &app, time(104)),
        Err(EventSubscriptionError::Conflict)
    );
    assert_eq!(
        resumed.acknowledge_release(
            paused.guard.activation_epoch,
            EventSubscriptionIntent::Paused,
            time(104)
        ),
        Err(EventSubscriptionError::Conflict)
    );
    let active = resumed
        .activate(boundary(&resumed, 50), &app, time(104))
        .unwrap();
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &observation(45), &app)
            .unwrap(),
        SourceInputEligibility::BeforeActivationBoundary
    );
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &observation(51), &app)
            .unwrap(),
        SourceInputEligibility::Eligible
    );
}

#[test]
fn withdrawal_remains_possible_after_revocation_but_cannot_expand_configuration() {
    let (pending, mut app) = fixture();
    let active = pending
        .activate(boundary(&pending, 0), &app, time(101))
        .unwrap();
    app.status = GoalSourceBindingStatus::Removed;
    assert_eq!(
        active.change(
            active.guard,
            EventSubscriptionIntent::Paused,
            Some(active.configuration.clone()),
            &app,
            time(102)
        ),
        Err(EventSubscriptionError::AuthorityChanged)
    );
    let ended = active
        .change(
            active.guard,
            EventSubscriptionIntent::Ended,
            None,
            &app,
            time(102),
        )
        .unwrap();
    assert!(ended.source_acknowledged_at.is_none());
    assert_eq!(
        ended
            .check_observation(ended.guard.activation_epoch, &observation(1), &app)
            .unwrap(),
        SourceInputEligibility::NotWatching
    );
    let acknowledged = ended
        .acknowledge_release(
            ended.guard.activation_epoch,
            EventSubscriptionIntent::Ended,
            time(103),
        )
        .unwrap();
    assert_eq!(acknowledged.source_acknowledged_at, Some(time(103)));
    app.status = GoalSourceBindingStatus::Enabled;
    assert_eq!(
        acknowledged.change(
            acknowledged.guard,
            EventSubscriptionIntent::Watching,
            None,
            &app,
            time(104)
        ),
        Err(EventSubscriptionError::Ended)
    );
}

#[test]
fn capture_identity_kind_and_configuration_are_not_inferred_from_event_time() {
    let (pending, app) = fixture();
    let mut wrong = boundary(&pending, 0);
    wrong.configuration_digest = Sha256::digest(b"different selection");
    assert_eq!(
        pending.activate(wrong, &app, time(101)),
        Err(EventSubscriptionError::Conflict)
    );
    let mut active = pending
        .activate(boundary(&pending, 0), &app, time(101))
        .unwrap();
    let mut event = observation(1);
    event.capture_generation = counter(2);
    assert_eq!(
        active.check_observation(active.guard.activation_epoch, &event, &app),
        Err(EventSubscriptionError::ForeignCapture)
    );
    event.capture_generation = counter(1);
    event.event_kind = key("mail.message.created@1");
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &event, &app)
            .unwrap(),
        SourceInputEligibility::EventKindNotSelected
    );
    active.configuration.capability_digest = Sha256::digest(b"changed source contract");
    assert_eq!(
        active.check_observation(active.guard.activation_epoch, &observation(1), &app),
        Err(EventSubscriptionError::InvalidConfiguration)
    );
}

#[test]
fn private_selection_is_bounded_and_canonical_in_fingerprints() {
    let first = EventSourceSelection::from_json(br#"{ "b":2, "a":"private-calendar" }"#).unwrap();
    let second = EventSourceSelection::from_json(br#"{"a":"private-calendar","b":2}"#).unwrap();
    assert_eq!(first.json_bytes(), second.json_bytes());
    assert!(!format!("{first:?}").contains("private-calendar"));
    assert!(EventSourceSelection::from_json(b"[]").is_err());
    assert!(EventSourceSelection::from_json(&vec![b' '; MAX_EVENT_SELECTION_BYTES + 1]).is_err());
    let deeply_nested = format!("{{\"x\":{}0{}}}", "[".repeat(17), "]".repeat(17));
    assert!(EventSourceSelection::from_json(deeply_nested.as_bytes()).is_err());
    let (mut subscription, _) = fixture();
    subscription.configuration.selection = first;
    let before = subscription.configuration.fingerprint();
    subscription.configuration.selection = second;
    assert_eq!(before, subscription.configuration.fingerprint());
    subscription.configuration.capability_digest = Sha256::digest(b"changed source contract");
    assert_ne!(before, subscription.configuration.fingerprint());
}

#[test]
fn another_consumer_cannot_reuse_a_baseline_and_coordination_does_not_reset_it() {
    let (pending, app) = fixture();
    let mut foreign = boundary(&pending, 20);
    foreign.subscription_id = id!(OwnerEventSubscriptionId, 66);
    assert_eq!(
        pending.activate(foreign, &app, time(101)),
        Err(EventSubscriptionError::Conflict)
    );
    let active = pending
        .activate(boundary(&pending, 20), &app, time(101))
        .unwrap();
    let optional = active.set_required(active.guard, false, time(102)).unwrap();
    assert!(!optional.required);
    assert_eq!(
        optional.guard.activation_epoch,
        active.guard.activation_epoch
    );
    assert_eq!(optional.boundary, active.boundary);
    assert_eq!(
        optional
            .check_observation(optional.guard.activation_epoch, &observation(21), &app)
            .unwrap(),
        SourceInputEligibility::Eligible
    );
}

#[test]
fn health_is_monotonic_and_does_not_race_the_users_control_revision() {
    let (pending, app) = fixture();
    let active = pending
        .activate(boundary(&pending, 0), &app, time(101))
        .unwrap();
    assert_eq!(
        active.guard, pending.guard,
        "readiness must not invalidate an in-flight user pause"
    );
    let ready = active.source_status.clone().unwrap();
    let retrying = SourceReadinessObservation {
        revision: counter(2),
        readiness: SourceReadiness::Retrying,
        ..ready.clone()
    };
    let degraded = active
        .observe_readiness(active.guard.activation_epoch, retrying, time(102))
        .unwrap();
    assert_eq!(degraded.guard, active.guard);
    let unchanged = degraded
        .observe_readiness(degraded.guard.activation_epoch, ready, time(103))
        .unwrap();
    assert_eq!(
        unchanged, degraded,
        "late health cannot reopen an unavailable source"
    );
    assert_eq!(
        degraded.check_observation(degraded.guard.activation_epoch, &observation(1), &app),
        Err(EventSubscriptionError::NotReady)
    );
    let recovered = degraded
        .observe_readiness(
            degraded.guard.activation_epoch,
            SourceReadinessObservation {
                revision: counter(3),
                readiness: SourceReadiness::Ready,
                ..degraded.source_status.clone().unwrap()
            },
            time(104),
        )
        .unwrap();
    assert_eq!(
        recovered
            .check_observation(recovered.guard.activation_epoch, &observation(1), &app)
            .unwrap(),
        SourceInputEligibility::Eligible
    );
    assert!(
        recovered
            .change(
                pending.guard,
                EventSubscriptionIntent::Paused,
                None,
                &app,
                time(105)
            )
            .is_ok()
    );
}

#[test]
fn late_delivery_cannot_borrow_a_new_activation_even_when_its_sequence_is_newer() {
    let (pending, app) = fixture();
    let active = pending
        .activate(boundary(&pending, 20), &app, time(101))
        .unwrap();
    let paused = active
        .change(
            active.guard,
            EventSubscriptionIntent::Paused,
            None,
            &app,
            time(102),
        )
        .unwrap();
    let resumed = paused
        .change(
            paused.guard,
            EventSubscriptionIntent::Watching,
            None,
            &app,
            time(103),
        )
        .unwrap();
    let current = resumed
        .activate(boundary(&resumed, 30), &app, time(104))
        .unwrap();
    assert_eq!(
        current
            .check_observation(active.guard.activation_epoch, &observation(40), &app)
            .unwrap(),
        SourceInputEligibility::SupersededActivation
    );
}

#[test]
fn a_retired_capture_cannot_be_revived_by_a_newer_health_message() {
    let (pending, app) = fixture();
    let active = pending
        .activate(boundary(&pending, 0), &app, time(101))
        .unwrap();
    let retired = active
        .observe_readiness(
            active.guard.activation_epoch,
            SourceReadinessObservation {
                revision: counter(2),
                readiness: SourceReadiness::Retired,
                ..active.source_status.clone().unwrap()
            },
            time(102),
        )
        .unwrap();
    assert_eq!(
        retired.observe_readiness(
            retired.guard.activation_epoch,
            SourceReadinessObservation {
                revision: counter(3),
                readiness: SourceReadiness::Ready,
                ..retired.source_status.clone().unwrap()
            },
            time(103)
        ),
        Err(EventSubscriptionError::Conflict)
    );
}

#[test]
fn required_source_changes_do_not_discard_an_in_flight_baseline_receipt() {
    let (pending, app) = fixture();
    let receipt = boundary(&pending, 20);
    let optional = pending
        .set_required(pending.guard, false, time(101))
        .unwrap();
    let active = optional.activate(receipt, &app, time(102)).unwrap();
    assert_eq!(active.guard, optional.guard);
    assert!(!active.required);
    assert_eq!(
        active
            .check_observation(active.guard.activation_epoch, &observation(21), &app)
            .unwrap(),
        SourceInputEligibility::Eligible
    );
}

#[test]
fn parent_resume_closes_the_old_epoch_without_overriding_source_intent_or_authority() {
    let (pending, mut app) = fixture();
    let active = pending
        .activate(boundary(&pending, 20), &app, time(101))
        .unwrap();
    let closed = active
        .invalidate_activation(active.guard, time(102))
        .unwrap();
    assert_eq!(closed.desired, EventSubscriptionIntent::Watching);
    assert!(closed.boundary.is_none());
    assert_eq!(
        closed
            .check_observation(active.guard.activation_epoch, &observation(40), &app)
            .unwrap(),
        SourceInputEligibility::SupersededActivation
    );
    assert_eq!(
        closed.check_observation(closed.guard.activation_epoch, &observation(40), &app),
        Err(EventSubscriptionError::NotReady)
    );
    let paused = active
        .change(
            active.guard,
            EventSubscriptionIntent::Paused,
            None,
            &app,
            time(102),
        )
        .unwrap();
    assert_eq!(
        paused
            .invalidate_activation(paused.guard, time(103))
            .unwrap(),
        paused
    );
    app.status = GoalSourceBindingStatus::Disabled;
    assert_eq!(
        closed.activate(boundary(&closed, 50), &app, time(103)),
        Err(EventSubscriptionError::AuthorityChanged)
    );
}
