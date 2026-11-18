use super::*;
use musteragent_goal::source_binding::GoalSourceBindingGuard;
use musteragent_identity::*;
use prost::Message;
use std::num::NonZeroU64;

macro_rules! identifier {
    ($kind:ident, $value:expr) => {
        $kind::from_uuid(uuid::Uuid::from_u128($value)).unwrap()
    };
}
fn number(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

#[test]
fn input_receipt_preserves_the_first_match_set_and_rejects_ambiguous_replay() {
    use musteragent_runtime_session::owner_event_sources::*;
    let source = fixture();
    let receipt = OwnerSourceInputReceipt {
        source_observation: None,
        source_object: None,
        goal_id: source.goal_id,
        event_id: identifier!(SourceEventId, 9),
        evidence_digest: Sha256::digest(b"event"),
        input_sequence: number(1),
        matches: vec![SourceInputMatch {
            subscription_id: source.subscription_id,
            guard: source.guard,
        }],
    };
    let encoded = input_receipt(&receipt).unwrap();
    assert_eq!(
        decode_input_receipt(
            wire::OwnerSourceInputReceipt::decode(encoded.encode_to_vec().as_slice()).unwrap()
        )
        .unwrap(),
        receipt
    );
    let mut duplicate = encoded.clone();
    duplicate.matches.push(duplicate.matches[0].clone());
    assert!(decode_input_receipt(duplicate).is_err());
    let mut overflow = encoded.clone();
    overflow.input_sequence = u64::MAX;
    assert!(decode_input_receipt(overflow).is_err());
    let mut empty = encoded;
    empty.matches.clear();
    assert!(decode_input_receipt(empty).is_err());
}
fn fixture() -> OwnerEventSubscription {
    let configuration = EventSubscriptionConfiguration {
        application_binding_id: identifier!(OwnerApplicationBindingId, 4),
        application_guard: GoalSourceBindingGuard {
            revision: number(1),
            authority_epoch: number(1),
        },
        capability_ref: VersionedKey::new("calendar.changes@1").unwrap(),
        capability_digest: Sha256::digest(b"capability"),
        selection: EventSourceSelection::from_json(br#"{"calendar_id":"private"}"#).unwrap(),
        event_kinds: [VersionedKey::new("calendar.changed@1").unwrap()].into(),
    };
    let boundary = SourceActivationBoundary {
        boundary_id: identifier!(SourceBoundaryId, 7),
        subscription_id: identifier!(OwnerEventSubscriptionId, 5),
        activation_epoch: number(1),
        configuration_digest: configuration.fingerprint(),
        capture_id: identifier!(SourceCaptureId, 8),
        capture_generation: number(1),
        after_source_sequence: 0,
        source_status_revision: number(1),
        evidence_digest: Sha256::digest(b"baseline"),
    };
    OwnerEventSubscription {
        subscription_id: boundary.subscription_id,
        goal_id: identifier!(GoalId, 6),
        subject: GoalSourceSubject {
            tenant_id: identifier!(TenantId, 1),
            owner_id: identifier!(PrincipalId, 2),
            agent_id: identifier!(AgentId, 3),
        },
        configuration,
        required: false,
        guard: EventSubscriptionGuard {
            revision: number(2),
            activation_epoch: number(1),
        },
        desired: EventSubscriptionIntent::Watching,
        source_status: Some(SourceReadinessObservation {
            capture_id: boundary.capture_id,
            capture_generation: boundary.capture_generation,
            revision: number(3),
            readiness: SourceReadiness::Retrying,
            evidence_digest: Sha256::digest(b"retry"),
        }),
        boundary: Some(boundary),
        source_acknowledged_at: Some(UtcTimestamp::from_unix_millis(101)),
        created_at: UtcTimestamp::from_unix_millis(100),
        updated_at: UtcTimestamp::from_unix_millis(102),
    }
}

#[test]
fn binary_roundtrip_preserves_independent_control_and_health() {
    let expected = fixture();
    let bytes = subscription(&expected).unwrap().encode_to_vec();
    let decoded = wire::OwnerEventSubscription::decode(bytes.as_slice()).unwrap();
    assert_eq!(decode_subscription(Some(decoded)).unwrap(), expected);
    let paused = expected
        .change(
            expected.guard,
            EventSubscriptionIntent::Paused,
            None,
            &musteragent_goal::source_binding::GoalSourceBinding {
                binding_id: expected.configuration.application_binding_id,
                subject: expected.subject,
                status: musteragent_goal::source_binding::GoalSourceBindingStatus::Disabled,
                guard: expected.configuration.application_guard,
            },
            UtcTimestamp::from_unix_millis(103),
        )
        .unwrap();
    assert_eq!(
        decode_subscription(Some(subscription(&paused).unwrap())).unwrap(),
        paused
    );
}

#[test]
fn malformed_or_cross_consumer_baselines_are_not_silently_adopted() {
    let original = subscription(&fixture()).unwrap();
    let mut foreign = original.clone();
    foreign.boundary.as_mut().unwrap().subscription_id = Some(wire::OwnerEventSubscriptionId {
        value: identifier!(OwnerEventSubscriptionId, 99).to_string(),
    });
    assert_eq!(decode_subscription(Some(foreign)), Err(BAD));
    let mut future = original.clone();
    future.guard.as_mut().unwrap().activation_epoch = 3;
    assert_eq!(decode_subscription(Some(future)), Err(BAD));
    let mut missing_health = original.clone();
    missing_health.source_status = None;
    assert_eq!(decode_subscription(Some(missing_health)), Err(BAD));
    let mut unknown = original;
    unknown.source_status.as_mut().unwrap().readiness = 99;
    assert_eq!(decode_subscription(Some(unknown)), Err(BAD));
}

#[test]
fn transport_budgets_and_duplicate_kinds_fail_before_configuration_adoption() {
    let original = configuration(&fixture().configuration).unwrap();
    let mut duplicate = original.clone();
    duplicate.event_kinds.push(duplicate.event_kinds[0].clone());
    assert_eq!(decode_configuration(Some(duplicate)), Err(BAD));
    let mut oversized = original;
    oversized.selection_json = vec![b' '; MAX_EVENT_SELECTION_BYTES + 1];
    assert_eq!(decode_configuration(Some(oversized)), Err(BAD));
    let mut record = subscription(&fixture()).unwrap();
    record.boundary.as_mut().unwrap().after_source_sequence = i64::MAX as u64 + 1;
    assert_eq!(decode_subscription(Some(record)), Err(BAD));
}

#[test]
fn canonical_source_object_roundtrips_and_rejects_unbounded_or_missing_identity() {
    let object = SourceObjectIdentity {
        provider: "host-calendar".into(),
        connection: "account-connection".into(),
        resource: "calendar/event".into(),
        version: "immutable-version".into(),
        observed_at: UtcTimestamp::from_unix_millis(1793500000000),
    };
    assert_eq!(
        decode_source_object(source_object(&object)).unwrap(),
        object
    );
    let mut encoded = source_object(&object);
    encoded.connection.clear();
    assert!(decode_source_object(encoded).is_err());
    let mut encoded = source_object(&object);
    encoded.resource = "x".repeat(2001);
    assert!(decode_source_object(encoded).is_err());
    let mut encoded = source_object(&object);
    encoded.observed_at_unix_ms = -1;
    assert!(decode_source_object(encoded).is_err());
}
