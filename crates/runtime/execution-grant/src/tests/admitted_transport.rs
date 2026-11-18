use super::*;
use crate::{SignedExecutionGrant, validate_admitted_transport_successor};

// Signatures are deliberately not authority in this unit: the function only
// constrains business/time. Issuer authentication and durable CAS are separate
// mandatory owners and need their own integration tests.
fn admission() -> SignedExecutionGrant {
    let mut claims = device_grant().0;
    claims.schema_version = DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).unwrap();
    claims.issuer = DottedKey::new("musteragent.backend").unwrap();
    claims.agent_contract = Some(VersionedKey::new("musteragent.runtime-contract@1").unwrap());
    let device = DeviceId::generate();
    claims.audience = DottedKey::new(format!("musteragent.runtime.device.id-{device}")).unwrap();
    claims.identity_binding = Some(ExecutionGrantIdentityBinding {
        principal_id: PrincipalId::generate(),
        authority_scope: ExecutionGrantAuthorityScope::Tenant {
            tenant_id: claims.tenant_id,
        },
        device_id: Some(device),
        device_workspace_id: Some(WorkspaceId::generate()),
    });
    claims.issued_at = UtcTimestamp::from_unix_millis(1000);
    claims.expires_at = UtcTimestamp::from_unix_millis(2000);
    assert_eq!(claims.validate(), Ok(()));
    SignedExecutionGrant {
        key_id: DottedKey::new("test.issuer").unwrap(),
        claims,
        signature: [0; 64],
        context_transport_annex: b"original model transport".to_vec(),
    }
}
fn successor(value: &SignedExecutionGrant) -> SignedExecutionGrant {
    let mut next = value.clone();
    next.claims.issued_at = value.claims.expires_at;
    next.claims.expires_at =
        UtcTimestamp::from_unix_millis(value.claims.expires_at.unix_millis() + 1000);
    next
}

#[test]
fn repeated_transport_windows_never_replace_the_admitted_grant() {
    let original = admission();
    let retained = original.clone();
    let first = successor(&original);
    let second = successor(&first);
    assert!(
        validate_admitted_transport_successor(&original, &original, &first, first.claims.issued_at)
            .is_ok()
    );
    assert!(
        validate_admitted_transport_successor(&original, &first, &second, second.claims.issued_at)
            .is_ok()
    );
    assert_eq!(original, retained);
}

#[test]
fn live_head_future_issue_expired_successor_and_larger_windows_are_rejected() {
    let original = admission();
    let next = successor(&original);
    assert!(
        validate_admitted_transport_successor(
            &original,
            &original,
            &next,
            original.claims.issued_at
        )
        .is_err()
    );
    assert!(
        validate_admitted_transport_successor(&original, &original, &next, next.claims.expires_at)
            .is_err()
    );
    let mut future = next.clone();
    future.claims.issued_at = UtcTimestamp::from_unix_millis(2500);
    assert!(
        validate_admitted_transport_successor(&original, &original, &future, next.claims.issued_at)
            .is_err()
    );
    let mut inflated = next.clone();
    inflated.claims.expires_at = UtcTimestamp::from_unix_millis(4000);
    assert!(
        validate_admitted_transport_successor(
            &original,
            &original,
            &inflated,
            next.claims.issued_at
        )
        .is_err()
    );
    let mut bad_head = next.clone();
    bad_head.claims.expires_at = inflated.claims.expires_at;
    let later = successor(&bad_head);
    assert!(
        validate_admitted_transport_successor(&original, &bad_head, &later, later.claims.issued_at)
            .is_err()
    );
}

#[test]
fn actor_workspace_model_nonce_and_transport_annex_cannot_change() {
    let original = admission();
    let good = successor(&original);
    for choice in 0..6 {
        let mut changed = good.clone();
        match choice {
            0 => {
                changed
                    .claims
                    .identity_binding
                    .as_mut()
                    .unwrap()
                    .principal_id = PrincipalId::generate()
            }
            1 => {
                changed
                    .claims
                    .identity_binding
                    .as_mut()
                    .unwrap()
                    .device_workspace_id = Some(WorkspaceId::generate())
            }
            2 => changed.claims.run_id = RunId::generate(),
            3 => changed.claims.model_route.resolved_model_key = "another-model".into(),
            4 => changed.claims.nonce = OpaqueExternalRef::new("another-source").unwrap(),
            _ => changed.context_transport_annex.push(1),
        }
        assert!(
            validate_admitted_transport_successor(
                &original,
                &original,
                &changed,
                good.claims.issued_at
            )
            .is_err(),
            "changed {choice}"
        );
    }
    let mut head = good.clone();
    head.context_transport_annex.push(1);
    assert!(
        validate_admitted_transport_successor(
            &original,
            &head,
            &successor(&head),
            head.claims.expires_at
        )
        .is_err()
    );
}

#[test]
fn committed_head_requires_the_original_device_contract_and_a_live_window() {
    use crate::validate_admitted_transport_head;
    let original = admission();
    let current = successor(&original);
    assert!(
        validate_admitted_transport_head(&original, &original, original.claims.issued_at).is_ok()
    );
    assert!(
        validate_admitted_transport_head(&original, &current, current.claims.issued_at).is_ok()
    );
    assert!(
        validate_admitted_transport_head(&original, &original, original.claims.expires_at).is_err()
    );
    let mut cloud = original.clone();
    cloud.claims.audience = DottedKey::new("musteragent.runtime.cloud").unwrap();
    assert!(validate_admitted_transport_head(&cloud, &current, current.claims.issued_at).is_err());
    let mut added = current.clone();
    added.claims.capability_entitlements[0]
        .allowed_scopes
        .insert(DottedKey::new("extra.read").unwrap());
    assert!(validate_admitted_transport_head(&original, &added, current.claims.issued_at).is_err());
}
