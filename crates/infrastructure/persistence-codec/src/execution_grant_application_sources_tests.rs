use crate::{execution_grant_decode::signed_execution_grant, wire};
use musteragent_content_ref::Sha256;
use musteragent_execution_grant::canonical_claims;

fn fixture() -> wire::SignedExecutionGrant {
    let mut grant = crate::grant_test_fixture::assigned_grant();
    grant.claims.reviewed_application_source_sha256 = vec![
        Sha256::from_bytes([b'a'; 32]).unwrap(),
        Sha256::from_bytes([b'b'; 32]).unwrap(),
    ];
    grant.claims.product_release = Some(
        musteragent_identity::ProductReleaseRef::new(
            musteragent_identity::ProductId::KERNEL,
            musteragent_identity::VersionedKey::new("kernel.release@1").unwrap(),
            [7; 32],
        )
        .unwrap(),
    );
    grant.claims.validate().unwrap();
    crate::signed_execution_grant_message(&grant)
}

#[test]
fn application_source_authority_binds_signature_payload_and_round_trips() {
    let wire = fixture();
    let signed = signed_execution_grant(Some(wire.clone())).unwrap();
    assert_eq!(
        signed.claims.reviewed_application_source_sha256,
        vec![
            Sha256::from_bytes([b'a'; 32]).unwrap(),
            Sha256::from_bytes([b'b'; 32]).unwrap()
        ]
    );
    let canonical = canonical_claims(&signed.claims).unwrap();

    assert_eq!(super::signed_execution_grant_message(&signed), wire);
    let mut stripped = signed.claims.clone();
    stripped.reviewed_application_source_sha256.clear();
    let legacy = canonical_claims(&stripped).unwrap();

    assert_ne!(
        canonical, legacy,
        "ignoring the new field cannot verify the new signature"
    );
}

#[test]
fn application_source_authority_rejects_noncanonical_wire_and_domain_sets() {
    let wire = fixture();
    for values in [
        vec![vec![1; 31]],
        vec![vec![0; 32]],
        vec![vec![1; 32], vec![1; 32]],
        vec![vec![2; 32], vec![1; 32]],
        (1..=65).map(|v| vec![v; 32]).collect(),
    ] {
        let mut changed = wire.clone();
        changed
            .claims
            .as_mut()
            .unwrap()
            .reviewed_application_source_sha256 = values;
        assert!(
            signed_execution_grant(Some(changed))
                .and_then(|signed| canonical_claims(&signed.claims)
                    .map_err(|error| crate::wire_decode::invalid("claims", error)))
                .is_err()
        );
    }
    let mut grant = signed_execution_grant(Some(wire)).unwrap().claims;
    grant.product_release = None;
    assert!(canonical_claims(&grant).is_err());
}
