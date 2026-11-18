use super::*;

fn original_bytes() -> Vec<u8> {
    crate::signed_execution_grant_message(&crate::grant_test_fixture::assigned_grant())
        .encode_to_vec()
}

#[test]
fn canonical_v10_round_trip_does_not_become_a_client_command_scope() {
    let original = original_bytes();
    let grant = distributed_execution_grant_bytes(&original).expect("V10 fixture");
    let wire = crate::signed_execution_grant_message(&grant);
    assert_eq!(wire.encode_to_vec(), original);
    assert!(
        crate::submission_decode::execution_scope(wire.claims.expect("claims").authority_scope)
            .is_err()
    );
}

#[test]
fn unknown_duplicate_truncated_and_oversize_proofs_are_rejected() {
    let original = original_bytes();
    let mut unknown = original.clone();
    unknown.extend_from_slice(&[0x20, 1]);
    let mut duplicate = original.clone();
    duplicate.extend_from_slice(&original);
    for bad in [
        unknown,
        duplicate,
        original[..original.len() - 1].to_vec(),
        vec![0; MAX_EXECUTION_GRANT_PROOF_BYTES + 1],
        Vec::new(),
    ] {
        assert!(distributed_execution_grant_bytes(&bad).is_err());
    }
}

#[test]
fn aliases_missing_assignment_digest_and_inexact_time_are_rejected() {
    let base =
        wire::SignedExecutionGrant::decode(original_bytes().as_slice()).expect("wire fixture");
    let mut variants = vec![base; 5];
    variants[0]
        .claims
        .as_mut()
        .expect("claims")
        .principal_id
        .as_mut()
        .expect("requester")
        .value = "0000000000000000000000000000000b".into();
    variants[1]
        .claims
        .as_mut()
        .expect("claims")
        .issued_at
        .as_mut()
        .expect("time")
        .nanos = 1;
    variants[2].claims.as_mut().expect("claims").schema_version =
        "musteragent.execution-grant.v9".into();
    for (index, length) in [(3, 0), (4, 31)] {
        let Some(wire::execution_scope::Scope::HostedProjectExecution(value)) = &mut variants
            [index]
            .claims
            .as_mut()
            .expect("claims")
            .authority_scope
            .as_mut()
            .expect("authority")
            .scope
        else {
            panic!("scope")
        };
        if length == 0 {
            value.execution_assignment_digest = None;
        } else {
            value
                .execution_assignment_digest
                .as_mut()
                .expect("digest")
                .value = vec![1; length];
        }
    }
    for bad in variants {
        assert!(distributed_execution_grant_bytes(&bad.encode_to_vec()).is_err());
    }
}
