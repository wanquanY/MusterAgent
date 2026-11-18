use super::*;

#[test]
fn exact_request_survives_prepare_and_host_validation() {
    for (key, request) in [
        ("memory.search@1", r#"{"query":"海盐 验收","limit":8}"#),
        ("memory.list@1", "{}"),
        (
            "memory.get@1",
            r#"{"memory_id":"00000000-0000-4000-8000-000000000001"}"#,
        ),
        (
            "experience.search@1",
            r#"{"query":"verified","kinds":["preference"],"limit":5}"#,
        ),
        (
            "experience.inspect@1",
            r#"{"proposal_id":"00000000-0000-4000-8000-000000000001"}"#,
        ),
    ] {
        let plan = ReadPreparation::prepare(key, request.as_bytes()).unwrap();
        assert_eq!(
            ReadPreparation::decode_for_request(
                key,
                request.as_bytes(),
                &plan.canonical_bytes().unwrap()
            )
            .unwrap(),
            plan
        );
    }
}

#[test]
fn malformed_duplicate_authority_and_nonsensical_requests_are_rejected() {
    for input in [
        r#"{"query":"x","query":"y"}"#,
        r#"{"query":"x","actor_id":"other"}"#,
        r#"{"query":"x","limit":0}"#,
        r#"{"query":"x","limit":21}"#,
        r#"{"query":"   "}"#,
        r#"{"query":"verified","limit":null}"#,
        r#"{"query":"!!!"}"#,
    ] {
        assert!(ReadPreparation::prepare("memory.search@1", input.as_bytes()).is_err());
    }
    assert!(
        ReadPreparation::prepare(
            "memory.get@1",
            br#"{"memory_id":"00000000-0000-0000-0000-000000000000"}"#
        )
        .is_err()
    );
    assert!(
        ReadPreparation::prepare(
            "experience.search@1",
            br#"{"query":"x","kinds":["preference","preference"]}"#
        )
        .is_err()
    );
}

#[test]
fn host_rejects_substitution_but_does_not_rerun_package_defaults() {
    let mut plan = ReadPreparation::prepare("memory.list@1", b"{}").unwrap();
    plan.plan = ReadPlan::MemoryList { limit: 8 };
    let bytes = plan.canonical_bytes().unwrap();
    assert!(ReadPreparation::decode_for_request("memory.list@1", b"{}", &bytes).is_ok());
    assert!(
        ReadPreparation::decode_for_request("memory.list@1", br#"{"limit":5}"#, &bytes).is_err()
    );
    assert!(
        ReadPreparation::decode_for_request("memory.search@1", br#"{"query":"x"}"#, &bytes)
            .is_err()
    );
    plan.plan = ReadPlan::MemoryList { limit: 101 };
    assert!(
        ReadPreparation::decode_for_request(
            "memory.list@1",
            b"{}",
            &plan.canonical_bytes().unwrap()
        )
        .is_err()
    );
}

#[test]
fn new_query_policy_can_change_features_without_changing_original_text() {
    let request = br#"{"query":"verified memory","limit":5}"#;
    let mut prepared = ReadPreparation::prepare("memory.search@1", request).unwrap();
    let ReadPlan::MemorySearch { strategy, .. } = &mut prepared.plan else {
        panic!()
    };
    strategy.index_features = "verified".into();
    strategy.normalized_features = vec!["verified".into()];
    strategy.candidate_limit = 60;
    assert!(
        ReadPreparation::decode_for_request(
            "memory.search@1",
            request,
            &prepared.canonical_bytes().unwrap()
        )
        .is_ok()
    );
    let ReadPlan::MemorySearch { query, .. } = &mut prepared.plan else {
        panic!()
    };
    *query = "different".into();
    assert!(
        ReadPreparation::decode_for_request(
            "memory.search@1",
            request,
            &prepared.canonical_bytes().unwrap()
        )
        .is_err()
    );
}
