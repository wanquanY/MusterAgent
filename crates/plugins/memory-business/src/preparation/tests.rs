use super::*;
use serde_json::{Value, json};

fn request(key: &str) -> Vec<u8> {
    let value = match key {
        "user_memory.remember@2" => json!({"fact_key":"reply.language","content":"中文"}),
        "experience.publish@1" => json!({"proposal_id":Uuid::from_u128(42)}),
        "experience.propose@2" => json!({"scope":"user","kind":"semantic","claim":"claim",
            "conditions":{"when":"asked"},"outcome":"reference","confidence":0.9,"sensitivity":"private"}),
        _ => json!({"scope":"agent_profile","kind":"semantic","claim":"claim",
            "conditions":["first","second"],"confidence":0.9,"sensitivity":"private"}),
    };
    serde_json::to_vec(&value).unwrap()
}

#[test]
fn exact_write_contracts_round_trip_without_inventing_authority() {
    for key in [
        "user_memory.remember@2",
        "experience.propose@2",
        "experience.propose@3",
        "experience.publish@1",
    ] {
        let original = request(key);
        let plan = WritePreparation::prepare(key, &original).unwrap();
        let bytes = plan.canonical_bytes().unwrap();
        assert_eq!(
            WritePreparation::decode_for_request(key, &original, &bytes).unwrap(),
            plan
        );
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["plan"]["operation"], key);
    }
    let WritePlan::ProposeV3(input) =
        WritePreparation::prepare("experience.propose@3", &request("experience.propose@3"))
            .unwrap()
            .plan
    else {
        panic!("v3")
    };
    assert_eq!(input.conditions["01"], "first");
    assert_eq!(input.conditions["02"], "second");
    assert_ne!(input.outcome, "published");
    assert!(
        WritePreparation::prepare("experience.propose@2", &request("experience.propose@3"))
            .is_err()
    );
    assert!(
        WritePreparation::prepare("experience.propose@3", &request("experience.propose@2"))
            .is_err()
    );
}

#[test]
fn immutable_request_meaning_and_exact_operation_cannot_change() {
    let key = "user_memory.remember@2";
    let original = request(key);
    let prepared = WritePreparation::prepare(key, &original).unwrap();
    for field in ["fact_key", "content"] {
        let mut forged = prepared.clone();
        let WritePlan::Remember(input) = &mut forged.plan else {
            panic!("remember")
        };
        if field == "fact_key" {
            input.fact_key = "different.fact".into();
        } else {
            input.content = "different".into();
        }
        assert!(
            WritePreparation::decode_for_request(
                key,
                &original,
                &forged.canonical_bytes().unwrap()
            )
            .is_err()
        );
    }
    assert!(
        WritePreparation::decode_for_request(
            "user_memory.remember@3",
            &original,
            &prepared.canonical_bytes().unwrap()
        )
        .is_err()
    );
    let key = "experience.publish@1";
    let original = request(key);
    let forged = WritePreparation::prepare(
        key,
        &serde_json::to_vec(&json!({"proposal_id":Uuid::from_u128(43)})).unwrap(),
    )
    .unwrap();
    assert!(
        WritePreparation::decode_for_request(key, &original, &forged.canonical_bytes().unwrap())
            .is_err()
    );
}

#[test]
fn forged_authority_unknown_protocol_and_noncanonical_bytes_are_rejected() {
    let key = "experience.propose@3";
    let original = request(key);
    let bytes = WritePreparation::prepare(key, &original)
        .unwrap()
        .canonical_bytes()
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    for field in [
        "tenant_id",
        "agent_id",
        "approved",
        "status",
        "sql",
        "implementation",
    ] {
        for location in 0..3 {
            let mut forged = value.clone();
            let target = match location {
                0 => &mut forged,
                1 => &mut forged["plan"],
                _ => &mut forged["plan"]["input"],
            };
            target[field] = json!("forged");
            assert!(
                WritePreparation::decode_for_request(
                    key,
                    &original,
                    &serde_json::to_vec(&forged).unwrap()
                )
                .is_err()
            );
        }
    }
    let mut forged = value;
    forged["protocol"] = json!("musteragent.memory.write-plan.v2");
    assert!(
        WritePreparation::decode_for_request(key, &original, &serde_json::to_vec(&forged).unwrap())
            .is_err()
    );
    let padded = [b" ".as_slice(), &bytes].concat();
    assert!(WritePreparation::decode_for_request(key, &original, &padded).is_err());
    let duplicated = String::from_utf8(bytes).unwrap().replacen(
        "\"protocol\":",
        "\"protocol\":\"musteragent.memory.write-plan.v1\",\"protocol\":",
        1,
    );
    assert!(WritePreparation::decode_for_request(key, &original, duplicated.as_bytes()).is_err());
}

#[test]
fn rejects_nil_publish_duplicate_request_fields_and_both_size_boundaries() {
    for bytes in [br#"{"proposal_id":"00000000-0000-0000-0000-000000000000"}"#.as_slice(), br#"{"proposal_id":"00000000-0000-0000-0000-000000000042","proposal_id":"00000000-0000-0000-0000-000000000043"}"#] {
        assert!(WritePreparation::prepare("experience.publish@1", bytes).is_err());
    }
    let key = "experience.propose@2";
    assert!(WritePreparation::prepare(key, &vec![b' '; MAX_PREPARATION_BYTES + 1]).is_err());
    assert!(
        WritePreparation::decode_for_request(
            key,
            &request(key),
            &vec![b' '; MAX_PREPARATION_BYTES + 1]
        )
        .is_err()
    );
    let mut value: Value = serde_json::from_slice(&request(key)).unwrap();
    value["conditions"]["large"] = json!("");
    let overhead = serde_json::to_vec(&value).unwrap().len();
    value["conditions"]["large"] = json!("x".repeat(MAX_PREPARATION_BYTES - overhead));
    let bytes = serde_json::to_vec(&value).unwrap();
    assert_eq!(bytes.len(), MAX_PREPARATION_BYTES);
    // Normalization and the protocol envelope would exceed the persisted bound.
    assert!(WritePreparation::prepare(key, &bytes).is_err());
}

#[test]
fn host_accepts_new_package_purpose_without_recomputing_policy() {
    let key = "experience.propose@3";
    let original = request(key);
    let mut next = WritePreparation::prepare(key, &original).unwrap();
    let WritePlan::ProposeV3(input) = &mut next.plan else {
        panic!("v3")
    };
    input.outcome = "用于检索相关事实并提示适用条件".into();
    assert_eq!(
        WritePreparation::decode_for_request(key, &original, &next.canonical_bytes().unwrap())
            .unwrap(),
        next
    );
    // A package can change purpose text, not the user's claim, visibility,
    // candidate kind, replacement target, confidence or ordered conditions.
    for field in [
        "scope",
        "kind",
        "claim",
        "sensitivity",
        "confidence",
        "supersedes_candidate_id",
        "conditions",
        "outcome",
    ] {
        let mut changed = next.clone();
        let WritePlan::ProposeV3(input) = &mut changed.plan else {
            panic!("v3")
        };
        match field {
            "scope" => input.scope = "user".into(),
            "kind" => input.kind = "policy_proposal".into(),
            "claim" => input.claim = "different".into(),
            "sensitivity" => input.sensitivity = "internal".into(),
            "confidence" => input.confidence = 1.0,
            "supersedes_candidate_id" => input.supersedes_candidate_id = Some(Uuid::from_u128(42)),
            "conditions" => {
                input.conditions.insert("01".into(), json!("second"));
            }
            "outcome" => input.outcome = "x".repeat(65),
            _ => unreachable!(),
        }
        assert!(
            WritePreparation::decode_for_request(
                key,
                &original,
                &changed.canonical_bytes().unwrap()
            )
            .is_err(),
            "{field}"
        );
    }
    let retained = "experience.propose@2";
    let original = request(retained);
    let mut changed = WritePreparation::prepare(retained, &original).unwrap();
    let WritePlan::ProposeV2(input) = &mut changed.plan else {
        panic!("v2")
    };
    input.outcome = "a new package purpose".into();
    // v2's purpose came from the immutable request, so it remains exact.
    assert!(
        WritePreparation::decode_for_request(
            retained,
            &original,
            &changed.canonical_bytes().unwrap()
        )
        .is_err()
    );
}
