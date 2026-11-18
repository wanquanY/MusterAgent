use super::*;
use crate::ExperienceClaimDocument;
use crate::tools::experience_kind;
use musteragent_memory_business::InvalidInput;
use serde_json::{Value, json};
use uuid::Uuid;

fn decode(version: u32, value: Value) -> Result<ProposalInput, InvalidInput> {
    ProposalInput::decode(
        &format!("experience.propose@{version}"),
        &serde_json::to_vec(&value).unwrap(),
    )
}

fn input() -> Value {
    json!({
        "scope": "agent_profile", "kind": "semantic",
        "claim": "虚构验收项目代号为白桦 K01。",
        "conditions": ["仅用于本伙伴的虚构验收，不作为真实项目资料。"],
        "confidence": 1, "sensitivity": "private"
    })
}

#[test]
fn new_proposal_preserves_every_condition_and_has_no_model_authored_status() {
    let mut value = input();
    value["conditions"] = json!(
        (1..=50)
            .map(|i| format!("条件 {i}\n第二行"))
            .collect::<Vec<_>>()
    );
    let parsed = decode(3, value.clone()).unwrap();
    assert_eq!(parsed.scope, "agent_profile");
    assert_eq!(parsed.kind, "semantic");
    assert_eq!(parsed.outcome, "为后续相关问题提供事实参考");
    assert_eq!(
        parsed.conditions.values().cloned().collect::<Vec<_>>(),
        value["conditions"].as_array().unwrap().clone()
    );
    let doc = ExperienceClaimDocument::manual(
        parsed.claim,
        parsed.conditions,
        parsed.outcome,
        parsed.sensitivity,
    );
    let retained = ExperienceClaimDocument::decode(&doc.encode().unwrap()).unwrap();
    assert_eq!(retained, doc);
    assert!(retained.metadata.is_empty());
    assert!(retained.evidence.is_empty());
}

#[test]
fn rejected_authority_and_display_fields_cannot_be_smuggled_as_v3_input() {
    for (field, value) in [
        ("outcome", json!("saved_as_agent_memory")),
        ("status", json!("active")),
        ("published", json!(true)),
        ("approved", json!(true)),
        ("agent_id", json!(Uuid::from_u128(42))),
        ("metadata", json!({"source": "admin"})),
    ] {
        let mut v = input();
        v[field] = value;
        assert!(decode(3, v).is_err(), "{field}");
    }
    for conditions in [
        json!({"memory_scope": "agent", "outcome": "published"}),
        json!([{"text": "condition"}]),
        json!([null]),
        json!([true]),
        json!([""]),
        json!([" \n\t"]),
        json!(["字".repeat(513)]),
        json!(vec!["condition"; 51]),
    ] {
        let mut v = input();
        v["conditions"] = conditions;
        assert!(decode(3, v).is_err());
    }
}

#[test]
fn accepted_meaning_is_not_filtered_or_rewritten() {
    let mut v = input();
    v["claim"] = json!("用户要求在代码中使用字段 saved_as_agent_memory。");
    v["conditions"] = json!(["在 Rust 示例中保留原始字段名 memory_scope。"]);
    let parsed = decode(3, v.clone()).unwrap();
    assert_eq!(parsed.claim, v["claim"]);
    assert_eq!(parsed.conditions["01"], v["conditions"][0]);
    // The contract constrains structure and authority, not the user's vocabulary.
}

#[test]
fn all_supported_kinds_have_a_non_status_purpose_and_use_original_claim_contract() {
    for kind in [
        "semantic",
        "procedure",
        "failure",
        "preference",
        "policy_proposal",
    ] {
        let mut v = input();
        v["kind"] = json!(kind);
        let p = decode(3, v).unwrap();
        assert_eq!(p.kind, kind);
        assert!(p.outcome.len() <= 64);
        ExperienceClaimDocument::manual(p.claim, p.conditions, p.outcome, p.sensitivity)
            .validate_for_kind(experience_kind(kind).unwrap())
            .unwrap();
    }
    for (field, value) in [
        ("kind", json!("persona_patch")),
        ("kind", json!("skill")),
        ("kind", json!("instinct")),
        ("scope", json!("another_agent")),
        ("sensitivity", json!("public")),
        ("confidence", json!(1.1)),
        ("confidence", json!(-0.1)),
        ("claim", json!("  ")),
        ("claim", json!("a".repeat(2001))),
        ("supersedes_candidate_id", json!(Uuid::nil())),
    ] {
        let mut v = input();
        v[field] = value;
        assert!(decode(3, v).is_err(), "{field}");
    }
}

#[test]
fn old_v2_bytes_keep_their_original_meaning_and_never_decode_as_v3() {
    let mut old = input();
    old["conditions"] = json!({"memory_scope": ["agent", {"nested": false}]});
    old["outcome"] = json!("approved_for_agent_memory_test");
    let parsed = decode(2, old.clone()).unwrap();
    assert_eq!(parsed.conditions, *old["conditions"].as_object().unwrap());
    assert_eq!(parsed.outcome, old["outcome"]);
    assert!(decode(3, old).is_err());
    assert!(decode(2, input()).is_err());
    assert!(decode(4, input()).is_err());
}

#[test]
fn scope_is_still_only_a_selector_and_nonempty_conditions_are_bounded_unicode_text() {
    for scope in ["user", "agent_profile"] {
        let mut v = input();
        v["scope"] = json!(scope);
        v["conditions"] = json!(["字".repeat(512)]);
        let parsed = decode(3, v).unwrap();
        assert_eq!(parsed.scope, scope);
        assert_eq!(parsed.conditions.len(), 1);
    }
    let mut v = input();
    v["conditions"] = json!([]);
    assert!(decode(3, v).unwrap().conditions.is_empty());
}
