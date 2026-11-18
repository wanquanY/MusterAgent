#![cfg(test)]

use musteragent_execution_context::{PublicDeliveryContract, PublicDeliveryDraft};
use serde_json::{Value, json};

fn partner() -> Value {
    json!({"kind":"partner_created","purpose":"result",
        "creation_command_id":"abeb1e22-074b-48a0-b8e7-d59e2a9f382f",
        "partner_tool_call_id":"call_create_partner"})
}
fn parse(value: &Value) -> bool {
    PublicDeliveryContract::V4
        .parse(&serde_json::to_vec(value).unwrap())
        .is_ok()
}

#[test]
fn partner_reference_is_closed_and_does_not_change_frozen_v3() {
    let schema = jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(&serde_json::from_str(PublicDeliveryContract::V4.schema()).unwrap())
        .unwrap();
    for purpose in [
        "reply",
        "result",
        "important_update",
        "clarification",
        "required_decision",
        "blocked",
    ] {
        for field in [
            None,
            Some("text"),
            Some("agent_id"),
            Some("avatar_ref"),
            Some("artifact_ref"),
        ] {
            let mut item = partner();
            item["purpose"] = json!(purpose);
            if let Some(field) = field {
                item[field] = Value::Null;
            }
            let value = json!({"deliveries":[item]});
            assert_eq!(schema.is_valid(&value), parse(&value));
            assert_eq!(parse(&value), purpose == "result" && field.is_none());
        }
    }
    let value = json!({"deliveries":[partner()]});
    assert!(
        PublicDeliveryContract::V3
            .parse(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    let parsed = PublicDeliveryContract::V4
        .parse(&serde_json::to_vec(&value).unwrap())
        .unwrap();
    assert!(
        matches!(&parsed[0].content, PublicDeliveryDraft::PartnerCreated { partner_tool_call_id,.. }
        if partner_tool_call_id.as_str()=="call_create_partner")
    );
    assert!(!parse(&json!({"deliveries":[]})));
    assert!(!parse(&json!({"deliveries":[partner(),partner()]})));
}

#[test]
fn partner_cannot_upgrade_missing_duplicate_or_foreign_fields() {
    for field in [
        "kind",
        "purpose",
        "creation_command_id",
        "partner_tool_call_id",
    ] {
        let mut item = partner();
        item.as_object_mut().unwrap().remove(field);
        assert!(!parse(&json!({"deliveries":[item]})));
    }
    for bad in ["", "contains space", "not a UUID"] {
        let mut item = partner();
        item["creation_command_id"] = json!(bad);
        assert!(!parse(&json!({"deliveries":[item]})));
    }
    let duplicate = r#"{"deliveries":[{"kind":"partner_created","kind":"partner_created","purpose":"result","creation_command_id":"abeb1e22-074b-48a0-b8e7-d59e2a9f382f","partner_tool_call_id":"call_original"}]}"#;
    assert!(
        PublicDeliveryContract::V4
            .parse(duplicate.as_bytes())
            .is_err()
    );
    let message = json!({"deliveries":[{"kind":"message","purpose":"reply","text":"203",
        "interaction_id":null,"artifact_ref":null,"artifact_tool_call_id":null}]});
    assert!(parse(&message));
    assert!(
        PublicDeliveryContract::V3
            .parse(&serde_json::to_vec(&message).unwrap())
            .is_ok()
    );
}
