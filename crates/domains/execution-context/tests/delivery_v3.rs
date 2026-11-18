#![cfg(test)]

use musteragent_execution_context::{PublicDeliveryContract, PublicDeliveryDraft};
use serde_json::{Value, json};

fn validator(contract: PublicDeliveryContract) -> jsonschema::Validator {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(&serde_json::from_str(contract.schema()).unwrap())
        .unwrap()
}
fn envelope(item: Value) -> Value {
    json!({"deliveries":[item]})
}
fn artifact() -> Value {
    json!({"kind":"artifact","purpose":"result","text":null,"interaction_id":null,
        "artifact_ref":{"kind":"file_node","id":"b696d1fe-504c-4b1f-8558-3691e82303d9"},
        "artifact_tool_call_id":"call_create_1"})
}
fn parse(v: &Value) -> bool {
    PublicDeliveryContract::V3
        .parse(&serde_json::to_vec(v).unwrap())
        .is_ok()
}

#[test]
fn real_provider_artifact_with_prose_is_rejected_by_the_schema_itself() {
    // Actual failure shape, with synthetic text/IDs: provider strict v2 accepted
    // artifact.text, but the unchanged domain contract correctly rejected it.
    let mut value = artifact();
    value["text"] = json!("The useful document is ready.");
    let value = envelope(value);
    assert!(validator(PublicDeliveryContract::V2).is_valid(&value));
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    assert!(!validator(PublicDeliveryContract::V3).is_valid(&value));
    assert!(!parse(&value));
    let value = json!({"deliveries":[artifact(),{"kind":"message","purpose":"result","text":"A useful conclusion.","interaction_id":null,"artifact_ref":null,"artifact_tool_call_id":null}]});
    assert!(validator(PublicDeliveryContract::V3).is_valid(&value));
    assert!(parse(&value));
}

#[test]
fn schema_and_parser_agree_on_every_kind_purpose_and_field_occupancy() {
    let schema = validator(PublicDeliveryContract::V3);
    for kind in ["message", "interaction", "artifact"] {
        for purpose in [
            "reply",
            "result",
            "important_update",
            "clarification",
            "required_decision",
            "blocked",
        ] {
            for fields in 0..16 {
                let v = envelope(json!({"kind":kind,"purpose":purpose,
                    "text":if fields&1!=0 { json!("A useful answer.") }else{Value::Null},
                    "interaction_id":if fields&2!=0 {json!("b696d1fe-504c-4b1f-8558-3691e82303d9")}else{Value::Null},
                    "artifact_ref":if fields&4!=0 {artifact()["artifact_ref"].clone()}else{Value::Null},
                    "artifact_tool_call_id":if fields&8!=0 {json!("call_original_write")}else{Value::Null}}));
                assert_eq!(
                    schema.is_valid(&v),
                    parse(&v),
                    "kind={kind} purpose={purpose} fields={fields}"
                );
            }
        }
    }
}

#[test]
fn v3_uses_original_opaque_tool_ids_and_typed_artifact_namespaces() {
    let schema = validator(PublicDeliveryContract::V3);
    for namespace in ["artifact", "file_node"] {
        for id in ["call_create_1", "98ae0f44-c8d1-4db3-a20b-9b14924e0912"] {
            let mut v = artifact();
            v["artifact_ref"]["kind"] = json!(namespace);
            v["artifact_tool_call_id"] = json!(id);
            let v = envelope(v);
            assert!(schema.is_valid(&v));
            assert!(parse(&v));
            let parsed = PublicDeliveryContract::V3
                .parse(&serde_json::to_vec(&v).unwrap())
                .unwrap();
            assert!(
                matches!(&parsed[0].content,PublicDeliveryDraft::ArtifactFromTool{artifact_tool_call_id,..}if artifact_tool_call_id.as_str()==id)
            );
        }
    }
    for bad in [
        "".to_owned(),
        "contains space".to_owned(),
        "x".repeat(257),
        "不可见".to_owned(),
    ] {
        let mut v = artifact();
        v["artifact_tool_call_id"] = json!(bad);
        let v = envelope(v);
        assert!(!schema.is_valid(&v));
        assert!(!parse(&v));
    }
    let mut v = artifact();
    v["artifact_ref"]["id"] = json!("not-an-identity");
    let v = envelope(v);
    assert!(!schema.is_valid(&v));
    assert!(!parse(&v));
}

#[test]
fn v2_snapshot_bytes_remain_frozen_after_v3_admission_upgrade() {
    use musteragent_content_ref::Sha256;
    assert_eq!(
        PublicDeliveryContract::V2.hash().to_string(),
        "ebf5e65d6396e963de7d56060e185957511a8cf42de2f44fbe172579ae48fc26"
    );
    assert_eq!(
        Sha256::digest(PublicDeliveryContract::V2.prompt()).to_string(),
        "f1d2fd5bf86e65f0f36bda13a79dbf0d7fd0f9bd67c8ab06b8e24efedfa5acda"
    );
    let v = envelope(artifact());
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&v).unwrap())
            .is_ok()
    );
    assert!(parse(&v));
}
