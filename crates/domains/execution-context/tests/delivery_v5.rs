#![cfg(test)]

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::ContentId;
use serde_json::{Value, json};

const ID: &str = "abeb1e22-074b-48a0-b8e7-d59e2a9f382f";
fn message() -> Value {
    json!({"kind":"message","purpose":"reply","text":"优先级：C → A → B。先保证消息可靠恢复，再支持并发任务，最后完善主题。"})
}
fn interaction() -> Value {
    json!({"kind":"interaction","purpose":"clarification","interaction_id":ID})
}
fn artifact() -> Value {
    json!({"kind":"artifact","purpose":"result","artifact_ref":{"kind":"file_node","id":ID},"artifact_tool_call_id":"call_original_write"})
}
fn partner() -> Value {
    json!({"kind":"partner_created","purpose":"result","creation_command_id":ID,"partner_tool_call_id":"call_original_create"})
}
fn envelope(item: Value) -> Value {
    json!({"deliveries":[item]})
}
fn parse(v: &Value) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryParseError> {
    PublicDeliveryContract::V5.parse(&serde_json::to_vec(v).unwrap())
}
fn schema() -> &'static jsonschema::Validator {
    static SCHEMA: std::sync::OnceLock<jsonschema::Validator> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| {
        jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&serde_json::from_str(PublicDeliveryContract::V5.schema()).unwrap())
            .unwrap()
    })
}
fn assert_valid(v: Value, valid: bool) {
    assert_eq!(schema().is_valid(&v), valid, "schema {v}");
    assert_eq!(parse(&v).is_ok(), valid, "parser {v}");
}

#[test]
fn real_tool_free_failure_shape_is_a_complete_v5_message_but_old_versions_stay_strict() {
    // Actual provider failure shape; public text is shortened, not repaired.
    let answer = envelope(message());
    assert_valid(answer.clone(), true);
    let result = parse(&answer).unwrap();
    assert_eq!(
        result[0].content,
        PublicDeliveryDraft::Message(answer["deliveries"][0]["text"].as_str().unwrap().into())
    );
    for old in [
        PublicDeliveryContract::V1,
        PublicDeliveryContract::V2,
        PublicDeliveryContract::V3,
        PublicDeliveryContract::V4,
    ] {
        assert_eq!(
            old.parse(&serde_json::to_vec(&answer).unwrap()),
            Err(PublicDeliveryParseError::Invalid)
        );
    }
}

#[test]
fn all_kind_purpose_pairs_and_field_occupancy_are_closed() {
    for item in [message(), interaction(), artifact(), partner()] {
        for purpose in [
            "reply",
            "result",
            "important_update",
            "clarification",
            "required_decision",
            "blocked",
            "status",
        ] {
            let mut v = item.clone();
            v["purpose"] = json!(purpose);
            let allowed = match v["kind"].as_str().unwrap() {
                "message" => purpose != "status",
                "interaction" => {
                    ["clarification", "required_decision", "blocked"].contains(&purpose)
                }
                "artifact" => ["reply", "result", "important_update"].contains(&purpose),
                _ => purpose == "result",
            };
            assert_valid(envelope(v), allowed);
        }
        for field in item.as_object().unwrap().keys() {
            let mut v = item.clone();
            v.as_object_mut().unwrap().remove(field);
            assert_valid(envelope(v), false);
            let mut v = item.clone();
            v[field] = Value::Null;
            assert_valid(envelope(v), false);
        }
        for field in [
            "text",
            "interaction_id",
            "artifact_ref",
            "artifact_tool_call_id",
            "creation_command_id",
            "partner_tool_call_id",
            "agent_id",
            "recipient",
            "status",
            "content_id",
        ] {
            if item.get(field).is_some() {
                continue;
            }
            let mut v = item.clone();
            v[field] = Value::Null;
            assert_valid(envelope(v), false);
        }
    }
    assert_valid(
        json!({"deliveries":[message(),interaction(),artifact(),partner()]}),
        true,
    );
    assert_valid(json!({"deliveries":[message()],"status":"done"}), false);
    assert_valid(
        envelope(json!({"kind":"tool","purpose":"reply","text":"hidden"})),
        false,
    );
}

#[test]
fn identifiers_and_tool_handles_remain_original_typed_references() {
    for namespace in ["artifact", "file_node"] {
        let mut item = artifact();
        item["artifact_ref"]["kind"] = json!(namespace);
        let result = parse(&envelope(item)).unwrap();
        match &result[0].content {
            PublicDeliveryDraft::ArtifactFromTool {
                artifact,
                artifact_tool_call_id,
            } => {
                assert_eq!(artifact_tool_call_id.as_str(), "call_original_write");
                assert_eq!(
                    matches!(artifact, AgentArtifactLocator::FileNode(_)),
                    namespace == "file_node"
                );
            }
            _ => panic!("not original artifact reference"),
        }
    }
    assert!(matches!(
        parse(&envelope(interaction())).unwrap()[0].content,
        PublicDeliveryDraft::Interaction(_)
    ));
    assert!(
        matches!(&parse(&envelope(partner())).unwrap()[0].content, PublicDeliveryDraft::PartnerCreated{partner_tool_call_id,..} if partner_tool_call_id.as_str()=="call_original_create")
    );
    for mut item in [interaction(), artifact(), partner()] {
        if item["kind"] == "interaction" {
            item["interaction_id"] = json!("invented");
        } else if item["kind"] == "artifact" {
            item["artifact_ref"]["id"] = json!("invented");
        } else {
            item["creation_command_id"] = json!("invented");
        }
        assert_valid(envelope(item), false);
    }
    for invalid in [
        "".to_string(),
        "contains space".into(),
        "x".repeat(257),
        "不可见".into(),
    ] {
        for (mut item, field) in [
            (artifact(), "artifact_tool_call_id"),
            (partner(), "partner_tool_call_id"),
        ] {
            item[field] = json!(invalid);
            assert_valid(envelope(item), false);
        }
    }
    let mut v = artifact();
    v["artifact_ref"]["kind"] = json!("private_content");
    assert_valid(envelope(v), false);
    let mut v = artifact();
    v["artifact_ref"]["path"] = json!("private/path");
    assert_valid(envelope(v), false);
}

#[test]
fn duplicate_keys_and_duplicate_deliveries_cannot_be_normalized_away() {
    for item in [message(), interaction(), artifact(), partner()] {
        let serialized = serde_json::to_string(&item).unwrap();
        for (field, value) in item.as_object().unwrap() {
            let duplicate = format!(
                "{{\"deliveries\":[{{{field:?}:{value},{}]}}",
                &serialized[1..]
            );
            assert!(
                PublicDeliveryContract::V5
                    .parse(duplicate.as_bytes())
                    .is_err(),
                "{duplicate}"
            );
        }
        assert!(parse(&json!({"deliveries":[item.clone(),item]})).is_err());
    }
    for raw in [
        r#"{"deliveries":[],"deliveries":[{"kind":"message","purpose":"reply","text":"answer"}]}"#,
        r#"{"deliveries":[{"kind":"artifact","purpose":"result","artifact_ref":{"kind":"artifact","kind":"file_node","id":"abeb1e22-074b-48a0-b8e7-d59e2a9f382f"},"artifact_tool_call_id":"call_write"}]}"#,
        r#"{"deliveries":[{"kind":"artifact","purpose":"result","artifact_ref":{"kind":"artifact","id":"abeb1e22-074b-48a0-b8e7-d59e2a9f382f","id":"abeb1e22-074b-48a0-b8e7-d59e2a9f382f"},"artifact_tool_call_id":"call_write"}]}"#,
    ] {
        assert!(PublicDeliveryContract::V5.parse(raw.as_bytes()).is_err());
    }
}

#[test]
fn limits_apply_to_utf8_bytes_and_original_delivery_identity() {
    assert_valid(json!({"deliveries":[]}), false);
    let items = (0..MAX_PUBLIC_DELIVERIES)
        .map(|n| json!({"kind":"message","purpose":"reply","text":format!("answer {n}")}))
        .collect::<Vec<_>>();
    assert_valid(json!({"deliveries":items}), true);
    let mut items = items;
    items.push(message());
    assert_valid(json!({"deliveries":items}), false);
    for text in [" ", "\n\t", ""] {
        let mut v = message();
        v["text"] = json!(text);
        assert_valid(envelope(v), false);
    }
    for text in [
        "x".repeat(MAX_DIALOGUE_MESSAGE_BYTES),
        format!("{}x", "中".repeat((MAX_DIALOGUE_MESSAGE_BYTES - 1) / 3)),
    ] {
        let mut v = message();
        v["text"] = json!(text);
        assert!(parse(&envelope(v)).is_ok());
    }
    let mut v = message();
    v["text"] = json!("中".repeat(MAX_DIALOGUE_MESSAGE_BYTES / 3 + 1));
    assert_eq!(parse(&envelope(v)), Err(PublicDeliveryParseError::TooLarge));
    let text = format!("a{}", "\u{1}".repeat(MAX_DIALOGUE_MESSAGE_BYTES - 1));
    let items = (0..16)
        .map(|n| json!({"kind":"message","purpose":"reply","text":format!("{n:x}{}",&text[1..])}))
        .collect::<Vec<_>>();
    assert_eq!(
        parse(&json!({"deliveries":items})),
        Err(PublicDeliveryParseError::TooLarge)
    );
    let mut second = artifact();
    second["artifact_tool_call_id"] = json!("call_new_version");
    assert!(parse(&json!({"deliveries":[artifact(),second]})).is_err());
}

#[test]
fn v5_pin_remains_explicit_after_new_admissions_advance() {
    let contract = PublicDeliveryContract::V5;
    let prompt = contract.prompt();
    let mut reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
        byte_length: prompt.len() as u64,
        sha256: Sha256::digest(prompt),
    };
    assert_eq!(
        PublicDeliveryContract::from_key(contract.key()),
        Some(contract)
    );
    assert_eq!(
        PublicDeliveryContract::from_prompt_ref(&reference),
        Some(contract)
    );
    reference.byte_length += 1;
    assert_eq!(PublicDeliveryContract::from_prompt_ref(&reference), None);
    assert_eq!(
        PublicDeliveryContract::from_key("musteragent.public-delivery@99"),
        None
    );
}

#[test]
fn correction_is_bounded_version_specific_and_examples_parse_without_tools() {
    for contract in [
        PublicDeliveryContract::V1,
        PublicDeliveryContract::V2,
        PublicDeliveryContract::V3,
        PublicDeliveryContract::V4,
        PublicDeliveryContract::V5,
    ] {
        for issue in [
            PublicDeliveryIssue::InvalidSchema,
            PublicDeliveryIssue::TooLarge,
            PublicDeliveryIssue::InvalidReference,
        ] {
            let text = contract.correction_guidance(issue);
            assert!(text.len() < 3000);
            assert!(text.contains(contract.key()));
            if issue == PublicDeliveryIssue::InvalidSchema {
                let start = text.find("{\"deliveries\":").unwrap();
                let end = text[start..].find("}]}").unwrap() + start + 3;
                let example = &text[start..end];
                let parsed = contract.parse(example.as_bytes()).unwrap();
                assert!(matches!(parsed[0].content, PublicDeliveryDraft::Message(_)));
                assert_eq!(
                    example.contains(":null"),
                    contract != PublicDeliveryContract::V5
                );
            }
        }
    }
}

#[test]
#[ignore = "requires explicitly supplied read-only public-output audit fixture"]
fn replay_original_partner_public_outputs_without_modifying_their_bytes() {
    let path =
        std::env::var("MUSTERAGENT_PUBLIC_DELIVERY_OUTPUT_AUDIT").expect("explicit audit path");
    let audit: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(audit.as_object().unwrap().len(), 2);
    for value in audit.as_object().unwrap().values() {
        let bytes = value["text"].as_str().unwrap().as_bytes();
        let original: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            PublicDeliveryContract::V4.parse(bytes),
            Err(PublicDeliveryParseError::Invalid)
        );
        let parsed = PublicDeliveryContract::V5.parse(bytes).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].content,
            PublicDeliveryDraft::Message(
                original["deliveries"][0]["text"].as_str().unwrap().into()
            )
        );
    }
}

#[test]
fn old_schema_and_sealed_prompt_bytes_remain_frozen() {
    for (contract, schema_hash, prompt_hash) in [
        (
            PublicDeliveryContract::V3,
            "80f87eff46d4f6f816cfd1caf1ae555a8f7ba511fe22f5e30de17d9626a7a5db",
            "e35cd8638bed820ad26f257e7ca627727c29a71f78df167abb7ceb08cb6501e8",
        ),
        (
            PublicDeliveryContract::V4,
            "3bed43bfbff34b31bf93099c4622d2104be3b4ffed897780bed2031cb3b22d66",
            "da197fd80364237d5b0b7ebd1737cb74030499dbf54915ffc5b16369bb25a494",
        ),
        (
            PublicDeliveryContract::V5,
            "f462dec38c178a159c97352d803dc6aec0529e02ab2d5296581e96978d037858",
            "5be650236d9ef07aa58bdadd7138a4e595e57130a58bca3eb25664b6e336740c",
        ),
        (
            PublicDeliveryContract::V6,
            "f462dec38c178a159c97352d803dc6aec0529e02ab2d5296581e96978d037858",
            "6894823d436cce39fa083556e11460948a56f6308f864b3311e9fb25fb24d984",
        ),
    ] {
        assert_eq!(contract.hash().to_string(), schema_hash);
        assert_eq!(Sha256::digest(contract.prompt()).to_string(), prompt_hash);
    }
}
