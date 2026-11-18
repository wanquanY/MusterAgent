#![cfg(test)]

use musteragent_execution_context::*;
use serde_json::json;

fn message(text: &str) -> serde_json::Value {
    json!({"kind":"message","purpose":"reply","text":text,"interaction_id":null,"artifact_id":null,"artifact_version_id":null})
}
fn parse(value: serde_json::Value) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryParseError> {
    parse_public_delivery(&serde_json::to_vec(&value).unwrap())
}
#[test]
fn only_explicit_complete_candidates_are_deliverable() {
    let parsed = parse(json!({"deliveries":[message("周五可以。")]})).unwrap();
    assert_eq!(
        parsed[0].content,
        PublicDeliveryDraft::Message("周五可以。".into())
    );
    for raw in [
        b"A plain assistant response".as_slice(),
        br#"{"deliveries":[{"kind":"message"}]}"#,
        br#"{"deliveries":[],"status":"completed"}"#,
        br#"{"deliveries":[],"deliveries":[]}"#,
    ] {
        assert!(parse_public_delivery(raw).is_err());
    }
    assert!(parse(json!({"deliveries":[]})).is_err());
    let mut m = message("A useful answer");
    m["content_id"] = json!("some-private-file");
    assert!(parse(json!({"deliveries":[m]})).is_err());
    let mut m = message("A useful answer");
    m.as_object_mut().unwrap().remove("interaction_id");
    assert!(parse(json!({"deliveries":[m]})).is_err());
}
#[test]
fn contract_rejects_forged_types_duplicate_files_and_oversized_text() {
    let artifact = musteragent_identity::ArtifactId::generate();
    let version = musteragent_identity::ArtifactVersionId::generate();
    let card = json!({"kind":"artifact","purpose":"result","text":null,"interaction_id":null,"artifact_id":artifact.to_string(),"artifact_version_id":version.to_string()});
    assert!(parse(json!({"deliveries":[card.clone()]})).is_ok());
    assert!(parse(json!({"deliveries":[card.clone(),card]})).is_err());
    let mut invalid = message("Here is a result");
    invalid["kind"] = json!("tool_call");
    assert!(parse(json!({"deliveries":[invalid]})).is_err());
    assert!(
        parse(json!({"deliveries":[message(&"x".repeat(MAX_DIALOGUE_MESSAGE_BYTES+1))]})).is_err()
    );
    assert!(parse(json!({"deliveries":[message("   ")]})).is_err());
}

#[test]
fn frozen_v1_bytes_and_decoder_survive_the_v2_upgrade() {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;
    assert_eq!(
        public_delivery_contract_hash().to_string(),
        "d6e978ba77f3dd5bf440d1c245f2c669bb431993e9859f91b8cd486b7053e2e7"
    );
    assert_eq!(
        Sha256::digest(public_delivery_prompt()).to_string(),
        "8cea9dbbc257cb12e9070187d04975708c288ce8788ec947c0d0ced87ec82839"
    );
    for contract in [
        PublicDeliveryContract::V1,
        PublicDeliveryContract::V2,
        PublicDeliveryContract::V3,
    ] {
        let prompt = contract.prompt();
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
            byte_length: prompt.len() as u64,
            sha256: Sha256::digest(prompt),
        };
        assert_eq!(
            PublicDeliveryContract::from_prompt_ref(&reference),
            Some(contract)
        );
        assert_eq!(
            PublicDeliveryContract::from_key(contract.key()),
            Some(contract)
        );
        let mut forged = reference;
        forged.byte_length += 1;
        assert_eq!(PublicDeliveryContract::from_prompt_ref(&forged), None);
    }
    assert_eq!(
        PublicDeliveryContract::from_key("musteragent.public-delivery@99"),
        None
    );
    let bytes =
        serde_json::to_vec(&json!({"deliveries":[message("Old admitted task still answers.")]}))
            .unwrap();
    assert!(PublicDeliveryContract::V1.parse(&bytes).is_ok());
    assert!(PublicDeliveryContract::V2.parse(&bytes).is_err());
}

#[test]
fn v2_uses_typed_locator_and_visible_tool_call_handle() {
    use musteragent_identity::{ArtifactId, ToolCallId};
    let id = ArtifactId::generate();
    let call = ToolCallId::new("call_file_create_1").unwrap();
    let card = json!({"kind":"artifact","purpose":"result","text":null,"interaction_id":null,
        "artifact_ref":{"kind":"artifact","id":id.to_string()},"artifact_tool_call_id":call.as_str()});
    let bytes = serde_json::to_vec(&json!({"deliveries":[card.clone()]})).unwrap();
    let parsed = PublicDeliveryContract::V2.parse(&bytes).unwrap();
    assert_eq!(
        parsed[0].content,
        PublicDeliveryDraft::ArtifactFromTool {
            artifact: AgentArtifactLocator::Artifact(id),
            artifact_tool_call_id: call
        }
    );
    assert!(PublicDeliveryContract::V1.parse(&bytes).is_err());
    for field in ["artifact_version_id", "content_id", "recipient"] {
        let mut invalid = card.clone();
        invalid[field] = json!("internal-authority-not-a-model-handle");
        assert!(
            PublicDeliveryContract::V2
                .parse(&serde_json::to_vec(&json!({"deliveries":[invalid]})).unwrap())
                .is_err()
        );
    }
    let mut missing = card.clone();
    missing
        .as_object_mut()
        .unwrap()
        .remove("artifact_tool_call_id");
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&json!({"deliveries":[missing]})).unwrap())
            .is_err()
    );
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&json!({"deliveries":[card.clone(),card]})).unwrap())
            .is_err()
    );
}

#[test]
fn v2_locator_namespaces_are_explicit_and_closed() {
    use musteragent_identity::ResourceNodeId;
    let node = ResourceNodeId::generate();
    let mut value = json!({"deliveries":[{"kind":"artifact","purpose":"result","text":null,"interaction_id":null,
        "artifact_ref":{"kind":"file_node","id":node.to_string()},"artifact_tool_call_id":"call-create"}]});
    let parsed = PublicDeliveryContract::V2
        .parse(&serde_json::to_vec(&value).unwrap())
        .unwrap();
    assert!(
        matches!(parsed[0].content,PublicDeliveryDraft::ArtifactFromTool { artifact: AgentArtifactLocator::FileNode(id), .. } if id==node)
    );
    value["deliveries"][0]["artifact_ref"]["kind"] = json!("file_path");
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    value["deliveries"][0]["artifact_ref"] =
        json!({"kind":"artifact","id":node.to_string(),"owner":"other"});
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    value["deliveries"][0]
        .as_object_mut()
        .unwrap()
        .remove("artifact_ref");
    assert!(
        PublicDeliveryContract::V2
            .parse(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
}
