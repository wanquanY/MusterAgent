use super::*;
use musteragent_execution_context::DialogueOutputContract;

fn request() -> CanonicalModelRequest {
    let mut v = CanonicalJsonContextCodec
        .decode_model_request(&serde_json::to_vec(&fixture()).unwrap(), bound())
        .unwrap();
    v.input[0].role = musteragent_runtime_model::CanonicalRole::Assistant;
    v.instructions = vec![ModelText::new(DialogueOutputContract::NativeToolsV1.prompt()).unwrap()];
    v.runtime_events = vec![CanonicalRuntimeEvent::NativePublicationRequired {
        source_invocation_id: SECOND.parse().unwrap(),
    }];
    v
}

#[test]
fn native_publication_v8_roundtrip_rejects_old_schema_aliases_and_unknown_fields() {
    let request = request();
    let bytes = CanonicalJsonContextCodec
        .encode_model_request(&request)
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["schema_version"],
        "musteragent.canonical-model-request.v8"
    );
    assert_eq!(
        CanonicalJsonContextCodec
            .decode_model_request(&bytes, bound())
            .unwrap(),
        request
    );
    for schema in 2..=7 {
        let mut old = value.clone();
        old["schema_version"] = json!(format!("musteragent.canonical-model-request.v{schema}"));
        assert!(
            CanonicalJsonContextCodec
                .decode_model_request(&serde_json::to_vec(&old).unwrap(), bound())
                .is_err()
        );
    }
    for bad in [
        json!({"kind":"native_publication_required","source_invocation_id":SECOND,"authority":"allow"}),
        json!({"kind":"native_publication_required","source_invocation_id":ID}),
        json!({"kind":"native_publication_required_v2","source_invocation_id":SECOND}),
    ] {
        let mut invalid = value.clone();
        invalid["runtime_events"][0] = bad;
        assert!(
            CanonicalJsonContextCodec
                .decode_model_request(&serde_json::to_vec(&invalid).unwrap(), bound())
                .is_err()
        );
    }
}
