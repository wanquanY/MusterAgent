use super::*;
use musteragent_context::ContextCodec;
use musteragent_execution_context::PublicDeliveryContract;
use musteragent_runtime_model::CanonicalModelError;

fn request() -> CanonicalModelRequest {
    let mut request = CanonicalJsonContextCodec
        .decode_model_request(&serde_json::to_vec(&fixture()).unwrap(), bound())
        .unwrap();
    let binding = crate::public_delivery_repair_codec_tests::rejection()
        .binding()
        .unwrap();
    request.input[0].role = CanonicalRole::Assistant;
    request.input[0].content = vec![CanonicalContentPart::Text(
        ModelText::new("private candidate stays in Assistant data").unwrap(),
    )];
    request
        .route
        .capabilities
        .insert(musteragent_identity::DottedKey::new("json_schema_output").unwrap());
    request.instructions = vec![ModelText::new(PublicDeliveryContract::V6.prompt()).unwrap()];
    request.response_schema = Some((
        SchemaRef {
            schema_key: binding.contract_ref.clone(),
            schema_hash: PublicDeliveryContract::V6.hash(),
        },
        CanonicalJsonContextCodec
            .decode_json(PublicDeliveryContract::V6.schema().as_bytes())
            .unwrap(),
    ));
    request.runtime_events = vec![CanonicalRuntimeEvent::PublicDeliveryRepair {
        source_invocation_id: binding.rejection_invocation_id,
        binding,
    }];
    request
}
#[test]
fn repair_stimulus_roundtrips_only_in_forward_schema_without_changing_assistant_identity() {
    let request = request();
    request.validate().unwrap();
    let bytes = CanonicalJsonContextCodec
        .encode_model_request(&request)
        .unwrap();
    assert_eq!(
        CanonicalJsonContextCodec
            .decode_model_request(&bytes, bound())
            .unwrap(),
        request
    );
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["schema_version"],
        "musteragent.canonical-model-request.v5"
    );
    assert_eq!(value["input"][0]["role"], "assistant");
    assert!(
        !value["runtime_events"]
            .to_string()
            .contains("private candidate stays")
    );
    for old in ["v2", "v3", "v4"] {
        let mut old_value = value.clone();
        old_value["schema_version"] = json!(format!("musteragent.canonical-model-request.{old}"));
        assert!(
            CanonicalJsonContextCodec
                .decode_model_request(&serde_json::to_vec(&old_value).unwrap(), bound())
                .is_err()
        );
    }
    let mut removed = request.clone();
    removed.runtime_events.clear();
    assert_eq!(
        removed.validate(),
        Err(CanonicalModelError::MissingTurnStimulus)
    );
    let mut unknown = value.clone();
    unknown["runtime_events"][0]["extra_authority"] = json!(true);
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&unknown).unwrap(), bound())
            .is_err()
    );
}
#[test]
fn repair_stimulus_rejects_source_pin_duplicate_forced_tool_and_auxiliary_route() {
    let original = request();
    let mut changed = original.clone();
    if let CanonicalRuntimeEvent::PublicDeliveryRepair {
        source_invocation_id,
        ..
    } = &mut changed.runtime_events[0]
    {
        *source_invocation_id = changed.invocation.invocation_id;
    }
    assert_eq!(
        changed.validate(),
        Err(CanonicalModelError::InvalidPublicDeliveryRepair)
    );
    let mut changed = original.clone();
    changed
        .runtime_events
        .push(changed.runtime_events[0].clone());
    assert_eq!(
        changed.validate(),
        Err(CanonicalModelError::InvalidPublicDeliveryRepair)
    );
    let mut changed = original.clone();
    changed.route.parameters.insert(
        musteragent_identity::DottedKey::new("tool_choice").unwrap(),
        ModelParameter::String("required".into()),
    );
    assert_eq!(
        changed.validate(),
        Err(CanonicalModelError::InvalidPublicDeliveryRepair)
    );
    let mut changed = original.clone();
    changed.route_role =
        ModelInvocationRouteRole::Auxiliary(musteragent_model::AuxiliaryModelRole::Compression);
    assert_eq!(
        changed.validate(),
        Err(CanonicalModelError::RuntimeEventOnAuxiliaryRoute)
    );
    let mut value: Value = serde_json::from_slice(
        &CanonicalJsonContextCodec
            .encode_model_request(&original)
            .unwrap(),
    )
    .unwrap();
    value["runtime_events"][0]["source_invocation_id"] =
        json!(original.invocation.invocation_id.to_string());
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&value).unwrap(), bound())
            .is_err()
    );
    value["runtime_events"][0]["binding_protobuf_base64"] = json!("AAAA");
    assert!(
        CanonicalJsonContextCodec
            .decode_model_request(&serde_json::to_vec(&value).unwrap(), bound())
            .is_err()
    );
}
#[test]
fn v7_canonical_request_uses_its_exact_sealed_policy_and_rejects_v6_relabelling() {
    use musteragent_execution_context::{
        PublicDeliveryDiagnostic, PublicDeliveryDiagnosticKind, PublicDeliveryRepairDisposition,
    };
    let mut request = request();
    let mut prerequisite = crate::public_delivery_repair_codec_tests::rejection();
    prerequisite.diagnostics = vec![
        PublicDeliveryDiagnostic::new(
            PublicDeliveryDiagnosticKind::ReferenceUnavailable,
            "/deliveries/0/interaction_id",
        )
        .unwrap(),
    ];
    prerequisite.disposition = PublicDeliveryRepairDisposition::PrerequisiteRecovery;
    let binding = prerequisite
        .binding_for(PublicDeliveryContract::V7)
        .unwrap();
    request.runtime_events = vec![CanonicalRuntimeEvent::PublicDeliveryRepair {
        source_invocation_id: binding.rejection_invocation_id,
        binding,
    }];
    request.instructions = vec![ModelText::new(PublicDeliveryContract::V7.prompt()).unwrap()];
    request.response_schema.as_mut().unwrap().0.schema_key =
        musteragent_identity::VersionedKey::new(PublicDeliveryContract::V7.key()).unwrap();
    request.validate().unwrap();
    let bytes = CanonicalJsonContextCodec
        .encode_model_request(&request)
        .unwrap();
    assert_eq!(
        CanonicalJsonContextCodec
            .decode_model_request(&bytes, bound())
            .unwrap(),
        request
    );
    let mut wrong = request.clone();
    wrong.instructions = vec![ModelText::new(PublicDeliveryContract::V6.prompt()).unwrap()];
    assert_eq!(
        wrong.validate(),
        Err(CanonicalModelError::InvalidPublicDeliveryRepair)
    );
    let mut wrong = request;
    wrong.response_schema.as_mut().unwrap().0.schema_key =
        musteragent_identity::VersionedKey::new(PublicDeliveryContract::V6.key()).unwrap();
    assert_eq!(
        wrong.validate(),
        Err(CanonicalModelError::InvalidPublicDeliveryRepair)
    );
}
