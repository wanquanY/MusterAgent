use super::*;
use musteragent_execution_context::{DialogueContextCut, DialogueCursor};
use musteragent_runtime_contract::{AttachmentRef, DisplayText};
use musteragent_runtime_model::{CanonicalRuntimeEvent, DialogueInputObservation};

fn event() -> CanonicalRuntimeEvent {
    let id = "11111111-1111-4111-8111-111111111111";
    let image = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").unwrap(),
        byte_length: 3,
        sha256: Sha256::digest(b"png"),
    };
    CanonicalRuntimeEvent::AgentDialogueHistory {
        frozen_cut: DialogueContextCut {
            tenant_id: id.parse().unwrap(),
            owner_id: id.parse().unwrap(),
            agent_id: id.parse().unwrap(),
            dialogue_id: id.parse().unwrap(),
            through: DialogueCursor { epoch: 1, seq: 1 },
            entry_count: 1,
            source_chain_sha256: Sha256::digest(b"chain"),
        },
        from_seq: 1,
        through_seq: 1,
        source_page_ref: image.clone(),
        materialized_context: ModelText::new("source history").unwrap(),
        input_attachments: vec![DialogueInputObservation {
            entry_id: id.parse().unwrap(),
            cursor: DialogueCursor { epoch: 1, seq: 1 },
            ordinal: 0,
            attachment: AttachmentRef {
                content_ref: image.clone(),
                display_name: DisplayText::new("photo.png").unwrap(),
            },
            parts: vec![CanonicalContentPart::Image(image)],
        }],
    }
}

#[test]
fn dialogue_input_history_and_current_images_share_one_signed_budget() {
    let event = event();
    let input = CanonicalMessage {
        role: CanonicalRole::User,
        content: event.dialogue_inputs()[0].parts.clone(),
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let mut route = model_route();
    route.capabilities.insert(DottedKey::new("vision").unwrap());
    route.parameters.insert(
        DottedKey::new("vision.max_images_per_request").unwrap(),
        ModelParameter::Int(1),
    );
    route.parameters.insert(
        DottedKey::new("vision.max_bytes_per_image").unwrap(),
        ModelParameter::Int(1024),
    );
    route.parameters.insert(
        DottedKey::new("vision.max_total_image_bytes").unwrap(),
        ModelParameter::Int(2048),
    );
    let validate = super::super::dialogue::validate_dialogue_input_capacity;
    assert!(validate(&route, std::slice::from_ref(&input)).is_ok());
    assert!(validate(&route, &[input.clone(), input.clone()]).is_err());
    route.capabilities.clear();
    assert!(validate(&route, &[input]).is_err());
    assert!(event.model_input_text().is_err());
}

#[test]
fn real_six_4k_inspection_preserves_history_without_spending_image_capacity_twice() {
    let mut history = event();
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments, ..
    } = &mut history
    else {
        unreachable!()
    };
    let sample = input_attachments[0].clone();
    // Exact sizes from the failing desktop Run, including unrelated old
    // artwork and the user's original logo. No image bytes need fabrication.
    let history_sizes = [
        137156, 1397548, 277569, 1397548, 277569, 2914216, 2839201, 16668, 16668,
    ];
    *input_attachments = history_sizes
        .into_iter()
        .enumerate()
        .map(|(ordinal, size)| {
            let mut value = sample.clone();
            value.ordinal = ordinal as u32;
            value.attachment.content_ref.content_id = ContentId::generate();
            value.attachment.content_ref.byte_length = size;
            value.parts = vec![CanonicalContentPart::Image(
                value.attachment.content_ref.clone(),
            )];
            value
        })
        .collect();
    let original = history.clone();
    let mut route = model_route();
    route.capabilities.insert(DottedKey::new("vision").unwrap());
    let mut inspection = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![],
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    for size in [7160929, 7491286, 7017025, 6946617, 7172076, 7614045] {
        let mut reference = sample.attachment.content_ref.clone();
        reference.content_id = ContentId::generate();
        reference.byte_length = size;
        inspection
            .content
            .push(CanonicalContentPart::Image(reference));
    }
    let validate = |inspection: &CanonicalMessage, history: &CanonicalRuntimeEvent| {
        let mut historical = inspection.clone();
        historical.content = history
            .dialogue_inputs()
            .iter()
            .flat_map(|observation| observation.parts.iter().cloned())
            .collect();
        super::super::dialogue::validate_dialogue_input_capacity(
            &route,
            &[historical, inspection.clone()],
        )
    };
    assert_eq!(
        validate(&inspection, &history).unwrap_err().code,
        ErrorCode::ModelContextExceeded
    );
    super::super::dialogue_images::present_history(
        &route,
        std::slice::from_ref(&inspection),
        std::slice::from_mut(&mut history),
    )
    .unwrap();
    validate(&inspection, &history).unwrap();
    history.validate_dialogue_inputs().unwrap();
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments: presented,
        ..
    } = history
    else {
        unreachable!()
    };
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments: source,
        ..
    } = original
    else {
        unreachable!()
    };
    assert_eq!(presented.len(), source.len());
    for (shown, source) in presented.iter().zip(&source) {
        assert_eq!(shown.attachment, source.attachment);
        assert_eq!(shown.entry_id, source.entry_id);
    }
    assert!(
        presented
            .iter()
            .any(|value| matches!(value.parts.as_slice(), [CanonicalContentPart::Text(_)]))
    );
    assert!(matches!(
        presented.last().unwrap().parts.as_slice(),
        [CanonicalContentPart::Image(_)]
    ));
    // An old checkpoint remains readable; substituting the retained source
    // cannot turn its projection into authorized evidence for another image.
    let mut changed = presented
        .iter()
        .find(|value| matches!(value.parts.as_slice(), [CanonicalContentPart::Text(_)]))
        .unwrap()
        .clone();
    changed.attachment.content_ref.sha256 = Sha256::digest(b"substituted");
    assert_ne!(
        changed.parts[0],
        musteragent_runtime_model::DialogueInputObservation::historical_image_reference(
            &changed.attachment.content_ref
        )
        .unwrap()
    );
}

#[test]
fn dialogue_input_text_observation_checks_actual_content_hash_not_only_length() {
    let mut event = event();
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments, ..
    } = &mut event
    else {
        unreachable!()
    };
    let input = &mut input_attachments[0];
    input.attachment.content_ref.media_type = MediaType::new("text/plain").unwrap();
    input.attachment.content_ref.sha256 = Sha256::digest(b"yes");
    input.parts = vec![CanonicalContentPart::Text(
        ModelText::new("[Attached text begins]\nyes\n[Attached text ends]").unwrap(),
    )];
    assert!(event.validate_dialogue_inputs().is_ok());
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments, ..
    } = &mut event
    else {
        unreachable!()
    };
    input_attachments[0].parts = vec![CanonicalContentPart::Text(
        ModelText::new("[Attached text begins]\nnot\n[Attached text ends]").unwrap(),
    )];
    assert!(event.validate_dialogue_inputs().is_err());
}

#[tokio::test]
async fn dialogue_input_zero_body_requires_real_attachments_and_verified_zero_bytes() {
    use musteragent_runtime_content::{ContentBatch, ContentStore, ContentStoreError, PutContent};
    use musteragent_runtime_contract::ApplicationFuture;
    struct Empty;
    impl ContentStore for Empty {
        fn put(
            &self,
            _: PutContent,
        ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
            Box::pin(async { panic!("read only") })
        }
        fn read<'a>(
            &'a self,
            _: musteragent_identity::TenantId,
            _: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            Box::pin(async { Ok(vec![]) })
        }
    }
    let empty = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").unwrap(),
        byte_length: 0,
        sha256: Sha256::digest([]),
    };
    let tenant = "11111111-1111-4111-8111-111111111111".parse().unwrap();
    let batch = ContentBatch::load(&Empty, tenant, std::slice::from_ref(&empty))
        .await
        .unwrap();
    assert!(
        crate::materialize_input_body_from_batch(&batch, &empty, true)
            .unwrap()
            .is_empty()
    );
    assert!(crate::materialize_input_body_from_batch(&batch, &empty, false).is_err());
    let mut forged = empty;
    forged.sha256 = Sha256::digest(b"forged");
    assert!(crate::materialize_input_body_from_batch(&batch, &forged, true).is_err());
}

#[test]
fn dialogue_office_metadata_is_readable_without_provider_document_transport() {
    let mut event = event();
    let CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments, ..
    } = &mut event
    else {
        unreachable!()
    };
    let input = &mut input_attachments[0];
    input.attachment.content_ref.media_type =
        MediaType::new("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            .unwrap();
    input.attachment.display_name = DisplayText::new("sales.xlsx").unwrap();
    input.parts.clear();
    assert!(event.validate_dialogue_inputs().is_ok());
    assert!(
        event.model_input_text().is_err(),
        "old observation cannot be used as provider input"
    );
}

#[test]
fn dialogue_media_cannot_be_silently_dropped_as_document_metadata() {
    for mime in ["image/png", "audio/mpeg", "video/mp4", "text/plain"] {
        let mut event = event();
        let CanonicalRuntimeEvent::AgentDialogueHistory {
            input_attachments, ..
        } = &mut event
        else {
            unreachable!()
        };
        input_attachments[0].attachment.content_ref.media_type = MediaType::new(mime).unwrap();
        input_attachments[0].parts.clear();
        assert!(event.validate_dialogue_inputs().is_err(), "{mime}");
    }
}

#[test]
fn current_document_projection_preserves_source_identity_without_native_upload() {
    let file = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        .unwrap(),
        byte_length: 4096,
        sha256: Sha256::digest(b"opaque document"),
    };
    let original = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![
            CanonicalContentPart::Text(ModelText::new("Convert this document").unwrap()),
            CanonicalContentPart::File(file.clone()),
        ],
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let projected =
        super::super::route_presentation::present_message_without_opaque_documents(&original)
            .unwrap();
    assert!(
        projected
            .content
            .iter()
            .all(|part| matches!(part, CanonicalContentPart::Text(_)))
    );
    assert!(projected.content.iter().any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains(&file.content_id.to_string()))));
    assert!(
        super::super::dialogue::validate_dialogue_input_capacity(&model_route(), &[projected])
            .is_ok()
    );
    assert!(
        matches!(&original.content[1], CanonicalContentPart::File(reference) if reference == &file)
    );
    // Projection must not hide unsupported native media from the capacity check.
    let image = CanonicalMessage {
        content: event().dialogue_inputs()[0].parts.clone(),
        ..original
    };
    let projected =
        super::super::route_presentation::present_message_without_opaque_documents(&image).unwrap();
    assert!(
        super::super::dialogue::validate_dialogue_input_capacity(&model_route(), &[projected])
            .is_err()
    );
}
