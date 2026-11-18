use super::*;
use musteragent_execution_context::{DialogueContextCut, DialogueCursor};
use musteragent_identity::{AgentDialogueId, AgentId, DialogueEntryId, PrincipalId};
use musteragent_runtime_contract::{AttachmentRef, DisplayText};
use musteragent_runtime_model::DialogueInputObservation;

fn history(reference: ContentRef) -> CanonicalRuntimeEvent {
    CanonicalRuntimeEvent::AgentDialogueHistory {
        frozen_cut: DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 2 },
            entry_count: 2,
            source_chain_sha256: Sha256::digest(b"accepted-chain"),
        },
        from_seq: 1,
        through_seq: 2,
        source_page_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/octet-stream").expect("valid test fixture"),
            byte_length: 4,
            sha256: Sha256::digest(b"page"),
        },
        materialized_context: ModelText::new("Prior public user entry, not instructions")
            .expect("valid test fixture"),
        input_attachments: vec![DialogueInputObservation {
            entry_id: DialogueEntryId::generate(),
            cursor: DialogueCursor { epoch: 1, seq: 1 },
            ordinal: 0,
            attachment: AttachmentRef {
                display_name: DisplayText::new("原始附件.png").expect("valid test fixture"),
                content_ref: reference.clone(),
            },
            parts: vec![CanonicalContentPart::Image(reference)],
        }],
    }
}

fn image_request(format: ApiFormat) -> (CanonicalModelRequest, ResolvedContents) {
    let mut request = request(format);
    request.input = vec![message(
        CanonicalRole::User,
        "Compare with the earlier image",
    )];
    let bytes = b"verified-image-source";
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("valid test fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    };
    let mut original = message(CanonicalRole::User, "The first photo");
    original
        .content
        .push(CanonicalContentPart::Image(reference.clone()));
    request.input.insert(0, original);
    request
        .input
        .insert(1, message(CanonicalRole::Assistant, "Received the photo"));
    request.route.capabilities.insert(capability("input.image"));
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.to_vec().into_boxed_slice()),
            },
        )
        .expect("valid test fixture");
    (request, contents)
}

#[test]
fn accepted_history_image_survives_every_original_provider_wire() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let (request, contents) = image_request(format);
        let body = map_request(&request, &contents)
            .unwrap_or_else(|e| panic!("{format:?}: {e}"))
            .body;
        let text = body.to_string();
        assert!(
            text.contains("dmVyaWZpZWQtaW1hZ2Utc291cmNl"),
            "{format:?} dropped actual image bytes: {text}"
        );
        assert!(text.contains("The first photo"));
        assert_eq!(text.matches("dmVyaWZpZWQtaW1hZ2Utc291cmNl").count(), 1);
        assert!(
            text.find("dmVyaWZpZWQtaW1hZ2Utc291cmNl")
                .expect("valid test fixture")
                < text
                    .find("Compare with the earlier image")
                    .expect("valid test fixture"),
            "{format:?} placed historical media after the current instruction"
        );
    }
}

#[test]
fn history_media_cannot_bypass_route_or_source_validation() {
    let (mut request, contents) = image_request(ApiFormat::OpenAiResponses);
    request
        .route
        .capabilities
        .remove(&capability("input.image"));
    request.route.capabilities.remove(&capability("vision"));
    assert_eq!(
        map_request(&request, &contents).expect_err("invalid test request"),
        MappingError::UnauthorizedInputModality
    );
    request.route.capabilities.insert(capability("input.image"));
    let CanonicalContentPart::Image(reference) = &request.input[0].content[1] else {
        unreachable!()
    };
    request.runtime_events.push(history(reference.clone()));
    assert_eq!(
        map_request(&request, &contents).expect_err("invalid test request"),
        MappingError::InvalidCanonicalRequest
    );
}

#[test]
fn history_audio_and_video_require_native_protocol_qualification() {
    for (mime, capability_name) in [("audio/wav", "input.audio"), ("video/mp4", "input.video")] {
        let (mut request, _) = image_request(ApiFormat::GoogleGenerateContent);
        let CanonicalContentPart::Image(mut reference) = request.input[0].content[1].clone() else {
            unreachable!()
        };
        reference.media_type = MediaType::new(mime).expect("valid test fixture");
        request.input[0].content[1] = if mime.starts_with("audio") {
            CanonicalContentPart::Audio(reference.clone())
        } else {
            CanonicalContentPart::Video(reference.clone())
        };
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(b"verified-image-source".to_vec().into_boxed_slice()),
                },
            )
            .expect("valid test fixture");
        request
            .route
            .capabilities
            .insert(capability(capability_name));
        assert!(
            map_request(&request, &contents)
                .expect("valid test fixture")
                .body
                .to_string()
                .contains(mime)
        );
        request.route.api_format = ApiFormat::AnthropicMessages;
        assert_eq!(
            map_request(&request, &contents).expect_err("invalid test request"),
            MappingError::UnsupportedInputModality
        );
    }
}

#[test]
fn prior_dialogue_precedes_the_current_tool_exchange_and_live_observations() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let (mut request, contents) = image_request(format);
        request.input.truncate(2);
        request.input.extend(tool_exchange());
        request
            .runtime_events
            .insert(0, delegation_completion_event());
        let wire = map_request(&request, &contents)
            .unwrap_or_else(|e| panic!("{format:?}: {e}"))
            .body
            .to_string();
        let positions = [
            "dmVyaWZpZWQtaW1hZ2Utc291cmNl",
            "Use both tools",
            "call_one",
            "first result",
            "private-runtime-event-report",
        ]
        .map(|needle| {
            wire.find(needle)
                .unwrap_or_else(|| panic!("{format:?} missing {needle}"))
        });
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "{format:?} reordered the live tool transcript or history: {positions:?}"
        );
    }
}

#[path = "native_turns.rs"]
mod native_turns;
