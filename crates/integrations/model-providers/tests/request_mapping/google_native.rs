use super::*;
use serde_json::json;

#[test]
fn interactions_accepts_portable_assistant_history_without_enabling_storage() {
    let mut request = request(ApiFormat::GoogleInteractions);
    request.input = vec![
        message(CanonicalRole::User, "earlier question"),
        message(CanonicalRole::Assistant, "earlier answer"),
        message(CanonicalRole::User, "continue"),
    ];
    let original = request.clone();
    let body = map_request(&request, &ResolvedContents::default())
        .expect("portable history")
        .body;
    assert_eq!(body["store"], false);
    assert_eq!(body["input"][1]["type"], "model_output");
    assert_eq!(body["input"][1]["content"][0]["text"], "earlier answer");
    assert!(body.get("previous_interaction_id").is_none());
    assert_eq!(request, original);
}

#[test]
fn google_native_full_replay_preserves_text_signatures_and_interaction_steps_once() {
    for format in [
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
    ] {
        let mut request = request(format);
        request.input = vec![
            message(CanonicalRole::User, "earlier question"),
            message(CanonicalRole::Assistant, "visible answer"),
            message(CanonicalRole::User, "continue"),
        ];
        let items = if format == ApiFormat::GoogleGenerateContent {
            serde_json::json!([{"type":"google_content","parts":[{"text":"visible answer","thoughtSignature":"private-native-signature"}]}])
        } else {
            serde_json::json!([{"type":"thought","signature":"private-native-signature","summary":[]},{"type":"model_output","content":[{"type":"text","text":"visible answer"}]}])
        };
        let bytes = serde_json::to_vec(&serde_json::json!({"schema":"musteragent.legacy.runtime.provider-reasoning-replay.v2", "source_api_format":format.as_str(), "source_model_key":request.route.resolved_model_key, "items":items})).expect("valid test fixture");
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(
                "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            )
            .expect("valid test fixture"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        };
        request.input[1].reasoning = Some(ReasoningReplay {
            content_ref: reference.clone(),
            source_route_ref: ModelRouteRef::from_key(request.route.route_ref.clone()),
            continuation_ref: None,
        });
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.into_boxed_slice()),
                },
            )
            .expect("valid test fixture");
        let body = map_request(&request, &contents)
            .expect("valid test fixture")
            .body;
        let text = body.to_string();
        assert_eq!(text.matches("visible answer").count(), 1, "{format:?}");
        assert_eq!(
            text.matches("private-native-signature").count(),
            1,
            "{format:?}"
        );
        // A changed source route must never lend its private state to this request.
        request.input[1]
            .reasoning
            .as_mut()
            .expect("valid test fixture")
            .source_route_ref = ModelRouteRef::from_key(versioned("foreign.route@1"));
        let portable = map_request(&request, &contents)
            .expect("valid test fixture")
            .body
            .to_string();
        assert!(!portable.contains("private-native-signature"));
        assert!(portable.contains("visible answer"));
    }
}

#[test]
fn interactions_encodes_audio_video_with_the_native_mime_and_data_contract() {
    let mut request = request(ApiFormat::GoogleInteractions);
    request.input = vec![message(CanonicalRole::User, "inspect")];
    let mut contents = ResolvedContents::default();
    for (mime, modality) in [("audio/wav", "input.audio"), ("video/mp4", "input.video")] {
        let bytes = b"native fixture".to_vec();
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(mime).expect("valid test fixture"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        };
        request.input[0].content.push(if mime.starts_with("audio") {
            CanonicalContentPart::Audio(reference.clone())
        } else {
            CanonicalContentPart::Video(reference.clone())
        });
        request.route.capabilities.insert(capability(modality));
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.into_boxed_slice()),
                },
            )
            .expect("valid test fixture");
    }
    let body = map_request(&request, &contents)
        .expect("valid test fixture")
        .body;
    assert_eq!(body["input"][0]["content"][1]["type"], "audio");
    assert_eq!(body["input"][0]["content"][1]["mime_type"], "audio/wav");
    assert!(body["input"][0]["content"][1]["data"].is_string());
    assert_eq!(body["input"][0]["content"][2]["type"], "video");
}

#[test]
fn google_chunked_private_replay_restores_large_native_parts_losslessly() {
    let mut request = request(ApiFormat::GoogleGenerateContent);
    request.input = vec![
        message(CanonicalRole::User, "earlier"),
        message(CanonicalRole::Assistant, "portable text"),
        message(CanonicalRole::User, "continue"),
    ];
    let text = "x".repeat(1_100_000);
    let native = json!({"type":"google_content","parts":[{"text":text,"thoughtSignature":"signed-boundary"}]});
    let encoded = native.to_string();
    let chunks: Vec<_> = encoded
        .as_bytes()
        .chunks(524_288)
        .map(|chunk| String::from_utf8(chunk.to_vec()).expect("valid test fixture"))
        .collect();
    let bytes=serde_json::to_vec(&json!({"schema":"musteragent.legacy.runtime.provider-reasoning-replay.v2","source_api_format":ApiFormat::GoogleGenerateContent.as_str(),"source_model_key":request.route.resolved_model_key,"items":[{"type":"google_native_chunks","wire_chunks":chunks}]})).expect("valid test fixture");
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(
            "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
        )
        .expect("valid test fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    request.input[1].reasoning = Some(ReasoningReplay {
        content_ref: reference.clone(),
        source_route_ref: ModelRouteRef::from_key(request.route.route_ref.clone()),
        continuation_ref: None,
    });
    let mut contents = ResolvedContents::default();
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("valid test fixture");
    let body = map_request(&request, &contents)
        .expect("valid test fixture")
        .body;
    assert!(
        body["contents"][1]["parts"] == native["parts"],
        "large signed native Parts must roundtrip exactly"
    );
}

#[test]
fn native_tool_result_ids_match_authorized_replay_even_with_nonportable_provider_ids() {
    for format in [
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
    ] {
        let mut request = request(format);
        let id = format!("native/+{}", "a".repeat(100));
        request.input[1].tool_calls.truncate(1);
        request.input[1].tool_calls[0].call_id =
            ToolCallId::new(id.clone()).expect("valid test fixture");
        request.input[2].tool_call_id =
            Some(ToolCallId::new(id.clone()).expect("valid test fixture"));
        request.input.truncate(3);
        let items = if format == ApiFormat::GoogleGenerateContent {
            json!([{"type":"google_content","parts":[{"functionCall":{"id":id,"name":"lookup","args":{"value":"call_one"}},"thoughtSignature":"signature"}]}])
        } else {
            json!([{"type":"function_call","id":id,"name":"lookup","arguments":{"value":"call_one"}}])
        };
        let bytes=serde_json::to_vec(&json!({"schema":"musteragent.legacy.runtime.provider-reasoning-replay.v2","source_api_format":format.as_str(),"source_model_key":request.route.resolved_model_key,"items":items})).expect("valid test fixture");
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(
                "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            )
            .expect("valid test fixture"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        };
        request.input[1].reasoning = Some(ReasoningReplay {
            content_ref: reference.clone(),
            source_route_ref: ModelRouteRef::from_key(request.route.route_ref.clone()),
            continuation_ref: None,
        });
        let mut contents = ResolvedContents::default();
        contents
            .insert(
                &reference,
                ResolvedContent::Inline {
                    media_type: reference.media_type.clone(),
                    bytes: SecretSlice::new(bytes.into_boxed_slice()),
                },
            )
            .expect("valid test fixture");
        let body = map_request(&request, &contents)
            .expect("valid test fixture")
            .body;
        if format == ApiFormat::GoogleGenerateContent {
            assert_eq!(
                body["contents"][2]["parts"][0]["functionResponse"]["id"],
                id
            );
        } else {
            assert_eq!(body["input"][2]["call_id"], id);
        }
    }
}
