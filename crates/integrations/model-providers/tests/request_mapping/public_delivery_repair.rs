use super::*;
use musteragent_execution_context::*;
use musteragent_identity::{ConversationSeq, MessageId, OutputId};

fn binding() -> PublicDeliveryRepairBinding {
    let text = b"PRIVATE_CANDIDATE_DATA_726";
    PublicDeliveryRejectionV1 {
        candidate: PublicDeliveryCandidateProof {
            invocation_id: InvocationId::generate(),
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            ledger_cutoff: ConversationSeq::new(3).expect("valid test fixture"),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").expect("valid test fixture"),
                byte_length: text.len() as u64,
                sha256: Sha256::digest(text),
            },
        },
        diagnostics: vec![
            PublicDeliveryDiagnostic::new(PublicDeliveryDiagnosticKind::MalformedJson, "/")
                .expect("valid test fixture"),
        ],
        disposition: PublicDeliveryRepairDisposition::FormatOnly,
        cut: PublicDeliveryRepairCut {
            progress_sequence: None,
            control_sequence: None,
        },
    }
    .binding()
    .expect("valid test fixture")
}
fn repair_request(format: ApiFormat) -> CanonicalModelRequest {
    let mut request = request(format);
    let binding = binding();
    request.input = vec![message(
        CanonicalRole::Assistant,
        "PRIVATE_CANDIDATE_DATA_726",
    )];
    request.runtime_events = vec![CanonicalRuntimeEvent::PublicDeliveryRepair {
        source_invocation_id: binding.rejection_invocation_id,
        binding: binding.clone(),
    }];
    request.instructions =
        vec![ModelText::new(PublicDeliveryContract::V6.prompt()).expect("valid test fixture")];
    request.tools.clear();
    request
        .route
        .capabilities
        .insert(capability("json_schema_output"));
    request.response_schema = Some((
        SchemaRef {
            schema_key: binding.contract_ref,
            schema_hash: PublicDeliveryContract::V6.hash(),
        },
        super::structured_output::canonical(
            serde_json::from_str(PublicDeliveryContract::V6.schema()).expect("valid test fixture"),
        ),
    ));
    request
}
#[test]
fn repair_is_a_private_canonical_event_and_uses_original_provider_event_projection() {
    for format in [
        ApiFormat::OpenAiResponses,
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let request = repair_request(format);
        let unchanged = request.clone();
        request.validate().expect("valid test fixture");
        let body = map_request(&request, &ResolvedContents::default())
            .unwrap_or_else(|e| panic!("{format:?}: {e}"))
            .body;
        let text = body.to_string();
        assert_eq!(
            text.matches("Runtime event: private Dialogue delivery repair")
                .count(),
            1,
            "{format:?}"
        );
        assert_eq!(
            text.matches("PRIVATE_CANDIDATE_DATA_726").count(),
            1,
            "candidate is not duplicated by runtime event"
        );
        assert_eq!(request, unchanged);
        assert_eq!(request.input[0].role, CanonicalRole::Assistant);
        assert!(request.tools.is_empty());
    }
}
#[test]
fn repair_cannot_use_a_forced_tool_route_or_gain_a_tool_in_the_adapter() {
    for format in [ApiFormat::OpenAiResponses, ApiFormat::OpenAiChatCompletions] {
        let mut request = repair_request(format);
        request.route.parameters.insert(
            musteragent_identity::DottedKey::new("tool_choice").expect("valid test fixture"),
            ModelParameter::String("required".into()),
        );
        assert!(map_request(&request, &ResolvedContents::default()).is_err());
        request.route.parameters.clear();
        request.tools.push(tool_definition());
        assert!(map_request(&request, &ResolvedContents::default()).is_err());
    }
}
