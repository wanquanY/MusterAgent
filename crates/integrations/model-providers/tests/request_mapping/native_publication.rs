use super::*;
use musteragent_execution_context::DialogueOutputContract;

fn native_request(format: ApiFormat) -> CanonicalModelRequest {
    let mut value = request(format);
    value.input = vec![message(
        CanonicalRole::Assistant,
        "private completed response",
    )];
    value.instructions = vec![
        ModelText::new(DialogueOutputContract::NativeToolsV1.prompt()).expect("valid test fixture"),
    ];
    value.runtime_events = vec![CanonicalRuntimeEvent::NativePublicationRequired {
        source_invocation_id: InvocationId::generate(),
    }];
    value.continuation_ref = None;
    value.response_schema = None;
    value
}

#[test]
fn native_publication_stimulus_preserves_original_provider_replay_requirements() {
    for format in [
        ApiFormat::OpenAiChatCompletions,
        ApiFormat::OpenAiResponses,
        ApiFormat::AnthropicMessages,
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
        ApiFormat::BedrockConverse,
        ApiFormat::BedrockInvokeModel,
    ] {
        let request = native_request(format);
        request.validate().expect("valid test fixture");
        let before = request.input.clone();
        let body = map_request(&request, &ResolvedContents::default())
            .expect("valid test fixture")
            .body
            .to_string();
        assert!(
            body.contains("Runtime event: public delivery required"),
            "{format:?}"
        );
        assert!(body.contains("private completed response"), "{format:?}");
        assert!(
            body.contains("dialogue publish tool now with purpose `reply`"),
            "{format:?}"
        );
        assert!(!body.contains(&request.invocation.invocation_id.to_string()));
        assert_eq!(request.input, before);
    }
}

#[test]
fn native_publication_cannot_mix_recovery_repair_or_override_a_new_stimulus() {
    let request = native_request(ApiFormat::OpenAiResponses);
    let source = InvocationId::generate();
    for other in [
        CanonicalRuntimeEvent::ModelRecovery {
            source_invocation_id: source,
        },
        CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id: source,
        },
        request.runtime_events[0].clone(),
    ] {
        let mut mixed = request.clone();
        mixed.runtime_events.push(other);
        assert_eq!(
            mixed.validate().expect_err("invalid test request"),
            CanonicalModelError::InvalidNativePublicationRequired
        );
    }
    let mut newer = request.clone();
    newer
        .input
        .push(message(CanonicalRole::User, "new actual task"));
    assert_eq!(
        newer.validate().expect_err("invalid test request"),
        CanonicalModelError::InvalidNativePublicationRequired
    );
    let mut self_source = request.clone();
    self_source.runtime_events = vec![CanonicalRuntimeEvent::NativePublicationRequired {
        source_invocation_id: request.invocation.invocation_id,
    }];
    assert_eq!(
        self_source.validate().expect_err("invalid test request"),
        CanonicalModelError::InvalidNativePublicationRequired
    );
    let mut ordinary = request.clone();
    ordinary.instructions = vec![ModelText::new("ordinary Chat").expect("valid test fixture")];
    assert_eq!(
        ordinary.validate().expect_err("invalid test request"),
        CanonicalModelError::InvalidNativePublicationRequired
    );
    let mut continuation = request;
    continuation.route.provider_state_policy = ProviderStatePolicy::OpaqueContinuation;
    continuation.continuation_ref = Some(ContinuationRef::from_opaque(
        OpaqueExternalRef::new("opaque-continuation").expect("valid test fixture"),
    ));
    assert_eq!(
        continuation.validate().expect_err("invalid test request"),
        CanonicalModelError::InvalidNativePublicationRequired
    );
}
