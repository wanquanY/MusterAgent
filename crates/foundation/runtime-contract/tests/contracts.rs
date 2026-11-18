use musteragent_identity::{
    Actor, CommandId, CorrelationId, DottedKey, IdempotencyKey, PrincipalId, PrincipalKind,
    TenantId, UtcTimestamp,
};
use musteragent_runtime_contract::{
    COMMAND_SCHEMA_VERSION, CommandEnvelope, CommandEnvelopeError, CommandMetadata, CommandPayload,
    CommandType, DisplayText, ErrorCategory, ErrorCode, ErrorDetails, FrameContent, InlineFrame,
    InlineInput, LEGACY_COMMAND_SCHEMA_VERSION, LEGACY_RUNTIME_ERROR_SCHEMA_VERSION, OutputChannel,
    OutputFrame, OutputSequence, RUNTIME_ERROR_SCHEMA_VERSION, RuntimeError,
    RuntimeErrorContractError, SafeDisplayText, TraceContext, TraceParent,
};

#[derive(Debug, Clone, Eq, PartialEq)]
struct TestPayload;

impl CommandPayload for TestPayload {
    const COMMAND_TYPE: CommandType = CommandType::SubmitMessage;
}

#[test]
fn bounded_text_and_inline_input_reject_unsafe_values() {
    assert!(DisplayText::new("Musteragent").is_ok());
    assert!(DisplayText::new("line\nbreak").is_err());
    assert!(SafeDisplayText::new("safe message").is_ok());
    assert!(InlineInput::new("multi\nline\tinput").is_ok());
    assert!(InlineInput::new("nul\0byte").is_err());
}

#[test]
fn traceparent_is_w3c_shaped_and_non_zero() {
    assert!(TraceParent::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_ok());
    assert!(TraceParent::new("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_err());
}

#[test]
fn command_type_is_derived_from_payload_and_deadline_is_monotonic() {
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let trace_context = TraceContext {
        traceparent: TraceParent::new("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("valid traceparent"),
        tracestate: None,
    };
    let envelope = CommandEnvelope::new(
        CommandMetadata {
            schema_version: DottedKey::new(COMMAND_SCHEMA_VERSION).expect("valid schema"),
            command_id: CommandId::generate(),
            actor,
            idempotency_key: IdempotencyKey::new("submit:test").expect("valid key"),
            issued_at: UtcTimestamp::from_unix_millis(1),
            deadline: UtcTimestamp::from_unix_millis(2),
            correlation_id: CorrelationId::generate(),
            causation_id: None,
            conversation_guard: None,
            trace_context,
        },
        TestPayload,
    )
    .expect("valid envelope");

    assert_eq!(envelope.command_type, CommandType::SubmitMessage);
    assert!(envelope.validate().is_ok());

    let mut legacy = envelope.clone();
    legacy.metadata.schema_version =
        DottedKey::new(LEGACY_COMMAND_SCHEMA_VERSION).expect("legacy schema");
    assert!(legacy.validate().is_ok());

    let mut unsupported = envelope;
    unsupported.metadata.schema_version =
        DottedKey::new("unrelated.runtime.command.v1").expect("unsupported schema");
    assert_eq!(
        unsupported.validate(),
        Err(CommandEnvelopeError::UnsupportedSchemaVersion)
    );
}

#[test]
fn output_sequence_enforces_hash_offsets_indices_and_finality() {
    let output_id = musteragent_identity::OutputId::generate();
    let message_id = musteragent_identity::MessageId::generate();
    let content = InlineFrame::new("Musteragent").expect("bounded frame");
    let content_byte_length = content.byte_length();
    let first = OutputFrame {
        frame_id: musteragent_identity::FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel: OutputChannel::AssistantText,
        frame_index: 0,
        byte_offset_start: 0,
        byte_offset_end: content_byte_length,
        content_hash: musteragent_content_ref::Sha256::digest(content.as_str().as_bytes()),
        content: FrameContent::InlineUtf8(content),
        final_for_output: true,
    };
    let mut sequence = OutputSequence::new(output_id);
    sequence.accept(&first).expect("first frame is valid");
    assert!(sequence.is_sealed());
    assert!(sequence.accept(&first).is_err());

    let terminal_reseal = OutputFrame {
        frame_id: musteragent_identity::FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel: OutputChannel::AssistantText,
        frame_index: 1,
        byte_offset_start: content_byte_length,
        byte_offset_end: content_byte_length,
        content_hash: musteragent_content_ref::Sha256::digest(b""),
        content: FrameContent::InlineUtf8(InlineFrame::final_marker()),
        final_for_output: true,
    };
    sequence
        .accept(&terminal_reseal)
        .expect("contiguous historical terminal re-seal is content-idempotent");
    assert!(sequence.is_sealed());

    let non_empty_after_terminal = OutputFrame {
        frame_id: musteragent_identity::FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel: OutputChannel::AssistantText,
        frame_index: 2,
        byte_offset_start: content_byte_length,
        byte_offset_end: content_byte_length + 1,
        content_hash: musteragent_content_ref::Sha256::digest(b"!"),
        content: FrameContent::InlineUtf8(InlineFrame::new("!").expect("frame")),
        final_for_output: true,
    };
    assert!(sequence.accept(&non_empty_after_terminal).is_err());
}

#[test]
fn runtime_error_rejects_code_category_and_retry_contract_drift() {
    let error = RuntimeError {
        schema_version: DottedKey::new(RUNTIME_ERROR_SCHEMA_VERSION).expect("schema"),
        code: ErrorCode::DependencyTimeout,
        category: ErrorCategory::Internal,
        message: SafeDisplayText::new("dependency timed out").expect("safe message"),
        retryable: false,
        retry_after_ms: Some(100),
        resource_ref: None,
        correlation_id: CorrelationId::generate(),
        details: Box::new(ErrorDetails::None),
    };
    assert!(error.validate().is_err());

    let mut legacy = error.clone();
    legacy.schema_version =
        DottedKey::new(LEGACY_RUNTIME_ERROR_SCHEMA_VERSION).expect("legacy schema");
    legacy.category = ErrorCategory::Timeout;
    legacy.retryable = true;
    assert!(legacy.validate().is_ok());

    legacy.schema_version =
        DottedKey::new("unrelated.runtime.error.v1").expect("unsupported schema");
    assert_eq!(
        legacy.validate(),
        Err(RuntimeErrorContractError::UnsupportedSchemaVersion)
    );
}
