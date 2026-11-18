//! Private Runtime observations use the same native media adapters as input.
use super::*;
use musteragent_runtime_model::CanonicalRuntimeEvent;

pub(super) enum Wire {
    Responses,
    Anthropic,
    Google,
    Interactions,
    Bedrock,
}

pub(super) fn chat(
    event: &CanonicalRuntimeEvent,
    request: &CanonicalModelRequest,
    contents: &ResolvedContents,
) -> Result<Value, MappingError> {
    let _ = (request, contents);
    event
        .model_input_text()
        .map(Value::String)
        .map_err(|_| MappingError::InvalidCanonicalRequest)
}

pub(super) fn parts(
    event: &CanonicalRuntimeEvent,
    _request: &CanonicalModelRequest,
    _contents: &ResolvedContents,
    wire: Wire,
) -> Result<Vec<Value>, MappingError> {
    let text = event
        .model_input_text()
        .map_err(|_| MappingError::InvalidCanonicalRequest)?;
    Ok(vec![match wire {
        Wire::Responses => json!({"type":"input_text","text":text}),
        Wire::Anthropic | Wire::Interactions => json!({"type":"text","text":text}),
        Wire::Google | Wire::Bedrock => json!({"text":text}),
    }])
}
