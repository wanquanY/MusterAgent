//! Shared canonical subvalues used by the Context RPC envelope.
use crate::wire;
use musteragent_runtime_contract::OpaqueExternalRef;
#[derive(Debug)]
pub(crate) enum ContextSupportError {
    Invalid,
}
type Result<T> = std::result::Result<T, ContextSupportError>;
fn checked<T, E>(v: std::result::Result<T, E>) -> Result<T> {
    v.map_err(|_| ContextSupportError::Invalid)
}
fn required<T>(v: Option<T>) -> Result<T> {
    v.ok_or(ContextSupportError::Invalid)
}
pub(crate) mod messages;
pub(crate) mod model_data;
pub(crate) mod recovery;

#[cfg(test)]
pub(crate) fn rejection() -> musteragent_execution_context::PublicDeliveryRejectionV1 {
    use musteragent_content_ref::*;
    use musteragent_execution_context::*;
    use musteragent_identity::*;

    let bytes = b"Original useful answer";
    PublicDeliveryRejectionV1 {
        candidate: PublicDeliveryCandidateProof {
            invocation_id: InvocationId::generate(),
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            ledger_cutoff: ConversationSeq::new(8).expect("valid test fixture"),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").expect("valid test fixture"),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(bytes),
            },
        },
        diagnostics: vec![
            PublicDeliveryDiagnostic::new(PublicDeliveryDiagnosticKind::MalformedJson, "/")
                .expect("valid test fixture"),
        ],
        disposition: PublicDeliveryRepairDisposition::FormatOnly,
        cut: PublicDeliveryRepairCut {
            progress_sequence: Some(ConversationSeq::new(2).expect("valid test fixture")),
            control_sequence: Some(ConversationSeq::new(4).expect("valid test fixture")),
        },
    }
}

use musteragent_runtime_model::ContinuationRef;
mod json_budget;
fn continuation(value: String) -> Result<ContinuationRef> {
    Ok(ContinuationRef::from_opaque(checked(
        OpaqueExternalRef::new(value),
    )?))
}
fn json(value: &musteragent_runtime_model::JsonValue) -> Result<wire::AgentJournalJson> {
    checked(value.validate())?;
    let canonical_json = checked(serde_json::to_vec(&crate::context_codec::json_to_serde(
        value,
    )))?;
    json_budget::check(&canonical_json)?;
    Ok(wire::AgentJournalJson { canonical_json })
}
fn decode_json(
    value: Option<wire::AgentJournalJson>,
) -> Result<musteragent_runtime_model::JsonValue> {
    let bytes = required(value)?.canonical_json;
    json_budget::check(&bytes)?;
    let raw = checked(serde_json::from_slice(&bytes))?;
    let value = checked(crate::context_codec::json_from_serde(raw))?;
    if json(&value)?.canonical_json != bytes {
        return Err(ContextSupportError::Invalid);
    }
    Ok(value)
}
