//! One immutable Tool-approval prompt contract for its producer and response
//! validator. Display detail cannot replace Runtime-owned authority fields.
pub use musteragent_runtime_capability::approval_prompt::{
    ApprovalPromptCapabilityDetail, ApprovalPromptEnvelope, ApprovalPromptRequestIdentity,
    ApprovalPromptTaskProvenanceRecord,
};

use crate::InteractionResponseValidationError as Error;
use musteragent_approval::{Interaction, InteractionKind};
use musteragent_content_ref::Sha256;
use musteragent_runtime_content::verify_content;

pub const APPROVAL_RESPONSE_SCHEMA_KEY: &str = "interaction.approval.response@1";
pub const APPROVAL_RESPONSE_SCHEMA: &[u8] = br#"{"type":"object","properties":{"decision":{"enum":["approve","approve_session","deny"]}},"required":["decision"],"additionalProperties":false}"#;

/// The authorization view reads only owner fields from the verified immutable
/// envelope. Provider detail and presentation text never confer session reuse.
#[derive(serde::Deserialize)]
struct SessionApproval {
    schema: String,
    invocation_id: String,
    consequence_fingerprint: String,
    session_reusable: bool,
}

pub(crate) fn validate_session_approval(
    interaction: &Interaction,
    bytes: &[u8],
) -> Result<(), Error> {
    if interaction.kind != InteractionKind::Approval
        || interaction.response_schema.schema_key.as_str() != APPROVAL_RESPONSE_SCHEMA_KEY
        || interaction.response_schema.schema_hash != Sha256::digest(APPROVAL_RESPONSE_SCHEMA)
        || interaction.prompt_ref.media_type.as_str() != "application/json"
    {
        return Err(Error::ContractMismatch);
    }
    verify_content(&interaction.prompt_ref, bytes).map_err(|_| Error::ContractMismatch)?;
    let prompt: SessionApproval =
        serde_json::from_slice(bytes).map_err(|_| Error::ContractMismatch)?;
    if !matches!(
        prompt.schema.as_str(),
        "musteragent.tool.approval.prompt.v4" | "musteragent.legacy.tool.approval.prompt.v3"
    ) || prompt.invocation_id != interaction.requested_by_invocation_id.to_string()
        || Sha256::from_hex(&prompt.consequence_fingerprint).is_err()
    {
        return Err(Error::ContractMismatch);
    }
    if !prompt.session_reusable {
        return Err(Error::InvalidResponse);
    }
    Ok(())
}

#[cfg(test)]
#[path = "approval_prompt_tests.rs"]
mod tests;
