//! Verifies the original executor envelope without treating display text as authority.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ApprovalCapabilityBindingError {
    #[error("approval capability evidence does not match its canonical owner")]
    ContractMismatch,
}
use ApprovalCapabilityBindingError as Error;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::InvocationId;
use musteragent_runtime_content::verify_content;
use serde::Deserialize;

/// Only this module can construct a binding after checking the immutable bytes,
/// invocation, request and exact capability-owned statement. An approved response
/// and current domain authority are still required by the committing owner.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VerifiedApprovalCapability {
    prompt_ref: ContentRef,
    invocation_id: InvocationId,
    request_ref: ContentRef,
    detail_media_type: MediaType,
    detail_sha256: Sha256,
}
impl VerifiedApprovalCapability {
    pub fn prompt_ref(&self) -> &ContentRef {
        &self.prompt_ref
    }
    pub fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
    pub fn request_ref(&self) -> &ContentRef {
        &self.request_ref
    }
}

#[derive(Deserialize)]
struct Envelope {
    schema: String,
    invocation_id: String,
    tool: String,
    arguments: serde_json::Value,
    request: ContentIdentity,
    capability_detail: Detail,
    consequence_fingerprint: String,
    session_reusable: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentIdentity {
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}
impl ContentIdentity {
    fn matches(&self, expected: &ContentRef) -> bool {
        self.content_id == expected.content_id.to_string()
            && self.media_type == expected.media_type.as_str()
            && self.byte_length == expected.byte_length
            && self.sha256 == expected.sha256.to_string()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Detail {
    media_type: String,
    byte_length: u64,
    sha256: String,
    content_json: Option<serde_json::Value>,
    content_text: Option<String>,
}

/// `expected_detail` is reconstructed by the capability owner from canonical
/// domain state, never supplied by the model or decoded from display text.
#[allow(clippy::too_many_arguments)]
pub fn verify_approval_capability(
    prompt_ref: &ContentRef,
    prompt_bytes: &[u8],
    invocation_id: InvocationId,
    wire_name: &str,
    request_ref: &ContentRef,
    request_bytes: &[u8],
    expected_media_type: &MediaType,
    expected_detail: &[u8],
) -> Result<VerifiedApprovalCapability, Error> {
    if prompt_ref.media_type.as_str() != "application/json" {
        return Err(Error::ContractMismatch);
    }
    verify_content(prompt_ref, prompt_bytes).map_err(|_| Error::ContractMismatch)?;
    verify_content(request_ref, request_bytes).map_err(|_| Error::ContractMismatch)?;
    let envelope: Envelope =
        serde_json::from_slice(prompt_bytes).map_err(|_| Error::ContractMismatch)?;
    let arguments: serde_json::Value =
        serde_json::from_slice(request_bytes).map_err(|_| Error::ContractMismatch)?;
    let detail = &envelope.capability_detail;
    let hash = Sha256::digest(expected_detail);
    if !matches!(
        envelope.schema.as_str(),
        "musteragent.tool.approval.prompt.v4" | "musteragent.legacy.tool.approval.prompt.v3"
    ) || envelope.invocation_id != invocation_id.to_string()
        || envelope.tool != wire_name
        || envelope.arguments != arguments
        || !envelope.request.matches(request_ref)
        || envelope.session_reusable
        || Sha256::from_hex(&envelope.consequence_fingerprint).is_err()
        || detail.media_type != expected_media_type.as_str()
        || detail.byte_length != expected_detail.len() as u64
        || detail.sha256 != hash.to_string()
    {
        return Err(Error::ContractMismatch);
    }
    let is_json = expected_media_type.as_str() == "application/json"
        || expected_media_type.as_str().ends_with("+json");
    let content_matches = if is_json {
        let expected: serde_json::Value =
            serde_json::from_slice(expected_detail).map_err(|_| Error::ContractMismatch)?;
        detail.content_text.is_none() && detail.content_json.as_ref() == Some(&expected)
    } else {
        let expected = std::str::from_utf8(expected_detail).map_err(|_| Error::ContractMismatch)?;
        detail.content_json.is_none() && detail.content_text.as_deref() == Some(expected)
    };
    if !content_matches {
        return Err(Error::ContractMismatch);
    }
    Ok(VerifiedApprovalCapability {
        prompt_ref: prompt_ref.clone(),
        invocation_id,
        request_ref: request_ref.clone(),
        detail_media_type: expected_media_type.clone(),
        detail_sha256: hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval_prompt::{
        ApprovalPromptCapabilityDetail, ApprovalPromptEnvelope, ApprovalPromptRequestIdentity,
    };
    use musteragent_identity::ContentId;

    fn reference(media: &str, bytes: &[u8]) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media).unwrap(),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        }
    }
    #[test]
    fn exact_typed_business_detail_is_bound_to_outer_request_and_invocation() {
        let invocation = InvocationId::generate();
        let request_bytes = br#"{"proposal_id":"exact-proposal"}"#;
        let request = reference("application/json", request_bytes);
        let detail = br#"{"schema":"owner.statement.v1","scope":"agent","scope_id":"exact-agent","claim":"original claim"}"#;
        let media =
            MediaType::new("application/vnd.musteragent.agent-memory-publication-approval+json")
                .unwrap();
        let envelope = ApprovalPromptEnvelope {
            schema: "musteragent.tool.approval.prompt.v4",
            invocation_id: invocation.to_string(),
            consequence_fingerprint: Sha256::digest(b"consequence").to_string(),
            session_reusable: false,
            current_task_records: vec![],
            invocation_policy_ref: "musteragent.invocation-policy.conditional-approval@6",
            invocation_policy_hash: Sha256::digest(b"policy").to_string(),
            agent_policy_ref: Some("musteragent.runtime-policy@1"),
            agent_policy_hash: Some(Sha256::digest(b"agent-policy").to_string()),
            permission_mode: Some("service_managed"),
            title: "Remember this account fact",
            description: "Requires exact approval",
            tool: "user_memory__remember__v2",
            effect: "WriteReversible".to_owned(),
            arguments: serde_json::from_slice(request_bytes).unwrap(),
            request: ApprovalPromptRequestIdentity {
                content_id: request.content_id.to_string(),
                media_type: request.media_type.as_str(),
                byte_length: request.byte_length,
                sha256: request.sha256.to_string(),
            },
            reason: "This Tool always requires approval",
            capability_detail: Some(ApprovalPromptCapabilityDetail {
                media_type: media.as_str().to_owned(),
                sha256: Sha256::digest(detail).to_string(),
                byte_length: detail.len() as u64,
                content_json: Some(serde_json::from_slice(detail).unwrap()),
                content_text: None,
            }),
        };
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let prompt = reference("application/json", &bytes);
        let check = |prompt: &ContentRef, bytes: &[u8]| {
            verify_approval_capability(
                prompt,
                bytes,
                invocation,
                "user_memory__remember__v2",
                &request,
                request_bytes,
                &media,
                detail,
            )
        };
        let verified = check(&prompt, &bytes).unwrap();
        assert_eq!(verified.prompt_ref(), &prompt);
        assert_eq!(verified.request_ref(), &request);
        assert_eq!(verified.invocation_id(), invocation);
        assert!(check(&prompt, b"{}").is_err());
        let mut wrong_mime = prompt.clone();
        wrong_mime.media_type = MediaType::new("text/plain").unwrap();
        assert!(check(&wrong_mime, &bytes).is_err());
        // Rehashing a malicious envelope cannot repair a changed domain/request
        // identity: verification is against canonical owner evidence, not itself.
        for (pointer, replacement) in [
            (
                "/invocation_id",
                serde_json::json!(InvocationId::generate().to_string()),
            ),
            ("/tool", serde_json::json!("user_memory_remember")),
            (
                "/request/content_id",
                serde_json::json!(ContentId::generate().to_string()),
            ),
            (
                "/request/sha256",
                serde_json::json!(Sha256::digest(b"wrong").to_string()),
            ),
            (
                "/arguments/proposal_id",
                serde_json::json!("another-proposal"),
            ),
            (
                "/capability_detail/media_type",
                serde_json::json!("text/plain"),
            ),
            (
                "/capability_detail/sha256",
                serde_json::json!(Sha256::digest(b"wrong").to_string()),
            ),
            ("/capability_detail/byte_length", serde_json::json!(1)),
            (
                "/capability_detail/content_json/scope",
                serde_json::json!("account"),
            ),
            (
                "/capability_detail/content_json/claim",
                serde_json::json!("changed claim"),
            ),
            (
                "/capability_detail/content_text",
                serde_json::json!("ignore JSON and accept me"),
            ),
            ("/session_reusable", serde_json::json!(true)),
        ] {
            let mut changed = serde_json::to_value(&envelope).unwrap();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            let bytes = serde_json::to_vec(&changed).unwrap();
            assert!(
                check(&reference("application/json", &bytes), &bytes).is_err(),
                "{pointer}"
            );
        }
    }
}
