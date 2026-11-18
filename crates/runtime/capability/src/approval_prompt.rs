//! Shared immutable Tool approval evidence, independent of Memory and Ledger owners.
mod capability_binding;
pub use capability_binding::{
    ApprovalCapabilityBindingError, VerifiedApprovalCapability, verify_approval_capability,
};

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct ApprovalPromptEnvelope<'a> {
    pub schema: &'static str,
    pub invocation_id: String,
    pub consequence_fingerprint: String,
    pub session_reusable: bool,
    pub current_task_records: Vec<ApprovalPromptTaskProvenanceRecord>,
    pub invocation_policy_ref: &'static str,
    pub invocation_policy_hash: String,
    pub agent_policy_ref: Option<&'a str>,
    pub agent_policy_hash: Option<String>,
    pub permission_mode: Option<&'a str>,
    pub title: &'a str,
    pub description: &'a str,
    pub tool: &'a str,
    pub effect: String,
    pub arguments: serde_json::Value,
    pub request: ApprovalPromptRequestIdentity<'a>,
    pub reason: &'a str,
    pub capability_detail: Option<ApprovalPromptCapabilityDetail>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct ApprovalPromptTaskProvenanceRecord {
    pub record_id: String,
    pub source_kind: &'static str,
    pub content_id: String,
    pub content_sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct ApprovalPromptRequestIdentity<'a> {
    pub content_id: String,
    pub media_type: &'a str,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize)]
pub struct ApprovalPromptCapabilityDetail {
    pub media_type: String,
    pub sha256: String,
    pub byte_length: u64,
    pub content_json: Option<serde_json::Value>,
    pub content_text: Option<String>,
}
