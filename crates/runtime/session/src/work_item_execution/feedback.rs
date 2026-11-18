//! The report Run identifies the task and executor. Only the requested change is model input.
use super::*;
use musteragent_content_ref::{ContentRef, SchemaRef};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemFeedback {
    pub feedback: String,
}
impl WorkItemFeedback {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        let value: Self = serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        if value.feedback.trim().is_empty() || value.feedback.chars().count() > 8192 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(value)
    }
}
pub const TASK_FEEDBACK_RECEIPT: &[u8] = br#"{"status":"feedback_sent"}"#;
#[derive(Debug, Clone)]
pub struct CommitWorkItemFeedback {
    pub invocation: ResolveWorkItemStart,
    pub request_bytes: Vec<u8>,
    pub partner_message: ContentRef,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
