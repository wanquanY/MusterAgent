//! Immutable private Task result identities and Tool contracts. The submission
//! receipt acknowledges private material. Only the accepted verification owner
//! can produce a public delivery.
use super::*;
use crate::{DialogueToolPublicationRequest, decode_dialogue_tool_publication};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use serde::Deserialize;

mod manifest;
pub use manifest::*;

pub const WORK_ITEM_RESULT_SUBMIT_TOOL: &str = "tasks.submit_result@1";
pub const WORK_ITEM_RESULT_VERIFY_TOOL: &str = "tasks.verify@3";
pub const WORK_ITEM_RESULT_MANIFEST_MEDIA: &str =
    "application/vnd.musteragent.task-result-manifest+json";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultSubmissionInput {
    pub task_id: WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
    pub delivery: DialogueToolPublicationRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitArguments {
    task_id: uuid::Uuid,
    goal_revision: u64,
    control_revision: u64,
    assignment_epoch: u64,
    delivery: DeliveryArguments,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DeliveryArguments {
    Message {
        text: String,
    },
    Artifacts {
        artifacts: Vec<serde_json::Value>,
        caption: Option<String>,
    },
    Files {
        files: Vec<serde_json::Value>,
        caption: Option<String>,
    },
}

impl WorkItemResultSubmissionInput {
    pub fn parse(bytes: &[u8]) -> Result<Self, DialogueStoreError> {
        use musteragent_execution_context::{
            ARTIFACTS_DELIVER_TOOL, DIALOGUE_PUBLISH_TOOL, MAX_PUBLICATION_TOOL_BYTES,
        };
        if bytes.len() > MAX_PUBLICATION_TOOL_BYTES {
            return Err(DialogueStoreError::Corrupt);
        }
        let a: SubmitArguments =
            serde_json::from_slice(bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        if [a.goal_revision, a.control_revision, a.assignment_epoch]
            .iter()
            .any(|v| *v == 0 || *v > i64::MAX as u64)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let (key, body) = match a.delivery {
            DeliveryArguments::Message { text } => (
                DIALOGUE_PUBLISH_TOOL,
                serde_json::json!({"text":text,"purpose":"reply"}),
            ),
            DeliveryArguments::Artifacts { artifacts, caption } => (
                ARTIFACTS_DELIVER_TOOL,
                serde_json::json!({"artifacts":artifacts,"caption":caption,"purpose":"reply"}),
            ),
            DeliveryArguments::Files { files, caption } => (
                crate::ARTIFACTS_DELIVER_V2_TOOL,
                serde_json::json!({"files":files,"caption":caption,"purpose":"reply"}),
            ),
        };
        let body = serde_json::to_vec(&body).map_err(|_| DialogueStoreError::Corrupt)?;
        Ok(Self {
            task_id: WorkItemId::from_uuid(a.task_id).map_err(|_| DialogueStoreError::Corrupt)?,
            goal_revision: a.goal_revision,
            control_revision: a.control_revision,
            assignment_epoch: a.assignment_epoch,
            delivery: decode_dialogue_tool_publication(key, &body)
                .map_err(|_| DialogueStoreError::Corrupt)?,
        })
    }
    pub fn validate_message(
        &self,
        invocation: InvocationId,
        message: Option<&ContentRef>,
    ) -> Result<(), DialogueStoreError> {
        match (self.delivery.publication.text.as_deref(), message) {
            (None, None) => Ok(()),
            (Some(text), Some(r))
                if r.content_id == work_item_result_message_id(invocation)
                    && r.media_type.as_str() == "text/markdown;charset=utf-8"
                    && r.byte_length == text.len() as u64
                    && r.sha256 == Sha256::digest(text.as_bytes()) =>
            {
                Ok(())
            }
            _ => Err(DialogueStoreError::Corrupt),
        }
    }
}

pub fn work_item_result_message_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-result-message@1",
    ))
    .expect("derived id")
}
pub fn work_item_result_manifest_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-result-manifest@1",
    ))
    .expect("derived id")
}
pub fn work_item_result_submit_receipt_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-result-submitted@1",
    ))
    .expect("derived id")
}
pub fn work_item_result_verify_receipt_id(invocation: InvocationId) -> ContentId {
    ContentId::from_uuid(uuid::Uuid::new_v5(
        &invocation.into_uuid(),
        b"work-item-result-verified@3",
    ))
    .expect("derived id")
}

#[derive(Debug, Clone)]
pub struct PrepareWorkItemResultSubmission {
    pub invocation: ResolveWorkItemStart,
    pub request_bytes: Vec<u8>,
    pub message: Option<ContentRef>,
}

impl PrepareWorkItemResultSubmission {
    pub fn validate(&self) -> Result<WorkItemResultSubmissionInput, DialogueStoreError> {
        verify_bytes(&self.invocation.request_ref, &self.request_bytes)?;
        let input = WorkItemResultSubmissionInput::parse(&self.request_bytes)?;
        input.validate_message(self.invocation.invocation_id, self.message.as_ref())?;
        Ok(input)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemResultSubmissionMaterial {
    pub manifest: WorkItemResultManifest,
}
#[derive(Debug, Clone)]
pub struct CommitWorkItemResultSubmission {
    pub preparation: PrepareWorkItemResultSubmission,
    pub material: WorkItemResultSubmissionMaterial,
    pub manifest_ref: ContentRef,
    pub result: ContentRef,
    pub result_schema: SchemaRef,
    pub at: UtcTimestamp,
}
impl CommitWorkItemResultSubmission {
    pub fn validate(&self) -> Result<WorkItemResultSubmissionReceipt, DialogueStoreError> {
        let args = self.preparation.validate()?;
        let m = &self.material.manifest;
        let i = &self.preparation.invocation;
        if m.submission_invocation_id != i.invocation_id
            || m.scope.work_item_id != args.task_id
            || m.scope.producing_run_id != i.run_context.run_id
            || m.request_sha256 != i.request_ref.sha256
            || m.scope.goal_revision != args.goal_revision
            || m.scope.control_revision != args.control_revision
            || m.scope.assignment_epoch != args.assignment_epoch
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let receipt = m.receipt()?;
        verify_ref(
            &self.manifest_ref,
            work_item_result_manifest_id(i.invocation_id),
            WORK_ITEM_RESULT_MANIFEST_MEDIA,
            &m.canonical_bytes()?,
        )?;
        verify_ref(
            &self.result,
            work_item_result_submit_receipt_id(i.invocation_id),
            "application/json",
            &receipt.canonical_bytes(),
        )?;
        Ok(receipt)
    }
}

fn verify_bytes(reference: &ContentRef, bytes: &[u8]) -> Result<(), DialogueStoreError> {
    if reference.byte_length == bytes.len() as u64 && reference.sha256 == Sha256::digest(bytes) {
        Ok(())
    } else {
        Err(DialogueStoreError::Corrupt)
    }
}
fn verify_ref(
    reference: &ContentRef,
    id: ContentId,
    media: &str,
    bytes: &[u8],
) -> Result<(), DialogueStoreError> {
    if reference.content_id != id || reference.media_type.as_str() != media {
        return Err(DialogueStoreError::Corrupt);
    }
    verify_bytes(reference, bytes)
}

#[cfg(test)]
mod tests;

mod verification;
pub use verification::*;
