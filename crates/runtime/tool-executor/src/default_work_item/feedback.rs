use super::*;
use musteragent_identity::ContentId;
use musteragent_runtime_session::{
    CommitWorkItemFeedback, TASK_FEEDBACK_RECEIPT, WorkItemFeedback,
};

impl DefaultWorkItemTools {
    pub(super) async fn feedback(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let changes = match WorkItemFeedback::parse(&bytes) {
            Ok(changes) => changes,
            Err(error) => return failure(error),
        };
        let store = |label: &'static [u8], media: &'static str, bytes: Vec<u8>| {
            let id = ContentId::from_uuid(uuid::Uuid::new_v5(
                &invocation.invocation_id.into_uuid(),
                label,
            ))
            .expect("derived id");
            self.content.put_reclaimable_at(
                id,
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new(media).expect("static media type"),
                    bytes,
                },
            )
        };
        let partner_message = store(
            b"task-feedback-partner@1",
            "text/plain",
            changes.feedback.into_bytes(),
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        let result_ref = store(
            b"task-feedback-receipt@1",
            "application/json",
            TASK_FEEDBACK_RECEIPT.to_vec(),
        )
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref: result_ref.clone(),
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        };
        stopping(activity)?;
        match self
            .start
            .commit_feedback(
                attempt,
                CommitWorkItemFeedback {
                    invocation: request,
                    request_bytes: bytes,
                    partner_message,
                    result: result_ref,
                    result_schema: invocation.result_schema,
                    at: self.clock.now(),
                },
            )
            .await
        {
            Ok(()) => Ok(WorkItemToolResult::Committed(result)),
            Err(e) => failure(e),
        }
    }
}
