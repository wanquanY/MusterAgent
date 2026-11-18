//! No user-facing status is emitted for a successful background handoff.
use super::*;
use musteragent_runtime_session::{
    CommitWorkItemHandoff, PrepareWorkItemHandoff, WorkItemHandoffInput,
    work_item_handoff_result_id,
};
impl DefaultWorkItemTools {
    pub(super) async fn handoff(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let input = match WorkItemHandoffInput::parse(&bytes) {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        let preparation = PrepareWorkItemHandoff {
            invocation: request,
            input,
            request_bytes: bytes,
        };
        let material = match self
            .start
            .prepare_handoff(attempt, preparation.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        stopping(activity)?;
        let result_ref = self
            .content
            .put_reclaimable_at(
                work_item_handoff_result_id(invocation.invocation_id),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: material.result_bytes(),
                },
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
            .commit_handoff(
                attempt,
                CommitWorkItemHandoff {
                    preparation,
                    material,
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
