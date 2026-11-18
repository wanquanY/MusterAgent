use super::*;
use musteragent_runtime_session::{
    CommitWorkItemFollowup, PrepareWorkItemFollowup, work_item_followup_result_id,
};
impl DefaultWorkItemTools {
    pub(super) async fn followup(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let preparation = PrepareWorkItemFollowup {
            invocation: request,
            request_bytes: bytes,
        };
        let material = match self
            .start
            .prepare_followup(attempt, preparation.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        stopping(activity)?;
        let goal = self
            .content
            .put_reclaimable_at(
                work_item_goal_content_id(material.task_id(), material.action().command_id),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new(WORK_ITEM_GOAL_MEDIA_TYPE)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: work_item_goal_bytes(&material.input.spec),
                },
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let partner_input =
            if material.input.executing_agent_id != Some(material.source.scope.agent_id) {
                Some(
                    self.content
                        .put_reclaimable_at(
                            work_item_partner_input_id(invocation.invocation_id),
                            PutContent {
                                tenant_id: invocation.tenant_id,
                                media_type: MediaType::new("text/plain")
                                    .map_err(|_| DialogueStoreError::Corrupt)?,
                                bytes: work_item_partner_input_bytes(&material.input.spec),
                            },
                        )
                        .await
                        .map_err(|_| DialogueStoreError::Infrastructure)?,
                )
            } else {
                None
            };
        let result_ref = self
            .content
            .put_reclaimable_at(
                work_item_followup_result_id(invocation.invocation_id),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: material.result_bytes(&invocation.tool_call_id),
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
            .commit_followup(
                attempt,
                CommitWorkItemFollowup {
                    preparation,
                    material,
                    goal,
                    result: result_ref,
                    partner_input,
                    result_schema: invocation.result_schema,
                    at: self.clock.now(),
                },
            )
            .await
        {
            Ok(_) => Ok(WorkItemToolResult::Committed(result)),
            Err(e) => failure(e),
        }
    }
}
