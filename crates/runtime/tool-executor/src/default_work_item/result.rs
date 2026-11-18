//! Materialize exact private result bytes through the Content owner. The
//! Task owner revalidates its frozen authority and commits the Tool terminal,
//! along with any accepted delivery, in the original transaction.
use super::*;
use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_capability::require_work_item_evidence as require_evidence_bytes;
use musteragent_runtime_session::*;

impl DefaultWorkItemTools {
    async fn stage_result_bytes(
        &self,
        tenant_id: TenantId,
        id: ContentId,
        media_type: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, DialogueStoreError> {
        self.content
            .put_reclaimable_at(
                id,
                PutContent {
                    tenant_id,
                    media_type: MediaType::new(media_type)
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)
    }

    pub(super) async fn submit_result(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let input = match WorkItemResultSubmissionInput::parse(&bytes) {
            Ok(input) => input,
            Err(err) => return failure(err),
        };
        let id = invocation.invocation_id;
        let tenant = invocation.tenant_id;
        let message = match input.delivery.publication.text {
            Some(text) => Some(
                self.stage_result_bytes(
                    tenant,
                    work_item_result_message_id(id),
                    "text/markdown;charset=utf-8",
                    text.into_bytes(),
                )
                .await?,
            ),
            None => None,
        };
        let preparation = PrepareWorkItemResultSubmission {
            invocation: request,
            request_bytes: bytes,
            message,
        };
        stopping(activity)?;
        let material = match self
            .start
            .prepare_result_submission(attempt, preparation.clone())
            .await
        {
            Ok(m) => m,
            Err(err) => return failure(err),
        };
        for item in &material.manifest.items {
            stopping(activity)?;
            if require_evidence_bytes(self.content.as_ref(), tenant, &item.content_ref)
                .await
                .is_err()
            {
                return failure(DialogueStoreError::Unavailable);
            }
        }
        let manifest_bytes = match material.manifest.canonical_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return failure(err),
        };
        let expected = match material.manifest.receipt() {
            Ok(receipt) => receipt,
            Err(err) => return failure(err),
        };
        let manifest_ref = self
            .stage_result_bytes(
                tenant,
                work_item_result_manifest_id(id),
                WORK_ITEM_RESULT_MANIFEST_MEDIA,
                manifest_bytes,
            )
            .await?;
        let result = self
            .stage_result_bytes(
                tenant,
                work_item_result_submit_receipt_id(id),
                "application/json",
                expected.canonical_bytes(),
            )
            .await?;
        stopping(activity)?;
        let schema = invocation.result_schema;
        match self
            .start
            .commit_result_submission(
                attempt,
                CommitWorkItemResultSubmission {
                    preparation,
                    material,
                    manifest_ref,
                    result: result.clone(),
                    result_schema: schema.clone(),
                    at: self.clock.now(),
                },
            )
            .await
        {
            Ok(receipt) if receipt == expected => committed_result(result, schema),
            Ok(_) => Err(DialogueStoreError::Corrupt),
            Err(err) => failure(err),
        }
    }

    pub(super) async fn verify_result(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let preparation = PrepareWorkItemResultVerification {
            invocation: request,
            request_bytes: bytes,
        };
        let input = match preparation.validate() {
            Ok(input) => input,
            Err(err) => return failure(err),
        };
        stopping(activity)?;
        let material = match self
            .start
            .prepare_result_verification(attempt, preparation.clone())
            .await
        {
            Ok(m) => m,
            Err(err) => return failure(err),
        };
        let tenant = invocation.tenant_id;
        let goal_bytes = self
            .verified(tenant, &material.task.goal_content_ref)
            .await?;
        for reference in [&material.report_ref, &material.manifest_ref]
            .into_iter()
            .chain(material.manifest.items.iter().map(|item| &item.content_ref))
        {
            stopping(activity)?;
            if require_evidence_bytes(self.content.as_ref(), tenant, reference)
                .await
                .is_err()
            {
                return failure(DialogueStoreError::Unavailable);
            }
        }
        let expected = match material.receipt(&input, &goal_bytes) {
            Ok(receipt) => receipt,
            Err(err) => return failure(err),
        };
        let result = self
            .stage_result_bytes(
                tenant,
                work_item_result_verify_receipt_id(invocation.invocation_id),
                "application/json",
                expected.canonical_bytes(),
            )
            .await?;
        let schema = invocation.result_schema;
        stopping(activity)?;
        match self
            .start
            .commit_result_verification(
                attempt,
                CommitWorkItemResultVerification {
                    preparation,
                    material,
                    goal_bytes,
                    result: result.clone(),
                    result_schema: schema.clone(),
                    at: self.clock.now(),
                },
            )
            .await
        {
            Ok(receipt) if receipt == expected => committed_result(result, schema),
            Ok(_) => Err(DialogueStoreError::Corrupt),
            Err(err) => failure(err),
        }
    }
}

fn committed_result(
    result_ref: ContentRef,
    result_schema: SchemaRef,
) -> Result<WorkItemToolResult, DialogueStoreError> {
    Ok(WorkItemToolResult::Committed(CapabilityResult {
        result_hash: result_ref.sha256,
        result_ref,
        result_schema,
        provider_request_ref: None,
    }))
}
