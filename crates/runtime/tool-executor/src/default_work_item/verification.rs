use super::*;
use musteragent_runtime_capability::require_work_item_evidence as require_evidence_bytes;
use musteragent_runtime_session::*;
impl DefaultWorkItemTools {
    pub(super) async fn verify(
        &self,
        attempt: &AttemptContext,
        request: ResolveWorkItemStart,
        invocation: CapabilityInvocation,
        bytes: Vec<u8>,
        activity: Option<&OperationContext>,
    ) -> Result<WorkItemToolResult, DialogueStoreError> {
        let input = match WorkItemVerificationInput::parse(&bytes) {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        let preparation = PrepareWorkItemVerification {
            invocation: request,
            input,
            request_bytes: bytes,
        };
        let material = match self
            .start
            .prepare_verification(attempt, preparation.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        let goal_bytes = match self
            .verified(invocation.tenant_id, &material.task.goal_content_ref)
            .await
        {
            Ok(bytes) => bytes,
            Err(_) => return failure(DialogueStoreError::Unavailable),
        };
        let mut command_bytes = Vec::new();
        for reference in &material.command_requests {
            match self.verified(invocation.tenant_id, reference).await {
                Ok(bytes) => command_bytes.push(bytes),
                Err(_) => return failure(DialogueStoreError::Unavailable),
            }
        }
        // An evidence ID is not evidence availability. The original owner has proved
        // provenance; Content owner now proves exact readable bytes before commit.
        if self
            .verified(invocation.tenant_id, &material.report_ref)
            .await
            .is_err()
        {
            return failure(DialogueStoreError::Unavailable);
        }
        for reference in &material.evidence {
            if require_evidence_bytes(self.content.as_ref(), invocation.tenant_id, reference)
                .await
                .is_err()
            {
                return failure(DialogueStoreError::Unavailable);
            }
        }
        let decision = match validate_work_item_verification_content(
            &preparation.input,
            &material,
            &goal_bytes,
            &command_bytes,
        ) {
            Ok(v) => v,
            Err(e) => return failure(e),
        };
        // Pending historical reviews are retired by their original scheduler
        // owner. No new invocation may create a result-confirmation wait.
        if material.human_acceptance == WorkItemHumanAcceptance::Pending {
            return failure(DialogueStoreError::RevisionConflict);
        }
        let human_prompt_bytes = if material.human_acceptance == WorkItemHumanAcceptance::Approved {
            let Some(reference) = material.human_prompt_ref.as_ref() else {
                return failure(DialogueStoreError::Unavailable);
            };
            match self.verified(invocation.tenant_id, reference).await {
                Ok(bytes) => Some(bytes),
                Err(_) => return failure(DialogueStoreError::Unavailable),
            }
        } else {
            None
        };
        if let Err(error) = validate_work_item_acceptance_prompt(
            &preparation.input,
            &material,
            &goal_bytes,
            human_prompt_bytes.as_deref(),
        ) {
            return failure(error);
        }
        let result_bytes = match work_item_verification_result_bytes(
            &preparation.input,
            decision,
            material.human_acceptance,
            material.direct_result_contract,
        ) {
            Ok(bytes) => bytes,
            Err(error) => return failure(error),
        };
        let result_ref = self
            .content
            .put_reclaimable_at(
                work_item_verification_result_id(invocation.invocation_id),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| DialogueStoreError::Corrupt)?,
                    bytes: result_bytes,
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
            .commit_verification(
                attempt,
                CommitWorkItemVerification {
                    preparation,
                    material,
                    goal_bytes,
                    command_bytes,
                    human_prompt_bytes,
                    decision,
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
    pub(super) async fn verified(
        &self,
        tenant: musteragent_identity::TenantId,
        reference: &musteragent_content_ref::ContentRef,
    ) -> Result<Vec<u8>, DialogueStoreError> {
        if reference.byte_length > 16 * 1024 * 1024 {
            return Err(DialogueStoreError::Unavailable);
        }
        let bytes = self
            .content
            .read(tenant, reference)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        verify_content(reference, &bytes).map_err(|_| DialogueStoreError::Corrupt)?;
        Ok(bytes)
    }
}
