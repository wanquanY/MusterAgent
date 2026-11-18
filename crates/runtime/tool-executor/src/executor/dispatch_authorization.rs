//! Independent authorization at a durable dispatch boundary. The original
//! Snapshot and journal facts are supplied by the persistence owner.
use super::*;
use musteragent_runtime_capability::AuthorizationReceiptQuery;

pub struct ToolDispatchAuthorizer {
    schemas: crate::schema_validation::ToolSchemaValidatorCache,
}
impl Default for ToolDispatchAuthorizer {
    fn default() -> Self {
        Self {
            schemas: crate::schema_validation::ToolSchemaValidatorCache::new(),
        }
    }
}
impl ToolDispatchAuthorizer {
    pub fn receipt_query(
        &self,
        material: &ToolExecutionMaterial,
        arguments: &serde_json::Value,
        observed_at: musteragent_identity::UtcTimestamp,
    ) -> Result<AuthorizationReceiptQuery, LoopPortError> {
        // Runtime progress narration was admitted at preparation only for its
        // reserved call id; its body is the strict decoder's short message.
        if !super::runtime_progress_body(material, arguments) {
            self.schemas
                .validate(
                    material.tool.manifest.input_schema.schema_hash,
                    &material.tool.input_schema_json,
                    arguments,
                )
                .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
        }
        let input = InvocationPolicyInput {
            workspace_id: workspace_id(&material.run_context.execution_scope),
            tool: &material.tool.manifest,
            arguments,
            policy_basis: material.policy_basis.as_ref(),
            authorized_content: &material.authorized_content,
        };
        Ok(authorization_receipt_query(
            material,
            session_approval_fingerprint(&input),
            observed_at,
        ))
    }
    pub fn session_reusable(
        &self,
        material: &ToolExecutionMaterial,
        arguments: &serde_json::Value,
    ) -> bool {
        let input = InvocationPolicyInput {
            workspace_id: workspace_id(&material.run_context.execution_scope),
            tool: &material.tool.manifest,
            arguments,
            policy_basis: material.policy_basis.as_ref(),
            authorized_content: &material.authorized_content,
        };
        session_authorization_is_reusable(
            material.capability_grant.policy_decision,
            material.tool.manifest.approval,
            crate::RuntimeInvocationPolicyEvaluator.evaluate(&input),
        )
    }
    pub fn check_dispatch(
        &self,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        arguments: &serde_json::Value,
        receipt: Option<&musteragent_runtime_capability::AuthorizationReceipt>,
        observed_at: musteragent_identity::UtcTimestamp,
    ) -> Result<(), LoopPortError> {
        self.effective_authorization(material, durable, arguments, receipt, observed_at)
            .map(|_| ())
    }

    /// Reused by atomic owners so the final structural fence applies the same
    /// argument-level decision as dispatch, including signed approval floors.
    pub(crate) fn effective_authorization(
        &self,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        arguments: &serde_json::Value,
        receipt: Option<&musteragent_runtime_capability::AuthorizationReceipt>,
        observed_at: musteragent_identity::UtcTimestamp,
    ) -> Result<InvocationAuthorization, LoopPortError> {
        let query = self.receipt_query(material, arguments, observed_at)?;
        let input = InvocationPolicyInput {
            workspace_id: workspace_id(&material.run_context.execution_scope),
            tool: &material.tool.manifest,
            arguments,
            policy_basis: material.policy_basis.as_ref(),
            authorized_content: &material.authorized_content,
        };
        let disposition = crate::RuntimeInvocationPolicyEvaluator.evaluate(&input);
        if crate::terminal_host_access_requested(material.tool.manifest.key.as_str(), arguments)
            && !crate::terminal_host_access_approved(durable)
        {
            return Err(port(ErrorCode::PolicyDenied, false));
        }
        let mut authorization = material.authorization.clone();
        authorization.approval_satisfied = matches!(
            durable.interaction_resolution,
            Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::Approve
                    | musteragent_approval::ApprovalDecision::ApproveSession
            ))
        );
        if durable.invocation.interaction_id.is_some() && !authorization.approval_satisfied {
            return Err(port(ErrorCode::PolicyDenied, false));
        }
        if durable.interaction_resolution.is_none()
            && self.session_reusable(material, arguments)
            && receipt.is_some_and(|v| {
                receipt_matches_query(v, &query)
                    && v.issued_at <= observed_at
                    && v.expires_at > observed_at
            })
        {
            authorization.approval_satisfied = true;
        }
        match disposition {
            InvocationPolicyDisposition::Deny(_) => {
                return Err(port(ErrorCode::PolicyDenied, false));
            }
            InvocationPolicyDisposition::Allow => {
                authorization.policy_requires_approval = durable.invocation.interaction_id.is_some()
            }
            InvocationPolicyDisposition::RequireApproval(_) => {
                authorization.policy_requires_approval = true
            }
            InvocationPolicyDisposition::Inherit
            | InvocationPolicyDisposition::RequirePolicyApproval(_) => {}
        }
        if authorize_invocation(
            &material.tool.manifest,
            &material.capability_grant,
            &material.tool.execution_target,
            &authorization,
        ) == Decision::RequireApproval
            && durable.interaction_resolution.is_none()
            && review_approval(ApprovalAutoReviewInput {
                permission_mode: material.permission_mode,
                effect: material.tool.manifest.effect,
                approval_requirement: material.tool.manifest.approval,
                capability_decision: material.capability_grant.policy_decision,
                invocation_disposition: disposition,
            }) == ApprovalAutoReviewDecision::Approve
        {
            authorization.approval_satisfied = true;
        }
        if authorize_invocation(
            &material.tool.manifest,
            &material.capability_grant,
            &material.tool.execution_target,
            &authorization,
        ) != Decision::Allow
        {
            return Err(port(ErrorCode::PolicyDenied, false));
        }
        Ok(authorization)
    }
}
