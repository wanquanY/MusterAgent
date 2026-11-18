//! The provider observes only the exact sealed invocation or its retained query.
use super::*;
impl<C: FencedArtifactToolCommitter + 'static> MediaArtifactCapabilityHandler<C> {
    pub(super) fn authorize_invoke(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        i: &CapabilityInvocation,
    ) -> Result<(), LoopPortError> {
        if !self.accepts_execution_target(&material.tool.execution_target)
            || i.actor != material.actor
            || i.actor.tenant_id != i.tenant_id
            || i.authorized_content != material.authorized_content
            || i.execution_grant_proof != material.execution_grant_proof
            || i.workspace_id != workspace_id(&material.run_context.execution_scope)
        {
            return Err(invalid());
        }
        crate::atomic_authorization::validate_sealed_invocation(
            self.manifest(),
            attempt,
            material,
            durable,
            i,
        )
    }
    pub(super) fn authorize_reconcile(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        r: &CapabilityReconciliation,
    ) -> Result<(), LoopPortError> {
        let i = &durable.invocation;
        let manifest = self.manifest();
        if !self.accepts_execution_target(&material.tool.execution_target)
            || material.run_context.run_id != attempt.run_id
            || i.run_id != attempt.run_id
            || manifest.provider_id != i.capability_provider_id
            || manifest.provider_id != material.capability_grant.provider_id
            || manifest.key != material.capability_grant.key
            || i.tool_key != material.tool.manifest.key
            || r.actor != material.actor
            || r.actor.tenant_id != r.tenant_id
            || r.tenant_id != material.run_context.tenant_id
            || r.conversation_id != material.run_context.conversation_id
            || r.run_id != attempt.run_id
            || r.execution_attempt_id != attempt.attempt_id
            || r.execution_attempt_number != attempt.attempt_number
            || r.invocation_id != i.invocation_id
            || r.provider_id != i.capability_provider_id
            || r.idempotency_key != i.idempotency_key
            || r.result_schema != material.tool.manifest.output_schema
            || r.workspace_id != workspace_id(&material.run_context.execution_scope)
            || r.execution_grant_proof != material.execution_grant_proof
            || r.reconciliation_ref != self.provider.reconciliation_identity(i.invocation_id)
            || durable.prebound_reconciliation_ref.as_ref() != Some(&r.reconciliation_ref)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::RuntimeSnapshotInvalid,
        retryable: false,
    }
}
