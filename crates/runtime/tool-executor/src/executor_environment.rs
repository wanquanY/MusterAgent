//! Environment discovery is a registered read. It does not dispatch work,
//! acquire capacity, renew a device, or put remote tools into a local catalog.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_argument_invocation,
    schema_validation::ToolSchemaValidatorCache,
};
use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::MediaType;
use musteragent_identity::PrincipalKind;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::*;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityResult, DurableToolInvocation,
    OperationContext, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};
use std::sync::Arc;

#[cfg(test)]
pub(crate) mod tests;

pub struct ExecutorEnvironmentCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    directory: Arc<dyn ExecutorEnvironmentDirectory>,
    clock: Arc<dyn RuntimeClock>,
}
impl ExecutorEnvironmentCapabilityHandler {
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        directory: Arc<dyn ExecutorEnvironmentDirectory>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            directory,
            clock,
        }
    }
    async fn read(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: Option<&OperationContext>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        if invocation.actor != material.actor
            || invocation.authorized_content != material.authorized_content
            || material.actor.principal_kind != PrincipalKind::User
            || !crate::atomic_authorization::dialogue_workspace_matches(
                material,
                invocation.workspace_id,
            )
            || material.run_context.parent_run_id.is_some()
            || material.run_context.delegation_id.is_some()
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        stopping(activity)?;
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let arguments =
            serde_json::from_slice(&bytes).map_err(|_| port(ErrorCode::InvalidCommand, false))?;
        validate_argument_invocation(
            &self.manifest,
            attempt,
            material,
            durable,
            &invocation,
            &arguments,
            self.clock.now(),
        )?;
        let (query, output_version) =
            parse_executor_environment_query(invocation.tool_key.as_str(), &bytes)
                .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
        let result = if matches!(
            &query,
            ExecutorEnvironmentQuery::Inspect {
                reference: ExecutorEnvironmentRef::Cloud {},
                ..
            }
        ) {
            let detail = matches!(&query, ExecutorEnvironmentQuery::Inspect { view, .. }
                if *view != ExecutorEnvironmentView::Summary);
            serde_json::json!({ "environment": cloud(material, detail) })
        } else {
            let page = self
                .directory
                .read(material.actor, material.agent_id, query.clone())
                .await
                .map_err(|e| match e {
                    DialogueStoreError::Infrastructure => {
                        port(ErrorCode::DependencyUnavailable, true)
                    }
                    DialogueStoreError::Corrupt => port(ErrorCode::RuntimeSnapshotInvalid, false),
                    _ => port(ErrorCode::CapabilityUnavailable, false),
                })?;
            stopping(activity)?;
            let mut records = page
                .environments
                .iter()
                .map(|v| {
                    v.projection(
                        page.observed_at_ms,
                        page.permission_revision,
                        output_version,
                    )
                })
                .collect::<Vec<_>>();
            match query {
                ExecutorEnvironmentQuery::List { after, .. } => {
                    if after.is_none() {
                        records.insert(0, cloud(material, false));
                    }
                    serde_json::json!({ "environments": records, "next_membership_id": page.next_membership_id,
                        "coverage": { "current_cloud_execution": true, "account_native_memberships": true, "web_local_execution": false },
                        "observed_at_ms": page.observed_at_ms, "permission_revision": page.permission_revision.to_string() })
                }
                ExecutorEnvironmentQuery::Inspect { view, .. } if records.len() == 1 => {
                    if view == ExecutorEnvironmentView::Summary {
                        records[0]["capabilities"] = serde_json::json!([]);
                        records[0]["capabilities_complete"] = serde_json::json!(false);
                    }
                    serde_json::json!({ "environment": records.remove(0) })
                }
                _ => return Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
            }
        };
        let bytes = serde_json::to_vec(&result)
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        if bytes.len() > 512 * 1024 {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        // The original Run selects the wire contract, including on retry after an upgrade.
        ToolSchemaValidatorCache::new()
            .validate_async(
                invocation.result_schema.schema_hash,
                &material.tool.output_schema_json,
                &result,
            )
            .await
            .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        stopping(activity)?;
        // A read may observe a newer state on retry. Content addressing and the
        // original Tool finalizer retain the one committed result, not a second
        // mutable directory or an invocation-named blob that cannot be updated.
        let reference = self
            .content
            .put(PutContent {
                tenant_id: invocation.tenant_id,
                media_type: MediaType::new("application/json")
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                bytes,
            })
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        stopping(activity)?;
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Succeeded(CapabilityResult {
                result_hash: reference.sha256,
                result_ref: reference,
                result_schema: invocation.result_schema,
                provider_request_ref: None,
            }),
        ))
    }
}
fn cloud(material: &ToolExecutionMaterial, detail: bool) -> serde_json::Value {
    let mut result = serde_json::json!({
        "environment_ref": ExecutorEnvironmentRef::Cloud {},
        "host_kind": "cloud", "source": "current_run_snapshot",
        "connection": { "state": "unknown" }, "capacity": { "state": "unknown" },
        "execution": { "dispatch_requires_fresh_validation": true },
        "capabilities_complete": false,
    });
    if detail {
        result["admitted_tools"] = serde_json::json!(
            material
                .authorized_tools
                .definitions()
                .iter()
                .map(|d| d.tool_ref.as_str())
                .collect::<Vec<_>>()
        );
    }
    result
}
fn stopping(activity: Option<&OperationContext>) -> Result<(), LoopPortError> {
    if activity.is_some_and(OperationContext::stopping) {
        Err(port(ErrorCode::RunTransitionInvalid, false))
    } else {
        Ok(())
    }
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}
impl AtomicCapabilityHandler for ExecutorEnvironmentCapabilityHandler {
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.read(attempt, material, durable, invocation, None))
    }
    fn invoke_supervised<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: &'a OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.read(attempt, material, durable, invocation, Some(activity)))
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}
