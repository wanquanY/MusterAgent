//! Registered Task operations use original WorkItem/Tool transaction owners.
//! Model arguments describe a bounded objective; they cannot choose authority.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_argument_invocation,
};
use musteragent_capability::CapabilityManifest;
use musteragent_identity::PrincipalKind;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, OperationContext, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_capability::{DialogueStoreError, WorkItemToolResult, WorkItemTools};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use std::sync::Arc;

pub struct WorkItemCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    start: Arc<dyn WorkItemTools>,
    clock: Arc<dyn RuntimeClock>,
}
impl WorkItemCapabilityHandler {
    /// Exact contracts implemented by this handler. Composition and invocation
    /// share this closed check; a published but unimplemented version fails closed.
    #[must_use]
    pub fn supports_tool_key(key: &musteragent_tool::ToolKey) -> bool {
        matches!(
            key.as_str(),
            "tasks.followup@1"
                | "tasks.start@1"
                | "tasks.start@2"
                | "tasks.start@3"
                | "tasks.start@4"
                | "tasks.executions.list@1"
                | "tasks.executions.list@2"
                | "tasks.list@1"
                | "tasks.inspect@1"
                | "tasks.control@1"
                | "tasks.amend@1"
                | "tasks.amend@2"
                | "tasks.verify@1"
                | "tasks.verify@2"
                | "tasks.submit_result@1"
                | "tasks.verify@3"
                | "tasks.feedback@1"
                | "tasks.handoff@1"
        )
    }

    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        start: Arc<dyn WorkItemTools>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            start,
            clock,
        }
    }
    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: Option<&OperationContext>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        if invocation.actor != material.actor
            || invocation.actor.tenant_id != invocation.tenant_id
            || invocation.authorized_content != material.authorized_content
            || invocation.requested_by_attempt_id != durable.invocation.requested_by_attempt_id
            || !Self::supports_tool_key(&invocation.tool_key)
            || material.run_context.run_id != attempt.run_id
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        if material.actor.principal_kind != PrincipalKind::User
            || material.run_context.parent_run_id.is_some()
            || material.run_context.delegation_id.is_some()
            || !crate::atomic_authorization::dialogue_workspace_matches(
                material,
                invocation.workspace_id,
            )
        {
            return failure(DialogueStoreError::Unavailable);
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
        crate::schema_validation::ToolSchemaValidatorCache::new()
            .validate_async(
                invocation.request_schema.schema_hash,
                &material.tool.input_schema_json,
                &arguments,
            )
            .await
            .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
        let outcome = match self
            .start
            .invoke(attempt, material, &invocation, &bytes, activity)
            .await
        {
            Ok(value) => value,
            Err(error) => return failure(error),
        };
        stopping(activity)?;
        let read = matches!(
            invocation.tool_key.as_str(),
            "tasks.executions.list@1"
                | "tasks.executions.list@2"
                | "tasks.list@1"
                | "tasks.inspect@1"
        );
        let result = match &outcome {
            WorkItemToolResult::Read(value) if read => &value.result,
            WorkItemToolResult::Committed(value) if !read => value,
            _ => return Err(port(ErrorCode::InternalInvariantViolated, false)),
        };
        if result.result_schema != invocation.result_schema
            || result.result_hash != result.result_ref.sha256
        {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
        let result_bytes = self
            .content
            .read(invocation.tenant_id, &result.result_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&result.result_ref, &result_bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let result_json = serde_json::from_slice(&result_bytes)
            .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        crate::schema_validation::ToolSchemaValidatorCache::new()
            .validate_async(
                invocation.result_schema.schema_hash,
                &material.tool.output_schema_json,
                &result_json,
            )
            .await
            .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        Ok(match outcome {
            WorkItemToolResult::Committed(result) => DispatchedCapabilityOutcome::Committed(result),
            WorkItemToolResult::Read(value) => {
                DispatchedCapabilityOutcome::Provider(if value.authorized_content.is_empty() {
                    CapabilityOutcome::Succeeded(value.result)
                } else {
                    CapabilityOutcome::SucceededWithAuthorizedContent {
                        result: value.result,
                        authorized_content: value.authorized_content,
                    }
                })
            }
        })
    }
}
impl AtomicCapabilityHandler for WorkItemCapabilityHandler {
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
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, None))
    }
    fn invoke_supervised<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        activity: &'a OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, Some(activity)))
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
fn failure(error: DialogueStoreError) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let (code, retryable, text) = match error {
        DialogueStoreError::Infrastructure => (
            ErrorCode::DependencyUnavailable,
            true,
            "Task acceptance is temporarily unavailable",
        ),
        DialogueStoreError::Unavailable | DialogueStoreError::Inactive => (
            ErrorCode::PolicyDenied,
            false,
            "The task's source, Agent or dependencies are unavailable to this operation",
        ),
        DialogueStoreError::IdempotencyConflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Task acceptance conflicts with the original invocation",
        ),
        DialogueStoreError::RevisionConflict => (
            ErrorCode::ResourceRevisionConflict,
            false,
            "The task or execution changed; inspect its current revisions before proposing another command",
        ),
        _ => (
            ErrorCode::InvalidCommand,
            false,
            "The bounded task request or its canonical state could not be verified",
        ),
    };
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new(text)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                retryable,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: if retryable {
                RetryDisposition::RetryNewAttempt
            } else {
                RetryDisposition::DoNotRetry
            },
            diagnostic_result: None,
        },
    ))
}
