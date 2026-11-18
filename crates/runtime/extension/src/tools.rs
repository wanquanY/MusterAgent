//! Registered physical tools use the existing durable executor's dispatcher seam.
use crate::*;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, DurableToolInvocation, ToolExecutionMaterial,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, execution_liveness::ExecutionBinding,
};
use musteragent_tool_executor::{
    CapabilityDispatcher, InvocationPolicyDisposition, InvocationPolicyEvaluator,
    InvocationPolicyInput, ToolPhysicalExecution, combine_dispositions,
};
use std::sync::Arc;
mod operation;

/// Typed scope given to registered physical tools. The enclosing durable
/// executor owns authorization, invocation state, result persistence and supervision.
pub struct ExtensionToolContext<'a> {
    /// Workspace directory, session key, run ID, channel and sandbox status.
    pub execution: &'a ExtensionContext,
    /// Typed runtime services.
    pub api: RuntimeApi,
}
/// Registered physical implementation. It must not perform I/O until the returned
/// operation is polled. The operation supplies its actual stop-control owner.
pub trait ExtensionTool: Send + Sync {
    /// Prepare one owned operation after durable authorization and start.
    fn execution<'a>(
        &'a self,
        context: ExtensionToolContext<'a>,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, HookError>;
}
/// Install with `DurableToolExecutor::new(..., dispatcher, ...)`. Registration
/// does not bypass pinned material, schema validation, grants, approval or the
/// executor's liveness/result journal. Unregistered tools use the original dispatcher.
pub struct RegisteredToolDispatcher {
    inner: Arc<dyn CapabilityDispatcher>,
    host: Arc<ExtensionHost>,
    context: ExtensionContext,
}
impl RegisteredToolDispatcher {
    /// Bind a frozen registration set to a verified attempt.
    pub fn new(
        inner: Arc<dyn CapabilityDispatcher>,
        host: Arc<ExtensionHost>,
        context: ExtensionContext,
    ) -> Result<Self, ExtensionError> {
        host.validate_context(&context)?;
        Ok(Self {
            inner,
            host,
            context,
        })
    }
}
impl CapabilityDispatcher for RegisteredToolDispatcher {
    fn bind(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        if material.run_context != self.context.run {
            return Err(LoopPortError {
                code: ErrorCode::RuntimeSnapshotInvalid,
                retryable: false,
            });
        }
        let inner = self
            .inner
            .bind(material)?
            .unwrap_or_else(|| self.inner.clone());
        Ok(Some(Arc::new(Self {
            inner,
            host: self.host.clone(),
            context: self.context.clone(),
        })))
    }
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        if attempt != &self.context.attempt || material.run_context != self.context.run {
            return Err(LoopPortError {
                code: ErrorCode::RuntimeSnapshotInvalid,
                retryable: false,
            });
        }
        let Some(tool) = self
            .host
            .registry
            .tools
            .get(material.tool.manifest.wire_name.as_str())
        else {
            return self
                .inner
                .execution(attempt, material, durable, invocation, binding);
        };
        if tool.definition.key != material.tool.manifest.key
            || tool.definition.input_schema_ref != material.tool.manifest.input_schema
        {
            return Err(LoopPortError {
                code: ErrorCode::RuntimeSnapshotInvalid,
                retryable: false,
            });
        }
        let id = tool.owner.as_str();
        let audit_key = format!("registered:{}:failed", invocation.tool_call_id.as_str());
        let result = guard(id, Hook::Tool, || {
            tool.implementation.execution(
                ExtensionToolContext {
                    execution: &self.context,
                    api: self.host.internal_api(&self.context, id),
                },
                invocation,
                binding,
            )
        });
        Ok(operation::wrap(
            result,
            self.host.clone(),
            self.context.clone(),
            id.into(),
            audit_key,
            binding,
        ))
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_tool_executor::DispatchedCapabilityOutcome, LoopPortError>,
    > {
        if self
            .host
            .registry
            .tools
            .contains_key(material.tool.manifest.wire_name.as_str())
        {
            return Box::pin(async {
                Err(LoopPortError {
                    code: ErrorCode::CapabilityUnavailable,
                    retryable: false,
                })
            });
        }
        self.inner.invoke(attempt, material, durable, invocation)
    }
    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<
        'a,
        Result<musteragent_tool_executor::DispatchedReconciliationOutcome, LoopPortError>,
    > {
        self.inner.reconcile(attempt, material, durable, request)
    }
    fn approval_prompt<'a>(
        &'a self,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<
        'a,
        Result<Option<musteragent_tool_executor::ApprovalPromptOverride>, LoopPortError>,
    > {
        self.inner.approval_prompt(material, invocation)
    }
    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<musteragent_runtime_contract::OpaqueExternalRef>, LoopPortError> {
        self.inner.reconciliation_identity(material, invocation)
    }
    fn retained_result_query_timeout(
        &self,
        material: &ToolExecutionMaterial,
    ) -> Result<Option<std::time::Duration>, LoopPortError> {
        self.inner.retained_result_query_timeout(material)
    }
    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        invocation: &'a DurableToolInvocation,
        now: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<musteragent_tool_executor::RetainedToolResult, LoopPortError>>
    {
        self.inner
            .lookup_retained_result(attempt, material, invocation, now)
    }
}

/// Install in `DurableToolExecutor::with_invocation_policy_evaluator`. Denials
/// dominate allowances. The real executor persists checked hook failures as
/// `InvocationPolicyEvaluationError` in the original failed invocation outcome.
pub struct ExtensionInvocationPolicy {
    inner: Arc<dyn InvocationPolicyEvaluator>,
    host: Arc<ExtensionHost>,
}
impl ExtensionInvocationPolicy {
    /// Compose the host policy and ordered extension parameter policies.
    pub fn new(inner: Arc<dyn InvocationPolicyEvaluator>, host: Arc<ExtensionHost>) -> Self {
        Self { inner, host }
    }
}
impl InvocationPolicyEvaluator for ExtensionInvocationPolicy {
    fn evaluate(&self, input: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition {
        self.evaluate_checked(input)
            .unwrap_or(InvocationPolicyDisposition::Deny(
                "extension_invocation_policy_failed",
            ))
    }
    fn evaluate_checked(
        &self,
        input: &InvocationPolicyInput<'_>,
    ) -> Result<
        InvocationPolicyDisposition,
        musteragent_tool_executor::InvocationPolicyEvaluationError,
    > {
        let mut disposition = self.inner.evaluate_checked(input)?;
        for extension in &self.host.registry.extensions {
            let next = guard(extension.id(), Hook::InvocationPolicy, || {
                extension.invocation_policy(input)
            })
            .map_err(|failure| {
                let (code, reason) = match failure.kind {
                    HookFailureKind::Panic => {
                        ("hook_panicked".into(), "invocation policy panicked".into())
                    }
                    HookFailureKind::Error(error) => (error.code, error.message),
                };
                musteragent_tool_executor::InvocationPolicyEvaluationError {
                    evaluator: failure.extension,
                    code,
                    reason,
                }
            })?;
            disposition = combine_dispositions(disposition, next);
        }
        Ok(disposition)
    }
}
