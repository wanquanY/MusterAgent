//! Explicit native message/file delivery. The publication owner derives every
//! identity from the admitted invocation; assistant output never enters here.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    atomic_authorization::validate_argument_invocation,
};
use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, OperationContext, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_capability::{
    DialoguePublicationActivity, DialogueToolPublicationError, FencedDialogueToolPublication,
};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use std::sync::Arc;

pub struct DialoguePublicationCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    publication: Arc<dyn FencedDialogueToolPublication>,
    clock: Arc<dyn RuntimeClock>,
}

impl DialoguePublicationCapabilityHandler {
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        publication: Arc<dyn FencedDialogueToolPublication>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            publication,
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
        if activity.is_some_and(OperationContext::stopping) {
            return Err(port(ErrorCode::RunTransitionInvalid, false));
        }
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
        // The owner repeats scope, protocol, reference and argument validation
        // against the original durable invocation inside its fenced commit.
        let receipt = if let Some(activity) = activity {
            self.publication
                .commit_with_activity(
                    attempt,
                    invocation.invocation_id,
                    &PublicationActivity(activity),
                )
                .await
        } else {
            self.publication
                .commit(attempt, invocation.invocation_id)
                .await
        };
        match receipt {
            Ok(receipt) => Ok(DispatchedCapabilityOutcome::Committed(
                musteragent_runtime_capability::CapabilityResult {
                    result_hash: receipt.result_ref.sha256,
                    result_ref: receipt.result_ref,
                    result_schema: receipt.result_schema,
                    provider_request_ref: None,
                },
            )),
            Err(error) => publication_error(error),
        }
    }
}

struct PublicationActivity<'a>(&'a OperationContext);
impl DialoguePublicationActivity for PublicationActivity<'_> {
    fn completed_stage(&self) -> Result<(), DialogueToolPublicationError> {
        self.0
            .completed_stage()
            .map_err(|_| DialogueToolPublicationError::StaleFence)
    }
    fn cancelled(&self) -> ApplicationFuture<'_, ()> {
        Box::pin(self.0.cancelled())
    }
}

impl AtomicCapabilityHandler for DialoguePublicationCapabilityHandler {
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
        // Runtime-transaction retries use the same invocation and read its
        // retained result. They never repeat an external generation operation.
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

fn publication_error(
    error: DialogueToolPublicationError,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    use DialogueToolPublicationError as E;
    let (code, message) = match error {
        E::StaleFence => return Err(port(ErrorCode::AttemptLeaseStale, false)),
        // A missing acknowledgement is not proof of failure. Leave the original
        // invocation recoverable so its retained committed receipt wins.
        E::Infrastructure => return Err(port(ErrorCode::DependencyUnavailable, true)),
        E::Corrupt => return Err(port(ErrorCode::InternalInvariantViolated, false)),
        E::Forbidden => (
            ErrorCode::PolicyDenied,
            "This execution is not authorized to publish to the dialogue",
        ),
        E::InvalidArguments => (
            ErrorCode::InvalidCommand,
            "Use the registered publication fields and bounded nonempty content",
        ),
        E::ReferenceUnavailable => (
            ErrorCode::InvalidCommand,
            "The exact file version or producer receipt is unavailable; inspect the currently authorized file and use this tool contract without inventing a receipt",
        ),
    };
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new(message)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        },
    ))
}
