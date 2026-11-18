//! Shared Cloud/Device Skill preview adapter. The original Creator owns the
//! preview transaction; the Tool journal separately owns the result receipt.
//! This intentionally returns Provider, never a claim of a combined commit.
mod requests;

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
    ToolPhysicalExecution,
};
use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::MediaType;
use musteragent_identity::UtcTimestamp;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, OperationContext, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
    execution_liveness::{ExecutionBinding, InvocationExecutionTiming},
};
use musteragent_runtime_skill::{
    SkillCreatorApplication, SkillCreatorApplicationError, SkillPackagePreviewStoreError,
};
use requests::{
    CreateSkillPackagePreviewRequest, SkillPackageToolError, SkillPackageTools,
    UpdateSkillPackagePreviewRequest,
};
use std::sync::Arc;

pub struct SkillPreviewCapabilityHandler {
    manifest: CapabilityManifest,
    content: Arc<dyn ContentStore>,
    tools: SkillPackageTools,
    clock: Arc<dyn RuntimeClock>,
}

impl SkillPreviewCapabilityHandler {
    pub fn new(
        manifest: CapabilityManifest,
        content: Arc<dyn ContentStore>,
        creator: Arc<SkillCreatorApplication>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            content,
            tools: SkillPackageTools::new(creator),
            clock,
        }
    }

    async fn invoke_inner(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: CapabilityInvocation,
        context: Option<&OperationContext>,
    ) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
        crate::atomic_authorization::validate_sealed_invocation(
            &self.manifest,
            attempt,
            material,
            durable,
            &invocation,
        )?;
        let check = || {
            available(
                &invocation.timing,
                self.clock.now(),
                context.is_some_and(OperationContext::stopping),
            )
        };
        check()?;
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        // Reading content may have awaited I/O. Check the same supervisor and
        // original deadline again immediately before entering the business owner.
        let result = match invocation.tool_key.as_str() {
            "skill.package.preview.create@1" => {
                let request =
                    match serde_json::from_slice::<CreateSkillPackagePreviewRequest>(&bytes) {
                        Ok(value) => value,
                        Err(_) => return rejected(ErrorCode::InvalidCommand),
                    };
                check()?;
                self.tools
                    .create_preview(
                        invocation.tenant_id,
                        invocation.actor.principal_id,
                        invocation.idempotency_key.clone(),
                        self.clock.now(),
                        request,
                    )
                    .await
            }
            "skill.package.preview.update@1" => {
                let request =
                    match serde_json::from_slice::<UpdateSkillPackagePreviewRequest>(&bytes) {
                        Ok(value) => value,
                        Err(_) => return rejected(ErrorCode::InvalidCommand),
                    };
                check()?;
                self.tools
                    .update_preview(
                        invocation.tenant_id,
                        invocation.actor.principal_id,
                        invocation.idempotency_key.clone(),
                        self.clock.now(),
                        request,
                    )
                    .await
            }
            _ => return Err(port(ErrorCode::RuntimeSnapshotInvalid, false)),
        };
        let value = match result {
            Ok(value) => value,
            Err(SkillPackageToolError::InvalidRequest) => {
                return rejected(ErrorCode::InvalidCommand);
            }
            Err(SkillPackageToolError::Creator(
                SkillCreatorApplicationError::Conflict
                | SkillCreatorApplicationError::IdempotencyConflict,
            )) => return rejected(ErrorCode::IdempotencyConflict),
            Err(SkillPackageToolError::Creator(SkillCreatorApplicationError::NotFound)) => {
                return rejected(ErrorCode::CapabilityUnavailable);
            }
            Err(SkillPackageToolError::Creator(
                SkillCreatorApplicationError::Audit(_)
                | SkillCreatorApplicationError::IdentityChange
                | SkillCreatorApplicationError::Archived
                | SkillCreatorApplicationError::InvalidTarget,
            )) => return rejected(ErrorCode::InvalidCommand),
            Err(SkillPackageToolError::Creator(
                SkillCreatorApplicationError::Store(SkillPackagePreviewStoreError::Unavailable)
                | SkillCreatorApplicationError::Content(
                    musteragent_runtime_content::ContentStoreError::Unavailable,
                ),
            )) => return Err(port(ErrorCode::DependencyUnavailable, true)),
            Err(_) => return Err(port(ErrorCode::InternalInvariantViolated, false)),
        };
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation.invocation_id, "result"),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: serde_json::to_vec(&value)
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
        Ok(DispatchedCapabilityOutcome::Provider(
            CapabilityOutcome::Succeeded(CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: invocation.result_schema,
                provider_request_ref: None,
            }),
        ))
    }
}

impl AtomicCapabilityHandler for SkillPreviewCapabilityHandler {
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        // Retain the original preview's independent transaction semantics; this
        // adapter does not manufacture a fenced atomic business commit.
        crate::opaque_atomic_execution(self, attempt, material, durable, invocation, binding)
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
        context: &'a OperationContext,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(self.invoke_inner(attempt, material, durable, invocation, Some(context)))
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::InternalInvariantViolated, false)) })
    }
}

fn available(
    timing: &InvocationExecutionTiming,
    now: UtcTimestamp,
    stopping: bool,
) -> Result<(), LoopPortError> {
    if stopping {
        return Err(port(ErrorCode::DependencyUnavailable, false));
    }
    let deadline = match timing {
        InvocationExecutionTiming::LegacyFixed { deadline } => Some(*deadline),
        InvocationExecutionTiming::ActivityV1(timing) => {
            timing.business_deadline.as_ref().map(|value| value.at)
        }
    };
    if deadline.is_some_and(|deadline| deadline <= now) {
        return Err(port(ErrorCode::DependencyTimeout, false));
    }
    Ok(())
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}
fn rejected(code: ErrorCode) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                retryable: false,
                safe_message: SafeDisplayText::new("Skill preview request was rejected")
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_stopped_or_expired_invocation_cannot_enter_the_preview_owner() {
        let now = UtcTimestamp::from_unix_millis(1_800_000_000_000);
        let timing = InvocationExecutionTiming::LegacyFixed { deadline: now };
        assert_eq!(
            available(&timing, now, false).unwrap_err().code,
            ErrorCode::DependencyTimeout
        );
        assert_eq!(
            available(&timing, now, true).unwrap_err().code,
            ErrorCode::DependencyUnavailable
        );
        let earlier = UtcTimestamp::from_unix_millis(now.unix_millis() - 1);
        available(&timing, earlier, false).unwrap();
    }
}
