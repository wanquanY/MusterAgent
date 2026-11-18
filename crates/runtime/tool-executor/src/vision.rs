use musteragent_capability::CapabilityManifest;
use musteragent_identity::CorrelationId;
use musteragent_loop::LoopPortError;
use musteragent_model::AuxiliaryModelRole;
use musteragent_run::{AttemptContext, InvocationContext};
use musteragent_runtime_capability::{
    BuiltinVisionCapability, CapabilityInvocation, CapabilityOutcome, DurableToolInvocation,
    InteractionResolution, ToolExecutionMaterial, VisionCapabilityError, VisionExecutionRequest,
    active_pinned_model, authorize_invocation,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub struct VisionCapabilityHandler {
    manifest: CapabilityManifest,
    runtime: BuiltinVisionCapability,
}

impl VisionCapabilityHandler {
    #[must_use]
    pub fn new(manifest: CapabilityManifest, runtime: BuiltinVisionCapability) -> Self {
        Self { manifest, runtime }
    }
}

impl VisionCapabilityHandler {
    fn invoke_observed<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        parent: Option<musteragent_runtime_capability::OperationContext>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
            let model_invocation_context = vision_model_context(attempt, material, durable)?;
            match self
                .runtime
                .execute_observed(
                    VisionExecutionRequest {
                        capability: invocation,
                        model_invocation_context,
                        primary_model: active_pinned_model(
                            &material.model,
                            material.active_model_route_ordinal,
                        )
                        .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?,
                        auxiliary_models: material
                            .auxiliary_models
                            .iter()
                            .filter(|route| route.role == AuxiliaryModelRole::Vision)
                            .map(|route| route.model.clone())
                            .collect(),
                        stream_budget: material.model_stream_budget,
                        task_context_refs: material
                            .policy_basis
                            .as_ref()
                            .map(|basis| {
                                basis
                                    .source_records
                                    .iter()
                                    .map(|record| record.content_ref.clone())
                                    .collect()
                            })
                            .unwrap_or_default(),
                    },
                    parent,
                )
                .await
            {
                Ok(result) => Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Succeeded(result),
                )),
                Err(error) => {
                    if let VisionCapabilityError::ModelFailure {
                        code: ErrorCode::AttemptLeaseStale,
                        ..
                    } = error
                    {
                        return Err(LoopPortError {
                            code: ErrorCode::AttemptLeaseStale,
                            retryable: false,
                        });
                    }
                    let outcome = vision_failure(error)?;
                    Ok(DispatchedCapabilityOutcome::Provider(outcome))
                }
            }
        })
    }
}

impl AtomicCapabilityHandler for VisionCapabilityHandler {
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
        crate::dispatcher::validate_physical_execution(
            attempt,
            material,
            durable,
            &invocation,
            binding,
        )?;
        validate_invocation(&self.manifest, attempt, material, durable, &invocation)?;
        material
            .model_timing(&invocation.timing)
            .map_err(|code| port(code, false))?;
        Ok(Box::new(
            musteragent_runtime_capability::CooperativeExecution::new(
                binding,
                || Err(port(ErrorCode::DependencyUnavailable, false)),
                move |context| {
                    Box::pin(async move {
                        let result = self
                            .invoke_observed(
                                attempt,
                                material,
                                durable,
                                invocation,
                                Some(context.clone()),
                            )
                            .await;
                        context.quiescent();
                        result
                    })
                },
            ),
        ))
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        self.invoke_observed(attempt, material, durable, invocation, None)
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

fn validate_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    durable
        .validate()
        .map_err(|_| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
    let tool = &material.tool.manifest;
    let grant = &material.capability_grant;
    let persisted = &durable.invocation;
    let workspace_id = workspace_id(&material.run_context.execution_scope);
    let mut authorization = material.authorization.clone();
    authorization.approval_satisfied = matches!(
        durable.interaction_resolution,
        Some(InteractionResolution::Approval(
            musteragent_approval::ApprovalDecision::Approve
                | musteragent_approval::ApprovalDecision::ApproveSession
        ))
    );
    if !location_matches_execution_home(manifest.location, material.run_context.execution_home)
        || authorize_invocation(tool, grant, &material.tool.execution_target, &authorization)
            != musteragent_policy::Decision::Allow
        || manifest.provider_id != grant.provider_id
        || manifest.key != grant.key
        || manifest.effect != grant.effect
        || manifest.provider_id != tool.capability_provider_id
        || manifest.key.as_str() != tool.capability_key.as_str()
        || manifest.effect != tool.effect
        || manifest.approval != tool.approval
        || manifest.required_scopes != tool.required_scopes
        || manifest.required_contexts != tool.required_contexts
        || !manifest.required_scopes.is_subset(&grant.allowed_scopes)
        || !tool
            .required_scopes
            .is_subset(&material.authorization.actor_scopes)
        || !tool
            .required_contexts
            .is_subset(&material.authorization.available_contexts)
        || material.tool.execution_target
            != musteragent_tool::CapabilityExecutionTarget::RustRuntime
        || material.run_context.run_id != attempt.run_id
        || material.actor != invocation.actor
        || invocation.actor.tenant_id != invocation.tenant_id
        || invocation.tenant_id != material.run_context.tenant_id
        || invocation.conversation_id != material.run_context.conversation_id
        || invocation.run_id != material.run_context.run_id
        || invocation.requested_by_attempt_id != persisted.requested_by_attempt_id
        || invocation.execution_attempt_id != attempt.attempt_id
        || invocation.execution_attempt_number != attempt.attempt_number
        || invocation.invocation_id != persisted.invocation_id
        || invocation.tool_call_id != persisted.tool_call_id
        || invocation.tool_key != tool.key
        || invocation.tool_key != persisted.tool_key
        || invocation.capability_key != manifest.key
        || invocation.capability_provider_id != manifest.provider_id
        || invocation.capability_provider_id != persisted.capability_provider_id
        || invocation.idempotency_key != persisted.idempotency_key
        || invocation.request_ref != persisted.request_ref
        || invocation.request_schema != tool.input_schema
        || invocation.result_schema != tool.output_schema
        || invocation.authorized_content != material.authorized_content
        || invocation.workspace_id != workspace_id
        || invocation.timing != persisted.timing
        || persisted.run_id != attempt.run_id
        || persisted.execution_attempt_id != Some(attempt.attempt_id)
        || persisted.status != musteragent_tool::ToolInvocationStatus::Running
        || persisted.effect != tool.effect
        || persisted.approval != tool.approval
        || durable.retry_contract != tool.retry_contract
        || durable.recorded_outcome.is_some()
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}

fn location_matches_execution_home(
    location: musteragent_capability::CapabilityLocation,
    home: musteragent_event_envelope::RuntimeHome,
) -> bool {
    matches!(
        (location, home),
        (
            musteragent_capability::CapabilityLocation::Cloud,
            musteragent_event_envelope::RuntimeHome::Cloud
        ) | (
            musteragent_capability::CapabilityLocation::Device,
            musteragent_event_envelope::RuntimeHome::Device { .. }
        )
    )
}

fn workspace_id(
    scope: &musteragent_event_envelope::ExecutionScope,
) -> Option<musteragent_identity::WorkspaceId> {
    match scope {
        musteragent_event_envelope::ExecutionScope::HostedProject { scope } => {
            Some(scope.workspace_id)
        }
        musteragent_event_envelope::ExecutionScope::Workspace { workspace_id }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: Some(workspace_id),
            ..
        } => Some(*workspace_id),
        musteragent_event_envelope::ExecutionScope::Tenant { .. }
        | musteragent_event_envelope::ExecutionScope::DevicePrivate {
            workspace_id: None, ..
        } => None,
    }
}

fn vision_failure(error: VisionCapabilityError) -> Result<CapabilityOutcome, LoopPortError> {
    let (code, retryable, message) = match error {
        VisionCapabilityError::InvalidInput => (
            ErrorCode::InvalidCommand,
            false,
            "Vision request is invalid",
        ),
        VisionCapabilityError::InvalidResult => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Vision analysis returned an invalid result",
        ),
        VisionCapabilityError::InputLimit => (
            ErrorCode::InvalidCommand,
            false,
            "The image batch exceeds the frozen visual input limits",
        ),
        VisionCapabilityError::JointInputLimit => (
            ErrorCode::InvalidCommand,
            false,
            "Joint visual analysis requires more images in one request than the frozen route supports",
        ),
        VisionCapabilityError::UnauthorizedContent => (
            ErrorCode::PolicyDenied,
            false,
            "Image content is not authorized in the current Run; attach it or import it from the authorized workspace in this Run before retrying vision analysis",
        ),
        VisionCapabilityError::AuthorizationUnavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Artifact content authorization is temporarily unavailable",
        ),
        VisionCapabilityError::UnsupportedImageContent => (
            ErrorCode::InvalidCommand,
            false,
            "vision_analyze accepts only authorized image content_ids; the supplied content_id identifies a non-image resource",
        ),
        VisionCapabilityError::IdentityMismatch | VisionCapabilityError::TaskContextInvalid => (
            ErrorCode::RuntimeSnapshotInvalid,
            false,
            "Vision execution context is invalid",
        ),
        VisionCapabilityError::AuxiliaryRouteUnavailable
        | VisionCapabilityError::AuxiliaryRouteInvalid => (
            ErrorCode::ModelRouteUnavailable,
            false,
            "The Admin auxiliary vision model is not configured for this Run",
        ),
        VisionCapabilityError::RouteProfileInvalid => (
            ErrorCode::RuntimeSnapshotInvalid,
            false,
            "The frozen visual input profile is invalid",
        ),
        VisionCapabilityError::ModelFailure {
            code, retryable, ..
        } => (code, retryable, "Vision model invocation failed"),
        VisionCapabilityError::CandidateSequenceStopped(code) => (
            code,
            false,
            "The visual analysis stopped before a complete result was available",
        ),
        VisionCapabilityError::ModelFailed => (
            ErrorCode::ModelResponseInvalid,
            false,
            "Vision model lifecycle or response is invalid",
        ),
        VisionCapabilityError::Content(
            musteragent_runtime_content::ContentStoreError::Unavailable,
        ) => (
            ErrorCode::ContentUnavailable,
            true,
            "Image content is temporarily unavailable",
        ),
        VisionCapabilityError::Content(
            musteragent_runtime_content::ContentStoreError::NotFound,
        ) => (
            ErrorCode::ResourceFileUnavailable,
            false,
            "Image content was not found",
        ),
        VisionCapabilityError::Content(
            musteragent_runtime_content::ContentStoreError::Unauthorized,
        ) => (
            ErrorCode::PolicyDenied,
            false,
            "Image content is not authorized",
        ),
        VisionCapabilityError::Content(
            musteragent_runtime_content::ContentStoreError::TooLarge,
        ) => (
            ErrorCode::ModelContextExceeded,
            false,
            "Image content exceeds the permitted size",
        ),
        VisionCapabilityError::Content(_) => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Image content failed integrity validation",
        ),
    };
    Ok(CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new(message)
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
    })
}

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

/// Derived from the same original Tool and immutable Home-resolved Attempt.
pub fn vision_model_context(
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
) -> Result<InvocationContext, LoopPortError> {
    let nested_id = crate::nested_invocation_id(
        durable.invocation.invocation_id,
        attempt.attempt_id,
        b"vision",
    )?;
    let context = InvocationContext {
        invocation_id: nested_id,
        run_context: material.run_context,
        attempt_context: attempt.clone(),
        parent_invocation_id: Some(durable.invocation.invocation_id),
        causation_id: durable.request_record_id,
        correlation_id: CorrelationId::from_uuid(durable.invocation.invocation_id.into_uuid())
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
        timing: material
            .model_timing(&durable.invocation.timing)
            .map_err(|code| port(code, false))?,
    };
    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(error: VisionCapabilityError) -> ErrorInfo {
        match vision_failure(error).expect("stable Vision failure") {
            CapabilityOutcome::Failed { error, .. } => error,
            other => panic!("expected failed Vision outcome, got {other:?}"),
        }
    }

    #[test]
    fn cross_run_content_rejection_tells_the_model_to_reauthorize_in_this_run() {
        let error = failed(VisionCapabilityError::UnauthorizedContent);
        assert_eq!(error.code, ErrorCode::PolicyDenied);
        assert!(!error.retryable);
        assert!(error.safe_message.as_str().contains("current Run"));
        assert!(error.safe_message.as_str().contains("import"));
    }

    #[test]
    fn invalid_provider_output_is_not_reported_as_invalid_model_arguments() {
        let error = failed(VisionCapabilityError::InvalidResult);
        assert_eq!(error.code, ErrorCode::ContentIntegrityFailed);
        assert!(!error.retryable);
    }

    #[test]
    fn non_image_content_id_is_reported_as_an_invalid_tool_argument() {
        let error = failed(VisionCapabilityError::UnsupportedImageContent);
        assert_eq!(error.code, ErrorCode::InvalidCommand);
        assert!(!error.retryable);
        assert!(error.safe_message.as_str().contains("non-image"));
    }
}
