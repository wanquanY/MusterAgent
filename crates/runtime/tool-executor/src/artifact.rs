//! Shared Artifact failure mapping and invocation validation for the Rust
//! Artifact-family capability handlers (file, media, application, office).
use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_artifact::{ArtifactToolCommitFailure, ArtifactToolError};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, InteractionResolution,
    ReconciliationOutcome, ToolExecutionMaterial, authorize_invocation,
};
use musteragent_runtime_content::ContentStoreError;
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText};

use crate::{DispatchedCapabilityOutcome, DispatchedReconciliationOutcome};

pub(crate) fn artifact_commit_failure(
    failure: ArtifactToolCommitFailure,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let (error, retry_disposition) = artifact_commit_error(failure)?;
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result: None,
        },
    ))
}

pub(crate) fn artifact_commit_reconciliation_failure(
    failure: ArtifactToolCommitFailure,
) -> Result<DispatchedReconciliationOutcome, LoopPortError> {
    let (error, retry_disposition) = artifact_commit_error(failure)?;
    Ok(DispatchedReconciliationOutcome::Provider(
        ReconciliationOutcome::ProvenFailed {
            error,
            retry_disposition,
            diagnostic_result: None,
        },
    ))
}

fn artifact_commit_error(
    failure: ArtifactToolCommitFailure,
) -> Result<(ErrorInfo, RetryDisposition), LoopPortError> {
    let (code, retryable, message) = match failure {
        ArtifactToolCommitFailure::StaleFence => {
            return Err(port(ErrorCode::AttemptLeaseStale, false));
        }
        ArtifactToolCommitFailure::Unavailable => {
            return Err(port(ErrorCode::DependencyUnavailable, true));
        }
        ArtifactToolCommitFailure::WriterBusy => (
            ErrorCode::CapabilityUnavailable,
            false,
            musteragent_runtime_resource::WORKSPACE_WRITER_BUSY_MESSAGE,
        ),
        ArtifactToolCommitFailure::InvalidCommand => (
            ErrorCode::InvalidCommand,
            false,
            "Artifact commit command or target is invalid",
        ),
        ArtifactToolCommitFailure::Authorization => (
            ErrorCode::PolicyDenied,
            false,
            "Artifact commit identity or content is not authorized",
        ),
        ArtifactToolCommitFailure::Conflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Artifact or Project file state changed and conflicts with this command",
        ),
        ArtifactToolCommitFailure::NameConflict => (
            ErrorCode::ResourceNameConflict,
            false,
            "A file with this name already exists in the target folder. Find and read the existing file before reusing or editing it, or choose a new path for a separate work. Other file writes remain available.",
        ),
        ArtifactToolCommitFailure::Corrupt => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Artifact durable state is invalid",
        ),
    };
    Ok((
        ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::bounded(message)
                .or_else(|| SafeDisplayText::new(code.as_str()).ok())
                .ok_or_else(invariant)?,
            retryable,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        if retryable {
            RetryDisposition::RetryNewAttempt
        } else {
            RetryDisposition::DoNotRetry
        },
    ))
}

pub(crate) fn artifact_failure(
    error: ArtifactToolError,
) -> Result<CapabilityOutcome, LoopPortError> {
    let (code, retryable, message) = match error {
        ArtifactToolError::Unavailable(reason) => {
            return artifact_failure_outcome(ErrorCode::CapabilityUnavailable, false, &reason);
        }
        ArtifactToolError::Reported {
            code,
            retryable,
            message,
        } => {
            return artifact_failure_outcome(code, retryable, &message);
        }
        ArtifactToolError::InvalidArgument { path, expected } => {
            return artifact_failure_outcome(
                ErrorCode::InvalidCommand,
                false,
                &format!(
                    "Invalid argument at {path}: {expected}. Correct this field before retrying."
                ),
            );
        }
        ArtifactToolError::UnsupportedTool => (
            ErrorCode::CapabilityUnavailable,
            false,
            "Artifact capability is unavailable for this Run",
        ),
        ArtifactToolError::InvalidJson
        | ArtifactToolError::InvalidInput
        | ArtifactToolError::InvalidIdentifier
        | ArtifactToolError::ArtifactKindMismatch
        | ArtifactToolError::Content(ContentStoreError::TooLarge) => (
            ErrorCode::InvalidCommand,
            false,
            "Artifact Tool input or target violates its pinned contract; inspect the active Tool definition and retry with corrected arguments",
        ),
        ArtifactToolError::ArtifactUnavailable => (
            ErrorCode::ArtifactNotFound,
            false,
            "Artifact is unavailable for this conversation",
        ),
        ArtifactToolError::ArtifactStorageUnavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Artifact storage is temporarily unavailable",
        ),
        ArtifactToolError::ArtifactStateCorrupt => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Artifact storage returned state that failed integrity validation",
        ),
        ArtifactToolError::VersionConflict => (
            ErrorCode::IdempotencyConflict,
            false,
            "Artifact version has advanced",
        ),
        ArtifactToolError::InvalidBackendResult
        | ArtifactToolError::Content(
            ContentStoreError::NotFound
            | ContentStoreError::LengthMismatch
            | ContentStoreError::HashMismatch
            | ContentStoreError::IdentityConflict
            | ContentStoreError::Invariant,
        ) => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Artifact content failed integrity validation",
        ),
        ArtifactToolError::Content(ContentStoreError::Unauthorized) => (
            ErrorCode::PolicyDenied,
            false,
            "Artifact content is not authorized in the current Run",
        ),
        ArtifactToolError::UnauthorizedContent => (
            ErrorCode::PolicyDenied,
            false,
            "Requested content is not reachable from the current Run or the target Artifact",
        ),
        ArtifactToolError::AuthorizationUnavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Artifact content authorization is temporarily unavailable",
        ),
        ArtifactToolError::Content(ContentStoreError::Unavailable) => (
            ErrorCode::DependencyUnavailable,
            true,
            "Artifact content dependency is temporarily unavailable",
        ),
    };
    artifact_failure_outcome(code, retryable, message)
}

fn artifact_failure_outcome(
    code: ErrorCode,
    retryable: bool,
    message: &str,
) -> Result<CapabilityOutcome, LoopPortError> {
    Ok(CapabilityOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::bounded(message)
                .or_else(|| SafeDisplayText::new(code.as_str()).ok())
                .ok_or_else(invariant)?,
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

pub(crate) fn validate_invocation(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    invocation: &CapabilityInvocation,
) -> Result<(), LoopPortError> {
    durable.validate().map_err(|_| snapshot_mismatch())?;
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
    if manifest.location != grant.location
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
        return Err(snapshot_mismatch());
    }
    Ok(())
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

const fn snapshot_mismatch() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::RuntimeSnapshotInvalid,
        retryable: false,
    }
}

fn invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(error: ArtifactToolError) -> (ErrorInfo, RetryDisposition) {
        match artifact_failure(error).expect("stable Artifact failure") {
            CapabilityOutcome::Failed {
                error,
                retry_disposition,
                ..
            } => (error, retry_disposition),
            other => panic!("expected failed Artifact outcome, got {other:?}"),
        }
    }

    #[test]
    fn dynamic_artifact_failure_keeps_its_classification_and_bounded_detail() {
        let (error, retry) = failed(ArtifactToolError::Unavailable(format!(
            "Missing folder\n{}",
            "界".repeat(400)
        )));
        assert_eq!(error.code, ErrorCode::CapabilityUnavailable);
        assert_eq!(retry, RetryDisposition::DoNotRetry);
        assert!(error.safe_message.as_str().starts_with("Missing folder "));
        assert!(error.safe_message.as_str().ends_with('…'));
        assert!(error.safe_message.as_str().len() <= 1024);
        let (reported, retry) = failed(ArtifactToolError::Reported {
            code: ErrorCode::ResourceRevisionConflict,
            retryable: true,
            message: "The captured file changed.".into(),
        });
        assert_eq!(reported.code, ErrorCode::ResourceRevisionConflict);
        assert_eq!(reported.safe_message.as_str(), "The captured file changed.");
        assert!(reported.retryable);
        assert_eq!(retry, RetryDisposition::RetryNewAttempt);
        let (empty, _) = failed(ArtifactToolError::Unavailable("\n".into()));
        assert_eq!(empty.safe_message.as_str(), empty.code.as_str());
    }

    #[test]
    fn deterministic_commit_conflict_is_a_terminal_provider_failure() {
        let outcome = artifact_commit_failure(ArtifactToolCommitFailure::Conflict)
            .expect("conflict must be represented as a Tool outcome");

        let DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Failed {
            error,
            retry_disposition,
            ..
        }) = outcome
        else {
            panic!("conflict must be a failed provider outcome");
        };
        assert_eq!(error.code, ErrorCode::IdempotencyConflict);
        assert!(!error.retryable);
        assert_eq!(retry_disposition, RetryDisposition::DoNotRetry);
    }

    #[test]
    fn actual_commit_dependency_outage_remains_retryable() {
        let error = artifact_commit_failure(ArtifactToolCommitFailure::Unavailable)
            .expect_err("dependency outage must stay on the retry path");

        assert_eq!(error.code, ErrorCode::DependencyUnavailable);
        assert!(error.retryable);
    }

    #[test]
    fn name_collision_reports_existing_file_and_a_concrete_recovery_action() {
        let (error, retry) =
            artifact_commit_error(ArtifactToolCommitFailure::NameConflict).unwrap();
        assert_eq!(error.code, ErrorCode::ResourceNameConflict);
        assert!(!error.retryable);
        assert_eq!(retry, RetryDisposition::DoNotRetry);
        assert!(error.safe_message.as_str().contains("existing file"));
    }

    #[test]
    fn unreachable_content_is_not_reported_as_a_fixed_contract_error() {
        let (unreachable, retry_disposition) = failed(ArtifactToolError::UnauthorizedContent);
        assert_eq!(unreachable.code, ErrorCode::PolicyDenied);
        assert!(!unreachable.retryable);
        assert_eq!(retry_disposition, RetryDisposition::DoNotRetry);

        let (unavailable, retry_disposition) = failed(ArtifactToolError::AuthorizationUnavailable);
        assert_eq!(unavailable.code, ErrorCode::DependencyUnavailable);
        assert!(unavailable.retryable);
        assert_eq!(retry_disposition, RetryDisposition::RetryNewAttempt);
    }
}
