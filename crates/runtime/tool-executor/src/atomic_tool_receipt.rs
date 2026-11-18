//! Original atomic Tool receipt observation. No preparation, mutation,
//! new result or proven-not-applied verdict may be produced by this adapter.
use crate::{DispatchedReconciliationOutcome, RetainedToolResult};
use musteragent_capability::CapabilityManifest;
use musteragent_content_ref::SchemaRef;
use musteragent_event_envelope::ExecutionScope;
use musteragent_identity::{UtcTimestamp, WorkspaceId};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityOutcome, CapabilityReconciliation, DurableToolInvocation, InteractionResolution,
    OriginalToolReceiptQuery, ReconciliationOutcome, RuntimeClock, ToolExecutionMaterial,
    ToolExecutionPortError, ToolTerminalOutcome, authorize_invocation,
    original_tool_receipt_identity,
};
use musteragent_runtime_contract::{ErrorCode, OpaqueExternalRef, SafeDisplayText};
use musteragent_tool::{CapabilityExecutionTarget, ToolRetryContract};
use std::{future::Future, time::Duration};

pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Only the newly released write identities opt in. Frozen RuntimeTransaction
/// and read-only contracts retain their original hooks and prebound identity.
pub(crate) fn enabled(key: &str) -> bool {
    matches!(
        key,
        "delegation.spawn@5"
            | "delegation.send@3"
            | "delegation.cancel@2"
            | "delegation.retry@2"
            | "collaboration.send@1"
            | "agenda.todo.create@1"
            | "agenda.todo.create@2"
            | "agenda.todo.update@1"
            | "agenda.todo.update@2"
            | "agenda.todo.control@1"
            | "agenda.todo.control@2"
            | "agenda.entry.create@1"
            | "agenda.entry.create@2"
            | "agenda.entry.update@1"
            | "agenda.entry.update@2"
            | "agenda.entry.control@1"
            | "agenda.entry.control@2"
            | "agenda.changes.apply@1"
            | "agenda.changes.apply@2"
            | "agenda.changes.apply@3"
            | "goal.create@3"
            | "goal.draft.save@1"
            | "goal.update@3"
            | "goal.sources.configure@1"
            | "goal.change_status@2"
            | "schedules.create@2"
            | "schedules.update@2"
            | "schedules.control@2"
            | "schedules.create@1"
            | "schedules.update@1"
            | "schedules.control@1"
            | "agents.self.configure@2"
            | "device.workspace.file.publish@1"
            | "video.scene.preview@1"
            | "video.scene.render@1"
            | "video.narrate@1"
            | "audio.narration.create@1"
            | "audio.music.create@1"
            | "audio.effect.create@1"
            | "video.template.search@1"
            | "video.template.use@1"
            | "video.template.package@1"
            | "resource.file.create@4"
            | "resource.file.edit.begin@5"
            | "resource.file.edit.append@4"
            | "resource.file.edit.patch@6"
            | "resource.file.edit.finalize@4"
            | "resource.file.edit.abort@4"
            | "resource.folder.create@5"
            | "resource.node.rename@5"
            | "resource.node.move@4"
            | "resource.node.trash@4"
            | "resource.node.restore@4"
            | "resource.change-set.commit@6"
    )
}

pub(crate) fn query(
    manifest: &CapabilityManifest,
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    now: UtcTimestamp,
    deadline: UtcTimestamp,
) -> Result<OriginalToolReceiptQuery, LoopPortError> {
    durable.validate().map_err(|e| port(e.code, e.retryable))?;
    let tool = &material.tool.manifest;
    let grant = &material.capability_grant;
    let original = &durable.invocation;
    let identity = original_tool_receipt_identity(original.invocation_id);
    let mut authorization = material.authorization.clone();
    authorization.approval_satisfied = matches!(
        durable.interaction_resolution,
        Some(InteractionResolution::Approval(
            musteragent_approval::ApprovalDecision::Approve
                | musteragent_approval::ApprovalDecision::ApproveSession
        ))
    );
    if !enabled(manifest.key.as_str())
        || material.tool.execution_target != CapabilityExecutionTarget::RustRuntime
        || material.actor.tenant_id != material.run_context.tenant_id
        || attempt.run_id != material.run_context.run_id
        || original.run_id != attempt.run_id
        || original.tool_key != tool.key
        || original.capability_provider_id != manifest.provider_id
        || original.effect != tool.effect
        || durable.retry_contract != ToolRetryContract::QueryBeforeRetry
        || durable.retry_contract != tool.retry_contract
        || durable.prebound_reconciliation_ref.as_ref() != Some(&identity)
        || manifest.location != grant.location
        || manifest.provider_id != grant.provider_id
        || manifest.key != grant.key
        || manifest.effect != grant.effect
        || manifest.provider_id != tool.capability_provider_id
        || manifest.key.as_str() != tool.capability_key.as_str()
        || manifest.required_scopes != tool.required_scopes
        || manifest.required_contexts != tool.required_contexts
        || authorize_invocation(tool, grant, &material.tool.execution_target, &authorization)
            != musteragent_policy::Decision::Allow
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    if deadline <= now {
        return Err(port(ErrorCode::DependencyTimeout, true));
    }
    let bounded = now
        .unix_millis()
        .checked_add(10_000)
        .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
    Ok(OriginalToolReceiptQuery {
        actor: material.actor,
        run_context: material.run_context,
        invocation: original.clone(),
        retry_contract: durable.retry_contract,
        request_schema: tool.input_schema.clone(),
        result_schema: tool.output_schema.clone(),
        reconciliation_ref: identity,
        deadline: UtcTimestamp::from_unix_millis(deadline.unix_millis().min(bounded)),
    })
}

pub(crate) fn validate_reconciliation(
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    request: &CapabilityReconciliation,
) -> Result<(), LoopPortError> {
    let original = &durable.invocation;
    if request.actor != material.actor
        || request.tenant_id != material.run_context.tenant_id
        || request.conversation_id != material.run_context.conversation_id
        || request.run_id != material.run_context.run_id
        || request.workspace_id != workspace_id(material)
        || request.execution_attempt_id != attempt.attempt_id
        || request.execution_attempt_number != attempt.attempt_number
        || request.invocation_id != original.invocation_id
        || request.provider_id != original.capability_provider_id
        || request.idempotency_key != original.idempotency_key
        || request.result_schema != material.tool.manifest.output_schema
        || request.execution_grant_proof != material.execution_grant_proof
        || request.reconciliation_ref != original_tool_receipt_identity(original.invocation_id)
    {
        return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
    }
    Ok(())
}
fn workspace_id(material: &ToolExecutionMaterial) -> Option<WorkspaceId> {
    match material.run_context.execution_scope {
        ExecutionScope::Workspace { workspace_id } => Some(workspace_id),
        _ => None,
    }
}

pub(crate) async fn lookup<F>(
    clock: &dyn RuntimeClock,
    deadline: UtcTimestamp,
    schema: &SchemaRef,
    query: F,
) -> Result<Option<ToolTerminalOutcome>, LoopPortError>
where
    F: Future<Output = Result<Option<ToolTerminalOutcome>, ToolExecutionPortError>> + Send,
{
    let remaining = deadline
        .unix_millis()
        .checked_sub(clock.now().unix_millis())
        .filter(|value| *value > 0)
        .ok_or_else(|| port(ErrorCode::DependencyTimeout, true))?;
    let window = Duration::from_millis(
        u64::try_from(remaining).map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
    )
    .min(QUERY_TIMEOUT);
    let outcome = tokio::time::timeout(window, query)
        .await
        .map_err(|_| port(ErrorCode::DependencyTimeout, true))?
        .map_err(|e| port(e.code, e.retryable))?;
    if clock.now() >= deadline {
        return Err(port(ErrorCode::DependencyTimeout, true));
    }
    let result = match &outcome {
        Some(
            ToolTerminalOutcome::Succeeded(result)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. },
        ) => Some(result),
        Some(ToolTerminalOutcome::Failed {
            diagnostic_result, ..
        }) => diagnostic_result.as_ref(),
        _ => None,
    };
    if let Some(result) = result {
        result
            .validate()
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        if result.result_schema != *schema {
            return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
        }
    }
    Ok(outcome)
}

pub(crate) fn retained(outcome: Option<ToolTerminalOutcome>) -> RetainedToolResult {
    match outcome {
        Some(
            ToolTerminalOutcome::Succeeded(result)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. },
        ) => {
            // The executor reloads the original journal, preserving its exact
            // authorized-content and terminal-Ledger variant without re-commit.
            RetainedToolResult::Committed(result)
        }
        Some(ToolTerminalOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        }) => RetainedToolResult::Complete(Box::new(CapabilityOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        })),
        Some(ToolTerminalOutcome::Indeterminate { .. }) => RetainedToolResult::Indeterminate,
        Some(ToolTerminalOutcome::Cancelled(_)) | None => RetainedToolResult::Incomplete,
    }
}
pub(crate) fn reconciled(
    outcome: Option<ToolTerminalOutcome>,
    reconciliation_ref: OpaqueExternalRef,
) -> DispatchedReconciliationOutcome {
    let outcome = match outcome {
        Some(
            ToolTerminalOutcome::Succeeded(result)
            | ToolTerminalOutcome::SucceededWithAuthorizedContent { result, .. },
        ) => return DispatchedReconciliationOutcome::Committed(result),
        Some(ToolTerminalOutcome::Failed {
            error,
            retry_disposition,
            diagnostic_result,
        }) => ReconciliationOutcome::ProvenFailed {
            error,
            retry_disposition,
            diagnostic_result,
        },
        _ => ReconciliationOutcome::StillIndeterminate {
            reason: SafeDisplayText::new("The original Tool has no confirmed terminal result")
                .expect("static safe text"),
            reconciliation_ref,
        },
    };
    DispatchedReconciliationOutcome::Provider(outcome)
}
const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

#[cfg(test)]
#[path = "atomic_tool_receipt_tests.rs"]
mod tests;
