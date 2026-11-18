use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_resource::{
    BuiltinResourceTools, ClassifyResourceToolCommitError, CommitPreparedResourceTool,
    FencedResourceToolCommitter, ResourceToolCommitFailure, ResourceToolContext, ResourceToolError,
    ResourceToolPreparation,
};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub struct ResourceCapabilityHandler<C>
where
    C: FencedResourceToolCommitter,
{
    manifest: CapabilityManifest,
    tools: BuiltinResourceTools,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
}

impl<C> ResourceCapabilityHandler<C>
where
    C: FencedResourceToolCommitter,
{
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        tools: BuiltinResourceTools,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            tools,
            committer,
            clock,
        }
    }
}

impl<C> AtomicCapabilityHandler for ResourceCapabilityHandler<C>
where
    C: FencedResourceToolCommitter + 'static,
    C::Error: ClassifyResourceToolCommitError,
{
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
        Box::pin(async move {
            crate::artifact::validate_invocation(
                &self.manifest,
                attempt,
                material,
                durable,
                &invocation,
            )?;
            let committed_at = self.clock.now();
            match self
                .tools
                .prepare(ResourceToolContext {
                    resolved_source: None,
                    invocation,
                    run_context: material.run_context,
                    now: committed_at,
                })
                .await
            {
                Ok(ResourceToolPreparation::ReadOnly(result)) => Ok(
                    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(result)),
                ),
                Ok(ResourceToolPreparation::Commit { result, mutation }) => {
                    let invocation_id = mutation.context().invocation_id.ok_or_else(invariant)?;
                    match self
                        .committer
                        .commit(
                            attempt,
                            CommitPreparedResourceTool {
                                run_context: material.run_context,
                                invocation_id,
                                result: result.clone(),
                                mutation: *mutation,
                                committed_at,
                            },
                        )
                        .await
                    {
                        Ok(_) => Ok(DispatchedCapabilityOutcome::Committed(result)),
                        Err(error) => commit_failure(error.failure()),
                    }
                }
                Err(error) => preparation_failure(error),
            }
        })
    }

    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<musteragent_runtime_contract::OpaqueExternalRef> {
        crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()).then(|| {
            musteragent_runtime_capability::original_tool_receipt_identity(invocation.invocation_id)
        })
    }

    fn retained_result_query_timeout(&self) -> Option<std::time::Duration> {
        crate::atomic_tool_receipt::enabled(self.manifest.key.as_str())
            .then_some(crate::atomic_tool_receipt::QUERY_TIMEOUT)
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<crate::RetainedToolResult, LoopPortError>> {
        Box::pin(async move {
            if !crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()) {
                return Ok(crate::RetainedToolResult::Incomplete);
            }
            let query = crate::atomic_tool_receipt::query(
                &self.manifest,
                attempt,
                material,
                durable,
                self.clock.now(),
                deadline,
            )?;
            let outcome = crate::atomic_tool_receipt::lookup(
                self.clock.as_ref(),
                query.deadline,
                &query.result_schema,
                self.committer
                    .lookup_original_receipt(attempt, query.clone()),
            )
            .await?;
            Ok(crate::atomic_tool_receipt::retained(outcome))
        })
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: musteragent_runtime_capability::CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            if !crate::atomic_tool_receipt::enabled(self.manifest.key.as_str()) {
                return Err(invariant());
            }
            crate::atomic_tool_receipt::validate_reconciliation(
                attempt, material, durable, &request,
            )?;
            let query = crate::atomic_tool_receipt::query(
                &self.manifest,
                attempt,
                material,
                durable,
                self.clock.now(),
                request.deadline,
            )?;
            let outcome = crate::atomic_tool_receipt::lookup(
                self.clock.as_ref(),
                query.deadline,
                &query.result_schema,
                self.committer
                    .lookup_original_receipt(attempt, query.clone()),
            )
            .await?;
            Ok(crate::atomic_tool_receipt::reconciled(
                outcome,
                request.reconciliation_ref,
            ))
        })
    }
}

fn preparation_failure(
    error: ResourceToolError,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    use musteragent_runtime_resource::{ResourceApplicationError, ResourceStoreError};

    let (code, retryable, message) = match error {
        ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::PrivateTaskResult,
        ))
        | ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::PrivateTaskResult,
        )) => (
            ErrorCode::PolicyDenied,
            false,
            musteragent_runtime_resource::PRIVATE_TASK_CHANGE_SET_MESSAGE,
        ),
        ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::WriterBusy,
        ))
        | ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::WriterBusy,
        )) => (
            ErrorCode::CapabilityUnavailable,
            false,
            musteragent_runtime_resource::WORKSPACE_WRITER_BUSY_MESSAGE,
        ),
        ResourceToolError::UnsupportedTool
        | ResourceToolError::InvalidInput
        | ResourceToolError::InvalidIdentifier
        | ResourceToolError::Search(
            musteragent_runtime_resource::ResourceSearchError::InvalidQuery,
        ) => (
            ErrorCode::InvalidCommand,
            false,
            "Resource Tool schema, identifier, or search query is invalid",
        ),
        ResourceToolError::Resource(
            ResourceApplicationError::InvalidPage | ResourceApplicationError::InvalidSortKey,
        ) => (
            ErrorCode::InvalidCommand,
            false,
            "Resource page request is invalid",
        ),
        ResourceToolError::Resource(ResourceApplicationError::InvalidPath) => {
            (ErrorCode::InvalidCommand, false, "Resource path is invalid")
        }
        ResourceToolError::Resource(ResourceApplicationError::InvalidFile) => (
            ErrorCode::InvalidCommand,
            false,
            "Resource file extension, media type, content, or authoring mode is invalid",
        ),
        ResourceToolError::Resource(ResourceApplicationError::InvalidChangeSet) => (
            ErrorCode::InvalidCommand,
            false,
            "Resource change set is invalid",
        ),
        ResourceToolError::IdentityMismatch
        | ResourceToolError::Search(
            musteragent_runtime_resource::ResourceSearchError::IdentityMismatch,
        )
        | ResourceToolError::Resource(ResourceApplicationError::IdentityMismatch) => (
            ErrorCode::PolicyDenied,
            false,
            "Resource Tool identity is not authorized",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::NameConflict,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::NameConflict,
        )) => (
            ErrorCode::ResourceNameConflict,
            false,
            "A Resource with this name already exists in the target folder",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::RevisionConflict,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::RevisionConflict,
        )) => (
            ErrorCode::ResourceRevisionConflict,
            false,
            "Resource changed while the command was running; inspect the current state and retry",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::MoveCycle,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::MoveCycle,
        )) => (
            ErrorCode::ResourceMoveCycle,
            false,
            "A folder cannot be moved into itself or one of its descendants",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::NodeUnavailable,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::NodeUnavailable,
        )) => (
            ErrorCode::ResourceNodeNotFound,
            false,
            "Resource node was not found",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::ParentUnavailable,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::ParentUnavailable,
        )) => (
            ErrorCode::ResourceParentNotFound,
            false,
            "Resource parent was not found",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::FileUnavailable,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::FileUnavailable,
        )) => (
            ErrorCode::ResourceFileUnavailable,
            false,
            "Resource file is unavailable or is already linked",
        ),
        ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::BoundsExceeded,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::BoundsExceeded,
        )) => (
            ErrorCode::InvalidCommand,
            false,
            "Resource query exceeds the source traversal budget; select a smaller subtree",
        ),
        ResourceToolError::ContentUnavailable
        | ResourceToolError::SearchUnavailable
        | ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::DependencyUnavailable,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::DependencyUnavailable,
        )) => (
            ErrorCode::DependencyUnavailable,
            true,
            "Resource persistence is temporarily unavailable",
        ),
        ResourceToolError::InvalidResult
        | ResourceToolError::Search(musteragent_runtime_resource::ResourceSearchError::Store(
            ResourceStoreError::Corrupt,
        ))
        | ResourceToolError::Resource(ResourceApplicationError::Store(
            ResourceStoreError::Corrupt,
        )) => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Resource durable state is invalid",
        ),
    };
    provider_failure(code, retryable, message)
}

fn commit_failure(
    failure: ResourceToolCommitFailure,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    match failure {
        ResourceToolCommitFailure::PrivateTaskResult => provider_failure(
            ErrorCode::PolicyDenied,
            false,
            musteragent_runtime_resource::PRIVATE_TASK_CHANGE_SET_MESSAGE,
        ),
        ResourceToolCommitFailure::WriterBusy => provider_failure(
            ErrorCode::CapabilityUnavailable,
            false,
            musteragent_runtime_resource::WORKSPACE_WRITER_BUSY_MESSAGE,
        ),
        ResourceToolCommitFailure::StaleFence => Err(port(ErrorCode::AttemptLeaseStale, false)),
        ResourceToolCommitFailure::Unavailable => Err(port(ErrorCode::DependencyUnavailable, true)),
        ResourceToolCommitFailure::InvalidCommand => provider_failure(
            ErrorCode::InvalidCommand,
            false,
            "Resource commit command or target is invalid",
        ),
        ResourceToolCommitFailure::Authorization => provider_failure(
            ErrorCode::PolicyDenied,
            false,
            "Resource commit identity is not authorized",
        ),
        ResourceToolCommitFailure::NameConflict => provider_failure(
            ErrorCode::ResourceNameConflict,
            false,
            "A Resource with this name already exists in the target folder",
        ),
        ResourceToolCommitFailure::Conflict => provider_failure(
            ErrorCode::ResourceRevisionConflict,
            false,
            "Resource changed while the command was running; inspect the current state and retry",
        ),
        ResourceToolCommitFailure::Corrupt => provider_failure(
            ErrorCode::InternalInvariantViolated,
            false,
            "Resource durable state is invalid",
        ),
    }
}

fn provider_failure(
    code: ErrorCode,
    retryable: bool,
    message: &'static str,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    Ok(DispatchedCapabilityOutcome::Provider(
        CapabilityOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new(message).map_err(|_| invariant())?,
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

const fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

const fn invariant() -> LoopPortError {
    port(ErrorCode::InternalInvariantViolated, false)
}
