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
    BuiltinFileEditTools, ClassifyFileEditToolCommitError, CommitPreparedFileEditTool,
    FencedFileEditToolCommitter, FileEditError, FileEditToolCommitFailure, FileEditToolError,
    FileEditToolPreparation, ProjectFileReadError, ResourceStoreError, ResourceToolContext,
};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub struct FileEditCapabilityHandler<C>
where
    C: FencedFileEditToolCommitter,
{
    manifest: CapabilityManifest,
    tools: BuiltinFileEditTools,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
}

impl<C> FileEditCapabilityHandler<C>
where
    C: FencedFileEditToolCommitter,
{
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        tools: BuiltinFileEditTools,
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

impl<C> AtomicCapabilityHandler for FileEditCapabilityHandler<C>
where
    C: FencedFileEditToolCommitter + 'static,
    C::Error: ClassifyFileEditToolCommitError,
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
                Ok(FileEditToolPreparation::ReadOnly(result)) => Ok(
                    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(result)),
                ),
                Ok(FileEditToolPreparation::Commit { results, mutation }) => {
                    let invocation_id = mutation.context().invocation_id.ok_or_else(invariant)?;
                    let committed_results = results.clone();
                    match self
                        .committer
                        .commit(
                            attempt,
                            CommitPreparedFileEditTool {
                                run_context: material.run_context,
                                invocation_id,
                                results,
                                mutation: *mutation,
                                committed_at,
                            },
                        )
                        .await
                    {
                        Ok(receipt) => {
                            let result = committed_results
                                .select(&receipt)
                                .map_err(|_| invariant())?;
                            Ok(DispatchedCapabilityOutcome::Committed(result))
                        }
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
    error: FileEditToolError,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    let failure = match error {
        FileEditToolError::EditTargetNotFound => {
            return provider_failure(
                ErrorCode::InvalidCommand,
                false,
                "old_text was not found in the edit checkpoint. Resume the session and copy exact source text before patching.",
            );
        }
        FileEditToolError::AmbiguousEditTarget => {
            return provider_failure(
                ErrorCode::InvalidCommand,
                false,
                "old_text matches more than once. Include enough surrounding text to identify one unique replacement.",
            );
        }
        FileEditToolError::Edit(FileEditError::WriterBusy)
        | FileEditToolError::Read(ProjectFileReadError::Store(ResourceStoreError::WriterBusy)) => {
            return provider_failure(
                ErrorCode::CapabilityUnavailable,
                false,
                musteragent_runtime_resource::WORKSPACE_WRITER_BUSY_MESSAGE,
            );
        }
        FileEditToolError::IdentityMismatch => FileEditToolCommitFailure::Authorization,
        FileEditToolError::UnsupportedTool
        | FileEditToolError::InvalidInput
        | FileEditToolError::InvalidIdentifier
        | FileEditToolError::Edit(
            FileEditError::InvalidExpiry
            | FileEditError::InvalidOperation
            | FileEditError::UnsupportedFile
            | FileEditError::FileTooLarge
            | FileEditError::InvalidEncoding
            | FileEditError::SessionUnavailable
            | FileEditError::FileUnavailable,
        ) => FileEditToolCommitFailure::InvalidCommand,
        FileEditToolError::Edit(FileEditError::IdentityMismatch) => {
            FileEditToolCommitFailure::Authorization
        }
        FileEditToolError::Read(ProjectFileReadError::IdentityMismatch) => {
            FileEditToolCommitFailure::Authorization
        }
        FileEditToolError::Edit(
            FileEditError::SessionConflict | FileEditError::BaseVersionConflict,
        ) => FileEditToolCommitFailure::Conflict,
        FileEditToolError::Read(
            ProjectFileReadError::InvalidRange
            | ProjectFileReadError::InvalidEncoding
            | ProjectFileReadError::Store(
                ResourceStoreError::NodeUnavailable
                | ResourceStoreError::PrivateTaskResult
                | ResourceStoreError::BoundsExceeded
                | ResourceStoreError::ParentUnavailable
                | ResourceStoreError::FileUnavailable,
            ),
        ) => FileEditToolCommitFailure::InvalidCommand,
        FileEditToolError::ContentUnavailable
        | FileEditToolError::Edit(
            FileEditError::ContentUnavailable | FileEditError::DependencyUnavailable,
        )
        | FileEditToolError::Read(
            ProjectFileReadError::ContentUnavailable
            | ProjectFileReadError::Store(ResourceStoreError::DependencyUnavailable),
        ) => FileEditToolCommitFailure::Unavailable,
        FileEditToolError::InvalidResult
        | FileEditToolError::Capability(_)
        | FileEditToolError::ResourceTool(_)
        | FileEditToolError::Edit(FileEditError::Corrupt)
        | FileEditToolError::Read(ProjectFileReadError::ContentIntegrity)
        | FileEditToolError::Read(ProjectFileReadError::Store(
            ResourceStoreError::Corrupt
            | ResourceStoreError::RevisionConflict
            | ResourceStoreError::NameConflict
            | ResourceStoreError::MoveCycle,
        )) => FileEditToolCommitFailure::Corrupt,
    };
    commit_failure(failure)
}

fn commit_failure(
    failure: FileEditToolCommitFailure,
) -> Result<DispatchedCapabilityOutcome, LoopPortError> {
    match failure {
        FileEditToolCommitFailure::WriterBusy => provider_failure(
            ErrorCode::CapabilityUnavailable,
            false,
            musteragent_runtime_resource::WORKSPACE_WRITER_BUSY_MESSAGE,
        ),
        FileEditToolCommitFailure::StaleFence => Err(port(ErrorCode::AttemptLeaseStale, false)),
        FileEditToolCommitFailure::Unavailable => Err(port(ErrorCode::DependencyUnavailable, true)),
        FileEditToolCommitFailure::InvalidCommand => provider_failure(
            ErrorCode::InvalidCommand,
            false,
            "File Edit command or target is invalid",
        ),
        FileEditToolCommitFailure::Authorization => provider_failure(
            ErrorCode::PolicyDenied,
            false,
            "File Edit identity is not authorized",
        ),
        FileEditToolCommitFailure::Conflict => provider_failure(
            ErrorCode::IdempotencyConflict,
            false,
            "File Edit Session changed or conflicts with this command",
        ),
        FileEditToolCommitFailure::Corrupt => provider_failure(
            ErrorCode::InternalInvariantViolated,
            false,
            "File Edit durable state is invalid",
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
