use std::sync::Arc;

use musteragent_capability::CapabilityManifest;
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_artifact::{
    ArtifactReader, ArtifactToolContext, BuiltinFileTools, ClassifyArtifactToolCommitError,
    CommitPreparedArtifactTool, FencedArtifactToolCommitter, FileToolPreparation,
};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, DurableToolInvocation, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

pub struct FileArtifactCapabilityHandler<A, C>
where
    A: ArtifactReader,
    C: FencedArtifactToolCommitter,
{
    manifest: CapabilityManifest,
    tools: BuiltinFileTools<A>,
    file_capture: Option<Arc<dyn musteragent_runtime_artifact::NativeFileCapture>>,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
}

impl<A, C> FileArtifactCapabilityHandler<A, C>
where
    A: ArtifactReader,
    C: FencedArtifactToolCommitter,
    C::Error: ClassifyArtifactToolCommitError,
{
    #[must_use]
    pub fn with_file_capture(
        mut self,
        capture: Arc<dyn musteragent_runtime_artifact::NativeFileCapture>,
    ) -> Self {
        self.file_capture = Some(capture);
        self
    }
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        tools: BuiltinFileTools<A>,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            file_capture: None,
            tools,
            committer,
            clock,
        }
    }
}

impl<A, C> AtomicCapabilityHandler for FileArtifactCapabilityHandler<A, C>
where
    A: ArtifactReader + 'static,
    C: FencedArtifactToolCommitter + 'static,
    C::Error: ClassifyArtifactToolCommitError,
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
            let context = ArtifactToolContext {
                execution: None,
                invocation,
                activity_id: material.run_context.activity_id,
                committed_by: material.run_context.executor_participant_id,
                now: self.clock.now(),
            };
            let prepared = if let Some(capture) = &self.file_capture {
                self.tools
                    .prepare_native_file(context, capture.as_ref())
                    .await
            } else {
                self.tools
                    .prepare(context)
                    .await
                    .map(|result| (result, Vec::new()))
            };
            let (prepared, authorized_content) = match prepared {
                Ok((value, content)) => (Ok(value), content),
                Err(error) => (Err(error), Vec::new()),
            };
            match prepared {
                Ok(FileToolPreparation::ReadOnly(result)) => Ok(
                    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(result)),
                ),
                Ok(FileToolPreparation::Commit {
                    result,
                    mutation,
                    resource_placement,
                }) => {
                    let committed_at = mutation.version().committed_at;
                    let invocation_id = mutation
                        .version()
                        .source_invocation_id
                        .ok_or_else(invariant)?;
                    match self
                        .committer
                        .commit(
                            attempt,
                            CommitPreparedArtifactTool {
                                resource_replacement: None,
                                run_context: material.run_context,
                                invocation_id,
                                result: result.clone(),
                                authorized_content,
                                mutation: *mutation,
                                resource_placement: resource_placement.map(|placement| *placement),
                                committed_at,
                            },
                        )
                        .await
                    {
                        Ok(_) => Ok(DispatchedCapabilityOutcome::Committed(result)),
                        Err(error) => crate::artifact::artifact_commit_failure(error.failure()),
                    }
                }
                Err(error) => Ok(DispatchedCapabilityOutcome::Provider(
                    crate::artifact::artifact_failure(error)?,
                )),
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

const fn invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}
