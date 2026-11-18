use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use musteragent_artifact::GeneratedFileContract;
use musteragent_capability::{
    BrowserDeclaredOutput, BrowserDiagnosticSeverity, BrowserInput, BrowserJobEngine,
    BrowserJobProgram, BrowserJobRequest, BrowserJobStatus, BrowserProviderContract,
    BrowserResourceLimits, BrowserSecurityProfile, CapabilityManifest,
};
use musteragent_content_ref::MediaType;
use musteragent_identity::{
    BrowserJobId, InvocationId, ResourceNodeId, VersionNumber, VersionedKey,
};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_artifact::{
    ArtifactReader, ArtifactToolCommitFailure, ArtifactToolContext, BuiltinFileTools,
    ClassifyArtifactToolCommitError, CommitPreparedArtifactTool, FencedArtifactToolCommitter,
    FileToolPreparation, GeneratedFile,
};
use musteragent_runtime_capability::{
    BrowserGatewayStore, BrowserProviderRuntimeError, CapabilityInvocation, CapabilityOutcome,
    CapabilityReconciliation, DurableToolInvocation, ReconciliationOutcome, RuntimeClock,
    ToolExecutionMaterial,
};
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_content::PutContent;
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueDiagnosticRef, OpaqueExternalRef,
    RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_resource::{
    ProjectFileVersionStore, ResolveResourceContext, ResourceCommandContext,
    ResourceContextAuthority, ResourceContextError, ResourceStoreError,
};
use serde::{Deserialize, Serialize};

use musteragent_runtime_file_query::{FileQueryAccess, FileQueryService, RunFileQueryAccess};

#[path = "python_inputs.rs"]
mod inputs;
use inputs::PythonInputFile;

use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};

const BROWSER_RUNTIME_RELEASE: &str = "browser-runtime@2";
const MAX_INPUT_FILES: usize = 63;
const DEFAULT_MAX_OUTPUT_BYTES: u64 = GeneratedFileContract::MAXIMUM_BYTES;
const MAX_WALL_TIME_MILLIS: u64 = 120_000;
// Browser Runtime v2 accepts at most 50 KiB for each terminal log tail.
// Keep the producer default inside that cross-origin contract so requests are
// valid before any bytes or execution authority are handed to the Runner.
const MAX_LOG_TAIL_BYTES: u64 = 50 * 1024;

/// Runtime-owned public `data.python.run` handler. Browser pages execute only
/// the closed internal Job; they never become the public Tool authority.
pub struct BrowserPythonCapabilityHandler<A, C>
where
    A: ArtifactReader,
    C: FencedArtifactToolCommitter,
{
    manifest: CapabilityManifest,
    project_files: Arc<dyn ProjectFileVersionStore>,
    resource_authority: Arc<dyn ResourceContextAuthority>,
    file_queries: Option<Arc<FileQueryService>>,
    content: Arc<dyn ContentStore>,
    browser: Arc<dyn BrowserGatewayStore>,
    files: BuiltinFileTools<A>,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
}

impl<A, C> BrowserPythonCapabilityHandler<A, C>
where
    A: ArtifactReader,
    C: FencedArtifactToolCommitter,
{
    async fn prepare_job(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<crate::browser_execution::BrowserPreparation, LoopPortError> {
        let contract = match python_provider_contract(invocation.capability_key.as_str()) {
            Ok(value) => value,
            Err(error) => return Ok(Err(failure(error)?)),
        };
        let input = match self.request(invocation).await {
            Ok(value) => value,
            Err(error) => return Ok(Err(failure(error)?)),
        };
        match self.build_job(attempt, material, invocation, &input).await {
            Ok(request) => Ok(Ok(crate::browser_execution::PreparedBrowserJob {
                contract,
                request,
            })),
            Err(error) => Ok(Err(failure(error)?)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        manifest: CapabilityManifest,
        project_files: Arc<dyn ProjectFileVersionStore>,
        resource_authority: Arc<dyn ResourceContextAuthority>,
        content: Arc<dyn ContentStore>,
        browser: Arc<dyn BrowserGatewayStore>,
        files: BuiltinFileTools<A>,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            project_files,
            resource_authority,
            file_queries: None,
            content,
            browser,
            files,
            committer,
            clock,
        }
    }

    /// The same source/version authority serves file discovery and execution inputs.
    #[must_use]
    pub fn with_file_queries(mut self, files: Arc<FileQueryService>) -> Self {
        self.file_queries = Some(files);
        self
    }

    async fn request(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Result<PythonRunInput, PythonToolError> {
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| PythonToolError::DependencyUnavailable)?;
        musteragent_runtime_content::verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| PythonToolError::Invariant)?;
        let input: PythonRunInput =
            serde_json::from_slice(&bytes).map_err(|_| PythonToolError::InvalidInput)?;
        input.validate()?;
        if input.environment_ref.is_some()
            && !matches!(
                invocation.capability_key.as_str(),
                "data.python.run@9" | "data.python.run@12"
            )
        {
            return Err(PythonToolError::InvalidInput);
        }
        if !matches!(
            invocation.capability_key.as_str(),
            "data.python.run@7" | "data.python.run@8" | "data.python.run@9" | "data.python.run@12"
        ) && input.inputs.iter().any(PythonInputFile::is_file_reference)
        {
            return Err(PythonToolError::InvalidInput);
        }
        Ok(input)
    }

    async fn build_job(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
        input: &PythonRunInput,
    ) -> Result<BrowserJobRequest, PythonToolError> {
        let resource_context = self.resource_context(invocation).await?;
        let source = self
            .load_file(
                &resource_context,
                &input.source.file_node_id,
                input.source.version,
            )
            .await?;
        if source.media_type.as_str() != "text/x-python"
            || !source.filename.to_ascii_lowercase().ends_with(".py")
        {
            return Err(PythonToolError::InvalidInput);
        }

        let mut mounted = vec![BrowserInput {
            path: input.entrypoint.clone(),
            content_ref: source.content_ref.clone(),
        }];
        let access = FileQueryAccess {
            actor: invocation.actor,
            conversation_id: Some(invocation.conversation_id),
            workspace_id: None,
            run: Some(RunFileQueryAccess {
                invocation_id: Some(invocation.invocation_id),
                context: material.run_context,
                attempt: attempt.clone(),
                authorized_content: invocation.authorized_content.clone(),
                timing: invocation.timing.clone(),
            }),
        };
        for item in &input.inputs {
            let content_ref = match item {
                PythonInputFile::Resource {
                    file_node_id,
                    version,
                    ..
                } => {
                    self.load_file(&resource_context, file_node_id, *version)
                        .await?
                        .content_ref
                }
                PythonInputFile::File { file_ref, .. } => {
                    inputs::resolve_original(
                        self.file_queries
                            .as_deref()
                            .ok_or(PythonToolError::DependencyUnavailable)?,
                        &access,
                        file_ref,
                    )
                    .await?
                }
            };
            mounted.push(BrowserInput {
                path: item.path().to_owned(),
                content_ref,
            });
        }
        let (maximum_output_bytes, declared_outputs) = match &input.output {
            Some(output) => {
                let maximum_output_bytes = output
                    .maximum_byte_length
                    .unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);
                let output_media = MediaType::new(output.media_type.clone())
                    .map_err(|_| PythonToolError::InvalidInput)?;
                let output_contract =
                    GeneratedFileContract::for_tool(invocation.capability_key.as_str())
                        .ok_or(PythonToolError::InvalidInput)?;
                if !output_contract.accepts(&output.path, &output_media)
                    || maximum_output_bytes == 0
                    || maximum_output_bytes > GeneratedFileContract::MAXIMUM_BYTES
                {
                    return Err(PythonToolError::InvalidInput);
                }
                (
                    maximum_output_bytes,
                    vec![BrowserDeclaredOutput {
                        path: output.path.clone(),
                        media_type: output_media,
                        maximum_byte_length: NonZeroU64::new(maximum_output_bytes)
                            .ok_or(PythonToolError::InvalidInput)?,
                        required: true,
                    }],
                )
            }
            None => (1, Vec::new()),
        };

        let total_input_bytes = mounted.iter().try_fold(0_u64, |total, item| {
            total.checked_add(item.content_ref.byte_length)
        });
        let request = BrowserJobRequest {
            execution_liveness: None,
            job_id: browser_job_id(invocation.invocation_id)?,
            invocation_id: invocation.invocation_id,
            conversation_id: invocation.conversation_id,
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new(BROWSER_RUNTIME_RELEASE)
                .map_err(|_| PythonToolError::Invariant)?,
            program: BrowserJobProgram::Python {
                source_ref: source.content_ref,
                entrypoint: input.entrypoint.clone(),
                environment: if let Some(id) = &input.environment_ref {
                    let scope = crate::browser_environment::scope_key(
                        invocation.tenant_id,
                        invocation.actor.principal_id,
                        invocation.workspace_id,
                        invocation.conversation_id,
                    );
                    let id = id.parse().map_err(|_| PythonToolError::InvalidInput)?;
                    let ready = crate::browser_environment::authorized_environment(
                        self.browser.as_ref(),
                        invocation.tenant_id,
                        invocation.actor.principal_id,
                        scope,
                        id,
                    )
                    .await
                    .map_err(|error| PythonToolError::Environment(error.code))?;
                    Some(
                        crate::browser_environment::ready_binding(&ready)
                            .map_err(|error| PythonToolError::Environment(error.code))?,
                    )
                } else {
                    None
                },
                require_prepared_environment: matches!(
                    invocation.capability_key.as_str(),
                    "data.python.run@9" | "data.python.run@12"
                ),
            },
            inputs: mounted,
            declared_outputs,
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(
                    input.wall_time_millis.unwrap_or(MAX_WALL_TIME_MILLIS),
                )
                .ok_or(PythonToolError::InvalidInput)?,
                maximum_input_bytes: NonZeroU64::new(
                    total_input_bytes.ok_or(PythonToolError::InvalidInput)?,
                )
                .ok_or(PythonToolError::InvalidInput)?,
                maximum_output_bytes: NonZeroU64::new(maximum_output_bytes)
                    .ok_or(PythonToolError::InvalidInput)?,
                maximum_files: NonZeroU32::new(
                    u32::try_from(input.inputs.len() + usize::from(input.output.is_some()) + 1)
                        .map_err(|_| PythonToolError::InvalidInput)?,
                )
                .ok_or(PythonToolError::InvalidInput)?,
                maximum_stdout_bytes: NonZeroU64::new(MAX_LOG_TAIL_BYTES)
                    .ok_or(PythonToolError::Invariant)?,
                maximum_stderr_bytes: NonZeroU64::new(MAX_LOG_TAIL_BYTES)
                    .ok_or(PythonToolError::Invariant)?,
            },
            security_profile: BrowserSecurityProfile::SafeOffline,
        };
        request
            .validate()
            .map_err(|_| PythonToolError::InvalidInput)?;
        Ok(request)
    }

    async fn resource_context(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Result<ResourceCommandContext, PythonToolError> {
        // Resolve the same original execution owner used by resource creation and
        // generated output placement. Internal Dialogue Conversations are never
        // ordinary Conversation resource scopes.
        let context = self
            .resource_authority
            .resolve(ResolveResourceContext {
                actor: invocation.actor,
                conversation_id: invocation.conversation_id,
                invocation_id: Some(invocation.invocation_id),
                now: self.clock.now(),
            })
            .await
            .map_err(|error| match error {
                ResourceContextError::ScopeUnavailable => PythonToolError::ResourceFileUnavailable,
                ResourceContextError::DependencyUnavailable => {
                    PythonToolError::DependencyUnavailable
                }
                ResourceContextError::Corrupt => PythonToolError::Invariant,
            })?;
        if context.validate().is_err()
            || context.tenant_id != invocation.tenant_id
            || context.actor != invocation.actor
            || context.conversation_id() != Some(invocation.conversation_id)
            || context.workspace_id() != invocation.workspace_id
            || context
                .invocation_id
                .is_some_and(|id| id != invocation.invocation_id)
            || context
                .tool_key
                .as_ref()
                .is_some_and(|key| key.as_str() != invocation.tool_key.as_str())
        {
            return Err(PythonToolError::ResourceFileUnavailable);
        }
        Ok(context)
    }

    async fn load_file(
        &self,
        context: &ResourceCommandContext,
        file_node_id: &str,
        version: Option<u64>,
    ) -> Result<ResolvedFile, PythonToolError> {
        let node_id: ResourceNodeId = file_node_id
            .parse()
            .map_err(|_| PythonToolError::InvalidInput)?;
        let version = version
            .map(VersionNumber::new)
            .transpose()
            .map_err(|_| PythonToolError::InvalidInput)?;
        // Every read revalidates this resolved owner in the version store, so
        // revocation cannot be hidden by sharing the context within this Job.
        let file = self
            .project_files
            .resolve_version(context, node_id, version)
            .await
            .map_err(PythonToolError::from_resource_store)?;
        Ok(ResolvedFile {
            filename: file.name,
            media_type: file.content_ref.media_type.clone(),
            content_ref: file.content_ref,
        })
    }

    async fn reconcile_completed(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: CapabilityInvocation,
        output_path: Option<String>,
        result: &musteragent_capability::BrowserJobResult,
    ) -> Result<DispatchedReconciliationOutcome, PythonToolError> {
        let Some(output) = result.outputs.first() else {
            let tool_result = self
                .store_execution_result(&invocation, result, None)
                .await?;
            return Ok(DispatchedReconciliationOutcome::Provider(
                ReconciliationOutcome::ProvenSucceeded(tool_result),
            ));
        };
        let FileToolPreparation::Commit {
            result: _,
            mutation,
            resource_placement,
        } = self
            .files
            .prepare_generated(
                ArtifactToolContext {
                    execution: None,
                    invocation: invocation.clone(),
                    activity_id: material.run_context.activity_id,
                    committed_by: material.run_context.executor_participant_id,
                    now: self.clock.now(),
                },
                GeneratedFile {
                    filename: output_path.clone().ok_or(PythonToolError::InvalidResult)?,
                    content_ref: output.content_ref.clone(),
                },
            )
            .await
            .map_err(|_| PythonToolError::InvalidResult)?
        else {
            return Err(PythonToolError::Invariant);
        };
        let expose_version = !matches!(
            invocation.capability_key.as_str(),
            "data.python.run@5"
                | "data.python.run@6"
                | "data.python.run@7"
                | "data.python.run@8"
                | "data.python.run@9"
                | "data.python.run@12"
        );
        let output_view = PythonExecutionOutput {
            file_artifact_id: mutation.artifact_id().to_string(),
            file_artifact_version_id: expose_version
                .then(|| mutation.version().artifact_version_id.to_string()),
            file_node_id: resource_placement
                .as_ref()
                .ok_or(PythonToolError::Invariant)?
                .node_id
                .to_string(),
            version: expose_version.then(|| mutation.version().version.get()),
            filename: output_path.ok_or(PythonToolError::Invariant)?,
            media_type: output.content_ref.media_type.as_str().to_owned(),
            byte_length: output.content_ref.byte_length,
            sha256: output.content_ref.sha256.to_string(),
        };
        let tool_result = self
            .store_execution_result(&invocation, result, Some(output_view))
            .await?;
        let authorized_content = Vec::new();
        let committed_at = mutation.version().committed_at;
        let invocation_id = mutation
            .version()
            .source_invocation_id
            .ok_or(PythonToolError::Invariant)?;
        self.committer
            .commit(
                attempt,
                CommitPreparedArtifactTool {
                    run_context: material.run_context,
                    invocation_id,
                    result: tool_result.clone(),
                    authorized_content,
                    mutation: *mutation,
                    resource_placement: resource_placement.map(|placement| *placement),
                    resource_replacement: None,
                    committed_at,
                },
            )
            .await
            .map_err(|error| PythonToolError::Commit(error.failure()))?;
        Ok(DispatchedReconciliationOutcome::Committed(tool_result))
    }

    async fn store_execution_result(
        &self,
        invocation: &CapabilityInvocation,
        result: &musteragent_capability::BrowserJobResult,
        output: Option<PythonExecutionOutput>,
    ) -> Result<musteragent_runtime_capability::CapabilityResult, PythonToolError> {
        let receipt = PythonExecutionResult {
            status: browser_status(result.status),
            job_id: result.job_id.to_string(),
            stdout: result.stdout_tail.clone(),
            stderr: result.stderr_tail.clone(),
            diagnostics: result
                .diagnostics
                .iter()
                .map(|diagnostic| PythonExecutionDiagnostic {
                    code: diagnostic.code.clone(),
                    severity: match diagnostic.severity {
                        BrowserDiagnosticSeverity::Info => "info",
                        BrowserDiagnosticSeverity::Warning => "warning",
                        BrowserDiagnosticSeverity::Error => "error",
                    },
                    message: diagnostic.message.clone(),
                    path: diagnostic.path.clone(),
                    line: diagnostic.line.map(NonZeroU32::get),
                    column: diagnostic.column.map(NonZeroU32::get),
                })
                .collect(),
            metrics: PythonExecutionMetrics {
                wall_time_millis: result.metrics.wall_time_millis,
                input_bytes: result.metrics.input_bytes,
                output_bytes: result.metrics.output_bytes,
                output_files: result.metrics.output_files,
            },
            output,
            result_hash: result.result_hash.to_string(),
        };
        let bytes = serde_json::to_vec(&receipt).map_err(|_| PythonToolError::Invariant)?;
        self.persist_result(invocation, bytes).await
    }

    async fn persist_result(
        &self,
        invocation: &CapabilityInvocation,
        bytes: Vec<u8>,
    ) -> Result<musteragent_runtime_capability::CapabilityResult, PythonToolError> {
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation.invocation_id, "result"),
                PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| PythonToolError::Invariant)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| PythonToolError::DependencyUnavailable)?;
        Ok(musteragent_runtime_capability::CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}

impl<A, C> AtomicCapabilityHandler for BrowserPythonCapabilityHandler<A, C>
where
    A: ArtifactReader + 'static,
    C: FencedArtifactToolCommitter + 'static,
{
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        let prepare_invocation = invocation.clone();
        crate::browser_execution::BrowserExecution {
            handler: self,
            store: self.browser.as_ref(),
            clock: self.clock.as_ref(),
            attempt,
            material,
            durable,
            invocation,
            prepare: Box::pin(async move {
                self.prepare_job(attempt, material, &prepare_invocation)
                    .await
            }),
            gateway_failure: crate::browser_execution::gateway_failure,
        }
        .operation(binding)
    }

    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn retained_result_query_timeout(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(10))
    }

    fn lookup_retained_result<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        deadline: musteragent_identity::UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<crate::RetainedToolResult, LoopPortError>> {
        Box::pin(crate::browser_execution::retained_result(
            self, attempt, material, durable, deadline,
        ))
    }

    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        reconciliation_ref(invocation.invocation_id).ok()
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
            let prepared = match self.prepare_job(attempt, material, &invocation).await? {
                Ok(value) => value,
                Err(outcome) => return Ok(DispatchedCapabilityOutcome::Provider(outcome)),
            };
            match (crate::sandbox_routing::SandboxDispatch {
                clock: self.clock.as_ref(),
                attempt,
                material,
                durable,
                invocation: &invocation,
                contract: prepared.contract,
                request: prepared.request,
            })
            .enqueue(self.browser.as_ref())
            .await
            {
                Ok(_) | Err(BrowserProviderRuntimeError::DuplicateJob) => Ok(
                    DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Indeterminate {
                        diagnostic_result: None,
                        observed_error: None,
                        reason: safe_text("Python execution is running in the active browser"),
                        reconciliation_ref: reconciliation_ref(invocation.invocation_id)?,
                    }),
                ),
                Err(error) => Ok(DispatchedCapabilityOutcome::Provider(
                    crate::browser_execution::gateway_failure(error)?,
                )),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            let invocation = reconciliation_invocation(attempt, material, durable, &request);
            let expected_ref = reconciliation_ref(invocation.invocation_id)?;
            if request.reconciliation_ref != expected_ref {
                return Err(invariant());
            }
            let Some(receipt) = self
                .browser
                .load_terminal_receipt(
                    invocation.tenant_id,
                    browser_job_id(invocation.invocation_id).map_err(|_| invariant())?,
                )
                .await
                .map_err(|_| dependency())?
            else {
                return Ok(DispatchedReconciliationOutcome::Provider(
                    ReconciliationOutcome::StillIndeterminate {
                        reason: safe_text("Python execution is still running in the browser"),
                        reconciliation_ref: expected_ref,
                    },
                ));
            };
            if receipt.invocation_id != invocation.invocation_id {
                return Err(invariant());
            }
            if receipt.result.status == BrowserJobStatus::Indeterminate {
                return Ok(DispatchedReconciliationOutcome::Provider(
                    ReconciliationOutcome::StillIndeterminate {
                        reason: safe_text(
                            "Browser execution receipt is indeterminate; do not replay automatically",
                        ),
                        reconciliation_ref: expected_ref,
                    },
                ));
            }
            let output_path = self
                .request(&invocation)
                .await
                .map_err(port_from_python)?
                .output
                .map(|output| output.path);
            // The durable Browser Gateway validates every terminal result
            // against the exact persisted closed Job before returning this
            // receipt. Do not re-resolve mutable Resource nodes here: source
            // files may legitimately be renamed, versioned, or trashed after
            // dispatch without invalidating the already executed Job.
            match receipt.result.status {
                BrowserJobStatus::Completed => match self
                    .reconcile_completed(
                        attempt,
                        material,
                        invocation,
                        output_path,
                        &receipt.result,
                    )
                    .await
                {
                    Ok(outcome) => Ok(outcome),
                    Err(PythonToolError::DependencyUnavailable) => {
                        Ok(DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::StillIndeterminate {
                                reason: safe_text(
                                    "Python output is complete and awaiting durable materialization",
                                ),
                                reconciliation_ref: expected_ref,
                            },
                        ))
                    }
                    Err(PythonToolError::Commit(ArtifactToolCommitFailure::Unavailable)) => {
                        Ok(DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::StillIndeterminate {
                                reason: safe_text(
                                    "Python output is complete and awaiting durable materialization",
                                ),
                                reconciliation_ref: expected_ref,
                            },
                        ))
                    }
                    Err(PythonToolError::Commit(failure)) => {
                        crate::artifact::artifact_commit_reconciliation_failure(failure)
                    }
                    Err(_) => Ok(DispatchedReconciliationOutcome::Provider(
                        ReconciliationOutcome::Unrecoverable {
                            reason: safe_text("Python output violates the pinned file contract"),
                            reconciliation_ref: expected_ref,
                        },
                    )),
                },
                status => match self
                    .store_execution_result(&invocation, &receipt.result, None)
                    .await
                {
                    Ok(diagnostic_result) => Ok(DispatchedReconciliationOutcome::Provider(
                        ReconciliationOutcome::ProvenFailed {
                            error: browser_failure(
                                status,
                                receipt.result.job_id,
                                &receipt.result.diagnostics,
                            )?,
                            retry_disposition: RetryDisposition::DoNotRetry,
                            diagnostic_result: Some(diagnostic_result),
                        },
                    )),
                    Err(PythonToolError::DependencyUnavailable) => {
                        Ok(DispatchedReconciliationOutcome::Provider(
                            ReconciliationOutcome::StillIndeterminate {
                                reason: safe_text(
                                    "Python failed and its execution receipt is awaiting durable storage",
                                ),
                                reconciliation_ref: expected_ref,
                            },
                        ))
                    }
                    Err(_) => Ok(DispatchedReconciliationOutcome::Provider(
                        ReconciliationOutcome::Unrecoverable {
                            reason: safe_text(
                                "Python execution receipt failed integrity validation",
                            ),
                            reconciliation_ref: expected_ref,
                        },
                    )),
                },
            }
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonRunInput {
    source: PythonSourceFile,
    environment_ref: Option<String>,
    #[serde(default = "python_entrypoint")]
    entrypoint: String,
    #[serde(default)]
    inputs: Vec<PythonInputFile>,
    output: Option<PythonOutputFile>,
    #[serde(default)]
    wall_time_millis: Option<u64>,
}

fn python_entrypoint() -> String {
    "main.py".to_owned()
}

impl PythonRunInput {
    fn validate(&self) -> Result<(), PythonToolError> {
        if self.entrypoint != "main.py"
            || self.inputs.len() > MAX_INPUT_FILES
            || self
                .wall_time_millis
                .is_some_and(|value| value == 0 || value > MAX_WALL_TIME_MILLIS)
        {
            return Err(PythonToolError::InvalidInput);
        }
        self.source.validate()?;
        let mut paths = std::collections::BTreeSet::from([self.entrypoint.to_lowercase()]);
        for input in &self.inputs {
            input.validate()?;
            if !paths.insert(input.path().to_lowercase()) {
                return Err(PythonToolError::InvalidInput);
            }
        }
        if let Some(output) = &self.output {
            validate_path(&output.path)?;
            if paths.contains(&output.path.to_lowercase()) {
                return Err(PythonToolError::InvalidInput);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonSourceFile {
    file_node_id: String,
    #[serde(default)]
    version: Option<u64>,
}

impl PythonSourceFile {
    fn validate(&self) -> Result<(), PythonToolError> {
        self.file_node_id
            .parse::<ResourceNodeId>()
            .map_err(|_| PythonToolError::InvalidInput)?;
        self.version
            .map(VersionNumber::new)
            .transpose()
            .map_err(|_| PythonToolError::InvalidInput)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonOutputFile {
    path: String,
    media_type: String,
    #[serde(default)]
    maximum_byte_length: Option<u64>,
}

#[derive(Debug, Serialize)]
struct PythonExecutionResult {
    status: &'static str,
    job_id: String,
    stdout: String,
    stderr: String,
    diagnostics: Vec<PythonExecutionDiagnostic>,
    metrics: PythonExecutionMetrics,
    output: Option<PythonExecutionOutput>,
    result_hash: String,
}

#[derive(Debug, Serialize)]
struct PythonExecutionDiagnostic {
    code: String,
    severity: &'static str,
    message: String,
    path: Option<String>,
    line: Option<u32>,
    column: Option<u32>,
}

#[derive(Debug, Serialize)]
struct PythonExecutionMetrics {
    wall_time_millis: u64,
    input_bytes: u64,
    output_bytes: u64,
    output_files: u32,
}

#[derive(Debug, Serialize)]
struct PythonExecutionOutput {
    file_node_id: String,
    file_artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_artifact_version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    filename: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

const fn browser_status(status: BrowserJobStatus) -> &'static str {
    match status {
        BrowserJobStatus::Completed => "completed",
        BrowserJobStatus::Failed => "failed",
        BrowserJobStatus::Cancelled => "cancelled",
        BrowserJobStatus::ResourceExceeded => "resource_exceeded",
        BrowserJobStatus::Indeterminate => "indeterminate",
    }
}

struct ResolvedFile {
    filename: String,
    media_type: MediaType,
    content_ref: musteragent_content_ref::ContentRef,
}

#[derive(Debug, Clone)]
enum PythonToolError {
    Environment(ErrorCode),
    InvalidInput,
    ResourceFileUnavailable,
    DependencyUnavailable,
    InvalidResult,
    Invariant,
    Commit(ArtifactToolCommitFailure),
}

impl PythonToolError {
    const fn from_resource_store(value: ResourceStoreError) -> Self {
        match value {
            ResourceStoreError::NodeUnavailable | ResourceStoreError::FileUnavailable => {
                Self::ResourceFileUnavailable
            }
            ResourceStoreError::WriterBusy | ResourceStoreError::DependencyUnavailable => {
                Self::DependencyUnavailable
            }
            ResourceStoreError::Corrupt
            | ResourceStoreError::PrivateTaskResult
            | ResourceStoreError::BoundsExceeded
            | ResourceStoreError::RevisionConflict
            | ResourceStoreError::ParentUnavailable
            | ResourceStoreError::NameConflict
            | ResourceStoreError::MoveCycle => Self::Invariant,
        }
    }
}

fn failure(error: PythonToolError) -> Result<CapabilityOutcome, LoopPortError> {
    let (code, message, retryable) = match error {
        PythonToolError::Commit(_) => return Err(invariant()),
        PythonToolError::Environment(code) => (
            code,
            match code {
                ErrorCode::CapabilityUnavailable => {
                    "The selected environment is not ready. Inspect its original installation operation and wait for ready before execution. User code was not dispatched."
                }
                ErrorCode::ResourceFileUnavailable => {
                    "The environment_ref does not exist. Prepare an environment and use the exact returned reference. User code was not dispatched."
                }
                ErrorCode::PolicyDenied => {
                    "The environment belongs to another principal or project. Prepare an environment in the current scope. User code was not dispatched."
                }
                ErrorCode::ContentIntegrityFailed => {
                    "The environment installation receipt failed integrity verification. Inspect the original operation. User code was not dispatched."
                }
                _ => {
                    "Environment authority is temporarily unavailable. Inspect the original operation before retrying. User code was not dispatched."
                }
            },
            false,
        ),
        PythonToolError::InvalidInput => (
            ErrorCode::InvalidCommand,
            "Python Tool input violates its pinned contract",
            false,
        ),
        PythonToolError::ResourceFileUnavailable => (
            ErrorCode::ResourceFileUnavailable,
            "A Python source or input file is unavailable at the requested version",
            false,
        ),
        PythonToolError::DependencyUnavailable => (
            ErrorCode::DependencyUnavailable,
            "Python execution storage is temporarily unavailable",
            true,
        ),
        PythonToolError::InvalidResult | PythonToolError::Invariant => (
            ErrorCode::ContentIntegrityFailed,
            "Python execution failed Runtime integrity validation",
            false,
        ),
    };
    Ok(CapabilityOutcome::Failed {
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
    })
}

fn browser_failure(
    status: BrowserJobStatus,
    job_id: BrowserJobId,
    diagnostics: &[musteragent_capability::BrowserDiagnostic],
) -> Result<ErrorInfo, LoopPortError> {
    let (code, message) = match status {
        BrowserJobStatus::Failed
            if diagnostics
                .iter()
                .any(|d| d.code == "content_integrity_failed") =>
        {
            (
                ErrorCode::ContentIntegrityFailed,
                "Python input bytes failed integrity verification; check content delivery transformations before retrying",
            )
        }
        BrowserJobStatus::Failed
            if diagnostics
                .iter()
                .any(|d| d.code == "browser_output_staging_failed") =>
        {
            (
                ErrorCode::DependencyUnavailable,
                "Python execution completed, but its output could not be stored; check output delivery before retrying execution",
            )
        }
        BrowserJobStatus::Failed => (
            ErrorCode::InvalidCommand,
            "Python script failed; inspect the browser execution diagnostics",
        ),
        BrowserJobStatus::Cancelled => (
            ErrorCode::DependencyTimeout,
            "Python execution was cancelled before completion",
        ),
        BrowserJobStatus::ResourceExceeded => (
            ErrorCode::QuotaExceeded,
            "Python execution exceeded its declared browser resource limits",
        ),
        BrowserJobStatus::Completed | BrowserJobStatus::Indeterminate => return Err(invariant()),
    };
    Ok(ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new(message).map_err(|_| invariant())?,
        retryable: false,
        dependency_ref: None,
        diagnostic_ref: Some(
            OpaqueDiagnosticRef::new(format!("browser-job:{job_id}")).map_err(|_| invariant())?,
        ),
    })
}

fn browser_job_id(invocation_id: InvocationId) -> Result<BrowserJobId, PythonToolError> {
    BrowserJobId::from_uuid(invocation_id.into_uuid()).map_err(|_| PythonToolError::Invariant)
}

fn reconciliation_ref(invocation_id: InvocationId) -> Result<OpaqueExternalRef, LoopPortError> {
    OpaqueExternalRef::new(format!("browser-job:{invocation_id}")).map_err(|_| invariant())
}

fn validate_path(value: &str) -> Result<(), PythonToolError> {
    let valid = !value.is_empty()
        && value.len() <= 512
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
    if valid {
        Ok(())
    } else {
        Err(PythonToolError::InvalidInput)
    }
}

fn reconciliation_invocation(
    attempt: &AttemptContext,
    material: &ToolExecutionMaterial,
    durable: &DurableToolInvocation,
    request: &CapabilityReconciliation,
) -> CapabilityInvocation {
    CapabilityInvocation {
        actor: material.actor,
        tenant_id: material.run_context.tenant_id,
        conversation_id: material.run_context.conversation_id,
        run_id: material.run_context.run_id,
        requested_by_attempt_id: durable.invocation.requested_by_attempt_id,
        execution_attempt_id: attempt.attempt_id,
        execution_attempt_number: attempt.attempt_number,
        invocation_id: durable.invocation.invocation_id,
        tool_call_id: durable.invocation.tool_call_id.clone(),
        tool_key: durable.invocation.tool_key.clone(),
        capability_key: material.capability_grant.key.clone(),
        capability_provider_id: durable.invocation.capability_provider_id,
        idempotency_key: durable.invocation.idempotency_key.clone(),
        request_ref: durable.invocation.request_ref.clone(),
        request_schema: material.tool.manifest.input_schema.clone(),
        result_schema: material.tool.manifest.output_schema.clone(),
        authorized_content: material.authorized_content.clone(),
        workspace_id: request.workspace_id,
        timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: request.deadline },
        trace_context: request.trace_context.clone(),
        execution_grant_proof: material.execution_grant_proof.clone(),
    }
}

fn port_from_python(error: PythonToolError) -> LoopPortError {
    match error {
        PythonToolError::Environment(code) => LoopPortError {
            code,
            retryable: false,
        },
        PythonToolError::DependencyUnavailable => dependency(),
        PythonToolError::InvalidInput | PythonToolError::ResourceFileUnavailable => LoopPortError {
            code: ErrorCode::InvalidCommand,
            retryable: false,
        },
        PythonToolError::InvalidResult
        | PythonToolError::Invariant
        | PythonToolError::Commit(_) => invariant(),
    }
}

const fn dependency() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::DependencyUnavailable,
        retryable: true,
    }
}

const fn invariant() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    }
}

fn python_provider_contract(key: &str) -> Result<BrowserProviderContract, PythonToolError> {
    match key {
        "data.python.run@9" | "data.python.run@12" => {
            Ok(BrowserProviderContract::PythonExecutePrepared)
        }
        "data.python.run@3" => Ok(BrowserProviderContract::PythonExecute),
        "data.python.run@4" | "data.python.run@5" | "data.python.run@6" | "data.python.run@7"
        | "data.python.run@8" => Ok(BrowserProviderContract::PythonExecuteWorkingTree),
        _ => Err(PythonToolError::InvalidInput),
    }
}

fn safe_text(value: &'static str) -> SafeDisplayText {
    SafeDisplayText::new(value).expect("static browser display text is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_contract_routes_only_to_a_compatible_runner() {
        assert!(matches!(
            python_provider_contract("data.python.run@3"),
            Ok(BrowserProviderContract::PythonExecute)
        ));
        assert!(matches!(
            python_provider_contract("data.python.run@4"),
            Ok(BrowserProviderContract::PythonExecuteWorkingTree)
        ));
        assert!(matches!(
            python_provider_contract("data.python.run@5"),
            Ok(BrowserProviderContract::PythonExecuteWorkingTree)
        ));
    }

    #[test]
    fn browser_failure_preserves_input_integrity_failure() {
        let error = browser_failure(
            BrowserJobStatus::Failed,
            BrowserJobId::generate(),
            &[musteragent_capability::BrowserDiagnostic {
                code: "content_integrity_failed".to_owned(),
                severity: BrowserDiagnosticSeverity::Error,
                message: "untrusted diagnostic text".to_owned(),
                path: None,
                line: None,
                column: None,
            }],
        )
        .expect("failure");
        assert_eq!(error.code, ErrorCode::ContentIntegrityFailed);
        assert!(!error.retryable);
    }

    #[test]
    fn browser_output_delivery_failure_preserves_completed_execution() {
        let error = browser_failure(
            BrowserJobStatus::Failed,
            BrowserJobId::generate(),
            &[musteragent_capability::BrowserDiagnostic {
                code: "browser_output_staging_failed".to_owned(),
                severity: BrowserDiagnosticSeverity::Error,
                message: "untrusted diagnostic text".to_owned(),
                path: None,
                line: None,
                column: None,
            }],
        )
        .expect("failure");
        assert_eq!(error.code, ErrorCode::DependencyUnavailable);
        assert!(error.safe_message.as_str().contains("execution completed"));
        assert!(!error.safe_message.as_str().contains("untrusted"));
        assert!(!error.retryable);
    }

    #[test]
    fn python_v3_accepts_only_explicit_resource_file_identities() {
        let input: PythonRunInput = serde_json::from_value(serde_json::json!({
            "source": {
                "file_node_id": "01982f80-0000-7000-8000-000000000001",
                "version": 3
            },
            "entrypoint": "main.py",
            "inputs": [{
                "file_node_id": "01982f80-0000-7000-8000-000000000002",
                "version": 5,
                "path": "data/input.csv"
            }]
        }))
        .expect("Python v3 input");
        assert!(input.validate().is_ok());

        assert!(
            serde_json::from_value::<PythonRunInput>(serde_json::json!({
                "source_file_id": "01982f80-0000-7000-8000-000000000001",
                "source_version": 3,
                "entrypoint": "main.py"
            }))
            .is_err()
        );
    }

    #[test]
    fn resource_lookup_failures_preserve_retry_semantics() {
        assert!(matches!(
            PythonToolError::from_resource_store(ResourceStoreError::FileUnavailable),
            PythonToolError::ResourceFileUnavailable
        ));
        assert!(matches!(
            PythonToolError::from_resource_store(ResourceStoreError::DependencyUnavailable),
            PythonToolError::DependencyUnavailable
        ));
        assert!(matches!(
            PythonToolError::from_resource_store(ResourceStoreError::Corrupt),
            PythonToolError::Invariant
        ));
    }
}
