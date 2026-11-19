//! Persistent Python execution delegates one complete selected filesystem to
//! the credential-free Runner and commits its verified result through Resource.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};
use musteragent_capability::{
    BrowserInput, BrowserJobEngine, BrowserJobProgram, BrowserJobRequest, BrowserJobStatus,
    BrowserProviderContract, BrowserResourceLimits, BrowserSecurityProfile, CapabilityManifest,
};
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{BrowserJobId, InvocationId, VersionedKey};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::*;
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_resource::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use crate::browser_workspace_feedback::PreparationFailure;

const MAXIMUM_ENTRIES: usize = 2048;
const MAXIMUM_BYTES: u64 = 256 * 1024 * 1024;

pub struct BrowserWorkspaceCapabilityHandler<C: FencedResourceToolCommitter> {
    manifest: CapabilityManifest,
    workspaces: Arc<dyn ExecutionWorkspacePreparationStore>,
    file_queries: Arc<musteragent_runtime_file_query::FileQueryService>,
    content: Arc<dyn ContentStore>,
    browser: Arc<dyn BrowserGatewayStore>,
    committer: Arc<C>,
    clock: Arc<dyn RuntimeClock>,
    unavailable_notice: crate::BrowserUnavailableNotice,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteInput {
    code: Option<String>,
    script: Option<String>,
    cwd: Option<String>,
    inputs: Option<Vec<crate::browser_workspace_inputs::WorkspaceInput>>,
    project: Option<ProjectMountInput>,
    #[serde(default)]
    unmount_project: bool,
    environment_ref: Option<String>,
    #[serde(default)]
    reset_environment: bool,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectMountInput {
    root_node_id: Option<String>,
    #[serde(default)]
    writable: bool,
}
impl ExecuteInput {
    fn validate(&self) -> Result<(), LoopPortError> {
        if self.code.is_some() == self.script.is_some()
            || self
                .code
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 64 * 1024)
            || self
                .script
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 512 || !s.ends_with(".py"))
            || (self.unmount_project && self.project.is_some())
            || (self.reset_environment && self.environment_ref.is_some())
        {
            return Err(port(ErrorCode::InvalidCommand));
        }
        Ok(())
    }
}

impl<C: FencedResourceToolCommitter> BrowserWorkspaceCapabilityHandler<C> {
    pub fn new(
        manifest: CapabilityManifest,
        workspaces: Arc<dyn ExecutionWorkspacePreparationStore>,
        content: Arc<dyn ContentStore>,
        browser: Arc<dyn BrowserGatewayStore>,
        committer: Arc<C>,
        clock: Arc<dyn RuntimeClock>,
        file_queries: Arc<musteragent_runtime_file_query::FileQueryService>,
    ) -> Self {
        Self {
            manifest,
            workspaces,
            content,
            browser,
            committer,
            clock,
            file_queries,
            unavailable_notice: crate::BrowserUnavailableNotice::default(),
        }
    }

    /// Host-configured text for a retryable `browser_execution_unavailable`
    /// failure; defaults to [`crate::BrowserUnavailableNotice::DEFAULT_TEXT`].
    #[must_use]
    pub fn with_unavailable_notice(mut self, notice: crate::BrowserUnavailableNotice) -> Self {
        self.unavailable_notice = notice;
        self
    }
    async fn read(&self, invocation: &CapabilityInvocation) -> Result<ExecuteInput, LoopPortError> {
        if invocation.capability_key.as_str() != "data.python.run@11" {
            return Err(port(ErrorCode::InvalidCommand));
        }
        let bytes = self
            .read_content(invocation, &invocation.request_ref)
            .await?;
        let input: ExecuteInput =
            serde_json::from_slice(&bytes).map_err(|_| port(ErrorCode::InvalidCommand))?;
        input.validate()?;
        Ok(input)
    }
    async fn read_content(
        &self,
        invocation: &CapabilityInvocation,
        reference: &ContentRef,
    ) -> Result<Vec<u8>, LoopPortError> {
        let bytes = self
            .content
            .read(invocation.tenant_id, reference)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
        verify_content(reference, &bytes).map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        Ok(bytes)
    }
    async fn put(
        &self,
        invocation: &CapabilityInvocation,
        media: &str,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, LoopPortError> {
        self.content
            .put(PutContent {
                tenant_id: invocation.tenant_id,
                media_type: MediaType::new(media)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
                bytes,
            })
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))
    }
    fn context(&self, invocation: &CapabilityInvocation) -> ResourceCommandContext {
        ResourceCommandContext {
            tenant_id: invocation.tenant_id,
            actor: invocation.actor,
            source: ResourceCommandSource::Conversation {
                conversation_id: invocation.conversation_id,
                workspace_id: invocation.workspace_id,
            },
            invocation_id: Some(invocation.invocation_id),
            tool_key: Some(
                VersionedKey::new(invocation.tool_key.as_str()).expect("validated Tool key"),
            ),
            now: self.clock.now(),
        }
    }
    async fn snapshot(
        &self,
        attempt: &AttemptContext,
        invocation: &CapabilityInvocation,
        input: &ExecuteInput,
    ) -> Result<ExecutionWorkspaceSnapshot, PreparationFailure> {
        let environment = match input.environment_ref.as_ref() {
            Some(id) => ExecutionWorkspaceEnvironmentChange::Select(
                id.parse().map_err(|_| port(ErrorCode::InvalidCommand))?,
            ),
            None if input.reset_environment => ExecutionWorkspaceEnvironmentChange::Clear,
            None => ExecutionWorkspaceEnvironmentChange::Keep,
        };
        // Validate an explicitly selected environment before changing its durable selection.
        if let ExecutionWorkspaceEnvironmentChange::Select(id) = environment {
            self.environment(invocation, id).await?;
        }
        let selection_ref = match &input.inputs {
            Some(inputs) => Some(
                self.put(
                    invocation,
                    "application/json",
                    serde_json::to_vec(inputs).map_err(|_| port(ErrorCode::InvalidCommand))?,
                )
                .await?,
            ),
            None => None,
        };
        self.workspaces
            .prepare_execution(PrepareExecutionWorkspace {
                context: self.context(invocation),
                attempt: attempt.clone(),
                request_fingerprint: invocation.request_ref.sha256,
                maximum_entries: MAXIMUM_ENTRIES - 2,
                maximum_total_bytes: MAXIMUM_BYTES,
                cwd: input
                    .cwd
                    .as_ref()
                    .map(|s| s.strip_prefix("/workspace/").unwrap_or(s).to_owned()),
                environment,
                selection_ref,
                project: match &input.project {
                    Some(p) => ExecutionWorkspaceProjectChange::Select {
                        root_node_id: p
                            .root_node_id
                            .as_ref()
                            .map(|id| id.parse())
                            .transpose()
                            .map_err(|_| port(ErrorCode::InvalidCommand))?,
                        writable: p.writable,
                    },
                    None if input.unmount_project => ExecutionWorkspaceProjectChange::Clear,
                    None => ExecutionWorkspaceProjectChange::Keep,
                },
            })
            .await
            .map_err(PreparationFailure::from)
    }

    async fn environment(
        &self,
        invocation: &CapabilityInvocation,
        id: BrowserJobId,
    ) -> Result<musteragent_capability::BrowserPythonEnvironmentBinding, LoopPortError> {
        let scope = crate::browser_environment::scope_key(
            invocation.tenant_id,
            invocation.actor.principal_id,
            invocation.workspace_id,
            invocation.conversation_id,
        );
        let ready = crate::browser_environment::authorized_environment(
            self.browser.as_ref(),
            invocation.tenant_id,
            invocation.actor.principal_id,
            scope,
            id,
        )
        .await?;
        crate::browser_environment::ready_binding(&ready)
    }
    async fn build(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<BrowserJobRequest, PreparationFailure> {
        let input = self.read(invocation).await?;
        let snapshot = self.snapshot(attempt, invocation, &input).await?;
        let mut manifest = WorkspaceManifest::from_snapshot(&snapshot, snapshot.cwd.clone())
            .map_err(|_| port(ErrorCode::InvalidCommand))?;
        let mut inputs: Vec<_> = snapshot
            .entries
            .iter()
            .filter_map(|entry| {
                entry.file.as_ref().map(|file| BrowserInput {
                    path: format!("work/{}", entry.path),
                    content_ref: file.content_ref.clone(),
                })
            })
            .collect();
        if let Some(project) = &snapshot.project {
            inputs.extend(project.entries.iter().filter_map(|entry| {
                entry.file.as_ref().map(|file| BrowserInput {
                    path: format!("project/{}", entry.path),
                    content_ref: file.content_ref.clone(),
                })
            }));
        }
        if let Some(reference) = &snapshot.selection_ref {
            let selected: Vec<crate::browser_workspace_inputs::WorkspaceInput> =
                serde_json::from_slice(&self.read_content(invocation, reference).await?)
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
            let access = musteragent_runtime_file_query::FileQueryAccess {
                actor: invocation.actor,
                conversation_id: Some(invocation.conversation_id),
                workspace_id: None,
                run: Some(musteragent_runtime_file_query::RunFileQueryAccess {
                    invocation_id: Some(invocation.invocation_id),
                    context: material.run_context,
                    attempt: attempt.clone(),
                    authorized_content: invocation.authorized_content.clone(),
                    timing: invocation.timing.clone(),
                }),
            };
            crate::browser_workspace_inputs::mount(
                self.file_queries.as_ref(),
                &access,
                &selected,
                &mut manifest,
                &mut inputs,
            )
            .await?;
        }
        let (source_ref, entrypoint) = if let Some(code) = input.code {
            (
                self.put(invocation, "text/x-python", code.into_bytes())
                    .await?,
                "main.py".to_owned(),
            )
        } else {
            let selected = resolve_path(
                &snapshot.cwd,
                input
                    .script
                    .as_deref()
                    .ok_or_else(|| port(ErrorCode::InvalidCommand))?,
            )?;
            let reference = inputs
                .iter()
                .find(|file| file.path == selected)
                .map(|file| file.content_ref.clone())
                .ok_or_else(|| port(ErrorCode::ResourceFileUnavailable))?;
            (reference, selected)
        };
        manifest
            .validate()
            .map_err(|_| port(ErrorCode::InvalidCommand))?;
        let workspace_ref = self
            .put(
                invocation,
                "application/json",
                serde_json::to_vec(&manifest)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
            )
            .await?;
        inputs.push(BrowserInput {
            path: "__program__.py".into(),
            content_ref: source_ref.clone(),
        });
        inputs.push(BrowserInput {
            path: "__workspace__.json".into(),
            content_ref: workspace_ref.clone(),
        });
        let total = inputs
            .iter()
            .try_fold(0_u64, |sum, f| sum.checked_add(f.content_ref.byte_length))
            .ok_or_else(|| port(ErrorCode::QuotaExceeded))?;
        if total > MAXIMUM_BYTES {
            return Err(port(ErrorCode::QuotaExceeded).into());
        }
        let environment = match snapshot.environment_ref {
            Some(id) => Some(self.environment(invocation, id).await.map_err(|error| PreparationFailure {
                code: error.code,
                message: "The selected Python environment is not ready or no longer authorized. No user code was dispatched. Inspect or prepare the required environment and select its ready reference; reset_environment is valid only if the script does not need those packages.",
            })?),
            None => None,
        };
        let environment_manifest_ref = if let Some(id) = snapshot.environment_ref {
            self.browser
                .load_job(invocation.tenant_id, id)
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable))?
                .and_then(|j| j.delivery.terminal_result)
                .and_then(|r| {
                    r.outputs
                        .into_iter()
                        .find(|o| o.path == "environment-lock.json")
                })
                .map(|o| o.content_ref)
        } else {
            None
        };
        if let Some(reference) = &environment_manifest_ref {
            inputs.push(BrowserInput {
                path: "__environment__.json".into(),
                content_ref: reference.clone(),
            });
        }
        let request = BrowserJobRequest {
            execution_liveness: None,
            job_id: job_id(invocation.invocation_id),
            invocation_id: invocation.invocation_id,
            conversation_id: invocation.conversation_id,
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new("browser-runtime@2")
                .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
            program: BrowserJobProgram::PythonWorkspace {
                execution_workspace_id: snapshot.binding.execution_workspace_id,
                source_ref,
                workspace_ref,
                entrypoint,
                environment,
                environment_manifest_ref,
            },
            inputs,
            declared_outputs: vec![],
            limits: BrowserResourceLimits {
                wall_time_millis: nonzero(120_000),
                maximum_input_bytes: nonzero(MAXIMUM_BYTES),
                maximum_output_bytes: nonzero(MAXIMUM_BYTES),
                maximum_files: NonZeroU32::new(MAXIMUM_ENTRIES as u32 + 2).expect("static bound"),
                maximum_stdout_bytes: nonzero(50 * 1024),
                maximum_stderr_bytes: nonzero(50 * 1024),
            },
            security_profile: BrowserSecurityProfile::SafeOffline,
        };
        request
            .validate()
            .map_err(|_| port(ErrorCode::InvalidCommand))?;
        Ok(request)
    }
    async fn prepare(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<crate::browser_execution::BrowserPreparation, LoopPortError> {
        Ok(match self.build(attempt, material, invocation).await {
            Ok(request) => Ok(crate::browser_execution::PreparedBrowserJob {
                contract: BrowserProviderContract::PythonExecuteWorkspace,
                request,
            }),
            Err(error) => Err(failed(error.code, error.message, None)),
        })
    }
    async fn verified_candidate(
        &self,
        invocation: &CapabilityInvocation,
        receipt: &BrowserJobTerminalReceipt,
    ) -> Result<
        (
            WorkspaceManifest,
            WorkspaceDelta,
            BTreeMap<String, ContentRef>,
        ),
        LoopPortError,
    > {
        let original = self
            .browser
            .load_job(invocation.tenant_id, receipt.job_id)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?
            .ok_or_else(|| port(ErrorCode::ContentIntegrityFailed))?;
        let BrowserJobProgram::PythonWorkspace { workspace_ref, .. } =
            &original.delivery.request.program
        else {
            return Err(port(ErrorCode::ContentIntegrityFailed));
        };
        let baseline: WorkspaceManifest =
            serde_json::from_slice(&self.read_content(invocation, workspace_ref).await?)
                .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        let delta_output = receipt
            .result
            .outputs
            .iter()
            .find(|f| f.path == WORKSPACE_DELTA_PATH)
            .ok_or_else(|| port(ErrorCode::ContentIntegrityFailed))?;
        let delta = WorkspaceDelta::parse(
            &self
                .read_content(invocation, &delta_output.content_ref)
                .await?,
        )
        .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        let outputs: BTreeMap<_, _> = receipt
            .result
            .outputs
            .iter()
            .filter(|f| f.path != WORKSPACE_DELTA_PATH)
            .map(|f| (f.path.clone(), f.content_ref.clone()))
            .collect();
        delta
            .validate_against(&baseline, &outputs)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        Ok((baseline, delta, outputs))
    }
    async fn result(
        &self,
        invocation: &CapabilityInvocation,
        result: &musteragent_capability::BrowserJobResult,
        status: &str,
        delta: Option<&WorkspaceDelta>,
    ) -> Result<CapabilityResult, LoopPortError> {
        #[derive(Serialize)]
        struct ResultView<'a> {
            status: &'a str,
            job_id: String,
            stdout: &'a str,
            stderr: &'a str,
            candidate_available: bool,
            files: Vec<FileView<'a>>,
            diagnostics: Vec<&'a str>,
        }
        #[derive(Serialize)]
        struct FileView<'a> {
            path: &'a str,
            kind: &'a str,
            byte_length: Option<u64>,
        }
        let view = ResultView {
            status,
            job_id: result.job_id.to_string(),
            stdout: &result.stdout_tail,
            stderr: &result.stderr_tail,
            candidate_available: !result.outputs.is_empty() && status != "committed",
            files: delta
                .map(|d| {
                    d.entries
                        .iter()
                        .filter(|e| e.path.contains('/') && !e.read_only)
                        .map(|e| FileView {
                            path: &e.path,
                            kind: if e.kind == WorkspaceEntryKind::File {
                                "file"
                            } else {
                                "directory"
                            },
                            byte_length: e.byte_length,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            diagnostics: result
                .diagnostics
                .iter()
                .map(|d| d.message.as_str())
                .collect(),
        };
        let result_ref = self
            .put(
                invocation,
                "application/json",
                serde_json::to_vec(&view)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
            )
            .await?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}

impl<C> AtomicCapabilityHandler for BrowserWorkspaceCapabilityHandler<C>
where
    C: FencedResourceToolCommitter + 'static,
    C::Error: ClassifyResourceToolCommitError,
{
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
        let preparing = invocation.clone();
        crate::browser_execution::BrowserExecution {
            handler: self,
            store: self.browser.as_ref(),
            clock: self.clock.as_ref(),
            attempt,
            material,
            durable,
            invocation,
            prepare: Box::pin(async move { self.prepare(attempt, material, &preparing).await }),
            gateway_failure: crate::browser_execution::gateway_failure,
            unavailable_notice: &self.unavailable_notice,
        }
        .operation(binding)
    }
    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        Some(reference(invocation.invocation_id))
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
            let prepared = match self.prepare(attempt, material, &invocation).await? {
                Ok(p) => p,
                Err(e) => return Ok(DispatchedCapabilityOutcome::Provider(e)),
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
                        reason: text("Workspace execution is running"),
                        reconciliation_ref: reference(invocation.invocation_id),
                    }),
                ),
                Err(_) => Ok(DispatchedCapabilityOutcome::Provider(failed(
                    ErrorCode::DependencyUnavailable,
                    "Browser execution provider is unavailable",
                    None,
                ))),
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
            let invocation = crate::reconciliation::reconciliation_invocation(
                attempt, material, durable, &request,
            );
            let identity = reference(invocation.invocation_id);
            if request.reconciliation_ref != identity {
                return Err(port(ErrorCode::InvalidCommand));
            }
            let Some(receipt) = self
                .browser
                .load_terminal_receipt(invocation.tenant_id, job_id(invocation.invocation_id))
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable))?
            else {
                return Ok(pending(
                    identity,
                    "Workspace execution is waiting for its original receipt",
                ));
            };
            if receipt.invocation_id != invocation.invocation_id {
                return Err(port(ErrorCode::ContentIntegrityFailed));
            }
            if receipt.result.status == BrowserJobStatus::Indeterminate {
                return Ok(pending(
                    identity,
                    "Execution outcome is unknown; do not rerun user code",
                ));
            }
            if receipt.result.status != BrowserJobStatus::Completed {
                let diagnostic = self
                    .result(&invocation, &receipt.result, "failed", None)
                    .await?;
                return Ok(proven_failed(
                    ErrorCode::InvalidCommand,
                    "Workspace execution did not complete; inspect its diagnostics",
                    diagnostic,
                ));
            }
            let (baseline, delta, outputs) = match self
                .verified_candidate(&invocation, &receipt)
                .await
            {
                Ok(candidate) => candidate,
                Err(error) if error.code == ErrorCode::DependencyUnavailable => {
                    return Ok(pending(
                        identity,
                        "Execution has a terminal receipt; candidate validation is waiting for storage. Recover this original receipt without rerunning user code",
                    ));
                }
                Err(error) => {
                    let diagnostic = self
                        .result(&invocation, &receipt.result, "failed", None)
                        .await?;
                    return Ok(proven_failed(
                        error.code,
                        "Execution finished, but its candidate failed integrity validation before commit. No workspace changes were published. Original staged outputs are retained for diagnosis; repair the provider protocol before a new execution. Do not replay the original invocation",
                        diagnostic,
                    ));
                }
            };
            if delta.execution == WorkspaceExecutionOutcome::Failed {
                let diagnostic = self
                    .result(
                        &invocation,
                        &receipt.result,
                        "failed_candidate",
                        Some(&delta),
                    )
                    .await?;
                return Ok(proven_failed(
                    ErrorCode::InvalidCommand,
                    "Python failed; its candidate files are retained separately and no work files were published",
                    diagnostic,
                ));
            }
            let input = self.read(&invocation).await?;
            let snapshot = self
                .snapshot(attempt, &invocation, &input)
                .await
                .map_err(|error| port(error.code))?;
            let mut context = self.context(&invocation);
            context.now = receipt.sealed_at;
            context.source = ResourceCommandSource::ExecutionWorkspace(snapshot.binding);
            let planned = plan_execution_workspace_changes(
                context.clone(),
                material.run_context,
                &snapshot,
                &baseline,
                &delta,
                &outputs,
            );
            let command = match planned {
                Ok(command) => command,
                Err(_) => {
                    let diagnostic = self
                        .result(
                            &invocation,
                            &receipt.result,
                            "failed_candidate",
                            Some(&delta),
                        )
                        .await?;
                    return Ok(proven_failed(
                        ErrorCode::ContentIntegrityFailed,
                        "Execution finished, but its candidate cannot form a valid atomic workspace change. Commit was not attempted; original files are unchanged and candidate files are retained. Repair the workspace protocol before another execution",
                        diagnostic,
                    ));
                }
            };
            let result = self
                .result(&invocation, &receipt.result, "committed", Some(&delta))
                .await?;
            let Some(command) = command else {
                return Ok(DispatchedReconciliationOutcome::Provider(
                    ReconciliationOutcome::ProvenSucceeded(result),
                ));
            };
            match self
                .committer
                .commit(
                    attempt,
                    CommitPreparedResourceTool {
                        run_context: material.run_context,
                        invocation_id: invocation.invocation_id,
                        result: result.clone(),
                        mutation: command,
                        committed_at: context.now,
                    },
                )
                .await
            {
                Ok(_) => Ok(DispatchedReconciliationOutcome::Committed(result)),
                Err(error) => match error.failure() {
                    ResourceToolCommitFailure::Unavailable => Ok(pending(
                        identity,
                        "Candidate files are retained; original commit is awaiting storage",
                    )),
                    ResourceToolCommitFailure::Conflict
                    | ResourceToolCommitFailure::NameConflict => {
                        let diagnostic = self
                            .result(
                                &invocation,
                                &receipt.result,
                                "conflict_candidate",
                                Some(&delta),
                            )
                            .await?;
                        Ok(proven_failed(
                            ErrorCode::ResourceRevisionConflict,
                            "Work files changed concurrently; candidate files are retained without overwriting the current versions",
                            diagnostic,
                        ))
                    }
                    _ => Err(port(ErrorCode::PolicyDenied)),
                },
            }
        })
    }
}

fn resolve_path(cwd: &str, path: &str) -> Result<String, LoopPortError> {
    let joined = if let Some(path) = path.strip_prefix("/workspace/") {
        path.to_owned()
    } else if path.starts_with('/') {
        return Err(port(ErrorCode::InvalidCommand));
    } else {
        format!("{cwd}/{path}")
    };
    let mut parts = Vec::new();
    for part in joined.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(port(ErrorCode::InvalidCommand));
                }
            }
            _ => parts.push(part),
        }
    }
    let path = parts.join("/");
    if !matches!(path.split('/').next(), Some("work" | "project" | "inputs")) {
        return Err(port(ErrorCode::InvalidCommand));
    }
    Ok(path)
}
fn nonzero(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("static positive bound")
}
fn job_id(id: InvocationId) -> BrowserJobId {
    BrowserJobId::from_uuid(id.into_uuid()).expect("nonzero invocation")
}
fn reference(id: InvocationId) -> OpaqueExternalRef {
    OpaqueExternalRef::new(format!("browser-workspace:{id}")).expect("bounded identity")
}
fn text(s: &str) -> SafeDisplayText {
    SafeDisplayText::new(s).expect("static safe text")
}
fn port(code: ErrorCode) -> LoopPortError {
    LoopPortError {
        code,
        retryable: code == ErrorCode::DependencyUnavailable,
    }
}
fn error(code: ErrorCode, message: &str) -> ErrorInfo {
    ErrorInfo {
        code,
        category: code.category(),
        safe_message: text(message),
        retryable: false,
        dependency_ref: None,
        diagnostic_ref: None,
    }
}
fn failed(
    code: ErrorCode,
    message: &str,
    diagnostic_result: Option<CapabilityResult>,
) -> CapabilityOutcome {
    CapabilityOutcome::Failed {
        error: error(code, message),
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result,
    }
}
fn pending(
    reconciliation_ref: OpaqueExternalRef,
    message: &str,
) -> DispatchedReconciliationOutcome {
    DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::StillIndeterminate {
        reason: text(message),
        reconciliation_ref,
    })
}
fn proven_failed(
    code: ErrorCode,
    message: &str,
    diagnostic: CapabilityResult,
) -> DispatchedReconciliationOutcome {
    DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenFailed {
        error: error(code, message),
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: Some(diagnostic),
    })
}
