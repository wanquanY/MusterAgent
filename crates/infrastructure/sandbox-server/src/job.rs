//! One job: materialize inputs, run the container, collect declared
//! outputs into `ContentRef`s and build the terminal `BrowserJobResult`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use musteragent_capability::{
    BrowserDiagnostic, BrowserDiagnosticSeverity, BrowserJobMetrics, BrowserJobProgram,
    BrowserJobRequest, BrowserJobResult, BrowserJobStatus, StagedBrowserOutput,
};
use musteragent_content_ref::Sha256;
use musteragent_identity::{BrowserJobId, TenantId};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent, verify_content};
use tokio_util::sync::CancellationToken;

use crate::config::ServerSandboxConfig;
use crate::paths::{JobDirs, PathError};
use crate::runtime::{ContainerExit, ContainerOutcome, ContainerRuntime, RuntimeError};
use crate::spec::ContainerSpec;

/// Prefix of the opaque staging receipt this provider attaches to outputs.
pub const STAGING_RECEIPT_PREFIX: &str = "server-sandbox:";

/// Why a running job was stopped from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause {
    CancelRequested,
    Shutdown,
}

/// A job whose directories and container spec are ready to run.
#[derive(Debug)]
pub struct PreparedJob {
    dirs: JobDirs,
    spec: ContainerSpec,
    input_bytes: u64,
    diagnostics: Vec<BrowserDiagnostic>,
    workspace: Option<musteragent_runtime_resource::WorkspaceManifest>,
}

impl PreparedJob {
    #[must_use]
    pub const fn spec(&self) -> &ContainerSpec {
        &self.spec
    }

    #[must_use]
    pub const fn dirs(&self) -> &JobDirs {
        &self.dirs
    }
}

impl Drop for PreparedJob {
    fn drop(&mut self) {
        if let Err(error) = self.dirs.remove() {
            tracing::warn!(dir = %self.dirs.root.display(), %error, "sandbox job directory was not removed");
        }
    }
}

#[derive(Debug)]
pub enum Prepared {
    Ready(Box<PreparedJob>),
    /// The job cannot run; this is its terminal result.
    Terminal(BrowserJobResult),
}

/// Executes jobs against a content store and a container runtime.
#[derive(Clone)]
pub struct JobExecutor {
    config: Arc<ServerSandboxConfig>,
    content: Arc<dyn ContentStore>,
    runtime: Arc<dyn ContainerRuntime>,
    environments: Arc<Mutex<BTreeMap<(TenantId, BrowserJobId), serde_json::Value>>>,
}

impl JobExecutor {
    #[must_use]
    pub fn new(
        config: Arc<ServerSandboxConfig>,
        content: Arc<dyn ContentStore>,
        runtime: Arc<dyn ContainerRuntime>,
    ) -> Self {
        Self {
            config,
            content,
            runtime,
            environments: Arc::default(),
        }
    }

    /// Validates the job, materializes its inputs and builds the container
    /// spec.
    pub async fn prepare(&self, tenant_id: TenantId, request: &BrowserJobRequest) -> Prepared {
        let job_id = request.job_id;
        if request.validate().is_err() {
            return Prepared::Terminal(failure(
                job_id,
                "invalid_request",
                "job request is invalid",
            ));
        }
        let diagnostics = Vec::new();
        if request
            .inputs
            .iter()
            .any(|i| i.path == "__kernel__" || i.path.starts_with("__kernel__/"))
        {
            return Prepared::Terminal(failure(
                job_id,
                "invalid_request",
                "reserved kernel input path",
            ));
        }
        if request.security_profile == musteragent_capability::BrowserSecurityProfile::ProxyEgress
            && self.config.egress.is_none()
        {
            return Prepared::Terminal(failure(
                job_id,
                "egress_unavailable",
                "networked profile requires a host egress policy",
            ));
        }
        let expected = match &request.program {
            BrowserJobProgram::PythonWorkspace {
                environment_manifest_ref: Some(reference),
                ..
            } => match self.content.read(tenant_id, reference).await {
                Ok(bytes) if verify_content(reference, &bytes).is_ok() => {
                    serde_json::from_slice(&bytes).ok()
                }
                _ => {
                    return Prepared::Terminal(failure(
                        job_id,
                        "content_integrity_failed",
                        "environment manifest is unavailable or corrupt",
                    ));
                }
            },
            BrowserJobProgram::Python {
                environment: Some(binding),
                ..
            } => self
                .environments
                .lock()
                .ok()
                .and_then(|e| e.get(&(tenant_id, binding.installation_id)).cloned()),
            _ => None,
        };
        if let BrowserJobProgram::Python { entrypoint, .. } = &request.program
            && !request.inputs.iter().any(|i| &i.path == entrypoint)
        {
            return Prepared::Terminal(failure(
                job_id,
                "entrypoint_missing",
                "entrypoint is not a job input",
            ));
        }

        let mut materialized = Vec::with_capacity(request.inputs.len());
        let mut input_bytes = 0_u64;
        for input in &request.inputs {
            let bytes = match self.content.read(tenant_id, &input.content_ref).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Prepared::Terminal(content_failure(job_id, error));
                }
            };
            if let Err(error) = verify_content(&input.content_ref, &bytes) {
                return Prepared::Terminal(content_failure(job_id, error));
            }
            input_bytes = input_bytes.saturating_add(input.content_ref.byte_length);
            materialized.push((input.path.clone(), bytes));
        }

        let work_root = self.config.work_root.clone();
        let outputs: Vec<String> = request
            .declared_outputs
            .iter()
            .map(|output| output.path.clone())
            .collect();
        let created = tokio::task::spawn_blocking(move || -> Result<JobDirs, PathError> {
            let dirs = JobDirs::create(&work_root, job_id)?;
            let placed = materialized
                .iter()
                .try_for_each(|(path, bytes)| dirs.place_input(path, bytes))
                .and_then(|()| {
                    outputs
                        .iter()
                        .try_for_each(|path| dirs.prepare_output(path))
                });
            if let Err(error) = placed {
                let _ = dirs.remove();
                return Err(error);
            }
            Ok(dirs)
        })
        .await;
        let dirs = match created {
            Ok(Ok(dirs)) => dirs,
            Ok(Err(PathError::Unsafe | PathError::Symlink)) => {
                return Prepared::Terminal(failure(
                    job_id,
                    "invalid_request",
                    "job paths are unsafe",
                ));
            }
            Ok(Err(error)) => {
                tracing::warn!(job = %job_id.into_uuid(), %error, "sandbox job directory setup failed");
                return Prepared::Terminal(failure(
                    job_id,
                    "server_sandbox_unavailable",
                    "the server sandbox could not prepare the job directory",
                ));
            }
            Err(_) => {
                return Prepared::Terminal(failure(
                    job_id,
                    "server_sandbox_unavailable",
                    "the server sandbox could not prepare the job directory",
                ));
            }
        };
        let workspace = match crate::program::prepare(request, &dirs, &self.config, expected) {
            Ok(workspace) => workspace,
            Err(error) => {
                let _ = dirs.remove();
                let code = if error.starts_with("python_environment_unavailable") {
                    "server_sandbox_environment_unavailable"
                } else {
                    "invalid_request"
                };
                return Prepared::Terminal(failure(job_id, code, &error));
            }
        };
        let spec = ContainerSpec::python_job(
            request,
            &self.config,
            &dirs,
            "/sandbox/input/__kernel__/runner.py",
        );
        Prepared::Ready(Box::new(PreparedJob {
            dirs,
            spec,
            input_bytes,
            diagnostics,
            workspace,
        }))
    }

    /// Runs the container. Cancelling `cancel` kills it.
    pub async fn run(
        &self,
        job: &PreparedJob,
        cancel: CancellationToken,
    ) -> Result<ContainerOutcome, RuntimeError> {
        self.runtime.run(&job.spec, cancel).await
    }

    /// Turns the container outcome into the terminal result, uploading
    /// declared outputs on success. Removes the job directory.
    pub async fn finish(
        &self,
        tenant_id: TenantId,
        request: &BrowserJobRequest,
        job: Box<PreparedJob>,
        outcome: Result<ContainerOutcome, RuntimeError>,
        stop: Option<StopCause>,
    ) -> BrowserJobResult {
        let job_id = request.job_id;
        let mut diagnostics = job.diagnostics.clone();
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let (status, code) = match &error {
                    RuntimeError::ImageMissing(_) => {
                        (BrowserJobStatus::Failed, "server_sandbox_image_missing")
                    }
                    RuntimeError::Unavailable(_) | RuntimeError::Start(_) => {
                        (BrowserJobStatus::Failed, "server_sandbox_unavailable")
                    }
                    RuntimeError::Indeterminate(_) => (
                        BrowserJobStatus::Indeterminate,
                        "server_sandbox_indeterminate",
                    ),
                };
                tracing::warn!(job = %job_id.into_uuid(), %error, "sandbox container runtime failed");
                diagnostics.push(diagnostic(
                    code,
                    BrowserDiagnosticSeverity::Error,
                    "the server sandbox container runtime failed",
                ));
                return seal(
                    job_id,
                    status,
                    Vec::new(),
                    diagnostics,
                    "",
                    "",
                    0,
                    job.input_bytes,
                );
            }
        };
        let stdout = outcome.stdout.tail_text();
        let stderr = outcome.stderr.tail_text();
        let wall = u64::try_from(outcome.elapsed.as_millis()).unwrap_or(u64::MAX);
        for event in &outcome.egress {
            let (code, severity) = if let Some(reason) = event.denial {
                (
                    format!("egress_denied_{}", reason.as_str()),
                    BrowserDiagnosticSeverity::Warning,
                )
            } else if event.mode == crate::egress::ResolutionMode::UpstreamResolves {
                (
                    "egress_upstream_resolves".into(),
                    BrowserDiagnosticSeverity::Info,
                )
            } else {
                (
                    "egress_checked_address".into(),
                    BrowserDiagnosticSeverity::Info,
                )
            };
            diagnostics.push(diagnostic(
                &code,
                severity,
                &format!(
                    "destination {}:{}; address policy {:?}",
                    event.host, event.port, event.mode
                ),
            ));
        }
        let (mut status, code, message) = match outcome.exit {
            ContainerExit::RuntimeFailed => (
                BrowserJobStatus::Indeterminate,
                "server_sandbox_indeterminate",
                "container runtime failed",
            ),
            ContainerExit::InputLimitExceeded => (
                BrowserJobStatus::ResourceExceeded,
                "stdin_limit_exceeded",
                "input stream limit exceeded",
            ),
            ContainerExit::Cancelled => (
                BrowserJobStatus::Cancelled,
                if stop == Some(StopCause::Shutdown) {
                    "server_sandbox_shutdown"
                } else {
                    "cancelled"
                },
                "job container cancelled and removed",
            ),
            ContainerExit::TimedOut => (
                BrowserJobStatus::ResourceExceeded,
                "wall_time_exceeded",
                "job container timed out and was removed",
            ),
            ContainerExit::OutputLimitExceeded => (
                BrowserJobStatus::ResourceExceeded,
                if outcome.stdout.exceeded() {
                    "stdout_limit_exceeded"
                } else {
                    "stderr_limit_exceeded"
                },
                "stream limit exceeded",
            ),
            ContainerExit::DiskQuotaExceeded => (
                BrowserJobStatus::ResourceExceeded,
                "disk_quota_exceeded",
                "bounded filesystem or output quota exceeded",
            ),
            ContainerExit::InvalidOutput => (
                BrowserJobStatus::Failed,
                "output_invalid",
                "output contains an unsafe filesystem object",
            ),
            ContainerExit::Exited(_) if outcome.oom_killed => (
                BrowserJobStatus::ResourceExceeded,
                "memory_exceeded",
                "memory limit exceeded",
            ),
            ContainerExit::Exited(code) if code != 0 => (
                BrowserJobStatus::Failed,
                if stderr.contains("python_environment_unavailable") {
                    "server_sandbox_environment_unavailable"
                } else {
                    "python_exit_nonzero"
                },
                "Python process exited unsuccessfully",
            ),
            ContainerExit::Exited(_) => (BrowserJobStatus::Completed, "", ""),
        };
        if !code.is_empty() {
            diagnostics.push(diagnostic(code, BrowserDiagnosticSeverity::Error, message));
        }
        if let Some(error) = &outcome.runtime_error {
            tracing::warn!(%error,"sandbox runtime failure after execution");
            status = BrowserJobStatus::Indeterminate;
            diagnostics.push(diagnostic(
                "server_sandbox_indeterminate",
                BrowserDiagnosticSeverity::Error,
                "container runtime or output capture failed",
            ));
        }
        let success = status == BrowserJobStatus::Completed;
        let collected = if let Some(workspace) = &job.workspace {
            crate::program::workspace_outputs(
                &job.dirs,
                workspace,
                success,
                request.limits.maximum_output_bytes.get(),
                request.limits.maximum_files.get(),
            )
            .map(|files| Collected {
                files,
                undeclared: 0,
                errors: Vec::new(),
            })
            .map_err(|_| {
                (
                    BrowserJobStatus::Failed,
                    "workspace_output_invalid",
                    "workspace output could not be collected",
                )
            })
        } else {
            self.collect(request, &job.dirs, success).await
        };
        let collected = match collected {
            Ok(collected) => collected,
            Err((failed, code, message)) => {
                if success {
                    status = failed;
                }
                diagnostics.push(diagnostic(code, BrowserDiagnosticSeverity::Error, message));
                Collected {
                    files: Vec::new(),
                    undeclared: 0,
                    errors: Vec::new(),
                }
            }
        };
        for (failed, code, message) in &collected.errors {
            if status == BrowserJobStatus::Completed {
                status = *failed;
            }
            diagnostics.push(diagnostic(code, BrowserDiagnosticSeverity::Error, message));
        }
        if collected.undeclared > 0 {
            diagnostics.push(diagnostic(
                "undeclared_output_ignored",
                BrowserDiagnosticSeverity::Warning,
                &format!("{} undeclared files discarded", collected.undeclared),
            ));
        }
        let mut outputs = Vec::new();
        for (declared, bytes) in collected.files {
            match self
                .content
                .put(PutContent {
                    tenant_id,
                    media_type: declared.media_type.clone(),
                    bytes: bytes.clone(),
                })
                .await
            {
                Ok(reference)
                    if reference.media_type == declared.media_type
                        && reference.byte_length == bytes.len() as u64
                        && reference.sha256 == Sha256::digest(&bytes) =>
                {
                    outputs.push(StagedBrowserOutput {
                        path: declared.path,
                        staging_receipt_ref: format!(
                            "{STAGING_RECEIPT_PREFIX}{}",
                            reference.content_id
                        ),
                        content_ref: reference,
                    });
                }
                _ => {
                    status = BrowserJobStatus::Failed;
                    diagnostics.push(diagnostic(
                        "output_staging_failed",
                        BrowserDiagnosticSeverity::Error,
                        "job output staging failed",
                    ));
                }
            }
        }
        if matches!(request.program, BrowserJobProgram::PythonEnvironment { .. })
            && status == BrowserJobStatus::Completed
            && let Ok(receipt) = serde_json::from_str(&stdout)
            && let Ok(mut env) = self.environments.lock()
        {
            if env.len() >= 128
                && let Some(key) = env.keys().next().copied()
            {
                env.remove(&key);
            }
            env.insert((tenant_id, request.job_id), receipt);
        }
        let result = seal(
            job_id,
            status,
            outputs,
            diagnostics,
            &stdout,
            &stderr,
            wall,
            job.input_bytes,
        );
        if request.validate_result(&result).is_err() {
            return failure(
                job_id,
                "result_contract_violation",
                "job result does not satisfy its output contract",
            );
        }
        result
    }

    async fn collect(
        &self,
        request: &BrowserJobRequest,
        dirs: &JobDirs,
        success: bool,
    ) -> Result<Collected, (BrowserJobStatus, &'static str, &'static str)> {
        let dirs = dirs.clone();
        let declared = request.declared_outputs.clone();
        let inputs: Vec<String> = request
            .inputs
            .iter()
            .map(|input| input.path.clone())
            .collect();
        let total_limit = request.limits.maximum_output_bytes.get();
        tokio::task::spawn_blocking(move || {
            let mut files = Vec::new();
            let mut total = 0_u64;
            let mut errors = Vec::new();
            for output in &declared {
                let remaining = total_limit.saturating_sub(total);
                let limit = output.maximum_byte_length.get().min(remaining);
                match dirs.read_output(&output.path, limit) {
                    Ok(Some(bytes)) => {
                        total = total.saturating_add(bytes.len() as u64);
                        files.push((output.clone(), bytes));
                    }
                    Ok(None) if output.required && success => {
                        errors.push((
                            BrowserJobStatus::Failed,
                            "required_output_missing",
                            "a required output was not produced",
                        ));
                    }
                    Ok(None) => {}
                    Err(PathError::TooLarge) => {
                        errors.push((
                            BrowserJobStatus::ResourceExceeded,
                            "output_too_large",
                            "an output exceeded its byte limit",
                        ));
                    }
                    Err(_) => {
                        errors.push((
                            BrowserJobStatus::Failed,
                            "output_invalid",
                            "an output is not a regular file inside the job directory",
                        ));
                    }
                }
            }
            let declared_paths: Vec<&str> = declared.iter().map(|o| o.path.as_str()).collect();
            let input_paths: Vec<&str> = inputs.iter().map(String::as_str).collect();
            let undeclared = dirs
                .undeclared_entries(&declared_paths, &input_paths, 1_000)
                .map_or(0, |entries| entries.len());
            Ok(Collected {
                files,
                undeclared,
                errors,
            })
        })
        .await
        .map_err(|_| {
            (
                BrowserJobStatus::Failed,
                "output_invalid",
                "job outputs could not be collected",
            )
        })?
    }
}

struct Collected {
    files: Vec<(musteragent_capability::BrowserDeclaredOutput, Vec<u8>)>,
    undeclared: usize,
    errors: Vec<(BrowserJobStatus, &'static str, &'static str)>,
}

fn diagnostic(code: &str, severity: BrowserDiagnosticSeverity, message: &str) -> BrowserDiagnostic {
    BrowserDiagnostic {
        code: code.to_owned(),
        severity,
        message: message.to_owned(),
        path: None,
        line: None,
        column: None,
    }
}

/// A failed result with one error diagnostic and no outputs.
#[must_use]
pub fn failure(job_id: BrowserJobId, code: &str, message: &str) -> BrowserJobResult {
    seal(
        job_id,
        BrowserJobStatus::Failed,
        Vec::new(),
        vec![diagnostic(code, BrowserDiagnosticSeverity::Error, message)],
        "",
        "",
        0,
        0,
    )
}

fn content_failure(job_id: BrowserJobId, error: ContentStoreError) -> BrowserJobResult {
    match error {
        ContentStoreError::LengthMismatch | ContentStoreError::HashMismatch => failure(
            job_id,
            "content_integrity_failed",
            "an input did not match its immutable content reference",
        ),
        ContentStoreError::NotFound | ContentStoreError::Unauthorized => failure(
            job_id,
            "content_unavailable",
            "an input is not available to this job",
        ),
        _ => failure(
            job_id,
            "server_sandbox_unavailable",
            "job inputs could not be read",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn seal(
    job_id: BrowserJobId,
    status: BrowserJobStatus,
    outputs: Vec<StagedBrowserOutput>,
    mut diagnostics: Vec<BrowserDiagnostic>,
    stdout: &str,
    stderr: &str,
    wall_time_millis: u64,
    input_bytes: u64,
) -> BrowserJobResult {
    let max = musteragent_capability::MAX_BROWSER_JOB_DIAGNOSTICS;
    if diagnostics.len() > max {
        let discarded = diagnostics.len() - (max - 1);
        diagnostics.truncate(max - 1);
        diagnostics.push(diagnostic(
            "diagnostics_truncated",
            BrowserDiagnosticSeverity::Warning,
            &format!("{discarded} additional output diagnostics omitted"),
        ));
    }
    let output_bytes = outputs
        .iter()
        .map(|output| output.content_ref.byte_length)
        .sum();
    let output_files = u32::try_from(outputs.len()).unwrap_or(u32::MAX);
    BrowserJobResult {
        job_id,
        status,
        outputs,
        diagnostics,
        stdout_tail: stdout.to_owned(),
        stderr_tail: stderr.to_owned(),
        metrics: BrowserJobMetrics {
            wall_time_millis,
            input_bytes,
            output_bytes,
            output_files,
        },
        result_hash: Sha256::digest(b""),
    }
    .with_canonical_hash()
}
