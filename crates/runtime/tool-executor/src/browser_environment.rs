//! Routed sandbox environment control plane. Tool completion acknowledges a durable
//! command, never completion of installation. Installation is owned by BrowserJob.
use crate::{
    AtomicCapabilityHandler, DispatchedCapabilityOutcome, DispatchedReconciliationOutcome,
};
use musteragent_capability::{
    BrowserJobEngine, BrowserJobProgram, BrowserJobRequest, BrowserJobStatus,
    BrowserProviderContract, BrowserPythonEnvironmentBinding, BrowserResourceLimits,
    BrowserSecurityProfile, CapabilityManifest,
};
use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    BrowserJobId, ConversationId, PrincipalId, TenantId, VersionedKey, WorkspaceId,
};
use musteragent_loop::LoopPortError;
use musteragent_run::AttemptContext;
use musteragent_runtime_capability::{
    BrowserGatewayStore, BrowserJobDeliveryState, BrowserProviderRuntimeError,
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableBrowserJob, DurableToolInvocation, ReconciliationOutcome, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorInfo, OpaqueExternalRef, RetryDisposition, SafeDisplayText,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

pub struct BrowserEnvironmentCapabilityHandler {
    manifest: CapabilityManifest,
    browser: Arc<dyn BrowserGatewayStore>,
    content: Arc<dyn ContentStore>,
    clock: Arc<dyn musteragent_runtime_capability::RuntimeClock>,
    unavailable_notice: crate::BrowserUnavailableNotice,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepare {
    requirements: Vec<String>,
    requires_python: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    operation_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Environment {
    environment_ref: String,
}

pub(crate) fn scope_key(
    tenant: TenantId,
    principal: PrincipalId,
    workspace: Option<WorkspaceId>,
    conversation: ConversationId,
) -> Sha256 {
    let scope = workspace.map_or_else(
        || format!("conversation:{conversation}"),
        |id| format!("workspace:{id}"),
    );
    Sha256::digest(format!(
        "musteragent.python-environment.scope.v1\0{tenant}\0{principal}\0{scope}"
    ))
}
fn job_id(value: &str) -> Result<BrowserJobId, LoopPortError> {
    value.parse().map_err(|_| port(ErrorCode::InvalidCommand))
}
fn port(code: ErrorCode) -> LoopPortError {
    LoopPortError {
        code,
        retryable: code == ErrorCode::DependencyUnavailable,
    }
}
fn text(value: &str) -> SafeDisplayText {
    SafeDisplayText::new(value).expect("static safe text")
}
fn reference(id: BrowserJobId) -> OpaqueExternalRef {
    OpaqueExternalRef::new(format!("browser-environment:{id}")).expect("bounded identity")
}

pub(crate) async fn authorized_environment(
    browser: &dyn BrowserGatewayStore,
    tenant: TenantId,
    principal: PrincipalId,
    scope: Sha256,
    id: BrowserJobId,
) -> Result<DurableBrowserJob, LoopPortError> {
    let job = browser
        .load_job(tenant, id)
        .await
        .map_err(|_| port(ErrorCode::DependencyUnavailable))?
        .ok_or_else(|| port(ErrorCode::ResourceFileUnavailable))?;
    if job.delivery.lease.principal_id != principal
        || job.delivery.lease.tenant_id != tenant
        || !matches!(job.delivery.request.program, BrowserJobProgram::PythonEnvironment { scope_key, .. } if scope_key == scope)
    {
        return Err(port(ErrorCode::PolicyDenied));
    }
    Ok(job)
}

pub(crate) fn ready_binding(
    job: &DurableBrowserJob,
) -> Result<BrowserPythonEnvironmentBinding, LoopPortError> {
    let result = job
        .delivery
        .terminal_result
        .as_ref()
        .filter(|r| r.status == BrowserJobStatus::Completed)
        .ok_or_else(|| port(ErrorCode::CapabilityUnavailable))?;
    let value: Value = serde_json::from_str(&result.stdout_tail)
        .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
    let BrowserJobProgram::PythonEnvironment { scope_key, .. } = job.delivery.request.program
    else {
        return Err(port(ErrorCode::PolicyDenied));
    };
    if value["schema"] != "musteragent.python-environment/1"
        || value["installationId"] != job.delivery.request.job_id.to_string()
        || value["scopeKey"] != scope_key.to_string()
    {
        return Err(port(ErrorCode::ContentIntegrityFailed));
    }
    let revision_hash = value["revisionHash"]
        .as_str()
        .and_then(|s| Sha256::from_hex(s).ok())
        .ok_or_else(|| port(ErrorCode::ContentIntegrityFailed))?;
    Ok(BrowserPythonEnvironmentBinding {
        installation_id: job.delivery.request.job_id,
        revision_hash,
        scope_key,
    })
}

impl BrowserEnvironmentCapabilityHandler {
    pub fn new(
        manifest: CapabilityManifest,
        browser: Arc<dyn BrowserGatewayStore>,
        content: Arc<dyn ContentStore>,
        clock: Arc<dyn musteragent_runtime_capability::RuntimeClock>,
    ) -> Self {
        Self {
            manifest,
            browser,
            content,
            clock,
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
    async fn store(
        &self,
        invocation_id: musteragent_identity::InvocationId,
        tenant: TenantId,
        schema: SchemaRef,
        value: Value,
    ) -> Result<CapabilityResult, LoopPortError> {
        // Reconciliation is a distinct observation of the same operation.
        // Keep each exact result immutable under its invocation, rather than
        // letting a later observation overwrite the earlier result identity.
        let bytes =
            serde_json::to_vec(&value).map_err(|_| port(ErrorCode::InternalInvariantViolated))?;
        let purpose = format!("result:{}", Sha256::digest(&bytes));
        let result_ref = self
            .content
            .put_at(
                crate::producer_content::identity(invocation_id, &purpose),
                PutContent {
                    tenant_id: tenant,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
                    bytes,
                },
            )
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: schema,
            provider_request_ref: None,
        })
    }
    async fn inspect(
        &self,
        tenant: TenantId,
        principal: PrincipalId,
        scope: Sha256,
        id: BrowserJobId,
    ) -> Result<Value, LoopPortError> {
        let job =
            authorized_environment(self.browser.as_ref(), tenant, principal, scope, id).await?;
        let result = job.delivery.terminal_result.as_ref();
        let connected = self
            .browser
            .lease_is_current(job.delivery.lease)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
        let status = match (job.delivery.state, result.map(|r| r.status)) {
            (_, Some(BrowserJobStatus::Completed)) => "ready",
            (_, Some(BrowserJobStatus::Failed)) => "failed",
            (_, Some(BrowserJobStatus::Cancelled)) => "cancelled",
            (_, Some(BrowserJobStatus::ResourceExceeded)) => "resource_exceeded",
            (_, Some(BrowserJobStatus::Indeterminate)) => "indeterminate",
            (_, None) if !connected => "indeterminate",
            (BrowserJobDeliveryState::Queued, None) => "queued",
            (BrowserJobDeliveryState::CancelRequested, None) => "cancel_requested",
            _ => "installing",
        };
        let ready = result
            .filter(|r| r.status == BrowserJobStatus::Completed)
            .map(|r| serde_json::from_str::<Value>(&r.stdout_tail))
            .transpose()
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        if ready.is_some() {
            ready_binding(&job)?;
        }
        let progress = self
            .browser
            .load_progress(tenant, id)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
        let mut diagnostics = result
            .map(|r| {
                r.diagnostics
                    .iter()
                    .map(|d| json!({"code":d.code,"message":d.message}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if status == "indeterminate" {
            diagnostics.push(json!({
                "code": "browser_provider_disconnected",
                "message": "The original browser execution lease is no longer current and no terminal receipt was received. Progress is the last durable observation, not live activity or proof of a stalled download. Repeated inspection cannot resume this operation. No installation is replayed automatically."
            }));
        }
        Ok(
            json!({"operation_id": id.to_string(), "status": status, "environment_ref": ready.as_ref().map(|_| id.to_string()), "environment": ready,
            "local_material": "verified_on_execution", "progress": progress,
            "provider_sequence": job.delivery.last_provider_sequence, "updated_at_ms": job.updated_at.unix_millis(),
            "diagnostics": diagnostics,
            "message": "Keep the browser page open. Ready is an immutable installation receipt; browser storage availability is verified again before execution. No job is replayed automatically."}),
        )
    }
    async fn execute_command(
        &self,
        attempt: &AttemptContext,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
        invocation: &CapabilityInvocation,
    ) -> Result<Value, LoopPortError> {
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
        verify_content(&invocation.request_ref, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
        let scope = scope_key(
            invocation.tenant_id,
            invocation.actor.principal_id,
            invocation.workspace_id,
            invocation.conversation_id,
        );
        match self.manifest.key.logical_key() {
            "sandbox.environment.prepare" => {
                let input: Prepare =
                    serde_json::from_slice(&bytes).map_err(|_| port(ErrorCode::InvalidCommand))?;
                let id = job_id(&invocation.invocation_id.to_string())?;
                let request = BrowserJobRequest {
                    execution_liveness: None,
                    job_id: id,
                    invocation_id: invocation.invocation_id,
                    conversation_id: invocation.conversation_id,
                    engine: BrowserJobEngine::PythonEnvironment,
                    runtime_release_ref: VersionedKey::new("browser-runtime@2")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated))?,
                    program: BrowserJobProgram::PythonEnvironment {
                        requirements: input.requirements,
                        requires_python: input.requires_python,
                        scope_key: scope,
                    },
                    inputs: vec![],
                    declared_outputs: vec![musteragent_capability::BrowserDeclaredOutput {
                        path: "environment-lock.json".into(),
                        media_type: musteragent_content_ref::MediaType::new("application/json")
                            .expect("media"),
                        maximum_byte_length: NonZeroU64::new(512 * 1024).expect("bound"),
                        required: true,
                    }],
                    limits: BrowserResourceLimits {
                        wall_time_millis: NonZeroU64::new(600_000).expect("positive"),
                        maximum_input_bytes: NonZeroU64::new(1).expect("positive"),
                        maximum_output_bytes: NonZeroU64::new(512 * 1024).expect("positive"),
                        maximum_files: NonZeroU32::new(1).expect("positive"),
                        maximum_stdout_bytes: NonZeroU64::new(50 * 1024).expect("positive"),
                        maximum_stderr_bytes: NonZeroU64::new(50 * 1024).expect("positive"),
                    },
                    security_profile: BrowserSecurityProfile::SafeOffline,
                };
                request
                    .validate()
                    .map_err(|_| port(ErrorCode::InvalidCommand))?;
                match (crate::sandbox_routing::SandboxDispatch {
                    clock: self.clock.as_ref(),
                    attempt,
                    material,
                    durable,
                    invocation,
                    contract: BrowserProviderContract::PythonEnvironmentPrepare,
                    request: request.clone(),
                })
                .enqueue(self.browser.as_ref())
                .await
                {
                    Ok(_) => {}
                    Err(BrowserProviderRuntimeError::DuplicateJob) => {
                        let original = authorized_environment(
                            self.browser.as_ref(),
                            invocation.tenant_id,
                            invocation.actor.principal_id,
                            scope,
                            id,
                        )
                        .await?;
                        if original.delivery.request != request {
                            return Err(port(ErrorCode::ContentIntegrityFailed));
                        }
                    }
                    Err(error) if crate::browser_execution::browser_unavailable(&error) => {
                        return Err(port(ErrorCode::BrowserExecutionUnavailable));
                    }
                    Err(
                        BrowserProviderRuntimeError::SandboxRoute(_)
                        | BrowserProviderRuntimeError::SandboxRoutingUnavailable,
                    ) => {
                        return Err(port(ErrorCode::CapabilityUnavailable));
                    }
                    Err(_) => return Err(port(ErrorCode::DependencyUnavailable)),
                }
                Ok(
                    json!({"operation_id":id.to_string(), "status":"accepted", "message":"Installation was durably accepted, not completed. Use sandbox_operation_inspect for progress and the ready environment_ref. Script authoring can proceed independently."}),
                )
            }
            "sandbox.operation.inspect"
            | "sandbox.operation.cancel"
            | "sandbox.environment.inspect" => {
                let id = if self.manifest.key.logical_key() == "sandbox.environment.inspect" {
                    job_id(
                        &serde_json::from_slice::<Environment>(&bytes)
                            .map_err(|_| port(ErrorCode::InvalidCommand))?
                            .environment_ref,
                    )?
                } else {
                    job_id(
                        &serde_json::from_slice::<Operation>(&bytes)
                            .map_err(|_| port(ErrorCode::InvalidCommand))?
                            .operation_id,
                    )?
                };
                // Check owner and project before both observation and cancellation.
                authorized_environment(
                    self.browser.as_ref(),
                    invocation.tenant_id,
                    invocation.actor.principal_id,
                    scope,
                    id,
                )
                .await?;
                if self.manifest.key.logical_key() == "sandbox.operation.cancel" {
                    self.browser
                        .request_cancel(id)
                        .await
                        .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
                }
                self.inspect(
                    invocation.tenant_id,
                    invocation.actor.principal_id,
                    scope,
                    id,
                )
                .await
            }
            _ => Err(port(ErrorCode::RuntimeSnapshotInvalid)),
        }
    }
}
impl AtomicCapabilityHandler for BrowserEnvironmentCapabilityHandler {
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn reconciliation_identity(
        &self,
        invocation: &CapabilityInvocation,
    ) -> Option<OpaqueExternalRef> {
        job_id(&invocation.invocation_id.to_string())
            .ok()
            .map(reference)
    }
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<crate::ToolPhysicalExecution<'a>, LoopPortError> {
        // The public command owns only its awaited database admission transaction.
        // The returned operation ID explicitly transfers installation ownership.
        crate::local_atomic_execution(self, attempt, material, durable, invocation, binding)
    }
    fn invoke<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            crate::atomic_authorization::validate_invocation(
                &self.manifest,
                attempt,
                material,
                durable,
                &invocation,
            )?;
            // Result materialization can fail after admission/cancellation commits.
            // Preserve the original reconciliation identity for that ambiguity too.
            let result = match self
                .execute_command(attempt, material, durable, &invocation)
                .await
            {
                Ok(value) => {
                    self.store(
                        invocation.invocation_id,
                        invocation.tenant_id,
                        invocation.result_schema,
                        value,
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(result) => Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Succeeded(result),
                )),
                Err(error)
                    if error.code == ErrorCode::DependencyUnavailable
                        && matches!(
                            self.manifest.key.logical_key(),
                            "sandbox.environment.prepare" | "sandbox.operation.cancel"
                        ) =>
                {
                    Ok(DispatchedCapabilityOutcome::Provider(
                        CapabilityOutcome::Indeterminate {
                            diagnostic_result: None,
                            observed_error: None,
                            reason: text(
                                "The environment command acknowledgement is unconfirmed; reconcile the original command instead of submitting a replacement.",
                            ),
                            reconciliation_ref: reference(job_id(
                                &invocation.invocation_id.to_string(),
                            )?),
                        },
                    ))
                }
                Err(error) if error.code == ErrorCode::BrowserExecutionUnavailable => Ok(
                    DispatchedCapabilityOutcome::Provider(self.unavailable_notice.failure()),
                ),
                Err(error) => Ok(DispatchedCapabilityOutcome::Provider(
                    CapabilityOutcome::Failed {
                        error: ErrorInfo {
                            code: error.code,
                            category: error.code.category(),
                            safe_message: text(match error.code {
                                ErrorCode::CapabilityUnavailable => {
                                    "No permitted sandbox executor or durable routing store is available. No installation was dispatched."
                                }
                                ErrorCode::AgentNotAvailable => {
                                    "The selected sandbox executor is unavailable; no fallback was attempted."
                                }
                                ErrorCode::PolicyDenied => {
                                    "This environment belongs to another principal or project."
                                }
                                ErrorCode::ResourceFileUnavailable => {
                                    "The environment operation does not exist in this scope."
                                }
                                ErrorCode::InvalidCommand => {
                                    "Invalid environment command. Use package requirements or an exact operation/environment reference returned by these tools."
                                }
                                _ => {
                                    "Environment control storage is unavailable; inspect the original operation before retrying."
                                }
                            }),
                            retryable: false,
                            dependency_ref: None,
                            diagnostic_ref: None,
                        },
                        retry_disposition: RetryDisposition::DoNotRetry,
                        diagnostic_result: None,
                    },
                )),
            }
        })
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move {
            let id = job_id(&request.invocation_id.to_string())?;
            if request.reconciliation_ref != reference(id) {
                return Err(port(ErrorCode::PolicyDenied));
            }
            if self.manifest.key.logical_key() == "sandbox.environment.prepare" {
                let scope = scope_key(
                    request.tenant_id,
                    request.actor.principal_id,
                    request.workspace_id,
                    request.conversation_id,
                );
                if authorized_environment(
                    self.browser.as_ref(),
                    request.tenant_id,
                    request.actor.principal_id,
                    scope,
                    id,
                )
                .await
                .is_ok()
                {
                    return Ok(DispatchedReconciliationOutcome::Provider(ReconciliationOutcome::ProvenSucceeded(self.store(request.invocation_id, request.tenant_id, request.result_schema, json!({"operation_id":id.to_string(),"status":"accepted","message":"Original installation admission confirmed; inspect its progress. No new installation was submitted."})).await?)));
                }
            }
            if self.manifest.key.logical_key() == "sandbox.operation.cancel" {
                let bytes = self
                    .content
                    .read(request.tenant_id, &durable.invocation.request_ref)
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable))?;
                verify_content(&durable.invocation.request_ref, &bytes)
                    .map_err(|_| port(ErrorCode::ContentIntegrityFailed))?;
                let operation: Operation =
                    serde_json::from_slice(&bytes).map_err(|_| port(ErrorCode::InvalidCommand))?;
                let scope = scope_key(
                    request.tenant_id,
                    request.actor.principal_id,
                    request.workspace_id,
                    request.conversation_id,
                );
                let original = authorized_environment(
                    self.browser.as_ref(),
                    request.tenant_id,
                    request.actor.principal_id,
                    scope,
                    job_id(&operation.operation_id)?,
                )
                .await?;
                if matches!(
                    original.delivery.state,
                    BrowserJobDeliveryState::CancelRequested | BrowserJobDeliveryState::Terminal
                ) {
                    let value = self
                        .inspect(
                            request.tenant_id,
                            request.actor.principal_id,
                            scope,
                            original.delivery.request.job_id,
                        )
                        .await?;
                    return Ok(DispatchedReconciliationOutcome::Provider(
                        ReconciliationOutcome::ProvenSucceeded(
                            self.store(
                                request.invocation_id,
                                request.tenant_id,
                                request.result_schema,
                                value,
                            )
                            .await?,
                        ),
                    ));
                }
            }
            Ok(DispatchedReconciliationOutcome::Provider(
                ReconciliationOutcome::StillIndeterminate {
                    reason: text(
                        "The original environment command has no confirmed admission receipt.",
                    ),
                    reconciliation_ref: request.reconciliation_ref,
                },
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn environments_share_only_within_the_same_principal_and_project() {
        let tenant = TenantId::generate();
        let principal = PrincipalId::generate();
        let workspace = Some(WorkspaceId::generate());
        let conversation = ConversationId::generate();
        let scope = scope_key(tenant, principal, workspace, conversation);
        assert_eq!(
            scope,
            scope_key(tenant, principal, workspace, ConversationId::generate())
        );
        assert_ne!(
            scope,
            scope_key(TenantId::generate(), principal, workspace, conversation)
        );
        assert_ne!(
            scope,
            scope_key(tenant, PrincipalId::generate(), workspace, conversation)
        );
        assert_ne!(
            scope,
            scope_key(
                tenant,
                principal,
                Some(WorkspaceId::generate()),
                conversation
            )
        );
        assert_ne!(
            scope_key(tenant, principal, None, conversation),
            scope_key(tenant, principal, None, ConversationId::generate())
        );
    }
}
