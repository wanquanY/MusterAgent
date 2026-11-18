//! Browser-provider lease and bounded Job contracts.

use musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    BrowserJobId, BrowserLeaseEpoch, BrowserLeaseId, BrowserSessionId, CapabilityProviderId,
    ConversationId, FencingToken, InvocationId, PageInstanceId, PrincipalId, TenantId,
    UtcTimestamp, VersionedKey,
};
use thiserror::Error;

use crate::CapabilityKey;

pub const BROWSER_JOB_SCHEMA_VERSION: &str = "musteragent.browser-job/1";
pub const BROWSER_JOB_RESULT_SCHEMA_VERSION: &str = "musteragent.browser-job-result/1";
pub const MAX_BROWSER_JOB_INPUTS: usize = 128;
pub const MAX_BROWSER_JOB_OUTPUTS: usize = 100;
pub const MAX_BROWSER_JOB_DIAGNOSTICS: usize = 200;
pub const MAX_BROWSER_JOB_LOG_TAIL_BYTES: usize = 50 * 1024;
pub const MAX_BROWSER_JOB_PATH_BYTES: usize = 512;
pub const MAX_BROWSER_RECEIPT_REF_BYTES: usize = 512;
pub const MAX_BROWSER_PROVIDER_CAPABILITIES: usize = 8;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserProviderLease {
    pub lease_id: BrowserLeaseId,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub browser_session_id: BrowserSessionId,
    pub page_instance_id: PageInstanceId,
    pub provider_id: CapabilityProviderId,
    pub runtime_release_ref: VersionedKey,
    pub capabilities: BTreeSet<CapabilityKey>,
    pub resource_profile_ref: VersionedKey,
    pub epoch: BrowserLeaseEpoch,
    pub fence: FencingToken,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
}

impl BrowserProviderLease {
    pub fn validate(&self) -> Result<(), BrowserJobContractError> {
        if self.capabilities.is_empty()
            || self.capabilities.len() > MAX_BROWSER_PROVIDER_CAPABILITIES
            || self.expires_at <= self.issued_at
            || self
                .capabilities
                .iter()
                .any(|capability| BrowserProviderContract::parse(capability.as_str()).is_err())
        {
            return Err(BrowserJobContractError::InvalidLease);
        }
        Ok(())
    }

    #[must_use]
    pub fn authorizes(&self, capability: &CapabilityKey, at: UtcTimestamp) -> bool {
        self.issued_at <= at && at < self.expires_at && self.capabilities.contains(capability)
    }
}

/// Exact page/epoch/fence proof carried by every post-registration frame.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrowserLeaseProof {
    pub lease_id: BrowserLeaseId,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub browser_session_id: BrowserSessionId,
    pub page_instance_id: PageInstanceId,
    pub provider_id: CapabilityProviderId,
    pub epoch: BrowserLeaseEpoch,
    pub fence: FencingToken,
}

impl From<&BrowserProviderLease> for BrowserLeaseProof {
    fn from(value: &BrowserProviderLease) -> Self {
        Self {
            lease_id: value.lease_id,
            tenant_id: value.tenant_id,
            principal_id: value.principal_id,
            browser_session_id: value.browser_session_id,
            page_instance_id: value.page_instance_id,
            provider_id: value.provider_id,
            epoch: value.epoch,
            fence: value.fence,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserJobEngine {
    PythonEnvironment,
    Python,
    StaticValidate,
    Esbuild,
    Presentation,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserRuntimeFeatureProbe {
    pub worker: bool,
    pub web_assembly: bool,
    pub crypto_subtle: bool,
    pub transferable_array_buffer: bool,
    pub service_worker: bool,
    pub indexed_db: bool,
    pub secure_context: bool,
    pub engines: BTreeSet<BrowserJobEngine>,
}

impl BrowserRuntimeFeatureProbe {
    pub fn validate(&self) -> Result<(), BrowserJobContractError> {
        if !self.worker
            || !self.web_assembly
            || !self.crypto_subtle
            || !self.transferable_array_buffer
            || self.engines.is_empty()
        {
            return Err(BrowserJobContractError::InvalidFeatureProbe);
        }
        Ok(())
    }

    #[must_use]
    pub const fn supports_preview(&self) -> bool {
        self.service_worker && self.indexed_db && self.secure_context
    }
}

/// Closed internal Provider contract. Public model-visible Tools remain owned
/// by Rust handlers and may delegate only to one of these exact operations.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserProviderContract {
    PythonExecute,
    PythonExecuteWorkingTree,
    PythonExecutePrepared,
    PythonExecuteWorkspace,
    PythonEnvironmentPrepare,
    WebBuild,
    WebValidate,
    /// Historical wire identity only; execution is permanently retired.
    PresentationExecute,
}

impl BrowserProviderContract {
    pub fn parse(value: &str) -> Result<Self, BrowserJobContractError> {
        match value {
            "browser.python.execute@1" => Ok(Self::PythonExecute),
            "browser.python.execute@2" => Ok(Self::PythonExecuteWorkingTree),
            "browser.python.execute@3" => Ok(Self::PythonExecutePrepared),
            "browser.python.execute@4" => Ok(Self::PythonExecuteWorkspace),
            "browser.python.environment@1" => Ok(Self::PythonEnvironmentPrepare),
            "browser.web.build@1" => Ok(Self::WebBuild),
            "browser.web.validate@1" => Ok(Self::WebValidate),
            "browser.presentation.execute@1" => Ok(Self::PresentationExecute),
            _ => Err(BrowserJobContractError::UnknownProviderContract),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PythonExecute => "browser.python.execute@1",
            Self::PythonExecuteWorkingTree => "browser.python.execute@2",
            Self::PythonExecutePrepared => "browser.python.execute@3",
            Self::PythonExecuteWorkspace => "browser.python.execute@4",
            Self::PythonEnvironmentPrepare => "browser.python.environment@1",
            Self::WebBuild => "browser.web.build@1",
            Self::WebValidate => "browser.web.validate@1",
            Self::PresentationExecute => "browser.presentation.execute@1",
        }
    }

    #[must_use]
    pub fn accepts_request(self, request: &BrowserJobRequest) -> bool {
        self.accepts_engine(request.engine)
            && match (&request.program, self) {
                (BrowserJobProgram::PythonWorkspace { .. }, Self::PythonExecuteWorkspace) => true,
                (BrowserJobProgram::PythonWorkspace { .. }, _)
                | (_, Self::PythonExecuteWorkspace) => false,
                _ => true,
            }
    }

    pub const fn accepts_engine(self, engine: BrowserJobEngine) -> bool {
        match self {
            Self::PythonExecute
            | Self::PythonExecuteWorkingTree
            | Self::PythonExecutePrepared
            | Self::PythonExecuteWorkspace => {
                matches!(engine, BrowserJobEngine::Python)
            }
            Self::PythonEnvironmentPrepare => matches!(engine, BrowserJobEngine::PythonEnvironment),
            Self::WebBuild => matches!(
                engine,
                BrowserJobEngine::StaticValidate | BrowserJobEngine::Esbuild
            ),
            Self::WebValidate => matches!(engine, BrowserJobEngine::StaticValidate),
            Self::PresentationExecute => false,
        }
    }

    pub fn capability_key(self) -> CapabilityKey {
        CapabilityKey::from_versioned_key(
            VersionedKey::new(self.as_str()).expect("closed Browser provider key is valid"),
        )
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserSecurityProfile {
    /// No networking.
    #[default]
    SafeOffline,
    /// Network access exclusively through the server egress policy.
    ProxyEgress,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserApplicationRoutingMode {
    MultiPage,
    Spa,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserApplicationPreviewProfile {
    StaticSafe,
    InteractiveIsolated,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserApplicationBuildContract {
    pub routing_mode: BrowserApplicationRoutingMode,
    pub routing_fallback: Option<String>,
    pub preview_profile: BrowserApplicationPreviewProfile,
    pub source_revision_hash: Sha256,
}

impl BrowserApplicationBuildContract {
    fn validate(&self) -> Result<(), BrowserJobContractError> {
        match (&self.routing_mode, &self.routing_fallback) {
            (BrowserApplicationRoutingMode::MultiPage, Some(_)) => {
                Err(BrowserJobContractError::InvalidRequest)
            }
            (BrowserApplicationRoutingMode::Spa, Some(path)) => validate_job_path(path),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserPythonEnvironmentBinding {
    pub installation_id: BrowserJobId,
    pub revision_hash: Sha256,
    pub scope_key: Sha256,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrowserJobProgram {
    PythonWorkspace {
        execution_workspace_id: musteragent_identity::ExecutionWorkspaceId,
        source_ref: ContentRef,
        workspace_ref: ContentRef,
        environment_manifest_ref: Option<ContentRef>,
        entrypoint: String,
        environment: Option<BrowserPythonEnvironmentBinding>,
    },
    PythonEnvironment {
        requirements: Vec<String>,
        requires_python: Option<String>,
        scope_key: Sha256,
    },
    Presentation {
        bundle_ref: ContentRef,
        component_generation: Option<Sha256>,
    },
    Python {
        source_ref: ContentRef,
        entrypoint: String,
        environment: Option<BrowserPythonEnvironmentBinding>,
        require_prepared_environment: bool,
    },
    StaticValidate {
        source_tree_ref: ContentRef,
        application: BrowserApplicationBuildContract,
    },
    Esbuild {
        source_tree_ref: ContentRef,
        dependency_lock_ref: ContentRef,
        application: BrowserApplicationBuildContract,
    },
}

impl BrowserJobProgram {
    #[must_use]
    pub const fn engine(&self) -> BrowserJobEngine {
        match self {
            Self::Presentation { .. } => BrowserJobEngine::Presentation,
            Self::Python { .. } | Self::PythonWorkspace { .. } => BrowserJobEngine::Python,
            Self::PythonEnvironment { .. } => BrowserJobEngine::PythonEnvironment,
            Self::StaticValidate { .. } => BrowserJobEngine::StaticValidate,
            Self::Esbuild { .. } => BrowserJobEngine::Esbuild,
        }
    }

    fn content_refs(&self) -> Vec<&ContentRef> {
        match self {
            Self::Presentation { bundle_ref, .. } => vec![bundle_ref],
            Self::Python { source_ref, .. } => vec![source_ref],
            Self::PythonWorkspace {
                source_ref,
                workspace_ref,
                environment_manifest_ref,
                ..
            } => {
                let mut refs = vec![source_ref, workspace_ref];
                refs.extend(environment_manifest_ref);
                refs
            }
            Self::PythonEnvironment { .. } => vec![],
            Self::StaticValidate {
                source_tree_ref, ..
            } => vec![source_tree_ref],
            Self::Esbuild {
                source_tree_ref,
                dependency_lock_ref,
                ..
            } => vec![source_tree_ref, dependency_lock_ref],
        }
    }

    fn validate(&self) -> Result<(), BrowserJobContractError> {
        match self {
            Self::Presentation { bundle_ref, .. } => {
                if bundle_ref.media_type.as_str()
                    != "application/vnd.musteragent.presentation-bundle"
                {
                    return Err(BrowserJobContractError::InvalidRequest);
                }
                Ok(())
            }
            Self::Python { .. } => Ok(()),
            Self::PythonWorkspace {
                source_ref,
                workspace_ref,
                entrypoint,
                ..
            } => {
                if source_ref.media_type.as_str() != "text/x-python"
                    || workspace_ref.media_type.as_str() != "application/json"
                    || !entrypoint.ends_with(".py")
                {
                    return Err(BrowserJobContractError::InvalidRequest);
                }
                validate_job_path(entrypoint)
            }
            Self::PythonEnvironment {
                requirements,
                requires_python,
                ..
            } => {
                if requirements.is_empty()
                    || requirements.len() > 32
                    || requirements.iter().any(|item| {
                        item.is_empty()
                            || item.len() > 256
                            || !item.is_ascii()
                            || item.chars().any(char::is_control)
                            || item.contains('@')
                            || item.contains('/')
                            || item.contains('\\')
                    })
                    || requires_python.as_ref().is_some_and(|value| {
                        value.len() > 128
                            || !value.is_ascii()
                            || value.chars().any(char::is_control)
                    })
                {
                    return Err(BrowserJobContractError::InvalidRequest);
                }
                Ok(())
            }
            Self::StaticValidate { application, .. } | Self::Esbuild { application, .. } => {
                application.validate()
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserInput {
    pub path: String,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserDeclaredOutput {
    pub path: String,
    pub media_type: MediaType,
    pub maximum_byte_length: NonZeroU64,
    pub required: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrowserResourceLimits {
    pub wall_time_millis: NonZeroU64,
    pub maximum_input_bytes: NonZeroU64,
    pub maximum_output_bytes: NonZeroU64,
    pub maximum_files: NonZeroU32,
    pub maximum_stdout_bytes: NonZeroU64,
    pub maximum_stderr_bytes: NonZeroU64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserJobRequest {
    /// Exact original Runtime policy. None preserves historical fixed budgets.
    pub execution_liveness: Option<Arc<ExecutionLivenessPolicy>>,
    pub job_id: BrowserJobId,
    pub invocation_id: InvocationId,
    pub conversation_id: ConversationId,
    pub engine: BrowserJobEngine,
    pub runtime_release_ref: VersionedKey,
    pub program: BrowserJobProgram,
    pub inputs: Vec<BrowserInput>,
    pub declared_outputs: Vec<BrowserDeclaredOutput>,
    pub limits: BrowserResourceLimits,
    pub security_profile: BrowserSecurityProfile,
}

impl BrowserJobRequest {
    pub fn validate(&self) -> Result<(), BrowserJobContractError> {
        let workspace = matches!(self.program, BrowserJobProgram::PythonWorkspace { .. });
        if self.engine != self.program.engine()
            || self.inputs.len()
                > if workspace {
                    10_002
                } else {
                    MAX_BROWSER_JOB_INPUTS
                }
            || self.declared_outputs.len() > MAX_BROWSER_JOB_OUTPUTS
        {
            return Err(BrowserJobContractError::InvalidRequest);
        }
        self.program.validate()?;
        if workspace && !self.declared_outputs.is_empty() {
            return Err(BrowserJobContractError::InvalidRequest);
        }
        if self.engine == BrowserJobEngine::PythonEnvironment
            && (!self.inputs.is_empty()
                || (self.declared_outputs.len() > 1
                    || self.declared_outputs.first().is_some_and(|o| {
                        o.path != "environment-lock.json"
                            || o.media_type.as_str() != "application/json"
                            || !o.required
                    }))
                || self.limits.wall_time_millis.get() > 600_000)
        {
            return Err(BrowserJobContractError::InvalidRequest);
        }
        if let BrowserJobProgram::Presentation { bundle_ref, .. } = &self.program
            && (self.inputs.len() != 1
                || self.inputs[0].path != "request.mwp"
                || self.inputs[0].content_ref != *bundle_ref
                || self.declared_outputs.len() != 1
                || self.declared_outputs[0].path != "result.mwp"
                || self.declared_outputs[0].media_type != bundle_ref.media_type
                || !self.declared_outputs[0].required
                || self.limits.wall_time_millis.get() > 600_000
                || self.limits.maximum_input_bytes.get() > 276_824_076
                || self.limits.maximum_output_bytes.get() > 276_824_076)
        {
            return Err(BrowserJobContractError::InvalidRequest);
        }
        if matches!(self.program, BrowserJobProgram::Python { ref entrypoint, .. } if entrypoint.trim().is_empty() || entrypoint.len() > MAX_BROWSER_JOB_PATH_BYTES)
        {
            return Err(BrowserJobContractError::InvalidRequest);
        }

        let mut input_paths = BTreeSet::new();
        let mut input_refs = BTreeMap::new();
        let mut input_bytes = 0_u64;
        for input in &self.inputs {
            validate_job_path(&input.path)?;
            if !input_paths.insert(input.path.to_lowercase()) {
                return Err(BrowserJobContractError::DuplicatePath);
            }
            input_bytes = input_bytes
                .checked_add(input.content_ref.byte_length)
                .ok_or(BrowserJobContractError::ResourceLimitExceeded)?;
            if let Some(existing) =
                input_refs.insert(input.content_ref.content_id, &input.content_ref)
                && existing != &input.content_ref
            {
                return Err(BrowserJobContractError::ContentIdentityConflict);
            }
        }
        if input_bytes > self.limits.maximum_input_bytes.get()
            || self.inputs.len()
                > usize::try_from(self.limits.maximum_files.get()).unwrap_or(usize::MAX)
        {
            return Err(BrowserJobContractError::ResourceLimitExceeded);
        }
        if self
            .program
            .content_refs()
            .iter()
            .any(|reference| !input_refs.contains_key(&reference.content_id))
        {
            return Err(BrowserJobContractError::ProgramInputMissing);
        }

        let mut output_paths = BTreeSet::new();
        let mut declared_bytes = 0_u64;
        for output in &self.declared_outputs {
            validate_job_path(&output.path)?;
            if !output_paths.insert(output.path.to_lowercase()) {
                return Err(BrowserJobContractError::DuplicatePath);
            }
            declared_bytes = declared_bytes
                .checked_add(output.maximum_byte_length.get())
                .ok_or(BrowserJobContractError::ResourceLimitExceeded)?;
        }
        if declared_bytes > self.limits.maximum_output_bytes.get() {
            return Err(BrowserJobContractError::ResourceLimitExceeded);
        }
        if self.engine == BrowserJobEngine::Python {
            // The isolated backing stores are projected into one Python cwd.
            // Reject ambiguous file/directory aliases before dispatch.
            let paths: Vec<_> = self
                .inputs
                .iter()
                .map(|input| input.path.to_lowercase())
                .chain(
                    self.declared_outputs
                        .iter()
                        .map(|output| output.path.to_lowercase()),
                )
                .chain(std::iter::once("musteragent_io.py".to_owned()))
                .collect();
            for (index, left) in paths.iter().enumerate() {
                if paths[..index].iter().any(|right| {
                    left == right
                        || left.starts_with(&format!("{right}/"))
                        || right.starts_with(&format!("{left}/"))
                }) {
                    return Err(BrowserJobContractError::DuplicatePath);
                }
            }
        }
        Ok(())
    }

    pub fn validate_result(
        &self,
        result: &BrowserJobResult,
    ) -> Result<(), BrowserJobContractError> {
        self.validate()?;
        result.validate()?;
        if result.job_id != self.job_id {
            return Err(BrowserJobContractError::ResultJobMismatch);
        }
        let declared = self
            .declared_outputs
            .iter()
            .map(|output| (output.path.as_str(), output))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        let mut total = 0_u64;
        let workspace = matches!(self.program, BrowserJobProgram::PythonWorkspace { .. });
        for output in &result.outputs {
            if workspace {
                if !seen.insert(output.path.as_str())
                    || !(output.path.starts_with("work/")
                        || output.path.starts_with("project/")
                        || (output.path == "__workspace_delta__.json"
                            && output.content_ref.media_type.as_str() == "application/json"))
                {
                    return Err(BrowserJobContractError::OutputContractMismatch);
                }
                total = total
                    .checked_add(output.content_ref.byte_length)
                    .ok_or(BrowserJobContractError::ResourceLimitExceeded)?;
                continue;
            }
            let Some(contract) = declared.get(output.path.as_str()) else {
                return Err(BrowserJobContractError::UndeclaredOutput);
            };
            if !seen.insert(output.path.as_str())
                || output.content_ref.media_type != contract.media_type
                || output.content_ref.byte_length > contract.maximum_byte_length.get()
            {
                return Err(BrowserJobContractError::OutputContractMismatch);
            }
            total = total
                .checked_add(output.content_ref.byte_length)
                .ok_or(BrowserJobContractError::ResourceLimitExceeded)?;
        }
        if total > self.limits.maximum_output_bytes.get()
            || (workspace
                && (result.outputs.len() > self.limits.maximum_files.get() as usize + 1
                    || (result.status == BrowserJobStatus::Completed
                        && !seen.contains("__workspace_delta__.json"))))
            || (result.status == BrowserJobStatus::Completed
                && self
                    .declared_outputs
                    .iter()
                    .any(|output| output.required && !seen.contains(output.path.as_str())))
        {
            return Err(BrowserJobContractError::OutputContractMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserJobStatus {
    Completed,
    Failed,
    Cancelled,
    ResourceExceeded,
    Indeterminate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StagedBrowserOutput {
    pub path: String,
    pub content_ref: ContentRef,
    /// Opaque staging receipt reference. It is revalidated by Runtime content
    /// authority and is never accepted as an object-store key.
    pub staging_receipt_ref: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BrowserDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserDiagnostic {
    pub code: String,
    pub severity: BrowserDiagnosticSeverity,
    pub message: String,
    pub path: Option<String>,
    pub line: Option<NonZeroU32>,
    pub column: Option<NonZeroU32>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrowserJobMetrics {
    pub wall_time_millis: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub output_files: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserJobResult {
    pub job_id: BrowserJobId,
    pub status: BrowserJobStatus,
    pub outputs: Vec<StagedBrowserOutput>,
    pub diagnostics: Vec<BrowserDiagnostic>,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub metrics: BrowserJobMetrics,
    pub result_hash: Sha256,
}

impl BrowserJobResult {
    /// Computes the domain-separated digest over every durable terminal field.
    /// Staged output bytes are covered transitively by their immutable
    /// `ContentRef`; the opaque staging receipt is covered directly.
    #[must_use]
    pub fn canonical_hash(&self) -> Sha256 {
        let mut material = Vec::new();
        material.extend_from_slice(b"musteragent.browser-job-result.v1\0");
        push_bytes(&mut material, self.job_id.into_uuid().as_bytes());
        material.push(match self.status {
            BrowserJobStatus::Completed => 1,
            BrowserJobStatus::Failed => 2,
            BrowserJobStatus::Cancelled => 3,
            BrowserJobStatus::ResourceExceeded => 4,
            BrowserJobStatus::Indeterminate => 5,
        });
        push_u64(&mut material, self.outputs.len() as u64);
        for output in &self.outputs {
            push_text(&mut material, &output.path);
            push_bytes(
                &mut material,
                output.content_ref.content_id.into_uuid().as_bytes(),
            );
            push_text(&mut material, output.content_ref.media_type.as_str());
            push_u64(&mut material, output.content_ref.byte_length);
            push_bytes(&mut material, &output.content_ref.sha256.into_bytes());
            push_text(&mut material, &output.staging_receipt_ref);
        }
        push_u64(&mut material, self.diagnostics.len() as u64);
        for diagnostic in &self.diagnostics {
            push_text(&mut material, &diagnostic.code);
            material.push(match diagnostic.severity {
                BrowserDiagnosticSeverity::Info => 1,
                BrowserDiagnosticSeverity::Warning => 2,
                BrowserDiagnosticSeverity::Error => 3,
            });
            push_text(&mut material, &diagnostic.message);
            push_optional_text(&mut material, diagnostic.path.as_deref());
            push_optional_u32(&mut material, diagnostic.line.map(NonZeroU32::get));
            push_optional_u32(&mut material, diagnostic.column.map(NonZeroU32::get));
        }
        push_text(&mut material, &self.stdout_tail);
        push_text(&mut material, &self.stderr_tail);
        push_u64(&mut material, self.metrics.wall_time_millis);
        push_u64(&mut material, self.metrics.input_bytes);
        push_u64(&mut material, self.metrics.output_bytes);
        material.extend_from_slice(&self.metrics.output_files.to_be_bytes());
        Sha256::digest(material)
    }

    /// Seals a freshly constructed result with its canonical terminal digest.
    #[must_use]
    pub fn with_canonical_hash(mut self) -> Self {
        self.result_hash = self.canonical_hash();
        self
    }

    pub fn validate(&self) -> Result<(), BrowserJobContractError> {
        if self.outputs.len() > 10_001
            || self.diagnostics.len() > MAX_BROWSER_JOB_DIAGNOSTICS
            || self.stdout_tail.len() > MAX_BROWSER_JOB_LOG_TAIL_BYTES
            || self.stderr_tail.len() > MAX_BROWSER_JOB_LOG_TAIL_BYTES
            || usize::try_from(self.metrics.output_files).unwrap_or(usize::MAX)
                != self.outputs.len()
            || self.metrics.output_bytes
                != self
                    .outputs
                    .iter()
                    .try_fold(0_u64, |total, output| {
                        total.checked_add(output.content_ref.byte_length)
                    })
                    .ok_or(BrowserJobContractError::ResourceLimitExceeded)?
        {
            return Err(BrowserJobContractError::InvalidResult);
        }
        for output in &self.outputs {
            validate_job_path(&output.path)?;
            if output.staging_receipt_ref.is_empty()
                || output.staging_receipt_ref.len() > MAX_BROWSER_RECEIPT_REF_BYTES
                || !output.staging_receipt_ref.is_ascii()
                || output.staging_receipt_ref.chars().any(char::is_whitespace)
            {
                return Err(BrowserJobContractError::InvalidReceipt);
            }
        }
        for diagnostic in &self.diagnostics {
            if diagnostic.code.is_empty()
                || diagnostic.code.len() > 128
                || diagnostic.message.trim().is_empty()
                || diagnostic.message.len() > 4_096
            {
                return Err(BrowserJobContractError::InvalidDiagnostic);
            }
            if let Some(path) = &diagnostic.path {
                validate_job_path(path)?;
            }
        }
        // Terminal failures may carry bounded partial outputs. Their paths,
        // ContentRefs, metrics and canonical digest obey the same contract.
        if self.result_hash != self.canonical_hash() {
            return Err(BrowserJobContractError::InvalidResultHash);
        }
        Ok(())
    }
}

fn push_u64(target: &mut Vec<u8>, value: u64) {
    target.extend_from_slice(&value.to_be_bytes());
}

fn push_bytes(target: &mut Vec<u8>, value: &[u8]) {
    push_u64(target, value.len() as u64);
    target.extend_from_slice(value);
}

fn push_text(target: &mut Vec<u8>, value: &str) {
    push_bytes(target, value.as_bytes());
}

fn push_optional_text(target: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            target.push(1);
            push_text(target, value);
        }
        None => target.push(0),
    }
}

fn push_optional_u32(target: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            target.push(1);
            target.extend_from_slice(&value.to_be_bytes());
        }
        None => target.push(0),
    }
}

fn validate_job_path(value: &str) -> Result<(), BrowserJobContractError> {
    if value.is_empty()
        || value.len() > MAX_BROWSER_JOB_PATH_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || value
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err(BrowserJobContractError::InvalidPath);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum BrowserJobContractError {
    #[error("Browser provider lease is invalid")]
    InvalidLease,
    #[error("Browser Job request is invalid")]
    InvalidRequest,
    #[error("unknown Browser provider contract")]
    UnknownProviderContract,
    #[error("Browser Runtime feature probe is invalid")]
    InvalidFeatureProbe,
    #[error("Browser Job result is invalid")]
    InvalidResult,
    #[error("Browser Job result hash does not match its terminal fields")]
    InvalidResultHash,
    #[error("Browser Job path is not canonical")]
    InvalidPath,
    #[error("Browser Job paths must be unique within each VFS namespace")]
    DuplicatePath,
    #[error("Browser Job program is not present in the authorized input set")]
    ProgramInputMissing,
    #[error("Browser Job reuses one ContentId with conflicting immutable identity")]
    ContentIdentityConflict,
    #[error("Browser Job resource limit was exceeded")]
    ResourceLimitExceeded,
    #[error("Browser Job result belongs to another Job")]
    ResultJobMismatch,
    #[error("Browser Job returned an undeclared output")]
    UndeclaredOutput,
    #[error("Browser Job output does not satisfy its declaration")]
    OutputContractMismatch,
    #[error("Browser Job staging receipt is invalid")]
    InvalidReceipt,
    #[error("Browser Job diagnostic is invalid")]
    InvalidDiagnostic,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{BrowserJobId, ContentId, InvocationId, VersionedKey};

    use super::*;

    fn content(name: &str, media_type: &str) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media type"),
            byte_length: u64::try_from(name.len()).expect("fixture length"),
            sha256: Sha256::digest(name),
        }
    }

    fn request() -> BrowserJobRequest {
        let source = content("print('ok')", "text/x-python");
        BrowserJobRequest {
            execution_liveness: None,
            job_id: BrowserJobId::generate(),
            invocation_id: InvocationId::generate(),
            conversation_id: ConversationId::generate(),
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
            program: BrowserJobProgram::Python {
                source_ref: source.clone(),
                entrypoint: "main.py".to_owned(),
                environment: None,
                require_prepared_environment: false,
            },
            inputs: vec![BrowserInput {
                path: "main.py".to_owned(),
                content_ref: source,
            }],
            declared_outputs: vec![BrowserDeclaredOutput {
                path: "result.csv".to_owned(),
                media_type: MediaType::new("text/csv").expect("media type"),
                maximum_byte_length: NonZeroU64::new(1_024).expect("limit"),
                required: true,
            }],
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(120_000).expect("limit"),
                maximum_input_bytes: NonZeroU64::new(1_024).expect("limit"),
                maximum_output_bytes: NonZeroU64::new(1_024).expect("limit"),
                maximum_files: NonZeroU32::new(10).expect("limit"),
                maximum_stdout_bytes: NonZeroU64::new(1_024).expect("limit"),
                maximum_stderr_bytes: NonZeroU64::new(1_024).expect("limit"),
            },
            security_profile: BrowserSecurityProfile::SafeOffline,
        }
    }

    #[test]
    fn python_job_rejects_ambiguous_working_tree_paths() {
        for path in [
            "main.py",
            "MAIN.py/child.csv",
            "musteragent_io.py",
            "musteragent_io.py/data",
        ] {
            let mut job = request();
            job.declared_outputs[0].path = path.to_owned();
            assert_eq!(
                job.validate(),
                Err(BrowserJobContractError::DuplicatePath),
                "{path}"
            );
        }
        let mut job = request();
        job.inputs.push(BrowserInput {
            path: "data".to_owned(),
            content_ref: content("x", "text/plain"),
        });
        job.declared_outputs[0].path = "data/result.csv".to_owned();
        assert_eq!(job.validate(), Err(BrowserJobContractError::DuplicatePath));
        job.inputs[1].path = "data/input.csv".to_owned();
        assert!(job.validate().is_ok());
    }

    #[test]
    fn request_rejects_program_outside_authorized_inputs() {
        let mut value = request();
        value.inputs.clear();
        assert_eq!(
            value.validate(),
            Err(BrowserJobContractError::ProgramInputMissing)
        );
    }

    #[test]
    fn python_request_rejects_output_overlapping_an_input_path() {
        let mut value = request();
        value.declared_outputs[0].path = "main.py".to_owned();
        assert_eq!(
            value.validate(),
            Err(BrowserJobContractError::DuplicatePath)
        );
    }

    #[test]
    fn result_must_match_declared_path_media_size_and_job() {
        let request = request();
        let output = content("a,b\n1,2\n", "text/csv");
        let result = BrowserJobResult {
            job_id: request.job_id,
            status: BrowserJobStatus::Completed,
            metrics: BrowserJobMetrics {
                wall_time_millis: 10,
                input_bytes: 11,
                output_bytes: output.byte_length,
                output_files: 1,
            },
            outputs: vec![StagedBrowserOutput {
                path: "result.csv".to_owned(),
                content_ref: output,
                staging_receipt_ref: "browser-stage-v1.receipt".to_owned(),
            }],
            diagnostics: Vec::new(),
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            result_hash: Sha256::digest(b"unsealed"),
        }
        .with_canonical_hash();
        assert_eq!(request.validate_result(&result), Ok(()));

        let mut tampered = result.clone();
        tampered.stderr_tail = "changed after sealing".to_owned();
        assert_eq!(
            request.validate_result(&tampered),
            Err(BrowserJobContractError::InvalidResultHash)
        );

        let mut wrong = result;
        wrong.outputs[0].path = "other.csv".to_owned();
        let wrong = wrong.with_canonical_hash();
        assert_eq!(
            request.validate_result(&wrong),
            Err(BrowserJobContractError::UndeclaredOutput)
        );
    }

    #[test]
    fn provider_contract_is_closed_and_engine_specific() {
        let working_tree = BrowserProviderContract::PythonExecuteWorkingTree;
        assert_eq!(
            BrowserProviderContract::parse(working_tree.as_str()),
            Ok(working_tree)
        );
        assert!(working_tree.accepts_engine(BrowserJobEngine::Python));
        assert!(!working_tree.accepts_engine(BrowserJobEngine::Esbuild));
        assert_eq!(
            BrowserProviderContract::parse("browser.python.execute@1").expect("contract"),
            BrowserProviderContract::PythonExecute
        );
        assert!(BrowserProviderContract::WebBuild.accepts_engine(BrowserJobEngine::Esbuild));
        assert!(BrowserProviderContract::WebBuild.accepts_engine(BrowserJobEngine::StaticValidate));
        assert!(!BrowserProviderContract::WebBuild.accepts_engine(BrowserJobEngine::Python));
        assert!(matches!(
            BrowserProviderContract::parse("browser.shell.execute@1"),
            Err(BrowserJobContractError::UnknownProviderContract)
        ));
    }

    #[test]
    fn terminal_hash_is_stable_across_language_implementations() {
        let result = BrowserJobResult {
            job_id: BrowserJobId::from_uuid(
                "019fa6de-38fd-7000-8000-000000000007"
                    .parse()
                    .expect("UUID"),
            )
            .expect("Browser Job id"),
            status: BrowserJobStatus::Failed,
            outputs: Vec::new(),
            diagnostics: vec![BrowserDiagnostic {
                code: "validation_failed".to_owned(),
                severity: BrowserDiagnosticSeverity::Error,
                message: "fixture failure".to_owned(),
                path: None,
                line: None,
                column: None,
            }],
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            metrics: BrowserJobMetrics {
                wall_time_millis: 1,
                input_bytes: 0,
                output_bytes: 0,
                output_files: 0,
            },
            result_hash: Sha256::digest(b"unsealed"),
        }
        .with_canonical_hash();

        assert_eq!(
            result.result_hash.to_string(),
            "df080fd27aef0abb131ba2ff4e52bf4e8a8c72580de3e093b85a4ae441470336"
        );
    }
}
