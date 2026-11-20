//! Model-visible Tool projection and invocation lifecycle.

#![forbid(unsafe_code)]

mod application_binding;
mod assembly;
pub use application_binding::{
    ApplicationImplementation, ApplicationToolBinding, HostedComponentPin,
};
/// Whether the original native workspace executor can dispatch this versioned Tool.
pub fn origin_device_tool(key: &str) -> bool {
    let Some((logical_key, version)) = key.rsplit_once('@') else {
        return false;
    };
    version.parse().is_ok_and(|version| {
        musteragent_runtime_contract::workspace_executor::supports_origin_tool(logical_key, version)
    })
}

mod catalog;
mod events;
mod execution_policy;
pub use execution_policy::ToolExecutionPolicy;
pub mod workspace;

pub use assembly::{
    CapabilityAssemblyEntry, CapabilityAssemblyError, CapabilityAssemblyPlan,
    CapabilityHandlerKind, ToolExecutionRequirement, requires_browser_execution,
};

pub use catalog::{
    BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_CLOUD_RELEASE_ID, BUILTIN_DEVICE_RELEASE_ID,
    BUILTIN_RELEASE_PROFILE, BUILTIN_RUST_PROVIDER_ID, BUILTIN_SKILL_RELEASE_SCHEMA,
    BackendOperation, BuiltinIdempotency, BuiltinToolCatalog, BuiltinToolDefinition,
    CapabilityExecutionTarget, ConcurrencyClass, FileOperationKind, FileOperationMetadata,
    HostProductRelease, HostPublicationError, MediaArtifactKind, ProductExecutionTarget,
    ProductReleaseMetadata, ProductToolDefinition, TaskResultProtocol, ToolCatalogError,
    compiled_product_release, compiled_product_releases_refs, install_host_product_releases,
    product_release_metadata, product_skill_source, require_product_release_admission,
    task_result_protocol, verified_task_results_for_release,
};
pub use events::ToolEvent;

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use musteragent_content_ref::{ContentRef, SchemaRef};
use musteragent_identity::{
    AttemptId, CapabilityProviderId, DottedKey, IdempotencyKey, InteractionId, InvocationId, RunId,
    ToolCallId, VersionedKey,
};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Effect, EgressPolicy, RequiredContext,
    UnsafeApprovalPolicy, validate_approval_floor,
};
use musteragent_runtime_contract::RetryDisposition;
use thiserror::Error;

/// Stable, explicitly versioned Tool key.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ToolKey(VersionedKey);

impl ToolKey {
    #[must_use]
    pub const fn from_versioned_key(value: VersionedKey) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn logical_key(&self) -> &str {
        self.0.logical_key()
    }

    #[must_use]
    pub fn version(&self) -> u32 {
        self.0.version()
    }
}

/// Canonical invocation name. Model-facing names are allocated separately by
/// the Discovery name catalog; this identity remains stable in durable invocation records.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WireName(String);

impl WireName {
    pub fn new(value: impl Into<String>) -> Result<Self, ToolContractError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !valid || !value.as_bytes()[0].is_ascii_lowercase() {
            return Err(ToolContractError::InvalidWireName);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Retry semantics projected to the model-visible Tool.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ToolRetryContract {
    ReadOnly,
    IdempotentByKey,
    QueryBeforeRetry,
    NotReplayable,
}

impl ToolRetryContract {
    /// Recovery decision after a Worker loses ownership while an external
    /// outcome is not yet proven.
    #[must_use]
    pub const fn after_unknown_outcome(self) -> RetryDisposition {
        match self {
            Self::ReadOnly | Self::IdempotentByKey => RetryDisposition::RetryNewAttempt,
            Self::QueryBeforeRetry => RetryDisposition::ReconcileBeforeRetry,
            Self::NotReplayable => RetryDisposition::DoNotRetry,
        }
    }
}

/// Immutable Tool definition pinned into a Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolManifest {
    pub application: Option<ApplicationToolBinding>,
    pub key: ToolKey,
    pub wire_name: WireName,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub capability_key: VersionedKey,
    pub capability_provider_id: CapabilityProviderId,
    pub effect: Effect,
    pub approval: ApprovalRequirement,
    pub execution_policy: ToolExecutionPolicy,
    pub max_attempts: NonZeroU32,
    pub retry_contract: ToolRetryContract,
    pub required_scopes: BTreeSet<DottedKey>,
    pub required_contexts: BTreeSet<RequiredContext>,
    pub data_classification: DataClassification,
    pub egress_policy: EgressPolicy,
}

impl ToolManifest {
    pub fn validate(&self) -> Result<(), ToolContractError> {
        if let Some(binding) = &self.application {
            binding
                .validate()
                .map_err(|_| ToolContractError::InvalidApplicationBinding)?;
            if binding
                .authority_role
                .as_ref()
                .is_some_and(|role| !role.matches_manifest(self))
            {
                return Err(ToolContractError::InvalidApplicationBinding);
            }
        }
        validate_approval_floor(self.effect, self.approval)?;
        self.execution_policy.validate()?;
        if self.required_scopes.is_empty() {
            return Err(ToolContractError::MissingAuthorizationScope);
        }
        match (self.effect, self.retry_contract) {
            (Effect::Read, ToolRetryContract::ReadOnly) => Ok(()),
            (Effect::Read, _) => Err(ToolContractError::ReadMustBeReadOnly),
            (_, ToolRetryContract::ReadOnly) => Err(ToolContractError::WriteCannotBeReadOnly),
            _ => Ok(()),
        }
    }
}

/// Canonical Tool invocation status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ToolInvocationStatus {
    Requested,
    WaitingInteraction,
    Running,
    Succeeded,
    Failed,
    Indeterminate,
    Cancelled,
}

impl ToolInvocationStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// Semantic Tool transition; callers cannot supply an arbitrary target status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ToolTransition {
    WaitForInteraction,
    Start,
    Succeed,
    Fail,
    MarkIndeterminate,
    Cancel,
}

/// Stable invocation identity and side-effect recovery fields.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ToolInvocation {
    pub invocation_id: InvocationId,
    pub run_id: RunId,
    pub requested_by_attempt_id: AttemptId,
    pub execution_attempt_id: Option<AttemptId>,
    pub tool_call_id: ToolCallId,
    pub tool_key: ToolKey,
    pub capability_provider_id: CapabilityProviderId,
    pub status: ToolInvocationStatus,
    pub effect: Effect,
    pub approval: ApprovalRequirement,
    pub idempotency_key: IdempotencyKey,
    pub request_ref: ContentRef,
    pub result_ref: Option<ContentRef>,
    pub interaction_id: Option<InteractionId>,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
}

impl ToolInvocation {
    /// Applies the closed invocation state machine.
    pub fn transition(&mut self, transition: ToolTransition) -> Result<(), ToolContractError> {
        let target = match (self.status, transition) {
            (ToolInvocationStatus::Requested, ToolTransition::WaitForInteraction) => {
                ToolInvocationStatus::WaitingInteraction
            }
            (
                ToolInvocationStatus::Requested | ToolInvocationStatus::WaitingInteraction,
                ToolTransition::Start,
            ) => ToolInvocationStatus::Running,
            (
                ToolInvocationStatus::Requested
                | ToolInvocationStatus::WaitingInteraction
                | ToolInvocationStatus::Running,
                ToolTransition::Fail,
            ) => ToolInvocationStatus::Failed,
            (
                ToolInvocationStatus::Requested
                | ToolInvocationStatus::WaitingInteraction
                | ToolInvocationStatus::Running,
                ToolTransition::Cancel,
            ) => ToolInvocationStatus::Cancelled,
            (ToolInvocationStatus::Running, ToolTransition::Succeed) => {
                ToolInvocationStatus::Succeeded
            }
            (ToolInvocationStatus::Running, ToolTransition::MarkIndeterminate) => {
                ToolInvocationStatus::Indeterminate
            }
            (ToolInvocationStatus::Indeterminate, ToolTransition::Start) => {
                ToolInvocationStatus::Running
            }
            (ToolInvocationStatus::Indeterminate, ToolTransition::Succeed) => {
                ToolInvocationStatus::Succeeded
            }
            (ToolInvocationStatus::Indeterminate, ToolTransition::Fail) => {
                ToolInvocationStatus::Failed
            }
            (ToolInvocationStatus::Indeterminate, ToolTransition::Cancel) => {
                ToolInvocationStatus::Cancelled
            }
            _ => return Err(ToolContractError::InvalidTransition),
        };
        match target {
            ToolInvocationStatus::WaitingInteraction if self.interaction_id.is_none() => {
                return Err(ToolContractError::MissingInteraction);
            }
            ToolInvocationStatus::Running if self.execution_attempt_id.is_none() => {
                return Err(ToolContractError::MissingExecutionAttempt);
            }
            ToolInvocationStatus::Succeeded if self.result_ref.is_none() => {
                return Err(ToolContractError::MissingResult);
            }
            _ => {}
        }
        self.status = target;
        Ok(())
    }
}

/// A Tool contract was unsafe or internally inconsistent.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ToolContractError {
    #[error("invalid sealed application release binding")]
    InvalidApplicationBinding,
    #[error("executor does not support the frozen execution policy")]
    UnsupportedExecutionPolicy,
    #[error(transparent)]
    UnsafeApproval(#[from] UnsafeApprovalPolicy),
    #[error("tool wire name must be lowercase ASCII, digits or underscore")]
    InvalidWireName,
    #[error("tool requires at least one authorization scope")]
    MissingAuthorizationScope,
    #[error("tool timeout must be positive")]
    ZeroTimeout,
    #[error("read tool must declare read-only retry behavior")]
    ReadMustBeReadOnly,
    #[error("write tool cannot declare read-only retry behavior")]
    WriteCannotBeReadOnly,
    #[error("invalid Tool invocation transition")]
    InvalidTransition,
    #[error("WaitingInteraction requires an Interaction ID")]
    MissingInteraction,
    #[error("Running requires the actual execution Attempt ID")]
    MissingExecutionAttempt,
    #[error("Succeeded requires an immutable result ContentRef")]
    MissingResult,
}

impl ToolContractError {
    /// Stable diagnostic code used by compatibility and transport layers.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidApplicationBinding => "invalid_application_binding",
            Self::UnsupportedExecutionPolicy => "unsupported_execution_policy",
            Self::UnsafeApproval(_) => "unsafe_approval",
            Self::InvalidWireName => "invalid_wire_name",
            Self::MissingAuthorizationScope => "missing_authorization_scope",
            Self::ZeroTimeout => "zero_timeout",
            Self::ReadMustBeReadOnly => "read_must_be_read_only",
            Self::WriteCannotBeReadOnly => "write_cannot_be_read_only",
            Self::InvalidTransition => "invalid_transition",
            Self::MissingInteraction => "missing_interaction",
            Self::MissingExecutionAttempt => "missing_execution_attempt",
            Self::MissingResult => "missing_result",
        }
    }
}
