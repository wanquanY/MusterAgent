//! Executable capability definitions, provider bindings and Run-pinned grants.

#![forbid(unsafe_code)]

mod browser;

pub use browser::{
    BROWSER_JOB_RESULT_SCHEMA_VERSION, BROWSER_JOB_SCHEMA_VERSION, BrowserApplicationBuildContract,
    BrowserApplicationPreviewProfile, BrowserApplicationRoutingMode, BrowserDeclaredOutput,
    BrowserDiagnostic, BrowserDiagnosticSeverity, BrowserInput, BrowserJobContractError,
    BrowserJobEngine, BrowserJobMetrics, BrowserJobProgram, BrowserJobRequest, BrowserJobResult,
    BrowserJobStatus, BrowserLeaseProof, BrowserProviderContract, BrowserProviderLease,
    BrowserPythonEnvironmentBinding, BrowserResourceLimits, BrowserRuntimeFeatureProbe,
    BrowserSecurityProfile, MAX_BROWSER_JOB_DIAGNOSTICS, MAX_BROWSER_JOB_LOG_TAIL_BYTES,
    MAX_BROWSER_JOB_PATH_BYTES, MAX_BROWSER_PROVIDER_CAPABILITIES, StagedBrowserOutput,
};

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use musteragent_identity::{CapabilityId, CapabilityProviderId, DottedKey, VersionedKey};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Effect, EgressPolicy, RequiredContext,
    UnsafeApprovalPolicy, validate_approval_floor,
};
use musteragent_runtime_contract::RetryDisposition;
use thiserror::Error;

/// Stable, explicitly versioned Capability key.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CapabilityKey(VersionedKey);

impl CapabilityKey {
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

/// Where an implementation is allowed to execute.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CapabilityLocation {
    Cloud,
    Backend,
    Device,
    RemoteDevice,
    /// Short-lived authenticated Web page provider. Browser capabilities have
    /// no OS, shell, native secret or unattended execution facet.
    Browser,
}

/// Recovery behavior for an external side effect.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IdempotencyContract {
    ReadOnly,
    IdempotentByKey,
    QueryBeforeRetry,
    NotReplayable,
}

impl IdempotencyContract {
    #[must_use]
    pub const fn after_unknown_outcome(self) -> RetryDisposition {
        match self {
            Self::ReadOnly | Self::IdempotentByKey => RetryDisposition::RetryNewAttempt,
            Self::QueryBeforeRetry => RetryDisposition::ReconcileBeforeRetry,
            Self::NotReplayable => RetryDisposition::DoNotRetry,
        }
    }
}

/// Versioned control-plane definition; never contains credentials or handles.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityManifest {
    pub capability_id: CapabilityId,
    pub key: CapabilityKey,
    pub effect: Effect,
    pub approval: ApprovalRequirement,
    pub location: CapabilityLocation,
    pub provider_id: CapabilityProviderId,
    pub timeout: Duration,
    pub max_attempts: NonZeroU32,
    pub idempotency: IdempotencyContract,
    pub required_scopes: BTreeSet<DottedKey>,
    pub required_contexts: BTreeSet<RequiredContext>,
    pub data_classification: DataClassification,
    pub egress_policy: EgressPolicy,
}

impl CapabilityManifest {
    /// Rejects unsafe policy and incoherent retry declarations.
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        validate_approval_floor(self.effect, self.approval)?;
        if self.timeout.is_zero() {
            return Err(CapabilityContractError::ZeroTimeout);
        }
        if self.required_scopes.is_empty() {
            return Err(CapabilityContractError::MissingAuthorizationScope);
        }
        match (self.effect, self.idempotency) {
            (Effect::Read, IdempotencyContract::ReadOnly) => Ok(()),
            (Effect::Read, _) => Err(CapabilityContractError::ReadMustBeReadOnly),
            (_, IdempotencyContract::ReadOnly) => {
                Err(CapabilityContractError::WriteCannotBeReadOnly)
            }
            _ => Ok(()),
        }
    }
}

/// Immutable provider grant pinned into a Run.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityGrantSnapshot {
    pub key: CapabilityKey,
    pub provider_id: CapabilityProviderId,
    pub location: CapabilityLocation,
    pub effect: Effect,
    pub allowed_scopes: BTreeSet<DottedKey>,
    pub policy_decision: musteragent_policy::Decision,
}

/// Control-plane authorization signed into one Execution Grant.
///
/// Unlike an installed Capability manifest, an entitlement is a tenant/Run
/// decision. Admission may pin only exact allowed or approval-obligated
/// entries; denied entries remain available solely to explain carry-forward
/// rejection and must never enter the Authorized Catalog.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CapabilityEntitlement {
    pub key: CapabilityKey,
    pub provider_id: CapabilityProviderId,
    pub location: CapabilityLocation,
    pub effect: Effect,
    pub allowed_scopes: BTreeSet<DottedKey>,
    pub policy_decision: musteragent_policy::Decision,
}

impl CapabilityEntitlement {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.policy_decision != musteragent_policy::Decision::Deny
            && self.allowed_scopes.is_empty()
        {
            return Err(CapabilityContractError::MissingAuthorizationScope);
        }
        if self.policy_decision == musteragent_policy::Decision::Deny
            && !self.allowed_scopes.is_empty()
        {
            return Err(CapabilityContractError::DeniedEntitlementHasScopes);
        }
        Ok(())
    }

    #[must_use]
    pub fn grant_snapshot(&self) -> Option<CapabilityGrantSnapshot> {
        (self.policy_decision != musteragent_policy::Decision::Deny).then(|| {
            CapabilityGrantSnapshot {
                key: self.key.clone(),
                provider_id: self.provider_id,
                location: self.location,
                effect: self.effect,
                allowed_scopes: self.allowed_scopes.clone(),
                policy_decision: self.policy_decision,
            }
        })
    }
}

/// A Capability definition would weaken policy or recovery semantics.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum CapabilityContractError {
    #[error(transparent)]
    UnsafeApproval(#[from] UnsafeApprovalPolicy),
    #[error("capability requires at least one authorization scope")]
    MissingAuthorizationScope,
    #[error("a denied Capability entitlement cannot carry allowed scopes")]
    DeniedEntitlementHasScopes,
    #[error("capability timeout must be positive")]
    ZeroTimeout,
    #[error("read capability must declare read-only idempotency")]
    ReadMustBeReadOnly,
    #[error("write capability cannot declare read-only idempotency")]
    WriteCannotBeReadOnly,
}
