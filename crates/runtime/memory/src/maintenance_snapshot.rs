use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_evolution::DreamWorkload;
use musteragent_identity::{
    AgentId, AgentVersionId, DottedKey, DreamJobId, InvocationId, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_model::{CredentialGrantRef, ModelRoute, SignedModelRouteHash};
use thiserror::Error;

use crate::{DreamModelRunRequest, is_dream_model_input_media_type};

pub const MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION: &str =
    "musteragent.runtime.maintenance-snapshot.v3";

/// Immutable role-specific material for one built-in Dream Agent Run.
///
/// The shared Agent Loop owns attempts, continuation, Tool turns, cancellation,
/// persistence, recovery and metering. This content only freezes the Dream
/// role identity, evidence input and independently selected Evolution route.
#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceRuntimeSnapshotContent {
    pub schema_version: DottedKey,
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub agent_version_id: AgentVersionId,
    pub agent_contract: VersionedKey,
    pub policy_version_ref: VersionedKey,
    pub growth_profile_ref: VersionedKey,
    pub growth_profile_hash: Sha256,
    pub evolution_route: ModelRoute,
    pub credential_grant_ref: CredentialGrantRef,
    pub dream_job_id: DreamJobId,
    pub invocation_id: InvocationId,
    pub input_manifest_ref: ContentRef,
    pub model_input_ref: ContentRef,
    pub workload: DreamWorkload,
    pub deadline: UtcTimestamp,
    pub execution_policy: Option<musteragent_runtime_snapshot::RunExecutionPolicySnapshot>,
}

impl MaintenanceRuntimeSnapshotContent {
    /// Current maintenance runs always use their sealed activity policy.
    pub fn invocation_timing(
        &self,
    ) -> Result<
        musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
        MaintenanceSnapshotContractError,
    > {
        self.validate()?;
        self.execution_policy
            .ok_or(MaintenanceSnapshotContractError::UnsupportedSchema)?
            .model_invocation_timing(self.deadline)
            .map_err(|_| MaintenanceSnapshotContractError::UnsupportedSchema)
    }
    pub fn validate(&self) -> Result<(), MaintenanceSnapshotContractError> {
        self.evolution_route
            .validate()
            .map_err(|_| MaintenanceSnapshotContractError::InvalidRoute)?;
        self.workload
            .validate()
            .map_err(|_| MaintenanceSnapshotContractError::InvalidWorkload)?;
        if self.schema_version.as_str() != MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION
            || !self
                .execution_policy
                .is_some_and(|policy| policy.liveness.is_some() && policy.validate().is_ok())
        {
            return Err(MaintenanceSnapshotContractError::UnsupportedSchema);
        }
        if !self.evolution_route.fallback_route_refs.is_empty() {
            return Err(MaintenanceSnapshotContractError::FallbackRouteForbidden);
        }
        if self.input_manifest_ref == self.model_input_ref
            || self.input_manifest_ref.byte_length == 0
            || self.model_input_ref.byte_length == 0
            || !is_dream_model_input_media_type(self.model_input_ref.media_type.as_str())
        {
            return Err(MaintenanceSnapshotContractError::InvalidInputBinding);
        }
        if self.workload.evidence_context_tokens > self.evolution_route.context_window.get() {
            return Err(MaintenanceSnapshotContractError::RouteCapacityExceeded);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &DreamModelRunRequest,
    ) -> Result<(), MaintenanceSnapshotContractError> {
        self.validate()?;
        request
            .validate()
            .map_err(|_| MaintenanceSnapshotContractError::RequestMismatch)?;
        let route_hash = SignedModelRouteHash::digest(&self.evolution_route)
            .map_err(|_| MaintenanceSnapshotContractError::InvalidRoute)?;
        if self.tenant_id != request.tenant_id
            || self.target_agent_id != request.target_agent_id
            || self.agent_version_id != request.agent_version_id
            || self.policy_version_ref != request.policy_version_ref
            || self.growth_profile_ref != request.growth_profile_ref
            || self.growth_profile_hash != request.growth_profile_hash
            || self.evolution_route.route_ref != request.model_route_ref
            || route_hash.into_bytes() != request.model_route_hash.into_bytes()
            || self.dream_job_id != request.dream_job_id
            || self.invocation_id != request.invocation_id
            || self.input_manifest_ref != request.input_manifest_ref
            || self.model_input_ref != request.model_input_ref
            || self.workload != request.workload
            || self.deadline != request.deadline
        {
            return Err(MaintenanceSnapshotContractError::RequestMismatch);
        }
        Ok(())
    }
}

/// Protocol-neutral codec boundary. The production implementation is the
/// deterministic Protobuf adapter; Runtime Memory owns the typed contract.
pub trait MaintenanceSnapshotCodec: Send + Sync {
    type Error: Send;

    fn encode_content(
        &self,
        content: &MaintenanceRuntimeSnapshotContent,
    ) -> Result<Vec<u8>, Self::Error>;

    fn decode_content(
        &self,
        bytes: &[u8],
    ) -> Result<MaintenanceRuntimeSnapshotContent, Self::Error>;
}

/// Sealed, content-addressed Maintenance Snapshot.
///
/// Fields stay private so callers cannot mutate typed content independently
/// from the bytes and hash persisted for crash recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct MaintenanceRuntimeSnapshot {
    content: MaintenanceRuntimeSnapshotContent,
    encoded_content: Vec<u8>,
    content_hash: Sha256,
    created_at: UtcTimestamp,
}

impl MaintenanceRuntimeSnapshot {
    pub fn seal<C: MaintenanceSnapshotCodec>(
        content: MaintenanceRuntimeSnapshotContent,
        created_at: UtcTimestamp,
        codec: &C,
    ) -> Result<Self, MaintenanceSnapshotSealError<C::Error>> {
        content
            .validate()
            .map_err(MaintenanceSnapshotSealError::Contract)?;
        let encoded_content = codec
            .encode_content(&content)
            .map_err(MaintenanceSnapshotSealError::Codec)?;
        if encoded_content.is_empty() {
            return Err(MaintenanceSnapshotSealError::Contract(
                MaintenanceSnapshotContractError::EmptyEncoding,
            ));
        }
        Ok(Self {
            content,
            content_hash: Sha256::digest(&encoded_content),
            encoded_content,
            created_at,
        })
    }

    pub fn restore<C: MaintenanceSnapshotCodec>(
        encoded_content: Vec<u8>,
        content_hash: Sha256,
        created_at: UtcTimestamp,
        codec: &C,
    ) -> Result<Self, MaintenanceSnapshotRestoreError<C::Error>> {
        if encoded_content.is_empty() {
            return Err(MaintenanceSnapshotRestoreError::Contract(
                MaintenanceSnapshotContractError::EmptyEncoding,
            ));
        }
        if Sha256::digest(&encoded_content) != content_hash {
            return Err(MaintenanceSnapshotRestoreError::ContentHashMismatch);
        }
        let content = codec
            .decode_content(&encoded_content)
            .map_err(MaintenanceSnapshotRestoreError::Codec)?;
        content
            .validate()
            .map_err(MaintenanceSnapshotRestoreError::Contract)?;
        Ok(Self {
            content,
            encoded_content,
            content_hash,
            created_at,
        })
    }

    #[must_use]
    pub const fn content(&self) -> &MaintenanceRuntimeSnapshotContent {
        &self.content
    }

    #[must_use]
    pub fn encoded_content(&self) -> &[u8] {
        &self.encoded_content
    }

    #[must_use]
    pub const fn content_hash(&self) -> Sha256 {
        self.content_hash
    }

    #[must_use]
    pub const fn created_at(&self) -> UtcTimestamp {
        self.created_at
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum MaintenanceSnapshotContractError {
    #[error("Maintenance Runtime Snapshot schema is unsupported")]
    UnsupportedSchema,
    #[error("Maintenance Runtime Snapshot encoding cannot be empty")]
    EmptyEncoding,
    #[error("Maintenance Runtime Snapshot Evolution route is invalid")]
    InvalidRoute,
    #[error("Maintenance Runtime Snapshot cannot contain model fallbacks")]
    FallbackRouteForbidden,
    #[error("Maintenance Runtime Snapshot Dream workload is invalid")]
    InvalidWorkload,
    #[error("Maintenance Runtime Snapshot input binding is invalid")]
    InvalidInputBinding,
    #[error("Maintenance Runtime Snapshot workload exceeds the Evolution route capacity")]
    RouteCapacityExceeded,
    #[error("Maintenance Runtime Snapshot does not bind the exact Dream request")]
    RequestMismatch,
}

#[derive(Debug, Error)]
pub enum MaintenanceSnapshotSealError<E> {
    #[error(transparent)]
    Contract(#[from] MaintenanceSnapshotContractError),
    #[error("Maintenance Runtime Snapshot codec rejected the content")]
    Codec(E),
}

#[derive(Debug, Error)]
pub enum MaintenanceSnapshotRestoreError<E> {
    #[error(transparent)]
    Contract(#[from] MaintenanceSnapshotContractError),
    #[error("Maintenance Runtime Snapshot content hash does not match its bytes")]
    ContentHashMismatch,
    #[error("Maintenance Runtime Snapshot codec rejected the bytes")]
    Codec(E),
}
