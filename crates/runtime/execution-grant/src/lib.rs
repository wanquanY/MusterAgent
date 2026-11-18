//! Signed, immutable Runtime admission authorization across Model and Capability domains.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};

use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_event_envelope::HostedProjectScope;
use musteragent_identity::{
    DeviceId, DottedKey, PrincipalId, RunId, TenantId, UtcTimestamp, VersionedKey, WorkspaceId,
};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelContractError, ModelEntryId,
    ModelProviderId, ModelRoute, ModelRouteEncodingError, canonical_model_route_body,
};
use musteragent_policy::{Decision, Effect, PermissionMode};
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

mod compatibility;
pub use compatibility::NUMBERED_EXECUTION_GRANT_SCHEMAS;
mod admitted_transport;
pub use admitted_transport::{
    AdmittedTransportGrantError, validate_admitted_transport_head,
    validate_admitted_transport_successor,
};

/// Current execution grant: signed actor, authority scope, execution placement,
/// model routes and exact tool contracts. All older grant versions are rejected.
pub const EXECUTION_GRANT_SCHEMA_V10: &str = "musteragent.execution-grant.v10";
pub const TOOL_CONTRACT_RELEASE_SCHEMA_V1: &str = "musteragent.tool-contract-release.v1";
/// Purpose-specific autonomous learning grant. Unlike interactive
/// grants, its sole route is the Evolution route and it carries no product
/// Capability entitlements or auxiliary/primary route bundle. The route must
/// support Tool calling for the Runtime-private candidate submission protocol.
pub const EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1: &str =
    "musteragent.execution-grant.evolution-maintenance.v1";
/// System-Agent Evaluation grant for one Standing Goal cycle. It preserves the
/// source Product identity and frozen Evaluation route, supports the private
/// Inspection/Decision Tool protocol, and cannot authorize fallback,
/// auxiliary or ordinary product Capability use.
pub const STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1: &str =
    "musteragent.execution-grant.standing-goal-judge.v1";
pub const EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1: &str =
    "musteragent.execution-grant.proof.protobuf.v1";
pub const MAX_EXECUTION_GRANT_PROOF_BYTES: usize = 256 * 1024;

/// Route ref prefix owned by device-direct (BYOK) primary routes. A BYOK
/// route never resolves through the Admin Model Registry; the signed grant
/// carries every routing fact and the Device-local credential reference.
pub const DEVICE_DIRECT_ROUTE_REF_PREFIX: &str = "byok.primary.conn-";
/// Route ref prefix owned by the BYOK Compression auxiliary route. It is the
/// only auxiliary route a device-direct grant may carry, and it shares the
/// primary route's provider binding so context compaction executes on the
/// user's own model and credential without cloud metering.
pub const DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX: &str = "byok.compression.conn-";
/// Route ref prefix owned by a BYOK Vision auxiliary route. It is signed only
/// when the user configured a dedicated vision connection for a primary model
/// without native vision, and it is bound to the grant's
/// `vision_direct_provider` field rather than to the primary binding.
pub const DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX: &str = "byok.vision.conn-";
/// Unpriced sentinel shared with the Backend signer. The Device Runtime skips
/// metering publication for routes carrying this pricing ref, so BYOK
/// invocations are never charged.
pub use musteragent_model::DEVICE_DIRECT_PRICING_VERSION_REF;
/// Credential references handed to the Device-local credential broker.
pub const DEVICE_DIRECT_CREDENTIAL_REF_PREFIX: &str = "musteragent-secure://model-credentials/";

/// Signed device-direct (BYOK) provider binding. The control plane attests
/// these routing facts verbatim; the referenced credential never leaves the
/// Device and is leased by the local credential broker at invocation time.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeviceDirectProvider {
    pub base_url: String,
    pub api_format: ApiFormat,
    pub model: String,
    pub credential_ref: String,
}

impl DeviceDirectProvider {
    fn validate(&self) -> Result<(), ExecutionGrantContractError> {
        let invalid = || ExecutionGrantContractError::InvalidDeviceDirectProvider;
        if self.base_url.is_empty() || self.base_url.len() > 2048 {
            return Err(invalid());
        }
        let parsed = url::Url::parse(&self.base_url).map_err(|_| invalid())?;
        if parsed.cannot_be_a_base()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid());
        }
        let host = parsed.host_str().ok_or_else(invalid)?.to_ascii_lowercase();
        let loopback = matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1");
        match parsed.scheme() {
            "https" => {}
            "http" if loopback => {}
            _ => return Err(invalid()),
        }
        if self.model.is_empty() || self.model.len() > 200 {
            return Err(invalid());
        }
        let tail = self
            .credential_ref
            .strip_prefix(DEVICE_DIRECT_CREDENTIAL_REF_PREFIX)
            .ok_or_else(invalid)?;
        if tail.is_empty()
            || tail.len() > 200
            || !tail
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

const SIGNING_DOMAIN_V10: &[u8] = b"musteragent.execution-grant.v10\0";
const DEVICE_RUNTIME_AUDIENCE_PREFIX: &str = "musteragent.runtime.device.id-";
const EVOLUTION_MAINTENANCE_SIGNING_DOMAIN_V1: &[u8] =
    b"musteragent.execution-grant.evolution-maintenance.v1\0";
const STANDING_GOAL_JUDGE_SIGNING_DOMAIN_V1: &[u8] =
    b"musteragent.execution-grant.standing-goal-judge.v1\0";

/// Product policy scope resolved by the durable Backend authority. Device
/// private placement is represented separately by `device_id` so the signed
/// policy scope can never be confused with a physical execution home.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ExecutionGrantAuthorityScope {
    Tenant {
        tenant_id: TenantId,
    },
    Workspace {
        workspace_id: WorkspaceId,
    },
    HostedProject {
        scope: HostedProjectScope,
    },
    HostedProjectExecution {
        scope: HostedProjectScope,
        assignment_digest: [u8; 32],
    },
}

/// Product actor, durable policy scope, and Device-local placement signed as
/// one binding. Private grants bind a local filesystem capability scope;
/// hosted grants bind the shared logical workspace, resolved by the managed
/// replica/overlay owner. Neither grants an arbitrary host directory handle.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionGrantIdentityBinding {
    pub principal_id: PrincipalId,
    pub authority_scope: ExecutionGrantAuthorityScope,
    pub device_id: Option<DeviceId>,
    pub device_workspace_id: Option<WorkspaceId>,
}

impl ExecutionGrantIdentityBinding {
    fn validate(self, tenant_id: TenantId) -> Result<(), ExecutionGrantContractError> {
        if let ExecutionGrantAuthorityScope::HostedProjectExecution {
            scope,
            assignment_digest,
        } = self.authority_scope
        {
            if scope.resource_tenant_id != tenant_id
                || self.device_id.is_none()
                || self.device_workspace_id != Some(scope.workspace_id)
                || assignment_digest == [0; 32]
            {
                return Err(ExecutionGrantContractError::BindingMismatch);
            }
            return Ok(());
        }
        if let ExecutionGrantAuthorityScope::HostedProject { scope } = self.authority_scope {
            if scope.resource_tenant_id != tenant_id
                || self.device_id != Some(scope.home.device_id)
                || self.device_workspace_id != Some(scope.workspace_id)
            {
                return Err(ExecutionGrantContractError::BindingMismatch);
            }
            return Ok(());
        }
        if matches!(
            self.authority_scope,
            ExecutionGrantAuthorityScope::Tenant {
                tenant_id: scope_tenant
            } if scope_tenant != tenant_id
        ) {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        if self.device_workspace_id.is_some()
            && (self.device_id.is_none()
                || !matches!(
                    self.authority_scope,
                    ExecutionGrantAuthorityScope::Tenant { .. }
                ))
        {
            return Err(ExecutionGrantContractError::InvalidDeviceWorkspaceBinding);
        }
        Ok(())
    }
}

/// One complete, credential-free auxiliary route and its separately signed
/// credential binding. The vector containing these entries is canonical only
/// when roles are strictly ascending and unique.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantedAuxiliaryModelRoute {
    pub role: AuxiliaryModelRole,
    pub route: ModelRoute,
    pub credential_grant_ref: CredentialGrantRef,
}

/// One immutable, credential-free model-facing contract compiled by a
/// Backend-owned control plane.
///
/// Model-visible material deliberately contains no model, provider, protocol,
/// credential, tariff or cost identity. The optional opaque execution binding
/// is retained in the signed Grant only and is never projected into a Tool.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GrantedToolContractRelease {
    pub release_ref: OpaqueExternalRef,
    pub schema_version: DottedKey,
    pub tool_key: VersionedKey,
    pub available: bool,
    pub description: Option<String>,
    pub input_schema_json: Option<Vec<u8>>,
    pub content_sha256: [u8; 32],
    pub execution_binding_ref: Option<String>,
}

impl GrantedToolContractRelease {
    #[must_use]
    pub fn digest(
        schema_version: &DottedKey,
        tool_key: &VersionedKey,
        available: bool,
        description: Option<&str>,
        input_schema_json: Option<&[u8]>,
        execution_binding_ref: Option<&str>,
    ) -> [u8; 32] {
        let mut payload = Vec::new();
        hash_field(&mut payload, schema_version.as_str().as_bytes());
        hash_field(&mut payload, tool_key.as_str().as_bytes());
        payload.push(u8::from(available));
        hash_optional_field(&mut payload, description.map(str::as_bytes));
        hash_optional_field(&mut payload, input_schema_json);
        hash_optional_field(&mut payload, execution_binding_ref.map(str::as_bytes));
        Sha256::digest(payload).into()
    }

    fn validate(&self, grant_schema: &str) -> Result<(), ExecutionGrantContractError> {
        let expected_schema = match grant_schema {
            EXECUTION_GRANT_SCHEMA_V10 => TOOL_CONTRACT_RELEASE_SCHEMA_V1,
            _ => return Err(ExecutionGrantContractError::InvalidToolContractRelease),
        };
        if self.schema_version.as_str() != expected_schema {
            return Err(ExecutionGrantContractError::InvalidToolContractRelease);
        }
        let material_shape_valid = if self.available {
            self.description
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty() && value.len() <= 2_000)
                && self
                    .input_schema_json
                    .as_ref()
                    .is_some_and(|value| !value.is_empty() && value.len() <= 64 * 1024)
        } else {
            self.description.is_none()
                && self.input_schema_json.is_none()
                && self.execution_binding_ref.is_none()
        };
        if !material_shape_valid
            || self.execution_binding_ref.as_ref().is_some_and(|value| {
                value.trim().is_empty()
                    || value.len() > 2_048
                    || !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
            })
            || self.content_sha256
                != Self::digest(
                    &self.schema_version,
                    &self.tool_key,
                    self.available,
                    self.description.as_deref(),
                    self.input_schema_json.as_deref(),
                    self.execution_binding_ref.as_deref(),
                )
        {
            return Err(ExecutionGrantContractError::InvalidToolContractRelease);
        }
        Ok(())
    }
}

fn hash_field(payload: &mut Vec<u8>, value: &[u8]) {
    payload.extend_from_slice(&(value.len() as u64).to_be_bytes());
    payload.extend_from_slice(value);
}

fn hash_optional_field(payload: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            payload.push(1);
            hash_field(payload, value);
        }
        None => payload.push(0),
    }
}

/// Verified authorization claims. The signature envelope is consumed by a
/// control-plane adapter before these claims enter admission.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionGrant {
    pub reviewed_application_source_sha256: Vec<musteragent_content_ref::Sha256>,
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    pub schema_version: DottedKey,
    pub grant_ref: CredentialGrantRef,
    pub issuer: DottedKey,
    pub audience: DottedKey,
    pub tenant_id: TenantId,
    pub model_provider_id: ModelProviderId,
    pub model_entry_id: ModelEntryId,
    pub run_id: RunId,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
    pub nonce: OpaqueExternalRef,
    pub requested_model_class: ModelClass,
    pub model_route: ModelRoute,
    /// Ordered complete routes sealed by admission. The primary route's
    /// `fallback_route_refs` is the compact execution-order index.
    pub fallback_model_routes: Vec<ModelRoute>,
    /// Ordered auxiliary routes introduced by V4. Earlier schema versions
    /// fail closed if transport supplies any entry here.
    pub auxiliary_model_routes: Vec<GrantedAuxiliaryModelRoute>,
    /// Signed admission identity. Private maintenance protocols cannot
    /// authorize Runtime Snapshot v3 discovery.
    pub agent_contract: Option<VersionedKey>,
    pub surface: Option<DottedKey>,
    /// Canonically ordered tenant/Run authorization decisions. Installed
    /// Capability manifests never imply entitlements.
    pub capability_entitlements: Vec<CapabilityEntitlement>,
    /// Canonically ordered dynamic tool contracts, bound by the current grant.
    pub tool_contract_releases: Vec<GrantedToolContractRelease>,
    /// Signed Product actor/scope and optional Device identity. V1-V4 must
    /// leave this absent; V5 requires it.
    pub identity_binding: Option<ExecutionGrantIdentityBinding>,
    /// Signed approval posture for one Product Run. V5 requires it; legacy
    /// schemas must leave it absent.
    pub permission_mode: Option<PermissionMode>,
    /// Optional device-direct (BYOK) provider binding. Only V6 Device grants
    /// may carry it, and only for a BYOK primary route with no fallback
    /// routes and at most the BYOK Compression/Vision auxiliary routes pinned
    /// to their signed provider bindings.
    pub device_direct_provider: Option<DeviceDirectProvider>,
    /// Optional BYOK vision auxiliary provider binding. Only V6 Device grants
    /// with a device-direct primary binding may carry it, and only when one
    /// auxiliary route is the BYOK Vision route shape-matched to this binding.
    pub vision_direct_provider: Option<DeviceDirectProvider>,
}

/// Transport-independent signed envelope produced by the control plane.
#[derive(Clone, PartialEq)]
pub struct SignedExecutionGrant {
    pub key_id: DottedKey,
    pub claims: ExecutionGrant,
    pub signature: [u8; 64],
    /// Opaque, independently signed transport admission. Kept in the durable
    /// proof; Core neither interprets it nor grants any cache authority.
    pub context_transport_annex: Vec<u8>,
}

impl std::fmt::Debug for SignedExecutionGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedExecutionGrant")
            .field("key_id", &self.key_id)
            .field("claims", &self.claims)
            .field("signature", &"[REDACTED]")
            .field("transport_annex_bytes", &self.context_transport_annex.len())
            .finish()
    }
}

/// Content-addressed transport envelope retained with a Device Run.
///
/// The encoded bytes contain public authorization claims and a signature, not
/// credentials. They are nevertheless redacted from `Debug` so diagnostics do
/// not accidentally turn an authorization proof into a reusable log payload.
#[derive(Clone, Eq, PartialEq)]
pub struct EncodedExecutionGrantProof {
    schema_version: DottedKey,
    encoded_envelope: Vec<u8>,
    content_hash: ExecutionGrantProofHash,
    claims_hash: ExecutionGrantProofHash,
}

/// SHA-256 identity scoped to a signed Execution Grant proof.
///
/// Keeping this value in the Grant owner avoids coupling authorization to the
/// ContentRef domain while still preventing untyped byte arrays at persistence
/// and transport boundaries.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ExecutionGrantProofHash([u8; 32]);

impl ExecutionGrantProofHash {
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, ExecutionGrantProofError> {
        let bytes = bytes
            .as_ref()
            .try_into()
            .map_err(|_| ExecutionGrantProofError::InvalidHash)?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Debug for EncodedExecutionGrantProof {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EncodedExecutionGrantProof")
            .field("schema_version", &self.schema_version)
            .field("encoded_bytes", &self.encoded_envelope.len())
            .field("content_hash", &self.content_hash)
            .field("claims_hash", &self.claims_hash)
            .finish_non_exhaustive()
    }
}

impl EncodedExecutionGrantProof {
    pub fn seal(
        schema_version: DottedKey,
        encoded_envelope: Vec<u8>,
        signed: &SignedExecutionGrant,
    ) -> Result<Self, ExecutionGrantProofError> {
        if signed.context_transport_annex.len() > 64 * 1024 {
            return Err(ExecutionGrantProofError::InvalidSize);
        }
        let claims_hash = ExecutionGrantProofHash::digest(&canonical_claims(&signed.claims)?);
        let content_hash = ExecutionGrantProofHash::digest(&encoded_envelope);
        Self::restore(schema_version, encoded_envelope, content_hash, claims_hash)
    }

    pub fn restore(
        schema_version: DottedKey,
        encoded_envelope: Vec<u8>,
        content_hash: ExecutionGrantProofHash,
        claims_hash: ExecutionGrantProofHash,
    ) -> Result<Self, ExecutionGrantProofError> {
        if !matches!(
            schema_version.as_str(),
            EXECUTION_GRANT_PROTOBUF_PROOF_SCHEMA_V1
        ) {
            return Err(ExecutionGrantProofError::UnsupportedSchema);
        }
        if encoded_envelope.is_empty() || encoded_envelope.len() > MAX_EXECUTION_GRANT_PROOF_BYTES {
            return Err(ExecutionGrantProofError::InvalidSize);
        }
        if ExecutionGrantProofHash::digest(&encoded_envelope) != content_hash {
            return Err(ExecutionGrantProofError::ContentHashMismatch);
        }
        Ok(Self {
            schema_version,
            encoded_envelope,
            content_hash,
            claims_hash,
        })
    }

    pub fn validate_for(&self, claims: &ExecutionGrant) -> Result<(), ExecutionGrantProofError> {
        if ExecutionGrantProofHash::digest(&canonical_claims(claims)?) != self.claims_hash {
            return Err(ExecutionGrantProofError::ClaimsHashMismatch);
        }
        Self::restore(
            self.schema_version.clone(),
            self.encoded_envelope.clone(),
            self.content_hash,
            self.claims_hash,
        )?;
        Ok(())
    }

    #[must_use]
    pub fn schema_version(&self) -> &DottedKey {
        &self.schema_version
    }

    #[must_use]
    pub fn encoded_envelope(&self) -> &[u8] {
        &self.encoded_envelope
    }

    #[must_use]
    pub const fn content_hash(&self) -> ExecutionGrantProofHash {
        self.content_hash
    }

    #[must_use]
    pub const fn claims_hash(&self) -> ExecutionGrantProofHash {
        self.claims_hash
    }
}

/// Adapter port for the canonical cross-language proof representation.
/// Runtime Core never depends on Protobuf; the protocol adapter implements
/// this port and admission persists only the sealed, content-addressed result.
pub trait ExecutionGrantProofEncoder: Send + Sync {
    fn encode(
        &self,
        signed: &SignedExecutionGrant,
    ) -> Result<EncodedExecutionGrantProof, ExecutionGrantProofError>;
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ExecutionGrantProofError {
    #[error("execution grant proof schema is unsupported")]
    UnsupportedSchema,
    #[error("execution grant proof size is invalid")]
    InvalidSize,
    #[error("execution grant proof hash must contain exactly 32 bytes")]
    InvalidHash,
    #[error("execution grant proof content hash does not match its bytes")]
    ContentHashMismatch,
    #[error("execution grant proof claims do not match the admitted grant")]
    ClaimsHashMismatch,
    #[error(transparent)]
    ClaimsEncoding(#[from] ExecutionGrantEncodingError),
}

impl ExecutionGrant {
    pub fn validate(&self) -> Result<(), ExecutionGrantContractError> {
        let reviewed = &self.reviewed_application_source_sha256;
        if reviewed.len() > 64
            || reviewed.iter().any(|v| v.into_bytes() == [0; 32])
            || reviewed
                .windows(2)
                .any(|pair| pair[0].into_bytes() >= pair[1].into_bytes())
            || (!reviewed.is_empty()
                && (self.product_release.is_none()
                    || !matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)))
        {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        if self
            .product_release
            .as_ref()
            .is_some_and(|r| r.digest == [0; 32])
        {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        let hosted = self.identity_binding.is_some_and(|binding| {
            matches!(
                binding.authority_scope,
                ExecutionGrantAuthorityScope::HostedProject { .. }
                    | ExecutionGrantAuthorityScope::HostedProjectExecution { .. }
            )
        });
        if hosted && self.schema_version.as_str() != EXECUTION_GRANT_SCHEMA_V10 {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        if self.expires_at <= self.issued_at {
            return Err(ExecutionGrantContractError::InvalidLifetime);
        }
        if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)
            && [self.issuer.as_str(), self.audience.as_str()]
                .iter()
                .any(|value| {
                    !value.starts_with("musteragent.") || value.starts_with("musteragent.legacy.")
                })
        {
            return Err(ExecutionGrantContractError::InvalidCurrentNamespace);
        }
        self.model_route.validate()?;

        match self.schema_version.as_str() {
            EXECUTION_GRANT_SCHEMA_V10
            | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
            | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1 => {
                let candidate_refs = self
                    .fallback_model_routes
                    .iter()
                    .map(|route| &route.route_ref)
                    .collect::<Vec<_>>();
                let indexed_refs = self
                    .model_route
                    .fallback_route_refs
                    .iter()
                    .collect::<Vec<_>>();
                if candidate_refs != indexed_refs {
                    return Err(ExecutionGrantContractError::FallbackChainMismatch);
                }
                let mut all_refs = BTreeSet::from([&self.model_route.route_ref]);
                for candidate in &self.fallback_model_routes {
                    candidate.validate()?;
                    if !candidate.fallback_route_refs.is_empty() {
                        return Err(ExecutionGrantContractError::NestedFallbackChain);
                    }
                    if !all_refs.insert(&candidate.route_ref) {
                        return Err(ExecutionGrantContractError::FallbacksNotUnique);
                    }
                }
            }
            _ => return Err(ExecutionGrantContractError::UnsupportedSchema),
        }
        if !matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)
            && !self.auxiliary_model_routes.is_empty()
        {
            return Err(ExecutionGrantContractError::PrivateProtocolContainsAuxiliaryRoutes);
        }
        if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10) {
            if !musteragent_model::valid_auxiliary_model_routes(
                self.auxiliary_model_routes
                    .iter()
                    .map(|item| (item.role, &item.route, &item.credential_grant_ref)),
            ) {
                return Err(ExecutionGrantContractError::AuxiliaryRoutesNotCanonical);
            }
            let mut all_route_refs = BTreeSet::from([&self.model_route.route_ref]);
            all_route_refs.extend(
                self.fallback_model_routes
                    .iter()
                    .map(|route| &route.route_ref),
            );
            for auxiliary in &self.auxiliary_model_routes {
                auxiliary.route.validate()?;
                if !auxiliary.route.fallback_route_refs.is_empty() {
                    return Err(ExecutionGrantContractError::AuxiliaryContainsFallbacks);
                }
                if !all_route_refs.insert(&auxiliary.route.route_ref) {
                    return Err(ExecutionGrantContractError::RouteRefsNotUniqueAcrossRoles);
                }
            }
        }
        if self.model_provider_id != self.model_route.model_provider_id
            || self.model_entry_id != self.model_route.model_entry_id
        {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }

        if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10) {
            if self.agent_contract.is_none()
                || self.surface.is_none()
                || self.capability_entitlements.is_empty()
            {
                return Err(ExecutionGrantContractError::MissingCapabilityEntitlements);
            }
            let mut previous = None;
            for entitlement in &self.capability_entitlements {
                entitlement
                    .validate()
                    .map_err(|_| ExecutionGrantContractError::InvalidCapabilityEntitlement)?;
                if previous.is_some_and(|key: &CapabilityKey| key >= &entitlement.key) {
                    return Err(ExecutionGrantContractError::CapabilityEntitlementsNotCanonical);
                }
                previous = Some(&entitlement.key);
            }
        }
        if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10) {
            let mut previous = None;
            for release in &self.tool_contract_releases {
                release.validate(self.schema_version.as_str())?;
                if previous.is_some_and(|key: &VersionedKey| key >= &release.tool_key) {
                    return Err(ExecutionGrantContractError::ToolContractReleasesNotCanonical);
                }
                previous = Some(&release.tool_key);
            }
        } else if !self.tool_contract_releases.is_empty() {
            return Err(ExecutionGrantContractError::PrivateProtocolContainsToolContractReleases);
        }
        let is_web_surface = self.surface.as_ref().map(DottedKey::as_str) == Some("web");
        let is_service_managed = self.permission_mode == Some(PermissionMode::ServiceManaged);
        if (is_service_managed && (!is_web_surface || hosted))
            || (self.schema_version.as_str() == EXECUTION_GRANT_SCHEMA_V10
                && !hosted
                && is_web_surface
                && !is_service_managed)
        {
            return Err(ExecutionGrantContractError::InvalidPermissionSurface);
        }
        if self.schema_version.as_str() == EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
            && (self.agent_contract.is_none()
                || self.surface.as_ref().map(DottedKey::as_str) != Some("maintenance")
                || !self.capability_entitlements.is_empty()
                || !self.auxiliary_model_routes.is_empty()
                || !self.fallback_model_routes.is_empty()
                || !self.model_route.fallback_route_refs.is_empty()
                || !self
                    .requested_model_class
                    .as_str()
                    .starts_with("chat.evolution")
                || !self
                    .model_route
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "tools")
                || self.permission_mode != Some(PermissionMode::Trusted))
        {
            return Err(ExecutionGrantContractError::InvalidEvolutionMaintenanceGrant);
        }
        if self.schema_version.as_str() == STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
            && (self.agent_contract.is_none()
                || self.surface.is_none()
                || !self.capability_entitlements.is_empty()
                || !self.auxiliary_model_routes.is_empty()
                || !self.fallback_model_routes.is_empty()
                || !self.model_route.fallback_route_refs.is_empty()
                || !self
                    .model_route
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "tools")
                || self.permission_mode.is_none())
        {
            return Err(ExecutionGrantContractError::InvalidStandingGoalJudgeGrant);
        }
        if matches!(
            self.schema_version.as_str(),
            EXECUTION_GRANT_SCHEMA_V10
                | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
                | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
        ) {
            let binding = self
                .identity_binding
                .ok_or(ExecutionGrantContractError::MissingIdentityBinding)?;
            binding.validate(self.tenant_id)?;
            if let ExecutionGrantAuthorityScope::HostedProject { scope } = binding.authority_scope {
                let expected = format!("{DEVICE_RUNTIME_AUDIENCE_PREFIX}{}", scope.home.device_id);
                if self.audience.as_str() != expected {
                    return Err(ExecutionGrantContractError::AudienceMismatch);
                }
            }
            if let ExecutionGrantAuthorityScope::HostedProjectExecution { .. } =
                binding.authority_scope
            {
                let device_id = binding
                    .device_id
                    .ok_or(ExecutionGrantContractError::BindingMismatch)?;
                if self.audience.as_str() != format!("{DEVICE_RUNTIME_AUDIENCE_PREFIX}{device_id}")
                {
                    return Err(ExecutionGrantContractError::AudienceMismatch);
                }
            }
            self.permission_mode
                .ok_or(ExecutionGrantContractError::MissingPermissionMode)?;

            if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)
                && !hosted
                && binding.device_id.is_some()
                && !matches!(
                    binding.authority_scope,
                    ExecutionGrantAuthorityScope::Tenant { .. }
                )
            {
                return Err(ExecutionGrantContractError::InvalidDeviceWorkspaceBinding);
            }
            if self.schema_version.as_str() == EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
                && !matches!(
                    binding.authority_scope,
                    ExecutionGrantAuthorityScope::Tenant { .. }
                )
            {
                return Err(ExecutionGrantContractError::InvalidEvolutionMaintenanceGrant);
            }
        }
        if let Some(provider) = &self.device_direct_provider {
            provider.validate()?;
            if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)
                && !provider
                    .credential_ref
                    .starts_with(DEVICE_DIRECT_CREDENTIAL_REF_PREFIX)
            {
                return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
            }
            let route_ref = self.model_route.route_ref.as_str();
            if !matches!(
                self.schema_version.as_str(),
                EXECUTION_GRANT_SCHEMA_V10 | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
            ) || !self.fallback_model_routes.is_empty()
                || !self.model_route.fallback_route_refs.is_empty()
                || !route_ref.starts_with(DEVICE_DIRECT_ROUTE_REF_PREFIX)
                || !route_ref.ends_with("@1")
                || self.model_route.api_format != provider.api_format
                || self.model_route.resolved_model_key != provider.model
                || self.model_route.pricing_version_ref.as_str()
                    != DEVICE_DIRECT_PRICING_VERSION_REF
            {
                return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
            }
            // A normal V6 BYOK grant may carry only Compression/Vision
            // sidecars. A Standing Goal Judge grant is already constrained to
            // no auxiliary routes above and reuses only the signed primary
            // Device-local provider binding for its private invocation.
            let mut vision_route_seen = false;
            for auxiliary in &self.auxiliary_model_routes {
                let auxiliary_ref = auxiliary.route.route_ref.as_str();
                match auxiliary.role {
                    AuxiliaryModelRole::Compression => {
                        if !auxiliary_ref.starts_with(DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX)
                            || !auxiliary_ref.ends_with("@1")
                            || auxiliary.route.api_format != provider.api_format
                            || auxiliary.route.resolved_model_key != provider.model
                            || auxiliary.route.model_provider_id
                                != self.model_route.model_provider_id
                            || auxiliary.route.model_entry_id != self.model_route.model_entry_id
                            || auxiliary.route.pricing_version_ref.as_str()
                                != DEVICE_DIRECT_PRICING_VERSION_REF
                            || auxiliary.credential_grant_ref != self.grant_ref
                        {
                            return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
                        }
                    }
                    AuxiliaryModelRole::Vision => {
                        // A BYOK vision route without its signed provider
                        // binding fails closed rather than executing an
                        // unsigned endpoint.
                        let Some(vision) = &self.vision_direct_provider else {
                            return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
                        };
                        vision.validate()?;
                        if matches!(self.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10)
                            && !vision
                                .credential_ref
                                .starts_with(DEVICE_DIRECT_CREDENTIAL_REF_PREFIX)
                        {
                            return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
                        }
                        if !auxiliary_ref.starts_with(DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX)
                            || !auxiliary_ref.ends_with("@1")
                            || auxiliary.route.api_format != vision.api_format
                            || auxiliary.route.resolved_model_key != vision.model
                            || auxiliary.route.pricing_version_ref.as_str()
                                != DEVICE_DIRECT_PRICING_VERSION_REF
                            || auxiliary.credential_grant_ref != self.grant_ref
                        {
                            return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
                        }
                        vision_route_seen = true;
                    }
                    _ => return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider),
                }
            }
            if self.vision_direct_provider.is_some() && !vision_route_seen {
                // A signed vision binding without its BYOK vision route is
                // never meaningful and must not widen the signed shape.
                return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
            }
        } else {
            if self.vision_direct_provider.is_some() {
                // The vision binding only exists alongside the primary
                // device-direct binding of a V6 Device grant.
                return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
            }
            let byok_route_without_binding = self
                .model_route
                .route_ref
                .as_str()
                .starts_with(DEVICE_DIRECT_ROUTE_REF_PREFIX)
                || self.auxiliary_model_routes.iter().any(|auxiliary| {
                    let route_ref = auxiliary.route.route_ref.as_str();
                    route_ref.starts_with(DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX)
                        || route_ref.starts_with(DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX)
                });
            if byok_route_without_binding {
                // A BYOK-looking route without the signed provider binding must
                // fail closed rather than execute an unsigned endpoint.
                return Err(ExecutionGrantContractError::InvalidDeviceDirectProvider);
            }
        }
        Ok(())
    }

    pub fn authorize(
        &self,
        request: &GrantAuthorizationRequest,
    ) -> Result<(), ExecutionGrantContractError> {
        self.validate()?;
        if self.audience != request.runtime_audience {
            return Err(ExecutionGrantContractError::AudienceMismatch);
        }
        if self.tenant_id != request.tenant_id
            || self.run_id != request.run_id
            || self.model_provider_id != request.model_provider_id
            || self.model_entry_id != request.model_entry_id
        {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        if self.identity_binding != Some(request.identity_binding) {
            return Err(ExecutionGrantContractError::BindingMismatch);
        }
        request.identity_binding.validate(request.tenant_id)?;
        if request.now < self.issued_at || request.now >= self.expires_at {
            return Err(ExecutionGrantContractError::Expired);
        }
        Ok(())
    }
}

/// Canonical, transport-independent signing bytes for every stable Execution
/// Grant schema. This encoder belongs to the signed contract rather than to a
/// particular keyring or transport adapter.
pub fn canonical_claims(grant: &ExecutionGrant) -> Result<Vec<u8>, ExecutionGrantEncodingError> {
    grant.validate()?;
    let mut encoded = Vec::with_capacity(512);
    match grant.schema_version.as_str() {
        EXECUTION_GRANT_SCHEMA_V10 => encoded.extend_from_slice(SIGNING_DOMAIN_V10),

        EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1 => {
            encoded.extend_from_slice(EVOLUTION_MAINTENANCE_SIGNING_DOMAIN_V1);
        }
        STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1 => {
            encoded.extend_from_slice(STANDING_GOAL_JUDGE_SIGNING_DOMAIN_V1);
        }
        _ => return Err(ExecutionGrantContractError::UnsupportedSchema.into()),
    }
    append(&mut encoded, grant.grant_ref.as_str().as_bytes())?;
    append(&mut encoded, grant.issuer.as_str().as_bytes())?;
    append(&mut encoded, grant.audience.as_str().as_bytes())?;
    append(&mut encoded, grant.tenant_id.to_string().as_bytes())?;
    append(&mut encoded, grant.model_provider_id.to_string().as_bytes())?;
    append(&mut encoded, grant.model_entry_id.to_string().as_bytes())?;
    append(&mut encoded, grant.run_id.to_string().as_bytes())?;
    encoded.extend_from_slice(&grant.issued_at.unix_millis().to_be_bytes());
    encoded.extend_from_slice(&grant.expires_at.unix_millis().to_be_bytes());
    append(&mut encoded, grant.nonce.as_str().as_bytes())?;
    append(
        &mut encoded,
        grant.requested_model_class.as_str().as_bytes(),
    )?;

    append(&mut encoded, grant.schema_version.as_str().as_bytes())?;

    append_model_route(&mut encoded, &grant.model_route)?;

    append_count(&mut encoded, grant.fallback_model_routes.len())?;
    for route in &grant.fallback_model_routes {
        append_model_route(&mut encoded, route)?;
    }

    if matches!(
        grant.schema_version.as_str(),
        EXECUTION_GRANT_SCHEMA_V10
            | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
            | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
    ) {
        append_count(&mut encoded, grant.auxiliary_model_routes.len())?;
        for auxiliary in &grant.auxiliary_model_routes {
            encoded.push(auxiliary.role.canonical_code());
            append_model_route(&mut encoded, &auxiliary.route)?;
            append(
                &mut encoded,
                auxiliary.credential_grant_ref.as_str().as_bytes(),
            )?;
        }
    }
    if matches!(
        grant.schema_version.as_str(),
        EXECUTION_GRANT_SCHEMA_V10
            | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
            | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
    ) {
        append(
            &mut encoded,
            grant
                .agent_contract
                .as_ref()
                .ok_or(ExecutionGrantContractError::MissingCapabilityEntitlements)?
                .as_str()
                .as_bytes(),
        )?;
        append(
            &mut encoded,
            grant
                .surface
                .as_ref()
                .ok_or(ExecutionGrantContractError::MissingCapabilityEntitlements)?
                .as_str()
                .as_bytes(),
        )?;
        append_count(&mut encoded, grant.capability_entitlements.len())?;
        for entitlement in &grant.capability_entitlements {
            append_capability_entitlement(&mut encoded, entitlement)?;
        }
    }
    if matches!(
        grant.schema_version.as_str(),
        EXECUTION_GRANT_SCHEMA_V10
            | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
            | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
    ) {
        let binding = grant
            .identity_binding
            .ok_or(ExecutionGrantContractError::MissingIdentityBinding)?;
        append(&mut encoded, binding.principal_id.to_string().as_bytes())?;
        match binding.authority_scope {
            ExecutionGrantAuthorityScope::Tenant { tenant_id } => {
                encoded.push(1);
                append(&mut encoded, tenant_id.to_string().as_bytes())?;
            }
            ExecutionGrantAuthorityScope::Workspace { workspace_id } => {
                encoded.push(2);
                append(&mut encoded, workspace_id.to_string().as_bytes())?;
            }
            ExecutionGrantAuthorityScope::HostedProject { scope } => {
                encoded.push(4);
                encoded.extend_from_slice(&scope.identity_bytes());
            }
            ExecutionGrantAuthorityScope::HostedProjectExecution {
                scope,
                assignment_digest,
            } => {
                encoded.push(5);
                encoded.extend_from_slice(&scope.identity_bytes());
                encoded.extend_from_slice(&assignment_digest);
            }
        }
        match binding.device_id {
            Some(device_id) => {
                encoded.push(1);
                append(&mut encoded, device_id.to_string().as_bytes())?;
            }
            None => encoded.push(0),
        }
        if matches!(
            grant.schema_version.as_str(),
            EXECUTION_GRANT_SCHEMA_V10
                | EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1
                | STANDING_GOAL_JUDGE_GRANT_SCHEMA_V1
        ) {
            match binding.device_workspace_id {
                Some(workspace_id) => {
                    encoded.push(1);
                    append(&mut encoded, workspace_id.to_string().as_bytes())?;
                }
                None => encoded.push(0),
            }
        }
        encoded.push(
            grant
                .permission_mode
                .ok_or(ExecutionGrantContractError::MissingPermissionMode)?
                .canonical_code(),
        );
    }
    if matches!(grant.schema_version.as_str(), EXECUTION_GRANT_SCHEMA_V10) {
        append_count(&mut encoded, grant.tool_contract_releases.len())?;
        for release in &grant.tool_contract_releases {
            append(&mut encoded, release.release_ref.as_str().as_bytes())?;
            append(&mut encoded, release.schema_version.as_str().as_bytes())?;
            append(&mut encoded, release.tool_key.as_str().as_bytes())?;
            encoded.push(u8::from(release.available));
            append_optional(
                &mut encoded,
                release.description.as_deref().map(str::as_bytes),
            )?;
            append_optional(&mut encoded, release.input_schema_json.as_deref())?;
            append_optional(
                &mut encoded,
                release.execution_binding_ref.as_deref().map(str::as_bytes),
            )?;
            encoded.extend_from_slice(&release.content_sha256);
        }
    }
    if let Some(provider) = &grant.device_direct_provider {
        // Trailing optional section: absent providers append nothing, so all
        // pre-BYOK canonical encodings remain byte-identical.
        encoded.push(1);
        append(&mut encoded, provider.base_url.as_bytes())?;
        append(&mut encoded, provider.api_format.as_str().as_bytes())?;
        append(&mut encoded, provider.model.as_bytes())?;
        append(&mut encoded, provider.credential_ref.as_bytes())?;
    }
    if let Some(provider) = &grant.vision_direct_provider {
        // Trailing optional section appended after the primary device-direct
        // binding: absent vision bindings append nothing, so every pre-vision
        // canonical encoding remains byte-identical.
        encoded.push(1);
        append(&mut encoded, provider.base_url.as_bytes())?;
        append(&mut encoded, provider.api_format.as_str().as_bytes())?;
        append(&mut encoded, provider.model.as_bytes())?;
        append(&mut encoded, provider.credential_ref.as_bytes())?;
    }
    if let Some(release) = &grant.product_release {
        encoded = release.bind_canonical(
            b"musteragent.execution-grant.product-release.v1\0",
            &encoded,
        );
    }

    if !grant.reviewed_application_source_sha256.is_empty() {
        let mut bound = b"musteragent.execution-grant.reviewed-application-sources.v1\0".to_vec();
        append(&mut bound, &encoded)?;
        append_count(&mut bound, grant.reviewed_application_source_sha256.len())?;
        for digest in &grant.reviewed_application_source_sha256 {
            bound.extend_from_slice(&digest.into_bytes());
        }
        encoded = bound;
    }
    Ok(encoded)
}

fn append_model_route(
    encoded: &mut Vec<u8>,
    route: &ModelRoute,
) -> Result<(), ExecutionGrantEncodingError> {
    encoded.extend_from_slice(&canonical_model_route_body(route)?);
    Ok(())
}

fn append_capability_entitlement(
    encoded: &mut Vec<u8>,
    entitlement: &CapabilityEntitlement,
) -> Result<(), ExecutionGrantEncodingError> {
    append(encoded, entitlement.key.as_str().as_bytes())?;
    append(encoded, entitlement.provider_id.to_string().as_bytes())?;
    encoded.push(match entitlement.location {
        CapabilityLocation::Cloud => 1,
        CapabilityLocation::Backend => 2,
        CapabilityLocation::Device => 3,
        CapabilityLocation::RemoteDevice => 4,
        CapabilityLocation::Browser => 5,
    });
    encoded.push(match entitlement.effect {
        Effect::Read => 1,
        Effect::WriteReversible => 2,
        Effect::WriteExternal => 3,
        Effect::Destructive => 4,
    });
    encoded.push(match entitlement.policy_decision {
        Decision::Allow => 1,
        Decision::Deny => 2,
        Decision::RequireApproval => 3,
    });
    append_count(encoded, entitlement.allowed_scopes.len())?;
    for scope in &entitlement.allowed_scopes {
        append(encoded, scope.as_str().as_bytes())?;
    }
    Ok(())
}

fn append_count(target: &mut Vec<u8>, value: usize) -> Result<(), ExecutionGrantEncodingError> {
    let value = u32::try_from(value).map_err(|_| ExecutionGrantEncodingError::ClaimFieldTooLong)?;
    target.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn append_optional(
    target: &mut Vec<u8>,
    value: Option<&[u8]>,
) -> Result<(), ExecutionGrantEncodingError> {
    match value {
        Some(value) => {
            target.push(1);
            append(target, value)
        }
        None => {
            target.push(0);
            Ok(())
        }
    }
}

fn append(target: &mut Vec<u8>, value: &[u8]) -> Result<(), ExecutionGrantEncodingError> {
    let length =
        u32::try_from(value.len()).map_err(|_| ExecutionGrantEncodingError::ClaimFieldTooLong)?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GrantAuthorizationRequest {
    pub runtime_audience: DottedKey,
    pub tenant_id: TenantId,
    pub identity_binding: ExecutionGrantIdentityBinding,
    pub run_id: RunId,
    pub model_provider_id: ModelProviderId,
    pub model_entry_id: ModelEntryId,
    pub now: UtcTimestamp,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ExecutionGrantContractError {
    #[error(transparent)]
    Model(#[from] ModelContractError),
    #[error("execution grant audience does not match this Runtime")]
    AudienceMismatch,
    #[error("execution grant tenant, Run or Provider binding mismatch")]
    BindingMismatch,
    #[error("execution grant is expired or not yet valid")]
    Expired,
    #[error("execution grant expiry must be after issuance")]
    InvalidLifetime,
    #[error("current Execution Grant must use the Musteragent namespace")]
    InvalidCurrentNamespace,
    #[error("execution grant schema version is unsupported")]
    UnsupportedSchema,
    #[error("private protocol grants cannot contain auxiliary model routes")]
    PrivateProtocolContainsAuxiliaryRoutes,
    #[error("auxiliary model routes must be unique and strictly ordered by role")]
    AuxiliaryRoutesNotCanonical,
    #[error("auxiliary model routes cannot contain fallback route references")]
    AuxiliaryContainsFallbacks,
    #[error("primary, fallback and auxiliary route references must be unique across roles")]
    RouteRefsNotUniqueAcrossRoles,
    #[error("execution grant requires agent, surface and capability entitlements")]
    MissingCapabilityEntitlements,
    #[error("execution grant requires a signed actor, authority scope and device binding")]
    MissingIdentityBinding,
    #[error("device-local workspace binding requires tenant authority and device identity")]
    InvalidDeviceWorkspaceBinding,
    #[error("execution grant requires a signed permission mode")]
    MissingPermissionMode,
    #[error("service-managed permission mode is valid only for the Web surface")]
    InvalidPermissionSurface,
    #[error(
        "Evolution Maintenance grant must authorize one trusted route with only the Runtime-private submission protocol"
    )]
    InvalidEvolutionMaintenanceGrant,
    #[error("Standing Goal Evaluation grant must authorize a private tool-capable Agent Run")]
    InvalidStandingGoalJudgeGrant,
    #[error(
        "device-direct grants require the current execution or standing Goal Judge schema, a bound BYOK primary route, no fallback routes and only the auxiliary routes allowed by that schema"
    )]
    InvalidDeviceDirectProvider,
    #[error("Capability entitlement is invalid")]
    InvalidCapabilityEntitlement,
    #[error("Capability entitlements must be unique and canonically ordered")]
    CapabilityEntitlementsNotCanonical,
    #[error("private protocol grants cannot contain tool contracts")]
    PrivateProtocolContainsToolContractReleases,
    #[error("Tool Contract release is invalid")]
    InvalidToolContractRelease,
    #[error("Tool Contract releases must be unique and canonically ordered")]
    ToolContractReleasesNotCanonical,
    #[error("execution grant fallback index does not match its complete route chain")]
    FallbackChainMismatch,
    #[error("execution grant fallback candidates cannot contain nested fallback chains")]
    NestedFallbackChain,
    #[error("execution grant fallback routes must be unique")]
    FallbacksNotUnique,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ExecutionGrantEncodingError {
    #[error(transparent)]
    Contract(#[from] ExecutionGrantContractError),
    #[error("execution grant claim field exceeds the canonical encoding bound")]
    ClaimFieldTooLong,
    #[error(transparent)]
    ModelRoute(#[from] ModelRouteEncodingError),
}

#[cfg(test)]
mod tests;
