//! Browser provider lease, fenced Job delivery, and durable Gateway port.
//!
//! The process-local registry and queue are deterministic domain simulators.
//! Production Gateway coordination is owned by `BrowserGatewayStore`; parent
//! Tool/Invocation terminal state remains owned by the Runtime handler.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_capability::{
    BrowserJobContractError, BrowserJobRequest, BrowserJobResult, BrowserLeaseProof,
    BrowserProviderContract, BrowserProviderLease, BrowserRuntimeFeatureProbe, CapabilityKey,
    MAX_BROWSER_PROVIDER_CAPABILITIES,
};
use musteragent_identity::{
    BrowserGatewayTicketId, BrowserJobId, BrowserLeaseEpoch, BrowserLeaseId, BrowserSessionId,
    CapabilityProviderId, FencingToken, InvocationId, PageInstanceId, PrincipalId, TenantId,
    UtcTimestamp, VersionedKey,
};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const MAX_BROWSER_LEASE_TTL_MILLIS: u64 = 120_000;

/// Immutable registration authority. Execution leases always remain actor-bound,
/// including leases issued by a global server pool; content custody never widens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserProviderScope {
    Actor,
    Tenant(TenantId),
    Global,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserLeaseRegistration {
    /// Authenticated registration owner declares the executor class.
    pub executor: musteragent_sandbox::SandboxExecutor,
    pub scope: BrowserProviderScope,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub browser_session_id: BrowserSessionId,
    pub page_instance_id: PageInstanceId,
    pub provider_id: CapabilityProviderId,
    pub runtime_release_ref: VersionedKey,
    pub capabilities: BTreeSet<CapabilityKey>,
    pub resource_profile_ref: VersionedKey,
    pub feature_probe: BrowserRuntimeFeatureProbe,
    pub ttl_millis: NonZeroU64,
}

impl BrowserLeaseRegistration {
    /// Stable authority tuple used by both in-memory and durable registries.
    pub fn authority(&self) -> (&'static str, Option<TenantId>, Option<PrincipalId>) {
        match self.scope {
            BrowserProviderScope::Actor => ("actor", Some(self.tenant_id), Some(self.principal_id)),
            BrowserProviderScope::Tenant(tenant) => ("tenant", Some(tenant), None),
            BrowserProviderScope::Global => ("global", None, None),
        }
    }
    pub fn validate(&self) -> Result<(), BrowserProviderRuntimeError> {
        self.feature_probe.validate()?;
        if (self.executor == musteragent_sandbox::SandboxExecutor::Browser
            && self.scope != BrowserProviderScope::Actor)
            || matches!(self.scope, BrowserProviderScope::Tenant(tenant) if tenant != self.tenant_id)
        {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        if self.capabilities.is_empty()
            || self.capabilities.len() > MAX_BROWSER_PROVIDER_CAPABILITIES
            || self.ttl_millis.get() > MAX_BROWSER_LEASE_TTL_MILLIS
            || self.capabilities.iter().any(|capability| {
                BrowserProviderContract::parse(capability.as_str()).map_or(true, |contract| {
                    !self
                        .feature_probe
                        .engines
                        .iter()
                        .copied()
                        .any(|engine| contract.accepts_engine(engine))
                        || matches!(
                            contract,
                            BrowserProviderContract::PythonEnvironmentPrepare
                                | BrowserProviderContract::PythonExecutePrepared
                        ) && self.executor == musteragent_sandbox::SandboxExecutor::Browser
                            && !self.feature_probe.indexed_db
                        || matches!(
                            contract,
                            BrowserProviderContract::WebBuild
                                | BrowserProviderContract::WebValidate
                        ) && !self.feature_probe.supports_preview()
                })
            })
        {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        Ok(())
    }
}

/// Durable Browser Job assigned to one exact page lease. Delivery sequence is
/// scoped to the lease and is allocated by the persistence authority, never by
/// a browser or a process-local counter.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DurableBrowserJob {
    pub delivery: BrowserJobDelivery,
    pub delivery_sequence: u64,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

/// The original Tool transaction determines whether uploaded candidate bytes are saved.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BrowserCandidateSettlement {
    Pending,
    Committed,
    NotCommitted,
}

/// Proof returned only after a terminal Browser result is committed. Runtime
/// handlers must hold this receipt before they may terminalize the parent Tool.
/// Provider sequence zero is reserved for a control-plane cancellation that
/// terminalized a still-queued Job before any browser claimed it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserJobTerminalReceipt {
    pub job_id: BrowserJobId,
    pub invocation_id: InvocationId,
    pub lease: BrowserLeaseProof,
    pub provider_sequence: u64,
    pub result: BrowserJobResult,
    pub sealed_at: UtcTimestamp,
}

/// Verified identity from one short-lived, origin-bound Browser Gateway
/// ticket. A durable store consumes the ticket exactly once before a WebSocket
/// may register a page provider.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrowserGatewayAdmission {
    pub ticket_id: BrowserGatewayTicketId,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub browser_session_id: BrowserSessionId,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
}

/// Durable authority shared by Cloud Workers and the Browser Gateway.
///
/// Implementations must use an authoritative database clock for leases and
/// must compare the complete lease proof on every post-registration mutation.
/// Returning a terminal receipt means its transaction has committed.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserExecutionProgress {
    pub phase: String,
    pub package_name: Option<String>,
    pub transferred_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub message: Option<String>,
}
impl BrowserExecutionProgress {
    pub fn validate(&self) -> Result<(), BrowserProviderRuntimeError> {
        if !matches!(
            self.phase.as_str(),
            "accepted"
                | "resolving"
                | "downloading"
                | "verifying"
                | "installing"
                | "initializing"
                | "running"
                | "collecting"
        ) || self.package_name.as_ref().is_some_and(|v| v.len() > 1000)
            || self.message.as_ref().is_some_and(|v| v.len() > 4000)
            || self
                .transferred_bytes
                .zip(self.total_bytes)
                .is_some_and(|(done, total)| done > total)
        {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        Ok(())
    }
}

pub trait BrowserGatewayStore: Send + Sync {
    /// Explicit routing/persistence owner. Older gateways fail closed until wired.
    fn sandbox_routing(
        &self,
    ) -> Result<&dyn crate::SandboxRoutingStore, BrowserProviderRuntimeError> {
        Err(BrowserProviderRuntimeError::SandboxRoutingUnavailable)
    }

    fn load_progress(
        &self,
        _tenant_id: TenantId,
        _job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Vec<BrowserExecutionProgress>, BrowserProviderRuntimeError>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn record_execution_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        detail: Option<BrowserExecutionProgress>,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            if let Some(detail) = detail {
                detail.validate()?;
            }
            self.record_progress(job_id, proof, sequence).await
        })
    }

    fn lease_is_current(
        &self,
        _proof: BrowserLeaseProof,
    ) -> ApplicationFuture<'_, Result<bool, BrowserProviderRuntimeError>> {
        Box::pin(async { Ok(false) })
    }

    fn consume_gateway_ticket(
        &self,
        admission: BrowserGatewayAdmission,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    fn register_provider(
        &self,
        registration: BrowserLeaseRegistration,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>>;

    fn renew_provider(
        &self,
        proof: BrowserLeaseProof,
        ttl_millis: NonZeroU64,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>>;

    fn release_provider(
        &self,
        proof: BrowserLeaseProof,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    /// Reassign delivery of an already produced result after the original page
    /// loses its lease. None means the original owner is still live. The program
    /// request and parent Tool identity remain immutable; no execution is granted.
    fn recover_candidate(
        &self,
        _proof: BrowserLeaseProof,
        _job_id: BrowserJobId,
        _request_hash: musteragent_content_ref::Sha256,
    ) -> ApplicationFuture<'_, Result<Option<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(async { Err(BrowserProviderRuntimeError::JobNotFound) })
    }

    /// Called only after recover_candidate authorizes the original job. Missing
    /// parent state must retain the candidate, never imply successful file commit.
    fn candidate_settlement(
        &self,
        _proof: BrowserLeaseProof,
        _job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<BrowserCandidateSettlement, BrowserProviderRuntimeError>>
    {
        Box::pin(async { Ok(BrowserCandidateSettlement::Pending) })
    }

    fn load_job(
        &self,
        _tenant_id: TenantId,
        _job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Option<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(async { Ok(None) })
    }

    fn route_and_enqueue(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> ApplicationFuture<'_, Result<DurableBrowserJob, BrowserProviderRuntimeError>>;

    fn list_deliverable(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>>;

    fn list_cancellations(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>>;

    fn claim_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        delivery_sequence: u64,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    fn start_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    fn record_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    fn request_cancel(
        &self,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>>;

    fn seal_terminal(
        &self,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
        result: BrowserJobResult,
    ) -> ApplicationFuture<'_, Result<BrowserJobTerminalReceipt, BrowserProviderRuntimeError>>;

    fn load_terminal_receipt(
        &self,
        tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Option<BrowserJobTerminalReceipt>, BrowserProviderRuntimeError>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct BrowserPageKey {
    tenant_id: TenantId,
    principal_id: PrincipalId,
    browser_session_id: BrowserSessionId,
    page_instance_id: PageInstanceId,
}

impl BrowserPageKey {
    fn registration(value: &BrowserLeaseRegistration) -> Self {
        Self {
            tenant_id: value.tenant_id,
            principal_id: value.principal_id,
            browser_session_id: value.browser_session_id,
            page_instance_id: value.page_instance_id,
        }
    }

    fn lease(value: &BrowserProviderLease) -> Self {
        Self {
            tenant_id: value.tenant_id,
            principal_id: value.principal_id,
            browser_session_id: value.browser_session_id,
            page_instance_id: value.page_instance_id,
        }
    }
}

#[derive(Debug, Default)]
struct BrowserPageSlot {
    executor: Option<musteragent_sandbox::SandboxExecutor>,
    last_epoch: u64,
    last_fence: u64,
    active_lease_id: Option<BrowserLeaseId>,
}

type ProviderIdentity = (
    musteragent_sandbox::SandboxExecutor,
    &'static str,
    Option<TenantId>,
    Option<PrincipalId>,
);

/// Process-local control plane for short-lived page providers. Counters remain
/// after release/expiry so a reused page identity cannot recreate an old fence.
#[derive(Debug, Default)]
pub struct BrowserLeaseRegistry {
    leases: BTreeMap<BrowserLeaseId, BrowserProviderLease>,
    pages: BTreeMap<BrowserPageKey, BrowserPageSlot>,
    identities: BTreeMap<CapabilityProviderId, ProviderIdentity>,
}

impl BrowserLeaseRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        registration: BrowserLeaseRegistration,
        now: UtcTimestamp,
    ) -> Result<BrowserProviderLease, BrowserProviderRuntimeError> {
        registration.validate()?;
        let (scope, tenant, principal) = registration.authority();
        let identity = (registration.executor, scope, tenant, principal);
        if self
            .identities
            .get(&registration.provider_id)
            .is_some_and(|old| old != &identity)
        {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }

        let key = BrowserPageKey::registration(&registration);
        let slot = self.pages.entry(key).or_default();
        if slot.executor.is_some_and(|e| e != registration.executor) {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let epoch = slot
            .last_epoch
            .checked_add(1)
            .and_then(|value| BrowserLeaseEpoch::new(value).ok())
            .ok_or(BrowserProviderRuntimeError::FenceExhausted)?;
        let fence = slot
            .last_fence
            .checked_add(1)
            .and_then(|value| FencingToken::new(value).ok())
            .ok_or(BrowserProviderRuntimeError::FenceExhausted)?;
        let ttl = i64::try_from(registration.ttl_millis.get())
            .map_err(|_| BrowserProviderRuntimeError::InvalidRegistration)?;
        let expires_at = now
            .checked_add_millis(ttl)
            .ok_or(BrowserProviderRuntimeError::InvalidRegistration)?;
        if let Some(previous) = slot.active_lease_id.take() {
            self.leases.remove(&previous);
        }
        let lease = BrowserProviderLease {
            lease_id: BrowserLeaseId::generate(),
            tenant_id: registration.tenant_id,
            principal_id: registration.principal_id,
            browser_session_id: registration.browser_session_id,
            page_instance_id: registration.page_instance_id,
            provider_id: registration.provider_id,
            runtime_release_ref: registration.runtime_release_ref,
            capabilities: registration.capabilities,
            resource_profile_ref: registration.resource_profile_ref,
            epoch,
            fence,
            issued_at: now,
            expires_at,
        };
        lease.validate()?;
        slot.executor = Some(registration.executor);
        self.identities.insert(registration.provider_id, identity);
        slot.last_epoch = epoch.get();
        slot.last_fence = fence.get();
        slot.active_lease_id = Some(lease.lease_id);
        self.leases.insert(lease.lease_id, lease.clone());
        Ok(lease)
    }

    pub fn renew(
        &mut self,
        proof: BrowserLeaseProof,
        ttl_millis: NonZeroU64,
        now: UtcTimestamp,
    ) -> Result<BrowserProviderLease, BrowserProviderRuntimeError> {
        if ttl_millis.get() > MAX_BROWSER_LEASE_TTL_MILLIS {
            return Err(BrowserProviderRuntimeError::InvalidRegistration);
        }
        let current = self.authorize_proof(proof, now)?.clone();
        let ttl = i64::try_from(ttl_millis.get())
            .map_err(|_| BrowserProviderRuntimeError::InvalidRegistration)?;
        let expires_at = now
            .checked_add_millis(ttl)
            .ok_or(BrowserProviderRuntimeError::InvalidRegistration)?;
        let mut renewed = current;
        renewed.expires_at = expires_at;
        renewed.validate()?;
        self.leases.insert(renewed.lease_id, renewed.clone());
        Ok(renewed)
    }

    pub fn release(&mut self, proof: BrowserLeaseProof) -> Result<(), BrowserProviderRuntimeError> {
        let lease = self
            .leases
            .get(&proof.lease_id)
            .ok_or(BrowserProviderRuntimeError::StaleLease)?;
        verify_proof(lease, proof)?;
        let key = BrowserPageKey::lease(lease);
        if self.pages.get(&key).and_then(|slot| slot.active_lease_id) != Some(lease.lease_id) {
            return Err(BrowserProviderRuntimeError::StaleLease);
        }
        self.leases.remove(&proof.lease_id);
        if let Some(slot) = self.pages.get_mut(&key) {
            slot.active_lease_id = None;
        }
        Ok(())
    }

    pub fn authorize(
        &self,
        proof: BrowserLeaseProof,
        capability: &CapabilityKey,
        runtime_release_ref: &VersionedKey,
        now: UtcTimestamp,
    ) -> Result<&BrowserProviderLease, BrowserProviderRuntimeError> {
        let lease = self.authorize_proof(proof, now)?;
        if lease.runtime_release_ref != *runtime_release_ref {
            return Err(BrowserProviderRuntimeError::RuntimeReleaseMismatch);
        }
        if !lease.authorizes(capability, now) {
            return Err(BrowserProviderRuntimeError::CapabilityNotAuthorized);
        }
        Ok(lease)
    }

    /// Select the newest valid page-bound provider for the exact actor,
    /// capability and immutable runtime release. Selection never crosses a
    /// principal boundary and returns the full fence proof used by delivery.
    pub fn select(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        capability: &CapabilityKey,
        runtime_release_ref: &VersionedKey,
        now: UtcTimestamp,
    ) -> Result<BrowserLeaseProof, BrowserProviderRuntimeError> {
        self.leases
            .values()
            .filter(|lease| {
                lease.tenant_id == tenant_id
                    && lease.principal_id == principal_id
                    && lease.runtime_release_ref == *runtime_release_ref
                    && lease.authorizes(capability, now)
                    && self
                        .pages
                        .get(&BrowserPageKey::lease(lease))
                        .and_then(|slot| slot.active_lease_id)
                        == Some(lease.lease_id)
            })
            .max_by_key(|lease| (lease.issued_at, lease.epoch, lease.fence, lease.lease_id))
            .map(BrowserLeaseProof::from)
            .ok_or(BrowserProviderRuntimeError::ProviderUnavailable)
    }

    pub fn prune_expired(&mut self, now: UtcTimestamp) -> usize {
        let expired = self
            .leases
            .values()
            .filter(|lease| lease.expires_at <= now)
            .map(|lease| (lease.lease_id, BrowserPageKey::lease(lease)))
            .collect::<Vec<_>>();
        for (lease_id, key) in &expired {
            self.leases.remove(lease_id);
            if let Some(slot) = self.pages.get_mut(key)
                && slot.active_lease_id == Some(*lease_id)
            {
                slot.active_lease_id = None;
            }
        }
        expired.len()
    }

    fn authorize_proof(
        &self,
        proof: BrowserLeaseProof,
        now: UtcTimestamp,
    ) -> Result<&BrowserProviderLease, BrowserProviderRuntimeError> {
        let lease = self
            .leases
            .get(&proof.lease_id)
            .ok_or(BrowserProviderRuntimeError::StaleLease)?;
        verify_proof(lease, proof)?;
        let key = BrowserPageKey::lease(lease);
        if self.pages.get(&key).and_then(|slot| slot.active_lease_id) != Some(lease.lease_id) {
            return Err(BrowserProviderRuntimeError::StaleLease);
        }
        if now < lease.issued_at || now >= lease.expires_at {
            return Err(BrowserProviderRuntimeError::LeaseExpired);
        }
        Ok(lease)
    }
}

fn verify_proof(
    lease: &BrowserProviderLease,
    proof: BrowserLeaseProof,
) -> Result<(), BrowserProviderRuntimeError> {
    if proof.lease_id != lease.lease_id
        || proof.tenant_id != lease.tenant_id
        || proof.principal_id != lease.principal_id
        || proof.browser_session_id != lease.browser_session_id
        || proof.page_instance_id != lease.page_instance_id
        || proof.provider_id != lease.provider_id
        || proof.epoch != lease.epoch
        || proof.fence != lease.fence
    {
        return Err(BrowserProviderRuntimeError::StaleLease);
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BrowserJobDeliveryState {
    Queued,
    Claimed,
    Running,
    CancelRequested,
    Terminal,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BrowserJobDelivery {
    pub request: BrowserJobRequest,
    pub capability: CapabilityKey,
    pub lease: BrowserLeaseProof,
    pub state: BrowserJobDeliveryState,
    pub last_provider_sequence: u64,
    pub terminal_result: Option<BrowserJobResult>,
}

/// Fenced, single-consumer Browser Job delivery state. Callers must durably
/// seal terminal receipts before using the returned result to finish a Tool.
#[derive(Debug, Default)]
pub struct BrowserJobQueue {
    jobs: BTreeMap<BrowserJobId, BrowserJobDelivery>,
}

impl BrowserJobQueue {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(
        &mut self,
        registry: &BrowserLeaseRegistry,
        proof: BrowserLeaseProof,
        capability: CapabilityKey,
        request: BrowserJobRequest,
        now: UtcTimestamp,
    ) -> Result<(), BrowserProviderRuntimeError> {
        request.validate()?;
        if request.security_profile == musteragent_capability::BrowserSecurityProfile::ProxyEgress
            && registry
                .identities
                .get(&proof.provider_id)
                .is_none_or(|i| i.0 != musteragent_sandbox::SandboxExecutor::Server)
        {
            return Err(BrowserProviderRuntimeError::ProviderContractMismatch);
        }
        registry.authorize(proof, &capability, &request.runtime_release_ref, now)?;
        if self.jobs.contains_key(&request.job_id) {
            return Err(BrowserProviderRuntimeError::DuplicateJob);
        }
        self.jobs.insert(
            request.job_id,
            BrowserJobDelivery {
                request,
                capability,
                lease: proof,
                state: BrowserJobDeliveryState::Queued,
                last_provider_sequence: 0,
                terminal_result: None,
            },
        );
        Ok(())
    }

    pub fn route_and_enqueue(
        &mut self,
        registry: &BrowserLeaseRegistry,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
        now: UtcTimestamp,
    ) -> Result<BrowserLeaseProof, BrowserProviderRuntimeError> {
        if !contract.accepts_request(&request) {
            return Err(BrowserProviderRuntimeError::ProviderContractMismatch);
        }
        let capability = contract.capability_key();
        let proof = registry.select(
            tenant_id,
            principal_id,
            &capability,
            &request.runtime_release_ref,
            now,
        )?;
        self.enqueue(registry, proof, capability, request, now)?;
        Ok(proof)
    }

    pub fn claim(
        &mut self,
        registry: &BrowserLeaseRegistry,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        now: UtcTimestamp,
    ) -> Result<BrowserJobRequest, BrowserProviderRuntimeError> {
        let job = self.job_mut(job_id, proof)?;
        authorize_job(registry, job, proof, now)?;
        if job.state != BrowserJobDeliveryState::Queued {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        advance_sequence(job, sequence)?;
        job.state = BrowserJobDeliveryState::Claimed;
        Ok(job.request.clone())
    }

    pub fn start(
        &mut self,
        registry: &BrowserLeaseRegistry,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        now: UtcTimestamp,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let job = self.job_mut(job_id, proof)?;
        authorize_job(registry, job, proof, now)?;
        if job.state != BrowserJobDeliveryState::Claimed {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        advance_sequence(job, sequence)?;
        job.state = BrowserJobDeliveryState::Running;
        Ok(())
    }

    pub fn progress(
        &mut self,
        registry: &BrowserLeaseRegistry,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        now: UtcTimestamp,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let job = self.job_mut(job_id, proof)?;
        authorize_job(registry, job, proof, now)?;
        if job.state != BrowserJobDeliveryState::Running
            && job.state != BrowserJobDeliveryState::CancelRequested
        {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        advance_sequence(job, sequence)
    }

    pub fn request_cancel(
        &mut self,
        job_id: BrowserJobId,
    ) -> Result<(), BrowserProviderRuntimeError> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(BrowserProviderRuntimeError::JobNotFound)?;
        if job.state != BrowserJobDeliveryState::Claimed
            && job.state != BrowserJobDeliveryState::Running
        {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        job.state = BrowserJobDeliveryState::CancelRequested;
        Ok(())
    }

    pub fn complete(
        &mut self,
        registry: &BrowserLeaseRegistry,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        result: BrowserJobResult,
        now: UtcTimestamp,
    ) -> Result<BrowserJobResult, BrowserProviderRuntimeError> {
        let job = self.job_mut(job_id, proof)?;
        authorize_job(registry, job, proof, now)?;
        if job.state != BrowserJobDeliveryState::Running
            && job.state != BrowserJobDeliveryState::CancelRequested
        {
            return Err(BrowserProviderRuntimeError::InvalidJobTransition);
        }
        advance_sequence(job, sequence)?;
        job.request.validate_result(&result)?;
        // Only the original execution owner's receipt establishes the outcome;
        // a cancellation request cannot overwrite completion or uncertainty.
        job.state = BrowserJobDeliveryState::Terminal;
        job.terminal_result = Some(result.clone());
        Ok(result)
    }

    #[must_use]
    pub fn get(&self, job_id: BrowserJobId) -> Option<&BrowserJobDelivery> {
        self.jobs.get(&job_id)
    }

    fn job_mut(
        &mut self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
    ) -> Result<&mut BrowserJobDelivery, BrowserProviderRuntimeError> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(BrowserProviderRuntimeError::JobNotFound)?;
        if job.lease != proof {
            return Err(BrowserProviderRuntimeError::StaleLease);
        }
        Ok(job)
    }
}

fn authorize_job(
    registry: &BrowserLeaseRegistry,
    job: &BrowserJobDelivery,
    proof: BrowserLeaseProof,
    now: UtcTimestamp,
) -> Result<(), BrowserProviderRuntimeError> {
    registry.authorize(
        proof,
        &job.capability,
        &job.request.runtime_release_ref,
        now,
    )?;
    Ok(())
}

fn advance_sequence(
    job: &mut BrowserJobDelivery,
    sequence: u64,
) -> Result<(), BrowserProviderRuntimeError> {
    let expected = job
        .last_provider_sequence
        .checked_add(1)
        .ok_or(BrowserProviderRuntimeError::SequenceExhausted)?;
    if sequence != expected {
        return Err(BrowserProviderRuntimeError::InvalidProviderSequence);
    }
    job.last_provider_sequence = sequence;
    Ok(())
}

#[derive(Debug, Error)]
pub enum BrowserProviderRuntimeError {
    #[error("sandbox routing and durable route persistence are not configured")]
    SandboxRoutingUnavailable,
    #[error(transparent)]
    SandboxRoute(#[from] musteragent_sandbox::SandboxRouteError),
    #[error("sandbox route conflicts with the previously committed decision")]
    RouteConflict,
    #[error("Browser provider registration is invalid")]
    InvalidRegistration,
    #[error("Browser provider epoch or fence namespace is exhausted")]
    FenceExhausted,
    #[error("Browser provider lease proof is stale")]
    StaleLease,
    #[error("Browser provider lease has expired")]
    LeaseExpired,
    #[error("Browser provider does not authorize the requested Capability")]
    CapabilityNotAuthorized,
    #[error("no active Browser provider satisfies the requested binding")]
    ProviderUnavailable,
    #[error("Browser provider contract does not accept the Job engine")]
    ProviderContractMismatch,
    #[error("Browser provider Runtime release differs from the Job release")]
    RuntimeReleaseMismatch,
    #[error("Browser Job already exists")]
    DuplicateJob,
    #[error("Browser Job does not exist")]
    JobNotFound,
    #[error("Browser Job state transition is invalid")]
    InvalidJobTransition,
    #[error("Browser provider frame sequence is not the next sequence")]
    InvalidProviderSequence,
    #[error("Browser provider frame sequence namespace is exhausted")]
    SequenceExhausted,
    #[error("Browser Gateway durable state is temporarily unavailable")]
    DurabilityUnavailable,
    #[error("Browser Gateway durable state is corrupt")]
    CorruptDurableState,
    #[error("Browser Gateway ticket was already consumed")]
    TicketReplayed,
    #[error(transparent)]
    JobContract(#[from] BrowserJobContractError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::{NonZeroU32, NonZeroU64};

    use musteragent_capability::{
        BrowserDeclaredOutput, BrowserInput, BrowserJobEngine, BrowserJobMetrics,
        BrowserJobProgram, BrowserJobStatus, BrowserResourceLimits, BrowserSecurityProfile,
        StagedBrowserOutput,
    };
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{
        BrowserSessionId, ContentId, InvocationId, PageInstanceId, PrincipalId, TenantId,
    };

    use super::*;

    fn capability() -> CapabilityKey {
        CapabilityKey::from_versioned_key(
            VersionedKey::new("browser.python.execute@1").expect("capability"),
        )
    }

    fn registration() -> BrowserLeaseRegistration {
        BrowserLeaseRegistration {
            executor: musteragent_sandbox::SandboxExecutor::Browser,
            scope: BrowserProviderScope::Actor,
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            browser_session_id: BrowserSessionId::generate(),
            page_instance_id: PageInstanceId::generate(),
            provider_id: CapabilityProviderId::generate(),
            runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
            capabilities: BTreeSet::from([capability()]),
            resource_profile_ref: VersionedKey::new("browser.resource@1").expect("profile"),
            feature_probe: BrowserRuntimeFeatureProbe {
                worker: true,
                web_assembly: true,
                crypto_subtle: true,
                transferable_array_buffer: true,
                service_worker: true,
                indexed_db: true,
                secure_context: true,
                engines: BTreeSet::from([BrowserJobEngine::Python]),
            },
            ttl_millis: NonZeroU64::new(30_000).expect("ttl"),
        }
    }

    fn content(name: &str, media_type: &str) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media"),
            byte_length: u64::try_from(name.len()).expect("length"),
            sha256: Sha256::digest(name),
        }
    }

    fn request() -> BrowserJobRequest {
        let source = content("print('ok')", "text/x-python");
        BrowserJobRequest {
            execution_liveness: None,
            job_id: BrowserJobId::generate(),
            invocation_id: InvocationId::generate(),
            conversation_id: musteragent_identity::ConversationId::generate(),
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
                media_type: MediaType::new("text/csv").expect("media"),
                maximum_byte_length: NonZeroU64::new(1_024).expect("limit"),
                required: true,
            }],
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(30_000).expect("limit"),
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
    fn web_provider_registration_requires_the_complete_preview_platform() {
        let mut registration = registration();
        registration.capabilities =
            BTreeSet::from([BrowserProviderContract::WebBuild.capability_key()]);
        registration.feature_probe.engines = BTreeSet::from([BrowserJobEngine::Esbuild]);
        registration.feature_probe.service_worker = false;

        assert!(matches!(
            registration.validate(),
            Err(BrowserProviderRuntimeError::InvalidRegistration)
        ));
        registration.feature_probe.service_worker = true;
        assert!(registration.validate().is_ok());
    }

    #[test]
    fn re_registration_rotates_epoch_and_fence_and_invalidates_old_proof() {
        let mut registry = BrowserLeaseRegistry::new();
        let registration = registration();
        let first = registry
            .register(registration.clone(), UtcTimestamp::from_unix_millis(1_000))
            .expect("first lease");
        let second = registry
            .register(registration, UtcTimestamp::from_unix_millis(2_000))
            .expect("second lease");

        assert_eq!(second.epoch.get(), first.epoch.get() + 1);
        assert_eq!(second.fence.get(), first.fence.get() + 1);
        assert!(matches!(
            registry.authorize(
                BrowserLeaseProof::from(&first),
                &capability(),
                &first.runtime_release_ref,
                UtcTimestamp::from_unix_millis(2_001),
            ),
            Err(BrowserProviderRuntimeError::StaleLease)
        ));
    }

    #[test]
    fn job_delivery_is_single_consumer_fenced_and_sequenced() {
        let mut registry = BrowserLeaseRegistry::new();
        let lease = registry
            .register(registration(), UtcTimestamp::from_unix_millis(1_000))
            .expect("lease");
        let proof = BrowserLeaseProof::from(&lease);
        let request = request();
        let job_id = request.job_id;
        let mut queue = BrowserJobQueue::new();
        queue
            .enqueue(
                &registry,
                proof,
                capability(),
                request.clone(),
                UtcTimestamp::from_unix_millis(1_001),
            )
            .expect("enqueue");
        queue
            .claim(
                &registry,
                job_id,
                proof,
                1,
                UtcTimestamp::from_unix_millis(1_002),
            )
            .expect("claim");
        assert!(matches!(
            queue.claim(
                &registry,
                job_id,
                proof,
                2,
                UtcTimestamp::from_unix_millis(1_003),
            ),
            Err(BrowserProviderRuntimeError::InvalidJobTransition)
        ));
        queue
            .start(
                &registry,
                job_id,
                proof,
                2,
                UtcTimestamp::from_unix_millis(1_004),
            )
            .expect("start");
        assert!(matches!(
            queue.progress(
                &registry,
                job_id,
                proof,
                4,
                UtcTimestamp::from_unix_millis(1_005),
            ),
            Err(BrowserProviderRuntimeError::InvalidProviderSequence)
        ));

        let output = content("a,b\n1,2\n", "text/csv");
        let result = BrowserJobResult {
            job_id,
            status: BrowserJobStatus::Completed,
            outputs: vec![StagedBrowserOutput {
                path: "result.csv".to_owned(),
                content_ref: output.clone(),
                staging_receipt_ref: "stage.receipt".to_owned(),
            }],
            diagnostics: Vec::new(),
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            metrics: BrowserJobMetrics {
                wall_time_millis: 10,
                input_bytes: request.inputs[0].content_ref.byte_length,
                output_bytes: output.byte_length,
                output_files: 1,
            },
            result_hash: Sha256::digest(b"unsealed"),
        }
        .with_canonical_hash();
        queue
            .complete(
                &registry,
                job_id,
                proof,
                3,
                result.clone(),
                UtcTimestamp::from_unix_millis(1_006),
            )
            .expect("complete");
        assert_eq!(
            queue.get(job_id).map(|job| job.state),
            Some(BrowserJobDeliveryState::Terminal)
        );
        assert_eq!(
            queue
                .get(job_id)
                .and_then(|job| job.terminal_result.as_ref()),
            Some(&result)
        );
    }

    #[test]
    fn routing_selects_only_an_exact_actor_release_and_contract() {
        let mut registry = BrowserLeaseRegistry::new();
        let registration = registration();
        let tenant_id = registration.tenant_id;
        let principal_id = registration.principal_id;
        let lease = registry
            .register(registration, UtcTimestamp::from_unix_millis(1_000))
            .expect("lease");
        let routed_request = request();
        let job_id = routed_request.job_id;
        let mut queue = BrowserJobQueue::new();
        let proof = queue
            .route_and_enqueue(
                &registry,
                tenant_id,
                principal_id,
                BrowserProviderContract::PythonExecute,
                routed_request,
                UtcTimestamp::from_unix_millis(1_001),
            )
            .expect("route");
        assert_eq!(proof, BrowserLeaseProof::from(&lease));
        assert_eq!(
            queue.get(job_id).map(|job| job.capability.as_str()),
            Some("browser.python.execute@1")
        );

        assert!(matches!(
            queue.route_and_enqueue(
                &registry,
                tenant_id,
                principal_id,
                BrowserProviderContract::WebBuild,
                request(),
                UtcTimestamp::from_unix_millis(1_002),
            ),
            Err(BrowserProviderRuntimeError::ProviderContractMismatch)
        ));
        assert!(matches!(
            registry.select(
                tenant_id,
                PrincipalId::generate(),
                &capability(),
                &lease.runtime_release_ref,
                UtcTimestamp::from_unix_millis(1_003),
            ),
            Err(BrowserProviderRuntimeError::ProviderUnavailable)
        ));
    }
    #[test]
    fn provider_scope_and_executor_are_immutable_after_release() {
        let mut registry = BrowserLeaseRegistry::new();
        let now = UtcTimestamp::from_unix_millis(1000);
        let mut tenant = registration();
        tenant.executor = musteragent_sandbox::SandboxExecutor::Server;
        tenant.scope = BrowserProviderScope::Tenant(tenant.tenant_id);
        let lease = registry.register(tenant.clone(), now).expect("tenant");
        registry
            .release(BrowserLeaseProof::from(&lease))
            .expect("release");
        let mut actor = tenant.clone();
        actor.principal_id = PrincipalId::generate();
        registry
            .register(actor, now)
            .expect("same tenant different principal");
        let mut foreign = tenant.clone();
        foreign.tenant_id = TenantId::generate();
        assert!(matches!(
            registry.register(foreign, now),
            Err(BrowserProviderRuntimeError::InvalidRegistration)
        ));
        let mut changed = tenant.clone();
        changed.scope = BrowserProviderScope::Global;
        assert!(matches!(
            registry.register(changed, now),
            Err(BrowserProviderRuntimeError::InvalidRegistration)
        ));
        tenant.executor = musteragent_sandbox::SandboxExecutor::Browser;
        tenant.scope = BrowserProviderScope::Actor;
        assert!(matches!(
            registry.register(tenant, now),
            Err(BrowserProviderRuntimeError::InvalidRegistration)
        ));
        let mut global = registration();
        global.executor = musteragent_sandbox::SandboxExecutor::Server;
        global.scope = BrowserProviderScope::Global;
        let first = registry.register(global.clone(), now).expect("global");
        global.tenant_id = TenantId::generate();
        global.principal_id = PrincipalId::generate();
        let second = registry
            .register(global, now)
            .expect("another global actor lease");
        assert_eq!(first.provider_id, second.provider_id);
        assert_ne!(first.tenant_id, second.tenant_id);
    }
}
