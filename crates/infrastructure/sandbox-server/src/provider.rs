//! Server-side provider: registers with the Browser Gateway store exactly as
//! a browser page would, renews its lease, claims deliverable jobs, executes
//! them in containers, reports progress and seals terminal results.
//!
//! Lease discipline: when renewal or any fenced store call reports a stale or
//! expired lease, every running container of that lease is killed and its
//! result is *not* sealed (the store's recovery owns those jobs); the provider
//! then registers a fresh lease.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use musteragent_capability::{
    BrowserJobEngine, BrowserLeaseProof, BrowserProviderContract, BrowserProviderLease,
    BrowserRuntimeFeatureProbe, CapabilityKey,
};
use musteragent_identity::{
    BrowserJobId, BrowserSessionId, CapabilityProviderId, PageInstanceId, PrincipalId, TenantId,
    VersionedKey,
};
use musteragent_runtime_capability::{
    BrowserExecutionProgress, BrowserGatewayStore, BrowserLeaseRegistration,
    BrowserProviderRuntimeError, BrowserProviderScope, DurableBrowserJob,
};
use musteragent_runtime_content::ContentStore;
use musteragent_sandbox::SandboxExecutor;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{ConfigError, ServerSandboxConfig};
use crate::job::{JobExecutor, Prepared, StopCause};
use crate::runtime::ContainerRuntime;

/// Shared job contracts implemented by the server sandbox.
pub const SERVER_PROVIDER_CONTRACTS: [BrowserProviderContract; 5] = [
    BrowserProviderContract::PythonExecute,
    BrowserProviderContract::PythonExecuteWorkingTree,
    BrowserProviderContract::PythonExecutePrepared,
    BrowserProviderContract::PythonExecuteWorkspace,
    BrowserProviderContract::PythonEnvironmentPrepare,
];

/// The tenant/principal a provider lease is bound to. The gateway contract
/// routes jobs only to providers of the exact same principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderBinding {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
}

/// One immutable pool registration, reusable for authorized actor bindings.
#[derive(Debug, Clone, Copy)]
pub struct ServerProviderRegistration {
    pub provider_id: CapabilityProviderId,
    pub scope: BrowserProviderScope,
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("server provider registration scope does not authorize this tenant")]
    RegistrationScope,
}

/// Server sandbox provider for one binding.
pub struct ServerSandboxProvider {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn BrowserGatewayStore>,
    executor: JobExecutor,
    config: Arc<ServerSandboxConfig>,
    binding: ProviderBinding,
    provider_id: CapabilityProviderId,
    scope: BrowserProviderScope,
    session_id: BrowserSessionId,
    page_id: PageInstanceId,
}

enum LeaseEnd {
    Shutdown,
    Lost,
}

impl ServerSandboxProvider {
    /// The executor kind this provider implements, for routing diagnostics.
    pub const EXECUTOR: SandboxExecutor = SandboxExecutor::Server;

    pub fn new(
        config: ServerSandboxConfig,
        binding: ProviderBinding,
        store: Arc<dyn BrowserGatewayStore>,
        content: Arc<dyn ContentStore>,
        runtime: Arc<dyn ContainerRuntime>,
    ) -> Result<Self, ProviderError> {
        Self::new_registered(
            config,
            binding,
            ServerProviderRegistration {
                provider_id: CapabilityProviderId::generate(),
                scope: BrowserProviderScope::Tenant(binding.tenant_id),
            },
            store,
            content,
            runtime,
        )
    }

    /// Bind a registered tenant/global pool to one actor before dispatch. A
    /// lease never grants a global provider access to another actor's content.
    pub fn new_registered(
        config: ServerSandboxConfig,
        binding: ProviderBinding,
        registration: ServerProviderRegistration,
        store: Arc<dyn BrowserGatewayStore>,
        content: Arc<dyn ContentStore>,
        runtime: Arc<dyn ContainerRuntime>,
    ) -> Result<Self, ProviderError> {
        config.validate()?;
        if matches!(registration.scope, BrowserProviderScope::Tenant(tenant) if tenant != binding.tenant_id)
        {
            return Err(ProviderError::RegistrationScope);
        }
        let config = Arc::new(config);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                executor: JobExecutor::new(Arc::clone(&config), content, runtime),
                config,
                binding,
                provider_id: registration.provider_id,
                scope: registration.scope,
                session_id: BrowserSessionId::generate(),
                page_id: PageInstanceId::generate(),
            }),
        })
    }

    #[must_use]
    pub fn provider_id(&self) -> CapabilityProviderId {
        self.inner.provider_id
    }

    /// The registration this provider presents to the gateway store.
    pub fn registration(&self) -> Result<BrowserLeaseRegistration, ProviderError> {
        self.inner.registration()
    }

    /// Runs until `shutdown` fires: register, serve, re-register on lease
    /// loss. On shutdown, running jobs are cancelled and sealed, then the
    /// lease is released.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), ProviderError> {
        let inner = &self.inner;
        let registration = inner.registration()?;
        let backoff = Duration::from_millis(inner.config.lease.reregister_backoff_millis);
        while !shutdown.is_cancelled() {
            let lease = match inner.store.register_provider(registration.clone()).await {
                Ok(lease) => lease,
                Err(error) => {
                    tracing::warn!(%error, "server sandbox provider registration failed");
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(backoff) => continue,
                    }
                }
            };
            tracing::info!(
                provider = %inner.provider_id.into_uuid(),
                lease = %lease.lease_id.into_uuid(),
                epoch = lease.epoch.get(),
                "server sandbox provider registered"
            );
            match Arc::clone(inner).serve_lease(lease, &shutdown).await {
                LeaseEnd::Shutdown => break,
                LeaseEnd::Lost => {
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(backoff) => {}
                    }
                }
            }
        }
        Ok(())
    }
}

impl Inner {
    fn registration(&self) -> Result<BrowserLeaseRegistration, ProviderError> {
        let invalid = || ProviderError::Config(ConfigError::InvalidReleaseRef);
        Ok(BrowserLeaseRegistration {
            executor: musteragent_sandbox::SandboxExecutor::Server,
            scope: self.scope,
            tenant_id: self.binding.tenant_id,
            principal_id: self.binding.principal_id,
            browser_session_id: self.session_id,
            page_instance_id: self.page_id,
            provider_id: self.provider_id,
            runtime_release_ref: VersionedKey::new(self.config.runtime_release_ref.as_str())
                .map_err(|_| invalid())?,
            capabilities: SERVER_PROVIDER_CONTRACTS
                .iter()
                .map(|contract| contract.capability_key())
                .collect::<BTreeSet<CapabilityKey>>(),
            resource_profile_ref: VersionedKey::new(self.config.resource_profile_ref.as_str())
                .map_err(|_| invalid())?,
            // The probe is browser-shaped; a container satisfies the
            // execution guarantees it stands for. Browser-only storage
            // features are reported absent; native prepared environments
            // use the immutable image and ContentRef receipt instead.
            feature_probe: BrowserRuntimeFeatureProbe {
                worker: true,
                web_assembly: true,
                crypto_subtle: true,
                transferable_array_buffer: true,
                service_worker: false,
                indexed_db: false,
                secure_context: true,
                engines: BTreeSet::from([
                    BrowserJobEngine::Python,
                    BrowserJobEngine::PythonEnvironment,
                ]),
            },
            ttl_millis: NonZeroU64::new(self.config.lease.ttl_millis)
                .ok_or(ProviderError::Config(ConfigError::InvalidLease))?,
        })
    }

    async fn serve_lease(
        self: Arc<Self>,
        lease: BrowserProviderLease,
        shutdown: &CancellationToken,
    ) -> LeaseEnd {
        let proof = BrowserLeaseProof::from(&lease);
        let lease_lost = CancellationToken::new();
        let mut running: HashMap<BrowserJobId, CancellationToken> = HashMap::new();
        let mut task_jobs: HashMap<tokio::task::Id, BrowserJobId> = HashMap::new();
        let mut tasks: JoinSet<()> = JoinSet::new();
        let renew_every = self.config.lease.renew_interval();
        let ttl = NonZeroU64::new(self.config.lease.ttl_millis).unwrap_or(NonZeroU64::MIN);
        let mut next_renew = Instant::now() + renew_every;
        let mut tick = tokio::time::interval(Duration::from_millis(
            self.config.lease.poll_interval_millis,
        ));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let end = loop {
            tokio::select! {
                () = shutdown.cancelled() => break LeaseEnd::Shutdown,
                () = lease_lost.cancelled() => break LeaseEnd::Lost,
                Some(joined) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    let id = match joined {
                        Ok((id, ())) => id,
                        Err(error) => error.id(),
                    };
                    if let Some(job_id) = task_jobs.remove(&id) {
                        running.remove(&job_id);
                    }
                    continue;
                }
                _ = tick.tick() => {}
            }

            if Instant::now() >= next_renew {
                match self.store.renew_provider(proof, ttl).await {
                    Ok(_) => next_renew = Instant::now() + renew_every,
                    Err(error) => {
                        tracing::warn!(%error, "server sandbox lease renewal failed");
                        if is_lease_error(&error) {
                            break LeaseEnd::Lost;
                        }
                    }
                }
            }

            match self.store.list_cancellations(proof, 64).await {
                Ok(jobs) => {
                    for job in jobs {
                        if let Some(token) = running.get(&job.delivery.request.job_id) {
                            token.cancel();
                        }
                    }
                }
                Err(error) if is_lease_error(&error) => break LeaseEnd::Lost,
                Err(error) => tracing::debug!(%error, "listing cancellations failed"),
            }

            let free = usize::from(self.config.max_concurrent_jobs).saturating_sub(running.len());
            if free == 0 {
                continue;
            }
            let limit = u16::try_from(free).unwrap_or(u16::MAX);
            match self.store.list_deliverable(proof, limit).await {
                Ok(jobs) => {
                    for job in jobs {
                        let job_id = job.delivery.request.job_id;
                        if running.contains_key(&job_id) || job.delivery.lease != proof {
                            continue;
                        }
                        let cancel = lease_lost.child_token();
                        running.insert(job_id, cancel.clone());
                        let signals = JobSignals {
                            cancel,
                            lease_lost: lease_lost.clone(),
                            shutdown: shutdown.clone(),
                        };
                        let handle = tasks.spawn(Arc::clone(&self).run_job(proof, job, signals));
                        task_jobs.insert(handle.id(), job_id);
                    }
                }
                Err(error) if is_lease_error(&error) => break LeaseEnd::Lost,
                Err(error) => tracing::debug!(%error, "listing deliverable jobs failed"),
            }
        };

        match end {
            LeaseEnd::Lost => lease_lost.cancel(),
            LeaseEnd::Shutdown => running.values().for_each(CancellationToken::cancel),
        }
        while tasks.join_next().await.is_some() {}
        if matches!(end, LeaseEnd::Shutdown)
            && let Err(error) = self.store.release_provider(proof).await
        {
            tracing::warn!(%error, "server sandbox lease release failed");
        }
        end
    }

    async fn run_job(
        self: Arc<Self>,
        proof: BrowserLeaseProof,
        job: DurableBrowserJob,
        signals: JobSignals,
    ) {
        let request = &job.delivery.request;
        let job_id = request.job_id;
        let mut sequence = 0_u64;
        let mut next = || {
            sequence += 1;
            sequence
        };
        if let Err(error) = self
            .store
            .claim_job(job_id, proof, job.delivery_sequence, next())
            .await
        {
            tracing::warn!(job = %job_id.into_uuid(), %error, "server sandbox job claim failed");
            signals.on_store_error(&error);
            return;
        }
        if let Err(error) = self.store.start_job(job_id, proof, next()).await {
            signals.on_store_error(&error);
            return;
        }
        if !self
            .progress(job_id, proof, next(), "initializing", &signals)
            .await
        {
            return;
        }
        let tenant_id = proof.tenant_id;
        let result = match self.executor.prepare(tenant_id, request).await {
            Prepared::Terminal(result) => result,
            Prepared::Ready(prepared) => {
                if !self
                    .progress(job_id, proof, next(), "running", &signals)
                    .await
                {
                    return;
                }
                let outcome = self.executor.run(&prepared, signals.cancel.clone()).await;
                if signals.lease_lost.is_cancelled() {
                    tracing::warn!(job = %job_id.into_uuid(), "server sandbox lease lost; job abandoned");
                    return;
                }
                if !self
                    .progress(job_id, proof, next(), "collecting", &signals)
                    .await
                {
                    return;
                }
                let stop = signals.cancel.is_cancelled().then(|| {
                    if signals.shutdown.is_cancelled() {
                        StopCause::Shutdown
                    } else {
                        StopCause::CancelRequested
                    }
                });
                self.executor
                    .finish(tenant_id, request, prepared, outcome, stop)
                    .await
            }
        };
        if signals.lease_lost.is_cancelled() {
            return;
        }
        match self.store.seal_terminal(proof, next(), result).await {
            Ok(receipt) => tracing::info!(
                job = %job_id.into_uuid(),
                status = ?receipt.result.status,
                "server sandbox job sealed"
            ),
            Err(error) => {
                tracing::warn!(job = %job_id.into_uuid(), %error, "server sandbox job seal failed");
                signals.on_store_error(&error);
            }
        }
    }

    async fn progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        phase: &str,
        signals: &JobSignals,
    ) -> bool {
        let detail = BrowserExecutionProgress {
            phase: phase.to_owned(),
            package_name: None,
            transferred_bytes: None,
            total_bytes: None,
            message: None,
        };
        match self
            .store
            .record_execution_progress(job_id, proof, sequence, Some(detail))
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(job = %job_id.into_uuid(), %error, "server sandbox progress rejected");
                signals.on_store_error(&error);
                false
            }
        }
    }
}

struct JobSignals {
    /// Kills this job's container (child of `lease_lost`).
    cancel: CancellationToken,
    lease_lost: CancellationToken,
    shutdown: CancellationToken,
}

impl JobSignals {
    fn on_store_error(&self, error: &BrowserProviderRuntimeError) {
        if is_lease_error(error) {
            self.lease_lost.cancel();
        } else {
            self.cancel.cancel();
        }
    }
}

fn is_lease_error(error: &BrowserProviderRuntimeError) -> bool {
    matches!(
        error,
        BrowserProviderRuntimeError::StaleLease
            | BrowserProviderRuntimeError::LeaseExpired
            | BrowserProviderRuntimeError::FenceExhausted
    )
}
