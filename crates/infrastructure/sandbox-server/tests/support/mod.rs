//! In-memory fakes of the gateway store and content store ports.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use musteragent_capability::{
    BrowserDeclaredOutput, BrowserInput, BrowserJobEngine, BrowserJobProgram, BrowserJobRequest,
    BrowserJobResult, BrowserLeaseProof, BrowserProviderContract, BrowserProviderLease,
    BrowserResourceLimits, BrowserSecurityProfile,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    BrowserJobId, ContentId, ConversationId, InvocationId, PrincipalId, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_runtime_capability::{
    BrowserExecutionProgress, BrowserGatewayAdmission, BrowserGatewayStore,
    BrowserJobDeliveryState, BrowserJobQueue, BrowserJobTerminalReceipt, BrowserLeaseRegistration,
    BrowserLeaseRegistry, BrowserProviderRuntimeError, DurableBrowserJob,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_sandbox_server::ServerSandboxConfig;
use musteragent_sandbox_server::config::{HostLimits, ImageConfig, LeaseConfig};

pub const RELEASE: &str = "browser.runtime@1";

fn now() -> UtcTimestamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    UtcTimestamp::from_unix_millis(i64::try_from(millis).expect("millis"))
}

#[derive(Default)]
struct StoreState {
    registry: BrowserLeaseRegistry,
    queue: BrowserJobQueue,
    jobs: Vec<BrowserJobId>,
    delivery_sequence: u64,
    receipts: HashMap<BrowserJobId, BrowserJobTerminalReceipt>,
    progress: HashMap<BrowserJobId, Vec<BrowserExecutionProgress>>,
    leases: Vec<BrowserProviderLease>,
    releases: usize,
}

/// Gateway store backed by the kernel's process-local registry and queue.
#[derive(Default)]
pub struct FakeGatewayStore {
    state: Mutex<StoreState>,
}

impl FakeGatewayStore {
    pub fn receipt(&self, job_id: BrowserJobId) -> Option<BrowserJobTerminalReceipt> {
        self.state
            .lock()
            .expect("lock")
            .receipts
            .get(&job_id)
            .cloned()
    }

    pub fn progress_phases(&self, job_id: BrowserJobId) -> Vec<String> {
        self.state
            .lock()
            .expect("lock")
            .progress
            .get(&job_id)
            .map(|items| items.iter().map(|item| item.phase.clone()).collect())
            .unwrap_or_default()
    }

    pub fn delivery_state(&self, job_id: BrowserJobId) -> Option<BrowserJobDeliveryState> {
        self.state
            .lock()
            .expect("lock")
            .queue
            .get(job_id)
            .map(|delivery| delivery.state)
    }

    pub fn lease_count(&self) -> usize {
        self.state.lock().expect("lock").leases.len()
    }

    pub fn release_count(&self) -> usize {
        self.state.lock().expect("lock").releases
    }

    pub fn current_lease(&self) -> Option<BrowserProviderLease> {
        self.state.lock().expect("lock").leases.last().cloned()
    }

    /// Simulates lease loss: the current lease is revoked behind the
    /// provider's back.
    pub fn revoke_current_lease(&self) {
        let mut state = self.state.lock().expect("lock");
        if let Some(lease) = state.leases.last().cloned() {
            let _ = state.registry.release(BrowserLeaseProof::from(&lease));
        }
    }

    pub fn enqueue(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> Result<DurableBrowserJob, BrowserProviderRuntimeError> {
        let mut state = self.state.lock().expect("lock");
        let StoreState {
            registry, queue, ..
        } = &mut *state;
        queue.route_and_enqueue(
            registry,
            tenant_id,
            principal_id,
            contract,
            request.clone(),
            now(),
        )?;
        state.jobs.push(request.job_id);
        state.delivery_sequence += 1;
        let delivery = state.queue.get(request.job_id).cloned().expect("delivery");
        Ok(DurableBrowserJob {
            delivery,
            delivery_sequence: state.delivery_sequence,
            created_at: now(),
            updated_at: now(),
        })
    }

    fn list(
        &self,
        proof: BrowserLeaseProof,
        wanted: BrowserJobDeliveryState,
        limit: u16,
    ) -> Vec<DurableBrowserJob> {
        let state = self.state.lock().expect("lock");
        state
            .jobs
            .iter()
            .filter_map(|id| state.queue.get(*id))
            .filter(|delivery| delivery.lease == proof && delivery.state == wanted)
            .take(usize::from(limit))
            .map(|delivery| DurableBrowserJob {
                delivery: delivery.clone(),
                delivery_sequence: 1,
                created_at: now(),
                updated_at: now(),
            })
            .collect()
    }
}

impl BrowserGatewayStore for FakeGatewayStore {
    fn record_execution_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        sequence: u64,
        detail: Option<BrowserExecutionProgress>,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            if let Some(detail) = &detail {
                detail.validate()?;
            }
            let mut state = self.state.lock().expect("lock");
            let StoreState {
                registry, queue, ..
            } = &mut *state;
            queue.progress(registry, job_id, proof, sequence, now())?;
            if let Some(detail) = detail {
                state.progress.entry(job_id).or_default().push(detail);
            }
            Ok(())
        })
    }

    fn consume_gateway_ticket(
        &self,
        _admission: BrowserGatewayAdmission,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async { Ok(()) })
    }

    fn register_provider(
        &self,
        registration: BrowserLeaseRegistration,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            let lease = state.registry.register(registration, now())?;
            state.leases.push(lease.clone());
            Ok(lease)
        })
    }

    fn renew_provider(
        &self,
        proof: BrowserLeaseProof,
        ttl_millis: NonZeroU64,
    ) -> ApplicationFuture<'_, Result<BrowserProviderLease, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            state.registry.renew(proof, ttl_millis, now())
        })
    }

    fn release_provider(
        &self,
        proof: BrowserLeaseProof,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            state.registry.release(proof)?;
            state.releases += 1;
            Ok(())
        })
    }

    fn route_and_enqueue(
        &self,
        tenant_id: TenantId,
        principal_id: PrincipalId,
        contract: BrowserProviderContract,
        request: BrowserJobRequest,
    ) -> ApplicationFuture<'_, Result<DurableBrowserJob, BrowserProviderRuntimeError>> {
        Box::pin(async move { self.enqueue(tenant_id, principal_id, contract, request) })
    }

    fn list_deliverable(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let current = {
                let state = self.state.lock().expect("lock");
                state.leases.last().map(BrowserLeaseProof::from) == Some(proof)
            };
            if !current {
                return Err(BrowserProviderRuntimeError::StaleLease);
            }
            Ok(self.list(proof, BrowserJobDeliveryState::Queued, limit))
        })
    }

    fn list_cancellations(
        &self,
        proof: BrowserLeaseProof,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<Vec<DurableBrowserJob>, BrowserProviderRuntimeError>> {
        Box::pin(
            async move { Ok(self.list(proof, BrowserJobDeliveryState::CancelRequested, limit)) },
        )
    }

    fn claim_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        _delivery_sequence: u64,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            let StoreState {
                registry, queue, ..
            } = &mut *state;
            queue.claim(registry, job_id, proof, provider_sequence, now())?;
            Ok(())
        })
    }

    fn start_job(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            let StoreState {
                registry, queue, ..
            } = &mut *state;
            queue.start(registry, job_id, proof, provider_sequence, now())
        })
    }

    fn record_progress(
        &self,
        job_id: BrowserJobId,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            let StoreState {
                registry, queue, ..
            } = &mut *state;
            queue.progress(registry, job_id, proof, provider_sequence, now())
        })
    }

    fn request_cancel(
        &self,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<(), BrowserProviderRuntimeError>> {
        Box::pin(async move {
            self.state
                .lock()
                .expect("lock")
                .queue
                .request_cancel(job_id)
        })
    }

    fn seal_terminal(
        &self,
        proof: BrowserLeaseProof,
        provider_sequence: u64,
        result: BrowserJobResult,
    ) -> ApplicationFuture<'_, Result<BrowserJobTerminalReceipt, BrowserProviderRuntimeError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("lock");
            let job_id = result.job_id;
            let StoreState {
                registry, queue, ..
            } = &mut *state;
            let result =
                queue.complete(registry, job_id, proof, provider_sequence, result, now())?;
            let invocation_id = queue.get(job_id).expect("job").request.invocation_id;
            let receipt = BrowserJobTerminalReceipt {
                job_id,
                invocation_id,
                lease: proof,
                provider_sequence,
                result,
                sealed_at: now(),
            };
            state.receipts.insert(job_id, receipt.clone());
            Ok(receipt)
        })
    }

    fn load_terminal_receipt(
        &self,
        _tenant_id: TenantId,
        job_id: BrowserJobId,
    ) -> ApplicationFuture<'_, Result<Option<BrowserJobTerminalReceipt>, BrowserProviderRuntimeError>>
    {
        Box::pin(async move { Ok(self.receipt(job_id)) })
    }
}

type Objects = BTreeMap<(TenantId, ContentId), (ContentRef, Vec<u8>)>;

/// Tenant-partitioned in-memory content store.
#[derive(Default)]
pub struct FakeContentStore {
    objects: Mutex<Objects>,
}

impl FakeContentStore {
    pub fn insert(&self, tenant_id: TenantId, media_type: &str, bytes: &[u8]) -> ContentRef {
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media"),
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        };
        self.objects.lock().expect("lock").insert(
            (tenant_id, reference.content_id),
            (reference.clone(), bytes.to_vec()),
        );
        reference
    }

    /// Stores a reference whose recorded bytes do not match its hash.
    pub fn insert_corrupt(
        &self,
        tenant_id: TenantId,
        media_type: &str,
        bytes: &[u8],
    ) -> ContentRef {
        let reference = self.insert(tenant_id, media_type, bytes);
        if let Some(entry) = self
            .objects
            .lock()
            .expect("lock")
            .get_mut(&(tenant_id, reference.content_id))
        {
            entry.1 = vec![b'X'; bytes.len()];
        }
        reference
    }

    pub fn bytes(&self, tenant_id: TenantId, reference: &ContentRef) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .expect("lock")
            .get(&(tenant_id, reference.content_id))
            .map(|(_, bytes)| bytes.clone())
    }
}

impl ContentStore for FakeContentStore {
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            request.validate()?;
            Ok(self.insert(
                request.tenant_id,
                request.media_type.as_str(),
                &request.bytes,
            ))
        })
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            self.objects
                .lock()
                .expect("lock")
                .get(&(tenant_id, content.content_id))
                .map(|(_, bytes)| bytes.clone())
                .ok_or(ContentStoreError::NotFound)
        })
    }
}

pub fn config(work_root: &std::path::Path, image: &str) -> ServerSandboxConfig {
    ServerSandboxConfig {
        image: ImageConfig {
            reference: image.to_owned(),
            env: BTreeMap::from([
                ("PYTHON_BIN".to_owned(), "python3".to_owned()),
                ("PYTHONNOUSERSITE".to_owned(), "1".to_owned()),
                ("PYTHONDONTWRITEBYTECODE".to_owned(), "1".to_owned()),
                ("MPLCONFIGDIR".to_owned(), "/tmp/matplotlib".to_owned()),
            ]),
            python_args: vec!["-s".to_owned()],
        },
        runtime_release_ref: RELEASE.to_owned(),
        resource_profile_ref: "server.sandbox@1".to_owned(),
        limits: HostLimits {
            cpu_millis: 500,
            memory_bytes: 128 * 1024 * 1024,
            pids: 64,
            max_wall_time_millis: 60_000,
            tmpfs_bytes: 16 * 1024 * 1024,
            nofile: 256,
        },
        container_user: "65534:65534".to_owned(),
        work_root: work_root.to_path_buf(),
        egress: None,
        lease: LeaseConfig {
            ttl_millis: 6_000,
            poll_interval_millis: 50,
            reregister_backoff_millis: 50,
        },
        max_concurrent_jobs: 2,
        labels: BTreeMap::from([
            ("purpose".to_owned(), "musteragent-sandbox-test".to_owned()),
            ("disposable".to_owned(), "true".to_owned()),
            (
                "owner".to_owned(),
                std::env::var("MUSTERAGENT_TEST_OWNER")
                    .unwrap_or_else(|_| "musteragent-sandbox-tests".into()),
            ),
            (
                "run-id".to_owned(),
                std::env::var("MUSTERAGENT_TEST_RUN_ID")
                    .unwrap_or_else(|_| format!("{}", std::process::id())),
            ),
            (
                "expires-at".to_owned(),
                format!(
                    "unix:{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock")
                        .as_secs()
                        + 3600
                ),
            ),
        ]),
    }
}

pub struct JobFixture {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
}

impl JobFixture {
    pub fn new() -> Self {
        Self {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
        }
    }

    /// A Python job with `main.py` and optional extra inputs/outputs.
    pub fn python_job(
        &self,
        content: &FakeContentStore,
        source: &str,
        extra_inputs: &[(&str, &[u8])],
        outputs: &[(&str, &str, u64, bool)],
        wall_time_millis: u64,
    ) -> BrowserJobRequest {
        let source_ref = content.insert(self.tenant_id, "text/x-python", source.as_bytes());
        let mut inputs = vec![BrowserInput {
            path: "main.py".to_owned(),
            content_ref: source_ref.clone(),
        }];
        for (path, bytes) in extra_inputs {
            inputs.push(BrowserInput {
                path: (*path).to_owned(),
                content_ref: content.insert(self.tenant_id, "application/octet-stream", bytes),
            });
        }
        self.request(source_ref, inputs, outputs, wall_time_millis)
    }

    pub fn request(
        &self,
        source_ref: ContentRef,
        inputs: Vec<BrowserInput>,
        outputs: &[(&str, &str, u64, bool)],
        wall_time_millis: u64,
    ) -> BrowserJobRequest {
        let input_bytes: u64 = inputs
            .iter()
            .map(|input| input.content_ref.byte_length)
            .sum();
        let declared: Vec<BrowserDeclaredOutput> = outputs
            .iter()
            .map(|(path, media, max, required)| BrowserDeclaredOutput {
                path: (*path).to_owned(),
                media_type: MediaType::new(*media).expect("media"),
                maximum_byte_length: NonZeroU64::new(*max).expect("max"),
                required: *required,
            })
            .collect();
        let output_bytes: u64 = declared
            .iter()
            .map(|output| output.maximum_byte_length.get())
            .sum::<u64>()
            .max(1);
        BrowserJobRequest {
            execution_liveness: None,
            job_id: BrowserJobId::generate(),
            invocation_id: InvocationId::generate(),
            conversation_id: ConversationId::generate(),
            engine: BrowserJobEngine::Python,
            runtime_release_ref: VersionedKey::new(RELEASE).expect("release"),
            program: BrowserJobProgram::Python {
                source_ref,
                entrypoint: "main.py".to_owned(),
                environment: None,
                require_prepared_environment: false,
            },
            limits: BrowserResourceLimits {
                wall_time_millis: NonZeroU64::new(wall_time_millis).expect("wall"),
                maximum_input_bytes: NonZeroU64::new(input_bytes.max(1)).expect("in"),
                maximum_output_bytes: NonZeroU64::new(output_bytes).expect("out"),
                maximum_files: NonZeroU32::new(u32::try_from(inputs.len()).expect("files"))
                    .expect("files"),
                maximum_stdout_bytes: NonZeroU64::new(64 * 1024).expect("stdout"),
                maximum_stderr_bytes: NonZeroU64::new(64 * 1024).expect("stderr"),
            },
            inputs,
            declared_outputs: declared,
            security_profile: BrowserSecurityProfile::SafeOffline,
        }
    }
}
