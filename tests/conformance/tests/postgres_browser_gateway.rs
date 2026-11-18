use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};

use musteragent_capability::{
    BrowserJobEngine, BrowserJobProgram, BrowserJobRequest, BrowserJobStatus,
    BrowserProviderContract, BrowserRuntimeFeatureProbe, BrowserSecurityProfile, CapabilityKey,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    BrowserJobId, BrowserSessionId, CapabilityProviderId, ContentId, ConversationId, InvocationId,
    PageInstanceId, PrincipalId, TenantId, VersionedKey,
};
use musteragent_postgres::PostgresBrowserGatewayStore;
use musteragent_runtime_capability::{BrowserGatewayStore, BrowserLeaseRegistration};
use musteragent_testkit::IsolatedPostgres;

#[tokio::test]
async fn browser_gateway_rebinds_only_queued_work_and_cancels_it_durably() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresBrowserGatewayStore::new(pool.clone());
    let registration = registration();

    let first_lease = store
        .register_provider(registration.clone())
        .await
        .expect("register first Browser lease");
    assert!(
        store
            .lease_is_current((&first_lease).into())
            .await
            .expect("current original lease")
    );
    let request = request(first_lease.runtime_release_ref.clone());
    seed_inputs(&pool, registration.tenant_id, &request).await;
    let queued = store
        .route_and_enqueue(
            registration.tenant_id,
            registration.principal_id,
            BrowserProviderContract::WebValidate,
            request.clone(),
        )
        .await
        .expect("enqueue Browser Job");
    assert_eq!(queued.delivery_sequence, 1);

    store
        .release_provider((&first_lease).into())
        .await
        .expect("release disconnected page");
    assert!(
        !store
            .lease_is_current((&first_lease).into())
            .await
            .expect("released lease cannot authorize input")
    );
    let second_lease = store
        .register_provider(registration)
        .await
        .expect("reconnect the same stable page provider");
    assert!(
        store
            .lease_is_current((&second_lease).into())
            .await
            .expect("renewed current lease")
    );
    assert!(
        !store
            .lease_is_current((&first_lease).into())
            .await
            .expect("old epoch remains fenced")
    );
    assert!(second_lease.epoch > first_lease.epoch);
    assert!(second_lease.fence > first_lease.fence);

    let delivered = store
        .list_deliverable((&second_lease).into(), 10)
        .await
        .expect("list rebound work");
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].delivery.request.job_id, request.job_id);
    assert_eq!(delivered[0].delivery.lease, (&second_lease).into());
    assert_eq!(delivered[0].delivery_sequence, 1);

    store
        .request_cancel(request.job_id)
        .await
        .expect("cancel queued work without dispatching it");
    store
        .request_cancel(request.job_id)
        .await
        .expect("queued cancellation is idempotent");
    assert!(
        store
            .load_terminal_receipt(TenantId::generate(), request.job_id)
            .await
            .expect("query another tenant")
            .is_none()
    );

    let receipt = store
        .load_terminal_receipt(second_lease.tenant_id, request.job_id)
        .await
        .expect("load cancellation receipt")
        .expect("queued cancellation is terminal");
    assert_eq!(receipt.provider_sequence, 0);
    assert_eq!(receipt.result.status, BrowserJobStatus::Cancelled);
    assert_eq!(receipt.result.validate(), Ok(()));
}

fn registration() -> BrowserLeaseRegistration {
    BrowserLeaseRegistration {
        executor: musteragent_sandbox::SandboxExecutor::Browser,
        scope: musteragent_runtime_capability::BrowserProviderScope::Actor,
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        browser_session_id: BrowserSessionId::generate(),
        page_instance_id: PageInstanceId::generate(),
        provider_id: CapabilityProviderId::generate(),
        runtime_release_ref: VersionedKey::new("browser.runtime@1").expect("release"),
        capabilities: BTreeSet::from([CapabilityKey::from_versioned_key(
            VersionedKey::new("browser.web.validate@1").expect("capability"),
        )]),
        resource_profile_ref: VersionedKey::new("browser.resource.safe@1")
            .expect("resource profile"),
        feature_probe: BrowserRuntimeFeatureProbe {
            worker: true,
            web_assembly: true,
            crypto_subtle: true,
            transferable_array_buffer: true,
            service_worker: true,
            indexed_db: true,
            secure_context: true,
            engines: BTreeSet::from([BrowserJobEngine::StaticValidate]),
        },
        ttl_millis: NonZeroU64::new(120_000).expect("ttl"),
    }
}

fn request(runtime_release_ref: VersionedKey) -> BrowserJobRequest {
    let source_tree = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.source-tree+json")
            .expect("media type"),
        byte_length: 2,
        sha256: Sha256::digest(b"{}"),
    };
    BrowserJobRequest {
        execution_liveness: None,
        job_id: BrowserJobId::generate(),
        invocation_id: InvocationId::generate(),
        conversation_id: ConversationId::generate(),
        engine: BrowserJobEngine::StaticValidate,
        runtime_release_ref,
        program: BrowserJobProgram::StaticValidate {
            source_tree_ref: source_tree.clone(),
            application: musteragent_capability::BrowserApplicationBuildContract {
                routing_mode: musteragent_capability::BrowserApplicationRoutingMode::MultiPage,
                routing_fallback: None,
                preview_profile:
                    musteragent_capability::BrowserApplicationPreviewProfile::InteractiveIsolated,
                source_revision_hash: source_tree.sha256,
            },
        },
        inputs: vec![musteragent_capability::BrowserInput {
            path: "source-tree.json".to_owned(),
            content_ref: source_tree,
        }],
        declared_outputs: Vec::new(),
        limits: musteragent_capability::BrowserResourceLimits {
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

async fn seed_inputs(pool: &sqlx::PgPool, tenant: TenantId, request: &BrowserJobRequest) {
    for input in &request.inputs {
        let reference = &input.content_ref;
        sqlx::query("INSERT INTO artifact.content_refs (content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES ($1,$2,$3,$4,$5,'fixture://browser',1)")
          .bind(reference.content_id.into_uuid()).bind(tenant.into_uuid()).bind(reference.media_type.as_str()).bind(reference.byte_length as i64).bind(reference.sha256.into_bytes().to_vec()).execute(pool).await.expect("seed inputs must succeed");
    }
}
#[test]
fn retired_presentation_contract_cannot_register_a_browser_provider() {
    let mut reg = registration();
    reg.capabilities =
        BTreeSet::from([BrowserProviderContract::PresentationExecute.capability_key()]);
    reg.feature_probe.engines = BTreeSet::from([BrowserJobEngine::Presentation]);
    assert!(matches!(
        reg.validate(),
        Err(musteragent_runtime_capability::BrowserProviderRuntimeError::InvalidRegistration)
    ));
    assert!(
        !BrowserProviderContract::PresentationExecute
            .accepts_engine(BrowserJobEngine::Presentation)
    );
}

#[tokio::test]
async fn browser_environment_progress_is_fenced_bounded_and_terminal_receipts_are_immutable() {
    use musteragent_capability::{
        BrowserDiagnostic, BrowserDiagnosticSeverity, BrowserJobMetrics, BrowserJobResult,
    };
    use musteragent_runtime_capability::BrowserExecutionProgress;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated database");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let store = PostgresBrowserGatewayStore::new(pool.clone());
    let mut registration = registration();
    registration.capabilities =
        BTreeSet::from([BrowserProviderContract::PythonEnvironmentPrepare.capability_key()]);
    registration.feature_probe.engines = BTreeSet::from([BrowserJobEngine::PythonEnvironment]);
    let lease = store
        .register_provider(registration.clone())
        .await
        .expect("environment provider");
    let mut request = request(lease.runtime_release_ref.clone());
    request.engine = BrowserJobEngine::PythonEnvironment;
    request.program = BrowserJobProgram::PythonEnvironment {
        requirements: vec!["numpy".into()],
        requires_python: None,
        scope_key: Sha256::digest(b"project-scope"),
    };
    request.inputs.clear();
    request.declared_outputs.clear();
    let job = store
        .route_and_enqueue(
            lease.tenant_id,
            lease.principal_id,
            BrowserProviderContract::PythonEnvironmentPrepare,
            request.clone(),
        )
        .await
        .expect("admit installation without input file");
    let proof = (&lease).into();
    store
        .claim_job(request.job_id, proof, job.delivery_sequence, 1)
        .await
        .expect("claim");
    store
        .start_job(request.job_id, proof, 2)
        .await
        .expect("start");
    for sequence in 3..=70 {
        let detail = BrowserExecutionProgress {
            phase: "downloading".into(),
            package_name: Some("numpy.whl".into()),
            transferred_bytes: Some(sequence),
            total_bytes: Some(100),
            message: None,
        };
        store
            .record_execution_progress(request.job_id, proof, sequence, Some(detail))
            .await
            .expect("fenced progress");
    }
    let progress = store
        .load_progress(lease.tenant_id, request.job_id)
        .await
        .expect("inspect progress");
    assert_eq!(progress.len(), 64);
    assert_eq!(progress.last().expect("browser environment progress is fenced bounded and terminal receipts are immutable must succeed").transferred_bytes, Some(70));
    assert!(
        store
            .load_progress(TenantId::generate(), request.job_id)
            .await
            .is_err()
    );
    assert!(
        store
            .record_execution_progress(request.job_id, proof, 70, None)
            .await
            .is_err()
    );
    let result = BrowserJobResult {
        job_id: request.job_id,
        status: BrowserJobStatus::Indeterminate,
        outputs: vec![],
        diagnostics: vec![BrowserDiagnostic {
            code: "runner_reloaded".into(),
            severity: BrowserDiagnosticSeverity::Error,
            message: "Generation ended without a receipt".into(),
            path: None,
            line: None,
            column: None,
        }],
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        metrics: BrowserJobMetrics {
            wall_time_millis: 0,
            input_bytes: 0,
            output_bytes: 0,
            output_files: 0,
        },
        result_hash: Sha256::digest(b""),
    }
    .with_canonical_hash();
    store
        .request_cancel(request.job_id)
        .await
        .expect("request does not prove cancellation");
    let sealed = store
        .seal_terminal(proof, 71, result.clone())
        .await
        .expect("seal unknown outcome");
    assert_eq!(sealed.result.status, BrowserJobStatus::Indeterminate);
    assert_eq!(
        store
            .seal_terminal(proof, 71, result)
            .await
            .expect("repeat same receipt"),
        sealed
    );
    assert!(
        store
            .record_execution_progress(request.job_id, proof, 72, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn browser_transport_resume_keeps_running_job_identity_and_rejects_expired_authority() {
    use musteragent_capability::BrowserLeaseProof;
    use musteragent_runtime_capability::{BrowserJobDeliveryState, BrowserProviderRuntimeError};
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated database");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let store = PostgresBrowserGatewayStore::new(pool.clone());
    let registration = registration();
    let lease = store.register_provider(registration.clone()).await.expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    let proof = BrowserLeaseProof::from(&lease);
    let request = request(lease.runtime_release_ref.clone());
    seed_inputs(&pool, lease.tenant_id, &request).await;
    let original = store
        .route_and_enqueue(
            lease.tenant_id,
            lease.principal_id,
            BrowserProviderContract::WebValidate,
            request.clone(),
        )
        .await
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    store
        .claim_job(request.job_id, proof, original.delivery_sequence, 1)
        .await
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    store.start_job(request.job_id, proof, 2).await.expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    store
        .record_execution_progress(request.job_id, proof, 3, None)
        .await
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    let renewed = store
        .renew_provider(proof, NonZeroU64::new(120_000).expect("browser transport resume keeps running job identity and rejects expired authority must succeed"))
        .await
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    assert_eq!(BrowserLeaseProof::from(&renewed), proof);
    let recovered = store
        .load_job(lease.tenant_id, request.job_id)
        .await
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed")
        .expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    assert_eq!(recovered.delivery.state, BrowserJobDeliveryState::Running);
    assert_eq!(recovered.delivery.last_provider_sequence, 3);
    assert_eq!(recovered.delivery_sequence, original.delivery_sequence);
    assert_eq!(recovered.delivery.request, original.delivery.request);
    assert!(store.list_deliverable(proof, 10).await.expect("browser transport resume keeps running job identity and rejects expired authority must succeed").is_empty());
    let foreign = BrowserLeaseProof {
        principal_id: PrincipalId::generate(),
        ..proof
    };
    assert!(
        store
            .renew_provider(foreign, NonZeroU64::new(120_000).expect("browser transport resume keeps running job identity and rejects expired authority must succeed"))
            .await
            .is_err()
    );
    assert!(
        store
            .load_job(TenantId::generate(), request.job_id)
            .await
            .expect("browser transport resume keeps running job identity and rejects expired authority must succeed")
            .is_none()
    );
    sqlx::query("UPDATE capability.browser_provider_slots SET issued_at_ms=1, expires_at_ms=2 WHERE active_lease_id=$1")
        .bind(proof.lease_id.into_uuid()).execute(&pool).await.expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    assert!(matches!(
        store
            .renew_provider(proof, NonZeroU64::new(120_000).expect("browser transport resume keeps running job identity and rejects expired authority must succeed"))
            .await,
        Err(BrowserProviderRuntimeError::LeaseExpired)
    ));
    let replacement = store.register_provider(registration).await.expect("browser transport resume keeps running job identity and rejects expired authority must succeed");
    assert_ne!(BrowserLeaseProof::from(&replacement), proof);
    assert!(
        store
            .list_deliverable((&replacement).into(), 10)
            .await
            .expect("browser transport resume keeps running job identity and rejects expired authority must succeed")
            .is_empty(),
        "a started computation must never be re-dispatched under a new lease"
    );
}

#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

#[tokio::test]
async fn a_new_page_recovers_only_the_original_job_after_its_owner_expires() {
    use musteragent_capability::{BrowserJobMetrics, BrowserJobResult};
    use musteragent_runtime_capability::{BrowserJobDeliveryState, BrowserProviderRuntimeError};
    use prost::Message as _;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    let pool = postgres
        .pool()
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    use musteragent_conformance::MemoryBlobs;
    use musteragent_content_ref::SchemaRef;
    use musteragent_postgres::{PostgresContentStore, PostgresFencedToolInvocationJournal};
    use musteragent_runtime_capability::{
        BrowserCandidateSettlement, CapabilityResult, FencedToolInvocationJournal,
        ToolTerminalOutcome,
    };
    use musteragent_runtime_content::{ContentStore, PutContent};
    use musteragent_wire::ProtobufLedgerPayloadCodec;
    use std::sync::Arc;
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        Arc::clone(&keys),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal =
        PostgresFencedToolInvocationJournal::new(pool.clone(), codec.clone(), codec, keys);
    let store = PostgresBrowserGatewayStore::new(pool.clone());
    let mut registration = registration();
    registration.tenant_id = fixture.tenant_id;
    registration.principal_id = fixture.principal_id;
    let old = store
        .register_provider(registration.clone())
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    let mut request = request(old.runtime_release_ref.clone());
    request.conversation_id = fixture.conversation_id;
    request.invocation_id = resource_support::start_resource_tool_with_contract(
        &pool,
        &content,
        &journal,
        &attempt,
        fixture,
        "browser-settlement",
        "browser.python.run@11",
        b"{}".to_vec(),
    )
    .await;
    seed_inputs(&pool, registration.tenant_id, &request).await;
    let queued = store
        .route_and_enqueue(
            registration.tenant_id,
            registration.principal_id,
            BrowserProviderContract::WebValidate,
            request.clone(),
        )
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    store
        .claim_job(request.job_id, (&old).into(), queued.delivery_sequence, 1)
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    store
        .start_job(request.job_id, (&old).into(), 2)
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    registration.browser_session_id = BrowserSessionId::generate();
    registration.page_instance_id = PageInstanceId::generate();
    registration.provider_id = CapabilityProviderId::generate();
    let fresh = store
        .register_provider(registration.clone())
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    let hash = Sha256::digest(
        musteragent_wire::encode_browser_job_request(&request)
            .expect(
                "a new page recovers only the original job after its owner expires must succeed",
            )
            .encode_to_vec(),
    );
    assert!(
        store
            .recover_candidate((&fresh).into(), request.job_id, hash)
            .await
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            )
            .is_none(),
        "live owner cannot be stolen"
    );
    assert!(matches!(
        store
            .recover_candidate((&fresh).into(), request.job_id, Sha256::digest(b"changed"))
            .await,
        Err(BrowserProviderRuntimeError::InvalidRegistration)
    ));
    sqlx::query("UPDATE capability.browser_provider_slots SET expires_at_ms=issued_at_ms+1 WHERE active_lease_id=$1")
        .bind(old.lease_id.into_uuid()).execute(&pool).await.expect("a new page recovers only the original job after its owner expires must succeed");
    let recovered = store
        .recover_candidate((&fresh).into(), request.job_id, hash)
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed")
        .expect("a new page recovers only the original job after its owner expires must succeed");
    assert_eq!(recovered.delivery.request, request);
    assert_eq!(recovered.delivery.state, BrowserJobDeliveryState::Running);
    assert_eq!(recovered.delivery.lease, (&fresh).into());
    assert_eq!(recovered.delivery.last_provider_sequence, 0);
    assert!(
        store
            .record_progress(request.job_id, (&old).into(), 3)
            .await
            .is_err(),
        "old page is fenced"
    );
    assert_eq!(
        store
            .recover_candidate((&fresh).into(), request.job_id, hash)
            .await
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            )
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            ),
        recovered
    );
    let result = BrowserJobResult {
        job_id: request.job_id,
        status: BrowserJobStatus::Completed,
        outputs: Vec::new(),
        diagnostics: Vec::new(),
        stdout_tail: String::new(),
        stderr_tail: String::new(),
        metrics: BrowserJobMetrics {
            wall_time_millis: 1,
            input_bytes: 0,
            output_bytes: 0,
            output_files: 0,
        },
        result_hash: Sha256::digest(b""),
    }
    .with_canonical_hash();
    store
        .seal_terminal((&fresh).into(), 1, result.clone())
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    registration.page_instance_id = PageInstanceId::generate();
    registration.provider_id = CapabilityProviderId::generate();
    let next = store
        .register_provider(registration.clone())
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    let terminal = store
        .recover_candidate((&next).into(), request.job_id, hash)
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed")
        .expect("a new page recovers only the original job after its owner expires must succeed");
    assert_eq!(terminal.delivery.terminal_result, Some(result));
    assert_eq!(
        store
            .candidate_settlement((&next).into(), request.job_id)
            .await
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            ),
        BrowserCandidateSettlement::Pending,
        "Browser upload ACK does not acknowledge Resource commit"
    );
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect(
                "a new page recovers only the original job after its owner expires must succeed",
            ),
            bytes: b"{\"revision\":1}".to_vec(),
        })
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    journal
        .complete(
            &attempt,
            request.invocation_id,
            ToolTerminalOutcome::Succeeded(CapabilityResult {
                result_hash: result_ref.sha256,
                result_ref,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("browser.python.run.output@11").expect("a new page recovers only the original job after its owner expires must succeed"),
                    schema_hash: Sha256::digest(b"workspace-result"),
                },
                provider_request_ref: None,
            }),
            running_loop::database_now(&pool).await,
        )
        .await
        .expect("a new page recovers only the original job after its owner expires must succeed");
    assert_eq!(
        store
            .candidate_settlement((&next).into(), request.job_id)
            .await
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            ),
        BrowserCandidateSettlement::Committed,
        "only the original Tool terminal success releases its local candidate"
    );
    let mut other_account = musteragent_runtime_capability::BrowserLeaseProof::from(&next);
    other_account.principal_id = PrincipalId::generate();
    assert_eq!(
        store
            .candidate_settlement(other_account, request.job_id)
            .await
            .expect(
                "a new page recovers only the original job after its owner expires must succeed"
            ),
        BrowserCandidateSettlement::Pending
    );
    assert_eq!(
        terminal.delivery.lease,
        (&fresh).into(),
        "observation does not rewrite receipt"
    );
    sqlx::query("UPDATE conversation.participants SET left_at_ms=1 WHERE tenant_id=$1 AND conversation_id=$2 AND principal_ref=$3")
        .bind(fixture.tenant_id.into_uuid()).bind(fixture.conversation_id.into_uuid()).bind(fixture.principal_id.into_uuid()).execute(&pool).await.expect("a new page recovers only the original job after its owner expires must succeed");
    assert!(matches!(
        store
            .recover_candidate((&next).into(), request.job_id, hash)
            .await,
        Err(BrowserProviderRuntimeError::JobNotFound)
    ));
}
