//! Real fenced enqueue selects only the recorded executor and original owner.
use super::test_fixture::Fixture;
use super::*;
use musteragent_capability::*;
use musteragent_identity::*;
use musteragent_runtime_capability::{
    BrowserGatewayStore, BrowserLeaseRegistration, BrowserProviderRuntimeError as Error,
    CapabilityInvocation, SandboxRoutingStore,
};
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_sandbox::*;
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};
fn capability() -> CapabilityKey {
    CapabilityKey::from_versioned_key(
        VersionedKey::new("browser.python.execute@1").expect("capability"),
    )
}

fn registration() -> BrowserLeaseRegistration {
    BrowserLeaseRegistration {
        scope: musteragent_runtime_capability::BrowserProviderScope::Actor,
        executor: musteragent_sandbox::SandboxExecutor::Browser,
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

#[tokio::test]
async fn captured_context_is_immutable_and_enqueue_never_falls_back_to_another_executor() {
    let f = Fixture::new().await;
    let routing = crate::PostgresSandboxRoutingStore::new(
        f.journal.clone(),
        Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
    );
    let gateway = crate::PostgresBrowserGatewayStore::new(f.pool.clone());
    let run = f.attempt.run_id;
    routing
        .bind_run_context(
            f.actor,
            run,
            SandboxClientSurface::Api,
            SandboxTrigger::Background,
            SandboxRoutingPolicy::ServerOnly,
        )
        .await
        .unwrap();
    routing
        .bind_run_context(
            f.actor,
            run,
            SandboxClientSurface::Api,
            SandboxTrigger::Background,
            SandboxRoutingPolicy::ServerOnly,
        )
        .await
        .unwrap();
    assert!(matches!(
        routing
            .bind_run_context(
                f.actor,
                run,
                SandboxClientSurface::Web,
                SandboxTrigger::Interactive,
                SandboxRoutingPolicy::BrowserOnly
            )
            .await,
        Err(Error::RouteConflict)
    ));
    let stranger = Actor {
        principal_id: PrincipalId::generate(),
        ..f.actor
    };
    assert!(matches!(
        routing
            .bind_run_context(
                stranger,
                run,
                SandboxClientSurface::Api,
                SandboxTrigger::Background,
                SandboxRoutingPolicy::ServerOnly
            )
            .await,
        Err(Error::CapabilityNotAuthorized)
    ));
    let original = f
        .journal
        .load_invocation(f.actor.tenant_id, f.invocation)
        .await
        .unwrap()
        .unwrap();
    let inv = original.invocation;
    let conversation: uuid::Uuid =
        sqlx::query_scalar("SELECT conversation_id FROM runtime.runs WHERE run_id=$1")
            .bind(run.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    let schema = SchemaRef {
        schema_key: VersionedKey::new("test.schema@1").unwrap(),
        schema_hash: Sha256::digest(b"schema"),
    };
    let invocation = CapabilityInvocation {
        actor: f.actor,
        tenant_id: f.actor.tenant_id,
        conversation_id: ConversationId::from_uuid(conversation).unwrap(),
        run_id: run,
        requested_by_attempt_id: inv.requested_by_attempt_id,
        execution_attempt_id: f.attempt.attempt_id,
        execution_attempt_number: f.attempt.attempt_number,
        invocation_id: f.invocation,
        tool_call_id: inv.tool_call_id,
        tool_key: inv.tool_key,
        capability_key: capability(),
        capability_provider_id: inv.capability_provider_id,
        idempotency_key: inv.idempotency_key,
        request_ref: inv.request_ref,
        request_schema: schema.clone(),
        result_schema: schema,
        authorized_content: vec![],
        workspace_id: None,
        timing: inv.timing,
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-00000000000000000000000000000001-0000000000000001-00",
            )
            .unwrap(),
            tracestate: None,
        },
        execution_grant_proof: None,
    };
    let mut req = request();
    req.invocation_id = f.invocation;
    req.conversation_id = invocation.conversation_id;
    for input in &req.inputs {
        let c = &input.content_ref;
        sqlx::query("INSERT INTO artifact.content_refs(content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,1)")
            .bind(c.content_id.into_uuid()).bind(f.actor.tenant_id.into_uuid()).bind(c.media_type.as_str()).bind(c.byte_length as i64).bind(c.sha256.into_bytes().to_vec()).bind(format!("test://{}",c.content_id)).execute(&f.pool).await.unwrap();
    }
    let mut browser = registration();
    browser.tenant_id = f.actor.tenant_id;
    browser.principal_id = f.actor.principal_id;
    gateway.register_provider(browser.clone()).await.unwrap();
    let committed = routing
        .commit_route(&f.attempt, f.commit(SandboxExecutor::Server))
        .await
        .unwrap()
        .sandbox_route
        .unwrap();
    assert!(matches!(
        routing
            .enqueue(
                &f.attempt,
                &invocation,
                committed,
                BrowserProviderContract::PythonExecute,
                req.clone()
            )
            .await,
        Err(Error::ProviderUnavailable)
    ));
    assert!(
        gateway
            .load_job(f.actor.tenant_id, req.job_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut server = registration();
    server.executor = SandboxExecutor::Server;
    server.tenant_id = f.actor.tenant_id;
    server.principal_id = f.actor.principal_id;
    let lease = gateway.register_provider(server).await.unwrap();
    let mut wrong = invocation.clone();
    wrong.actor = stranger;
    assert!(matches!(
        routing
            .enqueue(
                &f.attempt,
                &wrong,
                committed,
                BrowserProviderContract::PythonExecute,
                req.clone()
            )
            .await,
        Err(Error::CapabilityNotAuthorized)
    ));
    let mut wrong_route = committed;
    wrong_route.decided_at = UtcTimestamp::from_unix_millis(1235);
    assert!(matches!(
        routing
            .enqueue(
                &f.attempt,
                &invocation,
                wrong_route,
                BrowserProviderContract::PythonExecute,
                req.clone()
            )
            .await,
        Err(Error::RouteConflict)
    ));
    let durable = routing
        .enqueue(
            &f.attempt,
            &invocation,
            committed,
            BrowserProviderContract::PythonExecute,
            req.clone(),
        )
        .await
        .unwrap();
    assert_eq!(durable.delivery.lease.provider_id, lease.provider_id);
    assert_eq!(
        gateway
            .load_job(f.actor.tenant_id, req.job_id)
            .await
            .unwrap(),
        Some(durable)
    );
    assert!(
        gateway
            .load_job(TenantId::generate(), req.job_id)
            .await
            .unwrap()
            .is_none()
    );
    // Re-registering a page cannot relabel queued server jobs as browser work.
    browser.executor = SandboxExecutor::Server;
    assert!(matches!(
        gateway.register_provider(browser).await,
        Err(Error::InvalidRegistration)
    ));
    f.pool.close().await;
}

#[tokio::test]
async fn failed_outbox_append_rolls_back_route_and_ledger_together() {
    let f = Fixture::new().await;
    sqlx::raw_sql("CREATE FUNCTION event_store.reject_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test outbox failure'; END $$; CREATE TRIGGER reject_test_outbox BEFORE INSERT ON event_store.outbox FOR EACH ROW EXECUTE FUNCTION event_store.reject_test_outbox();").execute(&f.pool).await.unwrap();
    assert!(
        f.journal
            .commit_sandbox_route(&f.attempt, f.commit(SandboxExecutor::Server))
            .await
            .is_err()
    );
    assert!(
        f.journal
            .load_invocation(f.actor.tenant_id, f.invocation)
            .await
            .unwrap()
            .unwrap()
            .sandbox_route
            .is_none()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records WHERE run_id=$1")
            .bind(f.attempt.run_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
    f.pool.close().await;
}

#[tokio::test]
async fn server_provider_scope_and_executor_identity_survive_release_and_store_restart() {
    use musteragent_runtime_capability::BrowserProviderScope;
    let f = Fixture::new().await;
    let gateway = crate::PostgresBrowserGatewayStore::new(f.pool.clone());
    let mut tenant = registration();
    tenant.executor = SandboxExecutor::Server;
    tenant.scope = BrowserProviderScope::Tenant(tenant.tenant_id);
    let first = gateway
        .register_provider(tenant.clone())
        .await
        .expect("tenant registration");
    gateway
        .release_provider(BrowserLeaseProof::from(&first))
        .await
        .expect("release");
    let restarted = crate::PostgresBrowserGatewayStore::new(f.pool.clone());
    let mut another = tenant.clone();
    another.principal_id = PrincipalId::generate();
    another.page_instance_id = PageInstanceId::generate();
    restarted
        .register_provider(another)
        .await
        .expect("same tenant actor");
    let mut foreign = tenant.clone();
    foreign.tenant_id = TenantId::generate();
    assert!(matches!(
        restarted.register_provider(foreign).await,
        Err(Error::InvalidRegistration)
    ));
    let mut widened = tenant.clone();
    widened.scope = BrowserProviderScope::Global;
    assert!(matches!(
        restarted.register_provider(widened).await,
        Err(Error::InvalidRegistration)
    ));
    let mut changed_class = tenant;
    changed_class.executor = SandboxExecutor::Browser;
    changed_class.scope = BrowserProviderScope::Actor;
    assert!(matches!(
        restarted.register_provider(changed_class).await,
        Err(Error::InvalidRegistration)
    ));
    let mut global = registration();
    global.executor = SandboxExecutor::Server;
    global.scope = BrowserProviderScope::Global;
    let one = restarted
        .register_provider(global.clone())
        .await
        .expect("global first actor");
    global.tenant_id = TenantId::generate();
    global.principal_id = PrincipalId::generate();
    global.page_instance_id = PageInstanceId::generate();
    let two = restarted
        .register_provider(global)
        .await
        .expect("global second actor");
    assert_eq!(one.provider_id, two.provider_id);
    assert_ne!(one.tenant_id, two.tenant_id);
    assert_ne!(one.lease_id, two.lease_id);
    assert!(matches!(
        restarted
            .renew_provider(
                BrowserLeaseProof {
                    tenant_id: two.tenant_id,
                    ..BrowserLeaseProof::from(&one)
                },
                NonZeroU64::new(1000).expect("ttl")
            )
            .await,
        Err(Error::LeaseExpired)
    ));
}
