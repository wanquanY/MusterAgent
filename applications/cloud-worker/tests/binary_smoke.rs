//! The process boundary is real: PostgreSQL, S3 and HTTP survive worker death.
use musteragent_cloud_worker::BuildError;
use musteragent_conformance::fixtures::{
    context_snapshot, liveness_admission, source_selection_snapshot,
};
use musteragent_identity::*;
use musteragent_postgres::*;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_snapshot::*;
use musteragent_testkit::{
    ControlledOpenAiChatProvider, IsolatedMinio, IsolatedPostgres, MINIO_TEST_ACCESS_KEY,
    MINIO_TEST_SECRET_KEY,
};
use musteragent_tool::*;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::process::{Child, Command};
#[path = "../../../tests/conformance/tests/support/production_context.rs"]
mod production_context;
#[path = "../../../tests/conformance/tests/support/running_loop.rs"]
mod running_loop;

struct Smoke {
    pg: IsolatedPostgres,
    pool: sqlx::PgPool,
    minio: IsolatedMinio,
    content: Arc<dyn ContentStore>,
    dir: PathBuf,
    release: ProductReleaseRef,
}
impl Smoke {
    async fn start() -> Result<Self, BuildError> {
        Self::start_for("p9").await
    }
    async fn start_for(task: &str) -> Result<Self, BuildError> {
        let pg = IsolatedPostgres::start_with_pgvector().await?;
        let pool = pg.pool().await?;
        musteragent_conformance::migrate(&pool)
            .await
            .expect("fixture publication");
        let minio = IsolatedMinio::start().await?;
        let blobs = musteragent_object_store::ObjectStoreBlobStore::from_s3(
            musteragent_object_store::S3ObjectStoreConfig {
                endpoint: minio.endpoint.clone(),
                bucket: minio.bucket.into(),
                region: "us-east-1".into(),
                prefix: "smoke".into(),
                access_key_id: MINIO_TEST_ACCESS_KEY.into(),
                secret_access_key: MINIO_TEST_SECRET_KEY.into(),
                allow_http: true,
                virtual_hosted_style: false,
                write_once: Default::default(),
            },
        )?;
        let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
            pool.clone(),
            Arc::new(blobs),
            running_loop::keyring(),
        ));
        let root = std::env::var_os("MUSTERAGENT_TEST_ARTIFACT_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../.codex-work")
                    .join(task)
            });
        let dir = root.join(format!("smoke-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir)?;
        use base64::Engine as _;
        std::fs::write(
            dir.join("fixture-keyring.json"),
            serde_json::to_vec(
                &serde_json::json!({"schema_version": "musteragent.opaque-keyring.v2", "keys": [{"key_id":"runtime.loop.test", "key_base64":base64::engine::general_purpose::STANDARD.encode([31u8;32])}]}),
            )?,
        )?;
        let release = host_publication(&dir)?;
        Ok(Self {
            pg,
            pool,
            minio,
            content,
            dir,
            release,
        })
    }
    async fn seed(
        &self,
        model: &musteragent_model::ModelRoute,
        host: bool,
    ) -> running_loop::Fixture {
        self.seed_tool(
            model,
            host,
            "system.time.current",
            b"[e2e:time] Read the current time and report it.",
        )
        .await
    }
    async fn seed_tool(
        &self,
        model: &musteragent_model::ModelRoute,
        host: bool,
        tool_key: &str,
        input: &[u8],
    ) -> running_loop::Fixture {
        self.seed_tools(model, host, &[tool_key], input).await
    }
    async fn seed_tools(
        &self,
        model: &musteragent_model::ModelRoute,
        host: bool,
        tool_keys: &[&str],
        input: &[u8],
    ) -> running_loop::Fixture {
        self.seed_tools_with_limits(model, host, tool_keys, input, None, None)
            .await
    }
    async fn seed_tools_with_limits(
        &self,
        model: &musteragent_model::ModelRoute,
        host: bool,
        tool_keys: &[&str],
        input: &[u8],
        limits: Option<(u32, u32)>,
        retry: Option<musteragent_goal::RetrySpec>,
    ) -> running_loop::Fixture {
        let (fixture, _) =
            running_loop::seed_running_loop_with_input(&self.pool, self.content.as_ref(), input)
                .await;
        let mut snapshot = source_selection_snapshot::isolated_snapshot(
            self.content.clone(),
            running_loop::run_context(fixture),
            model.clone(),
        )
        .await;
        let provider = musteragent_tool::BUILTIN_RUST_PROVIDER_ID
            .parse()
            .expect("worker acceptance fixture");
        for grant in &mut snapshot.content.capabilities {
            grant.provider_id = provider;
        }
        for tool in &mut snapshot.content.tools {
            tool.manifest.capability_provider_id = provider;
        }
        let catalog = BuiltinToolCatalog::for_product_release(
            &compiled_product_release(ProductId::KERNEL),
            ProductExecutionTarget::Cloud,
        )
        .expect("worker acceptance fixture");
        for tool_key in tool_keys {
            let definition = catalog
                .definitions()
                .iter()
                .find(|d| d.key.logical_key() == *tool_key)
                .expect("released tool");
            snapshot
                .content
                .capabilities
                .push(musteragent_capability::CapabilityGrantSnapshot {
                    key: musteragent_capability::CapabilityKey::from_versioned_key(
                        definition.capability_key.clone(),
                    ),
                    provider_id: provider,
                    location: musteragent_capability::CapabilityLocation::Cloud,
                    effect: definition.effect,
                    allowed_scopes: definition.required_scopes.clone(),
                    policy_decision: musteragent_policy::Decision::Allow,
                });
            snapshot
                .content
                .capabilities
                .sort_by(|a, b| a.key.cmp(&b.key));
            snapshot.content.tools.push(ToolDefinitionSnapshot {
                manifest: definition.pin(provider),
                execution_target: definition.target.clone(),
                concurrency_class: definition.concurrency,
                idempotency: definition.idempotency,
                file_operation: definition.file_operation.clone(),
                title: definition.title.clone(),
                description: definition.description.clone(),
                input_schema_json: definition.input_schema_json.clone(),
                output_schema_json: definition.output_schema_json.clone(),
            });
            snapshot
                .content
                .tools
                .sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
            snapshot
                .content
                .discovery
                .as_mut()
                .expect("worker acceptance fixture")
                .initial_exposed_tool_keys
                .push(definition.key.clone());
        }
        if tool_keys.iter().any(|key| key.starts_with("delegation.")) {
            snapshot
                .content
                .discovery
                .as_mut()
                .expect("fixture")
                .discovery_policy_ref =
                VersionedKey::new(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1).expect("fixture");
        } else if tool_keys.iter().any(|key| key.starts_with("schedules.")) {
            snapshot
                .content
                .discovery
                .as_mut()
                .expect("fixture")
                .discovery_policy_ref =
                VersionedKey::new(CLOUD_SCHEDULE_DISCOVERY_POLICY_V1).expect("fixture");
        }
        let discovery = snapshot
            .content
            .discovery
            .as_mut()
            .expect("fixture discovery");
        if matches!(
            discovery.discovery_policy_ref.as_str(),
            CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1 | CLOUD_SCHEDULE_DISCOVERY_POLICY_V1
        ) {
            // Automation roles seal their own bootstrap. File tools remain
            // explicitly exposed, without being substituted for that contract.
            discovery.bootstrap_tool_keys.retain(|key| {
                matches!(
                    key.logical_key(),
                    "tool.catalog.search"
                        | "tool.catalog.activate"
                        | "interaction.request"
                        | "skills.search"
                        | "skills.list"
                        | "skills.view"
                        | "plan.get"
                        | "plan.update"
                )
            });
        }
        if let Some(spec) = retry {
            snapshot.content.execution_policy.recovery_policy =
                RecoveryPolicySnapshot::BoundedBackoff { spec };
        }
        if let Some((depth, children)) = limits {
            snapshot.content.execution_policy.max_delegation_depth =
                std::num::NonZeroU32::new(depth).expect("positive depth");
            snapshot
                .content
                .execution_policy
                .max_active_delegated_children =
                std::num::NonZeroU32::new(children).expect("positive concurrency");
        }
        let mut snapshot = liveness_admission::seal(
            snapshot,
            Arc::new(
                RunExecutionPolicySnapshot::production_defaults()
                    .liveness
                    .expect("worker acceptance fixture")
                    .model_policy(),
            ),
        );
        {
            snapshot.content.policy.permission_mode =
                musteragent_policy::PermissionMode::ServiceManaged;
            snapshot.content.product_release = Some(if host {
                self.release.clone()
            } else {
                compiled_product_release(ProductId::KERNEL)
            });
            for tool in &mut snapshot.content.tools {
                tool.manifest.execution_policy = catalog
                    .by_key(&tool.manifest.key)
                    .expect("exact release contract")
                    .pin(provider)
                    .execution_policy;
            }
            snapshot
                .content
                .discovery
                .as_mut()
                .expect("worker acceptance fixture")
                .authorized_tool_catalog_hash =
                authorized_tool_catalog(&snapshot.content.tools, &snapshot.content.capabilities)
                    .expect("worker acceptance fixture")
                    .catalog_hash();
            snapshot = RuntimeSnapshot::seal(
                fixture.runtime_snapshot_id,
                snapshot.content,
                running_loop::database_now(&self.pool).await,
                &musteragent_wire::ProtobufSnapshotCodec,
            )
            .expect("host-bound snapshot");
        }
        production_context::install_production_context(
            &self.pool,
            self.content.clone(),
            fixture,
            Arc::new(ProtobufLedgerPayloadCodec),
            snapshot.content,
            input,
        )
        .await;
        // The fixture represents an admitted predecessor that died. Only lease
        // expiry changes; the real scheduler writes the successor attempt.
        sqlx::query("UPDATE runtime.leases SET expires_at_ms=renewed_at_ms WHERE run_id=$1")
            .bind(fixture.run_id.into_uuid())
            .execute(&self.pool)
            .await
            .expect("worker acceptance fixture");
        fixture
    }
    fn worker(
        &self,
        name: &str,
        model: &musteragent_model::ModelRoute,
        endpoint: &str,
    ) -> Result<Child, BuildError> {
        self.worker_with_sandbox(name, model, endpoint, None)
    }
    fn worker_with_sandbox(
        &self,
        name: &str,
        model: &musteragent_model::ModelRoute,
        endpoint: &str,
        server: Option<running_loop::Fixture>,
    ) -> Result<Child, BuildError> {
        self.worker_with_model_recovery(name, model, endpoint, server, "")
    }
    fn worker_with_model_recovery(
        &self,
        name: &str,
        model: &musteragent_model::ModelRoute,
        endpoint: &str,
        server: Option<running_loop::Fixture>,
        recovery: &str,
    ) -> Result<Child, BuildError> {
        let config = self.dir.join(format!("{name}.toml"));
        let base = endpoint
            .strip_suffix("/chat/completions")
            .expect("worker acceptance fixture");
        let text = format!(
            r#"database_url = "{}"
worker_id = "{}"
key_id = "runtime.loop.test"
tool_releases = [{:?}]
keyring_file = {:?}
extension_state_root = {:?}
extension_workspace_root = {:?}
concurrency = 1
lease_ms = 3000
heartbeat_ms = 500
drain_ms = 5000
[object_store]
endpoint = "{}"
region = "us-east-1"
bucket = "{}"
prefix = "smoke"
access_key_env = "P9_S3_KEY"
secret_key_env = "P9_S3_SECRET"
allow_http = true
[[models]]
provider_id = "{}"
entry_id = "{}"
protocol = "openai-chat"
base_url = "{}"
credential_env = "P9_MODEL_KEY"
allow_loopback_http = true
[models.headers]
x-fixture-host = "p9"
"#,
            self.pg.connection_url(),
            name,
            self.dir.join("host-release.json"),
            self.dir.join("fixture-keyring.json"),
            self.dir.join("state"),
            self.dir.join("workspaces"),
            self.minio.endpoint,
            self.minio.bucket,
            model.model_provider_id,
            model.model_entry_id,
            base
        );
        let mut text = text;
        text.push_str(recovery);
        if let Some(fixture) = server {
            let root = self.dir.join("sandbox-work");
            std::fs::create_dir_all(&root)?;
            text.push_str(&format!(
                r#"
[[sandboxes]]
tenant_id = "{}"
principal_id = "{}"
provider_id = "{}"
[sandboxes.config]
runtime_release_ref = "browser-runtime@2"
resource_profile_ref = "server.sandbox@1"
work_root = {:?}
container_user = "65534:65534"
max_concurrent_jobs = 1
[sandboxes.config.image]
reference = "python:3.12-slim"
[sandboxes.config.labels]
owner = "codex-p9"
run-id = "p9-worker-sandbox"
purpose = "worker-routing-acceptance"
disposable = "true"
[sandboxes.config.lease]
ttl_millis = 6000
poll_interval_millis = 50
reregister_backoff_millis = 50
"#,
                fixture.tenant_id,
                fixture.principal_id,
                CapabilityProviderId::generate(),
                root
            ));
        }
        std::fs::write(&config, text)?;
        let output = std::fs::File::create(self.dir.join(format!("{name}.log")))?;
        Ok(Command::new(env!("CARGO_BIN_EXE_musteragent-cloud-worker"))
            .arg(config)
            // Ambient host deployment settings must never redirect a fixture.
            .env("MUSTERAGENT_DATABASE_URL", self.pg.connection_url())
            .env("MUSTERAGENT_WORKER_ID", name)
            .env("MUSTERAGENT_OTLP_MODE", "disabled")
            .env("P9_S3_KEY", MINIO_TEST_ACCESS_KEY)
            .env("P9_S3_SECRET", MINIO_TEST_SECRET_KEY)
            .env("P9_MODEL_KEY", "test-only-model-key")
            .env("RUST_LOG", "info")
            .stdout(output.try_clone()?)
            .stderr(output)
            .kill_on_drop(true)
            .spawn()?)
    }
    async fn completed(&self, fixture: running_loop::Fixture, child: &mut Child) {
        self.wait_completed(fixture, child).await;
        self.assert_tool_completion(fixture).await;
    }
    async fn wait_completed(&self, fixture: running_loop::Fixture, child: &mut Child) {
        let completion = tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
            loop {
                if let Some(exit) = child.try_wait().expect("worker acceptance fixture") {
                    panic!("worker exited: {exit}; evidence {}", self.dir.display());
                }
                let state: i16 =
                    sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
                        .bind(fixture.run_id.into_uuid())
                        .fetch_one(&self.pool)
                        .await
                        .expect("worker acceptance fixture");
                if state == 5 {
                    break;
                }
                if [6, 7].contains(&state) {
                    let tools: Vec<serde_json::Value> =
                        sqlx::query_scalar("SELECT to_jsonb(t) FROM tool.tool_invocations t")
                            .fetch_all(&self.pool)
                            .await
                            .expect("tool diagnostics");
                    let runs: Vec<serde_json::Value> =
                        sqlx::query_scalar("SELECT to_jsonb(t) FROM runtime.runs t")
                            .fetch_all(&self.pool)
                            .await
                            .expect("run diagnostics");
                    std::fs::write(
                        self.dir.join("terminal-state.json"),
                        serde_json::to_vec_pretty(&serde_json::json!({"runs":runs,"tools":tools}))
                            .expect("diagnostic JSON"),
                    )
                    .expect("diagnostic file");
                }
                assert!(
                    ![6, 7].contains(&state),
                    "unexpected run state {state}; {}",
                    self.dir.display()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        if completion.is_err() {
            let mut diagnostic = serde_json::Map::new();
            for table in [
                "runtime.runs",
                "runtime.attempts",
                "runtime.delegations",
                "tool.tool_invocations",
                "runtime.delegation_events",
                "runtime.delegation_deliveries",
            ] {
                let rows: Vec<serde_json::Value> =
                    sqlx::query_scalar(&format!("SELECT to_jsonb(r) FROM {table} r"))
                        .fetch_all(&self.pool)
                        .await
                        .expect("diagnostic state");
                diagnostic.insert(table.into(), rows.into());
            }
            std::fs::write(
                self.dir.join("timeout-state.json"),
                serde_json::to_vec_pretty(&diagnostic).expect("diagnostic JSON"),
            )
            .expect("write diagnostic");
            panic!("worker completion timeout; {}", self.dir.display());
        }
    }
    async fn assert_tool_completion(&self, fixture: running_loop::Fixture) {
        let tools: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1 AND status=4",
        )
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("worker acceptance fixture");
        if tools != 1 {
            use prost::Message;
            let events:Vec<Vec<u8>>=sqlx::query_scalar("SELECT encoded_event FROM event_store.domain_events JOIN event_store.ledger_records USING(record_id) WHERE run_id=$1").bind(fixture.run_id.into_uuid()).fetch_all(&self.pool).await.expect("worker acceptance fixture");
            for bytes in events {
                if let Ok(musteragent_wire::wire::DomainEvent {
                    event:
                        Some(musteragent_wire::wire::domain_event::Event::ToolInvocationFailed(event)),
                }) = musteragent_wire::wire::DomainEvent::decode(bytes.as_slice())
                {
                    eprintln!("tool failure {event:?}");
                }
            }

            let diagnostic: Vec<(String, i16, Option<Vec<u8>>)> = sqlx::query_as(
                "SELECT tool_ref,status,terminal_error FROM tool.tool_invocations WHERE run_id=$1",
            )
            .bind(fixture.run_id.into_uuid())
            .fetch_all(&self.pool)
            .await
            .expect("worker acceptance fixture");
            for (key, status, bytes) in diagnostic {
                eprintln!(
                    "tool {key} state {status}, error {:?}",
                    bytes.map(|b| String::from_utf8_lossy(&b).into_owned())
                );
            }
        }
        assert_eq!(
            tools,
            1,
            "one successful real tool terminal; {}",
            self.dir.display()
        );
        let usage: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime.model_usage_facts WHERE run_id=$1")
                .bind(fixture.run_id.into_uuid())
                .fetch_one(&self.pool)
                .await
                .expect("worker acceptance fixture");
        assert!(usage >= 2, "raw usage survives a None host publication");
        let attempts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime.attempts WHERE run_id=$1")
                .bind(fixture.run_id.into_uuid())
                .fetch_one(&self.pool)
                .await
                .expect("worker acceptance fixture");
        std::fs::write(self.dir.join(format!("{}.json",fixture.run_id)),serde_json::to_vec_pretty(&serde_json::json!({"run":fixture.run_id.to_string(),"status":"completed","successful_tools":tools,"raw_usage_facts":usage,"attempts":attempts})).expect("worker acceptance fixture")).expect("worker acceptance fixture");
    }
}
fn host_publication(dir: &Path) -> Result<ProductReleaseRef, BuildError> {
    let kernel = compiled_product_release(ProductId::KERNEL);
    let metadata = product_release_metadata(&kernel)?;
    let contract: serde_json::Value =
        serde_json::from_str(include_str!("../../../contracts/kernel-tools.json"))?;
    let doc = serde_json::json!({
        "schema_version":"musteragent.host-tool-release.v1", "product":"fixture-host", "release_id":"fixture-host.tools@1",
        "profile_id":"fixture-host", "cloud_head":"fixture.cloud@1", "device_head":"fixture.device@1", "skill_schema":metadata.skill_schema,
        "skill_release":{"product":kernel.product.as_str(),"release_id":kernel.release_id.as_str(),"digest":kernel.digest},
        "cloud":contract["products"]["musteragent"]["cloud"]["tools"], "device":contract["products"]["musteragent"]["device"]["tools"]
    });
    let bytes = serde_json::to_vec(&doc)?;
    let publication = HostProductRelease::from_json(&bytes)?;
    let reference = publication.reference().clone();
    install_host_product_releases(vec![publication.clone()])?;
    install_host_product_releases(vec![publication])?;
    let mut altered = reference.clone();
    altered.digest[0] ^= 1;
    assert!(
        BuiltinToolCatalog::for_product_release(&altered, ProductExecutionTarget::Cloud).is_err(),
        "a changed publication must not fall back"
    );
    assert_eq!(
        musteragent_skill::ProductSkillRelease::resolve(&reference)?.reference(),
        kernel,
        "host explicitly pins kernel skills"
    );
    std::fs::write(dir.join("host-release.json"), bytes)?;
    Ok(reference)
}
async fn terminate(child: &mut Child) {
    let id = child.id().expect("worker acceptance fixture");
    assert!(
        Command::new("kill")
            .args(["-TERM", &id.to_string()])
            .status()
            .await
            .expect("worker acceptance fixture")
            .success()
    );
    let exit = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("worker acceptance fixture")
        .expect("worker acceptance fixture");
    assert!(exit.success(), "graceful stop: {exit}");
}
#[tokio::test]
async fn real_worker_completes_tool_and_replacement_recovers_after_sigkill()
-> Result<(), BuildError> {
    let smoke = Smoke::start().await?;
    let mut model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    model.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    model.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    model.protocol_version = VersionedKey::new("openai.chat@1")?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let first = smoke.seed(&model, false).await;
    let mut worker = smoke.worker("worker-first", &model, provider.endpoint())?;
    smoke.completed(first, &mut worker).await;
    assert!(provider.request_count() >= 2);
    terminate(&mut worker).await;
    provider.shutdown().await?;
    let paused = ControlledOpenAiChatProvider::start_pinned_scenario(
        "127.0.0.1:0".parse()?,
        "[e2e:time]".into(),
        true,
    )
    .await?;
    let resumed = smoke.seed(&model, true).await;
    let mut doomed = smoke.worker("worker-killed", &model, paused.endpoint())?;
    paused.wait_for_requests(1, Duration::from_secs(30)).await?;
    doomed.kill().await?;
    doomed.wait().await?;
    paused.release()?;
    let mut replacement = smoke.worker("worker-replacement", &model, paused.endpoint())?;
    smoke.completed(resumed, &mut replacement).await;
    let generations: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT worker_id) FROM runtime.attempts WHERE run_id=$1",
    )
    .bind(resumed.run_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    assert_eq!(
        generations, 3,
        "fixture predecessor, killed worker, replacement"
    );
    terminate(&mut replacement).await;
    paused.shutdown().await?;
    eprintln!("worker process evidence: {}", smoke.dir.display());
    Ok(())
}

#[path = "binary_smoke/sandbox.rs"]
mod sandbox;

#[path = "binary_smoke/schedules.rs"]
mod schedules;

#[path = "binary_smoke/delegation.rs"]
mod delegation;

#[path = "binary_smoke/model_recovery.rs"]
mod model_recovery;
