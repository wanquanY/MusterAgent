use super::*;
use base64::engine::general_purpose::STANDARD;
use musteragent_testkit::{
    IsolatedMinio, IsolatedPostgres, MINIO_TEST_ACCESS_KEY, MINIO_TEST_SECRET_KEY,
};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::{Child, Command};
pub const TOKEN: &str = "p11-development-token-alpha";
pub const OTHER: &str = "p11-development-token-beta";
pub const TENANT: &str = "01982f80-0000-7000-8000-000000000801";
pub const USER: &str = "01982f80-0000-7000-8000-000000000802";
pub const PROVIDER: &str = "01982f80-0000-7000-8000-000000000803";
pub const ENTRY: &str = "01982f80-0000-7000-8000-000000000804";
pub const EMPTY_WORKSPACE_CATALOG: &[u8] =
    br#"{"schema_version":"musteragent.workspace-tool-releases.v1","releases":[]}"#;
pub struct Harness {
    _pg: IsolatedPostgres,
    pub pool: sqlx::PgPool,
    _minio: IsolatedMinio,
    pub dir: PathBuf,
    pub api: Child,
    pub worker: Option<Child>,
    pub model: Child,
    pub port: u16,
    pub client: reqwest::Client,
}
pub fn binary(name: &str) -> PathBuf {
    let binary = std::env::current_exe()
        .expect("test executable")
        .parent()
        .expect("deps")
        .parent()
        .expect("target profile")
        .join(name);
    assert!(
        binary.is_file(),
        "missing required binary {name} at {}; build first with: cargo build --offline -p musteragent-reference-api -p musteragent-cloud-worker -p musteragent-database-migrator --bins (use the same --target-dir and profile as this test)",
        binary.display()
    );
    binary
}
pub fn process(binary: &Path, config: &Path, dir: &Path, name: &str) -> Result<Child> {
    let log = std::fs::File::create(dir.join(format!("{name}.log")))?;
    Ok(Command::new(binary)
        .arg(config)
        .env_remove("MUSTERAGENT_DATABASE_URL")
        .env_remove("MUSTERAGENT_WORKER_ID")
        .env("MUSTERAGENT_OTLP_MODE", "disabled")
        .env("P11_TOKEN_A", TOKEN)
        .env("P11_TOKEN_B", OTHER)
        .env("P11_S3_KEY", MINIO_TEST_ACCESS_KEY)
        .env("P11_S3_SECRET", MINIO_TEST_SECRET_KEY)
        .env("P11_MODEL_KEY", "fixture-model-key")
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?)
}
impl Harness {
    pub async fn start() -> Result<Self> {
        Self::start_with_workspace_tools(true).await
    }

    pub async fn start_with_workspace_tools(include_workspace_tools: bool) -> Result<Self> {
        for name in [
            "musteragent-reference-api",
            "musteragent-cloud-worker",
            "musteragent-database-migrator",
        ] {
            binary(name);
        }
        let pg = IsolatedPostgres::start_with_pgvector().await?;
        let pool = pg.pool().await?;
        let dir = std::env::var_os("MUSTERAGENT_TEST_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.codex-work/p11"))
            .join(format!("acceptance-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir)?;
        let migrator_log = std::fs::File::create(dir.join("migrator.log"))?;
        let status = Command::new(binary("musteragent-database-migrator"))
            .arg("apply")
            .env("MUSTERAGENT_DATABASE_URL", pg.connection_url())
            .env("MUSTERAGENT_OTLP_MODE", "disabled")
            .stdout(migrator_log.try_clone()?)
            .stderr(migrator_log)
            .status()
            .await?;
        assert!(status.success(), "migrator failed: {}", dir.display());
        let minio = IsolatedMinio::start().await?;
        let keyring = dir.join("fixture-keyring.json");
        std::fs::write(
            &keyring,
            serde_json::to_vec(
                &json!({"schema_version":"musteragent.opaque-keyring.v2","keys":[{"key_id":"p11.fixture","key_base64":STANDARD.encode([17u8;32])}]}),
            )?,
        )?;
        let port = std::net::TcpListener::bind("127.0.0.1:0")?
            .local_addr()?
            .port();
        std::fs::write(
            dir.join("workspace-releases.json"),
            if include_workspace_tools {
                include_bytes!("../../examples/workspace-releases.json").as_slice()
            } else {
                EMPTY_WORKSPACE_CATALOG
            },
        )?;
        let common = format!(
            r#"database_url = {:?}
key_id = "p11.fixture"
keyring_file = {:?}
workspace_releases_file = "workspace-releases.json"
[object_store]
endpoint = {:?}
region = "us-east-1"
bucket = {:?}
prefix = "p11"
access_key_env = "P11_S3_KEY"
secret_key_env = "P11_S3_SECRET"
allow_http = true
"#,
            pg.connection_url(),
            keyring,
            minio.endpoint,
            minio.bucket
        );
        let workspace_grant = if include_workspace_tools {
            format!(
                r#"[[agents.workspace_tools]]
principal_id = "{USER}"
provider_id = "{PROVIDER}"
plugin_id = "mcp.example-search"
version = "1.0.0"
release_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
operation_ref = "mcp.example_search.find.h_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc@1"
scopes = []
"#
            )
        } else {
            String::new()
        };
        let api_config = format!(
            r#"listen = "127.0.0.1:{port}"
{common}
[[identities]]
token_env = "P11_TOKEN_A"
tenant_id = "{TENANT}"
user_id = "{USER}"
agents = ["reference-agent", "approval-agent"]
routes = ["reference.chat@1"]
[[identities]]
token_env = "P11_TOKEN_B"
tenant_id = "01982f80-0000-7000-8000-000000000811"
user_id = "01982f80-0000-7000-8000-000000000812"
agents = ["foreign-agent"]
routes = ["reference.chat@1"]
[[agents]]
key = "reference-agent"
id = "01982f80-0000-7000-8000-000000000805"
version_id = "01982f80-0000-7000-8000-000000000806"
tenant_id = "{TENANT}"
published_by = "{USER}"
instructions = "Use the requested tools and return the observed result."
tools = ["system.time.current", "delegation.spawn", "delegation.join", "delegation.list", "data.python.run", "schedules.create", "schedules.list", "schedules.update", "schedules.control", "schedules.history"]
{workspace_grant}[[agents]]
key = "approval-agent"
id = "01982f80-0000-7000-8000-000000000815"
version_id = "01982f80-0000-7000-8000-000000000816"
tenant_id = "{TENANT}"
published_by = "{USER}"
instructions = "Read the time after approval and report the observed tool outcome."
tools = ["system.time.current"]
approval_tools = ["system.time.current"]
[[agents]]
key = "foreign-agent"
id = "01982f80-0000-7000-8000-000000000825"
version_id = "01982f80-0000-7000-8000-000000000826"
tenant_id = "01982f80-0000-7000-8000-000000000811"
published_by = "01982f80-0000-7000-8000-000000000812"
instructions = "Read the time using authorized tools."
tools = ["system.time.current"]
[[routes]]
key = "reference.chat@1"
provider_id = "{PROVIDER}"
entry_id = "{ENTRY}"
model = "p11-fake-model"
context_window = 131072
max_output_tokens = 4096
"#
        );
        std::fs::write(dir.join("api.toml"), api_config)?;
        let model_log = std::fs::File::create(dir.join("model.log"))?;
        let model = Command::new("python3")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/model.py"))
            .arg(&dir)
            .stdout(Stdio::null())
            .stderr(model_log)
            .kill_on_drop(true)
            .spawn()?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !dir.join("model-port").exists() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await?;
        let model_port = std::fs::read_to_string(dir.join("model-port"))?;
        let sandbox = dir.join("sandbox");
        std::fs::create_dir_all(&sandbox)?;
        let worker_config = format!(
            r#"worker_id = "p11-worker"
publish_raw_usage = true
extension_state_root = {:?}
extension_workspace_root = {:?}
concurrency = 1
# Use the worker defaults: 30s lease, 5s heartbeat.
drain_ms = 5000
{common}
[[models]]
provider_id = "{PROVIDER}"
entry_id = "{ENTRY}"
protocol = "openai-chat"
base_url = "http://127.0.0.1:{model_port}/v1"
credential_env = "P11_MODEL_KEY"
allow_loopback_http = true
[[sandboxes]]
tenant_id = "{TENANT}"
principal_id = "{USER}"
provider_id = "01982f80-0000-7000-8000-000000000807"
[sandboxes.config]
runtime_release_ref = "browser-runtime@2"
resource_profile_ref = "server.sandbox@1"
work_root = {:?}
container_user = "65534:65534"
max_concurrent_jobs = 1
[sandboxes.config.image]
reference = "musteragent-sandbox-mcp:p6"
[sandboxes.config.labels]
owner = "codex-p11"
run-id = {:?}
purpose = "reference-api-resilience"
disposable = "true"
expires-at = "unix:{}"
[sandboxes.config.lease]
ttl_millis = 6000
poll_interval_millis = 50
reregister_backoff_millis = 50
"#,
            dir.join("state"),
            dir.join("workspaces"),
            sandbox,
            dir.file_name().expect("run dir").to_string_lossy(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                + 3600
        );
        std::fs::write(dir.join("worker.toml"), worker_config)?;
        let api = process(
            Path::new(env!("CARGO_BIN_EXE_musteragent-reference-api")),
            &dir.join("api.toml"),
            &dir,
            "api-first",
        )?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let mut h = Self {
            _pg: pg,
            pool,
            _minio: minio,
            dir,
            api,
            worker: None,
            model,
            port,
            client,
        };
        h.ready().await?;
        Ok(h)
    }
    pub async fn ready(&mut self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    self.api.try_wait()?.is_none(),
                    "API exited: {}",
                    self.dir.display()
                );
                if self
                    .client
                    .get(self.url("/v1/conversations"))
                    .bearer_auth(TOKEN)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    return Ok::<_, musteragent_reference_api::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;
        Ok(())
    }
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }
    pub fn start_worker(&mut self, name: &str) -> Result<()> {
        self.worker = Some(process(
            &binary("musteragent-cloud-worker"),
            &self.dir.join("worker.toml"),
            &self.dir,
            name,
        )?);
        Ok(())
    }
    pub async fn restart_api(&mut self) -> Result<()> {
        let previous = self.api.id();
        self.api.kill().await?;
        let status = self.api.wait().await?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        self.api = process(
            Path::new(env!("CARGO_BIN_EXE_musteragent-reference-api")),
            &self.dir.join("api.toml"),
            &self.dir,
            "api-replacement",
        )?;
        self.ready().await?;
        std::fs::write(
            self.dir.join("api-restart-receipt.json"),
            serde_json::to_vec_pretty(&json!({
                "previous_pid":previous, "exit":status.to_string(), "replacement_pid":self.api.id()
            }))?,
        )?;
        Ok(())
    }
    pub async fn get(&self, path: &str) -> Result<Value> {
        let r = self
            .client
            .get(self.url(path))
            .bearer_auth(TOKEN)
            .send()
            .await?;
        let s = r.status();
        let body = r.text().await?;
        assert!(
            s.is_success(),
            "GET {path} {s}: {body}; {}",
            self.dir.display()
        );
        Ok(serde_json::from_str(&body)?)
    }
    pub async fn post(&self, path: &str, body: Value) -> Result<Value> {
        let r = self
            .client
            .post(self.url(path))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await?;
        let s = r.status();
        let text = r.text().await?;
        assert!(
            s.is_success(),
            "POST {path} {s}: {text}; {}",
            self.dir.display()
        );
        Ok(serde_json::from_str(&text)?)
    }
    pub async fn submit(&self, case: &str, conversation: Option<&str>) -> Result<Value> {
        self.post("/v1/runs",json!({"command_id":uuid::Uuid::now_v7(),"conversation_id":conversation,"message":format!("[p11:parent:{case}] Delegate the calculation."),"agent":"reference-agent","model_route":"reference.chat@1","placement":"cloud"})).await
    }
    pub async fn status(&self, run: &str, expected: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let v = self.get(&format!("/v1/runs/{run}")).await?;
                if v["status"] == expected {
                    return Ok::<_, musteragent_reference_api::Error>(());
                }
                assert!(
                    v["status"] != "Failed",
                    "run failed: {v}; {}",
                    self.dir.display()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await??;
        Ok(())
    }
    pub async fn reached(&self, case: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            while !self.dir.join(format!("{case}.reached")).exists() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
        Ok(())
    }
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(worker) = &mut self.worker {
            worker.kill().await?;
            worker.wait().await?;
        }
        self.api.kill().await?;
        self.api.wait().await?;
        self.model.kill().await?;
        self.model.wait().await?;
        self.pool.close().await;
        std::fs::remove_file(self.dir.join("fixture-keyring.json"))?;
        Ok(())
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        // Stop our clients before the disposable storage guards remove their
        // containers, including assertion/timeout unwinding paths.
        if let Some(worker) = &mut self.worker {
            let _ = worker.start_kill();
        }
        let _ = self.api.start_kill();
        let _ = self.model.start_kill();
        let _ = std::fs::remove_file(self.dir.join("fixture-keyring.json"));
    }
}
