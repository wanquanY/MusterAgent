use std::process::Command;
use std::sync::Once;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sqlx::postgres::{PgPool, PgPoolOptions};
use testcontainers::core::{ContainerAsync, ContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ImageExt, TestcontainersError};
use testcontainers_modules::postgres::Postgres;
use thiserror::Error;

const POSTGRES_IMAGE_TAG: &str = "16-alpine";
const POSTGRES_POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
const POSTGRES_CONTAINER_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const POSTGRES_CONTAINER_START_ATTEMPTS: usize = 3;
const POSTGRES_READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const POSTGRES_READINESS_RETRY_DELAY: Duration = Duration::from_millis(200);
const ISOLATED_POSTGRES_PURPOSE: &str = "isolated-postgres-conformance";
static REAP_ORPHANED_CONTAINERS: Once = Once::new();

/// Disposable PostgreSQL 16 instance removed when the harness is dropped.
#[derive(Debug)]
pub struct IsolatedPostgres {
    _container: ContainerAsync<Postgres>,
    connection_url: String,
}

impl IsolatedPostgres {
    pub async fn start() -> Result<Self, PostgresHarnessError> {
        Self::start_image("postgres", POSTGRES_IMAGE_TAG, false).await
    }

    pub async fn start_with_pgvector() -> Result<Self, PostgresHarnessError> {
        Self::start_image("pgvector/pgvector", "0.8.6-pg16-bookworm", false).await
    }

    /// Bounded disposable database in container memory, for focused tests on
    /// hosts whose Docker data disk is full. No existing volume is touched.
    pub async fn start_in_memory() -> Result<Self, PostgresHarnessError> {
        Self::start_image("postgres", POSTGRES_IMAGE_TAG, true).await
    }

    async fn start_image(
        image: &str,
        tag: &str,
        in_memory: bool,
    ) -> Result<Self, PostgresHarnessError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let owner = std::env::var("MUSTERAGENT_TEST_OWNER")
            .unwrap_or_else(|_| "musteragent-testkit".into());
        let run_id = std::env::var("MUSTERAGENT_TEST_RUN_ID")
            .unwrap_or_else(|_| format!("{}-{}", std::process::id(), now.as_millis()));
        let expires_at = std::env::var("MUSTERAGENT_TEST_EXPIRES_AT")
            .unwrap_or_else(|_| format!("unix:{}", now.as_secs() + 3600));
        if std::env::var_os("MUSTERAGENT_TEST_RUN_ID").is_none() {
            REAP_ORPHANED_CONTAINERS.call_once(reap_orphaned_containers);
        }
        let mut attempt = 0;
        loop {
            attempt += 1;
            let request = Postgres::default()
                .with_name(image)
                .with_tag(tag)
                // Publish the PostgreSQL endpoint explicitly. Host port zero
                // asks Docker for a disposable ephemeral port; do not depend
                // on image EXPOSE inheritance plus publish_all_ports.
                .with_mapped_port(0, ContainerPort::Tcp(5432))
                .with_label("owner", owner.clone())
                .with_label("run-id", run_id.clone())
                .with_label("purpose", ISOLATED_POSTGRES_PURPOSE)
                .with_label("disposable", "true")
                .with_label("expires-at", expires_at.clone());
            let request = if in_memory {
                request
                    .with_shm_size(512 * 1024 * 1024)
                    .with_env_var("PGDATA", "/dev/shm/musteragent-testdata")
            } else {
                request
            };
            let container = request.start().await?;
            let host = container.get_host().await?;
            let port = match crate::container_readiness::published_port(
                &container,
                5432,
                POSTGRES_CONTAINER_PROBE_TIMEOUT,
            )
            .await
            {
                Ok(port) => port,
                Err(error @ TestcontainersError::PortNotExposed { .. }) => {
                    // A running container without a published endpoint is an
                    // incomplete fixture, just like a database that never
                    // accepts connections. Dispose it before another bounded
                    // startup attempt; never retry the test itself.
                    container.rm().await?;
                    if attempt == POSTGRES_CONTAINER_START_ATTEMPTS {
                        return Err(error.into());
                    }
                    eprintln!(
                        "isolated PostgreSQL startup attempt {} failed: {error}; removed container before retry",
                        attempt
                    );
                    tokio::time::sleep(POSTGRES_READINESS_RETRY_DELAY).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let connection_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
            let readiness =
                match connect_pool(&connection_url, POSTGRES_CONTAINER_PROBE_TIMEOUT).await {
                    Ok(pool) => {
                        let result = sqlx::query("SELECT 1").execute(&pool).await.map(|_| ());
                        pool.close().await;
                        result
                    }
                    Err(error) => Err(error),
                };
            match readiness {
                Ok(()) => {
                    return Ok(Self {
                        _container: container,
                        connection_url,
                    });
                }
                Err(error) => {
                    container.rm().await?;
                    if attempt == POSTGRES_CONTAINER_START_ATTEMPTS {
                        return Err(error.into());
                    }
                    eprintln!(
                        "isolated PostgreSQL startup attempt {} failed: {error}; removed container before retry",
                        attempt
                    );
                    tokio::time::sleep(POSTGRES_READINESS_RETRY_DELAY).await;
                }
            }
        }
    }

    #[must_use]
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    pub async fn pool(&self) -> Result<PgPool, sqlx::Error> {
        connect_pool(&self.connection_url, POSTGRES_READINESS_TIMEOUT).await
    }
}

/// Remove disposable containers whose test process no longer exists.
///
/// The container is normally removed when the harness drops, but a test binary
/// that is killed (timeout, SIGKILL, interrupted run) never drops it. Those
/// containers keep running with their anonymous data volume; on 2026-11-16 77
/// of them accumulated and Docker stopped answering. The default run id is
/// `{pid}-{millis}`, so a container whose owning pid is gone is an orphan.
/// Runs that set `MUSTERAGENT_TEST_RUN_ID` manage their own lifecycle and are
/// never reaped here. Best effort: without a Docker CLI nothing happens.
fn reap_orphaned_containers() {
    let Ok(output) = Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("label=purpose={ISOLATED_POSTGRES_PURPOSE}"),
            "--format",
            "{{.ID}} {{.Label \"run-id\"}}",
        ])
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let orphans: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let (id, run_id) = line.split_once(' ')?;
            let pid: u32 = run_id.split_once('-')?.0.parse().ok()?;
            (pid != std::process::id() && !process_exists(pid)).then(|| id.to_owned())
        })
        .collect();
    if orphans.is_empty() {
        return;
    }
    let _ = Command::new("docker")
        .args(["rm", "--force", "--volumes"])
        .args(&orphans)
        .output();
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .map_or(true, |status| status.success())
}

async fn connect_pool(connection_url: &str, timeout: Duration) -> Result<PgPool, sqlx::Error> {
    let started_at = Instant::now();
    loop {
        let result = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(POSTGRES_POOL_ACQUIRE_TIMEOUT)
            .connect(connection_url)
            .await;
        match result {
            Ok(pool) => return Ok(pool),
            Err(_error) if started_at.elapsed() < timeout => {
                tokio::time::sleep(POSTGRES_READINESS_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Error)]
pub enum PostgresHarnessError {
    #[error("isolated PostgreSQL container failed: {0}")]
    Container(#[from] TestcontainersError),
    #[error("isolated PostgreSQL connection failed: {0}")]
    Database(#[from] sqlx::Error),
}
