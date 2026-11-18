//! Disposable S3 API fixture; no existing bucket, volume or service is used.
use testcontainers::core::{ContainerAsync, ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt, TestcontainersError};

pub const MINIO_TEST_ACCESS_KEY: &str = "kernel-test-access";
pub const MINIO_TEST_SECRET_KEY: &str = "kernel-test-secret-for-disposable-fixture";

pub struct IsolatedMinio {
    container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub bucket: &'static str,
}
impl IsolatedMinio {
    async fn start_container() -> Result<ContainerAsync<GenericImage>, TestcontainersError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock");
        // MinIO no longer publishes images to Docker Hub or quay.io. Use the
        // Chainguard build, pinned by index digest for reproducible tests.
        GenericImage::new(
            "cgr.dev/chainguard/minio",
            "latest@sha256:7618b82bba99aadd77fcf59eb6cfd89b4b8b8e01ac796c474d57a1c1abf17e79",
        )
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_entrypoint("/bin/sh")
        .with_cmd([
            "-c",
            "mkdir -p /data/kernel-content; exec minio server /data",
        ])
        .with_mapped_port(0, ContainerPort::Tcp(9000))
        .with_env_var("MINIO_ROOT_USER", MINIO_TEST_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_TEST_SECRET_KEY)
        .with_label(
            "owner",
            std::env::var("MUSTERAGENT_TEST_OWNER")
                .unwrap_or_else(|_| "musteragent-testkit".into()),
        )
        .with_label(
            "run-id",
            std::env::var("MUSTERAGENT_TEST_RUN_ID")
                .unwrap_or_else(|_| format!("{}-{}", std::process::id(), now.as_millis())),
        )
        .with_label("purpose", "isolated-s3-conformance")
        .with_label("disposable", "true")
        .with_label("expires-at", format!("unix:{}", now.as_secs() + 3600))
        .start()
        .await
    }

    /// Create a new container with a test-only bucket and automatic teardown.
    pub async fn start() -> Result<Self, TestcontainersError> {
        // A log-ready container can still lack a published host port. Recreate
        // only that unusable fixture, before handing the endpoint to a test.
        let mut attempt = 1;
        loop {
            let container = Self::start_container().await?;
            match crate::container_readiness::published_port(
                &container,
                9000,
                std::time::Duration::from_secs(20),
            )
            .await
            {
                Ok(port) => {
                    return Ok(Self {
                        endpoint: format!("http://{}:{port}", container.get_host().await?),
                        container,
                        bucket: "kernel-content",
                    });
                }
                Err(error @ TestcontainersError::PortNotExposed { .. }) if attempt < 3 => {
                    container.rm().await?;
                    eprintln!("MinIO fixture had no published port on attempt {attempt}: {error}");
                    attempt += 1;
                }
                Err(error) => {
                    container.rm().await?;
                    return Err(error);
                }
            }
        }
    }
    /// Explicit teardown; Drop also owns cleanup if a test unwinds.
    pub async fn shutdown(self) -> Result<(), TestcontainersError> {
        self.container.rm().await
    }
}
