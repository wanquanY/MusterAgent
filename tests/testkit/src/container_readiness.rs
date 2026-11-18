//! Readiness shared by disposable services with dynamically published ports.
use std::time::{Duration, Instant};
use testcontainers::{TestcontainersError, core::ContainerAsync};

// Docker may report a running, log-ready container before publishing the host
// port in its inspection response. Wait only for that specific startup state;
// daemon errors, stopped containers and the bounded timeout still fail the test.
pub(crate) async fn published_port<I: testcontainers::Image>(
    container: &ContainerAsync<I>,
    port: u16,
    timeout: Duration,
) -> Result<u16, TestcontainersError> {
    let started = Instant::now();
    loop {
        match container.get_host_port_ipv4(port).await {
            Ok(port) => return Ok(port),
            Err(error @ TestcontainersError::PortNotExposed { .. }) => {
                if started.elapsed() >= timeout || !container.is_running().await? {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => return Err(error),
        }
    }
}
