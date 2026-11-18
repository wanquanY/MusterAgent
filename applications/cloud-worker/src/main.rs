use musteragent_cloud_worker::{BuildError, CloudWorkerSettings, WorkerOverrides, run_worker};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), BuildError> {
    let mut shutdown = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: musteragent-cloud-worker CONFIG.toml")?;
    let settings = CloudWorkerSettings::load(Path::new(&path))?;
    run_worker(settings, WorkerOverrides::default(), shutdown.recv()).await
}
