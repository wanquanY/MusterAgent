use musteragent_cloud_projector::{CloudProjectionHandler, subscription};
use musteragent_postgres::{PostgresProjectionStore, PostgresWakeChannel, PostgresWakeSignal};
use musteragent_projection::{DurableProjector, ProjectionPolicy};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let worker_id = std::env::var("MUSTERAGENT_PROJECTOR_ID")?;
    let telemetry = musteragent_telemetry_otlp::TelemetryRuntime::install(
        musteragent_telemetry_otlp::TelemetryConfig::from_environment(
            "musteragent-cloud-projector",
            env!("CARGO_PKG_VERSION"),
            "host",
            false,
        )?,
    )?;
    let database = musteragent_postgres::PostgresPoolConfig::standard(
        "musteragent-cloud-projector",
        &worker_id,
        8,
    )?
    .connect_runtime(
        &std::env::var("MUSTERAGENT_DATABASE_URL")?,
        telemetry.db_commit_telemetry(),
    )
    .await?;
    let pool = database.pool().clone();
    musteragent_postgres::verify_migrations(&pool).await?;
    let wake = Arc::new(PostgresWakeSignal::new());
    let projector = DurableProjector::new(
        ProjectionPolicy {
            claim_batch_size: 8,
            max_inflight: 8,
            lease_duration: Duration::from_secs(30),
            idle_wait: Duration::from_secs(1),
            retry_backoff_ceiling: Duration::from_secs(60),
        },
        worker_id,
        subscription()?,
        Arc::new(PostgresProjectionStore::new(pool.clone())),
        Arc::new(CloudProjectionHandler::new(pool.clone())),
        wake.clone(),
    )?;
    let stop = CancellationToken::new();
    let process = async {
        let listener = async {
            let result = wake
                .run(&pool, PostgresWakeChannel::Outbox, stop.clone())
                .await;
            stop.cancel();
            result
        };
        let consumer = async {
            let result = projector.run(stop.clone()).await;
            stop.cancel();
            result
        };
        let (a, b) = tokio::join!(listener, consumer);
        a?;
        b?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::pin!(process);
    let result = tokio::select! { result = &mut process => result, received = signal.recv() => { stop.cancel(); received?; process.await } };
    pool.close().await;
    telemetry.shutdown();
    result
}
