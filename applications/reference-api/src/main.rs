use musteragent_reference_api::{
    Api, Result, config::Settings, identity::StaticDevelopmentIdentity, router,
};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;
#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: musteragent-reference-api CONFIG.toml")?;
    let settings = Settings::load(Path::new(&path))?;
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    let identity = Arc::new(StaticDevelopmentIdentity::load(&settings.identities)?);
    let stop = CancellationToken::new();
    let api = Api::build(settings, identity, stop.clone()).await?;
    eprintln!(
        "reference API dev-only listening on {}",
        listener.local_addr()?
    );
    let mut signal = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let shutdown = stop.clone();
    let signal_task = tokio::spawn(async move {
        let result = signal.recv().await;
        shutdown.cancel();
        result
    });
    let result = axum::serve(listener, router(api.clone()))
        .with_graceful_shutdown(stop.clone().cancelled_owned())
        .await;
    stop.cancel();
    signal_task.abort();
    let _ = signal_task.await;
    api.close().await;
    Ok(result?)
}
