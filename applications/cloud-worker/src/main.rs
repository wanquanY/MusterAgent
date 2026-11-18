use musteragent_cloud_worker::*;
use musteragent_model_providers::*;
use musteragent_postgres::{PostgresContentStore, PostgresPoolConfig};
use musteragent_runtime_model::*;
use musteragent_storage_protection::{OpaqueRefKeyring, ProtectedContentKeyring};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Default deployment records raw usage locally. Hosts may add publication hooks.
struct LocalUsage {
    publish_raw: bool,
}
impl MeteringPublicationEncoder for LocalUsage {
    fn encode_billable_usage(
        &self,
        fact: &AttributedModelUsageFact,
        id: musteragent_identity::OutboxMessageId,
    ) -> Result<Option<MeteringPublication>, ModelContractError> {
        fact.validate()?;
        if self.publish_raw {
            self.encode(fact, id).map(Some)
        } else {
            Ok(None)
        }
    }
    fn encode(
        &self,
        fact: &AttributedModelUsageFact,
        id: musteragent_identity::OutboxMessageId,
    ) -> Result<MeteringPublication, ModelContractError> {
        musteragent_wire::ProtobufMeteringPublicationEncoder.encode(fact, id)
    }
}

#[tokio::main]
async fn main() -> Result<(), BuildError> {
    let mut shutdown = musteragent_cloud_lifecycle::ShutdownSignal::install()?;
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: musteragent-cloud-worker CONFIG.toml")?;
    let settings = CloudWorkerSettings::load(Path::new(&path))?;
    let workspace = settings
        .workspace_releases_file
        .as_deref()
        .map(workspace_releases::load_workspace_releases)
        .transpose()?;
    let policy = settings.policy()?;
    let publications = settings
        .tool_releases
        .iter()
        .map(|path| -> Result<_, BuildError> {
            Ok(musteragent_tool::HostProductRelease::from_json(
                &std::fs::read(path)?,
            )?)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let releases = publications.iter().map(|p| p.reference().clone()).collect();
    musteragent_tool::install_host_product_releases(publications)?;
    let telemetry = musteragent_telemetry_otlp::TelemetryRuntime::install(
        musteragent_telemetry_otlp::TelemetryConfig::from_environment(
            "musteragent-cloud-worker",
            env!("CARGO_PKG_VERSION"),
            "host",
            false,
        )?,
    )?;
    let database =
        PostgresPoolConfig::standard("musteragent-cloud-worker", &settings.worker_id, 16)?
            .connect_runtime(&settings.database_url, telemetry.db_commit_telemetry())
            .await?;
    let material = musteragent_cloud_config::load_opaque_keyring(
        &settings.key_id,
        None,
        Some(&settings.keyring_file),
        false,
    )?;
    let keys = Arc::new(OpaqueRefKeyring::new(
        material.active_key_id().clone(),
        material.keys(),
    )?);
    let recovery = Arc::new(ProtectedContentKeyring::new(
        material.active_key_id().clone(),
        material.keys(),
    )?);
    let content = Arc::new(PostgresContentStore::new(
        database.clone(),
        Arc::new(settings.object_store.build()?),
        keys.clone(),
    ));
    if let Some(catalog) = workspace {
        workspace_releases::publish_workspace_releases(&catalog, content.as_ref()).await?;
    }
    let mut endpoints = Vec::new();
    let mut headers = BTreeMap::new();
    let mut invocation_recovery = BTreeMap::new();
    for model in &settings.models {
        let endpoint = model.endpoint()?;
        let mut values = reqwest::header::HeaderMap::new();
        for (name, value) in &model.headers {
            values.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(value)?,
            );
        }
        headers.insert(
            (endpoint.model_provider_id, endpoint.model_entry_id),
            values,
        );
        if let Some(settings) = &model.existing_invocation {
            invocation_recovery.insert(
                (endpoint.model_provider_id, endpoint.model_entry_id),
                settings.build()?,
            );
        }
        endpoints.push(endpoint);
    }
    // Each endpoint was validated with its own explicit local-HTTP opt-in above.
    let model = Arc::new(
        DirectModelProvider::new(
            Arc::new(StaticEndpointRegistry::new(endpoints, true)?),
            EnvironmentCredentials::new(&settings.models)?,
            Arc::new(RuntimeModelContentResolver::new(content.clone())),
            ProviderAdapterConfig::default(),
        )
        .map_err(|e| format!("model provider configuration: {:?}", e.safe_code))?
        .with_host_headers(HostRequestHeaders {
            static_headers: Default::default(),
            hook: Some(Arc::new(ConfiguredModelHeaders(headers))),
        })
        .with_existing_invocation_recovery(invocation_recovery)
        .map_err(|e| format!("model recovery configuration: {:?}", e.safe_code))?,
    );
    let liveness_clock =
        Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default());
    let embeddings: Option<Arc<dyn musteragent_runtime_memory::MemoryEmbeddingProvider>> = settings
        .embeddings
        .clone()
        .map(HttpMemoryEmbeddingProvider::new)
        .transpose()?
        .map(|p| Arc::new(p) as Arc<_>);
    let dispatcher = kernel_dispatcher(ToolServices {
        database: database.clone(),
        content: content.clone(),
        keys: keys.clone(),
        clock: liveness_clock.clone(),
        providers: vec![],
        handlers: vec![],
        media: vec![],
        embeddings: embeddings.clone(),
        model: model.clone(),
        metering: Arc::new(LocalUsage {
            publish_raw: settings.publish_raw_usage,
        }),
        releases,
    })?;
    let composition = CloudWorkerComposition::build(
        WorkerHost {
            database: database.clone(),
            content: content.clone(),
            model,
            metering: Arc::new(LocalUsage {
                publish_raw: settings.publish_raw_usage,
            }),
            reference_keys: keys,
            recovery_keys: recovery,
            dispatcher,
            liveness_clock,
            extensions: WorkerExtensionConfig {
                registry: musteragent_extension::ExtensionRegistry::default(),
                state_root: settings.extension_state_root.clone(),
                workspace_root: settings.extension_workspace_root.clone(),
                subagent_agents: settings.extension_subagent_agents.clone(),
                loop_detection: musteragent_extension::LoopDetectionConfig::default(),
            },
            observer: Some(Arc::new(CloudWorkerRunnerObserver::new(
                telemetry.metrics(),
                telemetry.attempt_telemetry(),
            ))),
        },
        policy,
    )
    .await?;
    let stop = CancellationToken::new();
    let mut sandboxes = Vec::new();
    for sandbox in settings.sandboxes {
        sandboxes.push(sandbox.build(database.clone(), content.clone()).await?);
    }
    let mut tasks = tokio::task::JoinSet::<Result<(), BuildError>>::new();
    let worker_stop = stop.clone();
    tasks.spawn(async move { composition.run(worker_stop).await });
    if let Some(embeddings) = embeddings {
        let index = ResourceEmbeddingWorker::new(&database, embeddings);
        let index_stop = stop.clone();
        tasks.spawn(async move {
            index.run(index_stop).await;
            Ok(())
        });
    }
    for provider in sandboxes {
        let provider_stop = stop.clone();
        tasks.spawn(async move {
            provider
                .run(provider_stop)
                .await
                .map_err(|error| Box::new(error) as BuildError)
        });
    }
    let result = tokio::select! {
        first = tasks.join_next() => match first { Some(Ok(result)) => result, Some(Err(error)) => Err(Box::new(error) as BuildError), None => Err("worker generation has no tasks".into()) },
        signal = shutdown.recv() => signal.map_err(|error| Box::new(error) as BuildError),
    };
    stop.cancel();
    let mut result = result;
    while let Some(joined) = tasks.join_next().await {
        let completed = joined
            .map_err(|error| Box::new(error) as BuildError)
            .and_then(|value| value);
        if result.is_ok() {
            result = completed;
        }
    }
    database.pool().close().await;
    telemetry.shutdown();
    result
}
