//! File configuration plus explicit environment overrides; secrets are never Debug.
use crate::{BuildError, WorkerPolicy};
use musteragent_model::{ApiFormat, ModelEntryId, ModelProviderId};
use musteragent_model_providers::*;
use musteragent_runtime_contract::ApplicationFuture;
use serde::Deserialize;
use std::{collections::BTreeMap, num::NonZeroUsize, path::Path, sync::Arc, time::Duration};

/// Standalone worker configuration. Secrets are referenced by environment variable name.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudWorkerSettings {
    pub database_url: String,
    pub worker_id: String,
    pub key_id: String,
    pub keyring_file: std::path::PathBuf,
    pub extension_state_root: std::path::PathBuf,
    pub extension_workspace_root: std::path::PathBuf,
    /// Explicit named-agent allow-list for P5 extension spawning.
    #[serde(default)]
    pub extension_subagent_agents: Vec<String>,
    pub object_store: S3Settings,
    pub models: Vec<ModelSettings>,
    pub embeddings: Option<crate::EmbeddingSettings>,
    /// Publish raw usage through the host outbox; no price calculation or charge.
    #[serde(default)]
    pub publish_raw_usage: bool,
    /// Public workspace release bundle, relative to the settings file.
    pub workspace_releases_file: Option<std::path::PathBuf>,
    /// Trusted immutable tool publications installed before execution.
    #[serde(default)]
    pub tool_releases: Vec<std::path::PathBuf>,
    /// Explicit host-owned actor bindings for the server sandbox pool.
    #[serde(default)]
    pub sandboxes: Vec<SandboxSettings>,
    #[serde(default = "concurrency")]
    pub concurrency: usize,
    #[serde(default = "lease")]
    pub lease_ms: u64,
    #[serde(default = "heartbeat")]
    pub heartbeat_ms: u64,
    #[serde(default = "drain")]
    pub drain_ms: u64,
}
fn concurrency() -> usize {
    8
}
fn lease() -> u64 {
    30_000
}
fn heartbeat() -> u64 {
    5_000
}
fn drain() -> u64 {
    30_000
}
impl CloudWorkerSettings {
    /// TOML file, overridden only by named worker environment variables.
    pub fn load(path: &Path) -> Result<Self, BuildError> {
        let mut settings =
            Self::from_values(&std::fs::read_to_string(path)?, &std::env::vars().collect())?;
        if let (Some(dir), Some(file)) = (path.parent(), &mut settings.workspace_releases_file) {
            *file = dir.join(&*file);
        }
        Ok(settings)
    }
    /// Parse and validate a TOML document with an explicit environment snapshot.
    pub fn from_values(file: &str, env: &BTreeMap<String, String>) -> Result<Self, BuildError> {
        let mut settings: Self = toml::from_str(file)?;
        if let Some(value) = env.get("MUSTERAGENT_DATABASE_URL") {
            settings.database_url = value.clone();
        }
        if let Some(value) = env.get("MUSTERAGENT_WORKER_ID") {
            settings.worker_id = value.clone();
        }
        settings.policy()?;
        if !settings.extension_state_root.is_absolute()
            || !settings.extension_workspace_root.is_absolute()
        {
            return Err("extension roots must be absolute host paths".into());
        }
        if settings.models.is_empty() {
            return Err("at least one host model endpoint is required".into());
        }
        Ok(settings)
    }
    /// Validate bounded claims, lease renewal cadence and graceful drain limits.
    pub fn policy(&self) -> Result<WorkerPolicy, BuildError> {
        let concurrency = NonZeroUsize::new(self.concurrency).ok_or("zero concurrency")?;
        let policy = musteragent_runtime_worker::WorkerRunnerPolicy {
            max_inflight_runs: concurrency,
            lease_duration: Duration::from_millis(self.lease_ms),
            heartbeat_interval: Duration::from_millis(self.heartbeat_ms),
            shutdown_drain_timeout: Duration::from_millis(self.drain_ms),
        }
        .validate()?;
        if self.concurrency > 1000 {
            return Err("scheduler batch limit exceeded".into());
        }
        Ok(WorkerPolicy {
            worker_id: musteragent_run::WorkerId::new(&self.worker_id)?,
            concurrency,
            lease: policy.lease_duration,
            heartbeat: policy.heartbeat_interval,
            drain: policy.shutdown_drain_timeout,
        })
    }
}

/// Explicit S3 endpoint and addressing policy; no vendor or ambient credentials are inferred.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Settings {
    /// Bucket's atomic create protocol, shared by the worker and reference API.
    #[serde(default)]
    pub write_once: musteragent_object_store::WriteOnceStrategy,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key_env: String,
    pub secret_key_env: String,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub virtual_hosted_style: bool,
}
impl S3Settings {
    /// Resolve the named secrets and construct the immutable content adapter.
    pub fn build(&self) -> Result<musteragent_object_store::ObjectStoreBlobStore, BuildError> {
        Ok(musteragent_object_store::ObjectStoreBlobStore::from_s3(
            musteragent_object_store::S3ObjectStoreConfig {
                write_once: self.write_once.clone(),
                endpoint: self.endpoint.clone(),
                region: self.region.clone(),
                bucket: self.bucket.clone(),
                prefix: self.prefix.clone(),
                access_key_id: std::env::var(&self.access_key_env)?,
                secret_access_key: std::env::var(&self.secret_key_env)?,
                allow_http: self.allow_http,
                virtual_hosted_style: self.virtual_hosted_style,
            },
        )?)
    }
}

/// One host-pinned model route and its physical HTTP endpoint.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSettings {
    pub provider_id: String,
    pub entry_id: String,
    pub protocol: String,
    pub base_url: String,
    pub credential_env: String,
    #[serde(default)]
    pub allow_loopback_http: bool,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Host-owned idempotent gateway response classification and result lookup.
    pub existing_invocation: Option<ExistingInvocationSettings>,
}
/// A gateway promises that a repeated key never dispatches another generation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExistingInvocationSettings {
    pub already_accepted: musteragent_model_providers::AlreadyAcceptedResponse,
    /// Same-origin GET endpoint; omission produces an explicit typed error.
    pub result_url: Option<url::Url>,
    #[serde(default)]
    pub policy: musteragent_model_providers::ExistingInvocationLookupPolicy,
}
impl ExistingInvocationSettings {
    pub fn build(
        &self,
    ) -> Result<musteragent_model_providers::ExistingInvocationRecovery, BuildError> {
        use musteragent_model_providers::*;
        let resolver = self
            .result_url
            .clone()
            .map(HttpExistingInvocationResolver::new)
            .transpose()
            .map_err(|e| format!("existing invocation resolver: {:?}", e.safe_code))?
            .map(|r| Arc::new(r) as Arc<dyn ExistingInvocationResolver>);
        Ok(ExistingInvocationRecovery {
            already_accepted: self.already_accepted.clone(),
            resolver,
            policy: self.policy.clone(),
        })
    }
}
impl ModelSettings {
    /// Resolve a supported protocol suffix and enforce the transport policy.
    pub fn endpoint(&self) -> Result<ProviderEndpoint, BuildError> {
        let (api_format, suffix) = match self.protocol.as_str() {
            "openai-chat" => (ApiFormat::OpenAiChatCompletions, "chat/completions"),
            "openai-responses" => (ApiFormat::OpenAiResponses, "responses"),
            "anthropic-messages" => (ApiFormat::AnthropicMessages, "messages"),
            _ => return Err("unsupported host model protocol".into()),
        };
        let endpoint = ProviderEndpoint {
            model_provider_id: self.provider_id.parse()?,
            model_entry_id: self.entry_id.parse()?,
            api_format,
            invocation_url: format!("{}/{}", self.base_url.trim_end_matches('/'), suffix)
                .parse()?,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            total_timeout: Duration::from_secs(120),
            anthropic_version: Some("2023-06-01".into()),
            aws_region: None,
            origin: ProviderEndpointOrigin::Direct,
        };
        endpoint.validate(self.allow_loopback_http)?;
        Ok(endpoint)
    }
}

/// A host may replace this resolver with renewable credentials through the same port.
/// The executable reads its configured secret source on each authorization attempt.
pub struct EnvironmentCredentials(BTreeMap<(ModelProviderId, ModelEntryId), String>);
impl EnvironmentCredentials {
    /// Bind each provider/entry pair once to its host-owned credential source.
    pub fn new(models: &[ModelSettings]) -> Result<Arc<Self>, BuildError> {
        let mut routes = BTreeMap::new();
        for model in models {
            if routes
                .insert(
                    (model.provider_id.parse()?, model.entry_id.parse()?),
                    model.credential_env.clone(),
                )
                .is_some()
            {
                return Err("duplicate model credential binding".into());
            }
        }
        Ok(Arc::new(Self(routes)))
    }
}
impl CredentialResolver for EnvironmentCredentials {
    fn resolve<'a>(
        &'a self,
        _: &'a musteragent_model::CredentialGrantRef,
        route: &'a musteragent_model::ModelRoute,
        _: musteragent_runtime_model::ModelInvocationRouteRole,
        _: musteragent_identity::InvocationId,
    ) -> ApplicationFuture<'a, Result<CredentialMaterial, CredentialResolverError>> {
        Box::pin(async move {
            let name = self
                .0
                .get(&(route.model_provider_id, route.model_entry_id))
                .ok_or(CredentialResolverError::BindingMismatch)?;
            let secret = std::env::var(name).map_err(|_| CredentialResolverError::Unavailable)?;
            if secret.is_empty() {
                return Err(CredentialResolverError::NotFound);
            }
            Ok(CredentialMaterial::Bearer(secret.into()))
        })
    }
}

/// Route-specific static metadata uses the same hook as signed host context.
pub struct ConfiguredModelHeaders(
    pub BTreeMap<(ModelProviderId, ModelEntryId), reqwest::header::HeaderMap>,
);
impl ModelRequestHeaderHook for ConfiguredModelHeaders {
    fn headers<'a>(
        &'a self,
        authority: &'a musteragent_runtime_model::ModelRequestAuthority,
    ) -> ApplicationFuture<
        'a,
        Result<reqwest::header::HeaderMap, musteragent_runtime_model::ProviderError>,
    > {
        Box::pin(async move {
            self.0
                .get(&(
                    authority.route.model_provider_id,
                    authority.route.model_entry_id,
                ))
                .cloned()
                .ok_or(musteragent_runtime_model::ProviderError {
                    kind: musteragent_runtime_model::ProviderErrorKind::InvalidRequest,
                    safe_code:
                        musteragent_runtime_model::ProviderErrorCode::RouteConfigurationInvalid,
                    retryable: false,
                    retry_after_ms: None,
                    diagnostic_ref: None,
                })
        })
    }
}

/// A sandbox lease is authorized for exactly one host-admitted actor.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxSettings {
    pub tenant_id: String,
    pub principal_id: String,
    pub provider_id: String,
    pub config: musteragent_sandbox_server::ServerSandboxConfig,
}
impl SandboxSettings {
    /// Validate the local image and create an actor-scoped provider; never pull or substitute images.
    pub async fn build(
        self,
        database: musteragent_postgres::PostgresDatabaseRuntime,
        content: Arc<dyn musteragent_runtime_content::ContentStore>,
    ) -> Result<musteragent_sandbox_server::ServerSandboxProvider, BuildError> {
        use musteragent_sandbox_server::*;
        let runtime = DockerRuntime::connect_local()?;
        runtime.ping().await?;
        if !runtime.image_present(&self.config.image.reference).await? {
            return Err("configured sandbox image is unavailable locally".into());
        }
        let tenant_id = self.tenant_id.parse()?;
        Ok(ServerSandboxProvider::new_registered(
            self.config,
            ProviderBinding {
                tenant_id,
                principal_id: self.principal_id.parse()?,
            },
            ServerProviderRegistration {
                provider_id: self.provider_id.parse()?,
                scope: musteragent_runtime_capability::BrowserProviderScope::Tenant(tenant_id),
            },
            Arc::new(musteragent_postgres::PostgresBrowserGatewayStore::new(
                database,
            )),
            content,
            Arc::new(runtime),
        )?)
    }
}
