//! Account-scoped managed Embedding gateway, shared by native applications.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    MemoryEmbeddingBatch, MemoryEmbeddingDeadline, MemoryEmbeddingError, MemoryEmbeddingModel,
    MemoryEmbeddingProvider,
};
use secrecy::ExposeSecret as _;
use serde::{Deserialize, de::DeserializeOwned};
use url::Url;

use super::NativeModelCredentialAuthority;

/// Only the trusted native composition root selects this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEmbeddingHttpPolicy {
    /// Mobile: no desktop proxy discovery, no cleartext even on loopback.
    HttpsDirect,
    /// Desktop: OS proxy support and its explicit loopback development
    /// endpoint. Remote endpoints still require HTTPS.
    SystemProxyWithLoopbackHttp,
}

const MAX_CATALOG_BYTES: usize = 128 * 1024;
const MAX_EMBEDDING_BYTES: usize = 8 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct NativeMemoryEmbeddingProvider {
    client: reqwest::Client,
    endpoint: Url,
    catalog_endpoint: Url,
    credentials: Arc<NativeModelCredentialAuthority>,
}

impl NativeMemoryEmbeddingProvider {
    /// Does not issue HTTP or retain a credential snapshot. Each call resolves
    /// the current account credential; expiration/revocation fails closed.
    pub fn new(
        base_url: Url,
        credentials: Arc<NativeModelCredentialAuthority>,
        policy: NativeEmbeddingHttpPolicy,
    ) -> Result<Self, MemoryEmbeddingError> {
        validate_gateway(&base_url, policy)?;
        let builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(Duration::from_secs(30));
        let builder = match policy {
            NativeEmbeddingHttpPolicy::HttpsDirect => builder.https_only(true).no_proxy(),
            NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp => builder,
        };
        let client = builder
            .build()
            .map_err(|_| MemoryEmbeddingError::Unavailable)?;
        let mut endpoint = base_url;
        let mut catalog_endpoint = endpoint.clone();
        endpoint.set_path(&format!(
            "{}/embeddings",
            endpoint.path().trim_end_matches('/')
        ));
        catalog_endpoint.set_path(&format!(
            "{}/embedding-models",
            catalog_endpoint.path().trim_end_matches('/')
        ));
        Ok(Self {
            client,
            endpoint,
            catalog_endpoint,
            credentials,
        })
    }
}

fn validate_gateway(
    url: &Url,
    policy: NativeEmbeddingHttpPolicy,
) -> Result<(), MemoryEmbeddingError> {
    let loopback = match url.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    let cleartext_allowed = policy == NativeEmbeddingHttpPolicy::SystemProxyWithLoopbackHttp
        && url.scheme() == "http"
        && loopback;
    if (url.scheme() != "https" && !cleartext_allowed)
        || url.host().is_none()
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(MemoryEmbeddingError::InvalidInput);
    }
    Ok(())
}

impl MemoryEmbeddingProvider for NativeMemoryEmbeddingProvider {
    fn configured_model(
        &self,
        deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingModel, MemoryEmbeddingError>> {
        Box::pin(async move {
            let token = self
                .credentials
                .credential_ready(deadline.remaining()?)
                .await
                .map_err(|_| MemoryEmbeddingError::Unavailable)?;
            let response = self
                .client
                .get(self.catalog_endpoint.clone())
                .bearer_auth(token.expose_secret())
                .timeout(deadline.remaining()?)
                .send()
                .await
                .map_err(memory_embedding_transport_error)?;
            if !response.status().is_success() {
                return Err(memory_embedding_http_error(response.status()));
            }
            let catalog: EmbeddingCatalog =
                bounded_json(response, deadline, MAX_CATALOG_BYTES).await?;
            self.credentials
                .credential()
                .map_err(|_| MemoryEmbeddingError::Unavailable)?;
            catalog.into_configured_model()
        })
    }

    fn embed(
        &self,
        inputs: Vec<String>,
        deadline: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingBatch, MemoryEmbeddingError>> {
        Box::pin(async move {
            if inputs.is_empty()
                || inputs.len() > 32
                || inputs.iter().any(|value| value.trim().is_empty())
                || inputs
                    .iter()
                    .try_fold(0usize, |size, input| size.checked_add(input.len()))
                    .is_none_or(|size| size > MAX_INPUT_BYTES)
            {
                return Err(MemoryEmbeddingError::InvalidInput);
            }
            let token = self
                .credentials
                .credential_ready(deadline.remaining()?)
                .await
                .map_err(|_| MemoryEmbeddingError::Unavailable)?;
            let response = self
                .client
                .post(self.endpoint.clone())
                .header(
                    MemoryEmbeddingDeadline::timeout_header(),
                    deadline.remaining_millis()?.to_string(),
                )
                .bearer_auth(token.expose_secret())
                .timeout(deadline.remaining()?)
                .json(&serde_json::json!({"input": inputs}))
                .send()
                .await
                .map_err(memory_embedding_transport_error)?;
            if !response.status().is_success() {
                return Err(memory_embedding_http_error(response.status()));
            }
            let response: EmbeddingResponse =
                bounded_json(response, deadline, MAX_EMBEDDING_BYTES).await?;
            self.credentials
                .credential()
                .map_err(|_| MemoryEmbeddingError::Unavailable)?;
            response.into_batch(inputs.len())
        })
    }
}

async fn bounded_json<T: DeserializeOwned>(
    mut response: reqwest::Response,
    deadline: MemoryEmbeddingDeadline,
    limit: usize,
) -> Result<T, MemoryEmbeddingError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
        || response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_none_or(|value| !value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(MemoryEmbeddingError::InvalidResponse);
    }
    let mut body = Vec::new();
    while let Some(chunk) = deadline
        .run(async {
            response
                .chunk()
                .await
                .map_err(memory_embedding_transport_error)
        })
        .await?
    {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        body.extend_from_slice(&chunk);
    }
    deadline.remaining()?;
    let result =
        serde_json::from_slice(&body).map_err(|_| MemoryEmbeddingError::InvalidResponse)?;
    deadline.remaining()?;
    Ok(result)
}

fn memory_embedding_transport_error(error: reqwest::Error) -> MemoryEmbeddingError {
    // The owning backfiller reports first degradation and state changes. Keep
    // per-request transport diagnostics available without repeating that WARN.
    tracing::debug!(
        timeout = error.is_timeout(),
        connect = error.is_connect(),
        "Memory Embedding transport failed"
    );
    if error.is_timeout() {
        MemoryEmbeddingError::Timeout
    } else {
        MemoryEmbeddingError::Unavailable
    }
}

fn memory_embedding_http_error(status: reqwest::StatusCode) -> MemoryEmbeddingError {
    tracing::warn!(
        http_status = status.as_u16(),
        "Memory Embedding HTTP request failed"
    );
    if matches!(status.as_u16(), 408 | 504) {
        MemoryEmbeddingError::Timeout
    } else if matches!(status.as_u16(), 400 | 413 | 422) {
        MemoryEmbeddingError::InvalidInput
    } else {
        MemoryEmbeddingError::Unavailable
    }
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingItem>,
    musteragent_model_key: String,
    musteragent_model_revision: String,
}

#[derive(Deserialize)]
struct EmbeddingItem {
    index: usize,
    embedding: Vec<f32>,
}

impl EmbeddingResponse {
    fn into_batch(
        self,
        expected_inputs: usize,
    ) -> Result<MemoryEmbeddingBatch, MemoryEmbeddingError> {
        let mut data = self.data;
        data.sort_by_key(|item| item.index);
        if data.len() != expected_inputs
            || data
                .iter()
                .enumerate()
                .any(|(index, item)| item.index != index)
        {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        let vectors = data
            .into_iter()
            .map(|item| item.embedding)
            .collect::<Vec<_>>();
        let dimensions = vectors
            .first()
            .and_then(|value| u32::try_from(value.len()).ok())
            .and_then(NonZeroU32::new)
            .ok_or(MemoryEmbeddingError::InvalidResponse)?;
        if vectors.len() != expected_inputs
            || vectors.iter().any(|vector| {
                vector.len() != usize::try_from(dimensions.get()).unwrap_or(usize::MAX)
                    || vector.iter().any(|value| !value.is_finite())
            })
            || self.musteragent_model_key.trim().is_empty()
            || self.musteragent_model_revision.trim().is_empty()
        {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        Ok(MemoryEmbeddingBatch {
            model_key: self.musteragent_model_key,
            model_revision: self.musteragent_model_revision,
            dimensions,
            vectors,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmbeddingCatalog {
    ready: bool,
    degradation: Option<String>,
    default_model: Option<String>,
    models: Vec<EmbeddingCatalogModel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmbeddingCatalogModel {
    id: String,
    musteragent_model_revision: String,
    max_input_tokens: u32,
}

impl EmbeddingCatalog {
    fn into_configured_model(self) -> Result<MemoryEmbeddingModel, MemoryEmbeddingError> {
        if !self.ready {
            return if self.default_model.is_none()
                && self.degradation.as_deref() == Some("fts_only")
            {
                Err(MemoryEmbeddingError::NotConfigured)
            } else {
                Err(MemoryEmbeddingError::InvalidResponse)
            };
        }
        let model_key = self
            .default_model
            .ok_or(MemoryEmbeddingError::InvalidResponse)?;
        let mut configured = self
            .models
            .into_iter()
            .filter(|model| model.id == model_key);
        let selected = configured
            .next()
            .ok_or(MemoryEmbeddingError::InvalidResponse)?;
        if configured.next().is_some() {
            return Err(MemoryEmbeddingError::InvalidResponse);
        }
        let model = MemoryEmbeddingModel {
            model_key,
            model_revision: selected.musteragent_model_revision,
            max_input_tokens: NonZeroU32::new(selected.max_input_tokens)
                .ok_or(MemoryEmbeddingError::InvalidResponse)?,
        };
        model.validate()?;
        Ok(model)
    }
}

#[cfg(test)]
mod tests;
