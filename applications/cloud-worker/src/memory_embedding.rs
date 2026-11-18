//! Optional host-configured semantic retrieval through an OpenAI embeddings API.
use musteragent_model_providers::CredentialMaterial;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_memory::{
    MemoryEmbeddingBatch, MemoryEmbeddingDeadline, MemoryEmbeddingError, MemoryEmbeddingModel,
    MemoryEmbeddingProvider,
};
use secrecy::ExposeSecret as _;
use serde::Deserialize;
use std::{num::NonZeroU32, time::Duration};

/// Host route configuration; credentials are resolved afresh for every call.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingSettings {
    pub endpoint: url::Url,
    pub model_key: String,
    pub model_revision: String,
    pub upstream_model: String,
    pub credential_env: String,
    pub max_input_tokens: NonZeroU32,
    #[serde(default)]
    pub allow_loopback_http: bool,
}
struct EmbeddingRoute {
    endpoint: url::Url,
    model_key: String,
    model_revision: String,
    upstream_model: String,
    credential: CredentialMaterial,
    total_timeout: Duration,
    max_input_tokens: NonZeroU32,
}
/// Explicit physical provider for the kernel MemoryEmbeddingProvider port.
pub struct HttpMemoryEmbeddingProvider {
    settings: EmbeddingSettings,
    client: reqwest::Client,
}
impl HttpMemoryEmbeddingProvider {
    /// Validate host route identity and transport security before any network use.
    pub fn new(settings: EmbeddingSettings) -> Result<Self, crate::BuildError> {
        let local = settings.endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !settings.endpoint.username().is_empty()
            || settings.endpoint.password().is_some()
            || settings.endpoint.fragment().is_some()
            || (settings.endpoint.scheme() != "https"
                && !(settings.allow_loopback_http && local && settings.endpoint.scheme() == "http"))
        {
            return Err("embedding endpoint requires HTTPS or explicit loopback HTTP".into());
        }
        MemoryEmbeddingModel {
            model_key: settings.model_key.clone(),
            model_revision: settings.model_revision.clone(),
            max_input_tokens: settings.max_input_tokens,
        }
        .validate()
        .map_err(|e| format!("embedding model: {e:?}"))?;
        Ok(Self {
            settings,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
}
impl MemoryEmbeddingProvider for HttpMemoryEmbeddingProvider {
    fn configured_model(
        &self,
        _: MemoryEmbeddingDeadline,
    ) -> ApplicationFuture<'_, Result<MemoryEmbeddingModel, MemoryEmbeddingError>> {
        Box::pin(async move {
            Ok(MemoryEmbeddingModel {
                model_key: self.settings.model_key.clone(),
                model_revision: self.settings.model_revision.clone(),
                max_input_tokens: self.settings.max_input_tokens,
            })
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
                || inputs.iter().any(|input| input.trim().is_empty())
            {
                return Err(MemoryEmbeddingError::InvalidInput);
            }
            let secret = std::env::var(&self.settings.credential_env)
                .map_err(|_| MemoryEmbeddingError::Unavailable)?;
            if secret.is_empty() {
                return Err(MemoryEmbeddingError::Unavailable);
            }
            let route = EmbeddingRoute {
                endpoint: self.settings.endpoint.clone(),
                model_key: self.settings.model_key.clone(),
                model_revision: self.settings.model_revision.clone(),
                upstream_model: self.settings.upstream_model.clone(),
                credential: CredentialMaterial::Bearer(secret.into()),
                total_timeout: Duration::from_secs(120),
                max_input_tokens: self.settings.max_input_tokens,
            };
            embed_route(&self.client, route, inputs, deadline).await
        })
    }
}
async fn embed_route(
    client: &reqwest::Client,
    route: EmbeddingRoute,
    inputs: Vec<String>,
    deadline: MemoryEmbeddingDeadline,
) -> Result<MemoryEmbeddingBatch, MemoryEmbeddingError> {
    if inputs
        .iter()
        .any(|v| v.len() > route.max_input_tokens.get() as usize * 16)
    {
        return Err(MemoryEmbeddingError::InvalidInput);
    }
    let deadline = deadline.capped(route.total_timeout);
    let mut request = client
        .post(route.endpoint)
        .timeout(deadline.remaining()?)
        .json(&serde_json::json!({
            "model": route.upstream_model,
            "input": inputs,
        }));
    request = match route.credential {
        CredentialMaterial::Bearer(secret)
        | CredentialMaterial::ApiKey(secret)
        | CredentialMaterial::VersionedApiKey { secret, .. } => {
            request.bearer_auth(secret.expose_secret())
        }
        CredentialMaterial::AwsSigV4(_) | CredentialMaterial::ScopedBearer { .. } => {
            return Err(MemoryEmbeddingError::Unavailable);
        }
    };
    let response = request
        .send()
        .await
        .map_err(memory_embedding_transport_error)?;
    if !response.status().is_success() {
        return Err(memory_embedding_http_error(response.status()));
    }
    let response: EmbeddingResponse = response.json().await.map_err(|error| {
        if error.is_timeout() {
            MemoryEmbeddingError::Timeout
        } else {
            MemoryEmbeddingError::InvalidResponse
        }
    })?;
    let mut data = response.data;
    data.sort_by_key(|item| item.index);
    if data.iter().enumerate().any(|(i, item)| i != item.index) {
        return Err(MemoryEmbeddingError::InvalidResponse);
    }
    let vectors = data
        .into_iter()
        .map(|item| item.embedding)
        .collect::<Vec<_>>();
    let dimensions = vectors
        .first()
        .and_then(|vector| u32::try_from(vector.len()).ok())
        .and_then(NonZeroU32::new)
        .ok_or(MemoryEmbeddingError::InvalidResponse)?;
    if vectors.len() != inputs.len()
        || vectors.iter().any(|vector| {
            vector.len() != usize::try_from(dimensions.get()).unwrap_or(usize::MAX)
                || vector.iter().any(|value| !value.is_finite())
        })
    {
        return Err(MemoryEmbeddingError::InvalidResponse);
    }
    Ok(MemoryEmbeddingBatch {
        model_key: route.model_key,
        model_revision: route.model_revision,
        dimensions,
        vectors,
    })
}

fn memory_embedding_transport_error(error: reqwest::Error) -> MemoryEmbeddingError {
    tracing::warn!(
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
}

#[derive(Deserialize)]
struct EmbeddingItem {
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn delayed_route() -> (EmbeddingRoute, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test provider");
        let endpoint = format!(
            "http://{}/embeddings",
            listener.local_addr().expect("test provider address")
        )
        .parse()
        .expect("test provider URL");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept embedding request");
            let mut buffer = [0; 4096];
            let _ = stream
                .read(&mut buffer)
                .await
                .expect("read embedding request");
            tokio::time::sleep(Duration::from_secs(2)).await;
            let body = r#"{"data":[{"index":0,"embedding":[0.1,0.2]}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (
            EmbeddingRoute {
                model_key: "embedding".to_owned(),
                model_revision: "revision".to_owned(),
                upstream_model: "embedding".to_owned(),
                endpoint,
                credential: CredentialMaterial::Bearer(secrecy::SecretString::from("test-token")),
                total_timeout: Duration::from_secs(600),
                max_input_tokens: NonZeroU32::new(4096).expect("nonzero test input ceiling"),
            },
            server,
        )
    }

    #[tokio::test]
    async fn cloud_embedding_accepts_response_beyond_the_old_network_caps() {
        let (route, server) = delayed_route().await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client");
        let deadline = MemoryEmbeddingDeadline::after(Duration::from_secs(5));
        let result = deadline
            .run(embed_route(
                &client,
                route,
                vec!["query".to_owned()],
                deadline,
            ))
            .await;
        server.abort();
        assert_eq!(
            result.expect("successful delayed embedding").vectors,
            vec![vec![0.1, 0.2]]
        );
    }

    #[tokio::test]
    async fn cloud_embedding_timeout_remains_distinct_from_provider_failure() {
        let (route, server) = delayed_route().await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client");
        let result = embed_route(
            &client,
            route,
            vec!["query".to_owned()],
            MemoryEmbeddingDeadline::after(Duration::from_millis(50)),
        )
        .await;
        server.abort();
        assert_eq!(result, Err(MemoryEmbeddingError::Timeout));
        assert_eq!(
            memory_embedding_http_error(reqwest::StatusCode::GATEWAY_TIMEOUT),
            MemoryEmbeddingError::Timeout
        );
        assert_eq!(
            memory_embedding_http_error(reqwest::StatusCode::BAD_GATEWAY),
            MemoryEmbeddingError::Unavailable
        );
    }
}
