use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use musteragent_model::{ApiFormat, ModelEntryId, ModelProviderId, ModelRoute};
use thiserror::Error;
use url::Url;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProviderEndpointOrigin {
    Direct,
    /// The gateway accepts authenticated model-route identity headers.
    RouteGateway,
    /// Signed device-scoped endpoint. Its request identity is already bound.
    DeviceGateway,
}
impl ProviderEndpointOrigin {
    pub fn is_gateway(self) -> bool {
        self != Self::Direct
    }
    pub fn forwards_route_identity(self) -> bool {
        self == Self::RouteGateway
    }
}

#[derive(Debug, Clone)]
pub struct ProviderEndpoint {
    pub model_provider_id: ModelProviderId,
    pub model_entry_id: ModelEntryId,
    pub api_format: ApiFormat,
    /// Exact streaming invocation URL. Model identifiers are still taken from
    /// the pinned route body, never from mutable endpoint configuration.
    pub invocation_url: Url,
    pub connect_timeout: Duration,
    /// Maximum silence between raw Provider stream reads. Canonical-event
    /// inactivity is enforced independently by the Agent Loop so transport
    /// heartbeats cannot masquerade as model progress.
    pub read_timeout: Duration,
    /// Historical field name retained by endpoint configuration. It now bounds
    /// connection plus response-header startup only, never stream lifetime.
    pub total_timeout: Duration,
    pub anthropic_version: Option<String>,
    pub aws_region: Option<String>,
    /// Attach the immutable registry entry/provider identity when the target
    /// is Musteragent's authenticated model proxy. Direct upstream endpoints keep
    /// this disabled so internal control-plane identities never leave Musteragent.
    pub origin: ProviderEndpointOrigin,
}

pub(crate) fn is_loopback_origin(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

/// Keep requests to the local machine off externally configured proxies.
pub(crate) fn apply_proxy_policy(
    builder: reqwest::ClientBuilder,
    loopback: bool,
) -> reqwest::ClientBuilder {
    if loopback {
        builder.no_proxy()
    } else {
        builder
    }
}

impl ProviderEndpoint {
    pub fn validate(&self, allow_loopback_http: bool) -> Result<(), EndpointRegistryError> {
        if self.invocation_url.username() != ""
            || self.invocation_url.password().is_some()
            || self.invocation_url.fragment().is_some()
        {
            return Err(EndpointRegistryError::UnsafeUrl);
        }
        let host = self
            .invocation_url
            .host_str()
            .ok_or(EndpointRegistryError::UnsafeUrl)?;
        let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
        if self.invocation_url.scheme() != "https"
            && !(allow_loopback_http && loopback && self.invocation_url.scheme() == "http")
        {
            return Err(EndpointRegistryError::TlsRequired);
        }
        if self.connect_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.total_timeout.is_zero()
            || self.total_timeout < self.connect_timeout
        {
            return Err(EndpointRegistryError::InvalidTimeouts);
        }
        if matches!(
            self.api_format,
            ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
        ) && self.aws_region.as_deref().is_none_or(str::is_empty)
        {
            return Err(EndpointRegistryError::AwsRegionRequired);
        }
        Ok(())
    }
}

pub type EndpointFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderEndpoint, EndpointRegistryError>> + Send + 'a>>;

/// The provider owner supplies an opaque non-secret revision of the physical
/// configuration and stored credential. Per-Run grants and temporary access
/// tokens do not identify a shared provider.
pub type VersionedEndpointFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<(ProviderEndpoint, Option<String>), EndpointRegistryError>>
            + Send
            + 'a,
    >,
>;

pub trait EndpointRegistry: Send + Sync {
    fn resolve<'a>(&'a self, route: &'a ModelRoute) -> EndpointFuture<'a>;
    fn resolve_versioned<'a>(&'a self, route: &'a ModelRoute) -> VersionedEndpointFuture<'a> {
        Box::pin(async move { self.resolve(route).await.map(|endpoint| (endpoint, None)) })
    }
}

#[derive(Debug, Clone)]
pub struct StaticEndpointRegistry {
    routes: BTreeMap<ModelEntryId, ProviderEndpoint>,
    allow_loopback_http: bool,
}

impl StaticEndpointRegistry {
    pub fn new(
        endpoints: impl IntoIterator<Item = ProviderEndpoint>,
        allow_loopback_http: bool,
    ) -> Result<Self, EndpointRegistryError> {
        let mut routes = BTreeMap::new();
        for endpoint in endpoints {
            endpoint.validate(allow_loopback_http)?;
            let key = endpoint.model_entry_id;
            if routes.insert(key, endpoint).is_some() {
                return Err(EndpointRegistryError::DuplicateRoute);
            }
        }
        Ok(Self {
            routes,
            allow_loopback_http,
        })
    }
}

impl EndpointRegistry for StaticEndpointRegistry {
    fn resolve<'a>(&'a self, route: &'a ModelRoute) -> EndpointFuture<'a> {
        Box::pin(async move {
            let endpoint = self
                .routes
                .get(&route.model_entry_id)
                .cloned()
                .ok_or(EndpointRegistryError::RouteNotFound)?;
            endpoint.validate(self.allow_loopback_http)?;
            Ok(endpoint)
        })
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum EndpointRegistryError {
    #[error("Provider Route endpoint was not found")]
    RouteNotFound,
    #[error("Provider Route endpoint is duplicated")]
    DuplicateRoute,
    #[error("Provider endpoint URL contains forbidden components")]
    UnsafeUrl,
    #[error("Provider endpoint requires HTTPS")]
    TlsRequired,
    #[error("Provider endpoint deadlines are invalid")]
    InvalidTimeouts,
    #[error("AWS Bedrock endpoint requires an explicit region")]
    AwsRegionRequired,
    #[error("Provider endpoint API version is unsupported by this adapter contract")]
    UnsupportedApiVersion,
    #[error("Provider endpoint registry is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(connect_seconds: u64, read_seconds: u64, total_seconds: u64) -> ProviderEndpoint {
        ProviderEndpoint {
            model_provider_id: ModelProviderId::from_uuid(
                uuid::Uuid::parse_str("dc88a085-1078-485b-aa17-ea34cd5d6f9a")
                    .expect("provider UUID is valid"),
            )
            .expect("provider UUID is non-nil"),
            model_entry_id: ModelEntryId::from_uuid(
                uuid::Uuid::parse_str("9710aca7-dbbe-4843-bec2-1c386154951a")
                    .expect("model entry UUID is valid"),
            )
            .expect("model entry UUID is non-nil"),
            api_format: ApiFormat::OpenAiChatCompletions,
            invocation_url: Url::parse("https://provider.example/v1/chat/completions")
                .expect("test endpoint URL is valid"),
            connect_timeout: Duration::from_secs(connect_seconds),
            read_timeout: Duration::from_secs(read_seconds),
            total_timeout: Duration::from_secs(total_seconds),
            anthropic_version: None,
            aws_region: None,
            origin: crate::ProviderEndpointOrigin::Direct,
        }
    }

    #[test]
    fn loopback_exception_never_allows_non_http_protocols() {
        for protocol in ["ftp", "ws", "gopher"] {
            let mut endpoint = endpoint(10, 60, 60);
            endpoint.invocation_url =
                Url::parse(&format!("{protocol}://127.0.0.1/invoke")).expect("URL");
            assert_eq!(
                endpoint.validate(true),
                Err(EndpointRegistryError::TlsRequired)
            );
        }
    }

    #[test]
    fn response_start_timeout_may_be_shorter_than_stream_idle_timeout() {
        endpoint(30, 600, 600)
            .validate(false)
            .expect("Admin registry defaults are a valid timeout budget");
        endpoint(30, 600, 60)
            .validate(false)
            .expect("stream lifetime is not capped by response startup");
    }

    #[test]
    fn response_start_timeout_cannot_be_shorter_than_connect_timeout() {
        assert_eq!(
            endpoint(30, 600, 29).validate(false),
            Err(EndpointRegistryError::InvalidTimeouts)
        );
    }
}
