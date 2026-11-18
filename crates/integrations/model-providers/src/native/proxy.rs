use std::time::Duration;

use musteragent_execution_grant::{
    DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX, DEVICE_DIRECT_ROUTE_REF_PREFIX,
    DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX,
};
use musteragent_model::{ApiFormat, ModelRoute};
use musteragent_runtime_snapshot::DEFAULT_MODEL_INACTIVITY_TIMEOUT;
use url::Url;

use crate::{EndpointFuture, EndpointRegistry, EndpointRegistryError, ProviderEndpoint};

/// First-party model gateway routing. Native direct-provider routes are never
/// silently reinterpreted as first-party routes. No platform SDK is used here.
pub struct NativeProxyEndpointRegistry {
    base_url: Url,
    allow_loopback_http: bool,
}

impl NativeProxyEndpointRegistry {
    pub fn new(base_url: Url, allow_loopback_http: bool) -> Self {
        Self {
            base_url,
            allow_loopback_http,
        }
    }
}

pub(super) fn require_proxy_route(route: &ModelRoute) -> Result<(), EndpointRegistryError> {
    if [
        DEVICE_DIRECT_ROUTE_REF_PREFIX,
        DEVICE_DIRECT_COMPRESSION_ROUTE_REF_PREFIX,
        DEVICE_DIRECT_VISION_ROUTE_REF_PREFIX,
    ]
    .iter()
    .any(|prefix| route.route_ref.as_str().starts_with(prefix))
    {
        return Err(EndpointRegistryError::RouteNotFound);
    }
    Ok(())
}

impl EndpointRegistry for NativeProxyEndpointRegistry {
    fn resolve<'a>(&'a self, route: &'a ModelRoute) -> EndpointFuture<'a> {
        Box::pin(async move {
            require_proxy_route(route)?;
            if self.base_url.query().is_some() {
                return Err(EndpointRegistryError::UnsafeUrl);
            }
            let suffix = match route.api_format {
                ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => "decisions",
                ApiFormat::OpenAiResponses => "responses",
                ApiFormat::OpenAiChatCompletions => "chat/completions",
                ApiFormat::AnthropicMessages => "messages",
                ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => "bedrock/invoke",
                ApiFormat::GoogleGenerateContent => "google/generate",
                ApiFormat::GoogleInteractions => "google/interactions",
            };
            let mut invocation_url = self.base_url.clone();
            invocation_url.set_path(&format!(
                "{}/{suffix}",
                self.base_url.path().trim_end_matches('/')
            ));
            let endpoint = ProviderEndpoint {
                model_provider_id: route.model_provider_id,
                model_entry_id: route.model_entry_id,
                api_format: route.api_format,
                invocation_url,
                connect_timeout: Duration::from_secs(10),
                read_timeout: DEFAULT_MODEL_INACTIVITY_TIMEOUT,
                total_timeout: DEFAULT_MODEL_INACTIVITY_TIMEOUT,
                anthropic_version: (route.api_format == ApiFormat::AnthropicMessages)
                    .then(|| "2023-06-01".to_owned()),
                aws_region: matches!(
                    route.api_format,
                    ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
                )
                .then(|| "proxy".to_owned()),
                origin: crate::ProviderEndpointOrigin::RouteGateway,
            };
            endpoint.validate(self.allow_loopback_http)?;
            Ok(endpoint)
        })
    }
}
