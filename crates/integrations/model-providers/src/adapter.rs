use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_stream::stream;
use aws_smithy_eventstream::frame::{DecodedFrame, MessageFrameDecoder};
use aws_smithy_eventstream::smithy::parse_response_headers;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use bytes::BytesMut;
use eventsource_stream::{EventStreamError, Eventsource as _};
use futures_util::StreamExt as _;
use musteragent_model::ApiFormat;
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalModelRequest, ModelEventStream, ModelProvider, ModelRouteRef,
    ModelStreamEvent, ModelStreamState, ProviderError, ProviderErrorCode, ProviderErrorKind,
    ReasoningPolicy,
};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use secrecy::ExposeSecret as _;
use serde_json::Value;
use tokio::time::{Instant, timeout_at};

use crate::auth::{CredentialMaterial, CredentialResolver};
use crate::content::{ContentResolver, ResolvedContents};
use crate::endpoint::{EndpointRegistry, ProviderEndpoint};
use crate::mapping::{MappingError, map_request, preflight_request};
use crate::normalize::ProviderNormalizer;
use crate::recovery::{DirectProviderRecoveryProfile, DirectProviderWireProtocol};

#[cfg(feature = "direct-aws-signing")]
mod aws_signing;
mod credential_lease;
mod decision;
mod dependency_recovery;
mod execution;
mod existing_response;
mod google_continuation;
pub(crate) mod http_clients;
mod streaming;
use credential_lease::PreparedCredentialLease;
pub(crate) use credential_lease::credential_deadline;
#[cfg(test)]
mod aws_profile_tests;
mod failures;
mod retry_after;
use http_clients::ProviderHttpClients;

const MAX_PROVIDER_ERROR_BODY_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct ProviderAdapterConfig {
    pub connect_timeout: Duration,
    pub max_idle_per_host: usize,
    pub pool_idle_timeout: Duration,
    pub user_agent: String,
}

impl Default for ProviderAdapterConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            max_idle_per_host: 32,
            pool_idle_timeout: Duration::from_secs(90),
            user_agent: "musteragent-runtime/0.1".into(),
        }
    }
}

pub struct DirectModelProvider {
    per_request_authorization: bool,
    host_headers: crate::HostRequestHeaders,
    existing_invocations: BTreeMap<
        (
            musteragent_model::ModelProviderId,
            musteragent_model::ModelEntryId,
        ),
        crate::ExistingInvocationRecovery,
    >,
    clients: ProviderHttpClients,
    endpoints: Arc<dyn EndpointRegistry>,
    credentials: Arc<dyn CredentialResolver>,
    contents: Arc<dyn ContentResolver>,
    first_party_transport: Option<Arc<dyn crate::FirstPartyRequestTransport>>,
    dependency_recovery: Option<
        Arc<dyn musteragent_runtime_model::dependency_recovery::ModelDependencyRecoveryStore>,
    >,
    prepared: Mutex<BTreeMap<musteragent_identity::InvocationId, PreparedDirectRequest>>,
}

struct PreparedDirectRequest {
    provider_revision: Option<String>,
    recovery_headers: reqwest::header::HeaderMap,
    credential_lease: Option<PreparedCredentialLease>,
    client: reqwest::Client,
    endpoint: ProviderEndpoint,
    outbound: crate::PreparedModelHttpRequest,
    timing: crate::response_timing::ResponseTiming,
    wire_credit: Option<crate::transport_memory::TransportCredit<'static>>,
}

impl DirectModelProvider {
    pub fn new(
        endpoints: Arc<dyn EndpointRegistry>,
        credentials: Arc<dyn CredentialResolver>,
        contents: Arc<dyn ContentResolver>,
        config: ProviderAdapterConfig,
    ) -> Result<Self, ProviderError> {
        if config.connect_timeout.is_zero()
            || config.pool_idle_timeout.is_zero()
            || config.max_idle_per_host == 0
        {
            return Err(provider_error(
                ProviderErrorKind::InvalidRequest,
                ProviderErrorCode::RouteConfigurationInvalid,
                false,
                None,
            ));
        }
        let clients = ProviderHttpClients::new(config)?;
        Ok(Self {
            per_request_authorization: false,
            host_headers: Default::default(),
            existing_invocations: BTreeMap::new(),
            clients,
            endpoints,
            credentials,
            contents,
            first_party_transport: None,
            dependency_recovery: None,
            prepared: Mutex::new(BTreeMap::new()),
        })
    }

    /// Installs host metadata for all native chat, Responses and Messages calls.
    #[must_use]
    pub fn with_host_headers(mut self, headers: crate::HostRequestHeaders) -> Self {
        self.host_headers = headers;
        self
    }

    /// Re-sign native HTTP metadata when a scoped bearer renews. Disable automatic
    /// HTTP retries and redirects so each exchange has its own authorization.
    #[must_use]
    pub fn with_per_request_authorization(mut self) -> Self {
        self.per_request_authorization = true;
        self
    }

    /// Configure accepted-response classification and result lookup per pinned route.
    /// This does not infer gateway behavior from status codes or provider names.
    pub fn with_existing_invocation_recovery(
        mut self,
        routes: BTreeMap<
            (
                musteragent_model::ModelProviderId,
                musteragent_model::ModelEntryId,
            ),
            crate::ExistingInvocationRecovery,
        >,
    ) -> Result<Self, ProviderError> {
        for recovery in routes.values() {
            recovery.validate()?;
        }
        self.existing_invocations = routes;
        Ok(self)
    }

    #[must_use]
    pub fn with_first_party_transport(
        mut self,
        transport: Arc<dyn crate::FirstPartyRequestTransport>,
    ) -> Self {
        self.first_party_transport = Some(transport);
        self
    }

    async fn prepare_local(
        &self,
        request: &CanonicalModelRequest,
    ) -> Result<PreparedDirectRequest, ProviderError> {
        // This legacy port has no bound activity observer or stop owner. The
        // supervised port must supply both before activity dispatch is allowed.
        require_legacy_timing(request)?;
        self.prepare_request(request).await
    }

    async fn prepare_request(
        &self,
        request: &CanonicalModelRequest,
    ) -> Result<PreparedDirectRequest, ProviderError> {
        self.prepare_annotated_request(request, &Default::default())
            .await
    }

    async fn prepare_annotated_request(
        &self,
        request: &CanonicalModelRequest,
        annotations: &musteragent_runtime_model::ModelRequestAnnotations,
    ) -> Result<PreparedDirectRequest, ProviderError> {
        let timing = crate::response_timing::ResponseTiming::new(request);
        preflight_request(request).map_err(request_mapping_error)?;
        let (mut endpoint, provider_revision) = self
            .endpoints
            .resolve_versioned(&request.route)
            .await
            .map_err(|_| {
                provider_error(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorCode::RouteConfigurationInvalid,
                    false,
                    None,
                )
            })?;
        if endpoint.model_provider_id != request.route.model_provider_id
            || endpoint.model_entry_id != request.route.model_entry_id
            || endpoint.api_format != request.route.api_format
        {
            return Err(provider_error(
                ProviderErrorKind::Authorization,
                ProviderErrorCode::RouteConfigurationInvalid,
                false,
                None,
            ));
        }
        if request
            .route
            .capabilities
            .iter()
            .any(|flag| flag.as_str() == "long_running_generation")
        {
            endpoint.read_timeout = endpoint.read_timeout.max(Duration::from_secs(1200));
            endpoint.total_timeout = endpoint.total_timeout.max(Duration::from_secs(1200));
        }
        let contents = resolve_contents(self.contents.as_ref(), request).await?;
        let mapped = map_request(request, &contents).map_err(request_mapping_error)?;
        let (body_capacity, wire_credit) =
            if endpoint.origin.forwards_route_identity() && self.first_party_transport.is_some() {
                let (bytes, credit) = crate::transport_memory::reserve(&mapped.body)?;
                (bytes, credit)
            } else {
                (0, None)
            };
        let mut body = crate::NativeWireBody::serialize_with_capacity(&mapped.body, body_capacity)
            .map_err(|_| protocol_error())?;
        if body_capacity != 0 && wire_credit.is_none() {
            body.decline_private_framing();
        }
        tracing::debug!(
            invocation_id = %request.invocation.invocation_id,
            provider.request.encoded_bytes = body.as_bytes().len(),
            provider.request.layout_blocks = body.blocks().len(),
            provider.request.layout_version = crate::WIRE_BLOCK_LAYOUT_VERSION,
            "native Provider request serialized"
        );
        let client = self.clients.for_endpoint(
            endpoint.connect_timeout,
            self.per_request_authorization
                || self.existing_invocations.contains_key(&(
                    request.route.model_provider_id,
                    request.route.model_entry_id,
                )),
            &endpoint.invocation_url,
        )?;
        // Resolve scoped credentials only after potentially large content
        // reads and request mapping; those bytes carry no execution permission.
        let credentials = self
            .credentials
            .resolve(
                &request.credential_grant_ref,
                &request.route,
                request.route_role,
                request.invocation.invocation_id,
            )
            .await
            .map_err(failures::credential_resolution)?;
        let provider_revision = match &credentials {
            CredentialMaterial::VersionedApiKey { revision, .. } => Some(revision.clone()),
            _ => provider_revision,
        };
        let expiry = credential_deadline(&credentials)?;
        let mut outbound = build_request(
            &client,
            &endpoint,
            credentials,
            body.shared_bytes(),
            mapped.accept,
            mapped.content_type,
        )?;
        if endpoint.origin.forwards_route_identity()
            && matches!(
                request.route.api_format,
                ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
            )
        {
            outbound.headers_mut().insert(
                "x-musteragent-upstream-model",
                HeaderValue::from_str(&request.route.resolved_model_key)
                    .map_err(|_| protocol_error())?,
            );
        }
        // Invocation identity is persisted before HTTP dispatch. The native
        // default survives process/Attempt changes. Preserve P5's explicit host
        // annotation contract: a host-supplied key must identify this same
        // invocation across retries (it is never generated from a worker lease).
        let default_key = format!("model-{}", request.invocation.invocation_id);
        let idempotency_key = annotations
            .headers()
            .get("idempotency-key")
            .map(String::as_str)
            .unwrap_or(&default_key);
        if idempotency_key.is_empty() {
            return Err(protocol_error());
        }
        outbound.headers_mut().insert(
            "idempotency-key",
            HeaderValue::from_str(idempotency_key).map_err(|_| protocol_error())?,
        );
        let authority = musteragent_runtime_model::ModelRequestAuthority::from(request);
        let dynamic = self
            .host_headers
            .apply(&authority, outbound.headers_mut())
            .await?;
        let credential_lease = expiry.map(|deadline| {
            PreparedCredentialLease::new(deadline, authority, dynamic.keys().cloned().collect())
        });
        for (name, value) in annotations.headers() {
            // Selected exactly once above, before applying arbitrary metadata.
            if name == "idempotency-key" {
                continue;
            }
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| protocol_error())?;
            // Native authentication and signing have precedence. Never replace
            // a header installed by the transport, even via a custom name.
            if outbound.headers().contains_key(&name) {
                return Err(protocol_error());
            }
            outbound.headers_mut().insert(
                name,
                HeaderValue::from_str(value).map_err(|_| protocol_error())?,
            );
        }
        let recovery_headers = outbound.headers().clone();
        let outbound = if endpoint.origin.forwards_route_identity() {
            if let Some(transport) = &self.first_party_transport {
                transport
                    .prepare(
                        &musteragent_runtime_model::ModelRequestAuthority::from(request),
                        &endpoint,
                        body,
                        outbound,
                    )
                    .await?
            } else {
                crate::PreparedModelHttpRequest::Native(Box::new(outbound))
            }
        } else {
            // Third-party/BYOK endpoints never enter the private transport,
            // even when a Runtime has a first-party codec installed.
            crate::PreparedModelHttpRequest::Native(Box::new(outbound))
        };
        let client = if matches!(&outbound, crate::PreparedModelHttpRequest::FirstParty(_)) {
            self.clients
                .for_endpoint(endpoint.connect_timeout, true, &endpoint.invocation_url)?
        } else {
            client
        };
        timing.prepared();
        Ok(PreparedDirectRequest {
            provider_revision,
            recovery_headers,
            credential_lease,
            timing,
            wire_credit,
            client,
            endpoint,
            outbound,
        })
    }

    async fn preflight_local(&self, request: &CanonicalModelRequest) -> Result<(), ProviderError> {
        let prepared = self.prepare_local(request).await?;
        let mut entries = self
            .prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() >= 4_096 && !entries.contains_key(&request.invocation.invocation_id) {
            return Err(provider_error(
                ProviderErrorKind::Quota,
                ProviderErrorCode::QuotaExceeded,
                true,
                None,
            ));
        }
        entries.insert(request.invocation.invocation_id, prepared);
        Ok(())
    }
}

fn require_legacy_timing(request: &CanonicalModelRequest) -> Result<(), ProviderError> {
    if request.invocation.timing.legacy_deadline().is_none() {
        return Err(provider_error(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::RouteConfigurationInvalid,
            false,
            None,
        ));
    }
    Ok(())
}

impl ModelProvider for DirectModelProvider {
    fn supports_invocation_recovery(&self) -> bool {
        !self.existing_invocations.is_empty()
    }

    fn recover_execution<'provider>(
        &'provider self,
        request: CanonicalModelRequest,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<
        Box<dyn musteragent_runtime_model::ModelExecution<'provider> + 'provider>,
        ProviderError,
    > {
        if self.recovery_capability(&request)
            != musteragent_runtime_model::ProviderRecoveryCapability::IdempotentRequest
        {
            return Err(protocol_error());
        }
        Ok(Box::new(execution::DirectModelExecution::recovered(
            self, request, binding,
        )?))
    }

    fn execution<'provider>(
        &'provider self,
        request: CanonicalModelRequest,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<
        Box<dyn musteragent_runtime_model::ModelExecution<'provider> + 'provider>,
        ProviderError,
    > {
        Ok(Box::new(execution::DirectModelExecution::new(
            self, request, binding,
        )?))
    }

    fn recovery_capability(
        &self,
        request: &CanonicalModelRequest,
    ) -> musteragent_runtime_model::ProviderRecoveryCapability {
        if self.existing_invocations.contains_key(&(
            request.route.model_provider_id,
            request.route.model_entry_id,
        )) {
            musteragent_runtime_model::ProviderRecoveryCapability::IdempotentRequest
        } else {
            crate::DirectProviderRecoveryProfile::for_api_format(request.route.api_format).recovery
        }
    }

    fn preflight<'a>(
        &'a self,
        request: &'a CanonicalModelRequest,
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<(), ProviderError>> {
        Box::pin(async move { self.preflight_local(request).await })
    }

    fn discard_preflight(&self, invocation_id: musteragent_identity::InvocationId) {
        self.prepared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&invocation_id);
    }

    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        Box::pin(stream! {
            if let Err(error) = require_legacy_timing(&request) {
                self.discard_preflight(request.invocation.invocation_id);
                yield Err(error);
                return;
            }
            let prepared = self
                .prepared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&request.invocation.invocation_id);
            let prepared = match prepared {
                Some(prepared) => prepared,
                None => match self.prepare_local(&request).await {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                },
            };
            let mut stream = self.stream_prepared(request, prepared, None);
            while let Some(event) = stream.next().await {
                yield event;
            }
        })
    }
}

fn provider_event_subtype(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    object
        .get("delta")
        .and_then(Value::as_object)
        .and_then(|delta| delta.get("type"))
        .or_else(|| {
            object
                .get("content_block")
                .and_then(Value::as_object)
                .and_then(|block| block.get("type"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn finish_error(error: crate::NormalizeError) -> ProviderError {
    match error {
        crate::NormalizeError::ClosedBeforeStart | crate::NormalizeError::ClosedBeforeTerminal => {
            tracing::warn!(
                provider.failure = "stream_eof_before_terminal",
                "Model stream ended without a terminal marker"
            );
            transport_interruption()
        }
        _ => protocol_error(),
    }
}

fn transport_interruption() -> ProviderError {
    provider_error(
        ProviderErrorKind::Transport,
        ProviderErrorCode::TransportFailed,
        true,
        None,
    )
}

fn request_mapping_error(error: MappingError) -> ProviderError {
    let safe_code = match error {
        MappingError::UnsupportedReasoningPolicy
        | MappingError::UnsafeAuxiliaryParameter
        | MappingError::InvalidRouteConfiguration => ProviderErrorCode::RouteConfigurationInvalid,
        MappingError::InvalidCanonicalRequest
        | MappingError::UnauthorizedInputModality
        | MappingError::UnsupportedInputModality
        | MappingError::JsonEncoding
        | MappingError::InlineContentRequired
        | MappingError::InvalidUtf8
        | MappingError::InvalidProviderReplay
        | MappingError::InvalidReasoningReplay
        | MappingError::UnsupportedJsonSchemaOutput
        | MappingError::ExactProviderReplayRequired
        | MappingError::UnsupportedMedia
        | MappingError::UnsafeAuxiliaryRequest
        | MappingError::Content(_) => ProviderErrorCode::InvalidRequest,
    };
    provider_error(ProviderErrorKind::InvalidRequest, safe_code, false, None)
}

async fn resolve_contents(
    resolver: &dyn ContentResolver,
    request: &CanonicalModelRequest,
) -> Result<ResolvedContents, ProviderError> {
    let mut references = Vec::new();
    for message in &request.input {
        for part in &message.content {
            match part {
                CanonicalContentPart::Text(_) => {}
                CanonicalContentPart::Image(reference)
                | CanonicalContentPart::Audio(reference)
                | CanonicalContentPart::Video(reference)
                | CanonicalContentPart::File(reference) => references.push(reference),
            }
        }
        if provider_reasoning_replay_is_consumed(request, message)
            && let Some(reasoning) = &message.reasoning
            && reasoning.source_route_ref.as_str() == request.route.route_ref.as_str()
        {
            references.push(&reasoning.content_ref);
        }
        if request.route.api_format == musteragent_model::ApiFormat::GoogleGenerateContent {
            for call in &message.tool_calls {
                if let Some(reference) = &call.provider_replay_ref {
                    references.push(reference);
                }
            }
        }
    }
    for part in request
        .runtime_events
        .iter()
        .flat_map(|event| event.dialogue_inputs())
        .flat_map(|input| &input.parts)
    {
        if let CanonicalContentPart::Image(reference)
        | CanonicalContentPart::Audio(reference)
        | CanonicalContentPart::Video(reference)
        | CanonicalContentPart::File(reference) = part
        {
            references.push(reference);
        }
    }
    references.sort_by_key(|reference| reference.content_id);
    if references
        .windows(2)
        .any(|pair| pair[0].content_id == pair[1].content_id && pair[0] != pair[1])
    {
        return Err(provider_error(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::InvalidRequest,
            false,
            None,
        ));
    }
    references.dedup_by_key(|reference| reference.content_id);
    let mut contents = ResolvedContents::default();
    let tenant_id = request.invocation.run_context.tenant_id;
    let resolved = resolver
        .resolve_batch(tenant_id, &references)
        .await
        .map_err(failures::content_resolution)?;
    if resolved.len() != references.len() {
        return Err(failures::content_resolution(
            crate::ContentResolverError::Unauthorized,
        ));
    }
    for (reference, resolved) in references.into_iter().zip(resolved) {
        contents
            .insert(reference, resolved)
            .map_err(failures::content_resolution)?;
    }
    Ok(contents)
}

fn provider_reasoning_replay_is_consumed(
    request: &CanonicalModelRequest,
    message: &musteragent_runtime_model::CanonicalMessage,
) -> bool {
    consumes_provider_reasoning_replay(
        request.route.api_format,
        message.role == musteragent_runtime_model::CanonicalRole::Assistant,
        !message.tool_calls.is_empty(),
        message.reasoning.is_some(),
        request.route.capabilities.iter().any(|capability| {
            matches!(
                capability.as_str(),
                "reasoning_echo.allowed" | "reasoning_echo.required"
            )
        }),
    )
}

/// Whether the mapper will read this message's stored reasoning, so the
/// adapter must resolve its content first. OpenAI Chat echoes
/// `reasoning_content` on every assistant message with reasoning or tool
/// calls, including a reasoning-only message before an output-limit
/// continuation; it must be loaded for both.
const fn consumes_provider_reasoning_replay(
    api_format: ApiFormat,
    assistant: bool,
    has_tool_calls: bool,
    has_reasoning: bool,
    chat_reasoning_echo: bool,
) -> bool {
    if !assistant {
        return false;
    }
    match api_format {
        ApiFormat::OpenAiResponses
        | ApiFormat::AnthropicMessages
        | ApiFormat::BedrockConverse
        | ApiFormat::BedrockInvokeModel
        | ApiFormat::GoogleGenerateContent
        | ApiFormat::GoogleInteractions => true,
        ApiFormat::OpenAiChatCompletions => {
            chat_reasoning_echo && (has_tool_calls || has_reasoning)
        }
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => false,
    }
}

fn build_request(
    client: &reqwest::Client,
    endpoint: &ProviderEndpoint,
    credentials: CredentialMaterial,
    body: bytes::Bytes,
    accept: &str,
    content_type: &str,
) -> Result<reqwest::Request, ProviderError> {
    let mut request = client
        .post(endpoint.invocation_url.clone())
        .header(ACCEPT, accept)
        .header(CONTENT_TYPE, content_type);
    if let Some(version) = &endpoint.anthropic_version {
        request = request.header("anthropic-version", version);
    }
    if endpoint.origin.forwards_route_identity() {
        request = request
            .header(
                "x-musteragent-model-entry-id",
                endpoint.model_entry_id.to_string(),
            )
            .header(
                "x-musteragent-model-provider-id",
                endpoint.model_provider_id.to_string(),
            );
    }
    request = match credentials {
        CredentialMaterial::AwsSigV4(credentials) => {
            #[cfg(feature = "direct-aws-signing")]
            {
                let signed_headers =
                    aws_signing::sign_aws(endpoint, &credentials, &body, accept, content_type)?;
                let mut signed = request;
                for (name, value) in signed_headers {
                    signed = signed.header(name, value);
                }
                signed
            }
            #[cfg(not(feature = "direct-aws-signing"))]
            {
                drop(credentials);
                return Err(provider_error(
                    ProviderErrorKind::Authorization,
                    ProviderErrorCode::RouteConfigurationInvalid,
                    false,
                    None,
                ));
            }
        }
        token => authorize_token(request, endpoint.api_format, token)?,
    };
    request.body(body).build().map_err(|_| {
        provider_error(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::InvalidRequest,
            false,
            None,
        )
    })
}

/// Method-independent token authentication shared by inference and result lookup.
pub(crate) fn authorize_token(
    request: reqwest::RequestBuilder,
    api_format: ApiFormat,
    credentials: CredentialMaterial,
) -> Result<reqwest::RequestBuilder, ProviderError> {
    Ok(match credentials {
        CredentialMaterial::Bearer(secret) | CredentialMaterial::ScopedBearer { secret, .. } => {
            request.bearer_auth(secret.expose_secret())
        }
        CredentialMaterial::ApiKey(secret) | CredentialMaterial::VersionedApiKey { secret, .. } => {
            match api_format {
                ApiFormat::AnthropicMessages => request.header("x-api-key", secret.expose_secret()),
                ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions => {
                    request.header("x-goog-api-key", secret.expose_secret())
                }
                ApiFormat::OpenAiChatCompletions
                | ApiFormat::OpenAiResponses
                | ApiFormat::TypeSafeSystemOne
                | ApiFormat::OpenRouterDecisions => request.bearer_auth(secret.expose_secret()),
                ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => {
                    request.bearer_auth(secret.expose_secret())
                }
            }
        }
        CredentialMaterial::AwsSigV4(_) => {
            return Err(provider_error(
                ProviderErrorKind::Authorization,
                ProviderErrorCode::RouteConfigurationInvalid,
                false,
                None,
            ));
        }
    })
}

fn decode_aws_message(
    message: &aws_smithy_types::event_stream::Message,
    api_format: ApiFormat,
    max_event_bytes: usize,
) -> Result<(String, Value), ()> {
    let headers = parse_response_headers(message).map_err(|_| ())?;
    let event_name = headers.smithy_type.as_str().to_owned();
    if message.payload().len() > max_event_bytes {
        return Err(());
    }
    let mut value: Value = serde_json::from_slice(message.payload()).map_err(|_| ())?;
    if api_format == ApiFormat::BedrockInvokeModel && event_name == "chunk" {
        let encoded = value.get("bytes").and_then(Value::as_str).ok_or(())?;
        let decoded = BASE64_STANDARD.decode(encoded).map_err(|_| ())?;
        if decoded.len() > max_event_bytes {
            return Err(());
        }
        value = serde_json::from_slice(&decoded).map_err(|_| ())?;
        let nested_name = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("chunk")
            .to_owned();
        return Ok((nested_name, value));
    }
    Ok((event_name, value))
}

async fn http_status_error(
    format: ApiFormat,
    first_party: bool,
    response: reqwest::Response,
    max_body_bytes: usize,
) -> ProviderError {
    let status = response.status();
    let retry_after_ms = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| retry_after::delay_ms(value, std::time::SystemTime::now()));
    // Error-body detail is optional. A server that sends headers then stalls
    // must not prevent the known HTTP failure from reaching durable recovery.
    let details = tokio::time::timeout(
        Duration::from_secs(2),
        provider_error_details(response, max_body_bytes, format),
    )
    .await
    .ok()
    .flatten();
    tracing::warn!(
        provider.failure = "http_status",
        provider.http_status = u64::from(status.as_u16()),
        "Model Provider returned a non-success response"
    );
    let (metadata, overflow) = details.unwrap_or_default();
    if overflow {
        return provider_error(
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::ContextWindowExceeded,
            false,
            None,
        );
    }
    let (kind, code, retryable) = metadata
        .classify(first_party, Some(status.as_u16()))
        .unwrap_or((
            ProviderErrorKind::Protocol,
            ProviderErrorCode::ProtocolViolation,
            false,
        ));
    let delay = if retryable || (status.as_u16() == 429 && code == ProviderErrorCode::QuotaExceeded)
    {
        retry_after_ms.max(metadata.retry_after_ms)
    } else {
        None
    };
    provider_error(kind, code, retryable, delay)
}

async fn provider_error_details(
    response: reqwest::Response,
    max_body_bytes: usize,
    format: ApiFormat,
) -> Option<(crate::failure_metadata::FailureMetadata, bool)> {
    let status = response.status().as_u16();
    if max_body_bytes == 0 {
        return None;
    }
    let mut bytes = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if bytes.len().saturating_add(chunk.len()) > max_body_bytes {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let overflow = crate::normalize::overflow::classify_http_body(format, status, &value).is_some();
    Some((
        crate::failure_metadata::FailureMetadata::from_body(&value),
        overflow,
    ))
}

fn timeout_error() -> ProviderError {
    provider_error(
        ProviderErrorKind::Timeout,
        ProviderErrorCode::DeadlineExceeded,
        true,
        None,
    )
}

pub(crate) fn protocol_error() -> ProviderError {
    provider_error(
        ProviderErrorKind::Protocol,
        ProviderErrorCode::ProtocolViolation,
        false,
        None,
    )
}

const fn provider_error(
    kind: ProviderErrorKind,
    safe_code: ProviderErrorCode,
    retryable: bool,
    retry_after_ms: Option<u64>,
) -> ProviderError {
    ProviderError {
        kind,
        safe_code,
        retryable,
        retry_after_ms,
        diagnostic_ref: None,
    }
}

fn canonical_error_kind(
    error: musteragent_runtime_model::ModelStreamStateError,
) -> musteragent_telemetry::ProviderProtocolError {
    use musteragent_runtime_model::ModelStreamStateError as Error;
    use musteragent_telemetry::ProviderProtocolError as Kind;
    match error {
        Error::DuplicateStart => Kind::DuplicateStart,
        Error::OutputBeforeStart => Kind::OutputBeforeStart,
        Error::EventAfterTerminal => Kind::EventAfterTerminal,
        Error::DuplicateToolCall => Kind::DuplicateToolCall,
        Error::UnknownOrCompletedToolCall => Kind::UnknownOrCompletedToolCall,
        Error::ToolArgumentsNotObject => Kind::ToolArgumentsNotObject,
        Error::IncompleteToolCall => Kind::IncompleteToolCall,
        Error::InvalidUsage => Kind::InvalidUsage,
        Error::UsageRegressed => Kind::UsageRegressed,
        Error::InvalidProviderReplay => Kind::InvalidProviderReplay,
        Error::RuntimeDerivedEventFromProvider => Kind::RuntimeDerivedEventFromProvider,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use musteragent_model::ApiFormat;
    use secrecy::SecretString;
    use url::Url;

    use crate::{CredentialMaterial, ProviderEndpoint};

    use super::{build_request, consumes_provider_reasoning_replay};

    #[test]
    fn content_resolution_covers_every_protocol_that_consumes_private_reasoning() {
        for format in [
            ApiFormat::OpenAiResponses,
            ApiFormat::AnthropicMessages,
            ApiFormat::BedrockConverse,
            ApiFormat::BedrockInvokeModel,
            ApiFormat::GoogleGenerateContent,
            ApiFormat::GoogleInteractions,
        ] {
            assert!(consumes_provider_reasoning_replay(
                format, true, false, true, false
            ));
        }
        let chat = ApiFormat::OpenAiChatCompletions;
        assert!(consumes_provider_reasoning_replay(
            chat, true, true, true, true
        ));
        // A reasoning-only assistant message (output-limit continuation) is
        // echoed too, so its stored reasoning must be resolved.
        assert!(consumes_provider_reasoning_replay(
            chat, true, false, true, true
        ));
        assert!(!consumes_provider_reasoning_replay(
            chat, true, false, false, true
        ));
        assert!(!consumes_provider_reasoning_replay(
            chat, true, true, true, false
        ));
        assert!(!consumes_provider_reasoning_replay(
            ApiFormat::OpenAiResponses,
            false,
            false,
            true,
            false
        ));
    }

    #[test]
    fn musteragent_proxy_request_forwards_only_the_exact_signed_registry_identity() {
        let provider_id = "019fa6de-38fd-7000-8000-000000000751"
            .parse()
            .expect("provider");
        let entry_id = "019fa6de-38fd-7000-8000-000000000752"
            .parse()
            .expect("entry");
        let mut endpoint = ProviderEndpoint {
            model_provider_id: provider_id,
            model_entry_id: entry_id,
            api_format: ApiFormat::OpenAiResponses,
            invocation_url: Url::parse("http://127.0.0.1:8011/api/v1/llm-proxy/v1/responses")
                .expect("proxy URL"),
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(3),
            anthropic_version: None,
            aws_region: None,
            origin: crate::ProviderEndpointOrigin::RouteGateway,
        };
        let request = build_request(
            &reqwest::Client::new(),
            &endpoint,
            CredentialMaterial::Bearer(SecretString::from("runtime-token")),
            bytes::Bytes::from_static(br#"{"model":"provider-model"}"#),
            "text/event-stream",
            "application/json",
        )
        .expect("proxy request");

        assert_eq!(
            request.headers()["x-musteragent-model-entry-id"],
            entry_id.to_string()
        );
        assert_eq!(
            request.headers()["x-musteragent-model-provider-id"],
            provider_id.to_string()
        );

        endpoint.origin = crate::ProviderEndpointOrigin::Direct;
        let direct = build_request(
            &reqwest::Client::new(),
            &endpoint,
            CredentialMaterial::Bearer(SecretString::from("provider-token")),
            bytes::Bytes::from_static(br#"{"model":"provider-model"}"#),
            "text/event-stream",
            "application/json",
        )
        .expect("direct request");
        assert!(
            !direct
                .headers()
                .contains_key("x-musteragent-model-entry-id")
        );
        assert!(
            !direct
                .headers()
                .contains_key("x-musteragent-model-provider-id")
        );
    }

    #[cfg(feature = "direct-aws-signing")]
    #[test]
    fn bedrock_sigv4_scope_signs_the_resolved_authority_path_and_region() {
        let endpoint = ProviderEndpoint {
            model_provider_id: "019fa6de-38fd-7000-8000-000000000741"
                .parse()
                .expect("provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000742"
                .parse()
                .expect("entry"),
            api_format: ApiFormat::BedrockConverse,
            invocation_url: Url::parse(
                "https://bedrock-authority.example/model/anthropic.snapshot/converse-stream",
            )
            .expect("URL"),
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(3),
            anthropic_version: None,
            aws_region: Some("us-west-2".to_owned()),
            origin: crate::ProviderEndpointOrigin::Direct,
        };
        let request = build_request(
            &reqwest::Client::new(),
            &endpoint,
            CredentialMaterial::AwsSigV4(crate::AwsCredentialMaterial {
                access_key_id: SecretString::from("AKIDEXAMPLE"),
                secret_access_key: SecretString::from("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),
                session_token: None,
            }),
            bytes::Bytes::from_static(br#"{"inputText":"controlled"}"#),
            "application/vnd.amazon.eventstream",
            "application/json",
        )
        .expect("signed Bedrock request");

        assert_eq!(request.url().host_str(), Some("bedrock-authority.example"));
        assert_eq!(
            request.url().path(),
            "/model/anthropic.snapshot/converse-stream"
        );
        let authorization = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .expect("SigV4 Authorization");
        assert!(authorization.contains("/us-west-2/bedrock/aws4_request"));
        assert!(authorization.contains("SignedHeaders=accept;content-type;host;x-amz-date"));
    }
}
