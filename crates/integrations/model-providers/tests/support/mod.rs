#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use musteragent_activity::ActivityKind;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, CorrelationId, FencingToken, HomeEpoch,
    InvocationId, LeaseId, LedgerRecordId, ParticipantId, RunId, RuntimeSnapshotId, TenantId,
    UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_model_providers::{
    AwsCredentialMaterial, ContentResolver, ContentResolverError, CredentialMaterial,
    CredentialResolver, CredentialResolverError, DirectModelProvider, ProviderAdapterConfig,
    ProviderEndpoint, ResolvedContent, StaticEndpointRegistry,
};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    ModelInvocationRouteRole, ModelText, ReasoningPolicy,
};
use secrecy::SecretString;
use url::Url;

pub(crate) const REQUESTED_MODEL: &str = "requested-model";
pub(crate) const RESPONSE_MODEL: &str = "provider-reported-model";
pub(crate) const FIXTURE_SECRET: &str = "cap016-fixture-secret";
pub(crate) const FIXTURE_AWS_ACCESS_KEY: &str = "AKIDCAP010FIXTURE";
pub(crate) const FIXTURE_AWS_SESSION_TOKEN: &str = "cap010-session-token";

pub(crate) struct SourceFrame {
    pub event: &'static str,
    pub data: &'static str,
}

pub(crate) struct FixtureCredentials;

impl CredentialResolver for FixtureCredentials {
    fn resolve<'a>(
        &'a self,
        _grant_ref: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        _route_role: ModelInvocationRouteRole,
        _invocation_id: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        let api_format = route.api_format;
        Box::pin(async move {
            Ok(match api_format {
                ApiFormat::OpenAiResponses
                | ApiFormat::OpenAiChatCompletions
                | ApiFormat::TypeSafeSystemOne
                | ApiFormat::OpenRouterDecisions => {
                    CredentialMaterial::Bearer(SecretString::from(FIXTURE_SECRET))
                }
                ApiFormat::AnthropicMessages
                | ApiFormat::GoogleGenerateContent
                | ApiFormat::GoogleInteractions => {
                    CredentialMaterial::ApiKey(SecretString::from(FIXTURE_SECRET))
                }
                ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => {
                    CredentialMaterial::AwsSigV4(AwsCredentialMaterial {
                        access_key_id: SecretString::from(FIXTURE_AWS_ACCESS_KEY),
                        secret_access_key: SecretString::from("cap010-fixture-secret-access-key"),
                        session_token: Some(SecretString::from(FIXTURE_AWS_SESSION_TOKEN)),
                    })
                }
            })
        })
    }
}

pub(crate) struct NoContent;

impl ContentResolver for NoContent {
    fn resolve<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content_ref: &'a musteragent_content_ref::ContentRef,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedContent, ContentResolverError>> + Send + 'a>>
    {
        Box::pin(async { Err(ContentResolverError::NotFound) })
    }
}

pub(crate) fn provider(endpoint: &str, api_format: ApiFormat) -> DirectModelProvider {
    provider_with_read_timeout(endpoint, api_format, Duration::from_secs(1))
}

pub(crate) fn provider_with_read_timeout(
    endpoint: &str,
    api_format: ApiFormat,
    read_timeout: Duration,
) -> DirectModelProvider {
    provider_with_timeouts(endpoint, api_format, read_timeout, Duration::from_secs(3))
}

pub(crate) fn provider_with_timeouts(
    endpoint: &str,
    api_format: ApiFormat,
    read_timeout: Duration,
    total_timeout: Duration,
) -> DirectModelProvider {
    let endpoint = ProviderEndpoint {
        model_provider_id: model_provider_id(),
        model_entry_id: model_entry_id(),
        api_format,
        invocation_url: Url::parse(endpoint).expect("controlled wire URL"),
        connect_timeout: Duration::from_secs(1).min(total_timeout),
        read_timeout,
        total_timeout,
        anthropic_version: (api_format == ApiFormat::AnthropicMessages)
            .then(|| "2023-06-01".to_owned()),
        aws_region: matches!(
            api_format,
            ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel
        )
        .then(|| "us-east-1".to_owned()),
        origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
    };
    let endpoints = StaticEndpointRegistry::new([endpoint], true).expect("endpoint registry");
    DirectModelProvider::new(
        Arc::new(endpoints),
        Arc::new(FixtureCredentials),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    )
    .expect("Direct Provider")
}

pub(crate) fn request(api_format: ApiFormat) -> CanonicalModelRequest {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let attempt = AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id,
        attempt_number: AttemptNumber::new(1).expect("attempt"),
        worker_id: WorkerId::new("cap016-wire-worker").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("fence"),
        lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
    };
    let run = RunContext {
        tenant_id,
        conversation_id: ConversationId::generate(),
        activity_id: ActivityId::generate(),
        activity_kind: ActivityKind::Chat,
        run_id,
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: ParticipantId::generate(),
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    };
    CanonicalModelRequest {
        invocation: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: run,
            attempt_context: attempt,
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(60_000) },
        },
        route: ModelRoute {
            route_ref: versioned("model.route.cap016@1"),
            resolved_model_key: REQUESTED_MODEL.into(),
            model_provider_id: model_provider_id(),
            model_entry_id: model_entry_id(),
            api_format,
            adapter_version: versioned("adapter.cap016@1"),
            protocol_version: versioned("protocol.cap016@1"),
            context_window: non_zero(32_768),
            max_output_tokens: non_zero(4_096),
            capabilities: BTreeSet::new(),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:pricing.cap016@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        route_role: ModelInvocationRouteRole::Chat,
        credential_grant_ref: CredentialGrantRef::new("grant_cap016").expect("grant"),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![text("You are Musteragent.")],
        input: vec![CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(text("Hello"))],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        }],
        tools: Vec::new(),
        response_schema: None,
        reasoning_policy: ReasoningPolicy::Disabled,
        cache_policy: CachePolicy::Disabled,
        stream_budget:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_stream,
        max_output_tokens: non_zero(256),
        temperature: Some(0.2),
    }
}

pub(crate) fn compression_request(api_format: ApiFormat) -> CanonicalModelRequest {
    let mut request = request(api_format);
    request.route_role = ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression);
    request.credential_grant_ref =
        CredentialGrantRef::new("grant_cap010_compression").expect("Compression grant");
    request.reasoning_policy = ReasoningPolicy::Disabled;
    request.cache_policy = CachePolicy::Disabled;
    request.continuation_ref = None;
    request.tools.clear();
    request.response_schema = None;
    request
}

pub(crate) fn wire_chunks(api_format: ApiFormat, frames: &[SourceFrame]) -> Vec<Vec<u8>> {
    match api_format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            panic!("decisions are not streams")
        }
        ApiFormat::OpenAiResponses
        | ApiFormat::OpenAiChatCompletions
        | ApiFormat::AnthropicMessages
        | ApiFormat::GoogleGenerateContent
        | ApiFormat::GoogleInteractions => frames
            .iter()
            .map(|frame| format!("event: {}\ndata: {}\n\n", frame.event, frame.data).into_bytes())
            .collect(),
        ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => frames
            .iter()
            .map(|frame| aws_event(api_format, frame))
            .collect(),
    }
}

pub(crate) fn content_type(api_format: ApiFormat) -> &'static str {
    match api_format {
        ApiFormat::BedrockConverse | ApiFormat::BedrockInvokeModel => {
            "application/vnd.amazon.eventstream"
        }
        _ => "text/event-stream",
    }
}

fn aws_event(api_format: ApiFormat, frame: &SourceFrame) -> Vec<u8> {
    let (event_name, payload) = if api_format == ApiFormat::BedrockInvokeModel {
        (
            "chunk",
            serde_json::json!({"bytes": BASE64_STANDARD.encode(frame.data.as_bytes())})
                .to_string()
                .into_bytes(),
        )
    } else {
        (frame.event, frame.data.as_bytes().to_vec())
    };
    let message = Message::new(payload)
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String(event_name.into()),
        ))
        .add_header(Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ))
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ));
    let mut output = Vec::new();
    write_message_to(&message, &mut output).expect("AWS event-stream frame");
    output
}

fn model_provider_id() -> ModelProviderId {
    "019fa6de-38fd-7000-8000-000000000401"
        .parse()
        .expect("provider ID")
}

fn model_entry_id() -> ModelEntryId {
    "019fa6de-38fd-7000-8000-000000000402"
        .parse()
        .expect("entry ID")
}

fn versioned(value: &str) -> VersionedKey {
    VersionedKey::new(value).expect("versioned key")
}

fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero")
}

fn text(value: &str) -> ModelText {
    ModelText::new(value).expect("model text")
}
