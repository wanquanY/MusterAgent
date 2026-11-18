use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use musteragent_activity::ActivityKind;
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_context::{
    ContextCompactionModelInvoker, ContextCompactionRequest, ContextCompactionSlotV1,
};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, ConversationSeq, CorrelationId,
    DottedKey, FencingToken, HomeEpoch, InvocationId, LeaseId, LedgerRecordId, ParticipantId,
    RunId, RuntimeSnapshotId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_model_providers::{
    ContentResolver, ContentResolverError, CredentialMaterial, CredentialResolver,
    CredentialResolverError, DirectModelProvider, ProviderAdapterConfig, ProviderEndpoint,
    ResolvedContent, StaticEndpointRegistry,
};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalToolDefinition, JsonValue, ModelProvider, ModelStreamEvent, ModelText,
    ProviderErrorKind, ReasoningPolicy,
};
use musteragent_runtime_snapshot::{PinnedModelRoute, RunExecutionPolicySnapshot};
use musteragent_tool::{ToolKey, WireName};
use secrecy::{ExposeSecret as _, SecretString};
use url::Url;

struct CanaryCredentials(SecretString);

impl CredentialResolver for CanaryCredentials {
    fn resolve<'a>(
        &'a self,
        _grant_ref: &'a CredentialGrantRef,
        _route: &'a ModelRoute,
        _route_role: musteragent_runtime_model::ModelInvocationRouteRole,
        _invocation_id: InvocationId,
    ) -> Pin<
        Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>,
    > {
        Box::pin(async {
            Ok(CredentialMaterial::Bearer(SecretString::new(
                self.0.expose_secret().to_owned().into_boxed_str(),
            )))
        })
    }
}

struct NoContent;

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

/// Explicitly ignored because it consumes a controlled real Provider account.
/// Run only with MUSTERAGENT_CANARY_{API_KEY,BASE_URL,MODEL} populated.
#[tokio::test]
#[ignore = "requires an explicitly controlled real Provider account"]
async fn text_tool_reasoning_error_cancel_and_usage_use_the_direct_rust_path() {
    let api_key = required_env("MUSTERAGENT_CANARY_API_KEY");
    let base_url = required_env("MUSTERAGENT_CANARY_BASE_URL");
    let model = required_env("MUSTERAGENT_CANARY_MODEL");
    let provider = provider(&base_url, api_key);

    let text_started = Instant::now();
    let mut text_stream = provider.stream(request(
        &model,
        "Reply with exactly: Musteragent canary OK.",
        vec![],
    ));
    let mut text_events = Vec::new();
    let mut text_ttft = None;
    while let Some(event) = text_stream.next().await {
        if text_ttft.is_none() && matches!(event, Ok(ModelStreamEvent::TextDelta { .. })) {
            text_ttft = Some(text_started.elapsed());
        }
        text_events.push(event);
    }
    let text_total = text_started.elapsed();
    assert_successful_text_and_usage(&text_events);

    let tool_events = provider
        .stream(request(
            &model,
            "Call the current_time tool exactly once. Do not answer without the tool.",
            vec![current_time_tool()],
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(
        tool_events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::ToolCallCompleted { .. }))),
        "controlled Provider did not complete the required Tool call: {tool_events:?}"
    );
    assert!(tool_events.iter().any(
        |event| matches!(event, Ok(ModelStreamEvent::Usage(usage)) if usage.total_tokens.is_some())
    ));

    let reasoning_events = provider
        .stream(request(
            &model,
            "Think carefully and then answer only 7 for 3 + 4.",
            vec![],
        ))
        .collect::<Vec<_>>()
        .await;
    assert_successful_text_and_usage(&reasoning_events);
    assert!(
        reasoning_events.iter().any(|event| {
            matches!(
                event,
                Ok(ModelStreamEvent::ReasoningDelta { .. }
                    | ModelStreamEvent::ReasoningSummaryDelta { .. })
                    | Ok(ModelStreamEvent::Usage(
                        musteragent_runtime_model::ModelUsage {
                            reasoning_tokens: 1..,
                            ..
                        }
                    ))
            )
        }),
        "controlled reasoning route emitted no reasoning content or physical reasoning usage: {reasoning_events:?}"
    );

    let invalid_model_events = provider
        .stream(request(
            "legacy-canary-model-that-does-not-exist",
            "This request must fail safely.",
            vec![],
        ))
        .collect::<Vec<_>>()
        .await;
    assert!(invalid_model_events.iter().any(|event| matches!(
        event,
        Err(error)
            if matches!(
                error.kind,
                ProviderErrorKind::InvalidRequest | ProviderErrorKind::Server
            )
    )));

    let mut cancelled = provider.stream(request(
        &model,
        "Write a very long response so this stream can be cancelled.",
        vec![],
    ));
    let first = tokio::time::timeout(Duration::from_secs(30), cancelled.next()).await;
    assert!(matches!(first, Ok(Some(Ok(_)))));
    drop(cancelled);

    eprintln!(
        "legacy real Provider canary PASS: text_ttft_ms={} text_total_ms={} tool=true reasoning=true error=true cancel=true usage=true",
        text_ttft.expect("text delta timestamp").as_millis(),
        text_total.as_millis()
    );
}

/// Real-model canary for the exact Context compaction prompt and stream
/// contract. `MUSTERAGENT_COMPACTION_CANARY_CONTEXT_WINDOW` changes the simulated
/// admitted window without modifying any persistent Model Registry state.
#[tokio::test]
#[ignore = "requires an explicitly controlled real Provider account"]
async fn context_compaction_uses_the_real_provider_with_a_configurable_window() {
    const FACT: &str = "MUSTERAGENT-REAL-COMPACTION-FACT-7F4C9A21";
    let api_key = required_env("MUSTERAGENT_CANARY_API_KEY");
    let base_url = required_env("MUSTERAGENT_CANARY_BASE_URL");
    let model = required_env("MUSTERAGENT_CANARY_MODEL");
    let context_window = optional_u64_env(
        "MUSTERAGENT_COMPACTION_CANARY_CONTEXT_WINDOW",
        8_192,
        4_096..=400_000,
    );
    let provider: Arc<dyn ModelProvider> = Arc::new(provider(&base_url, api_key));
    let invoker = ContextCompactionModelInvoker::new(provider);
    let seed = request(&model, "compression seed", Vec::new());
    let invocation = seed.invocation;
    let mut route = seed.route;
    route.route_ref = versioned("model.route.canary-compression@1");
    route.context_window = non_zero(context_window);
    route.max_output_tokens = non_zero(1_024);
    route.capabilities.clear();
    route.fallback_route_refs.clear();
    route.provider_state_policy = ProviderStatePolicy::Stateless;
    let filler_chars =
        usize::try_from(context_window.saturating_mul(2)).expect("bounded Context window");
    let filler = "durable-history ".repeat(filler_chars.div_ceil(16));
    let history = serde_json::to_vec(&serde_json::json!({
        "schema_version": "musteragent.legacy.context.compaction-input.v1",
        "entries": [
            {
                "kind": "message",
                "role": "user",
                "content": format!(
                    "The exact critical fact that must survive compaction is {FACT}. {}",
                    &filler[..filler_chars.min(filler.len())]
                )
            },
            {
                "kind": "message",
                "role": "assistant",
                "content": format!("Acknowledged exact critical fact: {FACT}")
            }
        ]
    }))
    .expect("canonical history");
    let summarized_record_ids = vec![LedgerRecordId::generate(), LedgerRecordId::generate()];
    let retained_record_ids = vec![LedgerRecordId::generate()];
    let input_budget = context_window.saturating_sub(1_024);
    let mut compaction = ContextCompactionRequest {
        capture_provider_failure: false,
        continuation: None,
        recovery_policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot::legacy_default(),
        slot: ContextCompactionSlotV1 {
            source: Default::default(),
            loop_iteration: 2,
            generation: NonZeroU32::MIN,
            active_branch_id: None,
            ledger_cutoff: ConversationSeq::new(9).expect("ledger cutoff"),
            source_seq_from: ConversationSeq::new(1).expect("source from"),
            source_seq_to: ConversationSeq::new(8).expect("source to"),
            retained_record_ids,
            previous_summary_ref: None,
            input_budget,
            input_tokens_before: input_budget.saturating_mul(9) / 10,
            summary_budget: 768,
            compaction_policy_ref: versioned("musteragent.legacy.context-compaction.canary@1"),
            plan_fingerprint: Sha256::digest([]),
        },
        invocation: invocation.clone(),
        model: PinnedModelRoute {
            route,
            credential_grant_ref: CredentialGrantRef::new("grant_canary_compression")
                .expect("compression grant"),
        },
        stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
        serialized_history: history,
        previous_summary: None,
        summarized_record_ids,
        max_output_tokens: non_zero(768),
    };
    compaction.slot.plan_fingerprint = compaction
        .expected_plan_fingerprint()
        .expect("compaction fingerprint");
    compaction.validate().expect("valid compaction request");

    let started = Instant::now();
    let output = invoker
        .invoke(&compaction, invocation, Duration::from_secs(180))
        .await
        .unwrap_or_else(|failure| panic!("real Context compaction failed: {failure:?}"));

    assert!(matches!(
        &output.checkpoint,
        musteragent_context::CompactionCheckpoint::Complete(_)
    ));
    assert!(
        output.checkpoint.text().as_str().contains(FACT),
        "real summary lost the exact critical fact: {}",
        output.checkpoint.text().as_str()
    );
    assert!(output.checkpoint.text().as_str().contains("## Goal"));
    assert!(output.usage.total_tokens.is_some());
    eprintln!(
        "legacy real Context compaction PASS: context_window={context_window} input_tokens_before={} summary_bytes={} total_ms={}",
        compaction.slot.input_tokens_before,
        output.checkpoint.text().as_str().len(),
        started.elapsed().as_millis(),
    );
}

fn provider(base_url: &str, api_key: String) -> DirectModelProvider {
    let mut invocation_url = match Url::parse(base_url) {
        Ok(value) => value,
        Err(error) => panic!("MUSTERAGENT_CANARY_BASE_URL is invalid: {error}"),
    };
    let path = invocation_url.path().trim_end_matches('/');
    invocation_url.set_path(&format!("{path}/chat/completions"));
    let model_provider_id = canary_model_provider_id();
    let model_entry_id = canary_model_entry_id();
    let endpoints = match StaticEndpointRegistry::new(
        [ProviderEndpoint {
            model_provider_id,
            model_entry_id,
            api_format: ApiFormat::OpenAiChatCompletions,
            invocation_url,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(90),
            total_timeout: Duration::from_secs(180),
            anthropic_version: None,
            aws_region: None,
            origin: musteragent_model_providers::ProviderEndpointOrigin::Direct,
        }],
        false,
    ) {
        Ok(value) => value,
        Err(error) => panic!("canary endpoint policy rejected the route: {error}"),
    };
    match DirectModelProvider::new(
        Arc::new(endpoints),
        Arc::new(CanaryCredentials(SecretString::new(
            api_key.into_boxed_str(),
        ))),
        Arc::new(NoContent),
        ProviderAdapterConfig::default(),
    ) {
        Ok(value) => value,
        Err(error) => panic!("canary Provider construction failed: {error:?}"),
    }
}

fn request(
    model: &str,
    prompt: &str,
    tools: Vec<CanonicalToolDefinition>,
) -> CanonicalModelRequest {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let attempt = AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id,
        attempt_number: AttemptNumber::new(1).expect("attempt number"),
        worker_id: WorkerId::new("canary-worker").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("fencing token"),
        lease_expires_at: UtcTimestamp::from_unix_millis(4_102_444_800_000),
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
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(4_102_444_800_000) },
        },
        route: ModelRoute {
            route_ref: versioned("model.route.canary@1"),
            resolved_model_key: model.to_owned(),
            model_provider_id: canary_model_provider_id(),
            model_entry_id: canary_model_entry_id(),
            api_format: ApiFormat::OpenAiChatCompletions,
            adapter_version: versioned("adapter.openai-chat@1"),
            protocol_version: versioned("protocol.openai-chat@1"),
            context_window: non_zero(200_000),
            max_output_tokens: non_zero(4_096),
            capabilities: BTreeSet::from([
                dotted("streaming"),
                dotted("reasoning"),
                dotted("tools"),
            ]),
            parameters: BTreeMap::new(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:pricing.canary@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
        credential_grant_ref: CredentialGrantRef::new("grant_canary").expect("grant"),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![model_text(
            "You are Musteragent. Follow the canary request exactly.",
        )],
        input: vec![CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(model_text(prompt))],
            tool_calls: vec![],
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        }],
        tools,
        response_schema: None,
        reasoning_policy: ReasoningPolicy::Summary,
        cache_policy: CachePolicy::Disabled,
        stream_budget:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_stream,
        max_output_tokens: non_zero(512),
        temperature: Some(0.0),
    }
}

fn current_time_tool() -> CanonicalToolDefinition {
    let schema_bytes = br#"{"type":"object","properties":{},"additionalProperties":false}"#;
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(versioned("system.time.current@1")),
        model_name: WireName::new("current_time").expect("wire name"),
        name: WireName::new("current_time").expect("wire name"),
        description: model_text("Return the current time."),
        input_schema_ref: SchemaRef {
            schema_key: versioned("system.time.current.input@1"),
            schema_hash: Sha256::digest(schema_bytes),
        },
        input_schema: JsonValue::Object(BTreeMap::from([
            ("additionalProperties".to_owned(), JsonValue::Bool(false)),
            ("properties".to_owned(), JsonValue::Object(BTreeMap::new())),
            ("type".to_owned(), JsonValue::String("object".to_owned())),
        ])),
        strict: true,
    }
}

fn assert_successful_text_and_usage(
    events: &[Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>],
) {
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Ok(ModelStreamEvent::TextDelta { .. }))),
        "canary emitted no text: {events:?}"
    );
    assert!(
        events.iter().any(
            |event| matches!(event, Ok(ModelStreamEvent::Usage(usage)) if usage.total_tokens.is_some())
        ),
        "canary emitted no complete usage: {events:?}"
    );
    assert!(matches!(
        events.last(),
        Some(Ok(ModelStreamEvent::ResponseCompleted { .. }))
    ));
}

fn required_env(name: &str) -> String {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => panic!("{name} must be set for the ignored real Provider canary"),
    }
}

fn optional_u64_env(name: &str, default: u64, allowed: std::ops::RangeInclusive<u64>) -> u64 {
    let value = env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map_or(default, |value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        });
    assert!(
        allowed.contains(&value),
        "{name} is outside the supported range"
    );
    value
}

fn dotted(value: &str) -> DottedKey {
    DottedKey::new(value).expect("DottedKey")
}

fn canary_model_provider_id() -> ModelProviderId {
    "019fa6de-38fd-7000-8000-000000000311"
        .parse()
        .expect("model provider")
}

fn canary_model_entry_id() -> ModelEntryId {
    "019fa6de-38fd-7000-8000-000000000312"
        .parse()
        .expect("model entry")
}

fn versioned(value: &str) -> VersionedKey {
    VersionedKey::new(value).expect("VersionedKey")
}

fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero")
}

fn model_text(value: &str) -> ModelText {
    ModelText::new(value).expect("ModelText")
}
