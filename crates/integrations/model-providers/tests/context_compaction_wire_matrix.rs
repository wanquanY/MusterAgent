use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{SchemaRef, Sha256};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{
    ActivityId, AttemptId, AttemptNumber, ConversationId, CorrelationId, DottedKey, FencingToken,
    HomeEpoch, InvocationId, LeaseId, LedgerRecordId, ParticipantId, RunId, RuntimeSnapshotId,
    TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelParameter, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_model_providers::{MappingError, ResolvedContents, map_request};
use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest, CanonicalRole,
    CanonicalToolDefinition, ContinuationRef, JsonValue, ModelInvocationRouteRole, ModelText,
    ReasoningPolicy,
};
use musteragent_tool::{ToolKey, WireName};
use serde_json::Value;

const APPROVED_FORMATS: [ApiFormat; 7] = [
    ApiFormat::OpenAiResponses,
    ApiFormat::OpenAiChatCompletions,
    ApiFormat::AnthropicMessages,
    ApiFormat::GoogleGenerateContent,
    ApiFormat::GoogleInteractions,
    ApiFormat::BedrockConverse,
    ApiFormat::BedrockInvokeModel,
];

#[test]
fn aux_compression_route_is_admission_pinned_and_cache_isolated() {
    let request = compaction_request(ApiFormat::OpenAiResponses);
    assert_eq!(
        request.route_role,
        ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression)
    );
    assert_eq!(request.route.route_ref.as_str(), "aux.compression@1");
    assert_eq!(
        request.credential_grant_ref.as_str(),
        "grant_compression_matrix"
    );
    assert_ne!(request.route.route_ref.as_str(), "chat.primary@1");
    assert_eq!(request.cache_policy, CachePolicy::Disabled);
    assert_eq!(request.reasoning_policy, ReasoningPolicy::Disabled);
    assert!(request.continuation_ref.is_none());
    assert!(request.tools.is_empty());

    let mapped = map_request(&request, &ResolvedContents::default()).expect("map compression");
    assert_wire_contract(&mapped.body, ApiFormat::OpenAiResponses);
}

#[test]
fn context_compaction_approved_api_format_matrix() {
    for api_format in APPROVED_FORMATS {
        let mapped = map_request(
            &compaction_request(api_format),
            &ResolvedContents::default(),
        )
        .unwrap_or_else(|error| panic!("{} mapping failed: {error}", api_format.as_str()));
        assert_wire_contract(&mapped.body, api_format);
        assert!(
            find_values(&mapped.body, "temperature")
                .iter()
                .any(|value| value.as_f64() == Some(0.35)),
            "{} did not map the admitted safe sampling parameter",
            api_format.as_str()
        );
    }
}

#[test]
fn configured_large_output_capacity_reaches_every_compression_wire_format() {
    for api_format in APPROVED_FORMATS {
        let mut request = compaction_request(api_format);
        request.route.context_window = NonZeroU64::new(1_000_000).expect("valid test fixture");
        request.route.max_output_tokens = NonZeroU64::new(384_000).expect("valid test fixture");
        request.max_output_tokens = request.route.max_output_tokens;
        if matches!(
            api_format,
            ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions
        ) {
            request.route.capabilities.insert(
                musteragent_identity::DottedKey::new("usage_aggregates_requests")
                    .expect("valid test fixture"),
            );
        }
        let body = map_request(&request, &ResolvedContents::default())
            .expect("valid test fixture")
            .body;
        let values: Vec<_> = [
            "max_output_tokens",
            "max_tokens",
            "max_completion_tokens",
            "maxOutputTokens",
            "maxTokens",
        ]
        .into_iter()
        .flat_map(|key| find_values(&body, key))
        .collect();
        assert!(
            !values.is_empty(),
            "missing output budget for {}",
            api_format.as_str()
        );
        assert!(
            values.iter().all(|v| v.as_u64() == Some(384_000)),
            "output capacity changed for {}: {:?}",
            api_format.as_str(),
            values
        );
    }
}

#[test]
fn openai_responses_preserves_chunked_compaction_history_as_one_ordered_message() {
    let mut request = compaction_request(ApiFormat::OpenAiResponses);
    request.input[0].content = ["large ", "logical ", "history"]
        .into_iter()
        .map(|value| {
            CanonicalContentPart::Text(ModelText::new(value).expect("bounded history part"))
        })
        .collect();

    let body = map_request(&request, &ResolvedContents::default())
        .expect("map chunked compression history")
        .body;
    let messages = body["input"].as_array().expect("Responses input");
    assert_eq!(messages.len(), 1, "chunks must not become separate turns");
    let message = &messages[0];
    assert_eq!(message["type"], "message");
    assert_eq!(message["role"], "user");
    let reconstructed = message["content"]
        .as_array()
        .expect("Responses content")
        .iter()
        .map(|part| {
            assert_eq!(part["type"], "input_text");
            part["text"].as_str().expect("text part")
        })
        .collect::<String>();
    assert_eq!(reconstructed, "large logical history");
}

#[test]
fn context_compaction_rejects_every_reserved_parameter_alias_for_every_format() {
    let reserved = [
        "tools",
        "tool_choice",
        "tool_config",
        "reasoning",
        "reasoning_effort",
        "reasoning_budget_tokens",
        "thinking",
        "thinking_budget",
        "include",
        "previous_response_id",
        "previous_interaction_id",
        "cache",
        "cache_control",
        "cached_content",
        "prompt_cache_key",
        "prompt_cache_retention",
        "store",
        "additional_model_request_fields",
    ];
    for api_format in APPROVED_FORMATS {
        for key in reserved {
            let mut request = compaction_request(api_format);
            request.route.parameters.clear();
            request.route.parameters.insert(
                DottedKey::new(key).expect("reserved parameter key"),
                ModelParameter::Bool(true),
            );
            assert!(
                matches!(
                    map_request(&request, &ResolvedContents::default()),
                    Err(MappingError::UnsafeAuxiliaryParameter)
                ),
                "{} accepted reserved alias {key}",
                api_format.as_str()
            );
        }
    }
}

#[test]
fn compression_explicitly_disables_provider_default_thinking() {
    for (format, style, expected_key, expected_value) in [
        (
            ApiFormat::OpenAiResponses,
            "provider_default",
            "reasoning",
            serde_json::json!({"effort": "none"}),
        ),
        (
            ApiFormat::OpenAiChatCompletions,
            "openai_effort",
            "reasoning_effort",
            serde_json::json!("none"),
        ),
        (
            ApiFormat::OpenAiChatCompletions,
            "openrouter",
            "reasoning",
            serde_json::json!({"enabled": false}),
        ),
        (
            ApiFormat::OpenAiChatCompletions,
            "kimi",
            "thinking",
            serde_json::json!({"type": "disabled"}),
        ),
    ] {
        let mut request = compaction_request(format);
        request.route.capabilities.insert(
            DottedKey::new(format!("reasoning_style.{style}")).expect("valid test fixture"),
        );
        request.route.parameters.insert(
            DottedKey::new("reasoning_effort").expect("valid test fixture"),
            ModelParameter::String("none".into()),
        );
        let body = map_request(&request, &ResolvedContents::default())
            .expect("non-thinking compression")
            .body;
        assert_eq!(body[expected_key], expected_value);
        assert!(body.get("include").is_none());
        assert!(body.get("tools").is_none());
        assert!(body.get("previous_response_id").is_none());
        for effort in ["high", "enabled", "disabled"] {
            request.route.parameters.insert(
                DottedKey::new("reasoning_effort").expect("valid test fixture"),
                ModelParameter::String(effort.into()),
            );
            assert!(matches!(
                map_request(&request, &ResolvedContents::default()),
                Err(MappingError::UnsafeAuxiliaryParameter)
            ));
        }
    }
    let mut unsupported = compaction_request(ApiFormat::GoogleGenerateContent);
    unsupported.route.parameters.insert(
        DottedKey::new("reasoning_effort").expect("valid test fixture"),
        ModelParameter::String("none".into()),
    );
    assert!(matches!(
        map_request(&unsupported, &ResolvedContents::default()),
        Err(MappingError::UnsafeAuxiliaryParameter)
    ));
}

#[test]
fn context_compaction_owner_fields_fail_closed_before_wire_mapping() {
    let mut cases = Vec::new();
    let mut reasoning = compaction_request(ApiFormat::OpenAiResponses);
    reasoning.reasoning_policy = ReasoningPolicy::Summary;
    cases.push(("reasoning", reasoning));
    let mut cache = compaction_request(ApiFormat::OpenAiResponses);
    cache.cache_policy = CachePolicy::ProviderDefault;
    cases.push(("cache", cache));
    let mut provider_state = compaction_request(ApiFormat::OpenAiResponses);
    provider_state.route.provider_state_policy = ProviderStatePolicy::OpaqueContinuation;
    cases.push(("provider_state", provider_state));
    let mut continuation = compaction_request(ApiFormat::OpenAiResponses);
    continuation.route.provider_state_policy = ProviderStatePolicy::OpaqueContinuation;
    continuation.continuation_ref = Some(ContinuationRef::from_opaque(
        OpaqueExternalRef::new("previous-compression-response").expect("continuation"),
    ));
    cases.push(("continuation", continuation));
    let mut tools = compaction_request(ApiFormat::OpenAiResponses);
    tools
        .route
        .capabilities
        .insert(DottedKey::new("tools").expect("Tool capability"));
    tools.tools.push(tool_definition());
    cases.push(("tools", tools));
    let mut response_schema = compaction_request(ApiFormat::OpenAiResponses);
    response_schema
        .route
        .capabilities
        .insert(DottedKey::new("json_schema_output").expect("schema capability"));
    let definition = tool_definition();
    response_schema.response_schema = Some((
        definition.input_schema_ref.clone(),
        definition.input_schema.clone(),
    ));
    cases.push(("response_schema", response_schema));
    for (label, request) in cases {
        assert!(
            matches!(
                map_request(&request, &ResolvedContents::default()),
                Err(MappingError::UnsafeAuxiliaryRequest)
            ),
            "owner field escaped: {label}"
        );
    }
}

fn tool_definition() -> CanonicalToolDefinition {
    let input_schema = JsonValue::Object(BTreeMap::from([(
        "type".to_owned(),
        JsonValue::String("object".to_owned()),
    )]));
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(
            VersionedKey::new("fixture.compression-tool@1").expect("Tool key"),
        ),
        model_name: WireName::new("compression_tool").expect("wire name"),
        name: WireName::new("compression_tool").expect("wire name"),
        description: ModelText::new("must never reach Compression").expect("description"),
        input_schema_ref: SchemaRef {
            schema_key: VersionedKey::new("schema.compression-tool@1").expect("Schema key"),
            schema_hash: Sha256::digest(b"compression-tool-schema"),
        },
        input_schema,
        strict: true,
    }
}

fn assert_wire_contract(body: &Value, api_format: ApiFormat) {
    let mut fields = Vec::new();
    collect_fields(body, &mut fields);
    for forbidden in [
        "tools",
        "toolConfig",
        "thinking",
        "reasoning",
        "reasoning_effort",
        "include",
        "previous_response_id",
        "previous_interaction_id",
        "cache",
        "cache_control",
        "cached_content",
        "cachedContent",
        "prompt_cache_key",
        "prompt_cache_retention",
    ] {
        assert!(
            !fields.contains(&forbidden),
            "{} leaked forbidden field {forbidden}: {body}",
            api_format.as_str()
        );
    }
    for value in find_values(body, "store") {
        assert_eq!(
            value,
            &Value::Bool(false),
            "{} enabled provider persistence: {body}",
            api_format.as_str()
        );
    }
}

fn collect_fields<'a>(value: &'a Value, output: &mut Vec<&'a str>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                output.push(key);
                collect_fields(value, output);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_fields(value, output);
            }
        }
        _ => {}
    }
}

fn find_values<'a>(value: &'a Value, key: &str) -> Vec<&'a Value> {
    let mut values = Vec::new();
    match value {
        Value::Object(object) => {
            for (candidate, value) in object {
                if candidate == key {
                    values.push(value);
                }
                values.extend(find_values(value, key));
            }
        }
        Value::Array(array) => {
            for value in array {
                values.extend(find_values(value, key));
            }
        }
        _ => {}
    }
    values
}

fn compaction_request(api_format: ApiFormat) -> CanonicalModelRequest {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    CanonicalModelRequest {
        invocation: InvocationContext {
            invocation_id: InvocationId::generate(),
            run_context: RunContext {
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
                home_epoch: HomeEpoch::new(1).expect("epoch"),
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
            },
            attempt_context: AttemptContext {
                attempt_id: AttemptId::generate(),
                run_id,
                attempt_number: AttemptNumber::new(1).expect("attempt"),
                worker_id: WorkerId::new("compression-matrix").expect("worker"),
                lease_id: LeaseId::generate(),
                fencing_token: FencingToken::new(1).expect("fencing token"),
                lease_expires_at: UtcTimestamp::from_unix_millis(20_000),
            },
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(10_000) },
        },
        route: ModelRoute {
            route_ref: VersionedKey::new("aux.compression@1").expect("route"),
            resolved_model_key: "compression-model".to_owned(),
            model_provider_id: "019fa6de-38fd-7000-8000-000000000611"
                .parse()
                .expect("provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000612"
                .parse()
                .expect("entry"),
            api_format,
            adapter_version: VersionedKey::new("compression.adapter@1").expect("adapter"),
            protocol_version: VersionedKey::new("compression.protocol@1").expect("protocol"),
            context_window: NonZeroU64::new(200_000).expect("context"),
            max_output_tokens: NonZeroU64::new(8_192).expect("output"),
            capabilities: BTreeSet::new(),
            parameters: BTreeMap::from([(
                DottedKey::new("temperature").expect("parameter"),
                ModelParameter::Double(0.35),
            )]),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:compression.matrix@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression),
        credential_grant_ref: CredentialGrantRef::new("grant_compression_matrix")
            .expect("credential"),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![ModelText::new("Summarize the history.").expect("instruction")],
        input: vec![CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(
                ModelText::new("A deterministic history payload.").expect("history"),
            )],
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
        max_output_tokens: NonZeroU64::new(1_024).expect("summary output"),
        temperature: None,
    }
}
