//! Canonical chat request and replay fixtures for the mapping matrix.
use super::*;

pub(super) fn strict_value(format: ApiFormat, body: &serde_json::Value) -> Option<bool> {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => body["tools"][0]["function"].get("strict"),
        ApiFormat::OpenAiResponses => body["tools"][0].get("strict"),
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
            body["tools"][0].get("strict")
        }
        ApiFormat::BedrockConverse => body["toolConfig"]["tools"][0]["toolSpec"].get("strict"),
        ApiFormat::GoogleGenerateContent | ApiFormat::GoogleInteractions => None,
    }
    .and_then(serde_json::Value::as_bool)
}

pub(super) fn mapping_error(request: &CanonicalModelRequest) -> MappingError {
    match map_request(request, &ResolvedContents::default()) {
        Ok(_) => panic!("request unexpectedly mapped"),
        Err(error) => error,
    }
}

pub(super) fn assert_mapping(format: ApiFormat, body: &serde_json::Value) {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => {
            let messages = body["messages"].as_array().expect("messages");
            let assistant = messages
                .iter()
                .find(|message| message["role"] == "assistant")
                .expect("assistant");
            assert!(assistant["content"].is_null());
            let tool_messages = messages
                .iter()
                .filter(|message| message["role"] == "tool")
                .collect::<Vec<_>>();
            assert_eq!(tool_messages.len(), 2);
            assert!(
                tool_messages
                    .iter()
                    .all(|message| message.get("name").is_none())
            );
        }
        ApiFormat::OpenAiResponses => {
            let input = body["input"].as_array().expect("input");
            assert_eq!(
                input
                    .iter()
                    .filter(|item| item["type"] == "function_call")
                    .count(),
                2
            );
            let outputs = input
                .iter()
                .filter(|item| item["type"] == "function_call_output")
                .collect::<Vec<_>>();
            assert_eq!(outputs.len(), 2);
            assert_eq!(outputs[1]["output"], "(no tool output)");
        }
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => {
            let messages = body["messages"].as_array().expect("messages");
            let result_turn = messages.last().expect("tool result turn");
            assert_eq!(result_turn["role"], "user");
            let content = result_turn["content"].as_array().expect("content");
            assert_eq!(
                content
                    .iter()
                    .filter(|part| part["type"] == "tool_result")
                    .count(),
                2
            );
        }
        ApiFormat::GoogleGenerateContent => {
            let contents = body["contents"].as_array().expect("contents");
            let result_turn = contents.last().expect("tool result turn");
            let parts = result_turn["parts"].as_array().expect("parts");
            assert_eq!(parts.len(), 2);
            assert!(parts.iter().all(|part| part.get("text").is_none()));
            assert!(
                body["tools"][0]["functionDeclarations"][0]
                    .get("parametersJsonSchema")
                    .is_some()
            );
            assert!(
                body["tools"][0]["functionDeclarations"][0]
                    .get("parameters")
                    .is_none()
            );
        }
        ApiFormat::GoogleInteractions => {
            let input = body["input"].as_array().expect("input");
            let types = input
                .iter()
                .map(|step| step["type"].as_str().expect("step type"))
                .collect::<Vec<_>>();
            assert_eq!(types, vec!["function_result", "function_result"]);
            assert!(input.iter().all(|step| step.get("role").is_none()));
        }
        ApiFormat::BedrockConverse => {
            let messages = body["messages"].as_array().expect("messages");
            let result_turn = messages.last().expect("tool result turn");
            let content = result_turn["content"].as_array().expect("content");
            assert_eq!(content.len(), 2);
            assert!(content.iter().all(|part| part.get("text").is_none()));
            assert_eq!(content[1]["toolResult"]["content"][0]["text"], "<empty>");
        }
    }
}

pub(super) fn request(format: ApiFormat) -> CanonicalModelRequest {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    let model_provider_id = model_provider_id();
    let model_entry_id = model_entry_id();
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
                home_epoch: HomeEpoch::new(1).expect("home epoch"),
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
            },
            attempt_context: AttemptContext {
                attempt_id: AttemptId::generate(),
                run_id,
                attempt_number: AttemptNumber::new(1).expect("attempt number"),
                worker_id: WorkerId::new("mapping-fixture").expect("worker"),
                lease_id: LeaseId::generate(),
                fencing_token: FencingToken::new(1).expect("fence"),
                lease_expires_at: UtcTimestamp::from_unix_millis(2_000),
            },
            parent_invocation_id: None,
            causation_id: LedgerRecordId::generate(),
            correlation_id: CorrelationId::generate(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(2_000) },
        },
        route: ModelRoute {
            route_ref: versioned("model.route.mapping@1"),
            resolved_model_key: "model-fixture".into(),
            model_provider_id,
            model_entry_id,
            api_format: format,
            adapter_version: versioned("adapter.mapping@1"),
            protocol_version: versioned("protocol.mapping@1"),
            context_window: non_zero(32_768),
            max_output_tokens: non_zero(4_096),
            capabilities: BTreeSet::from([
                capability("input.text"),
                capability("input.image"),
                capability("input.document"),
            ]),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:pricing.fixture@1")
                .expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
        credential_grant_ref: CredentialGrantRef::new("grant_mapping").expect("grant"),
        continuation_ref: None,
        runtime_events: Vec::new(),
        instructions: vec![ModelText::new("You are Musteragent.").expect("instructions")],
        input: tool_exchange(),
        tools: vec![tool_definition()],
        response_schema: None,
        reasoning_policy: ReasoningPolicy::Disabled,
        cache_policy: CachePolicy::ProviderDefault,
        stream_budget:
            musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                .model_stream,
        max_output_tokens: non_zero(256),
        temperature: None,
    }
}

pub(super) fn attach_source_private_replay(
    source_format: ApiFormat,
    request: &mut CanonicalModelRequest,
    contents: &mut ResolvedContents,
) -> &'static str {
    let marker = private_marker(source_format);
    let (media_type, bytes, tool_replay) = match source_format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => {
            request
                .route
                .capabilities
                .insert(capability("reasoning_echo.allowed"));
            (
                "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
                marker.as_bytes().to_vec(),
                false,
            )
        }
        ApiFormat::OpenAiResponses => (
            "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            reasoning_replay_bytes(
                source_format,
                serde_json::json!({
                    "type": "reasoning",
                    "id": "rs_matrix",
                    "summary": [],
                    "encrypted_content": marker,
                }),
            ),
            false,
        ),
        ApiFormat::AnthropicMessages | ApiFormat::BedrockInvokeModel => (
            "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            reasoning_replay_bytes(
                source_format,
                serde_json::json!({
                    "type": "thinking",
                    "thinking": marker,
                    "signature": format!("signature-{marker}"),
                }),
            ),
            false,
        ),
        ApiFormat::BedrockConverse => (
            "text/vnd.musteragent.legacy.provider-reasoning-replay;charset=utf-8",
            reasoning_replay_bytes(
                source_format,
                serde_json::json!({
                    "reasoningContent": {
                        "reasoningText": {
                            "text": marker,
                            "signature": format!("signature-{marker}"),
                        }
                    }
                }),
            ),
            false,
        ),
        ApiFormat::GoogleGenerateContent => (
            "application/vnd.musteragent.legacy.provider-tool-replay+json",
            serde_json::to_vec(&serde_json::json!({
                "schema": "musteragent.legacy.runtime.provider-tool-replay.v2",
                "kind": "google_thought_signature",
                "source_route_ref": source_route(source_format).as_str(),
                "source_api_format": source_format.as_str(),
                "source_model_key": source_model(source_format),
                "signature": marker,
            }))
            .expect("Google replay JSON"),
            true,
        ),
        ApiFormat::GoogleInteractions => unreachable!("stateful-only format is tested separately"),
    };
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("private replay media type"),
        byte_length: u64::try_from(bytes.len()).expect("private replay length"),
        sha256: Sha256::digest(&bytes),
    };
    contents
        .insert(
            &reference,
            ResolvedContent::Inline {
                media_type: reference.media_type.clone(),
                bytes: SecretSlice::new(bytes.into_boxed_slice()),
            },
        )
        .expect("resolved private replay");
    if tool_replay {
        request.input[1].tool_calls[0].provider_replay_ref = Some(reference);
    } else {
        request.input[1].reasoning = Some(ReasoningReplay {
            content_ref: reference,
            source_route_ref: ModelRouteRef::from_key(source_route(source_format)),
            continuation_ref: None,
        });
    }
    marker
}

pub(super) fn reasoning_replay_bytes(source_format: ApiFormat, item: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema": "musteragent.legacy.runtime.provider-reasoning-replay.v2",
        "source_api_format": source_format.as_str(),
        "source_model_key": source_model(source_format),
        "items": [item],
    }))
    .expect("reasoning replay JSON")
}

pub(super) fn source_route(format: ApiFormat) -> VersionedKey {
    versioned(match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => "source.openai-chat@1",
        ApiFormat::OpenAiResponses => "source.openai-responses@1",
        ApiFormat::AnthropicMessages => "source.anthropic@1",
        ApiFormat::GoogleGenerateContent => "source.google-generate@1",
        ApiFormat::BedrockConverse => "source.bedrock-converse@1",
        ApiFormat::BedrockInvokeModel => "source.bedrock-invoke@1",
        ApiFormat::GoogleInteractions => "source.google-interactions@1",
    })
}

pub(super) fn target_route(format: ApiFormat) -> VersionedKey {
    versioned(match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => "target.openai-chat@1",
        ApiFormat::OpenAiResponses => "target.openai-responses@1",
        ApiFormat::AnthropicMessages => "target.anthropic@1",
        ApiFormat::GoogleGenerateContent => "target.google-generate@1",
        ApiFormat::BedrockConverse => "target.bedrock-converse@1",
        ApiFormat::BedrockInvokeModel => "target.bedrock-invoke@1",
        ApiFormat::GoogleInteractions => "target.google-interactions@1",
    })
}

pub(super) fn source_model(format: ApiFormat) -> &'static str {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => "source-openai-chat",
        ApiFormat::OpenAiResponses => "source-openai-responses",
        ApiFormat::AnthropicMessages => "source-anthropic",
        ApiFormat::GoogleGenerateContent => "source-google-generate",
        ApiFormat::BedrockConverse => "source-bedrock-converse",
        ApiFormat::BedrockInvokeModel => "source-bedrock-invoke",
        ApiFormat::GoogleInteractions => "source-google-interactions",
    }
}

pub(super) fn target_model(format: ApiFormat) -> &'static str {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => "target-openai-chat",
        ApiFormat::OpenAiResponses => "target-openai-responses",
        ApiFormat::AnthropicMessages => "target-anthropic",
        ApiFormat::GoogleGenerateContent => "target-google-generate",
        ApiFormat::BedrockConverse => "target-bedrock-converse",
        ApiFormat::BedrockInvokeModel => "target-bedrock-invoke",
        ApiFormat::GoogleInteractions => "target-google-interactions",
    }
}

pub(super) fn private_marker(format: ApiFormat) -> &'static str {
    match format {
        ApiFormat::TypeSafeSystemOne | ApiFormat::OpenRouterDecisions => {
            unreachable!("decision protocols do not use chat fixtures")
        }
        ApiFormat::OpenAiChatCompletions => "private-openai-chat",
        ApiFormat::OpenAiResponses => "private-openai-responses",
        ApiFormat::AnthropicMessages => "private-anthropic",
        ApiFormat::GoogleGenerateContent => "private-google-generate",
        ApiFormat::BedrockConverse => "private-bedrock-converse",
        ApiFormat::BedrockInvokeModel => "private-bedrock-invoke",
        ApiFormat::GoogleInteractions => "private-google-interactions",
    }
}

pub(super) fn interactions_continuation_request() -> CanonicalModelRequest {
    interactions_continuation(request(ApiFormat::GoogleInteractions))
}

pub(super) fn interactions_continuation(
    mut request: CanonicalModelRequest,
) -> CanonicalModelRequest {
    request.route.provider_state_policy = ProviderStatePolicy::OpaqueContinuation;
    request.continuation_ref = Some(ContinuationRef::from_opaque(
        OpaqueExternalRef::new("interaction-previous").expect("continuation"),
    ));
    request.input = request.input.split_off(2);
    request
}

pub(super) fn tool_exchange() -> Vec<CanonicalMessage> {
    let first = tool_call("call_one");
    let second = tool_call("call_two");
    vec![
        message(CanonicalRole::User, "Use both tools"),
        CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: Vec::new(),
            tool_calls: vec![first.clone(), second.clone()],
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        },
        tool_result(&first, "first result"),
        tool_result(&second, " "),
    ]
}

pub(super) fn message(role: CanonicalRole, text: &str) -> CanonicalMessage {
    CanonicalMessage {
        role,
        content: vec![CanonicalContentPart::Text(
            ModelText::new(text).expect("message text"),
        )],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    }
}

pub(super) fn delegation_completion_event() -> CanonicalRuntimeEvent {
    CanonicalRuntimeEvent::DelegationCompletion {
        delivery_id: DelegationDeliveryId::generate(),
        delegation_id: DelegationId::generate(),
        child_activity_id: ActivityId::generate(),
        child_run_id: RunId::generate(),
        report: ModelText::new("private-runtime-event-report").expect("ChildReport"),
    }
}

pub(super) fn tool_call(id: &str) -> CanonicalToolCall {
    CanonicalToolCall {
        call_id: ToolCallId::new(id).expect("call id"),
        name: WireName::new("lookup").expect("tool name"),
        arguments: JsonValue::Object(BTreeMap::from([(
            "value".into(),
            JsonValue::String(id.into()),
        )])),
        provider_item_ref: None,
        provider_replay_ref: None,
    }
}

pub(super) fn tool_result(call: &CanonicalToolCall, output: &str) -> CanonicalMessage {
    CanonicalMessage {
        role: CanonicalRole::Tool,
        content: vec![CanonicalContentPart::Text(
            ModelText::new(output).expect("tool output"),
        )],
        tool_calls: Vec::new(),
        tool_call_id: Some(call.call_id.clone()),
        tool_name: Some(call.name.clone()),
        reasoning: None,
    }
}

pub(super) fn tool_definition() -> CanonicalToolDefinition {
    let schema = JsonValue::Object(BTreeMap::from([
        ("type".into(), JsonValue::String("object".into())),
        (
            "properties".into(),
            JsonValue::Object(BTreeMap::from([(
                "value".into(),
                JsonValue::Object(BTreeMap::from([(
                    "type".into(),
                    JsonValue::String("string".into()),
                )])),
            )])),
        ),
    ]));
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(versioned("tool.lookup@1")),
        model_name: WireName::new("lookup").expect("tool name"),
        name: WireName::new("lookup").expect("tool name"),
        description: ModelText::new("Look up a value").expect("description"),
        input_schema_ref: SchemaRef {
            schema_key: versioned("schema.tool.lookup@1"),
            schema_hash: Sha256::digest(b"lookup"),
        },
        input_schema: schema,
        strict: true,
    }
}

pub(super) fn versioned(value: &str) -> VersionedKey {
    VersionedKey::new(value).expect("versioned key")
}

pub(super) fn capability(value: &str) -> DottedKey {
    DottedKey::new(value).expect("capability")
}

pub(super) fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero")
}

pub(super) fn model_provider_id() -> ModelProviderId {
    "019fa6de-38fd-7000-8000-000000000401"
        .parse()
        .expect("model provider")
}

pub(super) fn model_entry_id() -> ModelEntryId {
    "019fa6de-38fd-7000-8000-000000000402"
        .parse()
        .expect("model entry")
}
