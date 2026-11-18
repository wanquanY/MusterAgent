//! Canonical model request and event fixtures for durable loop tests.

use super::*;

pub(super) fn model_request(
    run: RunContext,
    attempt: AttemptContext,
    iteration: u32,
    active_model_route_ordinal: u32,
    continuation_ref: Option<musteragent_runtime_model::ContinuationRef>,
    consecutive_empty_responses: u32,
) -> ModelIteration {
    let invocation_id = InvocationId::generate();
    let request_bytes = b"canonical request";
    ModelIteration {
        delivery_repair: None,
        iteration,
        invocation_id,
        assistant_message_id: MessageId::generate(),
        assistant_output_id: OutputId::generate(),
        reasoning_output_id: OutputId::generate(),
        request_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: result_or_panic(MediaType::new("application/x-musteragent-model-request")),
            byte_length: request_bytes.len() as u64,
            sha256: Sha256::digest(request_bytes),
        },
        request: CanonicalModelRequest {
            invocation: InvocationContext {
                invocation_id,
                run_context: run,
                attempt_context: attempt,
                parent_invocation_id: None,
                causation_id: LedgerRecordId::generate(),
                correlation_id: CorrelationId::generate(),
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(60_000) },
            },
            route: ModelRoute {
                route_ref: versioned(if active_model_route_ordinal == 0 {
                    "model.route.fixture@1"
                } else {
                    "model.route.fallback@1"
                }),
                resolved_model_key: if active_model_route_ordinal == 0 {
                    "fixture-model"
                } else {
                    "fallback-model"
                }
                .to_owned(),
                model_provider_id: "019fa6de-38fd-7000-8000-000000000101"
                    .parse::<ModelProviderId>()
                    .expect("model provider"),
                model_entry_id: "019fa6de-38fd-7000-8000-000000000102"
                    .parse::<ModelEntryId>()
                    .expect("model entry"),
                api_format: ApiFormat::OpenAiResponses,
                adapter_version: versioned("adapter.fixture@1"),
                protocol_version: versioned("protocol.fixture@1"),
                context_window: non_zero_u64(32_768),
                max_output_tokens: non_zero_u64(4_096),
                capabilities: BTreeSet::new(),
                parameters: BTreeMap::new(),
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:pricing.fixture@1")
                    .expect("pricing"),
                provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
            },
            route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
            credential_grant_ref: result_or_panic(CredentialGrantRef::new("grant_fixture")),
            continuation_ref: continuation_ref.clone(),
            runtime_events: Vec::new(),
            instructions: if consecutive_empty_responses == 0 {
                vec![model_text("You are Musteragent.")]
            } else {
                vec![
                    model_text("You are Musteragent."),
                    model_text("Runtime empty-response recovery directive"),
                ]
            },
            input: model_input(continuation_ref),
            tools: [
                "current_time",
                "delegate_wait",
                "external_write",
                "request_approval",
                "visible_tool",
                "web_search",
            ]
            .into_iter()
            .map(tool_definition)
            .collect(),
            response_schema: None,
            reasoning_policy: ReasoningPolicy::Summary,
            cache_policy: CachePolicy::ProviderDefault,
            stream_budget: RunExecutionPolicySnapshot::production_defaults().model_stream,
            max_output_tokens: non_zero_u64(256),
            temperature: Some(0.2),
        },
    }
}

fn tool_definition(name: &str) -> CanonicalToolDefinition {
    let schema_bytes = format!("schema:{name}");
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(versioned(&format!("fixture.{name}@1"))),
        model_name: wire_name(name),
        name: wire_name(name),
        description: model_text("durable Agent Loop fixture Tool"),
        input_schema_ref: SchemaRef {
            schema_key: versioned(&format!("fixture.{name}.input@1")),
            schema_hash: Sha256::digest(schema_bytes),
        },
        input_schema: JsonValue::Object(BTreeMap::from([(
            "type".to_owned(),
            JsonValue::String("object".to_owned()),
        )])),
        strict: true,
    }
}

pub(super) fn model_input(
    continuation_ref: Option<musteragent_runtime_model::ContinuationRef>,
) -> Vec<CanonicalMessage> {
    let mut input = vec![CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![CanonicalContentPart::Text(model_text("Hello"))],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    }];
    if continuation_ref.is_some() {
        input.push(CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: vec![CanonicalContentPart::Text(model_text("Prior response"))],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        });
    }
    input
}

pub(super) fn assistant_events(
    response_ref: &str,
    text: &str,
) -> Vec<Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>> {
    vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref(response_ref),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: model_text(text),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 1,
            output_tokens: 1,
            total_tokens: Some(2),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        }),
    ]
}

pub(super) fn tool_batch_events(
    first: &ToolCallId,
    second: &ToolCallId,
) -> Vec<Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>> {
    vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("response-parallel-tools"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: first.clone(),
            name: wire_name("current_time"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id: first.clone(),
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 1,
            call_id: second.clone(),
            name: wire_name("web_search"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 1,
            call_id: second.clone(),
            arguments: JsonValue::Object(BTreeMap::new()),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 2,
            output_tokens: 2,
            total_tokens: Some(4),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        }),
    ]
}
