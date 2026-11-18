use super::*;

pub(super) fn model_iteration(
    fixture: Fixture,
    attempt: AttemptContext,
    request_ref: musteragent_content_ref::ContentRef,
    iteration: u32,
) -> ModelIteration {
    let invocation_id = InvocationId::generate();
    ModelIteration {
        delivery_repair: None,
        iteration,
        invocation_id,
        assistant_message_id: MessageId::generate(),
        assistant_output_id: OutputId::generate(),
        reasoning_output_id: OutputId::generate(),
        request_ref,
        request: CanonicalModelRequest {
            invocation: InvocationContext {
                invocation_id,
                run_context: RunContext {
                    tenant_id: fixture.tenant_id,
                    conversation_id: fixture.conversation_id,
                    activity_id: fixture.activity_id,
                    activity_kind: ActivityKind::Chat,
                    run_id: fixture.run_id,
                    executor_participant_id: fixture.executor_id,
                    requested_by_participant_id: fixture.requester_id,
                    parent_run_id: None,
                    delegation_id: None,
                    execution_scope: ExecutionScope::Tenant {
                        tenant_id: fixture.tenant_id,
                    },
                    control_home: RuntimeHome::Cloud,
                    execution_home: RuntimeHome::Cloud,
                    home_epoch: HomeEpoch::new(1).expect("home epoch"),
                    runtime_snapshot_id: fixture.runtime_snapshot_id,
                },
                attempt_context: attempt,
                parent_invocation_id: None,
                causation_id: LedgerRecordId::generate(),
                correlation_id: CorrelationId::generate(),
                timing: musteragent_conformance::fixtures::model_journal::model_timing(),
            },
            route: ModelRoute {
                route_ref: VersionedKey::new("model.route.test@1").expect("route"),
                resolved_model_key: "test-model".into(),
                model_provider_id: "019fa6de-38fd-7000-8000-000000000451"
                    .parse()
                    .expect("model provider"),
                model_entry_id: "019fa6de-38fd-7000-8000-000000000452"
                    .parse()
                    .expect("model entry"),
                api_format: ApiFormat::OpenAiResponses,
                adapter_version: VersionedKey::new("adapter.test@1").expect("adapter"),
                protocol_version: VersionedKey::new("protocol.test@1").expect("protocol"),
                context_window: NonZeroU64::new(32_768).expect("context"),
                max_output_tokens: NonZeroU64::new(4_096).expect("output"),
                capabilities: BTreeSet::new(),
                parameters: BTreeMap::new(),
                fallback_route_refs: Vec::new(),
                pricing_version_ref: PricingVersionRef::new("catalog:pricing.test@1")
                    .expect("pricing"),
                provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
            },
            route_role: musteragent_runtime_model::ModelInvocationRouteRole::Chat,
            credential_grant_ref: CredentialGrantRef::new("grant_test").expect("grant"),
            continuation_ref: None,
            runtime_events: Vec::new(),
            instructions: vec![ModelText::new("You are Kernel.").expect("instructions")],
            input: vec![CanonicalMessage {
                role: CanonicalRole::User,
                content: vec![CanonicalContentPart::Text(
                    ModelText::new("Hello").expect("input"),
                )],
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            }],
            tools: Vec::new(),
            response_schema: None,
            reasoning_policy: ReasoningPolicy::Summary,
            cache_policy: CachePolicy::ProviderDefault,
            stream_budget:
                musteragent_runtime_snapshot::RunExecutionPolicySnapshot::production_defaults()
                    .model_stream,
            max_output_tokens: NonZeroU64::new(256).expect("output"),
            temperature: Some(0.2),
        },
    }
}
