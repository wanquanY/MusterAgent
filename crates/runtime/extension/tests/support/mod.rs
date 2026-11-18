//! Deterministic ports around the real durable loop and execution supervision.
pub use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
pub use musteragent_context::{ContextPlanProfile, ContextViewProjection, ModelContextView};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
pub use musteragent_extension::*;
pub use musteragent_identity::*;
pub use musteragent_loop::*;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelKey, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
pub use musteragent_run::{AttemptContext, InvocationContext, RunContext, WorkerId};
pub use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, OpaqueExternalRef, TraceContext, TraceParent,
};
pub use musteragent_runtime_model::*;
pub use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, ToolExecutionMode};
pub use musteragent_telemetry::AttemptTelemetryContext;
pub use musteragent_tool::{ToolKey, WireName};
pub use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::{NonZeroU32, NonZeroU64},
    sync::{Arc, Mutex},
    time::Duration,
};
pub mod executor;
mod home;
mod journal;
pub use home::*;
pub use journal::*;

pub fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("valid fixture and successful tested operation")
}
pub fn text(v: impl Into<String>) -> ModelText {
    ModelText::new(v).expect("valid fixture and successful tested operation")
}
pub fn wire(v: &str) -> WireName {
    WireName::new(v).expect("valid fixture and successful tested operation")
}
pub fn key(v: &str) -> VersionedKey {
    VersionedKey::new(v).expect("valid fixture and successful tested operation")
}
pub fn json(v: serde_json::Value) -> JsonValue {
    match v {
        serde_json::Value::Null => JsonValue::Null,
        serde_json::Value::Bool(v) => JsonValue::Bool(v),
        serde_json::Value::Number(v) => v.as_i64().map(JsonValue::Integer).unwrap_or_else(|| {
            JsonValue::Number(
                v.as_f64()
                    .expect("valid fixture and successful tested operation"),
            )
        }),
        serde_json::Value::String(v) => JsonValue::String(v),
        serde_json::Value::Array(v) => JsonValue::Array(v.into_iter().map(json).collect()),
        serde_json::Value::Object(v) => {
            JsonValue::Object(v.into_iter().map(|(k, v)| (k, json(v))).collect())
        }
    }
}
pub fn message(role: CanonicalRole, s: &str) -> CanonicalMessage {
    CanonicalMessage {
        role,
        content: vec![CanonicalContentPart::Text(text(s))],
        tool_calls: vec![],
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    }
}
pub fn definition(name: &str) -> CanonicalToolDefinition {
    let schema = serde_json::json!({"type":"object"});
    let bytes = serde_json::to_vec(&schema).expect("valid fixture and successful tested operation");
    CanonicalToolDefinition {
        key: ToolKey::from_versioned_key(key(&format!("test.{name}@1"))),
        model_name: wire(name),
        name: wire(name),
        description: text("Fixture tool"),
        input_schema_ref: SchemaRef {
            schema_key: key(&format!("test.{name}.input@1")),
            schema_hash: Sha256::digest(bytes),
        },
        input_schema: json(schema),
        strict: true,
    }
}
pub fn context() -> ExtensionContext {
    let tenant_id = TenantId::generate();
    let run_id = RunId::generate();
    ExtensionContext {
        run: RunContext {
            tenant_id,
            conversation_id: ConversationId::generate(),
            activity_id: ActivityId::generate(),
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id,
            executor_participant_id: ParticipantId::generate(),
            requested_by_participant_id: ParticipantId::generate(),
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::Tenant { tenant_id },
            control_home: RuntimeHome::Cloud,
            execution_home: RuntimeHome::Cloud,
            home_epoch: HomeEpoch::new(1).expect("valid fixture and successful tested operation"),
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
        },
        attempt: AttemptContext {
            attempt_id: AttemptId::generate(),
            run_id,
            attempt_number: AttemptNumber::new(1)
                .expect("valid fixture and successful tested operation"),
            worker_id: WorkerId::new("extension-test")
                .expect("valid fixture and successful tested operation"),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(1)
                .expect("valid fixture and successful tested operation"),
            lease_expires_at: UtcTimestamp::from_unix_millis(60_000),
        },
        workspace_dir: std::env::current_dir()
            .expect("valid fixture and successful tested operation"),
        session_key: "session-test".into(),
        channel: Some("test-channel".into()),
        sandboxed: true,
        ingress: None,
    }
}
pub fn telemetry(c: &ExtensionContext) -> AttemptTelemetryContext {
    AttemptTelemetryContext {
        tenant_id: c.run.tenant_id,
        conversation_id: c.run.conversation_id,
        activity_id: c.run.activity_id,
        run_id: c.run.run_id,
        attempt_id: c.attempt.attempt_id,
        attempt_number: c.attempt.attempt_number,
        fencing_token: c.attempt.fencing_token,
        correlation_id: CorrelationId::generate(),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("valid fixture and successful tested operation"),
            tracestate: None,
        },
    }
}
pub fn checkpoint(c: &ExtensionContext) -> DurableLoopCheckpoint {
    DurableLoopCheckpoint {
        run_id: c.run.run_id,
        attempt_id: c.attempt.attempt_id,
        iteration: 0,
        active_model_route_ordinal: 0,
        phase: LoopPhase::ReadyForModel,
        completed_tool_calls: 0,
        consecutive_empty_responses: 0,
        total_tokens: 0,
        total_cost_microusd: 0,
        continuation_ref: None,
        continuation_source_invocation_id: None,
    }
}
pub struct Clock;
impl musteragent_ledger::Clock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1000)
    }
}
impl AgentLoopClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1000)
    }
}
pub struct ExecutionPolicy;
pub fn execution_snapshot() -> RunExecutionPolicySnapshot {
    let mut snapshot = RunExecutionPolicySnapshot::production_defaults();
    snapshot.liveness =
        Some(musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default());
    snapshot.default_tool_execution_mode = ToolExecutionMode::Sequential;
    snapshot
}
impl ExecutionPolicySource for ExecutionPolicy {
    fn load(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        Box::pin(async {
            Ok(PinnedRunExecutionPolicy {
                snapshot: execution_snapshot(),
                admitted_at: UtcTimestamp::from_unix_millis(1000),
                model_route_count: NonZeroU32::MIN,
            })
        })
    }
}
pub struct Controls;
impl LoopControlQueue for Controls {
    fn drain_steering(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
    fn drain_follow_up(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}
pub struct Planner {
    pub context: ExtensionContext,
    pub journal: Arc<Journal>,
    pub input: String,
    pub definitions: Vec<CanonicalToolDefinition>,
}
impl Planner {
    async fn prepare(
        &self,
        input: ContextPlanInput,
        projection: Option<Arc<dyn ContextViewProjection>>,
    ) -> Result<ModelIteration, ContextPlanError> {
        let invocation_id = InvocationId::generate();
        let mut messages = vec![message(CanonicalRole::User, &self.input)];
        for result in lock(&self.journal.results).iter() {
            let call = lock(&self.journal.calls)
                .iter()
                .find(|c| c.call_id == result.call_id)
                .cloned()
                .expect("valid fixture and successful tested operation");
            let mut assistant = message(CanonicalRole::Assistant, "Calling tool");
            assistant.tool_calls.push(CanonicalToolCall {
                call_id: call.call_id,
                name: call.name,
                arguments: call.arguments,
                provider_item_ref: None,
                provider_replay_ref: None,
            });
            messages.push(assistant);
            messages.push(result.message.clone());
        }
        let mut request = CanonicalModelRequest {
            invocation: InvocationContext {
                invocation_id,
                run_context: self.context.run,
                attempt_context: input.attempt.clone(),
                parent_invocation_id: None,
                causation_id: LedgerRecordId::generate(),
                correlation_id: CorrelationId::generate(),
                timing: input.invocation_timing,
            },
            route: route(),
            route_role: ModelInvocationRouteRole::Chat,
            credential_grant_ref: CredentialGrantRef::new("grant_fixture")
                .expect("valid fixture and successful tested operation"),
            continuation_ref: None,
            instructions: vec![text("Base system")],
            input: messages,
            runtime_events: vec![],
            tools: self.definitions.clone(),
            response_schema: None,
            reasoning_policy: ReasoningPolicy::Summary,
            cache_policy: CachePolicy::ProviderDefault,
            stream_budget: execution_snapshot().model_stream,
            max_output_tokens: NonZeroU64::new(256)
                .expect("valid fixture and successful tested operation"),
            temperature: None,
        };
        if let Some(projection) = projection {
            let view = projection
                .project(
                    &input.attempt,
                    &request,
                    ModelContextView {
                        instructions: request.instructions.clone(),
                        input: request.input.clone(),
                    },
                )
                .await?;
            request.instructions = view.instructions;
            request.input = view.input;
        }
        request
            .validate()
            .expect("valid fixture and successful tested operation");
        let bytes = format!("{request:?}").into_bytes();
        lock(&self.journal.sealed).push(request.clone());
        Ok(ModelIteration {
            delivery_repair: None,
            iteration: input.next_iteration,
            invocation_id,
            assistant_message_id: MessageId::generate(),
            assistant_output_id: OutputId::generate(),
            reasoning_output_id: OutputId::generate(),
            request_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(MODEL_REQUEST_MEDIA_TYPE)
                    .expect("valid fixture and successful tested operation"),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            },
            request,
        })
    }
}
impl ContextPlanner for Planner {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(self.prepare(input, None))
    }
    fn prepare_model_iteration_with_projection(
        &self,
        input: ContextPlanInput,
        _: Option<ContextPlanProfile>,
        projection: Arc<dyn ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(self.prepare(input, Some(projection)))
    }
}
pub fn route() -> ModelRoute {
    ModelRoute {
        route_ref: key("test.route@1"),
        resolved_model_key: "fixture-model".into(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000101"
            .parse::<ModelProviderId>()
            .expect("valid fixture and successful tested operation"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000102"
            .parse::<ModelEntryId>()
            .expect("valid fixture and successful tested operation"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: key("test.adapter@1"),
        protocol_version: key("test.protocol@1"),
        context_window: NonZeroU64::new(32768)
            .expect("valid fixture and successful tested operation"),
        max_output_tokens: NonZeroU64::new(4096)
            .expect("valid fixture and successful tested operation"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: vec![],
        pricing_version_ref: PricingVersionRef::new("catalog:test-pricing@1")
            .expect("valid fixture and successful tested operation"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    }
}
pub fn response_start() -> ModelStreamEvent {
    ModelStreamEvent::ResponseStarted {
        response_ref: ProviderRequestRef::from_opaque(
            OpaqueExternalRef::new(format!("response-{}", InvocationId::generate()))
                .expect("valid fixture and successful tested operation"),
        ),
        response_model_key: ModelKey::new("fixture-model")
            .expect("valid fixture and successful tested operation"),
    }
}
pub fn assistant() -> Vec<ModelStreamEvent> {
    vec![
        response_start(),
        ModelStreamEvent::TextDelta {
            output_index: 0,
            delta: text("Done"),
        },
        ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 3,
            output_tokens: 2,
            total_tokens: Some(5),
            ..Default::default()
        }),
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::Stop,
        },
    ]
}
pub fn tool_events(id: &str, name: &str, args: serde_json::Value) -> Vec<ModelStreamEvent> {
    vec![
        response_start(),
        ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: ToolCallId::new(id).expect("valid fixture and successful tested operation"),
            name: wire(name),
            provider_item_ref: None,
            provider_replay: None,
        },
        ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id: ToolCallId::new(id).expect("valid fixture and successful tested operation"),
            arguments: json(args),
        },
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        },
    ]
}

pub struct Provider {
    pub scripts: Mutex<VecDeque<Vec<ModelStreamEvent>>>,
    pub requests: Mutex<Vec<CanonicalModelRequest>>,
    pub annotations: Mutex<Vec<ModelRequestAnnotations>>,
}
impl Provider {
    pub fn new(scripts: Vec<Vec<ModelStreamEvent>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(vec![]),
            annotations: Mutex::new(vec![]),
        }
    }
}
impl ModelProvider for Provider {
    fn execution<'a>(
        &'a self,
        request: CanonicalModelRequest,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<Box<dyn ModelExecution<'a> + 'a>, ProviderError> {
        validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(ModelOperation {
            provider: self,
            request,
            control: Arc::new(LocalControl(binding)),
        }))
    }
    fn stream(&self, _: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("activity execution required")
    }
}
struct ModelOperation<'a> {
    provider: &'a Provider,
    request: CanonicalModelRequest,
    control: Arc<LocalControl>,
}
impl<'a> ModelExecution<'a> for ModelOperation<'a> {
    fn binding(&self) -> musteragent_runtime_contract::execution_liveness::ExecutionBinding {
        self.control.0
    }
    fn stop_control(&self) -> Arc<dyn musteragent_execution_liveness::ExecutionStopControl> {
        self.control.clone()
    }
    fn annotate(&mut self, annotations: ModelRequestAnnotations) -> Result<(), ProviderError> {
        lock(&self.provider.annotations).push(annotations);
        Ok(())
    }
    fn preflight(&mut self) -> ApplicationFuture<'_, Result<(), ProviderError>> {
        Box::pin(async { Ok(()) })
    }
    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        lock(&self.provider.requests).push(self.request);
        let events = lock(&self.provider.scripts)
            .pop_front()
            .expect("unexpected model call");
        Box::pin(futures_util::stream::iter(events.into_iter().map(Ok)))
    }
}
pub struct Fixture {
    pub context: ExtensionContext,
    pub host: Arc<ExtensionHost>,
    pub audit: Arc<dyn ExtensionJournal>,
    pub ledger: Arc<musteragent_ledger::InMemoryLedger<Clock>>,
    pub journal: Arc<Journal>,
    pub provider: Arc<Provider>,
    pub tools: Arc<executor::ToolFixture>,
    pub runtime: ExtendedAgentLoop,
    pub policy: Arc<dyn LoopPolicy>,
}
impl Fixture {
    pub fn new(
        registry: ExtensionRegistry,
        scripts: Vec<Vec<ModelStreamEvent>>,
        config: LoopDetectionConfig,
        input: &str,
    ) -> Self {
        let mut context = context();
        context.ingress = Some(CommandInput {
            id: "input-1".into(),
            text: text(input),
        });
        Self::with_ports(registry, scripts, config, input, context, None)
    }
    pub fn with_ports(
        registry: ExtensionRegistry,
        scripts: Vec<Vec<ModelStreamEvent>>,
        config: LoopDetectionConfig,
        input: &str,
        context: ExtensionContext,
        spawner: Option<Arc<dyn SubagentSpawner>>,
    ) -> Self {
        let definitions = if registry.tool_definitions().is_empty() {
            vec![definition("probe"), definition("poll"), definition("other")]
        } else {
            registry.tool_definitions()
        };
        let ledger = Arc::new(musteragent_ledger::InMemoryLedger::new(Clock));
        let audit: Arc<dyn ExtensionJournal> =
            Arc::new(LedgerExtensionJournal::new(ledger.clone(), Arc::new(Clock)));
        let state_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize()
            .expect("valid fixture and successful tested operation")
            .join(".codex-work/p5/state");
        let home = Arc::new(Home::new());
        let journal = Arc::new(Journal::new(context.clone(), home.clone(), ledger.clone()));
        let tools = Arc::new(executor::ToolFixture::new(
            context.clone(),
            home.clone(),
            &definitions,
        ));
        *lock(&journal.tools) = Some(tools.clone());
        let host = ExtensionHost::new(
            registry,
            audit.clone(),
            RuntimePorts {
                content: tools.content.clone(),
                history: journal.clone(),
                state_root,
                subagents: spawner,
            },
            config,
        )
        .expect("valid fixture and successful tested operation");
        let executor = tools.executor(host.clone());
        let policy = Arc::new(DefaultLoopPolicy::new(
            Arc::new(ExecutionPolicy),
            Arc::new(Planner {
                context: context.clone(),
                journal: journal.clone(),
                input: input.into(),
                definitions,
            }),
            executor,
            Arc::new(Controls),
        ));
        let provider = Arc::new(Provider::new(scripts));
        let runtime = host
            .compose_loop(
                context.clone(),
                ExtensionLoopPorts {
                    clock: Arc::new(Clock),
                    policy: policy.clone(),
                    model: provider.clone(),
                    journal: journal.clone(),
                    supervision: home.supervision(),
                },
            )
            .expect("valid fixture and successful tested operation");
        Self {
            context,
            host,
            audit,
            ledger,
            journal,
            provider,
            tools,
            runtime,
            policy,
        }
    }
    pub async fn run(&self) -> ExtensionRunReport {
        self.runtime
            .execute(telemetry(&self.context), InvocationSupervisor::new())
            .await
            .expect("valid fixture and successful tested operation")
    }
    pub async fn facts(&self) -> Vec<ExtensionFactRecord> {
        self.audit
            .read(&self.context)
            .await
            .expect("valid fixture and successful tested operation")
    }
}
