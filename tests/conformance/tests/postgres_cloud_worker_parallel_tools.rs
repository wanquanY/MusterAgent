use musteragent_conformance::fixtures::agent::{
    FIXTURE_RUNTIME_POLICY_V1, fixture_runtime_policy_content_ref,
};
#[path = "postgres_cloud_worker_parallel_tools/dispatch_failures.rs"]
mod dispatch_failures;
use std::collections::{BTreeMap, BTreeSet};
use std::future::pending;
use std::num::{NonZeroU32, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;
#[path = "postgres_cloud_worker_parallel_tools/grant_material.rs"]
mod grant_material;
#[path = "support/live_runtime.rs"]
mod live_runtime;
#[path = "support/owned_scripted_model.rs"]
mod owned_model;
#[path = "support/parallel_tool_events.rs"]
mod parallel_tool_events;
#[path = "support/production_context.rs"]
mod production_context;
#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "postgres_cloud_worker_parallel_tools/support.rs"]
mod scenario_support;
#[path = "support/tool_material.rs"]
mod tool_material;
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_cloud_worker::{
    CapabilityDispatcher, CloudWorkerRunner, DispatchedCapabilityOutcome,
    DispatchedReconciliationOutcome, DurableToolExecutor, WorkerRunnerPolicy,
};
use musteragent_conformance::MemoryBlobs;
use musteragent_conformance::fixtures::agent::{
    FIXTURE_PERSONA_V1 as FIXTURE_PERSONA_V3, fixture_version_v1 as fixture_version_v8,
};
use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_context::ContextPlanner;
use musteragent_identity::{
    CapabilityProviderId, DottedKey, InvocationId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::AttemptFence;
use musteragent_loop::{
    AgentLoopExit, DefaultLoopPolicy, DurableAgentLoop, ExecutionPolicySource,
    InvocationSupervisor, LoopControlQueue, LoopPortError, PinnedRunExecutionPolicy,
    PlannedToolCall, ToolExecutionOutcome, ToolExecutionPolicyDecision, ToolExecutor,
};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_policy::{ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy};
use musteragent_postgres::{
    PostgresAgentLoopJournal, PostgresContentStore, PostgresDispatchStore,
    PostgresFencedToolInvocationJournal, migrate,
};
use musteragent_run::{AttemptContext, WorkerId};
use musteragent_runtime_capability::{
    CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation, CapabilityResult,
    DurableToolInvocation, FencedToolInvocationJournal, ToolExecutionMaterial,
    ToolExecutionMaterialSource,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, InterruptionReason, OpaqueExternalRef,
};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalModelRequest, CanonicalRole, JsonValue, ModelEventStream,
    ModelProvider, ModelStreamEvent, ModelText, ModelUsage, ProviderRequestRef, StopReason,
};
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, ContextPolicySnapshot, OutputPolicySnapshot, PolicySnapshot,
    PromptAssemblySnapshot, PromptSectionSnapshot, ProtocolVersionSnapshot,
    RunExecutionPolicySnapshot, RuntimeSnapshotContent, SNAPSHOT_SCHEMA_VERSION,
    ToolDefinitionSnapshot, ToolExecutionMode,
};
use musteragent_scheduler::{
    AttemptControlError, AttemptControlStore, ClaimOutcome, DispatchStore, LeaseHeartbeat,
};
use musteragent_skill::{SkillAuthorizationRequest, Surface, builtin_skill_catalog};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, ToolKey, ToolManifest,
    ToolRetryContract, WireName,
};
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSchedulerEventEncoder};
use production_context::install_production_context;
use running_loop::{
    Fixture, attempt_telemetry, database_now, execute_loop, execution_receipt, keyring, run_context,
};
use scenario_support::{FixedClock, FixedTrace};
use tokio::sync::{Barrier, Notify, mpsc};
use tokio_stream::{self as stream, Stream};
use tokio_util::sync::CancellationToken;
use tool_material::{fixture_model_route as model_route, production_tool_material_source};

// These scenarios each own a PostgreSQL container and several timing-controlled
// Worker tasks. Run scenarios serially so host scheduling cannot alter their
// controlled completion order; tool calls inside every scenario remain truly
// parallel and are the concurrency contract under test.
static POSTGRES_PARALLEL_TOOLS_SUITE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ConcurrentProvider {
    content: Arc<dyn ContentStore>,
    active: AtomicUsize,
    peak: AtomicUsize,
    parallel_entry: Barrier,
}

impl CapabilityDispatcher for ConcurrentProvider {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        Ok(live_runtime::invoke_operation(
            binding,
            self.invoke(attempt, material, durable, invocation),
        ))
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            // Keep all expected physical calls open until each has entered;
            // elapsed time does not establish overlap across database I/O.
            tokio::time::timeout(Duration::from_secs(2), self.parallel_entry.wait())
                .await
                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: format!(
                        r#"{{"call_id":"{}","ok":true}}"#,
                        invocation.tool_call_id.as_str()
                    )
                    .into_bytes(),
                })
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::RunTransitionInvalid, false)) })
    }
}

struct BarrierProvider {
    content: Arc<dyn ContentStore>,
    active: AtomicUsize,
    peak: AtomicUsize,
    events: Mutex<Vec<String>>,
    parallel_entry: Barrier,
    first_pair_completed: Notify,
    second_pair_completed: Notify,
}

impl BarrierProvider {
    fn record(&self, event: String) {
        lock(&self.events).push(event);
    }
}

impl CapabilityDispatcher for BarrierProvider {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        Ok(live_runtime::invoke_operation(
            binding,
            self.invoke(attempt, material, durable, invocation),
        ))
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            let call_id = invocation.tool_call_id.as_str().to_owned();
            self.record(format!("start:{call_id}"));
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            if call_id != "write-barrier" {
                tokio::time::timeout(Duration::from_secs(2), self.parallel_entry.wait())
                    .await
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
            }
            // This provider controls physical completion order. Wall-clock
            // sleeps cannot establish that order across independent I/O.
            let predecessor = match call_id.as_str() {
                "parallel-a" => Some(&self.first_pair_completed),
                "parallel-c" => Some(&self.second_pair_completed),
                "parallel-b" | "parallel-d" | "write-barrier" => None,
                _ => return Err(port(ErrorCode::InternalInvariantViolated, false)),
            };
            if let Some(predecessor) = predecessor {
                tokio::time::timeout(Duration::from_secs(2), predecessor.notified())
                    .await
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
            }
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                    bytes: format!(r#"{{"call_id":"{call_id}","ok":true}}"#).into_bytes(),
                })
                .await
                .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.record(format!("end:{call_id}"));
            match call_id.as_str() {
                "parallel-b" => self.first_pair_completed.notify_one(),
                "parallel-d" => self.second_pair_completed.notify_one(),
                _ => {}
            }
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::RunTransitionInvalid, false)) })
    }
}

struct FixedExecutionPolicy {
    admitted_at: UtcTimestamp,
}

impl ExecutionPolicySource for FixedExecutionPolicy {
    fn load(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let admitted_at = self.admitted_at;
        Box::pin(async move {
            Ok(PinnedRunExecutionPolicy {
                snapshot: live_runtime::execution_policy(),
                admitted_at,
                model_route_count: NonZeroU32::new(1).expect("route count"),
            })
        })
    }

    fn tool_execution_policy(
        &self,
        _attempt: &AttemptContext,
        wire_name: &WireName,
        _default_mode: ToolExecutionMode,
    ) -> ApplicationFuture<'_, Result<ToolExecutionPolicyDecision, LoopPortError>> {
        let mode = match wire_name.as_str() {
            "write_barrier" | "external_write" => ToolExecutionMode::Sequential,
            _ => ToolExecutionMode::Parallel,
        };
        Box::pin(async move { Ok(ToolExecutionPolicyDecision::Allowed(mode)) })
    }
}

struct EmptyControls;

impl LoopControlQueue for EmptyControls {
    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { Ok(0) })
    }
}
const LOOP_INPUT: &[u8] = b"Run the requested Tools and report the result.";

/// Installs the admitted v14 Snapshot (the controlled loop Tools plus an
/// external write Tool) and returns the production Context planner over it.
/// The Run must be seeded with [`LOOP_INPUT`].
async fn install_loop_context(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
) -> Arc<dyn ContextPlanner> {
    let mut controlled = loop_runtime_snapshot(CapabilityProviderId::generate());
    let external = VersionedKey::new("fixture.external-write@1").expect("external write");
    let scope = DottedKey::new("fixture.external").expect("external scope");
    let provider_id = CapabilityProviderId::generate();
    let mut external_write = loop_snapshot_tool(
        "external_write",
        external.clone(),
        provider_id,
        Effect::WriteExternal,
        ToolRetryContract::QueryBeforeRetry,
        ConcurrencyClass::SequentialWrite,
        scope.clone(),
    );
    // An external write may not waive approval in its manifest; the admitted
    // ServiceManaged posture is what lets it start without an Interaction.
    external_write.manifest.approval = ApprovalRequirement::ByPolicy;
    // The Provider owns the external identity queried before any retry.
    external_write.idempotency = BuiltinIdempotency::ProviderTaskRecord;
    controlled.tools.push(external_write);
    controlled.policy.permission_mode = musteragent_policy::PermissionMode::ServiceManaged;
    controlled.capabilities.push(CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(external),
        provider_id,
        location: CapabilityLocation::Cloud,
        effect: Effect::WriteExternal,
        allowed_scopes: BTreeSet::from([scope]),
        policy_decision: Decision::Allow,
    });
    install_production_context(
        pool,
        content.clone(),
        fixture,
        Arc::new(ProtobufLedgerPayloadCodec),
        {
            let mode = controlled.policy.permission_mode;
            let mut admitted =
                live_runtime::upgrade_snapshot(content, run_context(fixture), controlled).await;
            admitted.policy.permission_mode = mode;
            admitted
        },
        LOOP_INPUT,
    )
    .await
}

async fn seed_loop(
    pool: &sqlx::PgPool,
    content: &Arc<dyn ContentStore>,
) -> (Fixture, AttemptContext) {
    running_loop::seed_running_loop_with_input(pool, content.as_ref(), LOOP_INPUT).await
}

fn loop_runtime_snapshot(read_provider_id: CapabilityProviderId) -> RuntimeSnapshotContent {
    let musteragent = fixture_version_v8();
    let core_time = VersionedKey::new("core.time.read@1").expect("core time Capability");
    let fixture_read = VersionedKey::new("fixture.read@1").expect("fixture read Capability");
    let fixture_write = VersionedKey::new("fixture.write@1").expect("fixture write Capability");
    let web_fetch = VersionedKey::new("web.fetch@2").expect("web fetch Capability");
    let web_search = VersionedKey::new("web.search@2").expect("web search Capability");
    let builtin_provider_id = CapabilityProviderId::generate();
    let write_provider_id = CapabilityProviderId::generate();
    let authorized_skill_capabilities =
        BTreeSet::from([core_time.clone(), web_fetch.clone(), web_search.clone()]);
    let skills = builtin_skill_catalog()
        .expect("Skill catalog")
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: musteragent.supported_runtime_contract.clone(),
            authorized_capabilities: authorized_skill_capabilities.clone(),
        })
        .expect("resolve Kernel base Skill")
        .into_iter()
        .filter(|skill| {
            skill
                .required_capabilities
                .is_subset(&authorized_skill_capabilities)
        })
        .collect();
    let read_scope = DottedKey::new("fixture.read").expect("read scope");
    let write_scope = DottedKey::new("fixture.write").expect("write scope");
    let mut tools = ["read_a", "read_b", "read_c", "read_d"]
        .into_iter()
        .map(|wire_name| {
            loop_snapshot_tool(
                wire_name,
                fixture_read.clone(),
                read_provider_id,
                Effect::Read,
                ToolRetryContract::ReadOnly,
                ConcurrencyClass::ParallelRead,
                read_scope.clone(),
            )
        })
        .collect::<Vec<_>>();
    tools.push(loop_snapshot_tool(
        "write_barrier",
        fixture_write.clone(),
        write_provider_id,
        Effect::WriteReversible,
        ToolRetryContract::IdempotentByKey,
        ConcurrencyClass::SequentialWrite,
        write_scope.clone(),
    ));
    RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("Snapshot schema"),
        agent: AgentVersionSnapshot::from_resolved(
            &musteragent_conformance::fixtures::agent::system_definition(),
            &musteragent,
        )
        .expect("Kernel Snapshot"),
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("policy section"),
                    content_ref: fixture_runtime_policy_content_ref().expect("policy content"),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.legacy.persona")
                        .expect("persona section"),
                    content_ref: musteragent.persona_content_ref.clone(),
                },
            ],
            assembled_hash: Sha256::digest(
                [FIXTURE_RUNTIME_POLICY_V1, FIXTURE_PERSONA_V3].concat(),
            ),
        },
        model: model_route(),
        auxiliary_models: Vec::new(),
        skills,
        tools,
        capabilities: vec![
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(core_time),
                provider_id: builtin_provider_id,
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([DottedKey::new("time.read").expect("time scope")]),
                policy_decision: Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(fixture_read),
                provider_id: read_provider_id,
                location: CapabilityLocation::Cloud,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([read_scope]),
                policy_decision: Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(fixture_write),
                provider_id: write_provider_id,
                location: CapabilityLocation::Cloud,
                effect: Effect::WriteReversible,
                allowed_scopes: BTreeSet::from([write_scope]),
                policy_decision: Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_fetch),
                provider_id: builtin_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([
                    DottedKey::new("network.public").expect("network scope")
                ]),
                policy_decision: Decision::Allow,
            },
            CapabilityGrantSnapshot {
                key: CapabilityKey::from_versioned_key(web_search),
                provider_id: builtin_provider_id,
                location: CapabilityLocation::Backend,
                effect: Effect::Read,
                allowed_scopes: BTreeSet::from([
                    DottedKey::new("network.search").expect("search scope")
                ]),
                policy_decision: Decision::Allow,
            },
        ],
        policy: PolicySnapshot {
            policy_ref: musteragent.policy_ref.clone(),
            policy_hash: musteragent.policy_hash,
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: musteragent.memory_policy_ref.clone(),
            selection_policy_hash: musteragent
                .material
                .as_ref()
                .expect("Kernel Agent material")
                .memory_policy_hash,
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: NonZeroU64::new(120_000).expect("input budget"),
            compaction_trigger_tokens: NonZeroU64::new(84_000).expect("70 percent trigger"),
            reserved_output_tokens: NonZeroU64::new(8_192).expect("reserved output"),
            compaction_policy_ref: VersionedKey::new("musteragent.legacy.context-compaction@1")
                .expect("compaction policy"),
            resolved_v2: None,
            source_selection: None,
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: true,
        },
        execution_policy: RunExecutionPolicySnapshot::legacy_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new("musteragent.runtime.command.v2")
                .expect("command schema"),
            record_schema: DottedKey::new("musteragent.runtime.record.v1").expect("record schema"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("Snapshot schema"),
            model_protocol: VersionedKey::new("musteragent.legacy.model.canonical@1")
                .expect("model protocol"),
        },
        discovery: None,
    }
}

fn loop_snapshot_tool(
    wire_name: &str,
    capability_key: VersionedKey,
    provider_id: CapabilityProviderId,
    effect: Effect,
    retry_contract: ToolRetryContract,
    concurrency_class: ConcurrencyClass,
    scope: DottedKey,
) -> ToolDefinitionSnapshot {
    let input_schema_json = br#"{"type":"object"}"#.to_vec();
    let output_schema_json = br#"{"type":"object"}"#.to_vec();
    let idempotency = if effect == Effect::Read {
        BuiltinIdempotency::Native
    } else {
        BuiltinIdempotency::RuntimeTransaction
    };
    ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(
                VersionedKey::new(format!("fixture.{wire_name}@1")).expect("fixture Tool key"),
            ),
            wire_name: WireName::new(wire_name).expect("fixture wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("fixture.input@1").expect("input schema"),
                schema_hash: Sha256::digest(&input_schema_json),
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("fixture.output@1").expect("output schema"),
                schema_hash: Sha256::digest(&output_schema_json),
            },
            capability_key,
            capability_provider_id: provider_id,
            effect,
            approval: ApprovalRequirement::Never,
            execution_policy: musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: Duration::from_secs(30),
            },
            max_attempts: NonZeroU32::new(2).expect("Tool attempts"),
            retry_contract,
            required_scopes: BTreeSet::from([scope]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::None,
        },
        execution_target: CapabilityExecutionTarget::RustRuntime,
        concurrency_class,
        idempotency,
        file_operation: None,
        title: format!("Fixture {wire_name}"),
        description: format!("Execute the {wire_name} conformance Tool."),
        input_schema_json,
        output_schema_json,
    }
}

struct TwoTurnModel {
    invocation: AtomicUsize,
    requests: Mutex<Vec<CanonicalModelRequest>>,
}

impl TwoTurnModel {
    fn new() -> Self {
        Self {
            invocation: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<CanonicalModelRequest> {
        lock(&self.requests).clone()
    }
}

impl ModelProvider for TwoTurnModel {
    fn stream(&self, request: CanonicalModelRequest) -> ModelEventStream<'_> {
        lock(&self.requests).push(request);
        let invocation = self.invocation.fetch_add(1, Ordering::SeqCst);
        let events = if invocation == 0 {
            mixed_tool_events()
        } else {
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("model-final"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("All tools completed.").expect("text"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 20,
                    output_tokens: 4,
                    total_tokens: Some(24),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}

struct SingleWriteModel;

impl ModelProvider for SingleWriteModel {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        let call_id = ToolCallId::new("write-unknown").expect("write call ID");
        let events = vec![
            ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("model-write"),
                response_model_key: response_model_key(),
            },
            ModelStreamEvent::ToolCallStarted {
                output_index: 0,
                call_id: call_id.clone(),
                name: WireName::new("external_write").expect("write Tool"),
                provider_item_ref: None,
                provider_replay: None,
            },
            ModelStreamEvent::ToolCallCompleted {
                output_index: 0,
                call_id,
                arguments: JsonValue::Object(BTreeMap::from([(
                    "value".into(),
                    JsonValue::String("side effect".into()),
                )])),
            },
            ModelStreamEvent::Usage(ModelUsage {
                input_tokens_uncached: 10,
                output_tokens: 4,
                total_tokens: Some(14),
                ..ModelUsage::default()
            }),
            ModelStreamEvent::ResponseCompleted {
                stop_reason: StopReason::ToolCalls,
            },
        ];
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
}

struct PartialModel {
    waiting: Arc<Notify>,
}

impl ModelProvider for PartialModel {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        panic!("supervised fixture must use execution ownership")
    }

    fn execution(
        &self,
        request: CanonicalModelRequest,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<
        Box<dyn musteragent_runtime_model::ModelExecution<'_> + '_>,
        musteragent_runtime_model::ProviderError,
    > {
        musteragent_runtime_model::validate_model_execution_binding(&request, binding)?;
        Ok(Box::new(PartialExecution {
            stop: Arc::new(PartialStop {
                binding,
                stopped: AtomicBool::new(false),
                waker: Mutex::new(None),
                released: Notify::new(),
                closed: AtomicBool::new(false),
            }),
            waiting: Arc::clone(&self.waiting),
        }))
    }
}

/// A local streaming connection: stopping it closes the stream, so the stop
/// is physically confirmed and the Loop drains what it already received.
struct PartialStop {
    binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    stopped: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
    released: Notify,
    closed: AtomicBool,
}

impl musteragent_execution_liveness::ExecutionStopControl for PartialStop {
    fn binding(&self) -> musteragent_runtime_contract::execution_liveness::ExecutionBinding {
        self.binding
    }

    fn stop(
        &self,
        _reason: musteragent_execution_liveness::StopReason,
    ) -> ApplicationFuture<
        '_,
        Result<
            musteragent_execution_liveness::ExecutionStopConfirmation,
            musteragent_execution_liveness::ExecutionStopError,
        >,
    > {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(waker) = lock(&self.waker).take() {
            waker.wake();
        }
        let binding = self.binding;
        Box::pin(async move {
            // Like a provider connection, stop is proven once the Loop has
            // released the physical stream, not when it is merely requested.
            let released = self.released.notified();
            if !self.closed.load(Ordering::SeqCst) {
                released.await;
            }
            Ok(musteragent_execution_liveness::ExecutionStopConfirmation { binding })
        })
    }
}

struct PartialExecution {
    stop: Arc<PartialStop>,
    waiting: Arc<Notify>,
}

impl<'a> musteragent_runtime_model::ModelExecution<'a> for PartialExecution {
    fn binding(&self) -> musteragent_runtime_contract::execution_liveness::ExecutionBinding {
        self.stop.binding
    }

    fn stop_control(&self) -> Arc<dyn musteragent_execution_liveness::ExecutionStopControl> {
        self.stop.clone()
    }

    fn preflight(
        &mut self,
    ) -> ApplicationFuture<'_, Result<(), musteragent_runtime_model::ProviderError>> {
        Box::pin(async { Ok(()) })
    }

    fn into_stream(self: Box<Self>) -> ModelEventStream<'a> {
        Box::pin(PartialModelStream {
            state: 0,
            waiting: self.waiting,
            stop: self.stop,
        })
    }
}

struct PartialModelStream {
    state: u8,
    waiting: Arc<Notify>,
    stop: Arc<PartialStop>,
}

impl Drop for PartialModelStream {
    fn drop(&mut self) {
        self.stop.closed.store(true, Ordering::SeqCst);
        self.stop.released.notify_waiters();
    }
}

struct RecoveryCompletionModel;

impl ModelProvider for RecoveryCompletionModel {
    fn stream(&self, _request: CanonicalModelRequest) -> ModelEventStream<'_> {
        Box::pin(stream::iter(
            vec![
                ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("model-after-process-recovery"),
                    response_model_key: response_model_key(),
                },
                ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("completed after process recovery").expect("text"),
                },
                ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 12,
                    output_tokens: 5,
                    total_tokens: Some(17),
                    ..ModelUsage::default()
                }),
                ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::Stop,
                },
            ]
            .into_iter()
            .map(Ok),
        ))
    }
}

impl Stream for PartialModelStream {
    type Item = Result<ModelStreamEvent, musteragent_runtime_model::ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.stop.stopped.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        match self.state {
            0 => {
                self.state = 1;
                Poll::Ready(Some(Ok(ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("partial-model"),
                    response_model_key: response_model_key(),
                })))
            }
            1 => {
                self.state = 2;
                Poll::Ready(Some(Ok(ModelStreamEvent::TextDelta {
                    output_index: 0,
                    delta: ModelText::new("durable before cancellation").expect("partial text"),
                })))
            }
            _ => {
                *lock(&self.stop.waker) = Some(context.waker().clone());
                if self.stop.stopped.load(Ordering::SeqCst) {
                    return Poll::Ready(None);
                }
                if self.state == 2 {
                    self.state = 3;
                    self.waiting.notify_one();
                }
                Poll::Pending
            }
        }
    }
}

struct SignalControls {
    cancellation_requested: Arc<AtomicBool>,
    lease_expires_at: UtcTimestamp,
}

impl AttemptControlStore for SignalControls {
    fn heartbeat(
        &self,
        _fence: &AttemptFence,
        _lease_duration: Duration,
    ) -> ApplicationFuture<'_, Result<LeaseHeartbeat, AttemptControlError>> {
        let cancellation_requested = self.cancellation_requested.load(Ordering::SeqCst);
        let lease_expires_at = self.lease_expires_at;
        Box::pin(async move {
            Ok(LeaseHeartbeat {
                database_now: UtcTimestamp::from_unix_millis(1_000),
                lease_expires_at,
                stop_intent: cancellation_requested
                    .then_some(musteragent_scheduler::AttemptStopIntent::UserCancellation),
            })
        })
    }

    fn surrender(
        &self,
        _fence: &AttemptFence,
        _reason: InterruptionReason,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
        Box::pin(async { Ok(()) })
    }
}

struct PendingProvider {
    cancellation_requested: Arc<AtomicBool>,
    cancel_after: usize,
    started: AtomicUsize,
    dropped: AtomicUsize,
    all_started: Notify,
    all_dropped: Notify,
}

struct PendingInvocation<'a> {
    provider: &'a PendingProvider,
}

impl Drop for PendingInvocation<'_> {
    fn drop(&mut self) {
        if self.provider.dropped.fetch_add(1, Ordering::SeqCst) + 1 == self.provider.cancel_after {
            self.provider.all_dropped.notify_one();
        }
    }
}

impl CapabilityDispatcher for PendingProvider {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        let invocation = self.invoke(attempt, material, durable, invocation);
        Ok(if material.tool.manifest.effect == Effect::WriteExternal {
            live_runtime::unconfirmable_invoke_operation(binding, invocation)
        } else {
            live_runtime::invoke_operation(binding, invocation)
        })
    }

    fn reconciliation_identity(
        &self,
        material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        if material.tool.manifest.retry_contract == ToolRetryContract::QueryBeforeRetry {
            Ok(Some(
                OpaqueExternalRef::new(format!("test-provider:{}", invocation.invocation_id))
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
            ))
        } else {
            Ok(None)
        }
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            let _guard = PendingInvocation { provider: self };
            if self.started.fetch_add(1, Ordering::SeqCst) + 1 == self.cancel_after {
                self.cancellation_requested.store(true, Ordering::SeqCst);
                self.all_started.notify_one();
            }
            pending::<Result<DispatchedCapabilityOutcome, LoopPortError>>().await
        })
    }

    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(port(ErrorCode::RunTransitionInvalid, false)) })
    }
}

fn mixed_tool_events() -> Vec<ModelStreamEvent> {
    let calls = [
        ("parallel-a", "read_a"),
        ("parallel-b", "read_b"),
        ("write-barrier", "write_barrier"),
        ("parallel-c", "read_c"),
        ("parallel-d", "read_d"),
    ];
    let mut events = vec![ModelStreamEvent::ResponseStarted {
        response_ref: provider_ref("model-tools"),
        response_model_key: response_model_key(),
    }];
    for (output_index, (call_id, name)) in calls.into_iter().enumerate() {
        let call_id = ToolCallId::new(call_id).expect("call ID");
        let query = call_id.as_str().to_owned();
        events.push(ModelStreamEvent::ToolCallStarted {
            output_index: u32::try_from(output_index).expect("output index"),
            call_id: call_id.clone(),
            name: WireName::new(name).expect("wire name"),
            provider_item_ref: None,
            provider_replay: None,
        });
        events.push(ModelStreamEvent::ToolCallCompleted {
            output_index: u32::try_from(output_index).expect("output index"),
            call_id,
            arguments: JsonValue::Object(BTreeMap::from([(
                "query".into(),
                JsonValue::String(query),
            )])),
        });
    }
    events.extend([
        ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 20,
            output_tokens: 10,
            total_tokens: Some(30),
            ..ModelUsage::default()
        }),
        ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        },
    ]);
    events
}

fn provider_ref(value: &str) -> ProviderRequestRef {
    ProviderRequestRef::from_opaque(OpaqueExternalRef::new(value).expect("provider ref"))
}

fn response_model_key() -> musteragent_model::ModelKey {
    musteragent_model::ModelKey::new(model_route().route.resolved_model_key).expect("model key")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn port(code: ErrorCode, retryable: bool) -> LoopPortError {
    LoopPortError { code, retryable }
}

fn planned_call(call_id: &str) -> PlannedToolCall {
    PlannedToolCall {
        argument_diagnostic: None,
        call_id: ToolCallId::new(call_id).expect("call ID"),
        name: WireName::new("read_a").expect("wire name"),
        arguments: JsonValue::Object(BTreeMap::from([(
            "query".into(),
            JsonValue::String(call_id.into()),
        )])),
        provider_replay: None,
        planning_failure: None,
    }
}

#[path = "postgres_cloud_worker_parallel_tools/scenarios.rs"]
mod scenarios;

async fn build_production_loop(
    pool: &sqlx::PgPool,
    keys: Arc<OpaqueRefKeyring>,
    content: Arc<dyn ContentStore>,
    context: Arc<dyn ContextPlanner>,
    model: Arc<dyn ModelProvider>,
    provider: Arc<dyn CapabilityDispatcher>,
) -> (Arc<DurableAgentLoop>, Arc<PostgresAgentLoopJournal>) {
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let tool_journal: Arc<dyn FencedToolInvocationJournal> = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let tool_executor: Arc<dyn ToolExecutor> = Arc::new(
        DurableToolExecutor::new(
            production_tool_material_source(pool, codec.clone()),
            tool_journal,
            content.clone(),
            provider,
            Arc::new(FixedClock(database_now(pool).await)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(pool)),
    );
    let now = database_now(pool).await;
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixedExecutionPolicy { admitted_at: now }),
        context,
        tool_executor,
        Arc::new(EmptyControls),
    ));
    let journal = Arc::new(
        PostgresAgentLoopJournal::new(
            pool.clone(),
            content,
            codec.clone(),
            codec,
            Arc::new(musteragent_wire::ProtobufMeteringPublicationEncoder),
            keys,
            running_loop::recovery_keyring(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    let runtime = Arc::new(
        DurableAgentLoop::new(Arc::new(FixedClock(now)), policy, model, journal.clone())
            .with_execution_supervision(live_runtime::supervision(pool)),
    );
    (runtime, journal)
}

#[path = "postgres_cloud_worker_parallel_tools/argument_failures.rs"]
mod argument_failures;
