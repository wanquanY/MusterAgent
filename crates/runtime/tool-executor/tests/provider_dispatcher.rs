//! Unit and routing tests for `ProviderCapabilityDispatcher`.
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU32, NonZeroU64};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use musteragent_activity::ActivityKind;
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_discovery::ExposureState;
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_liveness::{ExecutionAcceptance, ExecutionActivitySink, LivenessError};
use musteragent_identity::{
    ActivityId, Actor, AgentId, AttemptId, AttemptNumber, CapabilityProviderId, ContentId,
    ConversationId, DelegationId, DottedKey, FencingToken, HomeEpoch, IdempotencyKey, InvocationId,
    LeaseId, LedgerRecordId, ParticipantId, PrincipalId, PrincipalKind, ProductId, RunId,
    RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_loop::LoopPortError;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy, PermissionMode,
};
use musteragent_run::{AttemptContext, RunContext, WorkerId};
use musteragent_runtime_capability::{
    BuiltinTimeCapability, CapabilityInvocation, CapabilityOutcome, CapabilityReconciliation,
    CapabilityRegistry, CapabilityResult, DurableToolInvocation, InvocationAuthorization,
    ReconciliationOutcome, RuntimeClock, ToolExecutionMaterial,
};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ModelClass, OpaqueExternalRef, TraceContext, TraceParent,
    execution_liveness::{
        ActivityExecutionTiming, ActivitySource, ExecutionBinding, ExecutionScope as LivenessScope,
        InvocationExecutionTiming,
    },
};
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, RunExecutionPolicySnapshot, ToolDefinitionSnapshot, authorized_tool_catalog,
};
use musteragent_tool::{
    BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID, BackendOperation, BuiltinIdempotency,
    BuiltinToolCatalog, CapabilityExecutionTarget, ConcurrencyClass, ProductExecutionTarget,
    ToolExecutionPolicy, ToolInvocation, ToolInvocationStatus, ToolKey, ToolManifest,
    ToolRetryContract, WireName, compiled_product_release,
};
use musteragent_tool_executor::{
    ApprovalPromptOverride, CapabilityDispatcher, CompositeCapabilityDispatcher,
    DispatchedCapabilityOutcome, DispatchedReconciliationOutcome, ProductCapabilityDispatcher,
    ProviderCapabilityDispatcher, ProviderDispatcherError, RetainedToolResult,
    ToolPhysicalExecution, builtin_capability_manifest_at_location, opaque_dispatcher_execution,
};

fn poll_ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("in-memory test future unexpectedly returned Poll::Pending"),
    }
}

type StoredEntry = (TenantId, ContentRef, Vec<u8>);

#[derive(Default)]
struct TestContentStore {
    values: Mutex<BTreeMap<ContentId, StoredEntry>>,
}

impl ContentStore for TestContentStore {
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.put_at(ContentId::generate(), request)
    }

    fn put_at(
        &self,
        id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async move {
            let reference = ContentRef {
                content_id: id,
                media_type: request.media_type,
                byte_length: request.bytes.len() as u64,
                sha256: Sha256::digest(&request.bytes),
            };
            let mut values = self.values.lock().expect("content lock");
            values.insert(id, (request.tenant_id, reference.clone(), request.bytes));
            Ok(reference)
        })
    }

    fn read<'a>(
        &'a self,
        tenant: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async move {
            let values = self.values.lock().expect("content lock");
            let (owner, stored, bytes) = values
                .get(&reference.content_id)
                .ok_or(ContentStoreError::NotFound)?;
            if *owner != tenant || stored != reference {
                return Err(ContentStoreError::Unauthorized);
            }
            Ok(bytes.clone())
        })
    }
}

struct FixedClock;
impl RuntimeClock for FixedClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1_790_874_000_000)
    }
}

struct TestActivitySink(ExecutionBinding);
impl ExecutionActivitySink for TestActivitySink {
    fn binding(&self) -> ExecutionBinding {
        self.0
    }
    fn accepted(&self) -> Result<ExecutionAcceptance, LivenessError> {
        Ok(ExecutionAcceptance::Observed)
    }
    fn wait_for_acceptance(&self) -> ApplicationFuture<'_, Result<(), LivenessError>> {
        Box::pin(async { Ok(()) })
    }
    fn ended(&self) -> Result<(), LivenessError> {
        Ok(())
    }
    fn advanced(&self, _: ActivitySource, _: NonZeroU64) -> Result<(), LivenessError> {
        Ok(())
    }
}

struct RecordingHostDispatcher {
    tag: &'static str,
    bind_self: bool,
    observed: Arc<Mutex<Vec<(RunId, String, bool)>>>,
}

impl CapabilityDispatcher for RecordingHostDispatcher {
    fn bind(
        &self,
        _material: &ToolExecutionMaterial,
    ) -> Result<Option<Arc<dyn CapabilityDispatcher>>, LoopPortError> {
        if self.bind_self {
            Ok(Some(Arc::new(Self {
                tag: self.tag,
                bind_self: false,
                observed: self.observed.clone(),
            })))
        } else {
            Ok(None)
        }
    }

    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        opaque_dispatcher_execution(self, attempt, material, durable, invocation, binding)
    }

    fn approval_prompt<'a>(
        &'a self,
        _material: &'a ToolExecutionMaterial,
        _invocation: &'a CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<Option<ApprovalPromptOverride>, LoopPortError>> {
        let tag = self.tag;
        Box::pin(async move {
            Ok(Some(ApprovalPromptOverride {
                media_type: MediaType::new("application/json").expect("media type"),
                bytes: format!(r#"{{"provider":"{tag}"}}"#).into_bytes(),
            }))
        })
    }

    fn reconciliation_identity(
        &self,
        _material: &ToolExecutionMaterial,
        invocation: &CapabilityInvocation,
    ) -> Result<Option<OpaqueExternalRef>, LoopPortError> {
        Ok(Some(
            OpaqueExternalRef::new(format!(
                "{}-reconcile-{}",
                self.tag, invocation.invocation_id
            ))
            .expect("reconcile ref"),
        ))
    }

    fn retained_result_query_timeout(
        &self,
        _material: &ToolExecutionMaterial,
    ) -> Result<Option<Duration>, LoopPortError> {
        Ok(Some(Duration::from_secs(5)))
    }

    fn lookup_retained_result<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _deadline: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<RetainedToolResult, LoopPortError>> {
        Box::pin(async { Ok(RetainedToolResult::Incomplete) })
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        let observed = self.observed.clone();
        let tag = self.tag;
        Box::pin(async move {
            observed.lock().expect("observed lock").push((
                invocation.run_id,
                format!("{tag}:{}", invocation.tool_key.as_str()),
                invocation.execution_grant_proof.is_some(),
            ));
            let bytes = format!(r#"{{"handled_by":"{tag}"}}"#).into_bytes();
            let result_hash = Sha256::digest(&bytes);
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_ref: ContentRef {
                        content_id: ContentId::generate(),
                        media_type: MediaType::new("application/json").expect("media type"),
                        byte_length: bytes.len() as u64,
                        sha256: result_hash,
                    },
                    result_schema: invocation.result_schema,
                    result_hash,
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
        Box::pin(async {
            Ok(DispatchedReconciliationOutcome::Provider(
                ReconciliationOutcome::ProvenNotApplied,
            ))
        })
    }
}

fn test_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("test.route@1").expect("route ref"),
        resolved_model_key: "test-model".parse().expect("model key"),
        model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::from_u128(0x411))
            .expect("provider id"),
        model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::from_u128(0x412)).expect("entry id"),
        api_format: ApiFormat::OpenAiChatCompletions,
        adapter_version: VersionedKey::new("openai.chat-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.chat@1").expect("protocol"),
        context_window: NonZeroU64::new(64_000).expect("window"),
        max_output_tokens: NonZeroU64::new(4_096).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    }
}

fn dynamic_tool_pair(
    provider_id: CapabilityProviderId,
) -> (ToolDefinitionSnapshot, CapabilityGrantSnapshot) {
    let key = VersionedKey::new("inventory.quote.lookup@1").expect("key");
    let input_json =
        br#"{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"],"additionalProperties":false}"#
            .to_vec();
    let output_json = br#"{"type":"object"}"#.to_vec();
    let scope = DottedKey::new("inventory.quote.read").expect("scope");
    let tool = ToolDefinitionSnapshot {
        manifest: ToolManifest {
            application: None,
            key: ToolKey::from_versioned_key(key.clone()),
            wire_name: WireName::new("inventory_quote_lookup").expect("wire name"),
            input_schema: SchemaRef {
                schema_key: VersionedKey::new("inventory.quote.lookup.input@1").expect("in"),
                schema_hash: Sha256::digest(&input_json),
            },
            output_schema: SchemaRef {
                schema_key: VersionedKey::new("inventory.quote.lookup.output@1").expect("out"),
                schema_hash: Sha256::digest(&output_json),
            },
            capability_key: key.clone(),
            capability_provider_id: provider_id,
            effect: Effect::Read,
            approval: ApprovalRequirement::Never,
            execution_policy: ToolExecutionPolicy::builtin_activity(
                key.clone(),
                Duration::from_secs(30),
                None,
            ),
            max_attempts: NonZeroU32::MIN,
            retry_contract: ToolRetryContract::ReadOnly,
            required_scopes: BTreeSet::from([scope.clone()]),
            required_contexts: BTreeSet::new(),
            data_classification: DataClassification::Internal,
            egress_policy: EgressPolicy::ProviderBound,
        },
        execution_target: CapabilityExecutionTarget::BackendNamed(
            BackendOperation::new("InventoryService.LookupQuote", None).expect("backend op"),
        ),
        concurrency_class: ConcurrencyClass::BoundedNetworkRead,
        idempotency: BuiltinIdempotency::Native,
        file_operation: None,
        title: "Lookup Quote".to_owned(),
        description: "Look up an inventory quote.".to_owned(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: input_json,
        output_schema_json: output_json,
    };
    tool.validate().expect("valid dynamic tool");
    let grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(key),
        provider_id,
        location: CapabilityLocation::Backend,
        effect: Effect::Read,
        allowed_scopes: BTreeSet::from([scope]),
        policy_decision: Decision::Allow,
    };
    (tool, grant)
}

fn make_invocation_fixture(
    tool: &ToolDefinitionSnapshot,
    grant: &CapabilityGrantSnapshot,
    derived_parent: Option<RunId>,
    credential_grant: &str,
) -> (
    AttemptContext,
    ToolExecutionMaterial,
    DurableToolInvocation,
    CapabilityInvocation,
    ExecutionBinding,
) {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let run_id = RunId::generate();
    let attempt_id = AttemptId::generate();
    let attempt_number = AttemptNumber::new(1).expect("attempt number");
    let fencing_token = FencingToken::new(1).expect("fencing token");
    let home_epoch = HomeEpoch::new(1).expect("home epoch");
    let now = UtcTimestamp::from_unix_millis(1_790_874_000_000);
    let attempt = AttemptContext {
        run_id,
        attempt_id,
        attempt_number,
        worker_id: WorkerId::new("worker-test").expect("worker id"),
        lease_id: LeaseId::generate(),
        fencing_token,
        lease_expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 30_000),
    };
    let run_context = RunContext {
        tenant_id,
        conversation_id,
        activity_id: ActivityId::generate(),
        activity_kind: if derived_parent.is_some() {
            ActivityKind::Delegation
        } else {
            ActivityKind::Chat
        },
        run_id,
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: ParticipantId::generate(),
        parent_run_id: derived_parent,
        delegation_id: derived_parent.map(|_| DelegationId::generate()),
        execution_scope: ExecutionScope::Tenant { tenant_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch,
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    };
    let actor = Actor {
        tenant_id,
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let tools = vec![tool.clone()];
    let grants = vec![grant.clone()];
    let authorized = authorized_tool_catalog(&tools, &grants).expect("authorized catalog");
    let exposure =
        ExposureState::from_initial(&authorized, std::slice::from_ref(&tool.manifest.key))
            .expect("exposure");
    let mut exec_snap = RunExecutionPolicySnapshot::production_defaults();
    exec_snap.liveness =
        Some(musteragent_runtime_snapshot::RunLivenessPolicyV1::production_default());
    let material = ToolExecutionMaterial {
        application_capabilities: Vec::new(),
        product_release: Some(compiled_product_release(ProductId::KERNEL)),
        run_context,
        recovery_policy: NonZeroU32::new(2).expect("attempts").into(),
        actor,
        agent_id: AgentId::generate(),
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("test").expect("model class"),
            route: test_route(),
            credential_grant_ref: CredentialGrantRef::new(credential_grant).expect("grant ref"),
            fallback_candidates: Vec::new(),
        },
        active_model_route_ordinal: 0,
        auxiliary_models: Vec::new(),
        model_stream_budget: exec_snap.model_stream,
        model_liveness: Some(Arc::new(
            exec_snap.liveness.expect("liveness").model_policy(),
        )),
        tool: tool.clone(),
        authorized_tools: authorized,
        discovery: None,
        exposure_state: exposure,
        capability_grant: grant.clone(),
        authorization: InvocationAuthorization {
            actor_scopes: grant.allowed_scopes.clone(),
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        },
        permission_mode: PermissionMode::FullAccess,
        policy_basis: None,
        authorized_content: Vec::new(),
        prior_tool_results: Vec::new(),
        execution_grant_proof: None,
    };

    let invocation_id = InvocationId::generate();
    let call_id = ToolCallId::new("call_1").expect("call id");
    let req_bytes = br#"{"sku":"rack-unit-7"}"#;
    let request_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: req_bytes.len() as u64,
        sha256: Sha256::digest(req_bytes),
    };
    let ToolExecutionPolicy::ActivityV1(policy) = &tool.manifest.execution_policy else {
        panic!("expected ActivityV1 policy");
    };
    let timing = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
        policy: policy.reference().clone(),
        business_deadline: None,
    });
    let idempotency_key = IdempotencyKey::new(format!("tool-{invocation_id}")).expect("idem key");
    let tool_inv = ToolInvocation {
        invocation_id,
        run_id,
        requested_by_attempt_id: attempt_id,
        execution_attempt_id: Some(attempt_id),
        tool_call_id: call_id.clone(),
        tool_key: tool.manifest.key.clone(),
        capability_provider_id: tool.manifest.capability_provider_id,
        status: ToolInvocationStatus::Running,
        effect: tool.manifest.effect,
        approval: tool.manifest.approval,
        idempotency_key: idempotency_key.clone(),
        request_ref: request_ref.clone(),
        result_ref: None,
        interaction_id: None,
        timing: timing.clone(),
    };
    let durable = DurableToolInvocation {
        sandbox_route: None,
        request_record_id: LedgerRecordId::generate(),
        invocation: tool_inv,
        retry_contract: tool.manifest.retry_contract,
        prebound_reconciliation_ref: None,
        recorded_outcome: None,
        interaction_resolution: None,
    };
    let cap_inv = CapabilityInvocation {
        actor,
        tenant_id,
        conversation_id,
        run_id,
        requested_by_attempt_id: attempt_id,
        execution_attempt_id: attempt_id,
        execution_attempt_number: attempt_number,
        invocation_id,
        tool_call_id: call_id,
        tool_key: tool.manifest.key.clone(),
        capability_key: grant.key.clone(),
        capability_provider_id: tool.manifest.capability_provider_id,
        idempotency_key,
        request_ref,
        request_schema: tool.manifest.input_schema.clone(),
        result_schema: tool.manifest.output_schema.clone(),
        authorized_content: Vec::new(),
        workspace_id: None,
        timing,
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
        execution_grant_proof: None,
    };
    let binding = ExecutionBinding {
        tenant_id,
        run_id,
        scope: LivenessScope::Invocation(invocation_id),
        operation_identity: Sha256::digest(b"test-operation"),
        owner_attempt_id: attempt_id,
        owner_fence: fencing_token,
        producer_generation: NonZeroU64::MIN,
    };
    (attempt, material, durable, cap_inv, binding)
}

fn build_product_dispatcher(content: Arc<TestContentStore>) -> Arc<dyn CapabilityDispatcher> {
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog = BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)
        .expect("cloud catalog");
    let rust_provider: CapabilityProviderId =
        BUILTIN_RUST_PROVIDER_ID.parse().expect("rust provider");
    let time_def = catalog
        .definitions()
        .iter()
        .find(|d| d.key.as_str() == "system.time.current@1")
        .expect("time definition")
        .clone();
    let time_manifest = builtin_capability_manifest_at_location(
        &time_def,
        rust_provider,
        CapabilityLocation::Cloud,
    )
    .expect("time manifest");
    let mut registry = CapabilityRegistry::new();
    registry
        .register(Arc::new(
            BuiltinTimeCapability::new(time_manifest, content, Arc::new(FixedClock))
                .expect("time capability"),
        ))
        .expect("register time");
    let composite = Arc::new(CompositeCapabilityDispatcher::new(Arc::new(registry)));
    let mut products = ProductCapabilityDispatcher::default();
    products
        .register(release, catalog.definitions().to_vec(), composite)
        .expect("register product release");
    Arc::new(products)
}

#[test]
fn construction_rejects_builtin_provider_ids_and_duplicates() {
    let content = Arc::new(TestContentStore::default());
    let inner = build_product_dispatcher(content);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let disp: Arc<dyn CapabilityDispatcher> = Arc::new(RecordingHostDispatcher {
        tag: "host-a",
        bind_self: false,
        observed,
    });

    let rust_id: CapabilityProviderId = BUILTIN_RUST_PROVIDER_ID.parse().expect("rust id");
    let backend_id: CapabilityProviderId = BUILTIN_BACKEND_PROVIDER_ID.parse().expect("backend id");
    let host_id = CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        b"example.test/capability-provider/inventory.v1",
    ))
    .expect("host id");

    assert_eq!(
        ProviderCapabilityDispatcher::new(inner.clone(), [(rust_id, disp.clone())]).err(),
        Some(ProviderDispatcherError::BuiltinProvider(rust_id))
    );
    assert_eq!(
        ProviderCapabilityDispatcher::new(inner.clone(), [(backend_id, disp.clone())]).err(),
        Some(ProviderDispatcherError::BuiltinProvider(backend_id))
    );
    assert_eq!(
        ProviderCapabilityDispatcher::new(
            inner.clone(),
            [(host_id, disp.clone()), (host_id, disp.clone())]
        )
        .err(),
        Some(ProviderDispatcherError::DuplicateProvider(host_id))
    );

    let mut incremental = ProviderCapabilityDispatcher::with_inner(inner);
    assert!(incremental.is_empty());
    incremental
        .register(host_id, disp.clone())
        .expect("first registration succeeds");
    assert_eq!(incremental.len(), 1);
    assert_eq!(
        incremental.register(host_id, disp),
        Err(ProviderDispatcherError::DuplicateProvider(host_id))
    );
}

#[test]
fn routes_host_providers_falls_through_to_product_and_fails_unknown_host_provider() {
    let content = Arc::new(TestContentStore::default());
    let inner = build_product_dispatcher(content.clone());
    let observed_a = Arc::new(Mutex::new(Vec::new()));
    let observed_b = Arc::new(Mutex::new(Vec::new()));

    let provider_a = CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        b"example.test/capability-provider/inventory-a.v1",
    ))
    .expect("provider a");
    let provider_b = CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        b"example.test/capability-provider/inventory-b.v1",
    ))
    .expect("provider b");
    let unknown_provider = CapabilityProviderId::from_uuid(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        b"example.test/capability-provider/unregistered.v1",
    ))
    .expect("unknown provider");

    let disp_a: Arc<dyn CapabilityDispatcher> = Arc::new(RecordingHostDispatcher {
        tag: "host-a",
        bind_self: false,
        observed: observed_a.clone(),
    });
    let disp_b: Arc<dyn CapabilityDispatcher> = Arc::new(RecordingHostDispatcher {
        tag: "host-b",
        bind_self: true,
        observed: observed_b.clone(),
    });

    let router = ProviderCapabilityDispatcher::new(
        inner,
        BTreeMap::from([(provider_a, disp_a), (provider_b, disp_b)]),
    )
    .expect("valid host providers");

    // 1. Registered provider A routes both root run and derived child run (`grant_derived_*`).
    let (tool_a, grant_a) = dynamic_tool_pair(provider_a);
    let (root_attempt, root_mat, root_dur, root_inv, root_bind) =
        make_invocation_fixture(&tool_a, &grant_a, None, "grant_root_1");
    assert!(router.bind(&root_mat).expect("bind").is_none());
    assert!(
        router
            .reconciliation_identity(&root_mat, &root_inv)
            .expect("reconcile identity")
            .expect("some ref")
            .as_str()
            .starts_with("host-a-reconcile-")
    );
    assert_eq!(
        router
            .retained_result_query_timeout(&root_mat)
            .expect("query timeout"),
        Some(Duration::from_secs(5))
    );
    let prompt = poll_ready(router.approval_prompt(&root_mat, &root_inv))
        .expect("approval prompt")
        .expect("override");
    assert_eq!(prompt.bytes, br#"{"provider":"host-a"}"#);
    assert_eq!(
        poll_ready(router.lookup_retained_result(
            &root_attempt,
            &root_mat,
            &root_dur,
            UtcTimestamp::from_unix_millis(1_790_874_005_000)
        ))
        .expect("lookup retained"),
        RetainedToolResult::Incomplete
    );
    let op_root = router
        .execution(&root_attempt, &root_mat, &root_dur, root_inv, root_bind)
        .expect("root execution");
    let root_out =
        poll_ready(op_root.execute(Arc::new(TestActivitySink(root_bind)))).expect("root result");
    assert!(matches!(
        root_out,
        DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(_))
    ));

    // Derived child run with re-derived grant and `execution_grant_proof = None`.
    let (child_attempt, child_mat, child_dur, child_inv, child_bind) = make_invocation_fixture(
        &tool_a,
        &grant_a,
        Some(root_mat.run_context.run_id),
        "grant_derived_child",
    );
    let op_child = router
        .execution(
            &child_attempt,
            &child_mat,
            &child_dur,
            child_inv,
            child_bind,
        )
        .expect("child execution");
    let child_out =
        poll_ready(op_child.execute(Arc::new(TestActivitySink(child_bind)))).expect("child result");
    assert!(matches!(
        child_out,
        DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(_))
    ));
    let calls_a = observed_a.lock().expect("observed a");
    assert_eq!(calls_a.len(), 2);
    assert_eq!(calls_a[0].0, root_mat.run_context.run_id);
    assert_eq!(calls_a[1].0, child_mat.run_context.run_id);
    assert!(!calls_a[1].2, "derived run carries no root grant proof");
    drop(calls_a);

    // 2. Registered provider B returning `Some(bound)` from `bind` preserves physical validation.
    let (tool_b, grant_b) = dynamic_tool_pair(provider_b);
    let (b_attempt, b_mat, b_dur, b_inv, b_bind) =
        make_invocation_fixture(&tool_b, &grant_b, None, "grant_root_b");
    let bound_b = router
        .bind(&b_mat)
        .expect("bind b")
        .expect("provider B returns bound dispatcher");
    let op_b = bound_b
        .execution(&b_attempt, &b_mat, &b_dur, b_inv, b_bind)
        .expect("bound execution");
    assert!(matches!(
        poll_ready(op_b.execute(Arc::new(TestActivitySink(b_bind)))).expect("bound result"),
        DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(_))
    ));
    assert_eq!(observed_b.lock().expect("observed b").len(), 1);

    // 3. Mismatched capability_grant.provider_id vs tool.manifest.capability_provider_id -> RuntimeSnapshotInvalid.
    let mut mismatched_mat = root_mat.clone();
    mismatched_mat.capability_grant.provider_id = provider_b;
    assert_eq!(
        router.bind(&mismatched_mat).err(),
        Some(LoopPortError {
            code: ErrorCode::RuntimeSnapshotInvalid,
            retryable: false,
        })
    );

    // 4. Builtin tool falls through to inner `ProductCapabilityDispatcher`.
    let release = compiled_product_release(ProductId::KERNEL);
    let catalog = BuiltinToolCatalog::for_product_release(&release, ProductExecutionTarget::Cloud)
        .expect("cloud catalog");
    let rust_provider: CapabilityProviderId =
        BUILTIN_RUST_PROVIDER_ID.parse().expect("rust provider");
    let time_def = catalog
        .definitions()
        .iter()
        .find(|d| d.key.as_str() == "system.time.current@1")
        .expect("time definition");
    let time_tool = ToolDefinitionSnapshot {
        manifest: time_def.pin(rust_provider),
        execution_target: time_def.target.clone(),
        concurrency_class: time_def.concurrency,
        idempotency: time_def.idempotency,
        file_operation: time_def.file_operation.clone(),
        title: time_def.title.clone(),
        description: time_def.description.clone(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: time_def.input_schema_json.clone(),
        output_schema_json: time_def.output_schema_json.clone(),
    };
    let time_grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(time_def.capability_key.clone()),
        provider_id: rust_provider,
        location: CapabilityLocation::Cloud,
        effect: time_def.effect,
        allowed_scopes: time_def.required_scopes.clone(),
        policy_decision: Decision::Allow,
    };
    let (t_attempt, t_mat, mut t_dur, mut t_inv, t_bind) =
        make_invocation_fixture(&time_tool, &time_grant, None, "grant_root_time");
    let put_req = poll_ready(ContentStore::put(
        content.as_ref(),
        PutContent {
            tenant_id: t_mat.run_context.tenant_id,
            media_type: MediaType::new("application/json").expect("media type"),
            bytes: b"{}".to_vec(),
        },
    ))
    .expect("put time request");
    t_dur.invocation.request_ref = put_req.clone();
    t_inv.request_ref = put_req;
    let time_op = router
        .execution(&t_attempt, &t_mat, &t_dur, t_inv, t_bind)
        .expect("builtin time falls through to ProductCapabilityDispatcher");
    assert!(matches!(
        poll_ready(time_op.execute(Arc::new(TestActivitySink(t_bind)))).expect("time result"),
        DispatchedCapabilityOutcome::Provider(CapabilityOutcome::Succeeded(_))
    ));

    // 5. Unknown provider for a host-shaped call fails explicitly with `CapabilityUnavailable`,
    //    never silent success.
    let (unk_tool, unk_grant) = dynamic_tool_pair(unknown_provider);
    let (u_attempt, u_mat, u_dur, u_inv, u_bind) =
        make_invocation_fixture(&unk_tool, &unk_grant, None, "grant_root_unk");
    let expected_unavailable = LoopPortError {
        code: ErrorCode::CapabilityUnavailable,
        retryable: false,
    };
    assert_eq!(
        router
            .execution(&u_attempt, &u_mat, &u_dur, u_inv.clone(), u_bind)
            .err(),
        Some(expected_unavailable.clone())
    );
    assert_eq!(
        poll_ready(router.invoke(&u_attempt, &u_mat, &u_dur, u_inv.clone())),
        Err(expected_unavailable.clone())
    );
    assert_eq!(
        router.reconciliation_identity(&u_mat, &u_inv),
        Err(expected_unavailable.clone())
    );
    assert_eq!(
        router.retained_result_query_timeout(&u_mat),
        Err(expected_unavailable.clone())
    );
    assert_eq!(
        poll_ready(router.approval_prompt(&u_mat, &u_inv)),
        Err(expected_unavailable.clone())
    );
    let rec_req = CapabilityReconciliation {
        actor: u_mat.actor,
        tenant_id: u_mat.run_context.tenant_id,
        conversation_id: u_mat.run_context.conversation_id,
        run_id: u_mat.run_context.run_id,
        workspace_id: None,
        execution_attempt_id: u_attempt.attempt_id,
        execution_attempt_number: u_attempt.attempt_number,
        invocation_id: u_dur.invocation.invocation_id,
        provider_id: unknown_provider,
        idempotency_key: u_dur.invocation.idempotency_key.clone(),
        result_schema: unk_tool.manifest.output_schema,
        reconciliation_ref: OpaqueExternalRef::new("unk-reconcile").expect("reconcile ref"),
        deadline: UtcTimestamp::from_unix_millis(1_790_874_030_000),
        trace_context: u_inv.trace_context,
        execution_grant_proof: None,
    };
    assert_eq!(
        poll_ready(router.reconcile(&u_attempt, &u_mat, &u_dur, rec_req)),
        Err(expected_unavailable)
    );
}
