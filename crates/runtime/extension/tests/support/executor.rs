use super::*;
use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopControl,
};
use musteragent_policy::{
    ApprovalRequirement, DataClassification, Decision, Effect, EgressPolicy, PermissionMode,
};
use musteragent_runtime_capability::*;
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_runtime_snapshot::{ModelRouteSnapshot, ToolDefinitionSnapshot};
use musteragent_tool::{
    BuiltinIdempotency, CapabilityExecutionTarget, ConcurrencyClass, ToolExecutionPolicy,
    ToolInvocation, ToolInvocationStatus, ToolManifest, ToolRetryContract,
};
use musteragent_tool_executor::*;

type StoredContent = (TenantId, ContentRef, Vec<u8>);
#[derive(Default)]
pub struct Content {
    values: Mutex<BTreeMap<ContentId, StoredContent>>,
}
impl ContentStore for Content {
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
            let mut values = lock(&self.values);
            if let Some((tenant, old, bytes)) = values.get(&id) {
                if tenant != &request.tenant_id || old != &reference || bytes != &request.bytes {
                    return Err(ContentStoreError::IdentityConflict);
                }
                return Ok(old.clone());
            }
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
            let values = lock(&self.values);
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
pub type BeforeExecute = Arc<dyn Fn() + Send + Sync>;
#[derive(Default)]
pub struct RequestPause {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}
pub struct ToolFixture {
    pub before_execute: Mutex<Option<BeforeExecute>>,
    pub before_request: Mutex<Option<Arc<RequestPause>>>,
    pub context: ExtensionContext,
    pub home: Arc<Home>,
    pub content: Arc<Content>,
    pub materials: BTreeMap<String, ToolExecutionMaterial>,
    pub invocations: Mutex<BTreeMap<InvocationId, DurableToolInvocation>>,
    pub executions: Arc<Mutex<Vec<serde_json::Value>>>,
    pub result: Arc<Mutex<serde_json::Value>>,
}
impl ToolFixture {
    pub fn new(
        context: ExtensionContext,
        home: Arc<Home>,
        definitions: &[CanonicalToolDefinition],
    ) -> Self {
        let materials = definitions
            .iter()
            .map(|d| (d.name.as_str().into(), material(&context, d)))
            .collect();
        Self {
            before_execute: Mutex::new(None),
            before_request: Mutex::new(None),
            context,
            home,
            materials,
            content: Arc::new(Content::default()),
            invocations: Mutex::new(BTreeMap::new()),
            executions: Arc::new(Mutex::new(vec![])),
            result: Arc::new(Mutex::new(
                serde_json::json!({"value":"original","secret":"raw"}),
            )),
        }
    }
    pub fn executor(self: &Arc<Self>, host: Arc<ExtensionHost>) -> Arc<dyn ToolExecutor> {
        let dispatcher = Arc::new(
            RegisteredToolDispatcher::new(self.clone(), host.clone(), self.context.clone())
                .expect("valid fixture and successful tested operation"),
        );
        Arc::new(
            DurableToolExecutor::new(
                self.clone(),
                self.clone(),
                self.content.clone(),
                dispatcher,
                Arc::new(Clock),
                self.clone(),
            )
            .with_invocation_policy_evaluator(Arc::new(ExtensionInvocationPolicy::new(
                Arc::new(Inherit),
                host,
            )))
            .with_execution_supervision(self.home.supervision()),
        )
    }
}
struct Inherit;
impl InvocationPolicyEvaluator for Inherit {
    fn evaluate(&self, _: &InvocationPolicyInput<'_>) -> InvocationPolicyDisposition {
        InvocationPolicyDisposition::Inherit
    }
}
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(1000)
    }
}
impl WorkerTraceContextSource for ToolFixture {
    fn trace_context<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolCallId,
    ) -> ApplicationFuture<'a, Result<TraceContext, LoopPortError>> {
        Box::pin(async { Ok(telemetry(&self.context).trace_context) })
    }
}
impl ToolExecutionMaterialSource for ToolFixture {
    fn load<'a>(
        &'a self,
        a: &'a AttemptContext,
        name: &'a WireName,
    ) -> ApplicationFuture<'a, Result<ToolExecutionMaterial, ToolExecutionPortError>> {
        Box::pin(async move {
            assert_eq!(a, &self.context.attempt);
            self.materials
                .get(name.as_str())
                .cloned()
                .ok_or(tool_error())
        })
    }
}
fn tool_error() -> ToolExecutionPortError {
    ToolExecutionPortError {
        code: ErrorCode::CapabilityUnavailable,
        retryable: false,
    }
}
fn loop_error() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::CapabilityUnavailable,
        retryable: false,
    }
}
impl FencedToolInvocationJournal for ToolFixture {
    fn load_or_begin(
        &self,
        a: &AttemptContext,
        invocation: ToolInvocation,
        retry: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        assert_eq!(a, &self.context.attempt);
        Box::pin(async move {
            let pause = lock(&self.before_request).take();
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.release.notified().await;
            }
            let mut all = lock(&self.invocations);
            if let Some(old) = all.get(&invocation.invocation_id) {
                assert_eq!(old.invocation.request_ref, invocation.request_ref);
                return Ok(old.clone());
            }
            let durable = DurableToolInvocation {
                sandbox_route: None,
                request_record_id: LedgerRecordId::generate(),
                invocation,
                retry_contract: retry,
                prebound_reconciliation_ref: None,
                recorded_outcome: None,
                interaction_resolution: None,
            };
            all.insert(durable.invocation.invocation_id, durable.clone());
            Ok(durable)
        })
    }
    fn load_observation(
        &self,
        _: &AttemptContext,
        invocation: ToolInvocation,
        _: ToolRetryContract,
    ) -> ApplicationFuture<'_, Result<DurableToolObservation, ToolExecutionPortError>> {
        Box::pin(async move {
            let durable = lock(&self.invocations)
                .get(&invocation.invocation_id)
                .cloned()
                .ok_or(tool_error())?;
            Ok(DurableToolObservation::from_journal(
                durable.request_record_id,
                durable,
            ))
        })
    }
    fn request_approval(
        &self,
        _: &AttemptContext,
        _: ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        Box::pin(async { Err(tool_error()) })
    }
    fn request_input(
        &self,
        _: &AttemptContext,
        _: InputInteractionRequest,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        Box::pin(async { Err(tool_error()) })
    }
    fn start(
        &self,
        a: &AttemptContext,
        id: InvocationId,
        _: ToolStartMode,
        _: ToolExecutionStart,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        let a = a.clone();
        Box::pin(async move {
            let mut invocations = lock(&self.invocations);
            let durable = invocations.get_mut(&id).ok_or(tool_error())?;
            durable.invocation.status = ToolInvocationStatus::Running;
            durable.invocation.execution_attempt_id = Some(a.attempt_id);
            let material = self
                .materials
                .values()
                .find(|m| m.tool.manifest.key == durable.invocation.tool_key)
                .expect("valid fixture and successful tested operation");
            self.home.begin(
                &self.context,
                id,
                durable.invocation.request_ref.sha256,
                material
                    .tool
                    .manifest
                    .execution_policy
                    .liveness()
                    .expect("valid fixture and successful tested operation")
                    .clone(),
            );
            Ok(durable.clone())
        })
    }
    fn complete(
        &self,
        _: &AttemptContext,
        id: InvocationId,
        outcome: ToolTerminalOutcome,
        _: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<DurableToolInvocation, ToolExecutionPortError>> {
        Box::pin(async move {
            let mut all = lock(&self.invocations);
            let durable = all.get_mut(&id).ok_or(tool_error())?;
            if let Some(old) = &durable.recorded_outcome {
                assert_eq!(old, &outcome);
                return Ok(durable.clone());
            }
            durable.invocation.status = match &outcome {
                ToolTerminalOutcome::Succeeded(result) => {
                    durable.invocation.result_ref = Some(result.result_ref.clone());
                    ToolInvocationStatus::Succeeded
                }
                ToolTerminalOutcome::Failed { .. } => ToolInvocationStatus::Failed,
                ToolTerminalOutcome::Cancelled(_) => ToolInvocationStatus::Cancelled,
                _ => return Err(tool_error()),
            };
            durable.recorded_outcome = Some(outcome);
            self.home.close(id);
            Ok(durable.clone())
        })
    }
}
impl CapabilityDispatcher for ToolFixture {
    fn execution<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, LoopPortError> {
        Ok(Box::new(Operation {
            before_execute: lock(&self.before_execute).clone(),
            content: self.content.clone(),
            executions: self.executions.clone(),
            result: self.result.clone(),
            invocation,
            control: Arc::new(LocalControl(binding)),
        }))
    }
    fn invoke<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a DurableToolInvocation,
        _: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async { Err(loop_error()) })
    }
    fn reconcile<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a ToolExecutionMaterial,
        _: &'a DurableToolInvocation,
        _: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async { Err(loop_error()) })
    }
}
pub struct Operation {
    pub before_execute: Option<BeforeExecute>,
    pub content: Arc<Content>,
    pub executions: Arc<Mutex<Vec<serde_json::Value>>>,
    pub result: Arc<Mutex<serde_json::Value>>,
    pub invocation: CapabilityInvocation,
    pub control: Arc<LocalControl>,
}
impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> for Operation {
    fn binding(&self) -> ExecutionBinding {
        self.control.0
    }
    fn stop_control(&self) -> Arc<dyn ExecutionStopControl> {
        self.control.clone()
    }
    fn execute(
        self: Box<Self>,
        _: Arc<dyn ExecutionActivitySink>,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move {
            let bytes = self
                .content
                .read(self.invocation.tenant_id, &self.invocation.request_ref)
                .await
                .map_err(|_| loop_error())?;
            let arguments = serde_json::from_slice(&bytes)
                .expect("valid fixture and successful tested operation");
            if let Some(observe) = &self.before_execute {
                observe();
            }
            lock(&self.executions).push(arguments);
            let bytes = serde_json::to_vec(&*lock(&self.result))
                .expect("valid fixture and successful tested operation");
            let reference = self
                .content
                .put(PutContent {
                    tenant_id: self.invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .expect("valid fixture and successful tested operation"),
                    bytes,
                })
                .await
                .map_err(|_| loop_error())?;
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: reference.sha256,
                    result_ref: reference,
                    result_schema: self.invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }
}
fn material(context: &ExtensionContext, d: &CanonicalToolDefinition) -> ToolExecutionMaterial {
    let scopes = BTreeSet::from([
        DottedKey::new("test.execute").expect("valid fixture and successful tested operation")
    ]);
    let provider = CapabilityProviderId::generate();
    let input = serde_json::to_vec(&canonical_arguments(&d.input_schema))
        .expect("valid fixture and successful tested operation");
    let output = b"{\"type\":\"object\"}".to_vec();
    let manifest = ToolManifest {
        application: None,
        key: d.key.clone(),
        wire_name: d.name.clone(),
        input_schema: d.input_schema_ref.clone(),
        output_schema: SchemaRef {
            schema_key: key("test.output@1"),
            schema_hash: Sha256::digest(&output),
        },
        capability_key: key("test.execute@1"),
        capability_provider_id: provider,
        effect: Effect::Read,
        approval: ApprovalRequirement::Never,
        execution_policy: ToolExecutionPolicy::admitted_activity(
            key("test.execution@1"),
            Duration::from_secs(60),
        ),
        max_attempts: NonZeroU32::MIN,
        retry_contract: ToolRetryContract::ReadOnly,
        required_scopes: scopes.clone(),
        required_contexts: BTreeSet::new(),
        data_classification: DataClassification::Public,
        egress_policy: EgressPolicy::None,
    };
    let tool = ToolDefinitionSnapshot {
        manifest,
        execution_target: CapabilityExecutionTarget::RustRuntime,
        concurrency_class: ConcurrencyClass::ParallelRead,
        idempotency: BuiltinIdempotency::Native,
        file_operation: None,
        title: "Fixture".into(),
        description: "Fixture tool".into(),
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: input,
        output_schema_json: output,
    };
    tool.validate()
        .expect("valid fixture and successful tested operation");
    let grant = CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(key("test.execute@1")),
        provider_id: provider,
        location: CapabilityLocation::Cloud,
        effect: Effect::Read,
        allowed_scopes: scopes.clone(),
        policy_decision: Decision::Allow,
    };
    let authorized = musteragent_runtime_snapshot::authorized_tool_catalog(
        std::slice::from_ref(&tool),
        std::slice::from_ref(&grant),
    )
    .expect("valid fixture and successful tested operation");
    let exposure = musteragent_discovery::ExposureState::from_initial(&authorized, &[])
        .expect("valid fixture and successful tested operation");
    ToolExecutionMaterial {
        application_capabilities: vec![],
        product_release: None,
        run_context: context.run,
        recovery_policy: NonZeroU32::new(8)
            .expect("valid fixture and successful tested operation")
            .into(),
        actor: Actor {
            tenant_id: context.run.tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        agent_id: AgentId::generate(),
        model: ModelRouteSnapshot {
            requested_model_class: musteragent_runtime_contract::ModelClass::new("test")
                .expect("valid fixture and successful tested operation"),
            route: route(),
            credential_grant_ref: CredentialGrantRef::new("grant_fixture")
                .expect("valid fixture and successful tested operation"),
            fallback_candidates: vec![],
        },
        active_model_route_ordinal: 0,
        auxiliary_models: vec![],
        model_stream_budget: execution_snapshot().model_stream,
        model_liveness: Some(Arc::new(
            execution_snapshot()
                .liveness
                .expect("valid fixture and successful tested operation")
                .model_policy(),
        )),
        tool,
        authorized_tools: authorized,
        discovery: None,
        exposure_state: exposure,
        capability_grant: grant,
        authorization: InvocationAuthorization {
            actor_scopes: scopes,
            available_contexts: BTreeSet::new(),
            policy_requires_approval: false,
            approval_forbidden: false,
            manifest_approval_not_applicable: false,
            approval_satisfied: false,
        },
        permission_mode: PermissionMode::FullAccess,
        policy_basis: None,
        authorized_content: vec![],
        prior_tool_results: vec![],
        execution_grant_proof: None,
    }
}
