use super::*;
use musteragent_execution_liveness::{
    ExecutionActivitySink, ExecutionOperation, ExecutionStopControl,
};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityOutcome, CapabilityResult};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::execution_liveness::ExecutionBinding;
use musteragent_tool_executor::{DispatchedCapabilityOutcome, ToolPhysicalExecution};
use serde::{Deserialize, Serialize};

struct Owner;
impl KernelExtension for Owner {
    fn id(&self) -> &str {
        "registered"
    }
}
#[derive(Deserialize)]
struct Config {
    label: String,
}
struct Probe {
    content: Mutex<Option<Arc<executor::Content>>>,
    scopes: Arc<Mutex<Vec<ExtensionContext>>>,
}
impl ExtensionTool for Probe {
    fn execution<'a>(
        &'a self,
        context: ExtensionToolContext<'a>,
        invocation: CapabilityInvocation,
        binding: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, HookError> {
        let config: Config = context
            .api
            .config()
            .map_err(|e| HookError::new("config", e.to_string()))?;
        assert_eq!(config.label, "typed config");
        lock(&self.scopes).push(context.execution.clone());
        let content = lock(&self.content)
            .clone()
            .expect("valid fixture and successful tested operation");
        Ok(Box::new(MemoryOperation {
            content,
            invocation,
            api: context.api,
            control: Arc::new(LocalControl(binding)),
        }))
    }
}
struct MemoryOperation {
    content: Arc<executor::Content>,
    invocation: CapabilityInvocation,
    api: RuntimeApi,
    control: Arc<LocalControl>,
}
impl<'a> ExecutionOperation<'a, Result<DispatchedCapabilityOutcome, LoopPortError>>
    for MemoryOperation
{
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
            let path = self
                .api
                .state_dir()
                .expect("valid fixture and successful tested operation");
            assert!(path.is_dir());
            let value = self
                .api
                .memory(MemoryRequest {
                    operation: "recall".into(),
                    arguments: serde_json::json!({"query":"profile"}),
                })
                .await
                .expect("valid fixture and successful tested operation");
            let result_ref = self
                .content
                .put(PutContent {
                    tenant_id: self.invocation.tenant_id,
                    media_type: MediaType::new("application/json")
                        .expect("valid fixture and successful tested operation"),
                    bytes: serde_json::to_vec(&value)
                        .expect("valid fixture and successful tested operation"),
                })
                .await
                .expect("valid fixture and successful tested operation");
            Ok(DispatchedCapabilityOutcome::Provider(
                CapabilityOutcome::Succeeded(CapabilityResult {
                    result_hash: result_ref.sha256,
                    result_ref,
                    result_schema: self.invocation.result_schema,
                    provider_request_ref: None,
                }),
            ))
        })
    }
}
#[derive(Default)]
struct Memory {
    requests: Mutex<Vec<MemoryRequest>>,
}
impl MemoryProvider for Memory {
    fn call<'a>(
        &'a self,
        _: &'a ExtensionContext,
        request: MemoryRequest,
    ) -> ApplicationFuture<'a, Result<serde_json::Value, HookError>> {
        Box::pin(async move {
            lock(&self.requests).push(request);
            Ok(serde_json::json!({"memory":"stored"}))
        })
    }
}
struct Wrapper {
    id: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}
impl MemoryToolWrapper for Wrapper {
    fn before(&self, mut request: MemoryRequest) -> Result<MemoryRequest, HookError> {
        lock(&self.log).push(format!("before:{}", self.id));
        request.arguments[self.id] = true.into();
        Ok(request)
    }
    fn after(&self, _: &MemoryRequest, _: &serde_json::Value) -> Result<(), HookError> {
        lock(&self.log).push(format!("after:{}", self.id));
        Ok(())
    }
}
#[tokio::test]
async fn registered_tool_receives_typed_context_config_state_and_wrapped_memory() {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(Owner), serde_json::json!({"label":"typed config"}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_extension(Arc::new(samples::MemoryExtraction), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    let scopes = Arc::new(Mutex::new(vec![]));
    let probe = Arc::new(Probe {
        content: Mutex::new(None),
        scopes: scopes.clone(),
    });
    registry
        .register_tool(RegisteredTool {
            owner: "registered".into(),
            definition: definition("memory"),
            implementation: probe.clone(),
        })
        .expect("valid fixture and successful tested operation");
    let memory = Arc::new(Memory::default());
    registry
        .register_memory(memory.clone())
        .expect("valid fixture and successful tested operation");
    assert!(
        matches!(registry.register_memory(Arc::new(Memory::default())),Err(ExtensionError::Duplicate(slot)) if slot=="memory")
    );
    let log = Arc::new(Mutex::new(vec![]));
    for id in ["first", "second"] {
        registry.wrap_memory_tools(Arc::new(Wrapper {
            id,
            log: log.clone(),
        }));
    }
    let fixture = Fixture::new(
        registry,
        vec![
            tool_events("one", "memory", serde_json::json!({})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    *lock(&probe.content) = Some(fixture.tools.content.clone());
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    let scope = lock(&scopes)[0].clone();
    assert_eq!(scope.run.run_id, fixture.context.run.run_id);
    assert_eq!(scope.workspace_dir, fixture.context.workspace_dir);
    assert_eq!(scope.session_key, fixture.context.session_key);
    assert_eq!(scope.channel, Some("test-channel".into()));
    assert!(scope.sandboxed);
    assert_eq!(
        fixture
            .facts()
            .await
            .iter()
            .filter(|f| matches!(f.fact, ExtensionFact::MemoryRewritten { .. }))
            .count(),
        4
    );
    let requests = lock(&memory.requests);
    assert_eq!(
        requests
            .iter()
            .map(|r| r.operation.as_str())
            .collect::<Vec<_>>(),
        vec!["recall", "extract"]
    );
    for request in requests.iter() {
        assert_eq!(request.arguments["first"], true);
        assert_eq!(request.arguments["second"], true);
    }
    assert_eq!(
        *lock(&log),
        vec![
            "before:first",
            "before:second",
            "after:first",
            "after:second",
            "before:first",
            "before:second",
            "after:first",
            "after:second"
        ]
    );
    assert_eq!(
        lock(&fixture.tools.invocations).len(),
        1,
        "registered tools use real durable invocation journal"
    );
}
#[tokio::test]
async fn plan_command_and_profile_samples_change_the_real_model_input() {
    let plan = Arc::new(samples::PlanMode::default());
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(plan.clone(), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_command("plan", "plan", plan)
        .expect("valid fixture and successful tested operation");
    registry
        .register_extension(
            Arc::new(samples::UserProfileIngress {
                profile: "Prefers concise answers".into(),
            }),
            serde_json::json!({}),
        )
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "/plan on");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    let request = lock(&fixture.provider.requests)[0].clone();
    assert!(!format!("{:?}", request.input).contains("/plan on"));
    assert!(
        request
            .instructions
            .iter()
            .any(|t| t.as_str().contains("Plan mode:"))
    );
    assert!(
        request
            .instructions
            .iter()
            .any(|t| t.as_str().contains("Prefers concise answers"))
    );
    assert!(
        fixture
            .facts()
            .await
            .iter()
            .any(|r| matches!(r.fact, ExtensionFact::CommandHandled { .. }))
    );
}
struct Engine {
    planner: Mutex<Option<Arc<Planner>>>,
    calls: Arc<Mutex<usize>>,
}
impl ContextPlanner for Engine {
    fn prepare_model_iteration(
        &self,
        _: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        Box::pin(async { Err(ContextPlanError::invariant()) })
    }
    fn prepare_model_iteration_with_projection(
        &self,
        input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
        projection: Arc<dyn ContextViewProjection>,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let planner = lock(&self.planner)
            .clone()
            .expect("valid fixture and successful tested operation");
        *lock(&self.calls) += 1;
        Box::pin(async move {
            planner
                .prepare_model_iteration_with_projection(input, profile, projection)
                .await
        })
    }
}
#[tokio::test]
async fn context_engine_slot_is_exclusive_and_selected_by_real_loop() {
    let engine = Arc::new(Engine {
        planner: Mutex::new(None),
        calls: Arc::new(Mutex::new(0)),
    });
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(Owner), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_context_engine("registered", engine.clone())
        .expect("valid fixture and successful tested operation");
    assert!(
        matches!(registry.register_context_engine("registered",engine.clone()),Err(ExtensionError::Duplicate(slot)) if slot=="context_engine")
    );
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
    *lock(&engine.planner) = Some(Arc::new(Planner {
        context: fixture.context.clone(),
        journal: fixture.journal.clone(),
        input: "Chosen context engine".into(),
        definitions: vec![],
    }));
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(*lock(&engine.calls), 1);
    assert!(
        format!("{:?}", lock(&fixture.provider.requests)[0].input)
            .contains("Chosen context engine")
    );
}
#[derive(Deserialize)]
struct MethodInput {
    value: u32,
}
#[derive(Serialize, Deserialize)]
struct MethodOutput {
    value: u32,
    run: String,
}
struct Method;
impl GatewayMethod for Method {
    type Request = MethodInput;
    type Response = MethodOutput;
    fn call<'a>(
        &'a self,
        c: &'a ExtensionContext,
        r: MethodInput,
        _: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<MethodOutput, HookError>> {
        Box::pin(async move {
            Ok(MethodOutput {
                value: r.value + 1,
                run: c.run.run_id.to_string(),
            })
        })
    }
}
struct MethodCaller;
impl KernelExtension for MethodCaller {
    fn id(&self) -> &str {
        "method-caller"
    }
    fn lifecycle<'a>(
        &'a self,
        c: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            if matches!(event, LifecycleEvent::AgentStart) {
                let config: serde_json::Value = api
                    .config()
                    .expect("valid fixture and successful tested operation");
                assert_eq!(config["method"], "increment");
                assert!(!c.session_key.is_empty());
                let response = api
                    .gateway(GatewayRequest {
                        method: "increment".into(),
                        payload: serde_json::json!({"value":9}),
                    })
                    .await
                    .expect("valid fixture and successful tested operation");
                assert_eq!(response.payload["value"], 10);
            }
            Ok(())
        })
    }
}
#[tokio::test]
async fn typed_host_method_registry_works_without_transport_and_rejects_bad_requests() {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(MethodCaller),
            serde_json::json!({"method":"increment"}),
        )
        .expect("valid fixture and successful tested operation");
    registry
        .register_gateway_method("method-caller", "increment", Method)
        .expect("valid fixture and successful tested operation");
    assert!(matches!(
        registry.register_gateway_method("method-caller", "increment", Method),
        Err(ExtensionError::Duplicate(_))
    ));
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
    let response = fixture
        .host
        .gateway(
            &fixture.context,
            GatewayRequest {
                method: "increment".into(),
                payload: serde_json::json!({"value":4}),
            },
        )
        .await
        .expect("valid fixture and successful tested operation");
    assert_eq!(response.payload["value"], 5);
    assert_eq!(
        response.payload["run"],
        fixture.context.run.run_id.to_string()
    );
    assert!(
        fixture
            .host
            .gateway(
                &fixture.context,
                GatewayRequest {
                    method: "increment".into(),
                    payload: serde_json::json!({"value":"bad"})
                }
            )
            .await
            .is_err()
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
}

#[tokio::test]
async fn historical_slash_text_never_executes_without_current_ingress() {
    let plan = Arc::new(samples::PlanMode::default());
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(plan.clone(), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_command("plan", "plan", plan)
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::with_ports(
        registry,
        vec![assistant()],
        Default::default(),
        "/plan on",
        context(),
        None,
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        !fixture
            .facts()
            .await
            .iter()
            .any(|f| matches!(f.fact, ExtensionFact::CommandHandled { .. }))
    );
    assert!(
        !lock(&fixture.provider.requests)[0]
            .instructions
            .iter()
            .any(|t| t.as_str().contains("Plan mode:"))
    );
}

#[tokio::test]
async fn unknown_current_command_fails_before_model_and_receipts_reject_reused_input_identity() {
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        vec![assistant()],
        Default::default(),
        "/unknown",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            ..
        }
    ));
    assert!(lock(&fixture.provider.requests).is_empty());
    let plan = Arc::new(samples::PlanMode::default());
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(plan.clone(), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_command("plan", "plan", plan)
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "/plan on");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(matches!(
        fixture
            .host
            .command(
                &fixture.context,
                &CommandInput {
                    id: "input-1".into(),
                    text: text("/plan off")
                }
            )
            .await,
        Err(ExtensionError::IdentityMismatch)
    ));
}

struct PanickingTool;
impl ExtensionTool for PanickingTool {
    fn execution<'a>(
        &'a self,
        _: ExtensionToolContext<'a>,
        _: CapabilityInvocation,
        _: ExecutionBinding,
    ) -> Result<ToolPhysicalExecution<'a>, HookError> {
        panic!("fixture constructor panic")
    }
}
#[tokio::test]
async fn registered_tool_panic_is_recorded_through_durable_executor() {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(Owner), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    registry
        .register_tool(RegisteredTool {
            owner: "registered".into(),
            definition: definition("probe"),
            implementation: Arc::new(PanickingTool),
        })
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::new(
        registry,
        vec![
            tool_events("one", "probe", serde_json::json!({})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    fixture.run().await;
    assert!(fixture.facts().await.iter().any(|f| matches!(
        &f.fact,
        ExtensionFact::HookFailed {
            failure: HookFailure {
                hook: Hook::Tool,
                kind: HookFailureKind::Panic,
                ..
            },
            continues: false
        }
    )));
    assert_eq!(lock(&fixture.tools.invocations).len(), 1);
    assert!(lock(&fixture.tools.executions).is_empty());
}
