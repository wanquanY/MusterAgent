use super::*;

struct Recorder {
    id: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    spawn: bool,
}
impl KernelExtension for Recorder {
    fn id(&self) -> &str {
        self.id
    }
    fn lifecycle<'a>(
        &'a self,
        _: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            lock(&self.log).push(format!("{}:{:?}", self.id, event.hook()));
            if self.spawn && matches!(event, LifecycleEvent::AgentStart) {
                api.spawn_subagent(SubagentRequest {
                    task: "inspect".into(),
                    agent: "research".into(),
                    idempotency_key: "child-one".into(),
                })
                .await
                .map_err(|e| HookError::new("spawn", e.to_string()))?;
            }
            Ok(())
        })
    }
}
struct Spawner {
    parents: Mutex<Vec<RunId>>,
    child: RunId,
}
impl SubagentSpawner for Spawner {
    fn spawn<'a>(
        &'a self,
        parent: &'a ExtensionContext,
        request: SubagentRequest,
    ) -> ApplicationFuture<'a, Result<SubagentReceipt, HookError>> {
        Box::pin(async move {
            assert_eq!(request.task, "inspect");
            lock(&self.parents).push(parent.run.run_id);
            Ok(SubagentReceipt { run_id: self.child })
        })
    }
}
struct Service {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    fail_start: bool,
    fail_stop: bool,
}
impl ExtensionService for Service {
    fn start<'a>(
        &'a self,
        _: &'a ExtensionContext,
        _: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            lock(&self.log).push(format!("start:{}", self.name));
            if self.fail_start {
                Err(HookError::new("start_failed", "fixture"))
            } else {
                Ok(())
            }
        })
    }
    fn stop<'a>(
        &'a self,
        _: &'a ExtensionContext,
        _: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            lock(&self.log).push(format!("stop:{}", self.name));
            if self.fail_stop {
                Err(HookError::new("stop_failed", "fixture"))
            } else {
                Ok(())
            }
        })
    }
}
#[tokio::test]
async fn lifecycle_services_and_subagents_run_in_registration_order() {
    let log = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    for (id, spawn) in [("first", true), ("second", false)] {
        registry
            .register_extension(
                Arc::new(Recorder {
                    id,
                    log: log.clone(),
                    spawn,
                }),
                serde_json::json!({}),
            )
            .expect("valid fixture and successful tested operation");
        registry
            .register_service(
                id,
                id,
                Arc::new(Service {
                    name: id,
                    log: log.clone(),
                    fail_start: false,
                    fail_stop: false,
                }),
            )
            .expect("valid fixture and successful tested operation");
    }
    let spawner = Arc::new(Spawner {
        parents: Mutex::new(vec![]),
        child: RunId::generate(),
    });
    let fixture = Fixture::with_ports(
        registry,
        vec![assistant()],
        Default::default(),
        "Hello",
        context(),
        Some(spawner.clone()),
    );
    fixture
        .host
        .start_services(&fixture.context)
        .await
        .expect("valid fixture and successful tested operation");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    fixture
        .host
        .lifecycle(
            &fixture.context,
            LifecycleEvent::SubagentEnded {
                run_id: spawner.child,
                report: serde_json::json!({"done":true}),
            },
        )
        .await
        .expect("valid fixture and successful tested operation");
    fixture
        .host
        .lifecycle(&fixture.context, LifecycleEvent::SessionEnd)
        .await
        .expect("valid fixture and successful tested operation");
    fixture
        .host
        .lifecycle(&fixture.context, LifecycleEvent::SessionShutdown)
        .await
        .expect("valid fixture and successful tested operation");
    fixture
        .host
        .stop_services(&fixture.context)
        .await
        .expect("valid fixture and successful tested operation");
    assert_eq!(*lock(&spawner.parents), vec![fixture.context.run.run_id]);
    assert_eq!(
        *lock(&log),
        vec![
            "start:first",
            "start:second",
            "first:AgentStart",
            "first:SubagentSpawned",
            "second:SubagentSpawned",
            "second:AgentStart",
            "first:AgentEnd",
            "second:AgentEnd",
            "first:SubagentEnded",
            "second:SubagentEnded",
            "first:SessionEnd",
            "second:SessionEnd",
            "first:SessionShutdown",
            "second:SessionShutdown",
            "stop:first",
            "stop:second"
        ]
    );
}
#[tokio::test]
async fn service_start_failure_rolls_back_and_stop_failure_is_visible() {
    let log = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Recorder {
                id: "service-owner",
                log: Arc::new(Mutex::new(vec![])),
                spawn: false,
            }),
            serde_json::json!({}),
        )
        .expect("valid fixture and successful tested operation");
    for (name, fail_start) in [("first", false), ("second", true)] {
        registry
            .register_service(
                "service-owner",
                name,
                Arc::new(Service {
                    name,
                    log: log.clone(),
                    fail_start,
                    fail_stop: false,
                }),
            )
            .expect("valid fixture and successful tested operation");
    }
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
    assert!(matches!(
        fixture.host.start_services(&fixture.context).await,
        Err(ExtensionError::Hook(_))
    ));
    assert_eq!(
        *lock(&log),
        vec!["start:first", "start:second", "stop:first"]
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Recorder {
                id: "service-owner",
                log: Arc::new(Mutex::new(vec![])),
                spawn: false,
            }),
            serde_json::json!({}),
        )
        .expect("valid fixture and successful tested operation");
    for name in ["first", "second"] {
        registry
            .register_service(
                "service-owner",
                name,
                Arc::new(Service {
                    name,
                    log: log.clone(),
                    fail_start: false,
                    fail_stop: name == "first",
                }),
            )
            .expect("valid fixture and successful tested operation");
    }
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
    fixture
        .host
        .start_services(&fixture.context)
        .await
        .expect("valid fixture and successful tested operation");
    fixture.run().await;
    let failures = fixture
        .host
        .stop_services(&fixture.context)
        .await
        .expect("valid fixture and successful tested operation");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].hook, Hook::ServiceStop);
    assert!(fixture.facts().await.iter().any(|f| matches!(
        &f.fact,
        ExtensionFact::HookFailed {
            continues: true,
            ..
        }
    )));
}
struct Fault {
    hook: Hook,
    panic: bool,
}
impl Fault {
    fn check(&self, hook: Hook) -> Result<(), HookError> {
        if self.hook == hook {
            assert!(!self.panic, "deliberate hook panic");
            return Err(HookError::new("hook_rejected", "fixture rejection"));
        }
        Ok(())
    }
}
impl KernelExtension for Fault {
    fn id(&self) -> &str {
        "fault"
    }
    fn prompt_build(
        &self,
        _: &ExtensionContext,
        _: &CanonicalModelRequest,
    ) -> Result<PromptContribution, HookError> {
        self.check(Hook::PromptBuild)?;
        Ok(Default::default())
    }
    fn context_projection(
        &self,
        _: &ExtensionContext,
        view: ContextView,
    ) -> Result<ContextView, HookError> {
        self.check(Hook::ContextProjection)?;
        Ok(view)
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        self.check(Hook::BeforeToolCall)?;
        Ok(ToolDecision::Allow)
    }
    fn after_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
        _: &ToolExecutionOutcome,
    ) -> Result<(), HookError> {
        self.check(Hook::AfterToolCall)
    }
    fn tool_result_persist(
        &self,
        _: &ExtensionContext,
        message: CanonicalMessage,
    ) -> Result<CanonicalMessage, HookError> {
        self.check(Hook::ToolResultPersist)?;
        Ok(message)
    }
    fn before_provider_request(
        &self,
        _: &ExtensionContext,
        _: &CanonicalModelRequest,
        _: &mut ModelRequestAnnotations,
    ) -> Result<(), HookError> {
        self.check(Hook::BeforeProviderRequest)
    }
    fn llm_input(&self, _: &ExtensionContext, _: &CanonicalModelRequest) -> Result<(), HookError> {
        self.check(Hook::LlmInput)
    }
    fn llm_output(
        &self,
        _: &ExtensionContext,
        _: InvocationId,
        _: &ModelStreamEvent,
    ) -> Result<(), HookError> {
        self.check(Hook::LlmOutput)
    }
    fn lifecycle<'a>(
        &'a self,
        _: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        _: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move { self.check(event.hook()) })
    }
}
async fn fault(hook: Hook, panic: bool, continues: bool) {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(Fault { hook, panic }), serde_json::json!({}))
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
    let report = fixture.run().await;
    assert_eq!(
        matches!(report.exit, AgentLoopExit::Completed { .. }),
        continues,
        "{hook:?}: {:?}",
        report.exit
    );
    let facts = fixture.facts().await;
    let failures = facts
        .iter()
        .filter_map(|f| {
            if let ExtensionFact::HookFailed { failure, continues } = &f.fact {
                Some((failure, *continues))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert!(
        failures.iter().any(|(f, c)| f.hook == hook
            && *c == continues
            && matches!(
                (&f.kind, panic),
                (HookFailureKind::Panic, true) | (HookFailureKind::Error(_), false)
            )),
        "{facts:?}"
    );
    if hook == Hook::AgentEnd {
        assert_eq!(report.observer_failures.len(), 1);
    }
    if matches!(hook, Hook::AfterToolCall | Hook::ToolResultPersist) {
        assert_eq!(lock(&fixture.journal.results).len(), 1);
    }
}
macro_rules! hook_test {
    ($name:ident,$hook:ident,$panic:expr,$continues:expr) => {
        #[tokio::test]
        async fn $name() {
            fault(Hook::$hook, $panic, $continues).await;
        }
    };
}
hook_test!(
    agent_start_error_aborts_and_is_recorded,
    AgentStart,
    false,
    false
);
hook_test!(
    agent_start_panic_aborts_and_is_recorded,
    AgentStart,
    true,
    false
);
hook_test!(agent_end_error_records_and_continues, AgentEnd, false, true);
hook_test!(agent_end_panic_records_and_continues, AgentEnd, true, true);
hook_test!(
    prompt_error_aborts_before_dispatch,
    PromptBuild,
    false,
    false
);
hook_test!(
    prompt_panic_aborts_before_dispatch,
    PromptBuild,
    true,
    false
);
hook_test!(
    context_projection_panic_is_typed,
    ContextProjection,
    true,
    false
);
hook_test!(
    before_tool_error_aborts_without_effect,
    BeforeToolCall,
    false,
    false
);
hook_test!(
    before_tool_panic_aborts_without_effect,
    BeforeToolCall,
    true,
    false
);
hook_test!(
    after_tool_error_preserves_original_result,
    AfterToolCall,
    false,
    true
);
hook_test!(
    after_tool_panic_preserves_original_result,
    AfterToolCall,
    true,
    true
);
hook_test!(
    result_projection_error_keeps_raw_fact,
    ToolResultPersist,
    false,
    false
);
hook_test!(
    result_projection_panic_keeps_raw_fact,
    ToolResultPersist,
    true,
    false
);
hook_test!(
    provider_annotation_error_aborts,
    BeforeProviderRequest,
    false,
    false
);
hook_test!(
    provider_annotation_panic_aborts,
    BeforeProviderRequest,
    true,
    false
);
hook_test!(input_observer_error_is_recorded, LlmInput, false, true);
hook_test!(input_observer_panic_is_recorded, LlmInput, true, true);
hook_test!(output_observer_error_is_recorded, LlmOutput, false, true);
hook_test!(output_observer_panic_is_recorded, LlmOutput, true, true);
#[tokio::test]
async fn cancellation_still_notifies_agent_end() {
    let log = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Recorder {
                id: "observer",
                log: log.clone(),
                spawn: false,
            }),
            serde_json::json!({}),
        )
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::new(registry, vec![], Default::default(), "Hello");
    let supervisor = InvocationSupervisor::new();
    supervisor.cancel();
    let report = fixture
        .runtime
        .execute(telemetry(&fixture.context), supervisor)
        .await
        .expect("valid fixture and successful tested operation");
    assert!(matches!(report.exit, AgentLoopExit::Cancelled { .. }));
    assert!(lock(&log).contains(&"observer:AgentEnd".into()));
}
struct OutboxObserver {
    seen: Arc<Mutex<Vec<String>>>,
    fail: bool,
}
impl KernelExtension for OutboxObserver {
    fn id(&self) -> &str {
        if self.fail { "failed-outbox" } else { "outbox" }
    }
    fn outbox(
        &self,
        _: &ExtensionContext,
        event: &musteragent_ledger::CommittedOutbox,
    ) -> Result<(), HookError> {
        lock(&self.seen).push(event.message.topic.as_str().into());
        if self.fail {
            Err(HookError::new("outbox_failed", "fixture observer failure"))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn committed_outbox_subscription_is_ordered_scoped_and_does_not_recurse() {
    let seen = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    for fail in [false, true] {
        registry
            .register_extension(
                Arc::new(OutboxObserver {
                    seen: seen.clone(),
                    fail,
                }),
                serde_json::json!({}),
            )
            .expect("valid fixture and successful tested operation");
    }
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
    let messages = fixture
        .ledger
        .pending_outbox()
        .expect("valid fixture and successful tested operation");
    let mut failures = vec![];
    for message in &messages {
        failures.extend(
            fixture
                .host
                .consume_outbox(&fixture.context, message)
                .await
                .expect("valid fixture and successful tested operation"),
        );
    }
    assert_eq!(failures.len(), 2);
    assert_eq!(
        *lock(&seen),
        vec![
            "test.tool.result",
            "test.tool.result",
            "test.run.completed",
            "test.run.completed"
        ]
    );
    let extension_messages = fixture
        .ledger
        .pending_outbox()
        .expect("valid fixture and successful tested operation")
        .into_iter()
        .filter(|m| m.message.topic.as_str() == EXTENSION_OUTBOX_TOPIC)
        .collect::<Vec<_>>();
    let old = lock(&seen).len();
    for message in &extension_messages {
        fixture
            .host
            .consume_outbox(&fixture.context, message)
            .await
            .expect("valid fixture and successful tested operation");
    }
    assert_eq!(lock(&seen).len(), old);
    let mut foreign = messages[0].clone();
    foreign.tenant_id = TenantId::generate();
    assert!(matches!(
        fixture
            .host
            .consume_outbox(&fixture.context, &foreign)
            .await,
        Err(ExtensionError::IdentityMismatch)
    ));
}
#[tokio::test]
async fn session_terminal_errors_are_recorded_and_later_observers_still_run() {
    for hook in [Hook::SessionEnd, Hook::SessionShutdown, Hook::SubagentEnded] {
        let log = Arc::new(Mutex::new(vec![]));
        let mut registry = ExtensionRegistry::default();
        registry
            .register_extension(Arc::new(Fault { hook, panic: true }), serde_json::json!({}))
            .expect("valid fixture and successful tested operation");
        registry
            .register_extension(
                Arc::new(Recorder {
                    id: "later",
                    log: log.clone(),
                    spawn: false,
                }),
                serde_json::json!({}),
            )
            .expect("valid fixture and successful tested operation");
        let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
        fixture.run().await;
        let event = match hook {
            Hook::SessionEnd => LifecycleEvent::SessionEnd,
            Hook::SessionShutdown => LifecycleEvent::SessionShutdown,
            _ => LifecycleEvent::SubagentEnded {
                run_id: RunId::generate(),
                report: serde_json::json!({"done":true}),
            },
        };
        assert_eq!(
            fixture
                .host
                .lifecycle(&fixture.context, event)
                .await
                .expect("valid fixture and successful tested operation")
                .len(),
            1
        );
        assert!(lock(&log).contains(&format!("later:{hook:?}")));
    }
}

struct AuditOutage {
    inner: Arc<dyn ExtensionJournal>,
    offline: std::sync::atomic::AtomicBool,
}
impl ExtensionJournal for AuditOutage {
    fn append<'a>(
        &'a self,
        c: &'a ExtensionContext,
        key: &'a str,
        fact: ExtensionFact,
    ) -> ApplicationFuture<'a, Result<(), ExtensionError>> {
        if self.offline.load(std::sync::atomic::Ordering::SeqCst) {
            Box::pin(async { Err(ExtensionError::Audit("fixture outage".into())) })
        } else {
            self.inner.append(c, key, fact)
        }
    }
    fn select<'a>(
        &'a self,
        c: &'a ExtensionContext,
        query: FactQuery,
    ) -> ApplicationFuture<'a, Result<Vec<ExtensionFactRecord>, ExtensionError>> {
        self.inner.select(c, query)
    }
}
#[tokio::test]
async fn service_cleanup_continues_during_audit_outage_and_start_rolls_back() {
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        vec![assistant()],
        Default::default(),
        "Hello",
    );
    let log = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Recorder {
                id: "owner",
                log: log.clone(),
                spawn: false,
            }),
            serde_json::json!({}),
        )
        .expect("register owner");
    for name in ["first", "second"] {
        registry
            .register_service(
                "owner",
                name,
                Arc::new(Service {
                    name,
                    log: log.clone(),
                    fail_start: false,
                    fail_stop: false,
                }),
            )
            .expect("register service");
    }
    let audit = Arc::new(AuditOutage {
        inner: fixture.audit.clone(),
        offline: std::sync::atomic::AtomicBool::new(false),
    });
    let host = ExtensionHost::new(
        registry,
        audit.clone(),
        RuntimePorts {
            content: fixture.tools.content.clone(),
            history: fixture.journal.clone(),
            state_root: fixture.context.workspace_dir.clone(),
            subagents: None,
        },
        Default::default(),
    )
    .expect("host");
    host.start_services(&fixture.context).await.expect("start");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    audit
        .offline
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        host.stop_services(&fixture.context).await,
        Err(ExtensionError::Audit(_))
    ));
    assert_eq!(
        *lock(&log),
        vec!["start:first", "start:second", "stop:first", "stop:second"]
    );
    lock(&log).clear();
    assert!(matches!(
        host.start_services(&fixture.context).await,
        Err(ExtensionError::Audit(_))
    ));
    assert_eq!(*lock(&log), vec!["start:first", "stop:first"]);
}
