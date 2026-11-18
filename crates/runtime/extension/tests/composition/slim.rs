use super::*;
use musteragent_ledger::{LedgerError, LedgerStore};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Passive;
impl KernelExtension for Passive {
    fn id(&self) -> &str {
        "passive"
    }
}
fn registry() -> ExtensionRegistry {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(Passive), serde_json::json!({}))
        .expect("register");
    registry
}
fn policy(f: &Fixture, host: Arc<ExtensionHost>) -> ExtensionLoopPolicy {
    ExtensionLoopPolicy::new(f.policy.clone(), host, f.context.clone()).expect("policy")
}
fn step(f: &Fixture) -> StepContext {
    StepContext {
        attempt: f.context.attempt.clone(),
        checkpoint: checkpoint(&f.context),
        execution: PinnedRunExecutionPolicy {
            snapshot: execution_snapshot(),
            admitted_at: UtcTimestamp::from_unix_millis(1000),
            model_route_count: NonZeroU32::MIN,
        },
        now: UtcTimestamp::from_unix_millis(1000),
    }
}
#[tokio::test]
async fn untouched_tool_has_zero_extension_appends_and_outbox_messages() {
    let f = Fixture::new(registry(), vec![], Default::default(), "Hello");
    let policy = policy(&f, f.host.clone());
    let call = super::loops::history_call("untouched", "probe", serde_json::json!({}));
    let result = policy
        .execute_tool(&f.context.attempt, &telemetry(&f.context), call.clone())
        .await
        .expect("execute");
    policy
        .tool_result_committed(&call, &result, &step(&f))
        .await
        .expect("observe");
    assert!(f.facts().await.is_empty());
    assert!(f.ledger.pending_outbox().expect("outbox").is_empty());
    assert_eq!(lock(&f.tools.executions).len(), 1);
}
#[tokio::test]
async fn memory_rewrite_replays_exact_content_without_inline_payloads() {
    struct EchoMemory;
    impl MemoryProvider for EchoMemory {
        fn call<'a>(
            &'a self,
            _: &'a ExtensionContext,
            request: MemoryRequest,
        ) -> ApplicationFuture<'a, Result<serde_json::Value, HookError>> {
            Box::pin(async move { Ok(request.arguments) })
        }
    }
    struct RewriteMemory(&'static str, Arc<AtomicUsize>);
    impl MemoryToolWrapper for RewriteMemory {
        fn before(&self, mut request: MemoryRequest) -> Result<MemoryRequest, HookError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            request.arguments = serde_json::json!({"private": self.0});
            Ok(request)
        }
    }
    let visits = Arc::new(AtomicUsize::new(0));
    let register = |value| {
        let mut registry = registry();
        registry
            .register_memory(Arc::new(EchoMemory))
            .expect("memory");
        registry.wrap_memory_tools(Arc::new(RewriteMemory(value, visits.clone())));
        registry
    };
    let f = Fixture::new(
        register("original replacement"),
        vec![],
        Default::default(),
        "Hello",
    );
    let request = MemoryRequest {
        operation: "recall".into(),
        arguments: serde_json::json!({"private": "original input"}),
    };
    let first = f
        .host
        .api(&f.context, "passive")
        .expect("api")
        .memory(request.clone())
        .await
        .expect("memory");
    let facts = f.facts().await;
    assert_eq!(facts.len(), 1);
    let encoded = serde_json::to_string(&facts).expect("audit");
    assert!(!encoded.contains("original input"));
    assert!(!encoded.contains("original replacement"));
    let fresh = ExtensionHost::new(
        register("changed hook"),
        Arc::new(LedgerExtensionJournal::new(
            f.ledger.clone(),
            Arc::new(Clock),
        )),
        RuntimePorts {
            content: f.tools.content.clone(),
            history: f.journal.clone(),
            state_root: f.context.workspace_dir.clone(),
            subagents: None,
        },
        Default::default(),
    )
    .expect("fresh");
    let replay = fresh
        .api(&f.context, "passive")
        .expect("api")
        .memory(request)
        .await
        .expect("replay");
    assert_eq!(
        first,
        serde_json::json!({"private": "original replacement"})
    );
    assert_eq!(first, replay);
    assert_eq!(visits.load(Ordering::SeqCst), 1);
    assert_eq!(f.facts().await, facts);
}
struct BarrierHistory {
    inner: Arc<Journal>,
    barrier: tokio::sync::Barrier,
    entered: AtomicUsize,
}
impl ToolHistory for BarrierHistory {
    fn requested<'a>(
        &'a self,
        context: &'a ExtensionContext,
        call_id: &'a str,
    ) -> ApplicationFuture<'a, Result<bool, ExtensionError>> {
        self.inner.requested(context, call_id)
    }
    fn recent<'a>(
        &'a self,
        context: &'a ExtensionContext,
        through: Option<&'a str>,
        limit: usize,
        result_tools: &'a BTreeSet<String>,
    ) -> ApplicationFuture<'a, Result<Vec<ToolHistoryEntry>, ExtensionError>> {
        Box::pin(async move {
            if through.is_some() {
                self.entered.fetch_add(1, Ordering::SeqCst);
                self.barrier.wait().await;
            }
            self.inner
                .recent(context, through, limit, result_tools)
                .await
        })
    }
}
#[tokio::test]
async fn distinct_calls_overlap_inside_prepare() {
    let f = Fixture::new(registry(), vec![], Default::default(), "Hello");
    let history = Arc::new(BarrierHistory {
        inner: f.journal.clone(),
        barrier: tokio::sync::Barrier::new(2),
        entered: AtomicUsize::new(0),
    });
    let host = ExtensionHost::new(
        registry(),
        f.audit.clone(),
        RuntimePorts {
            state_root: f.context.workspace_dir.clone(),
            subagents: None,
            content: f.tools.content.clone(),
            history: history.clone(),
        },
        Default::default(),
    )
    .expect("host");
    let policy = policy(&f, host);
    let a = super::loops::history_call("a", "probe", serde_json::json!({"a":1}));
    let b = super::loops::history_call("b", "probe", serde_json::json!({"b":2}));
    let telemetry = telemetry(&f.context);
    let results = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            policy.execute_tool(&f.context.attempt, &telemetry, a),
            policy.execute_tool(&f.context.attempt, &telemetry, b)
        )
    })
    .await
    .expect("both preparations must reach the barrier concurrently");
    results.0.expect("a");
    results.1.expect("b");
    assert_eq!(history.entered.load(Ordering::SeqCst), 2);
    assert_eq!(lock(&f.tools.executions).len(), 2);
}
struct Rewrite {
    runs: Arc<AtomicUsize>,
    changed: bool,
    deny: bool,
}
impl KernelExtension for Rewrite {
    fn id(&self) -> &str {
        if self.deny { "deny" } else { "rewrite" }
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        if self.deny {
            Ok(ToolDecision::Deny(HookError::new(
                "blocked",
                "fixed denial",
            )))
        } else {
            Ok(ToolDecision::Rewrite(json(
                serde_json::json!({"private": if self.changed { "changed" } else { "exact-original-replacement" }}),
            )))
        }
    }
}
fn rewriting(runs: Arc<AtomicUsize>, changed: bool, deny: bool) -> ExtensionRegistry {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(Rewrite {
                runs: runs.clone(),
                changed,
                deny: false,
            }),
            serde_json::json!({}),
        )
        .expect("rewrite");
    if deny {
        registry
            .register_extension(
                Arc::new(Rewrite {
                    runs,
                    changed,
                    deny: true,
                }),
                serde_json::json!({}),
            )
            .expect("deny");
    }
    registry
}
#[tokio::test]
async fn rewritten_arguments_and_denials_survive_fresh_host_and_journal() {
    for deny in [false, true] {
        let runs = Arc::new(AtomicUsize::new(0));
        let f = Fixture::new(
            rewriting(runs.clone(), false, deny),
            vec![],
            Default::default(),
            "Hello",
        );
        let call =
            super::loops::history_call("replay", "probe", serde_json::json!({"private":"raw"}));
        let first = policy(&f, f.host.clone())
            .execute_tool(&f.context.attempt, &telemetry(&f.context), call.clone())
            .await;
        assert_eq!(first.is_err(), deny);
        let facts = f.facts().await;
        let serialized = serde_json::to_string(&facts).expect("serialize");
        assert!(!serialized.contains("exact-original-replacement"));
        assert!(!serialized.contains("\\\"raw\\\""));
        let runs_before = runs.load(Ordering::SeqCst);
        let fresh_audit = Arc::new(LedgerExtensionJournal::new(
            f.ledger.clone(),
            Arc::new(Clock),
        ));
        let fresh_host = ExtensionHost::new(
            rewriting(runs.clone(), true, deny),
            fresh_audit.clone(),
            RuntimePorts {
                state_root: f.context.workspace_dir.clone(),
                subagents: None,
                content: f.tools.content.clone(),
                history: f.journal.clone(),
            },
            Default::default(),
        )
        .expect("host");
        let replay = policy(&f, fresh_host)
            .execute_tool(&f.context.attempt, &telemetry(&f.context), call)
            .await;
        assert_eq!(replay.is_err(), deny);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            runs_before,
            "recorded intervention must not be rerun"
        );
        assert_eq!(fresh_audit.read(&f.context).await.expect("facts"), facts);
        if !deny {
            assert_eq!(
                lock(&f.tools.executions).as_slice(),
                &[serde_json::json!({"private":"exact-original-replacement"})]
            );
        }
    }
}
struct CountLedger {
    inner: Arc<musteragent_ledger::InMemoryLedger<Clock>>,
    loaded: AtomicUsize,
}
impl LedgerStore for CountLedger {
    type Error = LedgerError;
    fn append(
        &self,
        r: musteragent_ledger::AppendRequest,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, LedgerError>> {
        Box::pin(async move { self.inner.append(r) })
    }
    fn load(
        &self,
        c: musteragent_event_envelope::LedgerCursor,
        n: usize,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::LedgerPage, LedgerError>> {
        Box::pin(async move {
            let page = self.inner.load(c, n)?;
            self.loaded.fetch_add(page.records.len(), Ordering::SeqCst);
            Ok(page)
        })
    }
    fn pending_outbox(
        &self,
    ) -> ApplicationFuture<'_, Result<Vec<musteragent_ledger::CommittedOutbox>, LedgerError>> {
        Box::pin(async { self.inner.pending_outbox() })
    }
    fn current_offset(
        &self,
        c: ConversationId,
    ) -> ApplicationFuture<'_, Result<LedgerOffset, LedgerError>> {
        Box::pin(async move { self.inner.current_offset(c) })
    }
}
#[tokio::test]
async fn keyed_lookup_does_not_rescan_500_prior_facts() {
    let f = Fixture::new(registry(), vec![], Default::default(), "Hello");
    let ledger = Arc::new(CountLedger {
        inner: f.ledger.clone(),
        loaded: AtomicUsize::new(0),
    });
    let audit = Arc::new(LedgerExtensionJournal::new(ledger.clone(), Arc::new(Clock)));
    for i in 0..500 {
        audit
            .append(
                &f.context,
                &format!("prior:{i}"),
                ExtensionFact::Service {
                    service: format!("prior-{i}"),
                    hook: Hook::ServiceStart,
                },
            )
            .await
            .expect("seed");
    }
    // A fresh reference adapter reconstructs its index once. Every subsequent
    // hot-path lookup reads only the key and incremental ledger suffix.
    assert_eq!(
        audit
            .select(&f.context, FactQuery::Key("prior:499".into()))
            .await
            .expect("lookup")
            .len(),
        1
    );
    assert_eq!(ledger.loaded.swap(0, Ordering::SeqCst), 500);
    let host = ExtensionHost::new(
        registry(),
        audit.clone(),
        RuntimePorts {
            state_root: f.context.workspace_dir.clone(),
            subagents: None,
            content: f.tools.content.clone(),
            history: f.journal.clone(),
        },
        Default::default(),
    )
    .expect("host");
    policy(&f, host)
        .execute_tool(
            &f.context.attempt,
            &telemetry(&f.context),
            super::loops::history_call("fresh", "probe", serde_json::json!({})),
        )
        .await
        .expect("execute");
    assert_eq!(ledger.loaded.load(Ordering::SeqCst), 0);
    assert_eq!(ledger.inner.pending_outbox().expect("outbox").len(), 500);
}

struct IdentityRewrite;
impl KernelExtension for IdentityRewrite {
    fn id(&self) -> &str {
        "identity"
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        call: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        Ok(ToolDecision::Rewrite(call.arguments.clone()))
    }
}
#[tokio::test]
async fn unchanged_rewrite_context_and_headers_do_not_publish_facts() {
    let mut registry = registry();
    registry
        .register_extension(Arc::new(IdentityRewrite), serde_json::json!({}))
        .expect("identity");
    let f = Fixture::new(
        registry,
        vec![
            tool_events("one", "probe", serde_json::json!({})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    assert!(matches!(
        f.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        f.facts()
            .await
            .iter()
            .all(|f| matches!(f.fact, ExtensionFact::Lifecycle { .. }))
    );
}
#[tokio::test]
async fn same_call_preparations_reuse_one_rewrite() {
    let visits = Arc::new(AtomicUsize::new(0));
    let f = Fixture::new(
        rewriting(visits.clone(), false, false),
        vec![],
        Default::default(),
        "Hello",
    );
    let policy = policy(&f, f.host.clone());
    let call = super::loops::history_call("same", "probe", serde_json::json!({}));
    let telemetry = telemetry(&f.context);
    let (a, b) = tokio::join!(
        policy.execute_tool(&f.context.attempt, &telemetry, call.clone()),
        policy.execute_tool(&f.context.attempt, &telemetry, call)
    );
    a.expect("first");
    b.expect("second");
    assert_eq!(visits.load(Ordering::SeqCst), 1);
    assert_eq!(f.facts().await.len(), 1);
    assert_eq!(lock(&f.tools.executions).len(), 1);
}

#[tokio::test]
async fn future_unprepared_calls_do_not_enter_loop_history() {
    let f = Fixture::new(registry(), vec![], Default::default(), "Hello");
    for i in 0..30 {
        lock(&f.journal.calls).push(super::loops::history_call(
            &format!("planned-{i}"),
            "probe",
            serde_json::json!({}),
        ));
    }
    let policy = policy(&f, f.host.clone());
    let call = lock(&f.journal.calls)[0].clone();
    let outcome = policy
        .execute_tool(&f.context.attempt, &telemetry(&f.context), call.clone())
        .await
        .expect("first call");
    policy
        .tool_result_committed(&call, &outcome, &step(&f))
        .await
        .expect("unprepared siblings must not stop this call");
    let history = f
        .journal
        .recent(&f.context, None, 30, &BTreeSet::new())
        .await
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].call.call_id, "planned-0");
    assert!(f.facts().await.is_empty());
}

struct MutableAdmission(Arc<std::sync::atomic::AtomicBool>);
impl KernelExtension for MutableAdmission {
    fn id(&self) -> &str {
        "mutable-admission"
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        Ok(if self.0.load(Ordering::SeqCst) {
            ToolDecision::Deny(HookError::new("changed", "policy changed after admission"))
        } else {
            ToolDecision::Allow
        })
    }
}
#[tokio::test]
async fn duplicate_call_waits_for_original_kernel_admission_without_a_shadow_receipt() {
    let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = registry();
    registry
        .register_extension(
            Arc::new(MutableAdmission(changed.clone())),
            serde_json::json!({}),
        )
        .expect("register");
    let f = Fixture::new(registry, vec![], Default::default(), "Hello");
    let pause = Arc::new(executor::RequestPause::default());
    *lock(&f.tools.before_request) = Some(pause.clone());
    let policy = policy(&f, f.host.clone());
    let call = super::loops::history_call("admission-gap", "probe", serde_json::json!({}));
    let telemetry = telemetry(&f.context);
    let original = policy.execute_tool(&f.context.attempt, &telemetry, call.clone());
    let duplicate = async {
        pause.entered.notified().await;
        changed.store(true, Ordering::SeqCst);
        let second = policy.execute_tool(&f.context.attempt, &telemetry, call);
        tokio::pin!(second);
        // No wall-clock timing assumption: the duplicate must wait while the
        // original executor has not yet persisted its admission.
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        pause.release.notify_one();
        second.await.expect("reuse original admission")
    };
    let (first, _) = tokio::join!(original, duplicate);
    first.expect("original");
    assert!(f.facts().await.is_empty());
    assert!(f.ledger.pending_outbox().expect("outbox").is_empty());
    assert_eq!(lock(&f.tools.executions).len(), 1);
}
#[tokio::test]
async fn kernel_request_replays_admission_even_if_a_formerly_passive_hook_changes() {
    for rewrite in [false, true] {
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let visits = Arc::new(AtomicUsize::new(0));
        let registry = || {
            let mut r = if rewrite {
                rewriting(visits.clone(), false, false)
            } else {
                ExtensionRegistry::default()
            };
            r.register_extension(
                Arc::new(MutableAdmission(changed.clone())),
                serde_json::json!({}),
            )
            .expect("register");
            r
        };
        let f = Fixture::new(registry(), vec![], Default::default(), "Hello");
        let call = super::loops::history_call("already-requested", "probe", serde_json::json!({}));
        policy(&f, f.host.clone())
            .execute_tool(&f.context.attempt, &telemetry(&f.context), call.clone())
            .await
            .expect("original execution");
        let facts = f.facts().await;
        changed.store(true, Ordering::SeqCst);
        let fresh = ExtensionHost::new(
            registry(),
            Arc::new(LedgerExtensionJournal::new(
                f.ledger.clone(),
                Arc::new(Clock),
            )),
            RuntimePorts {
                content: f.tools.content.clone(),
                history: f.journal.clone(),
                state_root: f.context.workspace_dir.clone(),
                subagents: None,
            },
            Default::default(),
        )
        .expect("fresh");
        policy(&f, fresh)
            .execute_tool(&f.context.attempt, &telemetry(&f.context), call)
            .await
            .expect("kernel request owns replay admission");
        assert_eq!(f.facts().await, facts);
        assert_eq!(lock(&f.tools.executions).len(), 1);
        assert_eq!(visits.load(Ordering::SeqCst), usize::from(rewrite));
    }
}
