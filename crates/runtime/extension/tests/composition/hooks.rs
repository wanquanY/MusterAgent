use super::*;
use musteragent_runtime_content::ContentStore;
use musteragent_tool_executor::{InvocationPolicyDisposition, InvocationPolicyInput};

struct Ordered {
    id: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    deny: bool,
}
impl KernelExtension for Ordered {
    fn id(&self) -> &str {
        self.id
    }
    fn prompt_build(
        &self,
        _: &ExtensionContext,
        _: &CanonicalModelRequest,
    ) -> Result<PromptContribution, HookError> {
        lock(&self.log).push(format!("prompt:{}", self.id));
        Ok(PromptContribution {
            prepend: vec![text(format!("pre:{}", self.id))],
            append: vec![text(format!("post:{}", self.id))],
            sections: vec![(self.id.into(), text(format!("section:{}", self.id)))],
        })
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        call: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        lock(&self.log).push(format!("before:{}", self.id));
        if self.deny {
            return Ok(ToolDecision::Deny(HookError::new(
                "plan_read_only",
                "writes are disabled",
            )));
        }
        let mut arguments = canonical_arguments(&call.arguments);
        arguments["chain"]
            .as_array_mut()
            .expect("valid fixture and successful tested operation")
            .push(self.id.into());
        Ok(ToolDecision::Rewrite(json(arguments)))
    }
    fn after_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
        outcome: &ToolExecutionOutcome,
    ) -> Result<(), HookError> {
        lock(&self.log).push(format!("after:{}", self.id));
        let ToolExecutionOutcome::Completed { message, .. } = outcome else {
            panic!("expected original result")
        };
        assert!(format!("{message:?}").contains("original"));
        Ok(())
    }
    fn tool_result_persist(
        &self,
        _: &ExtensionContext,
        mut message: CanonicalMessage,
    ) -> Result<CanonicalMessage, HookError> {
        lock(&self.log).push(format!("result:{}", self.id));
        message
            .content
            .push(CanonicalContentPart::Text(text(format!(
                "result:{}",
                self.id
            ))));
        Ok(message)
    }
}
fn ordered(deny: bool) -> (ExtensionRegistry, Arc<Mutex<Vec<String>>>) {
    let log = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    for (id, denied) in [("first", deny), ("second", false)] {
        registry
            .register_extension(
                Arc::new(Ordered {
                    id,
                    log: log.clone(),
                    deny: denied,
                }),
                serde_json::json!({}),
            )
            .expect("valid fixture and successful tested operation");
    }
    (registry, log)
}
#[tokio::test]
async fn prompt_contributions_are_ordered_and_rebuilt_before_every_model_call() {
    let (registry, log) = ordered(false);
    let fixture = Fixture::new(
        registry,
        vec![
            tool_events("one", "probe", serde_json::json!({"chain":[]})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    let requests = lock(&fixture.provider.requests);
    for request in requests.iter() {
        assert_eq!(
            request
                .instructions
                .iter()
                .map(ModelText::as_str)
                .collect::<Vec<_>>(),
            vec![
                "pre:first",
                "pre:second",
                "Base system",
                "post:first",
                "[first]\nsection:first",
                "post:second",
                "[second]\nsection:second"
            ]
        );
    }
    assert_eq!(
        lock(&log)
            .iter()
            .filter(|s| s.starts_with("prompt:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            "prompt:first",
            "prompt:second",
            "prompt:first",
            "prompt:second"
        ]
    );
    assert_eq!(
        *requests,
        *lock(&fixture.journal.sealed),
        "projection must be part of the sealed request"
    );
}
#[tokio::test]
async fn argument_rewrites_compose_and_are_recorded_before_real_execution() {
    let (registry, _) = ordered(false);
    let fixture = Fixture::new(
        registry,
        vec![
            tool_events("one", "probe", serde_json::json!({"chain":[]})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    let ledger = fixture.ledger.clone();
    let context = fixture.context.clone();
    *lock(&fixture.tools.before_execute) = Some(Arc::new(move || {
        let page = ledger
            .load(
                musteragent_event_envelope::LedgerCursor {
                    conversation_id: context.run.conversation_id,
                    home_epoch: context.run.home_epoch,
                    after_conversation_seq: LedgerOffset::ZERO,
                },
                1000,
            )
            .expect("ledger at physical execution");
        let rewrites = page.records.iter().filter(|r| r.metadata.schema_version.as_str() == EXTENSION_FACT_SCHEMA).filter_map(|r| decode_extension_fact(&r.payload).ok()).filter(|r| matches!(&r.fact, ExtensionFact::ArgumentsRewritten { call_id, .. } if call_id == "one")).count();
        assert_eq!(
            rewrites, 2,
            "both rewrites must exist before physical execution"
        );
    }));
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(
        lock(&fixture.tools.executions)[0]["chain"],
        serde_json::json!(["first", "second"])
    );
    let facts = fixture.facts().await;
    let rewrites = facts
        .iter()
        .filter_map(|f| {
            if let ExtensionFact::ArgumentsRewritten {
                extension,
                before,
                after,
                ..
            } = &f.fact
            {
                Some((extension.as_str(), before.clone(), after.clone()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(rewrites.len(), 2);
    assert_eq!(rewrites[0].0, "first");
    assert_eq!(rewrites[1].1, rewrites[0].2);
    // Admission/result shadow facts are gone. Audit stores only digests and
    // a content identity; the real executor above received the full composition.
    let encoded = serde_json::to_string(&facts).expect("facts");
    assert!(!encoded.contains("\"chain\""));
    assert!(
        facts
            .iter()
            .filter(|f| matches!(f.fact, ExtensionFact::ArgumentsRewritten { .. }))
            .count()
            == 2
    );
}
#[tokio::test]
async fn deny_wins_over_later_allow_and_records_typed_reason_without_dispatch() {
    let (registry, log) = ordered(true);
    let fixture = Fixture::new(
        registry,
        vec![tool_events("one", "probe", serde_json::json!({"chain":[]}))],
        Default::default(),
        "Hello",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            receipt: Some(_),
            ..
        }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
    assert!(lock(&fixture.tools.invocations).is_empty());
    assert_eq!(
        lock(&log)
            .iter()
            .filter(|v| v.starts_with("before:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["before:first", "before:second"]
    );
    assert!(fixture.facts().await.iter().any(
        |f| matches!(&f.fact,ExtensionFact::ToolDenied {reason,..} if reason.code=="plan_read_only")
    ));
}
#[tokio::test]
async fn tool_result_view_composes_while_original_ledger_and_content_stay_unchanged() {
    let (registry, log) = ordered(false);
    let fixture = Fixture::new(
        registry,
        vec![
            tool_events("one", "probe", serde_json::json!({"chain":[]})),
            assistant(),
        ],
        Default::default(),
        "Hello",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    let original = lock(&fixture.journal.results)[0].message.clone();
    assert!(!format!("{original:?}").contains("result:first"));
    let page = fixture
        .ledger
        .load(
            musteragent_event_envelope::LedgerCursor {
                conversation_id: fixture.context.run.conversation_id,
                home_epoch: fixture.context.run.home_epoch,
                after_conversation_seq: LedgerOffset::ZERO,
            },
            1000,
        )
        .expect("read original ledger");
    let raw = page
        .records
        .iter()
        .find(|record| record.metadata.schema_version.as_str() == "test.tool.result")
        .expect("raw result fact");
    assert_eq!(raw.payload, format!("{original:?}").as_bytes());
    assert!(!String::from_utf8_lossy(&raw.payload).contains("result:first"));
    let projected = lock(&fixture.provider.requests)[1]
        .input
        .iter()
        .find(|m| m.role == CanonicalRole::Tool)
        .expect("valid fixture and successful tested operation")
        .clone();
    assert_eq!(
        &projected.content[..original.content.len()],
        original.content.as_slice()
    );
    assert_eq!(
        &projected.content[original.content.len()..],
        &[
            CanonicalContentPart::Text(text("result:first")),
            CanonicalContentPart::Text(text("result:second"))
        ]
    );
    let reference = lock(&fixture.tools.invocations)
        .values()
        .next()
        .expect("valid fixture and successful tested operation")
        .invocation
        .result_ref
        .clone()
        .expect("valid fixture and successful tested operation");
    let bytes = fixture
        .tools
        .content
        .read(fixture.context.run.tenant_id, &reference)
        .await
        .expect("valid fixture and successful tested operation");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .expect("valid fixture and successful tested operation")["secret"],
        "raw"
    );
    assert_eq!(
        lock(&log)
            .iter()
            .filter(|v| v.starts_with("after:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["after:first", "after:second"]
    );
}
struct ModelHooks {
    seen: Arc<Mutex<Vec<String>>>,
}
impl KernelExtension for ModelHooks {
    fn id(&self) -> &str {
        "model-hooks"
    }
    fn context_projection(
        &self,
        _: &ExtensionContext,
        mut view: ContextView,
    ) -> Result<ContextView, HookError> {
        view.instructions.push(text("Projected context"));
        Ok(view)
    }
    fn before_provider_request(
        &self,
        _: &ExtensionContext,
        _: &CanonicalModelRequest,
        annotations: &mut ModelRequestAnnotations,
    ) -> Result<(), HookError> {
        annotations
            .insert_header("x-extension-test", "annotated")
            .expect("valid fixture and successful tested operation");
        Ok(())
    }
    fn llm_input(
        &self,
        _: &ExtensionContext,
        request: &CanonicalModelRequest,
    ) -> Result<(), HookError> {
        lock(&self.seen).push(
            request
                .instructions
                .last()
                .expect("valid fixture and successful tested operation")
                .as_str()
                .into(),
        );
        Ok(())
    }
    fn llm_output(
        &self,
        _: &ExtensionContext,
        _: InvocationId,
        event: &ModelStreamEvent,
    ) -> Result<(), HookError> {
        if let ModelStreamEvent::Usage(usage) = event {
            lock(&self.seen).push(format!(
                "usage:{}",
                usage
                    .total_tokens
                    .expect("valid fixture and successful tested operation")
            ));
        }
        Ok(())
    }
}
#[tokio::test]
async fn model_wrapper_observes_projected_inputs_usage_and_outbound_annotations() {
    let seen = Arc::new(Mutex::new(vec![]));
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(ModelHooks { seen: seen.clone() }),
            serde_json::json!({}),
        )
        .expect("valid fixture and successful tested operation");
    let billing = Arc::new(samples::RuntimeBilling::default());
    registry
        .register_extension(billing.clone(), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    let fixture = Fixture::new(registry, vec![assistant()], Default::default(), "Hello");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(*lock(&seen), vec!["Projected context", "usage:5"]);
    assert_eq!(
        billing
            .usage()
            .expect("valid fixture and successful tested operation")
            .values()
            .next()
            .expect("valid fixture and successful tested operation")
            .total_tokens,
        Some(5)
    );
    assert_eq!(
        lock(&fixture.provider.annotations)[0].headers()["x-extension-test"],
        "annotated"
    );
    assert!(fixture.facts().await.iter().any(|f|matches!(&f.fact,ExtensionFact::RequestAnnotated {header_names,..} if header_names==&["x-extension-test"])));
    assert_eq!(
        *lock(&fixture.provider.requests),
        *lock(&fixture.journal.sealed)
    );
}
struct InvocationPolicy {
    id: &'static str,
    deny: bool,
    panic: bool,
}
impl KernelExtension for InvocationPolicy {
    fn id(&self) -> &str {
        self.id
    }
    fn invocation_policy(
        &self,
        _: &InvocationPolicyInput<'_>,
    ) -> Result<InvocationPolicyDisposition, HookError> {
        assert!(!self.panic, "fixture policy panic");
        Ok(if self.deny {
            InvocationPolicyDisposition::Deny("extension_policy_denied")
        } else {
            InvocationPolicyDisposition::Allow
        })
    }
}
#[tokio::test]
async fn invocation_policy_denial_dominates_allow_inside_durable_executor() {
    let mut registry = ExtensionRegistry::default();
    for (id, deny) in [("deny", true), ("allow", false)] {
        registry
            .register_extension(
                Arc::new(InvocationPolicy {
                    id,
                    deny,
                    panic: false,
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
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
    assert!(
        format!(
            "{:?}",
            lock(&fixture.tools.invocations)
                .values()
                .next()
                .expect("valid fixture and successful tested operation")
                .recorded_outcome
        )
        .contains("extension_policy_denied")
    );
}
#[tokio::test]
async fn invocation_policy_panic_is_typed_and_committed_by_original_tool_owner() {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(
            Arc::new(InvocationPolicy {
                id: "panic-policy",
                deny: false,
                panic: true,
            }),
            serde_json::json!({}),
        )
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
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
    let recorded = format!(
        "{:?}",
        lock(&fixture.tools.invocations)
            .values()
            .next()
            .expect("valid fixture and successful tested operation")
            .recorded_outcome
    );
    assert!(recorded.contains("hook_panicked"));
    assert!(recorded.contains("panic-policy"));
}

#[tokio::test]
async fn conflicting_audit_write_fails_closed_before_tool_dispatch() {
    let (registry, _) = ordered(false);
    let fixture = Fixture::new(
        registry,
        vec![tool_events("one", "probe", serde_json::json!({"chain":[]}))],
        Default::default(),
        "Hello",
    );
    fixture
        .audit
        .append(
            &fixture.context,
            "tool:one:first:rewrite",
            ExtensionFact::ToolDenied {
                extension: "first".into(),
                call_id: "unrelated".into(),
                reason: HookError::new("fixture_conflict", "reserved conflicting key"),
            },
        )
        .await
        .expect("valid fixture and successful tested operation");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::DependencyUnavailable,
            ..
        }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
    assert!(lock(&fixture.tools.invocations).is_empty());
    assert!(matches!(
        fixture.facts().await[0].fact,
        ExtensionFact::ToolDenied { .. }
    ));
}

struct InvalidRewrite;
impl KernelExtension for InvalidRewrite {
    fn id(&self) -> &str {
        "invalid-rewrite"
    }
    fn before_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
    ) -> Result<ToolDecision, HookError> {
        Ok(ToolDecision::Rewrite(JsonValue::Number(f64::NAN)))
    }
}
#[tokio::test]
async fn invalid_rewritten_arguments_have_a_typed_fact_and_never_dispatch() {
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(Arc::new(InvalidRewrite), serde_json::json!({}))
        .expect("register fixture");
    let fixture = Fixture::new(
        registry,
        vec![tool_events("one", "probe", serde_json::json!({}))],
        Default::default(),
        "Hello",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            ..
        }
    ));
    assert!(lock(&fixture.tools.invocations).is_empty());
    assert!(fixture.facts().await.iter().any(|f|matches!(&f.fact,ExtensionFact::HookFailed {failure:HookFailure {kind:HookFailureKind::Error(error),..},continues:false} if error.code=="invalid_rewritten_arguments")));
}
