use super::*;
fn disabled() -> LoopThresholds {
    LoopThresholds {
        warning: None,
        critical: None,
        circuit_breaker: None,
    }
}
fn config(pattern: LoopPattern, level: LoopLevel) -> LoopDetectionConfig {
    let mut config = LoopDetectionConfig {
        generic_repeat: disabled(),
        known_poll_no_progress: disabled(),
        ping_pong: disabled(),
        ..Default::default()
    };
    let thresholds = LoopThresholds {
        warning: Some(if level == LoopLevel::Warning { 4 } else { 2 }),
        critical: (level == LoopLevel::Critical).then_some(4),
        circuit_breaker: (level == LoopLevel::CircuitBreaker).then_some(4),
    };
    match pattern {
        LoopPattern::GenericRepeat => config.generic_repeat = thresholds,
        LoopPattern::KnownPollNoProgress => {
            config.known_poll_no_progress = thresholds;
            config.poll_tools.insert("poll".into());
        }
        LoopPattern::PingPong => config.ping_pong = thresholds,
    };
    config
}
async fn pattern_level(pattern: LoopPattern, level: LoopLevel) {
    let mut scripts = (0..5)
        .map(|i| {
            tool_events(
                &format!("call-{i}"),
                match pattern {
                    LoopPattern::KnownPollNoProgress => "poll",
                    LoopPattern::PingPong if i % 2 == 1 => "other",
                    _ => "probe",
                },
                serde_json::json!({"a":1,"b":2}),
            )
        })
        .collect::<Vec<_>>();
    scripts.push(assistant());
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        scripts,
        config(pattern, level),
        "Hello",
    );
    let report = fixture.run().await;
    let facts = fixture.facts().await;
    assert!(facts.iter().any(|f|matches!(&f.fact,ExtensionFact::LoopDetected {detection} if detection.pattern==pattern && detection.level==level && detection.count==4)),"{facts:?}");
    if level == LoopLevel::Warning {
        assert!(
            matches!(report.exit, AgentLoopExit::Completed { .. }),
            "{:?}",
            report.exit
        );
        assert!(lock(&fixture.provider.requests).iter().any(|r| {
            r.instructions
                .iter()
                .any(|t| t.as_str().contains("Tool loop warning"))
        }));
        assert_eq!(lock(&fixture.tools.executions).len(), 5);
    } else {
        assert!(
            matches!(
                report.exit,
                AgentLoopExit::Failed {
                    code: ErrorCode::PolicyDenied,
                    receipt: Some(_),
                    ..
                }
            ),
            "{:?}",
            report.exit
        );
        let expected = if pattern == LoopPattern::KnownPollNoProgress {
            4
        } else {
            3
        };
        assert_eq!(lock(&fixture.tools.executions).len(), expected);
        assert_eq!(
            lock(&fixture.journal.results).len(),
            expected,
            "original results must commit before poll stop"
        );
        assert_eq!(
            lock(&fixture.journal.failures).as_slice(),
            &[ErrorCode::PolicyDenied]
        );
    }
}
macro_rules! pattern_test {
    ($name:ident,$pattern:ident,$level:ident) => {
        #[tokio::test]
        async fn $name() {
            pattern_level(LoopPattern::$pattern, LoopLevel::$level).await;
        }
    };
}
pattern_test!(generic_repeat_warning, GenericRepeat, Warning);
pattern_test!(generic_repeat_critical, GenericRepeat, Critical);
pattern_test!(
    generic_repeat_circuit_breaker,
    GenericRepeat,
    CircuitBreaker
);
pattern_test!(known_poll_no_progress_warning, KnownPollNoProgress, Warning);
pattern_test!(
    known_poll_no_progress_critical,
    KnownPollNoProgress,
    Critical
);
pattern_test!(
    known_poll_no_progress_circuit_breaker,
    KnownPollNoProgress,
    CircuitBreaker
);
pattern_test!(ping_pong_warning, PingPong, Warning);
pattern_test!(ping_pong_critical, PingPong, Critical);
pattern_test!(ping_pong_circuit_breaker, PingPong, CircuitBreaker);
#[tokio::test]
async fn varied_arguments_do_not_trigger_any_pattern() {
    let mut scripts = (0..40)
        .map(|i| tool_events(&format!("call-{i}"), "poll", serde_json::json!({"page":i})))
        .collect::<Vec<_>>();
    scripts.push(assistant());
    let mut config = LoopDetectionConfig::default();
    config.poll_tools.insert("poll".into());
    let fixture = Fixture::new(ExtensionRegistry::default(), scripts, config, "Hello");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        !fixture
            .facts()
            .await
            .iter()
            .any(|f| matches!(f.fact, ExtensionFact::LoopDetected { .. }))
    );
    assert_eq!(lock(&fixture.tools.executions).len(), 40);
}
#[tokio::test]
async fn canonical_object_key_order_is_not_progress() {
    let mut scripts = vec![];
    for (i, s) in [
        r#"{"a":1,"b":{"x":2,"y":3}}"#,
        r#"{"b":{"y":3,"x":2},"a":1}"#,
    ]
    .iter()
    .cycle()
    .take(4)
    .enumerate()
    {
        scripts.push(tool_events(
            &format!("call-{i}"),
            "probe",
            serde_json::from_str(s).expect("valid fixture and successful tested operation"),
        ));
    }
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        scripts,
        config(LoopPattern::GenericRepeat, LoopLevel::Critical),
        "Hello",
    );
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            ..
        }
    ));
    assert_eq!(lock(&fixture.tools.executions).len(), 3);
}
#[test]
fn defaults_and_invalid_thresholds_are_explicit() {
    let defaults = LoopDetectionConfig::default();
    assert_eq!(defaults.history, 30);
    assert_eq!(
        defaults.generic_repeat,
        LoopThresholds {
            warning: Some(10),
            critical: Some(20),
            circuit_breaker: Some(30)
        }
    );
    let invalid = LoopDetectionConfig {
        history: 3,
        ..defaults
    };
    assert!(matches!(
        invalid.validate(),
        Err(ExtensionError::LoopConfig(_))
    ));
}
#[tokio::test]
async fn durable_history_is_reloaded_for_a_fresh_loop() {
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        vec![tool_events("fresh", "probe", serde_json::json!({"a":1}))],
        config(LoopPattern::GenericRepeat, LoopLevel::Critical),
        "Hello",
    );
    for i in 0..3 {
        let call = history_call(&format!("prior-{i}"), "probe", serde_json::json!({"a":1}));
        let mut result = message(CanonicalRole::Tool, "prior result");
        result.tool_call_id = Some(call.call_id.clone());
        result.tool_name = Some(call.name.clone());
        lock(&fixture.journal.results).push(ToolResultCommit {
            call_id: call.call_id.clone(),
            message: result,
        });
        lock(&fixture.journal.calls).push(call);
    }
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            ..
        }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
}
struct Progress {
    result: Mutex<Option<Arc<Mutex<serde_json::Value>>>>,
    counter: Mutex<u32>,
}
impl KernelExtension for Progress {
    fn id(&self) -> &str {
        "progress"
    }
    fn after_tool_call(
        &self,
        _: &ExtensionContext,
        _: &PlannedToolCall,
        _: &ToolExecutionOutcome,
    ) -> Result<(), HookError> {
        let mut counter = lock(&self.counter);
        *counter += 1;
        *lock(
            lock(&self.result)
                .as_ref()
                .expect("valid fixture and successful tested operation"),
        ) = serde_json::json!({"progress":*counter});
        Ok(())
    }
}
#[tokio::test]
async fn changed_poll_results_reset_the_no_progress_streak() {
    let progress = Arc::new(Progress {
        result: Mutex::new(None),
        counter: Mutex::new(0),
    });
    let mut registry = ExtensionRegistry::default();
    registry
        .register_extension(progress.clone(), serde_json::json!({}))
        .expect("valid fixture and successful tested operation");
    let mut scripts = (0..6)
        .map(|i| tool_events(&format!("call-{i}"), "poll", serde_json::json!({"job":1})))
        .collect::<Vec<_>>();
    scripts.push(assistant());
    let fixture = Fixture::new(
        registry,
        scripts,
        config(LoopPattern::KnownPollNoProgress, LoopLevel::Critical),
        "Hello",
    );
    *lock(&progress.result) = Some(fixture.tools.result.clone());
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        !fixture
            .facts()
            .await
            .iter()
            .any(|f| matches!(f.fact, ExtensionFact::LoopDetected { .. }))
    );
}
#[tokio::test]
async fn history_window_evicts_old_calls_and_attempts_are_isolated() {
    let mut config = config(LoopPattern::GenericRepeat, LoopLevel::Critical);
    config.history = 4;
    config.generic_repeat.warning = None;
    let mut scripts = (0..3)
        .map(|i| tool_events(&format!("old-{i}"), "probe", serde_json::json!({"a":1})))
        .collect::<Vec<_>>();
    scripts.extend(
        (0..4).map(|i| tool_events(&format!("gap-{i}"), "probe", serde_json::json!({"gap":i}))),
    );
    scripts.extend(
        (0..3).map(|i| tool_events(&format!("new-{i}"), "probe", serde_json::json!({"a":1}))),
    );
    scripts.push(assistant());
    let fixture = Fixture::new(ExtensionRegistry::default(), scripts, config, "Hello");
    let mut other = fixture.context.clone();
    other.attempt.attempt_id = AttemptId::generate();
    fixture
        .audit
        .append(
            &other,
            "foreign-stop",
            ExtensionFact::LoopDetected {
                detection: LoopDetection {
                    pattern: LoopPattern::GenericRepeat,
                    level: LoopLevel::CircuitBreaker,
                    count: 30,
                    call_id: "foreign".into(),
                },
            },
        )
        .await
        .expect("valid fixture and successful tested operation");
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Completed { .. }
    ));
    assert!(
        !fixture
            .facts()
            .await
            .iter()
            .any(|f| matches!(f.fact, ExtensionFact::LoopDetected { .. }))
    );
}

#[tokio::test]
async fn out_of_order_poll_completions_are_detected_with_an_unrelated_latest_call() {
    let fixture = Fixture::new(
        ExtensionRegistry::default(),
        vec![
            tool_events("other", "other", serde_json::json!({})),
            assistant(),
        ],
        config(LoopPattern::KnownPollNoProgress, LoopLevel::Critical),
        "Hello",
    );
    for i in 0..4 {
        lock(&fixture.journal.calls).push(history_call(
            &format!("poll-{i}"),
            "poll",
            serde_json::json!({"job":1}),
        ));
    }
    for i in [0, 3, 1, 2] {
        let mut message = message(CanonicalRole::Tool, "pending");
        message.tool_call_id = Some(ToolCallId::new(format!("poll-{i}")).expect("id"));
        message.tool_name = Some(wire("poll"));
        lock(&fixture.journal.results).push(ToolResultCommit {
            call_id: message.tool_call_id.clone().expect("id"),
            message,
        });
    }
    assert!(matches!(
        fixture.run().await.exit,
        AgentLoopExit::Failed {
            code: ErrorCode::PolicyDenied,
            ..
        }
    ));
    assert!(lock(&fixture.tools.executions).is_empty());
    assert!(fixture.facts().await.iter().any(|f|matches!(&f.fact,ExtensionFact::LoopDetected {detection} if detection.pattern==LoopPattern::KnownPollNoProgress && detection.count==4)));
}

pub(super) fn history_call(id: &str, name: &str, args: serde_json::Value) -> PlannedToolCall {
    PlannedToolCall {
        call_id: ToolCallId::new(id).expect("id"),
        name: wire(name),
        arguments: json(args),
        provider_replay: None,
        planning_failure: None,
        argument_diagnostic: None,
    }
}

#[tokio::test]
async fn all_patterns_recover_original_history_with_a_fresh_host_and_loop() {
    for pattern in [
        LoopPattern::GenericRepeat,
        LoopPattern::PingPong,
        LoopPattern::KnownPollNoProgress,
    ] {
        let mut settings = config(pattern, LoopLevel::Critical);
        settings.generic_repeat.warning = None;
        settings.ping_pong.warning = None;
        settings.known_poll_no_progress.warning = None;
        let name = if pattern == LoopPattern::KnownPollNoProgress {
            "poll"
        } else if pattern == LoopPattern::PingPong {
            "other"
        } else {
            "probe"
        };
        let f = Fixture::new(
            ExtensionRegistry::default(),
            vec![tool_events("fourth", name, serde_json::json!({}))],
            settings.clone(),
            "Hello",
        );
        for i in 0..3 {
            let name = if pattern == LoopPattern::KnownPollNoProgress {
                "poll"
            } else if pattern == LoopPattern::PingPong && i % 2 == 1 {
                "other"
            } else {
                "probe"
            };
            let call = history_call(&format!("prior-{i}"), name, serde_json::json!({}));
            let mut result = message(CanonicalRole::Tool, &lock(&f.tools.result).to_string());
            result.tool_name = Some(call.name.clone());
            result.tool_call_id = Some(call.call_id.clone());
            lock(&f.journal.results).push(ToolResultCommit {
                call_id: call.call_id.clone(),
                message: result,
            });
            lock(&f.journal.calls).push(call);
        }
        let fresh_host = ExtensionHost::new(
            ExtensionRegistry::default(),
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
            settings,
        )
        .expect("fresh host");
        let fresh_loop = fresh_host
            .compose_loop(
                f.context.clone(),
                ExtensionLoopPorts {
                    clock: Arc::new(Clock),
                    policy: f.policy.clone(),
                    model: f.provider.clone(),
                    journal: f.journal.clone(),
                    supervision: f.tools.home.supervision(),
                },
            )
            .expect("fresh loop");
        let result = fresh_loop
            .execute(telemetry(&f.context), InvocationSupervisor::new())
            .await
            .expect("loop");
        assert!(
            matches!(
                result.exit,
                AgentLoopExit::Failed {
                    code: ErrorCode::PolicyDenied,
                    ..
                }
            ),
            "{pattern:?}: {:?}",
            result.exit
        );
        assert!(f.facts().await.iter().any(|f| matches!(&f.fact, ExtensionFact::LoopDetected { detection } if detection.pattern == pattern && detection.count == 4)));
    }
}
