use super::*;
use musteragent_runtime_contract::ToolArgumentValidationIssueKind as Issue;

#[tokio::test]
async fn undecodable_arguments_are_durable_tool_failures_and_preserve_usage() {
    for kind in [
        Issue::MalformedJson,
        Issue::ObjectRequired,
        Issue::InvalidValue,
    ] {
        let fixture = Fixture::new();
        let call_id = tool_call_id("invalid-json-call");
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(vec![
                Ok(ModelStreamEvent::ResponseStarted {
                    response_ref: provider_ref("invalid-json-response"),
                    response_model_key: model_key(),
                }),
                Ok(ModelStreamEvent::ToolCallStarted {
                    output_index: 0,
                    call_id: call_id.clone(),
                    name: wire_name("external_write"),
                    provider_item_ref: None,
                    provider_replay: None,
                }),
                Ok(ModelStreamEvent::ToolCallRejected {
                    diagnostic: Some(musteragent_runtime_model::ArgumentDiagnostic::observed(
                        "{\"secret\":",
                        musteragent_runtime_model::ArgumentSource::Stream,
                        None,
                    )),
                    output_index: 0,
                    call_id,
                    kind,
                }),
                Ok(ModelStreamEvent::Usage(ModelUsage {
                    input_tokens_uncached: 10,
                    output_tokens: 5,
                    total_tokens: Some(15),
                    ..Default::default()
                })),
                Ok(ModelStreamEvent::ResponseCompleted {
                    stop_reason: StopReason::ToolCalls,
                }),
            ]),
            Script::Events(assistant_events("corrected-response", "Recovered.")),
        ]));
        let tools = Arc::new(RejectingArgumentsTools::default());
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let runtime = loop_runtime(
            &fixture,
            provider.clone(),
            tools.clone(),
            journal.clone(),
            Duration::from_secs(1),
        );
        assert!(matches!(
            runtime
                .execute(
                    fixture.attempt.clone(),
                    fixture.telemetry(),
                    InvocationSupervisor::new()
                )
                .await,
            AgentLoopExit::Completed { .. }
        ));
        assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
        assert_eq!(tools.validations.load(Ordering::SeqCst), 0);
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].route.route_ref, requests[1].route.route_ref);
        assert!(journal.failures().is_empty());
        assert!(journal.action_rejections().is_empty());
        assert_eq!(
            journal.committed_tool_calls(),
            vec![tool_call_id("invalid-json-call")]
        );
        let completions = lock(&journal.model_completions);
        assert_eq!(completions[0].usage.expect("valid fixture for undecodable arguments are durable tool failures and preserve usage").total_tokens, Some(15));
        assert_eq!(
            completions[0].tool_calls[0]
                .planning_failure
                .expect("valid fixture for undecodable arguments are durable tool failures and preserve usage")
                .as_str(),
            kind.as_str()
        );
        let diagnostic = completions[0].tool_calls[0]
            .argument_diagnostic
            .as_ref()
            .expect("retained evidence");
        assert_eq!(diagnostic.generation_limit, None);
        assert_eq!(diagnostic.reported_output_tokens, Some(5));
        assert_eq!(
            diagnostic.configured_output_tokens,
            Some(requests[0].max_output_tokens.get())
        );
        assert_eq!(
            diagnostic.configured_argument_bytes,
            Some(requests[0].stream_budget.max_tool_argument_bytes.get() as u64)
        );
        let results = lock(&journal.tool_results);
        let CanonicalContentPart::Text(text) = &results[0].message.content[0] else {
            panic!("text")
        };
        assert!(text.as_str().contains(kind.as_str()));
        assert!(
            journal
                .batches()
                .iter()
                .flat_map(|batch| &batch.events)
                .any(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
        );
    }
}

#[tokio::test]
async fn internal_tool_identity_cannot_bypass_exposure_and_gets_corrective_feedback() {
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                response_ref: provider_ref("bad-name"),
                response_model_key: model_key(),
            }),
            Ok(ModelStreamEvent::ToolCallUnexposed {
                name: wire_name("external_write"),
            }),
        ]),
        Script::Events(assistant_events("corrected", "Recovered.")),
    ]));
    let tools = Arc::new(RejectingArgumentsTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    );
    assert!(matches!(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new()
            )
            .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 2);
    assert!(journal.failures().is_empty());
    assert!(matches!(
        journal.action_rejections().as_slice(),
        [ModelActionRejectionCommit {
            rejection: ModelActionRejection::ToolNotExposed { .. },
            ..
        }]
    ));
}

#[derive(Default)]
struct MixedArgumentsTools {
    dispatched: AtomicUsize,
}
impl ToolExecutor for MixedArgumentsTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        if let Some(failure) = call.planning_failure {
            return Box::pin(async move { failure.outcome(&call) });
        }
        if call.call_id.as_str().starts_with("bad-") {
            return Box::pin(async move {
                musteragent_loop::ToolCallPlanningFailure::InvalidValue.outcome(&call)
            });
        }
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text("{\"ok\":true}"))],
                    tool_calls: vec![],
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: DurableToolCompletion::Succeeded,
            })
        })
    }
}

#[tokio::test]
async fn repeated_argument_errors_preserve_sibling_tools_and_do_not_exhaust_model_repair() {
    let fixture = Fixture::new();
    let scripts = (0..3)
        .map(|n| {
            Script::Events(tool_batch_events(
                &tool_call_id(&format!("bad-{n}")),
                &tool_call_id(&format!("good-{n}")),
            ))
        })
        .chain([Script::Events(assistant_events("repaired", "Done."))])
        .collect();
    let provider = Arc::new(ScriptedProvider::new(scripts));
    let tools = Arc::new(MixedArgumentsTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    );
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.requests().len(), 4);
    assert_eq!(tools.dispatched.load(Ordering::SeqCst), 3);
    assert_eq!(journal.committed_tool_calls().len(), 6);
    assert!(journal.action_rejections().is_empty());
    assert!(journal.failures().is_empty());
    for (i, result) in lock(&journal.tool_results).iter().enumerate() {
        let CanonicalContentPart::Text(text) = &result.message.content[0] else {
            panic!("feedback")
        };
        let value: serde_json::Value = serde_json::from_str(text.as_str())
            .expect("valid fixture for argument decode correction");
        assert_eq!(value["ok"], i % 2 == 1);
    }
}

#[tokio::test]
async fn length_truncation_takes_precedence_over_partial_json_validation() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("truncated-object");
    let mut events = tool_batch_events(&call_id, &tool_call_id("other-partial"));
    for event in &mut events {
        if let Ok(ModelStreamEvent::ToolCallCompleted { arguments, .. }) = event {
            *arguments = JsonValue::Object(BTreeMap::new());
        }
    }
    *events.last_mut().expect(
        "valid fixture for length truncation takes precedence over partial json validation",
    ) = Ok(ModelStreamEvent::ResponseCompleted {
        stop_reason: StopReason::MaxOutputTokens,
    });
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(events),
        Script::Events(assistant_events(
            "split-call",
            "Continuing with smaller calls.",
        )),
    ]));
    let tools = Arc::new(RejectingArgumentsTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let runtime = loop_runtime(
        &fixture,
        provider,
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    );
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(tools.validations.load(Ordering::SeqCst), 0);
    assert!(journal.action_rejections().is_empty());
    for result in lock(&journal.tool_results).iter() {
        let CanonicalContentPart::Text(text) = &result.message.content[0] else {
            panic!("feedback")
        };
        assert!(text.as_str().contains("token limit"));
        assert!(text.as_str().contains("never concatenate incomplete calls"));
    }
}
