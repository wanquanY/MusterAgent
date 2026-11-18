use super::*;
use musteragent_execution_context::*;
use musteragent_identity::ConversationSeq;
struct RepairContext {
    run: RunContext,
}
impl ContextPlanner for RepairContext {
    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>> {
        let mut iteration = model_request(
            self.run,
            input.attempt,
            input.next_iteration,
            input.active_model_route_ordinal,
            input.continuation_ref,
            input.consecutive_empty_responses,
        );
        let bytes = b"Original useful answer";
        iteration.delivery_repair = Some(
            PublicDeliveryRejectionV1 {
                candidate: PublicDeliveryCandidateProof {
                    invocation_id: InvocationId::generate(),
                    assistant_message_id: MessageId::generate(),
                    assistant_output_id: OutputId::generate(),
                    ledger_cutoff: ConversationSeq::new(4).expect("valid test fixture"),
                    content_ref: ContentRef {
                        content_id: ContentId::generate(),
                        media_type: MediaType::new("text/plain;charset=utf-8")
                            .expect("valid test fixture"),
                        byte_length: bytes.len() as u64,
                        sha256: Sha256::digest(bytes),
                    },
                },
                diagnostics: vec![
                    PublicDeliveryDiagnostic::new(PublicDeliveryDiagnosticKind::MalformedJson, "/")
                        .expect("valid test fixture"),
                ],
                disposition: PublicDeliveryRepairDisposition::FormatOnly,
                cut: PublicDeliveryRepairCut {
                    progress_sequence: None,
                    control_sequence: None,
                },
            }
            .binding()
            .expect("valid test fixture"),
        );
        iteration.request.tools.clear();
        Box::pin(async move { Ok(iteration) })
    }
}
fn runtime(
    fixture: &Fixture,
    provider: Arc<dyn ModelProvider>,
    tools: Arc<dyn ToolExecutor>,
    journal: Arc<FixtureJournal>,
) -> DurableAgentLoop {
    let mut execution = RunExecutionPolicySnapshot::production_defaults();
    execution.recovery_policy = non_zero_u32(8).into();
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution,
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(RepairContext { run: fixture.run }),
        tools,
        Arc::new(EmptyControls),
    ));
    activity_fixture::runtime(Arc::new(FixedClock), policy, provider, journal)
}
#[tokio::test]
async fn repair_tool_action_terminates_original_episode_without_dispatch_or_generic_retry() {
    let fixture = Fixture::new();
    let events = vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("repair-tool"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::ToolCallUnexposed {
            name: wire_name("interaction_question"),
        }),
    ];
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(events),
        Script::Events(assistant_events("must-not-run", "unused")),
    ]));
    let tools = Arc::new(CountingTools::default());
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let result = runtime(&fixture, provider.clone(), tools.clone(), journal.clone())
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert_durable_failure(
        result,
        AgentLoopFailureStage::ModelExecution,
        ErrorCode::ModelResponseInvalid,
        false,
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
    assert!(matches!(
        &journal.action_rejections()[0].rejection,
        ModelActionRejection::PublicDeliveryRepairViolation {
            violation: PublicDeliveryRepairViolation::ToolCallDuringRepair,
            ..
        }
    ));
    assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
}
#[tokio::test]
async fn repair_empty_and_incomplete_normal_completions_cannot_take_empty_response_retries() {
    for (text, stop, expected) in [
        (
            None,
            StopReason::Stop,
            PublicDeliveryRepairViolation::EmptyCompletion,
        ),
        (
            Some("{\"deliveries\":"),
            StopReason::MaxOutputTokens,
            PublicDeliveryRepairViolation::IncompleteCompletion,
        ),
    ] {
        let fixture = Fixture::new();
        let mut events = vec![Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("repair-ended"),
            response_model_key: model_key(),
        })];
        if let Some(text) = text {
            events.push(Ok(ModelStreamEvent::TextDelta {
                output_index: 0,
                delta: model_text(text),
            }));
        }
        events.push(Ok(ModelStreamEvent::Usage(ModelUsage {
            output_tokens: 4,
            total_tokens: Some(8),
            ..Default::default()
        })));
        events.push(Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: stop,
        }));
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(events),
            Script::Events(assistant_events("must-not-run", "unused")),
        ]));
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let result = runtime(
            &fixture,
            provider.clone(),
            Arc::new(CountingTools::default()),
            journal.clone(),
        )
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
        assert_durable_failure(
            result,
            AgentLoopFailureStage::ModelExecution,
            ErrorCode::ModelResponseInvalid,
            false,
        );
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(journal.action_rejections().len(), 1);
        assert!(
            matches!(&journal.action_rejections()[0].rejection,ModelActionRejection::PublicDeliveryRepairViolation{violation,..} if *violation==expected)
        );
        assert!(journal.completion_kinds().is_empty());
    }
}

#[tokio::test]
async fn repair_transport_interruption_remains_physical_failure_not_semantic_rejection() {
    use musteragent_runtime_model::{ProviderError, ProviderErrorCode, ProviderErrorKind};
    let fixture = Fixture::new();
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("repair-network"),
            response_model_key: model_key(),
        }),
        Err(ProviderError {
            kind: ProviderErrorKind::Transport,
            safe_code: ProviderErrorCode::TransportFailed,
            retryable: true,
            retry_after_ms: None,
            diagnostic_ref: None,
        }),
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let result = runtime(
        &fixture,
        provider.clone(),
        Arc::new(CountingTools::default()),
        journal.clone(),
    )
    .execute(
        fixture.attempt.clone(),
        fixture.telemetry(),
        InvocationSupervisor::new(),
    )
    .await;
    assert!(
        matches!(result, AgentLoopExit::Failed { code, .. } if code != ErrorCode::ModelResponseInvalid),
        "{result:?}"
    );
    assert_eq!(provider.requests().len(), 1);
    assert!(journal.action_rejections().is_empty());
    assert!(journal.completion_kinds().is_empty());
}
