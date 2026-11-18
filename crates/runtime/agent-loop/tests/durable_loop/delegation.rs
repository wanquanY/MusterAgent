use musteragent_identity::DelegationId;

use super::*;

#[tokio::test]
async fn synchronous_delegation_yields_the_real_loop_without_committing_a_tool_result() {
    let fixture = Fixture::new();
    let call_id = tool_call_id("delegate-wait-call");
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            response_ref: provider_ref("delegate-wait-response"),
            response_model_key: model_key(),
        }),
        Ok(ModelStreamEvent::ToolCallStarted {
            output_index: 0,
            call_id: call_id.clone(),
            name: wire_name("delegate_wait"),
            provider_item_ref: None,
            provider_replay: None,
        }),
        Ok(ModelStreamEvent::ToolCallCompleted {
            output_index: 0,
            call_id,
            arguments: JsonValue::Object(BTreeMap::from([(
                "goal".to_owned(),
                JsonValue::String("inspect the delegated boundary".to_owned()),
            )])),
        }),
        Ok(ModelStreamEvent::Usage(ModelUsage {
            input_tokens_uncached: 4,
            output_tokens: 2,
            total_tokens: Some(6),
            ..ModelUsage::default()
        })),
        Ok(ModelStreamEvent::ResponseCompleted {
            stop_reason: StopReason::ToolCalls,
        }),
    ])]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let delegation_id = DelegationId::generate();
    let child_activity_id = ActivityId::generate();
    let child_run_id = RunId::generate();
    let runtime = loop_runtime(
        &fixture,
        provider,
        Arc::new(DelegationWaitingTools {
            delegation_id,
            child_activity_id,
            child_run_id,
        }),
        Arc::clone(&journal),
        Duration::from_secs(1),
    );

    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    let AgentLoopExit::DelegationWaiting {
        delegation_id: observed_delegation_id,
        child_activity_id: observed_child_activity_id,
        child_run_id: observed_child_run_id,
        receipt,
    } = exit
    else {
        panic!("a synchronous Delegate must yield the worker at its durable child wait");
    };
    assert_eq!(observed_delegation_id, delegation_id);
    assert_eq!(observed_child_activity_id, child_activity_id);
    assert_eq!(observed_child_run_id, child_run_id);
    assert_eq!(receipt.attempt_outcome(), DurableAttemptOutcome::Waiting);
    assert_eq!(receipt.run_outcome(), DurableRunOutcome::Waiting);
    assert!(
        journal.committed_tool_calls().is_empty(),
        "the ChildReport, not child admission, is the Delegate Tool result"
    );
}

struct DelegationWaitingTools {
    delegation_id: DelegationId,
    child_activity_id: ActivityId,
    child_run_id: RunId,
}

impl ToolExecutor for DelegationWaitingTools {
    fn execute(
        &self,
        _attempt: &AttemptContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move {
            Ok(ToolExecutionOutcome::DelegationWaiting {
                delegation_id: self.delegation_id,
                child_activity_id: self.child_activity_id,
                child_run_id: self.child_run_id,
            })
        })
    }
}
