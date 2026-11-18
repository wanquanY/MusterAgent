use super::*;
use musteragent_approval::ApprovalRequest;

struct ApprovalBatchTools {
    journal: Arc<FixtureJournal>,
    barrier: Barrier,
    first_batch: AtomicBool,
    approved: Mutex<BTreeSet<ToolCallId>>,
    prepared: Mutex<BTreeMap<InvocationId, ToolCallId>>,
    executed: Mutex<Vec<ToolCallId>>,
    requested: Mutex<Vec<ToolCallId>>,
}
impl ToolExecutor for ApprovalBatchTools {
    fn execute(
        &self,
        _: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async move {
            if self.first_batch.load(Ordering::SeqCst) {
                self.barrier.wait().await;
            }
            lock(&self.executed).push(call.call_id.clone());
            if call.call_id.as_str().starts_with("approve-")
                && !lock(&self.approved).contains(&call.call_id)
            {
                let invocation_id = InvocationId::generate();
                lock(&self.prepared).insert(invocation_id, call.call_id);
                return Ok(ToolExecutionOutcome::ApprovalRequired {
                    request: ApprovalRequest {
                        invocation_id,
                        interaction_id: InteractionId::generate(),
                        prompt_ref: ContentRef {
                            content_id: ContentId::generate(),
                            media_type: MediaType::new("application/json")
                                .expect("valid fixture for execute"),
                            byte_length: 2,
                            sha256: Sha256::digest(b"{}"),
                        },
                        response_schema: SchemaRef {
                            schema_key: VersionedKey::new("approval.response@1")
                                .expect("valid fixture for execute"),
                            schema_hash: Sha256::digest(b"schema"),
                        },
                        requested_at: UtcTimestamp::from_unix_millis(1_000),
                        expires_at: None,
                    },
                });
            }
            let failed = call.call_id.as_str() == "failed-read";
            Ok(ToolExecutionOutcome::Completed {
                message: Box::new(CanonicalMessage {
                    role: CanonicalRole::Tool,
                    content: vec![CanonicalContentPart::Text(model_text(if failed {
                        r#"{"ok":false,"error":{"code":"invalid_command","message":"JSON pointer does not exist; inspect page index"}}"#
                    } else {
                        r#"{"ok":true}"#
                    }))],
                    tool_calls: vec![],
                    tool_call_id: Some(call.call_id),
                    tool_name: Some(call.name),
                    reasoning: None,
                }),
                terminal: if failed {
                    DurableToolCompletion::Failed {
                        code: ErrorCode::InvalidCommand,
                    }
                } else {
                    DurableToolCompletion::Succeeded
                },
            })
        })
    }
    fn request_approval(
        &self,
        _: &AttemptContext,
        request: ApprovalRequest,
    ) -> ApplicationFuture<'_, Result<InteractionId, LoopPortError>> {
        Box::pin(async move {
            assert!(
                lock(&self.executed).len() >= 4,
                "all concurrent siblings must finish first"
            );
            let committed = self.journal.committed_tool_calls();
            assert!(committed.contains(&tool_call_id("successful-read")));
            assert!(
                committed.contains(&tool_call_id("failed-read")),
                "a failed Tool must also be committed for the model"
            );
            assert!(lock(&self.journal.failures).is_empty());
            let call = lock(&self.prepared)
                .remove(&request.invocation_id)
                .expect("valid fixture for request approval");
            lock(&self.requested).push(call.clone());
            // Simulate the user's response before the next execute() recovery.
            lock(&self.approved).insert(call);
            Ok(request.interaction_id)
        })
    }
}

#[tokio::test]
async fn parallel_approvals_wait_after_success_and_failure_then_resume_without_duplicate_tools() {
    let fixture = Fixture::new();
    let calls = [
        "approve-first",
        "successful-read",
        "approve-second",
        "failed-read",
    ]
    .map(tool_call_id);
    let mut events = tool_batch_events(&calls[0], &calls[1]);
    let mut extras = tool_batch_events(&calls[2], &calls[3]);
    for event in &mut extras[1..5] {
        if let Ok(
            ModelStreamEvent::ToolCallStarted { output_index, .. }
            | ModelStreamEvent::ToolCallCompleted { output_index, .. },
        ) = event
        {
            *output_index += 2;
        }
    }
    events.splice(5..5, extras[1..5].iter().cloned());
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(events),
        Script::Events(assistant_events("resumed-after-approvals", "done")),
    ]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let tools = Arc::new(ApprovalBatchTools {
        journal: journal.clone(),
        barrier: Barrier::new(4),
        first_batch: AtomicBool::new(true),
        approved: Mutex::new(BTreeSet::new()),
        prepared: Mutex::new(BTreeMap::new()),
        executed: Mutex::new(vec![]),
        requested: Mutex::new(vec![]),
    });
    let runtime = loop_runtime(
        &fixture,
        provider.clone(),
        tools.clone(),
        journal.clone(),
        Duration::from_secs(1),
    );
    let first = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        ),
    )
    .await
    .expect("valid fixture for approval batch");
    assert!(matches!(first, AgentLoopExit::Waiting { .. }), "{first:?}");
    assert_eq!(
        journal.committed_tool_calls(),
        vec![calls[1].clone(), calls[3].clone()]
    );
    tools.first_batch.store(false, Ordering::SeqCst);
    let second = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(second, AgentLoopExit::Waiting { .. }),
        "{second:?}"
    );
    let third = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(third, AgentLoopExit::Completed { .. }),
        "{third:?}"
    );
    assert_eq!(
        *lock(&tools.requested),
        vec![calls[0].clone(), calls[2].clone()]
    );
    for id in [&calls[1], &calls[3]] {
        assert_eq!(
            lock(&tools.executed)
                .iter()
                .filter(|call| *call == id)
                .count(),
            1
        );
    }
    assert_eq!(journal.committed_tool_calls().len(), 4);
    assert_eq!(
        provider.requests().len(),
        2,
        "approval recovery must not repeat the model invocation"
    );
    assert!(lock(&journal.failures).is_empty());
}
