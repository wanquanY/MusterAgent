use super::*;

struct MixedFailureTools {
    failing: ToolCallId,
    code: ErrorCode,
    raw_port_error: bool,
}
impl ToolExecutor for MixedFailureTools {
    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let failed = call.call_id == self.failing;
        let code = self.code;
        let raw = self.raw_port_error;
        let completed = CompletingTools.execute(attempt, call);
        Box::pin(async move {
            if failed && raw {
                return Err(LoopPortError {
                    code,
                    retryable: true,
                });
            }
            let mut outcome = completed.await?;
            if failed && let ToolExecutionOutcome::Completed { message, terminal } = &mut outcome {
                message.content = vec![CanonicalContentPart::Text(model_text(&format!(
                    r#"{{"ok":false,"error":{{"code":"{}","retryable":true}}}}"#,
                    code.as_str()
                )))];
                *terminal = DurableToolCompletion::Failed { code };
            }
            Ok(outcome)
        })
    }
}

#[tokio::test]
async fn parallel_tool_feedback_requires_a_durable_outcome_and_preserves_siblings() {
    for (code, raw_port_error) in [
        (ErrorCode::DependencyTimeout, false),
        (ErrorCode::DependencyUnavailable, false),
        (ErrorCode::CredentialRefreshRequired, false),
        (ErrorCode::DependencyTimeout, true),
        (ErrorCode::DependencyUnavailable, true),
        (ErrorCode::CredentialRefreshRequired, true),
    ] {
        let fixture = Fixture::new();
        let failing = tool_call_id("failed-vision");
        let success = tool_call_id("successful-read");
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(tool_batch_events(&failing, &success)),
            Script::Events(assistant_events(
                "after-tool-failure",
                "Use the available result and change strategy.",
            )),
        ]));
        let runtime = loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(MixedFailureTools {
                failing: failing.clone(),
                code,
                raw_port_error,
            }),
            journal.clone(),
            Duration::from_secs(1),
        );
        let result = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        if raw_port_error {
            assert!(
                matches!(result, AgentLoopExit::Failed {
                stage: AgentLoopFailureStage::ToolExecution, code: actual, ..
            } if actual == code),
                "{result:?}"
            );
            assert_eq!(journal.committed_tool_calls(), vec![success]);
            assert_eq!(lock(&provider.requests).len(), 1);
            continue;
        }
        assert!(
            matches!(result, AgentLoopExit::Completed { .. }),
            "{result:?}"
        );
        assert_eq!(
            *lock(&journal.transitions),
            vec![
                FixtureTransition::ToolCommitted(failing),
                FixtureTransition::ToolCommitted(success)
            ]
        );
        assert!(journal.failures().is_empty());
        let requests = lock(&provider.requests);
        assert_eq!(
            requests.len(),
            2,
            "the Agent receives both observations in its next model turn"
        );
        let results = lock(&journal.tool_results);
        let feedback = results
            .iter()
            .flat_map(|result| &result.message.content)
            .filter_map(|part| {
                if let CanonicalContentPart::Text(text) = part {
                    Some(text.as_str())
                } else {
                    None
                }
            })
            .collect::<String>();
        assert!(feedback.contains(code.as_str()));
    }
}

#[tokio::test]
async fn preparation_and_postprocessing_failures_preserve_agent_and_sibling_results() {
    for after in [false, true] {
        let fixture = Fixture::new();
        let first = tool_call_id("first");
        let sibling = tool_call_id("sibling");
        let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
        let provider = Arc::new(ScriptedProvider::new(vec![
            Script::Events(tool_batch_events(&first, &sibling)),
            Script::Events(assistant_events(
                "continued",
                "Continue with the available results.",
            )),
        ]));
        let tools = Arc::new(CountingTools::default());
        let policy = Arc::new(DefaultLoopPolicy::new(
            Arc::new(FixtureExecutionPolicy {
                execution: RunExecutionPolicySnapshot::production_defaults(),
                admitted_at: UtcTimestamp::from_unix_millis(1_000),
                model_route_count: NonZeroU32::MIN,
            }),
            Arc::new(FixtureContext { run: fixture.run }),
            tools.clone(),
            Arc::new(EmptyControls),
        ));
        let runtime = activity_fixture::runtime_with_hook_fault(
            Arc::new(FixedClock),
            policy,
            provider.clone(),
            journal.clone(),
            Some((first.clone(), after)),
        );
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        if !after {
            assert!(matches!(exit, AgentLoopExit::Failed { .. }), "{exit:?}");
            assert_eq!(tools.executions.load(Ordering::SeqCst), 0);
            assert_eq!(lock(&provider.requests).len(), 1);
            assert!(
                lock(&journal.tool_results).is_empty(),
                "a policy dependency failure has no durable Tool terminal"
            );
            let failures = journal.failures();
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].0, ErrorCode::DependencyUnavailable);
            continue;
        }
        assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
        assert!(journal.failures().is_empty());
        assert_eq!(
            tools.executions.load(Ordering::SeqCst),
            if after { 2 } else { 1 }
        );
        assert_eq!(lock(&provider.requests).len(), 2);
        let results = lock(&journal.tool_results);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].message.tool_call_id.as_ref(), Some(&first));
        assert_eq!(results[1].message.tool_call_id.as_ref(), Some(&sibling));
        let text = results[0]
            .message
            .content
            .iter()
            .filter_map(|part| match part {
                CanonicalContentPart::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert!(text.contains("postprocessing_error"));
        assert!(text.contains(ErrorCode::DependencyUnavailable.as_str()));
    }
}
