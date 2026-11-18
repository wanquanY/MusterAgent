//! The fake Home joins original Tool outcomes to committed canonical feedback,
//! just as the SQL adapters do. It never interprets model-visible message JSON.
use super::*;
use musteragent_loop::{ToolRecoveryHistory, ToolRecoveryOutcome};

#[derive(Default)]
pub(crate) struct OriginalToolFacts {
    records: Mutex<Vec<(ToolCallId, ToolRecoveryOutcome)>>,
}

impl OriginalToolFacts {
    pub(super) fn require_permit(&self, journal: &FixtureJournal) -> Result<(), LoopPortError> {
        let records = lock(&self.records);
        let feedback = lock(&journal.tool_results);
        let mut history = ToolRecoveryHistory::default();
        for result in feedback.iter().rev() {
            let (_, outcome) = records
                .iter()
                .find(|(id, _)| id == &result.call_id)
                .expect("original outcome");
            if let Some(stop) =
                history.observe("files.list@4", Sha256::digest(b"same arguments"), *outcome)
            {
                return Err(port_error(stop.code, false));
            }
        }
        Ok(())
    }
}

struct FailingTools {
    facts: Arc<OriginalToolFacts>,
}

impl ToolExecutor for FailingTools {
    fn execute(
        &self,
        _: &AttemptContext,
        _: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        panic!("activity path required")
    }

    fn execute_activity_with_telemetry<'a>(
        &'a self,
        _: &'a AttemptContext,
        _: &'a AttemptTelemetryContext,
        _: &'a InvocationSupervisor,
        call: PlannedToolCall,
    ) -> ApplicationFuture<
        'a,
        Result<Result<ToolExecutionOutcome, LoopPortError>, musteragent_loop::InvocationAborted>,
    > {
        Box::pin(async move {
            lock(&self.facts.records).push((
                call.call_id.clone(),
                ToolRecoveryOutcome::Failed(ErrorCode::DependencyUnavailable),
            ));
            Ok(Ok(ToolExecutionOutcome::Completed {
                message: Box::new(feedback(call.call_id, call.name)),
                terminal: DurableToolCompletion::Failed {
                    code: ErrorCode::DependencyUnavailable,
                },
            }))
        })
    }
}

fn feedback(call_id: ToolCallId, name: WireName) -> CanonicalMessage {
    CanonicalMessage {
        role: CanonicalRole::Tool,
        content: vec![CanonicalContentPart::Text(model_text(
            "The original file query failed: dependency_unavailable.",
        ))],
        tool_call_id: Some(call_id),
        tool_name: Some(name),
        tool_calls: vec![],
        reasoning: None,
    }
}

#[tokio::test]
async fn third_committed_failure_stops_before_a_fourth_model_dispatch() {
    let facts = Arc::new(OriginalToolFacts::default());
    let (fixture, runtime, journal, provider) = runtime_with_scripts(
        (0..4)
            .map(|round| named_tool_script(&format!("failure-{round}"), 1))
            .collect(),
        Duration::ZERO,
        true,
        Arc::new(FailingTools {
            facts: facts.clone(),
        }),
        ToolExecutionMode::Sequential,
    );
    *lock(&journal.tool_recovery) = Some(facts.clone());
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(
        matches!(
            exit,
            AgentLoopExit::Failed {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
                receipt: Some(_),
                ..
            }
        ),
        "{exit:?}"
    );
    let requests = provider.scripts.requests();
    assert_eq!(
        requests.len(),
        3,
        "the fourth physical model call was never dispatched"
    );
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(
            request
                .input
                .iter()
                .filter(|message| message.role == CanonicalRole::Tool)
                .count(),
            index
        );
    }
    let feedback = lock(&journal.inner.tool_results);
    assert_eq!(
        feedback.len(),
        3,
        "the third original result is durably retained before stopping"
    );
    assert_eq!(feedback[2].call_id.as_str(), "failure-2-0");
    assert_eq!(journal.inner.snapshot().phase, LoopPhase::Failed);
    assert_eq!(
        journal.inner.failures(),
        [(ErrorCode::DependencyUnavailable, false)]
    );
}

#[tokio::test]
async fn recovered_unknown_write_is_handed_to_the_model_instead_of_ending_the_run() {
    assert_recovered_unknown(ErrorCode::DependencyUnavailable).await;
}

#[tokio::test]
async fn historical_fence_observation_is_model_feedback_without_another_dispatch() {
    assert_recovered_unknown(ErrorCode::AttemptLeaseStale).await;
}

/// A Worker lost while a write was in flight leaves an unknown outcome. The
/// recovered Run gives the model that observation and lets it decide; it does
/// not re-dispatch the original Tool on its own and does not fail the Run.
async fn assert_recovered_unknown(observed_error: ErrorCode) {
    let facts = Arc::new(OriginalToolFacts::default());
    let (fixture, runtime, journal, provider) = runtime_with_scripts(
        vec![Script::Events(assistant_events(
            "inspect-before-repeating",
            "The earlier step may not have finished. I'll check its result before repeating it.",
        ))],
        Duration::ZERO,
        true,
        Arc::new(FailingTools {
            facts: facts.clone(),
        }),
        ToolExecutionMode::Sequential,
    );
    let call = tool_call_id("original-unknown-write");
    lock(&facts.records).push((
        call.clone(),
        ToolRecoveryOutcome::Indeterminate {
            observed_error: Some(observed_error),
            may_have_side_effect: true,
        },
    ));
    lock(&journal.inner.tool_results).push(ToolResultCommit {
        call_id: call.clone(),
        message: feedback(call, wire_name("visible_tool")),
    });
    {
        let mut checkpoint = lock(&journal.inner.checkpoint);
        checkpoint.iteration = 1;
        checkpoint.completed_tool_calls = 1;
    }
    journal
        .inner
        .use_native(&[musteragent_loop::RunCompletionReadiness::Ready]);
    *lock(&journal.tool_recovery) = Some(facts.clone());
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    // The completed checkpoint needs no further model turn; nothing fails it.
    assert!(provider.scripts.requests().len() <= 1);
    assert!(journal.inner.failures().is_empty());
    assert_eq!(
        lock(&facts.records).len(),
        1,
        "no automatic second physical Tool dispatch"
    );
    assert_eq!(
        lock(&journal.inner.tool_results).len(),
        1,
        "unknown observation is retained"
    );
}
