//! The fake Home returns explicit original-owner facts; Loop tests deliberately
//! never infer publication from Tool message JSON or ordinary Assistant prose.
use super::*;
use musteragent_loop::RunCompletionReadiness as Readiness;

#[derive(Default)]
pub(super) struct CompletionFixture {
    readiness: VecDeque<Readiness>,
    required_work_race: bool,
    query_committed_counts: Vec<usize>,
    terminal_calls: usize,
}
impl FixtureJournal {
    pub(super) fn native_readiness(&self) -> Readiness {
        let count = lock(&self.committed_tool_calls).len();
        let mut native = lock(&self.native_completion);
        native.query_committed_counts.push(count);
        if native.readiness.len() > 1 {
            native.readiness.pop_front().expect("valid test fixture")
        } else {
            native
                .readiness
                .front()
                .copied()
                .unwrap_or(Readiness::Ordinary)
        }
    }
    pub(super) fn native_terminal_race(&self) -> bool {
        let mut native = lock(&self.native_completion);
        native.terminal_calls += 1;
        std::mem::take(&mut native.required_work_race)
    }
    pub(super) fn use_native(&self, states: &[Readiness]) {
        lock(&self.native_completion).readiness = states.iter().copied().collect();
    }
}
async fn run(
    fixture: &Fixture,
    journal: Arc<FixtureJournal>,
    provider: Arc<ScriptedProvider>,
    tools: Arc<dyn ToolExecutor>,
) -> AgentLoopExit {
    loop_runtime(fixture, provider, tools, journal, Duration::from_secs(1))
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await
}
fn publication_batch(first: &str, second: &str) -> Script {
    Script::Events(tool_batch_events(
        &tool_call_id(first),
        &tool_call_id(second),
    ))
}

#[tokio::test]
async fn complete_publication_batch_stops_without_an_extra_model_call() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Continue, Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![publication_batch(
        "publish", "deliver",
    )]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(CompletingTools)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.requests().len(), 1);
    let native = lock(&journal.native_completion);
    assert_eq!(
        native.query_committed_counts,
        [0, 2],
        "only query after all sibling receipts commit"
    );
    assert_eq!(native.terminal_calls, 1);
}

#[tokio::test]
async fn recovered_successful_publication_does_not_request_a_model_again() {
    let fixture = Fixture::new();
    let mut checkpoint = fixture.checkpoint();
    checkpoint.iteration = 1;
    checkpoint.completed_tool_calls = 2;
    let journal = Arc::new(FixtureJournal::new(checkpoint));
    journal.use_native(&[Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(CompletingTools)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn recovery_settles_pending_siblings_before_querying_publication_readiness() {
    let fixture = Fixture::new();
    let invocation_id = InvocationId::generate();
    let first = tool_call_id("publish");
    let second = tool_call_id("remaining-sibling");
    let mut checkpoint = fixture.checkpoint();
    checkpoint.iteration = 1;
    checkpoint.phase = LoopPhase::ToolsPending {
        invocation_id,
        remaining_tool_call_ids: vec![first.clone(), second.clone()],
    };
    let events = tool_batch_events(&first, &second)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    let journal = Arc::new(FixtureJournal::with_batches(
        checkpoint,
        vec![ModelEventBatch {
            invocation_id,
            iteration: 1,
            batch_index: 0,
            events,
        }],
    ));
    journal.use_native(&[Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(CompletingTools)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert!(provider.requests().is_empty());
    assert_eq!(lock(&journal.native_completion).query_committed_counts, [2]);
}

#[tokio::test]
async fn private_assistant_prose_cannot_finish_native_delivery() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Continue, Readiness::Continue, Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![
        Script::Events(assistant_events(
            "private",
            r#"{"done":true,"text":"not a public delivery"}"#,
        )),
        publication_batch("publish", "deliver"),
    ]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(CompletingTools)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(
        lock(&journal.native_completion).query_committed_counts,
        [0, 0, 2]
    );
}

#[tokio::test]
async fn terminal_required_work_race_rebuilds_context_instead_of_retrying_complete() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Ready]);
    lock(&journal.native_completion).required_work_race = true;
    let provider = Arc::new(ScriptedProvider::new(vec![publication_batch(
        "required-work",
        "publish",
    )]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(CompletingTools)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(
        provider.requests().len(),
        1,
        "required work must reach the next ContextPlan"
    );
    assert_eq!(lock(&journal.native_completion).terminal_calls, 2);
    assert_eq!(journal.snapshot().iteration, 1);
}

struct FailedSibling;
impl ToolExecutor for FailedSibling {
    fn execute(
        &self,
        attempt: &AttemptContext,
        call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        let failed = call.call_id == tool_call_id("failed-sibling");
        let completed = CompletingTools.execute(attempt, call);
        Box::pin(async move {
            let mut outcome = completed.await?;
            if failed && let ToolExecutionOutcome::Completed { terminal, .. } = &mut outcome {
                *terminal = DurableToolCompletion::Failed {
                    code: ErrorCode::DependencyUnavailable,
                };
            }
            Ok(outcome)
        })
    }
}
#[tokio::test]
async fn publication_with_failed_sibling_continues_until_home_proves_the_new_batch() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Continue, Readiness::Continue, Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![
        publication_batch("publish", "failed-sibling"),
        publication_batch("corrected-work", "new-publication"),
    ]));
    assert!(matches!(
        run(
            &fixture,
            journal.clone(),
            provider.clone(),
            Arc::new(FailedSibling)
        )
        .await,
        AgentLoopExit::Completed { .. }
    ));
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(
        lock(&journal.native_completion).query_committed_counts,
        [0, 2, 4]
    );
}

#[tokio::test]
async fn publication_with_unknown_sibling_waits_for_reconciliation() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Continue, Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![publication_batch(
        "publish", "unknown",
    )]));
    let tools = Arc::new(MixedReconciliationTools {
        wait_call: tool_call_id("unknown"),
        wait_invocation_id: InvocationId::generate(),
        observed_at: UtcTimestamp::from_unix_millis(1000),
        resume_not_before: UtcTimestamp::from_unix_millis(2000),
    });
    let exit = run(&fixture, journal.clone(), provider.clone(), tools).await;
    assert!(
        matches!(exit, AgentLoopExit::ReconciliationWaiting { .. }),
        "{exit:?}"
    );
    assert_eq!(provider.requests().len(), 1);
    let native = lock(&journal.native_completion);
    assert_eq!(native.query_committed_counts, [0]);
    assert_eq!(native.terminal_calls, 0);
}

struct RequiredProgress(AtomicUsize);
impl musteragent_context::RunProgressCoordinator for RequiredProgress {
    fn directive(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<musteragent_context::RunProgressDirective, ContextPlanError>>
    {
        let blocks_completion = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            Ok(musteragent_context::RunProgressDirective {
                private_instructions: vec![],
                blocks_completion,
            })
        })
    }
}
#[tokio::test]
async fn native_ready_still_respects_original_required_work_coordinator() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    journal.use_native(&[Readiness::Ready]);
    let provider = Arc::new(ScriptedProvider::new(vec![publication_batch(
        "finish-required-work",
        "deliver",
    )]));
    let progress = Arc::new(RequiredProgress(AtomicUsize::new(0)));
    let policy = DefaultLoopPolicy::new(
        Arc::new(FixtureExecutionPolicy {
            execution: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(1000),
            model_route_count: NonZeroU32::MIN,
        }),
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    )
    .with_run_progress_coordinator(progress.clone());
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        Arc::new(policy),
        provider.clone(),
        journal.clone(),
    );
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            InvocationSupervisor::new(),
        )
        .await;
    assert!(matches!(exit, AgentLoopExit::Completed { .. }), "{exit:?}");
    assert_eq!(provider.requests().len(), 1);
    assert!(progress.0.load(Ordering::SeqCst) >= 2);
    assert_eq!(lock(&journal.native_completion).terminal_calls, 1);
}
