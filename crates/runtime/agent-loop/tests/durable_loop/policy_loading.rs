use super::*;

struct PolicySource {
    failure: Mutex<Option<LoopPortError>>,
}
impl ExecutionPolicySource for PolicySource {
    fn load(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        Box::pin(async {
            if let Some(error) = lock(&self.failure).as_ref() {
                return Err(LoopPortError {
                    code: error.code,
                    retryable: error.retryable,
                });
            }
            Ok(PinnedRunExecutionPolicy {
                snapshot: RunExecutionPolicySnapshot::production_defaults(),
                admitted_at: UtcTimestamp::from_unix_millis(1_000),
                model_route_count: NonZeroU32::MIN,
            })
        })
    }
}

#[tokio::test]
async fn transient_policy_loading_preserves_checkpoint_for_lease_recovery() {
    let fixture = Fixture::new();
    let source = Arc::new(PolicySource {
        failure: Mutex::new(Some(LoopPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        })),
    });
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(
        assistant_events("recovered-policy", "Recovered the frozen policy."),
    )]));
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let policy = Arc::new(DefaultLoopPolicy::new(
        source.clone(),
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        journal.clone(),
    );
    let before = journal.snapshot();
    for _ in 0..2 {
        let exit = runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await;
        assert!(matches!(
            exit,
            AgentLoopExit::Failed {
                stage: AgentLoopFailureStage::LoadPolicy,
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                receipt: None,
                ..
            }
        ));
        assert!(journal.failures().is_empty());
        assert_eq!(journal.snapshot(), before);
        assert!(provider.requests().is_empty());
    }
    *lock(&source.failure) = None;
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
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn invalid_policy_loading_still_commits_a_terminal_failure() {
    let fixture = Fixture::new();
    let source = Arc::new(PolicySource {
        failure: Mutex::new(Some(LoopPortError {
            code: ErrorCode::InternalInvariantViolated,
            retryable: false,
        })),
    });
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let provider = Arc::new(ScriptedProvider::new(vec![]));
    let policy = Arc::new(DefaultLoopPolicy::new(
        source,
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    let runtime = activity_fixture::runtime(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        journal.clone(),
    );
    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::LoadPolicy,
        ErrorCode::InternalInvariantViolated,
        false,
    );
    assert!(provider.requests().is_empty());
}

#[tokio::test]
async fn obsolete_snapshot_is_an_explicit_compatibility_failure_before_dispatch() {
    struct Legacy;
    impl ExecutionPolicySource for Legacy {
        fn load(
            &self,
            _: &AttemptContext,
        ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
            Box::pin(async {
                Ok(PinnedRunExecutionPolicy {
                    snapshot: RunExecutionPolicySnapshot::legacy_defaults(),
                    admitted_at: UtcTimestamp::from_unix_millis(1_000),
                    model_route_count: NonZeroU32::MIN,
                })
            })
        }
    }
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let provider = Arc::new(ScriptedProvider::new(vec![]));
    let policy = Arc::new(DefaultLoopPolicy::new(
        Arc::new(Legacy),
        Arc::new(FixtureContext { run: fixture.run }),
        Arc::new(CompletingTools),
        Arc::new(EmptyControls),
    ));
    let runtime = DurableAgentLoop::new(
        Arc::new(FixedClock),
        policy,
        provider.clone(),
        journal.clone(),
    );
    assert_durable_failure(
        runtime
            .execute(
                fixture.attempt.clone(),
                fixture.telemetry(),
                InvocationSupervisor::new(),
            )
            .await,
        AgentLoopFailureStage::LoadPolicy,
        ErrorCode::UnsupportedSchemaVersion,
        false,
    );
    assert!(provider.requests().is_empty());
}
