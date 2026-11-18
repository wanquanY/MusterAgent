use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Boundary {
    Begin,
    ModelFailure,
    PortFailure,
}

#[tokio::test]
async fn durable_stop_at_model_admission_never_dispatches_provider() {
    check(Boundary::Begin).await;
}

#[tokio::test]
async fn durable_stop_receipt_overrides_late_model_failure() {
    check(Boundary::ModelFailure).await;
}

#[tokio::test]
async fn durable_stop_receipt_overrides_late_policy_failure() {
    check(Boundary::PortFailure).await;
}

struct InvalidPolicy;
impl ExecutionPolicySource for InvalidPolicy {
    fn load(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        Box::pin(async {
            Err(LoopPortError {
                code: ErrorCode::PolicyDenied,
                retryable: false,
            })
        })
    }
}

async fn check(boundary: Boundary) {
    let fixture = Fixture::new();
    let mut journal = FixtureJournal::new(fixture.checkpoint());
    journal.durable_stop_at = Some(boundary);
    let journal = Arc::new(journal);
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Events(vec![Err(
        ProviderError {
            kind: ProviderErrorKind::Authentication,
            safe_code: ProviderErrorCode::AuthenticationFailed,
            retryable: false,
            retry_after_ms: None,
            diagnostic_ref: None,
        },
    )])]));
    let runtime = if boundary == Boundary::PortFailure {
        activity_fixture::runtime(
            Arc::new(FixedClock),
            Arc::new(DefaultLoopPolicy::new(
                Arc::new(InvalidPolicy),
                Arc::new(FixtureContext { run: fixture.run }),
                Arc::new(CompletingTools),
                Arc::new(EmptyControls),
            )),
            provider.clone(),
            journal.clone(),
        )
    } else {
        loop_runtime(
            &fixture,
            provider.clone(),
            Arc::new(CompletingTools),
            journal.clone(),
            Duration::from_secs(1),
        )
    };
    let supervisor = InvocationSupervisor::new();
    let exit = runtime
        .execute(
            fixture.attempt.clone(),
            fixture.telemetry(),
            supervisor.clone(),
        )
        .await;
    assert!(
        !supervisor.is_cancelled(),
        "durable owner must win even before heartbeat observes stop"
    );
    let AgentLoopExit::Cancelled { receipt } = exit else {
        panic!("expected cancellation receipt, got {exit:?}");
    };
    assert_eq!(receipt.attempt_outcome(), DurableAttemptOutcome::Cancelled);
    assert_eq!(receipt.run_outcome(), DurableRunOutcome::Cancelled);
    assert_eq!(journal.snapshot().phase, LoopPhase::Cancelled);
    assert_eq!(
        provider.requests().len(),
        usize::from(boundary == Boundary::ModelFailure)
    );
}
