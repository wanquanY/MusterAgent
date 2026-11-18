use super::*;

#[tokio::test]
async fn fixed_protocol_exhaustion_fails_terminally_without_entering_budget_wait() {
    let fixture = Fixture::new();
    let journal = Arc::new(FixtureJournal::new(fixture.checkpoint()));
    let policy = Arc::new(TerminalGatePolicy {
        execution: PinnedRunExecutionPolicy {
            snapshot: RunExecutionPolicySnapshot::production_defaults(),
            admitted_at: UtcTimestamp::from_unix_millis(1_000),
            model_route_count: NonZeroU32::MIN,
        },
    });
    let provider = Arc::new(ScriptedProvider::new(vec![Script::Pending]));
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
        AgentLoopFailureStage::PolicyGate,
        ErrorCode::InvalidCommand,
        false,
    );
    assert!(provider.requests().is_empty());
    assert_eq!(journal.failures(), vec![(ErrorCode::InvalidCommand, false)]);
    assert_eq!(journal.snapshot().phase, LoopPhase::Failed);
}

struct TerminalGatePolicy {
    execution: PinnedRunExecutionPolicy,
}

impl LoopPolicy for TerminalGatePolicy {
    fn load_execution_policy(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<PinnedRunExecutionPolicy, LoopPortError>> {
        let execution = self.execution;
        Box::pin(async move { Ok(execution) })
    }

    fn gate(
        &self,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<GateVerdict, LoopPortError>> {
        Box::pin(async {
            Ok(GateVerdict::TerminalFailure {
                code: ErrorCode::InvalidCommand,
            })
        })
    }

    fn transform_context(
        &self,
        _input: ContextPlanInput,
    ) -> ApplicationFuture<'_, Result<ModelIteration, musteragent_context::ContextPlanError>> {
        Box::pin(async { unreachable!("terminal gate must run before context planning") })
    }

    fn before_tool(
        &self,
        _call: &PlannedToolCall,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolGate, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before Tool policy") })
    }

    fn execute_tool(
        &self,
        _attempt: &AttemptContext,
        _telemetry: &AttemptTelemetryContext,
        _call: PlannedToolCall,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before Tool execution") })
    }

    fn after_tool(
        &self,
        _call: &PlannedToolCall,
        _outcome: ToolExecutionOutcome,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<ToolExecutionOutcome, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before Tool completion") })
    }

    fn should_stop(
        &self,
        _turn: TurnSummary,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<bool, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before turn completion") })
    }

    fn prepare_next_turn(
        &self,
        _turn: TurnSummary,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<NextTurnDecision, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before the next turn") })
    }

    fn fallback_after_model_failure(
        &self,
        _failure: ModelFailure,
        _context: &StepContext,
    ) -> ApplicationFuture<'_, Result<FallbackDecision, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before model fallback") })
    }

    fn drain_steering(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before steering") })
    }

    fn drain_follow_up(
        &self,
        _attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<u32, LoopPortError>> {
        Box::pin(async { unreachable!("terminal gate must run before follow-up") })
    }
}
