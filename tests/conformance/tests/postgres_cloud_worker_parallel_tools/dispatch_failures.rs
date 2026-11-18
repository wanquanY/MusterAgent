use super::*;

struct RejectedDispatch(ErrorCode);
impl CapabilityDispatcher for RejectedDispatch {
    fn execution<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        material: &'a ToolExecutionMaterial,
        durable: &'a DurableToolInvocation,
        invocation: CapabilityInvocation,
        binding: musteragent_runtime_contract::execution_liveness::ExecutionBinding,
    ) -> Result<musteragent_tool_executor::ToolPhysicalExecution<'a>, LoopPortError> {
        // The dispatcher rejects while preparing the physical operation.
        let _ = (attempt, material, durable, invocation, binding);
        Err(port(self.0, false))
    }

    fn invoke<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _invocation: CapabilityInvocation,
    ) -> ApplicationFuture<'a, Result<DispatchedCapabilityOutcome, LoopPortError>> {
        Box::pin(async move { Err(port(self.0, false)) })
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: &'a AttemptContext,
        _material: &'a ToolExecutionMaterial,
        _durable: &'a DurableToolInvocation,
        _request: CapabilityReconciliation,
    ) -> ApplicationFuture<'a, Result<DispatchedReconciliationOutcome, LoopPortError>> {
        Box::pin(async move { Err(port(self.0, false)) })
    }
}

#[tokio::test]
async fn dispatch_failure_observation_preserves_root_cause_and_durable_rejections() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    migrate(&pool).await.expect("migrate");
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keyring(),
    ));
    let (fixture, attempt) = seed_loop(&pool, &content).await;
    install_loop_context(&pool, content.clone(), fixture).await;
    let journal = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keyring(),
        )
        .with_execution_liveness(
            Arc::new(musteragent_wire::ProtobufSnapshotCodec),
            live_runtime::clock(),
        ),
    );
    for code in [
        ErrorCode::InternalInvariantViolated,
        ErrorCode::DependencyUnavailable,
    ] {
        let executor = DurableToolExecutor::new(
            production_tool_material_source(&pool, codec.clone()),
            journal.clone(),
            content.clone(),
            Arc::new(RejectedDispatch(code)),
            Arc::new(FixedClock(database_now(&pool).await)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(live_runtime::supervision(&pool));
        let outcome = executor
            .execute(&attempt, planned_call(code.as_str()))
            .await;
        if code == ErrorCode::InternalInvariantViolated {
            assert!(
                matches!(&outcome, Err(error) if error.code == code),
                "journal observation must not mask the original invariant failure: {outcome:?}"
            );
        } else {
            assert!(
                outcome.is_ok(),
                "read-only dependency rejection becomes model feedback: {outcome:?}"
            );
        }
        let status: i16 = sqlx::query_scalar(
            "SELECT status FROM tool.tool_invocations WHERE run_id=$1 AND tool_call_id=$2",
        )
        .bind(fixture.run_id.into_uuid())
        .bind(code.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            status,
            if code == ErrorCode::InternalInvariantViolated {
                3
            } else {
                5
            }
        );
    }
}
