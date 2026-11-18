use std::sync::Arc;
#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_conformance::MemoryBlobs;
use musteragent_content_ref::MediaType;
use musteragent_identity::{
    CapabilityProviderId, IdempotencyKey, InvocationId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::{PostgresContentStore, PostgresFencedToolInvocationJournal};
use musteragent_runtime_capability::{FencedToolInvocationJournal, ToolTerminalOutcome};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};
use musteragent_wire::ProtobufLedgerPayloadCodec;

#[tokio::test]
async fn pre_dispatch_failures_commit_once_without_starting_or_terminating_run() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let keys = running_loop::keyring();
    let content = PostgresContentStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::in_memory()),
        keys.clone(),
    );
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    for code in [
        ErrorCode::CapabilityUnavailable,
        ErrorCode::InvalidCommand,
        ErrorCode::PolicyDenied,
    ] {
        let invocation_id = InvocationId::generate();
        let request = content
            .put(PutContent {
                tenant_id: fixture.tenant_id,
                media_type: MediaType::new("application/json").expect("media"),
                bytes: b"{}".to_vec(),
            })
            .await
            .expect("request");
        journal
            .load_or_begin(
                &attempt,
                ToolInvocation {
                    invocation_id,
                    run_id: fixture.run_id,
                    requested_by_attempt_id: attempt.attempt_id,
                    execution_attempt_id: None,
                    tool_call_id: ToolCallId::new(code.as_str()).expect("call"),
                    tool_key: ToolKey::from_versioned_key(
                        VersionedKey::new("web.search@1").expect("key"),
                    ),
                    capability_provider_id: CapabilityProviderId::generate(),
                    status: ToolInvocationStatus::Requested,
                    effect: Effect::Read,
                    approval: ApprovalRequirement::Never,
                    idempotency_key: IdempotencyKey::new(code.as_str()).expect("key"),
                    request_ref: request,
                    result_ref: None,
                    interaction_id: None,
                    timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
                },
                ToolRetryContract::ReadOnly,
            )
            .await
            .expect("requested");
        let outcome = ToolTerminalOutcome::Failed {
            error: ErrorInfo {
                code,
                category: code.category(),
                safe_message: SafeDisplayText::new("Tool rejected before dispatch").expect("safe"),
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result: None,
        };
        let now = running_loop::database_now(&pool).await;
        for _ in 0..2 {
            let completed = journal
                .complete(&attempt, invocation_id, outcome.clone(), now)
                .await
                .expect("durable rejection");
            assert_eq!(completed.invocation.status, ToolInvocationStatus::Failed);
            assert_eq!(completed.recorded_outcome, Some(outcome.clone()));
        }
        let state: (i16, Option<i64>, Option<uuid::Uuid>) = sqlx::query_as(
            "SELECT status, started_at_ms, execution_attempt_id FROM tool.tool_invocations WHERE invocation_id=$1")
            .bind(invocation_id.into_uuid()).fetch_one(&pool).await.expect("state");
        assert_eq!(state, (5, None, None));
    }
    let status: i16 = sqlx::query_scalar("SELECT status FROM runtime.runs WHERE run_id=$1")
        .bind(fixture.run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("Run");
    assert_eq!(status, 2, "ordinary Tool rejection must not end the Run");
}
