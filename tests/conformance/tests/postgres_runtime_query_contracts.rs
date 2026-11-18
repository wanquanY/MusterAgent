//! Execute authority/evaluation reads against the migrated Cloud schema.
#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{Actor, InvocationId, PrincipalKind};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    PostgresContentStore, PostgresDelegationHistorySource, PostgresStandingGoalJudgeRunSource,
};
use musteragent_runtime_automation::EvaluationAgentStateSource;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_data_access::delegation_history::{
    DelegationHistorySource, HistoryAccess, HistoryError, HistoryInput, HistoryView,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_wire::ProtobufLedgerPayloadCodec;
use std::sync::Arc;

async fn request(pool: &sqlx::PgPool, f: running_loop::Fixture) -> ContentRef {
    PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    )
    .put(PutContent {
        tenant_id: f.tenant_id,
        media_type: MediaType::new("application/json").expect("JSON"),
        bytes: b"{}".to_vec(),
    })
    .await
    .expect("retained request content")
}

async fn model(
    pool: &sqlx::PgPool,
    f: running_loop::Fixture,
    sequence: i64,
    stop: i16,
) -> InvocationId {
    let id = InvocationId::generate();
    let request = request(pool, f).await;
    sqlx::query(
        "INSERT INTO runtime.model_invocations (
        invocation_id, tenant_id, conversation_id, run_id, attempt_id, purpose, status,
        model_route_ref, idempotency_key, request_content_id, request_hash, started_at_ms,
        credential_grant_ref, route_role, terminal_stop_reason, started_conversation_seq
    ) VALUES ($1,$2,$3,$4,$5,1,2,'test.model@1',$6,$7,$8,100,'test.grant',0,$9,$10::text::numeric)",
    )
    .bind(id.into_uuid())
    .bind(f.tenant_id.into_uuid())
    .bind(f.conversation_id.into_uuid())
    .bind(f.run_id.into_uuid())
    .bind(f.attempt_id.into_uuid())
    .bind(id.to_string())
    .bind(request.content_id.into_uuid())
    .bind(request.sha256.into_bytes().to_vec())
    .bind(stop)
    .bind(sequence.to_string())
    .execute(pool)
    .await
    .expect("seed model terminal");
    id
}

#[tokio::test]
async fn goal_stall_uses_the_latest_three_durable_model_positions() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("valid test fixture");
    let pool = pg.pool().await.expect("valid test fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("valid test fixture");
    let (f, _) = running_loop::seed_running_loop(&pool).await;
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let source = PostgresStandingGoalJudgeRunSource::new(pool.clone(), content);
    for sequence in 1..=3 {
        model(&pool, f, sequence, 1).await;
        assert_eq!(
            source
                .decision_protocol_stalled(f.run_id)
                .await
                .expect("valid test fixture"),
            sequence == 3
        );
    }
    model(&pool, f, 4, 2).await;
    assert!(
        !source
            .decision_protocol_stalled(f.run_id)
            .await
            .expect("valid test fixture"),
        "the newest tool call breaks the consecutive no-decision sequence"
    );
    pool.close().await;
}

#[tokio::test]
async fn delegated_history_requires_a_live_lease_and_pending_tool() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("valid test fixture");
    let pool = pg.pool().await.expect("valid test fixture");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("valid test fixture");
    let (f, attempt) = running_loop::seed_running_loop(&pool).await;
    let model_id = model(&pool, f, 1, 2).await;
    let invocation_id = InvocationId::generate();
    let request = request(&pool, f).await;
    sqlx::query("INSERT INTO tool.tool_invocations (
        invocation_id, tenant_id, conversation_id, run_id, requested_by_attempt_id,
        execution_attempt_id, tool_call_id, tool_ref, capability_provider_id, status,
        effect, approval_policy, retry_contract, idempotency_key, request_content_id,
        request_hash, deadline_ms
    ) VALUES ($1,$2,$3,$4,$5,$5,'history','delegation.inspect@2',$1,3,1,1,1,$6,$7,$8,9223372036854775807)")
        .bind(invocation_id.into_uuid()).bind(f.tenant_id.into_uuid()).bind(f.conversation_id.into_uuid())
        .bind(f.run_id.into_uuid()).bind(attempt.attempt_id.into_uuid()).bind(invocation_id.to_string())
        .bind(request.content_id.into_uuid()).bind(request.sha256.into_bytes().to_vec())
        .execute(&pool).await.expect("valid test fixture");
    sqlx::query("UPDATE runtime.loop_checkpoints SET phase=3, model_invocation_id=$1, remaining_tool_call_ids=ARRAY['history'] WHERE run_id=$2")
        .bind(model_id.into_uuid())
        .bind(f.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("valid test fixture");
    sqlx::query("INSERT INTO runtime.pending_tool_calls (run_id,invocation_id,tool_call_id,wire_name,arguments_json,ordinal)
        VALUES ($1,$2,'history','delegation_inspect','{}',0)")
        .bind(f.run_id.into_uuid()).bind(model_id.into_uuid()).execute(&pool).await.expect("valid test fixture");
    let source =
        PostgresDelegationHistorySource::new(pool.clone(), Arc::new(ProtobufLedgerPayloadCodec));
    let access = HistoryAccess {
        actor: Actor {
            tenant_id: f.tenant_id,
            principal_id: f.principal_id,
            principal_kind: PrincipalKind::User,
        },
        current_run_id: f.run_id,
        tool_fence: Some((attempt.attempt_id, invocation_id)),
    };
    let input = HistoryInput {
        view: HistoryView::Summary,
        handle: None,
        record_id: None,
        cursor: None,
        limit: 20,
        max_bytes: 16384,
        offset: 0,
    };
    assert!(
        source
            .read(&access, &input)
            .await
            .expect("valid test fixture")
            .entries
            .is_empty()
    );
    sqlx::query("UPDATE runtime.pending_tool_calls SET completed_at_ms=100, result_content_id=$2, result_content_hash=$3, result_content_bytes=2 WHERE run_id=$1")
        .bind(f.run_id.into_uuid())
        .bind(request.content_id.into_uuid())
        .bind(request.sha256.into_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("valid test fixture");
    assert!(matches!(
        source.read(&access, &input).await,
        Err(HistoryError::Denied)
    ));
    sqlx::query("UPDATE runtime.pending_tool_calls SET completed_at_ms=NULL, result_content_id=NULL, result_content_hash=NULL, result_content_bytes=NULL WHERE run_id=$1")
        .bind(f.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("valid test fixture");
    assert!(source.read(&access, &input).await.is_ok());
    sqlx::query("UPDATE runtime.leases SET expires_at_ms=0 WHERE run_id=$1")
        .bind(f.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("valid test fixture");
    assert!(matches!(
        source.read(&access, &input).await,
        Err(HistoryError::Denied)
    ));
    pool.close().await;
}
