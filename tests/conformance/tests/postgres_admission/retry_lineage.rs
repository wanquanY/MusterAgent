//! Retry lineage reuses `parent_run_id` but is not a delegation edge: the
//! failed source must never cascade a stop into the Run retried from it.

use super::input_message::fixture;
use musteragent_identity::RunId;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

async fn cancel_requested(pool: &PgPool, run_id: RunId) -> Option<i64> {
    sqlx::query_scalar("SELECT cancel_requested_at_ms FROM runtime.runs WHERE run_id = $1")
        .bind(run_id.into_uuid())
        .fetch_one(pool)
        .await
        .expect("read run cancellation")
}

async fn parent_cascade_stops(pool: &PgPool, run_id: RunId) -> i64 {
    let key = format!(
        "orchestration-stop:{}",
        uuid::Uuid::new_v5(&run_id.into_uuid(), b"parent-cascade-stop")
    );
    sqlx::query_scalar("SELECT count(*) FROM event_store.ledger_records WHERE idempotency_key = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("count parent cascade stop records")
}

#[tokio::test]
async fn retried_run_survives_reconciliation_and_can_delegate() {
    let _suite_guard = super::POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = fixture::InputMessageHarness::new(pool.clone()).await;
    let (conversation_id, participant) = harness.create_conversation("retry-lineage").await;
    let (message_id, source_run_id) = harness
        .submit_user_message(conversation_id, participant, "retry-lineage-1", "hello")
        .await;
    harness
        .claim_and_fail_run(source_run_id, "worker-retry-lineage-1")
        .await;
    let retry_run_id = harness
        .retry_failed_run(
            conversation_id,
            participant,
            source_run_id,
            message_id,
            1,
            "retry-lineage-retry",
        )
        .await;

    harness
        .cloud_delegation()
        .tick(harness.now)
        .await
        .expect("delegation reconciliation tick");
    assert_eq!(
        cancel_requested(&pool, retry_run_id).await,
        None,
        "a failed retry source must not cancel the retry"
    );
    assert_eq!(parent_cascade_stops(&pool, retry_run_id).await, 0);

    // The failed source is lineage, not a stopped delegation parent: the retry
    // may still delegate, and its child is a live delegated Run.
    let attempt = harness
        .claim_run(retry_run_id, "worker-retry-lineage-2")
        .await;
    let child_run_id = harness
        .spawn_delegated_child(&attempt, "inspect the retried request")
        .await;
    harness
        .cloud_delegation()
        .tick(harness.now)
        .await
        .expect("second delegation reconciliation tick");
    assert_eq!(cancel_requested(&pool, retry_run_id).await, None);
    assert_eq!(cancel_requested(&pool, child_run_id).await, None);
}

#[tokio::test]
async fn delegated_child_of_a_failed_parent_is_still_stopped() {
    let _suite_guard = super::POSTGRES_ADMISSION_SUITE.lock().await;
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    let harness = fixture::InputMessageHarness::new(pool.clone()).await;
    let (conversation_id, participant) = harness.create_conversation("delegated-stop").await;
    let (_, parent_run_id) = harness
        .submit_user_message(conversation_id, participant, "delegated-stop-1", "hello")
        .await;
    let parent = harness
        .claim_run(parent_run_id, "worker-delegated-stop-1")
        .await;
    let child_run_id = harness
        .spawn_delegated_child(&parent, "work the parent abandons")
        .await;
    harness.fail_claimed_attempt(&parent).await;

    harness
        .cloud_delegation()
        .tick(harness.now)
        .await
        .expect("delegation reconciliation tick");
    assert!(
        cancel_requested(&pool, child_run_id).await.is_some(),
        "a live delegated child of a failed parent must be stopped"
    );
}
