use super::*;
use musteragent_identity::WorkspaceId;

#[tokio::test]
async fn stale_tenant_admission_is_rejected_after_workspace_assignment() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let batch = admission_batch(&pool, &fixture, "before-assignment").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    // Exercise the persistence admission boundary with a valid old Snapshot
    // after ownership changes. This setup is not the migration command itself.
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1 WHERE conversation_id = $2",
    )
    .bind(WorkspaceId::generate().into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("assign fixture");
    let error = admission_store(&pool)
        .admit(batch)
        .await
        .expect_err("stale frozen Tenant scope must not admit into an assigned Conversation");
    assert!(matches!(
        error,
        PostgresAdmissionError::ConversationUnavailable
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE conversation_id = $1")
            .bind(fixture.conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn accepted_admission_receipt_replays_after_assignment_without_a_new_run() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let batch = admission_batch(&pool, &fixture, "accepted-before-assignment").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    let store = admission_store(&pool);
    let first = store.admit(batch.clone()).await.expect("first admission");
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1 WHERE conversation_id = $2",
    )
    .bind(WorkspaceId::generate().into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    let duplicate = store
        .admit(batch)
        .await
        .expect("read-only historical receipt recovery");
    assert_eq!(duplicate.ledger.first_sequence, first.ledger.first_sequence);
    assert_eq!(duplicate.ledger.last_sequence, first.ledger.last_sequence);
    assert_eq!(duplicate.ledger.disposition, AppendDisposition::Duplicate);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE conversation_id = $1")
            .bind(fixture.conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}
