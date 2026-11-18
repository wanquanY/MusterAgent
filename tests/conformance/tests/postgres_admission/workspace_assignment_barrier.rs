use super::*;
use musteragent_identity::{CommandId, WorkspaceId};
use musteragent_postgres::{
    PostgresWorkspaceAssignmentPreparationStore, PostgresWorkspaceResourceRootAuthority,
};
use musteragent_runtime_resource::{EnsureWorkspaceResourceRoot, WorkspaceResourceRootAuthority};
use musteragent_runtime_session::workspace_assignment::{
    PrepareWorkspaceAssignment, WorkspaceAssignmentError, WorkspaceAssignmentPhase,
    WorkspaceAssignmentPreparationStore,
};

pub(super) async fn preparation(
    pool: &PgPool,
    fixture: &AdmissionFixture,
) -> PrepareWorkspaceAssignment {
    let request = PrepareWorkspaceAssignment {
        command_id: CommandId::generate(),
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        conversation_id: fixture.conversation_id,
        target_workspace_id: WorkspaceId::generate(),
        expected_home_epoch: HomeEpoch::new(1).unwrap(),
    };
    PostgresWorkspaceResourceRootAuthority::new(pool.clone())
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor: request.actor,
            workspace_id: request.target_workspace_id,
        })
        .await
        .unwrap();
    request
}

#[tokio::test]
async fn preparation_survives_recreation_and_cancellation_unblocks_admission() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    let receipt = store.prepare(request).await.unwrap();
    let restarted = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    assert_eq!(restarted.prepare(request).await.unwrap(), receipt);
    let batch = admission_batch(&pool, &fixture, "migration-fenced").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    assert!(matches!(
        admission_store(&pool).admit(batch.clone()).await,
        Err(PostgresAdmissionError::WorkspaceAssignmentPending)
    ));
    let cancelled = restarted.cancel(request).await.unwrap();
    assert_eq!(cancelled.phase, WorkspaceAssignmentPhase::Cancelled);
    assert_eq!(cancelled.prepared_at, receipt.prepared_at);
    assert_eq!(store.prepare(request).await.unwrap(), cancelled);
    assert_eq!(store.cancel(request).await.unwrap(), cancelled);
    admission_store(&pool)
        .admit(batch)
        .await
        .expect("cancelled barrier releases old scope");
    let updated_at: i64 = sqlx::query_scalar(
        "SELECT updated_at_ms FROM conversation.conversations WHERE conversation_id = $1",
    )
    .bind(fixture.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(updated_at, 1, "preparation is not user activity");
}

#[tokio::test]
async fn preparing_keeps_accepted_run_and_exact_admission_receipt() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let batch = admission_batch(&pool, &fixture, "accepted-before-barrier").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    let first = admission_store(&pool).admit(batch.clone()).await.unwrap();
    let request = preparation(&pool, &fixture).await;
    PostgresWorkspaceAssignmentPreparationStore::new(pool.clone())
        .prepare(request)
        .await
        .unwrap();
    let replay = admission_store(&pool).admit(batch).await.unwrap();
    assert_eq!(replay.ledger.disposition, AppendDisposition::Duplicate);
    assert_eq!(replay.ledger.last_sequence, first.ledger.last_sequence);
    let run_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE conversation_id = $1")
            .bind(fixture.conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(run_count, 1);
}

#[tokio::test]
async fn preparation_pins_owner_target_and_command_with_no_silent_retarget() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let store = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    let mut foreign = request;
    foreign.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        store.prepare(foreign).await,
        Err(WorkspaceAssignmentError::AuthorityChanged)
    );
    let mut epoch = request;
    epoch.expected_home_epoch = HomeEpoch::new(2).unwrap();
    assert_eq!(
        store.prepare(epoch).await,
        Err(WorkspaceAssignmentError::AuthorityChanged)
    );
    let other = preparation(&pool, &fixture).await;
    store.prepare(request).await.unwrap();
    assert_eq!(
        store.prepare(other).await,
        Err(WorkspaceAssignmentError::IdentityConflict)
    );
    assert_eq!(
        store
            .prepare(PrepareWorkspaceAssignment {
                target_workspace_id: other.target_workspace_id,
                ..request
            })
            .await,
        Err(WorkspaceAssignmentError::IdentityConflict)
    );
    assert_eq!(
        store.cancel(other).await,
        Err(WorkspaceAssignmentError::IdentityConflict)
    );
    store.cancel(request).await.unwrap();
    store
        .prepare(other)
        .await
        .expect("new command after explicit cancellation");
    let delete = sqlx::query(
        "DELETE FROM conversation.workspace_assignment_preparations WHERE command_id = $1",
    )
    .bind(request.command_id.into_uuid())
    .execute(&pool)
    .await;
    assert!(
        delete.is_err(),
        "durable command identity must survive cancellation"
    );
}

#[tokio::test]
async fn admission_waiting_on_conversation_observes_newly_committed_barrier() {
    let (_guard, _postgres, pool) = runtime_pool().await;
    let fixture = seed_fixture(&pool, None).await;
    let request = preparation(&pool, &fixture).await;
    let batch = admission_batch(&pool, &fixture, "barrier-lock-race").await;
    seed_ready_content(&pool, fixture.tenant_id, &batch.message.content).await;
    // Hold the migration's authoritative row lock. Admit starts its SELECT
    // snapshot before the barrier exists and waits for this transaction.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM conversation.conversations WHERE conversation_id = $1 FOR UPDATE")
        .bind(fixture.conversation_id.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    let store = admission_store(&pool);
    let waiter = tokio::spawn(async move { store.admit(batch).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'
                 AND query LIKE '%FROM conversation.conversations%' AND pid <> pg_backend_pid())",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("admission must be blocked on Conversation row");
    sqlx::query(
        "INSERT INTO conversation.workspace_assignment_preparations
        (command_id, tenant_id, conversation_id, owner_principal_id, target_workspace_id,
         expected_home_epoch, prepared_at_ms) VALUES ($1,$2,$3,$4,$5,1,1)",
    )
    .bind(request.command_id.into_uuid())
    .bind(request.actor.tenant_id.into_uuid())
    .bind(request.conversation_id.into_uuid())
    .bind(request.actor.principal_id.into_uuid())
    .bind(request.target_workspace_id.into_uuid())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        waiter.await.unwrap(),
        Err(PostgresAdmissionError::WorkspaceAssignmentPending)
    ));
}
