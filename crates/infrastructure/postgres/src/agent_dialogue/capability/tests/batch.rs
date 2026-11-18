//! Real canonical acceptance and original proof/index owners. Faults below
//! mutate only isolated retained storage inside rolled-back test transactions.
use super::*;
use crate::PostgresDialogueActivityProjection;

struct Fixture {
    _pg: IsolatedPostgres,
    pool: sqlx::PgPool,
    owner: Actor,
    active: OwnedAgentActivationReceipt,
}
impl Fixture {
    async fn new() -> Self {
        let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
        let pool = pg.pool().await.unwrap();
        migrate(&pool).await.unwrap();
        let owner = actor();
        let active = PostgresOwnedAgentStore::new(pool.clone())
            .activate(prepared(&pool, owner, true).await)
            .await
            .unwrap();
        Self {
            _pg: pg,
            pool,
            owner,
            active,
        }
    }
    async fn accept(&self) -> UserIntent {
        let request = input(self.owner, self.active.agent.dialogue_id);
        register_content(&self.pool, self.owner, &request.content_ref).await;
        PostgresAgentDialogueStore::new(self.pool.clone())
            .accept(request)
            .await
            .unwrap()
            .intent
    }
}

#[tokio::test]
async fn batched_root_proof_matches_scalar_and_preserves_capability_scope() {
    Box::pin(batched_root_proof_matches_scalar_and_preserves_capability_scope_journey()).await;
}

async fn batched_root_proof_matches_scalar_and_preserves_capability_scope_journey() {
    let f = Box::pin(Fixture::new()).await;
    let plain = f.accept().await;
    let target = selected_task(
        &f.pool,
        f.owner,
        f.active.agent.space_id,
        f.active.agent.agent_id,
        &plain,
    )
    .await;
    let mut request = input(f.owner, f.active.agent.dialogue_id);
    request.task_target = Some(target);
    request.observed_cursor = plain.accepted_cursor;
    request.accepted_at = UtcTimestamp::from_unix_millis(350);
    register_content(&f.pool, f.owner, &request.content_ref).await;
    let root = PostgresAgentDialogueStore::new(f.pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    let mut current = root.clone();
    for _ in 0..2 {
        current = Box::pin(complete_original_context(
            f.pool.clone(),
            f.owner,
            f.active.agent.agent_id,
            f.active.agent.published_version_id,
            current,
            "capability.request@2",
            2,
        ))
        .await;
    }
    let ids = [current.intent_id, plain.intent_id, root.intent_id];
    let mut tx = f.pool.begin().await.unwrap();
    let page = verified_roots_page(&mut tx, f.owner, f.active.agent.agent_id, &ids)
        .await
        .unwrap();
    assert_eq!(page.len(), ids.len());
    for (id, batch) in ids.into_iter().zip(page) {
        let scalar = verified_root_id(&mut tx, f.owner, f.active.agent.agent_id, id)
            .await
            .unwrap();
        assert_eq!(batch.current_intent, scalar.current_intent);
        assert_eq!(batch.root_intent, scalar.root_intent);
        assert_eq!(batch.is_direct_user_root, scalar.is_direct_user_root);
        assert!(batch.is_direct_user_root);
        if id == current.intent_id {
            assert_eq!(
                batch.current_intent.origin.surface,
                DialogueInputSurface::Capability
            );
            assert_eq!(batch.root_intent, root);
            assert_eq!(batch.current_intent.task_target, Some(target));
        }
    }
    // A valid direct item cannot make a corrupt continuation pass as direct.
    sqlx::query("UPDATE agent.dialogue_user_intents SET task_target=NULL WHERE intent_id=$1")
        .bind(current.intent_id.into_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        verified_roots_page(&mut tx, f.owner, f.active.agent.agent_id, &ids).await,
        Err(DialogueStoreError::Corrupt)
    ));
    tx.rollback().await.unwrap();
    let terminal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE tenant_id=$1 AND status=5")
            .bind(f.owner.tenant_id.into_uuid())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(
        terminal, 2,
        "failed proof cannot undo either completed capability"
    );
}

#[tokio::test]
async fn batched_root_proof_rejects_incomplete_foreign_duplicate_and_oversized_sets() {
    Box::pin(batched_root_proof_rejects_incomplete_foreign_duplicate_and_oversized_sets_journey())
        .await;
}

async fn batched_root_proof_rejects_incomplete_foreign_duplicate_and_oversized_sets_journey() {
    let f = Box::pin(Fixture::new()).await;
    let good = f.accept().await;
    let other = actor();
    let other_agent = PostgresOwnedAgentStore::new(f.pool.clone())
        .activate(prepared(&f.pool, other, true).await)
        .await
        .unwrap();
    let request = input(other, other_agent.agent.dialogue_id);
    register_content(&f.pool, other, &request.content_ref).await;
    let foreign = PostgresAgentDialogueStore::new(f.pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    let mut tx = f.pool.begin().await.unwrap();
    for ids in [
        vec![good.intent_id, UserIntentId::generate()],
        vec![good.intent_id, foreign.intent_id],
    ] {
        assert!(matches!(
            verified_roots_page(&mut tx, f.owner, f.active.agent.agent_id, &ids).await,
            Err(DialogueStoreError::Unavailable)
        ));
    }
    for ids in [
        vec![],
        vec![good.intent_id, good.intent_id],
        (0..=VERIFIED_ROOT_PAGE_SIZE)
            .map(|_| UserIntentId::generate())
            .collect(),
    ] {
        assert!(matches!(
            verified_roots_page(&mut tx, f.owner, f.active.agent.agent_id, &ids).await,
            Err(DialogueStoreError::Corrupt)
        ));
    }
    assert!(matches!(
        verified_roots_page(
            &mut tx,
            f.owner,
            other_agent.agent.agent_id,
            &[good.intent_id]
        )
        .await,
        Err(DialogueStoreError::Unavailable)
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn batched_root_index_rejects_one_damaged_receipt_source_destination_or_epoch() {
    Box::pin(batched_root_index_rejects_one_damaged_receipt_source_destination_or_epoch_journey())
        .await;
}

async fn batched_root_index_rejects_one_damaged_receipt_source_destination_or_epoch_journey() {
    let f = Box::pin(Fixture::new()).await;
    let good = f.accept().await;
    let bad = f.accept().await;
    for fault in [
        "receipt",
        "accepted_input",
        "destination",
        "missing_receipt",
        "epoch",
    ] {
        let mut tx = f.pool.begin().await.unwrap();
        crate::dialogue_handoff::progress::lock_account(&mut tx, f.owner)
            .await
            .unwrap();
        match fault {
            "receipt" => {
                sqlx::query(
                    "UPDATE agent.owner_command_receipts SET receipt=$2 WHERE command_id=$1",
                )
                .bind(bad.command_id.into_uuid())
                .bind(vec![255_u8])
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            "accepted_input" => {
                sqlx::query("UPDATE agent.dialogue_user_intents SET accepted_at_ms=accepted_at_ms+1 WHERE intent_id=$1")
                    .bind(bad.intent_id.into_uuid()).execute(&mut *tx).await.unwrap();
            }
            "destination" => {
                // Losing original custody is not permission to infer Cloud.
                sqlx::query("DELETE FROM agent.dialogue_intent_admissions WHERE intent_id=$1")
                    .bind(bad.intent_id.into_uuid())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            "missing_receipt" => {
                sqlx::query("DELETE FROM agent.owner_command_receipts WHERE command_id=$1")
                    .bind(bad.command_id.into_uuid())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            "epoch" => {
                sqlx::query("UPDATE agent.agent_dialogues SET epoch=epoch+1 WHERE dialogue_id=$1")
                    .bind(bad.dialogue_id.into_uuid())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let scalar = verified_root_id(&mut tx, f.owner, f.active.agent.agent_id, bad.intent_id)
            .await
            .err()
            .unwrap();
        let batch = verified_roots_page(
            &mut tx,
            f.owner,
            f.active.agent.agent_id,
            &[good.intent_id, bad.intent_id],
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            batch, scalar,
            "{fault}: preserve original error classification"
        );
        // The same original page owner may not commit the valid neighbour.
        if fault != "destination" {
            // The missing queue is deliberately outside candidate eligibility;
            // its explicit-id proof above is still required to fail closed.
            assert!(
                crate::dialogue_execution_control::source::retain_missing(
                    &mut tx,
                    f.owner,
                    f.active.agent.agent_id
                )
                .await
                .is_err()
            );
            let roots: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM agent.dialogue_execution_source_roots WHERE agent_id=$1",
            )
            .bind(f.active.agent.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .unwrap();
            assert_eq!(roots, 0, "an invalid page must write no neighbour's root");
        }
        tx.rollback().await.unwrap();
    }
    // Restored original source succeeds through the real projection; subsequent
    // scalar retains remain idempotent rather than overwriting immutable roots.
    PostgresDialogueActivityProjection::new(f.pool.clone())
        .refresh(f.active.agent.agent_id)
        .await
        .unwrap();
    let mut tx = f.pool.begin().await.unwrap();
    crate::dialogue_handoff::progress::lock_account(&mut tx, f.owner)
        .await
        .unwrap();
    for _ in 0..2 {
        crate::dialogue_execution_control::source::retain(
            &mut tx,
            f.owner,
            f.active.agent.agent_id,
            good.intent_id,
        )
        .await
        .unwrap();
    }
    let roots: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_execution_source_roots WHERE agent_id=$1",
    )
    .bind(f.active.agent.agent_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(roots, 2);
    tx.commit().await.unwrap();
}
