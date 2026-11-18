//! Real migrated owner transaction: fail only after its original outcome, replay,
//! rollback, wire payload and immutable terminal projection.
use super::*;
use crate::dialogue_fixture as fixture;
use crate::{PostgresAgentDialogueStore, PostgresOwnedAgentStore, migrate};
use fixture::{actor, content, input, prepared, register_content};
use musteragent_runtime_session::*;
use musteragent_testkit::IsolatedPostgres;
use prost::Message;

#[tokio::test]
async fn pre_run_failure_requires_the_original_receipt_and_is_atomic_and_once_only() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let activation = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let request = input(owner, activation.agent.dialogue_id);
    register_content(&pool, owner, &request.content_ref).await;
    let intent = PostgresAgentDialogueStore::new(pool.clone())
        .accept(request)
        .await
        .unwrap()
        .intent;
    let store = PostgresDialogueIntentHandoffStore::new(pool.clone());
    let claim = store
        .claim(uuid::Uuid::now_v7(), std::time::Duration::from_secs(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.intent_id, intent.intent_id);
    let failure_content = content("original failure explanation");
    register_content(&pool, owner, &failure_content).await;
    let mut invalid = pool.begin().await.unwrap();
    assert!(sqlx::query("INSERT INTO agent.dialogue_admission_progress(intent_id,revision,stage,blocker,observed_at_ms) VALUES($1,1,4,0,1)")
        .bind(intent.intent_id.into_uuid()).execute(&mut *invalid).await.is_err());
    invalid.rollback().await.unwrap();
    for commit in [false, true] {
        let mut tx = pool.begin().await.unwrap();
        lock_account(&mut tx, owner).await.unwrap();
        super::super::termination::lock_input(&mut tx, claim)
            .await
            .unwrap();
        assert!(matches!(
            fail_before_admission(&mut tx, owner, intent.intent_id).await,
            Err(DialogueStoreError::Corrupt)
        ));
        let at = crate::ledger::database_now_ms(&mut tx).await.unwrap();
        let message = super::super::termination::insert_message(
            &mut tx,
            claim,
            activation.agent.agent_id,
            activation.agent.dialogue_id,
            &failure_content,
            at,
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent.dialogue_admission_outcomes(intent_id,tenant_id,owner_id,agent_id,dialogue_id,outcome,reason,claim_generation,entry_id,created_at_ms,internal_failure_code) VALUES($1,$2,$3,$4,$5,'failed','runtime_contract_failure',$6,$7,$8,'invalid_preparation')")
            .bind(intent.intent_id.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid())
            .bind(activation.agent.agent_id.into_uuid()).bind(activation.agent.dialogue_id.into_uuid())
            .bind(claim.generation as i64).bind(message.entry_id.into_uuid()).bind(at.unix_millis())
            .execute(&mut *tx).await.unwrap();
        // Merely having a failure message/outcome cannot finish a still-live queue.
        assert!(matches!(
            fail_before_admission(&mut tx, owner, intent.intent_id).await,
            Err(DialogueStoreError::Corrupt)
        ));
        assert!(
            super::super::termination::finish(&mut tx, claim, "failed", at)
                .await
                .unwrap()
        );
        fail_before_admission(&mut tx, owner, intent.intent_id)
            .await
            .unwrap();
        assert!(matches!(
            fail_before_admission(&mut tx, actor(), intent.intent_id).await,
            Err(DialogueStoreError::Corrupt)
        ));
        fail_before_admission(&mut tx, owner, intent.intent_id)
            .await
            .unwrap();
        refresh(
            &mut tx,
            owner,
            intent.intent_id,
            Some(Blocker::DependencyUnavailable),
            true,
        )
        .await
        .unwrap();
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM agent.owner_outbox WHERE aggregate_id=$1 AND event_kind='admission_progress_changed'")
            .bind(intent.intent_id.into_uuid()).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(count, 1);
        let bytes:Vec<u8>=sqlx::query_scalar("SELECT payload FROM agent.owner_outbox WHERE aggregate_id=$1 AND event_kind='admission_progress_changed'")
            .bind(intent.intent_id.into_uuid()).fetch_one(&mut *tx).await.unwrap();
        let wire::account_change::Event::AdmissionProgressChanged(progress) =
            wire::AccountChange::decode(bytes.as_slice())
                .unwrap()
                .event
                .unwrap()
        else {
            panic!("progress event")
        };
        let projected = codec::decode(progress).unwrap();
        assert_eq!(
            projected.stage,
            musteragent_execution_context::DialogueAdmissionStage::Failed
        );
        assert_eq!(projected.entry_id, intent.source_entry_id);
        assert_eq!(projected.revision, 1);
        assert!(!projected.execution_finished);
        assert!(projected.blocker.is_none());
        if commit {
            tx.commit().await.unwrap()
        } else {
            tx.rollback().await.unwrap();
            let progress_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM agent.dialogue_admission_progress WHERE intent_id=$1",
            )
            .bind(intent.intent_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
            let event_count:i64=sqlx::query_scalar("SELECT count(*) FROM agent.owner_outbox WHERE aggregate_id=$1 AND event_kind='admission_progress_changed'")
                .bind(intent.intent_id.into_uuid()).fetch_one(&pool).await.unwrap();
            assert_eq!((progress_count, event_count), (0, 0));
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM agent.dialogue_admission_outcomes WHERE intent_id=$1",
            )
            .bind(intent.intent_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 0);
        }
    }
    let mut tx = pool.begin().await.unwrap();
    assert!(sqlx::query("UPDATE agent.dialogue_admission_progress SET stage=3,revision=revision+1 WHERE intent_id=$1")
        .bind(intent.intent_id.into_uuid()).execute(&mut *tx).await.is_err());
    tx.rollback().await.unwrap();
    let state: (i16, bool) = sqlx::query_as(
        "SELECT stage,execution_finished FROM agent.dialogue_admission_progress WHERE intent_id=$1",
    )
    .bind(intent.intent_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, (4, false));
}
