//! Extends the real completed-capability fixture through original preparation
//! and failure settlement. Existing terminal facts must survive unchanged.
use super::*;
use crate::{
    PostgresAdmissionStore, PostgresContentStore, PostgresDialogueExecutionStore,
    PostgresDialogueIntentHandoffStore, PostgresTieredBlobStore,
};
use musteragent_persistence_codec::ProtobufSnapshotCodec;
use musteragent_storage_protection::{GrantKeyring, OpaqueRefKeyring};
use std::time::Duration;

pub(super) async fn settle_corrupt_successor(
    pool: &sqlx::PgPool,
    owner: Actor,
    agent: AgentId,
    intent: UserIntentId,
) {
    let key = DottedKey::new("capability.failure.test").unwrap();
    let blobs = PostgresContentStore::new(
        pool.clone(),
        Arc::new(PostgresTieredBlobStore::new(
            pool.clone(),
            Arc::new(fixture::MemoryBlobs::default()),
        )),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key.clone(), [64; 32])]).unwrap()),
    );
    let queue = PostgresDialogueIntentHandoffStore::new(pool.clone()).with_rejection_authority(
        Arc::new(
            GrantKeyring::new([(
                key,
                ed25519_dalek::SigningKey::from_bytes(&[65; 32]).verifying_key(),
            )])
            .unwrap(),
        ),
        Arc::new(blobs.clone()),
    );
    let executions = PostgresDialogueExecutionStore::new(
        PostgresAdmissionStore::new(pool.clone(), Arc::new(ProtobufSnapshotCodec))
            .with_content_store(blobs),
    );
    // The terminal fixture has authentic existing Run receipts. Settle each
    // older queue through its original complete owner rather than editing it.
    let mut selected = None;
    for _ in 0..8 {
        let claim = queue
            .claim(uuid::Uuid::now_v7(), Duration::from_secs(120))
            .await
            .unwrap()
            .unwrap();
        if claim.intent_id == intent {
            selected = Some(claim);
            break;
        }
        if let Some(receipt) = executions.lookup(owner, claim.intent_id).await.unwrap() {
            assert!(queue.complete(claim, receipt).await.unwrap());
        } else {
            // The selected-task fixture also owns an unrelated initial input.
            // Leave it pending through the real retry owner; no fake Run or
            // terminal is created merely to reach this successor's queue row.
            assert!(
                queue
                    .retry(
                        claim,
                        DialogueHandoffFailure::DependencyUnavailable,
                        Duration::from_secs(300)
                    )
                    .await
                    .unwrap()
            );
        }
    }
    let claim = selected.expect("pending successor reached");
    assert_eq!(claim.actor, owner);
    assert_eq!(
        executions.prepare(owner, intent).await,
        Err(DialogueStoreError::Corrupt)
    );
    let terminal_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tool.tool_invocations WHERE tenant_id=$1 AND status=4",
    )
    .bind(owner.tenant_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(terminal_before > 0);
    assert_eq!(
        queue
            .fail_local(claim, LocalAdmissionFailure::preparation_corrupt())
            .await
            .unwrap(),
        DialogueAdmissionTermination::Committed
    );
    assert_eq!(
        queue
            .fail_local(claim, LocalAdmissionFailure::preparation_corrupt())
            .await
            .unwrap(),
        DialogueAdmissionTermination::Replayed
    );
    let status:(String,String,uuid::Uuid)=sqlx::query_as("SELECT q.status,o.internal_failure_code,o.agent_id FROM agent.dialogue_intent_admissions q JOIN agent.dialogue_admission_outcomes o USING(intent_id) WHERE q.intent_id=$1")
        .bind(intent.into_uuid()).fetch_one(pool).await.unwrap();
    assert_eq!(
        status,
        (
            "failed".into(),
            "invalid_preparation".into(),
            agent.into_uuid()
        )
    );
    let entries:i64=sqlx::query_scalar("SELECT count(*) FROM agent.agent_dialogue_entries WHERE source_intent_id=$1 AND kind='agent_message'")
        .bind(intent.into_uuid()).fetch_one(pool).await.unwrap();
    assert_eq!(entries, 1);
    let terminal_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tool.tool_invocations WHERE tenant_id=$1 AND status=4",
    )
    .bind(owner.tenant_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_after, terminal_before,
        "original completed capability is retained"
    );
    let progress: (i16, bool) = sqlx::query_as(
        "SELECT stage,execution_finished FROM agent.dialogue_admission_progress WHERE intent_id=$1",
    )
    .bind(intent.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(progress, (4, false));
    assert!(executions.lookup(owner, intent).await.unwrap().is_none());
    assert!(
        queue
            .claim(uuid::Uuid::now_v7(), Duration::from_secs(120))
            .await
            .unwrap()
            .is_none()
    );
}
