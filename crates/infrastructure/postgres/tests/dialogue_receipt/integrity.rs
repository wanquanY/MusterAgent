//! Fault injection is confined to a disposable PostgreSQL test database.
use super::*;
use musteragent_persistence_codec::wire;
use prost::Message;

#[tokio::test]
async fn receipt_requires_original_identity_payload_and_immutable_intent_agreement() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let dialogue = agent.agent.dialogue_id;
    let command = input(owner, dialogue);
    register_content(&pool, owner, &command.content_ref).await;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let accepted = store.accept(command.clone()).await.unwrap();
    let original: Vec<u8> =
        sqlx::query_scalar("SELECT receipt FROM agent.owner_command_receipts WHERE command_id=$1")
            .bind(command.command_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    let decoded = wire::OwnerCommandReceipt::decode(original.as_slice()).unwrap();
    for fault in [
        "tenant",
        "owner",
        "command",
        "source_entry",
        "replayed",
        "hash",
        "submillisecond_timestamp",
    ] {
        let mut corrupt = decoded.clone();
        let Some(wire::owner_command_receipt::Result::AcceptedIntent(receipt)) =
            &mut corrupt.result
        else {
            panic!("original acceptance must return its canonical receipt");
        };
        let intent = receipt.intent.as_mut().unwrap();
        match fault {
            "tenant" => intent.tenant_id.as_mut().unwrap().value = TenantId::generate().to_string(),
            "owner" => {
                intent.owner_id.as_mut().unwrap().value = PrincipalId::generate().to_string()
            }
            "command" => {
                intent.command_id.as_mut().unwrap().value = CommandId::generate().to_string()
            }
            "source_entry" => {
                intent.source_entry_id.as_mut().unwrap().value =
                    DialogueEntryId::generate().to_string()
            }
            "replayed" => receipt.replayed = true,
            "submillisecond_timestamp" => intent.accepted_at.as_mut().unwrap().nanos += 1,
            "hash" => {}
            _ => unreachable!(),
        }
        let hash = if fault == "hash" {
            Sha256::digest("inconsistent receipt payload")
        } else {
            command.fingerprint()
        };
        sqlx::query("UPDATE agent.owner_command_receipts SET receipt=$2,payload_sha256=$3 WHERE command_id=$1")
            .bind(command.command_id.into_uuid()).bind(corrupt.encode_to_vec())
            .bind(hash.into_bytes().as_slice()).execute(&pool).await.unwrap();
        assert_eq!(
            store
                .read_intent_receipt(owner, dialogue, command.command_id)
                .await,
            Err(DialogueStoreError::Corrupt),
            "{fault} inconsistency must fail closed"
        );
    }
    sqlx::query(
        "UPDATE agent.owner_command_receipts SET receipt=$2,payload_sha256=$3 WHERE command_id=$1",
    )
    .bind(command.command_id.into_uuid())
    .bind(original)
    .bind(command.fingerprint().into_bytes().as_slice())
    .execute(&pool)
    .await
    .unwrap();
    // Neither a valid receipt nor a valid row alone may overrule disagreement.
    sqlx::query(
        "UPDATE agent.dialogue_user_intents SET accepted_at_ms=accepted_at_ms+1 WHERE intent_id=$1",
    )
    .bind(accepted.intent.intent_id.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .read_intent_receipt(owner, dialogue, command.command_id)
            .await,
        Err(DialogueStoreError::Corrupt)
    );
    sqlx::query("UPDATE agent.dialogue_user_intents SET accepted_at_ms=$2 WHERE intent_id=$1")
        .bind(accepted.intent.intent_id.into_uuid())
        .bind(accepted.intent.accepted_at.unix_millis())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .read_intent_receipt(owner, dialogue, command.command_id)
            .await
            .unwrap(),
        DialogueIntentReceiptObservation::Accepted(accepted.intent)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
}
