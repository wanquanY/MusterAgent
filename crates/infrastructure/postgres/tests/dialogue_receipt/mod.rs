use super::*;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

pub(super) mod attachment_fixture;
mod attachment_index;
mod attachments;
mod integrity;

async fn named_pool(pool: &PgPool, name: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*pool.connect_options()).clone().application_name(name))
        .await
        .unwrap()
}

async fn waiting(pool: &PgPool, name: &str, advisory: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                 WHERE application_name=$1 AND wait_event_type='Lock'
                   AND (NOT $2 OR wait_event='advisory'))",
            )
            .bind(name)
            .bind(advisory)
            .fetch_one(pool)
            .await
            .unwrap();
            if blocked {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("original owner did not reach expected database lock");
}

#[tokio::test]
async fn nondefault_scene_admits_the_original_signed_desktop_command_once() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agents = PostgresOwnedAgentStore::new(pool.clone());
    let default = agents
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let mut partner = prepared(&pool, owner, false).await;
    partner.space_id = default.space.space_id;
    partner.agent.space_id = partner.space_id;
    let secondary = agents.activate(partner).await.unwrap().agent;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let mut command = input(owner, secondary.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    command.origin = DialogueInputOrigin {
        surface: DialogueInputSurface::Desktop,
        installation_id: Some(DeviceInstallationId::generate()),
    };
    let device = DialogueIntentExecutionTarget::DesktopTools(DeviceId::generate());
    let first = store
        .accept_on_target(command.clone(), device.clone())
        .await
        .unwrap();
    let replay = store.accept_on_target(command, device).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(first.intent, replay.intent);
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
}

#[tokio::test]
async fn receipt_is_exact_after_history_window_moves_and_all_scopes_are_checked() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agents = PostgresOwnedAgentStore::new(pool.clone());
    let activation = prepared(&pool, owner, true).await;
    let creation_id = activation.command_id;
    let first_agent = agents.activate(activation).await.unwrap();
    let dialogue = first_agent.agent.dialogue_id;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let command = input(owner, dialogue);
    register_content(&pool, owner, &command.content_ref).await;
    assert_eq!(
        store
            .read_intent_receipt(owner, dialogue, command.command_id)
            .await
            .unwrap(),
        DialogueIntentReceiptObservation::NotObservedAtRead
    );
    let receipt = store.accept(command.clone()).await.unwrap();
    for _ in 0..101 {
        let later = input(owner, dialogue);
        register_content(&pool, owner, &later.content_ref).await;
        store.accept(later).await.unwrap();
    }
    let recent = store
        .read_history(
            owner,
            dialogue,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert!(
        !recent
            .entries
            .iter()
            .any(|e| e.client_command_id == Some(command.command_id))
    );
    let before = count(&pool, "owner_outbox").await;
    for _ in 0..2 {
        assert_eq!(
            store
                .read_intent_receipt(owner, dialogue, command.command_id)
                .await
                .unwrap(),
            DialogueIntentReceiptObservation::Accepted(receipt.intent.clone())
        );
    }
    assert_eq!(count(&pool, "owner_outbox").await, before);
    assert_eq!(count(&pool, "dialogue_user_intents").await, 102);
    assert_eq!(
        store
            .read_intent_receipt(owner, dialogue, creation_id)
            .await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    for stranger in [
        actor(),
        Actor {
            principal_id: PrincipalId::generate(),
            ..owner
        },
        Actor {
            tenant_id: TenantId::generate(),
            ..owner
        },
        Actor {
            principal_kind: PrincipalKind::Service,
            ..owner
        },
    ] {
        assert_eq!(
            store
                .read_intent_receipt(stranger, dialogue, command.command_id)
                .await,
            Err(DialogueStoreError::Unavailable)
        );
    }
    let mut partner = prepared(&pool, owner, false).await;
    partner.space_id = first_agent.space.space_id;
    partner.agent.space_id = partner.space_id;
    let partner = agents.activate(partner).await.unwrap();
    assert_eq!(
        store
            .read_intent_receipt(owner, partner.agent.dialogue_id, command.command_id)
            .await,
        Err(DialogueStoreError::Unavailable)
    );
    // A legitimate retry still uses the original fingerprint/cursor and receipt.
    let replay = store.accept(command.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.intent, receipt.intent);
    let mut changed = command;
    changed.observed_cursor.seq = 1;
    assert_eq!(
        store.accept(changed).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
}

#[tokio::test]
async fn receipt_read_before_waiting_accept_is_not_a_negative_acceptance_receipt() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let command = input(owner, agent.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    let mut barrier = pool.begin().await.unwrap();
    let digest = Sha256::digest(format!(
        "dialogue-command:{}:{}:{}",
        owner.tenant_id, owner.principal_id, command.command_id
    ))
    .into_bytes();
    let lock = i64::from_be_bytes(digest[..8].try_into().unwrap());
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock)
        .execute(&mut *barrier)
        .await
        .unwrap();
    let reader_pool = named_pool(&pool, "receipt-first-reader").await;
    let writer_pool = named_pool(&pool, "receipt-second-accept").await;
    let reader = PostgresAgentDialogueStore::new(reader_pool.clone());
    let dialogue = command.dialogue_id;
    let id = command.command_id;
    let read = tokio::spawn(async move { reader.read_intent_receipt(owner, dialogue, id).await });
    waiting(&pool, "receipt-first-reader", true).await;
    let writer = PostgresAgentDialogueStore::new(writer_pool.clone());
    let accept = tokio::spawn(async move { writer.accept(command).await });
    waiting(&pool, "receipt-second-accept", true).await;
    barrier.commit().await.unwrap();
    assert_eq!(
        read.await.unwrap().unwrap(),
        DialogueIntentReceiptObservation::NotObservedAtRead
    );
    let accepted = accept.await.unwrap().unwrap();
    assert_eq!(
        PostgresAgentDialogueStore::new(pool.clone())
            .read_intent_receipt(owner, dialogue, id)
            .await
            .unwrap(),
        DialogueIntentReceiptObservation::Accepted(accepted.intent)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
    reader_pool.close().await;
    writer_pool.close().await;
}

#[tokio::test]
async fn receipt_read_waits_for_original_inflight_acceptance_commit() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let command = input(owner, agent.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    let mut barrier = pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM agent.owner_spaces WHERE space_id=$1 FOR UPDATE")
        .bind(agent.space.space_id.into_uuid())
        .execute(&mut *barrier)
        .await
        .unwrap();
    let writer_pool = named_pool(&pool, "receipt-first-accept").await;
    let reader_pool = named_pool(&pool, "receipt-second-reader").await;
    let writer = PostgresAgentDialogueStore::new(writer_pool.clone());
    let dialogue = command.dialogue_id;
    let id = command.command_id;
    let accept = tokio::spawn(async move { writer.accept(command).await });
    waiting(&pool, "receipt-first-accept", false).await;
    let reader = PostgresAgentDialogueStore::new(reader_pool.clone());
    let read = tokio::spawn(async move { reader.read_intent_receipt(owner, dialogue, id).await });
    waiting(&pool, "receipt-second-reader", true).await;
    barrier.commit().await.unwrap();
    let accepted = accept.await.unwrap().unwrap();
    assert_eq!(
        read.await.unwrap().unwrap(),
        DialogueIntentReceiptObservation::Accepted(accepted.intent)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
    reader_pool.close().await;
    writer_pool.close().await;
}
