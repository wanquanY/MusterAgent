#![cfg(test)]

#[path = "dialogue_fixture/mod.rs"]
mod fixture;
use fixture::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_definition::{
    AgentDefinitionStore, PublishedAgentVersionGuard, PublishedAgentVersionPointer,
};
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_postgres::{
    PostgresAgentDefinitionStore, PostgresAgentDialogueStore, PostgresOwnedAgentStore, migrate,
};
use musteragent_runtime_session::*;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
use std::num::NonZeroU64;
#[path = "dialogue_history_search/mod.rs"]
mod history_search;
#[path = "dialogue_publication/mod.rs"]
mod publication;
#[path = "dialogue_receipt/mod.rs"]
mod receipt;
#[path = "work_item_authority/mod.rs"]
mod work_item_authority;

#[tokio::test]
async fn concurrent_defaults_create_one_instance_and_retry_keeps_original_receipt() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let store = PostgresOwnedAgentStore::new(pool.clone());
    let a = prepared(&pool, owner, true).await;
    let b = prepared(&pool, owner, true).await;
    let (x, y) = tokio::join!(store.activate(a.clone()), store.activate(b.clone()));
    let x = x.unwrap();
    let y = y.unwrap();
    assert_eq!(x.agent.agent_id, y.agent.agent_id);
    assert_eq!(x.agent.dialogue_id, y.agent.dialogue_id);
    assert_eq!(count(&pool, "owned_agents").await, 1);
    assert_eq!(count(&pool, "agent_dialogues").await, 1);
    assert_eq!(count(&pool, "owned_agent_inboxes").await, 1);
    assert_eq!(count(&pool, "owner_outbox").await, 1);
    let retry = store.activate(a.clone()).await.unwrap();
    assert_eq!(retry.disposition, OwnedAgentActivationDisposition::Replayed);
    assert_eq!(retry.agent, x.agent);
    let mut conflict = a;
    conflict.configuration.profile.display_name = "Changed".into();
    assert_eq!(
        store.activate(conflict).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let home = store.home_snapshot(owner).await.unwrap().unwrap();
    assert_eq!(home.agents.len(), 1);
    assert_eq!(home.head_event_seq, 1);
    assert_eq!(home.sync_epoch, 1);
    assert!(store.home_snapshot(actor()).await.unwrap().is_none());
    assert!(
        store
            .configuration(actor(), x.agent.agent_id)
            .await
            .unwrap()
            .is_none()
    );
    let runs: i64 = sqlx::query_scalar(
        "SELECT count(*)
                 FROM runtime.runs",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(runs, 0, "activation must not create execution runs");
}
#[tokio::test]
async fn concurrent_inputs_are_durable_ordered_and_idempotent_without_runs() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agents = PostgresOwnedAgentStore::new(pool.clone());
    let ready = agents
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let dialogue = ready.agent.dialogue_id;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let a = input(owner, dialogue);
    let b = input(owner, dialogue);
    register_content(&pool, owner, &a.content_ref).await;
    register_content(&pool, owner, &b.content_ref).await;
    let (x, y) = tokio::join!(store.accept(a.clone()), store.accept(b));
    let x = x.unwrap();
    let y = y.unwrap();
    let mut seq = [x.intent.accepted_cursor.seq, y.intent.accepted_cursor.seq];
    seq.sort();
    assert_eq!(seq, [1, 2]);
    let retry = store.accept(a.clone()).await.unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.intent, x.intent);
    let mut changed = a;
    changed.content_ref = content("changed");
    assert_eq!(
        store.accept(changed).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 2);
    assert_eq!(count(&pool, "agent_dialogue_entries").await, 2);
    assert_eq!(count(&pool, "owner_outbox").await, 3);
    let first = store
        .read_page(owner, dialogue, DialogueCursor { epoch: 1, seq: 0 }, 1)
        .await
        .unwrap();
    assert_eq!(first.dialogue.head.seq, 2);
    assert!(first.has_more);
    let second = store
        .read_page(owner, dialogue, first.entries[0].entry.cursor, 1)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert!(!second.has_more);
    assert_ne!(
        first.entries[0].entry.entry_id,
        second.entries[0].entry.entry_id
    );
    let home = agents.home_snapshot(owner).await.unwrap().unwrap();
    assert_eq!(home.head_event_seq, 3);
    assert_eq!(
        store.accept(input(actor(), dialogue)).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert!(store.load(actor(), dialogue).await.unwrap().is_none());
    let mut future = input(owner, dialogue);
    future.observed_cursor.seq = 3;
    assert_eq!(
        store.accept(future).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 2);
    let mut reply = input(owner, dialogue);
    reply.reply_to_intent_id = Some(UserIntentId::generate());
    assert_eq!(
        store.accept(reply).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&pool, "agent_dialogue_entries").await, 2);
    let runs: i64 = sqlx::query_scalar(
        "SELECT count(*)
                 FROM runtime.runs",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(runs, 0);
}
#[tokio::test]
async fn failed_activation_rolls_back_space_and_foreign_publication_is_not_claimable() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let store = PostgresOwnedAgentStore::new(pool.clone());
    let mut command = prepared(&pool, owner, true).await;
    command.published_pointer_version = NonZeroU64::new(2).unwrap();
    assert_eq!(
        store.activate(command.clone()).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(count(&pool, "owner_spaces").await, 0);
    assert_eq!(count(&pool, "owner_command_receipts").await, 0);
    command.published_pointer_version = NonZeroU64::MIN;
    command.actor = Actor {
        principal_id: PrincipalId::generate(),
        ..owner
    };
    assert_eq!(
        store.activate(command.clone()).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(count(&pool, "owner_spaces").await, 0);
    command.actor = actor();
    assert_eq!(
        store.activate(command).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&pool, "owner_spaces").await, 0);
    assert_eq!(count(&pool, "owner_outbox").await, 0);
}

#[tokio::test]
async fn independent_configuration_owners_commit_without_lost_updates_and_same_owner_conflicts() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let store = PostgresOwnedAgentStore::new(pool.clone());
    let ready = store
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let command = |change| ConfigureOwnedAgent {
        actor: owner,
        command_id: CommandId::generate(),
        agent_id: ready.agent.agent_id,
        expected_revision: NonZeroU64::MIN,
        change,
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        changed_at: UtcTimestamp::from_unix_millis(300),
    };
    let profile = command(AgentConfigurationChange::Profile {
        display_name: "青禾".into(),
        description: "研究伙伴".into(),
        avatar_ref: None,
    });
    let instructions = command(AgentConfigurationChange::Instructions {
        content_ref: Some(content("instructions")),
    });
    register_content(&pool, owner, instructions.content().unwrap()).await;
    let (a, b) = tokio::join!(
        store.configure(profile.clone()),
        store.configure(instructions)
    );
    let a = a.unwrap();
    b.unwrap();
    let current = store
        .configuration(owner, ready.agent.agent_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.profile.display_name, "青禾");
    assert!(current.instructions_ref.is_some());
    assert_eq!(current.manifest_revision.get(), 3);
    assert_eq!(
        store.configure(profile.clone()).await.unwrap(),
        a,
        "retry returns accepted configuration even after another owner changes"
    );
    let mut stale = profile.clone();
    stale.command_id = CommandId::generate();
    assert_eq!(
        store.configure(stale).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    let mut one = profile.clone();
    one.command_id = CommandId::generate();
    one.expected_revision = current.profile.revision;
    let mut two = one.clone();
    two.command_id = CommandId::generate();
    two.change = AgentConfigurationChange::Profile {
        display_name: "Research".into(),
        description: "".into(),
        avatar_ref: None,
    };
    let (a, b) = tokio::join!(store.configure(one), store.configure(two));
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(
        matches!(a, Err(DialogueStoreError::RevisionConflict))
            || matches!(b, Err(DialogueStoreError::RevisionConflict))
    );
    let denied = command(AgentConfigurationChange::Permission {
        mode: musteragent_policy::PermissionMode::FullAccess,
    });
    assert_eq!(
        store.configure(denied.clone()).await,
        Err(DialogueStoreError::Unavailable)
    );
    let desktop = ConfigureOwnedAgent {
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Desktop,
            installation_id: Some(DeviceInstallationId::generate()),
        },
        ..denied
    };
    assert_eq!(
        store.configure(desktop).await.unwrap().permission_mode,
        musteragent_policy::PermissionMode::FullAccess
    );
    let home = store.home_snapshot(owner).await.unwrap().unwrap();
    assert_eq!(home.head_event_seq, 5);
    assert_eq!(count(&pool, "owner_outbox").await, 5);
    assert_eq!(count(&pool, "owner_command_receipts").await, 5);
}

#[tokio::test]
async fn unavailable_or_mismatched_content_rolls_back_acceptance_and_pins_only_current_configuration()
 {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agents = PostgresOwnedAgentStore::new(pool.clone());
    let ready = agents
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let mut command = input(owner, ready.agent.dialogue_id);
    assert_eq!(
        store.accept(command.clone()).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 0);
    assert_eq!(count(&pool, "agent_dialogue_entries").await, 0);
    register_content(&pool, owner, &command.content_ref).await;
    command.content_ref.sha256 = Sha256::digest("wrong digest");
    assert_eq!(
        store.accept(command).await,
        Err(DialogueStoreError::Unavailable)
    );
    let body = content("instruction one");
    register_content(&pool, owner, &body).await;
    let edit = ConfigureOwnedAgent {
        actor: owner,
        command_id: CommandId::generate(),
        agent_id: ready.agent.agent_id,
        expected_revision: NonZeroU64::MIN,
        change: AgentConfigurationChange::Instructions {
            content_ref: Some(body),
        },
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        changed_at: UtcTimestamp::from_unix_millis(400),
    };
    let config = agents.configure(edit.clone()).await.unwrap();
    assert_eq!(count(&pool, "dialogue_content_references").await, 1);
    let mut unavailable = edit.clone();
    unavailable.command_id = CommandId::generate();
    unavailable.expected_revision = config.instructions_revision;
    unavailable.change = AgentConfigurationChange::Instructions {
        content_ref: Some(content("not registered")),
    };
    assert_eq!(
        agents.configure(unavailable).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(
        agents
            .configuration(owner, ready.agent.agent_id)
            .await
            .unwrap()
            .unwrap(),
        config
    );
    assert_eq!(count(&pool, "dialogue_content_references").await, 1);
    let cleared = ConfigureOwnedAgent {
        command_id: CommandId::generate(),
        expected_revision: config.instructions_revision,
        change: AgentConfigurationChange::Instructions { content_ref: None },
        ..edit
    };
    agents.configure(cleared).await.unwrap();
    assert_eq!(count(&pool, "dialogue_content_references").await, 0);
    assert_eq!(
        agents
            .home_snapshot(owner)
            .await
            .unwrap()
            .unwrap()
            .head_event_seq,
        3,
        "failed transactions cannot leave cursor gaps"
    );
}

#[tokio::test]
async fn account_snapshot_to_tail_history_navigation_and_reconnect_do_not_lose_input() {
    use musteragent_postgres::{PostgresAccountChangeFeed, PostgresAccountStore};
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agents = PostgresOwnedAgentStore::new(pool.clone());
    let ready = agents
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let snapshot = agents.home_snapshot(owner).await.unwrap().unwrap();
    let cursor = AccountCursor {
        space_id: snapshot.space.space_id,
        epoch: snapshot.sync_epoch,
        seq: snapshot.head_event_seq,
    };
    let feed = Arc::new(PostgresAccountChangeFeed::new());
    let mut listener = feed.connect(&pool).await.unwrap();
    let listener_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut listener)
        .await
        .unwrap();
    let first = input(owner, ready.agent.dialogue_id);
    register_content(&pool, owner, &first.content_ref).await;
    let first_accepted = store.accept(first.clone()).await.unwrap();
    let source = Arc::new(PostgresAccountStore::new(pool.clone()));
    let app = AccountApplication::new(source.clone(), feed.clone());
    let mut tail = app.subscribe(owner, cursor).await.unwrap();
    let cancel = CancellationToken::new();
    let running_feed = feed.clone();
    let stopping = cancel.clone();
    let running = tokio::spawn(async move { running_feed.run(listener, stopping).await });
    let event = tokio::time::timeout(Duration::from_secs(3), tail.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.cursor.seq, 2);
    assert!(
        matches!(event.change,AccountChange::UserIntentAccepted(ref v) if v.command_id==first.command_id)
    );
    let a = input(owner, ready.agent.dialogue_id);
    let b = input(owner, ready.agent.dialogue_id);
    register_content(&pool, owner, &a.content_ref).await;
    register_content(&pool, owner, &b.content_ref).await;
    let (a, b) = tokio::join!(store.accept(a), store.accept(b));
    a.unwrap();
    b.unwrap();
    for expected in [3, 4] {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), tail.next())
                .await
                .unwrap()
                .unwrap()
                .cursor
                .seq,
            expected
        )
    }
    let page = store
        .read_history(
            owner,
            ready.agent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert!(page.has_before);
    assert!(!page.has_after);
    assert!(page.entries.iter().all(|v| v.client_command_id.is_some()));
    let around = store
        .read_history(
            owner,
            ready.agent.dialogue_id,
            DialogueHistoryQuery::Around {
                entry_id: first_accepted.intent.source_entry_id,
                radius: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(around.entries.len(), 2);
    assert_eq!(around.entries[0].client_command_id, Some(first.command_id));
    assert!(!around.has_before);
    assert!(around.has_after);
    let ahead = AccountCursor { seq: 99, ..cursor };
    assert!(matches!(
        source.read_changes(owner, ahead, 10).await,
        Err(AccountError::CursorMismatch { .. })
    ));
    let epoch = AccountCursor { epoch: 2, ..cursor };
    assert!(matches!(
        source.read_changes(owner, epoch, 10).await,
        Err(AccountError::CursorMismatch { .. })
    ));
    assert_eq!(
        source.read_changes(actor(), cursor, 10).await,
        Err(AccountError::Store(DialogueStoreError::Unavailable))
    );
    // Kill the actual listener connection. A commit during reconnection must be
    // recovered even when PostgreSQL could not deliver its notification.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(listener_pid)
        .execute(&pool)
        .await
        .unwrap();
    let reconnect_input = input(owner, ready.agent.dialogue_id);
    register_content(&pool, owner, &reconnect_input.content_ref).await;
    store.accept(reconnect_input).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), tail.next())
            .await
            .unwrap()
            .unwrap()
            .cursor
            .seq,
        5
    );
    cancel.cancel();
    running.await.unwrap().unwrap();
    assert_eq!(
        tail.next().await,
        Err(AccountError::Store(DialogueStoreError::Infrastructure))
    );
}
#[tokio::test]
async fn deleting_authorized_empty_space_wakes_subscription_and_revokes_reads() {
    use musteragent_postgres::{PostgresAccountChangeFeed, PostgresAccountStore};
    use std::{sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let space = OwnerSpaceId::generate();
    sqlx::query("INSERT INTO agent.owner_spaces(space_id,tenant_id,owner_id,product,revision,created_at_ms) VALUES($1,$2,$3,'musteragent',1,1)").bind(space.into_uuid()).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid()).execute(&pool).await.unwrap();
    let feed = Arc::new(PostgresAccountChangeFeed::new());
    let listener = feed.connect(&pool).await.unwrap();
    let source = Arc::new(PostgresAccountStore::new(pool.clone()));
    let app = AccountApplication::new(source, feed.clone());
    let mut tail = app
        .subscribe(
            owner,
            AccountCursor {
                space_id: space,
                epoch: 1,
                seq: 0,
            },
        )
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let stopping = cancel.clone();
    let running = tokio::spawn(async move { feed.run(listener, stopping).await });
    sqlx::query("DELETE FROM agent.owner_spaces WHERE space_id=$1")
        .bind(space.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), tail.next())
            .await
            .unwrap(),
        Err(AccountError::Store(DialogueStoreError::Unavailable))
    );
    cancel.cancel();
    running.await.unwrap().unwrap();
}
#[tokio::test]
async fn concurrent_default_preparation_preserves_first_complete_reservation() {
    use musteragent_postgres::PostgresOwnerDefaultReservationStore;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let store = PostgresOwnerDefaultReservationStore::new(pool.clone());
    let a = ReserveDialogueDefault {
        actor: owner,
        command_id: CommandId::generate(),
        template_ref: VersionedKey::new("dialogue.default@1").unwrap(),
        template_hash: Sha256::digest("template one"),
        requested_at: UtcTimestamp::from_unix_millis(100),
    };
    let b = ReserveDialogueDefault {
        command_id: CommandId::generate(),
        template_ref: VersionedKey::new("dialogue.default@2").unwrap(),
        template_hash: Sha256::digest("template two"),
        requested_at: UtcTimestamp::from_unix_millis(200),
        ..a.clone()
    };
    let (x, y) = tokio::join!(store.reserve_default(a.clone()), store.reserve_default(b));
    let x = x.unwrap();
    let y = y.unwrap();
    assert_eq!(x, y);
    assert_eq!(store.reserve_default(a.clone()).await.unwrap(), x);
    assert_eq!(count(&pool, "owner_default_reservations").await, 1);
    assert_eq!(count(&pool, "owned_agents").await, 0);
    assert_eq!(count(&pool, "owner_spaces").await, 0);
    let other = store
        .reserve_default(ReserveDialogueDefault {
            actor: actor(),
            ..a.clone()
        })
        .await
        .unwrap();
    assert_ne!(other.agent_id, x.agent_id);
    assert_ne!(other.dialogue_id, x.dialogue_id);
    sqlx::query("UPDATE agent.owner_default_reservations SET template_hash=NULL WHERE tenant_id=$1 AND owner_id=$2")
        .bind(owner.tenant_id.into_uuid())
        .bind(owner.principal_id.into_uuid())
        .execute(&pool).await.unwrap();
    assert_eq!(
        store.reserve_default(a.clone()).await,
        Err(DialogueStoreError::Corrupt)
    );
    let mut invalid = a;
    invalid.actor.principal_kind = PrincipalKind::Service;
    assert_eq!(
        store.reserve_default(invalid).await,
        Err(DialogueStoreError::Unavailable)
    );
}

#[tokio::test]
async fn avatar_configuration_uses_profile_cas_account_scope_replay_and_content_retention() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let store = PostgresOwnedAgentStore::new(pool.clone());
    let ready = store
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let mut image = content("portrait");
    image.media_type = MediaType::new("image/png").unwrap();
    register_content(&pool, owner, &image).await;
    let command = ConfigureOwnedAgent {
        actor: owner,
        command_id: CommandId::generate(),
        agent_id: ready.agent.agent_id,
        expected_revision: ready.configuration.profile.revision,
        change: AgentConfigurationChange::Avatar {
            content_ref: Some(image.clone()),
        },
        origin: DialogueInputOrigin {
            surface: DialogueInputSurface::Web,
            installation_id: None,
        },
        changed_at: UtcTimestamp::from_unix_millis(500),
    };
    let changed = store.configure(command.clone()).await.unwrap();
    assert_eq!(changed.profile.avatar_ref, Some(image));
    assert_eq!(
        changed.profile.display_name,
        ready.configuration.profile.display_name
    );
    assert_eq!(
        changed.instructions_revision,
        ready.configuration.instructions_revision
    );
    assert_eq!(store.configure(command.clone()).await.unwrap(), changed);
    let mut foreign = command.clone();
    foreign.command_id = CommandId::generate();
    foreign.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        store.configure(foreign).await,
        Err(DialogueStoreError::Unavailable)
    );
    let mut reset = command.clone();
    reset.command_id = CommandId::generate();
    reset.change = AgentConfigurationChange::Avatar { content_ref: None };
    assert_eq!(
        store.configure(reset.clone()).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    reset.expected_revision = changed.profile.revision;
    let cleared = store.configure(reset).await.unwrap();
    assert!(cleared.profile.avatar_ref.is_none());
    assert_eq!(count(&pool, "dialogue_content_references").await, 0);
    assert_eq!(
        store
            .home_snapshot(owner)
            .await
            .unwrap()
            .unwrap()
            .head_event_seq,
        3
    );
    assert_eq!(count(&pool, "owner_command_receipts").await, 3);
}

#[path = "dialogue_fixture/source_authority_tests.rs"]
mod source_authority_tests;
