#![cfg(test)]

#[path = "dialogue_fixture/mod.rs"]
mod fixture;
use fixture::*;
use musteragent_goal::ScheduleSpec;
use musteragent_identity::*;
use musteragent_postgres::{
    PostgresContentStore, PostgresOwnedAgentStore, PostgresOwnerScheduleStore,
    PostgresTieredBlobStore, migrate,
};
use musteragent_runtime_session::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use std::sync::Arc;
fn now(ms: i64) -> UtcTimestamp {
    UtcTimestamp::from_unix_millis(ms)
}
#[tokio::test]
async fn schedules_are_owned_idempotent_fenced_and_accept_once_across_restarts() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap()
        .agent;
    let store = PostgresOwnerScheduleStore::new(pool.clone());
    let key = DottedKey::new("test.schedules").unwrap();
    let keys = Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])]).unwrap());
    let blobs = Arc::new(PostgresTieredBlobStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::default()),
    ));
    let content = PostgresContentStore::new(pool.clone(), blobs, keys);
    let create = ChangeOwnerSchedule {
        actor: owner,
        agent_id: agent.agent_id,
        command_id: CommandId::generate(),
        schedule_id: uuid::Uuid::now_v7(),
        expected_revision: 0,
        action: OwnerScheduleAction::Create,
        definition: Some(OwnerScheduleDefinition {
            title: "Daily review".into(),
            instructions: "Summarize today's progress".into(),
            timezone: "Asia/Shanghai".into(),
            timing: ScheduleSpec::Cron {
                expression: "* * * * *".into(),
            },
        }),
        now: now(1700000000000),
    };
    let first = store.change(create.clone()).await.unwrap();
    let publication: Vec<u8> = sqlx::query_scalar(
        "SELECT product_release FROM agent.owner_schedules WHERE schedule_id=$1",
    )
    .bind(first.schedule_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    use prost::Message;
    let reference =
        musteragent_skill::ProductSkillRelease::for_product(ProductId::KERNEL).reference();
    assert_eq!(
        publication,
        musteragent_persistence_codec::encode_product_release(&reference).encode_to_vec()
    );
    assert!(
        sqlx::query("UPDATE agent.owner_schedules SET product_release=NULL WHERE schedule_id=$1")
            .bind(first.schedule_id)
            .execute(&pool)
            .await
            .is_err()
    );
    let retry = store.change(create.clone()).await.unwrap();
    assert_eq!(first.schedule_id, retry.schedule_id);
    assert_eq!(first.revision, retry.revision);
    assert!(matches!(
        store.list(actor(), agent.agent_id).await,
        Err(OwnerScheduleError::NotFound)
    ));
    let mut other = create.clone();
    other.definition.as_mut().unwrap().title = "changed".into();
    assert!(matches!(
        store.change(other).await,
        Err(OwnerScheduleError::Conflict)
    ));
    let due = first.next_run_at.unwrap();
    let (a, b) = tokio::join!(
        store.dispatch_one(first.schedule_id, due, &content),
        store.dispatch_one(first.schedule_id, due, &content)
    );
    assert_eq!(usize::from(a.unwrap()) + usize::from(b.unwrap()), 1);
    let occurrence_publication: Vec<u8> = sqlx::query_scalar("SELECT product_release FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='accepted'")
        .bind(first.schedule_id).fetch_one(&pool).await.unwrap();
    assert_eq!(occurrence_publication, publication);
    assert!(
        sqlx::query(
            "UPDATE agent.owner_schedule_occurrences SET product_release=NULL WHERE schedule_id=$1"
        )
        .bind(first.schedule_id)
        .execute(&pool)
        .await
        .is_err()
    );
    let restarted = PostgresOwnerScheduleStore::new(pool.clone());
    assert!(
        !restarted
            .dispatch_one(first.schedule_id, due, &content)
            .await
            .unwrap()
    );
    let list = store.list(owner, agent.agent_id).await.unwrap();
    assert_eq!(list[0].recent_runs[0].status, "queued");
    assert!(list[0].recent_runs[0].entry_id.is_some());
    // A pending original admission must still prevent overlap after >10 skipped occurrences.
    for n in 1..=12 {
        assert!(
            store
                .dispatch_one(
                    first.schedule_id,
                    now(due.unix_millis() + n * 60000),
                    &content
                )
                .await
                .unwrap()
        );
    }
    let accepted:i64=sqlx::query_scalar("SELECT count(*) FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='accepted'").bind(first.schedule_id).fetch_one(&pool).await.unwrap();
    assert_eq!(accepted, 1);
    let list = store.list(owner, agent.agent_id).await.unwrap();
    assert_eq!(list[0].recent_runs.len(), 10);
    assert!(list[0].recent_runs.iter().all(|v| v.status == "skipped"));
    let mut change = create.clone();
    change.now = now(due.unix_millis() + 20 * 60000);
    change.command_id = CommandId::generate();
    change.expected_revision = 1;
    change.action = OwnerScheduleAction::Pause;
    change.definition = None;
    let paused = store.change(change.clone()).await.unwrap();
    assert_eq!(paused.status, "paused");
    assert!(paused.next_run_at.is_none());
    assert!(
        !store
            .dispatch_one(
                first.schedule_id,
                now(due.unix_millis() + 20 * 60000),
                &content
            )
            .await
            .unwrap()
    );
    change.command_id = CommandId::generate();
    assert!(matches!(
        store.change(change.clone()).await,
        Err(OwnerScheduleError::Conflict)
    ));
    change.expected_revision = paused.revision;
    change.action = OwnerScheduleAction::RunNow;
    let manual = store.change(change.clone()).await.unwrap();
    assert_eq!(manual.recent_runs[0].status, "pending");
    let manual_publication: Vec<u8> = sqlx::query_scalar("SELECT product_release FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='pending'")
        .bind(first.schedule_id).fetch_one(&pool).await.unwrap();
    assert_eq!(manual_publication, publication);
    change.command_id = CommandId::generate();
    change.expected_revision = manual.revision;
    change.action = OwnerScheduleAction::Delete;
    let deleted = store.change(change).await.unwrap();
    assert_eq!(deleted.recent_runs[0].status, "cancelled");
    assert!(store.list(owner, agent.agent_id).await.unwrap().is_empty());
    assert!(
        !store
            .dispatch_one(
                first.schedule_id,
                now(due.unix_millis() + 30 * 60000),
                &content
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn schedules_frozen_on_a_retired_publication_continue_on_the_current_one() {
    use musteragent_persistence_codec::wire;
    use prost::Message;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap()
        .agent;
    let key = DottedKey::new("test.schedules").unwrap();
    let keys = Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])]).unwrap());
    let blobs = Arc::new(PostgresTieredBlobStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::default()),
    ));
    let content = PostgresContentStore::new(pool.clone(), blobs, keys);
    let current = musteragent_persistence_codec::encode_product_release(
        &musteragent_skill::ProductSkillRelease::for_product(ProductId::KERNEL).reference(),
    )
    .encode_to_vec();
    let mut retired =
        musteragent_skill::ProductSkillRelease::for_product(ProductId::KERNEL).reference();
    retired.release_id = VersionedKey::new("owner-schedules@1").unwrap();
    retired.digest[0] ^= 1;
    let retired = musteragent_persistence_codec::encode_product_release(&retired).encode_to_vec();
    assert_ne!(retired, current);
    let definition = wire::OwnerScheduleDefinition {
        title: "Retired publication schedule".into(),
        instructions: "Review progress".into(),
        timezone: "UTC".into(),
        timing: Some(wire::owner_schedule_definition::Timing::CronExpression(
            "* * * * *".into(),
        )),
    }
    .encode_to_vec();
    let due = now(1_780_000_000_000);
    let schedule = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO agent.owner_schedules(schedule_id,tenant_id,owner_id,agent_id,revision,definition,status,next_run_at_ms,created_at_ms,updated_at_ms,product_release) VALUES($1,$2,$3,$4,1,$5,'active',$6,$6,$6,$7)")
        .bind(schedule).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid()).bind(agent.agent_id.into_uuid())
        .bind(&definition).bind(due.unix_millis()).bind(&retired).execute(&pool).await.unwrap();
    let store = PostgresOwnerScheduleStore::new(pool.clone());
    assert!(store.dispatch_one(schedule, due, &content).await.unwrap());
    let occurrence: Vec<u8> = sqlx::query_scalar("SELECT product_release FROM agent.owner_schedule_occurrences WHERE schedule_id=$1 AND status='accepted'")
        .bind(schedule).fetch_one(&pool).await.unwrap();
    assert_eq!(occurrence, current);
}
