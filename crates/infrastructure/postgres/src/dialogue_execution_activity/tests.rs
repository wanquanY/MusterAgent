use super::*;
use crate::{PostgresOwnedAgentStore, migrate};
use musteragent_testkit::IsolatedPostgres;
use std::time::Duration;

use crate::dialogue_fixture as fixture;

#[tokio::test]
async fn concurrent_source_invalidation_aborts_projection_without_losing_dirty_work() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let actor = fixture::actor();
    let agent = PostgresOwnedAgentStore::new(pool.clone())
        .activate(fixture::prepared(&pool, actor, true).await)
        .await
        .unwrap()
        .agent;
    let projection = PostgresDialogueActivityProjection::new(pool.clone());
    projection.refresh(agent.agent_id).await.unwrap();
    // A real accepted intent changes the exact control cut, exercising staged
    // control rows as well as the visible activity/outbox publication.
    let input = fixture::input(actor, agent.dialogue_id);
    fixture::register_content(&pool, actor, &input.content_ref).await;
    crate::PostgresAgentDialogueStore::new(pool.clone())
        .accept(input)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO agent.dialogue_activity_dirty(agent_id) VALUES($1) ON CONFLICT DO NOTHING",
    )
    .bind(agent.agent_id.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.owner_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    let controls_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_execution_control_snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    crate::dialogue_handoff::progress::lock_account(&mut blocker, actor)
        .await
        .unwrap();
    let pending = tokio::spawn({
        let projection = projection.clone();
        async move { projection.refresh(agent.agent_id).await }
    });
    // Wait for the late publication lock, after snapshot aggregation. This is
    // PostgreSQL synchronization, not a timing assumption about query duration.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT space_id FROM agent.owner_spaces%FOR UPDATE')",
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
    .unwrap();
    // Same row mutation performed by original source triggers. Aggregation
    // must not hold the dirty row and delay source execution while it waits.
    tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query("UPDATE agent.dialogue_activity_dirty SET work_changed=true WHERE agent_id=$1")
            .bind(agent.agent_id.into_uuid())
            .execute(&pool),
    )
    .await
    .unwrap()
    .unwrap();
    blocker.commit().await.unwrap();
    assert!(pending.await.unwrap().is_err());
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.owner_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "stale snapshot cannot publish an activity/control cut"
    );
    let controls_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_execution_control_snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        controls_before, controls_after,
        "staged control cuts roll back"
    );
    let dirty: bool = sqlx::query_scalar(
        "SELECT work_changed FROM agent.dialogue_activity_dirty WHERE agent_id=$1",
    )
    .bind(agent.agent_id.into_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(dirty);
    projection.refresh(agent.agent_id).await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_activity_dirty WHERE agent_id=$1")
            .bind(agent.agent_id.into_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0);
    let work: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.owner_outbox WHERE aggregate_id=$1 AND event_kind='work_changed'")
        .bind(agent.agent_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        work, 1,
        "retry publishes the concurrent work invalidation exactly once"
    );
}
