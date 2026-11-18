#![cfg(test)]

//! Exact public-entry custody query regression. The small SQL fixture exercises
//! both execution subjects and negative authorization joins; full Resource
//! mutation/byte-read conformance lives in dialogue_device/canonical_resources.
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
use uuid::Uuid;

const QUERY: &str = include_str!("../src/artifact/device_binding.sql");
const FIXTURE: &str = include_str!("device_binding_fixture.sql");
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
async fn visible(
    pool: &PgPool,
    tenant: Uuid,
    owner: Uuid,
    dialogue: Uuid,
    entry: Uuid,
    archived: bool,
) -> bool {
    sqlx::query(QUERY)
        .bind(tenant)
        .bind(owner)
        .bind(dialogue)
        .bind(entry)
        .bind(archived)
        .fetch_optional(pool)
        .await
        .unwrap()
        .is_some()
}
async fn own(pool: &PgPool) -> bool {
    visible(pool, id(1), id(2), id(3), id(4), false).await
}

#[tokio::test]
async fn native_work_item_delivery_reads_without_fabricating_an_intent_or_cloud_run() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    sqlx::raw_sql(FIXTURE).execute(&pool).await.unwrap();
    assert!(own(&pool).await);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM agent.device_admissions")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    for (tenant, owner, dialogue, entry) in [
        (id(99), id(2), id(3), id(4)),
        (id(1), id(99), id(3), id(4)),
        (id(1), id(2), id(99), id(4)),
        (id(1), id(2), id(3), id(99)),
    ] {
        assert!(!visible(&pool, tenant, owner, dialogue, entry, false).await);
    }
    for (change, restore) in [
        (
            "UPDATE collaboration.work_items SET erasure_job_id='00000000-0000-0000-0000-000000000063'",
            "UPDATE collaboration.work_items SET erasure_job_id=NULL",
        ),
        (
            "UPDATE collaboration.work_item_device_admitted_custody SET signed_grant_hash=decode('03','hex')",
            "UPDATE collaboration.work_item_device_admitted_custody SET signed_grant_hash=decode('02','hex')",
        ),
        (
            "UPDATE collaboration.work_item_device_admitted_custody SET admitted_snapshot_hash=decode('03','hex')",
            "UPDATE collaboration.work_item_device_admitted_custody SET admitted_snapshot_hash=decode('01','hex')",
        ),
        (
            "UPDATE agent.dialogue_tool_publication_groups SET device_subject_kind='intent'",
            "UPDATE agent.dialogue_tool_publication_groups SET device_subject_kind='work_item'",
        ),
        (
            "UPDATE collaboration.work_items SET source_intent_id='00000000-0000-0000-0000-000000000063'",
            "UPDATE collaboration.work_items SET source_intent_id='00000000-0000-0000-0000-000000000006'",
        ),
        (
            "UPDATE collaboration.work_item_execution_reservations SET executing_agent_id='00000000-0000-0000-0000-000000000063'",
            "UPDATE collaboration.work_item_execution_reservations SET executing_agent_id='00000000-0000-0000-0000-000000000005'",
        ),
        (
            "UPDATE agent.device_resource_publications SET role='evidence'",
            "UPDATE agent.device_resource_publications SET role='delivery'",
        ),
        (
            "UPDATE agent.owned_agents SET status='deleted'",
            "UPDATE agent.owned_agents SET status='active'",
        ),
    ] {
        sqlx::query(change).execute(&pool).await.unwrap();
        assert!(!own(&pool).await, "{change}");
        sqlx::query(restore).execute(&pool).await.unwrap();
        assert!(own(&pool).await, "{restore}");
    }
    sqlx::query("UPDATE agent.owned_agents SET status='archived'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!own(&pool).await);
    assert!(visible(&pool, id(1), id(2), id(3), id(4), true).await);
}

#[tokio::test]
async fn ordinary_native_intent_keeps_its_admitted_revision_and_cannot_use_work_item_custody() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    sqlx::raw_sql(FIXTURE).execute(&pool).await.unwrap();
    sqlx::raw_sql("UPDATE agent.dialogue_publications SET device_subject_kind='intent'; UPDATE agent.dialogue_tool_publication_groups SET device_subject_kind='intent'")
        .execute(&pool).await.unwrap();
    assert!(
        !own(&pool).await,
        "a Task receipt cannot substitute for an Intent admission"
    );
    sqlx::raw_sql("INSERT INTO agent.device_admissions SELECT p.tenant_id,p.owner_id,p.agent_id,p.dialogue_id,p.source_intent_id,p.source_conversation_id,p.source_run_id,decode('01','hex'),1 FROM agent.dialogue_publications p; INSERT INTO agent.device_grant_heads VALUES('00000000-0000-0000-0000-000000000006',2); INSERT INTO agent.device_grant_versions VALUES('00000000-0000-0000-0000-000000000006',1,decode('03','hex')),('00000000-0000-0000-0000-000000000006',2,decode('02','hex'))")
        .execute(&pool).await.unwrap();
    assert!(
        own(&pool).await,
        "use admitted revision, not the first grant"
    );
    sqlx::query("UPDATE agent.device_grant_heads SET admitted_revision=1")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!own(&pool).await);
}
