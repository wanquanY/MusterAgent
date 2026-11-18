use super::*;
use crate::PostgresAccountChangeFeed;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

async fn start_listener(
    feed: Arc<PostgresAccountChangeFeed>,
    pool: &PgPool,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let listener = feed.connect(pool).await.unwrap();
    tokio::spawn(async move { feed.run(listener, stop).await.unwrap() })
}

async fn fixture() -> (IsolatedPostgres, PgPool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    sqlx::raw_sql(
        "CREATE SCHEMA agent;
         CREATE TABLE agent.dialogue_intent_admissions (
          intent_id uuid PRIMARY KEY, execution_target text NOT NULL DEFAULT 'cloud',
          status text NOT NULL, available_at_ms bigint NOT NULL, lease_expires_at_ms bigint
         );",
    )
    .execute(&pool)
    .await
    .expect("queue fixture");
    // Exercise the current baseline's notification function and triggers against
    // this deliberately narrow queue fixture; the full baseline has its own test.
    let baseline = include_str!("../../../migrations/0001_kernel_baseline.sql");
    let function = &baseline[baseline
        .find("CREATE FUNCTION agent.notify_dialogue_handoff()")
        .expect("baseline handoff function")..];
    let function = &function[..function.find("END $$;").expect("function terminator") + 7];
    sqlx::raw_sql(function)
        .execute(&pool)
        .await
        .expect("baseline notification function");
    for trigger in ["dialogue_handoff_due_wake", "dialogue_handoff_insert_wake"] {
        let statement = &baseline[baseline
            .find(&format!("CREATE TRIGGER {trigger} "))
            .expect("baseline handoff trigger")..];
        let statement = &statement[..statement.find(';').expect("trigger terminator") + 1];
        sqlx::raw_sql(statement)
            .execute(&pool)
            .await
            .expect("baseline notification trigger");
    }
    (postgres, pool)
}

#[tokio::test]
async fn commit_wakes_every_slot_and_rollback_or_device_input_does_not() {
    let (_postgres, pool) = fixture().await;
    let wake = Arc::new(PostgresAccountChangeFeed::new());
    let mut first = wake.subscribe_handoff();
    let mut second = wake.subscribe_handoff();
    let stop = CancellationToken::new();
    let task = start_listener(wake.clone(), &pool, stop.clone()).await;
    tokio::time::timeout(Duration::from_secs(2), first.changed())
        .await
        .unwrap()
        .unwrap();
    second.borrow_and_update();
    let id = uuid::Uuid::now_v7();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO agent.dialogue_intent_admissions VALUES($1,'cloud','pending',0,NULL)")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), first.changed())
            .await
            .is_err()
    );
    sqlx::query(
        "INSERT INTO agent.dialogue_intent_admissions VALUES($1,'origin_device','pending',0,NULL)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), first.changed())
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO agent.dialogue_intent_admissions VALUES($1,'cloud','pending',0,NULL)")
        .bind(uuid::Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
    // Deliberately wait after commit: the scan-to-wait race must retain a hint.
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(Duration::from_secs(2), first.changed())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), second.changed())
        .await
        .unwrap()
        .unwrap();
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn due_time_covers_retry_lease_and_ignores_device_or_terminal_history() {
    let (_postgres, pool) = fixture().await;
    let store = PostgresDialogueIntentHandoffStore::new(pool.clone());
    let ceiling = Duration::from_secs(30);
    assert_eq!(store.next_claim_delay(ceiling).await.unwrap(), ceiling);
    sqlx::query(
        "INSERT INTO agent.dialogue_intent_admissions VALUES
         ($1,'origin_device','pending',0,NULL),($2,'cloud','handed_off',0,NULL),
         ($3,'cloud','pending',floor(extract(epoch FROM clock_timestamp())*1000)::bigint+5000,NULL),
         ($4,'cloud','claimed',0,floor(extract(epoch FROM clock_timestamp())*1000)::bigint+2000)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();
    let delay = store.next_claim_delay(ceiling).await.unwrap();
    assert!(delay > Duration::ZERO && delay <= Duration::from_secs(2));
    sqlx::query(
        "UPDATE agent.dialogue_intent_admissions SET lease_expires_at_ms=0 WHERE status='claimed'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store.next_claim_delay(ceiling).await.unwrap(),
        Duration::from_millis(10)
    );
}

#[tokio::test]
async fn listener_restart_rescans_and_retry_changes_are_not_lost() {
    let (_postgres, pool) = fixture().await;
    let wake = Arc::new(PostgresAccountChangeFeed::new());
    let mut receiver = wake.subscribe_handoff();
    for _ in 0..2 {
        let stop = CancellationToken::new();
        let task = start_listener(wake.clone(), &pool, stop.clone()).await;
        tokio::time::timeout(Duration::from_secs(2), receiver.changed())
            .await
            .unwrap()
            .unwrap();
        let id = uuid::Uuid::now_v7();
        sqlx::query(
            "INSERT INTO agent.dialogue_intent_admissions VALUES($1,'cloud','claimed',0,1000)",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.changed())
                .await
                .is_err()
        );
        sqlx::query("UPDATE agent.dialogue_intent_admissions SET status='pending',available_at_ms=10000 WHERE intent_id=$1")
            .bind(id).execute(&pool).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), receiver.changed())
            .await
            .unwrap()
            .unwrap();
        stop.cancel();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn dropped_listener_connection_rescans_without_a_new_input() {
    let (_postgres, pool) = fixture().await;
    let wake = Arc::new(PostgresAccountChangeFeed::new());
    let mut receiver = wake.subscribe_handoff();
    let stop = CancellationToken::new();
    let task = start_listener(wake.clone(), &pool, stop.clone()).await;
    tokio::time::timeout(Duration::from_secs(5), receiver.changed())
        .await
        .unwrap()
        .unwrap();
    // Only this test owns this isolated PostgreSQL instance. Terminate the
    // LISTEN connection, not the service or any durable queue record.
    let dropped: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname=current_database() AND pid<>pg_backend_pid()
         AND query ILIKE '%LISTEN%musteragent_dialogue_handoff%'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(dropped, vec![true]);
    tokio::time::timeout(Duration::from_secs(5), receiver.changed())
        .await
        .expect("reconnect must rescan even without a new notification")
        .unwrap();
    stop.cancel();
    task.await.unwrap();
}
