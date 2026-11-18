//! Cancellation at the driver BEGIN boundary must not poison the next borrower.
use musteragent_testkit::IsolatedPostgres;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[tokio::test]
async fn cancelled_begin_cannot_hide_later_content_writes_in_an_open_transaction() {
    let database = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let observer = database.pool().await.expect("observer");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*observer.connect_options()).clone())
        .await
        .expect("single connection");
    sqlx::query("CREATE TABLE begin_cancel_probe (id int PRIMARY KEY)")
        .execute(&observer)
        .await
        .expect("probe table");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("borrower identity");
    let beginning = tokio::spawn({
        let pool = pool.clone();
        async move {
            pool.begin_with("BEGIN; SELECT pg_sleep(2) /* cancelled_begin_probe */")
                .await
        }
    });
    // Observe the server inside BEGIN before cancelling, rather than depending
    // on a timer race or cancelling a future that was never polled.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let sleeping: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE pid=$1 AND wait_event='PgSleep' AND query LIKE '%cancelled_begin_probe%')",
            )
            .bind(pid)
            .fetch_one(&observer)
            .await
            .expect("observe BEGIN");
            if sleeping {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("BEGIN reached PostgreSQL");
    beginning.abort();
    assert!(beginning.await.expect_err("cancelled BEGIN").is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        sqlx::query("INSERT INTO begin_cancel_probe VALUES (1)")
            .execute(&pool)
            .await
            .expect("subsequent content write");
    })
    .await
    .expect("connection drains and returns");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM begin_cancel_probe")
        .fetch_one(&observer)
        .await
        .expect("read from another connection");
    assert_eq!(
        count, 1,
        "a successful autocommit write must be durable and visible to other borrowers"
    );
    pool.close().await;
    observer.close().await;
}
