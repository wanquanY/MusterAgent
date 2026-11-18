use musteragent_testkit::IsolatedPostgres;
use serde_json::Value;
use sqlx::types::Json;

const FIXTURE_ROWS: i64 = 50_000;
const PENDING_ROWS: i64 = 2_000;
const CLAIM_LIMIT: i64 = 32;

#[tokio::test]
async fn large_outbox_claim_uses_the_bounded_partial_index_plan() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");

    sqlx::query(
        "INSERT INTO event_store.outbox(
             message_id, tenant_id, conversation_id, topic, payload, payload_hash,
             committed_at_ms, available_at_ms, published_at_ms
         )
         SELECT md5('message-' || value::text)::uuid,
                md5('tenant-' || (value % 32)::text)::uuid,
                md5('conversation-' || (value % 512)::text)::uuid,
                'projection.fixture', decode('01', 'hex'),
                decode(repeat('00', 32), 'hex'), value, value,
                CASE WHEN value <= $1 - $2 THEN value ELSE NULL END
         FROM generate_series(1, $1) AS value",
    )
    .bind(FIXTURE_ROWS)
    .bind(PENDING_ROWS)
    .execute(&pool)
    .await
    .expect("seed large deterministic Outbox fixture");
    sqlx::query("ALTER TABLE event_store.outbox ALTER COLUMN available_at_ms SET STATISTICS 500")
        .execute(&pool)
        .await
        .expect("set deterministic statistics target");
    sqlx::query("ANALYZE event_store.outbox")
        .execute(&pool)
        .await
        .expect("analyze large Outbox fixture");

    let Json(plan): Json<Value> = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
         SELECT message_id
         FROM event_store.outbox
         WHERE published_at_ms IS NULL
           AND quarantined_at_ms IS NULL
           AND available_at_ms <= $1
           AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= $1)
         ORDER BY available_at_ms, committed_at_ms, message_id
         FOR UPDATE SKIP LOCKED
         LIMIT $2",
    )
    .bind(FIXTURE_ROWS)
    .bind(CLAIM_LIMIT)
    .fetch_one(&pool)
    .await
    .expect("capture JSON query plan");
    let root = &plan[0]["Plan"];
    assert_eq!(root["Actual Rows"].as_i64(), Some(CLAIM_LIMIT), "{plan}");
    assert!(
        plan_contains_index(root, "outbox_pending_idx"),
        "Outbox claim did not use the approved partial index: {plan}"
    );
    assert!(
        !plan_contains_relation_scan(root, "Seq Scan", "outbox"),
        "Outbox claim regressed to a large-table sequential scan: {plan}"
    );
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL version");
    eprintln!(
        "query-plan evidence: postgres={version} fixture_rows={FIXTURE_ROWS} \
         pending_rows={PENDING_ROWS} claim_limit={CLAIM_LIMIT} \
         planning_ms={} execution_ms={} root_rows={} shared_hits={} shared_reads={}",
        plan[0]["Planning Time"],
        plan[0]["Execution Time"],
        root["Actual Rows"],
        root["Shared Hit Blocks"],
        root["Shared Read Blocks"],
    );

    pool.close().await;
    drop(postgres);
}

fn plan_contains_index(plan: &Value, expected: &str) -> bool {
    plan.get("Index Name").and_then(Value::as_str) == Some(expected)
        || plan
            .get("Plans")
            .and_then(Value::as_array)
            .is_some_and(|children| {
                children
                    .iter()
                    .any(|child| plan_contains_index(child, expected))
            })
}

fn plan_contains_relation_scan(plan: &Value, node_type: &str, relation: &str) -> bool {
    (plan.get("Node Type").and_then(Value::as_str) == Some(node_type)
        && plan.get("Relation Name").and_then(Value::as_str) == Some(relation))
        || plan
            .get("Plans")
            .and_then(Value::as_array)
            .is_some_and(|children| {
                children
                    .iter()
                    .any(|child| plan_contains_relation_scan(child, node_type, relation))
            })
}
