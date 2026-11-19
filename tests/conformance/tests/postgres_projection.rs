use std::time::Duration;

use musteragent_content_ref::Sha256;
use musteragent_identity::{ConversationId, DottedKey, OutboxMessageId, TenantId, UtcTimestamp};
use musteragent_postgres::PostgresProjectionStore;
use musteragent_projection::{ProjectionDisposition, ProjectionError, ProjectionStore};
use musteragent_testkit::IsolatedPostgres;

#[tokio::test]
async fn outbox_claim_retry_reclaim_and_completion_are_fenced_and_idempotent() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let store = PostgresProjectionStore::new(pool.clone());
    let message_id = OutboxMessageId::generate();
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let payload = b"projection-work".to_vec();
    let hash = Sha256::digest(&payload);
    let test_topic = DottedKey::new("projection.test").expect("topic");
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id, tenant_id, conversation_id, topic, payload,
             payload_hash, committed_at_ms, available_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, 1, 1)",
    )
    .bind(message_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(test_topic.as_str())
    .bind(&payload)
    .bind(hash.into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("seed Outbox");

    let foreign_message_id = OutboxMessageId::generate();
    let foreign_topic = DottedKey::new("projection.foreign").expect("foreign topic");
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id, tenant_id, conversation_id, topic, payload,
             payload_hash, committed_at_ms, available_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, 1, 1)",
    )
    .bind(foreign_message_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(foreign_topic.as_str())
    .bind(&payload)
    .bind(hash.into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("seed foreign Outbox topic");

    let first = store
        .claim(
            "projector-a",
            std::slice::from_ref(&test_topic),
            &[],
            1,
            Duration::from_secs(30),
        )
        .await
        .expect("first claim");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].message_id, message_id);
    assert_eq!(first[0].lease.generation, 1);
    let foreign_lease_owner: Option<String> = sqlx::query_scalar(
        "SELECT lease_owner
         FROM event_store.outbox
         WHERE message_id = $1",
    )
    .bind(foreign_message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read foreign Outbox lease");
    assert!(
        foreign_lease_owner.is_none(),
        "a projector must never claim a topic outside its subscription"
    );
    let stale = first[0].clone();
    store
        .retry(
            &first[0],
            UtcTimestamp::from_unix_millis(1),
            ProjectionError::DependencyUnavailable,
        )
        .await
        .expect("schedule retry");

    let second = store
        .claim(
            "projector-b",
            std::slice::from_ref(&test_topic),
            &[],
            1,
            Duration::from_secs(30),
        )
        .await
        .expect("reclaim");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].lease.generation, 2);
    assert_eq!(
        store.complete(&second[0]).await.expect("complete"),
        ProjectionDisposition::Applied
    );
    assert_eq!(
        store
            .complete(&second[0])
            .await
            .expect("idempotent complete"),
        ProjectionDisposition::Duplicate
    );
    assert_eq!(
        store.complete(&stale).await.expect("stale replay"),
        ProjectionDisposition::Duplicate,
        "a stale delivery may observe success but cannot publish twice"
    );

    let published: bool = sqlx::query_scalar(
        "SELECT published_at_ms IS NOT NULL
         FROM event_store.outbox
         WHERE message_id = $1",
    )
    .bind(message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read Outbox");
    assert!(published);

    let quarantined_message_id = OutboxMessageId::generate();
    sqlx::query(
        "INSERT INTO event_store.outbox (
             message_id, tenant_id, conversation_id, topic, payload,
             payload_hash, committed_at_ms, available_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, 2, 2)",
    )
    .bind(quarantined_message_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(
        DottedKey::new("projection.unknown")
            .expect("topic")
            .as_str(),
    )
    .bind(&payload)
    .bind(hash.into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("seed permanent failure");
    let unknown_topic = DottedKey::new("projection.unknown").expect("topic");
    let permanent = store
        .claim(
            "projector-a",
            std::slice::from_ref(&unknown_topic),
            &[],
            1,
            Duration::from_secs(30),
        )
        .await
        .expect("claim permanent failure");
    assert_eq!(permanent.len(), 1);
    assert_eq!(
        store
            .quarantine(&permanent[0], ProjectionError::UnsupportedTopic)
            .await
            .expect("quarantine"),
        ProjectionDisposition::Applied
    );
    assert_eq!(
        store
            .quarantine(&permanent[0], ProjectionError::UnsupportedTopic)
            .await
            .expect("idempotent quarantine"),
        ProjectionDisposition::Duplicate
    );
    assert!(
        store
            .claim(
                "projector-b",
                std::slice::from_ref(&unknown_topic),
                &[],
                1,
                Duration::from_secs(30),
            )
            .await
            .expect("claim after quarantine")
            .is_empty(),
        "quarantined work must never be hot-looped"
    );
    let quarantine_error: String = sqlx::query_scalar(
        "SELECT last_error_code
         FROM event_store.outbox
         WHERE message_id = $1
           AND quarantined_at_ms IS NOT NULL",
    )
    .bind(quarantined_message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read quarantine");
    assert_eq!(quarantine_error, "projection.unsupported_topic");

    let recovered = store
        .claim(
            "projector-c",
            std::slice::from_ref(&unknown_topic),
            std::slice::from_ref(&unknown_topic),
            1,
            Duration::from_secs(30),
        )
        .await
        .expect("recover stale unsupported-topic quarantine");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, quarantined_message_id);
    let quarantine_cleared: bool = sqlx::query_scalar(
        "SELECT quarantined_at_ms IS NULL AND last_error_code IS NULL
         FROM event_store.outbox
         WHERE message_id = $1",
    )
    .bind(quarantined_message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read recovered quarantine");
    assert!(quarantine_cleared);
    pool.close().await;
}

#[path = "postgres_projection/input_message.rs"]
mod input_message;
