use std::time::Duration;

#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{CorrelationId, LedgerOffset, LedgerRecordId, UtcTimestamp};
use musteragent_postgres::{PostgresLedgerChangeFeed, PostgresLedgerStore};
use musteragent_projection::ChangeFeed;
use musteragent_testkit::IsolatedPostgres;
use sqlx::{Postgres, Transaction};
use tokio_util::sync::CancellationToken;

use running_loop::{Fixture, seed_running_loop};

async fn stage_record(
    transaction: &mut Transaction<'_, Postgres>,
    fixture: Fixture,
    sequence: u64,
    payload: &[u8],
) {
    let record_id = LedgerRecordId::generate();
    let now = UtcTimestamp::from_unix_millis(i64::try_from(sequence).expect("sequence"));
    let hash = Sha256::digest(payload);
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.runtime.record.v1', $2, $3, $4, $5::numeric,
             NULL, NULL, NULL, NULL, NULL, 1, 2, $6, $6, NULL, $7, $8, $9
         )",
    )
    .bind(record_id.into_uuid())
    .bind(1_i16)
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(sequence.to_string())
    .bind(now.unix_millis())
    .bind(CorrelationId::generate().into_uuid())
    .bind(format!("publication:{sequence}"))
    .bind(hash.into_bytes().to_vec())
    .execute(&mut **transaction)
    .await
    .expect("stage Ledger record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(record_id.into_uuid())
    .bind(payload)
    .bind(hash.into_bytes().to_vec())
    .execute(&mut **transaction)
    .await
    .expect("stage Domain event");
    sqlx::query(
        "UPDATE event_store.ledger_streams
         SET last_sequence = $3::numeric, updated_at_ms = $4
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(sequence.to_string())
    .bind(now.unix_millis())
    .execute(&mut **transaction)
    .await
    .expect("advance Ledger stream");
}

#[tokio::test]
async fn loop_no_uncommitted_publish() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = seed_running_loop(&pool).await;
    let ledger = PostgresLedgerStore::new(pool.clone());
    let feed = PostgresLedgerChangeFeed::new(1);
    let cancellation = CancellationToken::new();
    let pg_listener = feed.connect(&pool).await.expect("establish LISTEN barrier");
    let listener = tokio::spawn({
        let feed = feed.clone();
        let cancellation = cancellation.clone();
        async move { feed.run(pg_listener, cancellation).await }
    });
    let watcher = feed
        .subscribe(fixture.conversation_id)
        .expect("subscribe change feed");
    let cursor = LedgerCursor {
        conversation_id: fixture.conversation_id,
        home_epoch: musteragent_identity::HomeEpoch::new(1).expect("home epoch"),
        after_conversation_seq: LedgerOffset::new(1),
    };

    let mut transaction = pool.begin().await.expect("begin append");
    stage_record(&mut transaction, fixture, 2, b"publication-two").await;
    assert!(
        ledger
            .load(cursor, 10)
            .await
            .expect("read before commit")
            .records
            .is_empty(),
        "an uncommitted record must not be Cursor-readable"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), watcher.wait())
            .await
            .is_err(),
        "PostgreSQL NOTIFY must not publish before its transaction commits"
    );
    transaction.commit().await.expect("commit first append");

    let mut transaction = pool.begin().await.expect("begin second append");
    stage_record(&mut transaction, fixture, 3, b"publication-three").await;
    transaction.commit().await.expect("commit second append");
    tokio::time::timeout(Duration::from_secs(2), watcher.wait())
        .await
        .expect("committed wake hint")
        .expect("change feed");
    let page = ledger.load(cursor, 10).await.expect("Cursor catch-up");
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.metadata.conversation_seq.get())
            .collect::<Vec<_>>(),
        [2, 3],
        "Cursor truth must recover every committed record even when capacity-one hints lag"
    );

    cancellation.cancel();
    listener
        .await
        .expect("listener task")
        .expect("listener shutdown");
}

#[tokio::test]
async fn ledger_byte_budget_admits_only_a_contiguous_payload_prefix() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = seed_running_loop(&pool).await;
    let ledger = PostgresLedgerStore::new(pool.clone());
    let cursor = LedgerCursor {
        conversation_id: fixture.conversation_id,
        home_epoch: musteragent_identity::HomeEpoch::new(1).expect("epoch"),
        after_conversation_seq: LedgerOffset::new(1),
    };
    let mut write = pool.begin().await.expect("append");
    for (sequence, length) in [(2, 1_024), (3, 1_024), (4, 1_024 * 1_024), (5, 1)] {
        stage_record(&mut write, fixture, sequence, &vec![b'x'; length]).await;
    }
    write.commit().await.expect("commit");
    let page = ledger
        .load_bounded(cursor, 128, 4_096)
        .await
        .expect("bounded page");
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.next_cursor.after_conversation_seq.get(), 3);
    assert!(page.has_more);
    assert!(matches!(
        ledger.load_bounded(page.next_cursor, 128, 4_096).await,
        Err(musteragent_postgres::PostgresLedgerError::Contract(
            musteragent_ledger::LedgerError::ReadBudgetExceeded
        ))
    ));
    let rest = ledger
        .load_bounded(page.next_cursor, 128, 2 * 1_024 * 1_024)
        .await
        .expect("larger reader");
    assert_eq!(rest.records.len(), 2);
    assert_eq!(rest.records[0].metadata.conversation_seq.get(), 4);
    assert!(!rest.has_more);
}
