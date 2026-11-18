//! Kernel ledger parity, atomicity and retention contracts restored from source.
use super::*;

#[tokio::test]
async fn sqlx_repository_matches_the_in_memory_ledger_contract() {
    let (_postgres, pool) = runtime_pool().await;
    let postgres = PostgresLedgerStore::new(pool.clone());
    let memory = InMemoryLedger::new(ManualClock::new(UtcTimestamp::from_unix_millis(500)));
    assert_ledger_store_port(&postgres);
    assert_ledger_store_port(&memory);

    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let epoch = HomeEpoch::new(3).expect("epoch");
    let append = request(
        tenant_id,
        conversation_id,
        epoch,
        LedgerOffset::ZERO,
        "submit:postgres-contract",
        b"message.accepted",
    );

    let memory_receipt = memory.append(append.clone()).expect("memory append");
    let postgres_receipt = postgres.append(append.clone()).await.expect("SQL append");
    assert_eq!(memory_receipt.disposition, postgres_receipt.disposition);
    assert_eq!(
        memory_receipt.first_sequence,
        postgres_receipt.first_sequence
    );
    assert_eq!(memory_receipt.last_sequence, postgres_receipt.last_sequence);

    let duplicate = postgres
        .append(append.clone())
        .await
        .expect("idempotent replay");
    assert_eq!(duplicate.disposition, AppendDisposition::Duplicate);
    assert_eq!(duplicate.first_sequence, postgres_receipt.first_sequence);
    assert_eq!(duplicate.last_sequence, postgres_receipt.last_sequence);

    let cursor = LedgerCursor {
        conversation_id,
        home_epoch: epoch,
        after_conversation_seq: LedgerOffset::ZERO,
    };
    let memory_page = memory.load(cursor, 100).expect("memory page");
    let postgres_page = postgres.load(cursor, 100).await.expect("SQL page");
    assert_eq!(postgres_page.next_cursor, memory_page.next_cursor);
    assert_eq!(postgres_page.has_more, memory_page.has_more);
    assert_eq!(postgres_page.records.len(), memory_page.records.len());
    for (actual, expected) in postgres_page.records.iter().zip(&memory_page.records) {
        assert_eq!(actual.payload, expected.payload);
        assert_eq!(actual.metadata.record_id, expected.metadata.record_id);
        assert_eq!(
            actual.metadata.conversation_seq,
            expected.metadata.conversation_seq
        );
        assert_eq!(actual.metadata.payload_hash, expected.metadata.payload_hash);
        assert_eq!(actual.metadata.occurred_at, expected.metadata.occurred_at);
    }

    let memory_outbox = memory.pending_outbox().expect("memory outbox");
    let postgres_outbox = postgres.pending_outbox().await.expect("SQL outbox");
    assert_eq!(postgres_outbox.len(), memory_outbox.len());
    assert_eq!(
        postgres_outbox[0].message.message_id,
        memory_outbox[0].message.message_id
    );
    assert_eq!(
        postgres
            .current_offset(conversation_id)
            .await
            .expect("offset"),
        LedgerOffset::new(1)
    );

    let mut conflicting = append;
    conflicting.command_fingerprint = Sha256::digest(b"different");
    assert!(matches!(
        postgres.append(conflicting).await,
        Err(PostgresLedgerError::Contract(
            LedgerError::IdempotencyConflict
        ))
    ));

    pool.close().await;
}

#[tokio::test]
async fn sqlx_repository_serializes_concurrent_writers_and_commits_outbox_atomically() {
    let (_postgres, pool) = runtime_pool().await;
    let store = Arc::new(PostgresLedgerStore::new(pool.clone()));
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let epoch = HomeEpoch::new(1).expect("epoch");
    let first = request(
        tenant_id,
        conversation_id,
        epoch,
        LedgerOffset::ZERO,
        "writer:postgres:1",
        b"one",
    );
    let second = request(
        tenant_id,
        conversation_id,
        epoch,
        LedgerOffset::ZERO,
        "writer:postgres:2",
        b"two",
    );
    let start_barrier = Arc::new(tokio::sync::Barrier::new(3));
    let left = {
        let store = Arc::clone(&store);
        let start_barrier = Arc::clone(&start_barrier);
        tokio::spawn(async move {
            start_barrier.wait().await;
            store.append(first).await
        })
    };
    let right = {
        let store = Arc::clone(&store);
        let start_barrier = Arc::clone(&start_barrier);
        tokio::spawn(async move {
            start_barrier.wait().await;
            store.append(second).await
        })
    };
    start_barrier.wait().await;
    let outcomes = [
        left.await.expect("left writer"),
        right.await.expect("right writer"),
    ];
    assert_eq!(
        outcomes.iter().filter(|result| result.is_ok()).count(),
        1,
        "unexpected concurrent append outcomes: {outcomes:#?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Err(PostgresLedgerError::Contract(
                    LedgerError::SequenceConflict { .. }
                ))
            ))
            .count(),
        1,
        "unexpected concurrent append outcomes: {outcomes:#?}"
    );
    let record_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_records WHERE conversation_id = $1",
    )
    .bind(conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("record count");
    let outbox_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_store.outbox WHERE conversation_id = $1")
            .bind(conversation_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("outbox count");
    assert_eq!((record_count, outbox_count), (1, 1));

    let wrong_tenant = request(
        TenantId::generate(),
        conversation_id,
        epoch,
        LedgerOffset::new(1),
        "writer:postgres:wrong-tenant",
        b"wrong-tenant",
    );
    assert!(matches!(
        store.append(wrong_tenant).await,
        Err(PostgresLedgerError::ConversationTenantMismatch)
    ));

    let stale = request(
        tenant_id,
        conversation_id,
        HomeEpoch::new(2).expect("stale"),
        LedgerOffset::new(1),
        "writer:postgres:stale",
        b"stale",
    );
    assert!(matches!(
        store.append(stale).await,
        Err(PostgresLedgerError::Contract(
            LedgerError::HomeEpochMismatch
        ))
    ));

    pool.close().await;
}

#[tokio::test]
async fn retention_floor_rejects_compacted_cursors_and_impossible_ranges() {
    let (_postgres, pool) = runtime_pool().await;
    let store = PostgresLedgerStore::new(pool.clone());
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let epoch = HomeEpoch::new(1).expect("epoch");
    let first = request(
        tenant_id,
        conversation_id,
        epoch,
        LedgerOffset::ZERO,
        "retention:1",
        b"one",
    );
    store.append(first).await.expect("first append");
    let second = request(
        tenant_id,
        conversation_id,
        epoch,
        LedgerOffset::new(1),
        "retention:2",
        b"two",
    );
    store.append(second).await.expect("second append");
    sqlx::query(
        "UPDATE event_store.ledger_streams
         SET retained_from_sequence = 2
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("advance retention floor");

    let cursor = LedgerCursor {
        conversation_id,
        home_epoch: epoch,
        after_conversation_seq: LedgerOffset::ZERO,
    };
    assert!(matches!(
        store.load(cursor, 100).await,
        Err(PostgresLedgerError::CursorCompacted {
            retained_from_sequence: 2
        })
    ));
    let impossible = sqlx::query(
        "UPDATE event_store.ledger_streams
         SET retained_from_sequence = 4
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .execute(&pool)
    .await;
    assert!(
        impossible.is_err(),
        "retention floor cannot advance beyond the stream head plus one"
    );
    pool.close().await;
}
