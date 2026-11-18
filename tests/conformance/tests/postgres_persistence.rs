#![cfg(test)]

use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerCursor, LedgerRecordKind, Producer};
use musteragent_identity::{
    ConversationId, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey, LedgerOffset,
    LedgerRecordId, OutboxMessageId, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    AppendDisposition, AppendRequest, InMemoryLedger, LedgerError, LedgerStore, ProposedOutbox,
    ProposedRecord,
};
use musteragent_postgres::{
    MigrationVerificationError, PostgresLedgerError, PostgresLedgerStore, RUNTIME_SCHEMAS,
    RUNTIME_TABLES, migrate, migration_set, verify_migrations,
};
use musteragent_testkit::{IsolatedPostgres, ManualClock};
use sqlx::migrate::Migrate;
use sqlx::{PgPool, Row};

fn record(payload: &[u8]) -> ProposedRecord {
    ProposedRecord {
        schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
        record_id: LedgerRecordId::generate(),
        record_kind: LedgerRecordKind::DomainEvent,
        activity_id: None,
        run_id: None,
        run_seq: None,
        attempt_id: None,
        participant_id: None,
        producer: Producer::RuntimeApi,
        occurred_at: UtcTimestamp::from_unix_millis(100),
        causation_id: None,
        correlation_id: CorrelationId::generate(),
        payload: payload.to_vec(),
        payload_hash: Sha256::digest(payload),
    }
}

fn request(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    epoch: HomeEpoch,
    offset: LedgerOffset,
    idempotency: &str,
    payload: &[u8],
) -> AppendRequest {
    AppendRequest {
        tenant_id,
        conversation_id,
        expected_home_epoch: epoch,
        expected_offset: offset,
        idempotency_key: IdempotencyKey::new(idempotency).expect("idempotency"),
        command_fingerprint: Sha256::digest([idempotency.as_bytes(), payload].concat()),
        records: vec![record(payload)],
        outbox: vec![ProposedOutbox {
            message_id: OutboxMessageId::generate(),
            topic: DottedKey::new("run.dispatch").expect("topic"),
            payload: b"dispatch".to_vec(),
            payload_hash: Sha256::digest(b"dispatch"),
        }],
    }
}

async fn runtime_pool() -> (IsolatedPostgres, PgPool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("apply Runtime migrations");
    (postgres, pool)
}

fn assert_ledger_store_port<T: LedgerStore>(_store: &T) {}

#[tokio::test]
async fn migrations_are_fresh_repeatable_owned_and_constraint_complete() {
    let (postgres, pool) = runtime_pool().await;
    migrate(&pool)
        .await
        .expect("reapplying migrations is idempotent");

    assert_eq!(
        RUNTIME_TABLES
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len(),
        RUNTIME_TABLES.len(),
        "the authoritative Runtime table inventory must not contain duplicates"
    );

    let schemas = sqlx::query_scalar::<_, String>(
        "SELECT schema_name
         FROM information_schema.schemata
         WHERE schema_name = ANY($1)",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&pool)
    .await
    .expect("list Runtime schemas")
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        schemas,
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    );

    let tables = sqlx::query(
        "SELECT schemaname, tablename
         FROM pg_tables
         WHERE schemaname = ANY($1)
         ORDER BY schemaname, tablename",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&pool)
    .await
    .expect("read exact table inventory")
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("schemaname"),
            row.get::<String, _>("tablename"),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(
        tables,
        RUNTIME_TABLES
            .iter()
            .map(|(schema, table)| ((*schema).to_owned(), (*table).to_owned()))
            .collect::<Vec<_>>()
    );

    let owner_rows = sqlx::query(
        "SELECT schemaname, tableowner
         FROM pg_tables
         WHERE schemaname = ANY($1)",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&pool)
    .await
    .expect("read table owners");
    assert!(!owner_rows.is_empty());
    assert!(owner_rows.iter().all(|row| {
        !row.get::<String, _>("tableowner").is_empty()
            && RUNTIME_SCHEMAS.contains(&row.get::<String, _>("schemaname").as_str())
    }));

    let tables_without_primary_key: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_tables AS tables
         WHERE tables.schemaname = ANY($1)
           AND NOT EXISTS (
               SELECT 1
               FROM pg_constraint AS constraint_record
               JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid
               JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
               WHERE constraint_record.contype = 'p'
                 AND namespace.nspname = tables.schemaname
                 AND relation.relname = tables.tablename
           )",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_one(&pool)
    .await
    .expect("verify primary key coverage");
    assert_eq!(tables_without_primary_key, 0);

    let critical_indexes = [
        "context_branch_edit_origin_idx",
        "context_branches_conversation_idx",
        "ledger_records_context_event_scan_idx",
        "ledger_records_context_branch_idx",
        "ledger_records_run_idx",
        "message_retractions_conversation_idx",
        "outbox_pending_idx",
        "one_active_foreground_run_per_conversation",
        "runs_context_visibility_idx",
        "runs_dispatch_idx",
        "attempts_recovery_idx",
        "leases_expiry_idx",
        "model_completion_intents_erasure_queue_idx",
        "model_completion_intents_privacy_pending_idx",
        "privacy_erasure_completion_intent_lookup_idx",
        "runtime_snapshots_conversation_idx",
        "tool_invocations_reconcile_idx",
        "interactions_pending_idx",
    ];
    let installed_indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname
         FROM pg_indexes
         WHERE schemaname = ANY($1) AND indexname = ANY($2)",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .bind(
        critical_indexes
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&pool)
    .await
    .expect("verify critical indexes")
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        installed_indexes,
        critical_indexes
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    );

    let removed_redundant_indexes: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_indexes
         WHERE (schemaname, indexname) IN (
             ('event_store', 'ledger_records_replay_idx'),
             ('conversation', 'messages_conversation_idx')
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("verify redundant indexes stay removed");
    assert_eq!(removed_redundant_indexes, 0);
    let covering_unique_constraints = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(constraint_record.oid)
         FROM pg_constraint AS constraint_record
         JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid
         JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         WHERE constraint_record.contype = 'u'
           AND (namespace.nspname, relation.relname) IN (
               ('event_store', 'ledger_records'),
               ('conversation', 'messages')
           )",
    )
    .fetch_all(&pool)
    .await
    .expect("read UNIQUE index authorities");
    assert!(covering_unique_constraints.iter().any(|definition| {
        definition == "UNIQUE (tenant_id, conversation_id, home_epoch, conversation_seq)"
    }));
    assert!(covering_unique_constraints.iter().any(|definition| {
        definition == "UNIQUE (tenant_id, conversation_id, committed_sequence)"
    }));

    let non_bigint_hot_columns: i64 = sqlx::query_scalar(
        "WITH expected(table_schema, table_name, column_name) AS (VALUES
             ('event_store', 'ledger_streams', 'home_epoch'),
             ('event_store', 'ledger_streams', 'last_sequence'),
             ('event_store', 'ledger_streams', 'retained_from_sequence'),
             ('event_store', 'command_idempotency', 'home_epoch'),
             ('event_store', 'command_idempotency', 'first_sequence'),
             ('event_store', 'command_idempotency', 'last_sequence'),
             ('event_store', 'ledger_records', 'conversation_seq'),
             ('event_store', 'ledger_records', 'run_seq'),
             ('event_store', 'ledger_records', 'home_epoch'),
             ('event_store', 'outbox', 'lease_generation'),
             ('event_store', 'snapshots', 'home_epoch'),
             ('event_store', 'snapshots', 'source_sequence_from'),
             ('event_store', 'snapshots', 'source_sequence_to'),
             ('event_store', 'projection_checkpoints', 'home_epoch'),
             ('event_store', 'projection_checkpoints', 'after_sequence'),
             ('event_store', 'projection_checkpoints', 'owner_epoch'),
             ('conversation', 'conversations', 'home_epoch'),
             ('conversation', 'messages', 'content_bytes'),
             ('conversation', 'messages', 'committed_sequence'),
             ('runtime', 'runs', 'home_epoch'),
             ('runtime', 'runs', 'admitted_sequence'),
             ('runtime', 'runs', 'last_run_sequence'),
             ('runtime', 'runs', 'next_fencing_token'),
             ('runtime', 'runs', 'run_version'),
             ('runtime', 'attempts', 'fencing_token'),
             ('runtime', 'leases', 'fencing_token'),
             ('runtime', 'loop_checkpoints', 'total_tokens'),
             ('runtime', 'loop_checkpoints', 'next_assistant_frame_index'),
             ('runtime', 'loop_checkpoints', 'next_assistant_byte_offset'),
             ('runtime', 'loop_checkpoints', 'next_reasoning_frame_index'),
             ('runtime', 'loop_checkpoints', 'next_reasoning_byte_offset'),
             ('runtime', 'loop_checkpoints', 'checkpoint_version'),
             ('runtime', 'model_invocations', 'started_conversation_seq'),
             ('runtime', 'pending_tool_calls', 'result_content_bytes')
         )
         SELECT count(*)
         FROM expected
         LEFT JOIN information_schema.columns AS column_record
           USING(table_schema, table_name, column_name)
         WHERE column_record.data_type IS DISTINCT FROM 'bigint'",
    )
    .fetch_one(&pool)
    .await
    .expect("verify native BIGINT hot columns");
    assert_eq!(non_bigint_hot_columns, 0);

    let secret_value_columns: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = ANY($1)
           AND column_name IN (
               'api_key', 'access_token', 'refresh_token', 'secret_value',
               'credential_value', 'oauth_token'
           )",
    )
    .bind(
        RUNTIME_SCHEMAS
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_one(&pool)
    .await
    .expect("scan secret columns");
    assert_eq!(secret_value_columns, 0);

    pool.close().await;
    drop(postgres);
}

#[tokio::test]
async fn context_hot_path_indexes_provide_ordered_event_and_covering_visibility_reads() {
    let (postgres, pool) = runtime_pool().await;
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let run_id = musteragent_identity::RunId::generate();
    let mut transaction = pool.begin().await.expect("begin query-plan inspection");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await
        .expect("prefer declared hot-path indexes");

    let event_plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (FORMAT TEXT)
         SELECT record.record_id, record.run_id, record.activity_id,
                record.payload_hash, event.encoded_event,
                source_run.admitted_sequence
         FROM event_store.ledger_records AS record
         JOIN event_store.domain_events AS event
           ON event.record_id = record.record_id
         LEFT JOIN runtime.runs AS source_run
           ON source_run.tenant_id = record.tenant_id
          AND source_run.conversation_id = record.conversation_id
          AND source_run.run_id = record.run_id
         WHERE record.tenant_id = $1
           AND record.conversation_id = $2
           AND record.record_kind = 1
           AND (
               record.conversation_seq <= $3::bigint
               OR record.run_id = $4
               OR ($6 AND source_run.admitted_sequence < $3::bigint)
           )
           AND record.conversation_seq <= $5::bigint
           AND NOT EXISTS (
               SELECT 1
               FROM runtime.loop_controls AS unapplied_control
               WHERE unapplied_control.tenant_id = record.tenant_id
                 AND unapplied_control.conversation_id = record.conversation_id
                 AND unapplied_control.source_record_id = record.record_id
                 AND unapplied_control.status <> 2
           )
         ORDER BY record.conversation_seq",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(i64::MAX)
    .bind(run_id.into_uuid())
    .bind(i64::MAX)
    .bind(true)
    .fetch_all(&mut *transaction)
    .await
    .expect("explain ordered Context event scan")
    .join("\n");
    assert!(
        event_plan.contains("ledger_records_context_event_scan_idx"),
        "{event_plan}"
    );
    assert!(!event_plan.contains("Sort"), "{event_plan}");

    let visibility_plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (FORMAT TEXT)
         SELECT run.admitted_sequence
         FROM runtime.runs AS run
         WHERE run.tenant_id = $1
           AND run.conversation_id = $2
           AND run.run_id = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await
    .expect("explain Context Run visibility lookup")
    .join("\n");
    assert!(
        visibility_plan.contains("runs_context_visibility_idx"),
        "{visibility_plan}"
    );

    transaction
        .rollback()
        .await
        .expect("release query-plan inspection");
    pool.close().await;
    drop(postgres);
}

#[path = "postgres_persistence/ledger_content.rs"]
mod ledger_content;

#[tokio::test]
async fn kernel_baseline_reapply_preserves_existing_ledger_bytes() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    apply_migrations_through(&pool, 1).await;
    verify_migrations(&pool)
        .await
        .expect("complete baseline is ready for workers");
    let store = PostgresLedgerStore::new(pool.clone());
    let original = request(
        TenantId::generate(),
        ConversationId::generate(),
        HomeEpoch::new(1).expect("epoch"),
        LedgerOffset::ZERO,
        "baseline-fixture",
        b"kernel baseline fact",
    );
    let receipt = store
        .append(original.clone())
        .await
        .expect("append baseline fact");
    let cursor = LedgerCursor {
        conversation_id: original.conversation_id,
        home_epoch: original.expected_home_epoch,
        after_conversation_seq: LedgerOffset::ZERO,
    };
    let before = store
        .load(cursor, 100)
        .await
        .expect("baseline ledger bytes");
    migrate(&pool).await.expect("reapply complete baseline");
    let after = store
        .load(cursor, 100)
        .await
        .expect("reopened ledger bytes");
    assert_eq!(
        after.records, before.records,
        "baseline reapplication preserves immutable facts"
    );
    verify_migrations(&pool)
        .await
        .expect("verified kernel schema");
    let replay = store
        .append(original)
        .await
        .expect("same original identity");
    assert_eq!(receipt.first_sequence, replay.first_sequence);
    assert_eq!(replay.disposition, AppendDisposition::Duplicate);
    pool.close().await;
    drop(postgres);
}

async fn apply_migrations_through(pool: &PgPool, maximum_version: i64) {
    let mut connection = pool.acquire().await.expect("acquire migration connection");
    connection
        .ensure_migrations_table()
        .await
        .expect("create migration ledger");
    for migration in migration_set()
        .iter()
        .filter(|migration| migration.version <= maximum_version)
    {
        connection
            .apply(migration)
            .await
            .expect("apply migration prefix");
    }
}

#[path = "postgres_persistence/forward_repository.rs"]
mod forward_repository;

#[tokio::test]
async fn runtime_migration_verification_is_read_only_on_pending_kernel_baseline() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    apply_migrations_through(&pool, 0).await;
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("migration ledger before verification");
    let error = verify_migrations(&pool)
        .await
        .expect_err("pending baseline");
    assert!(matches!(error, MigrationVerificationError::Pending(1)));
    let after: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("migration ledger after verification");
    assert_eq!(after, before);
    let extension_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('runtime.extension_facts')::text")
            .fetch_one(&pool)
            .await
            .expect("read schema state");
    assert_eq!(
        extension_table, None,
        "verification must not apply migrations"
    );
}

#[test]
fn runtime_service_compositions_are_verify_only_migration_consumers() {
    let compositions = [
        (
            "cloud-worker",
            include_str!("../../../applications/cloud-worker/src/composition.rs"),
        ),
        (
            "cloud-projector",
            include_str!("../../../applications/cloud-projector/src/main.rs"),
        ),
    ];
    for (owner, source) in compositions {
        assert!(
            source.contains("verify_migrations("),
            "{owner} must reject pending schema through the read-only verifier"
        );
        assert!(
            !source.contains("run_governed_migrations")
                && !source.contains("migrate(&")
                && !source.contains("migration_set()"),
            "{owner} composition must not own schema mutation"
        );
    }
}
