use super::*;
use musteragent_testkit::IsolatedPostgres;
use std::collections::BTreeSet;

#[test]
fn migration_set_contains_baseline_and_expand_only_migrations() {
    let migrations: Vec<_> = migration_set().iter().collect();
    assert_eq!(migrations.len(), 2);
    let baseline = migrations[0];
    assert_eq!(baseline.version, 1);
    assert_eq!(baseline.description, "kernel baseline");
    let pause_reason = migrations[1];
    assert_eq!(pause_reason.version, 2);
    assert_eq!(pause_reason.description, "schedule pause reason");
    let tables: BTreeSet<_> = migrations
        .iter()
        .flat_map(|migration| migration.sql.lines())
        .filter_map(|line| line.strip_prefix("CREATE TABLE "))
        .map(|line| {
            line.split_whitespace()
                .next()
                .expect("table name")
                .split_once('.')
                .expect("schema-qualified table")
        })
        .collect();
    assert_eq!(tables, RUNTIME_TABLES.iter().copied().collect());
    assert_eq!(tables.len(), RUNTIME_TABLES.len(), "no duplicates");
    assert_eq!(
        tables
            .iter()
            .map(|(schema, _)| *schema)
            .collect::<BTreeSet<_>>(),
        RUNTIME_SCHEMAS.iter().copied().collect()
    );
}

#[tokio::test]
async fn empty_database_migrates_verifies_and_matches_kernel_inventory() {
    let database = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start disposable pgvector database");
    let pool = database.pool().await.expect("connect disposable database");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::NotInitialized)
    ));
    migrate(&pool)
        .await
        .expect("install baseline on empty database");
    verify_migrations(&pool).await.expect("verify baseline");
    migrate(&pool).await.expect("idempotent migration retry");
    let tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT schemaname::text, tablename::text FROM pg_tables
         WHERE schemaname = ANY($1) ORDER BY schemaname, tablename",
    )
    .bind(RUNTIME_SCHEMAS)
    .fetch_all(&pool)
    .await
    .expect("read installed table inventory");
    let expected: Vec<_> = RUNTIME_TABLES
        .iter()
        .map(|(schema, table)| (schema.to_string(), table.to_string()))
        .collect();
    assert_eq!(tables, expected);
    let history: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .expect("read installed migration checksums");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].0, 1);
    assert_eq!(
        history[0]
            .1
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "f6b7502c04f384d9efbbad8ff9f7868fd96c8c22689b4db595d276564f81967a3b21cdc5fef7b01f480e0071b3c35400"
    );
    assert_eq!(history[1].0, 2);
    assert_eq!(
        history[1].1.as_slice(),
        migration_set()
            .iter()
            .nth(1)
            .expect("schedule pause reason migration")
            .checksum
            .as_ref()
    );
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM artifact.content_cache_revision WHERE singleton")
            .fetch_one(&pool)
            .await
            .expect("initialized content cache revision");
    assert_eq!(revision, 1);
    let cursor: (i64, String) = sqlx::query_as(
        "SELECT created_at_ms, content_id::text
         FROM artifact.content_blob_compaction_cursor WHERE singleton",
    )
    .fetch_one(&pool)
    .await
    .expect("initialized blob compaction cursor");
    assert_eq!(cursor, (-1, "00000000-0000-0000-0000-000000000000".into()));
    let capabilities: Vec<(String, bool)> = sqlx::query_as(
        "SELECT backend, available FROM memory.embedding_capability WHERE singleton
         UNION ALL
         SELECT backend, available FROM resource.embedding_capability WHERE singleton",
    )
    .fetch_all(&pool)
    .await
    .expect("initialized vector projections");
    assert_eq!(capabilities, vec![("pgvector".into(), true); 2]);
    pool.close().await;
}

#[tokio::test]
async fn verification_rejects_dirty_unknown_changed_and_missing_history() {
    let database = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start disposable pgvector database");
    let pool = database.pool().await.expect("connect disposable database");
    migrate(&pool).await.expect("install baseline");

    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 1")
        .execute(&pool)
        .await
        .expect("simulate interrupted migration");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::Dirty(1))
    ));
    let foreign_version = i64::MAX;
    sqlx::query("UPDATE _sqlx_migrations SET success = true, version = $1 WHERE version = 1")
        .bind(foreign_version)
        .execute(&pool)
        .await
        .expect("simulate foreign lineage");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::UnknownVersion(version)) if version == foreign_version
    ));
    sqlx::query("UPDATE _sqlx_migrations SET version = 1, checksum = ''::bytea WHERE version = $1")
        .bind(foreign_version)
        .execute(&pool)
        .await
        .expect("simulate altered checksum");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::ChecksumMismatch(1))
    ));
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(
            migration_set()
                .iter()
                .next()
                .expect("baseline")
                .checksum
                .as_ref(),
        )
        .execute(&pool)
        .await
        .expect("restore baseline checksum");
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 2")
        .execute(&pool)
        .await
        .expect("simulate pending follow-up migration");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::Pending(2))
    ));
    sqlx::query("DELETE FROM _sqlx_migrations")
        .execute(&pool)
        .await
        .expect("simulate missing baseline history");
    assert!(matches!(
        verify_migrations(&pool).await,
        Err(MigrationVerificationError::Pending(1))
    ));
    pool.close().await;
}
