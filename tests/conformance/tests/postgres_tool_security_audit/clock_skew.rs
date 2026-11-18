use super::*;
use musteragent_identity::CommandId;
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_runtime_session::{AdmissionDenialCommit, RuntimeApplicationStore};
use musteragent_wire::ProtobufSnapshotCodec;

#[tokio::test]
async fn admission_denial_and_audit_preserve_independent_clocks() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let store = musteragent_postgres::PostgresRuntimeApplicationStore::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
    );
    let observed = UtcTimestamp::from_unix_millis(
        running_loop::database_now(&pool).await.unix_millis() + 60_000,
    );
    let denial = AdmissionDenialCommit {
        actor: Actor {
            tenant_id: fixture.tenant_id,
            principal_id: fixture.principal_id,
            principal_kind: PrincipalKind::User,
        },
        command_id: CommandId::generate(),
        conversation_id: fixture.conversation_id,
        correlation_id: CorrelationId::generate(),
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
        command_fingerprint: Sha256::digest(b"clock-skew-denial"),
        error_code: ErrorCode::PolicyDenied,
        retryable: false,
        diagnostic_ref: None,
        occurred_at: observed,
    };
    let first = store
        .record_admission_denial(denial.clone())
        .await
        .expect("persist denial");
    let replay = store
        .record_admission_denial(denial.clone())
        .await
        .expect("replay denial");
    assert_eq!(first, replay);
    assert_eq!(first.occurred_at, observed);
    let times: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT denial.occurred_at_ms, denial.committed_at_ms,
                audit.occurred_at_ms, audit.committed_at_ms
         FROM security_audit.admission_denials AS denial
         JOIN security_audit.records AS audit ON audit.audit_record_id=denial.audit_record_id
         WHERE denial.tenant_id=$1 AND denial.command_id=$2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(denial.command_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("atomic denial and audit clocks");
    assert_eq!(times.0, observed.unix_millis());
    assert_eq!(times.2, observed.unix_millis());
    assert!(times.0 > times.1 && times.2 > times.3);
    pool.close().await;
}

#[tokio::test]
async fn tool_audit_preserves_source_time_across_database_clock_skew() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrate");
    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keys.clone(),
    ));
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    let id = InvocationId::generate();
    begin_running(
        &pool,
        &journal,
        content.clone(),
        fixture,
        &attempt,
        RunningToolSpec {
            invocation_id: id,
            key: "clock-skew",
            effect: Effect::WriteExternal,
            reconciliation_ref: Some("clock-skew-reconciliation"),
        },
    )
    .await;
    // A worker clock can lead PostgreSQL. Neither an observation nor its
    // reconciled result may be rolled back because those UTC clocks differ.
    let observed = UtcTimestamp::from_unix_millis(
        running_loop::database_now(&pool).await.unix_millis() + 60_000,
    );
    journal
        .complete(
            &attempt,
            id,
            indeterminate("clock-skew-reconciliation"),
            observed,
        )
        .await
        .expect("record indeterminate observation across clocks");
    let outcome = success_result(content, fixture.tenant_id, b"verified result").await;
    let finished = UtcTimestamp::from_unix_millis(observed.unix_millis() + 1);
    journal
        .complete(&attempt, id, outcome.clone(), finished)
        .await
        .expect("commit verified result and audit atomically across clocks");
    journal
        .complete(&attempt, id, outcome, finished)
        .await
        .expect("replay the exact terminal fact");
    let rows: Vec<(i16, i64, i64)> = sqlx::query_as(
        "SELECT action, occurred_at_ms, committed_at_ms FROM security_audit.records
         WHERE source_tool_invocation_id=$1 ORDER BY action",
    )
    .bind(id.into_uuid())
    .fetch_all(&pool)
    .await
    .expect("audit records");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1, finished.unix_millis());
    assert_eq!(rows[1].1, observed.unix_millis());
    assert!(
        rows.iter()
            .all(|(_, occurred, committed)| occurred > committed)
    );
    assert_audit_pair(&pool, fixture.tenant_id, fixture.run_id, id).await;
    pool.close().await;
}
