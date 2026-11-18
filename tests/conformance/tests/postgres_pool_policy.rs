use std::sync::Arc;
use std::time::Duration;

use musteragent_postgres::{
    PostgresPoolConfig, PostgresPoolSnapshot, PostgresTimeouts, PostgresTransactionAccess,
    PostgresTransactionError, PostgresTransactionPolicy, execute_transaction,
};
use musteragent_telemetry::machine::{
    MachineTelemetry, MachineTelemetryBackend, RuntimeSpanDescriptor, TypedMachineEvent,
};
use musteragent_telemetry::{
    DataEdgeOperation, DbCommitKind, DbTransactionPhase, DbTransactionResult, RuntimeSpan,
    TelemetryOutcome,
};
use musteragent_testkit::IsolatedPostgres;
use sqlx::Row as _;

struct NoopTelemetry;

impl MachineTelemetryBackend for NoopTelemetry {
    fn start_span(&self, _descriptor: RuntimeSpanDescriptor) -> RuntimeSpan {
        RuntimeSpan::none()
    }

    fn emit(&self, _event: TypedMachineEvent) {}

    fn record_data_edge(
        &self,
        _operation: DataEdgeOperation,
        _outcome: TelemetryOutcome,
        _duration: Duration,
    ) {
    }

    fn record_db_commit(
        &self,
        _kind: DbCommitKind,
        _outcome: TelemetryOutcome,
        _duration: Duration,
    ) {
    }

    fn record_db_transaction(
        &self,
        _kind: DbCommitKind,
        _phase: DbTransactionPhase,
        _result: DbTransactionResult,
        _duration: Duration,
    ) {
    }
}

#[tokio::test]
async fn governed_pool_applies_identity_lifecycle_and_session_timeouts() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let config = PostgresPoolConfig::new(
        "conformance",
        "pool-policy",
        2,
        3,
        PostgresTimeouts {
            acquire: Duration::from_secs(2),
            idle: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(120),
            statement: Duration::from_secs(17),
            lock: Duration::from_secs(3),
            idle_in_transaction: Duration::from_secs(11),
        },
    )
    .expect("valid governed pool config");
    let telemetry = MachineTelemetry::from_backend(Arc::new(NoopTelemetry)).db_commit();
    let database = config
        .connect_runtime(postgres.connection_url(), telemetry)
        .await
        .expect("connect through governed pool factory");
    let pool = database.pool();

    assert!(pool.size() >= 2, "configured minimum was not established");
    assert_eq!(pool.options().get_max_connections(), 3);
    let snapshot = PostgresPoolSnapshot::capture(pool);
    assert_eq!(snapshot.max, 3);
    assert_eq!(snapshot.used + snapshot.idle, pool.size());
    let row = sqlx::query(
        "SELECT current_setting('application_name') AS application_name,
                current_setting('statement_timeout')::interval = interval '17 seconds'
                    AS statement_timeout_matches,
                current_setting('lock_timeout')::interval = interval '3 seconds'
                    AS lock_timeout_matches,
                current_setting('idle_in_transaction_session_timeout')::interval
                    = interval '11 seconds' AS idle_transaction_timeout_matches",
    )
    .fetch_one(pool)
    .await
    .expect("read governed session policy");
    assert_eq!(
        row.get::<String, _>("application_name"),
        "musteragent-runtime/conformance/pool-policy"
    );
    assert!(row.get::<bool, _>("statement_timeout_matches"));
    assert!(row.get::<bool, _>("lock_timeout_matches"));
    assert!(row.get::<bool, _>("idle_transaction_timeout_matches"));

    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let deadline_result = execute_transaction(
        &database,
        PostgresTransactionPolicy::single_attempt(
            &database,
            DbCommitKind::Projection,
            PostgresTransactionAccess::ReadWrite,
        )
        .with_deadline(deadline),
        |_connection| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<(), sqlx::Error>(())
            })
        },
    )
    .await;
    assert!(matches!(
        deadline_result,
        Err(PostgresTransactionError::DeadlineExceeded { .. })
    ));
    let connection_is_reusable: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(pool)
        .await
        .expect("deadline rollback returns the pool to service");
    assert_eq!(connection_is_reusable, 1);

    pool.close().await;
    drop(postgres);
}
