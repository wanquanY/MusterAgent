//! Governed PostgreSQL Unit-of-Work execution and failure classification.

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use musteragent_telemetry::{DbCommitKind, DbTransactionPhase, DbTransactionResult};
use sqlx::{Connection as _, PgConnection, Postgres, Transaction};
use thiserror::Error;

use crate::database_runtime::PostgresDatabaseRuntime;

const MAX_REPLAY_ATTEMPTS: u8 = 4;
const BASE_RETRY_BACKOFF: Duration = Duration::from_millis(10);
const MAX_RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TransactionReplaySafety {
    Never,
    DeterministicIdempotent,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PostgresTransactionAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PostgresTransactionIsolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PostgresFailureClass {
    SerializationFailure,
    Deadlock,
    PoolAcquireTimeout,
    LockTimeout,
    StatementTimeout,
    Constraint,
    Unavailable,
    Other,
}

impl PostgresFailureClass {
    #[must_use]
    pub const fn telemetry_result(self) -> DbTransactionResult {
        match self {
            Self::SerializationFailure => DbTransactionResult::SerializationFailure,
            Self::Deadlock => DbTransactionResult::Deadlock,
            Self::PoolAcquireTimeout | Self::LockTimeout | Self::StatementTimeout => {
                DbTransactionResult::Timeout
            }
            Self::Constraint => DbTransactionResult::Constraint,
            Self::Unavailable => DbTransactionResult::Unavailable,
            Self::Other => DbTransactionResult::Error,
        }
    }

    const fn is_definite_abort(self) -> bool {
        matches!(
            self,
            Self::SerializationFailure
                | Self::Deadlock
                | Self::LockTimeout
                | Self::StatementTimeout
                | Self::Constraint
        )
    }

    const fn transient_before_commit(self) -> bool {
        matches!(
            self,
            Self::SerializationFailure
                | Self::Deadlock
                | Self::PoolAcquireTimeout
                | Self::LockTimeout
                | Self::StatementTimeout
                | Self::Unavailable
        )
    }
}

/// Mandatory contract for one semantic PostgreSQL Unit of Work.
///
/// The absolute deadline covers acquisition, configuration, SQL work, retry
/// backoff and commit. Callers cannot opt out of any policy dimension.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PostgresTransactionPolicy {
    pub kind: DbCommitKind,
    pub access: PostgresTransactionAccess,
    pub isolation: PostgresTransactionIsolation,
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
    pub deadline: tokio::time::Instant,
    pub replay_safety: TransactionReplaySafety,
    pub max_attempts: u8,
}

impl PostgresTransactionPolicy {
    #[must_use]
    pub fn single_attempt(
        database: &PostgresDatabaseRuntime,
        kind: DbCommitKind,
        access: PostgresTransactionAccess,
    ) -> Self {
        Self {
            kind,
            access,
            isolation: PostgresTransactionIsolation::ReadCommitted,
            statement_timeout: database.statement_timeout(),
            lock_timeout: database.lock_timeout(),
            deadline: database.transaction_deadline(),
            replay_safety: TransactionReplaySafety::Never,
            max_attempts: 1,
        }
    }

    #[must_use]
    pub fn deterministic_idempotent(
        database: &PostgresDatabaseRuntime,
        kind: DbCommitKind,
    ) -> Self {
        Self {
            kind,
            access: PostgresTransactionAccess::ReadWrite,
            isolation: PostgresTransactionIsolation::ReadCommitted,
            statement_timeout: database.statement_timeout(),
            lock_timeout: database.lock_timeout(),
            deadline: database.transaction_deadline(),
            replay_safety: TransactionReplaySafety::DeterministicIdempotent,
            max_attempts: 3,
        }
    }

    #[must_use]
    pub const fn with_isolation(mut self, isolation: PostgresTransactionIsolation) -> Self {
        self.isolation = isolation;
        self
    }

    #[must_use]
    pub const fn with_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = deadline;
        self
    }

    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u8) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn validate(self) -> Result<Self, PostgresTransactionPolicyError> {
        if self.max_attempts == 0 || self.max_attempts > MAX_REPLAY_ATTEMPTS {
            return Err(PostgresTransactionPolicyError::InvalidAttemptLimit);
        }
        if self.replay_safety == TransactionReplaySafety::Never && self.max_attempts != 1 {
            return Err(PostgresTransactionPolicyError::UnsafeReplayConfiguration);
        }
        if self.statement_timeout.is_zero()
            || self.lock_timeout.is_zero()
            || self.lock_timeout > self.statement_timeout
        {
            return Err(PostgresTransactionPolicyError::InvalidTimeouts);
        }
        Ok(self)
    }

    #[must_use]
    pub fn permits_retry(
        self,
        class: PostgresFailureClass,
        attempt: u8,
        commit_started: bool,
    ) -> bool {
        if self.replay_safety != TransactionReplaySafety::DeterministicIdempotent
            || attempt >= self.max_attempts
        {
            return false;
        }
        if commit_started {
            class.is_definite_abort()
        } else {
            class.transient_before_commit()
        }
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum PostgresTransactionPolicyError {
    #[error("PostgreSQL transaction max_attempts must be within 1..=4")]
    InvalidAttemptLimit,
    #[error("a non-replayable PostgreSQL transaction must have exactly one attempt")]
    UnsafeReplayConfiguration,
    #[error("PostgreSQL transaction statement/lock timeouts are invalid")]
    InvalidTimeouts,
}

#[derive(Debug, Error)]
pub enum PostgresSavepointError {
    #[error("PostgreSQL savepoint operation failed")]
    Operation(#[source] sqlx::Error),
    #[error(transparent)]
    Transaction(#[from] PostgresTransactionError),
}

/// The only explicit transaction handle exposed to PostgreSQL adapters.
///
/// Dropping an uncommitted handle delegates rollback to SQLx and records an
/// aborted Unit of Work. Explicit commit/rollback are deadline-bound and fully
/// classified. The handle dereferences only to `PgConnection`, preventing
/// nested transaction creation through this API.
pub struct GovernedPostgresTransaction<'connection> {
    database: PostgresDatabaseRuntime,
    transaction: Option<Transaction<'connection, Postgres>>,
    policy: PostgresTransactionPolicy,
    total_started: Instant,
}

impl std::fmt::Debug for GovernedPostgresTransaction<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GovernedPostgresTransaction")
            .field("kind", &self.policy.kind)
            .field("access", &self.policy.access)
            .field("isolation", &self.policy.isolation)
            .finish_non_exhaustive()
    }
}

impl Deref for GovernedPostgresTransaction<'_> {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.transaction
            .as_ref()
            .expect("governed transaction is finalized")
    }
}

impl DerefMut for GovernedPostgresTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.transaction
            .as_mut()
            .expect("governed transaction is finalized")
    }
}

impl AsMut<PgConnection> for GovernedPostgresTransaction<'_> {
    fn as_mut(&mut self) -> &mut PgConnection {
        self
    }
}

impl GovernedPostgresTransaction<'_> {
    /// Runs one recoverable SQL scope inside the governed Unit of Work.
    ///
    /// PostgreSQL aborts a transaction after a statement error. Callers that
    /// intentionally classify a constraint failure must use this API so the
    /// kernel owns savepoint cleanup and the parent transaction remains valid.
    pub async fn execute_savepoint<T, Operation>(
        &mut self,
        operation: Operation,
    ) -> Result<T, PostgresSavepointError>
    where
        Operation: for<'connection> FnOnce(
            &'connection mut PgConnection,
        )
            -> BoxFuture<'connection, Result<T, sqlx::Error>>,
    {
        self.execute_savepoint_statement("SAVEPOINT musteragent_governed_scope")
            .await?;
        match run_before_deadline(self.policy.deadline, operation(self)).await {
            Ok(Ok(value)) => {
                self.execute_savepoint_statement("RELEASE SAVEPOINT musteragent_governed_scope")
                    .await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                self.rollback_savepoint().await?;
                Err(PostgresSavepointError::Operation(error))
            }
            Err(()) => {
                self.rollback_savepoint().await?;
                Err(PostgresTransactionError::DeadlineExceeded {
                    phase: DbTransactionPhase::Work,
                }
                .into())
            }
        }
    }

    async fn rollback_savepoint(&mut self) -> Result<(), PostgresTransactionError> {
        self.execute_savepoint_statement("ROLLBACK TO SAVEPOINT musteragent_governed_scope")
            .await?;
        self.execute_savepoint_statement("RELEASE SAVEPOINT musteragent_governed_scope")
            .await
    }

    async fn execute_savepoint_statement(
        &mut self,
        statement: &'static str,
    ) -> Result<(), PostgresTransactionError> {
        match run_before_deadline(
            self.policy.deadline,
            sqlx::query(statement).execute(&mut **self),
        )
        .await
        {
            Err(()) => Err(PostgresTransactionError::DeadlineExceeded {
                phase: DbTransactionPhase::Work,
            }),
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(PostgresTransactionError::Database(error)),
        }
    }

    pub async fn commit(mut self) -> Result<(), PostgresTransactionError> {
        let transaction = self
            .transaction
            .take()
            .expect("governed transaction is finalized");
        let started = Instant::now();
        match run_before_deadline(self.policy.deadline, transaction.commit()).await {
            Err(()) => {
                self.observe(
                    DbTransactionPhase::Commit,
                    DbTransactionResult::CommitUnknown,
                    started.elapsed(),
                );
                self.observe_total(DbTransactionResult::CommitUnknown);
                Err(PostgresTransactionError::CommitOutcomeUnknown { source: None })
            }
            Ok(Ok(())) => {
                self.observe(
                    DbTransactionPhase::Commit,
                    DbTransactionResult::Success,
                    started.elapsed(),
                );
                self.observe_total(DbTransactionResult::Success);
                Ok(())
            }
            Ok(Err(error)) if commit_error_is_ambiguous(&error) => {
                self.observe(
                    DbTransactionPhase::Commit,
                    DbTransactionResult::CommitUnknown,
                    started.elapsed(),
                );
                self.observe_total(DbTransactionResult::CommitUnknown);
                Err(PostgresTransactionError::CommitOutcomeUnknown {
                    source: Some(error),
                })
            }
            Ok(Err(error)) => {
                let result = classify_postgres_error(&error).telemetry_result();
                self.observe(DbTransactionPhase::Commit, result, started.elapsed());
                self.observe_total(result);
                Err(PostgresTransactionError::Database(error))
            }
        }
    }

    pub async fn rollback(self) -> Result<(), PostgresTransactionError> {
        self.rollback_with_reason(DbTransactionResult::Aborted)
            .await
    }

    pub async fn rollback_with_reason(
        mut self,
        reason: DbTransactionResult,
    ) -> Result<(), PostgresTransactionError> {
        let transaction = self
            .transaction
            .take()
            .expect("governed transaction is finalized");
        let started = Instant::now();
        match run_before_deadline(self.policy.deadline, transaction.rollback()).await {
            Err(()) => {
                self.observe(
                    DbTransactionPhase::Rollback,
                    DbTransactionResult::Timeout,
                    started.elapsed(),
                );
                self.observe_total(reason);
                Err(PostgresTransactionError::DeadlineExceeded {
                    phase: DbTransactionPhase::Rollback,
                })
            }
            Ok(Ok(())) => {
                self.observe(
                    DbTransactionPhase::Rollback,
                    DbTransactionResult::Success,
                    started.elapsed(),
                );
                self.observe_total(reason);
                Ok(())
            }
            Ok(Err(error)) => {
                let result = classify_postgres_error(&error).telemetry_result();
                self.observe(DbTransactionPhase::Rollback, result, started.elapsed());
                self.observe_total(reason);
                Err(PostgresTransactionError::Database(error))
            }
        }
    }

    fn observe(&self, phase: DbTransactionPhase, result: DbTransactionResult, duration: Duration) {
        self.database
            .observe_transaction(self.policy.kind, phase, result, duration);
    }

    fn observe_total(&self, result: DbTransactionResult) {
        self.observe(
            DbTransactionPhase::Total,
            result,
            self.total_started.elapsed(),
        );
    }
}

impl Drop for GovernedPostgresTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() {
            // SQLx starts rollback when its Transaction is dropped. We record
            // the semantic abort here; the connection cannot be reused until
            // SQLx has completed that rollback.
            self.observe(
                DbTransactionPhase::Rollback,
                DbTransactionResult::Aborted,
                Duration::ZERO,
            );
            self.observe_total(DbTransactionResult::Aborted);
        }
    }
}

/// Starts one governed transaction from the Runtime-owned pool.
pub async fn begin_transaction(
    database: &PostgresDatabaseRuntime,
    policy: PostgresTransactionPolicy,
) -> Result<GovernedPostgresTransaction<'static>, PostgresTransactionError> {
    let policy = policy.validate()?;
    let total_started = Instant::now();
    let acquire_started = Instant::now();
    let transaction = match run_before_deadline(policy.deadline, database.pool().begin()).await {
        Err(()) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                DbTransactionResult::Timeout,
                acquire_started.elapsed(),
            );
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Total,
                DbTransactionResult::Timeout,
                total_started.elapsed(),
            );
            return Err(PostgresTransactionError::DeadlineExceeded {
                phase: DbTransactionPhase::Acquire,
            });
        }
        Ok(Ok(transaction)) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                DbTransactionResult::Success,
                acquire_started.elapsed(),
            );
            transaction
        }
        Ok(Err(error)) => {
            let result = classify_postgres_error(&error).telemetry_result();
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                result,
                acquire_started.elapsed(),
            );
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Total,
                result,
                total_started.elapsed(),
            );
            return Err(PostgresTransactionError::Database(error));
        }
    };
    initialize_transaction(database, policy, transaction, total_started).await
}

/// Starts one governed transaction on an already-pinned connection.
///
/// The migration runner uses this variant because its PostgreSQL advisory lock
/// is session-scoped and every migration transaction must remain on that exact
/// connection.
pub async fn begin_connection_transaction<'connection>(
    connection: &'connection mut PgConnection,
    database: &PostgresDatabaseRuntime,
    policy: PostgresTransactionPolicy,
) -> Result<GovernedPostgresTransaction<'connection>, PostgresTransactionError> {
    let policy = policy.validate()?;
    let total_started = Instant::now();
    let acquire_started = Instant::now();
    let transaction = match run_before_deadline(policy.deadline, connection.begin()).await {
        Err(()) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                DbTransactionResult::Timeout,
                acquire_started.elapsed(),
            );
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Total,
                DbTransactionResult::Timeout,
                total_started.elapsed(),
            );
            return Err(PostgresTransactionError::DeadlineExceeded {
                phase: DbTransactionPhase::Acquire,
            });
        }
        Ok(Ok(transaction)) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                DbTransactionResult::Success,
                acquire_started.elapsed(),
            );
            transaction
        }
        Ok(Err(error)) => {
            let result = classify_postgres_error(&error).telemetry_result();
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Acquire,
                result,
                acquire_started.elapsed(),
            );
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Total,
                result,
                total_started.elapsed(),
            );
            return Err(PostgresTransactionError::Database(error));
        }
    };
    initialize_transaction(database, policy, transaction, total_started).await
}

async fn initialize_transaction<'connection>(
    database: &PostgresDatabaseRuntime,
    policy: PostgresTransactionPolicy,
    mut transaction: Transaction<'connection, Postgres>,
    total_started: Instant,
) -> Result<GovernedPostgresTransaction<'connection>, PostgresTransactionError> {
    let work_started = Instant::now();
    let configuration = run_before_deadline(
        policy.deadline,
        configure_transaction(&mut transaction, policy),
    )
    .await;
    match configuration {
        Ok(Ok(())) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Work,
                DbTransactionResult::Success,
                work_started.elapsed(),
            );
            Ok(GovernedPostgresTransaction {
                database: database.clone(),
                transaction: Some(transaction),
                policy,
                total_started,
            })
        }
        Err(()) => {
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Work,
                DbTransactionResult::Timeout,
                work_started.elapsed(),
            );
            rollback_failed_begin(
                database,
                policy,
                transaction,
                total_started,
                DbTransactionResult::Timeout,
            )
            .await;
            Err(PostgresTransactionError::DeadlineExceeded {
                phase: DbTransactionPhase::Work,
            })
        }
        Ok(Err(error)) => {
            let result = classify_postgres_error(&error).telemetry_result();
            observe(
                database,
                policy.kind,
                DbTransactionPhase::Work,
                result,
                work_started.elapsed(),
            );
            rollback_failed_begin(database, policy, transaction, total_started, result).await;
            Err(PostgresTransactionError::Database(error))
        }
    }
}

async fn configure_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    policy: PostgresTransactionPolicy,
) -> Result<(), sqlx::Error> {
    let mode = match (policy.isolation, policy.access) {
        (PostgresTransactionIsolation::ReadCommitted, PostgresTransactionAccess::ReadOnly) => {
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"
        }
        (PostgresTransactionIsolation::ReadCommitted, PostgresTransactionAccess::ReadWrite) => {
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE"
        }
        (PostgresTransactionIsolation::RepeatableRead, PostgresTransactionAccess::ReadOnly) => {
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        }
        (PostgresTransactionIsolation::RepeatableRead, PostgresTransactionAccess::ReadWrite) => {
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE"
        }
        (PostgresTransactionIsolation::Serializable, PostgresTransactionAccess::ReadOnly) => {
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY"
        }
        (PostgresTransactionIsolation::Serializable, PostgresTransactionAccess::ReadWrite) => {
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"
        }
    };
    sqlx::query(mode).execute(&mut **transaction).await?;

    let remaining = policy
        .deadline
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or_else(|| sqlx::Error::Protocol("transaction deadline elapsed".to_owned()))?;
    let statement_timeout = postgres_timeout(policy.statement_timeout.min(remaining));
    let lock_timeout = postgres_timeout(policy.lock_timeout.min(remaining));
    sqlx::query(
        "SELECT set_config('statement_timeout', $1, true),
                set_config('lock_timeout', $2, true)",
    )
    .bind(statement_timeout)
    .bind(lock_timeout)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn postgres_timeout(duration: Duration) -> String {
    let millis = u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    format!("{millis}ms")
}

async fn rollback_failed_begin(
    database: &PostgresDatabaseRuntime,
    policy: PostgresTransactionPolicy,
    transaction: Transaction<'_, Postgres>,
    total_started: Instant,
    total_result: DbTransactionResult,
) {
    let started = Instant::now();
    let result = match run_before_deadline(policy.deadline, transaction.rollback()).await {
        Err(()) => DbTransactionResult::Timeout,
        Ok(Ok(())) => DbTransactionResult::Success,
        Ok(Err(error)) => classify_postgres_error(&error).telemetry_result(),
    };
    observe(
        database,
        policy.kind,
        DbTransactionPhase::Rollback,
        result,
        started.elapsed(),
    );
    observe(
        database,
        policy.kind,
        DbTransactionPhase::Total,
        total_result,
        total_started.elapsed(),
    );
}

/// Executes a SQL-only, deterministic and idempotent transaction closure.
///
/// The closure is invoked again only when the declared policy permits replay.
/// It must perform no network, file, provider, user-notification, or other
/// external side effect. Every replay gets a fresh transaction and must reread
/// mutable durable authority inside the closure.
pub async fn execute_transaction<T, Operation>(
    database: &PostgresDatabaseRuntime,
    policy: PostgresTransactionPolicy,
    mut operation: Operation,
) -> Result<T, PostgresTransactionError>
where
    T: Send,
    Operation: for<'connection> FnMut(
        &'connection mut PgConnection,
    ) -> BoxFuture<'connection, Result<T, sqlx::Error>>,
{
    let policy = policy.validate()?;
    let mut attempt = 1u8;
    loop {
        let mut transaction = match begin_transaction(database, policy).await {
            Ok(transaction) => transaction,
            Err(error) => {
                let class = error.failure_class();
                let retry_started = Instant::now();
                if !policy.permits_retry(class, attempt, false)
                    || !retry_delay(attempt, policy.deadline).await
                {
                    return Err(error);
                }
                observe(
                    database,
                    policy.kind,
                    DbTransactionPhase::RetryBackoff,
                    class.telemetry_result(),
                    retry_started.elapsed(),
                );
                attempt += 1;
                continue;
            }
        };

        let work_started = Instant::now();
        let value = match run_before_deadline(policy.deadline, operation(&mut transaction)).await {
            Err(()) => {
                transaction
                    .rollback_with_reason(DbTransactionResult::Timeout)
                    .await
                    .ok();
                return Err(PostgresTransactionError::DeadlineExceeded {
                    phase: DbTransactionPhase::Work,
                });
            }
            Ok(Ok(value)) => {
                observe(
                    database,
                    policy.kind,
                    DbTransactionPhase::Work,
                    DbTransactionResult::Success,
                    work_started.elapsed(),
                );
                value
            }
            Ok(Err(error)) => {
                let class = classify_postgres_error(&error);
                observe(
                    database,
                    policy.kind,
                    DbTransactionPhase::Work,
                    class.telemetry_result(),
                    work_started.elapsed(),
                );
                transaction
                    .rollback_with_reason(class.telemetry_result())
                    .await
                    .ok();
                let retry_started = Instant::now();
                if !policy.permits_retry(class, attempt, false)
                    || !retry_delay(attempt, policy.deadline).await
                {
                    return Err(PostgresTransactionError::Database(error));
                }
                observe(
                    database,
                    policy.kind,
                    DbTransactionPhase::RetryBackoff,
                    class.telemetry_result(),
                    retry_started.elapsed(),
                );
                attempt += 1;
                continue;
            }
        };

        match transaction.commit().await {
            Ok(()) => return Ok(value),
            Err(error @ PostgresTransactionError::CommitOutcomeUnknown { .. }) => {
                return Err(error);
            }
            Err(error) => {
                let class = error.failure_class();
                let retry_started = Instant::now();
                if !policy.permits_retry(class, attempt, true)
                    || !retry_delay(attempt, policy.deadline).await
                {
                    return Err(error);
                }
                observe(
                    database,
                    policy.kind,
                    DbTransactionPhase::RetryBackoff,
                    class.telemetry_result(),
                    retry_started.elapsed(),
                );
                attempt += 1;
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum PostgresTransactionError {
    #[error(transparent)]
    Policy(#[from] PostgresTransactionPolicyError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("PostgreSQL transaction command deadline exceeded during {phase:?}")]
    DeadlineExceeded { phase: DbTransactionPhase },
    #[error("PostgreSQL COMMIT outcome is unknown")]
    CommitOutcomeUnknown {
        #[source]
        source: Option<sqlx::Error>,
    },
}

impl PostgresTransactionError {
    #[must_use]
    pub fn failure_class(&self) -> PostgresFailureClass {
        match self {
            Self::Policy(_) => PostgresFailureClass::Other,
            Self::Database(error) => classify_postgres_error(error),
            Self::DeadlineExceeded {
                phase: DbTransactionPhase::Acquire,
            } => PostgresFailureClass::PoolAcquireTimeout,
            Self::DeadlineExceeded { .. } => PostgresFailureClass::StatementTimeout,
            Self::CommitOutcomeUnknown { .. } => PostgresFailureClass::Unavailable,
        }
    }
}

async fn run_before_deadline<T>(
    deadline: tokio::time::Instant,
    future: impl Future<Output = T>,
) -> Result<T, ()> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| ())
}

#[must_use]
pub fn classify_postgres_error(error: &sqlx::Error) -> PostgresFailureClass {
    if let Some(code) = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
    {
        return classify_sqlstate(code.as_ref());
    }
    match error {
        sqlx::Error::PoolTimedOut => PostgresFailureClass::PoolAcquireTimeout,
        sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::BeginFailed => PostgresFailureClass::Unavailable,
        _ => PostgresFailureClass::Other,
    }
}

#[must_use]
pub const fn classify_sqlstate(code: &str) -> PostgresFailureClass {
    match code.as_bytes() {
        b"40001" => PostgresFailureClass::SerializationFailure,
        b"40P01" => PostgresFailureClass::Deadlock,
        b"55P03" => PostgresFailureClass::LockTimeout,
        b"57014" => PostgresFailureClass::StatementTimeout,
        [b'2', b'3', ..] => PostgresFailureClass::Constraint,
        [b'0', b'8', ..] => PostgresFailureClass::Unavailable,
        _ => PostgresFailureClass::Other,
    }
}

fn commit_error_is_ambiguous(error: &sqlx::Error) -> bool {
    error.as_database_error().is_none()
}

fn observe(
    database: &PostgresDatabaseRuntime,
    kind: DbCommitKind,
    phase: DbTransactionPhase,
    result: DbTransactionResult,
    duration: Duration,
) {
    database.observe_transaction(kind, phase, result, duration);
}

async fn retry_delay(attempt: u8, deadline: tokio::time::Instant) -> bool {
    let exponent = u32::from(attempt.saturating_sub(1)).min(4);
    let base = BASE_RETRY_BACKOFF
        .saturating_mul(1u32 << exponent)
        .min(MAX_RETRY_BACKOFF);
    let jitter_cap = u64::try_from(base.as_millis()).unwrap_or(1).max(1);
    let jitter_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::from(duration.subsec_nanos()) % jitter_cap
        });
    let delay = base.saturating_add(Duration::from_millis(jitter_millis));
    if tokio::time::Instant::now() + delay >= deadline {
        return false;
    }
    tokio::time::sleep(delay).await;
    true
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use musteragent_telemetry::DbCommitKind;
    use sqlx::Error as SqlxError;

    use super::{
        PostgresFailureClass, PostgresTransactionAccess, PostgresTransactionIsolation,
        PostgresTransactionPolicy, TransactionReplaySafety, classify_postgres_error,
        classify_sqlstate,
    };

    fn policy(
        replay_safety: TransactionReplaySafety,
        max_attempts: u8,
    ) -> PostgresTransactionPolicy {
        PostgresTransactionPolicy {
            kind: DbCommitKind::Projection,
            access: PostgresTransactionAccess::ReadWrite,
            isolation: PostgresTransactionIsolation::ReadCommitted,
            statement_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(5),
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            replay_safety,
            max_attempts,
        }
    }

    #[test]
    fn sqlstate_classification_is_closed_and_does_not_retry_constraints() {
        assert_eq!(
            classify_sqlstate("40001"),
            PostgresFailureClass::SerializationFailure
        );
        assert_eq!(classify_sqlstate("40P01"), PostgresFailureClass::Deadlock);
        assert_eq!(
            classify_sqlstate("55P03"),
            PostgresFailureClass::LockTimeout
        );
        assert_eq!(classify_sqlstate("23505"), PostgresFailureClass::Constraint);
        let policy = policy(TransactionReplaySafety::DeterministicIdempotent, 3);
        assert!(!policy.permits_retry(PostgresFailureClass::Constraint, 1, false));
        assert!(policy.permits_retry(PostgresFailureClass::Deadlock, 1, true));
        assert!(!policy.permits_retry(PostgresFailureClass::Unavailable, 1, true));
        assert_eq!(
            classify_postgres_error(&SqlxError::PoolTimedOut),
            PostgresFailureClass::PoolAcquireTimeout
        );
        assert!(policy.permits_retry(PostgresFailureClass::PoolAcquireTimeout, 1, false));
        assert!(!policy.permits_retry(PostgresFailureClass::PoolAcquireTimeout, 1, true));
    }

    #[test]
    fn replay_safety_attempt_limit_and_timeouts_cannot_be_misconfigured() {
        assert!(
            policy(TransactionReplaySafety::Never, 2)
                .validate()
                .is_err()
        );
        assert!(
            policy(TransactionReplaySafety::DeterministicIdempotent, 5)
                .validate()
                .is_err()
        );
        let mut invalid_timeouts = policy(TransactionReplaySafety::Never, 1);
        invalid_timeouts.lock_timeout = invalid_timeouts.statement_timeout + Duration::from_secs(1);
        assert!(invalid_timeouts.validate().is_err());
    }
}
