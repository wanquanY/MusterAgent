use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use musteragent_telemetry::{
    DbCommitKind, DbCommitTelemetry, DbTransactionPhase, DbTransactionResult,
};
use sha2::{Digest as _, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row as _};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(1_800);
pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(15);
pub const DEFAULT_POOL_OBSERVATION_INTERVAL: Duration = Duration::from_secs(15);
const POSTGRES_APPLICATION_NAME_MAX_BYTES: usize = 63;
const POSTGRES_APPLICATION_NAME_HASH_BYTES: usize = 16;

trait PostgresTransactionObserver: Send + Sync {
    fn observe(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    );
}

struct MachineTransactionObserver {
    telemetry: DbCommitTelemetry,
}

impl PostgresTransactionObserver for MachineTransactionObserver {
    fn observe(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        self.telemetry
            .observe_transaction(kind, phase, result, duration);
    }
}

struct TracingTransactionObserver;

impl PostgresTransactionObserver for TracingTransactionObserver {
    fn observe(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        tracing::trace!(
            database.backend = "postgresql",
            transaction.kind = kind.as_str(),
            transaction.phase = phase.as_str(),
            transaction.result = result.as_str(),
            transaction.duration_micros = duration.as_micros(),
            "governed PostgreSQL transaction phase"
        );
    }
}

/// Cloneable owner of one governed PostgreSQL pool and its transaction policy.
///
/// Production composition constructs this value with machine telemetry. Tests
/// and local fixtures that start from an isolated SQLx pool still receive the
/// same deadline and tracing contract through [`Self::from_pool`]. Stores may
/// expose the underlying pool only for query execution; transaction creation
/// remains owned by the transaction kernel.
#[derive(Clone)]
pub struct PostgresDatabaseRuntime {
    pool: PgPool,
    observer: Arc<dyn PostgresTransactionObserver>,
    statement_timeout: Duration,
    lock_timeout: Duration,
}

impl std::fmt::Debug for PostgresDatabaseRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresDatabaseRuntime")
            .field("pool", &"[REDACTED]")
            .field("statement_timeout", &self.statement_timeout)
            .field("lock_timeout", &self.lock_timeout)
            .finish_non_exhaustive()
    }
}

impl PostgresDatabaseRuntime {
    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            observer: Arc::new(TracingTransactionObserver),
            statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }

    fn observed(
        pool: PgPool,
        telemetry: DbCommitTelemetry,
        statement_timeout: Duration,
        lock_timeout: Duration,
    ) -> Self {
        Self {
            pool,
            observer: Arc::new(MachineTransactionObserver { telemetry }),
            statement_timeout,
            lock_timeout,
        }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub(crate) fn transaction_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.statement_timeout
    }

    #[must_use]
    pub(crate) const fn statement_timeout(&self) -> Duration {
        self.statement_timeout
    }

    #[must_use]
    pub(crate) const fn lock_timeout(&self) -> Duration {
        self.lock_timeout
    }

    pub(crate) fn observe_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        self.observer.observe(kind, phase, result, duration);
    }

    /// Opens a non-replayable read-write Unit of Work under the Runtime's
    /// mandatory deadline, timeout and telemetry contract.
    pub async fn write_unit_of_work(
        &self,
        kind: DbCommitKind,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, crate::PostgresTransactionError> {
        crate::begin_transaction(
            self,
            crate::PostgresTransactionPolicy::single_attempt(
                self,
                kind,
                crate::PostgresTransactionAccess::ReadWrite,
            ),
        )
        .await
    }

    /// Opens a non-replayable read-only Unit of Work under the same kernel.
    pub async fn read_unit_of_work(
        &self,
        kind: DbCommitKind,
        isolation: crate::PostgresTransactionIsolation,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, crate::PostgresTransactionError> {
        crate::begin_transaction(
            self,
            crate::PostgresTransactionPolicy::single_attempt(
                self,
                kind,
                crate::PostgresTransactionAccess::ReadOnly,
            )
            .with_isolation(isolation),
        )
        .await
    }
}

impl From<PgPool> for PostgresDatabaseRuntime {
    fn from(pool: PgPool) -> Self {
        Self::from_pool(pool)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PostgresPoolSnapshot {
    pub used: u32,
    pub idle: u32,
    pub max: u32,
}

impl PostgresPoolSnapshot {
    #[must_use]
    pub fn capture(pool: &PgPool) -> Self {
        let size = pool.size();
        let idle = u32::try_from(pool.num_idle()).unwrap_or(u32::MAX).min(size);
        Self {
            used: size.saturating_sub(idle),
            idle,
            max: pool.options().get_max_connections(),
        }
    }
}

/// Samples SQLx's in-process pool authority at a fixed cadence. The observer
/// is synchronous by design: exporting metrics must never occupy a database
/// connection or delay pool acquisition.
pub async fn observe_postgres_pool<Observer>(
    pool: PgPool,
    cancellation: CancellationToken,
    observer: Observer,
) where
    Observer: Fn(PostgresPoolSnapshot) + Send + Sync + 'static,
{
    let mut ticks = tokio::time::interval(DEFAULT_POOL_OBSERVATION_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            _ = ticks.tick() => observer(PostgresPoolSnapshot::capture(&pool)),
        }
    }
}

/// Closed, validated configuration for one Runtime-owned PostgreSQL pool.
///
/// Application composition roots may select values, but they cannot construct
/// SQLx pools directly. This keeps connection identity, lifecycle and session
/// timeouts identical across API, Worker and Projector processes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostgresPoolConfig {
    application_name: String,
    min_connections: u32,
    max_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Duration,
    max_lifetime: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
    idle_in_transaction_timeout: Duration,
}

impl PostgresPoolConfig {
    pub fn standard(
        service: &str,
        instance: &str,
        max_connections: u32,
    ) -> Result<Self, PostgresPoolConfigError> {
        Self::new(
            service,
            instance,
            1,
            max_connections,
            PostgresTimeouts::default(),
        )
    }

    pub fn new(
        service: &str,
        instance: &str,
        min_connections: u32,
        max_connections: u32,
        timeouts: PostgresTimeouts,
    ) -> Result<Self, PostgresPoolConfigError> {
        validate_identity(service, "service")?;
        validate_identity(instance, "instance")?;
        if min_connections == 0 || max_connections == 0 || min_connections > max_connections {
            return Err(PostgresPoolConfigError::InvalidConnectionBounds);
        }
        timeouts.validate()?;
        Ok(Self {
            application_name: postgres_application_name(service, instance),
            min_connections,
            max_connections,
            acquire_timeout: timeouts.acquire,
            idle_timeout: timeouts.idle,
            max_lifetime: timeouts.max_lifetime,
            statement_timeout: timeouts.statement,
            lock_timeout: timeouts.lock,
            idle_in_transaction_timeout: timeouts.idle_in_transaction,
        })
    }

    #[must_use]
    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    #[must_use]
    pub const fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub async fn connect_runtime(
        &self,
        database_url: &str,
        telemetry: DbCommitTelemetry,
    ) -> Result<PostgresDatabaseRuntime, sqlx::Error> {
        let pool = self.connect_pool(database_url).await?;
        Ok(PostgresDatabaseRuntime::observed(
            pool,
            telemetry,
            self.statement_timeout,
            self.lock_timeout,
        ))
    }

    async fn connect_pool(&self, database_url: &str) -> Result<PgPool, sqlx::Error> {
        let options =
            PgConnectOptions::from_str(database_url)?.application_name(&self.application_name);
        let statement_timeout = postgres_duration(self.statement_timeout);
        let lock_timeout = postgres_duration(self.lock_timeout);
        let idle_in_transaction_timeout = postgres_duration(self.idle_in_transaction_timeout);
        let expected_application_name = self.application_name.clone();
        let pool = PgPoolOptions::new()
            .min_connections(self.min_connections)
            .max_connections(self.max_connections)
            .acquire_timeout(self.acquire_timeout)
            .idle_timeout(Some(self.idle_timeout))
            .max_lifetime(Some(self.max_lifetime))
            .after_connect(move |connection, _metadata| {
                let statement_timeout = statement_timeout.clone();
                let lock_timeout = lock_timeout.clone();
                let idle_in_transaction_timeout = idle_in_transaction_timeout.clone();
                let expected_application_name = expected_application_name.clone();
                Box::pin(async move {
                    let row = sqlx::query(
                        "SELECT set_config('statement_timeout', $1, false) AS statement_timeout,
                                set_config('lock_timeout', $2, false) AS lock_timeout,
                                set_config('idle_in_transaction_session_timeout', $3, false)
                                    AS idle_in_transaction_timeout,
                                current_setting('application_name') AS application_name",
                    )
                    .bind(&statement_timeout)
                    .bind(&lock_timeout)
                    .bind(&idle_in_transaction_timeout)
                    .fetch_one(&mut *connection)
                    .await?;
                    if row.try_get::<String, _>("application_name")? != expected_application_name {
                        return Err(sqlx::Error::Protocol(
                            "PostgreSQL application_name initialization drift".to_owned(),
                        ));
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        Ok(pool)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PostgresTimeouts {
    pub acquire: Duration,
    pub idle: Duration,
    pub max_lifetime: Duration,
    pub statement: Duration,
    pub lock: Duration,
    pub idle_in_transaction: Duration,
}

impl Default for PostgresTimeouts {
    fn default() -> Self {
        Self {
            acquire: DEFAULT_ACQUIRE_TIMEOUT,
            idle: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: DEFAULT_MAX_LIFETIME,
            statement: DEFAULT_STATEMENT_TIMEOUT,
            lock: DEFAULT_LOCK_TIMEOUT,
            idle_in_transaction: DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT,
        }
    }
}

impl PostgresTimeouts {
    fn validate(self) -> Result<(), PostgresPoolConfigError> {
        if self.acquire.is_zero()
            || self.idle.is_zero()
            || self.max_lifetime.is_zero()
            || self.statement.is_zero()
            || self.lock.is_zero()
            || self.idle_in_transaction.is_zero()
        {
            return Err(PostgresPoolConfigError::ZeroTimeout);
        }
        if self.acquire > self.statement
            || self.lock > self.statement
            || self.idle_in_transaction > self.statement
        {
            return Err(PostgresPoolConfigError::InvalidTimeoutOrder);
        }
        for duration in [
            self.acquire,
            self.idle,
            self.max_lifetime,
            self.statement,
            self.lock,
            self.idle_in_transaction,
        ] {
            let _ = u64::try_from(duration.as_millis())
                .map_err(|_| PostgresPoolConfigError::TimeoutTooLarge)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum PostgresPoolConfigError {
    #[error("PostgreSQL pool connection bounds are invalid")]
    InvalidConnectionBounds,
    #[error("PostgreSQL pool identity is invalid: {0}")]
    InvalidIdentity(&'static str),
    #[error("PostgreSQL pool timeout must be non-zero")]
    ZeroTimeout,
    #[error(
        "PostgreSQL acquire, lock and idle transaction timeouts must not exceed statement timeout"
    )]
    InvalidTimeoutOrder,
    #[error("PostgreSQL pool timeout exceeds the supported millisecond range")]
    TimeoutTooLarge,
}

fn validate_identity(value: &str, field: &'static str) -> Result<(), PostgresPoolConfigError> {
    if value.is_empty()
        || value.len() > 160
        || value.chars().any(|character| character.is_control())
    {
        return Err(PostgresPoolConfigError::InvalidIdentity(field));
    }
    Ok(())
}

fn postgres_application_name(service: &str, instance: &str) -> String {
    let full = format!("musteragent-runtime/{service}/{instance}");
    if full.len() <= POSTGRES_APPLICATION_NAME_MAX_BYTES {
        return full;
    }

    let digest = Sha256::digest(full.as_bytes());
    let suffix = digest[..POSTGRES_APPLICATION_NAME_HASH_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let prefix_budget = POSTGRES_APPLICATION_NAME_MAX_BYTES
        .saturating_sub(1)
        .saturating_sub(suffix.len());
    let mut prefix_end = prefix_budget.min(full.len());
    while !full.is_char_boundary(prefix_end) {
        prefix_end = prefix_end.saturating_sub(1);
    }
    format!("{}~{suffix}", &full[..prefix_end])
}

fn postgres_duration(duration: Duration) -> String {
    format!("{}ms", duration.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_pool_policy_is_bounded_and_identified() {
        let config = PostgresPoolConfig::standard("cloud-worker", "worker-1", 32)
            .expect("valid pool config");
        assert_eq!(
            config.application_name(),
            "musteragent-runtime/cloud-worker/worker-1"
        );
        assert_eq!(config.max_connections(), 32);
    }

    #[test]
    fn application_name_is_stable_unique_and_within_postgres_limit() {
        let first = PostgresPoolConfig::standard(
            "cloud-worker",
            "f12d-compression-release-worker-with-a-production-length-identity",
            32,
        )
        .expect("valid long pool identity");
        let repeated = PostgresPoolConfig::standard(
            "cloud-worker",
            "f12d-compression-release-worker-with-a-production-length-identity",
            32,
        )
        .expect("same valid long pool identity");
        let second = PostgresPoolConfig::standard(
            "cloud-worker",
            "f12d-compression-release-worker-with-a-production-length-identitx",
            32,
        )
        .expect("different valid long pool identity");

        assert_eq!(first.application_name(), repeated.application_name());
        assert_ne!(first.application_name(), second.application_name());
        assert!(first.application_name().is_ascii());
        assert_eq!(
            first.application_name().len(),
            POSTGRES_APPLICATION_NAME_MAX_BYTES
        );
        assert_eq!(first.application_name().matches('~').count(), 1);
    }

    #[test]
    fn application_name_truncation_preserves_utf8_boundaries() {
        let config = PostgresPoolConfig::standard(
            "cloud-worker",
            "长文档无感续写工作实例-长文档无感续写工作实例",
            32,
        )
        .expect("valid Unicode pool identity");

        assert!(config.application_name().len() <= POSTGRES_APPLICATION_NAME_MAX_BYTES);
        assert!(config.application_name().contains('~'));
        assert!(
            config
                .application_name()
                .is_char_boundary(config.application_name().len())
        );
    }

    #[test]
    fn invalid_timeout_order_is_rejected_before_connect() {
        let result = PostgresPoolConfig::new(
            "cloud-api",
            "api-1",
            1,
            32,
            PostgresTimeouts {
                lock: Duration::from_secs(31),
                ..PostgresTimeouts::default()
            },
        );
        assert_eq!(result, Err(PostgresPoolConfigError::InvalidTimeoutOrder));
    }

    #[test]
    fn empty_or_control_character_identity_is_rejected() {
        assert!(matches!(
            PostgresPoolConfig::standard("", "api-1", 32),
            Err(PostgresPoolConfigError::InvalidIdentity("service"))
        ));
        assert!(matches!(
            PostgresPoolConfig::standard("cloud-api", "api\n1", 32),
            Err(PostgresPoolConfigError::InvalidIdentity("instance"))
        ));
    }
}
