//! Postgres owner of shared endpoint recovery; short transactions only.
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_runtime_model::dependency_recovery::{
    DependencyAdmission, DependencyFuture, DependencyObservation, DependencyPermit,
    DependencyRecoveryError as Error, DependencyRecoveryState as State, ModelDependencyKey,
    ModelDependencyRecoveryStore,
};
use sqlx::Row as _;
use std::time::Duration;

#[derive(Clone)]
pub struct PostgresModelDependencyRecoveryStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresModelDependencyRecoveryStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}

impl ModelDependencyRecoveryStore for PostgresModelDependencyRecoveryStore {
    fn acquire<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        invocation_id: InvocationId,
        probe_lifetime: Duration,
    ) -> DependencyFuture<'a, DependencyAdmission> {
        Box::pin(async move {
            // Healthy endpoints need no exclusive row lock or write. This
            // read is the permit's linearization point; later outage reports
            // fence already-admitted concurrent requests by generation.
            if let Some(row) = bind_key(sqlx::query("SELECT generation,failures,repair_reason,retry_not_before_ms,probe_invocation_id,probe_expires_at_ms
                FROM runtime.model_dependency_recovery WHERE tenant_id=$1 AND provider_id=$2 AND model_entry_id=$3 AND endpoint_hash=$4"),key).fetch_optional(self.database.pool()).await.map_err(sql_error)? {
                let mut state = decode(&row)?;
                if state.failures == 0 {
                    return state.acquire(invocation_id,UtcTimestamp::from_unix_millis(0),probe_lifetime);
                }
            }
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::ModelTerminalUsage)
                .await
                .map_err(transaction_error)?;
            bind_key(sqlx::query("INSERT INTO runtime.model_dependency_recovery (tenant_id,provider_id,model_entry_id,endpoint_hash,
                generation,failures,retry_not_before_ms) VALUES ($1,$2,$3,$4,$5,0,0)
                ON CONFLICT (tenant_id,provider_id,model_entry_id,endpoint_hash) DO NOTHING"),key)
                .bind(InvocationId::generate().into_uuid()).execute(&mut *tx).await.map_err(sql_error)?;
            let row = bind_key(sqlx::query("SELECT generation,failures,repair_reason,retry_not_before_ms,probe_invocation_id,probe_expires_at_ms
                FROM runtime.model_dependency_recovery WHERE tenant_id=$1 AND provider_id=$2 AND model_entry_id=$3 AND endpoint_hash=$4 FOR UPDATE"),key)
                .fetch_optional(&mut *tx).await.map_err(sql_error)?.ok_or(Error::InvalidState)?;
            let mut state = decode(&row)?;
            let before = state.clone();
            let now = UtcTimestamp::from_unix_millis(
                sqlx::query_scalar::<_, i64>(
                    "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
                )
                .fetch_one(&mut *tx)
                .await
                .map_err(sql_error)?,
            );
            let admission = state.acquire(invocation_id, now, probe_lifetime)?;
            if state != before {
                write_state(&mut tx, key, &state).await?;
            }
            tx.commit().await.map_err(transaction_error)?;
            Ok(admission)
        })
    }
    fn observe<'a>(
        &'a self,
        key: &'a ModelDependencyKey,
        permit: DependencyPermit,
        observation: DependencyObservation,
    ) -> DependencyFuture<'a, ()> {
        Box::pin(async move {
            let mut tx = self
                .database
                .write_unit_of_work(musteragent_telemetry::DbCommitKind::ModelTerminalUsage)
                .await
                .map_err(transaction_error)?;
            if let Some(row) = bind_key(sqlx::query("SELECT generation,failures,repair_reason,retry_not_before_ms,probe_invocation_id,probe_expires_at_ms
                FROM runtime.model_dependency_recovery WHERE tenant_id=$1 AND provider_id=$2 AND model_entry_id=$3 AND endpoint_hash=$4 FOR UPDATE"),key)
                .fetch_optional(&mut *tx).await.map_err(sql_error)? {
                let mut state = decode(&row)?;
                let now = UtcTimestamp::from_unix_millis(sqlx::query_scalar::<_, i64>("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint").fetch_one(&mut *tx).await.map_err(sql_error)?);
                if state.observe(permit,observation,now)? { write_state(&mut tx,key,&state).await?; }
            }
            tx.commit().await.map_err(transaction_error)?;
            Ok(())
        })
    }
}
fn bind_key<'a>(
    query: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    key: &ModelDependencyKey,
) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(key.tenant_id.into_uuid())
        .bind(key.provider_id.into_uuid())
        .bind(
            key.model_entry_id
                .map(|entry| entry.into_uuid())
                .unwrap_or_default(),
        )
        .bind(key.endpoint_hash.into_bytes().to_vec())
}
async fn write_state(
    tx: &mut crate::GovernedPostgresTransaction<'_>,
    key: &ModelDependencyKey,
    state: &State,
) -> Result<(), Error> {
    let changed = bind_key(sqlx::query("UPDATE runtime.model_dependency_recovery SET generation=$5,failures=$6,retry_not_before_ms=$7,probe_invocation_id=$8,probe_expires_at_ms=$9,repair_reason=$10 WHERE tenant_id=$1 AND provider_id=$2 AND model_entry_id=$3 AND endpoint_hash=$4"),key)
        .bind(state.generation.into_uuid()).bind(i64::from(state.failures)).bind(state.retry_not_before.unix_millis())
        .bind(state.probe_invocation.map(|id| id.into_uuid())).bind(state.probe_expires_at.map(|time| time.unix_millis()))
        .bind(state.repair.map(|reason| reason.as_str()))
        .execute(&mut **tx).await.map_err(sql_error)?;
    if changed.rows_affected() != 1 {
        return Err(Error::InvalidState);
    }
    Ok(())
}
fn decode(row: &sqlx::postgres::PgRow) -> Result<State, Error> {
    let state = State {
        repair: row
            .try_get::<Option<String>, _>("repair_reason")
            .map_err(sql_error)?
            .as_deref()
            .map(musteragent_runtime_model::dependency_recovery::DependencyRepair::decode)
            .transpose()?,
        generation: InvocationId::from_uuid(
            row.try_get::<uuid::Uuid, _>("generation")
                .map_err(sql_error)?,
        )
        .map_err(|_| Error::InvalidState)?,
        failures: u32::try_from(row.try_get::<i64, _>("failures").map_err(sql_error)?)
            .map_err(|_| Error::InvalidState)?,
        retry_not_before: UtcTimestamp::from_unix_millis(
            row.try_get("retry_not_before_ms").map_err(sql_error)?,
        ),
        probe_invocation: row
            .try_get::<Option<uuid::Uuid>, _>("probe_invocation_id")
            .map_err(sql_error)?
            .map(InvocationId::from_uuid)
            .transpose()
            .map_err(|_| Error::InvalidState)?,
        probe_expires_at: row
            .try_get::<Option<i64>, _>("probe_expires_at_ms")
            .map_err(sql_error)?
            .map(UtcTimestamp::from_unix_millis),
    };
    state.validate()?;
    Ok(state)
}
fn transaction_error(error: crate::PostgresTransactionError) -> Error {
    use crate::PostgresFailureClass as Class;
    match error.failure_class() {
        Class::Constraint | Class::Other => Error::InvalidState,
        _ => Error::Unavailable,
    }
}
fn sql_error(error: sqlx::Error) -> Error {
    transaction_error(crate::PostgresTransactionError::Database(error))
}
