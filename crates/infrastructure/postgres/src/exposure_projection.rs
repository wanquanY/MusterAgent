use musteragent_discovery::{
    AuthorizedToolCatalog, DiscoveryEvent, ExposureState, ToolExposureItem,
};
use musteragent_identity::{
    ContentId, InvocationId, LedgerRecordId, RunId, TenantId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::DomainEvent;
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_tool::ToolKey;
use sqlx::Row;

use crate::run_ledger::{RunLedgerReadError, load_run_domain_events_in};

#[derive(Debug, Clone, Eq, PartialEq)]
struct ExposureFact {
    item: ToolExposureItem,
    source_invocation_id: InvocationId,
    reason_content_id: ContentId,
    exposed_at: UtcTimestamp,
    record_id: LedgerRecordId,
}

/// Reconciles the denormalized Tool-exposure projection against the canonical
/// Ledger before it can influence a PolicyHook decision.
pub(crate) async fn load_verified_exposure_projection(
    database: &crate::PostgresDatabaseRuntime,
    decoder: &dyn LedgerPayloadDecoder,
    tenant_id: TenantId,
    run_id: RunId,
    catalog: &AuthorizedToolCatalog,
    initial_exposed: &[ToolKey],
) -> Result<ExposureState, ExposureProjectionError> {
    let mut transaction = database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Projection,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await?;
    let canonical = load_run_domain_events_in(&mut transaction, decoder, tenant_id, run_id).await?;
    let mut ledger_state = ExposureState::from_initial(catalog, initial_exposed)
        .map_err(|_| ExposureProjectionError::Corrupt)?;
    let mut expected = Vec::new();
    for record in canonical {
        let DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(event)) = record.event else {
            continue;
        };
        ledger_state
            .apply(catalog, &event)
            .map_err(|_| ExposureProjectionError::Corrupt)?;
        expected.extend(event.exposures.into_iter().map(|item| ExposureFact {
            item,
            source_invocation_id: event.source_invocation_id,
            reason_content_id: event.reason_ref.content_id,
            exposed_at: event.exposed_at,
            record_id: record.record_id,
        }));
    }

    let rows = sqlx::query(
        "SELECT tool_key, schema_hash, ordinal, source_invocation_id,
                reason_content_id, exposed_at_ms, record_id
         FROM runtime.tool_exposures
         WHERE tenant_id = $1 AND run_id = $2
         ORDER BY ordinal",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await?;
    let actual = rows
        .into_iter()
        .map(decode_projection_fact)
        .collect::<Result<Vec<_>, _>>()?;
    transaction.commit().await?;
    if actual != expected {
        return Err(ExposureProjectionError::Corrupt);
    }
    let projected = actual.into_iter().map(|fact| fact.item).collect::<Vec<_>>();
    let projection_state = ExposureState::from_projection(catalog, initial_exposed, &projected)
        .map_err(|_| ExposureProjectionError::Corrupt)?;
    if projection_state != ledger_state {
        return Err(ExposureProjectionError::Corrupt);
    }
    Ok(ledger_state)
}

fn decode_projection_fact(
    row: sqlx::postgres::PgRow,
) -> Result<ExposureFact, ExposureProjectionError> {
    let ordinal = u32::try_from(row.try_get::<i32, _>("ordinal")?)
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .ok_or(ExposureProjectionError::Corrupt)?;
    Ok(ExposureFact {
        item: ToolExposureItem {
            tool_ref: ToolKey::from_versioned_key(
                VersionedKey::new(row.try_get::<String, _>("tool_key")?)
                    .map_err(|_| ExposureProjectionError::Corrupt)?,
            ),
            schema_hash: musteragent_content_ref::Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("schema_hash")?,
            )
            .map_err(|_| ExposureProjectionError::Corrupt)?,
            ordinal,
        },
        source_invocation_id: InvocationId::from_uuid(row.try_get("source_invocation_id")?)
            .map_err(|_| ExposureProjectionError::Corrupt)?,
        reason_content_id: ContentId::from_uuid(row.try_get("reason_content_id")?)
            .map_err(|_| ExposureProjectionError::Corrupt)?,
        exposed_at: UtcTimestamp::from_unix_millis(row.try_get("exposed_at_ms")?),
        record_id: LedgerRecordId::from_uuid(row.try_get("record_id")?)
            .map_err(|_| ExposureProjectionError::Corrupt)?,
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExposureProjectionError {
    #[error("Tool-exposure projection does not match the canonical Ledger")]
    Corrupt,
    #[error("Tool-exposure projection query failed")]
    Database(#[from] sqlx::Error),
    #[error("governed Tool-exposure projection Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
}

impl From<RunLedgerReadError> for ExposureProjectionError {
    fn from(error: RunLedgerReadError) -> Self {
        match error {
            RunLedgerReadError::Corrupt => Self::Corrupt,
            RunLedgerReadError::Database(error) => Self::Database(error),
        }
    }
}
