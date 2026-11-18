use musteragent_identity::{InvocationId, TenantId};
use sqlx::PgPool;
use thiserror::Error;

use crate::{PostgresSecurityAuditError, PostgresSecurityAuditPipeline};

pub(crate) async fn append_succeeded_in_transaction(
    audit: &PostgresSecurityAuditPipeline,
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<(), AtomicToolSecurityAuditError> {
    audit
        .append_tool_fact_in_transaction(transaction, tenant_id, invocation_id)
        .await?;
    Ok(())
}

/// Verifies an already committed lane by replaying the exact persisted Audit
/// fingerprint. Initial business success never uses this standalone path.
pub(crate) async fn replay_succeeded(
    audit: &PostgresSecurityAuditPipeline,
    pool: &PgPool,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<(), AtomicToolSecurityAuditError> {
    let terminal: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM tool.tool_invocations
             WHERE tenant_id = $1 AND invocation_id = $2 AND status = 4
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_one(pool)
    .await?;
    if !terminal {
        return Err(AtomicToolSecurityAuditError::MissingTerminal);
    }
    audit.write_tool_fact(tenant_id, invocation_id).await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum AtomicToolSecurityAuditError {
    #[error("committed Tool terminal authority is missing")]
    MissingTerminal,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Audit(#[from] PostgresSecurityAuditError),
}
