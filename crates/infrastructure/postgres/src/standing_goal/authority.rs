//! Immutable source-Run and Execution Grant proof verification for standing Goals.

use musteragent_execution_grant::EncodedExecutionGrantProof;
use musteragent_identity::{ConversationId, RunId, TenantId};
use sqlx::postgres::PgRow;

use super::PostgresStandingGoalError;

pub(super) async fn load_execution_grant_proof_tx(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<EncodedExecutionGrantProof, PostgresStandingGoalError> {
    let row = sqlx::query(
        "SELECT snapshot.execution_grant_proof_schema,
                snapshot.execution_grant_proof,
                snapshot.execution_grant_proof_hash,
                snapshot.execution_grant_claims_hash
         FROM runtime.runs AS run
         JOIN capability.runtime_snapshots AS snapshot
           ON snapshot.tenant_id = run.tenant_id
          AND snapshot.conversation_id = run.conversation_id
          AND snapshot.run_id = run.run_id
          AND snapshot.runtime_snapshot_id = run.runtime_snapshot_id
         WHERE run.tenant_id = $1 AND run.run_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresStandingGoalError::Corrupt)?;
    execution_grant_proof_from_row(&row)?.ok_or(PostgresStandingGoalError::Corrupt)
}

pub(super) async fn assert_source_run(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    run_id: RunId,
) -> Result<(), PostgresStandingGoalError> {
    let bound_conversation: uuid::Uuid = sqlx::query_scalar(
        "SELECT conversation_id FROM runtime.runs
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresStandingGoalError::Corrupt)?;
    if bound_conversation != conversation_id.into_uuid() {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    // Freeze only a Run whose immutable Runtime Snapshot already carries the
    // authenticated execution grant used for its admission.
    let _ = load_execution_grant_proof_tx(transaction, tenant_id, run_id).await?;
    Ok(())
}

fn execution_grant_proof_from_row(
    row: &PgRow,
) -> Result<Option<EncodedExecutionGrantProof>, PostgresStandingGoalError> {
    crate::execution_grant_proof::from_row(row).map_err(|error| match error {
        crate::execution_grant_proof::StoredGrantProofError::Database(error) => error.into(),
        crate::execution_grant_proof::StoredGrantProofError::Corrupt => {
            PostgresStandingGoalError::corrupt("execution_grant_proof")
        }
    })
}
