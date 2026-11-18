//! Original Resource receipts keep historical identity and unrelated fields.
//! Only captured fields with exact Task producer provenance are redacted.
use super::*;

pub(super) async fn collect(
    tx: &mut sqlx::PgConnection,
    s: &WorkItemProviderScope,
) -> Result<Vec<uuid::Uuid>, Error> {
    let ids:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT c.change_set_id FROM resource.change_sets c
        WHERE c.tenant_id=$1 AND (
          privacy_erasure.resource_snapshot_has_run(c.result_node_snapshot,$1,$2)
          OR EXISTS(SELECT 1 FROM resource.change_set_mutations m WHERE m.change_set_id=c.change_set_id
            AND privacy_erasure.resource_snapshot_has_run(m.result_node_snapshot,$1,$2)))
        ORDER BY c.change_set_id LIMIT 65537 FOR UPDATE")
        .bind(s.tenant()).bind(s.runs()).fetch_all(&mut *tx).await?;
    if ids.len() > 65536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(ids)
}

pub(super) async fn erase(
    tx: &mut sqlx::PgConnection,
    ids: &[uuid::Uuid],
    job: ErasureJobId,
) -> Result<(), Error> {
    for table in ["change_sets", "change_set_mutations"] {
        sqlx::query(&format!(
            "UPDATE resource.{table} SET result_node_snapshot=
            privacy_erasure.redact_resource_snapshot(result_node_snapshot,$2)
            WHERE change_set_id=ANY($1) AND result_node_snapshot IS NOT NULL"
        ))
        .bind(ids)
        .bind(job.into_uuid())
        .execute(&mut *tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn verify(
    tx: &mut sqlx::PgConnection,
    ids: &[uuid::Uuid],
    job: ErasureJobId,
) -> Result<(), Error> {
    for table in ["change_sets", "change_set_mutations"] {
        let remaining: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM resource.{table}
            WHERE change_set_id=ANY($1) AND result_node_snapshot IS DISTINCT FROM
              privacy_erasure.redact_resource_snapshot(result_node_snapshot,$2))"
        ))
        .bind(ids)
        .bind(job.into_uuid())
        .fetch_one(&mut *tx)
        .await?;
        if remaining {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    Ok(())
}
