use super::*;

/// Verify the complete, frozen online provider coverage. Absence proofs are
/// valid only for this admitted unexecuted-goals schema, never an active task.
pub(crate) async fn verify_work_item_receipt(
    tx: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
    job: &ErasureJob,
) -> Result<(), Error> {
    let lineage = load_lineage(tx, receipt.tenant_id, receipt.erasure_job_id)
        .await?
        .ok_or(Error::WorkItemExecutionClosureRequired)?;
    let plan = job.latest_plan()?;
    receipt.validate_against(receipt.tenant_id, plan)?;
    if job.request.schema_version.as_str() == WORK_ITEM_EXECUTION_ERASURE_SCHEMA {
        return super::execution_receipt::verify(tx, receipt, job, &lineage).await;
    }
    if job.request.schema_version.as_str() != WORK_ITEM_ERASURE_SCHEMA
        || receipt.schema_version.as_str() != WORK_ITEM_ERASURE_SCHEMA
        || plan.lineage_fingerprint != lineage.fingerprint()
        || job.request.root_resource_refs
            != lineage
                .goal_content_refs
                .iter()
                .cloned()
                .map(musteragent_runtime_contract::ResourceRef::Content)
                .collect::<Vec<_>>()
        || receipt.proof_digest != erasure_proof_digest(job, receipt.completed_at)
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let frozen:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE t.work_item_id=$1 AND t.tenant_id=$2 AND t.owner_id=$3 AND t.space_id=$4 AND t.agent_id=$5 AND t.erasure_job_id=$6 AND t.state_revision=$7 AND r.control_revision=$8)")
        .bind(lineage.work_item_id.into_uuid()).bind(lineage.actor.tenant_id.into_uuid()).bind(lineage.actor.principal_id.into_uuid())
        .bind(lineage.space_id.into_uuid()).bind(lineage.agent_id.into_uuid()).bind(receipt.erasure_job_id.into_uuid())
        .bind(super::super::to_i64(lineage.state_revision+1,"state")?).bind(super::super::to_i64(lineage.control_revision+1,"control")?)
        .fetch_one(&mut *tx).await?;
    let revisions:(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE status<>'queued' OR assignment_epoch IS NOT NULL) FROM collaboration.work_item_revisions WHERE work_item_id=$1")
        .bind(lineage.work_item_id.into_uuid()).fetch_one(&mut *tx).await?;
    if !frozen || revisions.0 as u64 != lineage.revision_count + 1 || revisions.1 != 0 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    eligibility::require_exclusive_roots(
        tx,
        lineage.actor,
        lineage.agent_id,
        lineage.work_item_id,
        &lineage.goal_content_refs,
    )
    .await?;
    // Pins are retention bookkeeping, not proof of deletion. Remove only this
    // task's goals after the caller's ContentCiphertext provider check succeeds.
    sqlx::query("DELETE FROM agent.dialogue_content_references WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND reference_kind='work_item_revision' AND content_id=ANY($4)")
        .bind(lineage.actor.tenant_id.into_uuid()).bind(lineage.actor.principal_id.into_uuid()).bind(lineage.agent_id.into_uuid())
        .bind(lineage.goal_content_refs.iter().map(|r|r.content_id.into_uuid()).collect::<Vec<_>>()).execute(&mut *tx).await?;
    Ok(())
}
