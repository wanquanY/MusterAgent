//! A terminal receipt is verified against the original frozen producer cut,
//! after physical content deletion. No count or cancellation ACK substitutes.
use super::*;

pub(super) async fn verify(
    tx: &mut sqlx::PgConnection,
    receipt: &ErasureReceipt,
    job: &ErasureJob,
    lineage: &WorkItemErasureLineage,
) -> Result<(), Error> {
    let scope = super::coverage::provider_scope(tx, lineage).await?;
    let coverage = super::frozen::load(tx, lineage, &scope)
        .await?
        .ok_or(Error::WorkItemExecutionClosureRequired)?;
    if receipt.schema_version.as_str() != WORK_ITEM_EXECUTION_ERASURE_SCHEMA
        || build_work_item_execution_plan(job, &coverage)? != *job.latest_plan()?
        || receipt.proof_digest != erasure_proof_digest(job, receipt.completed_at)
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    super::source_evidence::verify(tx, job, &coverage, receipt.completed_at).await?;
    super::private_payload::verify(tx, &scope, receipt.erasure_job_id).await?;
    let commands = super::source_commands::collect(tx, &scope, Some(&coverage.contents)).await?;
    super::source_commands::verify(tx, &commands, receipt.erasure_job_id).await?;
    for content in &coverage.contents {
        let deleted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs c WHERE c.tenant_id=$1 AND c.content_id=$2
            AND c.content_hash=$3 AND c.media_type=$4 AND c.byte_length=$5::numeric
            AND c.state=3 AND c.storage_ref IS NULL AND c.storage_ref_protected IS NULL
            AND NOT EXISTS(SELECT 1 FROM artifact.inline_content_blobs b WHERE b.content_id=c.content_id
                AND NOT EXISTS(SELECT 1 FROM artifact.content_refs h WHERE h.tenant_id=c.tenant_id AND h.blob_content_id=c.content_id
                    AND (h.state IN (1,2) OR h.storage_ref_protected IS NOT NULL OR h.storage_ref IS NOT NULL))))")
            .bind(scope.tenant()).bind(content.content_id.into_uuid()).bind(content.sha256.into_bytes().to_vec())
            .bind(content.media_type.as_str()).bind(content.byte_length.to_string()).fetch_one(&mut *tx).await?;
        if !deleted {
            return Err(Error::ContentCiphertextNotErased);
        }
    }
    let public = crate::dialogue_publication::work_item_erasure::collect(tx, &scope).await?;
    let context = crate::dialogue_context::work_item_erasure::collect(
        tx,
        &scope,
        &public.entries,
        Some(&coverage.contents),
    )
    .await?;
    crate::dialogue_context::work_item_erasure::verify_model_caches(tx, &context.models).await?;
    let resources = crate::resource::work_item_erasure::collect(tx, &scope).await?;
    crate::resource::work_item_erasure::verify_receipts(tx, &resources, receipt.erasure_job_id)
        .await?;
    let artifacts = crate::artifact::collect_work_item_artifacts(tx, &scope).await?;
    let memories = crate::memory::collect_work_item_memories(tx, &scope).await?;
    for (relation, column, ids, marker) in [
        (
            "agent.agent_dialogue_entries",
            "entry_id",
            &public.entries,
            "erasure_job_id",
        ),
        (
            "agent.dialogue_context_checkpoints",
            "checkpoint_id",
            &context.checkpoints,
            "erasure_job_id",
        ),
        (
            "runtime.dialogue_context_bindings",
            "ledger_record_id",
            &context.bindings,
            "erasure_job_id",
        ),
        (
            "resource.nodes",
            "node_id",
            &resources.nodes,
            "name_erasure_job_id",
        ),
        (
            "resource.nodes",
            "node_id",
            &resources.sort_nodes,
            "sort_key_erasure_job_id",
        ),
        (
            "resource.edit_operations",
            "operation_id",
            &resources.operations,
            "work_item_erasure_job_id",
        ),
        (
            "resource.edit_sessions",
            "session_id",
            &resources.sessions,
            "work_item_erasure_job_id",
        ),
        (
            "artifact.artifacts",
            "artifact_id",
            &artifacts.title_ids,
            "title_erasure_job_id",
        ),
        (
            "artifact.artifact_versions",
            "artifact_version_id",
            &artifacts.version_ids,
            "work_item_erasure_job_id",
        ),
        (
            "memory.memory_versions",
            "memory_version_id",
            &memories.version_ids,
            "work_item_erasure_job_id",
        ),
        (
            "memory.memories",
            "memory_id",
            &memories.key_ids,
            "key_erasure_job_id",
        ),
        (
            "artifact.artifacts",
            "artifact_id",
            &artifacts.artifact_ids,
            "work_item_erasure_job_id",
        ),
        (
            "memory.memories",
            "memory_id",
            &memories.memory_ids,
            "work_item_erasure_job_id",
        ),
    ] {
        let remaining:bool=sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {relation} WHERE {column}=ANY($1) AND {marker} IS DISTINCT FROM $2)"))
            .bind(ids).bind(receipt.erasure_job_id.into_uuid()).fetch_one(&mut *tx).await?;
        if remaining {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
    }
    let search_leak: bool = sqlx::query_scalar(
        "SELECT
        EXISTS(SELECT 1 FROM resource.search_manifests WHERE artifact_version_id=ANY($1))
        OR EXISTS(SELECT 1 FROM resource.search_index_jobs WHERE artifact_version_id=ANY($1))
        OR EXISTS(SELECT 1 FROM memory.search_index WHERE memory_version_id=ANY($2))
        OR EXISTS(SELECT 1 FROM memory.search_backfill_jobs WHERE memory_version_id=ANY($2))
        OR EXISTS(SELECT 1 FROM memory.embedding_backfill_jobs WHERE memory_version_id=ANY($2))",
    )
    .bind(&artifacts.version_ids)
    .bind(&memories.version_ids)
    .fetch_one(&mut *tx)
    .await?;
    if search_leak {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let public_leak:bool=sqlx::query_scalar("SELECT
        EXISTS(SELECT 1 FROM agent.dialogue_history_search_documents WHERE entry_id=ANY($1))
        OR EXISTS(SELECT 1 FROM agent.dialogue_history_search_pending WHERE entry_id=ANY($1))
        OR EXISTS(SELECT 1 FROM agent.dialogue_context_heads WHERE checkpoint_id=ANY($2))
        OR EXISTS(SELECT 1 FROM agent.dialogue_content_references WHERE tenant_id=$3 AND
           ((reference_kind='work_item_result' AND reference_id IN (SELECT result_id FROM collaboration.work_item_result_submissions WHERE tenant_id=$3 AND work_item_id=$5)) OR (reference_kind='public_entry' AND reference_id=ANY($1)) OR
            (reference_kind='work_item_report' AND reference_id=ANY($4))))")
        .bind(&public.entries).bind(&context.checkpoints).bind(scope.tenant()).bind(scope.runs()).bind(scope.task()).fetch_one(&mut *tx).await?;
    if public_leak {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(())
}
