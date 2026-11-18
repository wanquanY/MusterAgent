//! Artifact owns output provenance. A dependency is an input reference until
//! its own producer proves otherwise; it is never implicitly an erasure root.
use super::*;
use crate::PostgresPrivacyErasureError as Error;
use crate::privacy_erasure::WorkItemProviderScope;

pub(crate) struct WorkItemArtifactClosure {
    pub artifact_ids: Vec<uuid::Uuid>,
    pub version_ids: Vec<uuid::Uuid>,
    pub title_ids: Vec<uuid::Uuid>,
    pub version_contents: Vec<ContentRef>,
}

pub(crate) async fn collect_work_item_artifacts(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<WorkItemArtifactClosure, Error> {
    // The version producer owns the output, even when the aggregate was
    // originally authored by the user. Never erase unrelated older versions.
    let mut versions: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT v.artifact_version_id FROM artifact.artifact_versions v
         JOIN artifact.artifacts a USING(artifact_id)
         JOIN tool.tool_invocations i ON i.invocation_id=v.source_invocation_id AND i.tenant_id=a.tenant_id
         WHERE a.tenant_id=$1 AND i.run_id=ANY($2) ORDER BY v.artifact_version_id LIMIT 65537",
    ).bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    if versions.len() > MAX_CLOSURE {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let staged:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT version_id FROM artifact.work_item_result_asset_candidates WHERE tenant_id=$1 AND task_id=$2 AND producer_run_id=ANY($3) ORDER BY version_id LIMIT 65537")
        .bind(scope.tenant()).bind(scope.task()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    versions.extend(staged.iter().copied());
    versions.sort_unstable();
    versions.dedup();
    if versions.len() > MAX_CLOSURE {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let _: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT a.artifact_id FROM artifact.artifacts a WHERE a.tenant_id=$1 AND EXISTS(
           SELECT 1 FROM artifact.artifact_versions v WHERE v.artifact_id=a.artifact_id
            AND v.artifact_version_id=ANY($2)) ORDER BY a.artifact_id FOR UPDATE",
    )
    .bind(scope.tenant())
    .bind(&versions)
    .fetch_all(&mut *tx)
    .await?;
    let artifacts: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT a.artifact_id FROM artifact.artifacts a WHERE a.tenant_id=$1 AND a.created_by_run_id=ANY($2)
          AND EXISTS(SELECT 1 FROM artifact.artifact_versions v WHERE v.artifact_id=a.artifact_id)
          AND NOT EXISTS(SELECT 1 FROM artifact.artifact_versions v WHERE v.artifact_id=a.artifact_id
            AND NOT(v.artifact_version_id=ANY($3))) ORDER BY a.artifact_id",
    ).bind(scope.tenant()).bind(scope.runs()).bind(&versions).fetch_all(&mut *tx).await?;
    let unknown: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.artifacts a
        WHERE a.tenant_id=$1 AND NOT a.title_source_known AND EXISTS(
          SELECT 1 FROM artifact.artifact_versions v WHERE v.artifact_id=a.artifact_id AND v.artifact_version_id=ANY($2)))")
        .bind(scope.tenant()).bind(&versions).fetch_one(&mut *tx).await?;
    if unknown {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let title_ids: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT a.artifact_id FROM artifact.artifacts a
        JOIN tool.tool_invocations i ON i.invocation_id=a.title_source_invocation_id AND i.tenant_id=a.tenant_id
        WHERE a.tenant_id=$1 AND a.title_source_known AND i.run_id=ANY($2) ORDER BY a.artifact_id")
        .bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    let mut rows=sqlx::query(
        "SELECT DISTINCT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
         FROM artifact.artifact_versions v JOIN artifact.content_refs c
           ON c.content_id=v.content_id AND c.content_hash=v.content_hash AND c.tenant_id=$1
         WHERE v.artifact_version_id=ANY($2) ORDER BY c.content_id LIMIT 65537",
    ).bind(scope.tenant()).bind(&versions).fetch_all(&mut *tx).await?;
    if !staged.is_empty() {
        let candidate_roots=sqlx::query("SELECT DISTINCT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash FROM agent.dialogue_content_references p JOIN artifact.content_refs c ON c.tenant_id=p.tenant_id AND c.content_id=p.content_id WHERE p.tenant_id=$1 AND p.reference_kind='work_item_result_asset' AND p.reference_id=ANY($2) ORDER BY c.content_id LIMIT 65537")
            .bind(scope.tenant()).bind(&staged).fetch_all(&mut *tx).await?;
        rows.extend(candidate_roots);
    }
    if rows.len() > MAX_CLOSURE {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let version_contents = rows
        .iter()
        .map(|r| {
            Ok(ContentRef {
                content_id: musteragent_identity::ContentId::from_uuid(r.try_get("content_id")?)
                    .map_err(|_| Error::CorruptState("artifact content id"))?,
                media_type: MediaType::new(r.try_get::<String, _>("media_type")?)
                    .map_err(|_| Error::CorruptState("artifact media type"))?,
                byte_length: r
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| Error::CorruptState("artifact content length"))?,
                sha256: Sha256::from_bytes(r.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| Error::CorruptState("artifact content hash"))?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let referenced_inputs: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT d.content_id FROM artifact.artifact_version_dependencies d
         JOIN artifact.artifact_versions v USING(artifact_version_id) WHERE v.artifact_version_id=ANY($1)
         ORDER BY d.content_id LIMIT 65537",
    )
    .bind(&versions)
    .fetch_all(&mut *tx)
    .await?;
    if referenced_inputs.len() > MAX_CLOSURE {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(WorkItemArtifactClosure {
        artifact_ids: artifacts,
        version_ids: versions,
        title_ids,
        version_contents,
    })
}
const MAX_CLOSURE: usize = 65_536;

pub(crate) async fn erase_work_item_artifacts(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    closure: &WorkItemArtifactClosure,
    job: musteragent_identity::ErasureJobId,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query("DELETE FROM agent.dialogue_content_references p USING artifact.work_item_result_asset_candidates c WHERE p.tenant_id=$1 AND c.task_id=$2 AND c.tenant_id=p.tenant_id AND c.producer_run_id=ANY($3) AND p.reference_kind='work_item_result_asset' AND p.reference_id=c.version_id")
        .bind(scope.tenant()).bind(scope.task()).bind(scope.runs()).execute(&mut *tx).await?;
    sqlx::query("UPDATE artifact.work_item_result_asset_candidates SET capsule=NULL,root_content_id=NULL,erasure_job_id=$4 WHERE tenant_id=$1 AND task_id=$2 AND producer_run_id=ANY($3) AND erasure_job_id IS NULL")
        .bind(scope.tenant()).bind(scope.task()).bind(scope.runs()).bind(job.into_uuid()).execute(&mut *tx).await?;
    // Keep aggregate/head/history positions. The current title is independently
    // owned by its actual last writer, including for an otherwise owned file.
    sqlx::query(
        "UPDATE artifact.artifact_versions SET work_item_erasure_job_id=$2
        WHERE artifact_version_id=ANY($1) AND work_item_erasure_job_id IS NULL",
    )
    .bind(&closure.version_ids)
    .bind(job.into_uuid())
    .execute(&mut *tx)
    .await?;
    // Original search projection cascades remove chunks/embeddings and their
    // leased jobs. A DB guard rejects any stale worker trying to rebuild them.
    sqlx::query("DELETE FROM resource.search_manifests WHERE artifact_version_id=ANY($1)")
        .bind(&closure.version_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM resource.search_index_jobs WHERE artifact_version_id=ANY($1)")
        .bind(&closure.version_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE artifact.artifacts SET title='erased:'||artifact_id::text,
        title_erasure_job_id=$2,updated_at_ms=GREATEST(updated_at_ms,$3)
        WHERE artifact_id=ANY($1) AND title_erasure_job_id IS NULL",
    )
    .bind(&closure.title_ids)
    .bind(job.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE artifact.artifacts SET lifecycle=3,
        updated_at_ms=GREATEST(updated_at_ms,$4),work_item_erasure_job_id=$3
        WHERE tenant_id=$1 AND artifact_id=ANY($2) AND work_item_erasure_job_id IS NULL",
    )
    .bind(scope.tenant())
    .bind(&closure.artifact_ids)
    .bind(job.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await?;
    Ok(())
}
