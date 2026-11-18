//! Memory output provenance is distinct from a memory read/reference. Reading
//! an existing preference during a Task never authorizes deleting that memory.
use super::*;
use crate::PostgresPrivacyErasureError as Error;
use crate::privacy_erasure::WorkItemProviderScope;

pub(crate) struct WorkItemMemoryClosure {
    pub memory_ids: Vec<uuid::Uuid>,
    pub key_ids: Vec<uuid::Uuid>,
    pub version_ids: Vec<uuid::Uuid>,
    pub version_contents: Vec<ContentRef>,
}
pub(crate) async fn collect_work_item_memories(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
) -> Result<WorkItemMemoryClosure, Error> {
    // Explicit approval receipts and normalized source provenance are actual
    // authorship. memory_refs and context selections are deliberately absent.
    let versions:Vec<uuid::Uuid>=sqlx::query_scalar(
        "SELECT DISTINCT v.memory_version_id FROM memory.memories m JOIN memory.memory_versions v USING(memory_id)
         LEFT JOIN memory.memory_version_source_runs s USING(memory_version_id)
         LEFT JOIN memory.explicit_publications p USING(memory_version_id,memory_id)
         LEFT JOIN tool.tool_invocations i ON i.invocation_id=p.tool_invocation_id AND i.tenant_id=m.tenant_id
         WHERE m.tenant_id=$1 AND (s.run_id=ANY($2) OR i.run_id=ANY($2)) ORDER BY v.memory_version_id LIMIT 65537",
    ).bind(scope.tenant()).bind(scope.runs()).fetch_all(&mut *tx).await?;
    if versions.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let _:Vec<uuid::Uuid>=sqlx::query_scalar(
        "SELECT memory_id FROM memory.memories WHERE tenant_id=$1 AND memory_id IN (SELECT memory_id FROM memory.memory_versions WHERE memory_version_id=ANY($2)) ORDER BY memory_id FOR UPDATE",
    ).bind(scope.tenant()).bind(&versions).fetch_all(&mut *tx).await?;
    // Each selected version must have exclusive Task producer evidence. Other
    // versions of the same memory are outside this erase operation.
    let mixed:bool=sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM memory.memory_versions v WHERE v.memory_version_id=ANY($1) AND (
          NOT(EXISTS(SELECT 1 FROM memory.memory_version_source_runs s WHERE s.memory_version_id=v.memory_version_id AND s.run_id=ANY($2))
            OR EXISTS(SELECT 1 FROM memory.explicit_publications p JOIN tool.tool_invocations i ON i.invocation_id=p.tool_invocation_id
              WHERE p.memory_version_id=v.memory_version_id AND p.tenant_id=$3 AND i.tenant_id=$3 AND i.run_id=ANY($2)))
          OR EXISTS(SELECT 1 FROM memory.memory_version_source_runs s WHERE s.memory_version_id=v.memory_version_id AND NOT(s.run_id=ANY($2)))
          OR EXISTS(SELECT 1 FROM memory.explicit_publications p JOIN tool.tool_invocations i ON i.invocation_id=p.tool_invocation_id
            WHERE p.memory_version_id=v.memory_version_id AND (i.tenant_id<>$3 OR NOT(i.run_id=ANY($2))))
          OR EXISTS(SELECT 1 FROM memory.memory_version_evidence_records e JOIN event_store.ledger_records r ON r.record_id=e.ledger_record_id
            WHERE e.memory_version_id=v.memory_version_id AND (r.run_id IS NULL OR NOT(r.run_id=ANY($2))))))",
    ).bind(&versions).bind(scope.runs()).bind(scope.tenant()).fetch_one(&mut *tx).await?;
    if mixed {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let memories: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT m.memory_id FROM memory.memories m WHERE m.tenant_id=$1
         AND EXISTS(SELECT 1 FROM memory.memory_versions v WHERE v.memory_id=m.memory_id AND v.memory_version_id=ANY($2))
         AND NOT EXISTS(SELECT 1 FROM memory.memory_versions v WHERE v.memory_id=m.memory_id AND NOT(v.memory_version_id=ANY($2)))
         ORDER BY m.memory_id",
    ).bind(scope.tenant()).bind(&versions).fetch_all(&mut *tx).await?;
    let key_ids: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT memory_id FROM memory.memory_versions
        WHERE memory_version_id=ANY($1) AND version=1 ORDER BY memory_id",
    )
    .bind(&versions)
    .fetch_all(&mut *tx)
    .await?;
    // This version closure intentionally excludes a user's unrelated edit
    // draft and proposals for another version of the same Memory aggregate.
    let contents = crate::privacy_erasure_admission::memory_version_content_closures(
        tx,
        TenantId::from_uuid(scope.tenant()).map_err(|_| Error::CorruptState("tenant"))?,
        &versions,
    )
    .await?;
    if contents.len() > 65_536 {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(WorkItemMemoryClosure {
        memory_ids: memories,
        key_ids,
        version_ids: versions,
        version_contents: contents,
    })
}

/// Version tombstones never roll the head back to an older preference.
pub(crate) async fn erase_work_item_memories(
    tx: &mut sqlx::PgConnection,
    scope: &WorkItemProviderScope,
    closure: &WorkItemMemoryClosure,
    job: musteragent_identity::ErasureJobId,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE memory.memory_versions SET work_item_erasure_job_id=$2
        WHERE memory_version_id=ANY($1) AND work_item_erasure_job_id IS NULL",
    )
    .bind(&closure.version_ids)
    .bind(job.into_uuid())
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE memory.memories SET memory_key='erased:'||memory_id::text,
        key_erasure_job_id=$3,updated_at_ms=GREATEST(updated_at_ms,$4)
        WHERE tenant_id=$1 AND memory_id=ANY($2) AND key_erasure_job_id IS NULL",
    )
    .bind(scope.tenant())
    .bind(&closure.key_ids)
    .bind(job.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE memory.memories SET lifecycle=3,revoked_at_ms=COALESCE(revoked_at_ms,$4),
        updated_at_ms=GREATEST(updated_at_ms,$4),memory_key='erased:'||memory_id::text,work_item_erasure_job_id=$3
        WHERE tenant_id=$1 AND memory_id=ANY($2) AND work_item_erasure_job_id IS NULL")
        .bind(scope.tenant()).bind(&closure.memory_ids).bind(job.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?;
    let _:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT memory_version_id FROM memory.memory_versions WHERE memory_version_id=ANY($1) ORDER BY memory_version_id FOR UPDATE")
        .bind(&closure.version_ids).fetch_all(&mut *tx).await?;
    sqlx::query("INSERT INTO memory.search_erasure_tombstones(tenant_id,erasure_job_id,memory_version_id,memory_id,source_content_hash,erased_at_ms)
        SELECT m.tenant_id,$3,v.memory_version_id,m.memory_id,v.content_hash,$4 FROM memory.memories m
        JOIN memory.memory_versions v USING(memory_id) WHERE m.tenant_id=$1 AND v.memory_version_id=ANY($2) ON CONFLICT DO NOTHING")
        .bind(scope.tenant()).bind(&closure.version_ids).bind(job.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?;
    for table in [
        "search_backfill_jobs",
        "search_index",
        "embedding_backfill_jobs",
    ] {
        let sql = format!(
            "DELETE FROM memory.{table} p USING memory.search_erasure_tombstones t WHERE t.tenant_id=$1 AND t.erasure_job_id=$2 AND p.memory_version_id=t.memory_version_id"
        );
        sqlx::query(&sql)
            .bind(scope.tenant())
            .bind(job.into_uuid())
            .execute(&mut *tx)
            .await?;
    }
    let embeddings: bool =
        sqlx::query_scalar("SELECT to_regclass('memory.embedding_index') IS NOT NULL")
            .fetch_one(&mut *tx)
            .await?;
    if embeddings {
        sqlx::query("DELETE FROM memory.embedding_index p USING memory.search_erasure_tombstones t WHERE t.tenant_id=$1 AND t.erasure_job_id=$2 AND p.memory_version_id=t.memory_version_id")
            .bind(scope.tenant()).bind(job.into_uuid()).execute(&mut *tx).await?;
    }
    Ok(())
}
