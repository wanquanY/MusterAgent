//! The Resource Owner contributes exact authored names and edit-operation
//! outputs. File base versions and another editor's work are shared inputs.
use crate::PostgresPrivacyErasureError as Error;
use crate::privacy_erasure::WorkItemProviderScope;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, ErasureJobId, UtcTimestamp};
use sqlx::Row;
mod receipts;

pub(crate) struct Closure {
    pub nodes: Vec<uuid::Uuid>,
    pub sort_nodes: Vec<uuid::Uuid>,
    pub operations: Vec<uuid::Uuid>,
    pub sessions: Vec<uuid::Uuid>,
    pub receipts: Vec<uuid::Uuid>,
    pub contents: Vec<ContentRef>,
}
pub(crate) async fn collect(
    tx: &mut sqlx::PgConnection,
    s: &WorkItemProviderScope,
) -> Result<Closure, Error> {
    // Legacy mutable names have no reliable last-writer evidence. A Task that
    // created/touched the file must not be certified erased by guessing that
    // its current name belongs to another actor.
    let unknown:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.nodes n
        WHERE n.tenant_id=$1 AND NOT n.name_source_known AND (
          EXISTS(SELECT 1 FROM tool.tool_invocations i WHERE i.invocation_id=n.source_invocation_id AND i.tenant_id=$1 AND i.run_id=ANY($2))
          OR EXISTS(SELECT 1 FROM artifact.artifact_versions v JOIN tool.tool_invocations i ON i.invocation_id=v.source_invocation_id
            WHERE v.artifact_id=n.file_artifact_id AND i.tenant_id=$1 AND i.run_id=ANY($2))))")
        .bind(s.tenant()).bind(s.runs()).fetch_one(&mut *tx).await?;
    if unknown {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let nodes:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT n.node_id FROM resource.nodes n
        JOIN tool.tool_invocations i ON i.invocation_id=n.name_source_invocation_id AND i.tenant_id=n.tenant_id
        WHERE n.tenant_id=$1 AND n.name_source_known AND i.run_id=ANY($2) ORDER BY n.node_id")
        .bind(s.tenant()).bind(s.runs()).fetch_all(&mut *tx).await?;
    // sort_key has no mutable writer: it is an immutable creation field that
    // commonly contains the original filename, even after a user renames it.
    let sort_nodes:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT n.node_id FROM resource.nodes n
        JOIN tool.tool_invocations i ON i.invocation_id=n.source_invocation_id AND i.tenant_id=n.tenant_id
        WHERE n.tenant_id=$1 AND i.run_id=ANY($2) ORDER BY n.node_id")
        .bind(s.tenant()).bind(s.runs()).fetch_all(&mut *tx).await?;
    let mut affected = nodes.clone();
    affected.extend(&sort_nodes);
    affected.sort_unstable();
    affected.dedup();
    // Same lock order as ordinary Resource mutation: scope, node, edit session.
    let _:Vec<i64>=sqlx::query_scalar("SELECT h.revision FROM resource.scope_heads h WHERE h.tenant_id=$1
        AND EXISTS(SELECT 1 FROM resource.nodes n WHERE n.tenant_id=h.tenant_id AND n.scope_kind=h.scope_kind AND n.scope_id=h.scope_id
          AND (n.node_id=ANY($2) OR EXISTS(SELECT 1 FROM resource.edit_sessions e JOIN resource.edit_operations o USING(session_id)
            JOIN tool.tool_invocations i ON i.invocation_id=o.source_invocation_id AND i.tenant_id=e.tenant_id
            WHERE e.file_node_id=n.node_id AND i.run_id=ANY($3)))) ORDER BY h.scope_kind,h.scope_id FOR UPDATE")
        .bind(s.tenant()).bind(&affected).bind(s.runs()).fetch_all(&mut *tx).await?;
    let nodes:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT n.node_id FROM resource.nodes n
        JOIN tool.tool_invocations i ON i.invocation_id=n.name_source_invocation_id AND i.tenant_id=n.tenant_id
        WHERE n.node_id=ANY($1) AND n.name_source_known AND i.run_id=ANY($2) ORDER BY n.node_id FOR UPDATE OF n")
        .bind(&nodes).bind(s.runs()).fetch_all(&mut *tx).await?;
    let operations:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT o.operation_id FROM resource.edit_operations o
        JOIN tool.tool_invocations i ON i.invocation_id=o.source_invocation_id AND i.tenant_id=o.tenant_id
        WHERE o.tenant_id=$1 AND i.run_id=ANY($2) ORDER BY o.operation_id")
        .bind(s.tenant()).bind(s.runs()).fetch_all(&mut *tx).await?;
    let sessions:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT e.session_id FROM resource.edit_sessions e WHERE e.tenant_id=$1
        AND EXISTS(SELECT 1 FROM resource.edit_operations o WHERE o.session_id=e.session_id AND o.operation_id=ANY($2)
          AND o.checkpoint_content_id=e.checkpoint_content_id AND o.checkpoint_hash=e.checkpoint_hash)
        ORDER BY e.session_id FOR UPDATE")
        .bind(s.tenant()).bind(&operations).fetch_all(&mut *tx).await?;
    if nodes.len() > 65536
        || sort_nodes.len() > 65536
        || operations.len() > 65536
        || sessions.len() > 65536
    {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let rows=sqlx::query("WITH refs(id,hash) AS (
        SELECT operation_content_id,operation_hash FROM resource.edit_operations WHERE operation_id=ANY($2)
        UNION SELECT checkpoint_content_id,checkpoint_hash FROM resource.edit_operations WHERE operation_id=ANY($2)
      ) SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash FROM refs r
      JOIN artifact.content_refs c ON c.content_id=r.id AND c.content_hash=r.hash WHERE c.tenant_id=$1 ORDER BY c.content_id")
        .bind(s.tenant()).bind(&operations).fetch_all(&mut *tx).await?;
    let contents = rows
        .iter()
        .map(|r| {
            Ok(ContentRef {
                content_id: ContentId::from_uuid(r.try_get("content_id")?)
                    .map_err(|_| Error::CorruptState("edit content id"))?,
                media_type: MediaType::new(r.try_get::<String, _>("media_type")?)
                    .map_err(|_| Error::CorruptState("edit media type"))?,
                byte_length: r
                    .try_get::<String, _>("byte_length")?
                    .parse()
                    .map_err(|_| Error::CorruptState("edit byte length"))?,
                sha256: Sha256::from_bytes(r.try_get::<Vec<u8>, _>("content_hash")?)
                    .map_err(|_| Error::CorruptState("edit content hash"))?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let receipts = receipts::collect(tx, s).await?;
    Ok(Closure {
        nodes,
        sort_nodes,
        operations,
        sessions,
        receipts,
        contents,
    })
}
pub(crate) async fn erase(
    tx: &mut sqlx::PgConnection,
    c: &Closure,
    job: ErasureJobId,
    now: UtcTimestamp,
) -> Result<(), Error> {
    sqlx::query("UPDATE resource.scope_heads h SET revision=h.revision+1,updated_at_ms=GREATEST(h.updated_at_ms,$2)
        WHERE EXISTS(SELECT 1 FROM resource.nodes n WHERE ((n.node_id=ANY($1) AND n.name_erasure_job_id IS NULL)
          OR (n.node_id=ANY($3) AND n.sort_key_erasure_job_id IS NULL))
          AND n.tenant_id=h.tenant_id AND n.scope_kind=h.scope_kind AND n.scope_id=h.scope_id)")
        .bind(&c.nodes).bind(now.unix_millis()).bind(&c.sort_nodes).execute(&mut *tx).await?;
    sqlx::query("UPDATE resource.nodes n SET name='erased:'||n.node_id::text,comparison_name='erased:'||n.node_id::text,
        name_erasure_job_id=$2,updated_at_ms=GREATEST(n.updated_at_ms,$3),revision=h.revision
        FROM resource.scope_heads h WHERE n.node_id=ANY($1) AND n.name_erasure_job_id IS NULL
          AND n.tenant_id=h.tenant_id AND n.scope_kind=h.scope_kind AND n.scope_id=h.scope_id")
        .bind(&c.nodes).bind(job.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE resource.nodes n SET sort_key='erased:'||n.node_id::text,
        sort_key_erasure_job_id=$2,updated_at_ms=GREATEST(n.updated_at_ms,$3),revision=h.revision
        FROM resource.scope_heads h WHERE n.node_id=ANY($1) AND n.sort_key_erasure_job_id IS NULL
          AND n.tenant_id=h.tenant_id AND n.scope_kind=h.scope_kind AND n.scope_id=h.scope_id",
    )
    .bind(&c.sort_nodes)
    .bind(job.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *tx)
    .await?;
    for (table, column, ids) in [
        ("edit_operations", "operation_id", &c.operations),
        ("edit_sessions", "session_id", &c.sessions),
    ] {
        let extra = if table == "edit_sessions" {
            ",state=CASE WHEN state IN (1,2) THEN 4 ELSE state END,session_revision=session_revision+1"
        } else {
            ""
        };
        sqlx::query(&format!("UPDATE resource.{table} SET work_item_erasure_job_id=$2{extra} WHERE {column}=ANY($1) AND work_item_erasure_job_id IS NULL"))
            .bind(ids).bind(job.into_uuid()).execute(&mut *tx).await?;
    }
    receipts::erase(tx, &c.receipts, job).await?;
    Ok(())
}

pub(crate) async fn verify_receipts(
    tx: &mut sqlx::PgConnection,
    c: &Closure,
    job: ErasureJobId,
) -> Result<(), Error> {
    receipts::verify(tx, &c.receipts, job).await
}
