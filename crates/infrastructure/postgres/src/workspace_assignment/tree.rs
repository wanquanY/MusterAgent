use super::{corrupt, unavailable};
use musteragent_runtime_session::workspace_assignment::{
    PrepareWorkspaceAssignment, WorkspaceAssignmentError as Error,
};
use sqlx::Row as _;

pub(super) async fn transfer(
    tx: &mut sqlx::PgConnection,
    request: &PrepareWorkspaceAssignment,
    title: Option<&str>,
    sequence: u64,
) -> Result<(), Error> {
    let tenant = request.actor.tenant_id.into_uuid();
    let source = request.conversation_id.into_uuid();
    let target = request.target_workspace_id.into_uuid();
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    sqlx::query(
        "INSERT INTO resource.scope_heads(tenant_id,scope_kind,scope_id,revision,updated_at_ms)
        VALUES($1,2,$2,0,$3) ON CONFLICT DO NOTHING",
    )
    .bind(tenant)
    .bind(source)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    // Stable scope lock order across concurrent Conversations in one target.
    let heads = sqlx::query(
        "SELECT scope_kind,revision FROM resource.scope_heads
        WHERE tenant_id=$1 AND ((scope_kind=1 AND scope_id=$2) OR (scope_kind=2 AND scope_id=$3))
        ORDER BY scope_kind,scope_id FOR UPDATE",
    )
    .bind(tenant)
    .bind(target)
    .bind(source)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    if heads.len() != 2 {
        return Err(Error::AuthorityChanged);
    }
    let target_revision = heads[0]
        .try_get::<i64, _>("revision")
        .map_err(corrupt)?
        .checked_add(1)
        .ok_or(Error::Corrupt)?;
    let nodes = sqlx::query("SELECT node_id,parent_node_id,original_parent_node_id,lifecycle
        FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2 ORDER BY node_id FOR UPDATE")
        .bind(tenant).bind(source).fetch_all(&mut *tx).await.map_err(unavailable)?;
    sqlx::query("UPDATE conversation.conversations SET workspace_id=$1 WHERE tenant_id=$2 AND conversation_id=$3")
        .bind(target).bind(tenant).bind(source).execute(&mut *tx).await.map_err(unavailable)?;
    let folder = if nodes.is_empty() {
        None
    } else {
        let root = history_root(tx, request, now, target_revision).await?;
        let label = title.unwrap_or("未命名对话");
        Some(create_folder(tx, request, Some(root), label, now, target_revision).await?)
    };
    sqlx::query("INSERT INTO resource.workspace_assignment_receipts
        (command_id,tenant_id,conversation_id,target_workspace_id,archive_folder_id,moved_node_count,assigned_sequence,committed_at_ms)
        VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8)")
        .bind(request.command_id.into_uuid()).bind(tenant).bind(source).bind(target).bind(folder)
        .bind(i64::try_from(nodes.len()).map_err(|_|Error::Corrupt)?).bind(sequence.to_string()).bind(now)
        .execute(&mut *tx).await.map_err(unavailable)?;
    // Two-phase internal relocation: while all rows remain invisible until
    // commit, use the valid trashed/root shape to avoid same-name collisions
    // and parent/original-parent ordering dependencies. Every statement still
    // obeys scope/FK/history triggers. Restore every original lifecycle/edge
    // before committing; never publish a temporary trash operation.
    sqlx::query("UPDATE resource.nodes SET scope_kind=1,scope_id=$3,lifecycle=2,
        parent_node_id=NULL,original_parent_node_id=NULL,revision=$4,updated_at_ms=GREATEST(updated_at_ms,$5)
        WHERE tenant_id=$1 AND scope_kind=2 AND scope_id=$2")
        .bind(tenant).bind(source).bind(target).bind(target_revision).bind(now)
        .execute(&mut *tx).await.map_err(unavailable)?;
    for node in nodes {
        let lifecycle: i16 = node.try_get("lifecycle").map_err(corrupt)?;
        let parent: Option<uuid::Uuid> = node.try_get("parent_node_id").map_err(corrupt)?;
        let original_parent: Option<uuid::Uuid> =
            node.try_get("original_parent_node_id").map_err(corrupt)?;
        sqlx::query("UPDATE resource.nodes SET lifecycle=$2,parent_node_id=$3,original_parent_node_id=$4 WHERE node_id=$1")
            .bind(node.try_get::<uuid::Uuid,_>("node_id").map_err(corrupt)?)
            .bind(lifecycle).bind(if lifecycle==1 {parent.or(folder)} else {parent})
            .bind(if lifecycle==2 {original_parent.or(folder)} else {None})
            .execute(&mut *tx).await.map_err(unavailable)?;
    }
    sqlx::query("UPDATE resource.scope_heads SET revision=revision+1,updated_at_ms=GREATEST(updated_at_ms,$4)
        WHERE tenant_id=$1 AND ((scope_kind=1 AND scope_id=$2) OR(scope_kind=2 AND scope_id=$3))")
        .bind(tenant).bind(target).bind(source).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
    Ok(())
}

async fn history_root(
    tx: &mut sqlx::PgConnection,
    request: &PrepareWorkspaceAssignment,
    now: i64,
    revision: i64,
) -> Result<uuid::Uuid, Error> {
    let existing = sqlx::query("SELECT root.node_id, node.scope_kind, node.scope_id, node.kind, node.lifecycle
        FROM resource.workspace_history_roots root JOIN resource.nodes node ON node.node_id=root.node_id
        WHERE root.tenant_id=$1 AND root.workspace_id=$2")
        .bind(request.actor.tenant_id.into_uuid()).bind(request.target_workspace_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some(row) = existing {
        if row.try_get::<i16, _>("scope_kind").map_err(corrupt)? != 1
            || row.try_get::<uuid::Uuid, _>("scope_id").map_err(corrupt)?
                != request.target_workspace_id.into_uuid()
            || row.try_get::<i16, _>("kind").map_err(corrupt)? != 1
            || row.try_get::<i16, _>("lifecycle").map_err(corrupt)? != 1
        {
            return Err(Error::AuthorityChanged);
        }
        return row.try_get("node_id").map_err(corrupt);
    }
    let node = create_folder(tx, request, None, "历史会话资料", now, revision).await?;
    sqlx::query("INSERT INTO resource.workspace_history_roots(tenant_id,workspace_id,node_id) VALUES($1,$2,$3)")
        .bind(request.actor.tenant_id.into_uuid()).bind(request.target_workspace_id.into_uuid()).bind(node)
        .execute(&mut *tx).await.map_err(unavailable)?;
    Ok(node)
}
async fn create_folder(
    tx: &mut sqlx::PgConnection,
    request: &PrepareWorkspaceAssignment,
    parent: Option<uuid::Uuid>,
    label: &str,
    now: i64,
    revision: i64,
) -> Result<uuid::Uuid, Error> {
    let mut suffix = 0u32;
    loop {
        let identity_suffix = if suffix > 0 {
            format!(" · {}-{suffix}", request.conversation_id)
        } else if parent.is_some() {
            format!(" · {}", &request.conversation_id.to_string()[..8])
        } else {
            String::new()
        };
        let canonical =
            musteragent_resource::ResourceName::generated_folder(label, &identity_suffix)
                .map_err(|_| Error::Corrupt)?;
        let name = canonical.as_str();
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=1
            AND scope_id=$2 AND parent_node_id IS NOT DISTINCT FROM $3 AND lifecycle=1 AND comparison_name=$4)")
            .bind(request.actor.tenant_id.into_uuid()).bind(request.target_workspace_id.into_uuid()).bind(parent)
            .bind(canonical.comparison_key()).fetch_one(&mut *tx).await.map_err(unavailable)?;
        if exists {
            suffix = suffix.checked_add(1).ok_or(Error::Corrupt)?;
            continue;
        }
        let id = uuid::Uuid::now_v7();
        sqlx::query("INSERT INTO resource.nodes(node_id,tenant_id,scope_kind,scope_id,parent_node_id,name,comparison_name,
            kind,lifecycle,sort_key,actor_principal_id,actor_principal_kind,created_at_ms,updated_at_ms,revision)
            VALUES($1,$2,1,$3,$4,$5,$6,1,1,$5,$7,1,$8,$8,$9)")
            .bind(id).bind(request.actor.tenant_id.into_uuid()).bind(request.target_workspace_id.into_uuid()).bind(parent)
            .bind(name).bind(canonical.comparison_key()).bind(request.actor.principal_id.into_uuid()).bind(now).bind(revision)
            .execute(&mut *tx).await.map_err(unavailable)?;
        return Ok(id);
    }
}
