//! Project mounts are bounded snapshots of the original Resource owner. They
//! never copy a project into the private work tree or grant global project IO.
use musteragent_runtime_resource::{
    ExecutionWorkspacePreparationError as Error, ExecutionWorkspaceProjectChange,
    ExecutionWorkspaceProjectSnapshot, ExecutionWorkspaceSnapshotEntry, PrepareExecutionWorkspace,
    ResourceCommandSource,
};
use sqlx::{PgConnection, Row};

pub(crate) async fn prepare(
    connection: &mut PgConnection,
    request: &PrepareExecutionWorkspace,
    used_entries: usize,
    used_bytes: u64,
) -> Result<Option<ExecutionWorkspaceProjectSnapshot>, Error> {
    let ResourceCommandSource::ExecutionWorkspace(binding) = request.context.source else {
        return Err(Error::Unavailable);
    };
    match request.project {
        ExecutionWorkspaceProjectChange::Keep => {}
        ExecutionWorkspaceProjectChange::Clear => {
            sqlx::query("UPDATE resource.execution_workspace_settings SET project_enabled=false,project_root_node_id=NULL,project_writable=false WHERE execution_workspace_id=$1")
                .bind(binding.execution_workspace_id.into_uuid()).execute(&mut *connection).await.map_err(dependency)?;
        }
        ExecutionWorkspaceProjectChange::Select {
            root_node_id,
            writable,
        } => {
            if binding.project_id.is_none() {
                return Err(Error::Unavailable);
            }
            sqlx::query("UPDATE resource.execution_workspace_settings SET project_enabled=true,project_root_node_id=$2,project_writable=$3 WHERE execution_workspace_id=$1")
                .bind(binding.execution_workspace_id.into_uuid()).bind(root_node_id.map(|id|id.into_uuid())).bind(writable)
                .execute(&mut *connection).await.map_err(dependency)?;
        }
    }
    let row=sqlx::query("SELECT project_enabled,project_root_node_id,project_writable FROM resource.execution_workspace_settings WHERE execution_workspace_id=$1")
        .bind(binding.execution_workspace_id.into_uuid()).fetch_one(&mut *connection).await.map_err(dependency)?;
    if !row.try_get::<bool, _>("project_enabled").map_err(corrupt)? {
        return Ok(None);
    }
    let project = binding.project_id.ok_or(Error::Unavailable)?;
    let root = row
        .try_get::<Option<uuid::Uuid>, _>("project_root_node_id")
        .map_err(corrupt)?;
    let writable = row.try_get("project_writable").map_err(corrupt)?;
    let mut context = request.context.clone();
    context.source = ResourceCommandSource::Conversation {
        conversation_id: binding.conversation_id,
        workspace_id: Some(project),
    };
    crate::resource_file_catalog::authorize(connection, &context)
        .await
        .map_err(|_| Error::Unavailable)?;
    let revision:i64=sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=1 AND scope_id=$2 FOR SHARE")
        .bind(context.tenant_id.into_uuid()).bind(project.into_uuid()).fetch_optional(&mut *connection).await.map_err(dependency)?.ok_or(Error::Unavailable)?;
    if let Some(root) = root {
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=1 AND scope_id=$2 AND node_id=$3 AND kind=1 AND lifecycle=1)")
            .bind(context.tenant_id.into_uuid()).bind(project.into_uuid()).bind(root).fetch_one(&mut *connection).await.map_err(dependency)?;
        if !valid {
            return Err(Error::Unavailable);
        }
    }
    let remaining = request
        .maximum_entries
        .checked_sub(used_entries)
        .ok_or(Error::BoundsExceeded)?;
    let sql = format!(
        "{}{} AND node.node_id IN (SELECT node_id FROM file_subtree) ORDER BY node.node_id LIMIT $5",
        crate::resource_file_catalog::SUBTREE,
        crate::resource_file_catalog::ROW_QUERY
    );
    let rows = sqlx::query(&sql)
        .bind(context.tenant_id.into_uuid())
        .bind(1_i16)
        .bind(project.into_uuid())
        .bind(root)
        .bind((remaining + 1) as i64)
        .fetch_all(&mut *connection)
        .await
        .map_err(dependency)?;
    let entries = rows
        .into_iter()
        .map(crate::resource_file_catalog::decode)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::Corrupt)?;
    let root_node_id = root
        .map(musteragent_identity::ResourceNodeId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    let (_, entries) = musteragent_runtime_resource::catalog_tree(
        &context,
        root_node_id,
        remaining,
        request
            .maximum_total_bytes
            .checked_sub(used_bytes)
            .ok_or(Error::BoundsExceeded)?,
        entries,
    )
    .map_err(|e| match e {
        musteragent_runtime_resource::ResourceStoreError::BoundsExceeded => Error::BoundsExceeded,
        _ => Error::Corrupt,
    })?;
    Ok(Some(ExecutionWorkspaceProjectSnapshot {
        root_node_id,
        writable,
        revision: crate::resource::decode_revision(revision).map_err(corrupt)?,
        entries: entries
            .into_iter()
            .map(|e| ExecutionWorkspaceSnapshotEntry {
                node_id: e.resource.node.node_id,
                path: e.path,
                file: e.resource.file,
            })
            .collect(),
    }))
}
fn dependency<T>(_: T) -> Error {
    Error::DependencyUnavailable
}
fn corrupt<T>(_: T) -> Error {
    Error::Corrupt
}
