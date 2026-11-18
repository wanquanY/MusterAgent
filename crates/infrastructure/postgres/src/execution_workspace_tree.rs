use crate::resource_file_catalog::{ROW_QUERY, decode};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ExecutionWorkspaceTree, ExecutionWorkspaceTreeStore, ReadExecutionWorkspaceTree,
    ResourceStoreError,
};

impl ExecutionWorkspaceTreeStore for crate::PostgresResourceStore {
    fn read_tree(
        &self,
        request: ReadExecutionWorkspaceTree,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceTree, ResourceStoreError>> {
        Box::pin(async move {
            request.binding()?;
            let mut tx = self.authorized_resource_read(&request.context).await?;
            let tree = read_tree_on(&mut tx, &request).await?;
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(tree)
        })
    }
}
fn unavailable(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}

/// The caller owns the authority snapshot and, for preparation, the write lane.
pub(crate) async fn read_tree_on(
    connection: &mut sqlx::PgConnection,
    request: &ReadExecutionWorkspaceTree,
) -> Result<ExecutionWorkspaceTree, ResourceStoreError> {
    let binding = request.binding()?;
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM resource.scope_heads
        WHERE tenant_id=$1 AND scope_kind=3 AND scope_id=$2",
    )
    .bind(request.context.tenant_id.into_uuid())
    .bind(binding.execution_workspace_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?
    .ok_or(ResourceStoreError::NodeUnavailable)?;
    let sql = format!("{ROW_QUERY} ORDER BY node.node_id LIMIT $4");
    let rows = sqlx::query(&sql)
        .bind(request.context.tenant_id.into_uuid())
        .bind(3_i16)
        .bind(binding.execution_workspace_id.into_uuid())
        .bind((request.maximum_entries + 1) as i64)
        .fetch_all(&mut *connection)
        .await
        .map_err(unavailable)?;
    if rows.len() > request.maximum_entries {
        return Err(ResourceStoreError::BoundsExceeded);
    }
    let entries = rows
        .into_iter()
        .map(decode)
        .collect::<Result<Vec<_>, _>>()?;
    let tree = ExecutionWorkspaceTree::from_catalog(
        request,
        crate::resource::decode_revision(revision)?,
        entries,
    )?;
    Ok(tree)
}
