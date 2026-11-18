//! Resource pages read through one consistent database snapshot.
use musteragent_identity::{ResourceNodeId, TenantId};
use musteragent_resource::ResourceScope;
use musteragent_runtime_resource::{ResourceNodePage, ResourceStoreError};
use sqlx::PgConnection;

pub(crate) async fn page(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    scope: ResourceScope,
    parent_node_id: Option<ResourceNodeId>,
    include_trashed: bool,
    offset: usize,
    limit: usize,
) -> Result<ResourceNodePage, ResourceStoreError> {
    let (scope_kind, scope_id) = super::encode_scope(scope);
    let revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision
               FROM resource.scope_heads
              WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(super::dependency)?
    .unwrap_or(0);
    let rows = sqlx::query(
        "SELECT node_id, tenant_id, scope_kind, scope_id, parent_node_id,
                    original_parent_node_id, name, comparison_name, kind, lifecycle,
                    file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
                    source_conversation_id, source_invocation_id, source_tool_key,
                    created_at_ms, updated_at_ms, revision, is_result
               FROM resource.nodes
              WHERE tenant_id = $1
                AND scope_kind = $2
                AND scope_id = $3
                AND parent_node_id IS NOT DISTINCT FROM $4
                AND ($5 OR lifecycle = $6)
              ORDER BY lifecycle, kind, sort_key, node_id
              OFFSET $7 LIMIT $8",
    )
    .bind(tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(parent_node_id.map(ResourceNodeId::into_uuid))
    .bind(include_trashed)
    .bind(super::ACTIVE_NODE)
    .bind(i64::try_from(offset).map_err(|_| ResourceStoreError::Corrupt)?)
    .bind(i64::try_from(limit + 1).map_err(|_| ResourceStoreError::Corrupt)?)
    .fetch_all(&mut *connection)
    .await
    .map_err(super::dependency)?;
    let has_more = rows.len() > limit;
    let nodes = rows
        .into_iter()
        .take(limit)
        .map(super::decode_node)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResourceNodePage {
        scope,
        revision: super::decode_revision(revision)?,
        nodes,
        next_offset: has_more.then_some(offset + limit),
    })
}
