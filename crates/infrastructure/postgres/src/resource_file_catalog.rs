//! Unified file-query metadata uses the existing Resource identities and one
//! database snapshot. No file contents are fetched during enumeration.
use crate::resource::{PostgresResourceStore, decode_node, encode_scope};
use crate::resource_read::decode_project_file_version;
use musteragent_identity::ResourceNodeId;
use musteragent_resource::ResourceNodeKind;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ListProjectFiles, ProjectFileCatalogEntry, ProjectFileCatalogPage, ProjectFileCatalogStore,
    ResourceCommandContext, ResourceStoreError,
};
use sqlx::{PgConnection, Row};

pub(crate) const ROW_QUERY: &str =
    "SELECT node.*, version.artifact_version_id, artifact.updated_at_ms AS file_updated_at_ms,
    version.version::text AS version,
    content.content_id, content.media_type, content.byte_length::text AS byte_length,
    content.content_hash, NULL::jsonb AS production
 FROM resource.nodes AS node
 LEFT JOIN artifact.artifacts AS artifact
   ON artifact.tenant_id = node.tenant_id AND artifact.artifact_id = node.file_artifact_id
      AND artifact.lifecycle <> 3
 LEFT JOIN resource.file_versions AS version
   ON version.artifact_id = artifact.artifact_id AND version.version = artifact.current_version
      AND NOT EXISTS(SELECT 1 FROM artifact.artifact_versions erased WHERE erased.artifact_version_id=version.artifact_version_id AND erased.work_item_erasure_job_id IS NOT NULL)
 LEFT JOIN artifact.content_refs AS content
   ON content.tenant_id = artifact.tenant_id AND content.content_id = version.content_id
      AND content.content_hash = version.content_hash
 WHERE node.tenant_id = $1 AND node.scope_kind = $2 AND node.scope_id = $3
   AND node.lifecycle = 1";

fn includes_production(context: &ResourceCommandContext) -> bool {
    // The sealed invocation chooses its immutable result projection. Work,
    // product tree/copy and frozen file tools keep the original cheap query.
    matches!(
        context.scope(),
        musteragent_resource::ResourceScope::OwnedAgent(_)
    ) && context.tool_key.as_ref().is_some_and(|key| {
        matches!(
            key.as_str(),
            "files.list@5" | "files.inspect@5" | "files.list@6" | "files.inspect@6"
        )
    })
}

fn row_query(context: &ResourceCommandContext) -> &'static str {
    if includes_production(context) {
        include_str!("resource_file_catalog/production.sql")
    } else {
        ROW_QUERY
    }
}

// The source owner traverses only active descendants under the same authority
// snapshot. UNION de-duplicates defensively; original mutations prohibit cycles.
pub(crate) const SUBTREE: &str = "WITH RECURSIVE file_subtree(node_id) AS (
    SELECT node_id FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=$2
      AND scope_id=$3 AND lifecycle=1 AND parent_node_id IS NOT DISTINCT FROM $4
    UNION
    SELECT node.node_id FROM resource.nodes node JOIN file_subtree parent
      ON node.parent_node_id=parent.node_id WHERE node.tenant_id=$1
      AND node.scope_kind=$2 AND node.scope_id=$3 AND node.lifecycle=1
) ";
const MAX_SEARCH_NODES: i64 = 100_000;

fn record_failure(error: &sqlx::Error, outcome: &'static str) {
    // Provider messages/details can contain SQL, bind values or user filenames.
    // Keep the original error classification without exposing those values.
    let sqlstate = error
        .as_database_error()
        .and_then(|database| database.code());
    tracing::warn!(
        database.sqlstate = sqlstate.as_deref().unwrap_or(""),
        database.failure = crate::database_failure::safe_failure(error),
        file_catalog.outcome = outcome,
        "File catalog persistence operation failed"
    );
}
fn dependency(error: sqlx::Error) -> ResourceStoreError {
    record_failure(&error, "dependency_unavailable");
    ResourceStoreError::DependencyUnavailable
}
fn corrupt(error: sqlx::Error) -> ResourceStoreError {
    record_failure(&error, "corrupt");
    ResourceStoreError::Corrupt
}

/// This is a read authority check, so it executes inside the read snapshot
/// without taking write locks. Revalidation at application read completion
/// rejects a reference whose current authorization has since been revoked.
pub(crate) async fn authorize(
    connection: &mut PgConnection,
    context: &ResourceCommandContext,
) -> Result<(), ResourceStoreError> {
    if context.validate().is_err() {
        return Err(ResourceStoreError::NodeUnavailable);
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::DialogueExecution { .. }
    ) {
        return if crate::agent_resource::authorize(connection, context, false)
            .await
            .map_err(dependency)?
        {
            Ok(())
        } else {
            Err(ResourceStoreError::NodeUnavailable)
        };
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::DeviceExecution { .. }
    ) {
        return if crate::agent_resource::device::authorize(connection, context, false)
            .await
            .map_err(dependency)?
        {
            Ok(())
        } else {
            Err(ResourceStoreError::NodeUnavailable)
        };
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(_)
    ) {
        return crate::execution_workspace::authorize_read(connection, context).await;
    }
    let Some(cid) = context.conversation_id() else {
        let allowed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM resource.workspace_roots AS root
             JOIN resource.workspace_authority AS authority USING(tenant_id, workspace_id)
             WHERE root.tenant_id=$1 AND root.workspace_id=$2 AND root.owner_principal_id=$3 AND authority.status=2)")
            .bind(context.tenant_id.into_uuid()).bind(context.workspace_id().map(|id| id.into_uuid()))
            .bind(context.actor.principal_id.into_uuid()).fetch_one(connection).await.map_err(dependency)?;
        return if allowed {
            Ok(())
        } else {
            Err(ResourceStoreError::NodeUnavailable)
        };
    };
    let allowed = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
           SELECT 1 FROM conversation.conversations AS conversation
           JOIN conversation.participants AS participant
             ON participant.tenant_id = conversation.tenant_id
              AND participant.conversation_id = conversation.conversation_id
              AND participant.principal_ref = $4 AND participant.kind = 1 AND participant.left_at_ms IS NULL
           WHERE conversation.tenant_id = $1 AND conversation.conversation_id = $2
             AND conversation.workspace_id IS NOT DISTINCT FROM $3 AND conversation.lifecycle IN (1, 2) AND conversation.purpose='standard'
        )",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(cid.into_uuid())
    .bind(context.workspace_id().map(|w| w.into_uuid()))
    .bind(context.actor.principal_id.into_uuid())
    .fetch_one(connection)
    .await
    .map_err(dependency)?;
    if !allowed {
        return Err(ResourceStoreError::NodeUnavailable);
    }
    Ok(())
}
pub(crate) fn decode(
    row: sqlx::postgres::PgRow,
) -> Result<ProjectFileCatalogEntry, ResourceStoreError> {
    let node_id = ResourceNodeId::from_uuid(row.try_get("node_id").map_err(corrupt)?)
        .map_err(|_| ResourceStoreError::Corrupt)?;
    let kind: i16 = row.try_get("kind").map_err(corrupt)?;
    let file = if kind == 2 {
        if row
            .try_get::<Option<uuid::Uuid>, _>("artifact_version_id")
            .map_err(corrupt)?
            .is_none()
        {
            None
        } else {
            Some(decode_project_file_version(&row, node_id)?)
        }
    } else {
        None
    };
    let file_updated_at = row
        .try_get::<Option<i64>, _>("file_updated_at_ms")
        .map_err(corrupt)?
        .map(musteragent_identity::UtcTimestamp::from_unix_millis);
    let production = row
        .try_get::<Option<serde_json::Value>, _>("production")
        .map_err(corrupt)?
        .map(serde_json::from_value::<musteragent_runtime_resource::AgentFileProduction>)
        .transpose()
        .map_err(|_| ResourceStoreError::Corrupt)?;
    if production
        .as_ref()
        .is_some_and(|value| !value.is_valid() || file.is_none())
    {
        return Err(ResourceStoreError::Corrupt);
    }
    let node = decode_node(row)?;
    if node.kind == ResourceNodeKind::Folder && file.is_some() {
        return Err(ResourceStoreError::Corrupt);
    }
    Ok(ProjectFileCatalogEntry {
        production,
        node,
        file,
        file_updated_at,
    })
}

impl PostgresResourceStore {
    fn catalog_page(
        &self,
        query: ListProjectFiles,
        maximum: usize,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async move {
            if query.limit == 0
                || query.limit > maximum
                || query.produced_by.as_ref().is_some_and(|filter| {
                    !filter.is_valid()
                        || !matches!(
                            query.context.scope(),
                            musteragent_resource::ResourceScope::OwnedAgent(_)
                        )
                })
                || query
                    .name_query
                    .as_ref()
                    .is_some_and(|s| s.is_empty() || s.len() > 512)
            {
                return Err(ResourceStoreError::Corrupt);
            }
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            authorize(&mut tx, &query.context).await?;
            if query.produced_by.is_some() && !includes_production(&query.context) {
                return Err(ResourceStoreError::NodeUnavailable);
            }
            let (kind, scope_id) = encode_scope(query.context.scope());
            let producer = query.produced_by.as_ref().map(|filter| filter.selection());
            if let Some(parent) = query.parent_node_id {
                let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM resource.nodes WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND node_id = $4 AND lifecycle = 1 AND kind = 1)")
                    .bind(query.context.tenant_id.into_uuid()).bind(kind).bind(scope_id).bind(parent.into_uuid())
                    .fetch_one(&mut *tx).await.map_err(dependency)?;
                if !exists {
                    return Err(ResourceStoreError::ParentUnavailable);
                }
            }
            let revision = sqlx::query_scalar::<_, i64>("SELECT revision FROM resource.scope_heads WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3")
                .bind(query.context.tenant_id.into_uuid()).bind(kind).bind(scope_id)
                .fetch_optional(&mut *tx).await.map_err(dependency)?.unwrap_or(0);
            let (prefix, selection) = if query.recursive {
                let count_sql = format!(
                    "{SUBTREE} SELECT count(*) FROM (SELECT node_id FROM file_subtree LIMIT 100001) bounded"
                );
                let count: i64 = sqlx::query_scalar(&count_sql)
                    .bind(query.context.tenant_id.into_uuid())
                    .bind(kind)
                    .bind(scope_id)
                    .bind(query.parent_node_id.map(|id| id.into_uuid()))
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(dependency)?;
                if count > MAX_SEARCH_NODES {
                    return Err(ResourceStoreError::BoundsExceeded);
                }
                (
                    SUBTREE,
                    "node.node_id IN (SELECT node_id FROM file_subtree)",
                )
            } else {
                ("", "node.parent_node_id IS NOT DISTINCT FROM $4")
            };
            // Existing file edits advance Artifact versions without necessarily
            // mutating tree revision. Fingerprint the queried directory's small
            // identity/version tuples in this same snapshot; never fetch bodies
            // or silently combine pages from different file heads.
            let row_query = row_query(&query.context);
            let production_fingerprint = if includes_production(&query.context) {
                " || ':' || COALESCE(production::text, '')"
            } else {
                ""
            };
            let fingerprint_sql = format!("{prefix}SELECT sha256(convert_to(COALESCE(string_agg(
                node_id::text || ':' || revision::text || ':' || COALESCE(file_updated_at_ms::text, '') || ':' || COALESCE(version, '') || ':' || COALESCE(encode(content_hash, 'hex'), ''){production_fingerprint}, ',' ORDER BY node_id), ''), 'UTF8'))
                FROM ({row_query} AND {selection}
                AND ($5::text IS NULL OR strpos(lower(node.name), lower($5)) > 0)) AS files
                WHERE ($6::text IS NULL OR production->>$6 = $7)");
            let digest: Vec<u8> = sqlx::query_scalar(&fingerprint_sql)
                .bind(query.context.tenant_id.into_uuid())
                .bind(kind)
                .bind(scope_id)
                .bind(query.parent_node_id.map(|id| id.into_uuid()))
                .bind(query.name_query.as_deref())
                .bind(producer.map(|value| value.0))
                .bind(producer.map(|value| value.1))
                .fetch_one(&mut *tx)
                .await
                .map_err(dependency)?;
            let content_revision = musteragent_content_ref::Sha256::from_bytes(digest)
                .map_err(|_| ResourceStoreError::Corrupt)?;
            let sql = format!(
                "{prefix}SELECT * FROM ({row_query} AND {selection}
                AND ($5::text IS NULL OR strpos(lower(node.name), lower($5)) > 0)) AS files
                WHERE ($8::text IS NULL OR production->>$8 = $9)
                ORDER BY kind, sort_key, node_id OFFSET $6 LIMIT $7"
            );
            let rows = sqlx::query(&sql)
                .bind(query.context.tenant_id.into_uuid())
                .bind(kind)
                .bind(scope_id)
                .bind(query.parent_node_id.map(|id| id.into_uuid()))
                .bind(query.name_query.as_deref())
                .bind(i64::try_from(query.offset).map_err(|_| ResourceStoreError::Corrupt)?)
                .bind(i64::try_from(query.limit + 1).map_err(|_| ResourceStoreError::Corrupt)?)
                .bind(producer.map(|value| value.0))
                .bind(producer.map(|value| value.1))
                .fetch_all(&mut *tx)
                .await
                .map_err(dependency)?;
            let more = rows.len() > query.limit;
            let entries = rows
                .into_iter()
                .take(query.limit)
                .map(decode)
                .collect::<Result<Vec<_>, _>>()?;
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(ProjectFileCatalogPage {
                scope: query.context.scope(),
                revision: crate::resource::decode_revision(revision)?,
                content_revision,
                entries,
                next_offset: if more {
                    Some(
                        query
                            .offset
                            .checked_add(query.limit)
                            .ok_or(ResourceStoreError::Corrupt)?,
                    )
                } else {
                    None
                },
            })
        })
    }
}
impl ProjectFileCatalogStore for PostgresResourceStore {
    fn resolve_artifact_placement<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        artifact_id: musteragent_identity::ArtifactId,
    ) -> ApplicationFuture<'a, Result<Option<ProjectFileCatalogEntry>, ResourceStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            authorize(&mut tx, context).await?;
            let (kind, scope_id) = encode_scope(context.scope());
            let sql = format!("{} AND node.file_artifact_id = $4", row_query(context));
            let row = sqlx::query(&sql)
                .bind(context.tenant_id.into_uuid())
                .bind(kind)
                .bind(scope_id)
                .bind(artifact_id.into_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(dependency)?;
            let entry = row.map(decode).transpose()?;
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(entry)
        })
    }
    fn list_files(
        &self,
        query: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        self.catalog_page(query, 100)
    }
    fn snapshot_files(
        &self,
        query: ListProjectFiles,
    ) -> ApplicationFuture<'_, Result<ProjectFileCatalogPage, ResourceStoreError>> {
        Box::pin(async move {
            if !query.recursive || query.offset != 0 || query.name_query.is_some() {
                return Err(ResourceStoreError::Corrupt);
            }
            let page = self.catalog_page(query, 2048).await?;
            if page.next_offset.is_some() {
                return Err(ResourceStoreError::BoundsExceeded);
            }
            Ok(page)
        })
    }
    fn resolve_file<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        node_id: ResourceNodeId,
    ) -> ApplicationFuture<'a, Result<ProjectFileCatalogEntry, ResourceStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            authorize(&mut tx, context).await?;
            let (kind, scope_id) = encode_scope(context.scope());
            let sql = format!("{} AND node.node_id = $4", row_query(context));
            let row = sqlx::query(&sql)
                .bind(context.tenant_id.into_uuid())
                .bind(kind)
                .bind(scope_id)
                .bind(node_id.into_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(dependency)?
                .ok_or(ResourceStoreError::NodeUnavailable)?;
            let entry = decode(row)?;
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(entry)
        })
    }
}
