use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ContentId, ResourceNodeId, VersionNumber,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ProjectFileVersion, ProjectFileVersionStore, ResourceCommandContext, ResourceStoreError,
};
use sqlx::Row as _;

use crate::resource::{PostgresResourceStore, encode_scope};

const FILE_NODE: i16 = 2;
const ACTIVE_NODE: i16 = 1;
const DELETED_ARTIFACT: i16 = 3;

impl PostgresResourceStore {
    async fn resolve_project_file_version_inner(
        &self,
        context: &ResourceCommandContext,
        node_id: ResourceNodeId,
        version: Option<VersionNumber>,
    ) -> Result<ProjectFileVersion, ResourceStoreError> {
        let mut tx = self.authorized_resource_read(context).await?;
        let (scope_kind, scope_id) = encode_scope(context.scope());
        let row = sqlx::query(
            "SELECT node.file_artifact_id, node.name, version.artifact_version_id,
                    version.version::text AS version,
                    content.content_id, content.media_type,
                    content.byte_length::text AS byte_length,
                    content.content_hash
               FROM resource.nodes AS node
               JOIN artifact.artifacts AS artifact
                 ON artifact.tenant_id = node.tenant_id
                AND artifact.artifact_id = node.file_artifact_id
               JOIN resource.file_versions AS version
                 ON version.artifact_id = artifact.artifact_id
                AND version.version = COALESCE($7::numeric, artifact.current_version)
                AND NOT EXISTS(SELECT 1 FROM artifact.artifact_versions erased WHERE erased.artifact_version_id=version.artifact_version_id AND erased.work_item_erasure_job_id IS NOT NULL)
               JOIN artifact.content_refs AS content
                 ON content.tenant_id = artifact.tenant_id
                AND content.content_id = version.content_id
                AND content.content_hash = version.content_hash AND content.state=2
              WHERE node.tenant_id = $1 AND node.scope_kind = $2 AND node.scope_id = $3
                AND node.node_id = $4 AND node.kind = $5 AND node.lifecycle = $6
                AND artifact.lifecycle <> $8",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(scope_kind)
        .bind(scope_id)
        .bind(node_id.into_uuid())
        .bind(FILE_NODE)
        .bind(ACTIVE_NODE)
        .bind(version.map(|value| value.get().to_string()))
        .bind(DELETED_ARTIFACT)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| ResourceStoreError::DependencyUnavailable)?
        .ok_or(ResourceStoreError::FileUnavailable)?;
        let result = decode_project_file_version(&row, node_id)?;
        tx.commit()
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
        Ok(result)
    }
}

impl ProjectFileVersionStore for PostgresResourceStore {
    fn resolve_artifact<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<ProjectFileVersion, ResourceStoreError>> {
        Box::pin(async move {
            let (scope_kind, scope_id) = encode_scope(context.scope());
            let id = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT node_id FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=$2
                 AND scope_id=$3 AND file_artifact_id=$4 AND kind=2 AND lifecycle=1",
            )
            .bind(context.tenant_id.into_uuid())
            .bind(scope_kind)
            .bind(scope_id)
            .bind(artifact_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?
            .ok_or(ResourceStoreError::FileUnavailable)?;
            self.resolve_project_file_version_inner(
                context,
                ResourceNodeId::from_uuid(id).map_err(|_| ResourceStoreError::Corrupt)?,
                None,
            )
            .await
        })
    }

    fn resolve_version<'a>(
        &'a self,
        context: &'a ResourceCommandContext,
        node_id: ResourceNodeId,
        version: Option<VersionNumber>,
    ) -> ApplicationFuture<'a, Result<ProjectFileVersion, ResourceStoreError>> {
        Box::pin(self.resolve_project_file_version_inner(context, node_id, version))
    }
}

pub(crate) fn decode_project_file_version(
    row: &sqlx::postgres::PgRow,
    node_id: ResourceNodeId,
) -> Result<ProjectFileVersion, ResourceStoreError> {
    let artifact_id = ArtifactId::from_uuid(
        row.try_get("file_artifact_id")
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let artifact_version_id = ArtifactVersionId::from_uuid(
        row.try_get("artifact_version_id")
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let content_id = ContentId::from_uuid(
        row.try_get("content_id")
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let media_type = MediaType::new(
        row.try_get::<String, _>("media_type")
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let byte_length = row
        .try_get::<String, _>("byte_length")
        .map_err(|_| ResourceStoreError::Corrupt)?
        .parse()
        .map_err(|_| ResourceStoreError::Corrupt)?;
    let sha256 = Sha256::from_bytes(
        row.try_get::<Vec<u8>, _>("content_hash")
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    let version = VersionNumber::new(
        row.try_get::<String, _>("version")
            .map_err(|_| ResourceStoreError::Corrupt)?
            .parse()
            .map_err(|_| ResourceStoreError::Corrupt)?,
    )
    .map_err(|_| ResourceStoreError::Corrupt)?;
    Ok(ProjectFileVersion {
        node_id,
        artifact_id,
        artifact_version_id,
        name: row
            .try_get("name")
            .map_err(|_| ResourceStoreError::Corrupt)?,
        version,
        content_ref: ContentRef {
            content_id,
            media_type,
            byte_length,
            sha256,
        },
    })
}
