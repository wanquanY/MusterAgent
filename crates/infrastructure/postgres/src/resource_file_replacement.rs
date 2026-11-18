//! Byte-native file replacement shares Resource authority and Artifact versioning.
//! The caller commits this mutation with Tool completion in one fenced unit of work.
use musteragent_artifact::ArtifactVersion;
use musteragent_runtime_resource::{PreparedResourceFileReplacement, ResourceStoreError};
use sqlx::Row;

pub(crate) async fn replace_file_on(
    connection: &mut sqlx::PgConnection,
    target: &PreparedResourceFileReplacement,
    version: &ArtifactVersion,
) -> Result<(), ResourceStoreError> {
    let context = &target.context;
    context
        .validate()
        .map_err(|_| ResourceStoreError::FileUnavailable)?;
    musteragent_runtime_resource::validate_replacement(context, &target.file, version)
        .map_err(|_| ResourceStoreError::Corrupt)?;
    if !crate::resource_authority::lock_current_resource_context(connection, context).await? {
        return Err(ResourceStoreError::FileUnavailable);
    }
    let (scope_kind, scope_id) = crate::resource::encode_scope(context.scope());
    // Creation provenance is immutable. Fence its current project assignment too,
    // so a file moved out of this project cannot be written through an old handle.
    let source = sqlx::query(
        "SELECT conversation_id, workspace_id FROM artifact.artifacts
         WHERE tenant_id = $1 AND artifact_id = $2 AND kind = 6 AND lifecycle IN (1, 2)",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(target.file.artifact_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?
    .ok_or(ResourceStoreError::FileUnavailable)?;
    let conversation: Option<uuid::Uuid> =
        source.try_get("conversation_id").map_err(unavailable)?;
    if let Some(conversation) = conversation {
        let current = sqlx::query_scalar::<_, bool>(
            "SELECT workspace_id IS NOT DISTINCT FROM $3 AND lifecycle IN (1, 2)
             FROM conversation.conversations WHERE tenant_id = $1 AND conversation_id = $2 FOR SHARE",
        ).bind(context.tenant_id.into_uuid()).bind(conversation)
            .bind(context.workspace_id().map(|id| id.into_uuid()))
            .fetch_optional(&mut *connection).await.map_err(unavailable)?.unwrap_or(false);
        if !current {
            return Err(ResourceStoreError::FileUnavailable);
        }
    } else if source
        .try_get::<Option<uuid::Uuid>, _>("workspace_id")
        .map_err(unavailable)?
        != context.workspace_id().map(|id| id.into_uuid())
    {
        return Err(ResourceStoreError::FileUnavailable);
    }
    let node = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT node_id FROM resource.nodes
         WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND node_id=$4
           AND file_artifact_id=$5 AND kind=2 AND lifecycle=1 FOR SHARE",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(target.file.node_id.into_uuid())
    .bind(target.file.artifact_id.into_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?;
    if node.is_none() {
        return Err(ResourceStoreError::FileUnavailable);
    }
    let changed = sqlx::query(
        "UPDATE artifact.artifacts AS artifact
         SET current_version=$4::bigint, updated_at_ms=$5
         WHERE tenant_id=$1 AND artifact_id=$2 AND current_version=$3::bigint
           AND kind=6 AND lifecycle IN (1,2)
           AND EXISTS (SELECT 1 FROM artifact.artifact_versions AS base
               WHERE base.artifact_id=artifact.artifact_id AND base.version=artifact.current_version
                 AND base.artifact_version_id=$6 AND base.content_hash=$7)",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(target.file.artifact_id.into_uuid())
    .bind(target.file.version.get().to_string())
    .bind(version.version.get().to_string())
    .bind(version.committed_at.unix_millis())
    .bind(target.file.artifact_version_id.into_uuid())
    .bind(target.file.content_ref.sha256.into_bytes().as_slice())
    .execute(&mut *connection)
    .await
    .map_err(unavailable)?;
    if changed.rows_affected() != 1 {
        return Err(ResourceStoreError::RevisionConflict);
    }
    Ok(())
}
fn unavailable(_: sqlx::Error) -> ResourceStoreError {
    ResourceStoreError::DependencyUnavailable
}
