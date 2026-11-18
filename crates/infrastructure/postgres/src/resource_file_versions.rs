//! Current Product preview versions from the same owner used by FileEdit.
use crate::{resource::PostgresResourceStore, resource_read::decode_project_file_version};
use musteragent_identity::{ArtifactId, ResourceNodeId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::{
    ConversationFileVersion, ConversationFileVersions, FileQueryContext, FileQueryError,
};
use sqlx::Row;

impl ConversationFileVersions for PostgresResourceStore {
    fn current_versions<'a>(
        &'a self,
        context: &'a FileQueryContext,
        artifacts: Vec<ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFileVersion>, FileQueryError>> {
        Box::pin(async move {
            context.validate()?;
            let conversation = context.require_conversation()?;
            if context.content_allowlist.is_some() {
                return Err(FileQueryError::Denied);
            }
            if artifacts.is_empty() {
                return Ok(Vec::new());
            }
            // One statement observes head, content and placement together.
            // It cannot discover extra files or cross the current project.
            let rows = sqlx::query(
                "SELECT node.node_id, node.name, node.file_artifact_id, version.artifact_version_id,
                    version.version::text AS version, content.content_id, content.media_type,
                    content.byte_length::text AS byte_length, content.content_hash,
                    artifact.updated_at_ms
                 FROM resource.nodes node
                 JOIN artifact.artifacts artifact ON artifact.tenant_id=node.tenant_id
                    AND artifact.artifact_id=node.file_artifact_id
                    AND artifact.kind=6 AND artifact.lifecycle IN (1,2)
                 JOIN resource.file_versions version ON version.artifact_id=artifact.artifact_id
                    AND version.version=artifact.current_version
                 JOIN artifact.content_refs content ON content.tenant_id=artifact.tenant_id
                    AND content.content_id=version.content_id
                    AND content.content_hash=version.content_hash AND content.state=2
                 WHERE node.tenant_id=$1 AND node.file_artifact_id=ANY($2)
                    AND node.kind=2 AND node.lifecycle=1
                    AND ((node.scope_kind=1 AND node.scope_id=$3)
                        OR (node.scope_kind=2 AND node.scope_id=$4))
                    AND EXISTS (SELECT 1 FROM conversation.conversations conversation
                        JOIN conversation.participants participant
                          ON participant.tenant_id=conversation.tenant_id
                         AND participant.conversation_id=conversation.conversation_id
                        WHERE conversation.tenant_id=$1 AND conversation.conversation_id=$4
                          AND conversation.workspace_id IS NOT DISTINCT FROM $3
                          AND participant.principal_ref=$5 AND participant.kind=1
                          AND participant.left_at_ms IS NULL)",
            )
            .bind(context.actor.tenant_id.into_uuid())
            .bind(artifacts.into_iter().map(ArtifactId::into_uuid).collect::<Vec<_>>())
            .bind(context.project_workspace_id.map(|id| id.into_uuid()))
            .bind(conversation.into_uuid())
            .bind(context.actor.principal_id.into_uuid())
            .fetch_all(&self.pool).await.map_err(|_| FileQueryError::Unavailable)?;
            rows.into_iter()
                .map(|row| {
                    let node = ResourceNodeId::from_uuid(
                        row.try_get("node_id")
                            .map_err(|_| FileQueryError::Integrity)?,
                    )
                    .map_err(|_| FileQueryError::Integrity)?;
                    let file = decode_project_file_version(&row, node)
                        .map_err(|_| FileQueryError::Integrity)?;
                    Ok(ConversationFileVersion {
                        artifact_id: file.artifact_id,
                        version: file.version,
                        content_ref: file.content_ref,
                        updated_at: UtcTimestamp::from_unix_millis(
                            row.try_get("updated_at_ms")
                                .map_err(|_| FileQueryError::Integrity)?,
                        ),
                    })
                })
                .collect()
        })
    }
}
