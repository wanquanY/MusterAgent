//! Presentation metadata for existing conversation relations, never content heads.
use crate::resource::PostgresResourceStore;
use musteragent_identity::ArtifactId;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::{
    ConversationFilePlacement, ConversationFilePlacements, FileQueryContext, FileQueryError,
};
use sqlx::Row;

impl ConversationFilePlacements for PostgresResourceStore {
    fn placements<'a>(
        &'a self,
        context: &'a FileQueryContext,
        artifacts: Vec<ArtifactId>,
    ) -> ApplicationFuture<'a, Result<Vec<ConversationFilePlacement>, FileQueryError>> {
        Box::pin(async move {
            if artifacts.is_empty() {
                return Ok(Vec::new());
            }
            let rows = sqlx::query(
                "SELECT file_artifact_id, name, lifecycle = 1 AS available
                 FROM resource.nodes
                 WHERE tenant_id = $1 AND file_artifact_id = ANY($2) AND kind = 2
                   AND ((scope_kind = 1 AND scope_id = $3)
                     OR (scope_kind = 2 AND scope_id = $4)
                     OR (scope_kind = 3 AND scope_id IN (SELECT execution_workspace_id FROM resource.execution_workspaces WHERE tenant_id=$1 AND conversation_id=$4)))",
            )
            .bind(context.actor.tenant_id.into_uuid())
            .bind(
                artifacts
                    .into_iter()
                    .map(ArtifactId::into_uuid)
                    .collect::<Vec<_>>(),
            )
            .bind(context.project_workspace_id.map(|id| id.into_uuid()))
            .bind(context.conversation_id.map(|id| id.into_uuid()))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| FileQueryError::Unavailable)?;
            rows.into_iter()
                .map(|row| {
                    Ok(ConversationFilePlacement {
                        artifact_id: ArtifactId::from_uuid(
                            row.try_get("file_artifact_id")
                                .map_err(|_| FileQueryError::Integrity)?,
                        )
                        .map_err(|_| FileQueryError::Integrity)?,
                        name: row.try_get("name").map_err(|_| FileQueryError::Integrity)?,
                        available: row
                            .try_get("available")
                            .map_err(|_| FileQueryError::Integrity)?,
                    })
                })
                .collect()
        })
    }
}
