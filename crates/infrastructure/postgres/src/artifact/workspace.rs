use super::*;
use musteragent_identity::PrincipalKind;
use musteragent_runtime_artifact::{GetWorkspaceFileArtifact, WorkspaceFileArtifactApplication};

impl WorkspaceFileArtifactApplication for PostgresArtifactStore {
    fn get_workspace_file_artifact(
        &self,
        query: GetWorkspaceFileArtifact,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ArtifactView, RuntimeError>>
    {
        Box::pin(async move {
            self.load_workspace_file(query)
                .await
                .map_err(|error| map_public_query_error(error, CorrelationId::generate()))
        })
    }
}

impl PostgresArtifactStore {
    async fn load_workspace_file(
        &self,
        query: GetWorkspaceFileArtifact,
    ) -> Result<ArtifactView, PostgresArtifactError> {
        if query.actor.principal_kind != PrincipalKind::User {
            return Err(PostgresArtifactError::NotFound);
        }
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        // A source Conversation can move. Only the current resource placement
        // grants access; an immutable creation source alone never grants access.
        let authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM resource.workspace_roots AS root
                JOIN resource.nodes AS node ON node.tenant_id = root.tenant_id
                  AND node.scope_kind = 1 AND node.scope_id = root.workspace_id
                JOIN artifact.artifacts AS artifact ON artifact.tenant_id = node.tenant_id
                  AND artifact.artifact_id = node.file_artifact_id AND artifact.kind = 6
                WHERE root.tenant_id = $1 AND root.workspace_id = $2
                  AND root.owner_principal_id = $3 AND node.file_artifact_id = $4
                  AND node.kind = 2 AND node.lifecycle = 1
            )",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.workspace_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(query.artifact_id.into_uuid())
        .fetch_one(&mut *tx)
        .await?;
        if !authorized {
            return Err(PostgresArtifactError::NotFound);
        }
        let view = self
            .load_on(&mut tx, query.actor.tenant_id, query.artifact_id)
            .await?;
        tx.commit().await?;
        Ok(view)
    }
}
