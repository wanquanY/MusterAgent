//! Exact immutable version reachability shared by Conversation and Workspace reads.
use super::*;
use musteragent_identity::WorkspaceId;

pub(super) enum VersionReadScope {
    Conversation(ConversationId),
    Workspace(WorkspaceId),
}

impl PostgresArtifactStore {
    pub(super) fn version_reachability<'a>(
        &'a self,
        actor: Actor,
        scope: VersionReadScope,
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move {
            if references.is_empty() {
                return Ok(Vec::new());
            }
            let (scope_id, is_workspace) = match scope {
                VersionReadScope::Conversation(id) => (id.into_uuid(), false),
                VersionReadScope::Workspace(id) => (id.into_uuid(), true),
            };
            if is_workspace && actor.principal_kind != musteragent_identity::PrincipalKind::User {
                return Ok(vec![false; references.len()]);
            }
            let correlation_id = CorrelationId::generate();
            let requested_ids = references
                .iter()
                .map(|reference| reference.content_id.into_uuid())
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                "WITH authorized_version AS MATERIALIZED (
                     SELECT artifact.tenant_id, version.artifact_version_id,
                            version.content_id, version.content_hash
                     FROM artifact.artifacts AS artifact
                     JOIN artifact.artifact_versions AS version
                       ON version.artifact_id = artifact.artifact_id
                      AND version.artifact_version_id = $5
                     WHERE artifact.tenant_id = $1
                       AND artifact.artifact_id = $4
                       AND (
                         ($8 AND artifact.kind = 6 AND EXISTS (
                           SELECT 1 FROM resource.workspace_roots AS root
                           JOIN resource.nodes AS node ON node.tenant_id = root.tenant_id
                             AND node.scope_kind = 1 AND node.scope_id = root.workspace_id
                           WHERE root.tenant_id = artifact.tenant_id
                             AND root.workspace_id = $2 AND root.owner_principal_id = $3
                             AND node.file_artifact_id = artifact.artifact_id
                             AND node.kind = 2 AND node.lifecycle = 1
                         ))
                         OR (NOT $8 AND artifact.conversation_id = $2 AND (
                           EXISTS (
                               SELECT 1 FROM conversation.participants AS viewer
                               WHERE viewer.tenant_id = artifact.tenant_id
                                 AND viewer.conversation_id = artifact.conversation_id
                                 AND viewer.kind = 1
                                 AND viewer.principal_ref = $3
                                 AND viewer.left_at_ms IS NULL
                           )
                         ))
                       )
                 ), reachable_content AS (
                     SELECT content.content_id, content.media_type,
                            content.byte_length, content.content_hash
                     FROM authorized_version AS version
                     JOIN artifact.content_refs AS content
                       ON content.content_id = version.content_id
                      AND content.tenant_id = version.tenant_id
                      AND content.state = $7
                      AND content.content_hash = version.content_hash
                     WHERE content.content_id = ANY($6)
                     UNION
                     SELECT content.content_id, content.media_type,
                            content.byte_length, content.content_hash
                     FROM authorized_version AS version
                     JOIN artifact.artifact_version_dependencies AS dependency
                       ON dependency.artifact_version_id = version.artifact_version_id
                     JOIN artifact.content_refs AS content
                       ON content.content_id = dependency.content_id
                      AND content.tenant_id = version.tenant_id
                      AND content.state = $7
                      AND content.media_type = dependency.content_media_type
                      AND content.byte_length = dependency.content_byte_length
                      AND content.content_hash = dependency.content_hash
                     WHERE content.content_id = ANY($6)
                 )
                 SELECT content_id, media_type, byte_length::text AS byte_length,
                        content_hash
                 FROM reachable_content",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(scope_id)
            .bind(actor.principal_id.into_uuid())
            .bind(artifact_id.into_uuid())
            .bind(artifact_version_id.into_uuid())
            .bind(requested_ids)
            .bind(CONTENT_READY)
            .bind(is_workspace)
            .fetch_all(self.ledger.pool())
            .await
            .map_err(|_| {
                runtime_error(
                    ErrorCode::DependencyUnavailable,
                    correlation_id,
                    true,
                    "Artifact version authorization storage is unavailable",
                )
            })?;
            decode_content_reachability(rows, references, correlation_id)
        })
    }
}
