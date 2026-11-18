//! Original Artifact metadata snapshot for the unified Conversation query.
use super::*;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::{
    FileQueryContext, FileQueryError, IndependentConversationFileSource,
    MAX_INDEPENDENT_FILE_ARTIFACTS, MAX_INDEPENDENT_FILE_DEPENDENCIES,
};
impl IndependentConversationFileSource for PostgresArtifactStore {
    fn snapshot<'a>(
        &'a self,
        context: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<Vec<ArtifactSummary>, FileQueryError>> {
        Box::pin(async move {
            context.validate()?;
            let conversation = context.require_conversation()?;
            let mut tx = self
                .ledger
                .database()
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| FileQueryError::Unavailable)?;
            let authorized: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM conversation.participants
                 WHERE tenant_id=$1 AND conversation_id=$2 AND kind=1
                   AND principal_ref=$3 AND left_at_ms IS NULL)",
            )
            .bind(context.actor.tenant_id.into_uuid())
            .bind(conversation.into_uuid())
            .bind(context.actor.principal_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| FileQueryError::Unavailable)?;
            if !authorized {
                return Err(FileQueryError::Denied);
            }
            let rows = sqlx::query(
                "SELECT artifact.artifact_id, artifact.tenant_id,
                    artifact.conversation_id, artifact.workspace_id, artifact.activity_id,
                    artifact.created_by_run_id, artifact.origin,
                    artifact.kind, artifact.lifecycle,
                    artifact.current_version::text AS current_version,
                    artifact.title, artifact.created_at_ms, artifact.updated_at_ms,
                    version.artifact_version_id AS current_artifact_version_id,
                    version.version::text AS current_version_number,
                    version.content_id AS current_content_id,
                    version.content_hash AS current_content_hash,
                    version.source_invocation_id AS current_source_invocation_id,
                    version.committed_by AS current_committed_by,
                    version.committed_by_principal_id AS current_committed_by_principal_id,
                    version.committed_at_ms AS current_committed_at_ms,
                    content.media_type AS current_media_type,
                    content.byte_length::text AS current_byte_length,
                    content.content_hash AS current_ref_hash
             FROM artifact.artifacts AS artifact
             LEFT JOIN conversation.participants AS viewer
               ON viewer.tenant_id = artifact.tenant_id
              AND viewer.conversation_id = artifact.conversation_id
              AND viewer.kind = 1
              AND viewer.principal_ref = $3
              AND viewer.left_at_ms IS NULL
             LEFT JOIN artifact.artifact_versions AS version
               ON version.artifact_id = artifact.artifact_id
              AND version.version = artifact.current_version
             LEFT JOIN artifact.content_refs AS content
               ON content.content_id = version.content_id
              AND content.tenant_id = artifact.tenant_id
              AND content.state = $5
             WHERE artifact.origin IN (2, 3)
               AND artifact.tenant_id = $1
               AND artifact.conversation_id = $2
               AND viewer.participant_id IS NOT NULL
               AND ($4::uuid IS NULL OR artifact.artifact_id < $4)
               AND (
                   $6::text IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM unnest(regexp_split_to_array(lower(trim($6)), '\\s+')) AS token
                       WHERE artifact.search_text NOT LIKE '%' || token || '%'
                   )
               )
               AND (cardinality($7::smallint[]) = 0 OR artifact.kind = ANY($7))
             ORDER BY artifact.artifact_id DESC
             LIMIT $8",
            )
            .bind(context.actor.tenant_id.into_uuid())
            .bind(conversation.into_uuid())
            .bind(context.actor.principal_id.into_uuid())
            .bind(None::<uuid::Uuid>)
            .bind(CONTENT_READY)
            .bind(None::<String>)
            .bind(Vec::<i16>::new())
            .bind((MAX_INDEPENDENT_FILE_ARTIFACTS + 1) as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(|_| FileQueryError::Unavailable)?;
            if rows.len() > MAX_INDEPENDENT_FILE_ARTIFACTS {
                return Err(FileQueryError::Unavailable);
            }
            let mut summaries = rows
                .iter()
                .map(decode_summary)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| FileQueryError::Integrity)?;
            let dependency_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM artifact.artifact_version_dependencies
                 WHERE artifact_version_id = ANY($1) LIMIT $2) AS bounded_dependencies",
            )
            .bind(
                summaries
                    .iter()
                    .map(|s| s.current_version.artifact_version_id.into_uuid())
                    .collect::<Vec<_>>(),
            )
            .bind((MAX_INDEPENDENT_FILE_DEPENDENCIES + 1) as i64)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| FileQueryError::Unavailable)?;
            if dependency_count > MAX_INDEPENDENT_FILE_DEPENDENCIES as i64 {
                return Err(FileQueryError::Unavailable);
            }
            let versions = load_version_dependencies(
                &mut *tx,
                summaries
                    .iter()
                    .map(|s| s.current_version.clone())
                    .collect(),
            )
            .await
            .map_err(|_| FileQueryError::Integrity)?;
            if versions.len() != summaries.len() {
                return Err(FileQueryError::Integrity);
            }
            for (summary, version) in summaries.iter_mut().zip(versions) {
                summary.current_version = version;
            }
            tx.commit().await.map_err(|_| FileQueryError::Unavailable)?;
            Ok(summaries)
        })
    }
}
