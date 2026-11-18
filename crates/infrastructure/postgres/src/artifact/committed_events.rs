//! Artifact facts produced by Resource commands in the same transaction.
use super::*;
use musteragent_artifact::ArtifactEvent;
use musteragent_runtime_contract::DisplayText;
use sqlx::PgConnection;

pub(crate) async fn invocation_events(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    invocation_id: InvocationId,
) -> Result<Vec<ArtifactEvent>, PostgresArtifactError> {
    let rows = sqlx::query(
        "SELECT artifact.kind, artifact.title, version.artifact_version_id, version.artifact_id,
                version.version::text AS version, version.content_id, version.content_hash,
                version.source_invocation_id, version.committed_by,
                version.committed_by_principal_id, version.committed_at_ms,
                content.media_type, content.byte_length::text AS byte_length,
                content.content_hash AS ref_hash
         FROM artifact.artifact_versions AS version
         JOIN artifact.artifacts AS artifact USING (artifact_id)
         JOIN artifact.content_refs AS content
           ON content.content_id = version.content_id AND content.tenant_id = artifact.tenant_id
         WHERE artifact.tenant_id = $1 AND version.source_invocation_id = $2 AND content.state = 2
         ORDER BY version.artifact_id, version.version",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .fetch_all(&mut *connection)
    .await?;
    let mut events = Vec::new();
    for row in rows {
        let kind = decode_kind(row.try_get("kind")?)?;
        let title = row.try_get::<String, _>("title")?;
        let version = decode_version(row)?;
        let version = load_version_dependencies(&mut *connection, vec![version])
            .await?
            .pop()
            .ok_or(PostgresArtifactError::Corrupt)?;
        if version.version.get() == 1 {
            events.push(ArtifactEvent::Created {
                artifact_id: version.artifact_id,
                kind,
                title: DisplayText::new(title).map_err(|_| PostgresArtifactError::Corrupt)?,
                source_invocation_id: Some(invocation_id),
            });
        }
        events.push(ArtifactEvent::VersionCommitted {
            artifact_id: version.artifact_id,
            version: version.version,
            content_ref: version.content_ref,
            dependency_refs: version.dependency_refs,
            content_hash: version.content_hash,
            source_invocation_id: Some(invocation_id),
        });
    }
    Ok(events)
}
