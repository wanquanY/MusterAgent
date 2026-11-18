use super::*;

pub(super) fn decode_artifact(row: &PgRow) -> Result<Artifact, PostgresArtifactError> {
    let activity_id = optional_id::<ActivityId>(row.try_get("activity_id")?)?;
    let created_by_run_id = optional_id::<RunId>(row.try_get("created_by_run_id")?)?;
    let origin = match (
        row.try_get::<i16, _>("origin")?,
        activity_id,
        created_by_run_id,
    ) {
        (1, Some(activity_id), Some(created_by_run_id)) => ArtifactOrigin::Runtime {
            activity_id,
            created_by_run_id,
        },
        (2, None, None) => ArtifactOrigin::GovernedImport,
        (3, None, None) => ArtifactOrigin::Product,
        _ => return Err(PostgresArtifactError::Corrupt),
    };
    let artifact = Artifact {
        artifact_id: id(row.try_get("artifact_id")?)?,
        tenant_id: id(row.try_get("tenant_id")?)?,
        source: match (
            row.try_get::<Option<uuid::Uuid>, _>("conversation_id")?,
            row.try_get::<Option<uuid::Uuid>, _>("workspace_id")?,
        ) {
            (Some(conversation), None) => ArtifactSource::Conversation(id(conversation)?),
            (None, Some(workspace)) => ArtifactSource::Workspace(id(workspace)?),
            _ => return Err(PostgresArtifactError::Corrupt),
        },
        origin,
        kind: decode_kind(row.try_get("kind")?)?,
        lifecycle: decode_lifecycle(row.try_get("lifecycle")?)?,
        current_version: version(row.try_get("current_version")?)?,
        title: row.try_get("title")?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
    };
    artifact
        .validate()
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    Ok(artifact)
}

pub(crate) fn decode_version(row: PgRow) -> Result<ArtifactVersion, PostgresArtifactError> {
    let content_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    let reference_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("ref_hash")?)
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    if content_hash != reference_hash {
        return Err(PostgresArtifactError::Corrupt);
    }
    let value = ArtifactVersion {
        artifact_version_id: id(row.try_get("artifact_version_id")?)?,
        artifact_id: id(row.try_get("artifact_id")?)?,
        version: version(row.try_get("version")?)?,
        content_ref: ContentRef {
            content_id: id(row.try_get("content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("media_type")?)
                .map_err(|_| PostgresArtifactError::Corrupt)?,
            byte_length: parse_u64(row.try_get("byte_length")?)?,
            sha256: reference_hash,
        },
        dependency_refs: Vec::new(),
        content_hash,
        source_invocation_id: optional_id(row.try_get("source_invocation_id")?)?,
        committed_by: decode_author(&row, "committed_by", "committed_by_principal_id")?,
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("committed_at_ms")?),
    };
    value
        .validate()
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    Ok(value)
}

pub(super) fn decode_summary(row: &PgRow) -> Result<ArtifactSummary, PostgresArtifactError> {
    let artifact = decode_artifact(row)?;
    let content_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("current_content_hash")?)
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    let reference_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("current_ref_hash")?)
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    if content_hash != reference_hash {
        return Err(PostgresArtifactError::Corrupt);
    }
    let current_version = ArtifactVersion {
        artifact_version_id: id(row.try_get("current_artifact_version_id")?)?,
        artifact_id: artifact.artifact_id,
        version: version(row.try_get("current_version_number")?)?,
        content_ref: ContentRef {
            content_id: id(row.try_get("current_content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("current_media_type")?)
                .map_err(|_| PostgresArtifactError::Corrupt)?,
            byte_length: parse_u64(row.try_get("current_byte_length")?)?,
            sha256: reference_hash,
        },
        dependency_refs: Vec::new(),
        content_hash,
        source_invocation_id: optional_id(row.try_get("current_source_invocation_id")?)?,
        committed_by: decode_author(
            row,
            "current_committed_by",
            "current_committed_by_principal_id",
        )?,
        committed_at: UtcTimestamp::from_unix_millis(row.try_get("current_committed_at_ms")?),
    };
    current_version
        .validate()
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    if current_version.version != artifact.current_version {
        return Err(PostgresArtifactError::Corrupt);
    }
    Ok(ArtifactSummary {
        artifact,
        current_version,
    })
}

fn decode_author(
    row: &PgRow,
    participant: &str,
    principal: &str,
) -> Result<ArtifactVersionAuthor, PostgresArtifactError> {
    match (
        row.try_get::<Option<uuid::Uuid>, _>(participant)?,
        row.try_get::<Option<uuid::Uuid>, _>(principal)?,
    ) {
        (Some(value), None) => Ok(ArtifactVersionAuthor::Participant(id(value)?)),
        (None, Some(value)) => Ok(ArtifactVersionAuthor::ProductPrincipal(id(value)?)),
        _ => Err(PostgresArtifactError::Corrupt),
    }
}
