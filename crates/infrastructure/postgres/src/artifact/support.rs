//! Artifact storage codecs and public error translation.

use super::*;

pub(super) fn map_public_query_error(
    error: PostgresArtifactError,
    correlation_id: musteragent_identity::CorrelationId,
) -> RuntimeError {
    match error {
        PostgresArtifactError::NotFound => runtime_error(
            ErrorCode::ArtifactNotFound,
            correlation_id,
            false,
            "Artifact was not found",
        ),
        PostgresArtifactError::Corrupt => runtime_error(
            ErrorCode::InternalInvariantViolated,
            correlation_id,
            false,
            "Persisted Artifact data is invalid",
        ),
        _ => runtime_error(
            ErrorCode::DependencyUnavailable,
            correlation_id,
            true,
            "Artifact storage is unavailable",
        ),
    }
}

pub(super) fn id<T>(value: uuid::Uuid) -> Result<T, PostgresArtifactError>
where
    T: FromUuid,
{
    T::from_uuid(value).map_err(|_| PostgresArtifactError::Corrupt)
}

pub(super) fn optional_id<T>(value: Option<uuid::Uuid>) -> Result<Option<T>, PostgresArtifactError>
where
    T: FromUuid,
{
    value.map(id).transpose()
}

pub(super) trait FromUuid: Sized {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError>;
}

macro_rules! from_uuid {
    ($($identity:ty),+ $(,)?) => {
        $(impl FromUuid for $identity {
            fn from_uuid(
                value: uuid::Uuid,
            ) -> Result<Self, musteragent_identity::IdentityError> {
                Self::from_uuid(value)
            }
        })+
    };
}

from_uuid!(
    ArtifactId,
    ArtifactVersionId,
    TenantId,
    ConversationId,
    ActivityId,
    RunId,
    InvocationId,
    ParticipantId,
    musteragent_identity::ContentId,
    musteragent_identity::WorkspaceId,
    musteragent_identity::PrincipalId,
);

pub(super) fn version(value: String) -> Result<VersionNumber, PostgresArtifactError> {
    VersionNumber::new(parse_u64(value)?).map_err(|_| PostgresArtifactError::Corrupt)
}

pub(super) fn parse_u64(value: String) -> Result<u64, PostgresArtifactError> {
    value.parse().map_err(|_| PostgresArtifactError::Corrupt)
}

pub(super) const fn kind_code(value: ArtifactKind) -> i16 {
    match value {
        ArtifactKind::Note => 1,
        ArtifactKind::LegacyDiagram => 2,
        ArtifactKind::Presentation => 3,
        ArtifactKind::Image => 4,
        ArtifactKind::Video => 5,
        ArtifactKind::File => 6,
        ArtifactKind::Application => 7,
    }
}

pub(super) fn decode_kind(value: i16) -> Result<ArtifactKind, PostgresArtifactError> {
    match value {
        1 => Ok(ArtifactKind::Note),
        2 => Ok(ArtifactKind::LegacyDiagram),
        3 => Ok(ArtifactKind::Presentation),
        4 => Ok(ArtifactKind::Image),
        5 => Ok(ArtifactKind::Video),
        6 => Ok(ArtifactKind::File),
        7 => Ok(ArtifactKind::Application),
        _ => Err(PostgresArtifactError::Corrupt),
    }
}

pub(super) const fn lifecycle_code(value: ArtifactLifecycle) -> i16 {
    match value {
        ArtifactLifecycle::Draft => 1,
        ArtifactLifecycle::Published => 2,
        ArtifactLifecycle::Archived => 3,
        ArtifactLifecycle::Failed => 4,
    }
}

pub(super) fn decode_lifecycle(value: i16) -> Result<ArtifactLifecycle, PostgresArtifactError> {
    match value {
        1 => Ok(ArtifactLifecycle::Draft),
        2 => Ok(ArtifactLifecycle::Published),
        3 => Ok(ArtifactLifecycle::Archived),
        4 => Ok(ArtifactLifecycle::Failed),
        _ => Err(PostgresArtifactError::Corrupt),
    }
}

pub(super) fn map_artifact_insert(error: sqlx::Error) -> PostgresArtifactError {
    if constraint_name(&error) == Some("artifacts_pkey") {
        PostgresArtifactError::ArtifactConflict
    } else {
        PostgresArtifactError::Sql(error)
    }
}

pub(super) fn map_version_insert(error: sqlx::Error) -> PostgresArtifactError {
    if matches!(
        constraint_name(&error),
        Some("artifact_versions_pkey" | "artifact_versions_artifact_id_version_key")
    ) {
        PostgresArtifactError::VersionConflict
    } else {
        PostgresArtifactError::Sql(error)
    }
}

fn constraint_name(error: &sqlx::Error) -> Option<&str> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::constraint)
}
