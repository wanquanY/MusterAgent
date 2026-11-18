//! Preserve original owner error semantics. Infrastructure/corruption never
//! becomes a terminal rejection merely because the transport returned an error.
use super::Error;
use musteragent_runtime_artifact::ArtifactToolError;
use musteragent_runtime_file_query::FileQueryError;
use musteragent_runtime_resource::*;
use musteragent_runtime_session::DeviceResourceRejectionCode as Rejection;

pub(super) fn rejection(error: Error) -> Option<Rejection> {
    match error {
        Error::InvalidArguments => Some(Rejection::InvalidArguments),
        Error::Conflict => Some(Rejection::Conflict),
        Error::ReferenceUnavailable => Some(Rejection::ReferenceUnavailable),
        Error::Forbidden => Some(Rejection::AccessDenied),
        Error::StaleFence | Error::TargetPolicyChanged | Error::Infrastructure | Error::Corrupt => {
            None
        }
    }
}
pub(super) fn artifact(error: ArtifactToolError) -> Error {
    match error {
        ArtifactToolError::InvalidInput
        | ArtifactToolError::InvalidArgument { .. }
        | ArtifactToolError::InvalidIdentifier
        | ArtifactToolError::InvalidJson
        | ArtifactToolError::ArtifactKindMismatch => Error::InvalidArguments,
        ArtifactToolError::VersionConflict => Error::Conflict,
        ArtifactToolError::ArtifactUnavailable => Error::ReferenceUnavailable,
        ArtifactToolError::UnauthorizedContent => Error::Forbidden,
        ArtifactToolError::ArtifactStorageUnavailable
        | ArtifactToolError::AuthorizationUnavailable
        | ArtifactToolError::Unavailable(_)
        | ArtifactToolError::Content(_) => Error::Infrastructure,
        ArtifactToolError::UnsupportedTool
        | ArtifactToolError::ArtifactStateCorrupt
        | ArtifactToolError::InvalidBackendResult => Error::Corrupt,
    }
}
pub(super) fn resource_store(error: ResourceStoreError) -> Error {
    match error {
        ResourceStoreError::PrivateTaskResult
        | ResourceStoreError::BoundsExceeded
        | ResourceStoreError::MoveCycle => Error::InvalidArguments,
        ResourceStoreError::RevisionConflict | ResourceStoreError::NameConflict => Error::Conflict,
        ResourceStoreError::NodeUnavailable
        | ResourceStoreError::ParentUnavailable
        | ResourceStoreError::FileUnavailable => Error::ReferenceUnavailable,
        ResourceStoreError::WriterBusy | ResourceStoreError::DependencyUnavailable => {
            Error::Infrastructure
        }
        ResourceStoreError::Corrupt => Error::Corrupt,
    }
}
pub(super) fn resource(error: ResourceToolError) -> Error {
    match error {
        ResourceToolError::InvalidInput | ResourceToolError::InvalidIdentifier => {
            Error::InvalidArguments
        }
        ResourceToolError::ContentUnavailable | ResourceToolError::SearchUnavailable => {
            Error::Infrastructure
        }
        ResourceToolError::UnsupportedTool
        | ResourceToolError::InvalidResult
        | ResourceToolError::IdentityMismatch => Error::Corrupt,
        ResourceToolError::Resource(error) => match error {
            ResourceApplicationError::Store(error) => resource_store(error),
            ResourceApplicationError::IdentityMismatch => Error::Corrupt,
            _ => Error::InvalidArguments,
        },
        ResourceToolError::Search(error) => match error {
            ResourceSearchError::Store(error) => resource_store(error),
            ResourceSearchError::IdentityMismatch => Error::Corrupt,
            ResourceSearchError::InvalidQuery => Error::InvalidArguments,
        },
    }
}
pub(super) fn edit_store(error: FileEditError) -> Error {
    match error {
        FileEditError::SessionConflict | FileEditError::BaseVersionConflict => Error::Conflict,
        FileEditError::InvalidExpiry
        | FileEditError::InvalidOperation
        | FileEditError::UnsupportedFile
        | FileEditError::FileTooLarge
        | FileEditError::InvalidEncoding => Error::InvalidArguments,
        FileEditError::FileUnavailable | FileEditError::SessionUnavailable => {
            Error::ReferenceUnavailable
        }
        FileEditError::WriterBusy
        | FileEditError::ContentUnavailable
        | FileEditError::DependencyUnavailable => Error::Infrastructure,
        FileEditError::IdentityMismatch | FileEditError::Corrupt => Error::Corrupt,
    }
}
pub(super) fn edit(error: FileEditToolError) -> Error {
    match error {
        FileEditToolError::InvalidInput
        | FileEditToolError::InvalidIdentifier
        | FileEditToolError::EditTargetNotFound
        | FileEditToolError::AmbiguousEditTarget => Error::InvalidArguments,
        FileEditToolError::ContentUnavailable | FileEditToolError::Capability(_) => {
            Error::Infrastructure
        }
        FileEditToolError::IdentityMismatch
        | FileEditToolError::InvalidResult
        | FileEditToolError::UnsupportedTool => Error::Corrupt,
        FileEditToolError::Edit(error) => edit_store(error),
        FileEditToolError::ResourceTool(error) => resource(error),
        FileEditToolError::Read(error) => match error {
            ProjectFileReadError::Store(error) => resource_store(error),
            ProjectFileReadError::InvalidRange | ProjectFileReadError::InvalidEncoding => {
                Error::InvalidArguments
            }
            ProjectFileReadError::IdentityMismatch | ProjectFileReadError::ContentIntegrity => {
                Error::Corrupt
            }
            ProjectFileReadError::ContentUnavailable => Error::Infrastructure,
        },
    }
}
pub(super) fn query(error: FileQueryError) -> Error {
    match error {
        FileQueryError::InvalidQuery
        | FileQueryError::InvalidReference
        | FileQueryError::DirectorySearchTarget
        | FileQueryError::UnsupportedScope(_)
        | FileQueryError::Unsupported => Error::InvalidArguments,
        FileQueryError::Denied => Error::Forbidden,
        FileQueryError::NotFound => Error::ReferenceUnavailable,
        FileQueryError::Changed => Error::Conflict,
        FileQueryError::Incomplete
        | FileQueryError::Unavailable
        | FileQueryError::MissingContent => Error::Infrastructure,
        FileQueryError::Integrity => Error::Corrupt,
    }
}
pub(super) fn artifact_store(error: crate::PostgresArtifactError) -> Error {
    match error {
        crate::PostgresArtifactError::ArtifactConflict
        | crate::PostgresArtifactError::VersionConflict
        | crate::PostgresArtifactError::IdempotencyConflict => Error::Conflict,
        crate::PostgresArtifactError::NotFound => Error::ReferenceUnavailable,
        crate::PostgresArtifactError::Sql(_)
        | crate::PostgresArtifactError::Transaction(_)
        | crate::PostgresArtifactError::ContentUnavailable => Error::Infrastructure,
        _ => Error::Corrupt,
    }
}
