//! Preflight failures retain whether recovery can restore the same request.
use musteragent_runtime_model::{ProviderError, ProviderErrorCode, ProviderErrorKind};

use crate::auth::CredentialResolverError;
use crate::content::ContentResolverError;

pub(super) fn credential_resolution(error: CredentialResolverError) -> ProviderError {
    use CredentialResolverError as E;
    let (kind, code) = match error {
        E::Expired => (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::CredentialExpired,
        ),
        E::Unavailable => (
            ProviderErrorKind::Server,
            ProviderErrorCode::CredentialStoreUnavailable,
        ),
        E::NotFound => (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::CredentialResolutionFailed,
        ),
        E::BindingMismatch => (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::AuthorizationFailed,
        ),
    };
    super::provider_error(kind, code, code.permits_dependency_recovery(), None)
}

pub(super) fn content_resolution(error: ContentResolverError) -> ProviderError {
    use ContentResolverError as E;
    let (kind, code) = match error {
        E::Unavailable => (
            ProviderErrorKind::Server,
            ProviderErrorCode::ContentUnavailable,
        ),
        E::NotFound => (
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::ContentNotFound,
        ),
        E::Unauthorized => (
            ProviderErrorKind::Authorization,
            ProviderErrorCode::AuthorizationFailed,
        ),
        E::TooLarge => (
            ProviderErrorKind::InvalidRequest,
            ProviderErrorCode::ContentCapacityExceeded,
        ),
        E::MediaTypeMismatch | E::LengthMismatch | E::DuplicateContent | E::NotResolved => (
            ProviderErrorKind::Protocol,
            ProviderErrorCode::ContentResolutionFailed,
        ),
    };
    super::provider_error(kind, code, code.permits_dependency_recovery(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_or_unavailable_credentials_do_not_claim_the_model_is_missing() {
        for error in [
            CredentialResolverError::Expired,
            CredentialResolverError::Unavailable,
        ] {
            let failure = credential_resolution(error);
            assert!(failure.permits_dependency_recovery());
            assert_ne!(
                failure.safe_code,
                ProviderErrorCode::CredentialResolutionFailed
            );
        }
        assert!(!credential_resolution(CredentialResolverError::BindingMismatch).retryable);
        assert!(!credential_resolution(CredentialResolverError::NotFound).retryable);
    }

    #[test]
    fn transport_and_content_integrity_failures_cannot_be_confused() {
        assert!(content_resolution(ContentResolverError::Unavailable).retryable);
        for error in [
            ContentResolverError::LengthMismatch,
            ContentResolverError::MediaTypeMismatch,
            ContentResolverError::DuplicateContent,
            ContentResolverError::NotResolved,
        ] {
            let failure = content_resolution(error);
            assert!(!failure.retryable);
            assert_eq!(
                failure.safe_code,
                ProviderErrorCode::ContentResolutionFailed
            );
        }
        assert_eq!(
            content_resolution(ContentResolverError::Unauthorized).safe_code,
            ProviderErrorCode::AuthorizationFailed
        );
        assert_eq!(
            content_resolution(ContentResolverError::NotFound).safe_code,
            ProviderErrorCode::ContentNotFound
        );
    }
}
