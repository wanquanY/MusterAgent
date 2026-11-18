//! One closed failure classification shared by primary and auxiliary models.
use musteragent_runtime_contract::ErrorCode;

use crate::{ProviderError, ProviderErrorCode};

impl ProviderErrorCode {
    /// Append-only identity used in durable operation fingerprints by every
    /// persistence adapter. Existing codes must never be renumbered.
    #[must_use]
    pub const fn canonical_code(self) -> u8 {
        match self {
            Self::AuthenticationFailed => 1,
            Self::AuthorizationFailed => 2,
            Self::RateLimited => 3,
            Self::InvalidRequest => 4,
            Self::ProviderServer => 5,
            Self::ProtocolViolation => 6,
            Self::DeadlineExceeded => 7,
            Self::TransportFailed => 8,
            Self::Cancelled => 9,
            Self::CredentialResolutionFailed => 10,
            Self::ContentResolutionFailed => 11,
            Self::RouteConfigurationInvalid => 12,
            Self::QuotaExceeded => 13,
            Self::CredentialExpired => 14,
            Self::CredentialStoreUnavailable => 15,
            Self::ContentUnavailable => 16,
            Self::ContentNotFound => 17,
            Self::ContentCapacityExceeded => 18,
            Self::DependencyStoreUnavailable => 19,
            Self::DependencyStateInvalid => 20,
            Self::ContextWindowExceeded => 21,
            Self::CreditsExhausted => 22,
            Self::ModelNotEntitled => 23,
            Self::PaymentRequired => 24,
            Self::GatewayUnavailable => 25,
            Self::ExistingInvocationResolverUnavailable => 26,
            Self::ExistingInvocationTimeout => 27,
            Self::ExistingInvocationFailed => 28,
            Self::ExistingInvocationLookupFailed => 29,
        }
    }

    #[must_use]
    pub const fn runtime_code(self) -> ErrorCode {
        match self {
            Self::DeadlineExceeded | Self::ExistingInvocationTimeout => {
                ErrorCode::DependencyTimeout
            }
            Self::ExistingInvocationResolverUnavailable => ErrorCode::ModelRouteUnavailable,
            Self::ExistingInvocationFailed => ErrorCode::ModelRequestRejected,
            Self::ExistingInvocationLookupFailed => ErrorCode::DependencyUnavailable,
            Self::QuotaExceeded => ErrorCode::QuotaExceeded,
            Self::CreditsExhausted => ErrorCode::CreditsExhausted,
            Self::ModelNotEntitled => ErrorCode::ModelNotEntitled,
            Self::PaymentRequired => ErrorCode::ModelPaymentRequired,

            Self::GatewayUnavailable
            | Self::RateLimited
            | Self::ProviderServer
            | Self::TransportFailed
            | Self::CredentialStoreUnavailable
            | Self::DependencyStoreUnavailable => ErrorCode::DependencyUnavailable,
            Self::CredentialExpired => ErrorCode::CredentialRefreshRequired,
            Self::ContentUnavailable => ErrorCode::ContentUnavailable,
            Self::DependencyStateInvalid => ErrorCode::InternalInvariantViolated,
            Self::ContentNotFound => ErrorCode::ResourceFileUnavailable,
            Self::ContentCapacityExceeded | Self::ContextWindowExceeded => {
                ErrorCode::ModelContextExceeded
            }
            Self::CredentialResolutionFailed | Self::RouteConfigurationInvalid => {
                ErrorCode::ModelRouteUnavailable
            }
            Self::AuthenticationFailed => ErrorCode::Unauthenticated,
            Self::AuthorizationFailed => ErrorCode::PolicyDenied,
            Self::InvalidRequest => ErrorCode::ModelRequestRejected,
            Self::ProtocolViolation => ErrorCode::ModelResponseInvalid,
            Self::Cancelled => ErrorCode::RunTransitionInvalid,
            Self::ContentResolutionFailed => ErrorCode::ContentIntegrityFailed,
        }
    }

    /// An authenticated alternate may serve this invocation before output. Local
    /// admission, revoked product access and unresolved tool effects have no entry.
    #[must_use]
    pub const fn permits_alternate_provider(self) -> bool {
        matches!(
            self,
            Self::RateLimited
                | Self::ProviderServer
                | Self::DeadlineExceeded
                | Self::TransportFailed
                | Self::PaymentRequired
                | Self::QuotaExceeded
                | Self::AuthenticationFailed
        )
    }

    pub fn from_alternate_provider_cause(value: &str) -> Option<Self> {
        [
            Self::RateLimited,
            Self::ProviderServer,
            Self::DeadlineExceeded,
            Self::TransportFailed,
            Self::PaymentRequired,
            Self::QuotaExceeded,
            Self::AuthenticationFailed,
        ]
        .into_iter()
        .find(|code| code.as_str() == value)
    }

    /// Transient causes can recover without changing user intent or the
    /// sealed route. A provider retry flag cannot broaden this vocabulary.
    #[must_use]
    pub const fn permits_dependency_recovery(self) -> bool {
        matches!(
            self,
            Self::GatewayUnavailable
                | Self::DeadlineExceeded
                | Self::RateLimited
                | Self::ProviderServer
                | Self::TransportFailed
                | Self::CredentialExpired
                | Self::CredentialStoreUnavailable
                | Self::DependencyStoreUnavailable
                | Self::ContentUnavailable
        )
    }
}

impl ProviderError {
    #[must_use]
    pub const fn permits_dependency_recovery(&self) -> bool {
        self.retryable && self.safe_code.permits_dependency_recovery()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderErrorKind;

    #[test]
    fn accepted_invocation_failures_cannot_be_retried_even_with_a_host_retry_flag() {
        for code in [
            ProviderErrorCode::ExistingInvocationResolverUnavailable,
            ProviderErrorCode::ExistingInvocationTimeout,
            ProviderErrorCode::ExistingInvocationFailed,
            ProviderErrorCode::ExistingInvocationLookupFailed,
        ] {
            let error = ProviderError {
                kind: ProviderErrorKind::Transport,
                safe_code: code,
                retryable: true,
                retry_after_ms: Some(1),
                diagnostic_ref: None,
            };
            assert!(!error.permits_dependency_recovery());
            assert!(!code.permits_alternate_provider());
        }
    }

    #[test]
    fn expired_credential_is_recoverable_but_revoked_authority_is_not_replayed() {
        assert_eq!(
            ProviderErrorCode::CredentialExpired.runtime_code(),
            ErrorCode::CredentialRefreshRequired
        );
        for code in [
            ProviderErrorCode::PaymentRequired,
            ProviderErrorCode::AuthenticationFailed,
            ProviderErrorCode::AuthorizationFailed,
            ProviderErrorCode::InvalidRequest,
            ProviderErrorCode::ProtocolViolation,
            ProviderErrorCode::ContentResolutionFailed,
        ] {
            let error = ProviderError {
                kind: ProviderErrorKind::Authorization,
                safe_code: code,
                retryable: true,
                retry_after_ms: None,
                diagnostic_ref: None,
            };
            assert!(!error.permits_dependency_recovery());
        }
    }

    #[test]
    fn payment_required_does_not_claim_insufficient_first_party_credits() {
        let code = ProviderErrorCode::PaymentRequired;
        assert_eq!(code.runtime_code(), ErrorCode::ModelPaymentRequired);
        assert_eq!(code.as_str(), "payment_required");
        assert_eq!(code.canonical_code(), 24);
        assert!(!code.permits_dependency_recovery());
    }

    #[test]
    fn explicit_context_overflow_is_a_capacity_failure_not_a_dependency_outage() {
        let code = ProviderErrorCode::ContextWindowExceeded;
        assert_eq!(code.runtime_code(), ErrorCode::ModelContextExceeded);
        assert_eq!(code.as_str(), "context_window_exceeded");
        assert!(!code.permits_dependency_recovery());
        // Append-only durable identity; existing codes must never move.
        assert_eq!(
            ProviderErrorCode::ContentCapacityExceeded.canonical_code(),
            18
        );
        assert_eq!(code.canonical_code(), 21);
    }

    #[test]
    fn unavailable_content_is_distinct_from_missing_or_corrupt_content() {
        assert_eq!(
            ProviderErrorCode::ContentUnavailable.runtime_code(),
            ErrorCode::ContentUnavailable
        );
        assert!(ProviderErrorCode::ContentUnavailable.permits_dependency_recovery());
        assert_eq!(
            ProviderErrorCode::ContentNotFound.runtime_code(),
            ErrorCode::ResourceFileUnavailable
        );
        assert!(!ProviderErrorCode::ContentNotFound.permits_dependency_recovery());
        assert_eq!(
            ProviderErrorCode::ContentResolutionFailed.runtime_code(),
            ErrorCode::ContentIntegrityFailed
        );
    }
}
