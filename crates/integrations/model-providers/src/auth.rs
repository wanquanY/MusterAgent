use std::future::Future;
use std::pin::Pin;

use musteragent_identity::InvocationId;
use musteragent_model::{CredentialGrantRef, ModelRoute};
use musteragent_runtime_model::ModelInvocationRouteRole;
use secrecy::SecretString;
use thiserror::Error;

pub type CredentialFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CredentialMaterial, CredentialResolverError>> + Send + 'a>>;

/// Credential material has a closed shape. Arbitrary headers cannot be
/// injected by route configuration.
pub enum CredentialMaterial {
    Bearer(SecretString),
    /// A scoped credential may expire while the original Journal dispatch
    /// commits. The adapter can reauthorize that same prepared Invocation
    /// immediately before HTTP without rereading or replacing its model body.
    ScopedBearer {
        secret: SecretString,
        expires_at: musteragent_identity::UtcTimestamp,
    },
    ApiKey(SecretString),
    /// Obtained atomically from the credential authority with the actual key.
    /// The revision identifies its source and generation without key material.
    VersionedApiKey {
        secret: SecretString,
        revision: String,
    },
    AwsSigV4(AwsCredentialMaterial),
}

pub struct AwsCredentialMaterial {
    pub access_key_id: SecretString,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
}

/// Resolves the already admission-authorized short-lived grant reference.
/// Implementations must never return a long-lived secret for another provider.
pub trait CredentialResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        grant_ref: &'a CredentialGrantRef,
        route: &'a ModelRoute,
        route_role: ModelInvocationRouteRole,
        invocation_id: InvocationId,
    ) -> CredentialFuture<'a>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum CredentialResolverError {
    #[error("credential grant was not found")]
    NotFound,
    #[error("credential grant is expired")]
    Expired,
    #[error("credential grant binding does not match the requested Provider Route")]
    BindingMismatch,
    #[error("credential store is unavailable")]
    Unavailable,
}
