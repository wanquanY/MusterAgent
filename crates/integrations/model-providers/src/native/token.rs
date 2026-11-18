use musteragent_runtime_model::{CredentialDemand, RefreshFailure};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_model::ModelRoute;
use secrecy::{ExposeSecret as _, SecretString};

use super::CredentialMaterialSource;
use crate::auth::CredentialFuture;
use crate::{CredentialMaterial, CredentialResolverError};

struct NativeModelCredential {
    token: SecretString,
    expires_at: UtcTimestamp,
}

/// One account-scoped, short-lived model-proxy credential. It is not an
/// admission authority. AdmittedCredentialResolver exposes it to admitted model
/// invocations; the native Memory gateway uses it only for optional embedding
/// retrieval. Revocation is terminal even when another owner retains an Arc.
pub struct NativeModelCredentialAuthority {
    credential: RwLock<Option<NativeModelCredential>>,
    clock: fn() -> UtcTimestamp,
    demand: Arc<CredentialDemand>,
}

impl NativeModelCredentialAuthority {
    pub fn new(token: SecretString, expires_at: UtcTimestamp, clock: fn() -> UtcTimestamp) -> Self {
        Self {
            credential: RwLock::new(Some(NativeModelCredential { token, expires_at })),
            clock,
            demand: Arc::default(),
        }
    }

    /// Shared with transport authority and the host's one renewal worker.
    pub fn demand(&self) -> Arc<CredentialDemand> {
        Arc::clone(&self.demand)
    }

    pub async fn credential_ready(
        &self,
        timeout: Duration,
    ) -> Result<SecretString, CredentialResolverError> {
        self.ready_snapshot(timeout).await.map(|(token, _)| token)
    }

    async fn ready_snapshot(
        &self,
        timeout: Duration,
    ) -> Result<(SecretString, UtcTimestamp), CredentialResolverError> {
        self.demand
            .ready(timeout, || {
                let (token, expires_at) = self.snapshot().map_err(|_| RefreshFailure::Rejected)?;
                if token.expose_secret().is_empty() {
                    return Err(RefreshFailure::Rejected);
                }
                Ok((expires_at > (self.clock)()).then_some((token, expires_at)))
            })
            .await
            .map_err(|error| match error {
                RefreshFailure::Unavailable => CredentialResolverError::Unavailable,
                RefreshFailure::Rejected => CredentialResolverError::BindingMismatch,
            })
    }

    pub fn credential(&self) -> Result<SecretString, CredentialResolverError> {
        let credential = self
            .credential
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let credential = credential
            .as_ref()
            .ok_or(CredentialResolverError::Unavailable)?;
        self.validate_value(credential.token.expose_secret(), credential.expires_at)?;
        Ok(credential.token.clone())
    }

    fn validate_value(
        &self,
        token: &str,
        expires_at: UtcTimestamp,
    ) -> Result<(), CredentialResolverError> {
        if token.is_empty() || expires_at <= (self.clock)() {
            return Err(CredentialResolverError::Expired);
        }
        Ok(())
    }

    pub fn validate_rotation(
        &self,
        token: &str,
        expires_at: UtcTimestamp,
    ) -> Result<(), CredentialResolverError> {
        let current = self
            .credential
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.validate_replacement(current.as_ref(), token, expires_at)
    }

    fn validate_replacement(
        &self,
        current: Option<&NativeModelCredential>,
        token: &str,
        expires_at: UtcTimestamp,
    ) -> Result<(), CredentialResolverError> {
        let current = current.ok_or(CredentialResolverError::Unavailable)?;
        self.validate_value(token, expires_at)?;
        // Equal expiries are valid for idempotent pages of one renewal.
        if expires_at < current.expires_at {
            return Err(CredentialResolverError::Expired);
        }
        Ok(())
    }

    pub fn rotate(
        &self,
        token: SecretString,
        expires_at: UtcTimestamp,
    ) -> Result<(), CredentialResolverError> {
        let mut current = self
            .credential
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Validate while holding the same write lock: a preceding read check
        // alone would allow a concurrent renewal to move expiry backwards.
        self.validate_replacement(current.as_ref(), token.expose_secret(), expires_at)?;
        *current = Some(NativeModelCredential { token, expires_at });
        Ok(())
    }

    /// Trusted rollback of a coordinated credential transaction only. Revoked
    /// account state cannot be restored; create a new account-scoped owner.
    pub fn restore(
        &self,
        token: SecretString,
        expires_at: UtcTimestamp,
    ) -> Result<(), CredentialResolverError> {
        let mut current = self
            .credential
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.is_none() {
            return Err(CredentialResolverError::Unavailable);
        }
        *current = Some(NativeModelCredential { token, expires_at });
        Ok(())
    }

    pub fn snapshot(&self) -> Result<(SecretString, UtcTimestamp), CredentialResolverError> {
        let current = self
            .credential
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = current
            .as_ref()
            .ok_or(CredentialResolverError::Unavailable)?;
        Ok((current.token.clone(), current.expires_at))
    }

    pub fn revoke(&self) {
        self.credential
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.demand.completed(Err(RefreshFailure::Rejected));
    }
}

impl CredentialMaterialSource for NativeModelCredentialAuthority {
    fn resolve_material<'a>(
        &'a self,
        route: &'a ModelRoute,
        _invocation_id: InvocationId,
    ) -> CredentialFuture<'a> {
        Box::pin(async move {
            // A direct-provider binding cannot fall back to the proxy token.
            super::proxy::require_proxy_route(route)
                .map_err(|_| CredentialResolverError::BindingMismatch)?;
            let (secret, expires_at) = self.ready_snapshot(Duration::from_secs(60)).await?;
            Ok(CredentialMaterial::ScopedBearer { secret, expires_at })
        })
    }
}
