//! Authenticated storage envelopes and execution-grant verification.
//! Hosts supply all keys; this crate does not load OS or product credentials.

#![forbid(unsafe_code)]

mod dialogue_admission;
mod opaque_ref;
mod protected_content;
pub use opaque_ref::{OpaqueRefKeyring, OpaqueRefProtectionError, ProtectedOpaqueRef};
pub use protected_content::{ProtectedContent, ProtectedContentError, ProtectedContentKeyring};

use std::collections::BTreeMap;

use ed25519_dalek::{Signature, VerifyingKey};
use musteragent_execution_grant::{
    ExecutionGrant, ExecutionGrantContractError, ExecutionGrantEncodingError,
    GrantAuthorizationRequest, SignedExecutionGrant, canonical_claims,
};
use musteragent_identity::DottedKey;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct GrantKeyring<K = DottedKey> {
    verifying_keys: BTreeMap<K, VerifyingKey>,
}

impl<K> Default for GrantKeyring<K> {
    fn default() -> Self {
        Self {
            verifying_keys: BTreeMap::new(),
        }
    }
}

impl<K: Ord> GrantKeyring<K> {
    pub fn new(
        verifying_keys: impl IntoIterator<Item = (K, VerifyingKey)>,
    ) -> Result<Self, GrantVerificationError> {
        let mut keyring = Self::default();
        for (key_id, key) in verifying_keys {
            if keyring.verifying_keys.insert(key_id, key).is_some() {
                return Err(GrantVerificationError::DuplicateKeyId);
            }
        }
        if keyring.verifying_keys.is_empty() {
            return Err(GrantVerificationError::EmptyKeyring);
        }
        Ok(keyring)
    }
}

impl GrantKeyring {
    /// Authenticates the immutable claims and enforces all audience, tenant,
    /// Run, Provider and expiry bindings. Durable nonce consumption is a
    /// separate admission transaction concern.
    pub fn verify(
        &self,
        signed: &SignedExecutionGrant,
        request: &GrantAuthorizationRequest,
    ) -> Result<ExecutionGrant, GrantVerificationError> {
        let claims = self.authenticate(signed)?;
        claims.authorize(request)?;
        Ok(claims)
    }

    /// Authenticates the envelope without applying request-specific bindings.
    ///
    /// Admission performs binding after it resolves the immutable model route,
    /// so signature authentication and authorization remain distinct steps.
    pub fn authenticate(
        &self,
        signed: &SignedExecutionGrant,
    ) -> Result<ExecutionGrant, GrantVerificationError> {
        let key = self
            .verifying_keys
            .get(&signed.key_id)
            .ok_or(GrantVerificationError::UnknownKey)?;
        let message = canonical_claims(&signed.claims)?;
        key.verify_strict(&message, &Signature::from_bytes(&signed.signature))
            .map_err(|_| GrantVerificationError::InvalidSignature)?;
        signed.claims.validate()?;
        Ok(signed.claims.clone())
    }
}

#[derive(Debug, Error)]
pub enum GrantVerificationError {
    #[error("execution grant verification keyring cannot be empty")]
    EmptyKeyring,
    #[error("execution grant verification key ID is duplicated")]
    DuplicateKeyId,
    #[error("execution grant was signed by an unknown key")]
    UnknownKey,
    #[error("execution grant signature is invalid")]
    InvalidSignature,
    #[error(transparent)]
    Contract(#[from] ExecutionGrantContractError),
    #[error(transparent)]
    Encoding(#[from] ExecutionGrantEncodingError),
}

impl musteragent_scheduler::ExecutionGrantAuthenticator for GrantKeyring {
    fn authenticate(
        &self,
        signed: &SignedExecutionGrant,
    ) -> Result<ExecutionGrant, musteragent_runtime_contract::ErrorCode> {
        GrantKeyring::authenticate(self, signed).map_err(|error| match error {
            GrantVerificationError::UnknownKey | GrantVerificationError::InvalidSignature => {
                musteragent_runtime_contract::ErrorCode::Unauthenticated
            }
            GrantVerificationError::Contract(_)
            | GrantVerificationError::Encoding(_)
            | GrantVerificationError::EmptyKeyring
            | GrantVerificationError::DuplicateKeyId => {
                musteragent_runtime_contract::ErrorCode::InvalidCommand
            }
        })
    }
}

mod work_item_admission;
