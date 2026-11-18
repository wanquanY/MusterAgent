use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::RwLock;

use chacha20poly1305::aead::{Aead as _, KeyInit as _, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use musteragent_identity::DottedKey;
use thiserror::Error;
use zeroize::Zeroizing;

const ENVELOPE_VERSION: u8 = 1;
const NONCE_BYTES: usize = 24;
const MAX_KEY_ID_BYTES: usize = u8::MAX as usize;
const MAX_PLAINTEXT_BYTES: usize = 4096;

/// Versioned AEAD envelope. Debug never reveals ciphertext or metadata that
/// could help correlate a Provider reference.
#[derive(Clone, Eq, PartialEq)]
pub struct ProtectedOpaqueRef(Vec<u8>);

impl ProtectedOpaqueRef {
    pub fn from_bytes(value: Vec<u8>) -> Result<Self, OpaqueRefProtectionError> {
        validate_envelope(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for ProtectedOpaqueRef {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedOpaqueRef([REDACTED])")
    }
}

/// Rotatable in-process data-encryption keyring. Production composition loads
/// these 256-bit keys from KMS/secret management and keeps only the active and
/// decryption-retained generations.
pub struct OpaqueRefKeyring {
    active_key_id: DottedKey,
    keys: RwLock<Option<BTreeMap<DottedKey, Zeroizing<[u8; 32]>>>>,
}

impl Debug for OpaqueRefKeyring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpaqueRefKeyring")
            .field("active_key_id", &self.active_key_id)
            .field(
                "key_count",
                &self
                    .keys
                    .read()
                    .ok()
                    .and_then(|keys| keys.as_ref().map(BTreeMap::len)),
            )
            .finish()
    }
}

impl OpaqueRefKeyring {
    pub fn new(
        active_key_id: DottedKey,
        keys: impl IntoIterator<Item = (DottedKey, [u8; 32])>,
    ) -> Result<Self, OpaqueRefProtectionError> {
        let mut values = BTreeMap::new();
        for (key_id, key) in keys {
            if values.insert(key_id, Zeroizing::new(key)).is_some() {
                return Err(OpaqueRefProtectionError::DuplicateKeyId);
            }
        }
        if !values.contains_key(&active_key_id) {
            return Err(OpaqueRefProtectionError::ActiveKeyMissing);
        }
        Ok(Self {
            active_key_id,
            keys: RwLock::new(Some(values)),
        })
    }

    /// Waits for in-flight cryptographic operations, erases every retained
    /// generation and rejects subsequent use, including through old Arc clones.
    /// Revocation never removes persistent vault records or permits revival.
    pub fn revoke(&self) {
        self.keys
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    pub fn seal(
        &self,
        context: &[u8],
        plaintext: &str,
    ) -> Result<ProtectedOpaqueRef, OpaqueRefProtectionError> {
        if context.is_empty() {
            return Err(OpaqueRefProtectionError::EmptyContext);
        }
        if plaintext.is_empty() || plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(OpaqueRefProtectionError::InvalidPlaintext);
        }
        let key_id = self.active_key_id.as_str().as_bytes();
        let key_id_length =
            u8::try_from(key_id.len()).map_err(|_| OpaqueRefProtectionError::KeyIdTooLong)?;
        let keys = self
            .keys
            .read()
            .map_err(|_| OpaqueRefProtectionError::Unavailable)?;
        let key = keys
            .as_ref()
            .ok_or(OpaqueRefProtectionError::Unavailable)?
            .get(&self.active_key_id)
            .ok_or(OpaqueRefProtectionError::ActiveKeyMissing)?;
        let key: &Key = key
            .as_slice()
            .try_into()
            .map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let cipher = XChaCha20Poly1305::new(key);
        let mut nonce = [0_u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|_| OpaqueRefProtectionError::RandomUnavailable)?;
        let nonce_ref: &XNonce = nonce
            .as_slice()
            .try_into()
            .map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let ciphertext = cipher
            .encrypt(
                nonce_ref,
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: context,
                },
            )
            .map_err(|_| OpaqueRefProtectionError::EncryptFailed)?;
        let mut envelope = Vec::with_capacity(2 + key_id.len() + NONCE_BYTES + ciphertext.len());
        envelope.push(ENVELOPE_VERSION);
        envelope.push(key_id_length);
        envelope.extend_from_slice(key_id);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        ProtectedOpaqueRef::from_bytes(envelope)
    }

    pub fn open(
        &self,
        context: &[u8],
        protected: &ProtectedOpaqueRef,
    ) -> Result<String, OpaqueRefProtectionError> {
        if context.is_empty() {
            return Err(OpaqueRefProtectionError::EmptyContext);
        }
        let envelope = protected.as_bytes();
        validate_envelope(envelope)?;
        let key_id_length = usize::from(envelope[1]);
        let key_id_end = 2 + key_id_length;
        let key_id = std::str::from_utf8(&envelope[2..key_id_end])
            .map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let key_id =
            DottedKey::new(key_id).map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let keys = self
            .keys
            .read()
            .map_err(|_| OpaqueRefProtectionError::Unavailable)?;
        let key = keys
            .as_ref()
            .ok_or(OpaqueRefProtectionError::Unavailable)?
            .get(&key_id)
            .ok_or(OpaqueRefProtectionError::UnknownKey)?;
        let nonce_end = key_id_end + NONCE_BYTES;
        let nonce: &XNonce = (&envelope[key_id_end..nonce_end])
            .try_into()
            .map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let key: &Key = key
            .as_slice()
            .try_into()
            .map_err(|_| OpaqueRefProtectionError::MalformedEnvelope)?;
        let plaintext = XChaCha20Poly1305::new(key)
            .decrypt(
                nonce,
                Payload {
                    msg: &envelope[nonce_end..],
                    aad: context,
                },
            )
            .map_err(|_| OpaqueRefProtectionError::DecryptFailed)?;
        String::from_utf8(plaintext).map_err(|_| OpaqueRefProtectionError::InvalidUtf8)
    }
}

fn validate_envelope(value: &[u8]) -> Result<(), OpaqueRefProtectionError> {
    if value.len() < 2 + 1 + NONCE_BYTES + 16 || value[0] != ENVELOPE_VERSION {
        return Err(OpaqueRefProtectionError::MalformedEnvelope);
    }
    let key_id_length = usize::from(value[1]);
    if key_id_length == 0
        || key_id_length > MAX_KEY_ID_BYTES
        || value.len() < 2 + key_id_length + NONCE_BYTES + 16
    {
        return Err(OpaqueRefProtectionError::MalformedEnvelope);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum OpaqueRefProtectionError {
    #[error("opaque-reference keyring is unavailable")]
    Unavailable,
    #[error("opaque-reference active encryption key is missing")]
    ActiveKeyMissing,
    #[error("opaque-reference key ID is duplicated")]
    DuplicateKeyId,
    #[error("opaque-reference key ID is too long")]
    KeyIdTooLong,
    #[error("opaque-reference protection context cannot be empty")]
    EmptyContext,
    #[error("opaque-reference plaintext is empty or too long")]
    InvalidPlaintext,
    #[error("operating-system randomness is unavailable")]
    RandomUnavailable,
    #[error("opaque-reference encryption failed")]
    EncryptFailed,
    #[error("opaque-reference envelope is malformed")]
    MalformedEnvelope,
    #[error("opaque-reference envelope uses an unknown key")]
    UnknownKey,
    #[error("opaque-reference authentication or decryption failed")]
    DecryptFailed,
    #[error("opaque-reference plaintext is not UTF-8")]
    InvalidUtf8,
}

#[cfg(test)]
mod tests {
    use musteragent_identity::DottedKey;

    use super::{OpaqueRefKeyring, OpaqueRefProtectionError};

    #[test]
    fn poisoned_owner_fails_closed_and_revocation_erases_retained_keys() {
        let id = key_id("test.poisoned");
        let keys = OpaqueRefKeyring::new(id.clone(), [(id, [42; 32])]).expect("keys");
        let value = keys.seal(b"context", "reference").expect("reference");
        std::thread::scope(|scope| {
            assert!(
                scope
                    .spawn(|| {
                        let _guard = keys.keys.write().expect("healthy lock");
                        panic!("simulated owner panic");
                    })
                    .join()
                    .is_err()
            );
        });
        assert_eq!(
            keys.seal(b"context", "reference"),
            Err(OpaqueRefProtectionError::Unavailable)
        );
        assert_eq!(
            keys.open(b"context", &value),
            Err(OpaqueRefProtectionError::Unavailable)
        );
        keys.revoke();
        assert!(
            keys.keys
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .is_none()
        );
    }

    fn key_id(value: &str) -> DottedKey {
        match DottedKey::new(value) {
            Ok(value) => value,
            Err(error) => panic!("key ID fixture invalid: {error}"),
        }
    }

    #[test]
    fn envelope_is_randomized_context_bound_and_rotatable() {
        let old = key_id("runtime.ref.old");
        let active = key_id("runtime.ref.active");
        let keyring = match OpaqueRefKeyring::new(
            active.clone(),
            [(old.clone(), [7_u8; 32]), (active, [9_u8; 32])],
        ) {
            Ok(value) => value,
            Err(error) => panic!("keyring failed: {error}"),
        };
        let first = match keyring.seal(b"invocation:a", "provider-secret-ref") {
            Ok(value) => value,
            Err(error) => panic!("seal failed: {error}"),
        };
        let second = match keyring.seal(b"invocation:a", "provider-secret-ref") {
            Ok(value) => value,
            Err(error) => panic!("seal failed: {error}"),
        };
        assert_ne!(first, second);
        assert_eq!(
            keyring.open(b"invocation:a", &first),
            Ok("provider-secret-ref".into())
        );
        assert_eq!(
            keyring.open(b"invocation:b", &first),
            Err(OpaqueRefProtectionError::DecryptFailed)
        );
    }
}
