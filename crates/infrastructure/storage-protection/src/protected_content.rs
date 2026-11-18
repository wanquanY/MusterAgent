use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::RwLock;

use chacha20poly1305::aead::{Aead as _, KeyInit as _, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use musteragent_identity::DottedKey;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use zeroize::Zeroizing;

const ENVELOPE_VERSION: u8 = 1;
const NONCE_BYTES: usize = 24;
const TAG_BYTES: usize = 16;
const MAX_KEY_ID_BYTES: usize = u8::MAX as usize;
/// The Content Store is the outer storage bound. This owner deliberately
/// supports every admitted recovery payload without changing the 4 KiB
/// opaque-reference contract.
// A 256 MiB logical content body plus at most 64 KiB of canonical owner
// metadata. The ContentStore and each caller still enforce their own body cap.
const MAX_PLAINTEXT_BYTES: usize = 256 * 1_048_576 + 65_536;
const NONCE_DOMAIN: &[u8] = b"musteragent.protected-content.nonce.v2\0";
const LEGACY_NONCE_DOMAIN: &[u8] = b"musteragent.legacy.protected-content.nonce.v1\0";

/// Authenticated large-payload envelope intended for tenant-scoped Content
/// Store bytes. Debug output never exposes ciphertext or correlatable hashes.
#[derive(Clone, Eq, PartialEq)]
pub struct ProtectedContent(Vec<u8>);

impl ProtectedContent {
    pub fn from_bytes(value: Vec<u8>) -> Result<Self, ProtectedContentError> {
        validate_envelope(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn key_id(&self) -> Result<DottedKey, ProtectedContentError> {
        let key_id_length = usize::from(self.0[1]);
        let key_id = std::str::from_utf8(&self.0[2..2 + key_id_length])
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        DottedKey::new(key_id).map_err(|_| ProtectedContentError::MalformedEnvelope)
    }
}

impl Debug for ProtectedContent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedContent([REDACTED])")
    }
}

/// Rotatable AEAD owner for bounded recovery payloads stored as encrypted
/// Content Store blobs. This is intentionally distinct from OpaqueRefKeyring.
pub struct ProtectedContentKeyring {
    active_key_id: DottedKey,
    keys: RwLock<Option<BTreeMap<DottedKey, Zeroizing<[u8; 32]>>>>,
}

enum NonceMode {
    ImmutableIntent,
    RandomMaterial,
}

impl Debug for ProtectedContentKeyring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtectedContentKeyring")
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

impl ProtectedContentKeyring {
    pub fn new(
        active_key_id: DottedKey,
        keys: impl IntoIterator<Item = (DottedKey, [u8; 32])>,
    ) -> Result<Self, ProtectedContentError> {
        let mut values = BTreeMap::new();
        for (key_id, key) in keys {
            if values.insert(key_id, Zeroizing::new(key)).is_some() {
                return Err(ProtectedContentError::DuplicateKeyId);
            }
        }
        if !values.contains_key(&active_key_id) {
            return Err(ProtectedContentError::ActiveKeyMissing);
        }
        Ok(Self {
            active_key_id,
            keys: RwLock::new(Some(values)),
        })
    }

    /// A terminal in-memory retirement boundary, shared by all Arc holders.
    /// Existing bounded encryption/decryption finishes before keys are erased.
    /// The account owner separately retains the sealed persistent root.
    pub fn revoke(&self) {
        self.keys
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    /// Seals deterministically for one immutable, invocation-bound recovery
    /// intent. The nonce includes the authenticated context and plaintext hash,
    /// making a crash retry byte-identical while distinct invocations and
    /// distinct payloads cannot reuse a nonce.
    #[must_use]
    pub const fn active_key_id(&self) -> &DottedKey {
        &self.active_key_id
    }

    pub fn seal(
        &self,
        context: &[u8],
        plaintext: &[u8],
    ) -> Result<ProtectedContent, ProtectedContentError> {
        self.seal_with_key(&self.active_key_id, context, plaintext)
    }

    pub fn seal_with_key(
        &self,
        sealing_key_id: &DottedKey,
        context: &[u8],
        plaintext: &[u8],
    ) -> Result<ProtectedContent, ProtectedContentError> {
        self.seal_with_nonce_mode(
            sealing_key_id,
            context,
            plaintext,
            NonceMode::ImmutableIntent,
        )
    }

    /// Physical at-rest material has no byte-identical ciphertext retry
    /// contract. A fresh random nonce avoids publishing a deterministic digest
    /// of guessable plaintext/context. Existing recovery intents keep `seal`.
    pub fn seal_randomized(
        &self,
        context: &[u8],
        plaintext: &[u8],
    ) -> Result<ProtectedContent, ProtectedContentError> {
        self.seal_with_nonce_mode(
            &self.active_key_id,
            context,
            plaintext,
            NonceMode::RandomMaterial,
        )
    }

    fn seal_with_nonce_mode(
        &self,
        sealing_key_id: &DottedKey,
        context: &[u8],
        plaintext: &[u8],
        nonce_mode: NonceMode,
    ) -> Result<ProtectedContent, ProtectedContentError> {
        if context.is_empty() {
            return Err(ProtectedContentError::EmptyContext);
        }
        if plaintext.is_empty() || plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(ProtectedContentError::InvalidPlaintext);
        }
        let key_id = sealing_key_id.as_str().as_bytes();
        let key_id_length =
            u8::try_from(key_id.len()).map_err(|_| ProtectedContentError::KeyIdTooLong)?;
        let keys = self
            .keys
            .read()
            .map_err(|_| ProtectedContentError::Unavailable)?;
        let key = keys
            .as_ref()
            .ok_or(ProtectedContentError::Unavailable)?
            .get(sealing_key_id)
            .ok_or(ProtectedContentError::UnknownKey)?;
        let key: &Key = key
            .as_slice()
            .try_into()
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let cipher = XChaCha20Poly1305::new(key);
        let mut nonce_bytes = [0_u8; NONCE_BYTES];
        match nonce_mode {
            NonceMode::RandomMaterial => getrandom::fill(&mut nonce_bytes)
                .map_err(|_| ProtectedContentError::EncryptFailed)?,
            NonceMode::ImmutableIntent => {
                let mut nonce_material =
                    Vec::with_capacity(32 + context.len() + plaintext.len().min(64));
                nonce_material.extend_from_slice(nonce_domain(sealing_key_id));
                nonce_material.extend_from_slice(sealing_key_id.as_str().as_bytes());
                nonce_material.extend_from_slice(context);
                nonce_material.extend_from_slice(&Sha256::digest(plaintext));
                nonce_bytes.copy_from_slice(&Sha256::digest(&nonce_material)[..NONCE_BYTES]);
            }
        }
        let nonce: &XNonce = nonce_bytes
            .as_slice()
            .try_into()
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad: context,
                },
            )
            .map_err(|_| ProtectedContentError::EncryptFailed)?;
        let mut envelope = Vec::with_capacity(2 + key_id.len() + NONCE_BYTES + ciphertext.len());
        envelope.push(ENVELOPE_VERSION);
        envelope.push(key_id_length);
        envelope.extend_from_slice(key_id);
        envelope.extend_from_slice(nonce);
        envelope.extend_from_slice(&ciphertext);
        ProtectedContent::from_bytes(envelope)
    }

    pub fn open(
        &self,
        context: &[u8],
        protected: &ProtectedContent,
    ) -> Result<Zeroizing<Vec<u8>>, ProtectedContentError> {
        if context.is_empty() {
            return Err(ProtectedContentError::EmptyContext);
        }
        let envelope = protected.as_bytes();
        validate_envelope(envelope)?;
        let key_id_length = usize::from(envelope[1]);
        let key_id_end = 2 + key_id_length;
        let key_id = std::str::from_utf8(&envelope[2..key_id_end])
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let key_id =
            DottedKey::new(key_id).map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let keys = self
            .keys
            .read()
            .map_err(|_| ProtectedContentError::Unavailable)?;
        let key = keys
            .as_ref()
            .ok_or(ProtectedContentError::Unavailable)?
            .get(&key_id)
            .ok_or(ProtectedContentError::UnknownKey)?;
        let nonce_end = key_id_end + NONCE_BYTES;
        let nonce: &XNonce = (&envelope[key_id_end..nonce_end])
            .try_into()
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let key: &Key = key
            .as_slice()
            .try_into()
            .map_err(|_| ProtectedContentError::MalformedEnvelope)?;
        let plaintext = XChaCha20Poly1305::new(key)
            .decrypt(
                nonce,
                Payload {
                    msg: &envelope[nonce_end..],
                    aad: context,
                },
            )
            .map_err(|_| ProtectedContentError::DecryptFailed)?;
        if plaintext.len() > MAX_PLAINTEXT_BYTES {
            return Err(ProtectedContentError::InvalidPlaintext);
        }
        Ok(Zeroizing::new(plaintext))
    }
}

fn nonce_domain(key_id: &DottedKey) -> &'static [u8] {
    if key_id.as_str().starts_with("musteragent.legacy.") {
        LEGACY_NONCE_DOMAIN
    } else {
        NONCE_DOMAIN
    }
}

fn validate_envelope(value: &[u8]) -> Result<(), ProtectedContentError> {
    if value.len() < 2 + 1 + NONCE_BYTES + TAG_BYTES || value[0] != ENVELOPE_VERSION {
        return Err(ProtectedContentError::MalformedEnvelope);
    }
    let key_id_length = usize::from(value[1]);
    if key_id_length == 0
        || key_id_length > MAX_KEY_ID_BYTES
        || value.len() < 2 + key_id_length + NONCE_BYTES + TAG_BYTES
        || value.len() > MAX_PLAINTEXT_BYTES + 2 + MAX_KEY_ID_BYTES + NONCE_BYTES + TAG_BYTES
    {
        return Err(ProtectedContentError::MalformedEnvelope);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ProtectedContentError {
    #[error("protected-content keyring is unavailable")]
    Unavailable,
    #[error("protected-content active encryption key is missing")]
    ActiveKeyMissing,
    #[error("protected-content key ID is duplicated")]
    DuplicateKeyId,
    #[error("protected-content key ID is too long")]
    KeyIdTooLong,
    #[error("protected-content authentication context cannot be empty")]
    EmptyContext,
    #[error("protected-content plaintext is empty or too long")]
    InvalidPlaintext,
    #[error("protected-content encryption failed")]
    EncryptFailed,
    #[error("protected-content envelope is malformed")]
    MalformedEnvelope,
    #[error("protected-content envelope uses an unavailable key generation")]
    UnknownKey,
    #[error("protected-content authentication or decryption failed")]
    DecryptFailed,
}

#[cfg(test)]
mod tests {
    use musteragent_identity::DottedKey;

    use super::{
        LEGACY_NONCE_DOMAIN, NONCE_DOMAIN, ProtectedContentError, ProtectedContentKeyring,
        nonce_domain,
    };

    fn key_id(value: &str) -> DottedKey {
        DottedKey::new(value).expect("key ID")
    }

    #[test]
    fn poisoned_owner_fails_closed_and_revocation_erases_retained_keys() {
        let id = key_id("test.poisoned");
        let keys = ProtectedContentKeyring::new(id.clone(), [(id, [42; 32])]).expect("keys");
        let value = keys.seal(b"context", b"intent").expect("intent");
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
            keys.seal(b"context", b"intent"),
            Err(ProtectedContentError::Unavailable)
        );
        assert!(matches!(
            keys.open(b"context", &value),
            Err(ProtectedContentError::Unavailable)
        ));
        keys.revoke();
        assert!(
            keys.keys
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .is_none()
        );
    }

    #[test]
    fn large_envelope_is_context_bound_deterministic_and_rotatable() {
        let old = key_id("runtime.content.old");
        let active = key_id("runtime.content.active");
        let keyring = ProtectedContentKeyring::new(
            active.clone(),
            [(old.clone(), [7_u8; 32]), (active.clone(), [9_u8; 32])],
        )
        .expect("keyring");
        let plaintext = vec![b'x'; 8 * 1_048_576];
        let first = keyring
            .seal(b"tenant:a:run:a:invocation:a", &plaintext)
            .expect("seal");
        let second = keyring
            .seal(b"tenant:a:run:a:invocation:a", &plaintext)
            .expect("seal");
        assert_eq!(first, second);
        assert_eq!(first.key_id().expect("key"), active);
        assert_eq!(
            keyring
                .open(b"tenant:a:run:a:invocation:a", &first)
                .expect("open")
                .as_slice(),
            plaintext
        );
        assert_eq!(
            keyring.open(b"tenant:b:run:a:invocation:a", &first),
            Err(ProtectedContentError::DecryptFailed)
        );

        let rotated = ProtectedContentKeyring::new(
            key_id("runtime.content.next"),
            [
                (old, [7_u8; 32]),
                (active, [9_u8; 32]),
                (key_id("runtime.content.next"), [11_u8; 32]),
            ],
        )
        .expect("rotated");
        assert!(rotated.open(b"tenant:a:run:a:invocation:a", &first).is_ok());
        let removed = ProtectedContentKeyring::new(
            key_id("runtime.content.next"),
            [(key_id("runtime.content.next"), [11_u8; 32])],
        )
        .expect("removed");
        assert_eq!(
            removed.open(b"tenant:a:run:a:invocation:a", &first),
            Err(ProtectedContentError::UnknownKey)
        );
    }

    #[test]
    fn nonce_domains_separate_retained_and_current_keys() {
        assert_eq!(
            nonce_domain(&key_id("musteragent.legacy.device.recovery-content.v1")),
            LEGACY_NONCE_DOMAIN
        );
        assert_eq!(
            nonce_domain(&key_id("musteragent.device.recovery-content.v2")),
            NONCE_DOMAIN
        );
        assert_eq!(nonce_domain(&key_id("runtime.content.next")), NONCE_DOMAIN);
    }
}
