use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use base64::Engine as _;
use musteragent_identity::DottedKey;
use serde::Deserialize;
use thiserror::Error;

pub const OPAQUE_KEYRING_SCHEMA_VERSION: &str = "musteragent.opaque-keyring.v2";
const LEGACY_KERNEL_OPAQUE_KEYRING_SCHEMA_VERSION: &str = "kernel.opaque-keyring.v1";
const MAX_KEYRING_BYTES: u64 = 64 * 1024;
const MAX_KEYS: usize = 32;

/// Secret key material shared by API and Worker process generations.
///
/// The active key seals new opaque references. Every retained key can open
/// historical references, which makes rotation non-disruptive.
pub struct OpaqueKeyringMaterial {
    active_key_id: DottedKey,
    keys: BTreeMap<DottedKey, [u8; 32]>,
}

impl OpaqueKeyringMaterial {
    #[must_use]
    pub const fn active_key_id(&self) -> &DottedKey {
        &self.active_key_id
    }

    pub fn keys(&self) -> impl Iterator<Item = (DottedKey, [u8; 32])> + '_ {
        self.keys.iter().map(|(key_id, key)| (key_id.clone(), *key))
    }
}

/// Loads exactly one opaque-reference key source.
///
/// Development and test may use a single inline active key. Protected
/// deployments require a mounted keyring document so a new active key and
/// retired decryption keys can overlap during rolling upgrades.
pub fn load_opaque_keyring(
    active_key_id: &str,
    inline_active_key: Option<&str>,
    keyring_file: Option<&Path>,
    protected_deployment: bool,
) -> Result<OpaqueKeyringMaterial, OpaqueKeyringConfigError> {
    let active_key_id = DottedKey::new(active_key_id)?;
    if protected_deployment && keyring_file.is_none() {
        return Err(OpaqueKeyringConfigError::ProtectedFileRequired);
    }
    match (inline_active_key, keyring_file) {
        (Some(encoded), None) => {
            let key = decode_key(encoded)?;
            Ok(OpaqueKeyringMaterial {
                active_key_id: active_key_id.clone(),
                keys: BTreeMap::from([(active_key_id, key)]),
            })
        }
        (None, Some(path)) => load_document(active_key_id, path),
        (Some(_), Some(_)) | (None, None) => Err(OpaqueKeyringConfigError::InvalidSource),
    }
}

fn load_document(
    active_key_id: DottedKey,
    path: &Path,
) -> Result<OpaqueKeyringMaterial, OpaqueKeyringConfigError> {
    let metadata = fs::metadata(path).map_err(|_| OpaqueKeyringConfigError::UnreadableFile)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_KEYRING_BYTES {
        return Err(OpaqueKeyringConfigError::UnreadableFile);
    }
    let bytes = fs::read(path).map_err(|_| OpaqueKeyringConfigError::UnreadableFile)?;
    let document: KeyringDocument =
        serde_json::from_slice(&bytes).map_err(|_| OpaqueKeyringConfigError::InvalidDocument)?;
    if !matches!(
        document.schema_version.as_str(),
        OPAQUE_KEYRING_SCHEMA_VERSION | LEGACY_KERNEL_OPAQUE_KEYRING_SCHEMA_VERSION
    ) || document.keys.is_empty()
        || document.keys.len() > MAX_KEYS
    {
        return Err(OpaqueKeyringConfigError::InvalidDocument);
    }
    let mut seen = BTreeSet::new();
    let mut keys = BTreeMap::new();
    for entry in document.keys {
        let key_id =
            DottedKey::new(entry.key_id).map_err(|_| OpaqueKeyringConfigError::InvalidDocument)?;
        if !seen.insert(key_id.clone()) {
            return Err(OpaqueKeyringConfigError::DuplicateKey);
        }
        let key = decode_key(&entry.key_base64)?;
        keys.insert(key_id, key);
    }
    if !keys.contains_key(&active_key_id) {
        return Err(OpaqueKeyringConfigError::ActiveKeyMissing);
    }
    Ok(OpaqueKeyringMaterial {
        active_key_id,
        keys,
    })
}

fn decode_key(value: &str) -> Result<[u8; 32], OpaqueKeyringConfigError> {
    if value.trim() != value || value.chars().any(char::is_whitespace) {
        return Err(OpaqueKeyringConfigError::InvalidKey);
    }
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| OpaqueKeyringConfigError::InvalidKey)?
        .try_into()
        .map_err(|_| OpaqueKeyringConfigError::InvalidKey)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyringDocument {
    schema_version: String,
    keys: Vec<KeyDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyDocument {
    key_id: String,
    key_base64: String,
}

#[derive(Debug, Error)]
pub enum OpaqueKeyringConfigError {
    #[error("configure exactly one opaque-reference key source")]
    InvalidSource,
    #[error("protected deployments require a mounted opaque-reference keyring file")]
    ProtectedFileRequired,
    #[error("opaque-reference keyring file is unreadable or outside the size bound")]
    UnreadableFile,
    #[error("opaque-reference keyring document is invalid")]
    InvalidDocument,
    #[error("opaque-reference keyring contains a duplicate key ID")]
    DuplicateKey,
    #[error("opaque-reference active key is absent from the keyring")]
    ActiveKeyMissing,
    #[error("opaque-reference key must be canonical base64 containing exactly 32 bytes")]
    InvalidKey,
    #[error(transparent)]
    Identity(#[from] musteragent_identity::LogicalKeyError),
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn inline_key_is_allowed_only_as_the_single_source() {
        let encoded = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let material =
            load_opaque_keyring("runtime.ref.active", Some(&encoded), None, false).expect("inline");
        assert_eq!(material.active_key_id().as_str(), "runtime.ref.active");
        assert_eq!(material.keys().count(), 1);
        assert!(matches!(
            load_opaque_keyring("runtime.ref.active", Some(&encoded), None, true),
            Err(OpaqueKeyringConfigError::ProtectedFileRequired)
        ));
    }

    #[test]
    fn mounted_keyring_retains_old_keys_for_rotation() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        let old = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let active = base64::engine::general_purpose::STANDARD.encode([9_u8; 32]);
        write!(
            file,
            "{{\"schema_version\":\"{OPAQUE_KEYRING_SCHEMA_VERSION}\",\"keys\":[\
             {{\"key_id\":\"runtime.ref.old\",\"key_base64\":\"{old}\"}},\
             {{\"key_id\":\"runtime.ref.active\",\"key_base64\":\"{active}\"}}]}}"
        )
        .expect("write");
        let material =
            load_opaque_keyring("runtime.ref.active", None, Some(file.path()), true).expect("file");
        assert_eq!(material.keys().count(), 2);
    }

    #[test]
    fn historical_kernel_keyring_remains_readable_during_namespace_cutover() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        let active = base64::engine::general_purpose::STANDARD.encode([9_u8; 32]);
        write!(
            file,
            "{{\"schema_version\":\"{LEGACY_KERNEL_OPAQUE_KEYRING_SCHEMA_VERSION}\",\"keys\":[\
             {{\"key_id\":\"musteragent.ref.active\",\"key_base64\":\"{active}\"}}]}}"
        )
        .expect("write");

        let material = load_opaque_keyring("musteragent.ref.active", None, Some(file.path()), true)
            .expect("legacy document remains readable");
        assert_eq!(material.keys().count(), 1);
    }
}
