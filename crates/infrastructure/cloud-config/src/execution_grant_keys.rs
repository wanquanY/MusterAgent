use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use base64::Engine as _;
use musteragent_identity::DottedKey;
use serde::Deserialize;
use thiserror::Error;

pub const EXECUTION_GRANT_KEYRING_SCHEMA_VERSION: &str =
    "musteragent.execution-grant-verifying-keyring.v1";
const LEGACY_EXECUTION_GRANT_KEYRING_SCHEMA_VERSION: &str =
    "kernel.execution-grant-verifying-keyring.v1";
const MAX_KEYRING_BYTES: u64 = 64 * 1024;
const MAX_KEYS: usize = 32;

/// Public verification material retained across an Execution Grant signer rotation.
///
/// File order is meaningful: protected deployments must publish one monotonic
/// version series (`<owner>.v1`, `<owner>.v2`, ...). The Cloud admission owner
/// accepts every retained key and never receives a signing seed.
pub struct ExecutionGrantKeyringMaterial {
    keys: Vec<(DottedKey, [u8; 32])>,
}

impl ExecutionGrantKeyringMaterial {
    pub fn keys(&self) -> impl Iterator<Item = (DottedKey, [u8; 32])> + '_ {
        self.keys.iter().map(|(key_id, key)| (key_id.clone(), *key))
    }
}

/// Load exactly one public verification-key source.
///
/// Development and test may use one inline key. Staging and production require
/// a mounted ordered keyring so public-key publication can precede signer cutover.
pub fn load_execution_grant_keyring(
    inline_key_id: Option<&str>,
    inline_key: Option<&str>,
    keyring_file: Option<&Path>,
    protected_deployment: bool,
) -> Result<ExecutionGrantKeyringMaterial, ExecutionGrantKeyringConfigError> {
    if protected_deployment && keyring_file.is_none() {
        return Err(ExecutionGrantKeyringConfigError::ProtectedFileRequired);
    }
    match (inline_key_id, inline_key, keyring_file) {
        (Some(key_id), Some(encoded), None) => Ok(ExecutionGrantKeyringMaterial {
            keys: vec![(DottedKey::new(key_id)?, decode_key(encoded)?)],
        }),
        (None, None, Some(path)) => load_document(path),
        _ => Err(ExecutionGrantKeyringConfigError::InvalidSource),
    }
}

fn load_document(
    path: &Path,
) -> Result<ExecutionGrantKeyringMaterial, ExecutionGrantKeyringConfigError> {
    let metadata =
        fs::metadata(path).map_err(|_| ExecutionGrantKeyringConfigError::UnreadableFile)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_KEYRING_BYTES {
        return Err(ExecutionGrantKeyringConfigError::UnreadableFile);
    }
    let bytes = fs::read(path).map_err(|_| ExecutionGrantKeyringConfigError::UnreadableFile)?;
    let document: KeyringDocument = serde_json::from_slice(&bytes)
        .map_err(|_| ExecutionGrantKeyringConfigError::InvalidDocument)?;
    if !matches!(
        document.schema_version.as_str(),
        EXECUTION_GRANT_KEYRING_SCHEMA_VERSION | LEGACY_EXECUTION_GRANT_KEYRING_SCHEMA_VERSION
    ) || document.keys.is_empty()
        || document.keys.len() > MAX_KEYS
    {
        return Err(ExecutionGrantKeyringConfigError::InvalidDocument);
    }
    let mut keys = Vec::with_capacity(document.keys.len());
    let mut seen = BTreeSet::new();
    let mut series: Option<String> = None;
    let mut previous_version = 0_u64;
    for entry in document.keys {
        let key_id = DottedKey::new(entry.key_id)
            .map_err(|_| ExecutionGrantKeyringConfigError::InvalidDocument)?;
        if !seen.insert(key_id.clone()) {
            return Err(ExecutionGrantKeyringConfigError::DuplicateKey);
        }
        let (current_series, version) = versioned_key_id(key_id.as_str())?;
        if series
            .as_deref()
            .is_some_and(|value| value != current_series)
            || version <= previous_version
        {
            return Err(ExecutionGrantKeyringConfigError::InvalidOrder);
        }
        series.get_or_insert_with(|| current_series.to_owned());
        previous_version = version;
        keys.push((key_id, decode_key(&entry.key_base64)?));
    }
    Ok(ExecutionGrantKeyringMaterial { keys })
}

fn versioned_key_id(value: &str) -> Result<(&str, u64), ExecutionGrantKeyringConfigError> {
    let (series, version) = value
        .rsplit_once(".v")
        .ok_or(ExecutionGrantKeyringConfigError::InvalidVersionedKeyId)?;
    if series.is_empty()
        || version.is_empty()
        || version.starts_with('0')
        || !version.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ExecutionGrantKeyringConfigError::InvalidVersionedKeyId);
    }
    let version = version
        .parse::<u64>()
        .map_err(|_| ExecutionGrantKeyringConfigError::InvalidVersionedKeyId)?;
    Ok((series, version))
}

fn decode_key(value: &str) -> Result<[u8; 32], ExecutionGrantKeyringConfigError> {
    if value.trim() != value || value.chars().any(char::is_whitespace) {
        return Err(ExecutionGrantKeyringConfigError::InvalidKey);
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| ExecutionGrantKeyringConfigError::InvalidKey)?;
    if base64::engine::general_purpose::STANDARD.encode(&decoded) != value {
        return Err(ExecutionGrantKeyringConfigError::InvalidKey);
    }
    decoded
        .try_into()
        .map_err(|_| ExecutionGrantKeyringConfigError::InvalidKey)
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
pub enum ExecutionGrantKeyringConfigError {
    #[error("configure exactly one Execution Grant verification-key source")]
    InvalidSource,
    #[error("protected deployments require a mounted Execution Grant verification keyring")]
    ProtectedFileRequired,
    #[error("Execution Grant verification keyring is unreadable or outside the size bound")]
    UnreadableFile,
    #[error("Execution Grant verification keyring document is invalid")]
    InvalidDocument,
    #[error("Execution Grant verification keyring contains a duplicate key ID")]
    DuplicateKey,
    #[error("Execution Grant verification key IDs must use a positive .vN suffix")]
    InvalidVersionedKeyId,
    #[error("Execution Grant verification key IDs must be one strictly ordered version series")]
    InvalidOrder,
    #[error(
        "Execution Grant verification key must be canonical base64 containing exactly 32 bytes"
    )]
    InvalidKey,
    #[error(transparent)]
    Identity(#[from] musteragent_identity::LogicalKeyError),
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn protected_keyring_requires_an_ordered_version_series() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        let old = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        let new = base64::engine::general_purpose::STANDARD.encode([9_u8; 32]);
        write!(
            file,
            "{{\"schema_version\":\"{EXECUTION_GRANT_KEYRING_SCHEMA_VERSION}\",\"keys\":[\
             {{\"key_id\":\"musteragent.execution-grant.v1\",\"key_base64\":\"{old}\"}},\
             {{\"key_id\":\"musteragent.execution-grant.v2\",\"key_base64\":\"{new}\"}}]}}"
        )
        .expect("write keyring");
        let material = load_execution_grant_keyring(None, None, Some(file.path()), true)
            .expect("protected keyring");
        assert_eq!(material.keys().count(), 2);

        assert!(matches!(
            load_execution_grant_keyring(
                Some("musteragent.execution-grant.v1"),
                Some(&old),
                None,
                true
            ),
            Err(ExecutionGrantKeyringConfigError::ProtectedFileRequired)
        ));
    }

    #[test]
    fn unordered_or_cross_series_keyrings_fail_closed() {
        for ids in [
            [
                "musteragent.execution-grant.v2",
                "musteragent.execution-grant.v1",
            ],
            ["musteragent.execution-grant.v1", "other.execution-grant.v2"],
        ] {
            let mut file = tempfile::NamedTempFile::new().expect("temp file");
            let key = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
            write!(
                file,
                "{{\"schema_version\":\"{EXECUTION_GRANT_KEYRING_SCHEMA_VERSION}\",\"keys\":[\
                 {{\"key_id\":\"{}\",\"key_base64\":\"{key}\"}},\
                 {{\"key_id\":\"{}\",\"key_base64\":\"{key}\"}}]}}",
                ids[0], ids[1]
            )
            .expect("write keyring");
            assert!(matches!(
                load_execution_grant_keyring(None, None, Some(file.path()), true),
                Err(ExecutionGrantKeyringConfigError::InvalidOrder)
            ));
        }
    }

    #[test]
    fn legacy_keyring_schema_remains_readable_during_rotation() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        let key = base64::engine::general_purpose::STANDARD.encode([7_u8; 32]);
        write!(
            file,
            "{{\"schema_version\":\"{LEGACY_EXECUTION_GRANT_KEYRING_SCHEMA_VERSION}\",\"keys\":[\
             {{\"key_id\":\"kernel.execution-grant.v1\",\"key_base64\":\"{key}\"}}]}}"
        )
        .expect("write legacy keyring");

        let material = load_execution_grant_keyring(None, None, Some(file.path()), true)
            .expect("legacy public keyring remains readable");
        assert_eq!(material.keys().count(), 1);
    }
}
