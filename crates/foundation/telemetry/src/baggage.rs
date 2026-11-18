use std::collections::BTreeMap;

use thiserror::Error;

const MAX_PAIRS: usize = 4;
const MAX_VALUE_BYTES: usize = 64;

/// Bounded non-domain hints allowed to cross a synchronous process boundary.
///
/// Canonical identities are span attributes, never baggage. Tenant, principal,
/// content, credential and opaque references are intentionally absent from the
/// vocabulary so they cannot be copied into third-party requests by accident.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ControlledBaggage(BTreeMap<&'static str, String>);

impl ControlledBaggage {
    pub fn parse(value: &str) -> Result<Self, ControlledBaggageError> {
        if value.is_empty() {
            return Ok(Self::default());
        }
        let mut entries = BTreeMap::new();
        for item in value.split(',') {
            let (raw_key, raw_value) = item
                .trim()
                .split_once('=')
                .ok_or(ControlledBaggageError::InvalidPair)?;
            let key = match raw_key.trim() {
                "musteragent.runtime.surface" | "musteragent.legacy.runtime.surface" => {
                    "musteragent.runtime.surface"
                }
                "musteragent.runtime.trigger" | "musteragent.legacy.runtime.trigger" => {
                    "musteragent.runtime.trigger"
                }
                "musteragent.debug.mode" | "musteragent.legacy.debug.mode" => {
                    "musteragent.debug.mode"
                }
                _ => return Err(ControlledBaggageError::KeyNotAllowed),
            };
            let value = raw_value.trim();
            if value.is_empty()
                || value.len() > MAX_VALUE_BYTES
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            {
                return Err(ControlledBaggageError::InvalidValue);
            }
            if entries.insert(key, value.to_owned()).is_some() {
                return Err(ControlledBaggageError::DuplicateKey);
            }
            if entries.len() > MAX_PAIRS {
                return Err(ControlledBaggageError::TooManyPairs);
            }
        }
        Ok(Self(entries))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.0.iter().map(|(key, value)| (*key, value.as_str()))
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ControlledBaggageError {
    #[error("baggage pair is malformed")]
    InvalidPair,
    #[error("baggage key is not in the Runtime allowlist")]
    KeyNotAllowed,
    #[error("baggage value is empty, oversized or unsafe")]
    InvalidValue,
    #[error("baggage key is duplicated")]
    DuplicateKey,
    #[error("baggage contains too many pairs")]
    TooManyPairs,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controlled_baggage_rejects_identity_and_secret_shaped_fields() {
        assert!(ControlledBaggage::parse("tenant_id=tenant-a").is_err());
        assert!(ControlledBaggage::parse("authorization=Bearer-token").is_err());
        assert!(ControlledBaggage::parse("musteragent.runtime.surface=web").is_ok());
        assert_eq!(
            ControlledBaggage::parse("musteragent.legacy.runtime.surface=web")
                .expect("legacy baggage")
                .entries()
                .collect::<Vec<_>>(),
            vec![("musteragent.runtime.surface", "web")]
        );
    }
}
