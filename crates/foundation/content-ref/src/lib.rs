//! Content-addressed references shared without leaking storage implementations.

#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};

use musteragent_identity::{ContentId, VersionedKey};
use sha2::{Digest, Sha256 as Sha256Hasher};
use thiserror::Error;

/// A SHA-256 digest with fixed width at the type boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Sha256([u8; Self::LENGTH]);

impl Sha256 {
    pub const LENGTH: usize = 32;

    /// Hashes bytes with SHA-256.
    #[must_use]
    pub fn digest(bytes: impl AsRef<[u8]>) -> Self {
        Self(Sha256Hasher::digest(bytes.as_ref()).into())
    }

    /// Validates a wire digest.
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, DigestLengthError> {
        let bytes = bytes.as_ref();
        let value: [u8; Self::LENGTH] = bytes.try_into().map_err(|_| DigestLengthError {
            actual: bytes.len(),
        })?;
        Ok(Self(value))
    }

    /// Parses the canonical lowercase digest used by immutable component manifests.
    pub fn from_hex(value: &str) -> Result<Self, DigestHexError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(DigestHexError);
        }
        let mut bytes = [0; Self::LENGTH];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| DigestHexError)?;
        }
        Ok(Self(bytes))
    }

    /// Returns the fixed-width digest.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; Self::LENGTH] {
        self.0
    }
}

impl Display for Sha256 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("SHA-256 hex must contain exactly 64 lowercase hexadecimal characters")]
pub struct DigestHexError;

/// A digest did not contain exactly 32 bytes.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("SHA-256 digest must be 32 bytes, got {actual}")]
pub struct DigestLengthError {
    actual: usize,
}

/// A bounded media type carried by a content reference.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MediaType(String);

impl MediaType {
    /// Validates a compact `type/subtype` media type.
    pub fn new(value: impl Into<String>) -> Result<Self, MediaTypeError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 255
            && value.is_ascii()
            && value.contains('/')
            && value
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace());
        if !valid {
            return Err(MediaTypeError);
        }
        Ok(Self(value))
    }

    /// Returns the wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A media type violated the bounded protocol grammar.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("media type must be bounded visible ASCII in type/subtype form")]
pub struct MediaTypeError;

/// An immutable reference to bytes owned by a Content Store.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ContentRef {
    pub content_id: ContentId,
    pub media_type: MediaType,
    pub byte_length: u64,
    pub sha256: Sha256,
}

/// A versioned JSON Schema reference with a pinned digest.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SchemaRef {
    pub schema_key: VersionedKey,
    pub schema_hash: Sha256,
}

#[cfg(test)]
mod tests {
    use super::{MediaType, Sha256};

    #[test]
    fn digest_is_fixed_width_and_stable() {
        let digest = Sha256::digest(b"legacy-assistant");
        assert_eq!(digest.into_bytes().len(), Sha256::LENGTH);
        assert_eq!(digest.to_string().len(), Sha256::LENGTH * 2);
        assert!(Sha256::from_bytes([0_u8; 31]).is_err());
    }

    #[test]
    fn media_type_rejects_unbounded_or_ambiguous_values() {
        assert!(MediaType::new("application/json").is_ok());
        assert!(MediaType::new("json").is_err());
        assert!(MediaType::new("text/plain utf-8").is_err());
    }
}
