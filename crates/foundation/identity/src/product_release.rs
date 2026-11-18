//! A product release is an immutable execution identity, not a feature flag.
//!
//! The kernel does not know any concrete product. A host registers its own
//! product key (for example `acme-agent`) and publishes immutable releases of
//! its tool catalog and contracts under that key. Runs bind to the exact
//! release they were admitted with.
use crate::VersionedKey;
use thiserror::Error;

const MAX_PRODUCT_KEY_BYTES: usize = 32;

/// Host-defined product key: `[a-z][a-z0-9-]{0,31}`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProductId {
    len: u8,
    bytes: [u8; MAX_PRODUCT_KEY_BYTES],
}

impl ProductId {
    /// The product key used by the kernel's own reference catalog.
    pub const KERNEL: Self = Self::from_static("musteragent");

    const fn from_static(value: &str) -> Self {
        let source = value.as_bytes();
        let mut bytes = [0; MAX_PRODUCT_KEY_BYTES];
        let mut index = 0;
        while index < source.len() {
            bytes[index] = source[index];
            index += 1;
        }
        Self {
            len: source.len() as u8,
            bytes,
        }
    }

    pub fn parse(value: &str) -> Result<Self, ProductReleaseIdentityError> {
        let bytes = value.as_bytes();
        let valid = !bytes.is_empty()
            && bytes.len() <= MAX_PRODUCT_KEY_BYTES
            && bytes[0].is_ascii_lowercase()
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
        if !valid {
            return Err(ProductReleaseIdentityError);
        }
        Ok(Self::from_static(value))
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.len)])
            .expect("product key is validated ASCII")
    }
}

impl std::fmt::Debug for ProductId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ProductId")
            .field(&self.as_str())
            .finish()
    }
}

impl std::fmt::Display for ProductId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProductReleaseRef {
    pub product: ProductId,
    pub release_id: VersionedKey,
    pub digest: [u8; 32],
}

impl ProductReleaseRef {
    pub fn new(
        product: ProductId,
        release_id: VersionedKey,
        digest: [u8; 32],
    ) -> Result<Self, ProductReleaseIdentityError> {
        if digest == [0; 32] {
            return Err(ProductReleaseIdentityError);
        }
        Ok(Self {
            product,
            release_id,
            digest,
        })
    }

    /// Domain-separated, length-delimited publication binding.
    pub fn bind_canonical(&self, domain: &[u8], original: &[u8]) -> Vec<u8> {
        let mut encoded = domain.to_vec();
        for value in [
            original,
            self.product.as_str().as_bytes(),
            self.release_id.as_str().as_bytes(),
            &self.digest,
        ] {
            encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
            encoded.extend_from_slice(value);
        }
        encoded
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("invalid product release identity")]
pub struct ProductReleaseIdentityError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_keys_are_closed() {
        assert_eq!(ProductId::KERNEL.as_str(), "musteragent");
        assert_eq!(
            ProductId::parse("acme-agent").map(|p| p.to_string()),
            Ok("acme-agent".into())
        );
        for invalid in ["", "Acme", "1acme", "acme_agent", &"a".repeat(33)] {
            assert!(ProductId::parse(invalid).is_err(), "{invalid}");
        }
    }
}
