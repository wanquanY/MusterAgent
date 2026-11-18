use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::{ContractError, Result};

macro_rules! text_atom {
    ($name:ident, $validate:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl FromStr for $name {
            type Err = ContractError;
            fn from_str(value: &str) -> Result<Self> {
                ($validate)(value)?;
                Ok(Self(value.to_owned()))
            }
        }
        impl TryFrom<&str> for $name {
            type Error = ContractError;
            fn try_from(value: &str) -> Result<Self> {
                value.parse()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

text_atom!(CanonicalUuid, |value: &str| {
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| ContractError::InvalidField("uuid"))?;
    if parsed.is_nil() || parsed.hyphenated().to_string() != value {
        return Err(ContractError::InvalidField("uuid"));
    }
    Ok(())
});

fn validate_hex(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ContractError::InvalidField("sha256"));
    }
    Ok(())
}
text_atom!(Sha256Hex, validate_hex);
impl Sha256Hex {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(
            Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
    }
}

text_atom!(ReleaseRef, |value: &str| {
    validate_hex(
        value
            .strip_prefix("sha256:")
            .ok_or(ContractError::InvalidField("client_release_ref"))?,
    )
});
impl ReleaseRef {
    pub fn from_digest(digest: &Sha256Hex) -> Self {
        Self(format!("sha256:{digest}"))
    }
    pub fn matches(&self, digest: &Sha256Hex) -> bool {
        self.0[7..] == *digest.as_str()
    }
}

// Syntax only: the caller must match its configured origin exactly and enforce
// HTTPS (or its explicit isolated development policy). No normalization occurs.
text_atom!(DeploymentOrigin, |value: &str| {
    if value.len() > 512
        || !value.is_ascii()
        || value.bytes().any(|b| b.is_ascii_control() || b == b' ')
    {
        return Err(ContractError::InvalidField("deployment_origin"));
    }
    let url =
        url::Url::parse(value).map_err(|_| ContractError::InvalidField("deployment_origin"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.origin().ascii_serialization() != value
    {
        return Err(ContractError::InvalidField("deployment_origin"));
    }
    Ok(())
});

text_atom!(BackendKeyId, |value: &str| {
    let valid = (3..=128).contains(&value.len())
        && value.is_ascii()
        && value.split('.').all(|s| {
            s.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        })
        && value.rsplit_once(".v").is_some_and(|(prefix, n)| {
            !prefix.is_empty()
                && !n.is_empty()
                && !n.starts_with('0')
                && n.bytes().all(|b| b.is_ascii_digit())
        });
    if !valid {
        return Err(ContractError::InvalidField("backend_key_id"));
    }
    Ok(())
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PositiveDecimal(i64);
impl PositiveDecimal {
    pub fn new(value: i64) -> Result<Self> {
        if value < 1 {
            return Err(ContractError::InvalidField("positive_decimal"));
        }
        Ok(Self(value))
    }
    pub fn as_i64(self) -> i64 {
        self.0
    }
    pub fn as_u64(self) -> u64 {
        self.0 as u64
    }
    pub fn get(self) -> i64 {
        self.0
    }
    pub fn checked_next(self) -> Result<Self> {
        Self::new(
            self.0
                .checked_add(1)
                .ok_or(ContractError::InvalidField("epoch_overflow"))?,
        )
    }
}
impl fmt::Display for PositiveDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for PositiveDecimal {
    type Err = ContractError;
    fn from_str(value: &str) -> Result<Self> {
        if value.is_empty() || value.starts_with('0') || !value.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(ContractError::InvalidField("positive_decimal"));
        }
        Self::new(
            value
                .parse()
                .map_err(|_| ContractError::InvalidField("positive_decimal"))?,
        )
    }
}
impl TryFrom<&str> for PositiveDecimal {
    type Error = ContractError;
    fn try_from(value: &str) -> Result<Self> {
        value.parse()
    }
}
impl Serialize for PositiveDecimal {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for PositiveDecimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct UnixMillis(u64);
impl UnixMillis {
    pub const MAX: u64 = (1 << 53) - 1;
    pub fn new(value: u64) -> Result<Self> {
        Self::try_from(value)
    }
    pub fn as_u64(self) -> u64 {
        self.0
    }
    pub fn as_i64(self) -> i64 {
        self.0 as i64
    }
    pub fn get(self) -> u64 {
        self.0
    }
}
impl TryFrom<u64> for UnixMillis {
    type Error = ContractError;
    fn try_from(value: u64) -> Result<Self> {
        if !(1..=Self::MAX).contains(&value) {
            return Err(ContractError::InvalidField("unix_millis"));
        }
        Ok(Self(value))
    }
}
impl From<UnixMillis> for u64 {
    fn from(value: UnixMillis) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationId {
    #[serde(rename = "musteragent")]
    Musteragent,
}
impl ApplicationId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Musteragent => "musteragent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoginRevision {
    #[serde(rename = "1")]
    Active,
    #[serde(rename = "2")]
    Revoked,
}
impl LoginRevision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "1",
            Self::Revoked => "2",
        }
    }
}

/// Fixed-length bytes on the wire use canonical padded RFC 4648 base64.
#[derive(Clone, PartialEq, Eq)]
pub struct FixedBase64<const N: usize>([u8; N]);
pub type Nonce = FixedBase64<32>;
pub type PublicKey = FixedBase64<32>;
pub type Signature = FixedBase64<64>;
impl<const N: usize> FixedBase64<N> {
    pub fn new(bytes: [u8; N]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }
    pub fn get(&self) -> &[u8; N] {
        &self.0
    }
    pub fn to_base64(&self) -> String {
        STANDARD.encode(self.0)
    }
}
impl<const N: usize> fmt::Debug for FixedBase64<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedBase64<{N}>")
    }
}
impl<const N: usize> FromStr for FixedBase64<N> {
    type Err = ContractError;
    fn from_str(value: &str) -> Result<Self> {
        let bytes = decode_base64(value)?;
        Ok(Self(bytes.try_into().map_err(|_| {
            ContractError::InvalidField("base64_length")
        })?))
    }
}
impl<const N: usize> TryFrom<&str> for FixedBase64<N> {
    type Error = ContractError;
    fn try_from(value: &str) -> Result<Self> {
        value.parse()
    }
}
impl<const N: usize> Serialize for FixedBase64<N> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_base64())
    }
}
impl<'de, const N: usize> Deserialize<'de> for FixedBase64<N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

pub(crate) fn decode_base64(value: &str) -> Result<Vec<u8>> {
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| ContractError::InvalidField("base64"))?;
    if STANDARD.encode(&decoded) != value {
        return Err(ContractError::NonCanonical);
    }
    Ok(decoded)
}
