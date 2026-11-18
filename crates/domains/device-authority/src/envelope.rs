use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    Authority, BackendKeyId, CanonicalContract, ContractError, ENVELOPE_MAX_BYTES,
    PAYLOAD_MAX_BYTES, Result, Signature, SigningPayload, atoms::decode_base64, canonical_bytes,
    decode_canonical, tags::BackendEnvelopeSchema,
};

/// Raw canonical bytes are retained for signature verification; callers never
/// verify a reconstructed or normalized payload. Not proof of signature trust.
#[derive(Clone, PartialEq, Eq)]
pub struct PayloadBytes(Vec<u8>);
impl PayloadBytes {
    pub fn from_payload<T: SigningPayload>(payload: &T) -> Result<Self> {
        Self::try_from(canonical_bytes(payload)?)
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Debug for PayloadBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadBytes")
            .field("len", &self.0.len())
            .finish()
    }
}
impl TryFrom<Vec<u8>> for PayloadBytes {
    type Error = ContractError;
    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() > PAYLOAD_MAX_BYTES {
            return Err(ContractError::TooLarge);
        }
        Ok(Self(bytes))
    }
}
impl Serialize for PayloadBytes {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(&self.0))
    }
}
impl<'de> Deserialize<'de> for PayloadBytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let encoded = String::deserialize(d)?;
        if encoded.len() > PAYLOAD_MAX_BYTES.div_ceil(3) * 4 {
            return Err(serde::de::Error::custom("payload exceeds byte limit"));
        }
        Self::try_from(decode_base64(&encoded).map_err(serde::de::Error::custom)?)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendPayload {
    Authority(Box<Authority>),
}
macro_rules! payload_enum {
    ($name:ident, $($variant:ident),+) => {
        $(impl From<$variant> for $name {
            fn from(value: $variant) -> Self { Self::$variant(Box::new(value)) }
        })+
        impl CanonicalContract for $name {
            fn validate(&self) -> Result<()> { match self { $(Self::$variant(v) => v.validate()),+ } }
        }
        impl SigningPayload for $name {
            fn domain(&self) -> &'static [u8] { match self { $(Self::$variant(v) => v.domain()),+ } }
        }
    };
}
payload_enum!(BackendPayload, Authority);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendEnvelope {
    pub schema_version: BackendEnvelopeSchema,
    pub key_id: BackendKeyId,
    pub payload_base64: PayloadBytes,
    pub signature_base64: Signature,
}
impl BackendEnvelope {
    pub fn decode_payload(&self) -> Result<BackendPayload> {
        decode_canonical(self.payload_base64.as_bytes())
    }
    pub fn domain(&self) -> Result<&'static [u8]> {
        Ok(self.decode_payload()?.domain())
    }
    pub fn payload_bytes(&self) -> &[u8] {
        self.payload_base64.as_bytes()
    }
    pub fn signature(&self) -> &[u8; 64] {
        self.signature_base64.as_bytes()
    }
    pub fn signing_input(&self) -> Result<Vec<u8>> {
        let mut bytes = self.domain()?.to_vec();
        bytes.extend_from_slice(self.payload_bytes());
        Ok(bytes)
    }
}
impl CanonicalContract for BackendEnvelope {
    const MAX_BYTES: usize = ENVELOPE_MAX_BYTES;
    fn validate(&self) -> Result<()> {
        self.decode_payload().map(|_| ())
    }
}
