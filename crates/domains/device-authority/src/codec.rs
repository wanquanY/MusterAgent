use std::collections::BTreeMap;

use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, MapAccess, Visitor},
};
use serde_json::Value;

use crate::{ContractError, Result, Sha256Hex};

pub const PAYLOAD_MAX_BYTES: usize = 16 * 1024;
pub const ENVELOPE_MAX_BYTES: usize = 24 * 1024;
pub const HTTP_MAX_BYTES: usize = 64 * 1024;

/// Semantic validation is also run before encoding Rust-constructed values.
pub trait CanonicalContract: Serialize + DeserializeOwned {
    const MAX_BYTES: usize = PAYLOAD_MAX_BYTES;
    fn validate(&self) -> Result<()>;
}

pub fn canonical_bytes<T: CanonicalContract>(value: &T) -> Result<Vec<u8>> {
    value.validate()?;
    let bytes = encode_value(&serde_json::to_value(value)?)?;
    if bytes.len() > T::MAX_BYTES {
        return Err(ContractError::TooLarge);
    }
    Ok(bytes)
}

pub fn digest<T: CanonicalContract>(value: &T) -> Result<Sha256Hex> {
    Ok(Sha256Hex::of_bytes(&canonical_bytes(value)?))
}

pub fn decode_canonical<T: CanonicalContract>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > T::MAX_BYTES {
        return Err(ContractError::TooLarge);
    }
    let value = strict_value(bytes)?;
    if encode_value(&value)? != bytes {
        return Err(ContractError::NonCanonical);
    }
    let parsed: T = serde_json::from_value(value)?;
    parsed.validate()?;
    // Closed serde types must not ignore fields or normalize representations.
    if canonical_bytes(&parsed)? != bytes {
        return Err(ContractError::NonCanonical);
    }
    Ok(parsed)
}

pub(crate) fn strict_value(bytes: &[u8]) -> Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictValue::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value.0)
}

/// Explicit sorting keeps canonical bytes stable even if a downstream crate
/// enables serde_json's `preserve_order` feature through Cargo unification.
pub(crate) fn encode_value(value: &Value) -> Result<Vec<u8>> {
    fn write(value: &Value, out: &mut Vec<u8>) -> Result<()> {
        match value {
            Value::Object(fields) => {
                out.push(b'{');
                for (i, (key, value)) in fields
                    .iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .enumerate()
                {
                    if i > 0 {
                        out.push(b',');
                    }
                    serde_json::to_writer(&mut *out, key)?;
                    out.push(b':');
                    write(value, out)?;
                }
                out.push(b'}');
            }
            Value::Array(values) => {
                out.push(b'[');
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    write(value, out)?;
                }
                out.push(b']');
            }
            _ => serde_json::to_writer(out, value)?,
        }
        Ok(())
    }
    let mut bytes = Vec::new();
    write(value, &mut bytes)?;
    Ok(bytes)
}

struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("closed JSON with unique keys")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut fields = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if fields.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    fields.insert(key, map.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(Value::Object(fields)))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictValue>()? {
                    values.push(value.0);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v.into())))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                v: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(v.into())))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(v.into())))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Err(E::custom(
                    "floating point is not in the membership contract",
                ))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Err(E::custom("null is not in the membership contract"))
            }
        }
        d.deserialize_any(StrictVisitor)
    }
}
