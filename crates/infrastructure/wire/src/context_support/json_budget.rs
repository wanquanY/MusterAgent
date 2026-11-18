//! Validate JSON allocation complexity without first building a Value tree.
use super::{ContextSupportError, Result};
use serde::de::{DeserializeSeed, Deserializer, Error, MapAccess, SeqAccess, Visitor};
use std::fmt;

const MAX_NODES: usize = 16_384;
const MAX_DEPTH: usize = 64;
pub(super) fn check(bytes: &[u8]) -> Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let mut remaining = MAX_NODES;
    Seed {
        remaining: &mut remaining,
        depth: 0,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| ContextSupportError::Invalid)?;
    deserializer.end().map_err(|_| ContextSupportError::Invalid)
}
struct Seed<'a> {
    remaining: &'a mut usize,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        if self.depth > MAX_DEPTH || *self.remaining == 0 {
            return Err(D::Error::custom("Journal JSON budget exceeded"));
        }
        *self.remaining -= 1;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("bounded Journal JSON")
    }
    fn visit_bool<E: Error>(self, _: bool) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: Error>(self, _: i64) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: Error>(self, _: u64) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: Error>(self, n: f64) -> std::result::Result<(), E> {
        if n.is_finite() {
            Ok(())
        } else {
            Err(E::custom("nonfinite JSON number"))
        }
    }
    fn visit_str<E: Error>(self, _: &str) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: Error>(self) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<(), A::Error> {
        while sequence
            .next_element_seed(Seed {
                remaining: self.remaining,
                depth: self.depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        while map
            .next_key_seed(Seed {
                remaining: self.remaining,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            map.next_value_seed(Seed {
                remaining: self.remaining,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }
}
