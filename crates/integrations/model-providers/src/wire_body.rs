//! Serializer-owned byte boundaries for lossless transport. The Provider
//! mapper remains the only semantic owner; transport receives opaque bytes.

use bytes::Bytes;
use std::ops::Range;

use serde_json::Value;

pub const WIRE_BLOCK_LAYOUT_VERSION: u32 = 1;
pub const MAX_WIRE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_SEMANTIC_BLOCKS: usize = 2048;

/// Exactly the native serde_json request, with ordered, gap-free byte ranges.
/// Debug intentionally reveals neither the request nor content hashes.
pub struct NativeWireBody {
    private_framing_allowed: bool,
    bytes: Bytes,
    blocks: Vec<Range<usize>>,
}

struct WireBodyWriter {
    bytes: Vec<u8>,
    blocks: Vec<Range<usize>>,
    layout_overflow: bool,
}

impl std::fmt::Debug for NativeWireBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeWireBody")
            .field("byte_length", &self.bytes.len())
            .field("blocks", &self.blocks.len())
            .finish_non_exhaustive()
    }
}

impl NativeWireBody {
    pub fn serialize(value: &Value) -> Result<Self, serde_json::Error> {
        Self::serialize_with_capacity(value, 0)
    }

    pub(crate) fn serialize_with_capacity(
        value: &Value,
        capacity: usize,
    ) -> Result<Self, serde_json::Error> {
        let mut result = WireBodyWriter {
            bytes: Vec::with_capacity(capacity),
            blocks: Vec::new(),
            layout_overflow: false,
        };
        match value {
            Value::Object(fields) => {
                result.literal(b"{");
                for (index, (key, value)) in fields.iter().enumerate() {
                    // Keep punctuation apart from history items so appending
                    // an item never changes the preceding item's bytes.
                    if index != 0 {
                        result.literal(b",");
                    }
                    result.value(&Value::String(key.clone()))?;
                    result.literal(b":");
                    match value {
                        Value::Array(items) => {
                            result.literal(b"[");
                            for (index, item) in items.iter().enumerate() {
                                if index != 0 {
                                    result.literal(b",");
                                }
                                result.value(item)?;
                            }
                            result.literal(b"]");
                        }
                        _ => result.value(value)?,
                    }
                }
                result.literal(b"}");
            }
            _ => result.value(value)?,
        }
        if result.layout_overflow {
            // Too many tiny JSON items only change transport partitioning.
            // Keep metadata bounded and preserve every native byte.
            result.blocks = (0..result.bytes.len())
                .step_by(MAX_WIRE_BLOCK_BYTES)
                .map(|start| start..(start + MAX_WIRE_BLOCK_BYTES).min(result.bytes.len()))
                .collect();
        }
        Ok(Self {
            private_framing_allowed: true,
            bytes: result.bytes.into(),
            blocks: result.blocks,
        })
    }

    /// Resource policy for optional framing; native body and authority remain intact.
    pub const fn allows_private_framing(&self) -> bool {
        self.private_framing_allowed
    }

    pub(crate) fn decline_private_framing(&mut self) {
        self.private_framing_allowed = false;
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn blocks(&self) -> &[Range<usize>] {
        &self.blocks
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub(crate) fn shared_bytes(&self) -> Bytes {
        self.bytes.clone()
    }
}

impl WireBodyWriter {
    fn literal(&mut self, bytes: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        self.record(start);
    }

    fn value(&mut self, value: &Value) -> Result<(), serde_json::Error> {
        let start = self.bytes.len();
        serde_json::to_writer(&mut self.bytes, value)?;
        self.record(start);
        Ok(())
    }

    fn record(&mut self, start: usize) {
        if self.layout_overflow {
            return;
        }
        let end = self.bytes.len();
        for offset in (start..end).step_by(MAX_WIRE_BLOCK_BYTES) {
            if self.blocks.len() >= MAX_SEMANTIC_BLOCKS {
                self.blocks.clear();
                self.layout_overflow = true;
                return;
            }
            self.blocks
                .push(offset..offset.saturating_add(MAX_WIRE_BLOCK_BYTES).min(end));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::HashSet;

    fn assert_exact(value: Value) -> NativeWireBody {
        let body = NativeWireBody::serialize(&value).expect("serialize");
        assert_eq!(
            body.as_bytes(),
            serde_json::to_vec(&value).expect("original serializer")
        );
        let mut next = 0;
        for range in body.blocks() {
            assert_eq!(range.start, next, "no gaps or overlap");
            assert!(range.end > range.start && range.len() <= MAX_WIRE_BLOCK_BYTES);
            next = range.end;
        }
        assert_eq!(next, body.as_bytes().len(), "complete body coverage");
        body
    }

    #[test]
    fn native_bytes_match_for_scalars_nested_data_unicode_and_boundaries() {
        for value in [
            Value::Null,
            json!(true),
            json!(42),
            json!(-0.0),
            json!(1e30),
            json!("引号\"\n\\🎉\u{0000}"),
            json!([]),
            json!({}),
            json!({"input":[{"role":"user","content":"问题"}],"tools":[{"name":"x","schema":{"type":"object"}}],"stream":true}),
            json!({"messages":[{"content":"正文".repeat(70_000)}],"system":[{"text":"system"}]}),
        ] {
            assert_exact(value);
        }
    }

    #[test]
    fn appending_and_reordering_preserve_unchanged_large_item_blocks() {
        let first = json!({"role":"tool","content":"a".repeat(145_818)});
        let second = json!({"role":"assistant","content":"b".repeat(70_000)});
        let before = assert_exact(json!({"input":[first,second],"model":"test"}));
        let after = assert_exact(
            json!({"input":[second,first,{"role":"user","content":"继续"}],"model":"test"}),
        );
        let known: HashSet<&[u8]> = before
            .blocks()
            .iter()
            .map(|r| &before.as_bytes()[r.clone()])
            .collect();
        let reused: usize = after
            .blocks()
            .iter()
            .filter_map(|r| {
                known
                    .contains(&after.as_bytes()[r.clone()])
                    .then_some(r.len())
            })
            .sum();
        assert!(reused * 100 / after.as_bytes().len() >= 99);
    }

    #[test]
    fn many_tiny_items_fall_back_to_bounded_ranges_without_dropping_input() {
        let body = assert_exact(json!({"input": vec!["x"; 100_000]}));
        assert!(body.blocks().len() < 10);
        assert!(body.as_bytes().len() > 300_000);
    }

    #[test]
    fn duplicate_items_keep_all_original_positions() {
        let body = assert_exact(json!({"messages":[{"text":"same"},{"text":"same"}]}));
        let reconstructed: Vec<u8> = body
            .blocks()
            .iter()
            .flat_map(|r| body.as_bytes()[r.clone()].iter().copied())
            .collect();
        assert_eq!(reconstructed, body.as_bytes());
        assert!(!format!("{body:?}").contains("same"));
    }
}
