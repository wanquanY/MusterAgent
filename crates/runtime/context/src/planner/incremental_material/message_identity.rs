//! Complete typed identities of currently materialized entries. Streaming the
//! hash avoids allocating another serialized copy of a large tool result.
use super::*;
use musteragent_runtime_model::{CanonicalMessage, CanonicalRole};

impl Fingerprint {
    pub(super) fn entry(&mut self, entry: &ContextEntry) {
        self.number(entry.source_sequence.get());
        self.number(entry.presentation_order.anchor_sequence.get());
        self.number(u64::from(entry.presentation_order.exchange_ordinal));
        self.number(entry.source_record_ids.len() as u64);
        for id in &entry.source_record_ids {
            self.field(id.into_uuid().as_bytes());
        }
        self.optional(entry.source_run_id, |key, id| {
            key.field(id.into_uuid().as_bytes())
        });
        self.optional(entry.continuation_delta_for, |key, id| {
            key.field(id.into_uuid().as_bytes())
        });
        self.optional(entry.assistant_origin.as_ref(), |key, origin| {
            key.field(origin.invocation_id.into_uuid().as_bytes());
            key.field(origin.model_route_ref.as_str().as_bytes());
            key.field(origin.route_identity_hash.as_bytes());
            key.field(origin.requested_model_key.as_str().as_bytes());
            key.field(origin.response_model_key.as_str().as_bytes());
        });
        self.message(&entry.message);
    }

    pub(super) fn message(&mut self, message: &CanonicalMessage) {
        self.number(match message.role {
            CanonicalRole::User => 1,
            CanonicalRole::Assistant => 2,
            CanonicalRole::Tool => 3,
        });
        self.number(message.content.len() as u64);
        for part in &message.content {
            match part {
                CanonicalContentPart::Text(text) => {
                    self.number(1);
                    self.field(text.as_str().as_bytes());
                }
                CanonicalContentPart::Image(reference) => {
                    self.number(2);
                    self.content(reference);
                }
                CanonicalContentPart::Audio(reference) => {
                    self.number(3);
                    self.content(reference);
                }
                CanonicalContentPart::Video(reference) => {
                    self.number(4);
                    self.content(reference);
                }
                CanonicalContentPart::File(reference) => {
                    self.number(5);
                    self.content(reference);
                }
            }
        }
        self.number(message.tool_calls.len() as u64);
        for call in &message.tool_calls {
            self.field(call.call_id.as_str().as_bytes());
            self.field(call.name.as_str().as_bytes());
            self.json(&call.arguments);
            self.optional(call.provider_item_ref.as_ref(), |key, reference| {
                key.field(reference.as_str().as_bytes())
            });
            self.optional(call.provider_replay_ref.as_ref(), Self::content);
        }
        self.optional(message.tool_call_id.as_ref(), |key, id| {
            key.field(id.as_str().as_bytes())
        });
        self.optional(message.tool_name.as_ref(), |key, name| {
            key.field(name.as_str().as_bytes())
        });
        self.optional(message.reasoning.as_ref(), |key, reasoning| {
            key.content(&reasoning.content_ref);
            key.field(reasoning.source_route_ref.as_str().as_bytes());
            key.optional(reasoning.continuation_ref.as_ref(), |key, reference| {
                key.field(reference.as_str().as_bytes())
            });
        });
    }

    fn json(&mut self, value: &JsonValue) {
        match value {
            JsonValue::Null => self.number(0),
            JsonValue::Bool(value) => {
                self.number(1);
                self.number(u64::from(*value));
            }
            JsonValue::Integer(value) => {
                self.number(2);
                self.field(&value.to_be_bytes());
            }
            JsonValue::Number(value) => {
                self.number(3);
                self.field(&value.to_bits().to_be_bytes());
            }
            JsonValue::String(value) => {
                self.number(4);
                self.field(value.as_bytes());
            }
            JsonValue::Array(values) => {
                self.number(5);
                self.number(values.len() as u64);
                for value in values {
                    self.json(value);
                }
            }
            JsonValue::Object(values) => {
                self.number(6);
                self.number(values.len() as u64);
                for (key, value) in values {
                    self.field(key.as_bytes());
                    self.json(value);
                }
            }
        }
    }
}

// Conservative reachable-allocation accounting. ContentRef/replay identities
// contain bounded strings, whose full allocations are charged per occurrence.
fn reference_allocations(reference: &ContentRef) -> usize {
    reference.media_type.as_str().len().saturating_add(128)
}
pub(super) fn message_allocations(message: &CanonicalMessage) -> usize {
    let content = message.content.iter().fold(
        message
            .content
            .capacity()
            .saturating_mul(std::mem::size_of::<CanonicalContentPart>()),
        |n, part| {
            n.saturating_add(match part {
                CanonicalContentPart::Text(text) => text.as_str().len().saturating_add(32),
                CanonicalContentPart::Image(reference)
                | CanonicalContentPart::Audio(reference)
                | CanonicalContentPart::Video(reference)
                | CanonicalContentPart::File(reference) => reference_allocations(reference),
            })
        },
    );
    let calls = message.tool_calls.iter().fold(
        message
            .tool_calls
            .capacity()
            .saturating_mul(std::mem::size_of::<
                musteragent_runtime_model::CanonicalToolCall,
            >()),
        |n, call| {
            n.saturating_add(128)
                .saturating_add(call.call_id.as_str().len())
                .saturating_add(call.name.as_str().len())
                .saturating_add(json_allocations(&call.arguments))
                .saturating_add(
                    call.provider_item_ref
                        .as_ref()
                        .map_or(0, |value| value.as_str().len().saturating_add(128)),
                )
                .saturating_add(
                    call.provider_replay_ref
                        .as_ref()
                        .map_or(0, reference_allocations),
                )
        },
    );
    content
        .saturating_add(calls)
        .saturating_add(128)
        .saturating_add(
            message
                .tool_call_id
                .as_ref()
                .map_or(0, |value| value.as_str().len()),
        )
        .saturating_add(
            message
                .tool_name
                .as_ref()
                .map_or(0, |value| value.as_str().len()),
        )
        .saturating_add(message.reasoning.as_ref().map_or(0, |value| {
            reference_allocations(&value.content_ref)
                .saturating_add(value.source_route_ref.as_str().len())
                .saturating_add(128)
                .saturating_add(
                    value
                        .continuation_ref
                        .as_ref()
                        .map_or(0, |value| value.as_str().len().saturating_add(128)),
                )
        }))
}
