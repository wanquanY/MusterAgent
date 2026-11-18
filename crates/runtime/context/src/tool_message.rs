//! Pure Tool-result decoding is reusable; expected call/name checks are always
//! current. The storage adapters retain ownership of their persisted codecs.
use musteragent_content_ref::ContentRef;
use musteragent_identity::ToolCallId;
use musteragent_runtime_content::{ContentBatch, ContentStoreError, RetainedContentMaterial};
use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, CanonicalRole};
use musteragent_tool::WireName;

pub fn materialize_tool_message_from_batch(
    batch: &ContentBatch,
    reference: &ContentRef,
    codec: &'static str,
    expected_call_id: &ToolCallId,
    expected_name: &WireName,
    decode: impl FnOnce(&[u8]) -> Result<CanonicalMessage, ContentStoreError>,
) -> Result<CanonicalMessage, ContentStoreError> {
    let value = batch.decode(reference, codec, |bytes| {
        let message = decode(bytes)?;
        if message.role != CanonicalRole::Tool {
            return Err(ContentStoreError::Invariant);
        }
        message
            .validate()
            .map_err(|_| ContentStoreError::Invariant)?;
        Ok(DecodedToolMessage(message))
    })?;
    if value.0.tool_call_id.as_ref() != Some(expected_call_id)
        || value.0.tool_name.as_ref() != Some(expected_name)
    {
        return Err(ContentStoreError::Invariant);
    }
    Ok(value.0.clone())
}

struct DecodedToolMessage(CanonicalMessage);
impl RetainedContentMaterial for DecodedToolMessage {
    fn retained_bytes(&self) -> usize {
        // Only validated Tool-role messages enter: calls and reasoning cannot
        // retain data. Charge content allocations even when an Arc is shared.
        self.0.content.iter().fold(
            std::mem::size_of::<Self>()
                .saturating_add(128)
                .saturating_add(
                    self.0
                        .content
                        .capacity()
                        .saturating_mul(std::mem::size_of::<CanonicalContentPart>()),
                )
                .saturating_add(
                    self.0
                        .tool_calls
                        .capacity()
                        .saturating_mul(std::mem::size_of::<
                            musteragent_runtime_model::CanonicalToolCall,
                        >()),
                )
                .saturating_add(
                    self.0
                        .tool_call_id
                        .as_ref()
                        .map_or(0, |id| id.as_str().len()),
                )
                .saturating_add(
                    self.0
                        .tool_name
                        .as_ref()
                        .map_or(0, |name| name.as_str().len()),
                ),
            |n, part| {
                n.saturating_add(match part {
                    CanonicalContentPart::Text(text) => text.as_str().len().saturating_add(32),
                    CanonicalContentPart::Image(reference)
                    | CanonicalContentPart::Audio(reference)
                    | CanonicalContentPart::Video(reference)
                    | CanonicalContentPart::File(reference) => {
                        reference.media_type.as_str().len().saturating_add(128)
                    }
                })
            },
        )
    }
}

#[cfg(test)]
mod tests;
