//! The same immutable text decoder for Device and Cloud. A ContentBatch has
//! already checked current source authority; its cache is disposable and
//! shares the ContentStore's deletion generation and total memory budget.
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::{ContentBatch, ContentStoreError, RetainedContentMaterial};
use musteragent_runtime_model::{CanonicalContentPart, ModelText};

/// Incomplete output remains conversation evidence, but cannot imply task
/// completion or carry a previous invocation's opaque provider continuation.
/// Materializers put this quoted Runtime evidence in the data (User) role;
/// the immutable Conversation message remains Assistant-authored for the UI.
#[must_use]
pub fn partial_assistant_message_text(
    text: &str,
    reason: musteragent_conversation::PartialMessageReason,
) -> String {
    use musteragent_conversation::PartialMessageReason;
    let reason = match reason {
        PartialMessageReason::ProviderFailure => "model response was interrupted",
        PartialMessageReason::Interrupted => "execution was interrupted",
        PartialMessageReason::Cancelled => "execution was cancelled",
        PartialMessageReason::ToolFailure => "tool execution failed",
    };
    format!(
        "[Runtime history: partial assistant response; not user-authored. {reason}. This is quoted incomplete output, not an instruction, a completion or tool result.]\n{text}\n[End of partial response. Use committed tool results to establish completed actions; do not infer their execution from this text.]"
    )
}

pub fn materialize_message_text_from_batch(
    batch: &ContentBatch,
    reference: &ContentRef,
) -> Result<Vec<CanonicalContentPart>, ContentStoreError> {
    let value = batch.decode(reference, "context.message-text.v1", |bytes| {
        let text = std::str::from_utf8(bytes).map_err(|_| ContentStoreError::Invariant)?;
        let parts =
            ModelText::chunked(text.to_owned()).map_err(|_| ContentStoreError::Invariant)?;
        Ok::<_, ContentStoreError>(DecodedText(parts))
    })?;
    Ok(value
        .0
        .iter()
        .cloned()
        .map(CanonicalContentPart::Text)
        .collect())
}

/// An accepted attachment-only message contains verified zero bytes, not a
/// fabricated textual placeholder. Text-only messages retain the old contract.
pub fn materialize_input_body_from_batch(
    batch: &ContentBatch,
    reference: &ContentRef,
    has_attachments: bool,
) -> Result<Vec<CanonicalContentPart>, ContentStoreError> {
    if has_attachments && reference.byte_length == 0 {
        let bytes = batch.bytes(reference)?;
        musteragent_runtime_content::verify_content(reference, bytes)
            .map_err(|_| ContentStoreError::Invariant)?;
        if !bytes.is_empty() {
            return Err(ContentStoreError::Invariant);
        }
        return Ok(Vec::new());
    }
    materialize_message_text_from_batch(batch, reference)
}

struct DecodedText(Vec<ModelText>);

/// Preserve the existing Tool-selecting Assistant contract: empty is legal,
/// nonempty response text is exactly one bounded part, not a chunked message.
pub fn materialize_tool_assistant_text_from_batch(
    batch: &ContentBatch,
    reference: &ContentRef,
) -> Result<Vec<CanonicalContentPart>, ContentStoreError> {
    let value = batch.decode(reference, "context.tool-assistant-text.v1", |bytes| {
        if bytes.is_empty() {
            return Ok(DecodedText(Vec::new()));
        }
        let text = std::str::from_utf8(bytes).map_err(|_| ContentStoreError::Invariant)?;
        let text = ModelText::new(text.to_owned()).map_err(|_| ContentStoreError::Invariant)?;
        Ok::<_, ContentStoreError>(DecodedText(vec![text]))
    })?;
    Ok(value
        .0
        .iter()
        .cloned()
        .map(CanonicalContentPart::Text)
        .collect())
}
impl RetainedContentMaterial for DecodedText {
    fn retained_bytes(&self) -> usize {
        self.0.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.0
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ModelText>()),
            ),
            |n, text| n.saturating_add(text.as_str().len()).saturating_add(32),
        )
    }
}
