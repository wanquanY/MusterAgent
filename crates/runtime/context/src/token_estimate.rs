//! Provider-neutral input capacity estimates shared by all Runtime profiles.
//! These are planning estimates, never physical usage or billing facts.
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRuntimeEvent, CanonicalToolDefinition,
    ModelText,
};

pub fn estimate_instruction_tokens(instructions: &[ModelText]) -> u64 {
    instructions.iter().fold(0_u64, |total, value| {
        total.saturating_add(estimate_text_tokens(value.as_str()))
    })
}

pub fn estimate_tool_tokens(tools: &[CanonicalToolDefinition]) -> u64 {
    tools.iter().fold(0_u64, |total, tool| {
        total
            .saturating_add(estimate_text_tokens(tool.description.as_str()))
            .saturating_add(estimate_json(&tool.input_schema))
            .saturating_add(32)
    })
}

pub fn estimate_runtime_event_tokens(events: &[CanonicalRuntimeEvent]) -> u64 {
    events.iter().fold(0_u64, |total, event| {
        total.saturating_add(
            event
                .model_input_text()
                .map_or(u64::MAX, |text| estimate_text_tokens(&text)),
        )
    })
}

pub fn estimate_messages(messages: &[CanonicalMessage]) -> u64 {
    messages.iter().fold(0_u64, |total, message| {
        total.saturating_add(estimate_message(message))
    })
}

pub fn estimate_message(message: &CanonicalMessage) -> u64 {
    let content = message.content.iter().fold(0_u64, |total, part| {
        total.saturating_add(estimate_part(part))
    });
    // Private replay is physically sent to the Provider as well. Its opaque
    // bytes use a conservative byte/token upper estimate; do not expose or
    // decode Provider state just to account for capacity.
    let content = content.saturating_add(
        message
            .reasoning
            .as_ref()
            .map_or(0, |r| r.content_ref.byte_length),
    );
    message.tool_calls.iter().fold(content, |total, call| {
        total
            .saturating_add(estimate_json(&call.arguments))
            .saturating_add(
                call.provider_replay_ref
                    .as_ref()
                    .map_or(0, |r| r.byte_length),
            )
            .saturating_add(32)
    })
}

fn estimate_part(part: &CanonicalContentPart) -> u64 {
    match part {
        CanonicalContentPart::Text(value) => estimate_text_tokens(value.as_str()),
        CanonicalContentPart::Image(_) => estimate_bytes(4_800),
        CanonicalContentPart::Audio(reference) | CanonicalContentPart::Video(reference) => {
            estimate_bytes(usize::try_from(reference.byte_length).unwrap_or(usize::MAX))
        }
        CanonicalContentPart::File(reference) => {
            estimate_native_document(reference.media_type.as_str(), reference.byte_length)
        }
    }
}

/// Native documents are sent inline as their original bytes, so their weight
/// follows the referenced size instead of a fixed placeholder.
fn estimate_native_document(media_type: &str, byte_length: u64) -> u64 {
    // Previous fixed weight; a tiny file still costs a document block.
    const MIN_DOCUMENT_TOKENS: u64 = 256;
    // Providers bill a PDF page as extracted text plus a page image, roughly
    // 1.5k-4.5k tokens. Text PDFs carry ~20-60 KB per page, scanned ones far
    // more, so 16 bytes/token over-counts rather than under-counts typical
    // documents.
    const PDF_BYTES_PER_TOKEN: u64 = 16;
    // Providers process at most ~100 PDF pages at ~3k tokens each; a large
    // scanned file is bounded by its pages, not by its image bytes.
    const MAX_PDF_TOKENS: u64 = 300_000;
    let essence = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let tokens = match essence.as_str() {
        "application/pdf" => byte_length
            .div_ceil(PDF_BYTES_PER_TOKEN)
            .min(MAX_PDF_TOKENS),
        // Plain-text documents use the same 3-byte/token budget as text.
        value if value.starts_with("text/") => byte_length.div_ceil(3),
        _ => byte_length.div_ceil(4),
    };
    tokens.max(MIN_DOCUMENT_TOKENS)
}

fn estimate_json(value: &musteragent_runtime_model::JsonValue) -> u64 {
    match value {
        musteragent_runtime_model::JsonValue::Null => 1,
        musteragent_runtime_model::JsonValue::Bool(_) => 1,
        musteragent_runtime_model::JsonValue::Integer(_) => 4,
        musteragent_runtime_model::JsonValue::Number(_) => 4,
        musteragent_runtime_model::JsonValue::String(value) => estimate_text_tokens(value),
        musteragent_runtime_model::JsonValue::Array(values) => {
            values.iter().fold(2_u64, |total, value| {
                total.saturating_add(estimate_json(value))
            })
        }
        musteragent_runtime_model::JsonValue::Object(values) => {
            values.iter().fold(2_u64, |total, (key, value)| {
                total
                    .saturating_add(estimate_text_tokens(key))
                    .saturating_add(estimate_json(value))
            })
        }
    }
}

pub fn estimate_text_tokens(text: &str) -> u64 {
    // Provider-neutral conservative fallback for the unmeasured tail. ASCII
    // prose uses a 3-byte/token budget; non-ASCII UTF-8 uses 2 tokens per 3
    // bytes, which covers multi-token CJK/emoji far better than chars/4. Once
    // a Provider has reported usage, that physical count remains the prefix
    // authority and this estimate applies only to subsequently appended units.
    let ascii_bytes = text.bytes().filter(u8::is_ascii).count();
    let non_ascii_bytes = text.len().saturating_sub(ascii_bytes);
    let ascii = u64::try_from(ascii_bytes.saturating_add(2) / 3).unwrap_or(u64::MAX);
    let non_ascii = u64::try_from(non_ascii_bytes)
        .unwrap_or(u64::MAX)
        .saturating_mul(2)
        .saturating_add(2)
        / 3;
    ascii.saturating_add(non_ascii)
}

fn estimate_bytes(bytes: usize) -> u64 {
    // Fixed synthetic media weights retain their established byte/4 mapping;
    // actual text always goes through the Unicode-aware estimator above.
    u64::try_from(bytes.saturating_add(3) / 4).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;

    fn file(media_type: &str, byte_length: u64) -> CanonicalContentPart {
        CanonicalContentPart::File(ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).expect("media type"),
            byte_length,
            sha256: Sha256::digest(b"document"),
        })
    }

    #[test]
    fn native_documents_are_weighted_by_their_inline_size() {
        // A 20 MiB PDF is sent inline; it must not count as 256 tokens.
        let large_pdf = estimate_part(&file("application/pdf", 20 * 1024 * 1024));
        assert_eq!(large_pdf, 300_000);
        // A typical 40 KB PDF page is in the low thousands of tokens.
        assert_eq!(estimate_part(&file("application/pdf", 40_000)), 2_500);
        assert_eq!(estimate_part(&file("text/plain", 3_000)), 1_000);
        assert_eq!(estimate_part(&file("text/csv;charset=utf-8", 3_000)), 1_000);
        // Tiny documents keep the established minimum weight.
        assert_eq!(estimate_part(&file("application/pdf", 10)), 256);
        assert_eq!(
            estimate_part(&file("application/octet-stream", 40_000)),
            10_000
        );
    }
}
