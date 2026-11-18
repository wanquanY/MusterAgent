use musteragent_content_ref::ContentRef;
use musteragent_identity::TenantId;
use musteragent_runtime_content::{ContentBatch, ContentStore, ContentStoreError, verify_content};
use musteragent_runtime_model::{
    CanonicalContentPart, InputAttachmentKind, ModelText, input_attachment_kind,
};
use thiserror::Error;

const ATTACHMENT_TEXT_START: &str = "[Attached text begins]";
const ATTACHMENT_TEXT_END: &str = "[Attached text ends]";

/// Materializes one immutable Message attachment into provider-neutral model input.
///
/// Text attachments are deliberately resolved here, before a provider adapter is
/// selected. This gives SQLite and PostgreSQL the same semantics and avoids
/// relying on provider-specific file-upload support for ordinary UTF-8 content.
/// Binary media and opaque documents remain content-addressed references so
/// the selected provider adapter can use the exact native wire representation.
pub async fn materialize_message_attachment(
    content: &dyn ContentStore,
    tenant_id: TenantId,
    attachment: ContentRef,
) -> Result<Vec<CanonicalContentPart>, AttachmentMaterializationError> {
    let bytes = if message_attachment_text_ref(&attachment).is_some() {
        Some(
            content
                .read(tenant_id, &attachment)
                .await
                .map_err(AttachmentMaterializationError::Content)?,
        )
    } else {
        None
    };
    render_attachment(attachment, bytes.as_deref())
}

/// Only inline text is read by Context. Binary images/files retain their
/// reference and remain owned by the selected provider's binary transport.
pub fn message_attachment_text_ref(attachment: &ContentRef) -> Option<&ContentRef> {
    (input_attachment_kind(attachment) == InputAttachmentKind::Text).then_some(attachment)
}

/// Consume a fresh, authoritatively loaded batch without another storage read.
pub fn materialize_message_attachment_from_batch(
    batch: &ContentBatch,
    attachment: ContentRef,
) -> Result<Vec<CanonicalContentPart>, AttachmentMaterializationError> {
    let bytes = message_attachment_text_ref(&attachment)
        .map(|reference| batch.bytes(reference))
        .transpose()
        .map_err(AttachmentMaterializationError::Content)?;
    render_attachment(attachment, bytes)
}

/// In prior dialogue, retain recording identities and document metadata.
/// Current input uses materialize_message_attachment and keeps its native media.
fn dialogue_history_reference(attachment: &ContentRef) -> Option<Vec<CanonicalContentPart>> {
    if input_attachment_kind(attachment) == InputAttachmentKind::File {
        return Some(Vec::new());
    }
    musteragent_runtime_model::DialogueInputObservation::historical_recording_reference(attachment)
        .map(|part| vec![part])
}

pub async fn materialize_dialogue_attachment(
    content: &dyn ContentStore,
    tenant_id: TenantId,
    attachment: ContentRef,
) -> Result<Vec<CanonicalContentPart>, AttachmentMaterializationError> {
    if let Some(parts) = dialogue_history_reference(&attachment) {
        return Ok(parts);
    }
    materialize_message_attachment(content, tenant_id, attachment).await
}

pub fn materialize_dialogue_attachment_from_batch(
    batch: &ContentBatch,
    attachment: ContentRef,
) -> Result<Vec<CanonicalContentPart>, AttachmentMaterializationError> {
    if let Some(parts) = dialogue_history_reference(&attachment) {
        return Ok(parts);
    }
    materialize_message_attachment_from_batch(batch, attachment)
}

fn render_attachment(
    attachment: ContentRef,
    bytes: Option<&[u8]>,
) -> Result<Vec<CanonicalContentPart>, AttachmentMaterializationError> {
    if input_attachment_kind(&attachment) == InputAttachmentKind::Image {
        return Ok(vec![CanonicalContentPart::Image(attachment)]);
    }
    if input_attachment_kind(&attachment) == InputAttachmentKind::Audio {
        return Ok(vec![CanonicalContentPart::Audio(attachment)]);
    }
    if input_attachment_kind(&attachment) == InputAttachmentKind::Video {
        return Ok(vec![CanonicalContentPart::Video(attachment)]);
    }
    if input_attachment_kind(&attachment) != InputAttachmentKind::Text {
        return Ok(vec![CanonicalContentPart::File(attachment)]);
    }

    let bytes = bytes.ok_or(AttachmentMaterializationError::Corrupt)?;
    verify_content(&attachment, bytes).map_err(|_| AttachmentMaterializationError::Corrupt)?;
    let value = std::str::from_utf8(bytes).map_err(|_| AttachmentMaterializationError::Corrupt)?;
    let value = format!("{ATTACHMENT_TEXT_START}\n{value}\n{ATTACHMENT_TEXT_END}");
    ModelText::chunked(value)
        .map_err(|_| AttachmentMaterializationError::Corrupt)
        .map(|parts| parts.into_iter().map(CanonicalContentPart::Text).collect())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum AttachmentMaterializationError {
    #[error("attachment content is unavailable")]
    Content(ContentStoreError),
    #[error("attachment bytes or text violate their immutable content contract")]
    Corrupt,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;

    use super::{InputAttachmentKind, input_attachment_kind};

    #[test]
    fn previous_recordings_keep_their_source_but_current_input_keeps_its_media() {
        for mime in ["audio/wav", "video/mp4", "video/webm"] {
            let reference = attachment(mime);
            let history = super::dialogue_history_reference(&reference).unwrap();
            assert!(
                matches!(history.as_slice(), [super::CanonicalContentPart::Text(text)]
                if text.as_str().contains(&reference.content_id.to_string()))
            );
            let current = super::render_attachment(reference.clone(), None).unwrap();
            assert!(
                matches!(current.as_slice(), [super::CanonicalContentPart::Audio(r) | super::CanonicalContentPart::Video(r)]
                if r == &reference)
            );
        }
    }

    fn attachment(media_type: &str) -> ContentRef {
        ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(media_type).unwrap(),
            byte_length: 0,
            sha256: Sha256::digest([]),
        }
    }

    #[test]
    fn classifies_provider_neutral_attachment_presentation() {
        for (mime, expected) in [
            ("image/png", InputAttachmentKind::Image),
            ("audio/mpeg", InputAttachmentKind::Audio),
            ("video/mp4", InputAttachmentKind::Video),
            ("text/plain;charset=utf-8", InputAttachmentKind::Text),
            ("TEXT/PLAIN;charset=utf-8", InputAttachmentKind::Text),
            ("application/json", InputAttachmentKind::Text),
            ("application/manifest+json", InputAttachmentKind::Text),
            ("application/x-yaml", InputAttachmentKind::Text),
            ("application/pdf", InputAttachmentKind::File),
            ("application/octet-stream", InputAttachmentKind::File),
        ] {
            assert_eq!(input_attachment_kind(&attachment(mime)), expected);
        }
    }
}
