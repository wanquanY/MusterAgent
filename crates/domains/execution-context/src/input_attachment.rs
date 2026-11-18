//! An accepted input owns an exact attachment and its verified provenance.
//! Neither a display name nor a ContentId alone grants access to bytes.
use crate::{AgentDialogueError as Error, DialogueCursor};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{AgentDialogueId, DialogueEntryId};
use musteragent_runtime_contract::AttachmentRef;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DialogueInputAttachmentSource {
    Uploaded {
        upload_id: String,
    },
    AcceptedInput {
        dialogue_id: AgentDialogueId,
        entry_id: DialogueEntryId,
        cursor: DialogueCursor,
        ordinal: u32,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInputAttachment {
    pub attachment: AttachmentRef,
    pub source: DialogueInputAttachmentSource,
}

impl DialogueInputAttachment {
    pub fn validate(&self) -> Result<(), Error> {
        validate_dialogue_attachment(&self.attachment)?;
        match &self.source {
            DialogueInputAttachmentSource::Uploaded { upload_id }
                if upload_id.len() == 64
                    && upload_id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
            {
                Ok(())
            }
            DialogueInputAttachmentSource::AcceptedInput {
                cursor, ordinal, ..
            } if cursor.seq > 0 && *ordinal < 64 => cursor.validate(),
            _ => Err(Error::InvalidAttachment),
        }
    }
}

pub fn validate_dialogue_attachment(value: &AttachmentRef) -> Result<(), Error> {
    let name = value.display_name.as_str();
    if matches!(name, "." | "..") || name.contains(['/', '\\']) {
        return Err(Error::InvalidAttachment);
    }
    Ok(())
}

pub fn validate_dialogue_input_attachments(
    values: &[DialogueInputAttachment],
) -> Result<(), Error> {
    if values.len() > 64 {
        return Err(Error::InvalidAttachment);
    }
    for value in values {
        value.validate()?;
    }
    validate_dialogue_attachments(
        &values
            .iter()
            .map(|v| v.attachment.clone())
            .collect::<Vec<_>>(),
    )
}

pub fn validate_dialogue_attachments(values: &[AttachmentRef]) -> Result<(), Error> {
    if values.len() > 64 {
        return Err(Error::InvalidAttachment);
    }
    let mut identities = std::collections::BTreeMap::new();
    for value in values {
        validate_dialogue_attachment(value)?;
        if identities
            .insert(value.content_ref.content_id, &value.content_ref)
            .is_some_and(|previous| previous != &value.content_ref)
        {
            return Err(Error::InvalidAttachment);
        }
    }
    Ok(())
}

pub fn dialogue_attachment_bytes(value: &AttachmentRef) -> Vec<u8> {
    let reference = &value.content_ref;
    let mut bytes = reference.content_id.into_uuid().as_bytes().to_vec();
    frame(&mut bytes, reference.media_type.as_str().as_bytes());
    bytes.extend(reference.byte_length.to_be_bytes());
    bytes.extend(reference.sha256.into_bytes());
    let mut output = Vec::new();
    frame(&mut output, &bytes);
    frame(&mut output, value.display_name.as_str().as_bytes());
    output
}

pub fn dialogue_input_attachment_bytes(values: &[DialogueInputAttachment]) -> Vec<u8> {
    let mut bytes = b"musteragent.dialogue-input-attachments.v1\0".to_vec();
    bytes.extend((values.len() as u64).to_be_bytes());
    for value in values {
        frame(&mut bytes, &dialogue_attachment_bytes(&value.attachment));
        let source = match &value.source {
            DialogueInputAttachmentSource::Uploaded { upload_id } => {
                let mut source = vec![1];
                source.extend(upload_id.as_bytes());
                source
            }
            DialogueInputAttachmentSource::AcceptedInput {
                dialogue_id,
                entry_id,
                cursor,
                ordinal,
            } => {
                let mut source = vec![2];
                source.extend(dialogue_id.into_uuid().as_bytes());
                source.extend(entry_id.into_uuid().as_bytes());
                source.extend(cursor.epoch.to_be_bytes());
                source.extend(cursor.seq.to_be_bytes());
                source.extend(ordinal.to_be_bytes());
                source
            }
        };
        frame(&mut bytes, &source);
    }
    bytes
}

/// The source chain keeps every text-only hash unchanged. Only new kind 6 uses
/// this payload; provenance remains private acceptance evidence.
pub fn dialogue_attachment_entry_bytes(
    body: &ContentRef,
    attachments: &[AttachmentRef],
    encode_ref: impl Fn(&ContentRef) -> Vec<u8>,
) -> Vec<u8> {
    let mut bytes = b"musteragent.dialogue-user-entry-attachments.v1\0".to_vec();
    frame(&mut bytes, &encode_ref(body));
    bytes.extend((attachments.len() as u64).to_be_bytes());
    for attachment in attachments {
        frame(&mut bytes, &dialogue_attachment_bytes(attachment));
    }
    bytes
}

fn frame(target: &mut Vec<u8>, value: &[u8]) {
    target.extend((value.len() as u64).to_be_bytes());
    target.extend(value);
}
