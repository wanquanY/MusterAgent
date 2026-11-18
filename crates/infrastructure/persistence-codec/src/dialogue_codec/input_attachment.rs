//! Shared wire and persistence codec. Exact round trips reject normalization,
//! unknown fields and noncanonical receipts; the caller still authorizes IO.
use super::*;
use musteragent_runtime_contract::{AttachmentRef, DisplayText};
use prost::Message;

pub fn attachment(value: &AttachmentRef) -> wire::AttachmentRef {
    wire::AttachmentRef {
        content_ref: Some(encode_content_ref(&value.content_ref)),
        display_name: value.display_name.as_str().into(),
    }
}
pub fn decode_attachment(value: wire::AttachmentRef) -> Result<AttachmentRef, Error> {
    let result = AttachmentRef {
        content_ref: content(required(value.content_ref.clone())?)?,
        display_name: DisplayText::new(value.display_name.clone()).map_err(|_| Error::Corrupt)?,
    };
    validate_dialogue_attachment(&result).map_err(|_| Error::Corrupt)?;
    if attachment(&result) != value {
        return Err(Error::Corrupt);
    }
    Ok(result)
}
pub fn input_attachment(value: &DialogueInputAttachment) -> wire::DialogueInputAttachment {
    use wire::dialogue_input_attachment::Source;
    wire::DialogueInputAttachment {
        attachment: Some(attachment(&value.attachment)),
        source: Some(match &value.source {
            DialogueInputAttachmentSource::Uploaded { upload_id } => {
                Source::UploadId(upload_id.clone())
            }
            DialogueInputAttachmentSource::AcceptedInput {
                dialogue_id,
                entry_id,
                cursor: at,
                ordinal,
            } => Source::AcceptedInput(wire::DialogueAcceptedInputSource {
                dialogue_id: Some(wire::AgentDialogueId {
                    value: dialogue_id.to_string(),
                }),
                entry_id: Some(wire::DialogueEntryId {
                    value: entry_id.to_string(),
                }),
                cursor: Some(cursor(*at)),
                ordinal: *ordinal,
            }),
        }),
    }
}
pub fn decode_input_attachment(
    value: wire::DialogueInputAttachment,
) -> Result<DialogueInputAttachment, Error> {
    use wire::dialogue_input_attachment::Source;
    let result = DialogueInputAttachment {
        attachment: decode_attachment(required(value.attachment.clone())?)?,
        source: match required(value.source.clone())? {
            Source::UploadId(upload_id) => DialogueInputAttachmentSource::Uploaded { upload_id },
            Source::AcceptedInput(source) => DialogueInputAttachmentSource::AcceptedInput {
                dialogue_id: id(source.dialogue_id.map(|v| v.value))?,
                entry_id: id(source.entry_id.map(|v| v.value))?,
                cursor: decode_cursor(source.cursor)?,
                ordinal: source.ordinal,
            },
        },
    };
    result.validate().map_err(|_| Error::Corrupt)?;
    if input_attachment(&result) != value {
        return Err(Error::Corrupt);
    }
    Ok(result)
}
pub fn decode_input_attachments(
    values: Vec<wire::DialogueInputAttachment>,
) -> Result<Vec<DialogueInputAttachment>, Error> {
    let result = values
        .into_iter()
        .map(decode_input_attachment)
        .collect::<Result<Vec<_>, _>>()?;
    validate_dialogue_input_attachments(&result).map_err(|_| Error::Corrupt)?;
    Ok(result)
}
pub fn decode_attachments(values: Vec<wire::AttachmentRef>) -> Result<Vec<AttachmentRef>, Error> {
    let result = values
        .into_iter()
        .map(decode_attachment)
        .collect::<Result<Vec<_>, _>>()?;
    validate_dialogue_attachments(&result).map_err(|_| Error::Corrupt)?;
    Ok(result)
}
pub fn input_attachments_bytes(values: &[DialogueInputAttachment]) -> Vec<u8> {
    wire::DialogueInputAttachments {
        values: values.iter().map(input_attachment).collect(),
    }
    .encode_to_vec()
}
pub fn input_attachments_from_bytes(bytes: &[u8]) -> Result<Vec<DialogueInputAttachment>, Error> {
    if bytes.len() > 32 * 1024 {
        return Err(Error::Corrupt);
    }
    let values = wire::DialogueInputAttachments::decode(bytes).map_err(|_| Error::Corrupt)?;
    if values.encode_to_vec() != bytes {
        return Err(Error::Corrupt);
    }
    decode_input_attachments(values.values)
}
pub fn attachments_bytes(values: &[AttachmentRef]) -> Vec<u8> {
    wire::DialogueMessageAttachments {
        values: values.iter().map(attachment).collect(),
    }
    .encode_to_vec()
}
pub fn attachments_from_bytes(bytes: &[u8]) -> Result<Vec<AttachmentRef>, Error> {
    if bytes.len() > 32 * 1024 {
        return Err(Error::Corrupt);
    }
    let values = wire::DialogueMessageAttachments::decode(bytes).map_err(|_| Error::Corrupt)?;
    if values.encode_to_vec() != bytes {
        return Err(Error::Corrupt);
    }
    decode_attachments(values.values)
}
