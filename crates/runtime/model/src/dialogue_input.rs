//! Exact accepted input provenance survives Context projection and compaction.
//! These observations are private data; they never grant access by themselves.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_content_ref::ContentRef;
use musteragent_execution_context::{DialogueContextCut, DialogueCursor};
use musteragent_identity::DialogueEntryId;
use musteragent_runtime_contract::AttachmentRef;

use crate::{
    CanonicalContentPart, CanonicalModelError, CanonicalModelRequest, CanonicalRuntimeEvent,
    ModelText,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InputAttachmentKind {
    Text,
    Image,
    Audio,
    Video,
    File,
}

/// The same MIME classification is used by Context and persistence validation.
pub fn input_attachment_kind(reference: &ContentRef) -> InputAttachmentKind {
    let essence = reference
        .media_type
        .as_str()
        .split(';')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if essence.starts_with("image/") {
        InputAttachmentKind::Image
    } else if essence.starts_with("audio/") {
        InputAttachmentKind::Audio
    } else if essence.starts_with("video/") {
        InputAttachmentKind::Video
    } else if essence.starts_with("text/")
        || essence.ends_with("+json")
        || essence.ends_with("+xml")
        || matches!(
            essence.as_str(),
            "application/json"
                | "application/xml"
                | "application/javascript"
                | "application/ecmascript"
                | "application/yaml"
                | "application/x-yaml"
                | "application/toml"
                | "application/sql"
        )
    {
        InputAttachmentKind::Text
    } else {
        InputAttachmentKind::File
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueInputObservation {
    pub entry_id: DialogueEntryId,
    pub cursor: DialogueCursor,
    pub ordinal: u32,
    pub attachment: AttachmentRef,
    pub parts: Vec<CanonicalContentPart>,
}

impl DialogueInputObservation {
    /// Preserve exact history provenance when the current visual inspection
    /// needs the route's image capacity. This is a projection, not lost input.
    pub fn historical_image_reference(reference: &ContentRef) -> Option<CanonicalContentPart> {
        if input_attachment_kind(reference) != InputAttachmentKind::Image {
            return None;
        }
        Some(CanonicalContentPart::Text(ModelText::new(format!(
            "[Earlier image: {} content ID {}, SHA-256 {}. Pixels are not included because the current request uses the model's image capacity. Use vision_analyze with this original content ID before making visual claims about it.]",
            reference.media_type.as_str(), reference.content_id, reference.sha256
        )).expect("bounded typed image identity")))
    }

    /// Replay the identity of an earlier recording. A user selecting it again
    /// supplies a current attachment through the ordinary input path.
    pub fn historical_recording_reference(reference: &ContentRef) -> Option<CanonicalContentPart> {
        if !matches!(
            input_attachment_kind(reference),
            InputAttachmentKind::Audio | InputAttachmentKind::Video
        ) {
            return None;
        }
        let text = ModelText::new(format!(
            "[Earlier recording: {} content ID {}, SHA-256 {}. Media bytes are not included in this history entry. Use the original file reference when needed.]",
            reference.media_type.as_str(), reference.content_id, reference.sha256
        )).expect("bounded typed media identity");
        Some(CanonicalContentPart::Text(text))
    }

    pub fn validate(
        &self,
        cut: &DialogueContextCut,
        from: u64,
        through: u64,
    ) -> Result<(), CanonicalModelError> {
        let bad = || CanonicalModelError::InvalidDialogueInput;
        musteragent_execution_context::validate_dialogue_attachment(&self.attachment)
            .map_err(|_| bad())?;
        self.cursor.validate().map_err(|_| bad())?;
        if self.cursor.epoch != cut.through.epoch
            || self.cursor.seq < from
            || self.cursor.seq > through
            || self.ordinal >= 64
        {
            return Err(bad());
        }
        let reference = &self.attachment.content_ref;
        match input_attachment_kind(reference) {
            // The exact accepted File locator is already in this observation.
            // No parts means metadata only: the original file tools read it.
            // Retained native File parts remain valid for existing model plans.
            InputAttachmentKind::File if self.parts.is_empty() => {}
            InputAttachmentKind::Image
                if matches!(self.parts.as_slice(), [CanonicalContentPart::Text(_)])
                    && Self::historical_image_reference(reference).as_ref()
                        == self.parts.first() => {}
            InputAttachmentKind::Audio | InputAttachmentKind::Video
                if matches!(self.parts.as_slice(), [CanonicalContentPart::Text(_)])
                    && Self::historical_recording_reference(reference).as_ref()
                        == self.parts.first() => {}
            InputAttachmentKind::Text => {
                let mut length = 0_u64;
                for part in &self.parts {
                    let CanonicalContentPart::Text(text) = part else {
                        return Err(bad());
                    };
                    length = length
                        .checked_add(text.as_str().len() as u64)
                        .ok_or_else(bad)?;
                }
                // Context adds exactly these boundaries around verified UTF-8 bytes.
                let overhead = "[Attached text begins]\n\n[Attached text ends]".len() as u64;
                if reference.byte_length.checked_add(overhead) != Some(length)
                    || !matches!(self.parts.first(), Some(CanonicalContentPart::Text(t)) if t.as_str().starts_with("[Attached text begins]\n"))
                    || !matches!(self.parts.last(), Some(CanonicalContentPart::Text(t)) if t.as_str().ends_with("\n[Attached text ends]"))
                {
                    return Err(bad());
                }
                let mut text = String::with_capacity(length as usize);
                for part in &self.parts {
                    if let CanonicalContentPart::Text(value) = part {
                        text.push_str(value.as_str());
                    }
                }
                let original = text
                    .strip_prefix("[Attached text begins]\n")
                    .and_then(|value| value.strip_suffix("\n[Attached text ends]"))
                    .ok_or_else(bad)?;
                if musteragent_content_ref::Sha256::digest(original.as_bytes()) != reference.sha256
                {
                    return Err(bad());
                }
            }
            kind => {
                let correct = matches!((kind, self.parts.as_slice()),
                    (InputAttachmentKind::Image, [CanonicalContentPart::Image(r)])
                    | (InputAttachmentKind::Audio, [CanonicalContentPart::Audio(r)])
                    | (InputAttachmentKind::Video, [CanonicalContentPart::Video(r)])
                    | (InputAttachmentKind::File, [CanonicalContentPart::File(r)]) if r == reference);
                if !correct {
                    return Err(bad());
                }
            }
        }
        Ok(())
    }
}

impl CanonicalRuntimeEvent {
    pub fn dialogue_inputs(&self) -> &[DialogueInputObservation] {
        match self {
            Self::AgentDialogueHistory {
                input_attachments, ..
            } => input_attachments,
            _ => &[],
        }
    }

    /// The provider and sizing view use this one ordered projection. Dialogue
    /// history and attachment text are quoted data; actual media stays typed.
    /// Canonical persistence retains the original source bytes and identities.
    pub fn validate_dialogue_inputs(&self) -> Result<(), CanonicalModelError> {
        let Self::AgentDialogueHistory {
            frozen_cut,
            from_seq,
            through_seq,
            input_attachments,
            ..
        } = self
        else {
            return Ok(());
        };
        let bad = || CanonicalModelError::InvalidDialogueInput;
        frozen_cut.validate().map_err(|_| bad())?;
        if *from_seq == 0
            || from_seq > through_seq
            || *through_seq > frozen_cut.through.seq
            || input_attachments.len()
                > 64 * musteragent_execution_context::DIALOGUE_CONTEXT_PAGE_ENTRIES
        {
            return Err(bad());
        }
        let mut last = None;
        let mut entries = BTreeMap::new();
        for input in input_attachments {
            input.validate(frozen_cut, *from_seq, *through_seq)?;
            let key = (input.cursor.seq, input.ordinal);
            if last.is_some_and(|value| value >= key)
                || entries
                    .insert(input.cursor.seq, input.entry_id)
                    .is_some_and(|id| id != input.entry_id)
            {
                return Err(bad());
            }
            last = Some(key);
        }
        Ok(())
    }
}

pub(crate) fn validate_request(request: &CanonicalModelRequest) -> Result<(), CanonicalModelError> {
    let mut seen = BTreeSet::new();
    let mut references = BTreeMap::new();
    for event in &request.runtime_events {
        event.validate_dialogue_inputs()?;
        for input in event.dialogue_inputs() {
            if !seen.insert((input.entry_id, input.ordinal)) {
                return Err(CanonicalModelError::InvalidDialogueInput);
            }
            check_ref(&mut references, &input.attachment.content_ref)?;
        }
    }
    for message in &request.input {
        for part in &message.content {
            if let CanonicalContentPart::Image(r)
            | CanonicalContentPart::Audio(r)
            | CanonicalContentPart::Video(r)
            | CanonicalContentPart::File(r) = part
            {
                check_ref(&mut references, r)?;
            }
        }
    }
    Ok(())
}

fn check_ref<'a>(
    refs: &mut BTreeMap<musteragent_identity::ContentId, &'a ContentRef>,
    reference: &'a ContentRef,
) -> Result<(), CanonicalModelError> {
    if refs
        .insert(reference.content_id, reference)
        .is_some_and(|previous| previous != reference)
    {
        return Err(CanonicalModelError::InvalidDialogueInput);
    }
    Ok(())
}
