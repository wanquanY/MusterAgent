//! Exact portable Dialogue context sources. Unknown/malformed sources fail closed.
use crate::{dialogue_codec::*, encode_content_ref, wire};
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use prost::Message;
type Error = DialogueCodecError;

pub fn scene_member(member: &AgentSceneMember) -> wire::AgentSceneMember {
    wire::AgentSceneMember {
        agent_id: Some(wire::AgentId {
            value: member.agent_id.to_string(),
        }),
        name: member.name.clone(),
        responsibility: member.responsibility.clone(),
    }
}
pub fn decode_scene_member(member: wire::AgentSceneMember) -> Result<AgentSceneMember, Error> {
    Ok(AgentSceneMember {
        agent_id: id(member.agent_id.map(|id| id.value))?,
        name: member.name,
        responsibility: member.responsibility,
    })
}
fn hash(value: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: value.into_bytes().to_vec(),
    }
}
fn read_hash(value: Option<wire::Sha256>) -> Result<Sha256, Error> {
    Sha256::from_bytes(required(value)?.value).map_err(|_| Error::Corrupt)
}
pub fn cut(v: &DialogueContextCut) -> wire::DialogueContextCut {
    wire::DialogueContextCut {
        tenant_id: Some(wire::TenantId {
            value: v.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: v.owner_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: v.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: v.dialogue_id.to_string(),
        }),
        through: Some(cursor(v.through)),
        entry_count: v.entry_count,
        source_chain_sha256: Some(hash(v.source_chain_sha256)),
        source_contract_ref: Some(wire::VersionedRef {
            key: DIALOGUE_CONTEXT_SOURCE_CONTRACT.into(),
        }),
    }
}
pub fn decode_cut(v: wire::DialogueContextCut) -> Result<DialogueContextCut, Error> {
    if required(v.source_contract_ref)?.key != DIALOGUE_CONTEXT_SOURCE_CONTRACT {
        return Err(Error::Corrupt);
    }
    let v = DialogueContextCut {
        tenant_id: id(v.tenant_id.map(|v| v.value))?,
        owner_id: id(v.owner_id.map(|v| v.value))?,
        agent_id: id(v.agent_id.map(|v| v.value))?,
        dialogue_id: id(v.dialogue_id.map(|v| v.value))?,
        through: decode_cursor(v.through)?,
        entry_count: v.entry_count,
        source_chain_sha256: read_hash(v.source_chain_sha256)?,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    Ok(v)
}
pub fn checkpoint(v: &DialogueContextCheckpoint) -> wire::DialogueContextCheckpoint {
    wire::DialogueContextCheckpoint {
        checkpoint_id: Some(wire::DialogueContextCheckpointId {
            value: v.checkpoint_id.to_string(),
        }),
        coverage: Some(cut(&v.coverage)),
        parent_checkpoint_id: v
            .parent_checkpoint_id
            .map(|id| wire::DialogueContextCheckpointId {
                value: id.to_string(),
            }),
        summary_ref: Some(encode_content_ref(&v.summary_ref)),
        source_manifest_ref: Some(encode_content_ref(&v.source_manifest_ref)),
        generator_invocation_id: Some(wire::InvocationId {
            value: v.generator_invocation_id.to_string(),
        }),
        policy_ref: Some(wire::VersionedRef {
            key: v.policy_ref.as_str().to_owned(),
        }),
    }
}
pub fn decode_checkpoint(
    v: wire::DialogueContextCheckpoint,
) -> Result<DialogueContextCheckpoint, Error> {
    let v = DialogueContextCheckpoint {
        checkpoint_id: id(v.checkpoint_id.map(|v| v.value))?,
        coverage: decode_cut(required(v.coverage)?)?,
        parent_checkpoint_id: v
            .parent_checkpoint_id
            .map(|v| id(Some(v.value)))
            .transpose()?,
        summary_ref: content(required(v.summary_ref)?)?,
        source_manifest_ref: content(required(v.source_manifest_ref)?)?,
        generator_invocation_id: id(v.generator_invocation_id.map(|v| v.value))?,
        policy_ref: musteragent_identity::VersionedKey::new(required(v.policy_ref)?.key)
            .map_err(|_| Error::Corrupt)?,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    Ok(v)
}
pub fn page(v: &DialogueContextSourcePage) -> wire::DialogueContextSourcePage {
    wire::DialogueContextSourcePage {
        frozen_cut: Some(cut(&v.frozen_cut)),
        from_seq: v.from_seq,
        through_seq: v.through_seq,
        entries: v.entries.iter().map(entry).collect(),
        checkpoint: v.checkpoint.as_ref().map(checkpoint),
        prefix_chain_sha256: Some(hash(v.prefix_chain_sha256)),
        through_chain_sha256: Some(hash(v.through_chain_sha256)),
    }
}
pub fn decode_page(v: wire::DialogueContextSourcePage) -> Result<DialogueContextSourcePage, Error> {
    let v = DialogueContextSourcePage {
        frozen_cut: decode_cut(required(v.frozen_cut)?)?,
        from_seq: v.from_seq,
        through_seq: v.through_seq,
        entries: v
            .entries
            .into_iter()
            .map(decode_entry)
            .collect::<Result<_, _>>()?,
        checkpoint: v.checkpoint.map(decode_checkpoint).transpose()?,
        prefix_chain_sha256: read_hash(v.prefix_chain_sha256)?,
        through_chain_sha256: read_hash(v.through_chain_sha256)?,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    if v.checkpoint.is_none()
        && extend_page_chain(v.prefix_chain_sha256, &v.entries) != v.through_chain_sha256
    {
        return Err(Error::Corrupt);
    }
    Ok(v)
}
/// The canonical hash at an entry boundary, including one inside a source page.
pub fn extend_page_chain(prefix: Sha256, entries: &[AgentDialogueEntry]) -> Sha256 {
    entries.iter().fold(prefix, |chain, entry| {
        let payload = dialogue_context_entry_hash(entry, |r| encode_content_ref(r).encode_to_vec());
        let mut bytes = [0_u8; 64];
        bytes[..32].copy_from_slice(&chain.into_bytes());
        bytes[32..].copy_from_slice(&payload.into_bytes());
        Sha256::digest(bytes)
    })
}

/// Hash a cursor inside a verified page, including either endpoint.
pub fn page_chain_at(page: &DialogueContextSourcePage, through: u64) -> Option<Sha256> {
    if page.checkpoint.is_some()
        || through < page.from_seq.checked_sub(1)?
        || through > page.through_seq
    {
        return None;
    }
    let n = usize::try_from(through + 1 - page.from_seq).ok()?;
    Some(extend_page_chain(
        page.prefix_chain_sha256,
        page.entries.get(..n)?,
    ))
}

/// Keep the native suffix of a storage page that straddles a durable checkpoint.
pub fn after_checkpoint(
    mut page: DialogueContextSourcePage,
    checkpoint: &DialogueContextCheckpoint,
) -> Option<DialogueContextSourcePage> {
    if !page.frozen_cut.same_dialogue(&checkpoint.coverage) {
        return None;
    }
    let through = checkpoint.coverage.through.seq;
    if through < page.from_seq {
        return Some(page);
    }
    if through >= page.through_seq
        || page_chain_at(&page, through)? != checkpoint.coverage.source_chain_sha256
    {
        return None;
    }
    page.entries
        .drain(..usize::try_from(through + 1 - page.from_seq).ok()?);
    page.prefix_chain_sha256 = checkpoint.coverage.source_chain_sha256;
    page.from_seq = through + 1;
    page.validate().ok()?;
    Some(page)
}

pub fn bound(v: &DialogueContextBound) -> wire::DialogueContextBound {
    wire::DialogueContextBound {
        frozen_cut: Some(cut(&v.frozen_cut)),
        page_ordinal: v.page_ordinal.get(),
        from_seq: v.from_seq,
        through_seq: v.through_seq,
        source_page_ref: Some(encode_content_ref(&v.source_page_ref)),
        checkpoint_id: v.checkpoint_id.map(|id| wire::DialogueContextCheckpointId {
            value: id.to_string(),
        }),
        completes_cut: v.completes_cut,
    }
}
pub fn decode_bound(v: wire::DialogueContextBound) -> Result<DialogueContextBound, Error> {
    let v = DialogueContextBound {
        frozen_cut: decode_cut(required(v.frozen_cut)?)?,
        page_ordinal: std::num::NonZeroU32::new(v.page_ordinal).ok_or(Error::Corrupt)?,
        from_seq: v.from_seq,
        through_seq: v.through_seq,
        source_page_ref: content(required(v.source_page_ref)?)?,
        checkpoint_id: v.checkpoint_id.map(|v| id(Some(v.value))).transpose()?,
        completes_cut: v.completes_cut,
    };
    v.validate().map_err(|_| Error::Corrupt)?;
    Ok(v)
}
pub fn page_bytes(v: &DialogueContextSourcePage) -> Result<Vec<u8>, Error> {
    v.validate().map_err(|_| Error::Corrupt)?;
    let bytes = page(v).encode_to_vec();
    if bytes.len() > 256 * 1024 {
        return Err(Error::Corrupt);
    }
    Ok(bytes)
}
/// Persisted discriminator is separate from the immutable page hash. Legacy
/// formats may not carry attachments even if a caller supplies valid new bytes.
pub fn page_wire_version(value: &DialogueContextSourcePage) -> i16 {
    if value
        .entries
        .iter()
        .any(|entry| matches!(&entry.content, AgentDialogueContent::Erased { .. }))
    {
        return 4;
    }
    if value.entries.iter().any(|entry| matches!(&entry.content, AgentDialogueContent::UserMessage { input_attachments, .. } if !input_attachments.is_empty())) { 3 } else { 2 }
}
pub fn page_from_bytes_for_version(
    bytes: &[u8],
    version: i16,
) -> Result<DialogueContextSourcePage, Error> {
    let value = page_from_bytes(bytes)?;
    if !matches!(version, 2..=4) || page_wire_version(&value) != version {
        return Err(Error::Corrupt);
    }
    Ok(value)
}
pub fn page_from_bytes(bytes: &[u8]) -> Result<DialogueContextSourcePage, Error> {
    if bytes.len() > 256 * 1024 {
        return Err(Error::Corrupt);
    }
    let v = wire::DialogueContextSourcePage::decode(bytes).map_err(|_| Error::Corrupt)?;
    if v.encode_to_vec() != bytes {
        return Err(Error::Corrupt);
    }
    decode_page(v)
}

/// Verify frozen format-1 bytes against a complete authoritative source page.
/// The caller must select this only using the persisted format discriminator,
/// after the source owner has checked access, immutable payloads and their hash
/// chain. No field is inferred from the old wire or the surrounding dialogue.
/// Format 2 must use `page_from_bytes` and must never fall back to this path.
pub fn verify_legacy_page(
    bytes: &[u8],
    authoritative: &DialogueContextSourcePage,
) -> Result<(), Error> {
    if bytes.len() > 256 * 1024
        || authoritative.checkpoint.is_some()
        || page_wire_version(authoritative) != 2
    {
        return Err(Error::Corrupt);
    }
    let mut expected = page(authoritative);
    // Independently recheck the complete typed page, including author hashes.
    decode_page(expected.clone())?;
    for entry in &mut expected.entries {
        // Format 1 retained nested text authors, but had no envelope field.
        entry.author_agent_id = None;
    }
    if expected.encode_to_vec() != bytes {
        return Err(Error::Corrupt);
    }
    Ok(())
}
pub fn observation(
    v: &musteragent_runtime_model::CanonicalRuntimeEvent,
) -> Result<wire::DialogueContextModelObservation, Error> {
    let musteragent_runtime_model::CanonicalRuntimeEvent::AgentDialogueHistory {
        frozen_cut,
        from_seq,
        through_seq,
        source_page_ref,
        materialized_context,
        input_attachments,
    } = v
    else {
        return Err(Error::Corrupt);
    };
    v.validate_dialogue_inputs().map_err(|_| Error::Corrupt)?;
    Ok(wire::DialogueContextModelObservation {
        frozen_cut: Some(cut(frozen_cut)),
        from_seq: *from_seq,
        through_seq: *through_seq,
        source_page_ref: Some(encode_content_ref(source_page_ref)),
        materialized_context: materialized_context.as_str().into(),
        input_attachments: input_attachments.iter().map(input_observation).collect(),
    })
}
pub fn decode_observation(
    v: wire::DialogueContextModelObservation,
) -> Result<musteragent_runtime_model::CanonicalRuntimeEvent, Error> {
    let frozen_cut = decode_cut(required(v.frozen_cut)?)?;
    if v.from_seq == 0 || v.from_seq > v.through_seq || v.through_seq > frozen_cut.through.seq {
        return Err(Error::Corrupt);
    }
    let event = musteragent_runtime_model::CanonicalRuntimeEvent::AgentDialogueHistory {
        input_attachments: v
            .input_attachments
            .into_iter()
            .map(decode_input_observation)
            .collect::<Result<_, _>>()?,
        frozen_cut,
        from_seq: v.from_seq,
        through_seq: v.through_seq,
        source_page_ref: content(required(v.source_page_ref)?)?,
        materialized_context: musteragent_runtime_model::ModelText::new(v.materialized_context)
            .map_err(|_| Error::Corrupt)?,
    };
    event
        .validate_dialogue_inputs()
        .map_err(|_| Error::Corrupt)?;
    Ok(event)
}

pub(crate) fn input_observation(
    v: &musteragent_runtime_model::DialogueInputObservation,
) -> wire::DialogueInputModelObservation {
    use musteragent_runtime_model::{
        CanonicalContentPart, InputAttachmentKind, input_attachment_kind,
    };
    let materialization = match input_attachment_kind(&v.attachment.content_ref) {
        InputAttachmentKind::Text => 1,
        // A media reference is one validated, source-bound text fragment.
        // Previously persisted native media still decodes as native media.
        InputAttachmentKind::Image | InputAttachmentKind::Audio | InputAttachmentKind::Video
            if matches!(v.parts.as_slice(), [CanonicalContentPart::Text(_)]) =>
        {
            1
        }
        InputAttachmentKind::Image => 2,
        InputAttachmentKind::Audio => 3,
        InputAttachmentKind::Video => 4,
        InputAttachmentKind::File if v.parts.is_empty() => 6,
        InputAttachmentKind::File => 5,
    };
    wire::DialogueInputModelObservation {
        entry_id: Some(wire::DialogueEntryId {
            value: v.entry_id.to_string(),
        }),
        cursor: Some(cursor(v.cursor)),
        ordinal: v.ordinal,
        attachment: Some(attachment(&v.attachment)),
        materialization,
        text_parts: v
            .parts
            .iter()
            .filter_map(|part| match part {
                CanonicalContentPart::Text(t) => Some(t.as_str().into()),
                _ => None,
            })
            .collect(),
    }
}
pub(crate) fn decode_input_observation(
    v: wire::DialogueInputModelObservation,
) -> Result<musteragent_runtime_model::DialogueInputObservation, Error> {
    use musteragent_runtime_model::{
        CanonicalContentPart as Part, DialogueInputObservation, ModelText,
    };
    let a = decode_attachment(required(v.attachment.clone())?)?;
    let reference = a.content_ref.clone();
    if v.materialization != 1 && !v.text_parts.is_empty() {
        return Err(Error::Corrupt);
    }
    let parts = match v.materialization {
        1 => v
            .text_parts
            .iter()
            .map(|t| {
                ModelText::new(t.clone())
                    .map(Part::Text)
                    .map_err(|_| Error::Corrupt)
            })
            .collect::<Result<_, _>>()?,
        2 => vec![Part::Image(reference)],
        3 => vec![Part::Audio(reference)],
        4 => vec![Part::Video(reference)],
        5 => vec![Part::File(reference)],
        6 => Vec::new(),
        _ => return Err(Error::Corrupt),
    };
    let result = DialogueInputObservation {
        entry_id: id(v.entry_id.as_ref().map(|id| id.value.clone()))?,
        cursor: decode_cursor(v.cursor)?,
        ordinal: v.ordinal,
        attachment: a,
        parts,
    };
    if input_observation(&result) != v {
        return Err(Error::Corrupt);
    }
    Ok(result)
}
