//! Read original public facts at the frozen cut. A delivered bubble is
//! supplemental evidence until the originating model/tool exchange is available.
use crate::{
    AttachmentMaterializationError, ContextPlanError, materialize_dialogue_attachment_from_batch,
    materialize_input_body_from_batch, message_attachment_text_ref,
};
use musteragent_content_ref::ContentRef;
use musteragent_execution_context::{AgentDialogueContent, DialogueContextSourcePage};
use musteragent_runtime_content::{ContentBatch, ContentStoreError};
use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, CanonicalRole, ModelText};

/// Only referenced text belongs in the history read set. Binary inputs keep their
/// original handles for the active route and documents remain with the file tools.
pub fn dialogue_history_text_refs(page: &DialogueContextSourcePage) -> Vec<ContentRef> {
    let mut references = Vec::new();
    for entry in page
        .entries
        .iter()
        .filter(|entry| !entry.is_product_welcome())
    {
        match &entry.content {
            AgentDialogueContent::UserMessage {
                content_ref,
                input_attachments,
                ..
            } => {
                references.push(content_ref.clone());
                references.extend(
                    input_attachments
                        .iter()
                        .filter_map(|a| message_attachment_text_ref(&a.content_ref))
                        .cloned(),
                );
            }
            AgentDialogueContent::AgentMessage { content_ref } => {
                references.push(content_ref.clone())
            }
            _ => {}
        }
    }
    references
}

#[derive(Debug, thiserror::Error)]
pub enum DialogueHistoryMaterialError {
    #[error(transparent)]
    Content(#[from] ContentStoreError),
    #[error("invalid dialogue history material")]
    Corrupt,
}

/// The storage page is a batch, not a turn. A data block from an old public
/// delivery has no model-author identity. The planner can attach that block
/// to a real input without fabricating an assistant reply.
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum ConversationHistoryItem {
    Message(CanonicalMessage, Option<crate::AssistantOrigin>),
    Reference(Vec<ModelText>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DialogueHistoryMaterial {
    pub items: Vec<ConversationHistoryItem>,
    pub boundaries: Vec<DialogueHistoryBoundary>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DialogueHistoryBoundary {
    pub seq: u64,
    pub chain: musteragent_content_ref::Sha256,
    pub item_end: usize,
    pub intent: musteragent_identity::UserIntentId,
    pub begins: bool,
    pub closes: bool,
}

impl DialogueHistoryMaterial {
    pub fn summary(text: &str) -> Result<Self, ContextPlanError> {
        let parts = ModelText::chunked(format!("Historical reference (summary):\n{text}"))
            .map_err(|_| ContextPlanError::invariant())?;
        Ok(Self {
            items: vec![ConversationHistoryItem::Reference(parts)],
            boundaries: Vec::new(),
        })
    }
}

pub fn materialize_dialogue_history_with_transcripts(
    page: &DialogueContextSourcePage,
    batch: &ContentBatch,
    mut transcripts: std::collections::BTreeMap<
        musteragent_identity::DialogueEntryId,
        Vec<crate::ContextEntry>,
    >,
    closed: &std::collections::BTreeSet<musteragent_identity::DialogueEntryId>,
    extend_chain: impl Fn(
        musteragent_content_ref::Sha256,
        &[musteragent_execution_context::AgentDialogueEntry],
    ) -> musteragent_content_ref::Sha256,
) -> Result<DialogueHistoryMaterial, DialogueHistoryMaterialError> {
    use DialogueHistoryMaterialError::Corrupt;
    let mut items = Vec::with_capacity(page.entries.len());
    let mut boundaries = Vec::new();
    let mut chain = page.prefix_chain_sha256;
    for entry in &page.entries {
        chain = extend_chain(chain, std::slice::from_ref(entry));
        if entry.is_product_welcome() {
            continue;
        }
        let mut record_boundary = |item_end| {
            let begins = matches!(entry.content, AgentDialogueContent::UserMessage { .. });
            let closes = closed.contains(&entry.entry_id);
            if (begins || closes)
                && let Some(intent) = entry.source_intent_id
            {
                boundaries.push(DialogueHistoryBoundary {
                    seq: entry.cursor.seq,
                    chain,
                    item_end,
                    intent,
                    begins,
                    closes,
                });
            }
        };
        let mut original = transcripts.remove(&entry.entry_id);
        let append_original = |items: &mut Vec<ConversationHistoryItem>,
                               transcript: Vec<crate::ContextEntry>| {
            items.extend(transcript.into_iter().map(|entry| {
                ConversationHistoryItem::Message(entry.message, entry.assistant_origin)
            }));
        };
        if !matches!(entry.content, AgentDialogueContent::UserMessage { .. })
            && let Some(transcript) = original.take()
        {
            // A single native call may publish multiple UI entries.
            // A repeated anchor has an empty segment and contributes no second model event.
            append_original(&mut items, transcript);
            record_boundary(items.len());
            continue;
        }
        match &entry.content {
            AgentDialogueContent::UserMessage {
                content_ref,
                input_attachments,
            } => {
                let mut content = materialize_input_body_from_batch(
                    batch,
                    content_ref,
                    !input_attachments.is_empty(),
                )?;
                for (index, attachment) in input_attachments.iter().enumerate() {
                    // Opaque document bytes remain available through the file
                    // tools, while supported image/audio/video stay typed.
                    let parts = materialize_dialogue_attachment_from_batch(
                        batch,
                        attachment.content_ref.clone(),
                    )
                    .map_err(|e| match e {
                        AttachmentMaterializationError::Content(e) => {
                            DialogueHistoryMaterialError::Content(e)
                        }
                        AttachmentMaterializationError::Corrupt => Corrupt,
                    })?;
                    // Every attachment is named before its content, so text
                    // injected from one file and a document read through the
                    // file tools are never mistaken for the same upload.
                    let label = format!(
                        "Attachment {} of {}: {} ({}, content_id {})",
                        index + 1,
                        input_attachments.len(),
                        attachment.display_name.as_str(),
                        attachment.content_ref.media_type.as_str(),
                        attachment.content_ref.content_id,
                    );
                    content.push(CanonicalContentPart::Text(
                        ModelText::new(label).map_err(|_| Corrupt)?,
                    ));
                    content.extend(parts);
                }
                let message = CanonicalMessage {
                    role: CanonicalRole::User,
                    content,
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    tool_name: None,
                    reasoning: None,
                };
                message.validate().map_err(|_| Corrupt)?;
                items.push(ConversationHistoryItem::Message(message, None));
            }
            AgentDialogueContent::AgentMessage { content_ref } => {
                let text = std::str::from_utf8(batch.bytes(content_ref)?).map_err(|_| Corrupt)?;
                let parts =
                    ModelText::chunked(format!("Previously delivered to the user:\n{text}"))
                        .map_err(|_| Corrupt)?;
                items.push(ConversationHistoryItem::Reference(parts));
            }
            AgentDialogueContent::PartnerCreated(partner) => {
                let text = format!(
                    "Previously created partner: {} — {}",
                    partner.display_name, partner.description
                );
                items.push(ConversationHistoryItem::Reference(
                    ModelText::chunked(text).map_err(|_| Corrupt)?,
                ));
            }
            AgentDialogueContent::Artifact {
                artifact_id,
                artifact_version_id,
            } => {
                let delivered = ModelText::new(format!(
                    "Previously delivered artifact {artifact_id}, version {artifact_version_id}."
                ))
                .map_err(|_| Corrupt)?;
                items.push(ConversationHistoryItem::Reference(vec![delivered]));
            }
            AgentDialogueContent::Interaction { .. } | AgentDialogueContent::Erased { .. } => {}
        }
        if let Some(transcript) = original {
            append_original(&mut items, transcript);
        }
        record_boundary(items.len());
    }
    if chain != page.through_chain_sha256 {
        return Err(Corrupt);
    }
    Ok(DialogueHistoryMaterial { items, boundaries })
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundConversationHistory {
    pub cut: musteragent_execution_context::DialogueContextCut,
    pub from_seq: u64,
    pub through_seq: u64,
    pub source: ContentRef,
    pub history: DialogueHistoryMaterial,
}

impl DialogueHistoryMaterial {
    pub fn estimated_tokens(&self) -> u64 {
        Self::estimate(&self.items)
    }

    pub(crate) fn estimate(items: &[ConversationHistoryItem]) -> u64 {
        items.iter().fold(0u64, |total, item| {
            total.saturating_add(match item {
                ConversationHistoryItem::Message(m, _) => {
                    crate::token_estimate::estimate_message(m)
                }
                ConversationHistoryItem::Reference(parts) => parts.iter().fold(0u64, |n, p| {
                    n.saturating_add(crate::token_estimate::estimate_text_tokens(p.as_str()))
                }),
            })
        })
    }
}

/// Page boundaries are storage details. Pending reference parts may cross a
/// page; consume them once, keeping original media and role order.
pub fn append_conversation_history<'a>(
    items: impl IntoIterator<Item = &'a ConversationHistoryItem>,
    messages: &mut Vec<(CanonicalMessage, Option<crate::AssistantOrigin>)>,
    references: &mut Vec<CanonicalContentPart>,
) {
    for item in items {
        match item {
            ConversationHistoryItem::Reference(parts) => {
                references.extend(parts.iter().cloned().map(CanonicalContentPart::Text))
            }
            ConversationHistoryItem::Message(m, origin) => {
                let mut msg = m.clone();
                if msg.role == CanonicalRole::User && !references.is_empty() {
                    let mut content = std::mem::take(references);
                    content.append(&mut msg.content);
                    msg.content = content;
                }
                messages.push((msg, origin.clone()));
            }
        }
    }
}

impl BoundConversationHistory {
    pub fn entry(
        self,
        sequence: musteragent_identity::ConversationSeq,
        record: musteragent_identity::LedgerRecordId,
        run: musteragent_identity::RunId,
    ) -> crate::ContextEntry {
        crate::ContextEntry {
            history_page: Some(self),
            runtime_events: Vec::new(),
            source_sequence: sequence,
            presentation_order: crate::ContextPresentationOrder::ledger(sequence),
            source_record_ids: vec![record],
            source_run_id: Some(run),
            assistant_origin: None,
            message: CanonicalMessage {
                role: CanonicalRole::User,
                content: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            continuation_delta_for: None,
        }
    }
}

/// Select complete exchanges between two original publications, inclusive of
/// all siblings of the delivery call. The upper bound is the assistant's
/// exchange anchor; using the result's ledger sequence can split parallel calls.
pub fn conversation_history_segment(
    entries: &[crate::ContextEntry],
    previous: Option<musteragent_identity::ConversationSeq>,
    delivery_result: Option<musteragent_identity::LedgerRecordId>,
    finished_at_cut: bool,
) -> Option<Vec<crate::ContextEntry>> {
    // An unpublished terminal run attaches to its original user message. For
    // a published run only the last publication owns the closed private tail.
    let end = match delivery_result {
        Some(result) => {
            let anchor = entries
                .iter()
                .find(|entry| {
                    entry.message.role == CanonicalRole::Tool
                        && entry.source_record_ids.contains(&result)
                })?
                .presentation_order
                .anchor_sequence;
            (!finished_at_cut).then_some(anchor)
        }
        None if finished_at_cut => None,
        None => return None,
    };
    Some(
        entries
            .iter()
            .filter(|entry| {
                let position = entry.presentation_order.anchor_sequence;
                end.is_none_or(|end| position <= end)
                    && previous.is_none_or(|start| position > start)
            })
            .cloned()
            .collect(),
    )
}

impl DialogueHistoryMaterial {
    /// Only conversational text, tool arguments and returned text are searchable.
    /// Binary media, reasoning signatures and storage provenance stay at source.
    pub fn original_text(self) -> Vec<(&'static str, String)> {
        let mut rows = Vec::new();
        for item in self.items {
            match item {
                ConversationHistoryItem::Reference(parts) => rows.push((
                    "reference",
                    parts.iter().map(ModelText::as_str).collect::<String>(),
                )),
                ConversationHistoryItem::Message(message, _) => {
                    let role = match message.role {
                        CanonicalRole::User => "user",
                        CanonicalRole::Assistant => "assistant",
                        CanonicalRole::Tool => "tool",
                    };
                    let text = message
                        .content
                        .into_iter()
                        .filter_map(|part| {
                            if let CanonicalContentPart::Text(t) = part {
                                Some(t.as_str().to_owned())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !text.is_empty() {
                        rows.push((role, text));
                    }
                    for call in message.tool_calls {
                        let args = crate::compaction_history::json_to_serde(&call.arguments);
                        rows.push(("assistant", format!("{} {}", call.name.as_str(), args)));
                    }
                }
            }
        }
        rows
    }
}
