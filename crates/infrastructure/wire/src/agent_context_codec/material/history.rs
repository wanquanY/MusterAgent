//! Host-to-executor transfer of verified dialogue pages.
use super::*;
use wire::agent_context_dialogue_history_item::Item;

pub(super) fn encode(page: &BoundConversationHistory) -> Result<wire::AgentContextDialogueHistory> {
    let items = page
        .history
        .items
        .iter()
        .map(|item| {
            let assistant_origin = match item {
                ConversationHistoryItem::Message(_, o) => o.as_ref().map(origin),
                _ => None,
            };
            let item = match item {
                ConversationHistoryItem::Message(m, _) => {
                    Item::Message(checked(crate::context_support::messages::encode(m))?)
                }
                ConversationHistoryItem::Reference(text) => {
                    Item::Reference(wire::AgentContextHistoryReference {
                        parts: text.iter().map(|t| t.as_str().to_owned()).collect(),
                    })
                }
            };
            Ok(wire::AgentContextDialogueHistoryItem {
                item: Some(item),
                assistant_origin,
            })
        })
        .collect::<Result<_>>()?;
    Ok(wire::AgentContextDialogueHistory {
        cut: Some(crate::dialogue_context_codec::cut(&page.cut)),
        from_seq: page.from_seq,
        through_seq: page.through_seq,
        source: Some(crate::encode_content_ref(&page.source)),
        items,
        boundaries: page
            .history
            .boundaries
            .iter()
            .map(|b| wire::AgentContextHistoryBoundary {
                seq: b.seq,
                chain: Some(wire::Sha256 {
                    value: b.chain.into_bytes().to_vec(),
                }),
                item_end: b.item_end as u64,
                intent: b.intent.to_string(),
                begins: b.begins,
                closes: b.closes,
            })
            .collect(),
    })
}

pub(super) fn decode(page: wire::AgentContextDialogueHistory) -> Result<BoundConversationHistory> {
    let cut = checked(crate::dialogue_context_codec::decode_cut(required(
        page.cut,
    )?))?;
    if page.from_seq == 0 || page.from_seq > page.through_seq || page.through_seq > cut.through.seq
    {
        return Err(AgentContextCodecError::Invalid);
    }
    let items: Vec<_> = page
        .items
        .into_iter()
        .map(|item| match required(item.item)? {
            Item::Message(m) => Ok(ConversationHistoryItem::Message(
                checked(crate::context_support::messages::decode(m))?,
                item.assistant_origin.map(decode_origin).transpose()?,
            )),
            Item::Reference(reference) => {
                if item.assistant_origin.is_some() || reference.parts.is_empty() {
                    return Err(AgentContextCodecError::Invalid);
                }
                let parts = reference
                    .parts
                    .into_iter()
                    .map(|t| checked(musteragent_runtime_model::ModelText::new(t)))
                    .collect::<Result<_>>()?;
                Ok(ConversationHistoryItem::Reference(parts))
            }
        })
        .collect::<Result<_>>()?;
    let mut previous = (page.from_seq - 1, 0);
    let boundaries = page
        .boundaries
        .into_iter()
        .map(|b| {
            let end = checked(usize::try_from(b.item_end))?;
            if b.seq <= previous.0
                || b.seq > page.through_seq
                || end < previous.1
                || end > items.len()
                || !(b.begins || b.closes)
            {
                return Err(AgentContextCodecError::Invalid);
            }
            previous = (b.seq, end);
            Ok(musteragent_context::DialogueHistoryBoundary {
                seq: b.seq,
                chain: checked(musteragent_content_ref::Sha256::from_bytes(
                    required(b.chain)?.value,
                ))?,
                item_end: end,
                intent: id(Some(b.intent))?,
                begins: b.begins,
                closes: b.closes,
            })
        })
        .collect::<Result<_>>()?;
    Ok(BoundConversationHistory {
        cut,
        from_seq: page.from_seq,
        through_seq: page.through_seq,
        source: content(required(page.source)?)?,
        history: DialogueHistoryMaterial { items, boundaries },
    })
}
