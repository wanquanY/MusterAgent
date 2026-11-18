//! Closed, bounded reaction metadata. No content, authority, or Agent persona.
use crate::{dialogue_codec::id, wire};
use musteragent_execution_context::{
    PublicEntryReactions, PublicEntryReactionsChanged, PublicReactionSet,
};
pub fn encode(value: &PublicEntryReactions) -> wire::PublicEntryReactions {
    wire::PublicEntryReactions {
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: value.dialogue_id.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: value.entry_id.to_string(),
        }),
        revision: value.revision,
        emoji_keys: value.selected.keys(),
    }
}
pub fn decode(value: wire::PublicEntryReactions) -> Result<PublicEntryReactions, &'static str> {
    let result = PublicEntryReactions {
        agent_id: id(value.agent_id.map(|v| v.value)).map_err(|_| "agent")?,
        dialogue_id: id(value.dialogue_id.map(|v| v.value)).map_err(|_| "dialogue")?,
        entry_id: id(value.entry_id.map(|v| v.value)).map_err(|_| "entry")?,
        revision: value.revision,
        selected: PublicReactionSet::from_keys(&value.emoji_keys)?,
    };
    result.validate()?;
    Ok(result)
}
pub fn encode_changed(value: &PublicEntryReactionsChanged) -> wire::PublicEntryReactionsChanged {
    wire::PublicEntryReactionsChanged {
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: value.dialogue_id.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: value.entry_id.to_string(),
        }),
        revision: value.revision,
    }
}
pub fn decode_changed(
    value: wire::PublicEntryReactionsChanged,
) -> Result<PublicEntryReactionsChanged, &'static str> {
    if value.revision == 0 || value.revision > i64::MAX as u64 {
        return Err("reaction revision");
    }
    Ok(PublicEntryReactionsChanged {
        agent_id: id(value.agent_id.map(|v| v.value)).map_err(|_| "agent")?,
        dialogue_id: id(value.dialogue_id.map(|v| v.value)).map_err(|_| "dialogue")?,
        entry_id: id(value.entry_id.map(|v| v.value)).map_err(|_| "entry")?,
        revision: value.revision,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::{AgentDialogueId, AgentId, DialogueEntryId};
    #[test]
    fn canonical_reaction_wire_rejects_alias_duplicates_and_bad_revisions() {
        let current = PublicEntryReactions {
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            entry_id: DialogueEntryId::generate(),
            revision: 9007199254740993,
            selected: PublicReactionSet::EMPTY.set("❤️", true).unwrap(),
        };
        assert_eq!(decode(encode(&current)).unwrap(), current);
        assert_eq!(
            decode_changed(encode_changed(&current.changed())).unwrap(),
            current.changed()
        );
        let mut invalid = encode(&current);
        invalid.revision = 0;
        assert!(decode(invalid).is_err());
        let mut invalid = encode(&current);
        invalid.emoji_keys = vec!["❤".into()];
        assert!(decode(invalid).is_err());
        let mut invalid = encode(&current);
        invalid.emoji_keys = vec!["👍".into(), "👍".into()];
        assert!(decode(invalid).is_err());
        let mut invalid = encode_changed(&current.changed());
        invalid.revision = 0;
        assert!(decode_changed(invalid).is_err());
    }
}
