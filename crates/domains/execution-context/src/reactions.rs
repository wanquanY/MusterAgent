//! Current-account message feedback. Bit positions are persistent catalog IDs.
use musteragent_identity::{AgentDialogueId, AgentId, DialogueEntryId};

pub const PUBLIC_REACTION_KEYS: [&str; 12] = [
    "👍", "👎", "🙌", "👌", "👀", "❤️", "😂", "🎉", "🤔", "😮", "😢", "🚀",
];
pub const MAX_PUBLIC_REACTIONS: usize = 8;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PublicReactionSet(u16);
impl PublicReactionSet {
    pub const EMPTY: Self = Self(0);
    pub fn from_mask(mask: u16) -> Result<Self, &'static str> {
        if mask >= (1 << PUBLIC_REACTION_KEYS.len())
            || mask.count_ones() as usize > MAX_PUBLIC_REACTIONS
        {
            return Err("reaction bounds");
        }
        Ok(Self(mask))
    }
    pub fn from_keys(keys: &[String]) -> Result<Self, &'static str> {
        let mut mask = 0;
        for key in keys {
            let index = Self::index(key)?;
            if mask & (1 << index) != 0 {
                return Err("duplicate reaction");
            }
            mask |= 1 << index;
        }
        let result = Self::from_mask(mask)?;
        if result.keys() != keys {
            return Err("reaction ordering");
        }
        Ok(result)
    }
    pub fn index(key: &str) -> Result<usize, &'static str> {
        PUBLIC_REACTION_KEYS
            .iter()
            .position(|v| *v == key)
            .ok_or("unknown reaction")
    }
    pub fn mask(self) -> u16 {
        self.0
    }
    pub fn keys(self) -> Vec<String> {
        PUBLIC_REACTION_KEYS
            .iter()
            .enumerate()
            .filter(|(i, _)| self.0 & (1 << i) != 0)
            .map(|(_, v)| (*v).to_owned())
            .collect()
    }
    pub fn set(self, key: &str, present: bool) -> Result<Self, &'static str> {
        let bit = 1 << Self::index(key)?;
        Self::from_mask(if present { self.0 | bit } else { self.0 & !bit })
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicEntryReactions {
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub revision: u64,
    pub selected: PublicReactionSet,
}
impl PublicEntryReactions {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.revision > i64::MAX as u64
            || (self.revision == 0 && self.selected != PublicReactionSet::EMPTY)
        {
            return Err("reaction revision");
        }
        Ok(())
    }
    pub fn changed(&self) -> PublicEntryReactionsChanged {
        PublicEntryReactionsChanged {
            agent_id: self.agent_id,
            dialogue_id: self.dialogue_id,
            entry_id: self.entry_id,
            revision: self.revision,
        }
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PublicEntryReactionsChanged {
    pub agent_id: AgentId,
    pub dialogue_id: AgentDialogueId,
    pub entry_id: DialogueEntryId,
    pub revision: u64,
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_canonical_set_and_explicit_remove() {
        let mut value = PublicReactionSet::EMPTY;
        for key in &PUBLIC_REACTION_KEYS[..8] {
            value = value.set(key, true).unwrap();
        }
        assert!(value.set("🤔", true).is_err());
        assert_eq!(value.set("👍", true).unwrap(), value);
        assert!(value.set("❤", true).is_err());
        assert!(PublicReactionSet::from_keys(&["👍".into(), "👍".into()]).is_err());
        assert!(PublicReactionSet::from_keys(&["😂".into(), "👍".into()]).is_err());
        assert_eq!(PublicReactionSet::from_keys(&value.keys()).unwrap(), value);
        assert_eq!(value.set("👍", false).unwrap().keys().len(), 7);
    }
}
