//! Public Timeline query contract. Continuations never grant authority.
use crate::{DialogueHistoryEntry, DialogueStoreError};
use musteragent_execution_context::AgentDialogue;
use musteragent_identity::{Actor, AgentId, UtcTimestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogueHistoryCategory {
    All,
    Files,
    Images,
    Links,
}
impl DialogueHistoryCategory {
    pub fn key(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Files => "files",
            Self::Images => "images",
            Self::Links => "links",
        }
    }
}
#[derive(Debug, Clone)]
pub struct DialogueHistorySearchQuery {
    pub actor: Actor,
    pub text: String,
    pub category: DialogueHistoryCategory,
    pub agent_id: Option<AgentId>,
    pub from: Option<UtcTimestamp>,
    pub to: Option<UtcTimestamp>,
    pub cursor: Option<String>,
    pub limit: u16,
}
impl DialogueHistorySearchQuery {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != musteragent_identity::PrincipalKind::User
            || self.text.len() > 512
            || self.text.contains('\0')
            || self.text.split_whitespace().count() > 16
            || !(1..=50).contains(&self.limit)
            || self
                .cursor
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 512)
            || matches!((self.from, self.to), (Some(a),Some(b)) if a >= b)
        {
            return Err(DialogueStoreError::Unavailable);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DialogueHistorySearchHit {
    pub dialogue: AgentDialogue,
    pub entry: DialogueHistoryEntry,
    pub excerpt: String,
}
#[derive(Debug, Clone)]
pub struct DialogueHistorySearchPage {
    pub hits: Vec<DialogueHistorySearchHit>,
    pub next_cursor: Option<String>,
}
