use super::{AgentMemoryDraft, AgentMemoryDraftError, AgentMemoryDraftStatus};
use musteragent_identity::{AgentId, MemoryDraftId, PrincipalId, TenantId, UtcTimestamp};

/// A continuation belongs to one authenticated owner, Agent and lifecycle.
/// It is a position, not an authorization grant or a cross-request snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftCursor {
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub status: AgentMemoryDraftStatus,
    pub updated_at: UtcTimestamp,
    pub draft_id: MemoryDraftId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListAgentMemoryDrafts {
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub status: AgentMemoryDraftStatus,
    pub limit: u32,
    pub cursor: Option<AgentMemoryDraftCursor>,
}

impl ListAgentMemoryDrafts {
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if !(1..=50).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.tenant_id != self.tenant_id
                    || cursor.owner_principal_id != self.owner_principal_id
                    || cursor.agent_id != self.agent_id
                    || cursor.status != self.status
            })
        {
            return Err(AgentMemoryDraftError::InvalidListQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftPage {
    pub drafts: Vec<AgentMemoryDraft>,
    pub next_cursor: Option<AgentMemoryDraftCursor>,
}

impl AgentMemoryDraftPage {
    /// Stores fetch at most limit + 1 rows using the original owner index:
    /// updated_at DESC, draft_id ASC. The extra row detects continuation;
    /// truncation without a cursor, offset paging and unbounded reads are not
    /// valid implementations of this port.
    pub fn from_sorted_drafts(
        query: &ListAgentMemoryDrafts,
        mut drafts: Vec<AgentMemoryDraft>,
    ) -> Result<Self, AgentMemoryDraftError> {
        query.validate()?;
        if drafts.len() > query.limit as usize + 1 {
            return Err(AgentMemoryDraftError::InvalidListPage);
        }
        let mut previous = query
            .cursor
            .as_ref()
            .map(|cursor| (cursor.updated_at, cursor.draft_id));
        for draft in &drafts {
            draft.validate()?;
            if draft.tenant_id != query.tenant_id
                || draft.owner_principal_id != query.owner_principal_id
                || draft.agent_id != query.agent_id
                || draft.status != query.status
                || previous.is_some_and(|(updated_at, draft_id)| {
                    draft.updated_at > updated_at
                        || (draft.updated_at == updated_at && draft.draft_id <= draft_id)
                })
            {
                return Err(AgentMemoryDraftError::InvalidListPage);
            }
            previous = Some((draft.updated_at, draft.draft_id));
        }
        let has_more = drafts.len() > query.limit as usize;
        drafts.truncate(query.limit as usize);
        let next_cursor = if has_more {
            drafts.last().map(|draft| AgentMemoryDraftCursor {
                tenant_id: query.tenant_id,
                owner_principal_id: query.owner_principal_id,
                agent_id: query.agent_id,
                status: query.status,
                updated_at: draft.updated_at,
                draft_id: draft.draft_id,
            })
        } else {
            None
        };
        Ok(Self {
            drafts,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests;
