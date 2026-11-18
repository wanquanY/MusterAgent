use super::*;
use musteragent_evolution::{CandidateStatus, EvolutionScope};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftReviewItem {
    pub draft: AgentMemoryDraft,
    /// The same governance projection as Agent inspection, including actual
    /// Evaluation/Approval/publication facts. Pending and failed evaluations
    /// remain inspectable; no transport may synthesize publication evidence.
    pub proposal: crate::EvolutionProposalInspection,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftReviewBatch {
    pub batch_id: MemoryDraftBatchId,
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub submitted_at: UtcTimestamp,
    pub items: Vec<AgentMemoryDraftReviewItem>,
}

impl AgentMemoryDraftReviewBatch {
    /// Immutable batch membership, authored content and governance must describe
    /// one scoped snapshot. Readiness still belongs to the original proposal.
    pub fn validate(&self) -> Result<(), AgentMemoryDraftError> {
        if self.items.is_empty() || self.items.len() > 100 {
            return Err(AgentMemoryDraftError::InvalidBatchSize);
        }
        let mut drafts = BTreeSet::new();
        let mut memories = BTreeSet::new();
        let mut proposals = BTreeSet::new();
        for item in &self.items {
            let draft = &item.draft;
            let candidate = &item.proposal.candidate;
            draft.validate()?;
            item.proposal
                .validate_for_scope(self.tenant_id, candidate.target_scope)
                .map_err(|_| AgentMemoryDraftError::InvalidCandidate)?;
            if draft.tenant_id != self.tenant_id
                || draft.owner_principal_id != self.owner_principal_id
                || draft.agent_id != self.agent_id
                || !matches!(
                    draft.status,
                    AgentMemoryDraftStatus::Submitted | AgentMemoryDraftStatus::Published
                )
                || candidate.proposed_by != self.owner_principal_id
                || candidate.origin != ExperienceCandidateOrigin::UserDraft
                || candidate.source_draft_id != Some(draft.draft_id)
                || candidate.target_memory_id != Some(draft.memory_id)
                || draft.proposal_id != Some(candidate.proposal_id)
                || candidate.claim_ref != draft.content_ref
                || (draft.status == AgentMemoryDraftStatus::Published)
                    != (candidate.status == CandidateStatus::Promoted)
                || !match candidate.target_scope {
                    EvolutionScope::User { principal_id } => {
                        principal_id == self.owner_principal_id
                    }
                    EvolutionScope::Agent { agent_id } => agent_id == self.agent_id,
                }
                || !drafts.insert(draft.draft_id)
                || !memories.insert(draft.memory_id)
                || !proposals.insert(candidate.proposal_id)
            {
                return Err(AgentMemoryDraftError::InvalidBatch);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
