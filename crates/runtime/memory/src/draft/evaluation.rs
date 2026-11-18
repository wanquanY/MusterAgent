use super::{AgentMemoryDraft, AgentMemoryDraftError, AgentMemoryDraftStatus};
use musteragent_evolution::{
    CandidateStatus, EvolutionScope, ExperienceCandidate, ExperienceCandidateOrigin,
};
use musteragent_identity::{AgentId, PrincipalId, TenantId};

/// Derived from an authenticated account/Agent owner, never a queue cursor or
/// a renderer-provided identity. It does not grant global Evaluation access.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AgentMemoryDraftEvaluationScope {
    pub tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub agent_id: AgentId,
}

/// One original Submitted Draft and its immutable Candidate, read together.
/// This is not a lease or a second queue; original Evaluation persistence owns
/// concurrency/idempotency and only explicit confirmation can publish.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PendingAgentMemoryDraftEvaluation {
    pub draft: AgentMemoryDraft,
    pub candidate: ExperienceCandidate,
}

impl PendingAgentMemoryDraftEvaluation {
    pub fn validate_for(
        &self,
        scope: AgentMemoryDraftEvaluationScope,
    ) -> Result<(), AgentMemoryDraftError> {
        let draft = &self.draft;
        let candidate = &self.candidate;
        draft.validate()?;
        candidate
            .validate()
            .map_err(|_| AgentMemoryDraftError::InvalidCandidate)?;
        if draft.tenant_id != scope.tenant_id
            || draft.owner_principal_id != scope.owner_principal_id
            || draft.agent_id != scope.agent_id
            || draft.status != AgentMemoryDraftStatus::Submitted
            || draft.proposal_id != Some(candidate.proposal_id)
            || candidate.tenant_id != scope.tenant_id
            || candidate.proposed_by != scope.owner_principal_id
            || candidate.origin != ExperienceCandidateOrigin::UserDraft
            || candidate.source_draft_id != Some(draft.draft_id)
            || candidate.target_memory_id != Some(draft.memory_id)
            || candidate.claim_ref != draft.content_ref
            || !matches!(
                candidate.status,
                CandidateStatus::Candidate | CandidateStatus::Quarantined
            )
            || !match candidate.target_scope {
                EvolutionScope::User { principal_id } => principal_id == scope.owner_principal_id,
                EvolutionScope::Agent { agent_id } => agent_id == scope.agent_id,
            }
        {
            return Err(AgentMemoryDraftError::InvalidCandidate);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
