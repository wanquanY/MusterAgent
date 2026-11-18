//! Exact Tool inspection reuses original Evolution facts; it grants no authority.
use crate::{AgentEvolutionInspectionError, EvolutionProposalInspection};
use musteragent_evolution::{CandidateStatus, EvolutionScope};
use musteragent_identity::{
    AgentId, EvolutionProposalId, MemoryId, MemoryVersionId, PrincipalId, TenantId, VersionNumber,
};

/// Construct only from the executor's authenticated actor and frozen Agent.
/// No principal, Agent or target scope is accepted from Tool arguments.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceProposalInspectionQuery {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
}
impl ExperienceProposalInspectionQuery {
    pub fn permits_scope(&self, scope: EvolutionScope) -> bool {
        match scope {
            EvolutionScope::User { principal_id } => principal_id == self.principal_id,
            EvolutionScope::Agent { agent_id } => agent_id == self.agent_id,
        }
    }
}

/// Historic receipt only. Current applicability is read independently through
/// MemoryStore::load_published_head and immutable content validation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperiencePublicationReceipt {
    pub memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub revision: VersionNumber,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceProposalInspection {
    pub proposal: EvolutionProposalInspection,
    /// Missing/erased publication evidence must not become a success receipt.
    pub publication: Option<ExperiencePublicationReceipt>,
}
impl ExperienceProposalInspection {
    pub fn validate_for(
        &self,
        query: &ExperienceProposalInspectionQuery,
    ) -> Result<(), AgentEvolutionInspectionError> {
        let candidate = &self.proposal.candidate;
        if candidate.proposal_id != query.proposal_id
            || !query.permits_scope(candidate.target_scope)
        {
            return Err(AgentEvolutionInspectionError::ScopeMismatch);
        }
        self.proposal
            .validate_for_scope(query.tenant_id, candidate.target_scope)?;
        let evaluated = self.proposal.evaluation.as_ref();
        if (matches!(
            candidate.status,
            CandidateStatus::Candidate | CandidateStatus::Quarantined
        ) && evaluated.is_some())
            || (matches!(
                candidate.status,
                CandidateStatus::Verified | CandidateStatus::Promoted
            ) && evaluated.is_none_or(|value| !value.passed))
            || evaluated.is_some_and(|value| value.evaluated_at < candidate.created_at)
            || self.publication.as_ref().is_some_and(|receipt| {
                candidate.status != CandidateStatus::Promoted
                    || !candidate.kind.is_memory()
                    || self.proposal.published_memory_version_id != Some(receipt.memory_version_id)
            })
        {
            return Err(AgentEvolutionInspectionError::CorruptProjection);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scope_is_exact_current_agent_or_current_account() {
        let query = ExperienceProposalInspectionQuery {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            proposal_id: EvolutionProposalId::generate(),
        };
        assert!(query.permits_scope(EvolutionScope::Agent {
            agent_id: query.agent_id
        }));
        assert!(query.permits_scope(EvolutionScope::User {
            principal_id: query.principal_id
        }));
        assert!(!query.permits_scope(EvolutionScope::Agent {
            agent_id: AgentId::generate()
        }));
        assert!(!query.permits_scope(EvolutionScope::User {
            principal_id: PrincipalId::generate()
        }));
    }
}
