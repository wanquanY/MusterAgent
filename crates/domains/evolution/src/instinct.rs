use std::collections::BTreeSet;

use musteragent_content_ref::ContentRef;
use musteragent_identity::{
    AgentId, EvolutionEvaluationId, EvolutionPromotionId, EvolutionProposalId, IdempotencyKey,
    InstinctId, InteractionId, LedgerRecordId, PrincipalId, RunId, TenantId, UtcTimestamp,
    VersionNumber,
};
use thiserror::Error;

use crate::{
    CandidateStatus, EvolutionApproval, EvolutionEvaluation, EvolutionScope, ExperienceCandidate,
    ExperienceKind,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InstinctLifecycle {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Instinct {
    pub instinct_id: InstinctId,
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub approval_interaction_id: InteractionId,
    pub promotion_id: EvolutionPromotionId,
    pub payload_ref: ContentRef,
    pub evidence_run_ids: BTreeSet<RunId>,
    pub evidence_record_ids: BTreeSet<LedgerRecordId>,
    pub lifecycle: InstinctLifecycle,
    pub revision: VersionNumber,
    pub created_by: PrincipalId,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
}

impl Instinct {
    pub fn validate(&self) -> Result<(), InstinctContractError> {
        if self.evidence_run_ids.is_empty()
            || self.evidence_record_ids.is_empty()
            || self.updated_at < self.created_at
        {
            return Err(InstinctContractError::InvalidInstinct);
        }
        Ok(())
    }
}

/// Atomic publication of one evaluated and approved Instinct. Instinct is a
/// first-class runtime asset; it never masquerades as a MemoryVersion.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishInstinct {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub approval: EvolutionApproval,
    pub instinct: Instinct,
}

impl PublishInstinct {
    pub fn validate(&self) -> Result<(), InstinctContractError> {
        self.candidate.validate()?;
        self.approval
            .validate_for(&self.candidate, &self.evaluation)?;
        self.instinct.validate()?;
        let target_agent_id = match self.candidate.target_scope {
            EvolutionScope::Agent { agent_id } => agent_id,
            EvolutionScope::User { .. } => return Err(InstinctContractError::InvalidPublication),
        };
        if self.candidate.kind != ExperienceKind::Instinct
            || self.candidate.status != CandidateStatus::Promoted
            || self.candidate.target_memory_id.is_some()
            || !self.evaluation.passed
            || self.evaluation.proposal_id != self.candidate.proposal_id
            || self.approval.expected_current_memory_version_id.is_some()
            || self.instinct.tenant_id != self.candidate.tenant_id
            || self.instinct.target_agent_id != target_agent_id
            || self.instinct.proposal_id != self.candidate.proposal_id
            || self.instinct.evaluation_id != self.evaluation.evaluation_id
            || self.instinct.approval_interaction_id != self.approval.approval_interaction_id
            || self.instinct.payload_ref != self.candidate.claim_ref
            || self.instinct.evidence_run_ids != self.candidate.source_run_ids
            || self.instinct.evidence_record_ids != self.candidate.evidence_record_ids
            || self.instinct.lifecycle != InstinctLifecycle::Enabled
            || self.instinct.revision.get() != 1
            || self.instinct.created_by != self.approval.approved_by
            || self.instinct.created_at != self.instinct.updated_at
            || self.instinct.created_at < self.approval.approved_at
        {
            return Err(InstinctContractError::InvalidPublication);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetInstinctLifecycle {
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub instinct_id: InstinctId,
    pub expected_revision: VersionNumber,
    pub lifecycle: InstinctLifecycle,
    pub actor_principal_id: PrincipalId,
    pub reason_code: musteragent_identity::DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub changed_at: UtcTimestamp,
}

impl SetInstinctLifecycle {
    pub fn validate(&self) -> Result<(), InstinctContractError> {
        if self.expected_revision.get() == u64::MAX {
            return Err(InstinctContractError::RevisionOverflow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InstinctMutationReceipt {
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub instinct_id: InstinctId,
    pub previous_revision: VersionNumber,
    pub revision: VersionNumber,
    pub lifecycle: InstinctLifecycle,
    pub actor_principal_id: PrincipalId,
    pub reason_code: musteragent_identity::DottedKey,
    pub idempotency_key: IdempotencyKey,
    pub changed_at: UtcTimestamp,
}

impl InstinctMutationReceipt {
    pub fn validate(&self) -> Result<(), InstinctContractError> {
        if self
            .previous_revision
            .get()
            .checked_add(1)
            .is_none_or(|next| next != self.revision.get())
        {
            return Err(InstinctContractError::InvalidReceipt);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum InstinctMutationDisposition {
    Applied,
    Duplicate,
}

#[derive(Debug, Error)]
pub enum InstinctContractError {
    #[error(transparent)]
    Evolution(#[from] crate::EvolutionContractError),
    #[error("Instinct violates its immutable contract")]
    InvalidInstinct,
    #[error("Instinct publication does not bind one approved proposal")]
    InvalidPublication,
    #[error("Instinct revision counter is exhausted")]
    RevisionOverflow,
    #[error("Instinct mutation receipt is invalid")]
    InvalidReceipt,
}
