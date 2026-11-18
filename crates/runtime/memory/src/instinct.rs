use std::num::NonZeroU16;

use musteragent_evolution::{
    CandidateStatus, EvolutionApproval, EvolutionEvaluation, EvolutionScope, ExperienceCandidate,
    ExperienceKind, Instinct, InstinctContractError, InstinctMutationDisposition,
    InstinctMutationReceipt, PublishInstinct, SetInstinctLifecycle,
};
use musteragent_identity::{
    AgentId, EvolutionPromotionId, InstinctId, PrincipalId, TenantId, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::ExperienceClaimDocument;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PrepareInstinctPublication {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub approval: EvolutionApproval,
    pub instinct_id: InstinctId,
    pub promotion_id: EvolutionPromotionId,
    pub promoted_by: PrincipalId,
    pub promoted_at: UtcTimestamp,
}

pub async fn prepare_instinct_publication(
    content: &dyn ContentStore,
    input: PrepareInstinctPublication,
) -> Result<PublishInstinct, InstinctApplicationError> {
    if input.candidate.kind != ExperienceKind::Instinct
        || input.candidate.status != CandidateStatus::Verified
        || input.candidate.target_memory_id.is_some()
        || input.approval.expected_current_memory_version_id.is_some()
        || input.promoted_by != input.approval.approved_by
    {
        return Err(InstinctApplicationError::InvalidPublication);
    }
    let target_agent_id = match input.candidate.target_scope {
        EvolutionScope::Agent { agent_id } => agent_id,
        EvolutionScope::User { .. } => return Err(InstinctApplicationError::InvalidPublication),
    };
    let bytes = content
        .read(input.candidate.tenant_id, &input.candidate.claim_ref)
        .await
        .map_err(|_| InstinctApplicationError::ContentStore)?;
    verify_content(&input.candidate.claim_ref, &bytes)
        .map_err(|_| InstinctApplicationError::ContentIntegrity)?;
    let document = ExperienceClaimDocument::decode(&bytes)
        .map_err(|_| InstinctApplicationError::InvalidPayload)?;
    document
        .validate_for_kind(ExperienceKind::Instinct)
        .map_err(|_| InstinctApplicationError::InvalidPayload)?;

    let mut candidate = input.candidate;
    candidate
        .promote_artifact(&input.evaluation, &input.approval, input.promoted_at)
        .map_err(|_| InstinctApplicationError::InvalidPublication)?;
    let command = PublishInstinct {
        instinct: Instinct {
            instinct_id: input.instinct_id,
            tenant_id: candidate.tenant_id,
            target_agent_id,
            proposal_id: candidate.proposal_id,
            evaluation_id: input.evaluation.evaluation_id,
            approval_interaction_id: input.approval.approval_interaction_id,
            promotion_id: input.promotion_id,
            payload_ref: candidate.claim_ref.clone(),
            evidence_run_ids: candidate.source_run_ids.clone(),
            evidence_record_ids: candidate.evidence_record_ids.clone(),
            lifecycle: musteragent_evolution::InstinctLifecycle::Enabled,
            revision: VersionNumber::new(1)
                .map_err(|_| InstinctApplicationError::InvalidPublication)?,
            created_by: input.promoted_by,
            created_at: input.promoted_at,
            updated_at: input.promoted_at,
        },
        candidate,
        evaluation: input.evaluation,
        approval: input.approval,
    };
    command.validate()?;
    Ok(command)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListAgentInstincts {
    pub tenant_id: TenantId,
    pub target_agent_id: AgentId,
    pub include_disabled: bool,
    pub limit: NonZeroU16,
    pub observed_at: UtcTimestamp,
    /// Restrict authority checks to the original selection before pagination.
    /// New learning must not displace an existing selection during retry.
    pub instinct_ids: Option<std::collections::BTreeSet<InstinctId>>,
}

impl ListAgentInstincts {
    pub fn validate(&self) -> Result<(), InstinctApplicationError> {
        if self.limit.get() > 500
            || self
                .instinct_ids
                .as_ref()
                .is_some_and(|ids| ids.len() > 500)
        {
            return Err(InstinctApplicationError::LimitTooLarge);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InstinctMutation {
    pub disposition: InstinctMutationDisposition,
    pub receipt: InstinctMutationReceipt,
}

pub trait InstinctStore: Send + Sync {
    type Error: Send;

    fn publish_instinct(
        &self,
        command: PublishInstinct,
    ) -> ApplicationFuture<'_, Result<crate::MemoryPersistenceDisposition, Self::Error>>;

    fn set_instinct_lifecycle(
        &self,
        command: SetInstinctLifecycle,
    ) -> ApplicationFuture<'_, Result<InstinctMutation, Self::Error>>;

    fn list_agent_instincts(
        &self,
        query: ListAgentInstincts,
    ) -> ApplicationFuture<'_, Result<Vec<Instinct>, Self::Error>>;
}

#[derive(Debug, Error)]
pub enum InstinctApplicationError {
    #[error(transparent)]
    Contract(#[from] InstinctContractError),
    #[error("Instinct publication is invalid")]
    InvalidPublication,
    #[error("Instinct payload must contain a bounded trigger")]
    InvalidPayload,
    #[error("Instinct query limit exceeds 500")]
    LimitTooLarge,
    #[error("Instinct Content is unavailable")]
    ContentStore,
    #[error("Instinct ContentRef does not match immutable bytes")]
    ContentIntegrity,
}
