use musteragent_evolution::{
    EvolutionApproval, EvolutionEvaluation, EvolutionPromotion, EvolutionScope,
    ExperienceCandidate, ExperienceCandidateOrigin, ExperienceKind,
};
use musteragent_identity::{
    EvolutionPromotionId, IdempotencyKey, MemoryId, MemoryVersionId, PrincipalId, UtcTimestamp,
    VersionNumber,
};
use musteragent_memory::{
    MemoryConfidenceBps, MemoryKind, MemoryLifecycle, MemoryRecord, MemoryScope, MemorySource,
    MemoryVersion,
};
use musteragent_policy::DataClassification;
use musteragent_runtime_content::{ContentStore, verify_content};
use thiserror::Error;

use crate::{MemorySearchDocument, MemorySearchHit, PromoteExperience};

/// Frozen input for constructing the exact publication command used by both
/// Tool-origin and management-origin governance. Persistence still performs
/// the authoritative Approval/CAS checks in one transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PrepareEvolutionPublication {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub approval: EvolutionApproval,
    pub current_head: Option<MemorySearchHit>,
    pub new_memory_id: MemoryId,
    pub memory_version_id: MemoryVersionId,
    pub promotion_id: EvolutionPromotionId,
    pub promoted_by: PrincipalId,
    pub promoted_at: UtcTimestamp,
}

pub async fn prepare_evolution_publication(
    content: &dyn ContentStore,
    input: PrepareEvolutionPublication,
) -> Result<PromoteExperience, EvolutionPublicationPreparationError> {
    if !input.candidate.kind.is_memory() {
        return Err(EvolutionPublicationPreparationError::InvalidCandidate);
    }
    let expected_current_version_id = input
        .current_head
        .as_ref()
        .map(|head| head.version.memory_version_id);
    if input.approval.expected_current_memory_version_id != expected_current_version_id {
        return Err(EvolutionPublicationPreparationError::StaleApproval);
    }
    let mut candidate = input.candidate;
    let promotion = candidate
        .promote(
            &input.evaluation,
            &input.approval,
            input.promotion_id,
            input.memory_version_id,
            input.promoted_by,
            input.promoted_at,
        )
        .map_err(|_| EvolutionPublicationPreparationError::InvalidCandidate)?;
    let (memory_record, version, data_classification) = match input.current_head {
        Some(head) => {
            if head.record.updated_at > input.promoted_at
                || candidate.target_memory_id != Some(head.record.memory_id)
            {
                return Err(EvolutionPublicationPreparationError::StaleHead);
            }
            let next_version = head
                .version
                .version
                .get()
                .checked_add(1)
                .ok_or(EvolutionPublicationPreparationError::VersionOverflow)
                .and_then(|value| {
                    VersionNumber::new(value)
                        .map_err(|_| EvolutionPublicationPreparationError::VersionOverflow)
                })?;
            let mut record = head.record;
            record.current_version_id = input.memory_version_id;
            record.updated_at = input.promoted_at;
            record.revoked_at = None;
            (record, next_version, head.version.data_classification)
        }
        None => {
            if candidate.target_memory_id.is_some() {
                return Err(EvolutionPublicationPreparationError::StaleHead);
            }
            (
                MemoryRecord {
                    memory_id: input.new_memory_id,
                    tenant_id: candidate.tenant_id,
                    scope: evolution_memory_scope(candidate.target_scope),
                    memory_key: IdempotencyKey::new(format!("learned:{}", candidate.proposal_id))
                        .map_err(|_| {
                        EvolutionPublicationPreparationError::InvalidCandidate
                    })?,
                    kind: evolution_memory_kind(candidate.kind)?,
                    lifecycle: MemoryLifecycle::Active,
                    current_version_id: input.memory_version_id,
                    created_at: input.promoted_at,
                    updated_at: input.promoted_at,
                    revoked_at: None,
                },
                VersionNumber::new(1)
                    .map_err(|_| EvolutionPublicationPreparationError::VersionOverflow)?,
                match candidate.target_scope {
                    EvolutionScope::User { .. } => DataClassification::Confidential,
                    EvolutionScope::Agent { .. } => DataClassification::Internal,
                },
            )
        }
    };
    let memory_version = MemoryVersion {
        memory_version_id: input.memory_version_id,
        memory_id: memory_record.memory_id,
        version,
        source: match candidate.origin {
            ExperienceCandidateOrigin::UserDraft => MemorySource::UserAuthored,
            ExperienceCandidateOrigin::RuntimeTool | ExperienceCandidateOrigin::DreamInduction => {
                MemorySource::VerifiedExperience
            }
        },
        content_ref: candidate.claim_ref.clone(),
        confidence: MemoryConfidenceBps::new(
            candidate
                .confidence
                .ok_or(EvolutionPublicationPreparationError::InvalidCandidate)?
                .get(),
        )
        .map_err(|_| EvolutionPublicationPreparationError::InvalidCandidate)?,
        data_classification,
        source_run_ids: candidate.source_run_ids.clone(),
        evidence_record_ids: candidate.evidence_record_ids.clone(),
        created_at: input.promoted_at,
        valid_until: None,
    };
    let claim_bytes = content
        .read(candidate.tenant_id, &candidate.claim_ref)
        .await
        .map_err(|_| EvolutionPublicationPreparationError::ContentStore)?;
    verify_content(&candidate.claim_ref, &claim_bytes)
        .map_err(|_| EvolutionPublicationPreparationError::ContentIntegrity)?;
    let search_document =
        MemorySearchDocument::from_content(&memory_record, &memory_version, &claim_bytes)
            .map_err(|_| EvolutionPublicationPreparationError::InvalidCandidate)?;
    let command = PromoteExperience {
        tool_approval: None,
        candidate,
        evaluation: input.evaluation,
        approval: input.approval,
        promotion: EvolutionPromotion {
            promotion_id: promotion.promotion_id,
            proposal_id: promotion.proposal_id,
            published_memory_version_id: promotion.published_memory_version_id,
            evaluation_id: promotion.evaluation_id,
            approval_interaction_id: promotion.approval_interaction_id,
            promoted_by: promotion.promoted_by,
            promoted_at: promotion.promoted_at,
        },
        expected_current_version_id,
        memory_record,
        memory_version,
        search_document,
    };
    command
        .validate()
        .map_err(|_| EvolutionPublicationPreparationError::InvalidCandidate)?;
    Ok(command)
}

const fn evolution_memory_scope(scope: EvolutionScope) -> MemoryScope {
    match scope {
        EvolutionScope::User { principal_id } => MemoryScope::User { principal_id },
        EvolutionScope::Agent { agent_id } => MemoryScope::Agent { agent_id },
    }
}

fn evolution_memory_kind(
    kind: ExperienceKind,
) -> Result<MemoryKind, EvolutionPublicationPreparationError> {
    match kind {
        ExperienceKind::Semantic => Ok(MemoryKind::Semantic),
        ExperienceKind::Procedure => Ok(MemoryKind::Procedure),
        ExperienceKind::Failure => Ok(MemoryKind::FailureLesson),
        ExperienceKind::Preference => Ok(MemoryKind::Preference),
        ExperienceKind::PolicyProposal
        | ExperienceKind::Skill
        | ExperienceKind::Instinct
        | ExperienceKind::PersonaPatch => {
            Err(EvolutionPublicationPreparationError::InvalidCandidate)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum EvolutionPublicationPreparationError {
    #[error("Evolution candidate cannot be published as Memory")]
    InvalidCandidate,
    #[error("Approval is not bound to the current Memory head")]
    StaleApproval,
    #[error("Memory head changed before publication preparation")]
    StaleHead,
    #[error("Memory version counter overflowed")]
    VersionOverflow,
    #[error("publication content is unavailable")]
    ContentStore,
    #[error("publication content failed integrity verification")]
    ContentIntegrity,
}
