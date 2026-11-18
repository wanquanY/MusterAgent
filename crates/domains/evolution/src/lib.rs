//! Governed experience candidates that may publish new Memory versions.

#![forbid(unsafe_code)]

mod instinct;
mod learning;

pub use instinct::{
    Instinct, InstinctContractError, InstinctLifecycle, InstinctMutationDisposition,
    InstinctMutationReceipt, PublishInstinct, SetInstinctLifecycle,
};
pub use learning::{
    BranchEvidenceState, DreamJob, DreamJobStatus, DreamWorkload, EvidenceSpan,
    LearningContractError, LearningOutcome, LearningSignal, LearningSignalKind,
    RunTerminalEvidence,
};

use std::collections::BTreeSet;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{
    AgentId, EvolutionEvaluationId, EvolutionPromotionId, EvolutionProposalId, InteractionId,
    LedgerRecordId, MemoryDraftId, MemoryId, MemoryVersionId, PrincipalId, RunId, TenantId,
    UtcTimestamp,
};
use thiserror::Error;

pub const EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE: &str =
    "application/vnd.musteragent.legacy.evolution-approval.v2+text";

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ExperienceKind {
    Semantic,
    Procedure,
    Failure,
    Preference,
    /// Legacy catch-all proposal that is deliberately not publishable. New
    /// producers must select one of the closed governed artifact kinds below.
    PolicyProposal,
    Skill,
    Instinct,
    PersonaPatch,
}

impl ExperienceKind {
    #[must_use]
    pub const fn is_memory(self) -> bool {
        matches!(
            self,
            Self::Semantic | Self::Procedure | Self::Failure | Self::Preference
        )
    }

    #[must_use]
    pub const fn is_studio_revision(self) -> bool {
        matches!(self, Self::Skill | Self::PersonaPatch)
    }
}

/// Scope a proposal may influence after evaluation and promotion.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EvolutionScope {
    User { principal_id: PrincipalId },
    Agent { agent_id: AgentId },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CandidateStatus {
    Candidate,
    Quarantined,
    Verified,
    Rejected,
    Promoted,
}

/// Provenance class for a Proposal. A user Draft is authoritative user input,
/// not fabricated Runtime evidence, so it carries the immutable Draft ID and
/// deliberately has no synthetic Run/Ledger identifiers.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ExperienceCandidateOrigin {
    RuntimeTool,
    DreamInduction,
    UserDraft,
}

/// Confidence without floating-point equality or serialization drift.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConfidenceBps(u16);

impl ConfidenceBps {
    pub fn new(value: u16) -> Result<Self, EvolutionContractError> {
        if value <= 10_000 {
            Ok(Self(value))
        } else {
            Err(EvolutionContractError::InvalidConfidence)
        }
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// Candidate evidence remains separate from durable Memory.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExperienceCandidate {
    pub proposal_id: EvolutionProposalId,
    pub tenant_id: TenantId,
    pub proposed_by: PrincipalId,
    pub origin: ExperienceCandidateOrigin,
    pub source_draft_id: Option<MemoryDraftId>,
    pub target_scope: EvolutionScope,
    pub target_memory_id: Option<MemoryId>,
    pub supersedes_proposal_id: Option<EvolutionProposalId>,
    pub kind: ExperienceKind,
    pub claim_ref: ContentRef,
    pub source_run_ids: BTreeSet<RunId>,
    pub evidence_record_ids: BTreeSet<LedgerRecordId>,
    pub status: CandidateStatus,
    pub confidence: Option<ConfidenceBps>,
    pub created_at: UtcTimestamp,
}

impl ExperienceCandidate {
    pub fn validate(&self) -> Result<(), EvolutionContractError> {
        let provenance_valid = match self.origin {
            ExperienceCandidateOrigin::RuntimeTool | ExperienceCandidateOrigin::DreamInduction => {
                self.source_draft_id.is_none()
                    && !self.source_run_ids.is_empty()
                    && !self.evidence_record_ids.is_empty()
            }
            ExperienceCandidateOrigin::UserDraft => {
                self.source_draft_id.is_some()
                    && self.target_memory_id.is_some()
                    && self.kind.is_memory()
                    && self.source_run_ids.is_empty()
                    && self.evidence_record_ids.is_empty()
            }
        };
        let target_valid = if self.kind.is_memory() {
            true
        } else {
            self.target_memory_id.is_none()
                && matches!(self.target_scope, EvolutionScope::Agent { .. })
        };
        if !provenance_valid || !target_valid {
            return Err(EvolutionContractError::InvalidProposalProvenance);
        }
        if self.supersedes_proposal_id == Some(self.proposal_id) {
            return Err(EvolutionContractError::SelfSupersession);
        }
        if self.status == CandidateStatus::Verified && self.confidence.is_none() {
            return Err(EvolutionContractError::VerifiedWithoutConfidence);
        }
        Ok(())
    }

    pub fn transition(&mut self, target: CandidateStatus) -> Result<(), EvolutionContractError> {
        let allowed = match self.status {
            CandidateStatus::Candidate => matches!(
                target,
                CandidateStatus::Quarantined
                    | CandidateStatus::Verified
                    | CandidateStatus::Rejected
            ),
            CandidateStatus::Quarantined => {
                matches!(
                    target,
                    CandidateStatus::Verified | CandidateStatus::Rejected
                )
            }
            CandidateStatus::Verified => target == CandidateStatus::Rejected,
            CandidateStatus::Rejected | CandidateStatus::Promoted => false,
        };
        if !allowed {
            return Err(EvolutionContractError::InvalidTransition);
        }
        if target == CandidateStatus::Verified && self.confidence.is_none() {
            return Err(EvolutionContractError::VerifiedWithoutConfidence);
        }
        self.status = target;
        Ok(())
    }

    /// Promotes only through a passing evaluation for the same Proposal.
    pub fn promote(
        &mut self,
        evaluation: &EvolutionEvaluation,
        approval: &EvolutionApproval,
        promotion_id: EvolutionPromotionId,
        published_memory_version_id: MemoryVersionId,
        promoted_by: PrincipalId,
        promoted_at: UtcTimestamp,
    ) -> Result<EvolutionPromotion, EvolutionContractError> {
        if self.status != CandidateStatus::Verified {
            return Err(EvolutionContractError::NotVerified);
        }
        if evaluation.proposal_id != self.proposal_id {
            return Err(EvolutionContractError::EvaluationMismatch);
        }
        if !evaluation.passed {
            return Err(EvolutionContractError::FailedEvaluation);
        }
        approval.validate_for(self, evaluation)?;
        if approval.proposal_id != self.proposal_id {
            return Err(EvolutionContractError::ApprovalMismatch);
        }
        if promoted_at < approval.approved_at {
            return Err(EvolutionContractError::PromotionPredatesApproval);
        }
        self.status = CandidateStatus::Promoted;
        Ok(EvolutionPromotion {
            promotion_id,
            proposal_id: self.proposal_id,
            evaluation_id: evaluation.evaluation_id,
            approval_interaction_id: approval.approval_interaction_id,
            published_memory_version_id,
            promoted_by,
            promoted_at,
        })
    }

    /// Crosses the single governed publication boundary for a non-Memory
    /// artifact. Persistence records the target-specific promotion receipt;
    /// this method only proves the shared Evaluation/Approval/time binding.
    pub fn promote_artifact(
        &mut self,
        evaluation: &EvolutionEvaluation,
        approval: &EvolutionApproval,
        promoted_at: UtcTimestamp,
    ) -> Result<(), EvolutionContractError> {
        if self.kind.is_memory() || self.kind == ExperienceKind::PolicyProposal {
            return Err(EvolutionContractError::InvalidArtifactKind);
        }
        if self.status != CandidateStatus::Verified {
            return Err(EvolutionContractError::NotVerified);
        }
        if evaluation.proposal_id != self.proposal_id {
            return Err(EvolutionContractError::EvaluationMismatch);
        }
        if !evaluation.passed {
            return Err(EvolutionContractError::FailedEvaluation);
        }
        approval.validate_for(self, evaluation)?;
        if promoted_at < approval.approved_at {
            return Err(EvolutionContractError::PromotionPredatesApproval);
        }
        self.status = CandidateStatus::Promoted;
        Ok(())
    }
}

/// Evaluation is immutable evidence, not an in-place Agent mutation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvolutionEvaluation {
    pub evaluation_id: EvolutionEvaluationId,
    pub proposal_id: EvolutionProposalId,
    pub evaluated_by: PrincipalId,
    pub passed: bool,
    pub report_ref: ContentRef,
    pub evaluated_at: UtcTimestamp,
}

/// Immutable authorization to publish exactly one passing Proposal evaluation.
///
/// The referenced Interaction is owned by the Approval context. Persistence
/// adapters must verify that the canonical Interaction was answered `Approve`,
/// by the same Principal and at the same timestamp, and that its immutable
/// prompt matches `prompt_ref` before accepting this fact.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvolutionApproval {
    pub approval_interaction_id: InteractionId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    /// Exact active Memory head shown to the approver for a revision. `None`
    /// means the approval authorizes creation of a new aggregate only.
    pub expected_current_memory_version_id: Option<MemoryVersionId>,
    pub prompt_ref: ContentRef,
    pub approved_by: PrincipalId,
    pub approved_at: UtcTimestamp,
}

impl EvolutionApproval {
    pub fn validate(&self, evaluation: &EvolutionEvaluation) -> Result<(), EvolutionContractError> {
        if self.proposal_id != evaluation.proposal_id
            || self.evaluation_id != evaluation.evaluation_id
        {
            return Err(EvolutionContractError::ApprovalMismatch);
        }
        if !evaluation.passed {
            return Err(EvolutionContractError::FailedEvaluation);
        }
        if self.approved_at < evaluation.evaluated_at {
            return Err(EvolutionContractError::ApprovalPredatesEvaluation);
        }
        Ok(())
    }

    /// Verifies that the approved prompt is the canonical immutable binding
    /// for this exact Proposal, Evaluation, target scope and evidence set.
    pub fn validate_for(
        &self,
        candidate: &ExperienceCandidate,
        evaluation: &EvolutionEvaluation,
    ) -> Result<(), EvolutionContractError> {
        self.validate(evaluation)?;
        if self.proposal_id != candidate.proposal_id {
            return Err(EvolutionContractError::ApprovalMismatch);
        }
        let bytes = evolution_approval_prompt_bytes(
            candidate,
            evaluation,
            self.expected_current_memory_version_id,
        )?;
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| EvolutionContractError::ApprovalPromptMismatch)?;
        if self.prompt_ref.media_type.as_str() != EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE
            || self.prompt_ref.byte_length != byte_length
            || self.prompt_ref.sha256 != Sha256::digest(bytes)
        {
            return Err(EvolutionContractError::ApprovalPromptMismatch);
        }
        Ok(())
    }
}

/// Canonical, human-readable bytes presented by the Approval Interaction.
///
/// The encoding deliberately includes every immutable fact that can change
/// what is published. Persistence adapters recompute this value from their
/// canonical Proposal/Evaluation rows, so an unrelated approved Interaction
/// cannot be rebound by supplying a different `proposal_id` in memory.
pub fn evolution_approval_prompt_bytes(
    candidate: &ExperienceCandidate,
    evaluation: &EvolutionEvaluation,
    expected_current_memory_version_id: Option<MemoryVersionId>,
) -> Result<Vec<u8>, EvolutionContractError> {
    candidate.validate()?;
    if !matches!(
        candidate.status,
        CandidateStatus::Verified | CandidateStatus::Promoted
    ) {
        return Err(EvolutionContractError::NotVerified);
    }
    if evaluation.proposal_id != candidate.proposal_id {
        return Err(EvolutionContractError::EvaluationMismatch);
    }
    if !evaluation.passed {
        return Err(EvolutionContractError::FailedEvaluation);
    }
    let confidence = candidate
        .confidence
        .ok_or(EvolutionContractError::VerifiedWithoutConfidence)?;
    let (scope_kind, scope_id) = match candidate.target_scope {
        EvolutionScope::User { principal_id } => ("user", principal_id.to_string()),
        EvolutionScope::Agent { agent_id } => ("agent", agent_id.to_string()),
    };
    let kind = match candidate.kind {
        ExperienceKind::Semantic => "semantic",
        ExperienceKind::Procedure => "procedure",
        ExperienceKind::Failure => "failure",
        ExperienceKind::Preference => "preference",
        ExperienceKind::PolicyProposal => "policy_proposal",
        ExperienceKind::Skill => "skill",
        ExperienceKind::Instinct => "instinct",
        ExperienceKind::PersonaPatch => "persona_patch",
    };
    let target_memory_id = candidate
        .target_memory_id
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    let supersedes_proposal_id = candidate
        .supersedes_proposal_id
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    let expected_current_memory_version_id = expected_current_memory_version_id
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    let source_run_ids = candidate
        .source_run_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let evidence_record_ids = candidate
        .evidence_record_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut prompt = format!(
        "musteragent.legacy.evolution.approval.v2\n\
tenant_id={}\n\
proposal_id={}\n\
evaluation_id={}\n\
proposed_by={}\n\
evaluated_by={}\n\
scope_kind={scope_kind}\n\
scope_id={scope_id}\n\
kind={kind}\n\
target_memory_id={target_memory_id}\n\
expected_current_memory_version_id={expected_current_memory_version_id}\n\
supersedes_proposal_id={supersedes_proposal_id}\n\
confidence_bps={}\n\
claim_content_id={}\n\
claim_media_type={}\n\
claim_byte_length={}\n\
claim_sha256={}\n\
report_content_id={}\n\
report_media_type={}\n\
report_byte_length={}\n\
report_sha256={}\n\
source_run_ids={source_run_ids}\n\
evidence_record_ids={evidence_record_ids}\n\
candidate_created_at_ms={}\n\
evaluated_at_ms={}\n",
        candidate.tenant_id,
        candidate.proposal_id,
        evaluation.evaluation_id,
        candidate.proposed_by,
        evaluation.evaluated_by,
        confidence.get(),
        candidate.claim_ref.content_id,
        candidate.claim_ref.media_type.as_str(),
        candidate.claim_ref.byte_length,
        candidate.claim_ref.sha256,
        evaluation.report_ref.content_id,
        evaluation.report_ref.media_type.as_str(),
        evaluation.report_ref.byte_length,
        evaluation.report_ref.sha256,
        candidate.created_at.unix_millis(),
        evaluation.evaluated_at.unix_millis(),
    );
    if candidate.origin == ExperienceCandidateOrigin::UserDraft {
        prompt.push_str("origin=user_draft\n");
        prompt.push_str(&format!(
            "source_draft_id={}\n",
            candidate
                .source_draft_id
                .expect("validated user Draft Candidate has a Draft identity")
        ));
    }
    Ok(prompt.into_bytes())
}

/// Promotion records the target context version produced by an approved candidate.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct EvolutionPromotion {
    pub promotion_id: EvolutionPromotionId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub approval_interaction_id: InteractionId,
    pub published_memory_version_id: MemoryVersionId,
    pub promoted_by: PrincipalId,
    pub promoted_at: UtcTimestamp,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum EvolutionContractError {
    #[error("evolution candidate provenance does not match its declared origin")]
    InvalidProposalProvenance,
    #[error("confidence must be between 0 and 10000 basis points")]
    InvalidConfidence,
    #[error("verified candidate requires confidence")]
    VerifiedWithoutConfidence,
    #[error("invalid Evolution candidate transition")]
    InvalidTransition,
    #[error("only a verified candidate can be promoted")]
    NotVerified,
    #[error("evaluation does not belong to the candidate Proposal")]
    EvaluationMismatch,
    #[error("failed evaluation cannot authorize promotion")]
    FailedEvaluation,
    #[error("approval does not bind the same Proposal and Evaluation")]
    ApprovalMismatch,
    #[error("approval predates its Evaluation")]
    ApprovalPredatesEvaluation,
    #[error("approval prompt does not canonically bind the Proposal and Evaluation")]
    ApprovalPromptMismatch,
    #[error("promotion predates its Approval")]
    PromotionPredatesApproval,
    #[error("Evolution proposal cannot supersede itself")]
    SelfSupersession,
    #[error("Evolution proposal kind cannot publish through an artifact path")]
    InvalidArtifactKind,
}
