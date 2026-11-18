use super::*;
use crate::{EvolutionProposalInspection, ExperienceClaimInspection};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{ConfidenceBps, EvolutionEvaluation, ExperienceKind};
use musteragent_identity::{ContentId, EvolutionEvaluationId, InstinctId, InteractionId};

fn review() -> AgentMemoryDraftReviewBatch {
    let tenant_id = TenantId::generate();
    let owner_principal_id = PrincipalId::generate();
    let agent_id = AgentId::generate();
    let proposal_id = EvolutionProposalId::generate();
    let at = UtcTimestamp::from_unix_millis(20);
    let draft = AgentMemoryDraft {
        draft_id: MemoryDraftId::generate(),
        tenant_id,
        owner_principal_id,
        agent_id,
        memory_id: MemoryId::generate(),
        baseline_memory_version_id: MemoryVersionId::generate(),
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(crate::EXPERIENCE_CLAIM_MEDIA_TYPE).expect("claim MIME"),
            byte_length: 1,
            sha256: Sha256::digest(b"x"),
        },
        status: AgentMemoryDraftStatus::Submitted,
        proposal_id: Some(proposal_id),
        conflicting_memory_version_id: None,
        supersedes_draft_id: None,
        idempotency_key: IdempotencyKey::new("draft:review").expect("key"),
        created_at: at,
        updated_at: at,
    };
    let candidate = ExperienceCandidate {
        proposal_id,
        tenant_id,
        proposed_by: owner_principal_id,
        origin: ExperienceCandidateOrigin::UserDraft,
        source_draft_id: Some(draft.draft_id),
        target_scope: EvolutionScope::Agent { agent_id },
        target_memory_id: Some(draft.memory_id),
        supersedes_proposal_id: None,
        kind: ExperienceKind::Semantic,
        claim_ref: draft.content_ref.clone(),
        source_run_ids: Default::default(),
        evidence_record_ids: Default::default(),
        status: CandidateStatus::Candidate,
        confidence: Some(ConfidenceBps::new(10_000).expect("confidence")),
        created_at: at,
    };
    AgentMemoryDraftReviewBatch {
        batch_id: MemoryDraftBatchId::generate(),
        tenant_id,
        owner_principal_id,
        agent_id,
        submitted_at: at,
        items: vec![AgentMemoryDraftReviewItem {
            draft,
            proposal: EvolutionProposalInspection {
                candidate,
                claim_detail: ExperienceClaimInspection::content_unavailable(),
                evaluation: None,
                approval_interaction_id: None,
                published_memory_version_id: None,
                published_instinct_id: None,
                studio_revision_draft_id: None,
                studio_artifact_kind: None,
            },
        }],
    }
}

fn published() -> AgentMemoryDraftReviewBatch {
    let mut batch = review();
    let item = &mut batch.items[0];
    item.draft.status = AgentMemoryDraftStatus::Published;
    item.proposal.candidate.status = CandidateStatus::Promoted;
    item.proposal.evaluation = Some(EvolutionEvaluation {
        evaluation_id: EvolutionEvaluationId::generate(),
        proposal_id: item.proposal.candidate.proposal_id,
        evaluated_by: batch.owner_principal_id,
        passed: true,
        report_ref: item.draft.content_ref.clone(),
        evaluated_at: batch.submitted_at,
    });
    item.proposal.approval_interaction_id = Some(InteractionId::generate());
    item.proposal.published_memory_version_id = Some(MemoryVersionId::generate());
    batch
}

#[test]
fn pending_and_user_scoped_reviews_preserve_the_original_governance_boundary() {
    let mut batch = review();
    assert_eq!(batch.validate(), Ok(()));
    batch.items[0].proposal.candidate.target_scope = EvolutionScope::User {
        principal_id: batch.owner_principal_id,
    };
    assert_eq!(batch.validate(), Ok(()));
    batch.items[0].proposal.candidate.target_scope = EvolutionScope::User {
        principal_id: PrincipalId::generate(),
    };
    assert!(batch.validate().is_err());
}

#[test]
fn review_rejects_cross_owner_agent_membership_and_original_content_changes() {
    let original = review();
    for mutate in [
        |b: &mut AgentMemoryDraftReviewBatch| b.tenant_id = TenantId::generate(),
        |b: &mut AgentMemoryDraftReviewBatch| b.owner_principal_id = PrincipalId::generate(),
        |b: &mut AgentMemoryDraftReviewBatch| b.agent_id = AgentId::generate(),
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].draft.proposal_id = Some(EvolutionProposalId::generate())
        },
        |b: &mut AgentMemoryDraftReviewBatch| b.items[0].draft.memory_id = MemoryId::generate(),
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.candidate.proposed_by = PrincipalId::generate()
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.candidate.source_draft_id = Some(MemoryDraftId::generate())
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.candidate.origin = ExperienceCandidateOrigin::RuntimeTool
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.candidate.claim_ref.sha256 = Sha256::digest(b"y")
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.candidate.target_scope = EvolutionScope::Agent {
                agent_id: AgentId::generate(),
            }
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].draft.status = AgentMemoryDraftStatus::Published
        },
    ] {
        let mut changed = original.clone();
        mutate(&mut changed);
        assert!(changed.validate().is_err());
    }
}

#[test]
fn a_published_review_requires_the_actual_passing_approval_and_matching_target() {
    let original = published();
    assert_eq!(original.validate(), Ok(()));
    for mutate in [
        |b: &mut AgentMemoryDraftReviewBatch| b.items[0].proposal.approval_interaction_id = None,
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.published_memory_version_id = None
        },
        |b: &mut AgentMemoryDraftReviewBatch| b.items[0].proposal.evaluation = None,
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0]
                .proposal
                .evaluation
                .as_mut()
                .expect("fixture evaluation")
                .passed = false
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0]
                .proposal
                .evaluation
                .as_mut()
                .expect("fixture evaluation")
                .proposal_id = EvolutionProposalId::generate()
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].proposal.published_instinct_id = Some(InstinctId::generate())
        },
        |b: &mut AgentMemoryDraftReviewBatch| {
            b.items[0].draft.status = AgentMemoryDraftStatus::Submitted
        },
    ] {
        let mut changed = original.clone();
        mutate(&mut changed);
        assert!(changed.validate().is_err());
    }
}

#[test]
fn batch_bounds_and_duplicate_membership_are_not_weakened_by_projection() {
    let mut batch = review();
    let item = batch.items[0].clone();
    batch.items.clear();
    assert!(batch.validate().is_err());
    batch.items = vec![item.clone(); 101];
    assert!(batch.validate().is_err());
    batch.items = vec![item; 2];
    assert!(batch.validate().is_err());
}
