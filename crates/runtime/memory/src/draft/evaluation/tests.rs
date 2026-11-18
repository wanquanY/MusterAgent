use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{ConfidenceBps, ExperienceKind};
use musteragent_identity::{
    ContentId, EvolutionProposalId, IdempotencyKey, MemoryDraftId, MemoryId, MemoryVersionId,
    UtcTimestamp,
};

fn pending() -> (
    AgentMemoryDraftEvaluationScope,
    PendingAgentMemoryDraftEvaluation,
) {
    let scope = AgentMemoryDraftEvaluationScope {
        tenant_id: TenantId::generate(),
        owner_principal_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
    };
    let proposal_id = EvolutionProposalId::generate();
    let draft = AgentMemoryDraft {
        draft_id: MemoryDraftId::generate(),
        tenant_id: scope.tenant_id,
        owner_principal_id: scope.owner_principal_id,
        agent_id: scope.agent_id,
        memory_id: MemoryId::generate(),
        baseline_memory_version_id: MemoryVersionId::generate(),
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new(crate::EXPERIENCE_CLAIM_MEDIA_TYPE).expect("media"),
            byte_length: 1,
            sha256: Sha256::digest(b"x"),
        },
        status: AgentMemoryDraftStatus::Submitted,
        proposal_id: Some(proposal_id),
        conflicting_memory_version_id: None,
        supersedes_draft_id: None,
        idempotency_key: IdempotencyKey::new("draft:evaluation").expect("key"),
        created_at: UtcTimestamp::from_unix_millis(10),
        updated_at: UtcTimestamp::from_unix_millis(20),
    };
    let candidate = ExperienceCandidate {
        proposal_id,
        tenant_id: scope.tenant_id,
        proposed_by: scope.owner_principal_id,
        origin: ExperienceCandidateOrigin::UserDraft,
        source_draft_id: Some(draft.draft_id),
        target_scope: EvolutionScope::Agent {
            agent_id: scope.agent_id,
        },
        target_memory_id: Some(draft.memory_id),
        supersedes_proposal_id: None,
        kind: ExperienceKind::Semantic,
        claim_ref: draft.content_ref.clone(),
        source_run_ids: Default::default(),
        evidence_record_ids: Default::default(),
        status: CandidateStatus::Candidate,
        confidence: Some(ConfidenceBps::new(10_000).expect("confidence")),
        created_at: draft.updated_at,
    };
    (
        scope,
        PendingAgentMemoryDraftEvaluation { draft, candidate },
    )
}

#[test]
fn evaluation_accepts_only_the_exact_submitted_owner_and_candidate() {
    let (scope, pending) = pending();
    assert_eq!(pending.validate_for(scope), Ok(()));
    for mutate in [
        |p: &mut PendingAgentMemoryDraftEvaluation| p.draft.tenant_id = TenantId::generate(),
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.draft.owner_principal_id = PrincipalId::generate()
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| p.draft.agent_id = AgentId::generate(),
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.draft.status = AgentMemoryDraftStatus::Published
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.draft.proposal_id = Some(EvolutionProposalId::generate())
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| p.candidate.tenant_id = TenantId::generate(),
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.proposed_by = PrincipalId::generate()
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.source_draft_id = Some(MemoryDraftId::generate())
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.target_memory_id = Some(MemoryId::generate())
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.claim_ref.sha256 = Sha256::digest(b"y")
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| p.candidate.status = CandidateStatus::Verified,
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.target_scope = EvolutionScope::Agent {
                agent_id: AgentId::generate(),
            }
        },
        |p: &mut PendingAgentMemoryDraftEvaluation| {
            p.candidate.origin = ExperienceCandidateOrigin::RuntimeTool
        },
    ] {
        let mut altered = pending.clone();
        mutate(&mut altered);
        assert!(altered.validate_for(scope).is_err());
    }
}

#[test]
fn user_scoped_drafts_still_bind_the_same_owner_and_agent_workspace() {
    let (scope, mut pending) = pending();
    pending.candidate.target_scope = EvolutionScope::User {
        principal_id: scope.owner_principal_id,
    };
    pending.candidate.status = CandidateStatus::Quarantined;
    assert_eq!(pending.validate_for(scope), Ok(()));
    pending.candidate.target_scope = EvolutionScope::User {
        principal_id: PrincipalId::generate(),
    };
    assert!(pending.validate_for(scope).is_err());
}
