use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::{StreamExt as _, stream};
use musteragent_content_ref::ContentRef;
use musteragent_runtime_content::ContentStore;

use crate::claim::MaterializedExperienceClaim;
use crate::{
    AgentEvolutionInspection, ExperienceClaimDocument, ExperienceClaimInspection,
    is_experience_claim_media_type,
};

const MAX_CONCURRENT_CLAIM_READS: usize = 8;
use musteragent_evolution::ExperienceCandidateOrigin;

#[derive(Debug, Clone, Eq, PartialEq)]
enum ClaimMaterialization {
    Ready(MaterializedExperienceClaim),
    ContentUnavailable,
    ContractInvalid,
}

/// Resolves canonical Experience Claim documents inside Runtime and attaches a
/// bounded product projection to the immutable inspection. Product clients no
/// longer need to understand content media types, claim schema versions, or
/// raw evidence JSON.
pub async fn materialize_agent_evolution_claims(
    mut inspection: AgentEvolutionInspection,
    content: Arc<dyn ContentStore>,
) -> AgentEvolutionInspection {
    let mut references = BTreeMap::<String, ContentRef>::new();
    for proposal in &inspection.proposals {
        if is_experience_claim_media_type(proposal.candidate.claim_ref.media_type.as_str()) {
            references
                .entry(content_key(&proposal.candidate.claim_ref))
                .or_insert_with(|| proposal.candidate.claim_ref.clone());
        }
    }
    for node in &inspection.learning_graph {
        if is_experience_claim_media_type(node.payload_ref.media_type.as_str()) {
            references
                .entry(content_key(&node.payload_ref))
                .or_insert_with(|| node.payload_ref.clone());
        }
    }

    let resolved = resolve_claims(inspection.tenant_id, references, content).await;

    for proposal in &mut inspection.proposals {
        populate_proposal(proposal, &resolved);
    }
    for node in &mut inspection.learning_graph {
        node.claim_detail = if is_experience_claim_media_type(node.payload_ref.media_type.as_str())
        {
            Some(graph_materialization(
                resolved.get(&content_key(&node.payload_ref)),
            ))
        } else {
            None
        };
    }
    inspection
}

pub(crate) async fn materialize_agent_memory_draft_claims(
    review: &mut crate::AgentMemoryDraftReviewBatch,
    content: Arc<dyn ContentStore>,
) {
    let references = review
        .items
        .iter()
        .map(|item| &item.proposal.candidate.claim_ref)
        .filter(|reference| is_experience_claim_media_type(reference.media_type.as_str()))
        .map(|reference| (content_key(reference), reference.clone()))
        .collect();
    let resolved = resolve_claims(review.tenant_id, references, content).await;
    for item in &mut review.items {
        populate_proposal(&mut item.proposal, &resolved);
    }
}

/// Single-candidate Tool inspection shares the same claim/provenance owner.
pub(crate) async fn materialize_evolution_proposal_claim(
    proposal: &mut crate::EvolutionProposalInspection,
    content: Arc<dyn ContentStore>,
) {
    if proposal.candidate.claim_ref.byte_length > 1024 * 1024 {
        proposal.claim_detail = ExperienceClaimInspection::contract_invalid();
        return;
    }
    let references = BTreeMap::from([(
        content_key(&proposal.candidate.claim_ref),
        proposal.candidate.claim_ref.clone(),
    )]);
    let resolved = resolve_claims(proposal.candidate.tenant_id, references, content).await;
    populate_proposal(proposal, &resolved);
}

async fn resolve_claims(
    tenant_id: musteragent_identity::TenantId,
    references: BTreeMap<String, ContentRef>,
    content: Arc<dyn ContentStore>,
) -> BTreeMap<String, ClaimMaterialization> {
    let references = references.into_iter().collect::<Vec<_>>();
    let content_refs = references
        .iter()
        .map(|(_, reference)| reference.clone())
        .collect::<Vec<_>>();
    match content.read_batch(tenant_id, &content_refs).await {
        Ok(payloads) if payloads.len() == references.len() => references
            .iter()
            .zip(payloads)
            .map(|((key, _), bytes)| (key.clone(), decode_materialization(&bytes)))
            .collect(),
        Ok(_) | Err(_) => {
            // A batch is intentionally all-or-nothing. Fall back only on the
            // exceptional path so one erased historical object does not hide
            // every otherwise valid Experience Claim in the inspection.
            stream::iter(references.into_iter().map(|(key, reference)| {
                let content = Arc::clone(&content);
                async move {
                    let materialization = match content.read(tenant_id, &reference).await {
                        Ok(bytes) => decode_materialization(&bytes),
                        Err(_) => ClaimMaterialization::ContentUnavailable,
                    };
                    (key, materialization)
                }
            }))
            .buffer_unordered(MAX_CONCURRENT_CLAIM_READS)
            .collect::<BTreeMap<_, _>>()
            .await
        }
    }
}

fn populate_proposal(
    proposal: &mut crate::EvolutionProposalInspection,
    resolved: &BTreeMap<String, ClaimMaterialization>,
) {
    proposal.claim_detail =
        if !is_experience_claim_media_type(proposal.candidate.claim_ref.media_type.as_str()) {
            ExperienceClaimInspection::contract_invalid()
        } else {
            proposal_materialization(
                proposal.candidate.origin,
                resolved.get(&content_key(&proposal.candidate.claim_ref)),
                &proposal.candidate.source_run_ids,
                &proposal.candidate.evidence_record_ids,
            )
        };
}

fn proposal_materialization(
    origin: ExperienceCandidateOrigin,
    value: Option<&ClaimMaterialization>,
    expected_run_ids: &std::collections::BTreeSet<musteragent_identity::RunId>,
    expected_record_ids: &std::collections::BTreeSet<musteragent_identity::LedgerRecordId>,
) -> ExperienceClaimInspection {
    match value {
        Some(ClaimMaterialization::Ready(value))
            if match origin {
                ExperienceCandidateOrigin::DreamInduction => {
                    value.inspection.origin == Some(crate::ExperienceClaimOrigin::DreamEvidence)
                        && value.source_run_ids == *expected_run_ids
                        && value.evidence_record_ids == *expected_record_ids
                }
                ExperienceCandidateOrigin::RuntimeTool => {
                    value.inspection.origin == Some(crate::ExperienceClaimOrigin::Manual)
                        && value.source_run_ids.is_empty()
                        && value.evidence_record_ids.is_empty()
                }
                ExperienceCandidateOrigin::UserDraft => {
                    value.inspection.origin == Some(crate::ExperienceClaimOrigin::Manual)
                        && value.source_run_ids.is_empty()
                        && value.evidence_record_ids.is_empty()
                        && expected_run_ids.is_empty()
                        && expected_record_ids.is_empty()
                }
            } =>
        {
            value.inspection.clone()
        }
        Some(ClaimMaterialization::Ready(_) | ClaimMaterialization::ContractInvalid) => {
            ExperienceClaimInspection::contract_invalid()
        }
        Some(ClaimMaterialization::ContentUnavailable) | None => {
            ExperienceClaimInspection::content_unavailable()
        }
    }
}

fn graph_materialization(value: Option<&ClaimMaterialization>) -> ExperienceClaimInspection {
    match value {
        Some(ClaimMaterialization::Ready(value)) => value.inspection.clone(),
        Some(ClaimMaterialization::ContractInvalid) => {
            ExperienceClaimInspection::contract_invalid()
        }
        Some(ClaimMaterialization::ContentUnavailable) | None => {
            ExperienceClaimInspection::content_unavailable()
        }
    }
}

fn decode_materialization(bytes: &[u8]) -> ClaimMaterialization {
    match ExperienceClaimDocument::decode(bytes)
        .and_then(|document| document.materialize_inspection())
    {
        Ok(value) => ClaimMaterialization::Ready(value),
        Err(_) => ClaimMaterialization::ContractInvalid,
    }
}

fn content_key(reference: &ContentRef) -> String {
    format!(
        "{}:{}:{}:{}",
        reference.content_id,
        reference.sha256,
        reference.media_type.as_str(),
        reference.byte_length,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use musteragent_evolution::{BranchEvidenceState, LearningOutcome, RunTerminalEvidence};
    use musteragent_identity::{ConversationId, LedgerRecordId, RunId};

    use super::{ClaimMaterialization, proposal_materialization};
    use crate::claim::MaterializedExperienceClaim;
    use crate::{
        ExperienceClaimInspection, ExperienceClaimInspectionState, ExperienceClaimOrigin,
        ExperienceEvidenceInspection,
    };

    #[test]
    fn runtime_manual_claim_uses_candidate_ledger_provenance_not_fabricated_embedded_evidence() {
        let document = crate::ExperienceClaimDocument::manual(
            "Use Chinese".into(),
            Default::default(),
            "Explicit user request".into(),
            "private".into(),
        );
        let ready =
            ClaimMaterialization::Ready(document.materialize_inspection().expect("manual claim"));
        let runs = BTreeSet::from([RunId::generate()]);
        let records = BTreeSet::from([LedgerRecordId::generate()]);
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::RuntimeTool,
                Some(&ready),
                &runs,
                &records
            )
            .state,
            ExperienceClaimInspectionState::Ready
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::DreamInduction,
                Some(&ready),
                &runs,
                &records
            )
            .state,
            ExperienceClaimInspectionState::ContractInvalid
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::UserDraft,
                Some(&ready),
                &runs,
                &records
            )
            .state,
            ExperienceClaimInspectionState::ContractInvalid
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::UserDraft,
                Some(&ready),
                &BTreeSet::new(),
                &BTreeSet::new()
            )
            .state,
            ExperienceClaimInspectionState::Ready
        );
    }

    #[test]
    fn proposal_materialization_requires_exact_provenance_and_preserves_error_state() {
        let run_id = RunId::generate();
        let record_id = LedgerRecordId::generate();
        let ready = ClaimMaterialization::Ready(MaterializedExperienceClaim {
            inspection: ExperienceClaimInspection {
                state: ExperienceClaimInspectionState::Ready,
                origin: Some(ExperienceClaimOrigin::DreamEvidence),
                claim: "Verify delegation capability before spawning".to_owned(),
                outcome: "Avoid invalid child Runs".to_owned(),
                conditions: Vec::new(),
                evidence: vec![ExperienceEvidenceInspection {
                    conversation_id: ConversationId::generate(),
                    run_id,
                    branch_id: None,
                    branch_state: BranchEvidenceState::Active,
                    cursor_from: 1,
                    cursor_to: 2,
                    record_count: 1,
                    terminal: RunTerminalEvidence::Failed,
                    outcome: LearningOutcome::Failure,
                }],
            },
            source_run_ids: BTreeSet::from([run_id]),
            evidence_record_ids: BTreeSet::from([record_id]),
        });

        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::DreamInduction,
                Some(&ready),
                &BTreeSet::from([run_id]),
                &BTreeSet::from([record_id]),
            )
            .state,
            ExperienceClaimInspectionState::Ready,
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::DreamInduction,
                Some(&ready),
                &BTreeSet::from([RunId::generate()]),
                &BTreeSet::from([record_id]),
            )
            .state,
            ExperienceClaimInspectionState::ContractInvalid,
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::DreamInduction,
                Some(&ClaimMaterialization::ContentUnavailable),
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .state,
            ExperienceClaimInspectionState::ContentUnavailable,
        );
        assert_eq!(
            proposal_materialization(
                super::ExperienceCandidateOrigin::DreamInduction,
                Some(&ClaimMaterialization::ContractInvalid),
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .state,
            ExperienceClaimInspectionState::ContractInvalid,
        );
    }
}
