use std::sync::Arc;

use std::collections::BTreeSet;

use musteragent_content_ref::{MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, EvolutionApproval, EvolutionEvaluation, ExperienceCandidate,
    evolution_approval_prompt_bytes,
};
use musteragent_identity::{
    ContentId, EvolutionEvaluationId, EvolutionProposalId, InteractionId, LedgerRecordId,
    PrincipalId, RunId, TenantId, UtcTimestamp,
};
use musteragent_runtime_content::{ContentStore, PutContent, verify_content};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;
use uuid::Uuid;

mod draft;

use crate::{
    ApprovedEvolutionInteraction, EvolutionStore, ExperienceClaimDocument, ExperienceClaimOrigin,
    MemoryPersistenceDisposition, PromoteExperience, RecordEvolutionApproval,
    RecordEvolutionEvaluation,
};

const EVALUATION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe2, 0x01,
]);
const EVALUATION_REPORT_CONTENT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe2, 0x02,
]);
const APPROVAL_PROMPT_CONTENT_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe2, 0x03,
]);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CandidateEvaluationOutcome {
    pub passed: bool,
    pub report_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CandidateEvaluationInput {
    pub candidate: ExperienceCandidate,
    pub claim_bytes: Vec<u8>,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("candidate evaluation failed: {message}")]
pub struct CandidateEvaluatorError {
    pub message: String,
}

/// Evaluators receive the frozen Candidate identity. Implementations may
/// replay its Ledger evidence, run policy suites and scan for memory-injection
/// payloads, but must return an immutable report rather than mutate the Agent.
pub trait CandidateEvaluator: Send + Sync {
    fn evaluate(
        &self,
        input: CandidateEvaluationInput,
    ) -> ApplicationFuture<'_, Result<CandidateEvaluationOutcome, CandidateEvaluatorError>>;
}

/// Deterministic first-stage evaluator for the canonical Experience claim.
/// Dream claims must reproduce their complete embedded evidence manifest;
/// manual claims bind evidence through the Candidate's immutable provenance
/// rows and are forbidden from embedding a second evidence source. Passing
/// proves only structural safety and traceability; exact Approval and CAS
/// Publication are still required before production visibility.
#[derive(Debug, Copy, Clone, Default)]
pub struct EvidenceManifestCandidateEvaluator;

impl CandidateEvaluator for EvidenceManifestCandidateEvaluator {
    fn evaluate(
        &self,
        input: CandidateEvaluationInput,
    ) -> ApplicationFuture<'_, Result<CandidateEvaluationOutcome, CandidateEvaluatorError>> {
        Box::pin(async move {
            input
                .candidate
                .validate()
                .map_err(candidate_contract_error)?;
            let media_type_matches = crate::is_experience_claim_media_type(
                input.candidate.claim_ref.media_type.as_str(),
            );
            let search_projection =
                crate::retrieval::canonical_memory_search_features(&input.claim_bytes);
            let threat = match &search_projection {
                Err(crate::MemoryRetrievalError::Threat(reason)) => Some(*reason),
                _ => None,
            };
            let search_projection_valid = search_projection.is_ok();
            let parsed = serde_json::from_slice::<serde_json::Value>(&input.claim_bytes).ok();
            let document = ExperienceClaimDocument::decode(&input.claim_bytes).ok();
            let schema_matches = document.is_some();
            let kind_contract_matches = document
                .as_ref()
                .is_some_and(|document| document.validate_for_kind(input.candidate.kind).is_ok());
            let (declared_runs, declared_records) = match document
                .as_ref()
                .map(|value| value.origin)
            {
                Some(ExperienceClaimOrigin::DreamEvidence) => (
                    parsed
                        .as_ref()
                        .map(declared_run_ids)
                        .transpose()?
                        .unwrap_or_default(),
                    parsed
                        .as_ref()
                        .map(declared_record_ids)
                        .transpose()?
                        .unwrap_or_default(),
                ),
                Some(ExperienceClaimOrigin::Manual) | None => (BTreeSet::new(), BTreeSet::new()),
            };
            let evidence_matches =
                document
                    .as_ref()
                    .is_some_and(|document| match document.origin {
                        ExperienceClaimOrigin::Manual => true,
                        ExperienceClaimOrigin::DreamEvidence => {
                            declared_runs == input.candidate.source_run_ids
                                && declared_records == input.candidate.evidence_record_ids
                        }
                    });
            let passed = media_type_matches
                && threat.is_none()
                && schema_matches
                && kind_contract_matches
                && evidence_matches
                && search_projection_valid;
            let report_bytes = serde_json::to_vec(&serde_json::json!({
                "schema":"musteragent.evolution-evaluation-report.v2",
                "proposal_id":input.candidate.proposal_id.to_string(),
                "claim_hash":Sha256::digest(&input.claim_bytes).to_string(),
                "passed":passed,
                "checks":{
                    "canonical_media_type":media_type_matches,
                    "content_utf8_and_injection_safe":threat.is_none(),
                    "canonical_claim_schema":schema_matches,
                    "kind_specific_publication_contract":kind_contract_matches,
                    "embedded_evidence_exact_or_not_applicable":evidence_matches,
                    "bounded_semantic_search_projection":search_projection_valid,
                },
                "threat":threat.map(|value| format!("{value:?}")),
            }))
            .map_err(|_| CandidateEvaluatorError {
                message: "evaluation report could not be encoded".to_owned(),
            })?;
            Ok(CandidateEvaluationOutcome {
                passed,
                report_bytes,
            })
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RequestEvolutionApproval {
    pub tenant_id: TenantId,
    pub proposal_id: EvolutionProposalId,
    pub evaluation_id: EvolutionEvaluationId,
    pub prompt_ref: musteragent_content_ref::ContentRef,
    pub requested_by: PrincipalId,
    pub requested_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvolutionApprovalTicket {
    pub interaction_id: InteractionId,
    pub prompt_ref: musteragent_content_ref::ContentRef,
}

/// Adapter over the canonical Approval context. The port deliberately returns
/// the persisted prompt reference again when loading a decision so an
/// unrelated approval Interaction cannot be rebound to a proposal.
pub trait EvolutionApprovalPort: Send + Sync {
    type Error: Send;

    fn request(
        &self,
        command: RequestEvolutionApproval,
    ) -> ApplicationFuture<'_, Result<EvolutionApprovalTicket, Self::Error>>;

    fn load_approved(
        &self,
        interaction_id: InteractionId,
    ) -> ApplicationFuture<'_, Result<Option<ApprovedEvolutionInteraction>, Self::Error>>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EvaluatedCandidate {
    pub candidate: ExperienceCandidate,
    pub evaluation: EvolutionEvaluation,
    pub disposition: MemoryPersistenceDisposition,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CandidateEvaluationWorkResult {
    Idle,
    Completed(Box<EvaluatedCandidate>),
}

/// Automatic Evaluation owner for Dream-produced Candidates. The proposal
/// row is the durable queue state: content/evaluator failures leave it in
/// Candidate or Quarantined for retry, while `record_evaluation` atomically
/// inserts the immutable report and advances the proposal status.
pub struct EvolutionCandidateEvaluationService<S>
where
    S: EvolutionStore,
{
    store: Arc<S>,
    content: Arc<dyn ContentStore>,
}

impl<S> EvolutionCandidateEvaluationService<S>
where
    S: EvolutionStore,
{
    #[must_use]
    pub fn new(store: Arc<S>, content: Arc<dyn ContentStore>) -> Self {
        Self { store, content }
    }

    pub async fn evaluate<E>(
        &self,
        mut candidate: ExperienceCandidate,
        evaluator: &E,
        evaluated_by: PrincipalId,
        evaluated_at: UtcTimestamp,
    ) -> Result<EvaluatedCandidate, EvolutionApplicationServiceError>
    where
        E: CandidateEvaluator,
    {
        candidate.validate()?;
        if !matches!(
            candidate.status,
            CandidateStatus::Candidate | CandidateStatus::Quarantined
        ) || evaluated_at < candidate.created_at
        {
            return Err(EvolutionApplicationServiceError::InvalidState);
        }
        if !crate::is_experience_claim_media_type(candidate.claim_ref.media_type.as_str()) {
            return Err(EvolutionApplicationServiceError::InvalidClaimContentType);
        }
        let claim_bytes = self
            .content
            .read(candidate.tenant_id, &candidate.claim_ref)
            .await
            .map_err(|_| EvolutionApplicationServiceError::ContentStore)?;
        verify_content(&candidate.claim_ref, &claim_bytes)
            .map_err(|_| EvolutionApplicationServiceError::ContentIntegrity)?;
        let outcome = evaluator
            .evaluate(CandidateEvaluationInput {
                candidate: candidate.clone(),
                claim_bytes,
            })
            .await
            .map_err(EvolutionApplicationServiceError::Evaluation)?;
        if outcome.report_bytes.is_empty() {
            return Err(EvolutionApplicationServiceError::InvalidEvaluation);
        }
        let report_hash = Sha256::digest(&outcome.report_bytes);
        let report_content_id = content_id(
            EVALUATION_REPORT_CONTENT_ID_NAMESPACE,
            report_hash,
            EvolutionApplicationServiceError::InvalidEvaluation,
        )?;
        let report_ref = self
            .content
            .put_at(
                report_content_id,
                PutContent {
                    tenant_id: candidate.tenant_id,
                    media_type: MediaType::new(
                        "application/vnd.musteragent.evolution-evaluation.v2+json",
                    )
                    .map_err(|_| EvolutionApplicationServiceError::InvalidEvaluation)?,
                    bytes: outcome.report_bytes,
                },
            )
            .await
            .map_err(|_| EvolutionApplicationServiceError::ContentStore)?;
        let evaluation_id = evaluation_id(
            candidate.proposal_id,
            report_ref.sha256,
            evaluated_by,
            outcome.passed,
        )?;
        let evaluation = EvolutionEvaluation {
            evaluation_id,
            proposal_id: candidate.proposal_id,
            evaluated_by,
            passed: outcome.passed,
            report_ref,
            evaluated_at,
        };
        let disposition = self
            .store
            .record_evaluation(RecordEvolutionEvaluation {
                candidate: candidate.clone(),
                evaluation: evaluation.clone(),
            })
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)?;
        candidate.transition(if outcome.passed {
            CandidateStatus::Verified
        } else {
            CandidateStatus::Rejected
        })?;
        Ok(EvaluatedCandidate {
            candidate,
            evaluation,
            disposition,
        })
    }

    /// Evaluates at most one durable Dream Candidate. Multiple workers may
    /// perform the pure checks concurrently; proposal locking and the unique
    /// Evaluation identity make the state transition exactly once.
    pub async fn process_next<E>(
        &self,
        evaluator: &E,
        evaluated_by: PrincipalId,
        evaluated_at: UtcTimestamp,
    ) -> Result<CandidateEvaluationWorkResult, EvolutionApplicationServiceError>
    where
        E: CandidateEvaluator,
    {
        let Some(candidate) = self
            .store
            .load_next_candidate_for_evaluation()
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)?
        else {
            return Ok(CandidateEvaluationWorkResult::Idle);
        };
        self.evaluate(candidate, evaluator, evaluated_by, evaluated_at)
            .await
            .map(Box::new)
            .map(CandidateEvaluationWorkResult::Completed)
    }
}

pub struct EvolutionApplicationService<S, A>
where
    S: EvolutionStore + crate::MemoryStore,
    A: EvolutionApprovalPort,
{
    store: Arc<S>,
    approvals: Arc<A>,
    content: Arc<dyn ContentStore>,
}

impl<S, A> EvolutionApplicationService<S, A>
where
    S: EvolutionStore + crate::MemoryStore,
    A: EvolutionApprovalPort,
{
    #[must_use]
    pub fn new(store: Arc<S>, approvals: Arc<A>, content: Arc<dyn ContentStore>) -> Self {
        Self {
            store,
            approvals,
            content,
        }
    }

    pub async fn evaluate<E>(
        &self,
        candidate: ExperienceCandidate,
        evaluator: &E,
        evaluated_by: PrincipalId,
        evaluated_at: UtcTimestamp,
    ) -> Result<EvaluatedCandidate, EvolutionApplicationServiceError>
    where
        E: CandidateEvaluator,
    {
        EvolutionCandidateEvaluationService::new(Arc::clone(&self.store), Arc::clone(&self.content))
            .evaluate(candidate, evaluator, evaluated_by, evaluated_at)
            .await
    }

    pub async fn request_approval(
        &self,
        candidate: ExperienceCandidate,
        evaluation: EvolutionEvaluation,
        requested_by: PrincipalId,
        requested_at: UtcTimestamp,
    ) -> Result<EvolutionApprovalTicket, EvolutionApplicationServiceError> {
        if candidate.status != CandidateStatus::Verified
            || !evaluation.passed
            || evaluation.proposal_id != candidate.proposal_id
            || requested_at < evaluation.evaluated_at
        {
            return Err(EvolutionApplicationServiceError::InvalidState);
        }
        let expected_current_version_id = self.revision_head(&candidate).await?;
        let prompt_bytes =
            evolution_approval_prompt_bytes(&candidate, &evaluation, expected_current_version_id)?;
        let prompt_hash = Sha256::digest(&prompt_bytes);
        let prompt_content_id = content_id(
            APPROVAL_PROMPT_CONTENT_ID_NAMESPACE,
            prompt_hash,
            EvolutionApplicationServiceError::InvalidApproval,
        )?;
        let prompt_ref = self
            .content
            .put_at(
                prompt_content_id,
                PutContent {
                    tenant_id: candidate.tenant_id,
                    media_type: MediaType::new(
                        musteragent_evolution::EVOLUTION_APPROVAL_PROMPT_MEDIA_TYPE,
                    )
                    .map_err(|_| EvolutionApplicationServiceError::InvalidApproval)?,
                    bytes: prompt_bytes,
                },
            )
            .await
            .map_err(|_| EvolutionApplicationServiceError::ContentStore)?;
        let ticket = self
            .approvals
            .request(RequestEvolutionApproval {
                tenant_id: candidate.tenant_id,
                proposal_id: candidate.proposal_id,
                evaluation_id: evaluation.evaluation_id,
                prompt_ref: prompt_ref.clone(),
                requested_by,
                requested_at,
            })
            .await
            .map_err(|_| EvolutionApplicationServiceError::ApprovalPort)?;
        if ticket.prompt_ref != prompt_ref {
            return Err(EvolutionApplicationServiceError::InvalidApproval);
        }
        Ok(ticket)
    }

    pub async fn register_approval(
        &self,
        candidate: ExperienceCandidate,
        evaluation: EvolutionEvaluation,
        interaction_id: InteractionId,
    ) -> Result<EvolutionApproval, EvolutionApplicationServiceError> {
        let interaction = self
            .approvals
            .load_approved(interaction_id)
            .await
            .map_err(|_| EvolutionApplicationServiceError::ApprovalPort)?
            .ok_or(EvolutionApplicationServiceError::ApprovalPending)?;
        if interaction.interaction_id != interaction_id {
            return Err(EvolutionApplicationServiceError::InvalidApproval);
        }
        let expected_current_memory_version_id = self.revision_head(&candidate).await?;
        let approval = EvolutionApproval {
            approval_interaction_id: interaction_id,
            proposal_id: candidate.proposal_id,
            evaluation_id: evaluation.evaluation_id,
            expected_current_memory_version_id,
            prompt_ref: interaction.prompt_ref,
            approved_by: interaction.approved_by,
            approved_at: interaction.approved_at,
        };
        approval.validate_for(&candidate, &evaluation)?;
        self.store
            .record_approval(RecordEvolutionApproval {
                candidate,
                evaluation,
                approval: approval.clone(),
            })
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)?;
        Ok(approval)
    }

    pub async fn publish(
        &self,
        command: PromoteExperience,
    ) -> Result<MemoryPersistenceDisposition, EvolutionApplicationServiceError> {
        command.validate()?;
        self.store
            .promote(command)
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)
    }

    async fn revision_head(
        &self,
        candidate: &ExperienceCandidate,
    ) -> Result<Option<musteragent_identity::MemoryVersionId>, EvolutionApplicationServiceError>
    {
        let Some(memory_id) = candidate.target_memory_id else {
            return Ok(None);
        };
        let head = self
            .store
            .load_active_head(candidate.tenant_id, memory_id)
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)?
            .ok_or(EvolutionApplicationServiceError::InvalidState)?;
        let scope_matches = match (candidate.target_scope, head.record.scope) {
            (
                musteragent_evolution::EvolutionScope::User {
                    principal_id: expected,
                },
                musteragent_memory::MemoryScope::User {
                    principal_id: actual,
                },
            ) => expected == actual,
            (
                musteragent_evolution::EvolutionScope::Agent { agent_id: expected },
                musteragent_memory::MemoryScope::Agent { agent_id: actual },
            ) => expected == actual,
            _ => false,
        };
        let kind_matches = matches!(
            (candidate.kind, head.record.kind),
            (
                musteragent_evolution::ExperienceKind::Semantic,
                musteragent_memory::MemoryKind::Semantic
            ) | (
                musteragent_evolution::ExperienceKind::Procedure,
                musteragent_memory::MemoryKind::Procedure
            ) | (
                musteragent_evolution::ExperienceKind::Failure,
                musteragent_memory::MemoryKind::FailureLesson
            ) | (
                musteragent_evolution::ExperienceKind::Preference,
                musteragent_memory::MemoryKind::Preference
            )
        );
        if !scope_matches
            || !kind_matches
            || head.record.current_version_id != head.version.memory_version_id
        {
            return Err(EvolutionApplicationServiceError::InvalidState);
        }
        Ok(Some(head.version.memory_version_id))
    }
}

fn evaluation_id(
    proposal_id: EvolutionProposalId,
    report_hash: musteragent_content_ref::Sha256,
    evaluated_by: PrincipalId,
    passed: bool,
) -> Result<EvolutionEvaluationId, EvolutionApplicationServiceError> {
    let mut bytes = proposal_id.to_string().into_bytes();
    bytes.extend_from_slice(&report_hash.into_bytes());
    bytes.extend_from_slice(evaluated_by.to_string().as_bytes());
    bytes.push(u8::from(passed));
    EvolutionEvaluationId::from_uuid(Uuid::new_v5(&EVALUATION_ID_NAMESPACE, &bytes))
        .map_err(|_| EvolutionApplicationServiceError::InvalidEvaluation)
}

fn content_id(
    namespace: Uuid,
    hash: Sha256,
    error: EvolutionApplicationServiceError,
) -> Result<ContentId, EvolutionApplicationServiceError> {
    ContentId::from_uuid(Uuid::new_v5(&namespace, &hash.into_bytes())).map_err(|_| error)
}

fn declared_run_ids(claim: &serde_json::Value) -> Result<BTreeSet<RunId>, CandidateEvaluatorError> {
    declared_evidence_ids(claim, "run_id")
}

fn declared_record_ids(
    claim: &serde_json::Value,
) -> Result<BTreeSet<LedgerRecordId>, CandidateEvaluatorError> {
    claim
        .get("evidence")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(invalid_evidence_manifest)?
        .iter()
        .flat_map(|span| {
            span.get("record_ids")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        })
        .map(|value| {
            value
                .as_str()
                .ok_or_else(invalid_evidence_manifest)?
                .parse()
                .map_err(|_| invalid_evidence_manifest())
        })
        .collect()
}

fn declared_evidence_ids<T>(
    claim: &serde_json::Value,
    field: &str,
) -> Result<BTreeSet<T>, CandidateEvaluatorError>
where
    T: Ord + std::str::FromStr,
{
    claim
        .get("evidence")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(invalid_evidence_manifest)?
        .iter()
        .map(|span| {
            span.get(field)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(invalid_evidence_manifest)?
                .parse()
                .map_err(|_| invalid_evidence_manifest())
        })
        .collect()
}

fn invalid_evidence_manifest() -> CandidateEvaluatorError {
    CandidateEvaluatorError {
        message: "Candidate evidence manifest is malformed".to_owned(),
    }
}

fn candidate_contract_error(error: impl std::fmt::Display) -> CandidateEvaluatorError {
    CandidateEvaluatorError {
        message: format!("Candidate contract rejected evaluation input: {error}"),
    }
}

#[derive(Debug, Error)]
pub enum EvolutionApplicationServiceError {
    #[error(transparent)]
    Evolution(#[from] musteragent_evolution::EvolutionContractError),
    #[error(transparent)]
    Memory(#[from] crate::MemoryApplicationError),
    #[error("Evolution Candidate is not in the required state")]
    InvalidState,
    #[error("Evolution evaluation output is invalid")]
    InvalidEvaluation,
    #[error("Evolution Candidate claim does not use the canonical media type")]
    InvalidClaimContentType,
    #[error("Evolution approval binding is invalid")]
    InvalidApproval,
    #[error("Evolution approval is still pending or was denied")]
    ApprovalPending,
    #[error(transparent)]
    Evaluation(CandidateEvaluatorError),
    #[error("Evolution approval context is unavailable")]
    ApprovalPort,
    #[error("Evolution persistence is unavailable")]
    Store,
    #[error("Evolution Content Store is unavailable")]
    ContentStore,
    #[error("Evolution ContentRef does not match immutable bytes")]
    ContentIntegrity,
}
