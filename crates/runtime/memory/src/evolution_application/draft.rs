use super::*;
use crate::{AgentMemoryDraftEvaluationScope, AgentMemoryDraftStore};

impl<S: EvolutionStore + AgentMemoryDraftStore> EvolutionCandidateEvaluationService<S> {
    /// Same evaluator/report persistence as the system worker, with a separate
    /// original-store selection contract for an authenticated Draft owner.
    pub async fn process_next_draft<E: CandidateEvaluator>(
        &self,
        scope: AgentMemoryDraftEvaluationScope,
        evaluator: &E,
        evaluated_by: PrincipalId,
        clock: fn() -> UtcTimestamp,
    ) -> Result<CandidateEvaluationWorkResult, EvolutionApplicationServiceError> {
        let Some(pending) = self
            .store
            .load_next_draft_candidate(scope)
            .await
            .map_err(|_| EvolutionApplicationServiceError::Store)?
        else {
            return Ok(CandidateEvaluationWorkResult::Idle);
        };
        pending
            .validate_for(scope)
            .map_err(|_| EvolutionApplicationServiceError::InvalidState)?;
        // Same immutable saved-claim bound as Draft readback. Validate before
        // Content IO, even if a corrupted store returned a larger reference.
        if !(1..=64 * 1024).contains(&pending.candidate.claim_ref.byte_length) {
            return Err(EvolutionApplicationServiceError::ContentIntegrity);
        }
        // Selection may await a concurrent submission. Take time after that
        // physical read, not before a newly created Candidate could be visible.
        self.evaluate(pending.candidate, evaluator, evaluated_by, clock())
            .await
            .map(Box::new)
            .map(CandidateEvaluationWorkResult::Completed)
    }
}
