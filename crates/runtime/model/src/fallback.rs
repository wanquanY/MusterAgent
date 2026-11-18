use musteragent_model::ModelRoute;

/// Admission-pinned route sequence used by the Agent Loop. Credentials live in
/// the Runtime Snapshot alongside each route and are copied into one physical
/// canonical request only when that candidate is selected.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelFallbackPlan {
    candidates: Vec<ModelRoute>,
}

impl ModelFallbackPlan {
    pub fn new(candidates: Vec<ModelRoute>) -> Result<Self, FallbackContractError> {
        if candidates.is_empty() {
            return Err(FallbackContractError::EmptyPlan);
        }
        for candidate in &candidates {
            candidate
                .validate()
                .map_err(|_| FallbackContractError::InvalidRoute)?;
        }
        let route_refs = candidates
            .iter()
            .map(|candidate| &candidate.route_ref)
            .collect::<std::collections::BTreeSet<_>>();
        if route_refs.len() != candidates.len() {
            return Err(FallbackContractError::DuplicateCandidate);
        }
        Ok(Self { candidates })
    }

    #[must_use]
    pub fn candidates(&self) -> &[ModelRoute] {
        &self.candidates
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, thiserror::Error)]
pub enum FallbackContractError {
    #[error("model fallback plan requires a primary route")]
    EmptyPlan,
    #[error("model fallback plan contains an invalid route")]
    InvalidRoute,
    #[error("model fallback candidates must be unique; vector order is execution priority")]
    DuplicateCandidate,
}
