//! Only Host-verified material may enter the business package. Identity and
//! result content are projected from those original facts, never package JSON.
use std::collections::BTreeSet;

use musteragent_memory_business::ranking::{RankedSelection, RankingCandidate};

use super::{DynamicMemoryHit, MemoryRetrievalError};

pub(super) async fn rank_verified_hits(
    ranker: &dyn crate::MemoryRanker,
    query: Option<&str>,
    mut hits: Vec<DynamicMemoryHit>,
    limit: u16,
    execution: Option<&musteragent_runtime_capability::OperationContext>,
) -> Result<Vec<DynamicMemoryHit>, MemoryRetrievalError> {
    // Move the bodies into the finite business input without duplicating all
    // candidate text. Host-owned identity/visibility metadata stays here.
    let mut candidates = hits
        .iter_mut()
        .map(|hit| RankingCandidate {
            content: std::mem::take(&mut hit.content),
            semantic_similarity_bps: hit.semantic_similarity_bps,
        })
        .collect::<Vec<_>>();
    // The process Host owns cancellation/reaping. Do not abandon its physical
    // computation future; check the original execution fence before and after.
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryRetrievalError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    let selections = ranker.rank(query, &candidates, limit).await?;
    if execution.is_some_and(|context| context.stopping()) {
        return Err(MemoryRetrievalError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ));
    }
    validate_selections(&selections, hits.len(), limit)?;
    if selections.iter().any(|selection| {
        selection.semantic_similarity_bps
            > candidates[usize::from(selection.candidate_index)].semantic_similarity_bps
    }) {
        return Err(MemoryRetrievalError::InvalidSearchDocument);
    }
    // Authoritative list/get cannot report an existing verified head as absent
    // or substitute a different order. Only query ranking is business policy.
    if query.is_none()
        && (selections.len() != hits.len().min(usize::from(limit))
            || selections
                .iter()
                .enumerate()
                .any(|(index, selection)| usize::from(selection.candidate_index) != index))
    {
        return Err(MemoryRetrievalError::InvalidSearchDocument);
    }
    let mut hits = hits.into_iter().map(Some).collect::<Vec<_>>();
    selections
        .into_iter()
        .enumerate()
        .map(|(rank, selection)| {
            let index = usize::from(selection.candidate_index);
            let mut hit = hits[index]
                .take()
                .ok_or(MemoryRetrievalError::InvalidSearchDocument)?;
            hit.rank =
                u16::try_from(rank + 1).map_err(|_| MemoryRetrievalError::InvalidSearchDocument)?;
            hit.content = std::mem::take(&mut candidates[index].content);
            hit.lexical_relevance_bps = selection.lexical_relevance_bps;
            hit.semantic_similarity_bps = selection.semantic_similarity_bps;
            hit.relevance_bps = selection.relevance_bps;
            Ok(hit)
        })
        .collect()
}

fn validate_selections(
    selections: &[RankedSelection],
    candidate_count: usize,
    limit: u16,
) -> Result<(), MemoryRetrievalError> {
    let mut seen = BTreeSet::new();
    if selections.len() > usize::from(limit)
        || selections.iter().any(|selection| {
            usize::from(selection.candidate_index) >= candidate_count
                || !seen.insert(selection.candidate_index)
                || selection.lexical_relevance_bps > 10_000
                || selection.semantic_similarity_bps > 10_000
                || selection.relevance_bps > 10_000
        })
    {
        return Err(MemoryRetrievalError::InvalidSearchDocument);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(index: u16) -> RankedSelection {
        RankedSelection {
            candidate_index: index,
            lexical_relevance_bps: 5_000,
            semantic_similarity_bps: 8_000,
            relevance_bps: 9_000,
        }
    }

    #[test]
    fn rejects_fabricated_duplicate_excessive_or_corrupt_selections() {
        assert!(validate_selections(&[selection(0)], 1, 1).is_ok());
        assert!(validate_selections(&[selection(1)], 1, 1).is_err());
        assert!(validate_selections(&[selection(0), selection(0)], 2, 2).is_err());
        assert!(validate_selections(&[selection(0), selection(1)], 2, 1).is_err());
        for field in 0..3 {
            let mut corrupt = selection(0);
            match field {
                0 => corrupt.lexical_relevance_bps = 10_001,
                1 => corrupt.semantic_similarity_bps = 10_001,
                _ => corrupt.relevance_bps = 10_001,
            }
            assert!(validate_selections(&[corrupt], 1, 1).is_err());
        }
    }
}
