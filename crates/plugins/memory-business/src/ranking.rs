//! Deterministic relevance and diversity policy over host-verified facts.
//! Selections refer only to positions in the supplied candidate set; they
//! cannot introduce a Memory identity, content reference or new body.
use std::cmp::Reverse;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::InvalidInput;
use crate::features::{lexical_features, relevance_from_features};

pub const MAX_LANE_CANDIDATES: usize = 100;
pub const MAX_MERGED_CANDIDATES: usize = 2 * MAX_LANE_CANDIDATES;
pub const MAX_QUERY_BYTES: usize = 64 * 1_024;
pub const MAX_CANDIDATE_BYTES: usize = 1_024 * 1_024;
const MIN_LEXICAL_RELEVANCE_BPS: u64 = 1_500;
const MIN_SEMANTIC_SIMILARITY_BPS: u16 = 6_500;
const MMR_RELEVANCE_WEIGHT_BPS: u64 = 8_000;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankingCandidate {
    pub content: String,
    pub semantic_similarity_bps: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankedSelection {
    pub candidate_index: u16,
    pub lexical_relevance_bps: u64,
    pub semantic_similarity_bps: u16,
    pub relevance_bps: u64,
}

/// Low similarity is a business-policy decision; out-of-range scores are
/// corrupt input and must not be silently clamped or treated as empty recall.
pub fn semantic_relevance(score: u16) -> Result<u16, InvalidInput> {
    if score > 10_000 {
        return Err(InvalidInput);
    }
    Ok(if score < MIN_SEMANTIC_SIMILARITY_BPS {
        0
    } else {
        score
    })
}

/// `None` is an authoritative list/get: preserve supplied order and do not
/// invent a query. Query results use the retained threshold, fusion and MMR.
pub fn rank(
    query: Option<&str>,
    candidates: &[RankingCandidate],
    limit: u16,
) -> Result<Vec<RankedSelection>, InvalidInput> {
    if limit == 0
        || usize::from(limit) > MAX_LANE_CANDIDATES
        || candidates.len() > MAX_MERGED_CANDIDATES
    {
        return Err(InvalidInput);
    }
    let query_features = query
        .map(|text| {
            let text = text.trim();
            if text.is_empty() || text.len() > MAX_QUERY_BYTES {
                return Err(InvalidInput);
            }
            let features = lexical_features(text);
            if features.is_empty() {
                Err(InvalidInput)
            } else {
                Ok(features)
            }
        })
        .transpose()?;
    let mut ranked = Vec::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.content.len() > MAX_CANDIDATE_BYTES {
            return Err(InvalidInput);
        }
        let semantic = semantic_relevance(candidate.semantic_similarity_bps)?;
        let content_features = query_features
            .as_ref()
            .map(|_| lexical_features(&candidate.content))
            .unwrap_or_default();
        let lexical = query_features.as_ref().map_or(10_000, |features| {
            let score = relevance_from_features(features, &content_features);
            if score < MIN_LEXICAL_RELEVANCE_BPS {
                0
            } else {
                score
            }
        });
        let relevance = fused_relevance(lexical, u64::from(semantic));
        if relevance == 0 {
            continue;
        }
        ranked.push(RankedCandidate {
            content_features,
            selection: RankedSelection {
                candidate_index: u16::try_from(index).map_err(|_| InvalidInput)?,
                lexical_relevance_bps: lexical,
                semantic_similarity_bps: semantic,
                relevance_bps: relevance,
            },
        });
    }
    if query.is_some() {
        Ok(select_diverse(ranked, usize::from(limit)))
    } else {
        Ok(ranked
            .into_iter()
            .take(usize::from(limit))
            .map(|c| c.selection)
            .collect())
    }
}

struct RankedCandidate {
    content_features: BTreeSet<String>,
    selection: RankedSelection,
}

fn fused_relevance(lexical_bps: u64, semantic_bps: u64) -> u64 {
    lexical_bps
        .max(semantic_bps)
        .saturating_add(lexical_bps.min(semantic_bps) / 5)
        .min(10_000)
}

fn select_diverse(mut candidates: Vec<RankedCandidate>, limit: usize) -> Vec<RankedSelection> {
    let mut selected = Vec::with_capacity(limit.min(candidates.len()));
    let mut selected_features = Vec::with_capacity(selected.capacity());
    while selected.len() < limit && !candidates.is_empty() {
        let best_index = candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| {
                let redundancy = selected_features
                    .iter()
                    .map(|features| jaccard_similarity_bps(&candidate.content_features, features))
                    .max()
                    .unwrap_or_default();
                let diversity = 10_000_u64.saturating_sub(redundancy);
                let relevance = candidate.selection.relevance_bps;
                let score = relevance
                    .saturating_mul(MMR_RELEVANCE_WEIGHT_BPS)
                    .saturating_add(diversity.saturating_mul(10_000 - MMR_RELEVANCE_WEIGHT_BPS))
                    / 10_000;
                (
                    Reverse(score),
                    Reverse(relevance),
                    candidate.selection.candidate_index,
                )
            })
            .map(|(index, _)| index)
            .expect("non-empty candidate set");
        let candidate = candidates.swap_remove(best_index);
        selected_features.push(candidate.content_features);
        selected.push(candidate.selection);
    }
    selected
}

fn jaccard_similarity_bps(left: &BTreeSet<String>, right: &BTreeSet<String>) -> u64 {
    if left.is_empty() && right.is_empty() {
        return 10_000;
    }
    let intersection = u64::try_from(left.intersection(right).count()).unwrap_or(u64::MAX);
    let union = u64::try_from(left.union(right).count()).unwrap_or(u64::MAX);
    intersection
        .saturating_mul(10_000)
        .checked_div(union)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
