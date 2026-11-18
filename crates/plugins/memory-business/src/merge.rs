//! Fuse finite lexical/vector lanes while retaining exact candidate identity.
//! `K` is opaque to the package; it does not grant authority to load a record.
use std::collections::BTreeMap;

use crate::InvalidInput;
use crate::ranking::{MAX_LANE_CANDIDATES, semantic_relevance};

pub struct MergedCandidates<T, K> {
    pub candidates: Vec<T>,
    pub semantic_scores: BTreeMap<K, u16>,
}

pub fn merge_candidates<T: PartialEq, K: Ord + Clone>(
    lexical: Vec<T>,
    semantic: Vec<(T, u16)>,
    key: impl Fn(&T) -> K,
) -> Result<MergedCandidates<T, K>, InvalidInput> {
    if lexical.len() > MAX_LANE_CANDIDATES || semantic.len() > MAX_LANE_CANDIDATES {
        return Err(InvalidInput);
    }
    let mut candidates = lexical;
    let mut known = BTreeMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if known.insert(key(candidate), index).is_some() {
            return Err(InvalidInput);
        }
    }
    let mut semantic_scores: BTreeMap<K, u16> = BTreeMap::new();
    for (candidate, score) in semantic {
        let score = semantic_relevance(score)?;
        if score == 0 {
            continue;
        }
        let identity = key(&candidate);
        if let Some(&index) = known.get(&identity) {
            // A duplicate immutable version cannot identify conflicting
            // metadata/content merely because it came from another index.
            if candidates[index] != candidate {
                return Err(InvalidInput);
            }
        } else {
            known.insert(identity.clone(), candidates.len());
            candidates.push(candidate);
        }
        semantic_scores
            .entry(identity)
            .and_modify(|value| *value = (*value).max(score))
            .or_insert(score);
    }
    Ok(MergedCandidates {
        candidates,
        semantic_scores,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_lanes_keep_order_and_strongest_chunk_score() {
        let merged = merge_candidates(
            vec![1, 2],
            vec![(2, 8_000), (3, 9_000), (2, 9_500), (4, 6_499)],
            |id| *id,
        )
        .expect("finite candidates");
        assert_eq!(merged.candidates, vec![1, 2, 3]);
        assert_eq!(
            merged.semantic_scores,
            BTreeMap::from([(2, 9_500), (3, 9_000)])
        );
    }

    #[test]
    fn corruption_and_excess_are_errors_not_empty_recall() {
        assert!(merge_candidates(vec![1, 1], vec![], |id| *id).is_err());
        assert!(
            merge_candidates(
                vec![(1, "original")],
                vec![((1, "different"), 9_000)],
                |(id, _)| *id
            )
            .is_err()
        );
        assert!(merge_candidates(vec![], vec![(1, 10_001)], |id| *id).is_err());
        assert!(merge_candidates((0..101).collect(), vec![], |id| *id).is_err());
        assert!(
            merge_candidates(vec![], (0..101).map(|id| (id, 8_000)).collect(), |id| *id).is_err()
        );
    }
}
