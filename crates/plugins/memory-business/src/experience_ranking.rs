//! Published experience ranking over Host-verified text. Candidate identities
//! remain opaque: the result contains only positions in the supplied facts.
use crate::InvalidInput;
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BTreeSet};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperienceCandidate {
    pub searchable_text: String,
    pub created_at_ms: i64,
    /// Host-assigned position in canonical identity order, never a record ID.
    pub identity_ordinal: u16,
}

pub fn rank(
    query: &str,
    candidates: &[ExperienceCandidate],
    limit: u16,
) -> Result<Vec<u16>, InvalidInput> {
    if query.trim().is_empty()
        || query.len() > 1000
        || candidates.len() > 100
        || !(1..=100).contains(&limit)
        || candidates.iter().any(|candidate| {
            candidate.searchable_text.len() > 1024 * 1024
                || usize::from(candidate.identity_ordinal) >= candidates.len()
        })
        || candidates
            .iter()
            .map(|candidate| candidate.identity_ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != candidates.len()
    {
        return Err(InvalidInput);
    }
    let terms = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase)
        .collect::<BTreeSet<_>>();
    let mut selected = candidates
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            let text = candidate.searchable_text.to_lowercase();
            let relevance = terms.iter().filter(|term| text.contains(*term)).count();
            if relevance == 0 && !terms.is_empty() {
                return None;
            }
            Some((
                Reverse(relevance),
                Reverse(candidate.created_at_ms),
                candidate.identity_ordinal,
                index,
            ))
        })
        .collect::<Vec<_>>();
    selected.sort_unstable();
    selected
        .into_iter()
        .take(usize::from(limit))
        .map(|(_, _, _, index)| u16::try_from(index).map_err(|_| InvalidInput))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fact(text: &str, at: i64, identity: u16) -> ExperienceCandidate {
        ExperienceCandidate {
            searchable_text: text.into(),
            created_at_ms: at,
            identity_ordinal: identity,
        }
    }
    #[test]
    fn relevance_recency_and_stable_identity_order_preserve_retained_policy() {
        let values = vec![
            fact("verified memory", 10, 2),
            fact("verified memory", 10, 0),
            fact("verified memory", 20, 1),
            fact("verified", 100, 3),
        ];
        assert_eq!(
            rank("VERIFIED memory", &values, 4).unwrap(),
            vec![2, 1, 0, 3]
        );
        assert_eq!(rank("unrelated", &values, 4).unwrap(), Vec::<u16>::new());
        assert_eq!(rank("!!!", &values, 2).unwrap(), vec![3, 2]);
    }
    #[test]
    fn duplicate_identity_and_resource_excess_are_not_empty_results() {
        assert!(
            rank(
                "verified",
                &[fact("verified", 1, 0), fact("verified", 2, 0)],
                5
            )
            .is_err()
        );
        assert!(rank("verified", &[fact("verified", 1, 1)], 5).is_err());
        assert!(rank("verified", &[], 0).is_err());
        assert!(rank("", &[], 5).is_err());
    }
}
