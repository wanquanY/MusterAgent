//! Merge original immutable facts without applying package relevance policy.
//! A low score does not exempt a duplicate version from integrity checks.
use super::MemoryRetrievalError;
use std::collections::BTreeMap;

pub(super) fn merge<T: PartialEq, K: Ord + Clone>(
    lexical: &mut Vec<T>,
    semantic: Vec<(T, u16)>,
    key: impl Fn(&T) -> K,
) -> Result<BTreeMap<K, u16>, MemoryRetrievalError> {
    if lexical.len() > 100 || semantic.len() > 100 {
        return Err(MemoryRetrievalError::InvalidSearchDocument);
    }
    let mut known = BTreeMap::new();
    for (index, candidate) in lexical.iter().enumerate() {
        if known.insert(key(candidate), index).is_some() {
            return Err(MemoryRetrievalError::InvalidSearchDocument);
        }
    }
    let mut scores = BTreeMap::<K, u16>::new();
    for (candidate, score) in semantic {
        if score > 10_000 {
            return Err(MemoryRetrievalError::InvalidSearchDocument);
        }
        let identity = key(&candidate);
        if let Some(&index) = known.get(&identity) {
            if lexical[index] != candidate {
                return Err(MemoryRetrievalError::InvalidSearchDocument);
            }
        } else {
            known.insert(identity.clone(), lexical.len());
            lexical.push(candidate);
        }
        scores
            .entry(identity)
            .and_modify(|value| *value = (*value).max(score))
            .or_insert(score);
    }
    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn low_similarity_remains_package_policy_but_never_hides_conflicting_facts() {
        let mut values = vec![(1, "original")];
        let scores = merge(
            &mut values,
            vec![((1, "original"), 6_000), ((2, "second"), 4_000)],
            |v| v.0,
        )
        .unwrap();
        assert_eq!(values, vec![(1, "original"), (2, "second")]);
        assert_eq!(scores, BTreeMap::from([(1, 6_000), (2, 4_000)]));
        assert!(merge(&mut values, vec![((1, "different"), 0)], |v| v.0).is_err());
        assert!(merge(&mut vec![1], vec![(2, 10_001)], |v| *v).is_err());
    }
}
