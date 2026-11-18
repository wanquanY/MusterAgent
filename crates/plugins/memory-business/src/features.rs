//! Retained v1 text features. Host index projections and package ranking must
//! keep this format stable; changing index representation requires migration.
use std::collections::BTreeSet;

pub fn lexical_features(text: &str) -> BTreeSet<String> {
    let normalized = normalized_search_text(text);
    let mut features = BTreeSet::new();
    for token in normalized.split_whitespace() {
        if token.chars().count() >= 2 {
            features.insert(token.to_owned());
        }
        let characters = token.chars().collect::<Vec<_>>();
        for pair in characters.windows(2) {
            features.insert(pair.iter().collect());
        }
    }
    features
}

pub fn query_search_features(text: &str) -> String {
    let normalized = normalized_search_text(text);
    let mut features = BTreeSet::new();
    for token in normalized.split_whitespace() {
        let characters = token.chars().collect::<Vec<_>>();
        if characters.len() >= 2 {
            features.insert(token.to_owned());
        }
        // Latin words already have stable token boundaries. Indexing their
        // bigrams is useful for multilingual documents, but querying by those
        // bigrams creates accidental matches such as `migration` -> `at`.
        // Scripts without ASCII word boundaries retain bigrams so Chinese and
        // similar task text can still match partial phrases.
        if characters.iter().any(|character| !character.is_ascii()) {
            for pair in characters.windows(2) {
                features.insert(pair.iter().collect());
            }
        }
    }
    features.into_iter().collect::<Vec<_>>().join(" ")
}

fn normalized_search_text(text: &str) -> String {
    text.chars()
        .flat_map(char::to_lowercase)
        .map(|character| {
            if character.is_alphanumeric() {
                character
            } else {
                ' '
            }
        })
        .collect()
}

#[must_use]
pub fn canonical_search_features(text: &str) -> String {
    lexical_features(text)
        .into_iter()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn relevance_bps(query_features: &BTreeSet<String>, candidate: &str) -> u64 {
    relevance_from_features(query_features, &lexical_features(candidate))
}

pub fn relevance_from_features(
    query_features: &BTreeSet<String>,
    candidate_features: &BTreeSet<String>,
) -> u64 {
    if query_features.is_empty() {
        return 10_000;
    }
    let matched =
        u64::try_from(query_features.intersection(candidate_features).count()).unwrap_or(u64::MAX);
    matched
        .saturating_mul(10_000)
        .checked_div(u64::try_from(query_features.len()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
