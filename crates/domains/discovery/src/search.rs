//! Deterministic weighted lexical indexing and ranking for Tool and Skill catalogs.
//!
//! # Field Weights
//!
//! Search indexes four logical fields per catalog entry with super-increasing
//! weights (`key/name > tags > when_to_use > description`):
//!
//! - [`KEY_NAME_WEIGHT`] (`12`): canonical key (`tool_ref` / `skill_ref`),
//!   wire/model name, and human-readable `title`.
//! - [`TAGS_WEIGHT`] (`6`): optional descriptive `tags`.
//! - [`WHEN_TO_USE_WEIGHT`] (`3`): `when_to_use` guidance text.
//! - [`DESCRIPTION_WEIGHT`] (`1`): general `description` text.
//!
//! Because `12 > 6 + 3 + 1` and `6 > 3 + 1`, a match in a higher-priority field
//! strictly outranks an equivalent match even if repeated across every
//! lower-priority field.
//!
//! # Tokenization
//!
//! - **ASCII / non-CJK alphanumeric runs**: lowercased (with fullwidth ASCII
//!   folded to halfwidth) and split on whitespace, punctuation, and script
//!   transitions (`查询A股` and `查询 A 股` produce identical runs).
//! - **CJK runs**: character bigrams for runs of length `>= 2`, plus single
//!   character unigram matching for 1-character CJK runs (such as `股` in `A股`)
//!   and unigram fallback whenever no bigram of a multi-character CJK query run
//!   matches in the candidate entry.

use std::collections::BTreeSet;

/// Weight applied to matches in key, wire/model name, or title (`12`).
pub const KEY_NAME_WEIGHT: usize = 12;
/// Weight applied to matches in descriptive `tags` (`6`).
pub const TAGS_WEIGHT: usize = 6;
/// Weight applied to matches in `when_to_use` guidance (`3`).
pub const WHEN_TO_USE_WEIGHT: usize = 3;
/// Weight applied to matches in `description` text (`1`).
pub const DESCRIPTION_WEIGHT: usize = 1;

const EXACT_WORD_BASE: usize = 160;
const PREFIX_WORD_BASE: usize = 112;
const SUBSTRING_WORD_BASE: usize = 80;
const CJK_SINGLE_BASE: usize = 128;
const CJK_BIGRAM_BASE: usize = 160;
const CJK_CONTIGUOUS_RUN_BONUS: usize = 64;
const CJK_UNIGRAM_FALLBACK_BASE: usize = 1;
const MULTI_UNIT_COVERAGE_BONUS: usize = 160;
const PHRASE_MATCH_BONUS: usize = 48;
const EXACT_IDENTITY_BONUS: usize = 64;

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub(crate) struct FieldIndex {
    phrases: BTreeSet<String>,
    words: BTreeSet<String>,
    cjk_bigrams: BTreeSet<(char, char)>,
    cjk_unigrams: BTreeSet<char>,
}

impl FieldIndex {
    fn from_segments<'a>(segments: impl IntoIterator<Item = &'a str>) -> Self {
        let mut field = Self::default();
        for segment_text in segments {
            let runs = segment(segment_text);
            if runs.is_empty() {
                continue;
            }
            field.phrases.insert(join_runs(&runs));
            for run in runs {
                match run {
                    Run::Word(word) => {
                        field.words.insert(word);
                    }
                    Run::Cjk(chars) => {
                        for &ch in &chars {
                            field.cjk_unigrams.insert(ch);
                        }
                        for pair in chars.windows(2) {
                            field.cjk_bigrams.insert((pair[0], pair[1]));
                        }
                    }
                }
            }
        }
        field
    }

    fn contains_phrase(&self, needle: &str) -> bool {
        self.phrases.iter().any(|phrase| phrase.contains(needle))
    }

    fn word_quality(&self, word: &str) -> usize {
        if self.words.contains(word) {
            EXACT_WORD_BASE
        } else if word.len() >= 3 && self.words.iter().any(|cand| cand.starts_with(word)) {
            PREFIX_WORD_BASE
        } else if word.len() >= 3 && self.words.iter().any(|cand| cand.contains(word)) {
            SUBSTRING_WORD_BASE
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DocumentSearchIndex {
    exact_identities: BTreeSet<String>,
    key_name: FieldIndex,
    tags: FieldIndex,
    when_to_use: FieldIndex,
    description: FieldIndex,
}

impl DocumentSearchIndex {
    pub(crate) fn new<'a>(
        identities: impl IntoIterator<Item = &'a str>,
        tags: impl IntoIterator<Item = &'a str>,
        when_to_use: Option<&'a str>,
        description: &'a str,
    ) -> Self {
        let identity_list: Vec<&'a str> = identities.into_iter().collect();
        let mut exact_identities = BTreeSet::new();
        for &raw in &identity_list {
            let normalized = canonical_query(raw);
            if !normalized.is_empty() {
                exact_identities.insert(normalized);
            }
        }
        Self {
            exact_identities,
            key_name: FieldIndex::from_segments(identity_list),
            tags: FieldIndex::from_segments(tags),
            when_to_use: FieldIndex::from_segments(when_to_use),
            description: FieldIndex::from_segments([description]),
        }
    }

    fn weighted_fields(&self) -> [(&FieldIndex, usize); 4] {
        [
            (&self.key_name, KEY_NAME_WEIGHT),
            (&self.tags, TAGS_WEIGHT),
            (&self.when_to_use, WHEN_TO_USE_WEIGHT),
            (&self.description, DESCRIPTION_WEIGHT),
        ]
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ParsedQuery {
    canonical: String,
    run_count: usize,
    units: Vec<QueryUnit>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum QueryUnit {
    Word(String),
    CjkSingle(char),
    CjkRun {
        text: String,
        bigrams: Vec<(char, char)>,
        unigrams: Vec<char>,
    },
}

impl ParsedQuery {
    pub(crate) fn parse(raw: &str) -> Self {
        let runs = segment(raw);
        let canonical = join_runs(&runs);
        let run_count = runs.len();
        let mut seen_words = BTreeSet::new();
        let mut seen_singles = BTreeSet::new();
        let mut seen_cjk_runs = BTreeSet::new();
        let mut units = Vec::with_capacity(runs.len());

        for run in runs {
            match run {
                Run::Word(word) => {
                    if seen_words.insert(word.clone()) {
                        units.push(QueryUnit::Word(word));
                    }
                }
                Run::Cjk(chars) if chars.len() == 1 => {
                    let ch = chars[0];
                    if seen_singles.insert(ch) {
                        units.push(QueryUnit::CjkSingle(ch));
                    }
                }
                Run::Cjk(chars) => {
                    let text: String = chars.iter().collect();
                    if seen_cjk_runs.insert(text.clone()) {
                        let mut seen_bg = BTreeSet::new();
                        let mut bigrams = Vec::with_capacity(chars.len().saturating_sub(1));
                        for pair in chars.windows(2) {
                            let bg = (pair[0], pair[1]);
                            if seen_bg.insert(bg) {
                                bigrams.push(bg);
                            }
                        }
                        let mut seen_ug = BTreeSet::new();
                        let mut unigrams = Vec::with_capacity(chars.len());
                        for ch in chars {
                            if seen_ug.insert(ch) {
                                unigrams.push(ch);
                            }
                        }
                        units.push(QueryUnit::CjkRun {
                            text,
                            bigrams,
                            unigrams,
                        });
                    }
                }
            }
        }

        Self {
            canonical,
            run_count,
            units,
        }
    }

    pub(crate) fn canonical(&self) -> &str {
        &self.canonical
    }

    pub(crate) fn score(&self, doc: &DocumentSearchIndex) -> usize {
        if self.units.is_empty() {
            return 1;
        }
        let fields = doc.weighted_fields();
        let has_non_stopword = self
            .units
            .iter()
            .any(|unit| !matches!(unit, QueryUnit::Word(w) if is_ascii_stopword(w)));
        let mut content_total = 0usize;
        let mut stopword_total = 0usize;
        let mut matched_content_units = 0usize;
        let mut matched_stopword_units = 0usize;

        for unit in &self.units {
            match unit {
                QueryUnit::Word(word) => {
                    let mut unit_score = 0usize;
                    for (field, weight) in fields {
                        let quality = field.word_quality(word);
                        unit_score = unit_score.saturating_add(quality.saturating_mul(weight));
                    }
                    if unit_score > 0 {
                        if has_non_stopword && is_ascii_stopword(word) {
                            matched_stopword_units = matched_stopword_units.saturating_add(1);
                            stopword_total = stopword_total.saturating_add(unit_score);
                        } else {
                            matched_content_units = matched_content_units.saturating_add(1);
                            content_total = content_total.saturating_add(unit_score);
                        }
                    }
                }
                QueryUnit::CjkSingle(ch) => {
                    let mut unit_score = 0usize;
                    for (field, weight) in fields {
                        if field.cjk_unigrams.contains(ch) {
                            unit_score =
                                unit_score.saturating_add(CJK_SINGLE_BASE.saturating_mul(weight));
                        }
                    }
                    if unit_score > 0 {
                        matched_content_units = matched_content_units.saturating_add(1);
                        content_total = content_total.saturating_add(unit_score);
                    }
                }
                QueryUnit::CjkRun {
                    text,
                    bigrams,
                    unigrams,
                } => {
                    let doc_has_bigram = fields
                        .iter()
                        .any(|(field, _)| bigrams.iter().any(|bg| field.cjk_bigrams.contains(bg)));
                    if doc_has_bigram {
                        matched_content_units = matched_content_units.saturating_add(1);
                        for (field, weight) in fields {
                            let hits = bigrams
                                .iter()
                                .filter(|bg| field.cjk_bigrams.contains(bg))
                                .count();
                            if hits > 0 {
                                let contiguous = if field.contains_phrase(text) {
                                    CJK_CONTIGUOUS_RUN_BONUS
                                } else {
                                    0
                                };
                                let quality = hits
                                    .saturating_mul(CJK_BIGRAM_BASE)
                                    .saturating_add(contiguous);
                                content_total =
                                    content_total.saturating_add(quality.saturating_mul(weight));
                            }
                        }
                    } else {
                        let min_unigrams = unigrams.len().min(2);
                        let doc_unigram_hits = unigrams
                            .iter()
                            .filter(|ch| {
                                fields
                                    .iter()
                                    .any(|(field, _)| field.cjk_unigrams.contains(ch))
                            })
                            .count();
                        if doc_unigram_hits >= min_unigrams {
                            for (field, weight) in fields {
                                let hits = unigrams
                                    .iter()
                                    .filter(|ch| field.cjk_unigrams.contains(ch))
                                    .count();
                                if hits > 0 {
                                    let quality = hits.saturating_mul(CJK_UNIGRAM_FALLBACK_BASE);
                                    content_total = content_total
                                        .saturating_add(quality.saturating_mul(weight));
                                }
                            }
                        }
                    }
                }
            }
        }

        if content_total == 0 {
            return 0;
        }

        let mut total = content_total.saturating_add(stopword_total);
        let matched_primary_units = matched_content_units.saturating_add(matched_stopword_units);
        if matched_primary_units >= 2 {
            total = total.saturating_add(
                matched_primary_units
                    .saturating_sub(1)
                    .saturating_mul(MULTI_UNIT_COVERAGE_BONUS),
            );
        }
        if self.run_count >= 2 {
            for (field, weight) in fields {
                if field.contains_phrase(&self.canonical) {
                    total = total.saturating_add(PHRASE_MATCH_BONUS.saturating_mul(weight));
                }
            }
        }
        if doc.exact_identities.contains(&self.canonical) {
            total = total.saturating_add(EXACT_IDENTITY_BONUS.saturating_mul(KEY_NAME_WEIGHT));
        }

        total
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum Run {
    Word(String),
    Cjk(Vec<char>),
}

pub(crate) fn canonical_query(value: &str) -> String {
    join_runs(&segment(value))
}

fn join_runs(runs: &[Run]) -> String {
    let mut out = String::new();
    for (idx, run) in runs.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        match run {
            Run::Word(word) => out.push_str(word),
            Run::Cjk(chars) => out.extend(chars),
        }
    }
    out
}

fn segment(value: &str) -> Vec<Run> {
    let mut runs = Vec::new();
    let mut current_word = String::new();
    let mut current_cjk = Vec::new();

    let flush_word = |word: &mut String, out: &mut Vec<Run>| {
        if !word.is_empty() {
            out.push(Run::Word(std::mem::take(word)));
        }
    };
    let flush_cjk = |cjk: &mut Vec<char>, out: &mut Vec<Run>| {
        if !cjk.is_empty() {
            out.push(Run::Cjk(std::mem::take(cjk)));
        }
    };

    for raw_ch in value.chars() {
        let ch = fold_fullwidth_ascii(raw_ch);
        if is_cjk(ch) {
            flush_word(&mut current_word, &mut runs);
            current_cjk.push(ch);
        } else if ch.is_alphanumeric() {
            flush_cjk(&mut current_cjk, &mut runs);
            for lower in ch.to_lowercase() {
                current_word.push(lower);
            }
        } else {
            flush_word(&mut current_word, &mut runs);
            flush_cjk(&mut current_cjk, &mut runs);
        }
    }
    flush_word(&mut current_word, &mut runs);
    flush_cjk(&mut current_cjk, &mut runs);
    runs
}

const fn fold_fullwidth_ascii(ch: char) -> char {
    let code = ch as u32;
    if code >= 0xFF10 && code <= 0xFF19 {
        // Fullwidth '0'..'9'
        if let Some(folded) = char::from_u32(code - 0xFF10 + ('0' as u32)) {
            return folded;
        }
    } else if code >= 0xFF21 && code <= 0xFF3A {
        // Fullwidth 'A'..'Z' -> lowercase 'a'..'z'
        if let Some(folded) = char::from_u32(code - 0xFF21 + ('a' as u32)) {
            return folded;
        }
    } else if code >= 0xFF41 && code <= 0xFF5A {
        // Fullwidth 'a'..'z'
        if let Some(folded) = char::from_u32(code - 0xFF41 + ('a' as u32)) {
            return folded;
        }
    }
    ch
}

const fn is_ascii_stopword(word: &str) -> bool {
    matches!(
        word.as_bytes(),
        b"a" | b"an"
            | b"and"
            | b"are"
            | b"as"
            | b"at"
            | b"be"
            | b"by"
            | b"for"
            | b"from"
            | b"in"
            | b"into"
            | b"is"
            | b"it"
            | b"of"
            | b"on"
            | b"or"
            | b"the"
            | b"to"
            | b"with"
    )
}

const fn is_cjk(ch: char) -> bool {
    matches!(
        ch,
        '\u{2E80}'..='\u{2FDF}'     // CJK Radicals Supplement & Kangxi Radicals
        | '\u{3041}'..='\u{3096}'   // Hiragana
        | '\u{30A1}'..='\u{30FA}'   // Katakana (excluding U+30FB middle dot)
        | '\u{30FC}'..='\u{30FF}'   // Katakana prolonged sound mark & digraphs
        | '\u{3100}'..='\u{312F}'   // Bopomofo
        | '\u{31F0}'..='\u{31FF}'   // Katakana Phonetic Extensions
        | '\u{3400}'..='\u{4DBF}'   // CJK Unified Ideographs Extension A
        | '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
        | '\u{AC00}'..='\u{D7AF}'   // Hangul Syllables
        | '\u{F900}'..='\u{FAFF}'   // CJK Compatibility Ideographs
        | '\u{20000}'..='\u{2EE5F}' // CJK Unified Ideographs Extensions B..I
        | '\u{2F800}'..='\u{2FA1F}' // CJK Compatibility Ideographs Supplement
        | '\u{30000}'..='\u{323AF}' // CJK Unified Ideographs Extension J
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_weights_are_strictly_super_increasing() {
        const {
            assert!(KEY_NAME_WEIGHT > TAGS_WEIGHT + WHEN_TO_USE_WEIGHT + DESCRIPTION_WEIGHT);
            assert!(TAGS_WEIGHT > WHEN_TO_USE_WEIGHT + DESCRIPTION_WEIGHT);
            assert!(WHEN_TO_USE_WEIGHT > DESCRIPTION_WEIGHT);
            assert!(DESCRIPTION_WEIGHT > 0);
        }
    }

    #[test]
    fn mixed_script_segmentation_splits_cjk_and_ascii_without_spaces() {
        assert_eq!(
            canonical_query("查询A股个股实时行情"),
            "查询 a 股个股实时行情"
        );
        assert_eq!(
            canonical_query("查询 A 股个股实时行情"),
            "查询 a 股个股实时行情"
        );
        assert_eq!(canonical_query("查询Ａ股ETF行情"), "查询 a 股 etf 行情");
    }

    #[test]
    fn cjk_bigrams_match_non_contiguous_phrases_and_unigrams_fallback() {
        let doc = DocumentSearchIndex::new(
            ["market.quote@1", "market_quote", "Market Quote"],
            [],
            None,
            "查询 A 股个股实时行情",
        );
        assert!(ParsedQuery::parse("个股行情").score(&doc) > 0);
        assert!(ParsedQuery::parse("A股行情").score(&doc) > 0);
        assert!(ParsedQuery::parse("股").score(&doc) > 0);

        let unigram_only = DocumentSearchIndex::new(
            ["market.price@1", "market_price", "Market Price"],
            [],
            None,
            "查询 A 股实时价格",
        );
        let bigram_exact = DocumentSearchIndex::new(
            ["market.spot@1", "market_spot", "Market Spot"],
            [],
            None,
            "查询实时股价",
        );
        let q = ParsedQuery::parse("股价");
        let fallback_score = q.score(&unigram_only);
        let bigram_score = q.score(&bigram_exact);
        assert!(fallback_score > 0);
        assert!(bigram_score > fallback_score);
    }
}
