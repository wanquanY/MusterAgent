use super::*;
use crate::features::{query_search_features, relevance_bps};

fn candidate(content: &str, semantic_similarity_bps: u16) -> RankingCandidate {
    RankingCandidate {
        content: content.into(),
        semantic_similarity_bps,
    }
}

#[test]
fn ranking_keeps_semantic_only_matches_and_diversifies_duplicate_bodies() {
    let candidates = vec![
        candidate("rust memory", 0),
        candidate("rust memory", 0),
        candidate("safe ownership", 9_000),
        candidate("calendar reminders", 0),
    ];
    let ranked = rank(Some("rust memory"), &candidates, 3).expect("rank verified facts");
    assert_eq!(
        ranked.iter().map(|s| s.candidate_index).collect::<Vec<_>>(),
        [0, 2, 1]
    );
    assert_eq!(ranked[1].lexical_relevance_bps, 0);
    assert_eq!(ranked[1].semantic_similarity_bps, 9_000);
    assert_eq!(ranked[1].relevance_bps, 9_000);
}

#[test]
fn list_preserves_source_order_and_ties_have_stable_order() {
    let candidates = vec![
        candidate("same", 0),
        candidate("same", 0),
        candidate("same", 0),
    ];
    for query in [None, Some("same")] {
        let ranked = rank(query, &candidates, 2).expect("rank");
        assert_eq!(
            ranked.iter().map(|s| s.candidate_index).collect::<Vec<_>>(),
            [0, 1]
        );
    }
}

#[test]
fn relevance_thresholds_do_not_invent_recall_for_an_unrelated_query() {
    let candidates = vec![candidate("calendar", 6_499), candidate("calendar", 6_500)];
    let ranked = rank(Some("rust"), &candidates, 2).expect("rank");
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].candidate_index, 1);
    assert_eq!(ranked[0].relevance_bps, 6_500);
    assert!(
        rank(Some("rust"), &candidates[..1], 2)
            .expect("rank")
            .is_empty()
    );
}

#[test]
fn bounded_requests_fail_explicitly_instead_of_truncating_or_empty_success() {
    for query in ["", " ", "!?", "a"] {
        assert!(rank(Some(query), &[], 1).is_err());
    }
    assert!(rank(Some(&"a".repeat(MAX_QUERY_BYTES + 1)), &[], 1).is_err());
    assert!(rank(None, &[], 0).is_err());
    assert!(rank(None, &[], 101).is_err());
    assert!(rank(None, &[candidate("ok", 10_001)], 1).is_err());
    assert!(
        rank(
            None,
            &[candidate(&"a".repeat(MAX_CANDIDATE_BYTES + 1), 0)],
            1
        )
        .is_err()
    );
    let candidates = (0..201).map(|_| candidate("ok", 0)).collect::<Vec<_>>();
    assert!(rank(None, &candidates, 1).is_err());
    assert_eq!(
        rank(None, &candidates[..200], 100)
            .expect("bounded union")
            .len(),
        100
    );
}

#[test]
fn selection_wire_cannot_supply_content_identity_or_authority() {
    for field in [
        "content",
        "memory_id",
        "tenant_id",
        "approved",
        "status",
        "content_ref",
    ] {
        let mut wire = serde_json::json!({
            "candidate_index": 0, "lexical_relevance_bps": 10000,
            "semantic_similarity_bps": 0, "relevance_bps": 10000
        });
        wire[field] = serde_json::json!("forged");
        assert!(serde_json::from_value::<RankedSelection>(wire).is_err());
    }
}

#[test]
fn query_features_avoid_incidental_latin_bigrams_but_keep_chinese_bigrams() {
    assert_eq!(
        query_search_features("Database migration"),
        "database migration"
    );
    assert_eq!(
        query_search_features("数据库迁移"),
        "库迁 据库 数据 数据库迁移 迁移"
    );
}

#[test]
fn lexical_relevance_handles_chinese_and_case_without_provider_tokenizers() {
    let chinese = lexical_features("继续完善长期记忆检索");
    assert!(
        relevance_bps(&chinese, "长期记忆应当按当前任务相关性检索")
            > relevance_bps(&chinese, "桌面窗口尺寸与颜色设置")
    );

    let english = lexical_features("Postgres memory publication");
    assert!(
        relevance_bps(&english, "MEMORY publication is atomic in PostgreSQL")
            > relevance_bps(&english, "calendar reminder")
    );
}

#[test]
fn hybrid_fusion_boosts_agreement_without_weakening_a_strong_lane() {
    assert_eq!(fused_relevance(8_000, 0), 8_000);
    assert_eq!(fused_relevance(0, 8_000), 8_000);
    assert_eq!(fused_relevance(8_000, 7_000), 9_400);
    assert_eq!(fused_relevance(9_500, 9_000), 10_000);
}

#[test]
fn jaccard_similarity_detects_duplicate_content_features() {
    let original = lexical_features("postgres migration role ownership");
    let duplicate = lexical_features("postgres migration role ownership");
    let unrelated = lexical_features("calendar reminder meeting");

    assert_eq!(jaccard_similarity_bps(&original, &duplicate), 10_000);
    assert!(jaccard_similarity_bps(&original, &unrelated) < 2_000);
}
