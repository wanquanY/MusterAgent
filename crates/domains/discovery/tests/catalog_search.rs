use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{
    AuthorizedSkillCatalog, AuthorizedSkillDescriptor, AuthorizedToolCatalog,
    AuthorizedToolDescriptor, DiscoveryContractError, SkillAvailability, SkillCatalogActivation,
    SkillCatalogInstallScope, SkillCatalogLifecycle, SkillCatalogProvenance, SkillCatalogReference,
    SkillCatalogReferenceKind, SkillCatalogSearchRequest, SkillCatalogTrust, SkillOrigin,
    ToolCatalogSearchRequest,
    search::{DESCRIPTION_WEIGHT, KEY_NAME_WEIGHT, TAGS_WEIGHT, WHEN_TO_USE_WEIGHT},
};
use musteragent_identity::{
    CapabilityProviderId, ContentId, SkillId, SkillVersionId, TenantId, VersionedKey,
};
use musteragent_tool::{ToolKey, WireName};

fn make_tool(
    key: &str,
    title: &str,
    description: &str,
    tags: &[&str],
    when_to_use: Option<&str>,
    location: CapabilityLocation,
) -> AuthorizedToolDescriptor {
    let schema_bytes = format!(
        "{{\"additionalProperties\":false,\"properties\":{{}},\"title\":{:?},\"type\":\"object\"}}",
        key
    )
    .into_bytes();
    let provider_id: CapabilityProviderId = match location {
        CapabilityLocation::Backend => "019fa6de-38fd-7000-8000-000000000402",
        _ => "019fa6de-38fd-7000-8000-000000000401",
    }
    .parse()
    .expect("provider id");
    AuthorizedToolDescriptor {
        provided_capability: None,
        tool_ref: ToolKey::from_versioned_key(VersionedKey::new(key).expect("tool key")),
        capability_ref: VersionedKey::new(key).expect("capability key"),
        wire_name: WireName::new(key.replace(['.', '@'], "_")).expect("wire name"),
        title: title.to_owned().into(),
        description: description.to_owned().into(),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
        when_to_use: when_to_use.map(str::to_owned),
        schema_hash: Sha256::digest(&schema_bytes),
        schema: schema_bytes.into(),
        provider_id,
        location,
        required_contexts: BTreeSet::new(),
    }
}

fn make_skill(
    reference: &str,
    title: &str,
    description: &str,
    when_to_use: &str,
    tags: &[&str],
) -> AuthorizedSkillDescriptor {
    let instructions = format!("# {title}\n\n{description}").into_bytes();
    let hash = Sha256::digest(&instructions);
    let instructions_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/markdown;charset=utf-8").expect("media type"),
        byte_length: u64::try_from(instructions.len()).expect("length"),
        sha256: hash,
    };
    let manifest_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: 32,
        sha256: Sha256::digest(b"manifest"),
    };
    let package_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/zip").expect("media type"),
        byte_length: 64,
        sha256: Sha256::digest(b"package"),
    };
    AuthorizedSkillDescriptor {
        skill_id: SkillId::generate(),
        skill_version_id: SkillVersionId::generate(),
        skill_ref: VersionedKey::new(reference).expect("skill ref"),
        title: title.to_owned(),
        description: description.to_owned(),
        when_to_use: when_to_use.to_owned(),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
        instructions_ref: instructions_ref.clone(),
        instructions_hash: hash,
        required_tools: BTreeSet::from([
            VersionedKey::new("artifact.note.create@1").expect("tool ref")
        ]),
        optional_tools: BTreeSet::new(),
        compatible_agent_contracts: BTreeSet::from([VersionedKey::new(
            "musteragent.legacy.runtime-contract@1",
        )
        .expect("contract")]),
        compatible_surfaces: BTreeSet::from(["web".to_owned()]),
        activation: SkillCatalogActivation::ExplicitOrResolved,
        lifecycle: SkillCatalogLifecycle::Active,
        origin: SkillOrigin::Installed,
        trust: SkillCatalogTrust::Verified,
        provenance: SkillCatalogProvenance::Installed {
            source: "marketplace".to_owned(),
            source_ref: format!("market://{reference}"),
            publisher_ref: "publisher:test".to_owned(),
        },
        install_scope: SkillCatalogInstallScope::Tenant(TenantId::generate()),
        spec_hash: manifest_ref.sha256,
        references: vec![
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Manifest,
                content_ref: manifest_ref,
            },
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Instructions,
                content_ref: instructions_ref,
            },
            SkillCatalogReference {
                kind: SkillCatalogReferenceKind::Package,
                content_ref: package_ref,
            },
        ],
        availability: SkillAvailability::Available,
    }
}

fn search_tools(catalog: &AuthorizedToolCatalog, query: &str) -> Vec<String> {
    catalog
        .search(&ToolCatalogSearchRequest {
            query: query.to_owned(),
            locations: BTreeSet::new(),
            limit: NonZeroUsize::new(20).expect("limit"),
            cursor: None,
        })
        .expect("tool search")
        .items
        .into_iter()
        .map(|item| item.tool_ref.as_str().to_owned())
        .collect()
}

fn search_skills(catalog: &AuthorizedSkillCatalog, query: &str) -> Vec<String> {
    catalog
        .search(&SkillCatalogSearchRequest {
            query: query.to_owned(),
            limit: NonZeroUsize::new(20).expect("limit"),
            cursor: None,
        })
        .expect("skill search")
        .items
        .into_iter()
        .map(|item| item.skill_ref.as_str().to_owned())
        .collect()
}

#[test]
fn chinese_only_queries_match_segmented_bigrams_and_unigram_fallback() {
    let catalog = AuthorizedToolCatalog::new(vec![
        make_tool(
            "market.quote.get@1",
            "Market Quote",
            "查询 A 股个股实时行情",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
        make_tool(
            "market.price.snapshot@1",
            "Price Snapshot",
            "查询 A 股实时价格",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
        make_tool(
            "calendar.event.list@1",
            "Calendar Events",
            "查询个人日程安排",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
    ])
    .expect("catalog");

    // Non-contiguous Chinese phrase (`个股` + `行情` separated by `实时` in description)
    // matches via CJK bigrams and ranks ahead of unrelated single-character overlap (`个`).
    let results = search_tools(&catalog, "个股行情");
    assert_eq!(results[0], "market.quote.get@1");

    // Pure unigram fallback: `股价` has no matching bigram in `查询 A 股实时价格`,
    // but both `'股'` and `'价'` match via unigram fallback, while `个人日程安排` has zero overlap.
    let fallback_results = search_tools(&catalog, "股价");
    assert_eq!(fallback_results, vec!["market.price.snapshot@1"]);

    // Single-character CJK query (`股`) matches tools containing `'股'` and excludes unrelated entries.
    let single_char = search_tools(&catalog, "股");
    assert_eq!(
        single_char,
        vec!["market.price.snapshot@1", "market.quote.get@1"]
    );

    // Bigram match strictly outranks unigram-only fallback.
    let with_exact_bigram = AuthorizedToolCatalog::new(vec![
        make_tool(
            "market.price.snapshot@1",
            "股票实时价格",
            "查询股票实时价格",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
        make_tool(
            "market.spot.quote@1",
            "Spot Quote",
            "获取最新股价",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
    ])
    .expect("bigram vs unigram catalog");
    assert_eq!(
        search_tools(&with_exact_bigram, "股价"),
        vec!["market.spot.quote@1", "market.price.snapshot@1"]
    );
}

#[test]
fn mixed_cjk_and_ascii_queries_work_with_or_without_spaces() {
    let catalog = AuthorizedToolCatalog::new(vec![
        make_tool(
            "market.ashare.quote@1",
            "Equity Quote",
            "查询 A 股个股实时行情",
            &["etf"],
            None,
            CapabilityLocation::Backend,
        ),
        make_tool(
            "market.hk.quote@1",
            "Regional Quote",
            "查询港股个股实时行情",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
        make_tool(
            "image.generate@1",
            "Generate image",
            "Generate an illustration from a text prompt",
            &[],
            None,
            CapabilityLocation::Backend,
        ),
    ])
    .expect("catalog");

    for query in ["A股行情", "A 股行情", "Ａ股行情"] {
        let keys = search_tools(&catalog, query);
        assert_eq!(keys[0], "market.ashare.quote@1", "query={query}");
        // Single-letter ASCII 'a' must not substring-match "image" or "generate".
        assert!(
            !keys.contains(&"image.generate@1".to_owned()),
            "unexpected English substring match for {query}: {keys:?}"
        );
    }

    let etf = search_tools(&catalog, "ETF实时行情");
    assert_eq!(etf[0], "market.ashare.quote@1");
}

#[test]
fn tag_only_and_when_to_use_only_matches_succeed_for_tools_and_skills() {
    let tools = AuthorizedToolCatalog::new(vec![
        make_tool(
            "data.series.compute@1",
            "Series Compute",
            "Compute numeric series summaries",
            &["量化回测", "backtest"],
            None,
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "table.export.run@1",
            "Export Table",
            "Write structured rows to an output file",
            &[],
            Some("当用户需要导出财报表格或 CSV 清单时使用"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "system.noop@1",
            "Noop",
            "Perform no operation",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
    ])
    .expect("tool catalog");

    assert_eq!(
        search_tools(&tools, "量化回测"),
        vec!["data.series.compute@1"]
    );
    assert_eq!(
        search_tools(&tools, "backtest"),
        vec!["data.series.compute@1"]
    );
    assert_eq!(search_tools(&tools, "财报表格"), vec!["table.export.run@1"]);
    assert_eq!(search_tools(&tools, "csv 清单"), vec!["table.export.run@1"]);

    let skills = AuthorizedSkillCatalog::new(vec![
        make_skill(
            "workflow-alpha@1",
            "Workflow Alpha",
            "General document preparation workflow.",
            "Apply when assembling neutral summaries.",
            &["合规审查", "compliance"],
        ),
        make_skill(
            "workflow-beta@1",
            "Workflow Beta",
            "General numeric check workflow.",
            "当用户请求核对季度对账单时使用。",
            &[],
        ),
    ])
    .expect("skill catalog");

    assert_eq!(search_skills(&skills, "合规审查"), vec!["workflow-alpha@1"]);
    assert_eq!(
        search_skills(&skills, "compliance"),
        vec!["workflow-alpha@1"]
    );
    assert_eq!(
        search_skills(&skills, "季度对账单"),
        vec!["workflow-beta@1"]
    );
}

#[test]
fn field_weights_rank_key_name_above_tags_above_when_to_use_above_description() {
    const {
        assert!(KEY_NAME_WEIGHT > TAGS_WEIGHT);
        assert!(TAGS_WEIGHT > WHEN_TO_USE_WEIGHT);
        assert!(WHEN_TO_USE_WEIGHT > DESCRIPTION_WEIGHT);
    }

    // Order keys inversely to expected rank (`z`..`a`) so tie-breaking would
    // reverse the result if weights were equal.
    let tools = AuthorizedToolCatalog::new(vec![
        make_tool(
            "d.entry@1",
            "Generic Entry D",
            "Supports 资产配置 analysis in detail",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "c.entry@1",
            "Generic Entry C",
            "General helper",
            &[],
            Some("当用户询问资产配置方案时调用"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "b.entry@1",
            "Generic Entry B",
            "Supports 资产配置 in description too",
            &["资产配置"],
            Some("也可用于资产配置"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "z.entry@1",
            "资产配置工具",
            "General helper",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
    ])
    .expect("weighted tool catalog");

    assert_eq!(
        search_tools(&tools, "资产配置"),
        vec!["z.entry@1", "b.entry@1", "c.entry@1", "d.entry@1"]
    );

    let ascii_tools = AuthorizedToolCatalog::new(vec![
        make_tool(
            "a.desc@1",
            "Alpha Helper",
            "Inspects portfolio exposures",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "b.when@1",
            "Beta Helper",
            "Inspects exposures",
            &[],
            Some("Use when rebalancing a portfolio"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "c.tags@1",
            "Gamma Helper",
            "Inspects portfolio exposures",
            &["portfolio"],
            Some("Use when rebalancing a portfolio"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "z.name@1",
            "Portfolio Inspector",
            "Inspects exposures",
            &[],
            None,
            CapabilityLocation::Cloud,
        ),
    ])
    .expect("weighted ascii tool catalog");

    assert_eq!(
        search_tools(&ascii_tools, "portfolio"),
        vec!["z.name@1", "c.tags@1", "b.when@1", "a.desc@1"]
    );

    let skills = AuthorizedSkillCatalog::new(vec![
        make_skill(
            "a-desc@1",
            "Routine A",
            "Performs ledger reconciliation steps.",
            "Use for periodic review.",
            &[],
        ),
        make_skill(
            "b-when@1",
            "Routine B",
            "Performs periodic steps.",
            "Use when running reconciliation checks.",
            &[],
        ),
        make_skill(
            "c-tags@1",
            "Routine C",
            "Performs reconciliation steps.",
            "Use when running reconciliation checks.",
            &["reconciliation"],
        ),
        make_skill(
            "z-name@1",
            "Reconciliation Guide",
            "Performs periodic steps.",
            "Use for periodic review.",
            &[],
        ),
    ])
    .expect("weighted skill catalog");

    assert_eq!(
        search_skills(&skills, "reconciliation"),
        vec!["z-name@1", "c-tags@1", "b-when@1", "a-desc@1"]
    );
}

#[test]
fn equal_scores_break_ties_deterministically_by_canonical_key() {
    let defs = vec![
        make_tool(
            "gamma.item@1",
            "Neutral Gamma",
            "查询实时气象指标",
            &["气象"],
            Some("需要气象数据时调用"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "alpha.item@1",
            "Neutral Alpha",
            "查询实时气象指标",
            &["气象"],
            Some("需要气象数据时调用"),
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "beta.item@1",
            "Neutral Beta",
            "查询实时气象指标",
            &["气象"],
            Some("需要气象数据时调用"),
            CapabilityLocation::Cloud,
        ),
    ];
    let forward = AuthorizedToolCatalog::new(defs.clone()).expect("forward");
    let reverse = AuthorizedToolCatalog::new(defs.into_iter().rev().collect()).expect("reverse");

    let expected = vec!["alpha.item@1", "beta.item@1", "gamma.item@1"];
    assert_eq!(search_tools(&forward, "气象指标"), expected);
    assert_eq!(search_tools(&reverse, "气象指标"), expected);

    // Cursor pagination walks the exact deterministic tie-break sequence.
    let mut paged = Vec::new();
    let mut cursor = None;
    loop {
        let page = forward
            .search(&ToolCatalogSearchRequest {
                query: "气象指标".to_owned(),
                locations: BTreeSet::new(),
                limit: NonZeroUsize::new(1).expect("limit 1"),
                cursor,
            })
            .expect("page");
        paged.extend(
            page.items
                .into_iter()
                .map(|i| i.tool_ref.as_str().to_owned()),
        );
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(paged, expected);
}

#[test]
fn search_never_returns_unauthorized_entries_and_preserves_location_and_limit_bounds() {
    let admitted = AuthorizedToolCatalog::new(vec![
        make_tool(
            "authorized.cloud@1",
            "Cloud Quote",
            "查询个股行情",
            &["行情"],
            None,
            CapabilityLocation::Cloud,
        ),
        make_tool(
            "authorized.backend@1",
            "个股行情服务",
            "查询个股行情",
            &["个股行情"],
            Some("查询个股行情"),
            CapabilityLocation::Backend,
        ),
    ])
    .expect("admitted catalog");

    // Unauthorized tool identities cannot be injected via semantic scores.
    let injected = BTreeMap::from([("secret.unauthorized@1".to_owned(), 999)]);
    assert_eq!(
        admitted.search_with_semantic_scores(
            &ToolCatalogSearchRequest {
                query: "个股行情".to_owned(),
                locations: BTreeSet::new(),
                limit: NonZeroUsize::new(10).expect("limit"),
                cursor: None,
            },
            &injected,
        ),
        Err(DiscoveryContractError::ToolNotAuthorized)
    );

    // Location filter restricts results strictly to Cloud even though Backend has a higher score.
    let cloud_only = admitted
        .search(&ToolCatalogSearchRequest {
            query: "个股行情".to_owned(),
            locations: BTreeSet::from([CapabilityLocation::Cloud]),
            limit: NonZeroUsize::new(10).expect("limit"),
            cursor: None,
        })
        .expect("cloud search");
    assert_eq!(cloud_only.items.len(), 1);
    assert_eq!(cloud_only.items[0].tool_ref.as_str(), "authorized.cloud@1");

    // Limit bound is strictly respected.
    let limited = admitted
        .search(&ToolCatalogSearchRequest {
            query: "个股行情".to_owned(),
            locations: BTreeSet::new(),
            limit: NonZeroUsize::new(1).expect("limit 1"),
            cursor: None,
        })
        .expect("limited search");
    assert_eq!(limited.items.len(), 1);
    assert_eq!(limited.items[0].tool_ref.as_str(), "authorized.backend@1");
    assert!(limited.next_cursor.is_some());
}

#[test]
fn legacy_tool_catalog_hash_is_unchanged_when_tags_and_when_to_use_are_absent() {
    let legacy_tool = make_tool(
        "tool.catalog.search@1",
        "Search tools",
        "Search tools capability",
        &[],
        None,
        CapabilityLocation::Cloud,
    );
    let legacy_catalog = AuthorizedToolCatalog::new(vec![legacy_tool.clone()]).expect("legacy");

    // Recompute the exact historical pre-K7 canonical byte digest directly to prove
    // zero hash drift when `tags` is empty and `when_to_use` is `None`.
    let mut expected_bytes = Vec::new();
    let append_u64 = |buf: &mut Vec<u8>, v: usize| {
        buf.extend_from_slice(&u64::try_from(v).unwrap_or(u64::MAX).to_be_bytes());
    };
    let append_bytes = |buf: &mut Vec<u8>, v: &[u8]| {
        append_u64(buf, v.len());
        buf.extend_from_slice(v);
    };
    append_u64(&mut expected_bytes, 1);
    append_bytes(
        &mut expected_bytes,
        legacy_tool.tool_ref.as_str().as_bytes(),
    );
    append_bytes(
        &mut expected_bytes,
        legacy_tool.wire_name.as_str().as_bytes(),
    );
    append_bytes(&mut expected_bytes, legacy_tool.title.as_bytes());
    append_bytes(&mut expected_bytes, legacy_tool.description.as_bytes());
    append_bytes(&mut expected_bytes, &legacy_tool.schema_hash.into_bytes());
    append_bytes(
        &mut expected_bytes,
        legacy_tool.schema.inline_bytes().expect("inline schema"),
    );
    expected_bytes.push(1); // CapabilityLocation::Cloud
    append_u64(&mut expected_bytes, 0); // required_contexts.len()
    let expected_legacy_hash = Sha256::digest(expected_bytes);

    assert_eq!(legacy_catalog.catalog_hash(), expected_legacy_hash);

    // Populating `tags` or `when_to_use` commits into `catalog_hash`.
    let mut tagged = legacy_tool.clone();
    tagged.tags = vec!["discovery".to_owned()];
    let tagged_catalog = AuthorizedToolCatalog::new(vec![tagged]).expect("tagged");
    assert_ne!(tagged_catalog.catalog_hash(), expected_legacy_hash);

    let mut guided = legacy_tool;
    guided.when_to_use = Some("Use when searching the catalog".to_owned());
    let guided_catalog = AuthorizedToolCatalog::new(vec![guided]).expect("guided");
    assert_ne!(guided_catalog.catalog_hash(), expected_legacy_hash);
    assert_ne!(guided_catalog.catalog_hash(), tagged_catalog.catalog_hash());
}
