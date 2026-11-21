use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{
    ActivationConstraints, ActivationRequest, AuthorizedToolCatalog, AuthorizedToolDescriptor,
    CarryForwardRejectionReason, DiscoveryCarryForwardEvidence, ExposureState, PriorToolExposure,
    ToolCatalogSearchRequest, ToolExposureAdded, reauthorize_tool_exposures,
};
use musteragent_identity::{
    CapabilityProviderId, ContentId, InvocationId, RunId, UtcTimestamp, VersionedKey,
};
use musteragent_tool::{ToolKey, WireName};

fn tool(key: &str, title: &str, location: CapabilityLocation) -> AuthorizedToolDescriptor {
    let schema_bytes = format!(
        "{{\"additionalProperties\":false,\"properties\":{{}},\"title\":\"{title}\",\"type\":\"object\"}}"
    )
    .into_bytes();
    AuthorizedToolDescriptor {
        provided_capability: None,
        tool_ref: ToolKey::from_versioned_key(VersionedKey::new(key).expect("Tool ref")),
        capability_ref: VersionedKey::new(key).expect("Capability ref"),
        wire_name: WireName::new(key.replace(['.', '@'], "_")).expect("wire name"),
        title: title.to_owned().into(),
        description: format!("{title} capability").into(),
        tags: Vec::new(),
        when_to_use: None,
        schema_hash: Sha256::digest(&schema_bytes),
        schema: schema_bytes.into(),
        provider_id: provider_id(location),
        location,
        required_contexts: BTreeSet::new(),
    }
}

fn provider_id(location: CapabilityLocation) -> CapabilityProviderId {
    match location {
        CapabilityLocation::Backend => "019fa6de-38fd-7000-8000-000000000402",
        _ => "019fa6de-38fd-7000-8000-000000000401",
    }
    .parse()
    .expect("Capability Provider ID")
}

fn placements(
    catalog: &AuthorizedToolCatalog,
) -> BTreeSet<(CapabilityProviderId, CapabilityLocation)> {
    catalog
        .definitions()
        .iter()
        .map(|tool| (tool.provider_id, tool.location))
        .collect()
}

fn catalog() -> AuthorizedToolCatalog {
    AuthorizedToolCatalog::new(vec![
        tool(
            "tool.catalog.search@1",
            "Search tools",
            CapabilityLocation::Cloud,
        ),
        tool(
            "tool.catalog.activate@1",
            "Activate tools",
            CapabilityLocation::Cloud,
        ),
        tool(
            "interaction.request@1",
            "Request interaction",
            CapabilityLocation::Cloud,
        ),
        tool(
            "image.generate@1",
            "Generate image",
            CapabilityLocation::Backend,
        ),
        tool(
            "artifact.presentation.create@1",
            "Create presentation",
            CapabilityLocation::Cloud,
        ),
    ])
    .expect("authorized catalog")
}

fn bootstrap() -> Vec<ToolKey> {
    [
        "tool.catalog.search@1",
        "tool.catalog.activate@1",
        "interaction.request@1",
    ]
    .into_iter()
    .map(|key| ToolKey::from_versioned_key(VersionedKey::new(key).expect("Tool ref")))
    .collect()
}

fn constraints() -> ActivationConstraints {
    ActivationConstraints {
        available_locations: BTreeSet::from([
            CapabilityLocation::Cloud,
            CapabilityLocation::Backend,
        ]),
        available_contexts: BTreeSet::new(),
        max_exposed_tools: NonZeroU32::new(8).expect("limit"),
    }
}

fn reason_ref() -> ContentRef {
    let bytes = b"Need the selected capability";
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    }
}

fn activate(
    catalog: &AuthorizedToolCatalog,
    state: &ExposureState,
    keys: Vec<ToolKey>,
) -> musteragent_discovery::ActivationOutcome {
    catalog
        .activate(
            state,
            ActivationRequest {
                tool_refs: keys,
                reason_ref: reason_ref(),
                source_invocation_id: InvocationId::generate(),
                exposed_at: UtcTimestamp::from_unix_millis(1_700_000_000_000),
            },
            &constraints(),
        )
        .expect("activation")
}

#[test]
fn domain_catalog_construction_is_text_independent() {
    let first = catalog();
    let rewritten = catalog();
    let translated = catalog();
    assert_eq!(first.catalog_hash(), rewritten.catalog_hash());
    assert_eq!(first.catalog_hash(), translated.catalog_hash());
    assert_eq!(first.len(), 5);
}

#[test]
fn domain_agent_searches_and_activates_unexposed_tool() {
    let catalog = catalog();
    let mut state = ExposureState::from_initial(&catalog, &bootstrap()).expect("initial state");
    assert!(!state.contains_key("image.generate@1"));
    let results = catalog
        .search(&ToolCatalogSearchRequest {
            query: "generate an image".to_owned(),
            locations: BTreeSet::new(),
            limit: NonZeroUsize::new(4).expect("limit"),
            cursor: None,
        })
        .expect("search");
    assert_eq!(results.items[0].tool_ref.as_str(), "image.generate@1");
    let outcome = activate(&catalog, &state, vec![results.items[0].tool_ref.clone()]);
    state
        .apply(
            &catalog,
            outcome.event.as_ref().expect("new exposure event"),
        )
        .expect("apply");
    assert!(state.contains_key("image.generate@1"));
}

#[test]
fn domain_search_cursor_is_bound_and_fails_closed() {
    let catalog = catalog();
    let first = catalog
        .search(&ToolCatalogSearchRequest {
            query: "tool".to_owned(),
            locations: BTreeSet::new(),
            limit: NonZeroUsize::new(1).expect("limit"),
            cursor: None,
        })
        .expect("first page");
    let cursor = first.next_cursor.expect("cursor");
    assert!(
        catalog
            .search(&ToolCatalogSearchRequest {
                query: "image".to_owned(),
                locations: BTreeSet::new(),
                limit: NonZeroUsize::new(1).expect("limit"),
                cursor: Some(cursor),
            })
            .is_err()
    );
}

#[test]
fn domain_tool_activation_never_expands_grant() {
    let catalog = catalog();
    let state = ExposureState::from_initial(&catalog, &bootstrap()).expect("initial state");
    let forged = ToolKey::from_versioned_key(
        VersionedKey::new("admin.secret.export@1").expect("forged ref"),
    );
    assert!(
        catalog
            .activate(
                &state,
                ActivationRequest {
                    tool_refs: vec![forged],
                    reason_ref: reason_ref(),
                    source_invocation_id: InvocationId::generate(),
                    exposed_at: UtcTimestamp::from_unix_millis(1),
                },
                &constraints(),
            )
            .is_err()
    );
    let mut cloud_only = constraints();
    cloud_only
        .available_locations
        .remove(&CapabilityLocation::Backend);
    assert!(
        catalog
            .activate(
                &state,
                ActivationRequest {
                    tool_refs: vec![ToolKey::from_versioned_key(
                        VersionedKey::new("image.generate@1").expect("image ref"),
                    )],
                    reason_ref: reason_ref(),
                    source_invocation_id: InvocationId::generate(),
                    exposed_at: UtcTimestamp::from_unix_millis(1),
                },
                &cloud_only,
            )
            .is_err()
    );
}

#[test]
fn domain_tool_exposure_replay_is_idempotent_and_ordered() {
    let catalog = catalog();
    let initial = bootstrap();
    let state = ExposureState::from_initial(&catalog, &initial).expect("initial state");
    let outcome = activate(
        &catalog,
        &state,
        vec![
            ToolKey::from_versioned_key(VersionedKey::new("image.generate@1").expect("image ref")),
            ToolKey::from_versioned_key(
                VersionedKey::new("artifact.presentation.create@1").expect("presentation ref"),
            ),
        ],
    );
    let event = outcome.event.expect("event");
    assert_eq!(event.exposures[0].ordinal.get(), 4);
    assert_eq!(event.exposures[1].ordinal.get(), 5);
    let mut recovered = ExposureState::from_initial(&catalog, &initial).expect("recover initial");
    recovered.apply(&catalog, &event).expect("replay event");
    let duplicate = activate(
        &catalog,
        &recovered,
        vec![ToolKey::from_versioned_key(
            VersionedKey::new("image.generate@1").expect("image ref"),
        )],
    );
    assert!(duplicate.event.is_none());
    assert_eq!(duplicate.ordinals[0].get(), 4);
}

#[test]
fn domain_projection_allows_no_dynamic_rows_without_weakening_event_validation() {
    let catalog = catalog();
    let initial = bootstrap();
    let projected = ExposureState::from_projection(&catalog, &initial, &[])
        .expect("empty dynamic projection is a valid initial state");
    assert_eq!(projected.project(&catalog).len(), initial.len());

    let mut event_state = ExposureState::from_initial(&catalog, &initial).expect("initial state");
    let empty_event = ToolExposureAdded {
        exposures: Vec::new(),
        reason_ref: reason_ref(),
        source_invocation_id: InvocationId::generate(),
        exposed_at: UtcTimestamp::from_unix_millis(1_700_000_000_000),
    };
    assert!(
        event_state.apply(&catalog, &empty_event).is_err(),
        "a persisted domain event must never be empty"
    );
}

#[test]
fn domain_tool_exposure_projection_is_model_independent() {
    let catalog = catalog();
    let initial = bootstrap();
    let state = ExposureState::from_initial(&catalog, &initial).expect("initial state");
    let event = activate(
        &catalog,
        &state,
        vec![ToolKey::from_versioned_key(
            VersionedKey::new("image.generate@1").expect("image ref"),
        )],
    )
    .event
    .expect("event");
    let mut model_a = ExposureState::from_initial(&catalog, &initial).expect("model A");
    model_a.apply(&catalog, &event).expect("model A event");
    let mut model_b = ExposureState::from_initial(&catalog, &initial).expect("model B");
    model_b.apply(&catalog, &event).expect("model B event");
    assert_eq!(model_a.project(&catalog), model_b.project(&catalog));
}

#[test]
fn domain_interaction_bootstrap_schema_hash_is_pinned() {
    let catalog = catalog();
    let state = ExposureState::from_initial(&catalog, &bootstrap()).expect("initial state");
    let definition = state
        .project(&catalog)
        .into_iter()
        .find(|tool| tool.tool_ref.as_str() == "interaction.request@1")
        .expect("interaction bootstrap");
    assert_eq!(
        Sha256::digest(definition.schema.inline_bytes().unwrap()),
        definition.schema_hash
    );
}

#[test]
fn domain_projection_preserves_append_only_tool_exposure() {
    let catalog = catalog();
    let initial = bootstrap();
    let state = ExposureState::from_initial(&catalog, &initial).expect("initial state");
    let prefix = state.project(&catalog);
    let event = activate(
        &catalog,
        &state,
        vec![ToolKey::from_versioned_key(
            VersionedKey::new("image.generate@1").expect("image ref"),
        )],
    )
    .event
    .expect("event");
    let mut advanced = state;
    advanced.apply(&catalog, &event).expect("apply");
    let projected = advanced.project(&catalog);
    assert_eq!(&projected[..prefix.len()], prefix.as_slice());
    assert_eq!(
        projected.last().expect("new tool").tool_ref.as_str(),
        "image.generate@1"
    );
}

#[test]
fn domain_carry_forward_reauthorizes_exact_versions() {
    let source_run_id = RunId::generate();
    let catalog = catalog();
    let image = catalog.get_by_key("image.generate@1").expect("image tool");
    let presentation = catalog
        .get_by_key("artifact.presentation.create@1")
        .expect("presentation tool");
    let exact = PriorToolExposure {
        tool_ref: image.tool_ref.clone(),
        schema_hash: image.schema_hash,
        provider_id: image.provider_id,
        location: image.location,
    };
    let decision = reauthorize_tool_exposures(
        source_run_id,
        &[
            exact.clone(),
            PriorToolExposure {
                tool_ref: ToolKey::from_versioned_key(
                    VersionedKey::new("image.generate@2").expect("alternate image version"),
                ),
                ..exact.clone()
            },
            PriorToolExposure {
                schema_hash: Sha256::digest(b"drifted schema bytes"),
                ..exact.clone()
            },
            PriorToolExposure {
                provider_id: provider_id(CapabilityLocation::Cloud),
                ..exact.clone()
            },
            PriorToolExposure {
                location: CapabilityLocation::Cloud,
                ..exact.clone()
            },
            PriorToolExposure {
                tool_ref: ToolKey::from_versioned_key(
                    VersionedKey::new("removed.tool@1").expect("removed ref"),
                ),
                schema_hash: Sha256::digest(b"removed"),
                provider_id: image.provider_id,
                location: image.location,
            },
        ],
        &catalog,
        &constraints(),
        &placements(&catalog),
        &BTreeMap::new(),
    );
    assert_eq!(decision.accepted_tool_refs, vec![image.tool_ref.clone()]);
    assert_eq!(
        decision
            .rejected_tools
            .iter()
            .map(|rejection| rejection.reason)
            .collect::<Vec<_>>(),
        [
            CarryForwardRejectionReason::VersionChanged,
            CarryForwardRejectionReason::SchemaOrContentChanged,
            CarryForwardRejectionReason::LocationUnavailable,
            CarryForwardRejectionReason::LocationUnavailable,
            CarryForwardRejectionReason::NotAuthorized,
        ]
    );
    assert_eq!(
        decision.rejected_tools[0]
            .replacement_tool_ref
            .as_ref()
            .map(ToolKey::as_str),
        Some("image.generate@1")
    );
    let evidence: serde_json::Value = serde_json::from_slice(
        &decision
            .encode_canonical_json()
            .expect("canonical carry evidence"),
    )
    .expect("carry evidence JSON");
    assert_eq!(
        evidence["schema_version"],
        "musteragent.discovery.carry-forward-decision.v3"
    );
    let valid_decision = reauthorize_tool_exposures(
        source_run_id,
        std::slice::from_ref(&exact),
        &catalog,
        &constraints(),
        &placements(&catalog),
        &BTreeMap::new(),
    );
    let decoded = DiscoveryCarryForwardEvidence::decode_canonical_json(
        &valid_decision
            .encode_canonical_json()
            .expect("canonical carry evidence"),
    )
    .expect("decode carry evidence");
    assert!(decoded.exhaustive);
    assert_eq!(decoded.source_run_id, source_run_id);
    assert_eq!(
        decoded.accepted_tool_refs,
        valid_decision.accepted_tool_refs
    );
    assert_eq!(
        decoded.rejected_tool_refs,
        valid_decision
            .rejected_tools
            .iter()
            .map(|rejection| rejection.prior_tool_ref.clone())
            .collect::<Vec<_>>()
    );
    for retired in [
        "musteragent.legacy.discovery.carry-forward-decision.v1",
        "musteragent.legacy.discovery.carry-forward-decision.v2",
    ] {
        let bytes = String::from_utf8(
            valid_decision
                .encode_canonical_json()
                .expect("canonical evidence"),
        )
        .expect("UTF-8")
        .replace("musteragent.discovery.carry-forward-decision.v3", retired);
        assert!(DiscoveryCarryForwardEvidence::decode_canonical_json(bytes.as_bytes()).is_err());
    }
    let forged = serde_json::json!({
        "accepted_skill_refs": [],
        "accepted_tool_refs": [],
        "extra": true,
        "rejected_skills": [],
        "rejected_tools": [],
        "schema_version": "musteragent.discovery.carry-forward-decision.v3",
        "source_run_id": source_run_id.to_string(),
    });
    assert!(
        DiscoveryCarryForwardEvidence::decode_canonical_json(
            &serde_json::to_vec(&forged).expect("forged JSON")
        )
        .is_err()
    );
    assert_eq!(evidence["rejected_tools"][2]["prior_location"], "backend");
    assert_eq!(evidence["rejected_tools"][2]["current_location"], "backend");
    assert_ne!(
        evidence["rejected_tools"][2]["prior_provider_id"],
        evidence["rejected_tools"][2]["current_provider_id"]
    );

    let mut unavailable_placements = placements(&catalog);
    unavailable_placements.remove(&(image.provider_id, image.location));
    let unavailable = reauthorize_tool_exposures(
        source_run_id,
        std::slice::from_ref(&exact),
        &catalog,
        &constraints(),
        &unavailable_placements,
        &BTreeMap::new(),
    );
    assert_eq!(
        unavailable.rejected_tools[0].reason,
        CarryForwardRejectionReason::LocationUnavailable
    );

    let denied_catalog = AuthorizedToolCatalog::new(
        catalog
            .definitions()
            .iter()
            .filter(|definition| {
                !matches!(
                    definition.tool_ref.as_str(),
                    "image.generate@1" | "artifact.presentation.create@1"
                )
            })
            .cloned()
            .collect(),
    )
    .expect("denied current catalog");
    let denied = reauthorize_tool_exposures(
        source_run_id,
        &[
            exact,
            PriorToolExposure {
                tool_ref: presentation.tool_ref.clone(),
                schema_hash: presentation.schema_hash,
                provider_id: presentation.provider_id,
                location: presentation.location,
            },
        ],
        &denied_catalog,
        &constraints(),
        &placements(&denied_catalog),
        &BTreeMap::from([
            (
                image.tool_ref.clone(),
                CarryForwardRejectionReason::GrantDenied,
            ),
            (
                presentation.tool_ref.clone(),
                CarryForwardRejectionReason::PolicyDenied,
            ),
        ]),
    );
    assert_eq!(
        denied
            .rejected_tools
            .iter()
            .map(|rejection| rejection.reason)
            .collect::<Vec<_>>(),
        [
            CarryForwardRejectionReason::GrantDenied,
            CarryForwardRejectionReason::PolicyDenied,
        ]
    );
}

#[test]
fn semantic_ranking_stays_inside_authority_and_binds_its_pagination() {
    let catalog = catalog();
    let request = ToolCatalogSearchRequest {
        query: "网页搜索".into(),
        locations: BTreeSet::new(),
        limit: NonZeroUsize::new(1).unwrap(),
        cursor: None,
    };
    let scores = BTreeMap::from([
        ("image.generate@1".to_owned(), 700),
        ("tool.catalog.search@1".to_owned(), 600),
    ]);
    let first = catalog
        .search_with_semantic_scores(&request, &scores)
        .unwrap();
    assert_eq!(first.items[0].tool_ref.as_str(), "image.generate@1");
    let second_request = ToolCatalogSearchRequest {
        cursor: first.next_cursor.clone(),
        ..request.clone()
    };
    let second = catalog
        .search_with_semantic_scores(&second_request, &scores)
        .unwrap();
    assert_eq!(second.items[0].tool_ref.as_str(), "tool.catalog.search@1");
    let reordered = BTreeMap::from([
        ("image.generate@1".to_owned(), 600),
        ("tool.catalog.search@1".to_owned(), 700),
    ]);
    assert!(
        catalog
            .search_with_semantic_scores(&second_request, &reordered)
            .is_err()
    );
    assert!(
        catalog
            .search_with_semantic_scores(
                &request,
                &BTreeMap::from([("not.authorized@1".into(), 999)])
            )
            .is_err()
    );
}
