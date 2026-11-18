use super::*;
use crate::planner::tests::{assistant_tool_entry, content_ref, entry, model_route, user_entry};
use musteragent_identity::{ConversationSeq, InvocationId, LedgerRecordId, RunId, ToolCallId};
use musteragent_runtime_model::{ModelRouteRef, ReasoningReplay};

struct Fixture {
    cache: Arc<VerifiedContentCache>,
    tenant: TenantId,
    scope: Sha256,
    route: ModelRoute,
}
impl Fixture {
    fn new() -> Self {
        Self {
            cache: Arc::new(VerifiedContentCache::default()),
            tenant: TenantId::generate(),
            scope: Sha256::digest(b"authorized-test-lineage"),
            route: model_route(),
        }
    }
    fn reuse(&self) -> MaterialReuse {
        MaterialReuse(Some((
            self.cache.clone(),
            self.cache.begin_generation().expect("generation"),
        )))
    }
    fn compare(&self, entries: &[ContextEntry], compacted: bool) -> HistoryProjection {
        let entries = entries.iter().collect::<Vec<_>>();
        let expected = MaterialReuse(None).history_for_scope(
            self.tenant,
            self.scope,
            &entries,
            &self.route,
            compacted,
        );
        let result = self.reuse().history_for_scope(
            self.tenant,
            self.scope,
            &entries,
            &self.route,
            compacted,
        );
        assert_eq!(
            result, expected,
            "full and incremental projections, including errors, must match"
        );
        result.expect("valid projection")
    }
    fn material_hits(&self) -> u64 {
        self.cache.stats().expect("stats").material_hits
    }
    fn material_misses(&self) -> u64 {
        self.cache.stats().expect("stats").material_misses
    }
}

#[test]
fn warm_144_item_history_reuses_only_currently_selected_segments() {
    let fixture = Fixture::new();
    let mut entries = (1..=144)
        .map(|seq| user_entry(seq, &format!("entry {seq}: {}", "abc中\n".repeat(600))))
        .collect::<Vec<_>>();
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_misses(), 144);
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_hits(), 144);
    entries.push(user_entry(145, "new user tail"));
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_hits(), 288);
    assert_eq!(
        fixture.material_misses(),
        145,
        "one append builds one new segment"
    );
    entries.truncate(36);
    entries.reverse();
    let projected = fixture.compare(&entries, false);
    assert_eq!(
        projected.messages.len(),
        36,
        "a narrower cut cannot retain the previous tail"
    );
    assert_eq!(
        fixture.material_misses(),
        145,
        "current order is assembled from immutable segments"
    );
    assert_eq!(projected.messages[0], entries[0].message);
}

fn exchange() -> Vec<ContextEntry> {
    vec![
        user_entry(1, "request"),
        assistant_tool_entry(2, &["call-3", "call-4"]),
        entry(3, None, "first result"),
        entry(4, None, "second result"),
    ]
}

#[test]
fn atomic_tool_exchange_rebuilds_as_one_unit_when_result_or_surface_changes() {
    let fixture = Fixture::new();
    let mut entries = exchange();
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_misses(), 2);
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_hits(), 2);
    entries[3].message.content = vec![CanonicalContentPart::Text(
        ModelText::new("corrected result").expect("text"),
    )];
    fixture.compare(&entries, false);
    assert_eq!(
        fixture.material_misses(),
        3,
        "only the changed exchange is rebuilt"
    );
    let retained = fixture.compare(&entries, false);
    assert_eq!(retained.messages[1].role, CanonicalRole::Assistant);
    assert_eq!(retained.messages[1].tool_calls.len(), 2);
    let retired = fixture.compare(&entries, true);
    assert_eq!(retired.projection, HandoffProjection::CompactedHandoff);
    assert_eq!(
        retired.messages[1].tool_calls,
        retained.messages[1].tool_calls
    );
}

#[test]
fn warm_pairing_never_hides_missing_duplicate_or_foreign_tool_results() {
    let fixture = Fixture::new();
    let entries = exchange();
    fixture.compare(&entries, false);
    let mut missing = entries.clone();
    missing.pop();
    let mut duplicate = entries.clone();
    duplicate[3].message.tool_call_id = duplicate[2].message.tool_call_id.clone();
    let mut foreign = entries;
    foreign[3].message.tool_call_id = Some(ToolCallId::new("foreign-call").expect("call"));
    for invalid in [missing, duplicate, foreign] {
        let refs = invalid.iter().collect::<Vec<_>>();
        let expected = project(&refs, &fixture.route);
        assert_eq!(expected, Err(ContextPlanError::invariant()));
        assert_eq!(
            fixture.reuse().history_for_scope(
                fixture.tenant,
                fixture.scope,
                &refs,
                &fixture.route,
                false
            ),
            expected
        );
        assert!(
            crate::planner::atomic_context_units(&refs, false).is_err(),
            "current cross-segment boundary must also reject"
        );
    }
}

#[test]
fn full_identity_tracks_private_replay_origin_and_all_entry_provenance() {
    let mut fixture = Fixture::new();
    let mut entries = exchange();
    let origin = crate::AssistantOrigin {
        invocation_id: InvocationId::generate(),
        model_route_ref: fixture.route.route_ref.clone(),
        route_identity_hash: SignedModelRouteHash::digest(&fixture.route).expect("hash"),
        requested_model_key: musteragent_model::ModelKey::new(
            fixture.route.resolved_model_key.clone(),
        )
        .expect("model"),
        response_model_key: musteragent_model::ModelKey::new(
            fixture.route.resolved_model_key.clone(),
        )
        .expect("model"),
    };
    entries[1].assistant_origin = Some(origin);
    entries[1].message.reasoning = Some(ReasoningReplay {
        content_ref: content_ref(b"reasoning"),
        source_route_ref: ModelRouteRef::from_key(fixture.route.route_ref.clone()),
        continuation_ref: None,
    });
    let exact = fixture.compare(&entries, false);
    assert!(exact.messages[1].reasoning.is_some());
    let edits: &[fn(&mut ContextEntry)] = &[
        |entry| entry.source_record_ids[0] = LedgerRecordId::generate(),
        |entry| entry.source_run_id = Some(RunId::generate()),
        |entry| entry.continuation_delta_for = Some(InvocationId::generate()),
        |entry| entry.presentation_order.anchor_sequence = ConversationSeq::new(7).expect("seq"),
        |entry| entry.presentation_order.exchange_ordinal = 7,
        |entry| entry.source_sequence = ConversationSeq::new(9).expect("seq"),
        |entry| entry.assistant_origin = None,
        |entry| {
            entry
                .message
                .reasoning
                .as_mut()
                .expect("replay")
                .content_ref = content_ref(b"updated reasoning")
        },
        |entry| {
            entry.message.tool_calls[0].arguments = JsonValue::Object(BTreeMap::from([(
                "changed".to_owned(),
                JsonValue::Integer(42),
            )]))
        },
        |entry| entry.message.tool_calls[0].provider_replay_ref = Some(content_ref(b"tool replay")),
        |entry| {
            entry.message.content.push(CanonicalContentPart::Image(
                musteragent_content_ref::ContentRef {
                    media_type: musteragent_content_ref::MediaType::new("image/png")
                        .expect("image media"),
                    ..content_ref(b"image")
                },
            ))
        },
        |entry| {
            entry
                .message
                .content
                .push(CanonicalContentPart::File(content_ref(b"file")))
        },
    ];
    for edit in edits {
        let mut updated = entries.clone();
        edit(&mut updated[1]);
        let before = fixture.material_misses();
        fixture.compare(&updated, false);
        assert_eq!(
            fixture.material_misses(),
            before + 1,
            "source IDs alone cannot authorize reuse"
        );
    }
    fixture.route.resolved_model_key = "different-model".to_owned();
    let changed = fixture.compare(&entries, false);
    assert!(changed.messages[1].reasoning.is_none());
    assert!(changed.has_portable_history());
}

#[test]
fn tenant_lineage_change_and_erasure_cannot_reuse_or_republish_old_segments() {
    let mut fixture = Fixture::new();
    let entries = exchange();
    fixture.compare(&entries, false);
    fixture.tenant = TenantId::generate();
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_misses(), 4);
    fixture.scope = Sha256::digest(b"new branch or baseline");
    fixture.compare(&entries, false);
    assert_eq!(fixture.material_misses(), 6);
    let stale = fixture.reuse();
    fixture.cache.clear().expect("erase");
    let refs = entries.iter().collect::<Vec<_>>();
    assert_eq!(
        stale.history_for_scope(fixture.tenant, fixture.scope, &refs, &fixture.route, false),
        project(&refs, &fixture.route)
    );
    assert_eq!(
        fixture.cache.stats().expect("stats").entries,
        0,
        "late computation must not republish erased material"
    );
}

#[test]
fn parallel_run_private_suffix_is_never_selected_from_another_rebuild() {
    let fixture = Fixture::new();
    let prefix = user_entry(1, "admission shared prefix");
    let mut own = user_entry(4, "private run A suffix");
    own.source_run_id = Some(RunId::generate());
    let mut sibling = user_entry(5, "private run B suffix");
    sibling.source_run_id = Some(RunId::generate());
    fixture.compare(&[prefix.clone(), own.clone()], false);
    fixture.compare(&[prefix.clone(), sibling], false);
    let actual = fixture.compare(&[prefix, own.clone()], false);
    assert_eq!(actual.messages.len(), 2);
    assert_eq!(actual.messages[1], own.message);
}
