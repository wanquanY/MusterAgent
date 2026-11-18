use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ContentId, ConversationSeq, DottedKey, InvocationId, LedgerRecordId, ToolCallId, VersionedKey,
};
use musteragent_model::{
    ApiFormat, ModelEntryId, ModelKey, ModelParameter, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy, SignedModelRouteHash,
};
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall, JsonValue,
    ModelRouteRef, ModelText, ProviderRequestRef, ReasoningReplay,
};
use musteragent_tool::WireName;

use super::{HandoffProjection, compile_provider_context, target_route_owns_usage_anchor};
use crate::{AssistantOrigin, ContextEntry, ContextPresentationOrder};

#[test]
fn exact_route_preserves_private_reasoning_and_tool_replay() {
    let route = model_route("source-route@1", "source-model", 1);
    let entry = assistant_entry(Some(origin(&route)));

    let compiled = compile_exchange(&entry, &route, false).expect("compile exact");

    assert_eq!(compiled.projection, HandoffProjection::ExactReplay);
    assert_eq!(compiled.exact_assistant_entries, 1);
    assert_eq!(compiled.portable_assistant_entries, 0);
    assert!(compiled.messages[0].reasoning.is_some());
    assert!(
        compiled.messages[0].tool_calls[0]
            .provider_replay_ref
            .is_some()
    );
}

#[test]
fn foreign_route_keeps_portable_semantics_and_removes_private_artifacts() {
    let source = model_route("source-route@1", "source-model", 1);
    let mut target = model_route("target-route@1", "target-model", 2);
    target.api_format = ApiFormat::AnthropicMessages;
    let entry = assistant_entry(Some(origin(&source)));

    let compiled = compile_exchange(&entry, &target, false).expect("compile handoff");

    assert_eq!(compiled.projection, HandoffProjection::PortableProjection);
    assert_eq!(compiled.exact_assistant_entries, 0);
    assert_eq!(compiled.portable_assistant_entries, 1);
    let message = &compiled.messages[0];
    assert!(message.reasoning.is_none());
    assert_eq!(message.role, CanonicalRole::Assistant);
    assert_eq!(message.content, entry.message.content);
    assert_eq!(
        message.tool_calls[0].arguments,
        entry.message.tool_calls[0].arguments
    );
    assert_eq!(
        message.tool_calls[0].call_id,
        entry.message.tool_calls[0].call_id
    );
    assert!(message.tool_calls[0].provider_replay_ref.is_none());
    assert!(message.tool_calls[0].provider_item_ref.is_none());
    assert_eq!(compiled.messages[1], result_entry().message);
    assert!(entry.message.reasoning.is_some());
    assert!(entry.message.tool_calls[0].provider_item_ref.is_some());
}

#[test]
fn missing_or_response_model_mismatched_origin_never_becomes_exact_replay() {
    let route = model_route("source-route@1", "source-model", 1);
    let unknown = assistant_entry(None);
    let mut mismatched = assistant_entry(Some(origin(&route)));
    mismatched
        .assistant_origin
        .as_mut()
        .expect("origin")
        .response_model_key = ModelKey::new("provider-resolved-alias").expect("model key");

    for entry in [&unknown, &mismatched] {
        let compiled = compile_exchange(entry, &route, false).expect("compile portable");
        assert_eq!(compiled.projection, HandoffProjection::PortableProjection);
        assert!(compiled.messages[0].reasoning.is_none());
    }
}

#[test]
fn same_route_reference_with_a_different_signed_identity_is_portable() {
    let source = model_route("source-route@1", "source-model", 1);
    let mut target = source.clone();
    target.parameters.insert(
        DottedKey::new("provider.temperature").expect("parameter"),
        ModelParameter::Int(1),
    );
    let entry = assistant_entry(Some(origin(&source)));

    let compiled = compile_exchange(&entry, &target, false).expect("compile portable");

    assert_eq!(compiled.projection, HandoffProjection::PortableProjection);
    assert!(compiled.messages[0].reasoning.is_none());
    assert!(
        compiled.messages[0].tool_calls[0]
            .provider_replay_ref
            .is_none()
    );
}

#[test]
fn protocol_switch_invalidates_source_physical_usage_anchor() {
    let source = model_route("same-route@1", "same-model", 1);
    let mut target = source.clone();
    target.api_format = ApiFormat::OpenAiChatCompletions;
    target.adapter_version =
        VersionedKey::new("openai.chat-completions-adapter@1").expect("adapter");
    target.protocol_version = VersionedKey::new("openai.chat-completions@1").expect("protocol");
    let entry = assistant_entry(Some(origin(&source)));

    assert!(
        !target_route_owns_usage_anchor(&[&entry], entry.source_sequence, &target)
            .expect("reject source-protocol usage")
    );
}

#[test]
fn physical_usage_anchor_is_reused_only_by_its_exact_signed_route() {
    let source = model_route("source-route@1", "source-model", 1);
    let target = model_route("target-route@1", "target-model", 2);
    let entry = assistant_entry(Some(origin(&source)));

    assert!(
        target_route_owns_usage_anchor(&[&entry], entry.source_sequence, &source)
            .expect("compare exact source anchor")
    );
    assert!(
        !target_route_owns_usage_anchor(&[&entry], entry.source_sequence, &target)
            .expect("reject foreign anchor")
    );
}

#[test]
fn unknown_assistant_origin_never_anchors_target_physical_usage() {
    let target = model_route("target-route@1", "target-model", 2);
    let entry = assistant_entry(None);

    assert!(
        !target_route_owns_usage_anchor(&[&entry], entry.source_sequence, &target)
            .expect("reject unknown anchor")
    );
}

#[test]
fn ordinary_compaction_of_portable_history_is_diagnosed_as_compacted_handoff() {
    let target = model_route("target-route@1", "target-model", 2);
    let retained = assistant_entry(None);

    let compiled = compile_exchange(&retained, &target, true).expect("compile compacted handoff");

    assert_eq!(compiled.projection, HandoffProjection::CompactedHandoff);
    assert_eq!(compiled.portable_assistant_entries, 1);
}

#[test]
fn history_preserves_parallel_calls_and_reversed_results_without_any_active_definition() {
    let route = model_route("target@1", "target", 2);
    let mut original = assistant_entry(None);
    let mut second = original.message.tool_calls[0].clone();
    second.call_id = ToolCallId::new("second").unwrap();
    second.name = WireName::new("retired").unwrap();
    original.message.tool_calls.push(second);
    let mut second_result = result_entry();
    second_result.message = tool_result("second", "retired", "old receipt");
    let history =
        compile_provider_context(&[&original, &second_result, &result_entry()], &route, false)
            .unwrap();
    assert_eq!(history.messages[0].role, CanonicalRole::Assistant);
    assert_eq!(history.messages[0].tool_calls.len(), 2);
    assert_eq!(history.messages[0].tool_calls[1].name.as_str(), "retired");
    assert_eq!(history.messages[1], second_result.message);
    assert_eq!(history.messages[2], result_entry().message);
}

fn assistant_entry(assistant_origin: Option<AssistantOrigin>) -> ContextEntry {
    let sequence = ConversationSeq::new(1).expect("sequence");
    let replay_ref = content_ref(b"private replay");
    ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: sequence,
        presentation_order: ContextPresentationOrder::ledger(sequence),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin,
        message: CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: vec![CanonicalContentPart::Text(
                ModelText::new("portable answer").expect("text"),
            )],
            tool_calls: vec![CanonicalToolCall {
                call_id: ToolCallId::new("call_portable_1").expect("call id"),
                name: WireName::new("lookup").expect("wire name"),
                arguments: JsonValue::Object(BTreeMap::from([(
                    "query".to_owned(),
                    JsonValue::String("weather".to_owned()),
                )])),
                provider_item_ref: Some(ProviderRequestRef::from_opaque(
                    OpaqueExternalRef::new("provider-private-item").expect("provider item ref"),
                )),
                provider_replay_ref: Some(replay_ref.clone()),
            }],
            tool_call_id: None,
            tool_name: None,
            reasoning: Some(ReasoningReplay {
                content_ref: replay_ref,
                source_route_ref: ModelRouteRef::from_key(
                    VersionedKey::new("source-route@1").expect("route ref"),
                ),
                continuation_ref: None,
            }),
        },
        continuation_delta_for: None,
    }
}

fn tool_result(call_id: &str, name: &str, text: &str) -> CanonicalMessage {
    CanonicalMessage {
        role: CanonicalRole::Tool,
        content: vec![CanonicalContentPart::Text(
            ModelText::new(text).expect("Tool result text"),
        )],
        tool_calls: Vec::new(),
        tool_call_id: Some(ToolCallId::new(call_id).expect("call id")),
        tool_name: Some(WireName::new(name).expect("wire name")),
        reasoning: None,
    }
}

fn origin(route: &ModelRoute) -> AssistantOrigin {
    AssistantOrigin {
        invocation_id: InvocationId::generate(),
        model_route_ref: route.route_ref.clone(),
        route_identity_hash: SignedModelRouteHash::digest(route).expect("route hash"),
        requested_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("model key"),
        response_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("model key"),
    }
}

fn model_route(route_ref: &str, model_key: &str, ordinal: u128) -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new(route_ref).expect("route ref"),
        resolved_model_key: model_key.to_owned(),
        model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::from_u128(ordinal + 10))
            .expect("provider id"),
        model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::from_u128(ordinal + 20))
            .expect("entry id"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(128_000).expect("context window"),
        max_output_tokens: NonZeroU64::new(4_096).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test@1").expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    }
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    }
}

fn result_entry() -> ContextEntry {
    let mut result = assistant_entry(None);
    result.source_sequence = ConversationSeq::new(2).expect("sequence");
    result.message = tool_result("call_portable_1", "lookup", r#"{"weather":"sunny"}"#);
    result
}

fn compile_exchange(
    entry: &ContextEntry,
    target: &ModelRoute,
    compacted: bool,
) -> Result<super::ProviderContextCompilation, crate::ContextPlanError> {
    compile_provider_context(&[entry, &result_entry()], target, compacted)
}

#[test]
fn foreign_projection_rejects_missing_duplicate_and_unmatched_results() {
    let target = model_route("target-route@1", "target-model", 2);
    let entry = assistant_entry(None);
    let result = result_entry();
    let mut unmatched = result.clone();
    unmatched.message.tool_call_id = Some(ToolCallId::new("wrong_call").expect("id"));
    for entries in [
        vec![&entry],
        vec![&entry, &result, &result],
        vec![&entry, &unmatched],
    ] {
        assert!(compile_provider_context(&entries, &target, false,).is_err());
    }
}

#[test]
fn mixed_routes_preserve_exact_native_turns_and_foreign_multimodal_results() {
    let source = model_route("source-route@1", "source-model", 1);
    let target = model_route("target-route@1", "target-model", 2);
    let foreign = assistant_entry(Some(origin(&source)));
    let mut foreign_result = result_entry();
    let image = CanonicalContentPart::Image(ContentRef {
        media_type: MediaType::new("image/png").expect("image media"),
        ..content_ref(b"historical image")
    });
    foreign_result.message.content.push(image.clone());
    let exact = assistant_entry(Some(origin(&target)));
    let exact_result = result_entry();
    let compiled = compile_provider_context(
        &[&foreign, &foreign_result, &exact, &exact_result],
        &target,
        false,
    )
    .expect("mixed route history");
    assert_eq!(compiled.portable_assistant_entries, 1);
    assert_eq!(compiled.exact_assistant_entries, 1);
    assert_eq!(compiled.messages[0].tool_calls.len(), 1);
    assert!(compiled.messages[1].content.contains(&image));
    assert_eq!(compiled.messages[2], exact.message);
    assert_eq!(compiled.messages[3], exact_result.message);
}

#[test]
fn private_only_continuation_is_replayed_only_to_its_exact_route() {
    let source = model_route("source-route@1", "source-model", 1);
    let other = model_route("target-route@1", "target-model", 2);
    let mut entry = assistant_entry(Some(origin(&source)));
    entry.message.content.clear();
    entry.message.tool_calls.clear();
    assert!(
        entry.message.reasoning.is_some(),
        "fixture includes source reasoning"
    );
    entry
        .message
        .validate()
        .expect("private Assistant replay is legal");
    let exact = compile_provider_context(&[&entry], &source, false).expect("exact replay");
    assert_eq!(exact.messages, vec![entry.message.clone()]);
    let portable = compile_provider_context(&[&entry], &other, false).expect("portable projection");
    assert!(
        portable.messages.is_empty(),
        "no empty Assistant or invented text may replace private reasoning"
    );
    entry.message.reasoning = None;
    assert!(
        entry.message.validate().is_err(),
        "empty public Assistant is still invalid"
    );
}

#[test]
fn openai_google_openai_handoff_rebuilds_current_history_without_mutating_source_replay() {
    for format in [
        ApiFormat::GoogleGenerateContent,
        ApiFormat::GoogleInteractions,
    ] {
        let openai = model_route("openai.route@1", "openai-model", 1);
        let mut google = model_route("google.route@1", "gemini-4-carbon", 2);
        google.api_format = format;
        let mut a = assistant_entry(Some(origin(&openai)));
        a.message.content = vec![CanonicalContentPart::Text(
            ModelText::new("OpenAI completed answer").unwrap(),
        )];
        let result_a = result_entry();
        let original_a = a.clone();
        let outgoing = compile_provider_context(&[&a, &result_a], &google, false).unwrap();
        assert_eq!(outgoing.projection, HandoffProjection::PortableProjection);
        assert!(outgoing.messages.iter().all(|message| {
            message.reasoning.is_none()
                && message
                    .tool_calls
                    .iter()
                    .all(|c| c.provider_replay_ref.is_none())
        }));
        let mut b = assistant_entry(Some(origin(&google)));
        b.message.content = vec![CanonicalContentPart::Text(
            ModelText::new("Gemini intermediate answer").unwrap(),
        )];
        b.message.tool_calls[0].call_id = ToolCallId::new("google-call").unwrap();
        let mut result_b = result_entry();
        result_b.message.tool_call_id = Some(ToolCallId::new("google-call").unwrap());
        let original_b = b.clone();
        let incoming =
            compile_provider_context(&[&a, &result_a, &b, &result_b], &openai, false).unwrap();
        assert_eq!(incoming.exact_assistant_entries, 1);
        assert_eq!(incoming.portable_assistant_entries, 1);
        assert_eq!(incoming.messages[0], a.message);
        assert!(incoming.messages.iter().flat_map(|message| &message.content).any(|part| matches!(part, CanonicalContentPart::Text(text) if text.as_str() == "Gemini intermediate answer")));
        assert_eq!(a, original_a);
        assert_eq!(b, original_b);
    }
}

#[test]
fn aggregate_request_usage_cannot_be_reused_as_a_physical_context_size() {
    let mut target = model_route("long-decode.route@1", "gemini-4-carbon", 1);
    target
        .capabilities
        .insert(DottedKey::new("usage_aggregates_requests").unwrap());
    let entry = assistant_entry(Some(origin(&target)));
    assert!(!target_route_owns_usage_anchor(&[&entry], entry.source_sequence, &target).unwrap());
}

#[test]
fn opaque_continuation_delta_may_lead_with_results_of_the_held_calls() {
    let target = model_route("target-route@1", "target-model", 2);
    let mut delta = result_entry();
    delta.continuation_delta_for = Some(musteragent_identity::InvocationId::generate());
    let compiled = compile_provider_context(&[&delta], &target, false)
        .expect("a continuation delta carries only results for calls the Provider holds");
    assert_eq!(compiled.messages, vec![delta.message.clone()]);

    // Without a continuation source the same leading result is an orphan.
    let orphan = result_entry();
    assert!(compile_provider_context(&[&orphan], &target, false).is_err());
    // A delta result cannot follow another turn: it is no longer leading.
    let mut user = result_entry();
    user.message = musteragent_runtime_model::CanonicalMessage {
        role: CanonicalRole::User,
        content: delta.message.content.clone(),
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    assert!(compile_provider_context(&[&user, &delta], &target, false).is_err());
}
