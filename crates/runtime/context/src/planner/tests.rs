mod dialogue_input;
mod projection;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use musteragent_identity::{
    ContentId, ContextSnapshotId, ConversationSeq, DottedKey, InvocationId, LedgerRecordId, RunId,
    VersionedKey,
};
use musteragent_model::{
    ApiFormat, ModelEntryId, ModelParameter, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_runtime_contract::ErrorCode;
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall, JsonValue, ModelText,
};
use musteragent_tool::WireName;

use crate::{
    CONTEXT_SUMMARY_SCHEMA, ConsumedStimulusCut, ContextEntry, ContextPlanError,
    ContextPresentationOrder, ContextSummaryDocument, ContextUsageAnchor, MaterializedContent,
    MaterializedContextBaseline,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_snapshot::PinnedAuxiliaryModelRoute;
use musteragent_runtime_snapshot::ResolvedContextBudgets;

use super::turn_order::{order_consumed_stimuli, order_serial_chat_turn};
use super::{
    CompactionSelectionBudgets, accumulated_file_operations,
    append_empty_response_recovery_instruction, atomic_context_units, can_fallback_to_uncompressed,
    coalesce_progress_observations, compact_entries, continuation_entries,
    delegation_completion_events, estimate_context_tokens, estimate_text_tokens,
    file_identity_default_from_schema, instruction_rendering::EMPTY_RESPONSE_RECOVERY_INSTRUCTION,
    planning_budget::PlanningBudgets, planning_budget::uncompressed_request_fits,
    present_message_for_route, present_messages_for_route, record_file_operation,
    render_history_summary, resolve_context_baseline, rolling_summary_source_tokens,
};

#[test]
fn queued_root_chat_presents_its_admission_stimulus_after_predecessor_suffix() {
    let predecessor = RunId::generate();
    let target = RunId::generate();
    let mut predecessor_input = user_entry(1, "predecessor input");
    predecessor_input.source_run_id = Some(predecessor);
    let mut target_input = user_entry(4, "target input");
    target_input.source_run_id = Some(target);
    let mut predecessor_answer = portable_assistant_entry(10, "predecessor answer");
    predecessor_answer.source_run_id = Some(predecessor);

    let entries = vec![predecessor_input, target_input, predecessor_answer];
    let ordered = order_serial_chat_turn(
        &entries,
        target,
        ConversationSeq::new(6).expect("admission sequence"),
        None,
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 10, 4],
    );
    assert_eq!(
        ordered.last().map(|entry| entry.message.role),
        Some(CanonicalRole::User),
    );
}

#[test]
fn queued_root_chat_keeps_its_own_execution_suffix_after_the_stimulus() {
    let predecessor = RunId::generate();
    let target = RunId::generate();
    let mut target_input = user_entry(4, "target input");
    target_input.source_run_id = Some(target);
    let mut predecessor_answer = portable_assistant_entry(10, "predecessor answer");
    predecessor_answer.source_run_id = Some(predecessor);
    let mut target_tool_call = assistant_tool_entry(12, &["call-a"]);
    target_tool_call.source_run_id = Some(target);

    let entries = vec![target_input, predecessor_answer, target_tool_call];
    let ordered = order_serial_chat_turn(
        &entries,
        target,
        ConversationSeq::new(6).expect("admission sequence"),
        None,
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [10, 4, 12],
    );
}

#[test]
fn missing_compression_route_is_reported_only_by_the_compaction_boundary() {
    let routes = Vec::<PinnedAuxiliaryModelRoute>::new();

    assert_eq!(
        super::auxiliary_model_route(&routes, musteragent_model::AuxiliaryModelRole::Compression),
        Err(ContextPlanError::model_route_unavailable()),
    );
}

#[test]
fn retry_presents_exact_original_user_after_failed_source_without_rewriting_provenance() {
    let source = RunId::generate();
    let retry = RunId::generate();
    let mut original = user_entry(1, "original request");
    original.source_run_id = Some(source);
    let original_record = original.source_record_ids[0];
    let mut partial = portable_assistant_entry(7, "partial failed answer");
    partial.source_run_id = Some(source);
    let entries = vec![original, partial];
    let ordered = order_serial_chat_turn(
        &entries,
        retry,
        ConversationSeq::new(10).expect("valid fixture for tests"),
        Some(original_record),
    );
    assert_eq!(
        ordered
            .iter()
            .map(|e| e.source_sequence.get())
            .collect::<Vec<_>>(),
        [7, 1]
    );
    assert_eq!(
        ordered
            .last()
            .expect("valid fixture for tests")
            .source_run_id,
        Some(source)
    );
    assert_eq!(
        ordered
            .last()
            .expect("valid fixture for tests")
            .source_record_ids,
        [original_record]
    );
    assert_eq!(entries[0].source_sequence.get(), 1);
}

#[test]
fn retry_stimulus_precedes_its_own_atomic_tool_exchange_on_following_iterations() {
    let source = RunId::generate();
    let retry = RunId::generate();
    let mut original = user_entry(1, "original request");
    original.source_run_id = Some(source);
    let original_record = original.source_record_ids[0];
    let mut partial = portable_assistant_entry(7, "partial failed answer");
    partial.source_run_id = Some(source);
    let mut call = assistant_tool_entry(12, &["retry-call"]);
    call.source_run_id = Some(retry);
    let entries = vec![original, partial, call];
    let ordered = order_serial_chat_turn(
        &entries,
        retry,
        ConversationSeq::new(10).expect("valid fixture for tests"),
        Some(original_record),
    );
    assert_eq!(
        ordered
            .iter()
            .map(|e| e.source_sequence.get())
            .collect::<Vec<_>>(),
        [7, 1, 12]
    );
    // The same explicit original identity also covers a retry of a failed retry.
    let chained = order_serial_chat_turn(
        &entries,
        RunId::generate(),
        ConversationSeq::new(20).expect("valid fixture for tests"),
        Some(original_record),
    );
    assert_eq!(
        chained
            .last()
            .expect("valid fixture for tests")
            .source_record_ids,
        [original_record]
    );
}

#[test]
fn consumed_follow_up_stimulus_is_presented_at_its_consumption_cut() {
    let objective = user_entry(1, "objective");
    let follow_up = user_entry(10, "follow-up while the first reply is in flight");
    let follow_up_record = follow_up.source_record_ids[0];
    let old_reply = portable_assistant_entry(12, "stale reply committed after the follow-up");
    let entries = [objective, follow_up, old_reply];

    let ordered = order_consumed_stimuli(
        entries.iter().collect(),
        &[ConsumedStimulusCut {
            stimulus_record_id: follow_up_record,
            consumed_sequence: ConversationSeq::new(13).expect("consumption cut"),
            kind: musteragent_run::RunControlKind::FollowUp,
        }],
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 12, 10],
    );
    assert_eq!(
        ordered.last().map(|entry| entry.message.role),
        Some(CanonicalRole::User),
    );
    // Presentation reorders references only; the materialized entries keep
    // their immutable Ledger order and provenance.
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 10, 12],
    );
    assert_eq!(
        ordered.last().expect("stimulus").source_record_ids,
        [follow_up_record]
    );
}

#[test]
fn stimuli_consumed_by_one_fence_keep_stable_ledger_order() {
    let objective = user_entry(1, "objective");
    let first = user_entry(10, "first follow-up");
    let second = user_entry(11, "second follow-up");
    let first_record = first.source_record_ids[0];
    let second_record = second.source_record_ids[0];
    let old_reply = portable_assistant_entry(12, "stale reply");
    let entries = [objective, first, second, old_reply];
    let cut = ConversationSeq::new(13).expect("consumption cut");

    let ordered = order_consumed_stimuli(
        entries.iter().collect(),
        &[
            ConsumedStimulusCut {
                stimulus_record_id: first_record,
                consumed_sequence: cut,
                kind: musteragent_run::RunControlKind::FollowUp,
            },
            ConsumedStimulusCut {
                stimulus_record_id: second_record,
                consumed_sequence: cut,
                kind: musteragent_run::RunControlKind::FollowUp,
            },
        ],
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 12, 10, 11],
    );
}

#[test]
fn stimuli_consumed_by_separate_fences_anchor_at_their_own_cuts() {
    let objective = user_entry(1, "objective");
    let steering = user_entry(8, "steer the in-flight invocation");
    let steering_record = steering.source_record_ids[0];
    let follow_up = user_entry(10, "follow-up");
    let follow_up_record = follow_up.source_record_ids[0];
    let old_reply = portable_assistant_entry(12, "stale reply");
    let entries = [objective, steering, follow_up, old_reply];

    let ordered = order_consumed_stimuli(
        entries.iter().collect(),
        &[
            ConsumedStimulusCut {
                stimulus_record_id: steering_record,
                consumed_sequence: ConversationSeq::new(9).expect("steering cut"),
                kind: musteragent_run::RunControlKind::Steering,
            },
            ConsumedStimulusCut {
                stimulus_record_id: follow_up_record,
                consumed_sequence: ConversationSeq::new(13).expect("follow-up cut"),
                kind: musteragent_run::RunControlKind::FollowUp,
            },
        ],
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 8, 12, 10],
    );
}

#[test]
fn consumption_cuts_move_only_their_exact_stimulus() {
    let objective = user_entry(1, "objective");
    let ordinary_user_turn = user_entry(10, "ordinary user turn without a cut");
    let reply = portable_assistant_entry(12, "reply");

    let untouched = order_consumed_stimuli(
        [&objective, &ordinary_user_turn, &reply]
            .into_iter()
            .collect(),
        &[],
    );
    assert_eq!(
        untouched
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 10, 12],
    );

    let unrelated_cut = order_consumed_stimuli(
        [&objective, &ordinary_user_turn, &reply]
            .into_iter()
            .collect(),
        &[ConsumedStimulusCut {
            stimulus_record_id: LedgerRecordId::generate(),
            consumed_sequence: ConversationSeq::new(13).expect("consumption cut"),
            kind: musteragent_run::RunControlKind::FollowUp,
        }],
    );
    assert_eq!(
        unrelated_cut
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 10, 12],
    );
}

#[test]
fn root_chat_admission_order_composes_with_consumption_cuts() {
    let predecessor = RunId::generate();
    let target = RunId::generate();
    let mut predecessor_input = user_entry(1, "predecessor input");
    predecessor_input.source_run_id = Some(predecessor);
    let mut target_input = user_entry(4, "target input");
    target_input.source_run_id = Some(target);
    let mut predecessor_answer = portable_assistant_entry(10, "predecessor answer");
    predecessor_answer.source_run_id = Some(predecessor);
    let mut follow_up = user_entry(12, "follow-up admitted mid-invocation");
    follow_up.source_run_id = Some(target);
    let follow_up_record = follow_up.source_record_ids[0];
    let mut old_reply = portable_assistant_entry(13, "stale target reply");
    old_reply.source_run_id = Some(target);
    let entries = vec![
        predecessor_input,
        target_input,
        predecessor_answer,
        follow_up,
        old_reply,
    ];

    let admitted = order_serial_chat_turn(
        &entries,
        target,
        ConversationSeq::new(6).expect("admission sequence"),
        None,
    );
    let ordered = order_consumed_stimuli(
        admitted,
        &[ConsumedStimulusCut {
            stimulus_record_id: follow_up_record,
            consumed_sequence: ConversationSeq::new(14).expect("consumption cut"),
            kind: musteragent_run::RunControlKind::FollowUp,
        }],
    );

    assert_eq!(
        ordered
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        [1, 10, 4, 13, 12],
    );
    assert_eq!(
        ordered.last().map(|entry| entry.message.role),
        Some(CanonicalRole::User),
    );
}

#[test]
fn visual_route_preserves_authorized_image_pixels() {
    let message = image_message();
    let mut route = model_route();
    route.capabilities = BTreeSet::from([DottedKey::new("vision").expect("vision")]);

    let presented = present_message_for_route(&message, &route).expect("present visual message");
    assert_eq!(presented, message);
}

#[test]
fn visual_route_keeps_image_pixels_but_routes_documents_through_the_reader() {
    let document = document_part(b"docx-fixture");
    let document_id = match &document {
        CanonicalContentPart::File(reference) => reference.content_id.to_string(),
        _ => panic!("fixture must contain a document"),
    };
    let image = image_part(b"image-pixels");
    let image_id = match &image {
        CanonicalContentPart::Image(reference) => reference.content_id,
        _ => panic!("fixture must contain an image"),
    };
    let message = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![
            CanonicalContentPart::Text(ModelText::new("compare these").expect("text")),
            image,
            document,
        ],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let mut route = model_route();
    route.capabilities = BTreeSet::from([DottedKey::new("vision").expect("vision")]);

    let presented = present_message_for_route(&message, &route)
        .expect("present mixed visual and document message");

    assert!(presented.content.iter().any(|part| {
        matches!(part, CanonicalContentPart::Image(reference) if reference.content_id == image_id)
    }));
    assert!(
        !presented
            .content
            .iter()
            .any(|part| matches!(part, CanonicalContentPart::File(_)))
    );
    let document_hint = presented
        .content
        .iter()
        .find_map(|part| match part {
            CanonicalContentPart::Text(text)
                if text
                    .as_str()
                    .contains("file-reading tools in the current callable surface") =>
            {
                Some(text.as_str())
            }
            CanonicalContentPart::Text(_)
            | CanonicalContentPart::Image(_)
            | CanonicalContentPart::Audio(_)
            | CanonicalContentPart::Video(_)
            | CanonicalContentPart::File(_) => None,
        })
        .expect("document reader hint");
    assert!(document_hint.contains(&document_id));
    assert!(presented.content.iter().all(|part| {
        !matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains(&image_id.to_string()))
    }));
}

#[test]
fn route_sends_documents_natively_only_when_its_protocol_carries_the_type() {
    let pdf = pdf_part(b"%PDF-fixture");
    let docx = document_part(b"docx-fixture");
    let (pdf_id, docx_id) = match (&pdf, &docx) {
        (CanonicalContentPart::File(a), CanonicalContentPart::File(b)) => {
            (a.content_id, b.content_id)
        }
        _ => panic!("fixtures must contain documents"),
    };
    let message = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![pdf.clone(), docx.clone()],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let mut route = model_route();
    route.capabilities =
        BTreeSet::from([DottedKey::new("document_input").expect("document input")]);

    // Anthropic carries PDF natively; the Word file is read by tools.
    route.api_format = ApiFormat::AnthropicMessages;
    let presented = present_message_for_route(&message, &route).expect("present documents");
    assert!(presented.content.iter().any(|part| {
        matches!(part, CanonicalContentPart::File(reference) if reference.content_id == pdf_id)
    }));
    assert!(!presented.content.iter().any(|part| {
        matches!(part, CanonicalContentPart::File(reference) if reference.content_id == docx_id)
    }));
    assert!(presented.content.iter().any(|part| {
        matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains(&docx_id.to_string()))
    }));

    // Bedrock Converse carries Word documents as well.
    route.api_format = ApiFormat::BedrockConverse;
    let presented = present_message_for_route(&message, &route).expect("present documents");
    assert_eq!(presented, message);

    // OpenAI-compatible vendors reject the `file` / `input_file` part on both
    // OpenAI wires, so even a declared document is read through the file tools.
    for format in [ApiFormat::OpenAiChatCompletions, ApiFormat::OpenAiResponses] {
        route.api_format = format;
        let presented = present_message_for_route(&message, &route).expect("present documents");
        assert!(
            !presented
                .content
                .iter()
                .any(|part| matches!(part, CanonicalContentPart::File(_)))
        );
        assert!(presented.content.iter().any(|part| {
            matches!(part, CanonicalContentPart::Text(text) if text.as_str().contains(&pdf_id.to_string()))
        }));
    }

    // Without a document contract nothing is sent as original bytes.
    route.api_format = ApiFormat::BedrockConverse;
    route.capabilities.clear();
    let presented = present_message_for_route(&message, &route).expect("present documents");
    assert!(
        !presented
            .content
            .iter()
            .any(|part| matches!(part, CanonicalContentPart::File(_)))
    );
}

#[test]
fn route_keeps_native_documents_within_the_request_budget_newest_first() {
    let older = pdf_part(b"%PDF-older");
    let newer = (0..super::route_presentation::MAX_NATIVE_DOCUMENTS_PER_REQUEST)
        .map(|index| pdf_part(format!("%PDF-{index}").as_bytes()))
        .collect::<Vec<_>>();
    let message = |content| CanonicalMessage {
        role: CanonicalRole::User,
        content,
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let first = message(vec![older]);
    let second = message(newer.clone());
    let mut route = model_route();
    route.api_format = ApiFormat::AnthropicMessages;
    route.capabilities = BTreeSet::from([DottedKey::new("input.document").expect("document")]);

    let presented = present_messages_for_route(&[&first, &second], &route).expect("present");
    assert!(matches!(
        presented[0].content.as_slice(),
        [CanonicalContentPart::Text(_)]
    ));
    assert_eq!(presented[1], second);
}

#[test]
fn route_turns_audio_and_video_it_cannot_carry_into_a_notice() {
    let audio = CanonicalContentPart::Audio(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("audio/mpeg").expect("media type"),
        byte_length: 5,
        sha256: Sha256::digest(b"audio"),
    });
    let message = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![audio.clone()],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };
    let mut route = model_route();

    let presented = present_message_for_route(&message, &route).expect("present audio");
    let [CanonicalContentPart::Text(notice)] = presented.content.as_slice() else {
        panic!("audio must become one notice");
    };
    assert!(notice.as_str().contains("cannot hear or watch audio"));

    route.api_format = ApiFormat::GoogleGenerateContent;
    route.capabilities = BTreeSet::from([DottedKey::new("input.audio").expect("audio")]);
    assert_eq!(
        present_message_for_route(&message, &route).expect("present audio"),
        message
    );
}

#[test]
fn visual_route_keeps_the_newest_capacity_bounded_images_and_exposes_omitted_refs() {
    let mut message = image_message();
    message.content.push(image_part(b"second-image"));
    message.content.push(image_part(b"third-image"));
    let omitted_id = match &message.content[2] {
        CanonicalContentPart::Image(reference) => reference.content_id.to_string(),
        _ => panic!("fixture must contain an image"),
    };
    let retained_ids = message.content[..2]
        .iter()
        .map(|part| match part {
            CanonicalContentPart::Image(reference) => reference.content_id,
            _ => panic!("fixture must contain an image"),
        })
        .collect::<Vec<_>>();
    let mut route = model_route();
    route.capabilities = BTreeSet::from([DottedKey::new("vision").expect("vision")]);
    for (name, value) in [
        ("vision.max_images_per_request", 2),
        ("vision.max_bytes_per_image", 1_024),
        ("vision.max_total_image_bytes", 2_048),
    ] {
        route.parameters.insert(
            DottedKey::new(name).expect("parameter"),
            ModelParameter::Int(value),
        );
    }

    let presented = present_messages_for_route(&[&message], &route)
        .expect("present capacity-bounded visual message")
        .pop()
        .expect("message");
    assert_eq!(presented.content.len(), 3);
    let CanonicalContentPart::Text(hint) = &presented.content[2] else {
        panic!("omitted images must be identified in their original position");
    };
    assert!(hint.as_str().contains(&omitted_id));
    assert!(hint.as_str().contains("vision_analyze"));
    for retained_id in &retained_ids {
        assert!(!hint.as_str().contains(&retained_id.to_string()));
    }
    assert_eq!(
        presented.content[..2]
            .iter()
            .map(|part| match part {
                CanonicalContentPart::Image(reference) => reference.content_id,
                _ => panic!("retained parts must be images"),
            })
            .collect::<Vec<_>>(),
        retained_ids
    );
}

#[test]
fn visual_route_marks_an_oversized_image_as_tool_required_without_sending_pixels() {
    let message = image_message();
    let image_id = match &message.content[0] {
        CanonicalContentPart::Image(reference) => reference.content_id.to_string(),
        _ => panic!("fixture must contain an image"),
    };
    let mut route = model_route();
    route.capabilities = BTreeSet::from([DottedKey::new("vision").expect("vision")]);
    for (name, value) in [
        ("vision.max_images_per_request", 1),
        ("vision.max_bytes_per_image", 1),
        ("vision.max_total_image_bytes", 1),
    ] {
        route.parameters.insert(
            DottedKey::new(name).expect("parameter"),
            ModelParameter::Int(value),
        );
    }

    let presented = present_message_for_route(&message, &route)
        .expect("present oversized image for visual route");

    assert_eq!(presented.content.len(), 1);
    let CanonicalContentPart::Text(hint) = &presented.content[0] else {
        panic!("oversized image must be replaced by an access-state annotation");
    };
    assert!(hint.as_str().contains("vision_tool_required"));
    assert!(hint.as_str().contains(&image_id));
    assert!(!hint.as_str().contains("native_loaded"));
    assert!(hint.as_str().contains("vision_analyze"));
}

#[test]
fn nonvisual_route_replaces_image_pixels_with_authorized_tool_reference() {
    let message = image_message();
    let content_id = match &message.content[0] {
        CanonicalContentPart::Image(reference) => reference.content_id.to_string(),
        _ => panic!("fixture must contain an image"),
    };

    let presented =
        present_message_for_route(&message, &model_route()).expect("present text-only message");
    assert_eq!(presented.content.len(), 1);
    let CanonicalContentPart::Text(reference) = &presented.content[0] else {
        panic!("nonvisual route must never receive image pixels");
    };
    assert!(reference.as_str().contains(&content_id));
    assert!(reference.as_str().contains("vision_analyze"));
    assert!(reference.as_str().contains("vision_tool_required"));
    assert!(!reference.as_str().contains("native_loaded"));
}

#[test]
fn nonvisual_route_replaces_document_files_with_authorized_reader_reference() {
    let document = document_part(b"docx-fixture");
    let content_id = match &document {
        CanonicalContentPart::File(reference) => reference.content_id.to_string(),
        _ => panic!("fixture must contain a document"),
    };
    let message = CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![
            CanonicalContentPart::Text(ModelText::new("summarize this").expect("text")),
            document,
        ],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    };

    let presented = present_message_for_route(&message, &model_route())
        .expect("present document attachment for a text-only route");
    assert_eq!(presented.content.len(), 2);
    let CanonicalContentPart::Text(user_text) = &presented.content[0] else {
        panic!("user text must be preserved");
    };
    assert_eq!(user_text.as_str(), "summarize this");
    let CanonicalContentPart::Text(hint) = &presented.content[1] else {
        panic!("nonvisual route must never receive an opaque document file");
    };
    assert!(hint.as_str().contains(&content_id));
    assert!(
        hint.as_str()
            .contains("file-reading tools in the current callable surface")
    );
}

fn image_message() -> CanonicalMessage {
    let bytes = b"image-pixels";
    CanonicalMessage {
        role: CanonicalRole::User,
        content: vec![image_part(bytes)],
        tool_calls: Vec::new(),
        tool_call_id: None,
        tool_name: None,
        reasoning: None,
    }
}

fn image_part(bytes: &[u8]) -> CanonicalContentPart {
    CanonicalContentPart::Image(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("image/png").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("byte length"),
        sha256: Sha256::digest(bytes),
    })
}

fn pdf_part(bytes: &[u8]) -> CanonicalContentPart {
    CanonicalContentPart::File(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/pdf").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("byte length"),
        sha256: Sha256::digest(bytes),
    })
}

fn document_part(bytes: &[u8]) -> CanonicalContentPart {
    CanonicalContentPart::File(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        .expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("byte length"),
        sha256: Sha256::digest(bytes),
    })
}

pub(super) fn model_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("cloud.context-vision-test@1").expect("route ref"),
        resolved_model_key: "context-vision-test".to_owned(),
        model_provider_id: ModelProviderId::from_uuid(uuid::Uuid::from_u128(
            0x1111_1111_1111_4111_8111_1111_1111_1111,
        ))
        .expect("provider id"),
        model_entry_id: ModelEntryId::from_uuid(uuid::Uuid::from_u128(
            0x2222_2222_2222_4222_8222_2222_2222_2222,
        ))
        .expect("entry id"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(128_000).expect("context window"),
        max_output_tokens: NonZeroU64::new(4_096).expect("max output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:context-vision-test@1")
            .expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    }
}

#[test]
fn empty_response_recovery_instruction_is_private_and_conditional() {
    let mut instructions = Vec::new();
    append_empty_response_recovery_instruction(&mut instructions, 0)
        .expect("normal request has no recovery directive");
    assert!(instructions.is_empty());

    append_empty_response_recovery_instruction(&mut instructions, 1)
        .expect("empty response recovery directive");
    assert_eq!(instructions.len(), 1);
    assert_eq!(
        instructions[0].as_str(),
        EMPTY_RESPONSE_RECOVERY_INSTRUCTION
    );
}

#[test]
fn asynchronous_completion_context_preserves_order_and_exact_provenance() {
    use musteragent_identity::{ActivityId, DelegationDeliveryId, DelegationId, RunId};

    let first_delivery = DelegationDeliveryId::generate();
    let first_delegation = DelegationId::generate();
    let first_activity = ActivityId::generate();
    let first_run = RunId::generate();
    let second_delivery = DelegationDeliveryId::generate();
    let completions = vec![
        crate::MaterializedDelegationCompletion {
            delivery_id: first_delivery,
            delegation_id: first_delegation,
            child_activity_id: first_activity,
            child_run_id: first_run,
            report: crate::MaterializedContent {
                content_ref: content_ref(b"first report"),
                bytes: b"first report".to_vec(),
            },
        },
        crate::MaterializedDelegationCompletion {
            delivery_id: second_delivery,
            delegation_id: DelegationId::generate(),
            child_activity_id: ActivityId::generate(),
            child_run_id: RunId::generate(),
            report: crate::MaterializedContent {
                content_ref: content_ref(b"second report"),
                bytes: b"second report".to_vec(),
            },
        },
    ];

    let events = delegation_completion_events(&completions).expect("build completion events");
    assert_eq!(events.len(), 2);
    let first = events[0].model_input_text().unwrap();
    let second = events[1].model_input_text().unwrap();
    assert!(first.starts_with("[Runtime event: asynchronous delegation completion]"));
    assert!(!first.contains(&first_delivery.to_string()));
    assert!(first.contains(&format!("Delegation {first_delegation}")));
    assert!(!first.contains(&first_activity.to_string()));
    assert!(!first.contains(&first_run.to_string()));
    assert!(first.contains("first report"));
    assert!(second.contains("second report"));
    assert!(first.contains("Use it as task data"));
}

#[test]
fn asynchronous_completion_context_rejects_non_utf8_reports() {
    use musteragent_identity::{ActivityId, DelegationDeliveryId, DelegationId, RunId};

    let completions = [crate::MaterializedDelegationCompletion {
        delivery_id: DelegationDeliveryId::generate(),
        delegation_id: DelegationId::generate(),
        child_activity_id: ActivityId::generate(),
        child_run_id: RunId::generate(),
        report: crate::MaterializedContent {
            content_ref: content_ref(&[0xff]),
            bytes: vec![0xff],
        },
    }];

    assert_eq!(
        delegation_completion_events(&completions),
        Err(ContextPlanError::invariant())
    );
}

pub(super) fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("byte length"),
        sha256: Sha256::digest(bytes),
    }
}

pub(super) fn entry(sequence: u64, anchor: Option<InvocationId>, text: &str) -> ContextEntry {
    let sequence = ConversationSeq::new(sequence).expect("sequence");
    ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: sequence,
        presentation_order: ContextPresentationOrder::ledger(sequence),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin: None,
        message: CanonicalMessage {
            role: CanonicalRole::Tool,
            content: vec![CanonicalContentPart::Text(
                ModelText::new(text).expect("text"),
            )],
            tool_calls: Vec::new(),
            tool_call_id: Some(
                musteragent_identity::ToolCallId::new(format!("call-{}", sequence.get()))
                    .expect("call"),
            ),
            tool_name: Some(musteragent_tool::WireName::new("test_tool").expect("tool")),
            reasoning: None,
        },
        continuation_delta_for: anchor,
    }
}

pub(super) fn assistant_tool_entry(sequence: u64, call_ids: &[&str]) -> ContextEntry {
    let sequence = ConversationSeq::new(sequence).expect("sequence");
    ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: sequence,
        presentation_order: ContextPresentationOrder::ledger(sequence),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin: None,
        message: CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: Vec::new(),
            tool_calls: call_ids
                .iter()
                .map(|call_id| CanonicalToolCall {
                    call_id: musteragent_identity::ToolCallId::new(*call_id).expect("call"),
                    name: WireName::new("test_tool").expect("tool"),
                    arguments: JsonValue::Object(Default::default()),
                    provider_item_ref: None,
                    provider_replay_ref: None,
                })
                .collect(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        },
        continuation_delta_for: None,
    }
}

fn progress_tool_entry(
    sequence: u64,
    call_id: &str,
    wire_name: &str,
    handles: &[&str],
    visible_text: Option<&str>,
) -> ContextEntry {
    let sequence = ConversationSeq::new(sequence).expect("sequence");
    ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: sequence,
        presentation_order: ContextPresentationOrder::ledger(sequence),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin: None,
        message: CanonicalMessage {
            role: CanonicalRole::Assistant,
            content: visible_text
                .map(|text| {
                    vec![CanonicalContentPart::Text(
                        ModelText::new(text).expect("visible text"),
                    )]
                })
                .unwrap_or_default(),
            tool_calls: vec![CanonicalToolCall {
                call_id: musteragent_identity::ToolCallId::new(call_id).expect("call"),
                name: WireName::new(wire_name).expect("tool"),
                arguments: JsonValue::Object(BTreeMap::from([(
                    "handles".to_owned(),
                    JsonValue::Array(
                        handles
                            .iter()
                            .map(|handle| JsonValue::String((*handle).to_owned()))
                            .collect(),
                    ),
                )])),
                provider_item_ref: None,
                provider_replay_ref: None,
            }],
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        },
        continuation_delta_for: None,
    }
}

fn progress_tool_result(sequence: u64, call_id: &str, text: &str) -> ContextEntry {
    let mut result = entry(sequence, None, text);
    result.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new(call_id).expect("call"));
    result
}

#[test]
fn opaque_continuation_selects_only_the_exact_invocation_delta() {
    let first = InvocationId::generate();
    let second = InvocationId::generate();
    let entries = vec![
        entry(1, None, "history"),
        entry(2, Some(first), "first result"),
        entry(3, Some(second), "second result"),
    ];
    let selected = continuation_entries(&entries, second);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].source_sequence.get(), 3);
}

#[test]
fn repeated_progress_observations_keep_only_the_newest_atomic_exchange() {
    let old_call = progress_tool_entry(1, "old", "delegate_join", &["a", "b"], None);
    let old_result = progress_tool_result(2, "old", "both running at revision 4");
    let new_call = progress_tool_entry(3, "new", "delegate_join", &["a", "b"], None);
    let new_result = progress_tool_result(4, "new", "a completed; b running at revision 9");
    let entries = [old_call, old_result, new_call, new_result];

    let entry_refs = entries.iter().collect::<Vec<_>>();
    let projected = coalesce_progress_observations(&entry_refs).expect("coalesce observations");

    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].source_sequence.get(), 3);
    assert_eq!(projected[1].source_sequence.get(), 4);
}

#[test]
fn progress_observations_for_different_targets_remain_independent() {
    let first_call = progress_tool_entry(1, "first", "delegate_inspect", &["a"], None);
    let first_result = progress_tool_result(2, "first", "a running");
    let second_call = progress_tool_entry(3, "second", "delegate_inspect", &["b"], None);
    let second_result = progress_tool_result(4, "second", "b completed");
    let entries = [first_call, first_result, second_call, second_result];

    let entry_refs = entries.iter().collect::<Vec<_>>();
    let projected = coalesce_progress_observations(&entry_refs).expect("coalesce observations");

    assert_eq!(projected.len(), 4);
}

#[test]
fn progress_tool_calls_with_visible_assistant_content_are_not_coalesced() {
    let first_call = progress_tool_entry(
        1,
        "first",
        "delegate_join",
        &["a"],
        Some("I will report this intermediate checkpoint."),
    );
    let first_result = progress_tool_result(2, "first", "a running");
    let second_call = progress_tool_entry(3, "second", "delegate_join", &["a"], None);
    let second_result = progress_tool_result(4, "second", "a completed");
    let entries = [first_call, first_result, second_call, second_result];

    let entry_refs = entries.iter().collect::<Vec<_>>();
    let projected = coalesce_progress_observations(&entry_refs).expect("coalesce observations");

    assert_eq!(projected.len(), 4);
}

#[test]
fn physical_usage_anchors_only_the_tail_estimate() {
    let before_anchor = entry(1, None, "this prefix is already physically measured");
    let at_anchor = entry(2, None, "assistant response already in total usage");
    let tail = entry(3, None, "1234567890abcdef");
    let entries = [&before_anchor, &at_anchor, &tail];
    assert_eq!(
        estimate_context_tokens(
            999,
            &entries,
            Some(&ContextUsageAnchor {
                source_sequence: ConversationSeq::new(2).expect("anchor"),
                total_tokens: 100,
            }),
        ),
        106,
    );
}

#[test]
fn unicode_tail_estimate_is_conservative_without_assuming_one_provider_tokenizer() {
    assert_eq!(estimate_text_tokens("abcdef"), 2);
    assert_eq!(estimate_text_tokens("中文"), 4);
    assert_eq!(estimate_text_tokens("🙂"), 3);
    assert!(estimate_text_tokens(&"上下文".repeat(100_000)) >= 600_000);
}

#[test]
fn file_operations_use_pinned_json_pointer_metadata() {
    let mut read = BTreeSet::new();
    let mut written = BTreeSet::new();
    let mut edited = BTreeSet::new();
    record_file_operation(
        &musteragent_tool::FileOperationMetadata {
            kind: musteragent_tool::FileOperationKind::Read,
            identity_argument: "/document_id".to_owned(),
        },
        &JsonValue::Object(BTreeMap::from([(
            "document_id".to_owned(),
            JsonValue::String("document-123".to_owned()),
        )])),
        None,
        &mut read,
        &mut written,
        &mut edited,
    )
    .expect("extract file identity");
    assert_eq!(read, BTreeSet::from(["document-123".to_owned()]));
    assert!(written.is_empty() && edited.is_empty());
}

#[test]
fn file_operations_apply_the_optional_default_from_the_pinned_tool_schema() {
    let schema = br#"{
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object",
        "properties":{"path":{"type":"string","default":"."}},
        "required":["glob"]
    }"#;
    let default = file_identity_default_from_schema(schema, "/path")
        .expect("decode pinned schema")
        .expect("optional path default");
    assert_eq!(default, ".");

    let metadata = musteragent_tool::FileOperationMetadata {
        kind: musteragent_tool::FileOperationKind::Read,
        identity_argument: "/path".to_owned(),
    };
    let mut read = BTreeSet::new();
    let mut written = BTreeSet::new();
    let mut edited = BTreeSet::new();
    record_file_operation(
        &metadata,
        &JsonValue::Object(BTreeMap::from([(
            "glob".to_owned(),
            JsonValue::String("**/*.rs".to_owned()),
        )])),
        Some(&default),
        &mut read,
        &mut written,
        &mut edited,
    )
    .expect("project workspace-root file operation");

    assert_eq!(read, BTreeSet::from([".".to_owned()]));
    assert!(written.is_empty() && edited.is_empty());
}

#[test]
fn builtin_root_scoped_file_search_contracts_pin_the_workspace_identity_default() {
    let catalog =
        musteragent_tool::BuiltinToolCatalog::load_device().expect("built-in Device Tool catalog");
    for wire_name in ["find_files", "search_files"] {
        let definition = catalog
            .definitions()
            .iter()
            .find(|definition| definition.wire_name.as_str() == wire_name)
            .expect("root-scoped file search definition");
        let metadata = definition
            .file_operation
            .as_ref()
            .expect("file operation metadata");
        assert_eq!(metadata.identity_argument, "/path");
        assert_eq!(
            file_identity_default_from_schema(
                &definition.input_schema_json,
                &metadata.identity_argument,
            )
            .expect("valid pinned input schema")
            .as_deref(),
            Some("."),
            "{wire_name} must keep root-scope omission and Context projection aligned",
        );
    }
}

#[test]
fn explicit_file_identity_wins_and_invalid_explicit_values_never_fall_back() {
    let metadata = musteragent_tool::FileOperationMetadata {
        kind: musteragent_tool::FileOperationKind::Read,
        identity_argument: "/path".to_owned(),
    };
    let mut read = BTreeSet::new();
    let mut written = BTreeSet::new();
    let mut edited = BTreeSet::new();
    record_file_operation(
        &metadata,
        &JsonValue::Object(BTreeMap::from([(
            "path".to_owned(),
            JsonValue::String("src".to_owned()),
        )])),
        Some("."),
        &mut read,
        &mut written,
        &mut edited,
    )
    .expect("explicit identity");
    assert_eq!(read, BTreeSet::from(["src".to_owned()]));

    assert_eq!(
        record_file_operation(
            &metadata,
            &JsonValue::Object(BTreeMap::from([("path".to_owned(), JsonValue::Null,)])),
            Some("."),
            &mut read,
            &mut written,
            &mut edited,
        ),
        Err(ContextPlanError::invariant()),
        "a present but invalid identity is corrupted input, not an omitted optional field",
    );
    assert_eq!(
        record_file_operation(
            &metadata,
            &JsonValue::Object(BTreeMap::new()),
            None,
            &mut read,
            &mut written,
            &mut edited,
        ),
        Err(ContextPlanError::invariant()),
        "a required identity without a pinned default must still fail closed",
    );
}

#[test]
fn rolling_compaction_starts_from_the_complete_prior_file_operation_union() {
    let previous = ContextSummaryDocument {
        schema_version: CONTEXT_SUMMARY_SCHEMA,
        compacted: true,
        source_seq_from: ConversationSeq::new(1).expect("from"),
        source_seq_to: ConversationSeq::new(2).expect("to"),
        summary: Some("checkpoint".to_owned()),
        generator_invocation_id: Some(InvocationId::generate()),
        previous_summary_ref: None,
        summarized_record_ids: vec![LedgerRecordId::generate()],
        file_operations: crate::ContextFileOperations {
            read: vec!["/a".to_owned()],
            written: vec!["/b".to_owned()],
            edited: vec!["/c".to_owned()],
        },
        items: Vec::new(),
        omitted_item_count: 0,
    };
    let (read, written, edited) = accumulated_file_operations(Some(&previous));
    assert_eq!(read, BTreeSet::from(["/a".to_owned()]));
    assert_eq!(written, BTreeSet::from(["/b".to_owned()]));
    assert_eq!(edited, BTreeSet::from(["/c".to_owned()]));
}

#[test]
fn durable_compaction_boundary_becomes_the_next_iteration_context_baseline() {
    let entries = (1..=6)
        .map(|sequence| user_entry(sequence, &format!("message {sequence}")))
        .collect::<Vec<_>>();
    let summary = ContextSummaryDocument {
        schema_version: CONTEXT_SUMMARY_SCHEMA,
        compacted: true,
        source_seq_from: entries[0].source_sequence,
        source_seq_to: entries[1].source_sequence,
        summary: Some("durable checkpoint".to_owned()),
        generator_invocation_id: Some(InvocationId::generate()),
        previous_summary_ref: None,
        summarized_record_ids: entries[..2]
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied())
            .collect(),
        file_operations: Default::default(),
        items: Vec::new(),
        omitted_item_count: 0,
    };
    let baseline = MaterializedContextBaseline {
        snapshot_id: ContextSnapshotId::generate(),
        source_seq_to: entries[3].source_sequence,
        retained_record_ids: entries[2..4]
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied())
            .collect(),
        summary: MaterializedContent {
            content_ref: content_ref(b"summary"),
            bytes: b"summary".to_vec(),
        },
    };
    let logical = entries.iter().collect::<Vec<_>>();

    let resolved = resolve_context_baseline(Some(&baseline), &logical, Some(&summary))
        .expect("resolve durable baseline");

    assert_eq!(
        resolved.context.map(|value| value.snapshot_id),
        Some(baseline.snapshot_id)
    );
    assert_eq!(
        resolved
            .entries
            .iter()
            .map(|entry| entry.source_sequence.get())
            .collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
    let effective_tokens = estimate_context_tokens(10, &resolved.entries, None);
    let (retained, compacted) = compact_entries(
        &resolved.entries,
        10,
        selection(1_000, effective_tokens, effective_tokens, 900, Some(200)),
        false,
    )
    .expect("post-compaction tail remains below the trigger");
    assert_eq!(retained.len(), 4);
    assert!(compacted.is_empty());
}

#[test]
fn branch_scoped_checkpoint_survives_semantic_projection_of_summarized_audit_records() {
    let entries = (3..=5)
        .map(|sequence| user_entry(sequence, &format!("message {sequence}")))
        .collect::<Vec<_>>();
    let summarized_audit_record_id = LedgerRecordId::generate();
    let summary = ContextSummaryDocument {
        schema_version: CONTEXT_SUMMARY_SCHEMA,
        compacted: true,
        source_seq_from: ConversationSeq::new(1).expect("from"),
        source_seq_to: ConversationSeq::new(2).expect("to"),
        summary: Some("checkpoint with audit-only summarized provenance".to_owned()),
        generator_invocation_id: Some(InvocationId::generate()),
        previous_summary_ref: None,
        summarized_record_ids: vec![summarized_audit_record_id],
        file_operations: Default::default(),
        items: Vec::new(),
        omitted_item_count: 0,
    };
    let baseline = MaterializedContextBaseline {
        snapshot_id: ContextSnapshotId::generate(),
        source_seq_to: entries[1].source_sequence,
        retained_record_ids: entries[..2]
            .iter()
            .flat_map(|entry| entry.source_record_ids.iter().copied())
            .collect(),
        summary: MaterializedContent {
            content_ref: content_ref(b"summary"),
            bytes: b"summary".to_vec(),
        },
    };
    let logical = entries.iter().collect::<Vec<_>>();

    let resolved = resolve_context_baseline(Some(&baseline), &logical, Some(&summary))
        .expect("reuse branch-scoped checkpoint");

    assert_eq!(
        resolved.context.map(|value| value.snapshot_id),
        Some(baseline.snapshot_id)
    );
    assert_eq!(resolved.summary, Some(&summary));
    assert_eq!(resolved.entries, logical);
}

#[test]
fn rolling_summary_budget_accounts_for_the_previous_checkpoint_and_delta() {
    let delta = user_entry(2, "small delta");
    let previous = ContextSummaryDocument {
        schema_version: CONTEXT_SUMMARY_SCHEMA,
        compacted: true,
        source_seq_from: ConversationSeq::new(1).expect("from"),
        source_seq_to: ConversationSeq::new(1).expect("to"),
        summary: Some("checkpoint ".repeat(1_024)),
        generator_invocation_id: Some(InvocationId::generate()),
        previous_summary_ref: None,
        summarized_record_ids: vec![LedgerRecordId::generate()],
        file_operations: Default::default(),
        items: Vec::new(),
        omitted_item_count: 0,
    };

    let delta_tokens = super::estimate_entries(&[&delta]);
    let rolling_tokens = rolling_summary_source_tokens(Some(&previous), &[&delta]);

    assert!(rolling_tokens > delta_tokens);
    assert_eq!(
        rolling_tokens,
        estimate_text_tokens(previous.summary.as_deref().expect("summary")) + delta_tokens
    );
}

#[test]
fn tool_call_and_all_parallel_results_form_one_context_unit() {
    let assistant = assistant_tool_entry(1, &["call-a", "call-b"]);
    let first = entry(2, None, "first");
    let second = entry(3, None, "second");
    let user = ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: ConversationSeq::new(4).expect("sequence"),
        presentation_order: ContextPresentationOrder::ledger(
            ConversationSeq::new(4).expect("sequence"),
        ),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin: None,
        message: CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(
                ModelText::new("next").expect("text"),
            )],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        },
        continuation_delta_for: None,
    };
    let mut first = first;
    first.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new("call-a").expect("call"));
    let mut second = second;
    second.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new("call-b").expect("call"));
    let entries = [&assistant, &first, &second, &user];
    let units = atomic_context_units(&entries, false).expect("units");
    assert_eq!(units.len(), 2);
    assert_eq!((units[0].start, units[0].end), (0, 3));
    assert_eq!((units[1].start, units[1].end), (3, 4));
}

#[test]
fn stateless_context_rejects_orphan_tool_results() {
    let tool = entry(1, None, "orphan");
    assert!(atomic_context_units(&[&tool], false).is_err());
    assert!(atomic_context_units(&[&tool], true).is_ok());
}

#[test]
fn compaction_keeps_recent_parallel_tool_exchanges_as_atomic_units() {
    let old_text = "old request ".repeat(64);
    let old_user = user_entry(1, &old_text);
    let old_assistant = assistant_tool_entry(2, &["old-a", "old-b", "old-c"]);
    let mut old_a = entry(3, None, "old result a");
    old_a.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new("old-a").expect("old-a call"));
    let mut old_b = entry(4, None, "old result b");
    old_b.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new("old-b").expect("old-b call"));
    let mut old_c = entry(5, None, "old result c");
    old_c.message.tool_call_id =
        Some(musteragent_identity::ToolCallId::new("old-c").expect("old-c call"));
    let recent_user = user_entry(6, "recent request");
    let entries = [
        &old_user,
        &old_assistant,
        &old_a,
        &old_b,
        &old_c,
        &recent_user,
    ];

    let (retained, compacted) = compact_entries(
        &entries,
        0,
        selection(512, u64::MAX, u64::MAX, 1, None),
        false,
    )
    .expect("protected compaction");

    assert_eq!(compacted.len(), 1);
    assert_eq!(compacted[0].source_sequence.get(), 1);
    assert_eq!(retained.len(), 5);
    assert_eq!(retained[0].source_sequence.get(), 2);
}

#[test]
fn deterministic_hard_window_forces_compaction_when_provider_usage_underreports() {
    let old = user_entry(1, &"old ".repeat(100));
    let middle = user_entry(2, &"middle ".repeat(100));
    let recent = user_entry(3, &"recent ".repeat(100));
    let entries = [&old, &middle, &recent];

    let (retained, compacted) =
        compact_entries(&entries, 0, selection(512, 100, 513, 400, None), false)
            .expect("hard-window pressure must attempt a legal split");

    assert!(!retained.is_empty());
    assert!(!compacted.is_empty());
    assert_eq!(compacted[0].source_sequence.get(), 1);
    assert!(!uncompressed_request_fits(513, 512));
}

#[test]
fn large_item_count_below_target_token_budget_remains_uncompressed() {
    let owned = (1..=300)
        .map(|sequence| portable_assistant_entry(sequence, "small portable history item"))
        .collect::<Vec<_>>();
    let entries = owned.iter().collect::<Vec<_>>();
    let projection =
        crate::provider_context::compile_provider_context(&entries, &model_route(), false)
            .expect("compile portable target projection");
    let estimated = super::estimate_entries(&entries);

    let (retained, compacted) = compact_entries(
        &entries,
        0,
        selection(100_000, estimated, estimated, 90_000, Some(50_000)),
        false,
    )
    .expect("item count alone must not compact target-compatible history");

    assert!(estimated < 90_000);
    assert_eq!(projection.portable_assistant_entries, 300);
    assert_eq!(retained.len(), 300);
    assert!(compacted.is_empty());
    assert_eq!(retained[0].source_sequence.get(), 1);
    assert_eq!(retained.last().expect("tail").source_sequence.get(), 300);
}

#[test]
fn uncompressed_fallback_depends_on_safety_not_provider_retryability() {
    let cases = [
        (
            "retryable dependency fits",
            ContextPlanError::unavailable(),
            512,
            512,
            true,
        ),
        (
            "retryable dependency exceeds sealed budget",
            ContextPlanError::unavailable(),
            513,
            512,
            false,
        ),
        (
            "post-dispatch external outcome is indeterminate",
            ContextPlanError::indeterminate(),
            512,
            512,
            false,
        ),
        (
            "idempotency conflict is categorical even if marked retryable",
            ContextPlanError {
                early_provider_cause: None,
                retry_after_ms: None,
                code: ErrorCode::IdempotencyConflict,
                retryable: true,
            },
            512,
            512,
            false,
        ),
        (
            "known nonretryable quota failure still permits lossless continuation",
            ContextPlanError::quota(),
            512,
            512,
            true,
        ),
        (
            "known nonretryable quota failure cannot exceed the sealed budget",
            ContextPlanError::quota(),
            513,
            512,
            false,
        ),
        (
            "known truncated summary permits exact uncompressed fallback",
            ContextPlanError {
                early_provider_cause: None,
                code: ErrorCode::ModelResponseInvalid,
                retryable: false,
                retry_after_ms: None,
            },
            512,
            512,
            true,
        ),
        (
            "known truncated summary cannot authorize dropping oversized source",
            ContextPlanError {
                early_provider_cause: None,
                code: ErrorCode::ModelResponseInvalid,
                retryable: false,
                retry_after_ms: None,
            },
            513,
            512,
            false,
        ),
    ];

    for (label, error, tokens, budget, expected) in cases {
        assert_eq!(
            can_fallback_to_uncompressed(&error, tokens, budget),
            expected,
            "{label}"
        );
    }
}

#[test]
fn deterministic_hard_window_still_rejects_when_no_legal_split_exists() {
    let only = user_entry(1, &"indivisible ".repeat(256));
    let error = compact_entries(&[&only], 0, selection(512, 100, 513, 400, None), false)
        .expect_err("an indivisible over-budget Context cannot be compacted safely");
    assert_eq!(error.code, ErrorCode::QuotaExceeded);
}

#[test]
fn oversized_oldest_unit_is_compacted_when_the_recent_tail_fits() {
    let oversized_oldest = user_entry(1, &"old ".repeat(1_000));
    let recent = user_entry(2, "recent request");
    let entries = [&oversized_oldest, &recent];

    let (retained, compacted) =
        compact_entries(&entries, 0, selection(512, 100, 513, 400, None), false)
            .expect("an oversized old unit belongs in the compaction prefix");

    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].source_sequence.get(), 2);
    assert_eq!(compacted.len(), 1);
    assert_eq!(compacted[0].source_sequence.get(), 1);
}

#[test]
fn below_soft_and_hard_thresholds_does_not_compact() {
    let old = user_entry(1, "old");
    let recent = user_entry(2, "recent");
    let entries = [&old, &recent];

    let (retained, compacted) =
        compact_entries(&entries, 0, selection(512, 100, 200, 400, None), false)
            .expect("below-threshold Context remains lossless");

    assert_eq!(retained.len(), entries.len());
    assert!(compacted.is_empty());
    assert!(uncompressed_request_fits(200, 512));
}

#[test]
fn v2_uses_the_exact_recent_tail_budget() {
    let first = user_entry(1, &"a".repeat(30));
    let second = user_entry(2, &"b".repeat(30));
    let third = user_entry(3, &"c".repeat(30));
    let entries = [&first, &second, &third];

    let (retained, compacted) = compact_entries(
        &entries,
        0,
        selection(1_000, u64::MAX, u64::MAX, 1, Some(20)),
        false,
    )
    .expect("exact v2 tail");

    assert_eq!(compacted.len(), 1);
    assert_eq!(retained.len(), 2);
    assert_eq!(retained[0].source_sequence.get(), 2);
}

#[test]
fn newest_atomic_unit_may_exceed_soft_tail_when_it_fits_the_hard_window() {
    let old = user_entry(1, &"old ".repeat(150));
    let recent = user_entry(2, &"recent ".repeat(90));
    let entries = [&old, &recent];

    let (retained, compacted) = compact_entries(
        &entries,
        0,
        selection(1_000, u64::MAX, u64::MAX, 1, Some(100)),
        false,
    )
    .expect("atomic recent Message overrides only the soft tail target");

    assert_eq!(compacted.len(), 1);
    assert_eq!(compacted[0].source_sequence.get(), 1);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].source_sequence.get(), 2);
}

#[test]
fn v2_hysteresis_requires_a_compacted_request_below_the_trigger() {
    let budgets = PlanningBudgets::V2(ResolvedContextBudgets {
        policy_hash: Sha256::digest(b"planner-only hash"),
        hard_input_budget: NonZeroU64::new(1_000).expect("hard"),
        trigger_budget: NonZeroU64::new(850).expect("trigger"),
        reserved_output_budget: NonZeroU64::new(100).expect("output"),
        recent_tail_budget: NonZeroU64::new(170).expect("tail"),
        summary_budget: NonZeroU64::new(330).expect("summary"),
    });

    assert!(!budgets.requires_hysteresis(849));
    assert!(budgets.requires_hysteresis(850));
}

const fn selection(
    hard_input_budget: u64,
    physical_trigger_tokens: u64,
    deterministic_uncompressed_tokens: u64,
    soft_trigger_budget: u64,
    recent_tail_budget: Option<u64>,
) -> CompactionSelectionBudgets {
    CompactionSelectionBudgets {
        hard_input_budget,
        physical_trigger_tokens,
        deterministic_uncompressed_tokens,
        soft_trigger_budget,
        recent_tail_budget,
    }
}

pub(super) fn user_entry(sequence: u64, text: &str) -> ContextEntry {
    let sequence = ConversationSeq::new(sequence).expect("sequence");
    ContextEntry {
        history_page: None,
        runtime_events: Vec::new(),
        source_sequence: sequence,
        presentation_order: ContextPresentationOrder::ledger(sequence),
        source_record_ids: vec![LedgerRecordId::generate()],
        source_run_id: None,
        assistant_origin: None,
        message: CanonicalMessage {
            role: CanonicalRole::User,
            content: vec![CanonicalContentPart::Text(
                ModelText::new(text).expect("text"),
            )],
            tool_calls: Vec::new(),
            tool_call_id: None,
            tool_name: None,
            reasoning: None,
        },
        continuation_delta_for: None,
    }
}

fn portable_assistant_entry(sequence: u64, text: &str) -> ContextEntry {
    let mut entry = user_entry(sequence, text);
    entry.message.role = CanonicalRole::Assistant;
    entry
}

#[test]
fn checkpoint_attaches_to_the_real_user_message_without_changing_the_request() {
    let first = user_entry(1, "第四轮原话").message;
    let recent = assistant_tool_entry(2, &["call-1"]).message;
    let mut messages = vec![first.clone(), recent.clone()];
    let text = "Historical reference (summary):\n第一轮约定周五交付。";
    super::attach_history_summary(&mut messages, Some(text)).unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1], recent);
    assert!(
        matches!(&messages[0].content[0], CanonicalContentPart::Text(part) if part.as_str() == text)
    );
    assert_eq!(&messages[0].content[1..], first.content.as_slice());
}

#[test]
fn summary_rendering_is_lossless_and_fails_when_the_budget_cannot_hold_it() {
    let mut summary = ContextSummaryDocument {
        schema_version: CONTEXT_SUMMARY_SCHEMA,
        compacted: true,
        source_seq_from: ConversationSeq::new(1).expect("from"),
        source_seq_to: ConversationSeq::new(2).expect("to"),
        summary: Some("## Goal\n继续任务".to_owned()),
        generator_invocation_id: Some(InvocationId::generate()),
        previous_summary_ref: None,
        summarized_record_ids: vec![LedgerRecordId::generate()],
        file_operations: crate::ContextFileOperations::default(),
        items: vec![crate::ContextSummaryItem {
            source_sequence: ConversationSeq::new(1).expect("item"),
            source_record_ids: vec![LedgerRecordId::generate()],
            role: crate::ContextSummaryRole::User,
            excerpt: "高强度可恢复智能体上下文".repeat(64),
        }],
        omitted_item_count: 0,
    };
    let rendered = render_history_summary(&summary, 256, false).expect("render");
    assert!(rendered.len() <= 256);
    assert!(std::str::from_utf8(rendered.as_bytes()).is_ok());
    summary.summary = Some("高强度可恢复智能体上下文".repeat(64));
    assert_eq!(
        render_history_summary(&summary, 256, false),
        Err(ContextPlanError::quota())
    );
}

#[test]
fn explicit_child_history_pages_are_not_coalesced_as_status_polls() {
    let mut first = progress_tool_entry(1, "first", "delegate_inspect", &[], None);
    first.message.tool_calls[0].arguments = JsonValue::Object(BTreeMap::from([
        ("view".into(), JsonValue::String("detail".into())),
        ("handle".into(), JsonValue::String("child-a".into())),
        ("record_id".into(), JsonValue::String("record-a".into())),
    ]));
    let mut second = progress_tool_entry(3, "second", "delegate_inspect", &[], None);
    second.message.tool_calls[0].arguments = first.message.tool_calls[0].arguments.clone();
    let entries = [
        first,
        progress_tool_result(2, "first", "page one"),
        second,
        progress_tool_result(4, "second", "page two"),
    ];
    assert_eq!(
        coalesce_progress_observations(&entries.iter().collect::<Vec<_>>())
            .expect("valid planner fixture")
            .len(),
        4
    );
}

#[test]
fn output_continuation_requires_current_unsuperseded_durable_completion() {
    use musteragent_model::{ModelKey, SignedModelRouteHash};
    use musteragent_runtime_model::CanonicalRuntimeEvent;
    let run = RunId::generate();
    let invocation = InvocationId::generate();
    let mut entry = portable_assistant_entry(9, "partial PPT response");
    entry.source_run_id = Some(run);
    entry.continuation_delta_for = Some(invocation);
    let route = model_route();
    entry.assistant_origin = Some(crate::AssistantOrigin {
        invocation_id: invocation,
        model_route_ref: route.route_ref.clone(),
        route_identity_hash: SignedModelRouteHash::digest(&route).expect("route hash"),
        requested_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("key"),
        response_model_key: ModelKey::new(route.resolved_model_key.clone()).expect("key"),
    });
    assert_eq!(
        super::output_continuation::event(&[&entry], run),
        Some(CanonicalRuntimeEvent::OutputLimitContinuation {
            source_invocation_id: invocation
        })
    );
    assert_eq!(
        super::output_continuation::event(&[&entry], RunId::generate()),
        None
    );
    let user = user_entry(10, "new turn");
    assert_eq!(
        super::output_continuation::event(&[&entry, &user], run),
        None
    );
    let mut unknown = entry.clone();
    unknown.assistant_origin = None;
    assert_eq!(super::output_continuation::event(&[&unknown], run), None);
    let mut completed = entry.clone();
    completed.continuation_delta_for = None;
    assert_eq!(super::output_continuation::event(&[&completed], run), None);
    entry.continuation_delta_for = Some(InvocationId::generate());
    assert_eq!(super::output_continuation::event(&[&entry], run), None);
}
