use super::*;
use crate::{encode_content_ref, wire};
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_identity::ContentId;
use prost::Message as _;
use wire::domain_event::Event;

fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("valid media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
fn extract(event: Event) -> Result<Vec<ContentRef>, LedgerContentReferencesError> {
    ledger_content_references(
        LedgerRecordKind::DomainEvent,
        &wire::DomainEvent { event: Some(event) }.encode_to_vec(),
    )
}

#[test]
fn tool_result_and_authorized_attachments_are_exact_sorted_and_unique() {
    let a = reference(b"result");
    let b = reference(b"attachment");
    let result = extract(Event::ToolInvocationSucceeded(
        wire::ToolInvocationSucceeded {
            result_ref: Some(encode_content_ref(&a)),
            authorized_content: vec![encode_content_ref(&b), encode_content_ref(&a)],
            ..Default::default()
        },
    ))
    .expect("extract tool result references");
    let mut expected = vec![a, b];
    expected.sort_by_key(|r| r.content_id);
    assert_eq!(result, expected);
}

#[test]
fn artifact_dependencies_checkpoint_failed_diagnostic_and_output_frame_are_roots() {
    let a = reference(b"a");
    let b = reference(b"b");
    let mut expected = vec![a.clone(), b.clone()];
    expected.sort_by_key(|r| r.content_id);
    assert_eq!(
        extract(Event::ArtifactVersionCommitted(
            wire::ArtifactVersionCommitted {
                content_ref: Some(encode_content_ref(&a)),
                dependency_refs: vec![encode_content_ref(&b)],
                ..Default::default()
            }
        ))
        .expect("extract artifact dependencies"),
        expected
    );
    assert_eq!(
        extract(Event::PresentationAuthoringChanged(
            wire::PresentationAuthoringChanged {
                checkpoint_ref: Some(encode_content_ref(&a)),
                ..Default::default()
            }
        ))
        .expect("extract presentation checkpoint"),
        vec![a.clone()]
    );
    assert_eq!(
        extract(Event::ToolInvocationFailed(wire::ToolInvocationFailed {
            diagnostic_result_ref: Some(encode_content_ref(&a)),
            ..Default::default()
        }))
        .expect("extract failure diagnostic"),
        vec![a.clone()]
    );
    let frame = wire::OutputFrame {
        content: Some(wire::output_frame::Content::ContentRef(encode_content_ref(
            &a,
        ))),
        content_hash: Some(wire::Sha256 {
            value: a.sha256.into_bytes().to_vec(),
        }),
        final_for_output: true,
        ..Default::default()
    };
    assert_eq!(
        ledger_content_references(LedgerRecordKind::OutputFrame, &frame.encode_to_vec())
            .expect("extract output frame reference"),
        vec![a]
    );
}

#[test]
fn payload_strings_do_not_mint_references_and_late_unknown_fields_discard_all_results() {
    let a = reference(b"root");
    let inline = wire::OutputFrame {
        content: Some(wire::output_frame::Content::InlineUtf8(format!(
            "{{\"content_id\":\"{}\"}}",
            a.content_id
        ))),
        ..Default::default()
    };
    assert!(
        ledger_content_references(LedgerRecordKind::OutputFrame, &inline.encode_to_vec())
            .expect("inspect inline text without minting references")
            .is_empty()
    );
    let event = wire::DomainEvent {
        event: Some(Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                result_ref: Some(encode_content_ref(&a)),
                ..Default::default()
            },
        )),
    };
    let mut bytes = event.encode_to_vec();
    // Unknown varint field 127, after the complete known event. It may carry
    // future retention semantics; never return the preceding partial root set.
    bytes.extend([0xf8, 0x07, 0x01]);
    assert_eq!(
        ledger_content_references(LedgerRecordKind::DomainEvent, &bytes),
        Err(LedgerContentReferencesError::Incomplete)
    );
    assert!(ledger_content_references(LedgerRecordKind::DomainEvent, &[]).is_err());
    assert_eq!(
        ledger_content_references(
            LedgerRecordKind::DomainEvent,
            &vec![0; MAX_RECORD_PAYLOAD_BYTES + 1]
        ),
        Err(LedgerContentReferencesError::Bounds)
    );
}

#[test]
fn candidates_cover_typed_roots_even_where_typed_projection_fails() {
    let a = reference(b"result");
    let b = reference(b"attachment");
    let event = wire::DomainEvent {
        event: Some(Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                result_ref: Some(encode_content_ref(&a)),
                authorized_content: vec![encode_content_ref(&b)],
                ..Default::default()
            },
        )),
    };
    let mut bytes = event.encode_to_vec();
    let typed: Vec<_> = ledger_content_references(LedgerRecordKind::DomainEvent, &bytes)
        .expect("typed roots")
        .into_iter()
        .map(|r| r.content_id)
        .collect();
    assert_eq!(ledger_content_reference_candidates(&bytes), typed);

    bytes.extend([0xf8, 0x07, 0x01]);
    assert!(ledger_content_references(LedgerRecordKind::DomainEvent, &bytes).is_err());
    assert_eq!(ledger_content_reference_candidates(&bytes), typed);
}

#[test]
fn candidates_include_opaque_text_and_normalize_case_but_reject_non_canonical_forms() {
    let id = ContentId::generate();
    let upper = id.to_string().to_uppercase();
    let simple = id.into_uuid().simple().to_string();
    let payload = format!(
        "{{\"a\":\"{upper}\",\"b\":\"{simple}\",\"nil\":\"{}\"}}",
        uuid::Uuid::nil()
    );
    assert_eq!(
        ledger_content_reference_candidates(payload.as_bytes()),
        vec![id]
    );
    assert!(ledger_content_reference_candidates(&[]).is_empty());
}

#[test]
fn both_record_schema_names_select_the_current_contract() {
    assert!(ledger_record_schema_is_current(
        "musteragent.runtime.record.v1"
    ));
    assert!(ledger_record_schema_is_current(
        "musteragent.legacy.runtime.record.v1"
    ));
    assert!(!ledger_record_schema_is_current(
        "musteragent.runtime.record.v2"
    ));
}

#[test]
fn conflicting_identities_malformed_refs_and_duplicate_oneofs_fail_closed() {
    let a = reference(b"a");
    let mut conflict = a.clone();
    conflict.sha256 = Sha256::digest(b"different");
    assert!(
        extract(Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                result_ref: Some(encode_content_ref(&a)),
                authorized_content: vec![encode_content_ref(&conflict)],
                ..Default::default()
            }
        ))
        .is_err()
    );
    let mut invalid = encode_content_ref(&a);
    invalid.sha256 = None;
    assert!(
        extract(Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                result_ref: Some(invalid),
                ..Default::default()
            }
        ))
        .is_err()
    );
    let event = wire::DomainEvent {
        event: Some(Event::ToolInvocationSucceeded(
            wire::ToolInvocationSucceeded {
                result_ref: Some(encode_content_ref(&a)),
                ..Default::default()
            },
        )),
    }
    .encode_to_vec();
    assert!(
        ledger_content_references(
            LedgerRecordKind::DomainEvent,
            &[event.clone(), event].concat()
        )
        .is_err()
    );
}
