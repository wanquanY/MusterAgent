use musteragent_artifact::{ArtifactEvent, ArtifactKind};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{
    ArtifactId, ContentId, ContextBranchId, DelegationGroupId, FrameId, LedgerRecordId, MessageId,
    NoteAuthoringSessionId, NoteSegmentId, OutputId, PrincipalId, VersionNumber,
};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_contract::{
    DisplayText, FrameContent, InlineFrame, OutputChannel, OutputFrame,
};
use prost::Message;

use super::{ProtobufLedgerPayloadCodec, wire};

#[test]
fn workspace_assignment_round_trips_and_requires_both_identities() {
    let codec = ProtobufLedgerPayloadCodec;
    let assignment = LedgerPayload::DomainEvent(DomainEvent::Conversation(
        musteragent_conversation::ConversationEvent::WorkspaceAssigned {
            workspace_id: musteragent_identity::WorkspaceId::generate(),
            assignment_command_id: musteragent_identity::CommandId::generate(),
        },
    ));
    assert_eq!(
        codec
            .decode(
                LedgerRecordKind::DomainEvent,
                &codec.encode(assignment.clone())
            )
            .expect("decode"),
        assignment
    );
    for value in [
        wire::ConversationWorkspaceAssigned {
            workspace_id: None,
            assignment_command_id: Some(wire::CommandId {
                value: musteragent_identity::CommandId::generate().to_string(),
            }),
        },
        wire::ConversationWorkspaceAssigned {
            workspace_id: Some(wire::WorkspaceId {
                value: musteragent_identity::WorkspaceId::generate().to_string(),
            }),
            assignment_command_id: None,
        },
    ] {
        let wire = wire::DomainEvent {
            event: Some(wire::domain_event::Event::ConversationWorkspaceAssigned(
                value,
            )),
        };
        assert!(
            codec
                .decode(LedgerRecordKind::DomainEvent, &wire.encode_to_vec())
                .is_err()
        );
    }
}

#[test]
fn canonical_domain_and_output_payloads_round_trip() {
    let codec = ProtobufLedgerPayloadCodec;
    let domain = LedgerPayload::DomainEvent(DomainEvent::Conversation(
        musteragent_conversation::ConversationEvent::Archived,
    ));
    let encoded = codec.encode(domain.clone());
    assert_eq!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &encoded)
            .unwrap_or_else(|error| panic!("{error}")),
        domain
    );
    let unarchived = LedgerPayload::DomainEvent(DomainEvent::Conversation(
        musteragent_conversation::ConversationEvent::Unarchived,
    ));
    let encoded = codec.encode(unarchived.clone());
    assert_ne!(
        encoded,
        codec.encode(domain.clone()),
        "unarchive must not reuse the archive encoding"
    );
    assert_eq!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &encoded)
            .unwrap_or_else(|error| panic!("{error}")),
        unarchived
    );

    let title = LedgerPayload::DomainEvent(DomainEvent::Conversation(
        musteragent_conversation::ConversationEvent::TitleChanged {
            title: DisplayText::new("Canonical title").expect("title"),
            origin: musteragent_conversation::ConversationTitleOrigin::User,
            set_by: PrincipalId::generate(),
        },
    ));
    let encoded = codec.encode(title.clone());
    assert_eq!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &encoded)
            .unwrap_or_else(|error| panic!("{error}")),
        title
    );

    let text = InlineFrame::new("hello").unwrap_or_else(|error| panic!("{error}"));
    let output = LedgerPayload::OutputFrame(OutputFrame {
        frame_id: FrameId::generate(),
        output_id: OutputId::generate(),
        message_id: Some(MessageId::generate()),
        channel: OutputChannel::AssistantText,
        frame_index: 0,
        byte_offset_start: 0,
        byte_offset_end: text.byte_length(),
        content_hash: Sha256::digest(text.as_str().as_bytes()),
        content: FrameContent::InlineUtf8(text),
        final_for_output: true,
    });
    let encoded = codec.encode(output.clone());
    assert_eq!(
        codec
            .decode(LedgerRecordKind::OutputFrame, &encoded)
            .unwrap_or_else(|error| panic!("{error}")),
        output
    );
}

#[test]
fn note_authoring_recovery_events_round_trip_with_strong_identities() {
    let codec = ProtobufLedgerPayloadCodec;
    let artifact_id = ArtifactId::generate();
    let session_id = NoteAuthoringSessionId::generate();
    let events = [
        ArtifactEvent::NoteAuthoringStarted {
            artifact_id,
            session_id,
            base_revision: VersionNumber::new(1).expect("revision"),
        },
        ArtifactEvent::NoteSegmentCommitted {
            artifact_id,
            session_id,
            segment_id: NoteSegmentId::generate(),
            ordinal: 0,
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/markdown;charset=utf-8")
                    .expect("Markdown media type"),
                byte_length: 15,
                sha256: Sha256::digest(b"bounded segment"),
            },
            content_hash: Sha256::digest(b"bounded segment"),
            character_count: 15,
            utf8_byte_count: 15,
        },
        ArtifactEvent::NoteVersionFinalized {
            artifact_id,
            session_id,
            version: VersionNumber::new(2).expect("revision"),
            block_count: 1,
            character_count: 15,
            utf8_byte_count: 15,
        },
    ];
    for event in events {
        let payload = LedgerPayload::DomainEvent(DomainEvent::Artifact(event));
        let encoded = codec.encode(payload.clone());
        assert_eq!(
            codec
                .decode(LedgerRecordKind::DomainEvent, &encoded)
                .expect("Note authoring event must be replayable"),
            payload
        );
    }
}

#[test]
fn pre_content_ref_note_segment_remains_replayable_as_its_historical_contract() {
    let artifact_id = ArtifactId::generate();
    let session_id = NoteAuthoringSessionId::generate();
    let segment_id = NoteSegmentId::generate();
    let content_hash = Sha256::digest(b"historical segment");
    let wire_event = wire::DomainEvent {
        event: Some(wire::domain_event::Event::NoteSegmentCommitted(
            wire::NoteSegmentCommitted {
                artifact_id: Some(wire::ArtifactId {
                    value: artifact_id.to_string(),
                }),
                session_id: Some(wire::NoteAuthoringSessionId {
                    value: session_id.to_string(),
                }),
                segment_id: Some(wire::NoteSegmentId {
                    value: segment_id.to_string(),
                }),
                ordinal: 3,
                content_hash: Some(wire::Sha256 {
                    value: content_hash.into_bytes().to_vec(),
                }),
                character_count: 18,
                utf8_byte_count: 18,
                content_ref: None,
            },
        )),
    };
    let codec = ProtobufLedgerPayloadCodec;
    let decoded = codec
        .decode(LedgerRecordKind::DomainEvent, &wire_event.encode_to_vec())
        .expect("pre-content-ref Note segment must remain replayable");
    assert!(matches!(
        decoded,
        LedgerPayload::DomainEvent(DomainEvent::Artifact(
            ArtifactEvent::NoteSegmentCommittedV1 {
                artifact_id: decoded_artifact_id,
                session_id: decoded_session_id,
                segment_id: decoded_segment_id,
                ordinal: 3,
                content_hash: decoded_hash,
                character_count: 18,
                utf8_byte_count: 18,
            }
        )) if decoded_artifact_id == artifact_id
            && decoded_session_id == session_id
            && decoded_segment_id == segment_id
            && decoded_hash == content_hash
    ));

    let reencoded = wire::DomainEvent::decode(codec.encode(decoded).as_slice())
        .expect("historical Note segment wire event");
    let Some(wire::domain_event::Event::NoteSegmentCommitted(reencoded)) = reencoded.event else {
        panic!("re-encoded the wrong event");
    };
    assert_eq!(reencoded.content_ref, None);
}

#[test]
fn retired_diagram_artifact_events_remain_replayable_without_restoring_creation_tools() {
    let artifact_id = ArtifactId::generate();
    let wire_event = wire::DomainEvent {
        event: Some(wire::domain_event::Event::ArtifactCreated(
            wire::ArtifactCreated {
                artifact_id: Some(wire::ArtifactId {
                    value: artifact_id.to_string(),
                }),
                kind: 2,
                title: "Historical Diagram".to_owned(),
                source_invocation_id: None,
            },
        )),
    };
    let codec = ProtobufLedgerPayloadCodec;
    let decoded = codec
        .decode(LedgerRecordKind::DomainEvent, &wire_event.encode_to_vec())
        .expect("historical Diagram event must remain replayable");
    assert!(matches!(
        decoded,
        LedgerPayload::DomainEvent(DomainEvent::Artifact(ArtifactEvent::Created {
            artifact_id: decoded_id,
            kind: ArtifactKind::LegacyDiagram,
            ..
        })) if decoded_id == artifact_id
    ));

    let reencoded = codec.encode(decoded);
    let round_trip = wire::DomainEvent::decode(reencoded.as_slice()).expect("wire event");
    let Some(wire::domain_event::Event::ArtifactCreated(created)) = round_trip.event else {
        panic!("re-encoded the wrong event");
    };
    assert_eq!(created.kind, 2, "historical wire number is immutable");
}

#[test]
fn delegation_group_wait_event_round_trips_for_history_projection() {
    let codec = ProtobufLedgerPayloadCodec;
    let group_id = DelegationGroupId::generate();
    let waiting =
        LedgerPayload::DomainEvent(DomainEvent::Run(musteragent_run::RunEvent::Waiting {
            reason: musteragent_run::RunWaitReason::DelegationGroup { group_id },
            resume_not_before: None,
        }));

    let encoded = codec.encode(waiting.clone());
    assert_eq!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &encoded)
            .expect("Delegation-group waits must remain replayable"),
        waiting
    );

    let decoded_wire = wire::DomainEvent::decode(encoded.as_slice())
        .expect("decode canonical RunWaiting wire payload");
    let Some(wire::domain_event::Event::RunWaiting(wait)) = decoded_wire.event else {
        panic!("encoded the wrong event");
    };
    assert_eq!(
        wait.delegation_group_id.map(|value| value.value),
        Some(group_id.to_string())
    );
}

#[test]
fn malformed_or_kind_mismatched_payload_is_rejected() {
    let codec = ProtobufLedgerPayloadCodec;
    assert!(codec.decode(LedgerRecordKind::DomainEvent, b"bad").is_err());
    let encoded = codec.encode(LedgerPayload::DomainEvent(DomainEvent::Conversation(
        musteragent_conversation::ConversationEvent::Archived,
    )));
    assert!(
        codec
            .decode(LedgerRecordKind::OutputFrame, &encoded)
            .is_err()
    );
}

#[test]
fn legacy_context_branch_event_without_origin_remains_replayable() {
    let branch_id = ContextBranchId::generate();
    let fork_from_record_id = LedgerRecordId::generate();
    let created_by = PrincipalId::generate();
    let wire_event = wire::DomainEvent {
        event: Some(wire::domain_event::Event::ContextBranchCreated(
            wire::ContextBranchCreated {
                branch_id: Some(wire::ContextBranchId {
                    value: branch_id.to_string(),
                }),
                parent_branch_id: None,
                fork_from_record_id: Some(wire::LedgerRecordId {
                    value: fork_from_record_id.to_string(),
                }),
                branch_summary_ref: None,
                created_by: Some(wire::PrincipalId {
                    value: created_by.to_string(),
                }),
                origin: wire::ContextBranchOrigin::Unspecified as i32,
                edit_group_id: None,
                edit_target_message_id: None,
            },
        )),
    };
    let codec = ProtobufLedgerPayloadCodec;
    let decoded = codec
        .decode(LedgerRecordKind::DomainEvent, &wire_event.encode_to_vec())
        .expect("legacy Context branch event must remain replayable");
    let LedgerPayload::DomainEvent(DomainEvent::Context(
        musteragent_context::ContextEvent::BranchCreated {
            origin,
            edit_group_id,
            edit_target_message_id,
            ..
        },
    )) = decoded.clone()
    else {
        panic!("decoded the wrong legacy event");
    };
    assert_eq!(
        origin,
        musteragent_conversation::ContextBranchOrigin::LegacyUnclassified
    );
    assert_eq!(edit_group_id, None);
    assert_eq!(edit_target_message_id, None);

    assert_eq!(
        codec
            .decode(
                LedgerRecordKind::DomainEvent,
                &codec.encode(decoded.clone())
            )
            .expect("legacy event must remain stable across decode and encode"),
        decoded
    );
}

#[test]
fn pre_f1_usage_remains_replayable_without_becoming_canonical_usage() {
    let legacy = wire::DomainEvent {
        event: Some(wire::domain_event::Event::UsageRecorded(
            wire::UsageRecorded {
                invocation_id: Some(wire::InvocationId {
                    value: musteragent_identity::InvocationId::generate().to_string(),
                }),
                purpose: wire::ModelInvocationPurpose::Primary as i32,
                model_route_ref: Some(wire::ModelRouteRef {
                    route_key: "route.legacy@1".to_owned(),
                }),
                input_tokens: 11,
                output_tokens: 7,
                reasoning_tokens: 3,
                provider_cache_read_tokens: 2,
                provider_cache_write_tokens: 0,
                metering_dedup_key: "usage:legacy".to_owned(),
                requested_model_key: "openai/gpt-legacy".to_owned(),
                pricing_version_ref: "catalog:pricing.legacy@1".to_owned(),
                total_tokens: Some(23),
                credential_grant_ref: String::new(),
                response_model_key: String::new(),
                provider_cache_write_1h_tokens: 0,
                cancelled: false,
            },
        )),
    };
    let original = legacy.encode_to_vec();
    let codec = ProtobufLedgerPayloadCodec;
    let decoded = codec
        .decode(LedgerRecordKind::DomainEvent, &original)
        .expect("pre-F1 Usage must remain replayable");
    assert!(matches!(
        decoded,
        LedgerPayload::DomainEvent(DomainEvent::Model(
            musteragent_runtime_model::ModelEvent::LegacyUsageRecordedV1(_)
        ))
    ));
    assert_eq!(codec.encode(decoded), original);

    let mut mixed = legacy;
    let Some(wire::domain_event::Event::UsageRecorded(value)) = mixed.event.as_mut() else {
        panic!("usage event");
    };
    value.credential_grant_ref = "grant-present-but-response-missing".to_owned();
    assert!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &mixed.encode_to_vec())
            .is_err()
    );
}

#[test]
fn run_admitted_round_trips_input_message_and_decodes_legacy_records_without_it() {
    use musteragent_event_envelope::RuntimeHome;
    use musteragent_identity::{AttemptNumber, DelegationId, RunId, RuntimeSnapshotId};
    use musteragent_run::RunEvent;

    let codec = ProtobufLedgerPayloadCodec;
    let snapshot_id = RuntimeSnapshotId::generate();
    let input_message_id = MessageId::generate();
    let parent_run_id = RunId::generate();
    let delegation_id = DelegationId::generate();
    let attempt_one = AttemptNumber::new(1).expect("attempt one");

    for event in [
        RunEvent::Admitted {
            runtime_snapshot_id: snapshot_id,
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            input_message_id: Some(input_message_id),
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        },
        RunEvent::Admitted {
            runtime_snapshot_id: snapshot_id,
            execution_home: RuntimeHome::Cloud,
            parent_run_id: Some(parent_run_id),
            delegation_id: None,
            input_message_id: Some(input_message_id),
            retry_source_message_id: Some(input_message_id),
            retry_source_attempt_number: Some(attempt_one),
        },
        RunEvent::Admitted {
            runtime_snapshot_id: snapshot_id,
            execution_home: RuntimeHome::Cloud,
            parent_run_id: Some(parent_run_id),
            delegation_id: Some(delegation_id),
            input_message_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        },
    ] {
        let payload = LedgerPayload::DomainEvent(DomainEvent::Run(event));
        let encoded = codec.encode(payload.clone());
        assert_eq!(
            codec
                .decode(LedgerRecordKind::DomainEvent, &encoded)
                .expect("decode RunAdmitted"),
            payload
        );
        assert_eq!(
            crate::ledger_content_references(LedgerRecordKind::DomainEvent, &encoded)
                .expect("RunAdmitted content references"),
            Vec::new()
        );
    }

    let legacy_wire = wire::DomainEvent {
        event: Some(wire::domain_event::Event::RunAdmitted(wire::RunAdmitted {
            runtime_snapshot_id: Some(wire::RuntimeSnapshotId {
                value: snapshot_id.to_string(),
            }),
            execution_home: Some(wire::RuntimeHome {
                home: Some(wire::runtime_home::Home::Cloud(wire::CloudHome {})),
            }),
            parent_run_id: None,
            delegation_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: 0,
            input_message_id: None,
        })),
    };
    let decoded_legacy = codec
        .decode(LedgerRecordKind::DomainEvent, &legacy_wire.encode_to_vec())
        .expect("legacy RunAdmitted without input_message_id must remain decodable");
    assert_eq!(
        decoded_legacy,
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: snapshot_id,
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            input_message_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        }))
    );

    let invalid_wire = wire::DomainEvent {
        event: Some(wire::domain_event::Event::RunAdmitted(wire::RunAdmitted {
            runtime_snapshot_id: Some(wire::RuntimeSnapshotId {
                value: snapshot_id.to_string(),
            }),
            execution_home: Some(wire::RuntimeHome {
                home: Some(wire::runtime_home::Home::Cloud(wire::CloudHome {})),
            }),
            parent_run_id: None,
            delegation_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: 0,
            input_message_id: Some(wire::MessageId {
                value: "not-a-uuid".to_owned(),
            }),
        })),
    };
    assert!(
        codec
            .decode(LedgerRecordKind::DomainEvent, &invalid_wire.encode_to_vec())
            .is_err()
    );
}
