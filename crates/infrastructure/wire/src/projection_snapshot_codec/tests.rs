use musteragent_activity::{ActivityKind, ActivityStatus};
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::MessageRole;
use musteragent_identity::{
    ActivityId, ArtifactId, AttemptNumber, ContentId, ConversationId, ConversationSeq,
    DelegationId, HomeEpoch, InterSessionMessageId, InvocationId, LedgerOffset, MessageId,
    OutputId, ProjectionSnapshotId, RunId, TenantId, ToolCallId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_projection::{
    ConversationProjectionSnapshot, ConversationProjectionSnapshotCodec,
    PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedActivity, ProjectedArtifact,
    ProjectedContextCompaction, ProjectedContextCompactionStatus, ProjectedCoordinationOrigin,
    ProjectedDelegationOrigin, ProjectedMessage, ProjectedMessageStatus, ProjectedOutputCheckpoint,
    ProjectedRun, ProjectedToolItem, ProjectedToolStatus, canonical_content_manifest,
};
use musteragent_run::{RunStatus, RunWaitReason};
use musteragent_runtime_model::ModelRouteRef;
use prost::Message as _;

#[path = "terminal_failure.rs"]
mod terminal_failure;

use super::{
    ProjectionSnapshotCodecError, ProtobufConversationProjectionCodec,
    conversation_projection_snapshot_message,
};

fn snapshot(next_byte_offset: u64) -> ConversationProjectionSnapshot {
    let content_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: 12,
        sha256: Sha256::digest(b"hello assistant!"),
    };
    ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        through_conversation_seq: LedgerOffset::new(355_359),
        messages: vec![ProjectedMessage {
            project_task_target: None,
            message_id: MessageId::generate(),
            participant_id: None,
            role: MessageRole::Assistant,
            content_ref: content_ref.clone(),
            reasoning_summaries: Vec::new(),
            status: ProjectedMessageStatus::Streaming,
            source_run_id: None,
            committed_at: UtcTimestamp::from_unix_millis(1_000),
            attachment_refs: Vec::new(),
            requested_skill_refs: vec![
                VersionedKey::new("presentation-authoring@2").expect("skill ref"),
            ],
            imported_projection_ref: None,
            assistant_output: Some(ProjectedOutputCheckpoint {
                output_id: OutputId::generate(),
                next_frame_index: 41,
                next_byte_offset,
                completed: false,
            }),
            partial_reason: None,
        }],
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: Vec::new(),
        runs: Vec::new(),
        active_runs: Vec::new(),
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest: vec![content_ref],
        checksum: Sha256::digest(b"pending"),
        created_at: UtcTimestamp::from_unix_millis(2_000),
    }
}

fn terminal_activity(
    activity_id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
    occurred_at_ms: i64,
) -> ProjectedActivity {
    let occurred_at = UtcTimestamp::from_unix_millis(occurred_at_ms);
    ProjectedActivity {
        activity_id,
        kind,
        status,
        requested_by_participant_id: None,
        parent_activity_id: None,
        root_activity_id: activity_id,
        policy_snapshot_ref: VersionedKey::new("projection-lineage-policy@1").expect("policy ref"),
        started_at: occurred_at,
        finished_at: Some(occurred_at),
        delegation_origin: None,
        coordination_origin: None,
    }
}

fn terminal_run(
    run_id: RunId,
    activity_id: ActivityId,
    parent_run_id: Option<RunId>,
    delegation_id: Option<DelegationId>,
    status: RunStatus,
    admitted_at_ms: i64,
) -> ProjectedRun {
    let admitted_at = UtcTimestamp::from_unix_millis(admitted_at_ms);
    ProjectedRun {
        run_id,
        activity_id,
        executor_participant_id: None,
        parent_run_id,
        delegation_id,
        input_message_id: None,
        retry_source_message_id: None,
        retry_source_attempt_number: None,
        terminal_failure: None,
        retry_failure: None,
        status,
        wait_reason: None,
        attempt_number: None,
        next_attempt_number: None,
        retry_not_before: None,
        admitted_at,
        started_at: Some(admitted_at),
        finished_at: Some(admitted_at),
    }
}

#[test]
fn snapshot_round_trip_preserves_output_resume_checkpoint() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");
    let checkpoint = decoded.messages[0]
        .assistant_output
        .as_ref()
        .expect("assistant checkpoint");

    assert_eq!(checkpoint.next_frame_index, 41);
    assert_eq!(checkpoint.next_byte_offset, 12);
    assert_eq!(decoded, value);
}

#[test]
fn snapshot_round_trip_preserves_tool_output_artifact_links() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let output_artifact_id = ArtifactId::generate();
    let content_ref = value.content_manifest[0].clone();
    value.tool_timeline = vec![ProjectedToolItem {
        invocation_id: InvocationId::generate(),
        message_id: Some(value.messages[0].message_id),
        run_id: RunId::generate(),
        tool_call_id: ToolCallId::new("call-output-artifact").expect("tool call"),
        tool_ref: "artifact.file.create@1".to_owned(),
        status: ProjectedToolStatus::Completed,
        request_ref: content_ref.clone(),
        result_ref: Some(content_ref),
        requested_conversation_seq: ConversationSeq::new(1).expect("sequence"),
        requested_at: UtcTimestamp::from_unix_millis(1_000),
        started_at: Some(UtcTimestamp::from_unix_millis(1_100)),
        finished_at: Some(UtcTimestamp::from_unix_millis(1_200)),
        output_artifact_ids: vec![output_artifact_id],
    }];
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");

    assert_eq!(
        decoded.tool_timeline[0].output_artifact_ids,
        vec![output_artifact_id]
    );
    assert_eq!(decoded, value);
}

#[test]
fn retired_projection_snapshots_are_rejected_without_resealing() {
    let codec = ProtobufConversationProjectionCodec;
    for legacy_schema in [
        "musteragent.legacy.conversation-projection-snapshot.v1",
        "musteragent.legacy.conversation-projection-snapshot.v2",
    ] {
        let mut value = snapshot(12);
        value.checksum = codec.checksum(&value);
        let mut legacy = conversation_projection_snapshot_message(&value);
        legacy.schema_version = legacy_schema.to_owned();
        legacy.checksum = None;
        let legacy_checksum = Sha256::digest(legacy.encode_to_vec());
        legacy.checksum = Some(crate::wire::Sha256 {
            value: legacy_checksum.into_bytes().to_vec(),
        });

        assert!(matches!(
            codec.decode(&legacy.encode_to_vec()),
            Err(ProjectionSnapshotCodecError::UnsupportedSchema)
        ));
    }
}

#[test]
fn snapshot_round_trip_preserves_durable_retry_state() {
    let codec = ProtobufConversationProjectionCodec;
    let activity_id = ActivityId::generate();
    let mut run = terminal_run(
        RunId::generate(),
        activity_id,
        None,
        None,
        RunStatus::Waiting,
        1_000,
    );
    run.wait_reason = Some(RunWaitReason::RetryBackoff);
    run.attempt_number = Some(AttemptNumber::new(1).expect("attempt one"));
    run.next_attempt_number = Some(AttemptNumber::new(2).expect("attempt two"));
    run.retry_not_before = Some(UtcTimestamp::from_unix_millis(8_000));
    run.finished_at = None;
    let mut activity = terminal_activity(
        activity_id,
        ActivityKind::Chat,
        ActivityStatus::Active,
        1_000,
    );
    activity.finished_at = None;
    let mut value = snapshot(12);
    value.activities = vec![activity];
    value.runs = vec![run.clone()];
    value.active_runs = vec![run];
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");

    assert_eq!(decoded, value);
}

#[test]
fn snapshot_round_trip_preserves_retired_diagram_history_as_legacy_artifact() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/vnd.musteragent.legacy.diagram+json")
            .expect("legacy Diagram media type"),
        byte_length: 2,
        sha256: Sha256::digest(b"{}"),
    };
    let presentation_run_id = RunId::generate();
    value.artifacts.push(ProjectedArtifact {
        artifact_id: ArtifactId::generate(),
        activity_id: None,
        created_by_run_id: Some(presentation_run_id),
        updated_by_run_id: Some(presentation_run_id),
        presentation_run_id: None,
        kind: ArtifactKind::LegacyDiagram,
        lifecycle: ArtifactLifecycle::Published,
        current_version: VersionNumber::new(1).expect("version"),
        title: "Retired Diagram".to_owned(),
        current_content_ref: reference,
        current_dependency_refs: Vec::new(),
        updated_at: UtcTimestamp::from_unix_millis(1_500),
    });
    value.content_manifest = canonical_content_manifest(&value).expect("manifest");
    value.checksum = codec.checksum(&value);

    let wire = super::conversation_projection_snapshot_message(&value);
    assert_eq!(
        wire.artifacts[0].kind, 2,
        "historical wire number is immutable"
    );
    let mut legacy_wire = wire.clone();
    legacy_wire.checksum = None;
    let legacy_checksum = Sha256::digest(legacy_wire.encode_to_vec());
    legacy_wire.checksum = Some(super::sha256(legacy_checksum));
    let promoted = codec
        .decode(&legacy_wire.encode_to_vec())
        .expect("decode and promote v12 Artifact snapshot");
    assert_eq!(
        promoted, value,
        "the promoted legacy snapshot is resealed with the current checksum",
    );
    let encoded = codec.encode(&value).expect("encode legacy Diagram history");
    let decoded = codec
        .decode(&encoded)
        .expect("decode legacy Diagram history");

    assert_eq!(decoded, value);
}

#[test]
fn snapshot_round_trip_accepts_standing_goal_run_lineage_without_delegation() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let source_activity_id = ActivityId::generate();
    let continuation_activity_id = ActivityId::generate();
    let judge_activity_id = ActivityId::generate();
    let source_run_id = RunId::generate();
    let continuation_run_id = RunId::generate();
    let judge_run_id = RunId::generate();

    value.activities = vec![
        terminal_activity(
            source_activity_id,
            ActivityKind::Chat,
            ActivityStatus::Completed,
            100,
        ),
        terminal_activity(
            continuation_activity_id,
            ActivityKind::Automation,
            ActivityStatus::Completed,
            200,
        ),
        terminal_activity(
            judge_activity_id,
            ActivityKind::Evaluation,
            ActivityStatus::Failed,
            300,
        ),
    ];
    value.runs = vec![
        terminal_run(
            source_run_id,
            source_activity_id,
            None,
            None,
            RunStatus::Completed,
            110,
        ),
        terminal_run(
            continuation_run_id,
            continuation_activity_id,
            Some(source_run_id),
            None,
            RunStatus::Completed,
            210,
        ),
        terminal_run(
            judge_run_id,
            judge_activity_id,
            Some(continuation_run_id),
            None,
            RunStatus::Failed,
            310,
        ),
    ];
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode Standing Goal lineage");
    let decoded = codec
        .decode(&encoded)
        .expect("decode Standing Goal lineage");

    assert_eq!(decoded, value);
}

#[test]
fn snapshot_requires_parent_run_for_delegation_identity() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let source_activity_id = ActivityId::generate();
    let delegation_activity_id = ActivityId::generate();
    let source_run_id = RunId::generate();
    let delegation_run_id = RunId::generate();
    let mut delegation_activity = terminal_activity(
        delegation_activity_id,
        ActivityKind::Delegation,
        ActivityStatus::Completed,
        200,
    );
    delegation_activity.parent_activity_id = Some(source_activity_id);
    delegation_activity.root_activity_id = source_activity_id;
    value.activities = vec![
        terminal_activity(
            source_activity_id,
            ActivityKind::Chat,
            ActivityStatus::Completed,
            100,
        ),
        delegation_activity,
    ];
    value.runs = vec![
        terminal_run(
            source_run_id,
            source_activity_id,
            None,
            None,
            RunStatus::Completed,
            110,
        ),
        terminal_run(
            delegation_run_id,
            delegation_activity_id,
            Some(source_run_id),
            Some(DelegationId::generate()),
            RunStatus::Completed,
            210,
        ),
    ];
    value.checksum = codec.checksum(&value);
    codec
        .encode(&value)
        .expect("Delegation lineage with parent Run must encode");

    value.runs[1].parent_run_id = None;
    value.checksum = codec.checksum(&value);

    assert_eq!(
        codec.encode(&value),
        Err(ProjectionSnapshotCodecError::InvalidContract)
    );
}

#[test]
fn snapshot_round_trip_preserves_context_compaction_lifecycle() {
    for (status, input_tokens_after, error_code) in [
        (
            ProjectedContextCompactionStatus::Succeeded,
            Some(3_840),
            None,
        ),
        (
            ProjectedContextCompactionStatus::RetryScheduled,
            None,
            Some(musteragent_runtime_contract::ErrorCode::DependencyUnavailable),
        ),
    ] {
        let codec = ProtobufConversationProjectionCodec;
        let mut value = snapshot(12);
        value.context_compactions.push(ProjectedContextCompaction {
            invocation_id: InvocationId::generate(),
            run_id: RunId::generate(),
            loop_iteration: 2,
            status,
            model_route_ref: ModelRouteRef::from_key(
                VersionedKey::new("openai.compaction@1").expect("route"),
            ),
            input_tokens_before: 10_240,
            input_tokens_after,
            summarized_record_count: 14,
            retained_record_count: 5,
            source_seq_from: ConversationSeq::new(1).expect("source from"),
            source_seq_to: ConversationSeq::new(19).expect("source to"),
            started_conversation_seq: ConversationSeq::new(20).expect("start seq"),
            started_at: UtcTimestamp::from_unix_millis(1_700),
            finished_at: Some(UtcTimestamp::from_unix_millis(1_900)),
            error_code,
        });
        value.checksum = codec.checksum(&value);

        let encoded = codec.encode(&value).expect("encode snapshot");
        let decoded = codec.decode(&encoded).expect("decode snapshot");

        assert_eq!(decoded.context_compactions, value.context_compactions);
        assert_eq!(decoded, value);
    }
}

#[test]
fn snapshot_rejects_incoherent_context_compaction_terminal_metrics() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    value.context_compactions.push(ProjectedContextCompaction {
        invocation_id: InvocationId::generate(),
        run_id: RunId::generate(),
        loop_iteration: 2,
        status: ProjectedContextCompactionStatus::Succeeded,
        model_route_ref: ModelRouteRef::from_key(
            VersionedKey::new("openai.compaction@1").expect("route"),
        ),
        input_tokens_before: 10_240,
        input_tokens_after: None,
        summarized_record_count: 14,
        retained_record_count: 5,
        source_seq_from: ConversationSeq::new(1).expect("source from"),
        source_seq_to: ConversationSeq::new(19).expect("source to"),
        started_conversation_seq: ConversationSeq::new(20).expect("start seq"),
        started_at: UtcTimestamp::from_unix_millis(1_700),
        finished_at: Some(UtcTimestamp::from_unix_millis(1_900)),
        error_code: None,
    });
    value.checksum = codec.checksum(&value);

    assert_eq!(
        codec.encode(&value),
        Err(ProjectionSnapshotCodecError::InvalidContract)
    );
}

#[test]
fn snapshot_round_trip_preserves_delegation_objective_authority() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let activity_id = ActivityId::generate();
    let objective_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: 16,
        sha256: Sha256::digest(b"inspect boundary"),
    };
    value.activities.push(ProjectedActivity {
        activity_id,
        kind: ActivityKind::Delegation,
        status: ActivityStatus::Active,
        requested_by_participant_id: None,
        parent_activity_id: None,
        root_activity_id: activity_id,
        policy_snapshot_ref: VersionedKey::new("delegation-policy@1").expect("policy ref"),
        started_at: UtcTimestamp::from_unix_millis(1_500),
        finished_at: None,
        delegation_origin: Some(ProjectedDelegationOrigin {
            delegation_id: DelegationId::generate(),
            parent_invocation_id: InvocationId::generate(),
            objective_ref: Some(objective_ref.clone()),
        }),
        coordination_origin: None,
    });
    value.content_manifest = canonical_content_manifest(&value).expect("canonical manifest");
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");

    assert_eq!(
        decoded.activities[0]
            .delegation_origin
            .as_ref()
            .and_then(|origin| origin.objective_ref.as_ref()),
        Some(&objective_ref)
    );
    assert!(decoded.content_manifest.contains(&objective_ref));
    assert_eq!(decoded, value);
}

#[test]
fn snapshot_round_trip_preserves_coordination_ingress_authority() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let activity_id = ActivityId::generate();
    let occurred_at = UtcTimestamp::from_unix_millis(1_250);
    let payload_ref = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: 20,
        sha256: Sha256::digest(b"review the csv files"),
    };
    value.activities.push(ProjectedActivity {
        activity_id,
        kind: ActivityKind::Coordination,
        status: ActivityStatus::Active,
        requested_by_participant_id: None,
        parent_activity_id: None,
        root_activity_id: activity_id,
        policy_snapshot_ref: VersionedKey::new("coordination-policy@1").expect("policy ref"),
        started_at: occurred_at,
        finished_at: None,
        delegation_origin: None,
        coordination_origin: Some(ProjectedCoordinationOrigin {
            mailbox_message_id: InterSessionMessageId::generate(),
            source_conversation_id: ConversationId::generate(),
            payload_ref: Some(payload_ref.clone()),
            occurred_at: Some(occurred_at),
        }),
    });
    value.content_manifest = canonical_content_manifest(&value).expect("canonical manifest");
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");

    let origin = decoded.activities[0]
        .coordination_origin
        .as_ref()
        .expect("coordination origin");
    assert_eq!(origin.payload_ref.as_ref(), Some(&payload_ref));
    assert_eq!(origin.occurred_at, Some(occurred_at));
    assert!(decoded.content_manifest.contains(&payload_ref));
    assert_eq!(decoded, value);
}

#[test]
fn snapshot_rejects_checkpoint_that_disagrees_with_materialized_bytes() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(11);
    value.checksum = codec.checksum(&value);

    assert_eq!(
        codec.encode(&value),
        Err(ProjectionSnapshotCodecError::InvalidContract)
    );
}

#[test]
fn snapshot_rejects_a_checksum_valid_but_non_exact_manifest() {
    let codec = ProtobufConversationProjectionCodec;
    let mut extra = snapshot(12);
    extra.content_manifest.push(ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("media type"),
        byte_length: 4,
        sha256: Sha256::digest(b"else"),
    });
    extra.checksum = codec.checksum(&extra);
    assert_eq!(
        codec.encode(&extra),
        Err(ProjectionSnapshotCodecError::InvalidContract),
        "a manifest entry not reachable from projection state must not grant access"
    );

    let mut missing = snapshot(12);
    missing.content_manifest.clear();
    missing.checksum = codec.checksum(&missing);
    assert_eq!(
        codec.encode(&missing),
        Err(ProjectionSnapshotCodecError::InvalidContract),
        "every reachable ContentRef must be present in the exact manifest"
    );
}

#[test]
fn snapshot_rejects_every_conflicting_content_identity_dimension() {
    let codec = ProtobufConversationProjectionCodec;
    for conflict in ["hash", "length", "media"] {
        let mut value = snapshot(12);
        let mut reference = value.messages[0].content_ref.clone();
        match conflict {
            "hash" => reference.sha256 = Sha256::digest(b"different bytes"),
            "length" => reference.byte_length += 1,
            "media" => {
                reference.media_type =
                    MediaType::new("application/octet-stream").expect("media type")
            }
            _ => unreachable!(),
        }
        value.messages[0].imported_projection_ref = Some(reference);
        value.checksum = codec.checksum(&value);
        assert_eq!(
            codec.encode(&value),
            Err(ProjectionSnapshotCodecError::InvalidContract),
            "conflicting {conflict} must fail closed"
        );
    }
}

#[test]
fn selected_project_target_survives_snapshot_sealing_and_history_reload() {
    use musteragent_identity::{
        AssignmentEpoch, FencingToken, ProjectId, ProjectRevision, WorkItemId,
    };
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let target = musteragent_conversation::ProjectTaskTarget {
        project_id: ProjectId::generate(),
        work_item_id: WorkItemId::generate(),
        project_revision: ProjectRevision::new(9_007_199_254_740_993).expect("valid test fixture"),
        work_item_revision: ProjectRevision::new(2).expect("valid test fixture"),
        controller_conversation_id: value.conversation_id,
        controller_fencing_token: FencingToken::new(7).expect("valid test fixture"),
        assignment_epoch: Some(AssignmentEpoch::new(3).expect("valid test fixture")),
    };
    value.messages[0].role = MessageRole::User;
    value.messages[0].status = ProjectedMessageStatus::Committed;
    value.messages[0].assistant_output = None;
    value.messages[0].project_task_target = Some(target);
    value.checksum = codec.checksum(&value);
    let decoded = codec
        .decode(&codec.encode(&value).expect("valid test fixture"))
        .expect("valid test fixture");
    assert_eq!(decoded.messages[0].project_task_target, Some(target));
    assert_eq!(decoded, value);
    value.messages[0].project_task_target = None;
    value.checksum = codec.checksum(&value);
    let old = codec
        .decode(&codec.encode(&value).expect("valid test fixture"))
        .expect("valid test fixture");
    assert_eq!(old.messages[0].project_task_target, None);
}

#[test]
fn snapshot_round_trips_run_input_message_and_reads_legacy_snapshots_without_it() {
    let codec = ProtobufConversationProjectionCodec;
    let mut value = snapshot(12);
    let shared_ref = value.messages[0].content_ref.clone();
    let first_message_id = MessageId::generate();
    let second_message_id = MessageId::generate();
    let first_activity_id = ActivityId::generate();
    let second_activity_id = ActivityId::generate();
    let retry_activity_id = ActivityId::generate();
    let child_activity_id = ActivityId::generate();
    let first_run_id = RunId::generate();
    let second_run_id = RunId::generate();
    let retry_run_id = RunId::generate();
    let child_run_id = RunId::generate();

    value.messages = vec![
        ProjectedMessage {
            project_task_target: None,
            message_id: first_message_id,
            participant_id: None,
            role: MessageRole::User,
            content_ref: shared_ref.clone(),
            reasoning_summaries: Vec::new(),
            status: ProjectedMessageStatus::Committed,
            source_run_id: Some(first_run_id),
            committed_at: UtcTimestamp::from_unix_millis(100),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
            imported_projection_ref: None,
            assistant_output: None,
            partial_reason: None,
        },
        ProjectedMessage {
            project_task_target: None,
            message_id: second_message_id,
            participant_id: None,
            role: MessageRole::User,
            content_ref: shared_ref,
            reasoning_summaries: Vec::new(),
            status: ProjectedMessageStatus::Committed,
            source_run_id: Some(second_run_id),
            committed_at: UtcTimestamp::from_unix_millis(200),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
            imported_projection_ref: None,
            assistant_output: None,
            partial_reason: None,
        },
    ];
    let mut child_activity = terminal_activity(
        child_activity_id,
        ActivityKind::Delegation,
        ActivityStatus::Completed,
        350,
    );
    child_activity.parent_activity_id = Some(second_activity_id);
    child_activity.root_activity_id = second_activity_id;
    value.activities = vec![
        terminal_activity(
            first_activity_id,
            ActivityKind::Chat,
            ActivityStatus::Failed,
            100,
        ),
        terminal_activity(
            second_activity_id,
            ActivityKind::Chat,
            ActivityStatus::Completed,
            200,
        ),
        terminal_activity(
            retry_activity_id,
            ActivityKind::Chat,
            ActivityStatus::Completed,
            300,
        ),
        child_activity,
    ];
    let mut first_run = terminal_run(
        first_run_id,
        first_activity_id,
        None,
        None,
        RunStatus::Failed,
        110,
    );
    first_run.input_message_id = Some(first_message_id);
    let mut second_run = terminal_run(
        second_run_id,
        second_activity_id,
        None,
        None,
        RunStatus::Completed,
        210,
    );
    second_run.input_message_id = Some(second_message_id);
    let mut retry_run = terminal_run(
        retry_run_id,
        retry_activity_id,
        Some(first_run_id),
        None,
        RunStatus::Completed,
        310,
    );
    retry_run.input_message_id = Some(first_message_id);
    retry_run.retry_source_message_id = Some(first_message_id);
    retry_run.retry_source_attempt_number = Some(AttemptNumber::new(1).expect("attempt one"));
    let child_run = terminal_run(
        child_run_id,
        child_activity_id,
        Some(second_run_id),
        Some(DelegationId::generate()),
        RunStatus::Completed,
        360,
    );
    value.runs = vec![first_run, second_run, retry_run, child_run];
    value.content_manifest = canonical_content_manifest(&value).expect("manifest");
    value.checksum = codec.checksum(&value);

    let encoded = codec.encode(&value).expect("encode snapshot");
    let decoded = codec.decode(&encoded).expect("decode snapshot");
    assert_eq!(decoded, value);

    let mut legacy_wire = conversation_projection_snapshot_message(&value);
    for run in &mut legacy_wire.runs {
        run.input_message_id = None;
    }
    legacy_wire.checksum = None;
    let legacy_checksum = Sha256::digest(legacy_wire.encode_to_vec());
    legacy_wire.checksum = Some(crate::wire::Sha256 {
        value: legacy_checksum.into_bytes().to_vec(),
    });
    let decoded_legacy = codec
        .decode(&legacy_wire.encode_to_vec())
        .expect("decode legacy snapshot without input_message_id");
    assert!(
        decoded_legacy
            .runs
            .iter()
            .all(|run| run.input_message_id.is_none())
    );

    let mut invalid_wire = conversation_projection_snapshot_message(&value);
    invalid_wire.runs[0].input_message_id = Some(crate::wire::MessageId {
        value: "invalid".to_owned(),
    });
    invalid_wire.checksum = None;
    let invalid_checksum = Sha256::digest(invalid_wire.encode_to_vec());
    invalid_wire.checksum = Some(crate::wire::Sha256 {
        value: invalid_checksum.into_bytes().to_vec(),
    });
    assert_eq!(
        codec.decode(&invalid_wire.encode_to_vec()),
        Err(ProjectionSnapshotCodecError::InvalidContract)
    );
}
