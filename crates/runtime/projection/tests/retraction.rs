use musteragent_activity::{ActivityKind, ActivityStatus};
use musteragent_approval::InteractionKind;
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_conversation::MessageRole;
use musteragent_identity::{
    ActivityId, ArtifactId, ContentId, ConversationId, ConversationSeq, DelegationId, HomeEpoch,
    InteractionId, InvocationId, MessageId, OutputId, ParticipantId, ProjectionSnapshotId, RunId,
    TenantId, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_projection::{
    ConversationProjectionSnapshot, PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedActivity,
    ProjectedArtifact, ProjectedInteraction, ProjectedMessage, ProjectedMessageStatus,
    ProjectedOutputCheckpoint, ProjectedReasoningSummary, ProjectedRun, ProjectedStreamOutput,
    ProjectedToolItem, ProjectedToolStatus, ProjectionRetraction,
};
use musteragent_run::RunStatus;
use musteragent_runtime_contract::OutputChannel;

#[test]
fn snapshot_tail_retraction_removes_the_entire_derived_run() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let root_run_id = RunId::generate();
    let child_run_id = RunId::generate();
    let failed_child_run_id = RunId::generate();
    let retained_run_id = RunId::generate();
    let user_message_id = MessageId::generate();
    let assistant_message_id = MessageId::generate();
    let user_content = content_ref("user");
    let assistant_content = content_ref("assistant");
    let reasoning_content = content_ref("reasoning");
    let tool_request = content_ref("tool-request");
    let tool_result = content_ref("tool-result");
    let interaction_prompt = content_ref("interaction");
    let artifact_content = content_ref("artifact");
    let unbound_progress = content_ref("unbound-progress");
    let retained_content = content_ref("retained");
    let retained_message_id = MessageId::generate();
    let now = UtcTimestamp::from_unix_millis(1_800_000_000_000);
    let output_id = OutputId::generate();
    let unbound_output_id = OutputId::generate();
    let root_activity_id = ActivityId::generate();
    let child_activity_id = ActivityId::generate();
    let failed_child_activity_id = ActivityId::generate();
    let retained_activity_id = ActivityId::generate();
    let child_delegation_id = DelegationId::generate();
    let failed_child_delegation_id = DelegationId::generate();
    let mut snapshot = ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id,
        conversation_id,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        through_conversation_seq: ConversationSeq::new(9).expect("sequence").into(),
        messages: vec![
            projected_message(user_message_id, root_run_id, user_content.clone(), now),
            ProjectedMessage {
                project_task_target: None,
                reasoning_summaries: vec![ProjectedReasoningSummary {
                    checkpoint: checkpoint(output_id, reasoning_content.byte_length),
                    content_ref: reasoning_content.clone(),
                }],
                ..projected_message(
                    assistant_message_id,
                    child_run_id,
                    assistant_content.clone(),
                    now,
                )
            },
            projected_message(
                retained_message_id,
                retained_run_id,
                retained_content.clone(),
                now,
            ),
        ],
        tool_timeline: vec![ProjectedToolItem {
            invocation_id: InvocationId::generate(),
            message_id: Some(assistant_message_id),
            run_id: child_run_id,
            tool_call_id: ToolCallId::new("call-retracted").expect("call ID"),
            tool_ref: "web.fetch@2".to_owned(),
            status: ProjectedToolStatus::Completed,
            request_ref: tool_request.clone(),
            result_ref: Some(tool_result.clone()),
            requested_conversation_seq: ConversationSeq::new(4).expect("sequence"),
            requested_at: now,
            started_at: Some(now),
            finished_at: Some(now),
            output_artifact_ids: Vec::new(),
        }],
        context_compactions: Vec::new(),
        pending_interactions: vec![ProjectedInteraction {
            interaction_id: InteractionId::generate(),
            run_id: failed_child_run_id,
            kind: InteractionKind::Approval,
            prompt_ref: interaction_prompt.clone(),
            response_schema: SchemaRef {
                schema_key: VersionedKey::new("schema.approval@1").expect("schema"),
                schema_hash: Sha256::digest(b"schema.approval@1"),
            },
            requested_at: now,
            expires_at: None,
        }],
        activities: vec![
            projected_activity(
                root_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Cancelled,
                None,
                root_activity_id,
                now,
            ),
            projected_activity(
                child_activity_id,
                ActivityKind::Delegation,
                ActivityStatus::Completed,
                Some(root_activity_id),
                root_activity_id,
                now,
            ),
            projected_activity(
                failed_child_activity_id,
                ActivityKind::Delegation,
                ActivityStatus::Failed,
                Some(root_activity_id),
                root_activity_id,
                now,
            ),
            projected_activity(
                retained_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Completed,
                None,
                retained_activity_id,
                now,
            ),
        ],
        runs: vec![
            projected_run(
                root_run_id,
                root_activity_id,
                None,
                None,
                RunStatus::Cancelled,
                now,
            ),
            projected_run(
                child_run_id,
                child_activity_id,
                Some(root_run_id),
                Some(child_delegation_id),
                RunStatus::Completed,
                now,
            ),
            projected_run(
                failed_child_run_id,
                failed_child_activity_id,
                Some(root_run_id),
                Some(failed_child_delegation_id),
                RunStatus::Failed,
                now,
            ),
            projected_run(
                retained_run_id,
                retained_activity_id,
                None,
                None,
                RunStatus::Completed,
                now,
            ),
        ],
        active_runs: Vec::new(),
        artifacts: vec![ProjectedArtifact {
            artifact_id: ArtifactId::generate(),
            activity_id: Some(child_activity_id),
            created_by_run_id: Some(child_run_id),
            updated_by_run_id: Some(child_run_id),
            presentation_run_id: Some(child_run_id),
            kind: ArtifactKind::Note,
            lifecycle: ArtifactLifecycle::Published,
            current_version: VersionNumber::new(1).expect("version"),
            title: "artifact".to_owned(),
            current_content_ref: artifact_content.clone(),
            current_dependency_refs: Vec::new(),
            updated_at: now,
        }],
        stream_outputs: vec![ProjectedStreamOutput {
            checkpoint: checkpoint(unbound_output_id, unbound_progress.byte_length),
            message_id: None,
            run_id: child_run_id,
            channel: OutputChannel::ToolProgress,
            content_ref: unbound_progress.clone(),
            first_conversation_seq: ConversationSeq::new(5).expect("sequence"),
            occurred_at: now,
        }],
        content_manifest: vec![
            user_content,
            assistant_content,
            reasoning_content,
            tool_request,
            tool_result,
            interaction_prompt,
            artifact_content,
            unbound_progress,
            retained_content.clone(),
        ],
        checksum: Sha256::digest(b"snapshot-before-tail"),
        created_at: now,
    };

    ProjectionRetraction::new(user_message_id, None)
        .apply_to_snapshot(&mut snapshot)
        .expect("retraction must preserve a canonical Content manifest");
    // Historical SQLite tails may contain one retraction per model invocation
    // for a reused logical Assistant message. Replaying those duplicate facts
    // after the root graph has gone must remain a harmless no-op.
    for _ in 0..2 {
        ProjectionRetraction::new(assistant_message_id, Some(child_run_id))
            .apply_to_snapshot(&mut snapshot)
            .expect("duplicate child retraction must be idempotent");
    }

    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].message_id, retained_message_id);
    assert!(snapshot.tool_timeline.is_empty());
    assert!(snapshot.pending_interactions.is_empty());
    assert!(snapshot.active_runs.is_empty());
    assert_eq!(snapshot.runs.len(), 1);
    assert_eq!(snapshot.runs[0].run_id, retained_run_id);
    assert_eq!(snapshot.activities.len(), 1);
    assert_eq!(snapshot.activities[0].activity_id, retained_activity_id);
    assert!(snapshot.artifacts.is_empty());
    assert!(snapshot.stream_outputs.is_empty());
    assert_eq!(snapshot.content_manifest, vec![retained_content]);
}

fn projected_activity(
    activity_id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
    parent_activity_id: Option<ActivityId>,
    root_activity_id: ActivityId,
    now: UtcTimestamp,
) -> ProjectedActivity {
    ProjectedActivity {
        activity_id,
        kind,
        status,
        requested_by_participant_id: Some(ParticipantId::generate()),
        parent_activity_id,
        root_activity_id,
        policy_snapshot_ref: VersionedKey::new("policy.fixture@1").expect("policy"),
        started_at: now,
        finished_at: Some(now),
        delegation_origin: None,
        coordination_origin: None,
    }
}

fn projected_run(
    run_id: RunId,
    activity_id: ActivityId,
    parent_run_id: Option<RunId>,
    delegation_id: Option<DelegationId>,
    status: RunStatus,
    now: UtcTimestamp,
) -> ProjectedRun {
    ProjectedRun {
        run_id,
        activity_id,
        executor_participant_id: Some(ParticipantId::generate()),
        parent_run_id,
        delegation_id,
        retry_source_message_id: None,
        retry_source_attempt_number: None,
        terminal_failure: None,
        retry_failure: None,
        status,
        wait_reason: None,
        attempt_number: None,
        next_attempt_number: None,
        retry_not_before: None,
        admitted_at: now,
        started_at: Some(now),
        finished_at: Some(now),
    }
}

fn projected_message(
    message_id: MessageId,
    run_id: RunId,
    content_ref: ContentRef,
    now: UtcTimestamp,
) -> ProjectedMessage {
    ProjectedMessage {
        project_task_target: None,
        message_id,
        participant_id: Some(ParticipantId::generate()),
        role: MessageRole::User,
        content_ref,
        reasoning_summaries: Vec::new(),
        status: ProjectedMessageStatus::Committed,
        source_run_id: Some(run_id),
        committed_at: now,
        attachment_refs: Vec::new(),
        requested_skill_refs: Vec::new(),
        imported_projection_ref: None,
        assistant_output: None,
        partial_reason: None,
    }
}

fn checkpoint(output_id: OutputId, next_byte_offset: u64) -> ProjectedOutputCheckpoint {
    ProjectedOutputCheckpoint {
        output_id,
        next_frame_index: 1,
        next_byte_offset,
        completed: true,
    }
}

fn content_ref(seed: &str) -> ContentRef {
    let bytes = seed.as_bytes();
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}
