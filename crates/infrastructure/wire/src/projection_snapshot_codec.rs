use std::collections::BTreeSet;
use std::str::FromStr;

use musteragent_activity::{ActivityKind, ActivityStatus};
use musteragent_approval::InteractionKind;
use musteragent_artifact::{ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_conversation::{MessageRole, PartialMessageReason};
use musteragent_identity::{
    AttemptNumber, HomeEpoch, IdentityError, LedgerOffset, ToolCallId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_projection::{
    ConversationProjectionSnapshot, ConversationProjectionSnapshotCodec,
    PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedActivity, ProjectedArtifact,
    ProjectedContextCompaction, ProjectedContextCompactionStatus, ProjectedInteraction,
    ProjectedMessage, ProjectedMessageStatus, ProjectedOutputCheckpoint, ProjectedReasoningSummary,
    ProjectedRun, ProjectedRunFailure, ProjectedStreamOutput, ProjectedToolItem,
    ProjectedToolStatus, ProjectionSnapshotCodecError, canonical_content_manifest,
};
use musteragent_run::{RunStatus, RunWaitReason};
use musteragent_runtime_contract::{AttachmentRef, DisplayText, ErrorCode, OutputChannel};
use musteragent_runtime_model::ModelRouteRef;
use prost::Message;
use prost_types::Timestamp;

use crate::wire;

const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLISECOND: i32 = 1_000_000;
#[derive(Debug, Copy, Clone, Default)]
pub struct ProtobufConversationProjectionCodec;

impl ConversationProjectionSnapshotCodec for ProtobufConversationProjectionCodec {
    fn checksum(&self, snapshot: &ConversationProjectionSnapshot) -> Sha256 {
        let mut value = conversation_projection_snapshot_message(snapshot);
        value.checksum = None;
        Sha256::digest(value.encode_to_vec())
    }

    fn encode(
        &self,
        snapshot: &ConversationProjectionSnapshot,
    ) -> Result<Vec<u8>, ProjectionSnapshotCodecError> {
        validate_snapshot(snapshot)?;
        if self.checksum(snapshot) != snapshot.checksum {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
        Ok(conversation_projection_snapshot_message(snapshot).encode_to_vec())
    }

    fn decode(
        &self,
        bytes: &[u8],
    ) -> Result<ConversationProjectionSnapshot, ProjectionSnapshotCodecError> {
        let value = wire::ConversationProjectionSnapshot::decode(bytes)
            .map_err(|_| ProjectionSnapshotCodecError::Malformed)?;
        // Verify the exact wire bytes before validating the current domain contract.
        let supplied_checksum = value
            .checksum
            .as_ref()
            .ok_or(ProjectionSnapshotCodecError::InvalidContract)
            .and_then(|checksum| {
                Sha256::from_bytes(&checksum.value)
                    .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)
            })?;
        let mut checksumless = value.clone();
        checksumless.checksum = None;
        if Sha256::digest(checksumless.encode_to_vec()) != supplied_checksum {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }

        let snapshot = from_wire(value)?;
        validate_snapshot(&snapshot)?;
        if self.checksum(&snapshot) != snapshot.checksum {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
        Ok(snapshot)
    }
}

fn validate_snapshot(
    snapshot: &ConversationProjectionSnapshot,
) -> Result<(), ProjectionSnapshotCodecError> {
    if snapshot.schema_version != PROJECTION_SNAPSHOT_SCHEMA_VERSION {
        return Err(ProjectionSnapshotCodecError::UnsupportedSchema);
    }
    if snapshot
        .messages
        .windows(2)
        .any(|pair| pair[0].committed_at > pair[1].committed_at)
    {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    if snapshot.through_conversation_seq.get() == 0 {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    if canonical_content_manifest(snapshot)
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?
        != snapshot.content_manifest
    {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    let mut output_ids = BTreeSet::new();
    for message in &snapshot.messages {
        if let Some(checkpoint) = &message.assistant_output {
            validate_output_checkpoint(checkpoint, &message.content_ref, &mut output_ids)?;
        }
        for reasoning in &message.reasoning_summaries {
            validate_output_checkpoint(
                &reasoning.checkpoint,
                &reasoning.content_ref,
                &mut output_ids,
            )?;
        }
    }
    for output in &snapshot.stream_outputs {
        validate_output_checkpoint(&output.checkpoint, &output.content_ref, &mut output_ids)?;
    }
    let mut compaction_ids = BTreeSet::new();
    for compaction in &snapshot.context_compactions {
        let lifecycle_is_coherent = match compaction.status {
            ProjectedContextCompactionStatus::Running => {
                compaction.finished_at.is_none() && compaction.input_tokens_after.is_none()
            }
            ProjectedContextCompactionStatus::Succeeded => {
                compaction.finished_at.is_some()
                    && compaction.input_tokens_after.is_some()
                    && compaction.error_code.is_none()
            }
            ProjectedContextCompactionStatus::FailedContinued => {
                compaction.finished_at.is_some()
                    && compaction.input_tokens_after.is_some()
                    && compaction.error_code.is_some()
            }
            ProjectedContextCompactionStatus::RetryScheduled
            | ProjectedContextCompactionStatus::FailedStopped
            | ProjectedContextCompactionStatus::Indeterminate => {
                compaction.finished_at.is_some()
                    && compaction.input_tokens_after.is_none()
                    && compaction.error_code.is_some()
            }
            ProjectedContextCompactionStatus::Interrupted => {
                compaction.finished_at.is_some() && compaction.input_tokens_after.is_none()
            }
        };
        if compaction.loop_iteration == 0
            || compaction.input_tokens_before == 0
            || compaction.source_seq_from > compaction.source_seq_to
            || !compaction_ids.insert(compaction.invocation_id)
            || !lifecycle_is_coherent
            || compaction
                .input_tokens_after
                .is_some_and(|after| after == 0 || after > compaction.input_tokens_before)
        {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
    }
    if snapshot.context_compactions.windows(2).any(|pair| {
        (pair[0].started_conversation_seq, pair[0].invocation_id)
            >= (pair[1].started_conversation_seq, pair[1].invocation_id)
    }) {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    validate_activity_run_graph(snapshot)?;
    Ok(())
}

fn validate_activity_run_graph(
    snapshot: &ConversationProjectionSnapshot,
) -> Result<(), ProjectionSnapshotCodecError> {
    if snapshot.activities.windows(2).any(|pair| {
        (pair[0].started_at, pair[0].activity_id) >= (pair[1].started_at, pair[1].activity_id)
    }) || snapshot
        .runs
        .windows(2)
        .any(|pair| (pair[0].admitted_at, pair[0].run_id) >= (pair[1].admitted_at, pair[1].run_id))
    {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    let activity_ids = snapshot
        .activities
        .iter()
        .map(|item| item.activity_id)
        .collect::<BTreeSet<_>>();
    let run_ids = snapshot
        .runs
        .iter()
        .map(|item| item.run_id)
        .collect::<BTreeSet<_>>();
    if activity_ids.len() != snapshot.activities.len() || run_ids.len() != snapshot.runs.len() {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    for activity in &snapshot.activities {
        if !activity_ids.contains(&activity.root_activity_id)
            || activity
                .parent_activity_id
                .is_some_and(|parent| !activity_ids.contains(&parent))
            || matches!(
                activity.status,
                ActivityStatus::Completed | ActivityStatus::Failed | ActivityStatus::Cancelled
            ) != activity.finished_at.is_some()
        {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
    }
    for run in &snapshot.runs {
        if let Some(failure) = &run.retry_failure
            && (run.status != RunStatus::Waiting
                || run.wait_reason != Some(RunWaitReason::RetryBackoff)
                || failure.conversation_sequence.get() > snapshot.through_conversation_seq.get()
                || failure.error.validate().is_err())
        {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
        if let Some(failure) = &run.terminal_failure
            && (run.status != RunStatus::Failed
                || failure.conversation_sequence.get() > snapshot.through_conversation_seq.get()
                || failure.error.validate().is_err())
        {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
        if !activity_ids.contains(&run.activity_id)
            || run
                .parent_run_id
                .is_some_and(|parent| !run_ids.contains(&parent))
            || (run.delegation_id.is_some() && run.parent_run_id.is_none())
            || run.status.is_terminal() != run.finished_at.is_some()
        {
            return Err(ProjectionSnapshotCodecError::InvalidContract);
        }
    }
    let expected_active = snapshot
        .runs
        .iter()
        .filter(|item| !item.status.is_terminal())
        .collect::<Vec<_>>();
    if expected_active.len() != snapshot.active_runs.len()
        || expected_active
            .into_iter()
            .zip(&snapshot.active_runs)
            .any(|(expected, actual)| expected != actual)
    {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    Ok(())
}

fn validate_output_checkpoint(
    checkpoint: &ProjectedOutputCheckpoint,
    reference: &ContentRef,
    output_ids: &mut BTreeSet<musteragent_identity::OutputId>,
) -> Result<(), ProjectionSnapshotCodecError> {
    if checkpoint.next_frame_index == 0
        || checkpoint.next_byte_offset != reference.byte_length
        || !output_ids.insert(checkpoint.output_id)
    {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    Ok(())
}

/// Shared domain-to-wire mapping used by the persistence codec and gRPC edge.
#[must_use]
pub fn conversation_projection_snapshot_message(
    value: &ConversationProjectionSnapshot,
) -> wire::ConversationProjectionSnapshot {
    wire::ConversationProjectionSnapshot {
        schema_version: value.schema_version.clone(),
        snapshot_id: Some(wire::ProjectionSnapshotId {
            value: value.snapshot_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        conversation_id: Some(wire::ConversationId {
            value: value.conversation_id.to_string(),
        }),
        home_epoch: value.home_epoch.get(),
        through_conversation_seq: value.through_conversation_seq.get(),
        messages: value.messages.iter().map(projected_message).collect(),
        tool_timeline: value.tool_timeline.iter().map(projected_tool).collect(),
        context_compactions: value
            .context_compactions
            .iter()
            .map(projected_context_compaction)
            .collect(),
        pending_interactions: value
            .pending_interactions
            .iter()
            .map(projected_interaction)
            .collect(),
        activities: value.activities.iter().map(projected_activity).collect(),
        runs: value.runs.iter().map(projected_run).collect(),
        active_runs: value.active_runs.iter().map(projected_run).collect(),
        artifacts: value.artifacts.iter().map(projected_artifact).collect(),
        content_manifest: value
            .content_manifest
            .iter()
            .cloned()
            .map(content_ref)
            .collect(),
        checksum: Some(sha256(value.checksum)),
        created_at: Some(timestamp(value.created_at)),
        stream_outputs: value
            .stream_outputs
            .iter()
            .map(projected_stream_output)
            .collect(),
    }
}

fn from_wire(
    value: wire::ConversationProjectionSnapshot,
) -> Result<ConversationProjectionSnapshot, ProjectionSnapshotCodecError> {
    if value.schema_version != PROJECTION_SNAPSHOT_SCHEMA_VERSION {
        return Err(ProjectionSnapshotCodecError::UnsupportedSchema);
    }
    Ok(ConversationProjectionSnapshot {
        schema_version: value.schema_version,
        snapshot_id: parse_id(value.snapshot_id.map(|item| item.value))?,
        tenant_id: parse_id(value.tenant_id.map(|item| item.value))?,
        conversation_id: parse_id(value.conversation_id.map(|item| item.value))?,
        home_epoch: HomeEpoch::new(value.home_epoch)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        through_conversation_seq: LedgerOffset::new(value.through_conversation_seq),
        messages: value
            .messages
            .into_iter()
            .map(decode_projected_message)
            .collect::<Result<_, _>>()?,
        tool_timeline: value
            .tool_timeline
            .into_iter()
            .map(decode_projected_tool)
            .collect::<Result<_, _>>()?,
        context_compactions: value
            .context_compactions
            .into_iter()
            .map(decode_projected_context_compaction)
            .collect::<Result<_, _>>()?,
        pending_interactions: value
            .pending_interactions
            .into_iter()
            .map(decode_projected_interaction)
            .collect::<Result<_, _>>()?,
        activities: value
            .activities
            .into_iter()
            .map(decode_projected_activity)
            .collect::<Result<_, _>>()?,
        runs: value
            .runs
            .into_iter()
            .map(decode_projected_run)
            .collect::<Result<_, _>>()?,
        active_runs: value
            .active_runs
            .into_iter()
            .map(decode_projected_run)
            .collect::<Result<_, _>>()?,
        artifacts: value
            .artifacts
            .into_iter()
            .map(decode_projected_artifact)
            .collect::<Result<_, _>>()?,
        content_manifest: value
            .content_manifest
            .into_iter()
            .map(decode_content_ref)
            .collect::<Result<_, _>>()?,
        checksum: decode_sha256(value.checksum)?,
        created_at: decode_timestamp(value.created_at)?,
        stream_outputs: value
            .stream_outputs
            .into_iter()
            .map(decode_projected_stream_output)
            .collect::<Result<_, _>>()?,
    })
}

fn projected_activity(value: &ProjectedActivity) -> wire::ProjectedActivity {
    wire::ProjectedActivity {
        activity_id: Some(wire::ActivityId {
            value: value.activity_id.to_string(),
        }),
        kind: activity_kind(value.kind),
        status: activity_status(value.status),
        requested_by_participant_id: value.requested_by_participant_id.map(|item| {
            wire::ParticipantId {
                value: item.to_string(),
            }
        }),
        parent_activity_id: value.parent_activity_id.map(|item| wire::ActivityId {
            value: item.to_string(),
        }),
        root_activity_id: Some(wire::ActivityId {
            value: value.root_activity_id.to_string(),
        }),
        policy_snapshot_ref: value.policy_snapshot_ref.as_str().to_owned(),
        started_at: Some(timestamp(value.started_at)),
        finished_at: value.finished_at.map(timestamp),
        coordination_origin: value.coordination_origin.as_ref().map(|origin| {
            wire::ProjectedCoordinationOrigin {
                mailbox_message_id: Some(wire::InterSessionMessageId {
                    value: origin.mailbox_message_id.to_string(),
                }),
                source_conversation_id: Some(wire::ConversationId {
                    value: origin.source_conversation_id.to_string(),
                }),
                payload_ref: origin.payload_ref.clone().map(content_ref),
                occurred_at: origin.occurred_at.map(timestamp),
            }
        }),
        delegation_origin: value.delegation_origin.as_ref().map(|origin| {
            wire::ProjectedDelegationOrigin {
                delegation_id: Some(wire::DelegationId {
                    value: origin.delegation_id.to_string(),
                }),
                parent_invocation_id: Some(wire::InvocationId {
                    value: origin.parent_invocation_id.to_string(),
                }),
                objective_ref: origin.objective_ref.clone().map(content_ref),
            }
        }),
    }
}

fn decode_projected_activity(
    value: wire::ProjectedActivity,
) -> Result<ProjectedActivity, ProjectionSnapshotCodecError> {
    Ok(ProjectedActivity {
        activity_id: parse_id(value.activity_id.map(|item| item.value))?,
        kind: decode_activity_kind(value.kind)?,
        status: decode_activity_status(value.status)?,
        requested_by_participant_id: value
            .requested_by_participant_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        parent_activity_id: value
            .parent_activity_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        root_activity_id: parse_id(value.root_activity_id.map(|item| item.value))?,
        policy_snapshot_ref: VersionedKey::new(value.policy_snapshot_ref)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        started_at: decode_timestamp(value.started_at)?,
        finished_at: value.finished_at.map(decode_timestamp_value).transpose()?,
        delegation_origin: value
            .delegation_origin
            .map(|origin| {
                Ok(musteragent_projection::ProjectedDelegationOrigin {
                    delegation_id: parse_id(origin.delegation_id.map(|item| item.value))?,
                    parent_invocation_id: parse_id(
                        origin.parent_invocation_id.map(|item| item.value),
                    )?,
                    objective_ref: origin.objective_ref.map(decode_content_ref).transpose()?,
                })
            })
            .transpose()?,
        coordination_origin: value
            .coordination_origin
            .map(|origin| {
                Ok(musteragent_projection::ProjectedCoordinationOrigin {
                    mailbox_message_id: parse_id(origin.mailbox_message_id.map(|item| item.value))?,
                    source_conversation_id: parse_id(
                        origin.source_conversation_id.map(|item| item.value),
                    )?,
                    payload_ref: origin.payload_ref.map(decode_content_ref).transpose()?,
                    occurred_at: origin.occurred_at.map(decode_timestamp_value).transpose()?,
                })
            })
            .transpose()?,
    })
}

fn projected_stream_output(value: &ProjectedStreamOutput) -> wire::ProjectedStreamOutput {
    wire::ProjectedStreamOutput {
        output_id: Some(wire::OutputId {
            value: value.checkpoint.output_id.to_string(),
        }),
        message_id: value.message_id.map(|item| wire::MessageId {
            value: item.to_string(),
        }),
        channel: output_channel(value.channel),
        content_ref: Some(content_ref(value.content_ref.clone())),
        completed: value.checkpoint.completed,
        next_frame_index: value.checkpoint.next_frame_index,
        next_byte_offset: value.checkpoint.next_byte_offset,
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        first_conversation_seq: value.first_conversation_seq.get(),
        occurred_at: Some(timestamp(value.occurred_at)),
    }
}

fn decode_projected_stream_output(
    value: wire::ProjectedStreamOutput,
) -> Result<ProjectedStreamOutput, ProjectionSnapshotCodecError> {
    Ok(ProjectedStreamOutput {
        checkpoint: decode_output_checkpoint(
            value.output_id.map(|item| item.value),
            value.next_frame_index,
            value.next_byte_offset,
            value.completed,
        )?,
        message_id: value
            .message_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        run_id: parse_id(value.run_id.map(|item| item.value))?,
        channel: decode_output_channel(value.channel)?,
        content_ref: decode_content_ref(
            value
                .content_ref
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        first_conversation_seq: musteragent_identity::ConversationSeq::new(
            value.first_conversation_seq,
        )
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        occurred_at: decode_timestamp(value.occurred_at)?,
    })
}

fn projected_message(value: &ProjectedMessage) -> wire::ProjectedMessage {
    wire::ProjectedMessage {
        message_id: Some(wire::MessageId {
            value: value.message_id.to_string(),
        }),
        participant_id: value.participant_id.map(|item| wire::ParticipantId {
            value: item.to_string(),
        }),
        role: message_role(value.role),
        content_ref: Some(content_ref(value.content_ref.clone())),
        reasoning_summaries: value
            .reasoning_summaries
            .iter()
            .map(|summary| wire::ProjectedReasoningSummary {
                output_id: Some(wire::OutputId {
                    value: summary.checkpoint.output_id.to_string(),
                }),
                content_ref: Some(content_ref(summary.content_ref.clone())),
                completed: summary.checkpoint.completed,
                next_frame_index: summary.checkpoint.next_frame_index,
                next_byte_offset: summary.checkpoint.next_byte_offset,
            })
            .collect(),
        status: projected_message_status(value.status),
        source_run_id: value.source_run_id.map(|item| wire::RunId {
            value: item.to_string(),
        }),
        committed_at: Some(timestamp(value.committed_at)),
        attachment_refs: value
            .attachment_refs
            .iter()
            .map(|attachment| wire::AttachmentRef {
                content_ref: Some(content_ref(attachment.content_ref.clone())),
                display_name: attachment.display_name.as_str().to_owned(),
            })
            .collect(),
        requested_skill_refs: value
            .requested_skill_refs
            .iter()
            .map(|reference| reference.as_str().to_owned())
            .collect(),
        project_task_target: value
            .project_task_target
            .map(crate::project_task_codec::encode_target),
        imported_projection_ref: value.imported_projection_ref.clone().map(content_ref),
        assistant_output_id: value.assistant_output.as_ref().map(|item| wire::OutputId {
            value: item.output_id.to_string(),
        }),
        partial_reason: value.partial_reason.map(partial_reason).unwrap_or_default(),
        assistant_next_frame_index: value
            .assistant_output
            .as_ref()
            .map_or(0, |item| item.next_frame_index),
        assistant_next_byte_offset: value
            .assistant_output
            .as_ref()
            .map_or(0, |item| item.next_byte_offset),
        assistant_output_completed: value
            .assistant_output
            .as_ref()
            .is_some_and(|item| item.completed),
    }
}

fn output_channel(value: OutputChannel) -> i32 {
    (match value {
        OutputChannel::AssistantText => wire::OutputChannel::AssistantText,
        OutputChannel::ReasoningSummary => wire::OutputChannel::ReasoningSummary,
        OutputChannel::ToolArguments => wire::OutputChannel::ToolArguments,
        OutputChannel::ToolProgress => wire::OutputChannel::ToolProgress,
        OutputChannel::ArtifactContent => wire::OutputChannel::ArtifactContent,
    }) as i32
}

fn decode_output_channel(value: i32) -> Result<OutputChannel, ProjectionSnapshotCodecError> {
    match wire::OutputChannel::try_from(value) {
        Ok(wire::OutputChannel::AssistantText) => Ok(OutputChannel::AssistantText),
        Ok(wire::OutputChannel::ReasoningSummary) => Ok(OutputChannel::ReasoningSummary),
        Ok(wire::OutputChannel::ToolArguments) => Ok(OutputChannel::ToolArguments),
        Ok(wire::OutputChannel::ToolProgress) => Ok(OutputChannel::ToolProgress),
        Ok(wire::OutputChannel::ArtifactContent) => Ok(OutputChannel::ArtifactContent),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn decode_projected_message(
    value: wire::ProjectedMessage,
) -> Result<ProjectedMessage, ProjectionSnapshotCodecError> {
    Ok(ProjectedMessage {
        message_id: parse_id(value.message_id.map(|item| item.value))?,
        participant_id: value
            .participant_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        role: decode_message_role(value.role)?,
        content_ref: decode_content_ref(
            value
                .content_ref
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        reasoning_summaries: value
            .reasoning_summaries
            .into_iter()
            .map(|summary| {
                Ok(ProjectedReasoningSummary {
                    checkpoint: decode_output_checkpoint(
                        summary.output_id.map(|item| item.value),
                        summary.next_frame_index,
                        summary.next_byte_offset,
                        summary.completed,
                    )?,
                    content_ref: decode_content_ref(
                        summary
                            .content_ref
                            .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
                    )?,
                })
            })
            .collect::<Result<_, ProjectionSnapshotCodecError>>()?,
        status: decode_projected_message_status(value.status)?,
        source_run_id: value
            .source_run_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        committed_at: decode_timestamp(value.committed_at)?,
        attachment_refs: value
            .attachment_refs
            .into_iter()
            .map(|attachment| {
                Ok(AttachmentRef {
                    content_ref: decode_content_ref(
                        attachment
                            .content_ref
                            .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
                    )?,
                    display_name: DisplayText::new(attachment.display_name)
                        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
                })
            })
            .collect::<Result<_, ProjectionSnapshotCodecError>>()?,
        requested_skill_refs: value
            .requested_skill_refs
            .into_iter()
            .map(|reference| {
                VersionedKey::new(reference)
                    .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)
            })
            .collect::<Result<_, _>>()?,
        project_task_target: value
            .project_task_target
            .map(crate::project_task_codec::decode_target)
            .transpose()
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        imported_projection_ref: value
            .imported_projection_ref
            .map(decode_content_ref)
            .transpose()?,
        assistant_output: value
            .assistant_output_id
            .map(|item| {
                decode_output_checkpoint(
                    Some(item.value),
                    value.assistant_next_frame_index,
                    value.assistant_next_byte_offset,
                    value.assistant_output_completed,
                )
            })
            .transpose()?,
        partial_reason: decode_optional_partial_reason(value.partial_reason)?,
    })
}

fn decode_output_checkpoint(
    output_id: Option<String>,
    next_frame_index: u64,
    next_byte_offset: u64,
    completed: bool,
) -> Result<ProjectedOutputCheckpoint, ProjectionSnapshotCodecError> {
    if next_frame_index == 0 {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    Ok(ProjectedOutputCheckpoint {
        output_id: parse_id(output_id)?,
        next_frame_index,
        next_byte_offset,
        completed,
    })
}

fn projected_tool(value: &ProjectedToolItem) -> wire::ProjectedToolItem {
    wire::ProjectedToolItem {
        invocation_id: Some(wire::InvocationId {
            value: value.invocation_id.to_string(),
        }),
        message_id: value.message_id.map(|item| wire::MessageId {
            value: item.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        tool_call_id: value.tool_call_id.as_str().to_owned(),
        tool_ref: value.tool_ref.clone(),
        status: projected_tool_status(value.status),
        request_ref: Some(content_ref(value.request_ref.clone())),
        result_ref: value.result_ref.clone().map(content_ref),
        started_at: value.started_at.map(timestamp),
        finished_at: value.finished_at.map(timestamp),
        requested_at: Some(timestamp(value.requested_at)),
        requested_conversation_seq: value.requested_conversation_seq.get(),
        output_artifact_ids: value
            .output_artifact_ids
            .iter()
            .map(|item| wire::ArtifactId {
                value: item.to_string(),
            })
            .collect(),
    }
}

fn projected_context_compaction(
    value: &ProjectedContextCompaction,
) -> wire::ProjectedContextCompaction {
    wire::ProjectedContextCompaction {
        invocation_id: Some(wire::InvocationId {
            value: value.invocation_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        loop_iteration: value.loop_iteration,
        status: projected_context_compaction_status(value.status),
        model_route_ref: Some(wire::ModelRouteRef {
            route_key: value.model_route_ref.as_str().to_owned(),
        }),
        input_tokens_before: value.input_tokens_before,
        input_tokens_after: value.input_tokens_after,
        summarized_record_count: value.summarized_record_count,
        retained_record_count: value.retained_record_count,
        source_seq_from: value.source_seq_from.get(),
        source_seq_to: value.source_seq_to.get(),
        started_conversation_seq: value.started_conversation_seq.get(),
        started_at: Some(timestamp(value.started_at)),
        finished_at: value.finished_at.map(timestamp),
        error_code: value.error_code.map(|code| code.as_str().to_owned()),
    }
}

fn decode_projected_context_compaction(
    value: wire::ProjectedContextCompaction,
) -> Result<ProjectedContextCompaction, ProjectionSnapshotCodecError> {
    Ok(ProjectedContextCompaction {
        invocation_id: parse_id(value.invocation_id.map(|item| item.value))?,
        run_id: parse_id(value.run_id.map(|item| item.value))?,
        loop_iteration: value.loop_iteration,
        status: decode_projected_context_compaction_status(value.status)?,
        model_route_ref: ModelRouteRef::from_key(
            VersionedKey::new(
                value
                    .model_route_ref
                    .ok_or(ProjectionSnapshotCodecError::InvalidContract)?
                    .route_key,
            )
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        ),
        input_tokens_before: value.input_tokens_before,
        input_tokens_after: value.input_tokens_after,
        summarized_record_count: value.summarized_record_count,
        retained_record_count: value.retained_record_count,
        source_seq_from: musteragent_identity::ConversationSeq::new(value.source_seq_from)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        source_seq_to: musteragent_identity::ConversationSeq::new(value.source_seq_to)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        started_conversation_seq: musteragent_identity::ConversationSeq::new(
            value.started_conversation_seq,
        )
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        started_at: decode_timestamp(value.started_at)?,
        finished_at: value.finished_at.map(decode_timestamp_value).transpose()?,
        error_code: value
            .error_code
            .map(|code| {
                ErrorCode::from_safe_code(&code)
                    .ok_or(ProjectionSnapshotCodecError::InvalidContract)
            })
            .transpose()?,
    })
}

fn decode_projected_tool(
    value: wire::ProjectedToolItem,
) -> Result<ProjectedToolItem, ProjectionSnapshotCodecError> {
    Ok(ProjectedToolItem {
        invocation_id: parse_id(value.invocation_id.map(|item| item.value))?,
        message_id: value
            .message_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        run_id: parse_id(value.run_id.map(|item| item.value))?,
        tool_call_id: ToolCallId::new(value.tool_call_id)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        tool_ref: value.tool_ref,
        status: decode_projected_tool_status(value.status)?,
        request_ref: decode_content_ref(
            value
                .request_ref
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        result_ref: value.result_ref.map(decode_content_ref).transpose()?,
        requested_conversation_seq: musteragent_identity::ConversationSeq::new(
            value.requested_conversation_seq,
        )
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        requested_at: decode_timestamp(value.requested_at)?,
        started_at: value.started_at.map(decode_timestamp_value).transpose()?,
        finished_at: value.finished_at.map(decode_timestamp_value).transpose()?,
        output_artifact_ids: value
            .output_artifact_ids
            .into_iter()
            .map(|item| parse_id(Some(item.value)))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn projected_interaction(value: &ProjectedInteraction) -> wire::ProjectedInteraction {
    wire::ProjectedInteraction {
        interaction_id: Some(wire::InteractionId {
            value: value.interaction_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        kind: interaction_kind(value.kind),
        prompt_ref: Some(content_ref(value.prompt_ref.clone())),
        response_schema: Some(schema_ref(value.response_schema.clone())),
        requested_at: Some(timestamp(value.requested_at)),
        expires_at: value.expires_at.map(timestamp),
    }
}

fn decode_projected_interaction(
    value: wire::ProjectedInteraction,
) -> Result<ProjectedInteraction, ProjectionSnapshotCodecError> {
    Ok(ProjectedInteraction {
        interaction_id: parse_id(value.interaction_id.map(|item| item.value))?,
        run_id: parse_id(value.run_id.map(|item| item.value))?,
        kind: decode_interaction_kind(value.kind)?,
        prompt_ref: decode_content_ref(
            value
                .prompt_ref
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        response_schema: decode_schema_ref(
            value
                .response_schema
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        requested_at: decode_timestamp(value.requested_at)?,
        expires_at: value.expires_at.map(decode_timestamp_value).transpose()?,
    })
}

fn projected_run(value: &ProjectedRun) -> wire::ProjectedRun {
    let (
        wait_reason,
        wait_interaction_id,
        wait_invocation_id,
        wait_child_activity_id,
        wait_delegation_group_id,
    ) = run_wait(value.wait_reason);
    wire::ProjectedRun {
        recovery_error_code: recovery_error_code(value.wait_reason),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        activity_id: Some(wire::ActivityId {
            value: value.activity_id.to_string(),
        }),
        status: run_status(value.status),
        wait_reason,
        wait_interaction_id,
        admitted_at: Some(timestamp(value.admitted_at)),
        started_at: value.started_at.map(timestamp),
        wait_invocation_id,
        executor_participant_id: value
            .executor_participant_id
            .map(|item| wire::ParticipantId {
                value: item.to_string(),
            }),
        parent_run_id: value.parent_run_id.map(|item| wire::RunId {
            value: item.to_string(),
        }),
        delegation_id: value.delegation_id.map(|item| wire::DelegationId {
            value: item.to_string(),
        }),
        finished_at: value.finished_at.map(timestamp),
        wait_child_activity_id,
        wait_delegation_group_id,
        attempt_number: value.attempt_number.map_or(0, AttemptNumber::get),
        next_attempt_number: value.next_attempt_number.map_or(0, AttemptNumber::get),
        retry_not_before: value.retry_not_before.map(timestamp),
        retry_source_message_id: value.retry_source_message_id.map(|item| wire::MessageId {
            value: item.to_string(),
        }),
        retry_source_attempt_number: value
            .retry_source_attempt_number
            .map_or(0, AttemptNumber::get),
        terminal_failure: value
            .terminal_failure
            .as_ref()
            .map(encode_projected_run_failure),
        retry_failure: value
            .retry_failure
            .as_ref()
            .map(encode_projected_run_failure),
    }
}

fn encode_projected_run_failure(
    failure: &musteragent_projection::ProjectedRunFailure,
) -> wire::ProjectedRunFailure {
    wire::ProjectedRunFailure {
        error: Some(crate::error_codec::encode_error_info(&failure.error)),
        ledger_record_id: Some(wire::LedgerRecordId {
            value: failure.ledger_record_id.to_string(),
        }),
        correlation_id: Some(wire::CorrelationId {
            value: failure.correlation_id.to_string(),
        }),
        conversation_sequence: failure.conversation_sequence.get(),
    }
}

fn decode_projected_run(
    value: wire::ProjectedRun,
) -> Result<ProjectedRun, ProjectionSnapshotCodecError> {
    Ok(ProjectedRun {
        run_id: parse_id(value.run_id.map(|item| item.value))?,
        activity_id: parse_id(value.activity_id.map(|item| item.value))?,
        executor_participant_id: value
            .executor_participant_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        parent_run_id: value
            .parent_run_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        delegation_id: value
            .delegation_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        retry_source_message_id: value
            .retry_source_message_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        retry_source_attempt_number: decode_optional_attempt_number(
            value.retry_source_attempt_number,
        )?,
        retry_failure: value
            .retry_failure
            .map(decode_projected_run_failure)
            .transpose()?,
        terminal_failure: value
            .terminal_failure
            .map(decode_projected_run_failure)
            .transpose()?,
        status: decode_run_status(value.status)?,
        wait_reason: decode_run_wait(
            value.wait_reason,
            value.wait_interaction_id.map(|item| item.value),
            value.wait_invocation_id.map(|item| item.value),
            value.wait_child_activity_id.map(|item| item.value),
            value.wait_delegation_group_id.map(|item| item.value),
            value.recovery_error_code,
        )?,
        attempt_number: decode_optional_attempt_number(value.attempt_number)?,
        next_attempt_number: decode_optional_attempt_number(value.next_attempt_number)?,
        retry_not_before: value
            .retry_not_before
            .map(decode_timestamp_value)
            .transpose()?,
        admitted_at: decode_timestamp(value.admitted_at)?,
        started_at: value.started_at.map(decode_timestamp_value).transpose()?,
        finished_at: value.finished_at.map(decode_timestamp_value).transpose()?,
    })
}

fn decode_projected_run_failure(
    value: wire::ProjectedRunFailure,
) -> Result<ProjectedRunFailure, ProjectionSnapshotCodecError> {
    Ok(ProjectedRunFailure {
        error: crate::error_codec::decode_error_info(
            value
                .error
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        ledger_record_id: parse_id(value.ledger_record_id.map(|item| item.value))?,
        correlation_id: parse_id(value.correlation_id.map(|item| item.value))?,
        conversation_sequence: musteragent_identity::ConversationSeq::new(
            value.conversation_sequence,
        )
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
    })
}

fn decode_optional_attempt_number(
    value: u32,
) -> Result<Option<AttemptNumber>, ProjectionSnapshotCodecError> {
    if value == 0 {
        return Ok(None);
    }
    AttemptNumber::new(value)
        .map(Some)
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)
}

fn projected_artifact(value: &ProjectedArtifact) -> wire::ProjectedArtifact {
    wire::ProjectedArtifact {
        artifact_id: Some(wire::ArtifactId {
            value: value.artifact_id.to_string(),
        }),
        activity_id: value.activity_id.map(|item| wire::ActivityId {
            value: item.to_string(),
        }),
        created_by_run_id: value.created_by_run_id.map(|item| wire::RunId {
            value: item.to_string(),
        }),
        updated_by_run_id: value.updated_by_run_id.map(|item| wire::RunId {
            value: item.to_string(),
        }),
        kind: artifact_kind(value.kind),
        lifecycle: artifact_lifecycle(value.lifecycle),
        current_version: value.current_version.get(),
        title: value.title.clone(),
        current_content_ref: Some(content_ref(value.current_content_ref.clone())),
        updated_at: Some(timestamp(value.updated_at)),
        current_dependency_refs: value
            .current_dependency_refs
            .iter()
            .cloned()
            .map(content_ref)
            .collect(),
    }
}

fn decode_projected_artifact(
    value: wire::ProjectedArtifact,
) -> Result<ProjectedArtifact, ProjectionSnapshotCodecError> {
    let created_by_run_id = value
        .created_by_run_id
        .map(|item| parse_id(Some(item.value)))
        .transpose()?;
    let updated_by_run_id = value
        .updated_by_run_id
        .map(|item| parse_id(Some(item.value)))
        .transpose()?;
    Ok(ProjectedArtifact {
        artifact_id: parse_id(value.artifact_id.map(|item| item.value))?,
        activity_id: value
            .activity_id
            .map(|item| parse_id(Some(item.value)))
            .transpose()?,
        created_by_run_id,
        updated_by_run_id,
        presentation_run_id: None,
        kind: decode_artifact_kind(value.kind)?,
        lifecycle: decode_artifact_lifecycle(value.lifecycle)?,
        current_version: VersionNumber::new(value.current_version)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        title: value.title,
        current_content_ref: decode_content_ref(
            value
                .current_content_ref
                .ok_or(ProjectionSnapshotCodecError::InvalidContract)?,
        )?,
        current_dependency_refs: value
            .current_dependency_refs
            .into_iter()
            .map(decode_content_ref)
            .collect::<Result<Vec<_>, _>>()?,
        updated_at: decode_timestamp(value.updated_at)?,
    })
}

fn content_ref(value: ContentRef) -> wire::ContentRef {
    wire::ContentRef {
        content_id: Some(wire::ContentId {
            value: value.content_id.to_string(),
        }),
        media_type: value.media_type.as_str().to_owned(),
        byte_length: value.byte_length,
        sha256: Some(sha256(value.sha256)),
    }
}

fn decode_content_ref(value: wire::ContentRef) -> Result<ContentRef, ProjectionSnapshotCodecError> {
    Ok(ContentRef {
        content_id: parse_id(value.content_id.map(|item| item.value))?,
        media_type: MediaType::new(value.media_type)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        byte_length: value.byte_length,
        sha256: decode_sha256(value.sha256)?,
    })
}

fn schema_ref(value: SchemaRef) -> wire::SchemaRef {
    wire::SchemaRef {
        schema_key: value.schema_key.as_str().to_owned(),
        schema_hash: Some(sha256(value.schema_hash)),
    }
}

fn decode_schema_ref(value: wire::SchemaRef) -> Result<SchemaRef, ProjectionSnapshotCodecError> {
    Ok(SchemaRef {
        schema_key: VersionedKey::new(value.schema_key)
            .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
        schema_hash: decode_sha256(value.schema_hash)?,
    })
}

fn sha256(value: Sha256) -> wire::Sha256 {
    wire::Sha256 {
        value: value.into_bytes().to_vec(),
    }
}

fn decode_sha256(value: Option<wire::Sha256>) -> Result<Sha256, ProjectionSnapshotCodecError> {
    Sha256::from_bytes(
        value
            .ok_or(ProjectionSnapshotCodecError::InvalidContract)?
            .value,
    )
    .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)
}

fn timestamp(value: UtcTimestamp) -> Timestamp {
    let millis = value.unix_millis();
    Timestamp {
        seconds: millis.div_euclid(MILLIS_PER_SECOND),
        nanos: i32::try_from(millis.rem_euclid(MILLIS_PER_SECOND))
            .expect("millisecond remainder fits i32")
            * NANOS_PER_MILLISECOND,
    }
}

fn decode_timestamp(
    value: Option<Timestamp>,
) -> Result<UtcTimestamp, ProjectionSnapshotCodecError> {
    decode_timestamp_value(value.ok_or(ProjectionSnapshotCodecError::InvalidContract)?)
}

fn decode_timestamp_value(value: Timestamp) -> Result<UtcTimestamp, ProjectionSnapshotCodecError> {
    if !(0..1_000_000_000).contains(&value.nanos) {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    let millis = value
        .seconds
        .checked_mul(MILLIS_PER_SECOND)
        .and_then(|seconds| seconds.checked_add(i64::from(value.nanos / NANOS_PER_MILLISECOND)))
        .ok_or(ProjectionSnapshotCodecError::InvalidContract)?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}

fn parse_id<T>(value: Option<String>) -> Result<T, ProjectionSnapshotCodecError>
where
    T: FromStr<Err = IdentityError>,
{
    value
        .ok_or(ProjectionSnapshotCodecError::InvalidContract)?
        .parse()
        .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)
}

fn message_role(value: MessageRole) -> i32 {
    (match value {
        MessageRole::User => wire::MessageRole::User,
        MessageRole::Assistant => wire::MessageRole::Assistant,
        MessageRole::SystemNotice => wire::MessageRole::SystemNotice,
    }) as i32
}

fn decode_message_role(value: i32) -> Result<MessageRole, ProjectionSnapshotCodecError> {
    match wire::MessageRole::try_from(value) {
        Ok(wire::MessageRole::User) => Ok(MessageRole::User),
        Ok(wire::MessageRole::Assistant) => Ok(MessageRole::Assistant),
        Ok(wire::MessageRole::SystemNotice) => Ok(MessageRole::SystemNotice),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn projected_message_status(value: ProjectedMessageStatus) -> i32 {
    (match value {
        ProjectedMessageStatus::Streaming => wire::ProjectedMessageStatus::Streaming,
        ProjectedMessageStatus::Committed => wire::ProjectedMessageStatus::Committed,
        ProjectedMessageStatus::Partial => wire::ProjectedMessageStatus::Partial,
    }) as i32
}

fn decode_projected_message_status(
    value: i32,
) -> Result<ProjectedMessageStatus, ProjectionSnapshotCodecError> {
    match wire::ProjectedMessageStatus::try_from(value) {
        Ok(wire::ProjectedMessageStatus::Streaming) => Ok(ProjectedMessageStatus::Streaming),
        Ok(wire::ProjectedMessageStatus::Committed) => Ok(ProjectedMessageStatus::Committed),
        Ok(wire::ProjectedMessageStatus::Partial) => Ok(ProjectedMessageStatus::Partial),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn projected_tool_status(value: ProjectedToolStatus) -> i32 {
    (match value {
        ProjectedToolStatus::Requested => wire::ProjectedToolStatus::Requested,
        ProjectedToolStatus::Running => wire::ProjectedToolStatus::Running,
        ProjectedToolStatus::Waiting => wire::ProjectedToolStatus::Waiting,
        ProjectedToolStatus::Completed => wire::ProjectedToolStatus::Completed,
        ProjectedToolStatus::Failed => wire::ProjectedToolStatus::Failed,
        ProjectedToolStatus::Cancelled => wire::ProjectedToolStatus::Cancelled,
        ProjectedToolStatus::ResultUnknown => wire::ProjectedToolStatus::ResultUnknown,
    }) as i32
}

fn decode_projected_tool_status(
    value: i32,
) -> Result<ProjectedToolStatus, ProjectionSnapshotCodecError> {
    match wire::ProjectedToolStatus::try_from(value) {
        Ok(wire::ProjectedToolStatus::Requested) => Ok(ProjectedToolStatus::Requested),
        Ok(wire::ProjectedToolStatus::Running) => Ok(ProjectedToolStatus::Running),
        Ok(wire::ProjectedToolStatus::Waiting) => Ok(ProjectedToolStatus::Waiting),
        Ok(wire::ProjectedToolStatus::Completed) => Ok(ProjectedToolStatus::Completed),
        Ok(wire::ProjectedToolStatus::Failed) => Ok(ProjectedToolStatus::Failed),
        Ok(wire::ProjectedToolStatus::Cancelled) => Ok(ProjectedToolStatus::Cancelled),
        Ok(wire::ProjectedToolStatus::ResultUnknown) => Ok(ProjectedToolStatus::ResultUnknown),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn projected_context_compaction_status(value: ProjectedContextCompactionStatus) -> i32 {
    (match value {
        ProjectedContextCompactionStatus::RetryScheduled => {
            wire::ProjectedContextCompactionStatus::RetryScheduled
        }
        ProjectedContextCompactionStatus::Running => {
            wire::ProjectedContextCompactionStatus::Running
        }
        ProjectedContextCompactionStatus::Succeeded => {
            wire::ProjectedContextCompactionStatus::Succeeded
        }
        ProjectedContextCompactionStatus::FailedContinued => {
            wire::ProjectedContextCompactionStatus::FailedContinued
        }
        ProjectedContextCompactionStatus::FailedStopped => {
            wire::ProjectedContextCompactionStatus::FailedStopped
        }
        ProjectedContextCompactionStatus::Indeterminate => {
            wire::ProjectedContextCompactionStatus::Indeterminate
        }
        ProjectedContextCompactionStatus::Interrupted => {
            wire::ProjectedContextCompactionStatus::Interrupted
        }
    }) as i32
}

fn decode_projected_context_compaction_status(
    value: i32,
) -> Result<ProjectedContextCompactionStatus, ProjectionSnapshotCodecError> {
    match wire::ProjectedContextCompactionStatus::try_from(value) {
        Ok(wire::ProjectedContextCompactionStatus::RetryScheduled) => {
            Ok(ProjectedContextCompactionStatus::RetryScheduled)
        }
        Ok(wire::ProjectedContextCompactionStatus::Running) => {
            Ok(ProjectedContextCompactionStatus::Running)
        }
        Ok(wire::ProjectedContextCompactionStatus::Succeeded) => {
            Ok(ProjectedContextCompactionStatus::Succeeded)
        }
        Ok(wire::ProjectedContextCompactionStatus::FailedContinued) => {
            Ok(ProjectedContextCompactionStatus::FailedContinued)
        }
        Ok(wire::ProjectedContextCompactionStatus::FailedStopped) => {
            Ok(ProjectedContextCompactionStatus::FailedStopped)
        }
        Ok(wire::ProjectedContextCompactionStatus::Indeterminate) => {
            Ok(ProjectedContextCompactionStatus::Indeterminate)
        }
        Ok(wire::ProjectedContextCompactionStatus::Interrupted) => {
            Ok(ProjectedContextCompactionStatus::Interrupted)
        }
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn interaction_kind(value: InteractionKind) -> i32 {
    (match value {
        InteractionKind::Approval => wire::InteractionKind::Approval,
        InteractionKind::Clarification => wire::InteractionKind::Clarification,
        InteractionKind::Confirmation => wire::InteractionKind::Confirmation,
        InteractionKind::Selection => wire::InteractionKind::Selection,
        InteractionKind::Credential => wire::InteractionKind::Credential,
        InteractionKind::OAuth => wire::InteractionKind::Oauth,
    }) as i32
}

fn decode_interaction_kind(value: i32) -> Result<InteractionKind, ProjectionSnapshotCodecError> {
    match wire::InteractionKind::try_from(value) {
        Ok(wire::InteractionKind::Approval) => Ok(InteractionKind::Approval),
        Ok(wire::InteractionKind::Clarification) => Ok(InteractionKind::Clarification),
        Ok(wire::InteractionKind::Confirmation) => Ok(InteractionKind::Confirmation),
        Ok(wire::InteractionKind::Selection) => Ok(InteractionKind::Selection),
        Ok(wire::InteractionKind::Credential) => Ok(InteractionKind::Credential),
        Ok(wire::InteractionKind::Oauth) => Ok(InteractionKind::OAuth),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn activity_kind(value: ActivityKind) -> i32 {
    (match value {
        ActivityKind::Chat => wire::ActivityKind::Chat,
        ActivityKind::Coordination => wire::ActivityKind::Coordination,
        ActivityKind::Automation => wire::ActivityKind::Automation,
        ActivityKind::Delegation => wire::ActivityKind::Delegation,
        ActivityKind::Maintenance => wire::ActivityKind::Maintenance,
        ActivityKind::Evaluation => wire::ActivityKind::Evaluation,
    }) as i32
}

fn decode_activity_kind(value: i32) -> Result<ActivityKind, ProjectionSnapshotCodecError> {
    match wire::ActivityKind::try_from(value) {
        Ok(wire::ActivityKind::Chat) => Ok(ActivityKind::Chat),
        Ok(wire::ActivityKind::Coordination) => Ok(ActivityKind::Coordination),
        Ok(wire::ActivityKind::Automation) => Ok(ActivityKind::Automation),
        Ok(wire::ActivityKind::Delegation) => Ok(ActivityKind::Delegation),
        Ok(wire::ActivityKind::Maintenance) => Ok(ActivityKind::Maintenance),
        Ok(wire::ActivityKind::Evaluation) => Ok(ActivityKind::Evaluation),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn activity_status(value: ActivityStatus) -> i32 {
    (match value {
        ActivityStatus::Planned => wire::ProjectedActivityStatus::Planned,
        ActivityStatus::Active => wire::ProjectedActivityStatus::Active,
        ActivityStatus::Waiting => wire::ProjectedActivityStatus::Waiting,
        ActivityStatus::Completed => wire::ProjectedActivityStatus::Completed,
        ActivityStatus::Failed => wire::ProjectedActivityStatus::Failed,
        ActivityStatus::Cancelled => wire::ProjectedActivityStatus::Cancelled,
    }) as i32
}

fn decode_activity_status(value: i32) -> Result<ActivityStatus, ProjectionSnapshotCodecError> {
    match wire::ProjectedActivityStatus::try_from(value) {
        Ok(wire::ProjectedActivityStatus::Planned) => Ok(ActivityStatus::Planned),
        Ok(wire::ProjectedActivityStatus::Active) => Ok(ActivityStatus::Active),
        Ok(wire::ProjectedActivityStatus::Waiting) => Ok(ActivityStatus::Waiting),
        Ok(wire::ProjectedActivityStatus::Completed) => Ok(ActivityStatus::Completed),
        Ok(wire::ProjectedActivityStatus::Failed) => Ok(ActivityStatus::Failed),
        Ok(wire::ProjectedActivityStatus::Cancelled) => Ok(ActivityStatus::Cancelled),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn run_status(value: RunStatus) -> i32 {
    (match value {
        RunStatus::Queued => wire::RunStatus::Queued,
        RunStatus::Running => wire::RunStatus::Running,
        RunStatus::Waiting => wire::RunStatus::Waiting,
        RunStatus::Cancelling => wire::RunStatus::Cancelling,
        RunStatus::Completed => wire::RunStatus::Completed,
        RunStatus::Failed => wire::RunStatus::Failed,
        RunStatus::Cancelled => wire::RunStatus::Cancelled,
        RunStatus::Interrupted => wire::RunStatus::Interrupted,
    }) as i32
}

fn decode_run_status(value: i32) -> Result<RunStatus, ProjectionSnapshotCodecError> {
    match wire::RunStatus::try_from(value) {
        Ok(wire::RunStatus::Queued) => Ok(RunStatus::Queued),
        Ok(wire::RunStatus::Running) => Ok(RunStatus::Running),
        Ok(wire::RunStatus::Waiting) => Ok(RunStatus::Waiting),
        Ok(wire::RunStatus::Cancelling) => Ok(RunStatus::Cancelling),
        Ok(wire::RunStatus::Completed) => Ok(RunStatus::Completed),
        Ok(wire::RunStatus::Failed) => Ok(RunStatus::Failed),
        Ok(wire::RunStatus::Cancelled) => Ok(RunStatus::Cancelled),
        Ok(wire::RunStatus::Interrupted) => Ok(RunStatus::Interrupted),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn recovery_error_code(reason: Option<RunWaitReason>) -> i32 {
    match reason {
        Some(RunWaitReason::RecoveryRequired { code }) => {
            crate::error_codec::error_code(code) as i32
        }
        _ => wire::ErrorCode::Unspecified as i32,
    }
}

fn run_wait(
    value: Option<RunWaitReason>,
) -> (
    i32,
    Option<wire::InteractionId>,
    Option<wire::InvocationId>,
    Option<wire::ActivityId>,
    Option<wire::DelegationGroupId>,
) {
    match value {
        None => (
            wire::RunWaitReason::Unspecified as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Approval { interaction_id }) => (
            wire::RunWaitReason::Approval as i32,
            Some(wire::InteractionId {
                value: interaction_id.to_string(),
            }),
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Input { interaction_id }) => (
            wire::RunWaitReason::Input as i32,
            Some(wire::InteractionId {
                value: interaction_id.to_string(),
            }),
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Device { invocation_id }) => (
            wire::RunWaitReason::Device as i32,
            None,
            Some(wire::InvocationId {
                value: invocation_id.to_string(),
            }),
            None,
            None,
        ),
        Some(RunWaitReason::RetryBackoff) => (
            wire::RunWaitReason::RetryBackoff as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::RecoveryRequired { .. }) => (
            wire::RunWaitReason::RecoveryRequired as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Budget) => (wire::RunWaitReason::Budget as i32, None, None, None, None),
        Some(RunWaitReason::ExecutionRecovery) => (
            wire::RunWaitReason::ExecutionRecovery as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::Delegation { child_activity_id }) => (
            wire::RunWaitReason::Delegation as i32,
            None,
            None,
            Some(wire::ActivityId {
                value: child_activity_id.to_string(),
            }),
            None,
        ),
        Some(RunWaitReason::DelegationGroup { group_id }) => (
            wire::RunWaitReason::DelegationGroup as i32,
            None,
            None,
            None,
            Some(wire::DelegationGroupId {
                value: group_id.to_string(),
            }),
        ),
        Some(RunWaitReason::DelegationCompletions) => (
            wire::RunWaitReason::DelegationCompletions as i32,
            None,
            None,
            None,
            None,
        ),
        Some(RunWaitReason::ExternalOperation { invocation_id }) => (
            wire::RunWaitReason::ExternalOperation as i32,
            None,
            Some(wire::InvocationId {
                value: invocation_id.to_string(),
            }),
            None,
            None,
        ),
    }
}

fn decode_run_wait(
    value: i32,
    interaction_id: Option<String>,
    invocation_id: Option<String>,
    child_activity_id: Option<String>,
    delegation_group_id: Option<String>,
    recovery_error_code: i32,
) -> Result<Option<RunWaitReason>, ProjectionSnapshotCodecError> {
    if value != wire::RunWaitReason::RecoveryRequired as i32 && recovery_error_code != 0 {
        return Err(ProjectionSnapshotCodecError::InvalidContract);
    }
    match wire::RunWaitReason::try_from(value) {
        Ok(wire::RunWaitReason::Unspecified) => Ok(None),
        Ok(wire::RunWaitReason::Approval) => Ok(Some(RunWaitReason::Approval {
            interaction_id: parse_id(interaction_id)?,
        })),
        Ok(wire::RunWaitReason::Input) => Ok(Some(RunWaitReason::Input {
            interaction_id: parse_id(interaction_id)?,
        })),
        Ok(wire::RunWaitReason::Device) => Ok(Some(RunWaitReason::Device {
            invocation_id: parse_id(invocation_id)?,
        })),
        Ok(wire::RunWaitReason::RetryBackoff) => Ok(Some(RunWaitReason::RetryBackoff)),
        Ok(wire::RunWaitReason::RecoveryRequired)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(Some(RunWaitReason::RecoveryRequired {
                code: crate::error_codec::decode_error_code(recovery_error_code)
                    .map_err(|_| ProjectionSnapshotCodecError::InvalidContract)?,
            }))
        }
        Ok(wire::RunWaitReason::Budget) => Ok(Some(RunWaitReason::Budget)),
        Ok(wire::RunWaitReason::ExecutionRecovery)
            if interaction_id.is_none()
                && invocation_id.is_none()
                && child_activity_id.is_none()
                && delegation_group_id.is_none() =>
        {
            Ok(Some(RunWaitReason::ExecutionRecovery))
        }
        Ok(wire::RunWaitReason::Delegation) => Ok(Some(RunWaitReason::Delegation {
            child_activity_id: parse_id(child_activity_id)?,
        })),
        Ok(wire::RunWaitReason::DelegationGroup) => Ok(Some(RunWaitReason::DelegationGroup {
            group_id: parse_id(delegation_group_id)?,
        })),
        Ok(wire::RunWaitReason::DelegationCompletions) => {
            Ok(Some(RunWaitReason::DelegationCompletions))
        }
        Ok(wire::RunWaitReason::ExternalOperation) => Ok(Some(RunWaitReason::ExternalOperation {
            invocation_id: parse_id(invocation_id)?,
        })),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn artifact_kind(value: ArtifactKind) -> i32 {
    (match value {
        ArtifactKind::Presentation => wire::ArtifactKind::Presentation,
        ArtifactKind::Note => wire::ArtifactKind::Note,
        ArtifactKind::LegacyDiagram => wire::ArtifactKind::LegacyDiagram,
        ArtifactKind::Image => wire::ArtifactKind::Image,
        ArtifactKind::Video => wire::ArtifactKind::Video,
        ArtifactKind::File => wire::ArtifactKind::File,
        ArtifactKind::Application => wire::ArtifactKind::Application,
    }) as i32
}

fn decode_artifact_kind(value: i32) -> Result<ArtifactKind, ProjectionSnapshotCodecError> {
    match wire::ArtifactKind::try_from(value) {
        Ok(wire::ArtifactKind::Presentation) => Ok(ArtifactKind::Presentation),
        Ok(wire::ArtifactKind::Note) => Ok(ArtifactKind::Note),
        Ok(wire::ArtifactKind::LegacyDiagram) => Ok(ArtifactKind::LegacyDiagram),
        Ok(wire::ArtifactKind::Image) => Ok(ArtifactKind::Image),
        Ok(wire::ArtifactKind::Video) => Ok(ArtifactKind::Video),
        Ok(wire::ArtifactKind::File) => Ok(ArtifactKind::File),
        Ok(wire::ArtifactKind::Application) => Ok(ArtifactKind::Application),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn artifact_lifecycle(value: ArtifactLifecycle) -> i32 {
    (match value {
        ArtifactLifecycle::Draft => wire::ArtifactLifecycle::Draft,
        ArtifactLifecycle::Published => wire::ArtifactLifecycle::Published,
        ArtifactLifecycle::Archived => wire::ArtifactLifecycle::Archived,
        ArtifactLifecycle::Failed => wire::ArtifactLifecycle::Failed,
    }) as i32
}

fn decode_artifact_lifecycle(
    value: i32,
) -> Result<ArtifactLifecycle, ProjectionSnapshotCodecError> {
    match wire::ArtifactLifecycle::try_from(value) {
        Ok(wire::ArtifactLifecycle::Draft) => Ok(ArtifactLifecycle::Draft),
        Ok(wire::ArtifactLifecycle::Published) => Ok(ArtifactLifecycle::Published),
        Ok(wire::ArtifactLifecycle::Archived) => Ok(ArtifactLifecycle::Archived),
        Ok(wire::ArtifactLifecycle::Failed) => Ok(ArtifactLifecycle::Failed),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

fn partial_reason(value: PartialMessageReason) -> i32 {
    (match value {
        PartialMessageReason::Cancelled => wire::PartialMessageReason::Cancelled,
        PartialMessageReason::Interrupted => wire::PartialMessageReason::Interrupted,
        PartialMessageReason::ProviderFailure => wire::PartialMessageReason::ProviderFailure,
        PartialMessageReason::ToolFailure => wire::PartialMessageReason::ToolFailure,
    }) as i32
}

fn decode_optional_partial_reason(
    value: i32,
) -> Result<Option<PartialMessageReason>, ProjectionSnapshotCodecError> {
    match wire::PartialMessageReason::try_from(value) {
        Ok(wire::PartialMessageReason::Unspecified) => Ok(None),
        Ok(wire::PartialMessageReason::Cancelled) => Ok(Some(PartialMessageReason::Cancelled)),
        Ok(wire::PartialMessageReason::Interrupted) => Ok(Some(PartialMessageReason::Interrupted)),
        Ok(wire::PartialMessageReason::ProviderFailure) => {
            Ok(Some(PartialMessageReason::ProviderFailure))
        }
        Ok(wire::PartialMessageReason::ToolFailure) => Ok(Some(PartialMessageReason::ToolFailure)),
        _ => Err(ProjectionSnapshotCodecError::InvalidContract),
    }
}

#[cfg(test)]
mod tests;
