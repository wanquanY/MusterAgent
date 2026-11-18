//! Deterministic product projection reducer shared by every persistence adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use musteragent_activity::{ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_approval::InteractionEvent;
use musteragent_artifact::{
    ArtifactEvent, ArtifactKind, ArtifactLifecycle, validate_dependency_refs,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{ContextCompactionOutcome, ContextEvent};
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_identity::{
    ArtifactId, ConversationId, ConversationSeq, HomeEpoch, InteractionId, InvocationId,
    LedgerOffset, MessageId, OutputId, ProjectionSnapshotId, RunId, TenantId, UtcTimestamp,
};
use musteragent_ledger::{CanonicalLedgerRecord, DomainEvent, LedgerPayload};
use musteragent_run::{RunEvent, RunStatus, RunWaitReason};
use musteragent_runtime_contract::{ApplicationFuture, FrameContent, OutputChannel, OutputFrame};
use musteragent_tool::ToolEvent;

use crate::{
    ConversationProjectionSnapshot, PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedActivity,
    ProjectedArtifact, ProjectedContextCompaction, ProjectedContextCompactionStatus,
    ProjectedCoordinationOrigin, ProjectedDelegationOrigin, ProjectedInteraction, ProjectedMessage,
    ProjectedMessageStatus, ProjectedOutputCheckpoint, ProjectedReasoningSummary, ProjectedRun,
    ProjectedRunFailure, ProjectedStreamOutput, ProjectedToolItem, ProjectedToolStatus,
    ProjectionRetraction, SubscriptionError, canonical_content_manifest,
};

/// Recovery resumes an Attempt, not a Run that has already entered its
/// cancellation or terminal lane. Product projection is rebuilt from an
/// append-only stream, so late/replayed non-terminal facts must be monotonic.
const fn accepts_nonterminal_progress(status: RunStatus) -> bool {
    !status.is_terminal() && !matches!(status, RunStatus::Cancelling)
}

/// Minimal Content boundary needed while rebuilding a product projection.
/// Storage adapters own authorization, integrity verification, and durability.
pub trait ProjectionContentStore: Send + Sync {
    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, SubscriptionError>>;

    fn put(
        &self,
        tenant_id: TenantId,
        media_type: MediaType,
        bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, SubscriptionError>>;
}

pub struct ProductProjectionReducer {
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    through: LedgerOffset,
    messages: BTreeMap<MessageId, MutableMessage>,
    tools: BTreeMap<InvocationId, ProjectedToolItem>,
    context_compactions: BTreeMap<InvocationId, ProjectedContextCompaction>,
    interactions: BTreeMap<InteractionId, ProjectedInteraction>,
    activities: BTreeMap<musteragent_identity::ActivityId, ProjectedActivity>,
    pending_delegation_origins:
        BTreeMap<musteragent_identity::ActivityId, ProjectedDelegationOrigin>,
    pending_coordination_origins:
        BTreeMap<musteragent_identity::ActivityId, ProjectedCoordinationOrigin>,
    runs: BTreeMap<RunId, ProjectedRun>,
    artifacts: BTreeMap<ArtifactId, ProjectedArtifact>,
    pending_artifacts: BTreeMap<ArtifactId, PendingArtifact>,
    outputs: BTreeMap<OutputId, MutableOutput>,
    output_messages: BTreeMap<OutputId, MessageId>,
}

impl ProductProjectionReducer {
    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    #[must_use]
    pub const fn home_epoch(&self) -> HomeEpoch {
        self.home_epoch
    }

    #[must_use]
    pub const fn through(&self) -> LedgerOffset {
        self.through
    }

    pub fn advance_through(&mut self, through: LedgerOffset) -> Result<(), SubscriptionError> {
        if through < self.through {
            return Err(SubscriptionError::CorruptRecord);
        }
        self.through = through;
        Ok(())
    }

    pub fn new(
        tenant_id: TenantId,
        conversation_id: ConversationId,
        home_epoch: HomeEpoch,
        snapshot: Option<ConversationProjectionSnapshot>,
        output_messages: BTreeMap<OutputId, MessageId>,
    ) -> Result<Self, SubscriptionError> {
        let mut reducer = Self {
            tenant_id,
            conversation_id,
            home_epoch,
            through: LedgerOffset::new(0),
            messages: BTreeMap::new(),
            tools: BTreeMap::new(),
            context_compactions: BTreeMap::new(),
            interactions: BTreeMap::new(),
            activities: BTreeMap::new(),
            pending_delegation_origins: BTreeMap::new(),
            pending_coordination_origins: BTreeMap::new(),
            runs: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            pending_artifacts: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_messages,
        };
        if let Some(snapshot) = snapshot {
            reducer.through = snapshot.through_conversation_seq;
            for message in snapshot.messages {
                if let Some(checkpoint) = message.assistant_output.clone() {
                    let run_id = message
                        .source_run_id
                        .ok_or(SubscriptionError::CorruptRecord)?;
                    reducer.outputs.insert(
                        checkpoint.output_id,
                        MutableOutput::from_ref(
                            checkpoint,
                            Some(message.message_id),
                            run_id,
                            OutputChannel::AssistantText,
                            message.content_ref.clone(),
                            ConversationSeq::new(1)
                                .map_err(|_| SubscriptionError::CorruptRecord)?,
                            message.committed_at,
                        )?,
                    );
                }
                for reasoning in &message.reasoning_summaries {
                    let run_id = message
                        .source_run_id
                        .ok_or(SubscriptionError::CorruptRecord)?;
                    reducer.outputs.insert(
                        reasoning.checkpoint.output_id,
                        MutableOutput::from_ref(
                            reasoning.checkpoint.clone(),
                            Some(message.message_id),
                            run_id,
                            OutputChannel::ReasoningSummary,
                            reasoning.content_ref.clone(),
                            ConversationSeq::new(1)
                                .map_err(|_| SubscriptionError::CorruptRecord)?,
                            message.committed_at,
                        )?,
                    );
                }
                reducer
                    .messages
                    .insert(message.message_id, MutableMessage::from_projected(message));
            }
            reducer.tools = snapshot
                .tool_timeline
                .into_iter()
                .map(|item| (item.invocation_id, item))
                .collect();
            reducer.context_compactions = snapshot
                .context_compactions
                .into_iter()
                .map(|item| (item.invocation_id, item))
                .collect();
            reducer.interactions = snapshot
                .pending_interactions
                .into_iter()
                .map(|item| (item.interaction_id, item))
                .collect();
            reducer.activities = snapshot
                .activities
                .into_iter()
                .map(|item| (item.activity_id, item))
                .collect();
            reducer.runs = snapshot
                .runs
                .into_iter()
                .map(|item| (item.run_id, item))
                .collect();
            reducer.artifacts = snapshot
                .artifacts
                .into_iter()
                .map(|item| (item.artifact_id, item))
                .collect();
            for output in snapshot.stream_outputs {
                reducer.outputs.insert(
                    output.checkpoint.output_id,
                    MutableOutput::from_ref(
                        output.checkpoint,
                        output.message_id,
                        output.run_id,
                        output.channel,
                        output.content_ref,
                        output.first_conversation_seq,
                        output.occurred_at,
                    )?,
                );
            }
        }
        Ok(reducer)
    }

    pub async fn apply(
        &mut self,
        record: CanonicalLedgerRecord,
        content: Arc<dyn ProjectionContentStore>,
    ) -> Result<(), SubscriptionError> {
        match record.payload.clone() {
            LedgerPayload::OutputFrame(frame) => self.apply_frame(&record, frame, content).await,
            LedgerPayload::DomainEvent(DomainEvent::Artifact(event)) => {
                self.apply_artifact_event(&record, event)
            }
            LedgerPayload::DomainEvent(event) => {
                self.apply_event(&record, event);
                Ok(())
            }
        }
    }

    async fn apply_frame(
        &mut self,
        record: &CanonicalLedgerRecord,
        frame: OutputFrame,
        content: Arc<dyn ProjectionContentStore>,
    ) -> Result<(), SubscriptionError> {
        let terminal_reseal = frame.final_for_output
            && matches!(
                &frame.content,
                FrameContent::InlineUtf8(value) if value.byte_length() == 0
            );
        let bytes = match &frame.content {
            FrameContent::InlineUtf8(value) => value.as_str().as_bytes().to_vec(),
            FrameContent::ContentRef(reference) => content
                .read(self.tenant_id, reference)
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)?,
        };
        if bytes.is_empty() && frame.final_for_output && frame.byte_offset_start == 0 {
            return Ok(());
        }
        let run_id = record
            .metadata
            .run_id
            .ok_or(SubscriptionError::CorruptRecord)?;
        if self.is_private_model_run(run_id) {
            return Ok(());
        }
        let declared_message_id = frame
            .message_id
            .or_else(|| self.output_messages.get(&frame.output_id).copied());
        let existing_message_id = self
            .outputs
            .get(&frame.output_id)
            .and_then(|output| output.message_id);
        let message_id =
            declared_message_id
                .or(existing_message_id)
                .or_else(|| match frame.channel {
                    OutputChannel::AssistantText | OutputChannel::ReasoningSummary => {
                        self.latest_message_for_run(run_id)
                    }
                    OutputChannel::ToolArguments
                    | OutputChannel::ToolProgress
                    | OutputChannel::ArtifactContent => None,
                });
        let output = self.outputs.entry(frame.output_id).or_insert_with(|| {
            MutableOutput::new(
                frame.output_id,
                message_id,
                run_id,
                frame.channel,
                record.metadata.conversation_seq,
                record.metadata.occurred_at,
            )
        });
        if output.channel != frame.channel
            || output.run_id != run_id
            || output
                .message_id
                .zip(declared_message_id)
                .is_some_and(|(existing, incoming)| existing != incoming)
        {
            tracing::error!(
                conversation_id = %record.metadata.conversation_id,
                record_id = %record.metadata.record_id,
                conversation_seq = record.metadata.conversation_seq.get(),
                output_id = %frame.output_id,
                channel = ?frame.channel,
                run_id = %run_id,
                existing_channel = ?output.channel,
                existing_run_id = %output.run_id,
                existing_message_id = ?output.message_id,
                declared_message_id = ?declared_message_id,
                "product projection rejected inconsistent output identity"
            );
            return Err(SubscriptionError::CorruptRecord);
        }
        if output.message_id.is_none() {
            output.message_id = message_id;
        }
        let expected_frame_index = output.next_frame_index;
        let expected_byte_offset = output.bytes.as_ref().map_or_else(
            || output.reference.as_ref().map_or(0, |item| item.byte_length),
            |item| item.len() as u64,
        );
        let output_was_completed = output.completed;
        if let Err(error) = output
            .append(OutputAppendInput {
                tenant_id: self.tenant_id,
                content,
                frame_index: frame.frame_index,
                byte_offset_start: frame.byte_offset_start,
                bytes,
                final_for_output: frame.final_for_output,
                terminal_reseal,
            })
            .await
        {
            tracing::error!(
                conversation_id = %record.metadata.conversation_id,
                record_id = %record.metadata.record_id,
                conversation_seq = record.metadata.conversation_seq.get(),
                output_id = %frame.output_id,
                channel = ?frame.channel,
                frame_index = frame.frame_index,
                expected_frame_index,
                byte_offset_start = frame.byte_offset_start,
                expected_byte_offset,
                final_for_output = frame.final_for_output,
                output_was_completed,
                error = ?error,
                "product projection rejected a non-contiguous output frame"
            );
            return Err(error);
        }
        if matches!(
            frame.channel,
            OutputChannel::AssistantText | OutputChannel::ReasoningSummary
        ) && output.message_id.is_some()
        {
            let message_id = output.message_id.ok_or(SubscriptionError::CorruptRecord)?;
            self.messages.entry(message_id).or_insert(MutableMessage {
                message_id,
                participant_id: record.metadata.participant_id,
                role: MessageRole::Assistant,
                content_ref: None,
                reasoning_summaries: Vec::new(),
                status: ProjectedMessageStatus::Streaming,
                source_run_id: record.metadata.run_id,
                committed_at: record.metadata.occurred_at,
                attachment_refs: Vec::new(),
                requested_skill_refs: Vec::new(),
                project_task_target: None,
                imported_projection_ref: None,
                assistant_output: (frame.channel == OutputChannel::AssistantText)
                    .then(|| output.checkpoint()),
                partial_reason: None,
            });
        }
        Ok(())
    }

    fn apply_event(&mut self, record: &CanonicalLedgerRecord, event: DomainEvent) {
        match event {
            DomainEvent::Conversation(event) => self.apply_conversation_event(record, event),
            DomainEvent::Activity(event) => self.apply_activity_event(record, event),
            DomainEvent::Run(event) => self.apply_run_event(record, event),
            DomainEvent::Tool(event) => self.apply_tool_event(record, event),
            DomainEvent::Interaction(event) => self.apply_interaction_event(record, event),
            DomainEvent::Context(event) => self.apply_context_event(record, event),
            DomainEvent::Artifact(_) => {}
            DomainEvent::Model(_) | DomainEvent::Discovery(_) => {}
        }
    }

    fn apply_context_event(&mut self, record: &CanonicalLedgerRecord, event: ContextEvent) {
        let Some(run_id) = record.metadata.run_id else {
            return;
        };
        if self.is_private_model_run(run_id) {
            return;
        }
        match event {
            ContextEvent::DialogueBound { .. } => {}
            ContextEvent::CompactionStarted {
                invocation_id,
                loop_iteration,
                source_seq_from,
                source_seq_to,
                model_route_ref,
                input_tokens_before,
            } => {
                self.context_compactions.insert(
                    invocation_id,
                    ProjectedContextCompaction {
                        invocation_id,
                        run_id,
                        loop_iteration,
                        status: ProjectedContextCompactionStatus::Running,
                        model_route_ref,
                        input_tokens_before,
                        input_tokens_after: None,
                        summarized_record_count: 0,
                        retained_record_count: 0,
                        source_seq_from,
                        source_seq_to,
                        started_conversation_seq: record.metadata.conversation_seq,
                        started_at: record.metadata.occurred_at,
                        finished_at: None,
                        error_code: None,
                    },
                );
            }
            ContextEvent::CompactionFailed {
                invocation_id,
                error,
            } => {
                if let Some(compaction) = self.context_compactions.get_mut(&invocation_id) {
                    compaction.error_code = Some(error.code);
                }
            }
            ContextEvent::CompactionResolved { resolution } => {
                if let Some(compaction) =
                    self.context_compactions.get_mut(&resolution.invocation_id)
                {
                    compaction.status = match resolution.outcome {
                        ContextCompactionOutcome::RetryScheduled => {
                            ProjectedContextCompactionStatus::RetryScheduled
                        }
                        ContextCompactionOutcome::Succeeded => {
                            ProjectedContextCompactionStatus::Succeeded
                        }
                        ContextCompactionOutcome::FailedContinuedUncompressed => {
                            ProjectedContextCompactionStatus::FailedContinued
                        }
                        ContextCompactionOutcome::FailedStopped => {
                            ProjectedContextCompactionStatus::FailedStopped
                        }
                        ContextCompactionOutcome::IndeterminateStopped => {
                            ProjectedContextCompactionStatus::Indeterminate
                        }
                    };
                    compaction.input_tokens_before = resolution.input_tokens_before;
                    compaction.input_tokens_after = resolution.input_tokens_after;
                    compaction.summarized_record_count = resolution.summarized_record_count;
                    compaction.retained_record_count = resolution.retained_record_count;
                    compaction.error_code = resolution.error.map(|error| error.code);
                    compaction.finished_at = Some(record.metadata.occurred_at);
                }
            }
            ContextEvent::SnapshotCreated { .. }
            | ContextEvent::BranchCreated { .. }
            | ContextEvent::BranchActivated { .. }
            | ContextEvent::Derived { .. } => {}
        }
    }

    fn apply_activity_event(&mut self, record: &CanonicalLedgerRecord, event: ActivityEvent) {
        let Some(activity_id) = record.metadata.activity_id else {
            return;
        };
        match event {
            ActivityEvent::Started {
                kind,
                root_activity_id,
                parent_activity_id,
                policy_snapshot_ref,
            } => {
                self.activities.insert(
                    activity_id,
                    ProjectedActivity {
                        activity_id,
                        kind,
                        status: ActivityStatus::Active,
                        requested_by_participant_id: record.metadata.participant_id,
                        parent_activity_id,
                        root_activity_id,
                        policy_snapshot_ref,
                        started_at: record.metadata.occurred_at,
                        finished_at: None,
                        delegation_origin: self.pending_delegation_origins.remove(&activity_id),
                        coordination_origin: self.pending_coordination_origins.remove(&activity_id),
                    },
                );
            }
            ActivityEvent::DelegationContextProjected {
                delegation_id,
                parent_invocation_id,
                objective_ref,
            } => {
                // The objective remains immutable content and never becomes a
                // parent-transcript Message. Its reference is safe to expose
                // to the exact Child Activity projection used by the panel.
                let origin = ProjectedDelegationOrigin {
                    delegation_id,
                    parent_invocation_id,
                    objective_ref: Some(objective_ref),
                };
                if let Some(activity) = self.activities.get_mut(&activity_id) {
                    activity.delegation_origin = Some(origin);
                } else {
                    self.pending_delegation_origins.insert(activity_id, origin);
                }
            }
            ActivityEvent::CoordinationContextProjected {
                mailbox_message_id,
                source_conversation_id,
                payload_ref,
            } => {
                // The same immutable payload feeds private model context and
                // a typed Product ingress. It never becomes a UserMessage.
                let origin = ProjectedCoordinationOrigin {
                    mailbox_message_id,
                    source_conversation_id,
                    payload_ref: Some(payload_ref),
                    occurred_at: Some(record.metadata.occurred_at),
                };
                if let Some(activity) = self.activities.get_mut(&activity_id) {
                    activity.coordination_origin = Some(origin);
                } else {
                    self.pending_coordination_origins
                        .insert(activity_id, origin);
                }
            }
            ActivityEvent::MaintenanceContextProjected { .. } => {
                // Runtime-private autonomous learning input. It is visible to
                // the maintenance executor only and never to the transcript.
            }
            ActivityEvent::StandingGoalContextProjected { .. } => {
                // Goal objective and Evaluation summary remain private model
                // context. Goal UI is projected from the dedicated durable
                // Goal read model, never by impersonating a transcript entry.
            }
            ActivityEvent::StandingGoalJudgeContextProjected { .. } => {
                // Independent Judge inputs and receipts remain Runtime-private.
                // The dedicated Goal read model projects only the resulting
                // authoritative Evaluation.
            }
            ActivityEvent::Completed { .. } => {
                if let Some(activity) = self.activities.get_mut(&activity_id) {
                    activity.status = ActivityStatus::Completed;
                    activity.finished_at = Some(record.metadata.occurred_at);
                }
            }
            ActivityEvent::Failed { .. } => {
                if let Some(activity) = self.activities.get_mut(&activity_id) {
                    activity.status = ActivityStatus::Failed;
                    activity.finished_at = Some(record.metadata.occurred_at);
                }
            }
            ActivityEvent::Cancelled { .. } => {
                if let Some(activity) = self.activities.get_mut(&activity_id) {
                    activity.status = ActivityStatus::Cancelled;
                    activity.finished_at = Some(record.metadata.occurred_at);
                }
            }
        }
    }

    fn apply_conversation_event(
        &mut self,
        record: &CanonicalLedgerRecord,
        event: ConversationEvent,
    ) {
        if record
            .metadata
            .run_id
            .is_some_and(|run_id| self.is_private_model_run(run_id))
        {
            return;
        }
        match event {
            ConversationEvent::MessageAccepted {
                project_task_target,
                message_id,
                role,
                content_ref,
                attachment_refs,
                requested_skill_refs,
            } => {
                self.upsert_message(
                    record,
                    message_id,
                    role,
                    content_ref,
                    attachment_refs,
                    requested_skill_refs,
                    None,
                    ProjectedMessageStatus::Committed,
                    None,
                );
                if let Some(message) = self.messages.get_mut(&message_id) {
                    message.project_task_target = project_task_target;
                }
            }
            ConversationEvent::MessageCommitted {
                message_id,
                role,
                content_ref,
                ..
            } => self.upsert_message(
                record,
                message_id,
                role,
                content_ref,
                Vec::new(),
                Vec::new(),
                None,
                ProjectedMessageStatus::Committed,
                None,
            ),
            ConversationEvent::MessagePartiallyCommitted {
                message_id,
                content_ref,
                reason,
                ..
            } => self.upsert_message(
                record,
                message_id,
                MessageRole::Assistant,
                content_ref,
                Vec::new(),
                Vec::new(),
                None,
                ProjectedMessageStatus::Partial,
                Some(reason),
            ),
            ConversationEvent::MessageImported {
                message_id,
                role,
                content_ref,
                projection_ref,
                attachment_refs,
                ..
            } => self.upsert_message(
                record,
                message_id,
                role,
                content_ref,
                attachment_refs,
                Vec::new(),
                projection_ref,
                ProjectedMessageStatus::Committed,
                None,
            ),
            ConversationEvent::MessageRetracted {
                message_id,
                source_run_id,
                ..
            } => {
                self.retract_message(message_id, source_run_id);
            }
            ConversationEvent::WorkspaceAssigned { .. }
            | ConversationEvent::Created { .. }
            | ConversationEvent::Archived
            | ConversationEvent::TitleChanged { .. }
            | ConversationEvent::ParticipantJoined { .. }
            | ConversationEvent::ParticipantLeft => {}
        }
    }

    fn is_private_model_run(&self, run_id: RunId) -> bool {
        self.runs
            .get(&run_id)
            .and_then(|run| self.activities.get(&run.activity_id))
            .is_some_and(|activity| activity.kind == ActivityKind::Evaluation)
    }

    fn retract_message(&mut self, message_id: MessageId, source_run_id: Option<RunId>) {
        let inferred_run_id = self
            .messages
            .get(&message_id)
            .and_then(|message| message.source_run_id);
        let scope = ProjectionRetraction::new(message_id, source_run_id)
            .with_inferred_run(inferred_run_id)
            .with_projection_graph(self.runs.values(), self.activities.values());
        let retracted_message_ids = self
            .messages
            .iter()
            .filter(|(candidate_id, message)| {
                scope.retracts_message(**candidate_id, message.source_run_id)
            })
            .map(|(candidate_id, _)| *candidate_id)
            .collect::<BTreeSet<_>>();
        self.messages.retain(|candidate_id, message| {
            !scope.retracts_message(*candidate_id, message.source_run_id)
        });
        self.tools
            .retain(|_, tool| !scope.retracts_run(tool.run_id));
        self.context_compactions
            .retain(|_, compaction| !scope.retracts_run(compaction.run_id));
        self.interactions
            .retain(|_, interaction| !scope.retracts_run(interaction.run_id));
        self.runs.retain(|run_id, _| !scope.retracts_run(*run_id));
        self.activities
            .retain(|activity_id, _| !scope.retracts_activity(*activity_id));
        self.pending_delegation_origins
            .retain(|activity_id, _| !scope.retracts_activity(*activity_id));
        self.pending_coordination_origins
            .retain(|activity_id, _| !scope.retracts_activity(*activity_id));
        self.artifacts.retain(|_, artifact| {
            !scope.retracts_artifact(artifact.created_by_run_id, artifact.activity_id)
        });
        self.pending_artifacts
            .retain(|_, artifact| !scope.retracts_artifact(artifact.run_id, artifact.activity_id));
        let retracted_output_ids = self
            .outputs
            .iter()
            .filter(|(output_id, output)| {
                let bound_message_id = output
                    .message_id
                    .or_else(|| self.output_messages.get(output_id).copied());
                scope.retracts_output(bound_message_id, output.run_id)
                    || bound_message_id.is_some_and(|id| retracted_message_ids.contains(&id))
            })
            .map(|(output_id, _)| *output_id)
            .collect::<BTreeSet<_>>();
        self.outputs
            .retain(|output_id, _| !retracted_output_ids.contains(output_id));
        self.output_messages.retain(|output_id, bound_message_id| {
            !retracted_output_ids.contains(output_id)
                && !retracted_message_ids.contains(bound_message_id)
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_message(
        &mut self,
        record: &CanonicalLedgerRecord,
        message_id: MessageId,
        role: MessageRole,
        content_ref: ContentRef,
        attachment_refs: Vec<musteragent_runtime_contract::AttachmentRef>,
        requested_skill_refs: Vec<musteragent_identity::VersionedKey>,
        imported_projection_ref: Option<ContentRef>,
        status: ProjectedMessageStatus,
        partial_reason: Option<musteragent_conversation::PartialMessageReason>,
    ) {
        let value = self.messages.entry(message_id).or_insert(MutableMessage {
            message_id,
            participant_id: record.metadata.participant_id,
            role,
            content_ref: None,
            reasoning_summaries: Vec::new(),
            status,
            source_run_id: record.metadata.run_id,
            committed_at: record.metadata.occurred_at,
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
            project_task_target: None,
            imported_projection_ref: None,
            assistant_output: None,
            partial_reason: None,
        });
        value.role = role;
        value.content_ref = Some(content_ref);
        value.status = status;
        value.committed_at = record.metadata.occurred_at;
        value.participant_id = value.participant_id.or(record.metadata.participant_id);
        value.source_run_id = value.source_run_id.or(record.metadata.run_id);
        if !attachment_refs.is_empty() {
            value.attachment_refs = attachment_refs;
        }
        if !requested_skill_refs.is_empty() {
            value.requested_skill_refs = requested_skill_refs;
        }
        if imported_projection_ref.is_some() {
            value.imported_projection_ref = imported_projection_ref;
        }
        value.partial_reason = partial_reason;
        if role == MessageRole::Assistant {
            value.assistant_output = self
                .outputs
                .values()
                .find(|output| {
                    output.channel == OutputChannel::AssistantText
                        && output.message_id == Some(message_id)
                })
                .map(MutableOutput::checkpoint)
                .or_else(|| value.assistant_output.clone());
        }
    }

    fn apply_run_event(&mut self, record: &CanonicalLedgerRecord, event: RunEvent) {
        let Some(run_id) = record.metadata.run_id else {
            return;
        };
        match event {
            RunEvent::Admitted {
                parent_run_id,
                delegation_id,
                retry_source_message_id,
                retry_source_attempt_number,
                ..
            } => {
                let Some(activity_id) = record.metadata.activity_id else {
                    return;
                };
                self.runs.insert(
                    run_id,
                    ProjectedRun {
                        run_id,
                        activity_id,
                        executor_participant_id: record.metadata.participant_id,
                        parent_run_id,
                        delegation_id,
                        retry_source_message_id,
                        retry_source_attempt_number,
                        terminal_failure: None,
                        retry_failure: None,
                        status: RunStatus::Queued,
                        wait_reason: None,
                        attempt_number: None,
                        next_attempt_number: None,
                        retry_not_before: None,
                        admitted_at: record.metadata.occurred_at,
                        started_at: None,
                        finished_at: None,
                    },
                );
            }
            RunEvent::Started { attempt_number, .. } | RunEvent::Resumed { attempt_number, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && accepts_nonterminal_progress(run.status)
                {
                    run.status = RunStatus::Running;
                    run.wait_reason = None;
                    run.attempt_number = Some(attempt_number);
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                    run.started_at.get_or_insert(record.metadata.occurred_at);
                }
            }
            RunEvent::RecoveryRequested { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && accepts_nonterminal_progress(run.status)
                {
                    run.status = RunStatus::Queued;
                    run.wait_reason = None;
                    run.retry_failure = None;
                    run.retry_not_before = None;
                    run.next_attempt_number = None;
                }
            }
            RunEvent::Waiting {
                reason,
                resume_not_before,
                ..
            } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && accepts_nonterminal_progress(run.status)
                {
                    run.status = RunStatus::Waiting;
                    run.wait_reason = Some(reason);
                    if reason == RunWaitReason::RetryBackoff {
                        run.retry_not_before = resume_not_before.or(run.retry_not_before);
                    } else {
                        run.next_attempt_number = None;
                        run.retry_not_before = None;
                        run.retry_failure = None;
                    }
                }
            }
            RunEvent::CancelRequested { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && !run.status.is_terminal()
                {
                    run.status = RunStatus::Cancelling;
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                }
            }
            RunEvent::RetryScheduled {
                next_attempt_number,
                not_before,
                error,
                ..
            } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && accepts_nonterminal_progress(run.status)
                {
                    run.status = RunStatus::Waiting;
                    run.wait_reason = Some(RunWaitReason::RetryBackoff);
                    run.next_attempt_number = Some(next_attempt_number);
                    run.retry_not_before = Some(not_before);
                    run.retry_failure = Some(ProjectedRunFailure {
                        error,
                        ledger_record_id: record.metadata.record_id,
                        correlation_id: record.metadata.correlation_id,
                        conversation_sequence: record.metadata.conversation_seq,
                    });
                }
            }
            RunEvent::BudgetAmended { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id)
                    && accepts_nonterminal_progress(run.status)
                {
                    run.status = RunStatus::Queued;
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                }
            }
            RunEvent::Completed { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.status = RunStatus::Completed;
                    run.terminal_failure = None;
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                    run.finished_at = Some(record.metadata.occurred_at);
                }
            }
            RunEvent::Failed { error, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.status = RunStatus::Failed;
                    run.terminal_failure = Some(ProjectedRunFailure {
                        error,
                        ledger_record_id: record.metadata.record_id,
                        correlation_id: record.metadata.correlation_id,
                        conversation_sequence: record.metadata.conversation_seq,
                    });
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                    run.finished_at = Some(record.metadata.occurred_at);
                }
            }
            RunEvent::Cancelled { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.status = RunStatus::Cancelled;
                    run.terminal_failure = None;
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                    run.finished_at = Some(record.metadata.occurred_at);
                }
            }
            RunEvent::Interrupted { .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.status = RunStatus::Interrupted;
                    run.terminal_failure = None;
                    run.wait_reason = None;
                    run.next_attempt_number = None;
                    run.retry_not_before = None;
                    run.retry_failure = None;
                    run.finished_at = Some(record.metadata.occurred_at);
                }
            }
            RunEvent::AttemptClaimed { .. }
            | RunEvent::BudgetExceeded { .. }
            | RunEvent::ControlQueued { .. }
            | RunEvent::ControlConsumed { .. }
            | RunEvent::ExecutionLivenessRecorded { .. }
            | RunEvent::ExecutionPlanUpdated { .. }
            | RunEvent::AttemptSucceeded { .. }
            | RunEvent::AttemptFailed { .. }
            | RunEvent::AttemptAbandoned { .. }
            | RunEvent::AttemptSuspended { .. } => {}
        }
        if self
            .runs
            .get(&run_id)
            .is_some_and(|run| run.status.is_terminal())
        {
            for compaction in self
                .context_compactions
                .values_mut()
                .filter(|compaction| compaction.run_id == run_id)
            {
                if !compaction.status.is_terminal() {
                    compaction.status = ProjectedContextCompactionStatus::Interrupted;
                    compaction.finished_at = Some(record.metadata.occurred_at);
                }
            }
        }
    }

    fn apply_tool_event(&mut self, record: &CanonicalLedgerRecord, event: ToolEvent) {
        match event {
            ToolEvent::InvocationRequested {
                invocation_id,
                tool_call_id,
                tool_key,
                request_ref,
                ..
            } => {
                let Some(run_id) = record.metadata.run_id else {
                    return;
                };
                self.tools.insert(
                    invocation_id,
                    ProjectedToolItem {
                        invocation_id,
                        message_id: self.latest_message_for_run(run_id),
                        run_id,
                        tool_call_id,
                        tool_ref: tool_key.as_str().to_owned(),
                        status: ProjectedToolStatus::Requested,
                        request_ref,
                        result_ref: None,
                        requested_conversation_seq: record.metadata.conversation_seq,
                        requested_at: record.metadata.occurred_at,
                        started_at: None,
                        finished_at: None,
                        output_artifact_ids: Vec::new(),
                    },
                );
            }
            // Routing is an append-only diagnostic fact, not a Tool status change.
            ToolEvent::SandboxRouted { .. } => {}
            ToolEvent::InvocationWaitingInteraction { invocation_id, .. } => {
                self.update_tool(invocation_id, ProjectedToolStatus::Waiting, record, None);
            }
            ToolEvent::InvocationStarted { invocation_id, .. } => {
                self.update_tool(invocation_id, ProjectedToolStatus::Running, record, None);
            }
            ToolEvent::InvocationSucceeded {
                invocation_id,
                result_ref,
                ..
            } => self.update_tool(
                invocation_id,
                ProjectedToolStatus::Completed,
                record,
                Some(result_ref),
            ),
            ToolEvent::InvocationFailed {
                invocation_id,
                diagnostic_result_ref,
                ..
            } => {
                self.update_tool(
                    invocation_id,
                    ProjectedToolStatus::Failed,
                    record,
                    diagnostic_result_ref,
                );
            }
            ToolEvent::InvocationIndeterminate {
                invocation_id,
                diagnostic_result_ref,
                ..
            } => self.update_tool(
                invocation_id,
                ProjectedToolStatus::ResultUnknown,
                record,
                diagnostic_result_ref,
            ),
            ToolEvent::InvocationCancelled { invocation_id, .. } => {
                self.update_tool(invocation_id, ProjectedToolStatus::Cancelled, record, None);
            }
        }
    }

    fn update_tool(
        &mut self,
        invocation_id: InvocationId,
        status: ProjectedToolStatus,
        record: &CanonicalLedgerRecord,
        result_ref: Option<ContentRef>,
    ) {
        if let Some(tool) = self.tools.get_mut(&invocation_id) {
            tool.status = status;
            if status == ProjectedToolStatus::Running && tool.started_at.is_none() {
                tool.started_at = Some(record.metadata.occurred_at);
            }
            if result_ref.is_some() {
                tool.result_ref = result_ref;
            }
            if matches!(
                status,
                ProjectedToolStatus::Completed
                    | ProjectedToolStatus::Failed
                    | ProjectedToolStatus::Cancelled
                    | ProjectedToolStatus::ResultUnknown
            ) {
                tool.finished_at = Some(record.metadata.occurred_at);
            }
        }
    }

    fn apply_interaction_event(&mut self, record: &CanonicalLedgerRecord, event: InteractionEvent) {
        match event {
            InteractionEvent::Requested {
                interaction_id,
                kind,
                prompt_ref,
                response_schema,
                expires_at,
                ..
            } => {
                let Some(run_id) = record.metadata.run_id else {
                    return;
                };
                self.interactions.insert(
                    interaction_id,
                    ProjectedInteraction {
                        interaction_id,
                        run_id,
                        kind,
                        prompt_ref,
                        response_schema,
                        requested_at: record.metadata.occurred_at,
                        expires_at,
                    },
                );
            }
            InteractionEvent::Responded { interaction_id, .. }
            | InteractionEvent::Expired { interaction_id, .. }
            | InteractionEvent::Cancelled { interaction_id, .. } => {
                self.interactions.remove(&interaction_id);
            }
        }
    }

    fn apply_artifact_event(
        &mut self,
        record: &CanonicalLedgerRecord,
        event: ArtifactEvent,
    ) -> Result<(), SubscriptionError> {
        match event {
            ArtifactEvent::Created {
                artifact_id,
                kind,
                title,
                source_invocation_id,
            } => {
                let runtime_owned = source_invocation_id.is_some();
                if runtime_owned
                    != (record.metadata.activity_id.is_some() && record.metadata.run_id.is_some())
                {
                    return Err(SubscriptionError::CorruptRecord);
                }
                self.pending_artifacts.insert(
                    artifact_id,
                    PendingArtifact {
                        kind,
                        title: title.as_str().to_owned(),
                        activity_id: record.metadata.activity_id,
                        run_id: record.metadata.run_id,
                    },
                );
                if let Some(invocation_id) = source_invocation_id {
                    self.link_tool_output(invocation_id, artifact_id);
                }
            }
            ArtifactEvent::VersionCommitted {
                artifact_id,
                version,
                content_ref,
                dependency_refs,
                source_invocation_id,
                ..
            } => {
                if let Some(invocation_id) = source_invocation_id {
                    self.link_tool_output(invocation_id, artifact_id);
                }
                validate_dependency_refs(&content_ref, &dependency_refs)
                    .map_err(|_| SubscriptionError::CorruptRecord)?;
                if let Some(artifact) = self.artifacts.get_mut(&artifact_id) {
                    artifact.current_version = version;
                    artifact.current_content_ref = content_ref;
                    artifact.current_dependency_refs = dependency_refs;
                    artifact.lifecycle = ArtifactLifecycle::Published;
                    artifact.updated_by_run_id = record.metadata.run_id;
                    if record.metadata.run_id.is_some() {
                        artifact.presentation_run_id = record.metadata.run_id;
                    }
                    artifact.updated_at = record.metadata.occurred_at;
                } else if let Some(pending) = self.pending_artifacts.remove(&artifact_id) {
                    self.artifacts.insert(
                        artifact_id,
                        ProjectedArtifact {
                            artifact_id,
                            activity_id: pending.activity_id,
                            created_by_run_id: pending.run_id,
                            updated_by_run_id: record.metadata.run_id,
                            presentation_run_id: record.metadata.run_id.or(pending.run_id),
                            kind: pending.kind,
                            lifecycle: ArtifactLifecycle::Published,
                            current_version: version,
                            title: pending.title,
                            current_content_ref: content_ref,
                            current_dependency_refs: dependency_refs,
                            updated_at: record.metadata.occurred_at,
                        },
                    );
                }
            }
            ArtifactEvent::NoteAuthoringStarted { artifact_id, .. } => {
                if let Some(artifact) = self.artifacts.get_mut(&artifact_id) {
                    artifact.lifecycle = ArtifactLifecycle::Draft;
                    artifact.updated_at = record.metadata.occurred_at;
                }
            }
            ArtifactEvent::NoteVersionFinalized { artifact_id, .. } => {
                if let Some(artifact) = self.artifacts.get_mut(&artifact_id) {
                    artifact.lifecycle = ArtifactLifecycle::Published;
                    artifact.updated_at = record.metadata.occurred_at;
                }
            }
            ArtifactEvent::NoteSegmentCommittedV1 { .. }
            | ArtifactEvent::NoteSegmentCommitted { .. } => {}
            ArtifactEvent::Failed { artifact_id, .. } => {
                if let Some(artifact) = self.artifacts.get_mut(&artifact_id) {
                    artifact.lifecycle = ArtifactLifecycle::Failed;
                    artifact.updated_at = record.metadata.occurred_at;
                }
            }
            ArtifactEvent::EditProposed { .. }
            | ArtifactEvent::EditApplied { .. }
            | ArtifactEvent::EditRejected { .. }
            | ArtifactEvent::EditExpired { .. } => {}
        }
        Ok(())
    }

    fn link_tool_output(&mut self, invocation_id: InvocationId, artifact_id: ArtifactId) {
        let Some(tool) = self.tools.get_mut(&invocation_id) else {
            return;
        };
        if !tool.output_artifact_ids.contains(&artifact_id) {
            tool.output_artifact_ids.push(artifact_id);
        }
    }

    fn latest_message_for_run(&self, run_id: RunId) -> Option<MessageId> {
        self.messages
            .values()
            .filter(|message| message.source_run_id == Some(run_id))
            .max_by_key(|message| message.committed_at)
            .map(|message| message.message_id)
    }

    pub async fn finish(
        mut self,
        content: Arc<dyn ProjectionContentStore>,
        through: LedgerOffset,
        created_at: UtcTimestamp,
    ) -> Result<ConversationProjectionSnapshot, SubscriptionError> {
        let output_ids = self.outputs.keys().copied().collect::<Vec<_>>();
        let mut stream_outputs = Vec::new();
        let mut assistant_text_by_run = BTreeMap::<RunId, Vec<(ConversationSeq, Vec<u8>)>>::new();
        for output_id in output_ids {
            let output = self
                .outputs
                .get_mut(&output_id)
                .ok_or(SubscriptionError::CorruptRecord)?;
            let reference = output
                .materialize(self.tenant_id, Arc::clone(&content))
                .await?;
            match output.channel {
                OutputChannel::AssistantText => {
                    let text = output.text(Arc::clone(&content), self.tenant_id).await?;
                    assistant_text_by_run
                        .entry(output.run_id)
                        .or_default()
                        .push((output.first_conversation_seq, text.into_bytes()));
                    stream_outputs.push(ProjectedStreamOutput {
                        checkpoint: output.checkpoint(),
                        message_id: output.message_id,
                        run_id: output.run_id,
                        channel: output.channel,
                        content_ref: reference,
                        first_conversation_seq: output.first_conversation_seq,
                        occurred_at: output.occurred_at,
                    });
                }
                OutputChannel::ReasoningSummary => {
                    if reference.byte_length == 0
                        || output
                            .text(Arc::clone(&content), self.tenant_id)
                            .await?
                            .trim()
                            .is_empty()
                    {
                        continue;
                    }
                    stream_outputs.push(ProjectedStreamOutput {
                        checkpoint: output.checkpoint(),
                        message_id: output.message_id,
                        run_id: output.run_id,
                        channel: output.channel,
                        content_ref: reference,
                        first_conversation_seq: output.first_conversation_seq,
                        occurred_at: output.occurred_at,
                    });
                }
                OutputChannel::ToolArguments
                | OutputChannel::ToolProgress
                | OutputChannel::ArtifactContent => {
                    stream_outputs.push(ProjectedStreamOutput {
                        checkpoint: output.checkpoint(),
                        message_id: output.message_id,
                        run_id: output.run_id,
                        channel: output.channel,
                        content_ref: reference,
                        first_conversation_seq: output.first_conversation_seq,
                        occurred_at: output.occurred_at,
                    });
                }
            }
        }

        self.messages = consolidate_assistant_messages(
            self.tenant_id,
            Arc::clone(&content),
            self.messages,
            assistant_text_by_run,
        )
        .await?;
        let empty_message_ids = self
            .messages
            .values()
            .filter(|message| message.content_ref.is_none())
            .map(|message| message.message_id)
            .collect::<Vec<_>>();
        for message_id in empty_message_ids {
            let reference = content
                .put(
                    self.tenant_id,
                    MediaType::new("text/plain;charset=utf-8")
                        .map_err(|_| SubscriptionError::CorruptRecord)?,
                    Vec::new(),
                )
                .await
                .map_err(|_| SubscriptionError::SourceUnavailable)?;
            if let Some(message) = self.messages.get_mut(&message_id) {
                message.content_ref = Some(reference);
            }
        }
        let mut messages = self
            .messages
            .into_values()
            .map(MutableMessage::finish)
            .collect::<Result<Vec<_>, _>>()?;
        messages.sort_by_key(|message| (message.committed_at, message.message_id));
        let mut tool_timeline = self.tools.into_values().collect::<Vec<_>>();
        tool_timeline.sort_by_key(|item| (item.requested_conversation_seq, item.invocation_id));
        let mut context_compactions = self.context_compactions.into_values().collect::<Vec<_>>();
        context_compactions.sort_by_key(|item| (item.started_conversation_seq, item.invocation_id));
        let mut pending_interactions = self.interactions.into_values().collect::<Vec<_>>();
        pending_interactions.sort_by_key(|item| (item.requested_at, item.interaction_id));
        let mut activities = self.activities.into_values().collect::<Vec<_>>();
        activities.sort_by_key(|item| (item.started_at, item.activity_id));
        let mut runs = self.runs.into_values().collect::<Vec<_>>();
        runs.sort_by_key(|item| (item.admitted_at, item.run_id));
        let active_runs = runs
            .iter()
            .filter(|item| !item.status.is_terminal())
            .cloned()
            .collect::<Vec<_>>();
        let mut artifacts = self.artifacts.into_values().collect::<Vec<_>>();
        artifacts.sort_by_key(|item| (item.updated_at, item.artifact_id));
        stream_outputs.sort_by_key(|item| (item.first_conversation_seq, item.checkpoint.output_id));
        let mut snapshot = ConversationProjectionSnapshot {
            schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
            snapshot_id: ProjectionSnapshotId::generate(),
            tenant_id: self.tenant_id,
            conversation_id: self.conversation_id,
            home_epoch: self.home_epoch,
            through_conversation_seq: through,
            messages,
            tool_timeline,
            context_compactions,
            pending_interactions,
            activities,
            runs,
            active_runs,
            artifacts,
            stream_outputs,
            content_manifest: Vec::new(),
            checksum: Sha256::digest(b"pending-projection-checksum"),
            created_at,
        };
        snapshot.content_manifest =
            canonical_content_manifest(&snapshot).map_err(|_| SubscriptionError::CorruptRecord)?;
        Ok(snapshot)
    }
}

async fn consolidate_assistant_messages(
    tenant_id: TenantId,
    content: Arc<dyn ProjectionContentStore>,
    messages: BTreeMap<MessageId, MutableMessage>,
    mut assistant_text_by_run: BTreeMap<RunId, Vec<(ConversationSeq, Vec<u8>)>>,
) -> Result<BTreeMap<MessageId, MutableMessage>, SubscriptionError> {
    let mut projected = BTreeMap::new();
    let mut grouped = BTreeMap::<RunId, Vec<MutableMessage>>::new();
    for message in messages.into_values() {
        if message.role == MessageRole::Assistant
            && let Some(run_id) = message.source_run_id
        {
            grouped.entry(run_id).or_default().push(message);
        } else {
            projected.insert(message.message_id, message);
        }
    }

    for (run_id, mut run_messages) in grouped {
        run_messages.sort_by_key(|message| (message.committed_at, message.message_id));
        let mut message = run_messages
            .drain(..1)
            .next()
            .ok_or(SubscriptionError::CorruptRecord)?;
        for segment in run_messages {
            message.participant_id = message.participant_id.or(segment.participant_id);
            message.status = merge_message_status(message.status, segment.status);
            message.attachment_refs.extend(segment.attachment_refs);
            message.imported_projection_ref = message
                .imported_projection_ref
                .or(segment.imported_projection_ref);
            message.partial_reason = message.partial_reason.or(segment.partial_reason);
        }

        if let Some(mut segments) = assistant_text_by_run.remove(&run_id) {
            segments.sort_by_key(|(sequence, _)| *sequence);
            let bytes = segments
                .into_iter()
                .flat_map(|(_, bytes)| bytes)
                .collect::<Vec<_>>();
            message.content_ref = Some(
                content
                    .put(
                        tenant_id,
                        MediaType::new("text/plain;charset=utf-8")
                            .map_err(|_| SubscriptionError::CorruptRecord)?,
                        bytes,
                    )
                    .await
                    .map_err(|_| SubscriptionError::SourceUnavailable)?,
            );
        }

        // Every physical Assistant Output is retained as an ordered stream
        // output. Embedding one arbitrary checkpoint in the Run-level Message
        // would both lose earlier iterations and duplicate Output identity.
        message.reasoning_summaries.clear();
        message.assistant_output = None;
        projected.insert(message.message_id, message);
    }
    if !assistant_text_by_run.is_empty() {
        return Err(SubscriptionError::CorruptRecord);
    }
    Ok(projected)
}

fn merge_message_status(
    left: ProjectedMessageStatus,
    right: ProjectedMessageStatus,
) -> ProjectedMessageStatus {
    match (left, right) {
        (ProjectedMessageStatus::Partial, _) | (_, ProjectedMessageStatus::Partial) => {
            ProjectedMessageStatus::Partial
        }
        (ProjectedMessageStatus::Committed, _) | (_, ProjectedMessageStatus::Committed) => {
            ProjectedMessageStatus::Committed
        }
        _ => ProjectedMessageStatus::Streaming,
    }
}

struct MutableMessage {
    message_id: MessageId,
    participant_id: Option<musteragent_identity::ParticipantId>,
    role: MessageRole,
    content_ref: Option<ContentRef>,
    reasoning_summaries: Vec<ProjectedReasoningSummary>,
    status: ProjectedMessageStatus,
    source_run_id: Option<RunId>,
    committed_at: UtcTimestamp,
    attachment_refs: Vec<musteragent_runtime_contract::AttachmentRef>,
    requested_skill_refs: Vec<musteragent_identity::VersionedKey>,
    project_task_target: Option<musteragent_conversation::ProjectTaskTarget>,
    imported_projection_ref: Option<ContentRef>,
    assistant_output: Option<ProjectedOutputCheckpoint>,
    partial_reason: Option<musteragent_conversation::PartialMessageReason>,
}

impl MutableMessage {
    fn from_projected(value: ProjectedMessage) -> Self {
        Self {
            message_id: value.message_id,
            participant_id: value.participant_id,
            role: value.role,
            content_ref: Some(value.content_ref),
            reasoning_summaries: value.reasoning_summaries,
            status: value.status,
            source_run_id: value.source_run_id,
            committed_at: value.committed_at,
            attachment_refs: value.attachment_refs,
            requested_skill_refs: value.requested_skill_refs,
            project_task_target: value.project_task_target,
            imported_projection_ref: value.imported_projection_ref,
            assistant_output: value.assistant_output,
            partial_reason: value.partial_reason,
        }
    }

    fn finish(self) -> Result<ProjectedMessage, SubscriptionError> {
        Ok(ProjectedMessage {
            message_id: self.message_id,
            participant_id: self.participant_id,
            role: self.role,
            content_ref: self.content_ref.ok_or(SubscriptionError::CorruptRecord)?,
            reasoning_summaries: self.reasoning_summaries,
            status: self.status,
            source_run_id: self.source_run_id,
            committed_at: self.committed_at,
            attachment_refs: self.attachment_refs,
            requested_skill_refs: self.requested_skill_refs,
            project_task_target: self.project_task_target,
            imported_projection_ref: self.imported_projection_ref,
            assistant_output: self.assistant_output,
            partial_reason: self.partial_reason,
        })
    }
}

struct MutableOutput {
    output_id: OutputId,
    message_id: Option<MessageId>,
    run_id: RunId,
    channel: OutputChannel,
    first_conversation_seq: ConversationSeq,
    occurred_at: UtcTimestamp,
    reference: Option<ContentRef>,
    bytes: Option<Vec<u8>>,
    next_frame_index: u64,
    completed: bool,
}

struct OutputAppendInput {
    tenant_id: TenantId,
    content: Arc<dyn ProjectionContentStore>,
    frame_index: u64,
    byte_offset_start: u64,
    bytes: Vec<u8>,
    final_for_output: bool,
    terminal_reseal: bool,
}

impl MutableOutput {
    fn new(
        output_id: OutputId,
        message_id: Option<MessageId>,
        run_id: RunId,
        channel: OutputChannel,
        first_conversation_seq: ConversationSeq,
        occurred_at: UtcTimestamp,
    ) -> Self {
        Self {
            output_id,
            message_id,
            run_id,
            channel,
            first_conversation_seq,
            occurred_at,
            reference: None,
            bytes: Some(Vec::new()),
            next_frame_index: 0,
            completed: false,
        }
    }

    fn from_ref(
        checkpoint: ProjectedOutputCheckpoint,
        message_id: Option<MessageId>,
        run_id: RunId,
        channel: OutputChannel,
        reference: ContentRef,
        first_conversation_seq: ConversationSeq,
        occurred_at: UtcTimestamp,
    ) -> Result<Self, SubscriptionError> {
        if reference.byte_length != checkpoint.next_byte_offset || checkpoint.next_frame_index == 0
        {
            return Err(SubscriptionError::CorruptRecord);
        }
        Ok(Self {
            output_id: checkpoint.output_id,
            message_id,
            run_id,
            channel,
            first_conversation_seq,
            occurred_at,
            reference: Some(reference),
            bytes: None,
            next_frame_index: checkpoint.next_frame_index,
            completed: checkpoint.completed,
        })
    }

    async fn append(&mut self, input: OutputAppendInput) -> Result<(), SubscriptionError> {
        let OutputAppendInput {
            tenant_id,
            content,
            frame_index,
            byte_offset_start,
            bytes,
            final_for_output,
            terminal_reseal,
        } = input;
        if frame_index != self.next_frame_index {
            return Err(SubscriptionError::CorruptRecord);
        }
        if self.completed {
            let next_byte_offset = self.bytes.as_ref().map_or_else(
                || self.reference.as_ref().map_or(0, |item| item.byte_length),
                |item| item.len() as u64,
            );
            if !terminal_reseal || byte_offset_start != next_byte_offset || !bytes.is_empty() {
                return Err(SubscriptionError::CorruptRecord);
            }
            self.next_frame_index = self
                .next_frame_index
                .checked_add(1)
                .ok_or(SubscriptionError::CorruptRecord)?;
            return Ok(());
        }
        if self.bytes.is_none() {
            let reference = self
                .reference
                .as_ref()
                .ok_or(SubscriptionError::CorruptRecord)?;
            self.bytes = Some(
                content
                    .read(tenant_id, reference)
                    .await
                    .map_err(|_| SubscriptionError::SourceUnavailable)?,
            );
        }
        let target = self
            .bytes
            .as_mut()
            .ok_or(SubscriptionError::CorruptRecord)?;
        if target.len() as u64 != byte_offset_start {
            return Err(SubscriptionError::CorruptRecord);
        }
        target.extend_from_slice(&bytes);
        self.reference = None;
        self.next_frame_index = self
            .next_frame_index
            .checked_add(1)
            .ok_or(SubscriptionError::CorruptRecord)?;
        self.completed = final_for_output;
        Ok(())
    }

    fn checkpoint(&self) -> ProjectedOutputCheckpoint {
        ProjectedOutputCheckpoint {
            output_id: self.output_id,
            next_frame_index: self.next_frame_index,
            next_byte_offset: self.bytes.as_ref().map_or_else(
                || self.reference.as_ref().map_or(0, |item| item.byte_length),
                |item| item.len() as u64,
            ),
            completed: self.completed,
        }
    }

    async fn materialize(
        &mut self,
        tenant_id: TenantId,
        content: Arc<dyn ProjectionContentStore>,
    ) -> Result<ContentRef, SubscriptionError> {
        if let Some(reference) = self.reference.clone() {
            return Ok(reference);
        }
        let media_type = match self.channel {
            OutputChannel::ToolArguments => "application/json",
            OutputChannel::AssistantText
            | OutputChannel::ReasoningSummary
            | OutputChannel::ToolProgress
            | OutputChannel::ArtifactContent => "text/plain;charset=utf-8",
        };
        let reference = content
            .put(
                tenant_id,
                MediaType::new(media_type).map_err(|_| SubscriptionError::CorruptRecord)?,
                self.bytes.clone().unwrap_or_default(),
            )
            .await
            .map_err(|_| SubscriptionError::SourceUnavailable)?;
        self.reference = Some(reference.clone());
        Ok(reference)
    }

    async fn text(
        &mut self,
        content: Arc<dyn ProjectionContentStore>,
        tenant_id: TenantId,
    ) -> Result<String, SubscriptionError> {
        if self.bytes.is_none() {
            let reference = self
                .reference
                .as_ref()
                .ok_or(SubscriptionError::CorruptRecord)?;
            self.bytes = Some(
                content
                    .read(tenant_id, reference)
                    .await
                    .map_err(|_| SubscriptionError::SourceUnavailable)?,
            );
        }
        String::from_utf8(self.bytes.clone().unwrap_or_default())
            .map_err(|_| SubscriptionError::CorruptRecord)
    }
}

struct PendingArtifact {
    kind: ArtifactKind,
    title: String,
    activity_id: Option<musteragent_identity::ActivityId>,
    run_id: Option<RunId>,
}

#[cfg(test)]
mod run_transition_tests {
    use super::accepts_nonterminal_progress;
    use musteragent_run::RunStatus;

    #[test]
    fn recovery_progress_cannot_reverse_cancellation_or_terminal_state() {
        for status in [
            RunStatus::Cancelling,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
            RunStatus::Interrupted,
        ] {
            assert!(!accepts_nonterminal_progress(status), "{status:?}");
        }
        for status in [RunStatus::Queued, RunStatus::Running, RunStatus::Waiting] {
            assert!(accepts_nonterminal_progress(status), "{status:?}");
        }
    }
}
