use std::collections::{BTreeMap, BTreeSet};

use musteragent_activity::{ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_approval::{InteractionEvent, InteractionEventResponse, InteractionKind};
use musteragent_artifact::{ArtifactEvent, ArtifactKind, ArtifactLifecycle};
use musteragent_content_ref::{ContentRef, SchemaRef, Sha256};
use musteragent_conversation::{ConversationEvent, MessageRole, PartialMessageReason};
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{
    ActivityId, Actor, ArtifactId, AttemptNumber, ContentId, ContextBranchId, ConversationId,
    ConversationSeq, CorrelationId, DelegationId, InterSessionMessageId, InteractionId,
    InvocationId, LedgerRecordId, MessageId, OutputId, ParticipantId, ProjectionSnapshotId, RunId,
    TenantId, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey,
};
use musteragent_ledger::{CanonicalLedgerRecord, DomainEvent, LedgerPayload};
use musteragent_run::{RunStatus, RunWaitReason};
use musteragent_runtime_contract::{
    ApplicationFuture, AttachmentRef, CursorRecoveryAuthority, ErrorCode, ErrorInfo, FrameContent,
    OutputChannel, RuntimeError,
};
use musteragent_runtime_model::ModelRouteRef;
use musteragent_tool::ToolEvent;
use thiserror::Error;

use crate::SubscriptionError;

/// Current projection snapshot identity. The namespace cutover is a schema
/// evolution even though the Protobuf field layout remains additive-compatible:
/// persisted legacy v1/v2 payloads are decoded and resealed into this v3 form.
pub const PROJECTION_SNAPSHOT_SCHEMA_VERSION: &str =
    "musteragent.conversation-projection-snapshot.v3";
/// Projection implementations whose encoded snapshots remain valid recovery
/// bases for the current reducer. This registry is deliberately explicit: a
/// reducer upgrade adds a generation here only after compatibility tests pass,
/// and removing one requires a governed storage migration first.
pub const SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS: &[u16] =
    &[18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7];
pub const CURRENT_PROJECTION_SNAPSHOT_GENERATION: u16 = 18;
/// Persistence identity for the current root product projection implementation.
///
/// This changes when reducer semantics change without changing the wire schema,
/// forcing retained Ledger records to be rebuilt into a fresh Snapshot.
pub const PROJECTION_SNAPSHOT_ROOT_KIND: &str = "conversation_projection.v18.root";
/// Persistence identity prefix for product projections on a Context branch.
pub const PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX: &str = "conversation_projection.v18.";
/// Immediate predecessor accepted only as a recovery base during v18 migration.
pub const LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND: &str = "conversation_projection.v17.root";
/// Immediate predecessor prefix accepted only as a recovery base during v18 migration.
pub const LEGACY_PROJECTION_SNAPSHOT_BRANCH_KIND_PREFIX: &str = "conversation_projection.v17.";

/// Returns the persistence identity for a supported reducer generation on
/// either the root Conversation or a Context branch.
#[must_use]
pub fn projection_snapshot_kind(
    generation: u16,
    branch_id: Option<ContextBranchId>,
) -> Option<String> {
    SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS
        .contains(&generation)
        .then(|| {
            branch_id.map_or_else(
                || format!("conversation_projection.v{generation}.root"),
                |branch_id| format!("conversation_projection.v{generation}.{branch_id}"),
            )
        })
}

/// Snapshot identities accepted as recovery bases, newest first.
#[must_use]
pub fn supported_projection_snapshot_kinds(branch_id: Option<ContextBranchId>) -> Vec<String> {
    SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS
        .iter()
        .filter_map(|generation| projection_snapshot_kind(*generation, branch_id))
        .collect()
}
/// Maximum number of records that may remain after a reusable public
/// Projection Snapshot. Data-plane authorization relies on this bound when it
/// proves that a ContentRef is reachable from the Snapshot plus its Tail.
pub const MAX_PUBLIC_PROJECTION_TAIL_RECORDS: u64 = 512;

/// Produces a fresh, self-contained cursor recovery authority at one captured
/// Ledger head.
///
/// This is deliberately separate from the bounded public-content
/// authorization read. Recovery correctness must not depend on how many
/// Domain Events a Run emitted since the last persisted projection snapshot.
pub trait CursorRecoveryAuthoritySource: Send + Sync {
    fn current_cursor_recovery_authority(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<CursorRecoveryAuthority, SubscriptionError>>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct GetConversationProjection {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub preferred_snapshot_id: Option<ProjectionSnapshotId>,
}

/// Bounded Product-history read. `before` is an exact boundary inside one
/// immutable authority Snapshot; it is never interpreted against a newer
/// Snapshot, so concurrent Tail records cannot move or duplicate an older
/// page.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GetConversationProjectionWindow {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub limit: usize,
    pub before: Option<ConversationProjectionWindowCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ConversationProjectionWindowCursor {
    pub snapshot_id: ProjectionSnapshotId,
    pub snapshot_checksum: Sha256,
    pub before_message_index: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationProjectionWindow {
    /// A bounded, self-contained Product projection. It is deliberately not a
    /// cursor-recovery authority even though it reuses the projection wire
    /// shape and integrity rules.
    pub projection: ConversationProjectionSnapshot,
    /// Exact full-Snapshot identity on which all pages in this history read
    /// are anchored.
    pub authority_snapshot_id: ProjectionSnapshotId,
    pub head_cursor: LedgerCursor,
    pub older_cursor: Option<ConversationProjectionWindowCursor>,
    pub has_older: bool,
}

pub const DEFAULT_PROJECTION_WINDOW_MESSAGES: usize = 40;
pub const MAX_PROJECTION_WINDOW_MESSAGES: usize = 100;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ProjectedMessageStatus {
    Streaming,
    Committed,
    Partial,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedOutputCheckpoint {
    pub output_id: OutputId,
    pub next_frame_index: u64,
    pub next_byte_offset: u64,
    pub completed: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedReasoningSummary {
    pub checkpoint: ProjectedOutputCheckpoint,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedStreamOutput {
    pub checkpoint: ProjectedOutputCheckpoint,
    pub message_id: Option<MessageId>,
    pub run_id: RunId,
    pub channel: OutputChannel,
    pub content_ref: ContentRef,
    pub first_conversation_seq: ConversationSeq,
    pub occurred_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedMessage {
    pub message_id: MessageId,
    pub participant_id: Option<ParticipantId>,
    pub role: MessageRole,
    pub content_ref: ContentRef,
    pub reasoning_summaries: Vec<ProjectedReasoningSummary>,
    pub status: ProjectedMessageStatus,
    pub source_run_id: Option<RunId>,
    pub committed_at: UtcTimestamp,
    pub attachment_refs: Vec<AttachmentRef>,
    /// Explicit Product-selected skills retained with an immutable user turn.
    pub requested_skill_refs: Vec<musteragent_identity::VersionedKey>,
    pub project_task_target: Option<musteragent_conversation::ProjectTaskTarget>,
    /// Optional product projection accepted only by governed legacy import.
    pub imported_projection_ref: Option<ContentRef>,
    /// Durable logical continuation point for assistant output. A Snapshot
    /// consumer resumes with the first frame whose index and byte offset equal
    /// this checkpoint; it never guesses either value from transport chunks.
    pub assistant_output: Option<ProjectedOutputCheckpoint>,
    pub partial_reason: Option<PartialMessageReason>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ProjectedToolStatus {
    Requested,
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
    ResultUnknown,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedToolItem {
    pub invocation_id: InvocationId,
    pub message_id: Option<MessageId>,
    pub run_id: RunId,
    pub tool_call_id: ToolCallId,
    pub tool_ref: String,
    pub status: ProjectedToolStatus,
    pub request_ref: ContentRef,
    pub result_ref: Option<ContentRef>,
    pub requested_conversation_seq: ConversationSeq,
    pub requested_at: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub finished_at: Option<UtcTimestamp>,
    /// Durable Artifacts produced or versioned by this exact invocation.
    /// Product clients consume this relation directly instead of decoding
    /// capability-specific Tool result payloads.
    pub output_artifact_ids: Vec<ArtifactId>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ProjectedContextCompactionStatus {
    Running,
    Succeeded,
    FailedContinued,
    FailedStopped,
    Indeterminate,
    Interrupted,
    RetryScheduled,
}

impl ProjectedContextCompactionStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedContextCompaction {
    pub invocation_id: InvocationId,
    pub run_id: RunId,
    pub loop_iteration: u32,
    pub status: ProjectedContextCompactionStatus,
    pub model_route_ref: ModelRouteRef,
    pub input_tokens_before: u64,
    pub input_tokens_after: Option<u64>,
    pub summarized_record_count: u64,
    pub retained_record_count: u64,
    pub source_seq_from: ConversationSeq,
    pub source_seq_to: ConversationSeq,
    pub started_conversation_seq: ConversationSeq,
    pub started_at: UtcTimestamp,
    pub finished_at: Option<UtcTimestamp>,
    pub error_code: Option<ErrorCode>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedInteraction {
    pub interaction_id: InteractionId,
    pub run_id: RunId,
    pub kind: InteractionKind,
    pub prompt_ref: ContentRef,
    pub response_schema: SchemaRef,
    pub requested_at: UtcTimestamp,
    pub expires_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedActivity {
    pub activity_id: ActivityId,
    pub kind: ActivityKind,
    pub status: ActivityStatus,
    pub requested_by_participant_id: Option<ParticipantId>,
    pub parent_activity_id: Option<ActivityId>,
    pub root_activity_id: ActivityId,
    pub policy_snapshot_ref: VersionedKey,
    pub started_at: UtcTimestamp,
    pub finished_at: Option<UtcTimestamp>,
    /// Stable parent-side identity and immutable objective reference for a
    /// delegated child. Product surfaces may resolve the objective inside the
    /// Child Activity view without inserting it into the parent transcript.
    pub delegation_origin: Option<ProjectedDelegationOrigin>,
    /// Provenance and immutable body for a target-side Coordination ingress.
    /// Product consumers render it as a distinct typed timeline item rather
    /// than impersonating the target Conversation's user participant.
    pub coordination_origin: Option<ProjectedCoordinationOrigin>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ProjectedDelegationOrigin {
    pub delegation_id: DelegationId,
    pub parent_invocation_id: InvocationId,
    /// Present on v9 projections. Optional only so a compacted v8 Snapshot can
    /// remain a recovery base during rolling upgrades.
    pub objective_ref: Option<ContentRef>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ProjectedCoordinationOrigin {
    pub mailbox_message_id: InterSessionMessageId,
    pub source_conversation_id: ConversationId,
    /// Present on v10 projections. Optional only so a compacted v9 Snapshot
    /// can remain a recovery base while retained Ledger records are rebuilt.
    pub payload_ref: Option<ContentRef>,
    /// Timestamp of the CoordinationContextProjected record. Optional only for
    /// the same rolling-upgrade recovery boundary as `payload_ref`.
    pub occurred_at: Option<UtcTimestamp>,
}

/// Safe failure data owned by the Run, independent of any assistant message.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedRunFailure {
    pub error: ErrorInfo,
    pub ledger_record_id: LedgerRecordId,
    pub correlation_id: CorrelationId,
    pub conversation_sequence: ConversationSeq,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedRun {
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub executor_participant_id: Option<ParticipantId>,
    /// General causal Run lineage. Standing Goal continuation and judge Runs
    /// use this without claiming Delegation ownership.
    pub parent_run_id: Option<RunId>,
    /// Delegation-specific identity. Its presence requires `parent_run_id`,
    /// but a causal parent does not require a Delegation.
    pub delegation_id: Option<DelegationId>,
    /// Exact immutable user turn reused by a user-requested terminal retry.
    pub retry_source_message_id: Option<MessageId>,
    /// Last physical Attempt observed on the failed source Run.
    pub retry_source_attempt_number: Option<AttemptNumber>,
    /// Present for failures projected from retained Ledger records. Older
    /// compacted snapshots may have a failed status without recoverable detail.
    pub terminal_failure: Option<ProjectedRunFailure>,
    /// Cause and ledger provenance for the current durable retry wait.
    pub retry_failure: Option<ProjectedRunFailure>,
    pub status: RunStatus,
    pub wait_reason: Option<RunWaitReason>,
    /// Currently executing or most recently executed physical Attempt.
    pub attempt_number: Option<AttemptNumber>,
    /// Replacement Attempt selected by a durable RetryScheduled transition.
    pub next_attempt_number: Option<AttemptNumber>,
    /// Runtime-owned retry deadline. Product clients render but never derive it.
    pub retry_not_before: Option<UtcTimestamp>,
    pub admitted_at: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub finished_at: Option<UtcTimestamp>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectedArtifact {
    pub artifact_id: ArtifactId,
    pub activity_id: Option<ActivityId>,
    pub created_by_run_id: Option<RunId>,
    /// Run that committed the current version. Unlike `created_by_run_id`, this
    /// advances when a persistent Artifact is edited in a later turn.
    pub updated_by_run_id: Option<RunId>,
    /// Most recent agent Run that owns presentation of this persistent
    /// Artifact in Product timelines. A user-authored version keeps this
    /// anchor while `updated_by_run_id` becomes `None` for truthful audit.
    pub presentation_run_id: Option<RunId>,
    pub kind: ArtifactKind,
    pub lifecycle: ArtifactLifecycle,
    pub current_version: VersionNumber,
    pub title: String,
    pub current_content_ref: ContentRef,
    pub current_dependency_refs: Vec<ContentRef>,
    pub updated_at: UtcTimestamp,
}

/// Rebuildable product read model. It is never an execution source of truth.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConversationProjectionSnapshot {
    pub schema_version: String,
    pub snapshot_id: ProjectionSnapshotId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: musteragent_identity::HomeEpoch,
    pub through_conversation_seq: musteragent_identity::LedgerOffset,
    pub messages: Vec<ProjectedMessage>,
    pub tool_timeline: Vec<ProjectedToolItem>,
    pub context_compactions: Vec<ProjectedContextCompaction>,
    pub pending_interactions: Vec<ProjectedInteraction>,
    /// Complete Activity DAG retained across terminal Run lifecycle.
    pub activities: Vec<ProjectedActivity>,
    /// Complete Run history retained so team/subagent lineage is rebuildable.
    pub runs: Vec<ProjectedRun>,
    /// Compatibility/index view derived exactly from non-terminal `runs`.
    pub active_runs: Vec<ProjectedRun>,
    pub artifacts: Vec<ProjectedArtifact>,
    /// Materialized public streams that retain per-Output continuation
    /// identity. Run-level Assistant Messages hold aggregate visible text;
    /// each physical Assistant Output remains here exactly once.
    pub stream_outputs: Vec<ProjectedStreamOutput>,
    pub content_manifest: Vec<ContentRef>,
    pub checksum: Sha256,
    pub created_at: UtcTimestamp,
}

impl ConversationProjectionSnapshot {
    #[must_use]
    pub const fn cursor(&self) -> LedgerCursor {
        LedgerCursor {
            conversation_id: self.conversation_id,
            home_epoch: self.home_epoch,
            after_conversation_seq: self.through_conversation_seq,
        }
    }
}

/// Derives one deterministic Product window from an immutable full Snapshot.
///
/// The closure includes every Run/Activity needed by selected timeline rows,
/// plus all active controls and pending interactions. Heavy historical Tool,
/// Output and Artifact objects outside that closure are omitted. The exact
/// manifest is rebuilt from the bounded result, so Content authorization and
/// immutable identity validation remain unchanged.
pub fn projection_window_from_snapshot(
    snapshot: &ConversationProjectionSnapshot,
    before: Option<&ConversationProjectionWindowCursor>,
    limit: usize,
    codec: &dyn ConversationProjectionSnapshotCodec,
) -> Result<ConversationProjectionWindow, ProjectionWindowError> {
    if limit == 0 || limit > MAX_PROJECTION_WINDOW_MESSAGES {
        return Err(ProjectionWindowError::InvalidLimit);
    }
    let end = if let Some(cursor) = before {
        if cursor.snapshot_id != snapshot.snapshot_id
            || cursor.snapshot_checksum != snapshot.checksum
        {
            return Err(ProjectionWindowError::CursorAuthorityMismatch);
        }
        usize::try_from(cursor.before_message_index)
            .map_err(|_| ProjectionWindowError::InvalidCursor)?
    } else {
        snapshot.messages.len()
    };
    if end > snapshot.messages.len() {
        return Err(ProjectionWindowError::InvalidCursor);
    }
    let start = end.saturating_sub(limit);
    let selected_messages = snapshot.messages[start..end].to_vec();
    let selected_message_ids = selected_messages
        .iter()
        .map(|message| message.message_id)
        .collect::<BTreeSet<_>>();
    let mut run_ids = selected_messages
        .iter()
        .filter_map(|message| message.source_run_id)
        .collect::<BTreeSet<_>>();
    run_ids.extend(snapshot.active_runs.iter().map(|run| run.run_id));
    run_ids.extend(
        snapshot
            .pending_interactions
            .iter()
            .map(|interaction| interaction.run_id),
    );

    // Preserve causal parents required to render team/subagent lineage.
    loop {
        let parents = snapshot
            .runs
            .iter()
            .filter(|run| run_ids.contains(&run.run_id))
            .filter_map(|run| run.parent_run_id)
            .filter(|parent| !run_ids.contains(parent))
            .collect::<Vec<_>>();
        if parents.is_empty() {
            break;
        }
        run_ids.extend(parents);
    }
    let runs = snapshot
        .runs
        .iter()
        .filter(|run| run_ids.contains(&run.run_id))
        .cloned()
        .collect::<Vec<_>>();
    let mut activity_ids = runs
        .iter()
        .map(|run| run.activity_id)
        .collect::<BTreeSet<_>>();
    loop {
        let parents = snapshot
            .activities
            .iter()
            .filter(|activity| activity_ids.contains(&activity.activity_id))
            .filter_map(|activity| activity.parent_activity_id)
            .filter(|parent| !activity_ids.contains(parent))
            .collect::<Vec<_>>();
        if parents.is_empty() {
            break;
        }
        activity_ids.extend(parents);
    }

    let mut projection = ConversationProjectionSnapshot {
        schema_version: snapshot.schema_version.clone(),
        snapshot_id: snapshot.snapshot_id,
        tenant_id: snapshot.tenant_id,
        conversation_id: snapshot.conversation_id,
        home_epoch: snapshot.home_epoch,
        through_conversation_seq: snapshot.through_conversation_seq,
        messages: selected_messages,
        tool_timeline: snapshot
            .tool_timeline
            .iter()
            .filter(|tool| {
                run_ids.contains(&tool.run_id)
                    || tool
                        .message_id
                        .is_some_and(|message_id| selected_message_ids.contains(&message_id))
            })
            .cloned()
            .collect(),
        context_compactions: snapshot
            .context_compactions
            .iter()
            .filter(|item| run_ids.contains(&item.run_id))
            .cloned()
            .collect(),
        pending_interactions: snapshot.pending_interactions.clone(),
        activities: snapshot
            .activities
            .iter()
            .filter(|activity| activity_ids.contains(&activity.activity_id))
            .cloned()
            .collect(),
        runs,
        active_runs: snapshot.active_runs.clone(),
        artifacts: snapshot
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact
                    .presentation_run_id
                    .or(artifact.updated_by_run_id)
                    .or(artifact.created_by_run_id)
                    .is_some_and(|run_id| run_ids.contains(&run_id))
            })
            .cloned()
            .collect(),
        stream_outputs: snapshot
            .stream_outputs
            .iter()
            .filter(|output| run_ids.contains(&output.run_id))
            .cloned()
            .collect(),
        content_manifest: Vec::new(),
        checksum: Sha256::digest([]),
        created_at: snapshot.created_at,
    };
    projection.content_manifest = canonical_content_manifest(&projection)
        .map_err(|_| ProjectionWindowError::ContentIdentityConflict)?;
    projection.checksum = codec.checksum(&projection);
    codec
        .encode(&projection)
        .map_err(|_| ProjectionWindowError::InvalidProjection)?;
    let has_older = start > 0;
    Ok(ConversationProjectionWindow {
        projection,
        authority_snapshot_id: snapshot.snapshot_id,
        head_cursor: snapshot.cursor(),
        older_cursor: has_older.then_some(ConversationProjectionWindowCursor {
            snapshot_id: snapshot.snapshot_id,
            snapshot_checksum: snapshot.checksum,
            before_message_index: start as u64,
        }),
        has_older,
    })
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ProjectionWindowError {
    #[error("projection window limit is invalid")]
    InvalidLimit,
    #[error("projection window cursor is invalid")]
    InvalidCursor,
    #[error("projection window cursor belongs to another authority Snapshot")]
    CursorAuthorityMismatch,
    #[error("projection window has conflicting ContentRef identity")]
    ContentIdentityConflict,
    #[error("projection window contract is invalid")]
    InvalidProjection,
}

/// Recomputes the exact, canonical set of ContentRefs reachable from public
/// projection fields. The manifest is an integrity index, never an independent
/// authorization source. Reusing one ContentId with different immutable
/// identity is corruption and must not be collapsed by map insertion.
pub fn canonical_content_manifest(
    snapshot: &ConversationProjectionSnapshot,
) -> Result<Vec<ContentRef>, ProjectionManifestError> {
    let mut values = BTreeMap::<ContentId, ContentRef>::new();
    let mut insert = |reference: &ContentRef| -> Result<(), ProjectionManifestError> {
        if let Some(existing) = values.get(&reference.content_id) {
            if existing != reference {
                return Err(ProjectionManifestError::ContentIdentityConflict);
            }
        } else {
            values.insert(reference.content_id, reference.clone());
        }
        Ok(())
    };
    for message in &snapshot.messages {
        insert(&message.content_ref)?;
        for attachment in &message.attachment_refs {
            insert(&attachment.content_ref)?;
        }
        if let Some(reference) = &message.imported_projection_ref {
            insert(reference)?;
        }
        for reasoning in &message.reasoning_summaries {
            insert(&reasoning.content_ref)?;
        }
    }
    for tool in &snapshot.tool_timeline {
        insert(&tool.request_ref)?;
        if let Some(reference) = &tool.result_ref {
            insert(reference)?;
        }
    }
    for interaction in &snapshot.pending_interactions {
        insert(&interaction.prompt_ref)?;
    }
    for activity in &snapshot.activities {
        if let Some(reference) = activity
            .delegation_origin
            .as_ref()
            .and_then(|origin| origin.objective_ref.as_ref())
        {
            insert(reference)?;
        }
        if let Some(reference) = activity
            .coordination_origin
            .as_ref()
            .and_then(|origin| origin.payload_ref.as_ref())
        {
            insert(reference)?;
        }
    }
    for artifact in &snapshot.artifacts {
        insert(&artifact.current_content_ref)?;
        for dependency in &artifact.current_dependency_refs {
            insert(dependency)?;
        }
    }
    for output in &snapshot.stream_outputs {
        insert(&output.content_ref)?;
    }
    Ok(values.into_values().collect())
}

/// Proves that one immutable public Ledger record directly exposes a
/// `ContentRef` that the Product projector may hydrate while applying that
/// record.
///
/// This is deliberately narrower than "every ContentRef anywhere in the
/// payload". Model responses, context snapshots, discovery instructions and
/// other Runtime-private references never become Content authorities merely
/// because their records share a Conversation sequence. Keeping this closed
/// union beside the Product projection makes live-record authorization follow
/// exactly the same visibility boundary as Snapshot manifests.
#[must_use]
pub fn ledger_record_reaches_product_content(
    record: &CanonicalLedgerRecord,
    reference: &ContentRef,
) -> bool {
    match &record.payload {
        LedgerPayload::OutputFrame(frame) => {
            matches!(&frame.content, FrameContent::ContentRef(value) if value == reference)
        }
        LedgerPayload::DomainEvent(DomainEvent::Conversation(event)) => match event {
            ConversationEvent::MessageAccepted {
                content_ref,
                attachment_refs,
                ..
            }
            | ConversationEvent::MessageImported {
                content_ref,
                attachment_refs,
                ..
            } => {
                content_ref == reference
                    || attachment_refs
                        .iter()
                        .any(|attachment| &attachment.content_ref == reference)
                    || matches!(
                        event,
                        ConversationEvent::MessageImported {
                            projection_ref: Some(value),
                            ..
                        } if value == reference
                    )
            }
            ConversationEvent::MessageCommitted { content_ref, .. }
            | ConversationEvent::MessagePartiallyCommitted { content_ref, .. } => {
                content_ref == reference
            }
            ConversationEvent::WorkspaceAssigned { .. }
            | ConversationEvent::Created { .. }
            | ConversationEvent::Archived
            | ConversationEvent::TitleChanged { .. }
            | ConversationEvent::ParticipantJoined { .. }
            | ConversationEvent::ParticipantLeft
            | ConversationEvent::MessageRetracted { .. } => false,
        },
        LedgerPayload::DomainEvent(DomainEvent::Activity(event)) => match event {
            ActivityEvent::DelegationContextProjected { objective_ref, .. } => {
                objective_ref == reference
            }
            ActivityEvent::CoordinationContextProjected { payload_ref, .. } => {
                payload_ref == reference
            }
            ActivityEvent::Started { .. }
            | ActivityEvent::MaintenanceContextProjected { .. }
            | ActivityEvent::StandingGoalContextProjected { .. }
            | ActivityEvent::StandingGoalJudgeContextProjected { .. }
            | ActivityEvent::Completed { .. }
            | ActivityEvent::Failed { .. }
            | ActivityEvent::Cancelled { .. } => false,
        },
        LedgerPayload::DomainEvent(DomainEvent::Tool(event)) => match event {
            ToolEvent::InvocationRequested { request_ref, .. } => request_ref == reference,
            ToolEvent::InvocationSucceeded { result_ref, .. } => result_ref == reference,
            ToolEvent::InvocationFailed {
                diagnostic_result_ref,
                ..
            } => diagnostic_result_ref.as_ref() == Some(reference),
            ToolEvent::SandboxRouted { .. }
            | ToolEvent::InvocationWaitingInteraction { .. }
            | ToolEvent::InvocationStarted { .. }
            | ToolEvent::InvocationIndeterminate { .. }
            | ToolEvent::InvocationCancelled { .. } => false,
        },
        LedgerPayload::DomainEvent(DomainEvent::Interaction(event)) => match event {
            InteractionEvent::Requested { prompt_ref, .. } => prompt_ref == reference,
            InteractionEvent::Responded {
                response: InteractionEventResponse::Content(value),
                ..
            } => value == reference,
            InteractionEvent::Responded {
                response:
                    InteractionEventResponse::Approval(_) | InteractionEventResponse::SecureAccepted,
                ..
            }
            | InteractionEvent::Expired { .. }
            | InteractionEvent::Cancelled { .. } => false,
        },
        LedgerPayload::DomainEvent(DomainEvent::Artifact(event)) => match event {
            ArtifactEvent::VersionCommitted {
                content_ref,
                dependency_refs,
                ..
            } => {
                content_ref == reference
                    || dependency_refs
                        .iter()
                        .any(|dependency| dependency == reference)
            }
            ArtifactEvent::NoteSegmentCommitted { content_ref, .. } => content_ref == reference,
            ArtifactEvent::Created { .. }
            | ArtifactEvent::NoteAuthoringStarted { .. }
            | ArtifactEvent::NoteSegmentCommittedV1 { .. }
            | ArtifactEvent::NoteVersionFinalized { .. }
            | ArtifactEvent::Failed { .. }
            | ArtifactEvent::EditProposed { .. }
            | ArtifactEvent::EditApplied { .. }
            | ArtifactEvent::EditRejected { .. }
            | ArtifactEvent::EditExpired { .. } => false,
        },
        LedgerPayload::DomainEvent(
            DomainEvent::Run(_)
            | DomainEvent::Model(_)
            | DomainEvent::Context(_)
            | DomainEvent::Discovery(_),
        ) => false,
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ProjectionManifestError {
    #[error("one ContentId has conflicting immutable ContentRef identity")]
    ContentIdentityConflict,
}

/// Deterministic storage codec for rebuildable product snapshots.
///
/// The Port lives with the projection domain so PostgreSQL never depends on
/// protobuf-generated transport structs.
pub trait ConversationProjectionSnapshotCodec: Send + Sync {
    /// Hashes the canonical snapshot payload with the checksum field omitted.
    fn checksum(&self, snapshot: &ConversationProjectionSnapshot) -> Sha256;

    fn encode(
        &self,
        snapshot: &ConversationProjectionSnapshot,
    ) -> Result<Vec<u8>, ProjectionSnapshotCodecError>;

    fn decode(
        &self,
        bytes: &[u8],
    ) -> Result<ConversationProjectionSnapshot, ProjectionSnapshotCodecError>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ProjectionSnapshotCodecError {
    #[error("projection snapshot bytes are malformed")]
    Malformed,
    #[error("projection snapshot schema is unsupported")]
    UnsupportedSchema,
    #[error("projection snapshot contract is invalid")]
    InvalidContract,
}

pub trait ConversationProjectionApplication: Send + Sync {
    fn get_product_read_projection_window(
        &self,
        query: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>>;

    /// Selects the Product bootstrap authority.
    ///
    /// Ordinary reads receive one self-contained Snapshot at a captured Ledger
    /// head. An explicit preferred Snapshot is reserved for cursor recovery,
    /// where the consumer already owns that exact historical authority.
    fn get_product_read_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            if query.preferred_snapshot_id.is_some() {
                self.get_conversation_projection(query).await
            } else {
                self.get_head_projection(query).await
            }
        })
    }

    /// Loads a reusable persisted Snapshot, or the explicitly preferred exact
    /// Snapshot. Callers that expose an ordinary Product bootstrap must use
    /// [`Self::get_product_read_projection`] instead.
    fn get_conversation_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>>;

    /// Reduces the retained Snapshot and complete Tail to one captured Ledger
    /// head. Unlike the authorization-specific bounded read, this Product
    /// bootstrap remains available when a projector is more than one bounded
    /// authorization window behind.
    fn get_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>>;

    /// Reduces the latest persisted Snapshot plus its complete, bounded Tail
    /// to the captured Ledger head. Content authorization uses this operation
    /// so reachability is decided from current projection state rather than
    /// from historical occurrences.
    fn get_bounded_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>>;
}

#[cfg(test)]
mod snapshot_kind_compatibility_tests {
    use super::{
        CURRENT_PROJECTION_SNAPSHOT_GENERATION, LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND,
        PROJECTION_SNAPSHOT_ROOT_KIND, SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS,
        projection_snapshot_kind, supported_projection_snapshot_kinds,
    };
    use musteragent_identity::ContextBranchId;

    #[test]
    fn compatibility_registry_is_newest_first_and_matches_named_constants() {
        assert_eq!(
            SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS.first().copied(),
            Some(CURRENT_PROJECTION_SNAPSHOT_GENERATION)
        );
        assert!(
            SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS
                .windows(2)
                .all(|window| window[0] > window[1])
        );
        assert_eq!(
            projection_snapshot_kind(CURRENT_PROJECTION_SNAPSHOT_GENERATION, None).as_deref(),
            Some(PROJECTION_SNAPSHOT_ROOT_KIND)
        );
        assert_eq!(
            projection_snapshot_kind(CURRENT_PROJECTION_SNAPSHOT_GENERATION - 1, None).as_deref(),
            Some(LEGACY_PROJECTION_SNAPSHOT_ROOT_KIND)
        );
        assert_eq!(projection_snapshot_kind(6, None), None);
    }

    #[test]
    fn every_supported_generation_has_a_branch_scoped_identity() {
        let branch_id = ContextBranchId::generate();
        let kinds = supported_projection_snapshot_kinds(Some(branch_id));
        assert_eq!(kinds.len(), SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS.len());
        for (kind, generation) in kinds.iter().zip(SUPPORTED_PROJECTION_SNAPSHOT_GENERATIONS) {
            assert_eq!(
                kind,
                &format!("conversation_projection.v{generation}.{branch_id}")
            );
        }
    }
}
