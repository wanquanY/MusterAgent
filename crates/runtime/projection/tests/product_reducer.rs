#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use musteragent_activity::{ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_artifact::{ArtifactEvent, ArtifactKind};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{ContextCompactionOutcome, ContextCompactionResolution, ContextEvent};
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_event_envelope::{LedgerMetadata, LedgerRecordKind, Producer, RuntimeHome};
use musteragent_identity::{
    ActivityId, ArtifactId, AttemptId, AttemptNumber, CapabilityProviderId, ContentId,
    ContextSnapshotId, ConversationId, ConversationSeq, CorrelationId, DelegationId, DottedKey,
    FrameId, HomeEpoch, IdempotencyKey, InterSessionMessageId, InteractionId, InvocationId,
    LedgerOffset, LedgerRecordId, MessageId, OutputId, ParticipantId, ProjectionSnapshotId, RunId,
    RuntimeSnapshotId, TenantId, ToolCallId, UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{CanonicalLedgerRecord, DomainEvent, LedgerPayload};
use musteragent_policy::Effect;
use musteragent_projection::{
    ConversationProjectionSnapshot, PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProductProjectionReducer,
    ProjectedActivity, ProjectedMessage, ProjectedMessageStatus, ProjectedRun, ProjectedToolStatus,
    ProjectionContentStore, SubscriptionError,
};
use musteragent_run::{RunEvent, RunStatus, RunWaitReason};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, DisplayText, ErrorCode, ErrorInfo, FrameContent, InlineFrame,
    OpaqueExternalRef, OutputChannel, OutputFrame, RetryDisposition, SafeDisplayText,
};
use musteragent_runtime_model::ModelRouteRef;
use musteragent_tool::{ToolEvent, ToolKey};

#[path = "product_reducer/dialogue_context.rs"]
mod dialogue_context;
#[path = "product_reducer/private_activity_context.rs"]
mod private_activity_context;
#[path = "product_reducer/terminal_failure.rs"]
mod terminal_failure;

struct NoIoContent;

impl ProjectionContentStore for NoIoContent {
    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, SubscriptionError>> {
        Box::pin(async { Err(SubscriptionError::SourceUnavailable) })
    }

    fn put(
        &self,
        _tenant_id: TenantId,
        _media_type: MediaType,
        _bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, SubscriptionError>> {
        Box::pin(async { Err(SubscriptionError::SourceUnavailable) })
    }
}

#[tokio::test]
async fn run_retry_schedule_is_preserved_in_the_product_snapshot() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let retry_source_message_id = MessageId::generate();
    let attempt_id = AttemptId::generate();
    let attempt_one = AttemptNumber::new(1).expect("attempt one");
    let attempt_two = AttemptNumber::new(2).expect("attempt two");
    let not_before = UtcTimestamp::from_unix_millis(8_000);
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    let mut admitted = record(
        tenant_id,
        conversation_id,
        home_epoch,
        1,
        Some(run_id),
        None,
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            input_message_id: Some(retry_source_message_id),
            retry_source_message_id: Some(retry_source_message_id),
            retry_source_attempt_number: Some(attempt_one),
        }),
    );
    admitted.metadata.activity_id = Some(activity_id);
    reducer
        .apply(admitted, Arc::clone(&content))
        .await
        .expect("run admitted");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                2,
                Some(run_id),
                None,
                DomainEvent::Run(RunEvent::Started {
                    attempt_id,
                    attempt_number: attempt_one,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("run started");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                3,
                Some(run_id),
                None,
                DomainEvent::Run(RunEvent::RetryScheduled {
                    failed_attempt_id: attempt_id,
                    next_attempt_number: attempt_two,
                    not_before,
                    error: ErrorInfo {
                        code: ErrorCode::DependencyTimeout,
                        category: ErrorCode::DependencyTimeout.category(),
                        safe_message: SafeDisplayText::new("provider temporarily unavailable")
                            .expect("safe message"),
                        retryable: true,
                        dependency_ref: None,
                        diagnostic_ref: None,
                    },
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("retry scheduled");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                4,
                Some(run_id),
                None,
                DomainEvent::Run(RunEvent::Waiting {
                    reason: RunWaitReason::RetryBackoff,
                    resume_not_before: Some(not_before),
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("retry wait");

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(4),
            UtcTimestamp::from_unix_millis(4_000),
        )
        .await
        .expect("snapshot");
    let run = snapshot
        .runs
        .iter()
        .find(|run| run.run_id == run_id)
        .expect("run");
    assert_eq!(run.status, RunStatus::Waiting);
    assert_eq!(run.wait_reason, Some(RunWaitReason::RetryBackoff));
    assert_eq!(run.attempt_number, Some(attempt_one));
    assert_eq!(run.next_attempt_number, Some(attempt_two));
    assert_eq!(run.retry_not_before, Some(not_before));
    let failure = run.retry_failure.as_ref().expect("durable retry cause");
    assert_eq!(failure.error.code, ErrorCode::DependencyTimeout);
    assert_eq!(failure.conversation_sequence.get(), 3);
    assert!(run.terminal_failure.is_none());
    assert_eq!(run.input_message_id, Some(retry_source_message_id));
    assert_eq!(run.retry_source_message_id, Some(retry_source_message_id));
    assert_eq!(run.retry_source_attempt_number, Some(attempt_one));
}

#[tokio::test]
async fn context_compaction_lifecycle_is_durable_and_ordered_in_product_snapshot() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let invocation_id = InvocationId::generate();
    let context_snapshot_id = ContextSnapshotId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                10,
                Some(run_id),
                None,
                DomainEvent::Context(ContextEvent::CompactionStarted {
                    invocation_id,
                    loop_iteration: 3,
                    source_seq_from: ConversationSeq::new(1).expect("source from"),
                    source_seq_to: ConversationSeq::new(9).expect("source to"),
                    model_route_ref: ModelRouteRef::from_key(
                        VersionedKey::new("openai.compaction@1").expect("route"),
                    ),
                    input_tokens_before: 12_800,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("compaction start");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                11,
                Some(run_id),
                None,
                DomainEvent::Context(ContextEvent::CompactionResolved {
                    resolution: Box::new(ContextCompactionResolution {
                        invocation_id,
                        outcome: ContextCompactionOutcome::Succeeded,
                        loop_iteration: 3,
                        context_snapshot_id: Some(context_snapshot_id),
                        dialogue_checkpoint_id: None,
                        input_tokens_before: 12_800,
                        input_tokens_after: Some(4_200),
                        summarized_record_count: 17,
                        retained_record_count: 6,
                        error: None,
                    }),
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("compaction resolution");

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(11),
            UtcTimestamp::from_unix_millis(12_000),
        )
        .await
        .expect("snapshot");
    let compaction = snapshot
        .context_compactions
        .first()
        .expect("context compaction");
    assert_eq!(compaction.invocation_id, invocation_id);
    assert_eq!(compaction.run_id, run_id);
    assert_eq!(compaction.loop_iteration, 3);
    assert_eq!(
        compaction.status,
        musteragent_projection::ProjectedContextCompactionStatus::Succeeded
    );
    assert_eq!(compaction.input_tokens_before, 12_800);
    assert_eq!(compaction.input_tokens_after, Some(4_200));
    assert_eq!(compaction.summarized_record_count, 17);
    assert_eq!(compaction.retained_record_count, 6);
    assert_eq!(compaction.started_conversation_seq.get(), 10);
    assert_eq!(compaction.started_at, UtcTimestamp::from_unix_millis(10));
    assert_eq!(
        compaction.finished_at,
        Some(UtcTimestamp::from_unix_millis(11))
    );
}

#[tokio::test]
async fn historical_retraction_tail_removes_message_less_delegated_descendants() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let user_message_id = MessageId::generate();
    let root_run_id = RunId::generate();
    let successful_child_run_id = RunId::generate();
    let failed_child_run_id = RunId::generate();
    let retained_run_id = RunId::generate();
    let root_activity_id = ActivityId::generate();
    let successful_child_activity_id = ActivityId::generate();
    let child_activity_id = ActivityId::generate();
    let retained_activity_id = ActivityId::generate();
    let root_at = UtcTimestamp::from_unix_millis(1_000);
    let child_at = UtcTimestamp::from_unix_millis(2_000);
    let retained_at = UtcTimestamp::from_unix_millis(3_000);
    let user_content = content_ref(b"retracted user turn");
    let assistant_content = content_ref(b"retracted child output");
    let retained_content = content_ref(b"retained prior turn");
    let assistant_message_id = MessageId::generate();
    let retained_message_id = MessageId::generate();
    let snapshot = ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id,
        conversation_id,
        home_epoch,
        through_conversation_seq: LedgerOffset::new(9),
        messages: vec![
            projected_test_message(user_message_id, root_run_id, user_content.clone(), root_at),
            projected_test_message(
                assistant_message_id,
                successful_child_run_id,
                assistant_content.clone(),
                child_at,
            ),
            projected_test_message(
                retained_message_id,
                retained_run_id,
                retained_content.clone(),
                retained_at,
            ),
        ],
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: vec![
            projected_test_activity(
                root_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Cancelled,
                None,
                root_activity_id,
                root_at,
            ),
            projected_test_activity(
                successful_child_activity_id,
                ActivityKind::Delegation,
                ActivityStatus::Completed,
                Some(root_activity_id),
                root_activity_id,
                child_at,
            ),
            projected_test_activity(
                child_activity_id,
                ActivityKind::Delegation,
                ActivityStatus::Failed,
                Some(root_activity_id),
                root_activity_id,
                child_at,
            ),
            projected_test_activity(
                retained_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Completed,
                None,
                retained_activity_id,
                retained_at,
            ),
        ],
        runs: vec![
            projected_test_run(
                root_run_id,
                root_activity_id,
                None,
                None,
                RunStatus::Cancelled,
                root_at,
            ),
            projected_test_run(
                successful_child_run_id,
                successful_child_activity_id,
                Some(root_run_id),
                Some(DelegationId::generate()),
                RunStatus::Completed,
                child_at,
            ),
            projected_test_run(
                failed_child_run_id,
                child_activity_id,
                Some(root_run_id),
                Some(DelegationId::generate()),
                RunStatus::Failed,
                child_at,
            ),
            projected_test_run(
                retained_run_id,
                retained_activity_id,
                None,
                None,
                RunStatus::Completed,
                retained_at,
            ),
        ],
        active_runs: Vec::new(),
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest: vec![user_content, assistant_content, retained_content.clone()],
        checksum: Sha256::digest(b"valid checkpoint before historical tail"),
        created_at: retained_at,
    };
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        Some(snapshot),
        BTreeMap::new(),
    )
    .expect("checkpoint reducer");

    for (sequence, message_id, source_run_id) in [
        (10, user_message_id, root_run_id),
        // Existing Device ledgers can contain repeated child retractions from
        // a reused Assistant message. Once the root graph is removed, those
        // facts must remain idempotent instead of reviving or corrupting it.
        (11, assistant_message_id, successful_child_run_id),
        (12, assistant_message_id, successful_child_run_id),
    ] {
        reducer
            .apply(
                record(
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    sequence,
                    None,
                    Some(ParticipantId::generate()),
                    DomainEvent::Conversation(ConversationEvent::MessageRetracted {
                        message_id,
                        reason: SafeDisplayText::new("user requested rewind").expect("reason"),
                        source_run_id: Some(source_run_id),
                    }),
                ),
                Arc::clone(&content),
            )
            .await
            .expect("historical retraction fact");
    }

    let projection = reducer
        .finish(
            content,
            LedgerOffset::new(12),
            UtcTimestamp::from_unix_millis(4_000),
        )
        .await
        .expect("historical tail must rebuild a valid projection");

    assert_eq!(projection.messages.len(), 1);
    assert_eq!(projection.messages[0].message_id, retained_message_id);
    assert_eq!(projection.runs.len(), 1);
    assert_eq!(projection.runs[0].run_id, retained_run_id);
    assert_eq!(projection.activities.len(), 1);
    assert_eq!(projection.activities[0].activity_id, retained_activity_id);
    assert_eq!(projection.content_manifest, vec![retained_content]);
}

#[tokio::test]
async fn user_artifact_versions_preserve_the_latest_agent_presentation_anchor() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let artifact_id = musteragent_identity::ArtifactId::generate();
    let first = content_ref(b"agent version");
    let second = content_ref(b"user version");
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    let mut created = record(
        tenant_id,
        conversation_id,
        home_epoch,
        1,
        Some(run_id),
        Some(ParticipantId::generate()),
        DomainEvent::Artifact(ArtifactEvent::Created {
            artifact_id,
            kind: ArtifactKind::Note,
            title: DisplayText::new("Persistent note").expect("title"),
            source_invocation_id: Some(InvocationId::generate()),
        }),
    );
    created.metadata.activity_id = Some(ActivityId::generate());
    reducer
        .apply(created, Arc::clone(&content))
        .await
        .expect("Artifact created");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                2,
                Some(run_id),
                Some(ParticipantId::generate()),
                DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                    artifact_id,
                    version: musteragent_identity::VersionNumber::new(1).expect("version"),
                    content_ref: first,
                    dependency_refs: Vec::new(),
                    content_hash: Sha256::digest(b"agent version"),
                    source_invocation_id: None,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("agent version");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                3,
                None,
                Some(ParticipantId::generate()),
                DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                    artifact_id,
                    version: musteragent_identity::VersionNumber::new(2).expect("version"),
                    content_ref: second,
                    dependency_refs: Vec::new(),
                    content_hash: Sha256::digest(b"user version"),
                    source_invocation_id: None,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("user version");

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(3),
            UtcTimestamp::from_unix_millis(3),
        )
        .await
        .expect("snapshot");
    let artifact = snapshot.artifacts.first().expect("artifact");
    assert_eq!(artifact.current_version.get(), 2);
    assert_eq!(artifact.created_by_run_id, Some(run_id));
    assert_eq!(artifact.updated_by_run_id, None);
    assert_eq!(artifact.presentation_run_id, Some(run_id));
}

#[derive(Default)]
struct MemoryContent {
    values: Mutex<BTreeMap<ContentId, Vec<u8>>>,
}

impl ProjectionContentStore for MemoryContent {
    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, SubscriptionError>> {
        Box::pin(async move {
            self.values
                .lock()
                .expect("content lock")
                .get(&content.content_id)
                .cloned()
                .ok_or(SubscriptionError::SourceUnavailable)
        })
    }

    fn put(
        &self,
        _tenant_id: TenantId,
        media_type: MediaType,
        bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, SubscriptionError>> {
        Box::pin(async move {
            let content_id = ContentId::generate();
            let reference = ContentRef {
                content_id,
                media_type,
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            };
            self.values
                .lock()
                .expect("content lock")
                .insert(content_id, bytes);
            Ok(reference)
        })
    }
}

#[tokio::test]
async fn coordination_ingress_is_projected_without_impersonating_the_user() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let source_conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let participant_id = ParticipantId::generate();
    let activity_id = ActivityId::generate();
    let mailbox_message_id = InterSessionMessageId::generate();
    let payload_ref = content_ref(b"private cross-session payload");
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    for (sequence, event) in [
        (
            1,
            ActivityEvent::CoordinationContextProjected {
                mailbox_message_id,
                source_conversation_id,
                payload_ref: payload_ref.clone(),
            },
        ),
        (
            2,
            ActivityEvent::Started {
                kind: ActivityKind::Coordination,
                root_activity_id: activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: VersionedKey::new("coordination-policy@1").expect("policy"),
            },
        ),
    ] {
        let mut record = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            None,
            Some(participant_id),
            DomainEvent::Activity(event),
        );
        record.metadata.activity_id = Some(activity_id);
        reducer
            .apply(record, Arc::clone(&content))
            .await
            .expect("coordination record");
        reducer
            .advance_through(LedgerOffset::new(sequence))
            .expect("coordination cursor");
    }

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(2),
            UtcTimestamp::from_unix_millis(20_000),
        )
        .await
        .expect("snapshot");
    let activity = snapshot.activities.first().expect("coordination activity");
    assert_eq!(activity.kind, ActivityKind::Coordination);
    assert_eq!(
        activity.coordination_origin.as_ref(),
        Some(&musteragent_projection::ProjectedCoordinationOrigin {
            mailbox_message_id,
            source_conversation_id,
            payload_ref: Some(payload_ref.clone()),
            occurred_at: Some(UtcTimestamp::from_unix_millis(1)),
        })
    );
    assert!(snapshot.content_manifest.contains(&payload_ref));
}

#[tokio::test]
async fn canonical_message_and_tool_records_reduce_to_one_exact_product_snapshot() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let participant_id = ParticipantId::generate();
    let run_id = RunId::generate();
    let invocation_id = InvocationId::generate();
    let message_content = content_ref(b"hello");
    let request_ref = content_ref(br#"{"value":"hello"}"#);
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                1,
                None,
                Some(participant_id),
                DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                    project_task_target: None,
                    message_id: musteragent_identity::MessageId::generate(),
                    role: MessageRole::User,
                    content_ref: message_content.clone(),
                    attachment_refs: Vec::new(),
                    requested_skill_refs: vec![
                        VersionedKey::new("file-analysis@2").expect("skill ref"),
                    ],
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("message record");
    reducer
        .advance_through(LedgerOffset::new(1))
        .expect("message cursor");
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                2,
                Some(run_id),
                None,
                DomainEvent::Tool(ToolEvent::InvocationRequested {
                    invocation_id,
                    tool_call_id: ToolCallId::new("call-1").expect("tool call"),
                    tool_key: ToolKey::from_versioned_key(
                        VersionedKey::new("fixture.echo@1").expect("tool key"),
                    ),
                    effect: Effect::Read,
                    request_ref: request_ref.clone(),
                    timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(10_000) },
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("tool record");
    reducer
        .advance_through(LedgerOffset::new(2))
        .expect("tool cursor");

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(2),
            UtcTimestamp::from_unix_millis(20_000),
        )
        .await
        .expect("snapshot");

    assert_eq!(snapshot.tenant_id, tenant_id);
    assert_eq!(snapshot.conversation_id, conversation_id);
    assert_eq!(snapshot.home_epoch, home_epoch);
    assert_eq!(snapshot.through_conversation_seq, LedgerOffset::new(2));
    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].role, MessageRole::User);
    assert_eq!(snapshot.messages[0].content_ref, message_content);
    assert_eq!(
        snapshot.messages[0].requested_skill_refs,
        vec![VersionedKey::new("file-analysis@2").expect("skill ref")]
    );
    assert_eq!(snapshot.tool_timeline.len(), 1);
    assert_eq!(snapshot.tool_timeline[0].invocation_id, invocation_id);
    assert_eq!(snapshot.tool_timeline[0].run_id, run_id);
    assert_eq!(snapshot.tool_timeline[0].tool_ref, "fixture.echo@1");
    assert_eq!(
        snapshot.tool_timeline[0].status,
        ProjectedToolStatus::Requested
    );
    assert_eq!(snapshot.tool_timeline[0].request_ref, request_ref);
    assert_eq!(snapshot.content_manifest.len(), 2);
    assert!(snapshot.content_manifest.contains(&message_content));
    assert!(snapshot.content_manifest.contains(&request_ref));
}

#[tokio::test]
async fn reasoning_outputs_remain_in_the_sequence_owned_stream_timeline() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let message_id = MessageId::generate();
    let first_reasoning_id = OutputId::generate();
    let assistant_text_id = OutputId::generate();
    let second_reasoning_id = OutputId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(MemoryContent::default());
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    for (sequence, output_id, channel, value) in [
        (
            1,
            first_reasoning_id,
            OutputChannel::ReasoningSummary,
            "first thought",
        ),
        (
            3,
            assistant_text_id,
            OutputChannel::AssistantText,
            "first answer",
        ),
        (
            4,
            second_reasoning_id,
            OutputChannel::ReasoningSummary,
            "second thought",
        ),
    ] {
        apply_output(
            &mut reducer,
            Arc::clone(&content),
            tenant_id,
            conversation_id,
            home_epoch,
            run_id,
            message_id,
            sequence,
            output_id,
            channel,
            value,
        )
        .await;
    }

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(4),
            UtcTimestamp::from_unix_millis(20_000),
        )
        .await
        .expect("snapshot");

    assert_eq!(snapshot.messages.len(), 1);
    assert!(snapshot.messages[0].reasoning_summaries.is_empty());
    assert_eq!(
        snapshot
            .stream_outputs
            .iter()
            .map(|output| (
                output.checkpoint.output_id,
                output.channel,
                output.first_conversation_seq.get(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (first_reasoning_id, OutputChannel::ReasoningSummary, 1),
            (assistant_text_id, OutputChannel::AssistantText, 3),
            (second_reasoning_id, OutputChannel::ReasoningSummary, 4),
        ]
    );
}

#[tokio::test]
async fn historical_recovery_terminal_reseal_does_not_corrupt_the_projection() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let message_id = MessageId::generate();
    let output_id = OutputId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(MemoryContent::default());
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    let text = "durable answer";
    let first_content = InlineFrame::new(text).expect("text frame");
    let first = OutputFrame {
        frame_id: FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel: OutputChannel::AssistantText,
        frame_index: 0,
        byte_offset_start: 0,
        byte_offset_end: first_content.byte_length(),
        content_hash: Sha256::digest(text.as_bytes()),
        content: FrameContent::InlineUtf8(first_content),
        final_for_output: true,
    };
    let reseal = OutputFrame {
        frame_id: FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel: OutputChannel::AssistantText,
        frame_index: 1,
        byte_offset_start: text.len() as u64,
        byte_offset_end: text.len() as u64,
        content_hash: Sha256::digest(b""),
        content: FrameContent::InlineUtf8(InlineFrame::final_marker()),
        final_for_output: true,
    };
    for (sequence, frame) in [(1, first), (2, reseal)] {
        let mut output_record = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            Some(run_id),
            None,
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                project_task_target: None,
                message_id,
                role: MessageRole::Assistant,
                content_ref: content_ref(b"placeholder"),
                attachment_refs: Vec::new(),
                requested_skill_refs: Vec::new(),
            }),
        );
        output_record.metadata.record_kind = LedgerRecordKind::OutputFrame;
        output_record.payload = LedgerPayload::OutputFrame(frame);
        reducer
            .apply(output_record, Arc::clone(&content))
            .await
            .expect("historical output frame");
        reducer
            .advance_through(LedgerOffset::new(sequence))
            .expect("output cursor");
    }

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(2),
            UtcTimestamp::from_unix_millis(20_000),
        )
        .await
        .expect("projection remains readable");
    assert_eq!(snapshot.stream_outputs.len(), 1);
    assert_eq!(snapshot.stream_outputs[0].checkpoint.next_frame_index, 2);
    assert_eq!(
        snapshot.stream_outputs[0].checkpoint.next_byte_offset,
        text.len() as u64
    );
    assert!(snapshot.stream_outputs[0].checkpoint.completed);
}

#[tokio::test]
async fn unbound_tool_output_survives_an_interleaved_assistant_message() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let first_message_id = MessageId::generate();
    let second_message_id = MessageId::generate();
    let first_assistant_output_id = OutputId::generate();
    let second_assistant_output_id = OutputId::generate();
    let tool_output_id = OutputId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(MemoryContent::default());
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    apply_output(
        &mut reducer,
        Arc::clone(&content),
        tenant_id,
        conversation_id,
        home_epoch,
        run_id,
        first_message_id,
        1,
        first_assistant_output_id,
        OutputChannel::AssistantText,
        "first answer",
    )
    .await;

    let tool_arguments = "list_goals";
    let tool_content = InlineFrame::new(tool_arguments).expect("tool arguments");
    let initial_tool_frame = OutputFrame {
        frame_id: FrameId::generate(),
        output_id: tool_output_id,
        message_id: None,
        channel: OutputChannel::ToolArguments,
        frame_index: 0,
        byte_offset_start: 0,
        byte_offset_end: tool_content.byte_length(),
        content_hash: Sha256::digest(tool_arguments.as_bytes()),
        content: FrameContent::InlineUtf8(tool_content),
        final_for_output: false,
    };
    apply_frame(
        &mut reducer,
        Arc::clone(&content),
        tenant_id,
        conversation_id,
        home_epoch,
        run_id,
        2,
        initial_tool_frame,
    )
    .await;

    apply_output(
        &mut reducer,
        Arc::clone(&content),
        tenant_id,
        conversation_id,
        home_epoch,
        run_id,
        second_message_id,
        3,
        second_assistant_output_id,
        OutputChannel::AssistantText,
        "second answer",
    )
    .await;

    let terminal_tool_frame = OutputFrame {
        frame_id: FrameId::generate(),
        output_id: tool_output_id,
        message_id: None,
        channel: OutputChannel::ToolArguments,
        frame_index: 1,
        byte_offset_start: tool_arguments.len() as u64,
        byte_offset_end: tool_arguments.len() as u64,
        content_hash: Sha256::digest(b""),
        content: FrameContent::InlineUtf8(InlineFrame::final_marker()),
        final_for_output: true,
    };
    apply_frame(
        &mut reducer,
        Arc::clone(&content),
        tenant_id,
        conversation_id,
        home_epoch,
        run_id,
        4,
        terminal_tool_frame,
    )
    .await;

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(4),
            UtcTimestamp::from_unix_millis(20_000),
        )
        .await
        .expect("projection remains readable");
    let tool_output = snapshot
        .stream_outputs
        .iter()
        .find(|output| output.checkpoint.output_id == tool_output_id)
        .expect("tool output");
    assert_eq!(tool_output.message_id, None);
    assert_eq!(tool_output.checkpoint.next_frame_index, 2);
    assert_eq!(
        tool_output.checkpoint.next_byte_offset,
        tool_arguments.len() as u64
    );
    assert!(tool_output.checkpoint.completed);
}

#[tokio::test]
async fn every_tool_event_variant_survives_the_shared_product_reducer() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let run_id = RunId::generate();
    let completed_id = InvocationId::generate();
    let failed_id = InvocationId::generate();
    let indeterminate_id = InvocationId::generate();
    let cancelled_id = InvocationId::generate();
    let result_ref = content_ref(br#"{"ok":true}"#);
    let failed_result_ref = content_ref(br#"{"status":"failed","stderr":"boom"}"#);
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");
    let requests = [completed_id, failed_id, indeterminate_id, cancelled_id];
    let mut sequence = 1;
    for invocation_id in requests {
        apply_tool(
            &mut reducer,
            Arc::clone(&content),
            tenant_id,
            conversation_id,
            home_epoch,
            run_id,
            sequence,
            ToolEvent::InvocationRequested {
                invocation_id,
                tool_call_id: ToolCallId::new(format!("call-{sequence}")).expect("tool call"),
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new("fixture.echo@1").expect("tool key"),
                ),
                effect: Effect::Read,
                request_ref: content_ref(format!("request-{sequence}").as_bytes()),
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(20_000) },
            },
        )
        .await;
        sequence += 1;
    }
    let lifecycle = [
        ToolEvent::InvocationWaitingInteraction {
            invocation_id: completed_id,
            interaction_id: InteractionId::generate(),
        },
        ToolEvent::InvocationStarted {
            invocation_id: completed_id,
            execution_attempt_id: AttemptId::generate(),
            capability_provider_id: CapabilityProviderId::generate(),
            execution_timing: Some(musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(30_000) }),
        },
        ToolEvent::InvocationSucceeded {
            invocation_id: completed_id,
            result_ref: result_ref.clone(),
            result_hash: result_ref.sha256,
            provider_request_ref: None,
            authorized_content: Vec::new(),
        },
        ToolEvent::InvocationFailed {
            invocation_id: failed_id,
            error: ErrorInfo {
                code: ErrorCode::ToolNotExposed,
                category: ErrorCode::ToolNotExposed.category(),
                safe_message: SafeDisplayText::new("historical tool is no longer exposed")
                    .expect("safe text"),
                retryable: false,
                dependency_ref: None,
                diagnostic_ref: None,
            },
            retry_disposition: RetryDisposition::DoNotRetry,
            diagnostic_result_ref: Some(failed_result_ref.clone()),
            diagnostic_result_hash: Some(failed_result_ref.sha256),
        },
        ToolEvent::InvocationIndeterminate {
            diagnostic_result_hash: None,
            diagnostic_result_ref: None,
            observed_error: None,
            invocation_id: indeterminate_id,
            reason: SafeDisplayText::new("outcome unknown").expect("safe text"),
            reconciliation_ref: OpaqueExternalRef::new("reconcile-fixture").expect("opaque ref"),
        },
        ToolEvent::InvocationCancelled {
            invocation_id: cancelled_id,
            reason: CancelReason::UserRequested,
        },
    ];
    for event in lifecycle {
        apply_tool(
            &mut reducer,
            Arc::clone(&content),
            tenant_id,
            conversation_id,
            home_epoch,
            run_id,
            sequence,
            event,
        )
        .await;
        sequence += 1;
    }
    let output_artifact_id = ArtifactId::generate();
    let output_content_ref = content_ref(b"tool output");
    let mut created = record(
        tenant_id,
        conversation_id,
        home_epoch,
        sequence,
        Some(run_id),
        None,
        DomainEvent::Artifact(ArtifactEvent::Created {
            artifact_id: output_artifact_id,
            kind: ArtifactKind::File,
            title: DisplayText::new("result.csv").expect("artifact title"),
            source_invocation_id: Some(completed_id),
        }),
    );
    created.metadata.activity_id = Some(ActivityId::generate());
    reducer
        .apply(created, Arc::clone(&content))
        .await
        .expect("tool output artifact created");
    sequence += 1;
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                sequence,
                Some(run_id),
                None,
                DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                    artifact_id: output_artifact_id,
                    version: musteragent_identity::VersionNumber::new(1).expect("artifact version"),
                    content_ref: output_content_ref,
                    dependency_refs: Vec::new(),
                    content_hash: Sha256::digest(b"tool output"),
                    source_invocation_id: Some(completed_id),
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("tool output artifact committed");
    sequence += 1;

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(sequence - 1),
            UtcTimestamp::from_unix_millis(30_000),
        )
        .await
        .expect("snapshot");
    let statuses = snapshot
        .tool_timeline
        .iter()
        .map(|tool| (tool.invocation_id, tool.status))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(statuses[&completed_id], ProjectedToolStatus::Completed);
    assert_eq!(statuses[&failed_id], ProjectedToolStatus::Failed);
    assert_eq!(
        statuses[&indeterminate_id],
        ProjectedToolStatus::ResultUnknown
    );
    assert_eq!(statuses[&cancelled_id], ProjectedToolStatus::Cancelled);
    let completed = snapshot
        .tool_timeline
        .iter()
        .find(|tool| tool.invocation_id == completed_id)
        .expect("completed tool");
    assert_eq!(completed.result_ref.as_ref(), Some(&result_ref));
    assert_eq!(completed.output_artifact_ids, vec![output_artifact_id]);
    let failed = snapshot
        .tool_timeline
        .iter()
        .find(|tool| tool.invocation_id == failed_id)
        .expect("failed tool");
    assert_eq!(failed.result_ref.as_ref(), Some(&failed_result_ref));
    assert!(completed.started_at.is_some());
    assert!(completed.finished_at.is_some());
    assert!(snapshot.content_manifest.contains(&result_ref));
    assert!(snapshot.content_manifest.contains(&failed_result_ref));
}

#[test]
fn reducer_cursor_is_monotonic_and_snapshot_identity_is_exact() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(7).expect("epoch");
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    assert_eq!(reducer.tenant_id(), tenant_id);
    assert_eq!(reducer.conversation_id(), conversation_id);
    assert_eq!(reducer.home_epoch(), home_epoch);
    reducer
        .advance_through(LedgerOffset::new(9))
        .expect("advance");
    assert_eq!(reducer.through(), LedgerOffset::new(9));
    assert_eq!(
        reducer
            .advance_through(LedgerOffset::new(8))
            .expect_err("cursor cannot move backward"),
        SubscriptionError::CorruptRecord
    );
}

#[tokio::test]
async fn identical_user_messages_retry_and_delegated_runs_preserve_exact_input_links() {
    use musteragent_identity::RunSeq;

    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let user_participant = ParticipantId::generate();
    let executor_participant = ParticipantId::generate();
    let identical_text = typed_content_ref(b"repeat this exact prompt", "text/plain;charset=utf-8");
    let first_message_id = MessageId::generate();
    let second_message_id = MessageId::generate();
    let imported_message_id = MessageId::generate();
    let first_activity_id = ActivityId::generate();
    let second_activity_id = ActivityId::generate();
    let retry_activity_id = ActivityId::generate();
    let imported_retry_activity_id = ActivityId::generate();
    let child_activity_id = ActivityId::generate();
    let coord_activity_id = ActivityId::generate();
    let first_run_id = RunId::generate();
    let second_run_id = RunId::generate();
    let retry_run_id = RunId::generate();
    let imported_retry_run_id = RunId::generate();
    let child_run_id = RunId::generate();
    let coord_run_id = RunId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    let mut sequence = 0_u64;
    for (message_id, activity_id, run_id) in [
        (first_message_id, first_activity_id, first_run_id),
        (second_message_id, second_activity_id, second_run_id),
    ] {
        sequence += 1;
        let mut accepted = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            None,
            Some(user_participant),
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                project_task_target: None,
                message_id,
                role: MessageRole::User,
                content_ref: identical_text.clone(),
                attachment_refs: Vec::new(),
                requested_skill_refs: Vec::new(),
            }),
        );
        accepted.metadata.activity_id = Some(activity_id);
        assert!(accepted.metadata.run_id.is_none());
        assert!(accepted.metadata.run_seq.is_none());
        reducer
            .apply(accepted, Arc::clone(&content))
            .await
            .expect("accept user message");

        sequence += 1;
        let mut started = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            None,
            Some(user_participant),
            DomainEvent::Activity(ActivityEvent::Started {
                kind: ActivityKind::Chat,
                root_activity_id: activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: VersionedKey::new("policy.chat@1").expect("policy"),
            }),
        );
        started.metadata.activity_id = Some(activity_id);
        reducer
            .apply(started, Arc::clone(&content))
            .await
            .expect("start activity");

        sequence += 1;
        let mut admitted = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            Some(run_id),
            Some(executor_participant),
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
                execution_home: RuntimeHome::Cloud,
                parent_run_id: None,
                delegation_id: None,
                input_message_id: Some(message_id),
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        );
        admitted.metadata.activity_id = Some(activity_id);
        admitted.metadata.run_seq = Some(RunSeq::new(1).expect("run seq 1"));
        reducer
            .apply(admitted, Arc::clone(&content))
            .await
            .expect("admit run");
    }

    // Tool requested before any assistant message must not bind to the user input message.
    sequence += 1;
    let tool_invocation_id = InvocationId::generate();
    apply_tool(
        &mut reducer,
        Arc::clone(&content),
        tenant_id,
        conversation_id,
        home_epoch,
        second_run_id,
        sequence,
        ToolEvent::InvocationRequested {
            invocation_id: tool_invocation_id,
            tool_call_id: ToolCallId::new("call-before-assistant").expect("tool call"),
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new("fixture.echo@1").expect("tool key"),
            ),
            effect: Effect::Read,
            request_ref: content_ref(br#"{"echo":1}"#),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(20_000),
            },
        },
    )
    .await;

    // Retry of first_run_id links retry_run_id -> first_message_id without overwriting first_message_id.source_run_id.
    sequence += 1;
    let mut retry_admitted = record(
        tenant_id,
        conversation_id,
        home_epoch,
        sequence,
        Some(retry_run_id),
        Some(executor_participant),
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: Some(first_run_id),
            delegation_id: None,
            input_message_id: Some(first_message_id),
            retry_source_message_id: Some(first_message_id),
            retry_source_attempt_number: Some(AttemptNumber::new(1).expect("attempt one")),
        }),
    );
    retry_admitted.metadata.activity_id = Some(retry_activity_id);
    retry_admitted.metadata.run_seq = Some(RunSeq::new(1).expect("run seq 1"));
    reducer
        .apply(retry_admitted, Arc::clone(&content))
        .await
        .expect("admit retry run");

    // Imported message with no source_run_id must also remain None when referenced by a retry.
    sequence += 1;
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                sequence,
                None,
                Some(user_participant),
                DomainEvent::Conversation(ConversationEvent::MessageImported {
                    message_id: imported_message_id,
                    role: MessageRole::User,
                    content_ref: identical_text.clone(),
                    projection_ref: None,
                    attachment_refs: Vec::new(),
                    legacy_message_id: SafeDisplayText::new("legacy-msg-1").expect("legacy id"),
                    source_sequence: 1,
                    source_payload_hash: identical_text.sha256,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("import message");
    sequence += 1;
    let mut imported_retry = record(
        tenant_id,
        conversation_id,
        home_epoch,
        sequence,
        Some(imported_retry_run_id),
        Some(executor_participant),
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: Some(first_run_id),
            delegation_id: None,
            input_message_id: Some(imported_message_id),
            retry_source_message_id: Some(imported_message_id),
            retry_source_attempt_number: Some(AttemptNumber::new(1).expect("attempt one")),
        }),
    );
    imported_retry.metadata.activity_id = Some(imported_retry_activity_id);
    imported_retry.metadata.run_seq = Some(RunSeq::new(1).expect("run seq 1"));
    reducer
        .apply(imported_retry, Arc::clone(&content))
        .await
        .expect("admit retry on imported message");

    // Delegated and coordination runs carry no input message.
    for (activity_id, run_id, parent, delegation) in [
        (
            child_activity_id,
            child_run_id,
            Some(second_run_id),
            Some(DelegationId::generate()),
        ),
        (coord_activity_id, coord_run_id, None, None),
    ] {
        sequence += 1;
        let mut admitted = record(
            tenant_id,
            conversation_id,
            home_epoch,
            sequence,
            Some(run_id),
            Some(executor_participant),
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: RuntimeSnapshotId::generate(),
                execution_home: RuntimeHome::Cloud,
                parent_run_id: parent,
                delegation_id: delegation,
                input_message_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        );
        admitted.metadata.activity_id = Some(activity_id);
        admitted.metadata.run_seq = Some(RunSeq::new(1).expect("run seq 1"));
        reducer
            .apply(admitted, Arc::clone(&content))
            .await
            .expect("admit non-user run");
    }

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(sequence),
            UtcTimestamp::from_unix_millis(50_000),
        )
        .await
        .expect("snapshot");

    let by_message = snapshot
        .messages
        .iter()
        .map(|message| (message.message_id, message.source_run_id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_message[&first_message_id], Some(first_run_id));
    assert_eq!(by_message[&second_message_id], Some(second_run_id));
    assert_eq!(by_message[&imported_message_id], None);

    let by_run = snapshot
        .runs
        .iter()
        .map(|run| (run.run_id, run))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_run[&first_run_id].input_message_id,
        Some(first_message_id)
    );
    assert_eq!(by_run[&first_run_id].retry_source_message_id, None);
    assert_eq!(
        by_run[&second_run_id].input_message_id,
        Some(second_message_id)
    );
    assert_eq!(
        by_run[&retry_run_id].input_message_id,
        Some(first_message_id)
    );
    assert_eq!(
        by_run[&retry_run_id].retry_source_message_id,
        Some(first_message_id)
    );
    assert_eq!(by_run[&retry_run_id].parent_run_id, Some(first_run_id));
    assert_eq!(
        by_run[&imported_retry_run_id].input_message_id,
        Some(imported_message_id)
    );
    assert_eq!(by_run[&child_run_id].input_message_id, None);
    assert_eq!(by_run[&coord_run_id].input_message_id, None);
    assert_eq!(snapshot.tool_timeline[0].message_id, None);
}

#[tokio::test]
async fn cold_start_from_legacy_events_and_replay_after_retraction_preserve_run_inputs() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let user_participant = ParticipantId::generate();
    let executor_participant = ParticipantId::generate();
    let content_one = typed_content_ref(b"legacy turn one", "text/plain;charset=utf-8");
    let content_two = typed_content_ref(b"legacy turn two", "text/plain;charset=utf-8");
    let first_message_id = MessageId::generate();
    let second_message_id = MessageId::generate();
    let first_activity_id = ActivityId::generate();
    let retry_activity_id = ActivityId::generate();
    let second_activity_id = ActivityId::generate();
    let first_run_id = RunId::generate();
    let retry_run_id = RunId::generate();
    let second_run_id = RunId::generate();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer = ProductProjectionReducer::new(
        tenant_id,
        conversation_id,
        home_epoch,
        None,
        BTreeMap::new(),
    )
    .expect("reducer");

    // Legacy events without RunAdmitted.input_message_id:
    let mut accepted_one = record(
        tenant_id,
        conversation_id,
        home_epoch,
        1,
        None,
        Some(user_participant),
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id: first_message_id,
            role: MessageRole::User,
            content_ref: content_one,
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    accepted_one.metadata.activity_id = Some(first_activity_id);
    reducer
        .apply(accepted_one, Arc::clone(&content))
        .await
        .expect("legacy accept 1");

    let mut admitted_one = record(
        tenant_id,
        conversation_id,
        home_epoch,
        2,
        Some(first_run_id),
        Some(executor_participant),
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            input_message_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        }),
    );
    admitted_one.metadata.activity_id = Some(first_activity_id);
    reducer
        .apply(admitted_one, Arc::clone(&content))
        .await
        .expect("legacy admit 1");

    let mut legacy_retry = record(
        tenant_id,
        conversation_id,
        home_epoch,
        3,
        Some(retry_run_id),
        Some(executor_participant),
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: Some(first_run_id),
            delegation_id: None,
            input_message_id: None,
            retry_source_message_id: Some(first_message_id),
            retry_source_attempt_number: Some(AttemptNumber::new(1).expect("attempt one")),
        }),
    );
    legacy_retry.metadata.activity_id = Some(retry_activity_id);
    reducer
        .apply(legacy_retry, Arc::clone(&content))
        .await
        .expect("legacy retry");

    // Retract turn one without explicit source_run_id, then replay a fresh turn.
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                4,
                None,
                Some(user_participant),
                DomainEvent::Conversation(ConversationEvent::MessageRetracted {
                    message_id: first_message_id,
                    reason: SafeDisplayText::new("retract legacy turn").expect("reason"),
                    source_run_id: None,
                }),
            ),
            Arc::clone(&content),
        )
        .await
        .expect("retract turn 1");

    let mut accepted_two = record(
        tenant_id,
        conversation_id,
        home_epoch,
        5,
        None,
        Some(user_participant),
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id: second_message_id,
            role: MessageRole::User,
            content_ref: content_two,
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    accepted_two.metadata.activity_id = Some(second_activity_id);
    reducer
        .apply(accepted_two, Arc::clone(&content))
        .await
        .expect("accept turn 2");

    let mut admitted_two = record(
        tenant_id,
        conversation_id,
        home_epoch,
        6,
        Some(second_run_id),
        Some(executor_participant),
        DomainEvent::Run(RunEvent::Admitted {
            runtime_snapshot_id: RuntimeSnapshotId::generate(),
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            input_message_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        }),
    );
    admitted_two.metadata.activity_id = Some(second_activity_id);
    reducer
        .apply(admitted_two, Arc::clone(&content))
        .await
        .expect("legacy admit 2");

    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(6),
            UtcTimestamp::from_unix_millis(10_000),
        )
        .await
        .expect("snapshot");

    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].message_id, second_message_id);
    assert_eq!(snapshot.messages[0].source_run_id, Some(second_run_id));
    assert_eq!(snapshot.runs.len(), 1);
    assert_eq!(snapshot.runs[0].run_id, second_run_id);
    assert_eq!(snapshot.runs[0].input_message_id, Some(second_message_id));
}

#[allow(clippy::too_many_arguments)]
fn record(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    sequence: u64,
    run_id: Option<RunId>,
    participant_id: Option<ParticipantId>,
    event: DomainEvent,
) -> CanonicalLedgerRecord {
    CanonicalLedgerRecord {
        metadata: LedgerMetadata {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
            record_id: LedgerRecordId::generate(),
            record_kind: LedgerRecordKind::DomainEvent,
            tenant_id,
            conversation_id,
            conversation_seq: ConversationSeq::new(sequence).expect("sequence"),
            activity_id: None,
            run_id,
            run_seq: None,
            attempt_id: None,
            participant_id,
            home_epoch,
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(sequence as i64),
            committed_at: UtcTimestamp::from_unix_millis(sequence as i64),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            idempotency_key: IdempotencyKey::new(format!("projection:{sequence}"))
                .expect("idempotency"),
            payload_hash: Sha256::digest(sequence.to_be_bytes()),
        },
        payload: LedgerPayload::DomainEvent(event),
    }
}

fn projected_test_message(
    message_id: MessageId,
    run_id: RunId,
    content_ref: ContentRef,
    committed_at: UtcTimestamp,
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
        committed_at,
        attachment_refs: Vec::new(),
        requested_skill_refs: Vec::new(),
        imported_projection_ref: None,
        assistant_output: None,
        partial_reason: None,
    }
}

fn projected_test_activity(
    activity_id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
    parent_activity_id: Option<ActivityId>,
    root_activity_id: ActivityId,
    occurred_at: UtcTimestamp,
) -> ProjectedActivity {
    ProjectedActivity {
        activity_id,
        kind,
        status,
        requested_by_participant_id: Some(ParticipantId::generate()),
        parent_activity_id,
        root_activity_id,
        policy_snapshot_ref: VersionedKey::new("policy.retraction-fixture@1").expect("policy"),
        started_at: occurred_at,
        finished_at: Some(occurred_at),
        delegation_origin: None,
        coordination_origin: None,
    }
}

fn projected_test_run(
    run_id: RunId,
    activity_id: ActivityId,
    parent_run_id: Option<RunId>,
    delegation_id: Option<DelegationId>,
    status: RunStatus,
    occurred_at: UtcTimestamp,
) -> ProjectedRun {
    ProjectedRun {
        run_id,
        activity_id,
        executor_participant_id: Some(ParticipantId::generate()),
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
        admitted_at: occurred_at,
        started_at: Some(occurred_at),
        finished_at: Some(occurred_at),
    }
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    typed_content_ref(bytes, "application/json")
}

fn typed_content_ref(bytes: &[u8], media_type: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_output(
    reducer: &mut ProductProjectionReducer,
    content: Arc<dyn ProjectionContentStore>,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    run_id: RunId,
    message_id: MessageId,
    sequence: u64,
    output_id: OutputId,
    channel: OutputChannel,
    value: &str,
) {
    let frame_content = InlineFrame::new(value).expect("inline output");
    let frame = OutputFrame {
        frame_id: FrameId::generate(),
        output_id,
        message_id: Some(message_id),
        channel,
        frame_index: 0,
        byte_offset_start: 0,
        byte_offset_end: frame_content.byte_length(),
        content_hash: Sha256::digest(value.as_bytes()),
        content: FrameContent::InlineUtf8(frame_content),
        final_for_output: true,
    };
    let mut output_record = record(
        tenant_id,
        conversation_id,
        home_epoch,
        sequence,
        Some(run_id),
        None,
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id,
            role: MessageRole::Assistant,
            content_ref: content_ref(b"placeholder"),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    output_record.metadata.record_kind = LedgerRecordKind::OutputFrame;
    output_record.payload = LedgerPayload::OutputFrame(frame);
    reducer
        .apply(output_record, content)
        .await
        .expect("output frame");
    reducer
        .advance_through(LedgerOffset::new(sequence))
        .expect("output cursor");
}

#[allow(clippy::too_many_arguments)]
async fn apply_frame(
    reducer: &mut ProductProjectionReducer,
    content: Arc<dyn ProjectionContentStore>,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    run_id: RunId,
    sequence: u64,
    frame: OutputFrame,
) {
    let mut output_record = record(
        tenant_id,
        conversation_id,
        home_epoch,
        sequence,
        Some(run_id),
        None,
        DomainEvent::Conversation(ConversationEvent::MessageAccepted {
            project_task_target: None,
            message_id: MessageId::generate(),
            role: MessageRole::Assistant,
            content_ref: content_ref(b"placeholder"),
            attachment_refs: Vec::new(),
            requested_skill_refs: Vec::new(),
        }),
    );
    output_record.metadata.record_kind = LedgerRecordKind::OutputFrame;
    output_record.payload = LedgerPayload::OutputFrame(frame);
    reducer
        .apply(output_record, content)
        .await
        .expect("output frame");
    reducer
        .advance_through(LedgerOffset::new(sequence))
        .expect("output cursor");
}

#[allow(clippy::too_many_arguments)]
async fn apply_tool(
    reducer: &mut ProductProjectionReducer,
    content: Arc<dyn ProjectionContentStore>,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    home_epoch: HomeEpoch,
    run_id: RunId,
    sequence: u64,
    event: ToolEvent,
) {
    reducer
        .apply(
            record(
                tenant_id,
                conversation_id,
                home_epoch,
                sequence,
                Some(run_id),
                None,
                DomainEvent::Tool(event),
            ),
            content,
        )
        .await
        .expect("tool event");
    reducer
        .advance_through(LedgerOffset::new(sequence))
        .expect("tool cursor");
}
