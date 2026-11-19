use std::sync::Mutex;

use musteragent_activity::{ActivityKind, ActivityStatus};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::MessageRole;
use musteragent_identity::*;
use musteragent_projection::*;
use musteragent_run::RunStatus;
use musteragent_runtime_contract::ApplicationFuture;

struct Source {
    records: Vec<RetainedProjectionRunInput>,
    calls: Mutex<Vec<ProjectionRunInputQuery>>,
}

impl ProjectionRunInputSource for Source {
    fn load_run_inputs(
        &self,
        query: ProjectionRunInputQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunInput>, SubscriptionError>> {
        self.calls.lock().expect("record query").push(query);
        let records = self.records.clone();
        Box::pin(async move { Ok(records) })
    }
}

fn content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn activity(
    activity_id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
) -> ProjectedActivity {
    ProjectedActivity {
        activity_id,
        kind,
        status,
        requested_by_participant_id: None,
        parent_activity_id: None,
        root_activity_id: activity_id,
        policy_snapshot_ref: VersionedKey::new("policy.fixture@1").expect("policy"),
        started_at: UtcTimestamp::from_unix_millis(100),
        finished_at: status
            .ne(&ActivityStatus::Active)
            .then_some(UtcTimestamp::from_unix_millis(200)),
        delegation_origin: None,
        coordination_origin: None,
    }
}

fn run(
    run_id: RunId,
    activity_id: ActivityId,
    parent_run_id: Option<RunId>,
    delegation_id: Option<DelegationId>,
    retry_source_message_id: Option<MessageId>,
    status: RunStatus,
) -> ProjectedRun {
    ProjectedRun {
        run_id,
        activity_id,
        executor_participant_id: None,
        parent_run_id,
        delegation_id,
        input_message_id: None,
        retry_source_message_id,
        retry_source_attempt_number: retry_source_message_id
            .map(|_| AttemptNumber::new(1).expect("attempt")),
        terminal_failure: None,
        retry_failure: None,
        status,
        wait_reason: None,
        attempt_number: None,
        next_attempt_number: None,
        retry_not_before: None,
        admitted_at: UtcTimestamp::from_unix_millis(100),
        started_at: None,
        finished_at: status
            .is_terminal()
            .then_some(UtcTimestamp::from_unix_millis(200)),
    }
}

fn user_message(message_id: MessageId, content: ContentRef) -> ProjectedMessage {
    ProjectedMessage {
        message_id,
        participant_id: None,
        role: MessageRole::User,
        content_ref: content,
        reasoning_summaries: Vec::new(),
        status: ProjectedMessageStatus::Committed,
        source_run_id: None,
        committed_at: UtcTimestamp::from_unix_millis(100),
        attachment_refs: Vec::new(),
        requested_skill_refs: Vec::new(),
        project_task_target: None,
        imported_projection_ref: None,
        assistant_output: None,
        partial_reason: None,
    }
}

#[tokio::test]
async fn backfills_visible_inputs_and_retries_without_resurrecting_retracted_rows_or_overwriting_sources()
 {
    let shared_text = content_ref(b"identical text");
    let first_message_id = MessageId::generate();
    let second_message_id = MessageId::generate();
    let retracted_message_id = MessageId::generate();
    let first_activity_id = ActivityId::generate();
    let retry_activity_id = ActivityId::generate();
    let second_activity_id = ActivityId::generate();
    let child_activity_id = ActivityId::generate();
    let first_run_id = RunId::generate();
    let retry_run_id = RunId::generate();
    let second_run_id = RunId::generate();
    let retracted_run_id = RunId::generate();
    let child_run_id = RunId::generate();

    let active_second = run(
        second_run_id,
        second_activity_id,
        None,
        None,
        None,
        RunStatus::Running,
    );
    let mut snapshot = ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        home_epoch: HomeEpoch::new(1).expect("epoch"),
        through_conversation_seq: LedgerOffset::new(50),
        messages: vec![
            user_message(first_message_id, shared_text.clone()),
            user_message(second_message_id, shared_text.clone()),
        ],
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: vec![
            activity(
                first_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Failed,
            ),
            activity(
                retry_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Completed,
            ),
            activity(
                second_activity_id,
                ActivityKind::Chat,
                ActivityStatus::Active,
            ),
            activity(
                child_activity_id,
                ActivityKind::Delegation,
                ActivityStatus::Completed,
            ),
        ],
        runs: vec![
            run(
                first_run_id,
                first_activity_id,
                None,
                None,
                None,
                RunStatus::Failed,
            ),
            run(
                retry_run_id,
                retry_activity_id,
                Some(first_run_id),
                None,
                Some(first_message_id),
                RunStatus::Completed,
            ),
            active_second.clone(),
            run(
                child_run_id,
                child_activity_id,
                Some(second_run_id),
                Some(DelegationId::generate()),
                None,
                RunStatus::Completed,
            ),
        ],
        active_runs: vec![active_second],
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest: vec![shared_text],
        checksum: Sha256::digest(b"legacy-v18"),
        created_at: UtcTimestamp::from_unix_millis(500),
    };

    let source = Source {
        records: vec![
            RetainedProjectionRunInput {
                run_id: retracted_run_id,
                input_message_id: retracted_message_id,
                admitted_sequence: ConversationSeq::new(1).expect("seq"),
            },
            RetainedProjectionRunInput {
                run_id: first_run_id,
                input_message_id: first_message_id,
                admitted_sequence: ConversationSeq::new(10).expect("seq"),
            },
            RetainedProjectionRunInput {
                run_id: second_run_id,
                input_message_id: second_message_id,
                admitted_sequence: ConversationSeq::new(25).expect("seq"),
            },
        ],
        calls: Mutex::new(Vec::new()),
    };

    backfill_projection_run_inputs(&mut snapshot, &source)
        .await
        .expect("backfill legacy snapshot");

    assert_eq!(
        snapshot.messages.len(),
        2,
        "retracted message stays excluded"
    );
    assert_eq!(snapshot.messages[0].source_run_id, Some(first_run_id));
    assert_eq!(snapshot.messages[1].source_run_id, Some(second_run_id));
    assert_eq!(snapshot.runs.len(), 4, "retracted run stays excluded");
    assert_eq!(snapshot.runs[0].input_message_id, Some(first_message_id));
    assert_eq!(snapshot.runs[1].input_message_id, Some(first_message_id));
    assert_eq!(snapshot.runs[2].input_message_id, Some(second_message_id));
    assert_eq!(snapshot.runs[3].input_message_id, None);
    assert_eq!(snapshot.active_runs.len(), 1);
    assert_eq!(
        snapshot.active_runs[0].input_message_id,
        Some(second_message_id)
    );
    assert_eq!(source.calls.lock().expect("calls").len(), 1);

    // Already populated snapshots require zero further queries.
    backfill_projection_run_inputs(&mut snapshot, &source)
        .await
        .expect("idempotent backfill");
    assert_eq!(source.calls.lock().expect("calls").len(), 1);
}

#[tokio::test]
async fn rejects_future_sequence_or_conflicting_admission_links() {
    let text = content_ref(b"prompt");
    let message_id = MessageId::generate();
    let activity_id = ActivityId::generate();
    let run_id = RunId::generate();
    let base = ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: ProjectionSnapshotId::generate(),
        tenant_id: TenantId::generate(),
        conversation_id: ConversationId::generate(),
        home_epoch: HomeEpoch::new(1).expect("epoch"),
        through_conversation_seq: LedgerOffset::new(10),
        messages: vec![user_message(message_id, text.clone())],
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: vec![activity(
            activity_id,
            ActivityKind::Chat,
            ActivityStatus::Completed,
        )],
        runs: vec![run(
            run_id,
            activity_id,
            None,
            None,
            None,
            RunStatus::Completed,
        )],
        active_runs: Vec::new(),
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest: vec![text],
        checksum: Sha256::digest(b"base"),
        created_at: UtcTimestamp::from_unix_millis(500),
    };

    let mut future_seq = base.clone();
    let source = Source {
        records: vec![RetainedProjectionRunInput {
            run_id,
            input_message_id: message_id,
            admitted_sequence: ConversationSeq::new(11).expect("future seq"),
        }],
        calls: Mutex::new(Vec::new()),
    };
    assert_eq!(
        backfill_projection_run_inputs(&mut future_seq, &source).await,
        Err(SubscriptionError::CorruptRecord)
    );

    let mut duplicate_msg = base;
    let conflicting_source = Source {
        records: vec![
            RetainedProjectionRunInput {
                run_id,
                input_message_id: message_id,
                admitted_sequence: ConversationSeq::new(1).expect("seq"),
            },
            RetainedProjectionRunInput {
                run_id: RunId::generate(),
                input_message_id: message_id,
                admitted_sequence: ConversationSeq::new(2).expect("seq"),
            },
        ],
        calls: Mutex::new(Vec::new()),
    };
    assert_eq!(
        backfill_projection_run_inputs(&mut duplicate_msg, &conflicting_source).await,
        Err(SubscriptionError::CorruptRecord)
    );
}
