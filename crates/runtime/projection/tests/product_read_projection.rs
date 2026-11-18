use std::sync::atomic::{AtomicUsize, Ordering};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::MessageRole;
use musteragent_identity::{
    Actor, ContentId, ConversationId, HomeEpoch, LedgerOffset, MessageId, PrincipalId,
    PrincipalKind, ProjectionSnapshotId, TenantId, UtcTimestamp,
};
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshot,
    ConversationProjectionSnapshotCodec, ConversationProjectionWindow,
    ConversationProjectionWindowCursor, GetConversationProjection, GetConversationProjectionWindow,
    PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedMessage, ProjectedMessageStatus,
    ProjectionSnapshotCodecError, ProjectionWindowError, projection_window_from_snapshot,
};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};

#[derive(Default)]
struct ProjectionAuthorityProbe {
    reusable_reads: AtomicUsize,
    head_reads: AtomicUsize,
    bounded_head_reads: AtomicUsize,
}

impl ConversationProjectionApplication for ProjectionAuthorityProbe {
    fn get_product_read_projection_window(
        &self,
        query: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>> {
        Box::pin(async move {
            let mut authority = snapshot(
                GetConversationProjection {
                    actor: query.actor,
                    conversation_id: query.conversation_id,
                    preferred_snapshot_id: query.before.as_ref().map(|cursor| cursor.snapshot_id),
                },
                11,
            );
            authority.checksum = TestCodec.checksum(&authority);
            Ok(projection_window_from_snapshot(
                &authority,
                query.before.as_ref(),
                query.limit,
                &TestCodec,
            )
            .expect("probe window"))
        })
    }

    fn get_conversation_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        self.reusable_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(snapshot(query, 7)) })
    }

    fn get_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        self.head_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(snapshot(query, 11)) })
    }

    fn get_bounded_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        self.bounded_head_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(snapshot(query, 13)) })
    }
}

#[tokio::test]
async fn ordinary_product_reads_are_self_contained_at_the_current_head() {
    let probe = ProjectionAuthorityProbe::default();

    let projection = probe
        .get_product_read_projection(query(None))
        .await
        .expect("current head projection");

    assert_eq!(projection.through_conversation_seq.get(), 11);
    assert_eq!(probe.reusable_reads.load(Ordering::Relaxed), 0);
    assert_eq!(probe.head_reads.load(Ordering::Relaxed), 1);
    assert_eq!(probe.bounded_head_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn explicit_cursor_recovery_preserves_the_exact_snapshot_authority() {
    let probe = ProjectionAuthorityProbe::default();

    let projection = probe
        .get_product_read_projection(query(Some(ProjectionSnapshotId::generate())))
        .await
        .expect("exact recovery projection");

    assert_eq!(projection.through_conversation_seq.get(), 7);
    assert_eq!(probe.reusable_reads.load(Ordering::Relaxed), 1);
    assert_eq!(probe.head_reads.load(Ordering::Relaxed), 0);
    assert_eq!(probe.bounded_head_reads.load(Ordering::Relaxed), 0);
}

#[test]
fn product_windows_are_bounded_and_walk_one_immutable_snapshot_backwards() {
    let query = query(None);
    let mut authority = snapshot(query, 99);
    authority.messages = (0..85).map(projected_message).collect();
    let codec = TestCodec;
    authority.checksum = codec.checksum(&authority);

    let newest =
        projection_window_from_snapshot(&authority, None, 40, &codec).expect("newest window");
    assert_eq!(newest.projection.messages.len(), 40);
    assert_eq!(newest.projection.messages[0].content_ref.byte_length, 45);
    assert_eq!(
        newest
            .older_cursor
            .as_ref()
            .expect("newest page has an older cursor")
            .before_message_index,
        45
    );
    assert_eq!(newest.head_cursor, authority.cursor());

    let middle =
        projection_window_from_snapshot(&authority, newest.older_cursor.as_ref(), 40, &codec)
            .expect("middle window");
    assert_eq!(middle.projection.messages.len(), 40);
    assert_eq!(middle.projection.messages[0].content_ref.byte_length, 5);
    assert_eq!(
        middle
            .older_cursor
            .as_ref()
            .expect("middle page has an older cursor")
            .before_message_index,
        5
    );

    let oldest =
        projection_window_from_snapshot(&authority, middle.older_cursor.as_ref(), 40, &codec)
            .expect("oldest window");
    assert_eq!(oldest.projection.messages.len(), 5);
    assert!(!oldest.has_older);
    assert!(oldest.older_cursor.is_none());
}

#[test]
fn product_window_cursor_cannot_cross_snapshot_authority() {
    let query = query(None);
    let mut authority = snapshot(query, 10);
    authority.messages = (0..2).map(projected_message).collect();
    let codec = TestCodec;
    authority.checksum = codec.checksum(&authority);
    let tampered = ConversationProjectionWindowCursor {
        snapshot_id: authority.snapshot_id,
        snapshot_checksum: Sha256::digest(b"another snapshot"),
        before_message_index: 1,
    };
    assert_eq!(
        projection_window_from_snapshot(&authority, Some(&tampered), 1, &codec),
        Err(ProjectionWindowError::CursorAuthorityMismatch),
    );
}

fn query(preferred_snapshot_id: Option<ProjectionSnapshotId>) -> GetConversationProjection {
    GetConversationProjection {
        actor: Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        conversation_id: ConversationId::generate(),
        preferred_snapshot_id,
    }
}

fn snapshot(query: GetConversationProjection, sequence: u64) -> ConversationProjectionSnapshot {
    ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id: query
            .preferred_snapshot_id
            .unwrap_or_else(ProjectionSnapshotId::generate),
        tenant_id: query.actor.tenant_id,
        conversation_id: query.conversation_id,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        through_conversation_seq: LedgerOffset::new(sequence),
        messages: Vec::new(),
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: Vec::new(),
        runs: Vec::new(),
        active_runs: Vec::new(),
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest: Vec::new(),
        checksum: Sha256::digest([]),
        created_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
    }
}

fn projected_message(index: u64) -> ProjectedMessage {
    let bytes = index.to_be_bytes();
    ProjectedMessage {
        project_task_target: None,
        message_id: MessageId::generate(),
        participant_id: None,
        role: MessageRole::User,
        content_ref: ContentRef {
            content_id: ContentId::generate(),
            sha256: Sha256::digest(bytes),
            media_type: MediaType::new("text/plain").expect("media type"),
            byte_length: index,
        },
        reasoning_summaries: Vec::new(),
        status: ProjectedMessageStatus::Committed,
        source_run_id: None,
        committed_at: UtcTimestamp::from_unix_millis(1_800_000_000_000 + index as i64),
        attachment_refs: Vec::new(),
        requested_skill_refs: Vec::new(),
        imported_projection_ref: None,
        assistant_output: None,
        partial_reason: None,
    }
}

struct TestCodec;

impl ConversationProjectionSnapshotCodec for TestCodec {
    fn checksum(&self, snapshot: &ConversationProjectionSnapshot) -> Sha256 {
        let bytes = snapshot
            .messages
            .iter()
            .flat_map(|message| message.message_id.to_string().into_bytes())
            .collect::<Vec<_>>();
        Sha256::digest(bytes)
    }

    fn encode(
        &self,
        snapshot: &ConversationProjectionSnapshot,
    ) -> Result<Vec<u8>, ProjectionSnapshotCodecError> {
        Ok(snapshot.checksum.into_bytes().to_vec())
    }

    fn decode(
        &self,
        _bytes: &[u8],
    ) -> Result<ConversationProjectionSnapshot, ProjectionSnapshotCodecError> {
        Err(ProjectionSnapshotCodecError::Malformed)
    }
}
