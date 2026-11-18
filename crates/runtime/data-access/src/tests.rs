use std::collections::{BTreeSet, HashMap};
mod member_content;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use musteragent_approval::{InteractionEvent, InteractionEventResponse};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::{
    Conversation, ConversationKind, ConversationLifecycle, ConversationView, GetConversation,
    MessageRole,
};
use musteragent_event_envelope::RuntimeHome;
use musteragent_event_envelope::{LedgerCursor, LedgerMetadata, LedgerRecordKind, Producer};
use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, ContentId, ConversationId, ConversationSeq,
    CorrelationId, DottedKey, FrameId, HomeEpoch, IdempotencyKey, InteractionId, LedgerOffset,
    LedgerRecordId, MessageId, OutputId, ParticipantId, PrincipalId, PrincipalKind,
    ProjectionSnapshotId, RuntimeDataTicketId, TenantId, UtcTimestamp,
};
use musteragent_ledger::{
    CanonicalLedgerRecord, DomainEvent, LedgerApplication, LedgerPayload, ListConversationRecords,
    RecordPage, SubscribeConversationRecords,
};
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshot,
    ConversationProjectionSnapshotCodec, ConversationProjectionWindow, GetConversationProjection,
    GetConversationProjectionWindow, PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedMessage,
    ProjectedMessageStatus, ProjectionSnapshotCodecError,
};
use musteragent_runtime_content::{
    ArtifactContentAuthorization, ContentStore, ContentStoreError, PutContent,
};
use musteragent_runtime_contract::{
    ApplicationFuture, ApplicationStream, FrameContent, OutputChannel, OutputFrame, RuntimeError,
};

use super::{
    ContentReadAuthority, ConversationDataGrant, ConversationReadAuthority,
    RUNTIME_DATA_CONTRACT_VERSION, RUNTIME_DATA_TICKET_AUDIENCE,
    RUNTIME_DATA_TICKET_SCHEMA_VERSION, RuntimeDataAccessError, RuntimeDataAccessTicketClaims,
    RuntimeDataApplication, RuntimeDataScope,
};

#[tokio::test]
async fn conversation_bootstrap_accepts_a_control_view_captured_after_the_projection() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let mut projection = snapshot(
        tenant_id,
        conversation_id,
        ProjectionSnapshotId::generate(),
        Vec::new(),
    );
    projection.through_conversation_seq = LedgerOffset::new(7);
    let projection = Arc::new(ProjectionProbe {
        exact: projection.clone(),
        current: projection,
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection,
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id,
            reference: empty_reference(),
            bytes: Vec::new(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    )
    .with_conversation_authority(Arc::new(StaticConversation {
        view: conversation_view(tenant_id, conversation_id, 9),
    }));

    let bootstrap = application
        .get_conversation_read_bootstrap(
            &claims(tenant_id, conversation_id),
            conversation_id,
            40,
            None,
            true,
        )
        .await
        .expect("a later control view covers the captured Projection");

    assert_eq!(
        bootstrap.projection.through_conversation_seq,
        LedgerOffset::new(7)
    );
    assert_eq!(
        bootstrap.observed_head_cursor.after_conversation_seq.get(),
        9
    );
}

#[tokio::test]
async fn conversation_bootstrap_eagerly_reads_presentation_content_from_the_same_snapshot() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let bytes = b"one coherent projection payload".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let mut current = snapshot(
        tenant_id,
        conversation_id,
        ProjectionSnapshotId::generate(),
        vec![reference.clone()],
    );
    current.messages.push(ProjectedMessage {
        project_task_target: None,
        message_id: MessageId::generate(),
        participant_id: None,
        role: MessageRole::Assistant,
        content_ref: reference.clone(),
        reasoning_summaries: Vec::new(),
        status: ProjectedMessageStatus::Committed,
        source_run_id: None,
        committed_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
        attachment_refs: Vec::new(),
        requested_skill_refs: Vec::new(),
        imported_projection_ref: None,
        assistant_output: None,
        partial_reason: None,
    });
    let application = RuntimeDataApplication::new(
        Arc::new(ProjectionProbe {
            exact: current.clone(),
            current,
            exact_reads: AtomicUsize::new(0),
            bounded_reads: AtomicUsize::new(0),
        }),
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id,
            reference: reference.clone(),
            bytes: bytes.clone(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    )
    .with_conversation_authority(Arc::new(StaticConversation {
        view: conversation_view(tenant_id, conversation_id, 1),
    }));

    let bootstrap = application
        .get_conversation_read_bootstrap(
            &claims(tenant_id, conversation_id),
            conversation_id,
            40,
            None,
            true,
        )
        .await
        .expect("bootstrap content is read with the captured projection");

    assert_eq!(bootstrap.eager_content, Some(vec![(reference, bytes)]));
}

#[tokio::test]
async fn conversation_bootstrap_reports_read_skew_instead_of_invalid_ownership() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let mut projection = snapshot(
        tenant_id,
        conversation_id,
        ProjectionSnapshotId::generate(),
        Vec::new(),
    );
    projection.through_conversation_seq = LedgerOffset::new(7);
    let projection = Arc::new(ProjectionProbe {
        exact: projection.clone(),
        current: projection,
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection,
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id,
            reference: empty_reference(),
            bytes: Vec::new(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    )
    .with_conversation_authority(Arc::new(StaticConversation {
        view: conversation_view(tenant_id, conversation_id, 6),
    }));

    let result = application
        .get_conversation_read_bootstrap(
            &claims(tenant_id, conversation_id),
            conversation_id,
            40,
            None,
            true,
        )
        .await;

    assert!(matches!(
        result,
        Err(RuntimeDataAccessError::BootstrapReadSkew)
    ));
}

#[tokio::test]
async fn exact_snapshot_authority_survives_a_moving_projection_head() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let authority_snapshot_id = ProjectionSnapshotId::generate();
    let current_snapshot_id = ProjectionSnapshotId::generate();
    let bytes = b"checkpoint visible in the issued snapshot".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let projection = Arc::new(ProjectionProbe {
        exact: snapshot(
            tenant_id,
            conversation_id,
            authority_snapshot_id,
            vec![reference.clone()],
        ),
        current: snapshot(tenant_id, conversation_id, current_snapshot_id, Vec::new()),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id,
            reference: reference.clone(),
            bytes: bytes.clone(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    );
    let claims = claims(tenant_id, conversation_id);

    let resolved = application
        .read_content(
            &claims,
            conversation_id,
            &reference,
            Some(ContentReadAuthority::ProjectionSnapshot(
                authority_snapshot_id,
            )),
        )
        .await
        .expect("the immutable Snapshot is an exact content authority");
    assert_eq!(resolved, bytes);
    assert_eq!(projection.exact_reads.load(Ordering::Relaxed), 1);
    assert_eq!(projection.bounded_reads.load(Ordering::Relaxed), 0);

    let moving_head = application
        .read_content(&claims, conversation_id, &reference, None)
        .await;
    assert!(matches!(
        moving_head,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
    assert_eq!(projection.bounded_reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn exact_artifact_version_authority_reads_historical_content_only_for_that_version() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let artifact_id = ArtifactId::generate();
    let artifact_version_id = ArtifactVersionId::generate();
    let bytes = b"immutable historical Artifact body".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/markdown;charset=utf-8").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let projection = Arc::new(ProjectionProbe {
        exact: snapshot(
            tenant_id,
            conversation_id,
            ProjectionSnapshotId::generate(),
            Vec::new(),
        ),
        current: snapshot(
            tenant_id,
            conversation_id,
            ProjectionSnapshotId::generate(),
            Vec::new(),
        ),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id,
            reference: reference.clone(),
            bytes: bytes.clone(),
        }),
        Arc::new(ExactArtifactVersion {
            artifact_id,
            artifact_version_id,
            reference: reference.clone(),
        }),
        Arc::new(TestCodec),
    );
    let claims = claims(tenant_id, conversation_id);

    let resolved = application
        .read_content(
            &claims,
            conversation_id,
            &reference,
            Some(ContentReadAuthority::ArtifactVersion {
                artifact_id,
                artifact_version_id,
            }),
        )
        .await
        .expect("the exact historical Artifact version authorizes its body");
    assert_eq!(resolved, bytes);
    assert_eq!(projection.exact_reads.load(Ordering::Relaxed), 0);
    assert_eq!(projection.bounded_reads.load(Ordering::Relaxed), 0);

    let wrong_version = application
        .read_content(
            &claims,
            conversation_id,
            &reference,
            Some(ContentReadAuthority::ArtifactVersion {
                artifact_id,
                artifact_version_id: ArtifactVersionId::generate(),
            }),
        )
        .await;
    assert!(matches!(
        wrong_version,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
}

#[tokio::test]
async fn exact_snapshot_batch_loads_authority_and_content_once() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let authority_snapshot_id = ProjectionSnapshotId::generate();
    let entries = (0..130)
        .map(|index| {
            let bytes = format!("historical-content-{index}").into_bytes();
            let reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            };
            (reference, bytes)
        })
        .collect::<Vec<_>>();
    let references = entries
        .iter()
        .map(|(reference, _)| reference.clone())
        .collect::<Vec<_>>();
    let expected = entries
        .iter()
        .map(|(_, bytes)| bytes.clone())
        .collect::<Vec<_>>();
    let projection = Arc::new(ProjectionProbe {
        exact: snapshot(
            tenant_id,
            conversation_id,
            authority_snapshot_id,
            references.clone(),
        ),
        current: snapshot(
            tenant_id,
            conversation_id,
            ProjectionSnapshotId::generate(),
            Vec::new(),
        ),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let artifact = Arc::new(BatchArtifactProbe {
        batch_reads: AtomicUsize::new(0),
        scalar_reads: AtomicUsize::new(0),
    });
    let content = Arc::new(BatchContentProbe {
        tenant_id,
        entries: entries.into_iter().collect(),
        batch_reads: AtomicUsize::new(0),
        scalar_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        content.clone(),
        artifact.clone(),
        Arc::new(TestCodec),
    );

    let resolved = application
        .read_content_batch(
            &claims(tenant_id, conversation_id),
            conversation_id,
            &references,
            Some(ContentReadAuthority::ProjectionSnapshot(
                authority_snapshot_id,
            )),
        )
        .await
        .expect("one exact Snapshot authorizes the complete content batch");

    assert_eq!(resolved, expected);
    assert_eq!(projection.exact_reads.load(Ordering::Relaxed), 1);
    assert_eq!(projection.bounded_reads.load(Ordering::Relaxed), 0);
    assert_eq!(artifact.batch_reads.load(Ordering::Relaxed), 1);
    assert_eq!(artifact.scalar_reads.load(Ordering::Relaxed), 0);
    assert_eq!(content.batch_reads.load(Ordering::Relaxed), 1);
    assert_eq!(content.scalar_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn content_batch_fails_closed_before_storage_when_one_reference_is_unreachable() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let authority_snapshot_id = ProjectionSnapshotId::generate();
    let entries = [b"reachable".to_vec(), b"not-reachable".to_vec()]
        .into_iter()
        .map(|bytes| {
            let reference = ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain;charset=utf-8").expect("media type"),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(&bytes),
            };
            (reference, bytes)
        })
        .collect::<Vec<_>>();
    let references = entries
        .iter()
        .map(|(reference, _)| reference.clone())
        .collect::<Vec<_>>();
    let projection = Arc::new(ProjectionProbe {
        exact: snapshot(
            tenant_id,
            conversation_id,
            authority_snapshot_id,
            vec![references[0].clone()],
        ),
        current: snapshot(
            tenant_id,
            conversation_id,
            ProjectionSnapshotId::generate(),
            Vec::new(),
        ),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let artifact = Arc::new(BatchArtifactProbe {
        batch_reads: AtomicUsize::new(0),
        scalar_reads: AtomicUsize::new(0),
    });
    let content = Arc::new(BatchContentProbe {
        tenant_id,
        entries: entries.into_iter().collect(),
        batch_reads: AtomicUsize::new(0),
        scalar_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        content.clone(),
        artifact.clone(),
        Arc::new(TestCodec),
    );

    let result = application
        .read_content_batch(
            &claims(tenant_id, conversation_id),
            conversation_id,
            &references,
            Some(ContentReadAuthority::ProjectionSnapshot(
                authority_snapshot_id,
            )),
        )
        .await;

    assert!(matches!(
        result,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
    assert_eq!(projection.exact_reads.load(Ordering::Relaxed), 1);
    assert_eq!(artifact.batch_reads.load(Ordering::Relaxed), 1);
    assert_eq!(content.batch_reads.load(Ordering::Relaxed), 0);
    assert_eq!(content.scalar_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn exact_ledger_record_authority_survives_projection_lag() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("home epoch");
    let conversation_seq = ConversationSeq::new(83).expect("conversation sequence");
    let record_id = LedgerRecordId::generate();
    let bytes = b"live frame committed before projection head".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let record = CanonicalLedgerRecord {
        metadata: LedgerMetadata {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
            record_id,
            record_kind: LedgerRecordKind::OutputFrame,
            tenant_id,
            conversation_id,
            conversation_seq,
            activity_id: None,
            run_id: None,
            run_seq: None,
            attempt_id: None,
            participant_id: None,
            home_epoch,
            producer: Producer::Worker,
            occurred_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
            committed_at: UtcTimestamp::from_unix_millis(1_800_000_000_001),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            idempotency_key: IdempotencyKey::new("record-authority-test").expect("idempotency"),
            payload_hash: Sha256::digest(b"record"),
        },
        payload: LedgerPayload::OutputFrame(OutputFrame {
            frame_id: FrameId::generate(),
            output_id: OutputId::generate(),
            message_id: None,
            channel: OutputChannel::ToolProgress,
            frame_index: 0,
            byte_offset_start: 0,
            byte_offset_end: reference.byte_length,
            content: FrameContent::ContentRef(reference.clone()),
            content_hash: reference.sha256,
            final_for_output: true,
        }),
    };
    let application = RuntimeDataApplication::new(
        Arc::new(ProjectionProbe {
            exact: snapshot(
                tenant_id,
                conversation_id,
                ProjectionSnapshotId::generate(),
                Vec::new(),
            ),
            current: snapshot(
                tenant_id,
                conversation_id,
                ProjectionSnapshotId::generate(),
                Vec::new(),
            ),
            exact_reads: AtomicUsize::new(0),
            bounded_reads: AtomicUsize::new(0),
        }),
        Arc::new(ExactRecordLedger {
            record: record.clone(),
        }),
        Arc::new(StaticContent {
            tenant_id,
            reference: reference.clone(),
            bytes: bytes.clone(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    );
    let claims = claims(tenant_id, conversation_id);

    let resolved = application
        .read_content(
            &claims,
            conversation_id,
            &reference,
            Some(ContentReadAuthority::LedgerRecord {
                record_id,
                conversation_seq,
                home_epoch,
            }),
        )
        .await
        .expect("the exact live Ledger record is an immutable content authority");
    assert_eq!(resolved, bytes);

    let wrong_record = application
        .read_content(
            &claims,
            conversation_id,
            &reference,
            Some(ContentReadAuthority::LedgerRecord {
                record_id: LedgerRecordId::generate(),
                conversation_seq,
                home_epoch,
            }),
        )
        .await;
    assert!(matches!(
        wrong_record,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
}

#[tokio::test]
async fn exact_interaction_response_record_authorizes_its_public_structured_content() {
    let tenant_id = TenantId::generate();
    let conversation_id = ConversationId::generate();
    let home_epoch = HomeEpoch::new(1).expect("home epoch");
    let conversation_seq = ConversationSeq::new(84).expect("conversation sequence");
    let record_id = LedgerRecordId::generate();
    let bytes = br#"{"value":"keep the generated script"}"#.to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let record = CanonicalLedgerRecord {
        metadata: LedgerMetadata {
            schema_version: DottedKey::new("musteragent.runtime.record.v1").expect("schema"),
            record_id,
            record_kind: LedgerRecordKind::DomainEvent,
            tenant_id,
            conversation_id,
            conversation_seq,
            activity_id: None,
            run_id: None,
            run_seq: None,
            attempt_id: None,
            participant_id: None,
            home_epoch,
            producer: Producer::RuntimeApi,
            occurred_at: UtcTimestamp::from_unix_millis(1_800_000_000_002),
            committed_at: UtcTimestamp::from_unix_millis(1_800_000_000_003),
            causation_id: None,
            correlation_id: CorrelationId::generate(),
            idempotency_key: IdempotencyKey::new("interaction-response-authority-test")
                .expect("idempotency"),
            payload_hash: Sha256::digest(b"interaction-response-record"),
        },
        payload: LedgerPayload::DomainEvent(DomainEvent::Interaction(
            InteractionEvent::Responded {
                interaction_id: InteractionId::generate(),
                responded_by: PrincipalId::generate(),
                response: InteractionEventResponse::Content(reference.clone()),
                responded_at: UtcTimestamp::from_unix_millis(1_800_000_000_002),
            },
        )),
    };
    let application = RuntimeDataApplication::new(
        Arc::new(ProjectionProbe {
            exact: snapshot(
                tenant_id,
                conversation_id,
                ProjectionSnapshotId::generate(),
                Vec::new(),
            ),
            current: snapshot(
                tenant_id,
                conversation_id,
                ProjectionSnapshotId::generate(),
                Vec::new(),
            ),
            exact_reads: AtomicUsize::new(0),
            bounded_reads: AtomicUsize::new(0),
        }),
        Arc::new(ExactRecordLedger {
            record: record.clone(),
        }),
        Arc::new(StaticContent {
            tenant_id,
            reference: reference.clone(),
            bytes: bytes.clone(),
        }),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    );

    let resolved = application
        .read_content(
            &claims(tenant_id, conversation_id),
            conversation_id,
            &reference,
            Some(ContentReadAuthority::LedgerRecord {
                record_id,
                conversation_seq,
                home_epoch,
            }),
        )
        .await
        .expect("the exact Interaction response record authorizes public structured content");

    assert_eq!(resolved, bytes);
}

struct ProjectionProbe {
    exact: ConversationProjectionSnapshot,
    current: ConversationProjectionSnapshot,
    exact_reads: AtomicUsize,
    bounded_reads: AtomicUsize,
}

impl ConversationProjectionApplication for ProjectionProbe {
    fn get_product_read_projection_window(
        &self,
        _query: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>> {
        let projection = self.current.clone();
        Box::pin(async move {
            Ok(ConversationProjectionWindow {
                authority_snapshot_id: projection.snapshot_id,
                head_cursor: projection.cursor(),
                projection,
                older_cursor: None,
                has_older: false,
            })
        })
    }

    fn get_conversation_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        self.exact_reads.fetch_add(1, Ordering::Relaxed);
        let snapshot = self.exact.clone();
        Box::pin(async move {
            assert_eq!(query.preferred_snapshot_id, Some(snapshot.snapshot_id));
            Ok(snapshot)
        })
    }

    fn get_head_projection(
        &self,
        _query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        let snapshot = self.current.clone();
        Box::pin(async move { Ok(snapshot) })
    }

    fn get_bounded_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        self.bounded_reads.fetch_add(1, Ordering::Relaxed);
        assert_eq!(query.preferred_snapshot_id, None);
        let snapshot = self.current.clone();
        Box::pin(async move { Ok(snapshot) })
    }
}

struct StaticContent {
    tenant_id: TenantId,
    reference: ContentRef,
    bytes: Vec<u8>,
}

struct StaticConversation {
    view: ConversationView,
}

impl ConversationReadAuthority for StaticConversation {
    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        let view = self.view.clone();
        Box::pin(async move {
            assert_eq!(query.actor.tenant_id, view.conversation.tenant_id);
            assert_eq!(query.conversation_id, view.conversation.conversation_id);
            Ok(view)
        })
    }
}

struct BatchContentProbe {
    tenant_id: TenantId,
    entries: HashMap<ContentRef, Vec<u8>>,
    batch_reads: AtomicUsize,
    scalar_reads: AtomicUsize,
}

impl ContentStore for BatchContentProbe {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { unreachable!("content read does not write") })
    }

    fn read<'a>(
        &'a self,
        _tenant_id: TenantId,
        _reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        self.scalar_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { unreachable!("optimized batch must not use scalar content reads") })
    }

    fn read_batch<'a>(
        &'a self,
        tenant_id: TenantId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>> {
        self.batch_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            assert_eq!(tenant_id, self.tenant_id);
            references
                .iter()
                .map(|reference| {
                    self.entries
                        .get(reference)
                        .cloned()
                        .ok_or(ContentStoreError::NotFound)
                })
                .collect()
        })
    }
}

impl ContentStore for StaticContent {
    fn put(
        &self,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { unreachable!("content read does not write") })
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        assert_eq!(tenant_id, self.tenant_id);
        let matches = reference == &self.reference;
        let bytes = self.bytes.clone();
        Box::pin(async move {
            if matches {
                Ok(bytes)
            } else {
                Err(ContentStoreError::NotFound)
            }
        })
    }
}

struct NoCurrentArtifact;

impl ArtifactContentAuthorization for NoCurrentArtifact {
    fn is_current_content_reachable(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _reference: &ContentRef,
    ) -> ApplicationFuture<'_, Result<bool, RuntimeError>> {
        Box::pin(async { Ok(false) })
    }

    fn resolve_current_content(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _content_id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, RuntimeError>> {
        Box::pin(async { Ok(None) })
    }
}

struct ExactArtifactVersion {
    artifact_id: ArtifactId,
    artifact_version_id: ArtifactVersionId,
    reference: ContentRef,
}

impl ArtifactContentAuthorization for ExactArtifactVersion {
    fn is_current_content_reachable(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _reference: &ContentRef,
    ) -> ApplicationFuture<'_, Result<bool, RuntimeError>> {
        Box::pin(async { Ok(false) })
    }

    fn version_content_reachability<'a>(
        &'a self,
        _actor: Actor,
        _conversation_id: ConversationId,
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        let expected_artifact = self.artifact_id;
        let expected_version = self.artifact_version_id;
        let expected_reference = self.reference.clone();
        Box::pin(async move {
            Ok(references
                .iter()
                .map(|reference| {
                    artifact_id == expected_artifact
                        && artifact_version_id == expected_version
                        && reference == &expected_reference
                })
                .collect())
        })
    }

    fn workspace_version_content_reachability<'a>(
        &'a self,
        _actor: Actor,
        _workspace_id: musteragent_identity::WorkspaceId,
        artifact_id: ArtifactId,
        version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move {
            Ok(references
                .iter()
                .map(|reference| {
                    artifact_id == self.artifact_id
                        && version_id == self.artifact_version_id
                        && reference == &self.reference
                })
                .collect())
        })
    }

    fn resolve_current_content(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _content_id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, RuntimeError>> {
        Box::pin(async { Ok(None) })
    }
}

struct BatchArtifactProbe {
    batch_reads: AtomicUsize,
    scalar_reads: AtomicUsize,
}

impl ArtifactContentAuthorization for BatchArtifactProbe {
    fn is_current_content_reachable(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _reference: &ContentRef,
    ) -> ApplicationFuture<'_, Result<bool, RuntimeError>> {
        self.scalar_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { unreachable!("optimized batch must not use scalar Artifact reads") })
    }

    fn current_content_reachability<'a>(
        &'a self,
        _actor: Actor,
        _conversation_id: ConversationId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        self.batch_reads.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(vec![false; references.len()]) })
    }

    fn resolve_current_content(
        &self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _content_id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, RuntimeError>> {
        Box::pin(async { Ok(None) })
    }
}

struct UnusedLedger;

impl LedgerApplication for UnusedLedger {
    fn list_conversation_records(
        &self,
        _query: ListConversationRecords,
    ) -> ApplicationFuture<'_, Result<RecordPage, RuntimeError>> {
        Box::pin(async { unreachable!("content read does not list records") })
    }

    fn subscribe_conversation_records(
        &self,
        _query: SubscribeConversationRecords,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>, RuntimeError>,
    > {
        Box::pin(async { unreachable!("content read does not subscribe") })
    }
}

struct ExactRecordLedger {
    record: CanonicalLedgerRecord,
}

impl LedgerApplication for ExactRecordLedger {
    fn list_conversation_records(
        &self,
        query: ListConversationRecords,
    ) -> ApplicationFuture<'_, Result<RecordPage, RuntimeError>> {
        let record = self.record.clone();
        Box::pin(async move {
            assert_eq!(query.conversation_id, record.metadata.conversation_id);
            assert_eq!(query.cursor.home_epoch, record.metadata.home_epoch);
            assert_eq!(
                query.cursor.after_conversation_seq.get() + 1,
                record.metadata.conversation_seq.get()
            );
            assert_eq!(query.limit, 1);
            let next_cursor = LedgerCursor {
                conversation_id: record.metadata.conversation_id,
                home_epoch: record.metadata.home_epoch,
                after_conversation_seq: record.metadata.conversation_seq.into(),
            };
            Ok(RecordPage {
                records: vec![record],
                next_cursor,
                has_more: false,
                projection_checkpoint: next_cursor,
            })
        })
    }

    fn subscribe_conversation_records(
        &self,
        _query: SubscribeConversationRecords,
    ) -> ApplicationFuture<
        '_,
        Result<ApplicationStream<Result<CanonicalLedgerRecord, RuntimeError>>, RuntimeError>,
    > {
        Box::pin(async { unreachable!("content authority does not subscribe") })
    }
}

struct TestCodec;

impl ConversationProjectionSnapshotCodec for TestCodec {
    fn checksum(&self, snapshot: &ConversationProjectionSnapshot) -> Sha256 {
        snapshot.checksum
    }

    fn encode(
        &self,
        _snapshot: &ConversationProjectionSnapshot,
    ) -> Result<Vec<u8>, ProjectionSnapshotCodecError> {
        Ok(vec![1])
    }

    fn decode(
        &self,
        _bytes: &[u8],
    ) -> Result<ConversationProjectionSnapshot, ProjectionSnapshotCodecError> {
        Err(ProjectionSnapshotCodecError::Malformed)
    }
}

fn snapshot(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    snapshot_id: ProjectionSnapshotId,
    content_manifest: Vec<ContentRef>,
) -> ConversationProjectionSnapshot {
    ConversationProjectionSnapshot {
        schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        snapshot_id,
        tenant_id,
        conversation_id,
        home_epoch: HomeEpoch::new(1).expect("home epoch"),
        through_conversation_seq: LedgerOffset::new(1),
        messages: Vec::new(),
        tool_timeline: Vec::new(),
        context_compactions: Vec::new(),
        pending_interactions: Vec::new(),
        activities: Vec::new(),
        runs: Vec::new(),
        active_runs: Vec::new(),
        artifacts: Vec::new(),
        stream_outputs: Vec::new(),
        content_manifest,
        checksum: Sha256::digest([]),
        created_at: UtcTimestamp::from_unix_millis(1_800_000_000_000),
    }
}

fn empty_reference() -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/octet-stream").expect("media type"),
        byte_length: 0,
        sha256: Sha256::digest([]),
    }
}

fn conversation_view(
    tenant_id: TenantId,
    conversation_id: ConversationId,
    latest_sequence: u64,
) -> ConversationView {
    let participant_id = ParticipantId::generate();
    let principal_id = PrincipalId::generate();
    let home_epoch = HomeEpoch::new(1).expect("home epoch");
    let created_at = UtcTimestamp::from_unix_millis(1_800_000_000_000);
    ConversationView {
        conversation: Conversation {
            conversation_id,
            tenant_id,
            workspace_id: None,
            kind: ConversationKind::Direct,
            execution_owner_participant_id: participant_id,
            title: None,
            control_home: RuntimeHome::Cloud,
            home_epoch,
            next_conversation_seq: ConversationSeq::new(latest_sequence + 1)
                .expect("next conversation sequence"),
            lifecycle: ConversationLifecycle::Active,
            created_by: principal_id,
            created_at,
            updated_at: created_at,
            archived_at: None,
        },
        participants: Vec::new(),
        latest_cursor: LedgerCursor {
            conversation_id,
            home_epoch,
            after_conversation_seq: LedgerOffset::new(latest_sequence),
        },
        active_activity_ids: Vec::new(),
        active_run_ids: Vec::new(),
        active_context_branch_id: None,
        context_branches: Vec::new(),
    }
}

fn claims(tenant_id: TenantId, conversation_id: ConversationId) -> RuntimeDataAccessTicketClaims {
    let issued_at = UtcTimestamp::from_unix_millis(1_800_000_000_000);
    RuntimeDataAccessTicketClaims {
        schema_version: DottedKey::new(RUNTIME_DATA_TICKET_SCHEMA_VERSION).expect("schema"),
        issuer: DottedKey::new("musteragent.legacy.test").expect("issuer"),
        audience: DottedKey::new(RUNTIME_DATA_TICKET_AUDIENCE).expect("audience"),
        ticket_id: RuntimeDataTicketId::generate(),
        actor: Actor {
            tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        workspace_file_grants: vec![],
        conversation_grants: vec![ConversationDataGrant {
            conversation_id,
            allowed_home_epoch: Some(HomeEpoch::new(1).expect("home epoch")),
            scopes: BTreeSet::from([
                RuntimeDataScope::ContentRead,
                RuntimeDataScope::ProjectionRead,
            ]),
        }],
        issued_at,
        expires_at: UtcTimestamp::from_unix_millis(issued_at.unix_millis() + 60_000),
        contract_version: DottedKey::new(RUNTIME_DATA_CONTRACT_VERSION).expect("contract"),
    }
}

#[tokio::test]
async fn workspace_file_ticket_reads_only_exact_version_without_conversation_projection() {
    use super::{WORKSPACE_FILE_TICKET_SCHEMA_VERSION, WorkspaceFileDataGrant};
    let tenant = TenantId::generate();
    let conversation = ConversationId::generate();
    let grant = WorkspaceFileDataGrant {
        workspace_id: musteragent_identity::WorkspaceId::generate(),
        artifact_id: ArtifactId::generate(),
        artifact_version_id: ArtifactVersionId::generate(),
    };
    let reference = empty_reference();
    let projection = Arc::new(ProjectionProbe {
        exact: snapshot(
            tenant,
            conversation,
            ProjectionSnapshotId::generate(),
            vec![],
        ),
        current: snapshot(
            tenant,
            conversation,
            ProjectionSnapshotId::generate(),
            vec![],
        ),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let application = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        Arc::new(StaticContent {
            tenant_id: tenant,
            reference: reference.clone(),
            bytes: vec![],
        }),
        Arc::new(ExactArtifactVersion {
            artifact_id: grant.artifact_id,
            artifact_version_id: grant.artifact_version_id,
            reference: reference.clone(),
        }),
        Arc::new(TestCodec),
    );
    let original = claims(tenant, conversation);
    let mut native = original.clone();
    native.schema_version =
        DottedKey::new(WORKSPACE_FILE_TICKET_SCHEMA_VERSION).expect("workspace ticket schema");
    native.conversation_grants.clear();
    native.workspace_file_grants = vec![grant];
    native.validate(native.issued_at).expect("workspace ticket");
    assert!(original.authorize_workspace_file(grant).is_err());
    assert!(
        native
            .authorize(conversation, RuntimeDataScope::ContentRead)
            .is_err()
    );
    assert_eq!(
        application
            .read_workspace_file_content(&native, grant, &reference)
            .await
            .expect("workspace content"),
        Vec::<u8>::new()
    );
    for wrong in [
        WorkspaceFileDataGrant {
            workspace_id: musteragent_identity::WorkspaceId::generate(),
            ..grant
        },
        WorkspaceFileDataGrant {
            artifact_id: ArtifactId::generate(),
            ..grant
        },
        WorkspaceFileDataGrant {
            artifact_version_id: ArtifactVersionId::generate(),
            ..grant
        },
    ] {
        assert!(matches!(
            application
                .read_workspace_file_content(&native, wrong, &reference)
                .await,
            Err(RuntimeDataAccessError::NotAuthorized)
        ));
    }
    let mut wrong_ref = reference;
    wrong_ref.sha256 = Sha256::digest(b"forged");
    assert!(matches!(
        application
            .read_workspace_file_content(&native, grant, &wrong_ref)
            .await,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
    assert_eq!(projection.exact_reads.load(Ordering::SeqCst), 0);
    assert_eq!(projection.bounded_reads.load(Ordering::SeqCst), 0);
    let mut mixed = native.clone();
    mixed.conversation_grants = original.conversation_grants;
    assert!(mixed.validate(mixed.issued_at).is_err());
    let mut duplicate = native.clone();
    duplicate.workspace_file_grants.push(grant);
    assert!(duplicate.validate(duplicate.issued_at).is_err());
    let mut old_schema = native;
    old_schema.schema_version =
        DottedKey::new(RUNTIME_DATA_TICKET_SCHEMA_VERSION).expect("conversation ticket schema");
    assert!(old_schema.validate(old_schema.issued_at).is_err());
}
