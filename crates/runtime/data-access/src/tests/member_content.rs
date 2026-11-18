use super::*;
use musteragent_identity::WorkspaceId;
use musteragent_runtime_contract::{ErrorCode, ErrorDetails, SafeDisplayText};

struct Membership {
    actor: Actor,
    before: ConversationView,
    after: ConversationView,
    reject_at: Option<usize>,
    reads: AtomicUsize,
}
impl ConversationReadAuthority for Membership {
    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        let call = self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if Some(call) == self.reject_at
                || query.actor != self.actor
                || query.conversation_id != self.before.conversation.conversation_id
            {
                let code = ErrorCode::ParticipantNotAuthorized;
                return Err(RuntimeError {
                    schema_version: DottedKey::new("musteragent.runtime.error.v2").expect("schema"),
                    code,
                    category: code.category(),
                    message: SafeDisplayText::new("Membership required").expect("safe"),
                    retryable: false,
                    retry_after_ms: None,
                    resource_ref: None,
                    correlation_id: CorrelationId::generate(),
                    details: Box::new(ErrorDetails::None),
                });
            }
            Ok(if call == 0 {
                self.before.clone()
            } else {
                self.after.clone()
            })
        })
    }
}
struct Fixture {
    app: RuntimeDataApplication,
    membership: Arc<Membership>,
    content: Arc<BatchContentProbe>,
    projection: Arc<ProjectionProbe>,
    reference: ContentRef,
    snapshot_id: ProjectionSnapshotId,
    query: GetConversation,
}
fn fixture(change: impl FnOnce(&mut Membership)) -> Fixture {
    let tenant = TenantId::generate();
    let conversation = ConversationId::generate();
    let actor = claims(tenant, conversation).actor;
    let mut view = conversation_view(tenant, conversation, 9);
    // An explicit historical Snapshot remains accessible to a current member
    // even when the transport-independent authority now has a newer epoch.
    view.conversation.home_epoch = HomeEpoch::new(2).expect("epoch");
    view.latest_cursor.home_epoch = view.conversation.home_epoch;
    let mut membership = Membership {
        actor,
        before: view.clone(),
        after: view,
        reject_at: None,
        reads: AtomicUsize::new(0),
    };
    change(&mut membership);
    let membership = Arc::new(membership);
    let bytes = b"authorized historical body".to_vec();
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("media"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let snapshot_id = ProjectionSnapshotId::generate();
    let exact = snapshot(tenant, conversation, snapshot_id, vec![reference.clone()]);
    let projection = Arc::new(ProjectionProbe {
        exact,
        current: snapshot(
            tenant,
            conversation,
            ProjectionSnapshotId::generate(),
            Vec::new(),
        ),
        exact_reads: AtomicUsize::new(0),
        bounded_reads: AtomicUsize::new(0),
    });
    let content = Arc::new(BatchContentProbe {
        tenant_id: tenant,
        entries: HashMap::from([(reference.clone(), bytes)]),
        batch_reads: AtomicUsize::new(0),
        scalar_reads: AtomicUsize::new(0),
    });
    let app = RuntimeDataApplication::new(
        projection.clone(),
        Arc::new(UnusedLedger),
        content.clone(),
        Arc::new(NoCurrentArtifact),
        Arc::new(TestCodec),
    )
    .with_conversation_authority(membership.clone());
    Fixture {
        app,
        membership,
        content,
        projection,
        reference,
        snapshot_id,
        query: GetConversation {
            actor,
            conversation_id: conversation,
        },
    }
}
async fn read(f: &Fixture) -> Result<Vec<Vec<u8>>, RuntimeDataAccessError> {
    f.app
        .read_member_content_batch(
            f.query,
            std::slice::from_ref(&f.reference),
            Some(ContentReadAuthority::ProjectionSnapshot(f.snapshot_id)),
        )
        .await
}

#[tokio::test]
async fn native_membership_reuses_exact_reachability_without_a_fabricated_ticket() {
    let f = fixture(|_| {});
    assert_eq!(
        read(&f).await.expect("historical membership"),
        vec![b"authorized historical body".to_vec()]
    );
    assert_eq!(f.membership.reads.load(Ordering::SeqCst), 2);
    assert_eq!(f.content.batch_reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.content.scalar_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.projection.exact_reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.projection.bounded_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_content_cannot_escape_membership_revocation_before_or_during_io() {
    for reject_at in [0, 1] {
        let f = fixture(|membership| membership.reject_at = Some(reject_at));
        assert!(
            matches!(read(&f).await, Err(RuntimeDataAccessError::Conversation(error)) if error.code == ErrorCode::ParticipantNotAuthorized)
        );
        assert_eq!(f.content.batch_reads.load(Ordering::SeqCst), reject_at);
    }
    let mut f = fixture(|_| {});
    f.query.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        read(&f).await,
        Err(RuntimeDataAccessError::Conversation(_))
    ));
    assert_eq!(f.content.batch_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_content_fences_changed_ownership_and_foreign_authority_responses() {
    for variant in 0..5 {
        let f = fixture(|membership| match variant {
            0 => membership.after.conversation.workspace_id = Some(WorkspaceId::generate()),
            1 => membership.after.conversation.tenant_id = TenantId::generate(),
            2 => membership.after.conversation.conversation_id = ConversationId::generate(),
            3 => membership.after.conversation.home_epoch = HomeEpoch::new(3).expect("epoch"),
            _ => membership.before.conversation.tenant_id = TenantId::generate(),
        });
        assert!(matches!(
            read(&f).await,
            Err(RuntimeDataAccessError::BootstrapContractInvalid)
        ));
        assert_eq!(
            f.content.batch_reads.load(Ordering::SeqCst),
            usize::from(variant != 4)
        );
    }
}

#[tokio::test]
async fn native_content_has_no_membership_only_or_empty_batch_shortcut() {
    let mut f = fixture(|_| {});
    f.reference.sha256 = Sha256::digest(b"same id does not grant different content");
    assert!(matches!(
        read(&f).await,
        Err(RuntimeDataAccessError::ContentNotReachable)
    ));
    assert_eq!(f.content.batch_reads.load(Ordering::SeqCst), 0);
    let denied = fixture(|membership| membership.reject_at = Some(0));
    assert!(matches!(
        denied
            .app
            .read_member_content_batch(denied.query, &[], None)
            .await,
        Err(RuntimeDataAccessError::Conversation(_))
    ));
    let f = fixture(|_| {});
    assert!(
        f.app
            .read_member_content_batch(f.query, &[], None)
            .await
            .expect("empty authorized result")
            .is_empty()
    );
    assert_eq!(f.membership.reads.load(Ordering::SeqCst), 2);
    assert_eq!(f.content.batch_reads.load(Ordering::SeqCst), 0);
}
