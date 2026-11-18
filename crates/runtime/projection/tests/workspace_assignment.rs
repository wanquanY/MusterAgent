use musteragent_conversation::{ConversationEvent, ConversationKind, ConversationLifecycle};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    CommandId, ConversationId, HomeEpoch, LedgerRecordId, TenantId, UtcTimestamp, WorkspaceId,
};
use musteragent_projection::{
    AttentionRevision, CatalogActivityPosition, CatalogAttentionAudience, CatalogEventContext,
    CatalogProjectionApplyDisposition, CatalogSourceClaim, ConversationCatalogProjection,
    adapt_conversation_event,
};

#[test]
fn assignment_rebuilds_ownership_without_activity_or_unread_and_rejects_reassignment() {
    let initial = UtcTimestamp::from_unix_millis(1000);
    let later = UtcTimestamp::from_unix_millis(2000);
    let mut projection = ConversationCatalogProjection::new(
        RuntimeHome::Cloud,
        HomeEpoch::new(1).expect("valid initial Home epoch"),
        TenantId::generate(),
        ConversationId::generate(),
        None,
        ConversationKind::Direct,
        ConversationLifecycle::Archived,
        initial,
    );
    let context = CatalogEventContext {
        control_home: projection.control_home,
        home_epoch: projection.home_epoch,
        tenant_id: projection.tenant_id,
        conversation_id: projection.conversation_id,
        record_id: LedgerRecordId::generate(),
        audience: CatalogAttentionAudience::AllParticipants,
        occurred_at: later,
        committed_at: later,
    };
    let workspace_id = WorkspaceId::generate();
    let event = ConversationEvent::WorkspaceAssigned {
        workspace_id,
        assignment_command_id: CommandId::generate(),
    };
    let drafts = adapt_conversation_event(context, &event).expect("adapt workspace assignment");
    assert_eq!(drafts.len(), 1);
    assert_eq!(
        projection
            .apply(
                drafts[0].clone().claimed(CatalogSourceClaim::New),
                CatalogActivityPosition::new(1)
            )
            .expect("apply first workspace assignment"),
        CatalogProjectionApplyDisposition::Applied
    );
    assert_eq!(projection.workspace_id, Some(workspace_id));
    assert_eq!(projection.last_user_visible_activity_at, initial);
    assert_eq!(
        projection.last_activity_position,
        CatalogActivityPosition::ZERO
    );
    assert_eq!(projection.attention_revision, AttentionRevision::ZERO);
    assert!(projection.latest_attention.is_none());
    assert_eq!(projection.lifecycle, ConversationLifecycle::Archived);
    let before = projection.clone();
    assert_eq!(
        projection
            .apply(
                drafts[0].clone().claimed(CatalogSourceClaim::Duplicate),
                CatalogActivityPosition::new(2)
            )
            .expect("replay workspace assignment"),
        CatalogProjectionApplyDisposition::Duplicate
    );
    assert_eq!(projection, before);
    let other = ConversationEvent::WorkspaceAssigned {
        workspace_id: WorkspaceId::generate(),
        assignment_command_id: CommandId::generate(),
    };
    let draft = adapt_conversation_event(context, &other)
        .expect("adapt attempted reassignment")
        .remove(0);
    assert!(
        projection
            .apply(
                draft.claimed(CatalogSourceClaim::New),
                CatalogActivityPosition::new(2)
            )
            .is_err()
    );
    assert_eq!(projection, before);
}
