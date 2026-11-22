use musteragent_conversation::{ConversationEvent, ConversationKind, ConversationLifecycle};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{ConversationId, HomeEpoch, LedgerRecordId, TenantId, UtcTimestamp};
use musteragent_projection::{
    CatalogActivityPosition, CatalogAttentionAudience, CatalogEventContext,
    CatalogProjectionApplyDisposition, CatalogSourceClaim, ConversationCatalogProjection,
    adapt_conversation_event,
};

fn context(projection: &ConversationCatalogProjection, at: i64) -> CatalogEventContext {
    let at = UtcTimestamp::from_unix_millis(at);
    CatalogEventContext {
        control_home: projection.control_home,
        home_epoch: projection.home_epoch,
        tenant_id: projection.tenant_id,
        conversation_id: projection.conversation_id,
        record_id: LedgerRecordId::generate(),
        audience: CatalogAttentionAudience::AllParticipants,
        occurred_at: at,
        committed_at: at,
    }
}

#[test]
fn unarchive_restores_active_lifecycle_and_every_transition_is_projected() {
    let mut projection = ConversationCatalogProjection::new(
        RuntimeHome::Cloud,
        HomeEpoch::new(1).expect("valid initial Home epoch"),
        TenantId::generate(),
        ConversationId::generate(),
        None,
        ConversationKind::Direct,
        ConversationLifecycle::Active,
        UtcTimestamp::from_unix_millis(1_000),
    );
    let transitions = [
        (ConversationEvent::Archived, ConversationLifecycle::Archived),
        (ConversationEvent::Unarchived, ConversationLifecycle::Active),
        (ConversationEvent::Archived, ConversationLifecycle::Archived),
        (ConversationEvent::Unarchived, ConversationLifecycle::Active),
    ];
    let mut position = 0;
    for (index, (event, lifecycle)) in transitions.into_iter().enumerate() {
        let at = 2_000 + i64::try_from(index).expect("small index") * 1_000;
        let mut drafts =
            adapt_conversation_event(context(&projection, at), &event).expect("adapt lifecycle");
        assert_eq!(drafts.len(), 1);
        let draft = drafts.remove(0);
        let revision = projection.catalog_revision;
        position += 1;
        assert_eq!(
            projection
                .apply(
                    draft.clone().claimed(CatalogSourceClaim::New),
                    CatalogActivityPosition::new(position)
                )
                .expect("apply lifecycle change"),
            CatalogProjectionApplyDisposition::Applied,
            "{event:?}"
        );
        assert_eq!(projection.lifecycle, lifecycle);
        // A visible catalog change is published for every transition.
        assert!(projection.catalog_revision > revision);
        assert_eq!(
            projection.metadata_updated_at,
            UtcTimestamp::from_unix_millis(at)
        );
        // Replaying the same Ledger record is a duplicate, never a new change.
        let before = projection.clone();
        position += 1;
        assert_eq!(
            projection
                .apply(
                    draft.claimed(CatalogSourceClaim::Duplicate),
                    CatalogActivityPosition::new(position)
                )
                .expect("replay lifecycle change"),
            CatalogProjectionApplyDisposition::Duplicate
        );
        assert_eq!(projection, before);
    }
}
