use super::*;
use musteragent_execution_context::{DialogueContextBound, DialogueContextCut, DialogueCursor};
use musteragent_identity::{AgentDialogueId, AgentId, PrincipalId};

#[tokio::test]
async fn private_dialogue_source_never_becomes_a_product_message_or_status() {
    let tenant = TenantId::generate();
    let conversation = ConversationId::generate();
    let epoch = HomeEpoch::new(1).unwrap();
    let content: Arc<dyn ProjectionContentStore> = Arc::new(NoIoContent);
    let mut reducer =
        ProductProjectionReducer::new(tenant, conversation, epoch, None, BTreeMap::new()).unwrap();
    let cut = DialogueContextCut {
        tenant_id: tenant,
        owner_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
        dialogue_id: AgentDialogueId::generate(),
        through: DialogueCursor { epoch: 1, seq: 1 },
        entry_count: 1,
        source_chain_sha256: Sha256::digest(b"frozen public source"),
    };
    let binding = DialogueContextBound {
        frozen_cut: cut,
        page_ordinal: std::num::NonZeroU32::MIN,
        from_seq: 1,
        through_seq: 1,
        source_page_ref: content_ref(b"private history bytes"),
        checkpoint_id: None,
        completes_cut: true,
    };
    binding.validate().unwrap();
    reducer
        .apply(
            record(
                tenant,
                conversation,
                epoch,
                1,
                Some(RunId::generate()),
                None,
                DomainEvent::Context(ContextEvent::DialogueBound {
                    binding: Box::new(binding),
                }),
            ),
            content.clone(),
        )
        .await
        .unwrap();
    reducer.advance_through(LedgerOffset::new(1)).unwrap();
    let snapshot = reducer
        .finish(
            content,
            LedgerOffset::new(1),
            UtcTimestamp::from_unix_millis(100),
        )
        .await
        .unwrap();
    assert!(snapshot.messages.is_empty());
    assert!(snapshot.runs.is_empty());
    assert!(snapshot.context_compactions.is_empty());
}
