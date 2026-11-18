use super::*;

#[tokio::test]
async fn exact_published_reply_is_frozen_replayable_and_never_selected_by_name() {
    let f = Box::pin(fixture_with_reply_contract(true)).await;
    let published = PostgresDialoguePublicationStore::new(f.pool.clone())
        .append(f.command.clone())
        .await
        .unwrap();
    let store = PostgresAgentDialogueStore::new(f.pool.clone());
    let history = store
        .read_history(
            f.owner,
            f.intent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 20,
            },
        )
        .await
        .unwrap();
    let selected = history
        .entries
        .iter()
        .find(|e| e.entry.entry_id == published.entry.entry_id)
        .unwrap()
        .reply_target
        .expect("qualified exact publication hint");
    let mut command = input(f.owner, f.intent.dialogue_id);
    command.observed_cursor = published.entry.cursor;
    command.accepted_at = UtcTimestamp::from_unix_millis(300);
    command.reply_target = Some(selected);
    register_content(&f.pool, f.owner, &command.content_ref).await;
    let (first, retry) = tokio::join!(store.accept(command.clone()), store.accept(command.clone()));
    let first = first.unwrap();
    let retry = retry.unwrap();
    assert_eq!(first.intent, retry.intent);
    let binding = first.intent.reply_binding.as_ref().unwrap();
    assert_eq!(binding.target, selected);
    assert_eq!(
        binding.content_ref,
        match &f.command.content {
            PublicDeliveryContent::Message(c) => c.clone(),
            _ => unreachable!(),
        }
    );
    assert_eq!(
        binding.publication.as_ref().unwrap().run_id,
        f.command.fence.run_id
    );
    assert_eq!(
        binding.subject,
        DialogueReplySubject::IntentOutput {
            intent_id: f.intent.intent_id
        }
    );
    assert!(
        first.intent.reply_to_intent_id.is_none(),
        "new typed reply does not invent legacy input lineage"
    );
    let next_history = store
        .read_history(
            f.owner,
            f.intent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        next_history
            .entries
            .iter()
            .find(|e| e.entry.entry_id == first.intent.source_entry_id)
            .unwrap()
            .reply_context,
        Some(selected)
    );
    let mut changed = command.clone();
    changed.reply_target.as_mut().unwrap().visible_revision += 1;
    assert!(
        changed.validate().is_ok(),
        "a stale selection is a valid command shape"
    );
    assert_eq!(
        store.accept(changed.clone()).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let mut changed_entry = command.clone();
    changed_entry.reply_target.as_mut().unwrap().entry_id = first.intent.source_entry_id;
    assert_eq!(
        store.accept(changed_entry).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let mut removed = command.clone();
    removed.reply_target = None;
    assert_eq!(
        store.accept(removed).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    changed.command_id = CommandId::generate();
    assert_eq!(
        store.accept(changed).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    let other = Box::pin(super::super::receipt::attachment_fixture::activate_partner(
        &f.pool,
        f.owner,
        &f.activation,
        f.content.clone().unwrap(),
    ))
    .await;
    let mut changed_partner = command.clone();
    changed_partner.dialogue_id = other.agent.dialogue_id;
    assert_eq!(
        store.accept(changed_partner).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let mut wrong_partner = command.clone();
    wrong_partner.command_id = CommandId::generate();
    wrong_partner.dialogue_id = other.agent.dialogue_id;
    let other_dialogue = store
        .load(f.owner, other.agent.dialogue_id)
        .await
        .unwrap()
        .unwrap();
    wrong_partner.observed_cursor = other_dialogue.head;
    // Prove this partner is a qualified input owner before testing a foreign
    // publication. Neither a future cursor nor an old input contract may mask it.
    let mut own_input = input(f.owner, other.agent.dialogue_id);
    own_input.observed_cursor = other_dialogue.head;
    register_content(&f.pool, f.owner, &own_input.content_ref).await;
    let own = store.accept(own_input).await.unwrap().intent;
    let own_history = store
        .read_history(
            f.owner,
            other.agent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 20,
            },
        )
        .await
        .unwrap();
    let own_target = own_history
        .entries
        .iter()
        .find(|e| e.entry.entry_id == own.source_entry_id)
        .unwrap()
        .reply_target
        .expect("second partner has typed reply qualification");
    let mut own_reply = input(f.owner, other.agent.dialogue_id);
    own_reply.observed_cursor = own.accepted_cursor;
    own_reply.reply_target = Some(own_target);
    register_content(&f.pool, f.owner, &own_reply.content_ref).await;
    assert!(
        store
            .accept(own_reply)
            .await
            .unwrap()
            .intent
            .reply_binding
            .is_some()
    );
    wrong_partner.observed_cursor = store
        .load(f.owner, other.agent.dialogue_id)
        .await
        .unwrap()
        .unwrap()
        .head;
    assert_eq!(
        store.accept(wrong_partner).await,
        Err(DialogueStoreError::Unavailable)
    );
    let mut foreign = command.clone();
    foreign.actor = actor();
    foreign.command_id = CommandId::generate();
    assert!(store.accept(foreign).await.is_err());
    let unchanged = store.accept(command.clone()).await.unwrap();
    assert!(unchanged.replayed);
    assert_eq!(
        unchanged.intent, first.intent,
        "conflicting attempts never rewrite accepted selection"
    );
    // Reusing identical text in another new request does not select a result.
    let mut unselected = command;
    unselected.command_id = CommandId::generate();
    unselected.reply_target = None;
    let plain = store.accept(unselected.clone()).await.unwrap();
    assert!(plain.intent.reply_binding.is_none());
    unselected.reply_target = Some(selected);
    assert_eq!(
        store.accept(unselected).await,
        Err(DialogueStoreError::IdempotencyConflict),
        "an already accepted plain command cannot acquire a selected result on replay"
    );
    let outputs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM collaboration.work_items WHERE tenant_id=$1 AND owner_id=$2",
    )
    .bind(f.owner.tenant_id.into_uuid())
    .bind(f.owner.principal_id.into_uuid())
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        outputs, 0,
        "a reply or thank-you is not itself a task creation"
    );
}

#[tokio::test]
async fn foreign_publisher_has_no_new_reply_hint_or_acceptance_authority() {
    let f = fixture().await;
    let published = PostgresDialoguePublicationStore::new(f.pool.clone())
        .append(f.command.clone())
        .await
        .unwrap();
    let store = PostgresAgentDialogueStore::new(f.pool.clone());
    crate::fixture::publish_successor(
        &f.pool,
        f.activation.agent.agent_id,
        PrincipalId::generate(),
    )
    .await;
    let history = store
        .read_history(
            f.owner,
            f.intent.dialogue_id,
            DialogueHistoryQuery::Before {
                before: None,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert!(history.entries.iter().all(|e| e.reply_target.is_none()));
    let mut command = input(f.owner, f.intent.dialogue_id);
    command.observed_cursor = published.entry.cursor;
    command.reply_target = Some(DialogueReplyTarget {
        entry_id: published.entry.entry_id,
        visible_revision: published.entry.visible_revision.get(),
    });
    register_content(&f.pool, f.owner, &command.content_ref).await;
    assert_eq!(
        store.accept(command.clone()).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(count(&f.pool, "dialogue_user_intents").await, 1);
    // Ordinary inputs obey the same original publisher guard.
    command.reply_target = None;
    command.reply_to_intent_id = Some(f.intent.intent_id);
    assert_eq!(
        store.accept(command).await,
        Err(DialogueStoreError::RevisionConflict)
    );
}
