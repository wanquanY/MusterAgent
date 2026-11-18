//! Original input admission binds a real same-dialogue task, never its title.
use super::*;
#[tokio::test]
async fn selected_task_receipt_survives_new_version_and_other_task_is_untouched() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, a) = fixture(&pool).await;
    let b = another(&a, "second-same-title");
    authority.create(a.clone()).await.unwrap();
    authority.create(b.clone()).await.unwrap();
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let mut request = input(a.actor, a.source_dialogue_id);
    request.accepted_at = UtcTimestamp::from_unix_millis(350);
    request.task_target = Some(AgentTaskTarget {
        work_item_id: a.work_item_id(),
        goal_revision: 1,
        control_revision: 1,
    });
    register_content(&pool, a.actor, &request.content_ref).await;
    let (first, duplicate) =
        tokio::join!(store.accept(request.clone()), store.accept(request.clone()));
    let first = first.unwrap();
    let duplicate = duplicate.unwrap();
    assert_eq!(first.intent, duplicate.intent);
    assert_eq!(first.intent.task_target, request.task_target);
    authority.amend(amendment(&a)).await.unwrap();
    assert_eq!(
        store.accept(request.clone()).await.unwrap().intent,
        first.intent
    );
    let mut stale = request.clone();
    stale.command_id = CommandId::generate();
    assert_eq!(
        store.accept(stale).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    let mut redefined = request.clone();
    redefined.task_target.as_mut().unwrap().work_item_id = b.work_item_id();
    assert_eq!(
        store.accept(redefined).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let other = authority
        .inspect(InspectAgentWorkItem {
            actor: a.actor,
            scope: a.scope,
            work_item_id: b.work_item_id(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other.core.revisions.goal.get(), 1);
    assert_eq!(other.core.revisions.control.get(), 1);
    let outsider = actor();
    let mut wrong = request;
    wrong.actor = outsider;
    wrong.command_id = CommandId::generate();
    assert!(store.accept(wrong).await.is_err());
}

#[tokio::test]
async fn selected_task_rejects_another_agent_and_invalid_observed_versions() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (authority, a) = fixture(&pool).await;
    authority.create(a.clone()).await.unwrap();
    let store = PostgresAgentDialogueStore::new(pool.clone());
    for revision in [0, 2, u64::MAX] {
        let mut request = input(a.actor, a.source_dialogue_id);
        request.accepted_at = UtcTimestamp::from_unix_millis(350);
        request.task_target = Some(AgentTaskTarget {
            work_item_id: a.work_item_id(),
            goal_revision: revision,
            control_revision: 1,
        });
        register_content(&pool, a.actor, &request.content_ref).await;
        assert!(store.accept(request).await.is_err());
    }
    let (other_authority, other) = fixture(&pool).await;
    other_authority.create(other.clone()).await.unwrap();
    let mut request = input(a.actor, a.source_dialogue_id);
    request.task_target = Some(AgentTaskTarget {
        work_item_id: other.work_item_id(),
        goal_revision: 1,
        control_revision: 1,
    });
    register_content(&pool, a.actor, &request.content_ref).await;
    assert_eq!(
        store.accept(request).await,
        Err(DialogueStoreError::Unavailable)
    );
}
