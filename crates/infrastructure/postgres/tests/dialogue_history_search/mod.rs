use super::*;
pub(super) use crate::fixture::MemoryBlobs;
use musteragent_postgres::{PostgresContentStore, PostgresTieredBlobStore};
use musteragent_runtime_content::upload::{content_upload_key, idempotent_content_id};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{AttachmentRef, DisplayText};
use musteragent_storage_protection::OpaqueRefKeyring;
use std::sync::Arc;
async fn fixture() -> (IsolatedPostgres, PgPool, Arc<PostgresContentStore>) {
    let guard = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start disposable history-search database");
    let pool = guard.pool().await.expect("connect history-search database");
    migrate(&pool).await.unwrap();
    let key = DottedKey::new("test.history.search").unwrap();
    let keys = Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [47; 32])]).unwrap());
    let blobs = Arc::new(PostgresTieredBlobStore::new(
        pool.clone(),
        Arc::new(MemoryBlobs::default()),
    ));
    let content = Arc::new(PostgresContentStore::new(pool.clone(), blobs, keys));
    (guard, pool, content)
}
fn query(actor: Actor) -> DialogueHistorySearchQuery {
    DialogueHistorySearchQuery {
        actor,
        text: String::new(),
        category: DialogueHistoryCategory::All,
        agent_id: None,
        from: None,
        to: None,
        cursor: None,
        limit: 50,
    }
}
async fn publish(
    store: &PostgresAgentDialogueStore,
    content: &PostgresContentStore,
    actor: Actor,
    dialogue: AgentDialogueId,
    text: String,
    at: i64,
    attachments: Vec<DialogueInputAttachment>,
) -> UserIntent {
    let body = content
        .put(PutContent {
            tenant_id: actor.tenant_id,
            media_type: MediaType::new("text/plain;charset=utf-8").unwrap(),
            bytes: text.into_bytes(),
        })
        .await
        .unwrap();
    let mut command = input(actor, dialogue);
    command.content_ref = body;
    command.accepted_at = UtcTimestamp::from_unix_millis(at);
    command.input_attachments = attachments;
    command.observed_cursor = store.load(actor, dialogue).await.unwrap().unwrap().head;
    store
        .accept(command)
        .await
        .unwrap_or_else(|error| panic!("history input at {at} was not accepted: {error:?}"))
        .intent
}
#[tokio::test]
async fn dialogue_history_search_uses_public_owner_index_snapshot_and_content_verification() {
    let (_guard, pool, content) = fixture().await;
    let owner = actor();
    let other = Actor {
        principal_id: PrincipalId::generate(),
        ..owner
    };
    // Search uses an owner-published Agent and one actual content store.
    let active =
        receipt::attachment_fixture::activate_with_content(&pool, owner, content.clone()).await;
    let foreign =
        receipt::attachment_fixture::activate_with_content(&pool, other, content.clone()).await;
    let version = PostgresAgentDefinitionStore::new(pool.clone())
        .load_version(active.agent.published_version_id)
        .await
        .unwrap()
        .unwrap();
    version
        .validate_complete_for_publication()
        .expect("history fixture must qualify for accepted attachments");
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let first = publish(
        &store,
        &content,
        owner,
        active.agent.dialogue_id,
        "客户反馈\0 100%_".into(),
        1000,
        vec![],
    )
    .await;
    let second = publish(
        &store,
        &content,
        owner,
        active.agent.dialogue_id,
        format!("{}对象存储反馈", "字".repeat(100_000)),
        2000,
        vec![],
    )
    .await;
    let upload_id = "f".repeat(64);
    let upload_key = content_upload_key(&upload_id, None).unwrap();
    let image = content
        .put_at(
            idempotent_content_id(owner, &upload_key).unwrap(),
            PutContent {
                tenant_id: owner.tenant_id,
                media_type: MediaType::new("image/png").unwrap(),
                bytes: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    let attachment = DialogueInputAttachment {
        attachment: AttachmentRef {
            content_ref: image,
            display_name: DisplayText::new("反馈截图.png").unwrap(),
        },
        source: DialogueInputAttachmentSource::Uploaded { upload_id },
    };
    let third = publish(
        &store,
        &content,
        owner,
        active.agent.dialogue_id,
        "反馈链接 https://example.com".into(),
        3000,
        vec![attachment],
    )
    .await;
    publish(
        &store,
        &content,
        other,
        foreign.agent.dialogue_id,
        "客户反馈 OTHER_ACCOUNT".into(),
        9000,
        vec![],
    )
    .await;
    content
        .put(PutContent {
            tenant_id: owner.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"PRIVATE_ONLY internal tool output".to_vec(),
        })
        .await
        .unwrap();
    let mut q = query(owner);
    q.text = "反馈".into();
    q.limit = 1;
    let page = store.search_history(&content, q.clone()).await.unwrap();
    assert_eq!(page.hits[0].entry.entry.entry_id, third.source_entry_id);
    let cursor = page.next_cursor.unwrap();
    publish(
        &store,
        &content,
        owner,
        active.agent.dialogue_id,
        "最新反馈".into(),
        4000,
        vec![],
    )
    .await;
    q.cursor = Some(cursor.clone());
    let page2 = store.search_history(&content, q.clone()).await.unwrap();
    assert_eq!(page2.hits[0].entry.entry.entry_id, second.source_entry_id);
    assert!(page2.hits[0].excerpt.contains("反馈"));
    q.cursor = page2.next_cursor;
    let page3 = store.search_history(&content, q.clone()).await.unwrap();
    assert_eq!(page3.hits[0].entry.entry.entry_id, first.source_entry_id);
    assert!(page3.next_cursor.is_none());
    let mut foreign_cursor = q.clone();
    foreign_cursor.actor = other;
    foreign_cursor.cursor = Some(cursor);
    assert!(
        store
            .search_history(&content, foreign_cursor)
            .await
            .is_err()
    );
    let mut date = query(owner);
    date.from = Some(UtcTimestamp::from_unix_millis(2000));
    date.to = Some(UtcTimestamp::from_unix_millis(3000));
    let dates = store.search_history(&content, date).await.unwrap();
    assert_eq!(dates.hits.len(), 1);
    assert_eq!(dates.hits[0].entry.entry.entry_id, second.source_entry_id);
    for category in [
        DialogueHistoryCategory::Files,
        DialogueHistoryCategory::Images,
        DialogueHistoryCategory::Links,
    ] {
        let mut category_query = query(owner);
        category_query.category = category;
        let files = store
            .search_history(&content, category_query)
            .await
            .unwrap();
        assert_eq!(files.hits.len(), 1);
        assert_eq!(files.hits[0].entry.entry.entry_id, third.source_entry_id);
    }
    for text in ["PRIVATE_ONLY", "OTHER_ACCOUNT"] {
        let mut q = query(owner);
        q.text = text.into();
        assert!(
            store
                .search_history(&content, q)
                .await
                .unwrap()
                .hits
                .is_empty()
        );
    }
    let mut literal = query(owner);
    literal.text = "100%_".into();
    assert_eq!(
        store.search_history(&content, literal).await.unwrap().hits[0]
            .entry
            .entry
            .entry_id,
        first.source_entry_id
    );
    let mut denied = query(owner);
    denied.agent_id = Some(foreign.agent.agent_id);
    assert!(store.search_history(&content, denied).await.is_err());
    let mut own_agent = query(owner);
    own_agent.agent_id = Some(active.agent.agent_id);
    assert_eq!(
        store
            .search_history(&content, own_agent)
            .await
            .unwrap()
            .hits
            .len(),
        4, // Exactly the four user messages; activation creates no content.
    );
    sqlx::query("UPDATE artifact.content_refs SET state=3 WHERE content_id=$1")
        .bind(second.content_ref.content_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let mut revoked = query(owner);
    revoked.text = "对象存储反馈".into();
    assert!(
        store
            .search_history(&content, revoked)
            .await
            .unwrap()
            .hits
            .is_empty()
    );
    // A corrupt/unavailable source does not become a silently complete empty search.
    let mut missing = input(owner, active.agent.dialogue_id);
    missing.observed_cursor = store
        .load(owner, active.agent.dialogue_id)
        .await
        .unwrap()
        .unwrap()
        .head;
    register_content(&pool, owner, &missing.content_ref).await;
    store.accept(missing).await.unwrap();
    assert!(matches!(
        store.search_history(&content, query(owner)).await,
        Err(DialogueStoreError::Infrastructure)
    ));
}
