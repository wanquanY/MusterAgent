use super::*;
use musteragent_persistence_codec::dialogue_context_codec as context_codec;
use musteragent_postgres::{
    PostgresDialogueContextSourceStore, PostgresDialogueInputAttachmentAccess,
};
use musteragent_runtime_content::upload::{content_upload_key, idempotent_content_id};
use musteragent_runtime_contract::{AttachmentRef, DisplayText};

fn uploaded(owner: Actor, upload_id: &str) -> DialogueInputAttachment {
    DialogueInputAttachment {
        attachment: AttachmentRef {
            content_ref: ContentRef {
                content_id: idempotent_content_id(
                    owner,
                    &content_upload_key(upload_id, None).unwrap(),
                )
                .unwrap(),
                media_type: MediaType::new("text/csv").unwrap(),
                byte_length: 5,
                sha256: Sha256::digest("x\n123"),
            },
            display_name: DisplayText::new("反馈.csv").unwrap(),
        },
        source: DialogueInputAttachmentSource::Uploaded {
            upload_id: upload_id.into(),
        },
    }
}

#[tokio::test]
async fn attachment_foreign_publisher_is_rejected_before_acceptance_retention_or_outbox() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let active = PostgresOwnedAgentStore::new(pool.clone())
        .activate(prepared(&pool, owner, true).await)
        .await
        .unwrap();
    let mut command = input(owner, active.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    command.input_attachments = vec![uploaded(owner, &"c".repeat(64))];
    register_content(
        &pool,
        owner,
        &command.input_attachments[0].attachment.content_ref,
    )
    .await;
    crate::fixture::publish_successor(&pool, active.agent.agent_id, PrincipalId::generate()).await;
    let before = count(&pool, "owner_outbox").await;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    assert_eq!(
        store.accept(command.clone()).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 0);
    assert_eq!(count(&pool, "agent_dialogue_entries").await, 0);
    assert_eq!(count(&pool, "owner_outbox").await, before);
    assert_eq!(
        store
            .read_intent_receipt(owner, command.dialogue_id, command.command_id)
            .await
            .unwrap(),
        DialogueIntentReceiptObservation::NotObservedAtRead
    );
}

#[tokio::test]
async fn attachment_history_rechecks_published_pointer_but_committed_receipt_remains_exact() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let active = attachment_fixture::activate(&pool, owner).await;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let mut command = input(owner, active.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    command.input_attachments = vec![uploaded(owner, &"d".repeat(64))];
    register_content(
        &pool,
        owner,
        &command.input_attachments[0].attachment.content_ref,
    )
    .await;
    let accepted = store.accept(command.clone()).await.unwrap();
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    let mut incompatible = registry
        .load_version(active.agent.published_version_id)
        .await
        .unwrap()
        .unwrap();
    incompatible.agent_version_id = AgentVersionId::generate();
    incompatible.version = VersionNumber::new(incompatible.version.get() + 1).unwrap();
    incompatible.supersedes = Some(active.agent.published_version_id);
    // A newer publication does not implicitly repin the admitted Agent.
    registry.insert_version(incompatible.clone()).await.unwrap();
    let pointer = registry
        .load_published(active.agent.agent_id)
        .await
        .unwrap()
        .unwrap();
    registry
        .compare_and_swap_published(
            active.agent.agent_id,
            Some(PublishedAgentVersionGuard {
                agent_version_id: pointer.agent_version_id,
                pointer_version: pointer.version,
            }),
            PublishedAgentVersionPointer {
                agent_id: active.agent.agent_id,
                agent_version_id: incompatible.agent_version_id,
                version: pointer.version + 1,
                published_at: incompatible.published_at,
                published_by: owner.principal_id,
            },
        )
        .await
        .unwrap();
    let before = count(&pool, "owner_outbox").await;
    let mut followup = input(owner, active.agent.dialogue_id);
    followup.observed_cursor = accepted.intent.accepted_cursor;
    register_content(&pool, owner, &followup.content_ref).await;
    assert!(followup.input_attachments.is_empty());
    assert_eq!(
        store.accept(followup).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    let replay = store.accept(command).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.intent, accepted.intent);
    assert_eq!(count(&pool, "dialogue_user_intents").await, 1);
    assert_eq!(count(&pool, "owner_outbox").await, before);
}

#[tokio::test]
async fn attachment_acceptance_retention_receipt_and_source_are_one_authorized_snapshot() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    eprintln!(
        "Dialogue attachments isolated PostgreSQL port={}; testcontainers drop owns cleanup",
        pool.connect_options().get_port()
    );
    migrate(&pool).await.unwrap();
    let owner = actor();
    let active = attachment_fixture::activate(&pool, owner).await;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let access = PostgresDialogueInputAttachmentAccess::new(pool.clone());
    let mut command = input(owner, active.agent.dialogue_id);
    command.content_ref.byte_length = 0;
    command.content_ref.sha256 = Sha256::digest([]);
    register_content(&pool, owner, &command.content_ref).await;
    let upload = "a".repeat(64);
    let foreign = uploaded(
        Actor {
            principal_id: PrincipalId::generate(),
            ..owner
        },
        &upload,
    );
    register_content(&pool, owner, &foreign.attachment.content_ref).await;
    command.input_attachments = vec![foreign];
    assert_eq!(
        store.accept(command.clone()).await,
        Err(DialogueStoreError::Unavailable)
    );
    let original = uploaded(owner, &upload);
    register_content(&pool, owner, &original.attachment.content_ref).await;
    command.input_attachments = vec![original.clone()];
    sqlx::query("UPDATE artifact.content_refs SET state=1,storage_ref=NULL WHERE content_id=$1")
        .bind(original.attachment.content_ref.content_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store.accept(command.clone()).await,
        Err(DialogueStoreError::Unavailable)
    );
    sqlx::query("UPDATE artifact.content_refs SET state=2,storage_ref=$2 WHERE content_id=$1")
        .bind(original.attachment.content_ref.content_id.into_uuid())
        .bind(format!(
            "test://{}",
            original.attachment.content_ref.content_id
        ))
        .execute(&pool)
        .await
        .unwrap();
    let mut malformed = command.clone();
    malformed.input_attachments[0].attachment.content_ref.sha256 = Sha256::digest("wrong");
    assert_eq!(
        store.accept(malformed).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 0);
    let receipt = store.accept(command.clone()).await.unwrap();
    assert_eq!(receipt.intent.content_ref.byte_length, 0);
    assert_eq!(receipt.intent.input_attachments, command.input_attachments);
    let pins: i64 = sqlx::query_scalar("SELECT count(*) FROM agent.dialogue_content_references WHERE reference_id=$1 AND reference_kind='user_intent'")
        .bind(receipt.intent.intent_id.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(pins, 2);
    assert_eq!(
        store.accept(command.clone()).await.unwrap().intent,
        receipt.intent
    );
    assert_eq!(
        store
            .read_intent_receipt(owner, command.dialogue_id, command.command_id)
            .await
            .unwrap(),
        DialogueIntentReceiptObservation::Accepted(receipt.intent.clone())
    );
    let mut changed = command.clone();
    changed.input_attachments[0].attachment.display_name = DisplayText::new("另一份.csv").unwrap();
    assert_eq!(
        store.accept(changed).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    let query = DialogueInputAttachmentQuery {
        actor: owner,
        dialogue_id: command.dialogue_id,
        entry_id: receipt.intent.source_entry_id,
        cursor: receipt.intent.accepted_cursor,
        ordinal: 0,
    };
    assert_eq!(access.resolve(query).await.unwrap(), original.attachment);
    for invalid in [
        DialogueInputAttachmentQuery {
            actor: Actor {
                principal_id: PrincipalId::generate(),
                ..owner
            },
            ..query
        },
        DialogueInputAttachmentQuery {
            actor: actor(),
            ..query
        },
        DialogueInputAttachmentQuery {
            cursor: DialogueCursor { epoch: 2, seq: 1 },
            ..query
        },
        DialogueInputAttachmentQuery {
            entry_id: DialogueEntryId::generate(),
            ..query
        },
        DialogueInputAttachmentQuery {
            ordinal: 1,
            ..query
        },
    ] {
        assert_eq!(
            access.resolve(invalid).await,
            Err(DialogueStoreError::Unavailable)
        );
    }
    let mut reused = command.clone();
    reused.command_id = CommandId::generate();
    reused.observed_cursor = receipt.intent.accepted_cursor;
    reused.input_attachments[0].source = DialogueInputAttachmentSource::AcceptedInput {
        dialogue_id: query.dialogue_id,
        entry_id: query.entry_id,
        cursor: query.cursor,
        ordinal: 0,
    };
    let mut forged = reused.clone();
    if let DialogueInputAttachmentSource::AcceptedInput { ordinal, .. } =
        &mut forged.input_attachments[0].source
    {
        *ordinal = 1;
    }
    assert_eq!(
        store.accept(forged).await,
        Err(DialogueStoreError::Unavailable)
    );
    let next = store.accept(reused).await.unwrap();
    let source = PostgresDialogueContextSourceStore::new(pool.clone());
    let cut = source
        .cut(
            owner,
            active.agent.agent_id,
            command.dialogue_id,
            next.intent.accepted_cursor,
        )
        .await
        .unwrap();
    let page = source.page(owner, &cut, 1).await.unwrap();
    let bytes = context_codec::page_bytes(&page).unwrap();
    assert_eq!(context_codec::page_wire_version(&page), 3);
    assert_eq!(
        context_codec::page_from_bytes_for_version(&bytes, 3).unwrap(),
        page
    );
    assert!(context_codec::page_from_bytes_for_version(&bytes, 2).is_err());
    assert!(context_codec::verify_legacy_page(&bytes, &page).is_err());
    assert!(
        sqlx::query(
            "UPDATE agent.dialogue_input_attachments SET display_name='changed' WHERE intent_id=$1"
        )
        .bind(receipt.intent.intent_id.into_uuid())
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM artifact.content_refs WHERE content_id=$1")
            .bind(original.attachment.content_ref.content_id.into_uuid())
            .execute(&pool)
            .await
            .is_err()
    );
    let mut removed = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM agent.dialogue_input_attachments WHERE intent_id=$1")
        .bind(receipt.intent.intent_id.into_uuid())
        .execute(&mut *removed)
        .await
        .unwrap();
    assert!(removed.commit().await.is_err());
    let mut replaced = pool.begin().await.unwrap();
    // The count could otherwise be preserved by deleting and reinserting the
    // same ordinal with forged metadata; even an identical replacement fails.
    sqlx::query("CREATE TEMP TABLE replacement_input ON COMMIT DROP AS SELECT * FROM agent.dialogue_input_attachments WHERE intent_id=$1")
        .bind(receipt.intent.intent_id.into_uuid()).execute(&mut *replaced).await.unwrap();
    sqlx::query("DELETE FROM agent.dialogue_input_attachments WHERE intent_id=$1")
        .bind(receipt.intent.intent_id.into_uuid())
        .execute(&mut *replaced)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO agent.dialogue_input_attachments SELECT * FROM replacement_input")
            .execute(&mut *replaced)
            .await
            .is_err()
    );
    replaced.rollback().await.unwrap();
    assert_eq!(access.resolve(query).await.unwrap(), original.attachment);
    sqlx::query("UPDATE agent.owned_agents SET status='deleted' WHERE agent_id=$1")
        .bind(active.agent.agent_id.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        access.resolve(query).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(
        store.accept(command).await,
        Err(DialogueStoreError::Unavailable)
    );
    pool.close().await;
    assert_eq!(
        access.resolve(query).await,
        Err(DialogueStoreError::Infrastructure)
    );
    eprintln!(
        "Dialogue attachment migration, actor/entry authorization, retention, receipt and SQL/Rust source hash passed"
    );
}

#[tokio::test]
async fn attachment_acceptance_and_content_deletion_serialize_without_dangling_input() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let active = attachment_fixture::activate(&pool, owner).await;
    let attachment = uploaded(owner, &"b".repeat(64));
    register_content(&pool, owner, &attachment.attachment.content_ref).await;
    let mut command = input(owner, active.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    command.input_attachments = vec![attachment.clone()];
    let mut gc = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM artifact.content_refs WHERE content_id=$1")
        .bind(attachment.attachment.content_ref.content_id.into_uuid())
        .execute(&mut *gc)
        .await
        .unwrap();
    let writer_pool = named_pool(&pool, "input-attachment-gc-race").await;
    let writer = PostgresAgentDialogueStore::new(writer_pool.clone());
    let acceptance = tokio::spawn(async move { writer.accept(command).await });
    waiting(&pool, "input-attachment-gc-race", false).await;
    gc.commit().await.unwrap();
    assert_eq!(
        acceptance.await.unwrap(),
        Err(DialogueStoreError::Unavailable)
    );
    assert_eq!(count(&pool, "dialogue_user_intents").await, 0);
    // Failed input acceptance leaves no dialogue entry.
    let user_entries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.agent_dialogue_entries WHERE kind='user_message'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(user_entries, 0);
    assert_eq!(count(&pool, "agent_dialogue_entries").await, 0);
    writer_pool.close().await;
}
