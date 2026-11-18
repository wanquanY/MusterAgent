use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_postgres::{PostgresResourceStore, migrate};
use musteragent_resource::ResourceName;
use musteragent_runtime_resource::*;
use musteragent_testkit::IsolatedPostgres;

async fn seed(pool: &sqlx::PgPool) -> (ResourceCommandContext, ParticipantId) {
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let cid = ConversationId::generate();
    let owner = ParticipantId::generate();
    let workspace = WorkspaceId::generate();
    let mut tx = pool
        .begin()
        .await
        .expect("valid fixture and authorized query");
    sqlx::query("INSERT INTO conversation.conversations (conversation_id, tenant_id, kind, title, control_home_kind, home_epoch, lifecycle, created_by, created_at_ms, updated_at_ms, workspace_id, execution_owner_participant_id) VALUES ($1,$2,1,'File query',1,1,1,$3,1,1,$4,$5)")
        .bind(cid.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(workspace.into_uuid()).bind(owner.into_uuid())
        .execute(&mut *tx).await.expect("valid fixture and authorized query");
    let agent = AgentId::generate();
    sqlx::query("INSERT INTO agent.agent_definitions(agent_id,agent_key,built_in,lifecycle,display_name,created_at_ms,updated_at_ms) VALUES($1,$2,false,1,'Test executor',1,1)")
        .bind(agent.into_uuid()).bind(format!("test-{agent}")).execute(&mut *tx).await.expect("custom Agent");
    sqlx::query("INSERT INTO conversation.participants (participant_id, tenant_id, conversation_id, kind, agent_ref, display_name, joined_at_ms) VALUES ($1,$2,$3,2,$4,'Agent',1)")
        .bind(owner.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(cid.into_uuid()).bind(agent.into_uuid()).execute(&mut *tx).await.expect("valid fixture and authorized query");
    sqlx::query("INSERT INTO conversation.participants (participant_id, tenant_id, conversation_id, kind, principal_ref, display_name, joined_at_ms) VALUES ($1,$2,$3,1,$4,'User',1)")
        .bind(ParticipantId::generate().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(cid.into_uuid()).bind(actor.principal_id.into_uuid()).execute(&mut *tx).await.expect("valid fixture and authorized query");
    tx.commit()
        .await
        .expect("valid fixture and authorized query");
    (
        ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: ResourceCommandSource::Conversation {
                conversation_id: cid,
                workspace_id: Some(workspace),
            },
            invocation_id: None,
            tool_key: None,
            now: UtcTimestamp::from_unix_millis(100),
        },
        owner,
    )
}
fn mutation() -> ResourceMutationIdentity {
    let id = ResourceChangeSetId::generate();
    ResourceMutationIdentity {
        change_set_id: id,
        idempotency_key: IdempotencyKey::new(id.to_string())
            .expect("valid fixture and authorized query"),
        command_fingerprint: Sha256::digest(id.to_string()),
    }
}
async fn content(pool: &sqlx::PgPool, tenant: TenantId, body: &[u8]) -> ContentRef {
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("valid fixture and authorized query"),
        byte_length: body.len() as u64,
        sha256: Sha256::digest(body),
    };
    // Metadata fixture: this catalog must never attempt to open this locator.
    sqlx::query("INSERT INTO artifact.content_refs (content_id, tenant_id, media_type, byte_length, content_hash, storage_ref, created_at_ms) VALUES ($1,$2,'text/plain',$3::numeric,$4,'test://metadata-only',1)")
        .bind(reference.content_id.into_uuid()).bind(tenant.into_uuid()).bind(body.len().to_string()).bind(reference.sha256.into_bytes().to_vec()).execute(pool).await.expect("valid fixture and authorized query");
    reference
}
async fn file(
    pool: &sqlx::PgPool,
    context: &ResourceCommandContext,
    owner: ParticipantId,
    revision: u64,
    name: &str,
) -> (ResourceNodeId, ArtifactId) {
    let source = content(pool, context.tenant_id, name.as_bytes()).await;
    let artifact_id = ArtifactId::generate();
    let node_id = ResourceNodeId::generate();
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .create_file(CreateResourceFile {
            context: context.clone(),
            identity: mutation(),
            expected_revision: ResourceRevision::new(revision),
            node_id,
            parent_node_id: None,
            name: ResourceName::new(name).expect("valid fixture and authorized query"),
            sort_key: name.into(),
            artifact: Artifact {
                artifact_id,
                tenant_id: context.tenant_id,
                source: ArtifactSource::Conversation(
                    context
                        .conversation_id()
                        .expect("valid fixture and authorized query"),
                ),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("valid fixture and authorized query"),
                title: name.into(),
                created_at: context.now,
                updated_at: context.now,
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id,
                version: VersionNumber::new(1).expect("valid fixture and authorized query"),
                content_ref: source.clone(),
                content_hash: source.sha256,
                dependency_refs: vec![],
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(owner),
                committed_at: context.now,
            },
        })
        .await
        .expect("valid fixture and authorized query");
    (node_id, artifact_id)
}
fn query(context: &ResourceCommandContext) -> ListProjectFiles {
    ListProjectFiles {
        produced_by: None,
        context: context.clone(),
        recursive: false,
        parent_node_id: None,
        offset: 0,
        limit: 1,
        name_query: None,
    }
}

#[tokio::test]
async fn directory_query_batches_versions_rejects_revocation_and_detects_body_changes() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL container");
    let pool = pg.pool().await.expect("valid fixture and authorized query");
    migrate(&pool)
        .await
        .expect("valid fixture and authorized query");
    let (context, owner) = seed(&pool).await;
    let (node_id, artifact_id) = file(&pool, &context, owner, 0, "100%_计划.txt").await;
    file(&pool, &context, owner, 1, "second.txt").await;
    let store = PostgresResourceStore::new(pool.clone());
    let first = store
        .list_files(query(&context))
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.next_offset, Some(1));
    assert!(first.entries[0].file.is_some());
    let second = store
        .list_files(ListProjectFiles {
            offset: 1,
            ..query(&context)
        })
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(first.content_revision, second.content_revision);
    assert_eq!(second.next_offset, None);
    let found = store
        .list_files(ListProjectFiles {
            name_query: Some("%_".into()),
            ..query(&context)
        })
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(
        found.entries.len(),
        1,
        "name search uses literal characters, not SQL wildcard matching"
    );
    assert_eq!(found.entries[0].node.node_id, node_id);

    let source = content(&pool, context.tenant_id, b"edited").await;
    let mut tx = pool
        .begin()
        .await
        .expect("valid fixture and authorized query");
    sqlx::query("INSERT INTO artifact.artifact_versions (artifact_version_id, artifact_id, version, content_id, content_hash, committed_by, committed_at_ms) VALUES ($1,$2,2,$3,$4,$5,200)")
        .bind(ArtifactVersionId::generate().into_uuid()).bind(artifact_id.into_uuid()).bind(source.content_id.into_uuid()).bind(source.sha256.into_bytes().to_vec()).bind(owner.into_uuid()).execute(&mut *tx).await.expect("valid fixture and authorized query");
    sqlx::query(
        "UPDATE artifact.artifacts SET current_version=2, updated_at_ms=200 WHERE artifact_id=$1",
    )
    .bind(artifact_id.into_uuid())
    .execute(&mut *tx)
    .await
    .expect("valid fixture and authorized query");
    tx.commit()
        .await
        .expect("valid fixture and authorized query");
    let after = store
        .list_files(query(&context))
        .await
        .expect("valid fixture and authorized query");
    assert_eq!(
        first.revision, after.revision,
        "existing Artifact write does not change the tree revision"
    );
    assert_ne!(
        first.content_revision, after.content_revision,
        "file version must invalidate the page cursor too"
    );
    assert_eq!(
        store
            .resolve_file(&context, node_id)
            .await
            .expect("valid fixture and authorized query")
            .file
            .expect("valid fixture and authorized query")
            .content_ref,
        source
    );
    let mut unauthorized = context.clone();
    unauthorized.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        store.list_files(query(&unauthorized)).await,
        Err(ResourceStoreError::NodeUnavailable)
    ));
    sqlx::query("UPDATE conversation.participants SET left_at_ms=201 WHERE conversation_id=$1 AND principal_ref=$2")
        .bind(context.conversation_id().expect("valid fixture and authorized query").into_uuid()).bind(context.actor.principal_id.into_uuid()).execute(&pool).await.expect("valid fixture and authorized query");
    assert!(matches!(
        store.resolve_file(&context, node_id).await,
        Err(ResourceStoreError::NodeUnavailable)
    ));
    assert!(matches!(
        store.list_files(query(&context)).await,
        Err(ResourceStoreError::NodeUnavailable)
    ));
}

#[tokio::test]
async fn recursive_catalog_pins_subtree_versions_and_excludes_siblings() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("schema");
    let (context, owner) = seed(&pool).await;
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let parent = ResourceNodeId::generate();
    let nested = ResourceNodeId::generate();
    for (revision, node_id, parent_node_id, name) in [
        (0, parent, None, "chosen"),
        (1, nested, Some(parent), "nested"),
    ] {
        app.create_folder(CreateResourceFolder {
            context: context.clone(),
            identity: mutation(),
            expected_revision: ResourceRevision::new(revision),
            node_id,
            parent_node_id,
            name: ResourceName::new(name).expect("name"),
            sort_key: name.into(),
        })
        .await
        .expect("original folder mutation");
    }
    let (deep, artifact) = file(&pool, &context, owner, 2, "deep.txt").await;
    app.move_node(MoveResourceNode {
        context: context.clone(),
        identity: mutation(),
        expected_revision: ResourceRevision::new(3),
        node_id: deep,
        target_parent_node_id: Some(nested),
        name: None,
    })
    .await
    .expect("original nested move");
    file(&pool, &context, owner, 4, "sibling.txt").await;
    let store = PostgresResourceStore::new(pool.clone());
    let mut selected = query(&context);
    selected.parent_node_id = Some(parent);
    let children = store.list_files(selected.clone()).await.expect("children");
    assert_eq!(children.entries[0].node.node_id, nested);
    assert!(children.next_offset.is_none());
    selected.recursive = true;
    let first = store
        .list_files(selected.clone())
        .await
        .expect("subtree first");
    assert_eq!(first.entries[0].node.node_id, nested);
    selected.offset = first.next_offset.expect("deep page");
    let tail = store
        .list_files(selected.clone())
        .await
        .expect("subtree tail");
    assert_eq!(tail.entries[0].node.node_id, deep);
    assert!(tail.next_offset.is_none());
    assert_eq!(first.content_revision, tail.content_revision);
    // An edit deep under the selected parent invalidates even the first page.
    sqlx::query("UPDATE artifact.artifacts SET updated_at_ms=updated_at_ms+1 WHERE artifact_id=$1")
        .bind(artifact.into_uuid())
        .execute(&pool)
        .await
        .expect("original source metadata change");
    selected.offset = 0;
    assert_ne!(
        store
            .list_files(selected.clone())
            .await
            .expect("changed subtree")
            .content_revision,
        first.content_revision
    );
    selected.context.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        store.list_files(selected).await,
        Err(ResourceStoreError::NodeUnavailable)
    ));
}

#[tokio::test]
async fn artifact_reads_use_current_participants_without_legacy_import_tables() {
    use musteragent_postgres::PostgresArtifactStore;
    use musteragent_runtime_artifact::{
        ArtifactApplication, GetArtifact, ListConversationArtifacts,
    };
    use musteragent_runtime_file_query::{FileQueryContext, IndependentConversationFileSource};
    use musteragent_storage_protection::OpaqueRefKeyring;
    use std::sync::Arc;
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let (context, owner) = seed(&pool).await;
    let (_, artifact_id) = file(&pool, &context, owner, 0, "import-free.txt").await;
    let key = DottedKey::new("test.artifact-key").unwrap();
    let store = PostgresArtifactStore::new(
        pool.clone(),
        Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [19; 32])]).unwrap()),
    );
    let get = GetArtifact {
        actor: context.actor,
        artifact_id,
    };
    let loaded = store.get_artifact(get).await.unwrap();
    assert_eq!(loaded.artifact.artifact_id, artifact_id);
    assert_eq!(loaded.versions.len(), 1);
    let query = ListConversationArtifacts {
        actor: context.actor,
        conversation_id: context.conversation_id().unwrap(),
        limit: 10,
        before_artifact_id: None,
        search_query: None,
        kinds: vec![],
    };
    let page = store
        .list_conversation_artifacts(query.clone())
        .await
        .unwrap();
    assert_eq!(page.artifacts.len(), 1);
    let read = FileQueryContext {
        dialogue_source: None,
        actor: context.actor,
        conversation_id: context.conversation_id(),
        now: context.now,
        authority_revision: "test:1".into(),
        content_allowlist: None,
        project_workspace_id: None,
    };
    let independent = store.snapshot(&read).await.unwrap();
    assert_eq!(independent.len(), 1);
    assert_eq!(independent[0].artifact.artifact_id, artifact_id);
    sqlx::query("UPDATE conversation.participants SET left_at_ms=2 WHERE tenant_id=$1 AND conversation_id=$2 AND principal_ref=$3")
        .bind(context.tenant_id.into_uuid()).bind(context.conversation_id().unwrap().into_uuid()).bind(context.actor.principal_id.into_uuid()).execute(&pool).await.unwrap();
    assert!(store.get_artifact(get).await.is_err());
    assert!(
        store
            .list_conversation_artifacts(query)
            .await
            .unwrap()
            .artifacts
            .is_empty()
    );
    assert!(store.snapshot(&read).await.is_err());
    pool.close().await;
}
