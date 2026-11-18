#![cfg(test)]

#[path = "support/file_events.rs"]
mod file_events;
use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::sync::Arc;

#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

use resource_support::*;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::{MediaType, SchemaRef, Sha256};
use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, CapabilityProviderId, ConversationId, IdempotencyKey,
    InvocationId, ParticipantId, PrincipalId, PrincipalKind, ResourceChangeSetId, ResourceNodeId,
    ResourceRevision, TenantId, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey, WorkspaceId,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{
    PostgresContentStore, PostgresFencedResourceToolCommitter, PostgresFencedToolInvocationJournal,
    PostgresResourceStore,
};
use musteragent_resource::{ResourceNodeKind, ResourceNodeLifecycle, ResourceScope};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityKey, CapabilityResult};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{TraceContext, TraceParent};
use musteragent_runtime_resource::{
    BuiltinResourceTools, CommitPreparedResourceTool, CommitResourceChangeSet, CreateResourceFile,
    CreateResourceFolder, FencedResourceToolCommitter, ListResourceChildren, MoveResourceNode,
    PreparedResourceMutation, RenameResourceNode, ResourceApplication, ResourceApplicationError,
    ResourceChangeSetMutation, ResourceFileNameTargets, ResourceMutationIdentity,
    ResourceStoreError, ResourceToolContext, ResourceToolPreparation, RestoreResourceNode,
    TrashResourceNode,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::ToolKey;
use musteragent_wire::ProtobufLedgerPayloadCodec;

#[tokio::test]
async fn project_resource_tree_is_shared_fenced_idempotent_and_recursively_recoverable() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");

    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let workspace_id = WorkspaceId::generate();
    let first_conversation_id = ConversationId::generate();
    let second_conversation_id = ConversationId::generate();
    seed_project_conversations(
        &pool,
        tenant_id,
        principal_id,
        workspace_id,
        [first_conversation_id, second_conversation_id],
    )
    .await;

    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let root_id = ResourceNodeId::generate();
    let child_id = ResourceNodeId::generate();
    let create_root = CreateResourceFolder {
        context: context(
            tenant_id,
            principal_id,
            first_conversation_id,
            workspace_id,
            100,
        ),
        identity: mutation("create-root"),
        expected_revision: ResourceRevision::ZERO,
        node_id: root_id,
        parent_node_id: None,
        name: name("Sources"),
        sort_key: "a".to_owned(),
    };
    let created = application
        .create_folder(create_root.clone())
        .await
        .expect("create root folder");
    assert_eq!(created.scope, ResourceScope::Project(workspace_id));
    assert_eq!(created.previous_revision, ResourceRevision::ZERO);
    assert_eq!(created.revision, ResourceRevision::new(1));
    assert!(!created.duplicate);

    let replayed = application
        .create_folder(create_root)
        .await
        .expect("replay exactly idempotent create");
    assert!(replayed.duplicate);
    assert_eq!(replayed.node.node_id, root_id);
    assert_eq!(replayed.revision, ResourceRevision::new(1));

    let stale = application
        .create_folder(CreateResourceFolder {
            context: context(
                tenant_id,
                principal_id,
                first_conversation_id,
                workspace_id,
                101,
            ),
            identity: mutation("stale-create"),
            expected_revision: ResourceRevision::ZERO,
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("Stale"),
            sort_key: "b".to_owned(),
        })
        .await
        .expect_err("reject stale tree revision");
    assert_eq!(
        stale,
        ResourceApplicationError::Store(ResourceStoreError::RevisionConflict)
    );

    application
        .create_folder(CreateResourceFolder {
            context: context(
                tenant_id,
                principal_id,
                second_conversation_id,
                workspace_id,
                102,
            ),
            identity: mutation("create-child"),
            expected_revision: ResourceRevision::new(1),
            node_id: child_id,
            parent_node_id: Some(root_id),
            name: name("Components"),
            sort_key: "a".to_owned(),
        })
        .await
        .expect("create from another Project Conversation");

    let no_op_move = MoveResourceNode {
        context: context(
            tenant_id,
            principal_id,
            first_conversation_id,
            workspace_id,
            103,
        ),
        identity: mutation("move-child-to-current-parent"),
        expected_revision: ResourceRevision::new(2),
        node_id: child_id,
        target_parent_node_id: Some(root_id),
        name: None,
    };
    let unchanged = application
        .move_node(no_op_move.clone())
        .await
        .expect("treat a move to the current parent as an idempotent no-op");
    assert_eq!(unchanged.previous_revision, ResourceRevision::new(2));
    assert_eq!(unchanged.revision, ResourceRevision::new(2));
    assert_eq!(unchanged.node.parent_node_id, Some(root_id));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM resource.scope_heads
              WHERE tenant_id = $1 AND scope_kind = 1 AND scope_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(workspace_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read Resource scope revision after no-op move"),
        2,
    );
    let replayed_no_op = application
        .move_node(no_op_move)
        .await
        .expect("replay the persisted no-op move receipt");
    assert!(replayed_no_op.duplicate);
    assert_eq!(replayed_no_op.revision, ResourceRevision::new(2));

    let cycle = application
        .move_node(MoveResourceNode {
            context: context(
                tenant_id,
                principal_id,
                first_conversation_id,
                workspace_id,
                104,
            ),
            identity: mutation("cycle"),
            expected_revision: ResourceRevision::new(2),
            node_id: root_id,
            target_parent_node_id: Some(child_id),
            name: None,
        })
        .await
        .expect_err("reject recursive move");
    assert_eq!(
        cycle,
        ResourceApplicationError::Store(ResourceStoreError::MoveCycle)
    );

    let trashed = application
        .trash_node(TrashResourceNode {
            context: context(
                tenant_id,
                principal_id,
                first_conversation_id,
                workspace_id,
                105,
            ),
            identity: mutation("trash-subtree"),
            expected_revision: ResourceRevision::new(2),
            node_id: root_id,
        })
        .await
        .expect("trash subtree atomically");
    assert_eq!(trashed.revision, ResourceRevision::new(3));
    assert_eq!(trashed.node.lifecycle, ResourceNodeLifecycle::Trashed);

    let trashed_children = application
        .list_children(ListResourceChildren {
            context: context(
                tenant_id,
                principal_id,
                second_conversation_id,
                workspace_id,
                106,
            ),
            parent_node_id: Some(root_id),
            include_trashed: true,
            offset: 0,
            limit: 100,
        })
        .await
        .expect("list durable trashed subtree");
    assert_eq!(trashed_children.revision, ResourceRevision::new(3));
    assert_eq!(trashed_children.nodes.len(), 1);
    assert_eq!(trashed_children.nodes[0].node_id, child_id);
    assert_eq!(
        trashed_children.nodes[0].lifecycle,
        ResourceNodeLifecycle::Trashed
    );

    let restored = application
        .restore_node(RestoreResourceNode {
            context: context(
                tenant_id,
                principal_id,
                second_conversation_id,
                workspace_id,
                107,
            ),
            identity: mutation("restore-subtree"),
            expected_revision: ResourceRevision::new(3),
            node_id: root_id,
            target_parent_node_id: None,
            name: None,
        })
        .await
        .expect("restore subtree atomically");
    assert_eq!(restored.revision, ResourceRevision::new(4));
    assert_eq!(restored.node.lifecycle, ResourceNodeLifecycle::Active);

    let active_children = application
        .list_children(ListResourceChildren {
            context: context(
                tenant_id,
                principal_id,
                first_conversation_id,
                workspace_id,
                108,
            ),
            parent_node_id: Some(root_id),
            include_trashed: false,
            offset: 0,
            limit: 100,
        })
        .await
        .expect("list restored subtree");
    assert_eq!(active_children.revision, ResourceRevision::new(4));
    assert_eq!(active_children.nodes.len(), 1);
    assert_eq!(active_children.nodes[0].kind, ResourceNodeKind::Folder);
    assert_eq!(
        active_children.nodes[0].lifecycle,
        ResourceNodeLifecycle::Active
    );
}

#[tokio::test]
async fn product_file_create_commits_artifact_version_and_resource_node_atomically() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");

    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let workspace_id = WorkspaceId::generate();
    let conversation_id = ConversationId::generate();
    seed_project_conversations(
        &pool,
        tenant_id,
        principal_id,
        workspace_id,
        [conversation_id, ConversationId::generate()],
    )
    .await;
    let participant_id = ParticipantId::from_uuid(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT execution_owner_participant_id
               FROM conversation.conversations
              WHERE conversation_id = $1",
        )
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read Product participant"),
    )
    .expect("participant identity");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let source = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/markdown").expect("Markdown media type"),
            bytes: Vec::new(),
        })
        .await
        .expect("stage empty Product file");
    let artifact_id = ArtifactId::generate();
    let node_id = ResourceNodeId::generate();
    let now = UtcTimestamp::from_unix_millis(100);
    let command = CreateResourceFile {
        context: context(tenant_id, principal_id, conversation_id, workspace_id, 100),
        identity: mutation("product-readme"),
        expected_revision: ResourceRevision::ZERO,
        node_id,
        parent_node_id: None,
        name: name("README.md"),
        sort_key: "README.md".to_owned(),
        artifact: Artifact {
            artifact_id,
            tenant_id,
            source: ArtifactSource::Conversation(conversation_id),
            origin: ArtifactOrigin::Product,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).expect("first version"),
            title: "README.md".to_owned(),
            created_at: now,
            updated_at: now,
        },
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(1).expect("first version"),
            content_ref: source.clone(),
            dependency_refs: Vec::new(),
            content_hash: source.sha256,
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(participant_id),
            committed_at: now,
        },
    };
    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let committed = application
        .create_file(command.clone())
        .await
        .expect("atomically create Product file");
    assert_eq!(committed.node.file_artifact_id, Some(artifact_id));
    assert_eq!(committed.revision, ResourceRevision::new(1));
    assert_eq!(
        sqlx::query_as::<_, (i16, i64, i64)>(
            "SELECT artifact.origin,
                    (SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id = $1),
                    (SELECT count(*) FROM resource.nodes WHERE file_artifact_id = $1)
               FROM artifact.artifacts AS artifact
              WHERE artifact.artifact_id = $1",
        )
        .bind(artifact_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read atomic Product file state"),
        (3, 1, 1),
    );
    assert!(
        application
            .create_file(command)
            .await
            .expect("replay create")
            .duplicate
    );

    let conflicting_artifact_id = ArtifactId::generate();
    let conflict = application
        .create_file(CreateResourceFile {
            context: context(tenant_id, principal_id, conversation_id, workspace_id, 101),
            identity: mutation("product-readme-conflict"),
            expected_revision: ResourceRevision::new(1),
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("README.md"),
            sort_key: "README.md".to_owned(),
            artifact: Artifact {
                artifact_id: conflicting_artifact_id,
                tenant_id,
                source: ArtifactSource::Conversation(conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("first version"),
                title: "README.md".to_owned(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id: conflicting_artifact_id,
                version: VersionNumber::new(1).expect("first version"),
                content_ref: source.clone(),
                dependency_refs: Vec::new(),
                content_hash: source.sha256,
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(participant_id),
                committed_at: now,
            },
        })
        .await
        .expect_err("reject duplicate active sibling name");
    assert_eq!(
        conflict,
        ResourceApplicationError::Store(ResourceStoreError::NameConflict)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $1",
        )
        .bind(conflicting_artifact_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("verify Artifact insert rollback"),
        0,
    );

    let binary = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/pdf").expect("PDF media type"),
            bytes: b"%PDF-1.7 governed user upload".to_vec(),
        })
        .await
        .expect("stage Product binary file");
    let binary_artifact_id = ArtifactId::generate();
    let binary_committed = application
        .create_file(CreateResourceFile {
            context: context(tenant_id, principal_id, conversation_id, workspace_id, 102),
            identity: mutation("product-binary-upload"),
            expected_revision: ResourceRevision::new(1),
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("source.pdf"),
            sort_key: "source.pdf".to_owned(),
            artifact: Artifact {
                artifact_id: binary_artifact_id,
                tenant_id,
                source: ArtifactSource::Conversation(conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("first version"),
                title: "source.pdf".to_owned(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: ArtifactVersionId::generate(),
                artifact_id: binary_artifact_id,
                version: VersionNumber::new(1).expect("first version"),
                content_ref: binary.clone(),
                dependency_refs: Vec::new(),
                content_hash: binary.sha256,
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(participant_id),
                committed_at: now,
            },
        })
        .await
        .expect("atomically create Product binary file");
    assert_eq!(
        binary_committed.node.file_artifact_id,
        Some(binary_artifact_id)
    );
    assert_eq!(binary_committed.revision, ResourceRevision::new(2));

    // File cards know the Artifact, which need not share the Resource's UUID.
    // Resolve it through the original owner, then reuse the normal rename.
    let store = PostgresResourceStore::new(pool.clone());
    // File-name resolution, like Product file reads, requires the actual
    // human viewer; the Project fixture seeds that membership (5c7a0320c2).
    let rename_context = context(tenant_id, principal_id, conversation_id, workspace_id, 103);
    let target = store
        .resolve(&rename_context, artifact_id)
        .await
        .expect("resolve original placement");
    assert_eq!(target.node_id, node_id);
    assert_ne!(target.node_id.into_uuid(), artifact_id.into_uuid());
    assert_eq!(target.revision, ResourceRevision::new(2));
    let mut foreign = rename_context.clone();
    foreign.actor.principal_id = PrincipalId::generate();
    assert!(matches!(
        store.resolve(&foreign, artifact_id).await,
        Err(ResourceStoreError::NodeUnavailable)
    ));
    let collision = application
        .rename_node(RenameResourceNode {
            context: rename_context.clone(),
            identity: mutation("rename-collision"),
            expected_revision: target.revision,
            node_id,
            name: name("source.pdf"),
        })
        .await
        .expect_err("rename never replaces another file");
    assert_eq!(
        collision,
        ResourceApplicationError::Store(ResourceStoreError::NameConflict)
    );
    let rename = RenameResourceNode {
        context: rename_context.clone(),
        identity: mutation("rename-current-file"),
        expected_revision: target.revision,
        node_id,
        name: name("工作报告.md"),
    };
    let renamed = application
        .rename_node(rename.clone())
        .await
        .expect("rename by resolved file identity");
    assert_eq!(renamed.node.node_id, node_id);
    assert_eq!(renamed.node.file_artifact_id, Some(artifact_id));
    assert_eq!(renamed.node.name.as_str(), "工作报告.md");
    assert!(
        application
            .rename_node(rename)
            .await
            .expect("replay rename")
            .duplicate
    );
    let original: (String, String, i64) = sqlx::query_as(
        "SELECT title, current_version::text, (SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$1)
         FROM artifact.artifacts WHERE artifact_id=$1")
        .bind(artifact_id.into_uuid()).fetch_one(&pool).await.expect("original artifact history");
    assert_eq!(original, ("README.md".to_owned(), "1".to_owned(), 1));
    assert_eq!(
        store
            .resolve(&rename_context, artifact_id)
            .await
            .expect("same source after rename")
            .node_id,
        node_id
    );
}

#[tokio::test]
async fn multi_file_change_set_is_one_revision_idempotent_and_all_or_nothing() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let tenant_id = TenantId::generate();
    let principal_id = PrincipalId::generate();
    let workspace_id = WorkspaceId::generate();
    let conversation_id = ConversationId::generate();
    seed_project_conversations(
        &pool,
        tenant_id,
        principal_id,
        workspace_id,
        [conversation_id, ConversationId::generate()],
    )
    .await;
    let participant_id = ParticipantId::from_uuid(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT execution_owner_participant_id
               FROM conversation.conversations WHERE conversation_id = $1",
        )
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read Product participant"),
    )
    .expect("participant identity");
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let html = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/html").expect("HTML media type"),
            bytes: b"<main>Hello</main>".to_vec(),
        })
        .await
        .expect("stage HTML");
    let css = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("text/css").expect("CSS media type"),
            bytes: b"main { color: blue; }".to_vec(),
        })
        .await
        .expect("stage CSS");
    let now = UtcTimestamp::from_unix_millis(200);
    let folder_id = ResourceNodeId::generate();
    let html_node_id = ResourceNodeId::generate();
    let css_node_id = ResourceNodeId::generate();
    let html_artifact_id = ArtifactId::generate();
    let css_artifact_id = ArtifactId::generate();
    let command = CommitResourceChangeSet {
        context: context(tenant_id, principal_id, conversation_id, workspace_id, 200),
        identity: mutation("web-source-set"),
        expected_revision: ResourceRevision::ZERO,
        mutations: vec![
            ResourceChangeSetMutation::CreateFolder {
                mutation_key: idempotency("folder"),
                node_id: folder_id,
                parent_node_id: None,
                name: name("src"),
                sort_key: "src".to_owned(),
            },
            product_file_mutation(
                "html",
                tenant_id,
                conversation_id,
                participant_id,
                html_node_id,
                folder_id,
                "index.html",
                html_artifact_id,
                html,
                now,
            ),
            product_file_mutation(
                "css",
                tenant_id,
                conversation_id,
                participant_id,
                css_node_id,
                folder_id,
                "style.css",
                css_artifact_id,
                css,
                now,
            ),
            ResourceChangeSetMutation::Rename {
                mutation_key: idempotency("rename-html"),
                node_id: html_node_id,
                name: name("app.html"),
            },
        ],
    };
    let application = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let committed = application
        .commit_change_set(command.clone())
        .await
        .expect("commit one multi-file revision");
    assert_eq!(committed.previous_revision, ResourceRevision::ZERO);
    assert_eq!(committed.revision, ResourceRevision::new(1));
    assert_eq!(committed.mutations.len(), 4);
    assert!(
        committed
            .mutations
            .iter()
            .all(|receipt| receipt.node.revision == ResourceRevision::new(1))
    );
    assert_eq!(committed.mutations[3].node.name.as_str(), "app.html");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM resource.change_set_mutations WHERE change_set_id = $1",
        )
        .bind(command.identity.change_set_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("read ordered change-set receipts"),
        4,
    );
    let replayed = application
        .commit_change_set(command)
        .await
        .expect("replay exact multi-file change set");
    assert!(replayed.duplicate);
    assert_eq!(replayed.mutations, committed.mutations);

    let first_conflict_artifact = ArtifactId::generate();
    let second_conflict_artifact = ArtifactId::generate();
    let failing_identity = mutation("conflicting-web-source-set");
    let failing = CommitResourceChangeSet {
        context: context(tenant_id, principal_id, conversation_id, workspace_id, 201),
        identity: failing_identity.clone(),
        expected_revision: ResourceRevision::new(1),
        mutations: vec![
            product_file_mutation(
                "conflict-a",
                tenant_id,
                conversation_id,
                participant_id,
                ResourceNodeId::generate(),
                folder_id,
                "same.css",
                first_conflict_artifact,
                content
                    .put(PutContent {
                        tenant_id,
                        media_type: MediaType::new("text/css").expect("CSS media type"),
                        bytes: b"a{}".to_vec(),
                    })
                    .await
                    .expect("stage first conflict"),
                now,
            ),
            product_file_mutation(
                "conflict-b",
                tenant_id,
                conversation_id,
                participant_id,
                ResourceNodeId::generate(),
                folder_id,
                "same.css",
                second_conflict_artifact,
                content
                    .put(PutContent {
                        tenant_id,
                        media_type: MediaType::new("text/css").expect("CSS media type"),
                        bytes: b"b{}".to_vec(),
                    })
                    .await
                    .expect("stage second conflict"),
                now,
            ),
        ],
    };
    assert_eq!(
        application
            .commit_change_set(failing)
            .await
            .expect_err("reject whole set before publishing a partial tree"),
        ResourceApplicationError::Store(ResourceStoreError::NameConflict),
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT
                (SELECT count(*) FROM artifact.artifacts WHERE artifact_id IN ($1, $2)),
                (SELECT count(*) FROM resource.change_sets WHERE change_set_id = $3),
                (SELECT revision FROM resource.scope_heads
                  WHERE tenant_id = $4 AND scope_kind = 1 AND scope_id = $5)",
        )
        .bind(first_conflict_artifact.into_uuid())
        .bind(second_conflict_artifact.into_uuid())
        .bind(failing_identity.change_set_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(workspace_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("verify no partial change-set state"),
        (0, 0, 1),
    );
}

#[tokio::test]
async fn resource_tool_commit_is_atomic_conflict_safe_and_restart_idempotent() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    sqlx::query(
        "UPDATE conversation.conversations
            SET workspace_id = $1
          WHERE tenant_id = $2 AND conversation_id = $3",
    )
    .bind(workspace_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("attach running Conversation to Project");

    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::clone(&keys),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        Arc::clone(&keys),
    );
    let committer =
        PostgresFencedResourceToolCommitter::new(pool.clone(), codec, Arc::clone(&keys));
    let invocation_id =
        start_resource_tool(&pool, &content, &journal, &attempt, fixture, "atomic").await;
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"status":"committed","revision":1}"#.to_vec(),
        })
        .await
        .expect("persist Resource Tool result");
    let committed_at = running_loop::database_now(&pool).await;
    let node_id = ResourceNodeId::from_uuid(invocation_id.into_uuid()).expect("Resource node ID");
    let command = CommitPreparedResourceTool {
        run_context: running_loop::run_context(fixture),
        invocation_id,
        result: CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("resource.folder.create.output@1")
                    .expect("result schema"),
                schema_hash: Sha256::digest(b"resource.folder.create.output.v1"),
            },
            provider_request_ref: None,
        },
        mutation: PreparedResourceMutation::CreateFolder(CreateResourceFolder {
            context: tool_context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                invocation_id,
                committed_at,
            ),
            identity: ResourceMutationIdentity {
                change_set_id: ResourceChangeSetId::from_uuid(invocation_id.into_uuid())
                    .expect("change set ID"),
                idempotency_key: IdempotencyKey::new("resource-tool:atomic")
                    .expect("Resource idempotency"),
                command_fingerprint: Sha256::digest(b"resource-tool:atomic"),
            },
            expected_revision: ResourceRevision::ZERO,
            node_id,
            parent_node_id: None,
            name: name("Atomic"),
            sort_key: "atomic".to_owned(),
        }),
        committed_at,
    };

    sqlx::raw_sql(
        "CREATE FUNCTION tool.reject_resource_result_commit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected Resource result failure'; END $$;
         CREATE TRIGGER reject_resource_result_commit
         BEFORE INSERT ON tool.tool_results
         FOR EACH ROW EXECUTE FUNCTION tool.reject_resource_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("install Resource commit failure trigger");
    assert!(committer.commit(&attempt, command.clone()).await.is_err());
    let rolled_back: (i16, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.nodes WHERE node_id = $2),
                (SELECT count(*) FROM resource.change_sets WHERE change_set_id = $3),
                run.last_run_sequence::text, stream.last_sequence::text
           FROM tool.tool_invocations AS invocation
           JOIN runtime.runs AS run ON run.run_id = invocation.run_id
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = invocation.tenant_id
            AND stream.conversation_id = invocation.conversation_id
          WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(node_id.into_uuid())
    .bind(command.mutation.identity().change_set_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read atomically rolled-back Resource state");
    assert_eq!(rolled_back.0, 3, "Tool must remain Running after rollback");
    assert_eq!((rolled_back.1, rolled_back.2), (0, 0));
    assert_eq!(rolled_back.3, rolled_back.4);

    sqlx::raw_sql(
        "DROP TRIGGER reject_resource_result_commit ON tool.tool_results;
         DROP FUNCTION tool.reject_resource_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("remove Resource commit failure trigger");
    drop(committer);
    let restarted = PostgresFencedResourceToolCommitter::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    let committed = restarted
        .commit(&attempt, command.clone())
        .await
        .expect("commit Resource mutation after restart");
    assert!(!committed.duplicate);
    assert_eq!(committed.node.node_id, node_id);
    assert_eq!(committed.revision, ResourceRevision::new(1));
    let duplicate = restarted
        .commit(&attempt, command)
        .await
        .expect("replay committed Resource mutation");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.node, committed.node);
    assert_eq!(duplicate.revision, committed.revision);

    let conflict_invocation_id =
        start_resource_tool(&pool, &content, &journal, &attempt, fixture, "conflict").await;
    let conflict_result = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"status":"committed","revision":1}"#.to_vec(),
        })
        .await
        .expect("persist conflicting Tool result");
    let conflict_time = running_loop::database_now(&pool).await;
    let conflict_node_id =
        ResourceNodeId::from_uuid(conflict_invocation_id.into_uuid()).expect("Resource node ID");
    let conflict_change_set =
        ResourceChangeSetId::from_uuid(conflict_invocation_id.into_uuid()).expect("change set ID");
    let before_conflict: (String, String) = sqlx::query_as(
        "SELECT run.last_run_sequence::text, stream.last_sequence::text
           FROM runtime.runs AS run
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = run.tenant_id
            AND stream.conversation_id = run.conversation_id
          WHERE run.run_id = $1",
    )
    .bind(fixture.run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read pre-conflict sequences");
    let conflict = restarted
        .commit(
            &attempt,
            CommitPreparedResourceTool {
                run_context: running_loop::run_context(fixture),
                invocation_id: conflict_invocation_id,
                result: CapabilityResult {
                    result_hash: conflict_result.sha256,
                    result_ref: conflict_result,
                    result_schema: SchemaRef {
                        schema_key: VersionedKey::new("resource.folder.create.output@1")
                            .expect("result schema"),
                        schema_hash: Sha256::digest(b"resource.folder.create.output.v1"),
                    },
                    provider_request_ref: None,
                },
                mutation: PreparedResourceMutation::CreateFolder(CreateResourceFolder {
                    context: tool_context(
                        fixture.tenant_id,
                        fixture.principal_id,
                        fixture.conversation_id,
                        workspace_id,
                        conflict_invocation_id,
                        conflict_time,
                    ),
                    identity: ResourceMutationIdentity {
                        change_set_id: conflict_change_set,
                        idempotency_key: IdempotencyKey::new("resource-tool:conflict")
                            .expect("Resource idempotency"),
                        command_fingerprint: Sha256::digest(b"resource-tool:conflict"),
                    },
                    expected_revision: ResourceRevision::ZERO,
                    node_id: conflict_node_id,
                    parent_node_id: None,
                    name: name("Conflict"),
                    sort_key: "conflict".to_owned(),
                }),
                committed_at: conflict_time,
            },
        )
        .await
        .expect_err("reject stale Resource revision");
    assert!(matches!(
        conflict,
        musteragent_postgres::PostgresFencedResourceToolError::Resource(
            ResourceStoreError::RevisionConflict
        )
    ));
    let after_conflict: (i16, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.nodes WHERE node_id = $2),
                (SELECT count(*) FROM resource.change_sets WHERE change_set_id = $3),
                run.last_run_sequence::text, stream.last_sequence::text
           FROM tool.tool_invocations AS invocation
           JOIN runtime.runs AS run ON run.run_id = invocation.run_id
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = invocation.tenant_id
            AND stream.conversation_id = invocation.conversation_id
          WHERE invocation.invocation_id = $1",
    )
    .bind(conflict_invocation_id.into_uuid())
    .bind(conflict_node_id.into_uuid())
    .bind(conflict_change_set.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read stale-revision rollback");
    assert_eq!(after_conflict.0, 3);
    assert_eq!((after_conflict.1, after_conflict.2), (0, 0));
    assert_eq!((after_conflict.3, after_conflict.4), before_conflict);
}

#[tokio::test]
async fn resource_change_set_tool_prepares_typed_folder_and_file_mutations() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    let folder_id = ResourceNodeId::generate();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let request_ref = content
        .put(PutContent {
        tenant_id: fixture.tenant_id,
        media_type: MediaType::new("application/json").expect("JSON media type"),
        bytes: format!(
            r#"{{"expected_revision":0,"mutations":[{{"operation":"create_folder","mutation_key":"folder","node_id":"{folder_id}","name":"src"}},{{"operation":"create_file","mutation_key":"file","parent":{{"mutation_key":"folder"}},"name":"main.py","media_type":"text/x-python","content":"print('ready')\n"}}]}}"#
        )
        .into_bytes(),
    })
    .await
    .expect("persist Tool request");
    let resources = Arc::new(ResourceApplication::new(PostgresResourceStore::new(
        pool.clone(),
    )));
    let tools = BuiltinResourceTools::new(content, resources);
    let invocation_id = InvocationId::generate();
    let prepared = tools
        .prepare(ResourceToolContext {
                    resolved_source: None,
            invocation: CapabilityInvocation {

                actor: Actor {
                    tenant_id: fixture.tenant_id,
                    principal_id: fixture.principal_id,
                    principal_kind: PrincipalKind::User,
                },
                tenant_id: fixture.tenant_id,
                conversation_id: fixture.conversation_id,
                run_id: fixture.run_id,
                requested_by_attempt_id: attempt.attempt_id,
                execution_attempt_id: attempt.attempt_id,
                execution_attempt_number: attempt.attempt_number,
                invocation_id,
                tool_call_id: ToolCallId::new("resource-change-set-prepare").expect("Tool call"),
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new("resource.change-set.commit@2").expect("Tool key"),
                ),
                capability_key: CapabilityKey::from_versioned_key(
                    VersionedKey::new("resource.change-set.commit@2").expect("Capability key"),
                ),
                capability_provider_id: CapabilityProviderId::generate(),
                idempotency_key: IdempotencyKey::new("resource-change-set-prepare")
                    .expect("idempotency"),
                request_ref,
                request_schema: SchemaRef {
                    schema_key: VersionedKey::new("resource.change-set.commit.input@2")
                        .expect("input schema"),
                    schema_hash: Sha256::digest(b"change-set-input"),
                },
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("resource.change-set.commit.output@2")
                        .expect("output schema"),
                    schema_hash: Sha256::digest(b"change-set-output"),
                },
                authorized_content: Vec::new(),
                workspace_id: Some(workspace_id),
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
                trace_context: TraceContext {
                    traceparent: TraceParent::new(
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    )
                    .expect("traceparent"),
                    tracestate: None,
                },
                execution_grant_proof: None,
            },
            run_context: running_loop::run_context(fixture),
            now: running_loop::database_now(&pool).await,
        })
        .await
        .expect("prepare atomic change set");
    let ResourceToolPreparation::Commit { result, mutation } = prepared else {
        panic!("change-set Tool must prepare a commit");
    };
    assert_eq!(result.result_ref.media_type.as_str(), "application/json");
    let PreparedResourceMutation::ChangeSet(command) = *mutation else {
        panic!("change-set Tool must preserve the typed batch command");
    };
    assert_eq!(command.expected_revision, ResourceRevision::ZERO);
    assert_eq!(command.mutations.len(), 2);
    assert!(matches!(
        &command.mutations[0],
        ResourceChangeSetMutation::CreateFolder { node_id, .. } if *node_id == folder_id
    ));
    assert!(matches!(
        &command.mutations[1],
        ResourceChangeSetMutation::CreateFile { parent_node_id, artifact, version, .. }
            if *parent_node_id == Some(folder_id)
                && matches!(
                    artifact.origin,
                    ArtifactOrigin::Runtime { activity_id, created_by_run_id }
                        if activity_id == fixture.activity_id
                            && created_by_run_id == fixture.run_id
                )
                && version.source_invocation_id == Some(invocation_id)
    ));
}

#[tokio::test]
async fn resource_change_set_tool_commits_files_ledger_and_result_as_one_unit() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    let (fixture, attempt) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1
          WHERE tenant_id = $2 AND conversation_id = $3",
    )
    .bind(workspace_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("attach running Conversation to Project");

    let keys = running_loop::keyring();
    let content: Arc<dyn ContentStore> = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::clone(&keys),
    ));
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let journal = PostgresFencedToolInvocationJournal::new(
        pool.clone(),
        codec.clone(),
        codec.clone(),
        Arc::clone(&keys),
    );
    let invocation_id = start_resource_tool_with_contract(
        &pool,
        &content,
        &journal,
        &attempt,
        fixture,
        "change-set",
        "resource.change-set.commit@1",
        br#"{"expected_revision":0,"mutations":[]}"#.to_vec(),
    )
    .await;
    let committed_at = running_loop::database_now(&pool).await;
    let folder_id = ResourceNodeId::generate();
    let file_node_id = ResourceNodeId::generate();
    let artifact_id = ArtifactId::generate();
    let artifact_version_id = ArtifactVersionId::generate();
    let artifact_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/markdown").expect("Markdown media type"),
            bytes: b"# Atomic project\n".to_vec(),
        })
        .await
        .expect("persist File content");
    let result_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: br#"{"status":"committed","revision":1,"mutations":["folder","file"]}"#.to_vec(),
        })
        .await
        .expect("persist change-set Tool result");
    let change_set_id =
        ResourceChangeSetId::from_uuid(invocation_id.into_uuid()).expect("change set ID");
    let command = CommitPreparedResourceTool {
        run_context: running_loop::run_context(fixture),
        invocation_id,
        result: CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: SchemaRef {
                schema_key: VersionedKey::new("resource.change-set.commit.output@1")
                    .expect("result schema"),
                schema_hash: Sha256::digest(b"resource.change-set.commit.output.v1"),
            },
            provider_request_ref: None,
        },
        mutation: PreparedResourceMutation::ChangeSet(Box::new(CommitResourceChangeSet {
            context: tool_context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                invocation_id,
                committed_at,
            ),
            identity: ResourceMutationIdentity {
                change_set_id,
                idempotency_key: IdempotencyKey::new("resource-tool:change-set")
                    .expect("Resource idempotency"),
                command_fingerprint: Sha256::digest(b"resource-tool:change-set"),
            },
            expected_revision: ResourceRevision::ZERO,
            mutations: vec![
                ResourceChangeSetMutation::CreateFolder {
                    mutation_key: IdempotencyKey::new("folder").expect("mutation key"),
                    node_id: folder_id,
                    parent_node_id: None,
                    name: name("src"),
                    sort_key: "src".to_owned(),
                },
                ResourceChangeSetMutation::CreateFile {
                    mutation_key: IdempotencyKey::new("file").expect("mutation key"),
                    node_id: file_node_id,
                    parent_node_id: Some(folder_id),
                    name: name("README.md"),
                    sort_key: "readme.md".to_owned(),
                    artifact: Artifact {
                        artifact_id,
                        tenant_id: fixture.tenant_id,
                        source: ArtifactSource::Conversation(fixture.conversation_id),
                        origin: ArtifactOrigin::Runtime {
                            activity_id: fixture.activity_id,
                            created_by_run_id: fixture.run_id,
                        },
                        kind: ArtifactKind::File,
                        lifecycle: ArtifactLifecycle::Draft,
                        current_version: VersionNumber::new(1).expect("version"),
                        title: "README.md".to_owned(),
                        created_at: committed_at,
                        updated_at: committed_at,
                    },
                    version: Box::new(ArtifactVersion {
                        artifact_version_id,
                        artifact_id,
                        version: VersionNumber::new(1).expect("version"),
                        content_hash: artifact_content.sha256,
                        content_ref: artifact_content,
                        dependency_refs: Vec::new(),
                        source_invocation_id: Some(invocation_id),
                        committed_by: ArtifactVersionAuthor::Participant(fixture.executor_id),
                        committed_at,
                    }),
                },
            ],
        })),
        committed_at,
    };

    let committer =
        PostgresFencedResourceToolCommitter::new(pool.clone(), codec, Arc::clone(&keys));
    sqlx::raw_sql(
        "CREATE FUNCTION tool.reject_change_set_result_commit()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'injected change-set result failure'; END $$;
         CREATE TRIGGER reject_change_set_result_commit
         BEFORE INSERT ON tool.tool_results
         FOR EACH ROW EXECUTE FUNCTION tool.reject_change_set_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("install result failure trigger");
    assert!(committer.commit(&attempt, command.clone()).await.is_err());
    let rolled_back: (i16, i64, i64, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.nodes WHERE node_id IN ($2, $3)),
                (SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $4),
                (SELECT count(*) FROM resource.change_sets WHERE change_set_id = $5),
                (SELECT count(*) FROM resource.change_set_mutations WHERE change_set_id = $5),
                run.last_run_sequence::text, stream.last_sequence::text
           FROM tool.tool_invocations AS invocation
           JOIN runtime.runs AS run ON run.run_id = invocation.run_id
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = invocation.tenant_id
            AND stream.conversation_id = invocation.conversation_id
          WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(folder_id.into_uuid())
    .bind(file_node_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(change_set_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read rolled-back change set");
    assert_eq!(rolled_back.0, 3, "Tool must remain Running after rollback");
    assert_eq!(
        (rolled_back.1, rolled_back.2, rolled_back.3, rolled_back.4),
        (0, 0, 0, 0)
    );
    assert_eq!(rolled_back.5, rolled_back.6);

    sqlx::raw_sql(
        "DROP TRIGGER reject_change_set_result_commit ON tool.tool_results;
         DROP FUNCTION tool.reject_change_set_result_commit();",
    )
    .execute(&pool)
    .await
    .expect("remove result failure trigger");
    drop(committer);
    let restarted = PostgresFencedResourceToolCommitter::new(
        pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        keys,
    );
    let committed = restarted
        .commit(&attempt, command.clone())
        .await
        .expect("commit change set after restart");
    assert!(!committed.duplicate);
    assert_eq!(committed.node.node_id, folder_id);
    assert_eq!(committed.revision, ResourceRevision::new(1));
    let duplicate = restarted
        .commit(&attempt, command)
        .await
        .expect("replay committed change set");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.node, committed.node);
    let durable: (i16, i64, i64, i64, i64, i64, String, String) = sqlx::query_as(
        "SELECT invocation.status,
                (SELECT count(*) FROM resource.nodes WHERE node_id IN ($2, $3)),
                (SELECT count(*) FROM artifact.artifacts WHERE artifact_id = $4),
                (SELECT count(*) FROM artifact.artifact_versions
                  WHERE artifact_version_id = $5),
                (SELECT count(*) FROM resource.change_sets WHERE change_set_id = $6),
                (SELECT count(*) FROM resource.change_set_mutations WHERE change_set_id = $6),
                run.last_run_sequence::text, stream.last_sequence::text
           FROM tool.tool_invocations AS invocation
           JOIN runtime.runs AS run ON run.run_id = invocation.run_id
           JOIN event_store.ledger_streams AS stream
             ON stream.tenant_id = invocation.tenant_id
            AND stream.conversation_id = invocation.conversation_id
          WHERE invocation.invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(folder_id.into_uuid())
    .bind(file_node_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(artifact_version_id.into_uuid())
    .bind(change_set_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable change-set state");
    assert_eq!(durable.0, 4);
    assert_eq!(
        (durable.1, durable.2, durable.3, durable.4, durable.5),
        (2, 1, 1, 1, 2)
    );
    assert_eq!(durable.6, durable.7);
    let events = file_events::artifacts(&pool, fixture.run_id).await;
    // Simulate the old projection that had the success receipt but no file facts.
    let reference = match &events[1] {
        musteragent_artifact::ArtifactEvent::VersionCommitted { content_ref, .. } => {
            content_ref.clone()
        }
        _ => panic!("version fact"),
    };
    let mut legacy = file_events::legacy_snapshot(fixture, invocation_id, reference, None);
    musteragent_postgres::PostgresArtifactStore::recover_resource_file_projection(
        &pool,
        &mut legacy,
    )
    .await
    .unwrap();
    assert_eq!(legacy.artifacts.len(), 1);
    assert_eq!(legacy.artifacts[0].artifact_id, artifact_id);
    assert_eq!(
        legacy.tool_timeline[0].output_artifact_ids,
        vec![artifact_id]
    );
    let recovered = legacy.clone();
    musteragent_postgres::PostgresArtifactStore::recover_resource_file_projection(
        &pool,
        &mut legacy,
    )
    .await
    .unwrap();
    assert_eq!(legacy, recovered);
    legacy.tool_timeline.clear();
    legacy.artifacts.clear(); // retracted/sibling source is absent
    musteragent_postgres::PostgresArtifactStore::recover_resource_file_projection(
        &pool,
        &mut legacy,
    )
    .await
    .unwrap();
    assert!(legacy.artifacts.is_empty());

    assert_eq!(
        events.len(),
        2,
        "exactly one creation and committed version after replay"
    );
    assert!(
        matches!(&events[0], musteragent_artifact::ArtifactEvent::Created {
        artifact_id: id, title, source_invocation_id: Some(invocation), ..
    } if *id == artifact_id && title.as_str() == "README.md" && *invocation == invocation_id)
    );
    assert!(
        matches!(&events[1], musteragent_artifact::ArtifactEvent::VersionCommitted {
        artifact_id: id, version, content_ref, ..
    } if *id == artifact_id && version.get() == 1 && content_ref.byte_length > 0)
    );
}
