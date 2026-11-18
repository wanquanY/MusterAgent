use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_postgres::{PostgresExecutionWorkspaceAuthority, PostgresResourceStore, migrate};
use musteragent_resource::{ResourceName, ResourceScope};
use musteragent_runtime_resource::*;
use musteragent_testkit::IsolatedPostgres;

async fn seed(
    pool: &sqlx::PgPool,
    actor: Actor,
    project: Option<WorkspaceId>,
) -> EnsureExecutionWorkspace {
    let cid = ConversationId::generate();
    let owner = ParticipantId::generate();
    let mut tx = pool.begin().await.expect("fixture transaction");
    sqlx::query("INSERT INTO conversation.conversations (conversation_id,tenant_id,kind,title,control_home_kind,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,workspace_id,execution_owner_participant_id) VALUES ($1,$2,1,'Private work',1,1,1,$3,1,1,$4,$5)")
        .bind(cid.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(project.map(WorkspaceId::into_uuid)).bind(owner.into_uuid()).execute(&mut *tx).await.expect("conversation");
    let agent = AgentId::generate();
    sqlx::query("INSERT INTO agent.agent_definitions(agent_id,agent_key,built_in,lifecycle,display_name,created_at_ms,updated_at_ms) VALUES($1,$2,false,1,'Test executor',1,1)")
        .bind(agent.into_uuid()).bind(format!("test-{agent}")).execute(&mut *tx).await.expect("custom Agent");
    sqlx::query("INSERT INTO conversation.participants (participant_id,tenant_id,conversation_id,kind,agent_ref,display_name,joined_at_ms) VALUES ($1,$2,$3,2,$4,'Agent',1)")
        .bind(owner.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(cid.into_uuid()).bind(agent.into_uuid()).execute(&mut *tx).await.expect("execution owner");
    sqlx::query("INSERT INTO conversation.participants (participant_id,tenant_id,conversation_id,kind,principal_ref,display_name,joined_at_ms) VALUES ($1,$2,$3,1,$4,'User',1)")
        .bind(ParticipantId::generate().into_uuid()).bind(actor.tenant_id.into_uuid()).bind(cid.into_uuid()).bind(actor.principal_id.into_uuid()).execute(&mut *tx).await.expect("participant");
    sqlx::query("INSERT INTO event_store.ledger_streams (tenant_id,conversation_id,home_epoch,created_at_ms,updated_at_ms) VALUES ($1,$2,1,1,1)")
        .bind(actor.tenant_id.into_uuid()).bind(cid.into_uuid()).execute(&mut *tx).await.expect("ledger authority");
    tx.commit().await.expect("seed");
    EnsureExecutionWorkspace {
        actor,
        conversation_id: cid,
        expected_branch_id: None,
        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
        now: UtcTimestamp::from_unix_millis(100),
    }
}

fn actor() -> Actor {
    Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    }
}
fn context(
    request: EnsureExecutionWorkspace,
    receipt: &ExecutionWorkspaceReceipt,
) -> ResourceCommandContext {
    ResourceCommandContext {
        tenant_id: request.actor.tenant_id,
        actor: request.actor,
        source: ResourceCommandSource::ExecutionWorkspace(receipt.binding),
        invocation_id: None,
        tool_key: None,
        now: request.now,
    }
}
fn identity() -> ResourceMutationIdentity {
    let id = ResourceChangeSetId::generate();
    ResourceMutationIdentity {
        change_set_id: id,
        idempotency_key: IdempotencyKey::new(id.to_string()).expect("key"),
        command_fingerprint: Sha256::digest(id.to_string()),
    }
}
fn folder(context: &ResourceCommandContext, revision: u64, name: &str) -> CreateResourceFolder {
    CreateResourceFolder {
        context: context.clone(),
        identity: identity(),
        expected_revision: ResourceRevision::new(revision),
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: ResourceName::new(name).expect("name"),
        sort_key: name.into(),
    }
}
fn list(context: &ResourceCommandContext) -> ListResourceChildren {
    ListResourceChildren {
        context: context.clone(),
        parent_node_id: None,
        include_trashed: false,
        offset: 0,
        limit: 100,
    }
}

#[tokio::test]
async fn concurrent_registration_and_new_store_restore_the_same_private_tree() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let user = actor();
    let project = Some(WorkspaceId::generate());
    let request = seed(&pool, user, project).await;
    let authority = PostgresExecutionWorkspaceAuthority::new(pool.clone());
    let (a, b) = tokio::join!(authority.ensure(request), authority.ensure(request));
    let a = a.expect("first registration");
    assert_eq!(a, b.expect("concurrent registration"));
    let ctx = context(request, &a);
    assert_eq!(
        ctx.scope(),
        ResourceScope::ExecutionWorkspace(a.binding.execution_workspace_id)
    );
    let store = PostgresResourceStore::new(pool.clone());
    let command = folder(&ctx, 0, "工作文件");
    let receipt = store
        .create_folder(&command)
        .await
        .expect("write private folder");
    assert_eq!(receipt.revision.get(), 1);
    assert!(
        store
            .create_folder(&command)
            .await
            .expect("recover ACK loss")
            .duplicate
    );
    let reopened = PostgresExecutionWorkspaceAuthority::new(pool.clone())
        .ensure(request)
        .await
        .expect("reopen");
    assert_eq!(a, reopened);
    let files = PostgresResourceStore::new(pool.clone())
        .list_children(&list(&context(request, &reopened)))
        .await
        .expect("restored tree");
    assert_eq!(files.nodes.len(), 1);
    assert_eq!(files.nodes[0].node_id, command.node_id);
    let mut shared = ctx.clone();
    shared.source = ResourceCommandSource::Conversation {
        conversation_id: request.conversation_id,
        workspace_id: project,
    };
    assert!(
        store
            .list_children(&list(&shared))
            .await
            .expect("shared tree")
            .nodes
            .is_empty()
    );
    let other = seed(&pool, user, project).await;
    let second = authority.ensure(other).await.expect("another conversation");
    assert_ne!(
        a.binding.execution_workspace_id,
        second.binding.execution_workspace_id
    );
    assert!(
        store
            .list_children(&list(&context(other, &second)))
            .await
            .expect("other tree")
            .nodes
            .is_empty()
    );
    let mut forged = ctx.clone();
    forged.source = ResourceCommandSource::ExecutionWorkspace(ExecutionWorkspaceBinding {
        conversation_id: other.conversation_id,
        ..a.binding
    });
    assert!(store.list_children(&list(&forged)).await.is_err());
    assert!(
        store
            .create_folder(&folder(&forged, 1, "forged"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn stale_writes_conflict_and_multi_node_changes_roll_back_together() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let request = seed(&pool, actor(), None).await;
    let binding = PostgresExecutionWorkspaceAuthority::new(pool.clone())
        .ensure(request)
        .await
        .expect("root");
    let ctx = context(request, &binding);
    let store = PostgresResourceStore::new(pool.clone());
    let a = folder(&ctx, 0, "a");
    let b = folder(&ctx, 0, "b");
    let (left, right) = tokio::join!(store.create_folder(&a), store.create_folder(&b));
    assert_ne!(left.is_ok(), right.is_ok());
    let current = store.list_children(&list(&ctx)).await.expect("one winner");
    assert_eq!(current.revision.get(), 1);
    assert_eq!(current.nodes.len(), 1);
    let key = |s: &str| IdempotencyKey::new(s).expect("key");
    let changes = CommitResourceChangeSet {
        context: ctx.clone(),
        identity: identity(),
        expected_revision: current.revision,
        mutations: vec![
            ResourceChangeSetMutation::CreateFolder {
                mutation_key: key("create"),
                node_id: ResourceNodeId::generate(),
                parent_node_id: None,
                name: ResourceName::new("candidate").expect("name"),
                sort_key: "candidate".into(),
            },
            ResourceChangeSetMutation::Move {
                mutation_key: key("invalid-move"),
                node_id: current.nodes[0].node_id,
                target_parent_node_id: Some(ResourceNodeId::generate()),
                name: None,
            },
        ],
    };
    assert!(store.commit_change_set(&changes).await.is_err());
    let after = store
        .list_children(&list(&ctx))
        .await
        .expect("no partial changes");
    assert_eq!(after.revision, current.revision);
    assert_eq!(after.nodes, current.nodes);
}

#[tokio::test]
async fn revocation_and_home_changes_invalidate_reads_writes_and_duplicate_receipts() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let request = seed(&pool, actor(), None).await;
    let authority = PostgresExecutionWorkspaceAuthority::new(pool.clone());
    let binding = authority.ensure(request).await.expect("root");
    let ctx = context(request, &binding);
    let store = PostgresResourceStore::new(pool.clone());
    let command = folder(&ctx, 0, "private");
    store.create_folder(&command).await.expect("write");
    sqlx::query("UPDATE conversation.conversations SET home_epoch=2 WHERE conversation_id=$1")
        .bind(request.conversation_id.into_uuid())
        .execute(&pool)
        .await
        .expect("Home change");
    assert_eq!(
        authority.ensure(request).await,
        Err(ExecutionWorkspaceError::AuthorityChanged)
    );
    assert!(store.list_children(&list(&ctx)).await.is_err());
    assert!(store.create_folder(&command).await.is_err());
    let fresh_request = EnsureExecutionWorkspace {
        expected_home_epoch: HomeEpoch::new(2).expect("epoch"),
        ..request
    };
    let fresh = authority
        .ensure(fresh_request)
        .await
        .expect("new authority");
    assert_eq!(
        fresh.binding.execution_workspace_id,
        binding.binding.execution_workspace_id
    );
    let fresh_ctx = context(fresh_request, &fresh);
    assert_eq!(
        store
            .list_children(&list(&fresh_ctx))
            .await
            .expect("fresh read")
            .nodes
            .len(),
        1
    );
    sqlx::query(
        "UPDATE conversation.participants SET left_at_ms=101 WHERE conversation_id=$1 AND kind=1",
    )
    .bind(request.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("revoke");
    assert_eq!(
        authority.ensure(fresh_request).await,
        Err(ExecutionWorkspaceError::Unavailable)
    );
    assert!(store.list_children(&list(&fresh_ctx)).await.is_err());
    assert!(
        store
            .create_folder(&CreateResourceFolder {
                context: fresh_ctx,
                ..command
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn database_rejects_unbound_roots_rebinding_and_legacy_private_scope_transfers() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let request = seed(&pool, actor(), Some(WorkspaceId::generate())).await;
    let binding = PostgresExecutionWorkspaceAuthority::new(pool.clone())
        .ensure(request)
        .await
        .expect("root");
    let ctx = context(request, &binding);
    let store = PostgresResourceStore::new(pool.clone());
    let command = folder(&ctx, 0, "private");
    store.create_folder(&command).await.expect("write");
    assert!(sqlx::query("INSERT INTO resource.scope_heads (tenant_id,scope_kind,scope_id,revision,updated_at_ms) VALUES ($1,3,$2,0,1)")
        .bind(ctx.tenant_id.into_uuid()).bind(ExecutionWorkspaceId::generate().into_uuid()).execute(&pool).await.is_err());
    assert!(
        sqlx::query("DELETE FROM resource.execution_workspaces WHERE execution_workspace_id=$1")
            .bind(binding.binding.execution_workspace_id.into_uuid())
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("UPDATE resource.execution_workspaces SET created_at_ms=999 WHERE execution_workspace_id=$1")
        .bind(binding.binding.execution_workspace_id.into_uuid()).execute(&pool).await.is_err());
    assert!(
        sqlx::query("UPDATE resource.nodes SET scope_kind=1,scope_id=$2 WHERE node_id=$1")
            .bind(command.node_id.into_uuid())
            .bind(ctx.workspace_id().expect("project").into_uuid())
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE resource.nodes SET source_conversation_id=NULL WHERE node_id=$1")
            .bind(command.node_id.into_uuid())
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn tree_is_complete_bounded_and_uses_original_file_versions_without_body_reads() {
    use musteragent_artifact::{
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
        ArtifactVersionAuthor,
    };
    use musteragent_content_ref::{ContentRef, MediaType};
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let request = seed(&pool, actor(), Some(WorkspaceId::generate())).await;
    let receipt = PostgresExecutionWorkspaceAuthority::new(pool.clone())
        .ensure(request)
        .await
        .expect("root");
    let ctx = context(request, &receipt);
    let store = PostgresResourceStore::new(pool.clone());
    let dir = folder(&ctx, 0, "原始 数据");
    store.create_folder(&dir).await.expect("directory");
    let reference = ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/csv").expect("type"),
        byte_length: 100,
        sha256: Sha256::digest(b"metadata fixture"),
    };
    // Deliberately unreadable locator: tree preparation must query metadata only.
    sqlx::query("INSERT INTO artifact.content_refs (content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms) VALUES ($1,$2,'text/csv',100,$3,'test://metadata-only',1)")
        .bind(reference.content_id.into_uuid()).bind(ctx.tenant_id.into_uuid()).bind(reference.sha256.into_bytes().to_vec()).execute(&pool).await.expect("metadata fixture");
    let participant: uuid::Uuid = sqlx::query_scalar(
        "SELECT participant_id FROM conversation.participants WHERE conversation_id=$1 AND kind=1",
    )
    .bind(request.conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("author");
    let aid = ArtifactId::generate();
    let version_id = ArtifactVersionId::generate();
    let node = ResourceNodeId::generate();
    let artifact = Artifact {
        artifact_id: aid,
        tenant_id: ctx.tenant_id,
        source: ArtifactSource::Conversation(request.conversation_id),
        origin: ArtifactOrigin::Product,
        kind: ArtifactKind::File,
        lifecycle: ArtifactLifecycle::Draft,
        current_version: VersionNumber::new(1).expect("version"),
        title: "成交.csv".into(),
        created_at: ctx.now,
        updated_at: ctx.now,
    };
    let version = ArtifactVersion {
        artifact_version_id: version_id,
        artifact_id: aid,
        version: artifact.current_version,
        content_ref: reference.clone(),
        content_hash: reference.sha256,
        dependency_refs: vec![],
        source_invocation_id: None,
        committed_by: ArtifactVersionAuthor::Participant(
            ParticipantId::from_uuid(participant).expect("id"),
        ),
        committed_at: ctx.now,
    };
    ResourceApplication::new(store.clone())
        .create_file(CreateResourceFile {
            context: ctx.clone(),
            identity: identity(),
            expected_revision: ResourceRevision::new(1),
            node_id: node,
            parent_node_id: Some(dir.node_id),
            name: ResourceName::new("成交.csv").expect("name"),
            sort_key: "csv".into(),
            artifact,
            version,
        })
        .await
        .expect("file");
    store
        .create_folder(&folder(&ctx, 2, "空目录"))
        .await
        .expect("empty directory");
    let query = ReadExecutionWorkspaceTree {
        context: ctx.clone(),
        maximum_entries: 10,
        maximum_total_bytes: 100,
    };
    let tree = store.read_tree(query.clone()).await.expect("complete tree");
    assert_eq!(tree.revision.get(), 3);
    assert_eq!(tree.total_bytes, 100);
    assert_eq!(
        tree.entries
            .iter()
            .map(|e| e.path.as_str())
            .collect::<Vec<_>>(),
        ["原始 数据", "原始 数据/成交.csv", "空目录"]
    );
    let file = tree.entries[1]
        .resource
        .file
        .as_ref()
        .expect("file version");
    assert_eq!(file.artifact_version_id, version_id);
    assert_eq!(file.content_ref, reference);
    let located = store
        .resolve_artifact_placement(&ctx, aid)
        .await
        .expect("indexed original placement")
        .expect("file");
    assert_eq!(located.node.node_id, node);
    assert_eq!(located.file.unwrap().content_ref, reference);
    assert!(
        store
            .resolve_artifact_placement(&ctx, ArtifactId::generate())
            .await
            .expect("absent placement")
            .is_none()
    );
    assert!(matches!(
        store
            .read_tree(ReadExecutionWorkspaceTree {
                maximum_entries: 2,
                ..query.clone()
            })
            .await,
        Err(ResourceStoreError::BoundsExceeded)
    ));
    assert!(matches!(
        store
            .read_tree(ReadExecutionWorkspaceTree {
                maximum_total_bytes: 99,
                ..query.clone()
            })
            .await,
        Err(ResourceStoreError::BoundsExceeded)
    ));
    // The normal directory owner observes exactly these same file identities.
    let catalog = store
        .list_files(ListProjectFiles {
            produced_by: None,
            context: ctx.clone(),
            recursive: true,
            parent_node_id: None,
            offset: 0,
            limit: 100,
            name_query: None,
        })
        .await
        .expect("same catalog");
    assert_eq!(catalog.entries.len(), 3);
    assert_eq!(
        catalog
            .entries
            .iter()
            .find_map(|e| e.file.as_ref())
            .expect("catalog file")
            .artifact_version_id,
        version_id
    );
    let base = file.clone();
    let next_version = ArtifactVersion {
        artifact_version_id: ArtifactVersionId::generate(),
        artifact_id: aid,
        version: VersionNumber::new(2).expect("version"),
        content_ref: reference.clone(),
        content_hash: reference.sha256,
        dependency_refs: vec![],
        source_invocation_id: None,
        committed_by: ArtifactVersionAuthor::Participant(
            ParticipantId::from_uuid(participant).expect("author"),
        ),
        committed_at: ctx.now,
    };
    let replace = ResourceChangeSetMutation::ReplaceFile {
        mutation_key: IdempotencyKey::new("replace").expect("key"),
        file: base.clone(),
        version: Box::new(next_version.clone()),
    };
    let batch = CommitResourceChangeSet {
        context: ctx.clone(),
        identity: identity(),
        expected_revision: ResourceRevision::new(3),
        mutations: vec![
            replace.clone(),
            ResourceChangeSetMutation::Rename {
                mutation_key: IdempotencyKey::new("rename").expect("key"),
                node_id: node,
                name: ResourceName::new("已清洗.csv").expect("name"),
            },
        ],
    };
    batch.validate().expect("valid batch");
    let committed = store
        .commit_change_set(&batch)
        .await
        .expect("one atomic replacement and rename");
    assert_eq!(committed.revision.get(), 4);
    assert_eq!(committed.mutations[0].node.name.as_str(), "已清洗.csv");
    assert_eq!(
        committed.mutations[0].artifact_version_id,
        Some(next_version.artifact_version_id)
    );
    assert!(
        store
            .commit_change_set(&batch)
            .await
            .expect("ACK loss recovery")
            .duplicate
    );
    let reopened = PostgresResourceStore::new(pool.clone())
        .read_tree(query.clone())
        .await
        .expect("read new version");
    let edited = reopened
        .entries
        .iter()
        .find_map(|e| e.resource.file.as_ref())
        .expect("edited file");
    assert_eq!(edited.artifact_id, aid);
    assert_eq!(edited.version.get(), 2);
    assert_eq!(edited.artifact_version_id, next_version.artifact_version_id);
    assert!(
        reopened
            .entries
            .iter()
            .any(|e| e.path == "原始 数据/已清洗.csv")
    );
    // Prevalidation succeeds, the first real mutation writes, then the stale
    // byte replacement CAS fails. The transaction must undo all prior writes.
    let rejected = CommitResourceChangeSet {
        context: ctx.clone(),
        identity: identity(),
        expected_revision: ResourceRevision::new(4),
        mutations: vec![
            ResourceChangeSetMutation::CreateFolder {
                mutation_key: IdempotencyKey::new("must-rollback").expect("key"),
                node_id: ResourceNodeId::generate(),
                parent_node_id: None,
                name: ResourceName::new("partial").expect("name"),
                sort_key: "partial".into(),
            },
            replace,
        ],
    };
    assert_eq!(
        store.commit_change_set(&rejected).await,
        Err(ResourceStoreError::RevisionConflict)
    );
    let restored = store
        .read_tree(query.clone())
        .await
        .expect("after rollback");
    assert_eq!(restored.revision.get(), 4);
    assert_eq!(restored.entries.len(), 3);
    assert_eq!(
        store
            .resolve_version(&ctx, node, None)
            .await
            .expect("preserved version")
            .version
            .get(),
        2
    );
    let directory_query = ListResourceChildren {
        parent_node_id: Some(dir.node_id),
        ..list(&ctx)
    };
    let before_purpose = store
        .list_children(&directory_query)
        .await
        .expect("directory revision before Tool mutation");
    assert!(!before_purpose.nodes[0].is_result);
    let mark = CommitResourceChangeSet {
        context: ctx.clone(),
        identity: identity(),
        expected_revision: before_purpose.revision,
        mutations: vec![ResourceChangeSetMutation::SetResult {
            mutation_key: IdempotencyKey::new("result-purpose").expect("key"),
            node_id: node,
            is_result: true,
        }],
    };
    let marked = store.commit_change_set(&mark).await.expect("mark result");
    assert!(marked.mutations[0].node.is_result);
    let listed = store
        .list_children(&directory_query)
        .await
        .expect("result purpose in directory");
    assert_eq!(listed.revision, marked.revision);
    assert_eq!(listed.nodes[0], marked.mutations[0].node);
    let duplicate = store
        .commit_change_set(&mark)
        .await
        .expect("idempotent purpose");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.mutations, marked.mutations);
    assert_eq!(
        store
            .resolve_version(&ctx, node, None)
            .await
            .expect("same bytes")
            .artifact_version_id,
        next_version.artifact_version_id
    );
    let unmark = CommitResourceChangeSet {
        identity: identity(),
        expected_revision: marked.revision,
        mutations: vec![ResourceChangeSetMutation::SetResult {
            mutation_key: IdempotencyKey::new("working-purpose").expect("key"),
            node_id: node,
            is_result: false,
        }],
        ..mark.clone()
    };
    assert!(
        !store
            .commit_change_set(&unmark)
            .await
            .expect("keep as work")
            .mutations[0]
            .node
            .is_result
    );
    assert!(
        !store
            .list_children(&directory_query)
            .await
            .expect("working purpose in directory")
            .nodes[0]
            .is_result
    );
    assert!(
        store
            .commit_change_set(&mark)
            .await
            .expect("historical purpose receipt")
            .mutations[0]
            .node
            .is_result
    );
    let invalid_folder = CommitResourceChangeSet {
        identity: identity(),
        expected_revision: ResourceRevision::new(6),
        mutations: vec![ResourceChangeSetMutation::SetResult {
            mutation_key: IdempotencyKey::new("invalid-folder").expect("key"),
            node_id: dir.node_id,
            is_result: true,
        }],
        ..mark.clone()
    };
    assert!(store.commit_change_set(&invalid_folder).await.is_err());
    let project_context = ResourceCommandContext {
        source: ResourceCommandSource::Conversation {
            conversation_id: request.conversation_id,
            workspace_id: ctx.workspace_id(),
        },
        ..ctx.clone()
    };
    let copied_root = ResourceNodeId::generate();
    let copy = CommitResourceChangeSet {
        context: project_context.clone(),
        identity: identity(),
        expected_revision: ResourceRevision::new(0),
        mutations: vec![ResourceChangeSetMutation::CopyWorkTree {
            mutation_key: IdempotencyKey::new("copy-selected").expect("key"),
            node_id: copied_root,
            parent_node_id: None,
            name: ResourceName::new("共享报告").expect("name"),
            source_workspace_id: receipt.binding.execution_workspace_id,
            source_node_id: dir.node_id,
            source_revision: ResourceRevision::new(6),
            committed_by: ParticipantId::from_uuid(participant).expect("author"),
        }],
    };
    let copied = store
        .commit_change_set(&copy)
        .await
        .expect("atomic private to project copy");
    let files = store
        .list_files(ListProjectFiles {
            produced_by: None,
            context: project_context.clone(),
            recursive: true,
            parent_node_id: None,
            offset: 0,
            limit: 100,
            name_query: None,
        })
        .await
        .expect("project copy");
    assert_eq!(
        files.entries.len(),
        2,
        "only the selected subtree is shared"
    );
    let copied_file = files
        .entries
        .iter()
        .find_map(|e| e.file.as_ref())
        .expect("copied file");
    assert_ne!(copied_file.artifact_id, aid);
    assert_ne!(copied_file.node_id, node);
    assert_eq!(copied_file.content_ref, reference);
    let origin: uuid::Uuid = sqlx::query_scalar(
        "SELECT source_artifact_version_id FROM resource.copy_origins WHERE target_node_id=$1",
    )
    .bind(copied_file.node_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("formal source identity");
    assert_eq!(origin, next_version.artifact_version_id.into_uuid());
    store
        .create_folder(&folder(&ctx, 6, "新的私有文件夹"))
        .await
        .expect("advance source");
    assert!(
        store
            .commit_change_set(&copy)
            .await
            .expect("copy receipt survives source edits")
            .duplicate
    );
    let mut stale = copy.clone();
    stale.identity = identity();
    stale.expected_revision = copied.revision;
    if let ResourceChangeSetMutation::CopyWorkTree { node_id, name, .. } = &mut stale.mutations[0] {
        *node_id = ResourceNodeId::generate();
        *name = ResourceName::new("不能部分提交").expect("name");
    }
    assert_eq!(
        store.commit_change_set(&stale).await,
        Err(ResourceStoreError::RevisionConflict)
    );
    assert_eq!(
        store
            .list_files(ListProjectFiles {
                produced_by: None,
                context: project_context,
                recursive: true,
                parent_node_id: None,
                offset: 0,
                limit: 100,
                name_query: None
            })
            .await
            .expect("after conflict")
            .entries
            .len(),
        2
    );
    sqlx::query(
        "UPDATE conversation.participants SET left_at_ms=101 WHERE conversation_id=$1 AND kind=1",
    )
    .bind(request.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("revoke");
    assert!(store.read_tree(query).await.is_err());
    assert!(store.resolve_file(&ctx, node).await.is_err());
    assert!(store.resolve_version(&ctx, node, None).await.is_err());
    assert!(
        store.commit_change_set(&copy).await.is_err(),
        "revocation also fences receipt recovery"
    );
}

#[tokio::test]
async fn branch_switch_rejects_old_context_and_creates_a_distinct_root() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("PostgreSQL");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let request = seed(&pool, actor(), None).await;
    let authority = PostgresExecutionWorkspaceAuthority::new(pool.clone());
    let old = authority
        .ensure(request)
        .await
        .expect("initial branch root");
    let ctx = context(request, &old);
    let store = PostgresResourceStore::new(pool.clone());
    store
        .create_folder(&folder(&ctx, 0, "old branch"))
        .await
        .expect("write");
    let record = LedgerRecordId::generate();
    let branch = ContextBranchId::generate();
    let mut tx = pool.begin().await.expect("activation transaction");
    sqlx::query("UPDATE event_store.ledger_streams SET last_sequence=1 WHERE conversation_id=$1")
        .bind(request.conversation_id.into_uuid())
        .execute(&mut *tx)
        .await
        .expect("original branch fence");
    sqlx::query("INSERT INTO event_store.ledger_records (record_id,schema_version,record_kind,tenant_id,conversation_id,conversation_seq,home_epoch,producer,occurred_at_ms,committed_at_ms,correlation_id,idempotency_key,payload_hash) VALUES ($1,'musteragent.test/1',1,$2,$3,1,1,1,1,1,$4,'fixture',decode(repeat('00',32),'hex'))")
        .bind(record.into_uuid()).bind(ctx.tenant_id.into_uuid()).bind(request.conversation_id.into_uuid()).bind(CorrelationId::generate().into_uuid()).execute(&mut *tx).await.expect("branch origin record");
    sqlx::query("INSERT INTO conversation.context_branches (branch_id,tenant_id,conversation_id,fork_from_record_id,fork_from_sequence,created_by,created_sequence,created_at_ms,origin) VALUES ($1,$2,$3,$4,1,$5,1,1,1)")
        .bind(branch.into_uuid()).bind(ctx.tenant_id.into_uuid()).bind(request.conversation_id.into_uuid()).bind(record.into_uuid()).bind(ctx.actor.principal_id.into_uuid()).execute(&mut *tx).await.expect("branch");
    sqlx::query("INSERT INTO conversation.context_branch_state VALUES ($1,$2,$3,1,1)")
        .bind(ctx.tenant_id.into_uuid())
        .bind(request.conversation_id.into_uuid())
        .bind(branch.into_uuid())
        .execute(&mut *tx)
        .await
        .expect("activate");
    tx.commit().await.expect("activation commit");
    assert_eq!(
        authority.ensure(request).await,
        Err(ExecutionWorkspaceError::AuthorityChanged)
    );
    assert!(store.list_children(&list(&ctx)).await.is_err());
    assert!(
        store
            .create_folder(&folder(&ctx, 1, "stale"))
            .await
            .is_err()
    );
    let selected = EnsureExecutionWorkspace {
        expected_branch_id: Some(branch),
        ..request
    };
    let fresh = authority.ensure(selected).await.expect("selected branch");
    assert_ne!(
        fresh.binding.execution_workspace_id,
        old.binding.execution_workspace_id
    );
    assert!(
        store
            .list_children(&list(&context(selected, &fresh)))
            .await
            .expect("branch private tree")
            .nodes
            .is_empty()
    );
    assert_eq!(
        authority.ensure(selected).await.expect("reopen branch"),
        fresh
    );
}
