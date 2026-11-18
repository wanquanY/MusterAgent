#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;
use musteragent_identity::{
    Actor, ConversationId, PrincipalId, PrincipalKind, ResourceNodeId, ResourceRevision, TenantId,
    WorkspaceId,
};
use musteragent_postgres::{PostgresResourceStore, PostgresWorkspaceResourceRootAuthority};
use musteragent_runtime_resource::{
    CreateResourceFolder, EnsureWorkspaceResourceRoot, ListResourceChildren,
    ListWorkspaceResourceChildren, ResourceApplication, WorkspaceResourceRootAuthority,
    WorkspaceResourceRootError,
};
use musteragent_testkit::IsolatedPostgres;
use resource_support::*;

#[tokio::test]
async fn empty_workspace_read_creates_no_conversation_and_denies_other_owners() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    sqlx::query("INSERT INTO resource.workspace_authority (tenant_id,workspace_id,status,authority_generation,created_at_ms,updated_at_ms) VALUES ($1,$2,1,1,1,1)")
        .bind(actor.tenant_id.into_uuid()).bind(workspace.into_uuid()).execute(&pool).await.unwrap();
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let readiness: (i16, i64) = sqlx::query_as("SELECT status,authority_generation FROM resource.workspace_authority WHERE tenant_id=$1 AND workspace_id=$2")
        .bind(actor.tenant_id.into_uuid()).bind(workspace.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(readiness, (2, 2));
    let query = ListWorkspaceResourceChildren {
        actor,
        workspace_id: workspace,
        parent_node_id: None,
        include_trashed: false,
        offset: 0,
        limit: 50,
    };
    let page = roots.list_children(query).await.unwrap();
    assert!(page.nodes.is_empty());
    assert_eq!(page.revision, ResourceRevision::ZERO);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.conversations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    for bad in [
        ListWorkspaceResourceChildren {
            actor: Actor {
                principal_id: PrincipalId::generate(),
                ..actor
            },
            ..query
        },
        ListWorkspaceResourceChildren {
            actor: Actor {
                tenant_id: TenantId::generate(),
                ..actor
            },
            ..query
        },
        ListWorkspaceResourceChildren {
            workspace_id: WorkspaceId::generate(),
            ..query
        },
        ListWorkspaceResourceChildren {
            parent_node_id: Some(ResourceNodeId::generate()),
            ..query
        },
    ] {
        assert_eq!(
            roots.list_children(bad).await.unwrap_err(),
            WorkspaceResourceRootError::IdentityConflict
        );
    }
    assert_eq!(
        roots
            .list_children(ListWorkspaceResourceChildren { limit: 0, ..query })
            .await
            .unwrap_err(),
        WorkspaceResourceRootError::InvalidQuery
    );
    sqlx::query("UPDATE resource.workspace_authority SET status=3,ready_at_ms=NULL,authority_generation=authority_generation+1 WHERE tenant_id=$1 AND workspace_id=$2")
        .bind(actor.tenant_id.into_uuid()).bind(workspace.into_uuid()).execute(&pool).await.unwrap();
    assert_eq!(
        roots.list_children(query).await.unwrap_err(),
        WorkspaceResourceRootError::IdentityConflict
    );
    assert_eq!(
        roots
            .ensure_root(EnsureWorkspaceResourceRoot {
                actor,
                workspace_id: workspace
            })
            .await
            .unwrap_err(),
        WorkspaceResourceRootError::IdentityConflict
    );
    let held: (i16, i64) = sqlx::query_as("SELECT status,authority_generation FROM resource.workspace_authority WHERE tenant_id=$1 AND workspace_id=$2")
        .bind(actor.tenant_id.into_uuid()).bind(workspace.into_uuid()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        held,
        (3, 3),
        "repeated root attestation must not revive held authority"
    );
}

#[tokio::test]
async fn independent_workspace_reads_existing_tree_with_stable_paging() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let tenant = TenantId::generate();
    let principal = PrincipalId::generate();
    let workspace = WorkspaceId::generate();
    let conversation = ConversationId::generate();
    seed_project_conversations(
        &pool,
        tenant,
        principal,
        workspace,
        [conversation, ConversationId::generate()],
    )
    .await;
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let actor = Actor {
        tenant_id: tenant,
        principal_id: principal,
        principal_kind: PrincipalKind::User,
    };
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let mut revision = ResourceRevision::ZERO;
    let mut ids = Vec::new();
    for label in ["a", "b"] {
        let node_id = ResourceNodeId::generate();
        ids.push(node_id);
        revision = app
            .create_folder(CreateResourceFolder {
                context: context(tenant, principal, conversation, workspace, 100),
                identity: mutation(label),
                expected_revision: revision,
                node_id,
                parent_node_id: None,
                name: name(label),
                sort_key: label.to_owned(),
            })
            .await
            .unwrap()
            .revision;
    }
    let query = ListWorkspaceResourceChildren {
        actor,
        workspace_id: workspace,
        parent_node_id: None,
        include_trashed: false,
        offset: 0,
        limit: 1,
    };
    let first = roots.list_children(query).await.unwrap();
    assert_eq!(first.nodes[0].node_id, ids[0]);
    assert_eq!(first.revision, revision);
    assert_eq!(first.next_offset, Some(1));
    let second = roots
        .list_children(ListWorkspaceResourceChildren { offset: 1, ..query })
        .await
        .unwrap();
    assert_eq!(second.nodes[0].node_id, ids[1]);
    assert_eq!(second.next_offset, None);
    let folder_query = ListWorkspaceResourceChildren {
        parent_node_id: Some(ids[0]),
        ..query
    };
    assert!(
        roots
            .list_children(folder_query)
            .await
            .unwrap()
            .nodes
            .is_empty()
    );
    let other_workspace = WorkspaceId::generate();
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: other_workspace,
        })
        .await
        .unwrap();
    assert_eq!(
        roots
            .list_children(ListWorkspaceResourceChildren {
                workspace_id: other_workspace,
                ..folder_query
            })
            .await
            .unwrap_err(),
        WorkspaceResourceRootError::IdentityConflict
    );
}

#[tokio::test]
async fn workspace_and_conversation_resources_share_the_full_page_boundary() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let tenant = TenantId::generate();
    let principal = PrincipalId::generate();
    let workspace = WorkspaceId::generate();
    let conversation = ConversationId::generate();
    seed_project_conversations(
        &pool,
        tenant,
        principal,
        workspace,
        [conversation, ConversationId::generate()],
    )
    .await;
    let actor = Actor {
        tenant_id: tenant,
        principal_id: principal,
        principal_kind: PrincipalKind::User,
    };
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let app = ResourceApplication::new(PostgresResourceStore::new(pool));
    let source = context(tenant, principal, conversation, workspace, 100);
    let mut revision = ResourceRevision::ZERO;
    let mut ids = Vec::new();
    for index in 0..501 {
        let label = format!("folder-{index:03}");
        let node_id = ResourceNodeId::generate();
        ids.push(node_id);
        revision = app
            .create_folder(CreateResourceFolder {
                context: source.clone(),
                identity: mutation(&label),
                expected_revision: revision,
                node_id,
                parent_node_id: None,
                name: name(&label),
                sort_key: label,
            })
            .await
            .unwrap()
            .revision;
    }
    let query = ListWorkspaceResourceChildren {
        actor,
        workspace_id: workspace,
        parent_node_id: None,
        include_trashed: false,
        offset: 0,
        limit: 500,
    };
    for offset in [0, 500] {
        let workspace_page = roots
            .list_children(ListWorkspaceResourceChildren { offset, ..query })
            .await
            .unwrap();
        let conversation_page = app
            .list_children(ListResourceChildren {
                context: source.clone(),
                parent_node_id: None,
                include_trashed: false,
                offset,
                limit: 500,
            })
            .await
            .unwrap();
        assert_eq!(workspace_page, conversation_page);
        assert_eq!(workspace_page.revision, revision);
        assert_eq!(workspace_page.next_offset, (offset == 0).then_some(500));
        assert_eq!(
            workspace_page
                .nodes
                .iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>(),
            ids[offset..(offset + 500).min(ids.len())],
        );
    }
    for limit in [0, 501] {
        assert_eq!(
            roots
                .list_children(ListWorkspaceResourceChildren { limit, ..query })
                .await
                .unwrap_err(),
            WorkspaceResourceRootError::InvalidQuery
        );
    }
}

#[tokio::test]
async fn workspace_mutations_preserve_real_origin_and_require_current_owner() {
    use musteragent_identity::UtcTimestamp;
    use musteragent_runtime_resource::{
        RenameResourceNode, ResourceCommandContext, ResourceCommandSource, RestoreResourceNode,
        TrashResourceNode,
    };
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let actor = Actor {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let workspace = WorkspaceId::generate();
    let context = ResourceCommandContext {
        tenant_id: actor.tenant_id,
        actor,
        source: ResourceCommandSource::Workspace {
            workspace_id: workspace,
        },
        invocation_id: None,
        tool_key: None,
        now: UtcTimestamp::from_unix_millis(100),
    };
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let command = CreateResourceFolder {
        context: context.clone(),
        identity: mutation("workspace-folder"),
        expected_revision: ResourceRevision::ZERO,
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: name("documents"),
        sort_key: "documents".to_owned(),
    };
    assert!(app.create_folder(command.clone()).await.is_err());
    roots
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor,
            workspace_id: workspace,
        })
        .await
        .unwrap();
    let created = app.create_folder(command.clone()).await.unwrap();
    assert_eq!(created.node.provenance.conversation_id, None);
    let duplicate = app.create_folder(command.clone()).await.unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, created.revision);
    let mut wrong_owner = command.clone();
    wrong_owner.context.actor.principal_id = PrincipalId::generate();
    assert!(app.create_folder(wrong_owner).await.is_err());
    let renamed = app
        .rename_node(RenameResourceNode {
            context: context.clone(),
            identity: mutation("workspace-rename"),
            expected_revision: created.revision,
            node_id: created.node.node_id,
            name: name("资料"),
        })
        .await
        .unwrap();
    let trashed = app
        .trash_node(TrashResourceNode {
            context: context.clone(),
            identity: mutation("workspace-trash"),
            expected_revision: renamed.revision,
            node_id: created.node.node_id,
        })
        .await
        .unwrap();
    let restored = app
        .restore_node(RestoreResourceNode {
            context,
            identity: mutation("workspace-restore"),
            expected_revision: trashed.revision,
            node_id: created.node.node_id,
            target_parent_node_id: None,
            name: None,
        })
        .await
        .unwrap();
    assert_eq!(restored.revision, ResourceRevision::new(4));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.conversations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
