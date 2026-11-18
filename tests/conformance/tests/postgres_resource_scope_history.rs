//! P2 scope-history conformance: ownership proof survives a scope transfer.
//!
//! Assignment retains historical receipts on the immutable
//! `resource.scope_history` proof ledger.
//! This test exercises the trigger and the re-anchored foreign keys at the
//! SQL boundary the migration owns: create a node under one scope, record a
//! committed receipt, transfer the node to another scope, then verify that
//! both ownership generations are retained and the historical receipt still
//! resolves.

#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_identity::{
    ConversationId, PrincipalId, ResourceNodeId, ResourceRevision, TenantId, WorkspaceId,
};
use musteragent_postgres::PostgresResourceStore;
use musteragent_runtime_resource::{
    CommitResourceChangeSet, CreateResourceFolder, RenameResourceNode, ResourceApplication,
    ResourceChangeSetMutation,
};
use musteragent_testkit::IsolatedPostgres;
use resource_support::{context, mutation, name, seed_project_conversations};

#[tokio::test]
async fn migrated_conversation_fences_new_writes_but_preserves_exact_receipt_replay() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrations");
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
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = NULL WHERE conversation_id = $1",
    )
    .bind(conversation.into_uuid())
    .execute(&pool)
    .await
    .expect("ordinary conversation fixture");
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let mut old_context = context(tenant, principal, conversation, workspace, 100);
    if let musteragent_runtime_resource::ResourceCommandSource::Conversation {
        workspace_id: target,
        ..
    } = &mut old_context.source
    {
        *target = None;
    }
    let original_command = CreateResourceFolder {
        context: old_context.clone(),
        identity: mutation("pre-migration-folder"),
        expected_revision: ResourceRevision::ZERO,
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: name("Original"),
        sort_key: "a".to_owned(),
    };
    let original = app
        .create_folder(original_command.clone())
        .await
        .expect("original write");
    // Simulate the authoritative assignment boundary without exposing a product
    // migration API. A context resolved before this transaction is now stale.
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id = $1 WHERE conversation_id = $2",
    )
    .bind(workspace.into_uuid())
    .bind(conversation.into_uuid())
    .execute(&pool)
    .await
    .expect("assignment boundary");
    let stale = app
        .create_folder(CreateResourceFolder {
            context: old_context.clone(),
            identity: mutation("stale-folder-after-migration"),
            expected_revision: original.revision,
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("Stale"),
            sort_key: "b".to_owned(),
        })
        .await;
    assert!(
        matches!(
            stale,
            Err(
                musteragent_runtime_resource::ResourceApplicationError::Store(
                    musteragent_runtime_resource::ResourceStoreError::RevisionConflict
                )
            )
        ),
        "stale scope must reject new writes: {stale:?}"
    );
    let replay = app
        .create_folder(original_command)
        .await
        .expect("read original receipt");
    assert!(replay.duplicate);
    assert_eq!(replay.node, original.node);
    assert_eq!(replay.revision, original.revision);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.nodes WHERE tenant_id = $1")
        .bind(tenant.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("count nodes");
    assert_eq!(count, 1, "stale command must not leave a node behind");
    let batch = app
        .commit_change_set(CommitResourceChangeSet {
            context: old_context,
            identity: mutation("stale-batch-after-migration"),
            expected_revision: original.revision,
            mutations: vec![ResourceChangeSetMutation::CreateFolder {
                mutation_key: resource_support::idempotency("new-folder"),
                node_id: ResourceNodeId::generate(),
                parent_node_id: None,
                name: name("Stale batch"),
                sort_key: "c".to_owned(),
            }],
        })
        .await;
    assert!(matches!(
        batch,
        Err(
            musteragent_runtime_resource::ResourceApplicationError::Store(
                musteragent_runtime_resource::ResourceStoreError::RevisionConflict
            )
        )
    ));
}

#[tokio::test]
async fn owner_replays_original_receipt_after_node_changes() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrations");
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
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let command = CreateResourceFolder {
        context: context(tenant, principal, conversation, workspace, 100),
        identity: mutation("historical-receipt"),
        expected_revision: ResourceRevision::ZERO,
        node_id: ResourceNodeId::generate(),
        parent_node_id: None,
        name: name("Original"),
        sort_key: "a".to_owned(),
    };
    let original = app.create_folder(command.clone()).await.expect("create");
    app.rename_node(RenameResourceNode {
        context: context(tenant, principal, conversation, workspace, 101),
        identity: mutation("rename-after-receipt"),
        expected_revision: original.revision,
        node_id: command.node_id,
        name: name("Changed"),
    })
    .await
    .expect("rename");
    let replay = app.create_folder(command).await.expect("historical replay");
    assert!(replay.duplicate);
    assert_eq!(
        replay.node, original.node,
        "replay must not substitute the current node"
    );
    assert_eq!(replay.revision, original.revision);
}

#[tokio::test]
async fn batch_replays_original_nodes_and_rejects_snapshot_rewrite() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("migrations");
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
    let app = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let root = ResourceNodeId::generate();
    let child = ResourceNodeId::generate();
    let command = CommitResourceChangeSet {
        context: context(tenant, principal, conversation, workspace, 100),
        identity: mutation("batch-history"),
        expected_revision: ResourceRevision::ZERO,
        mutations: vec![
            ResourceChangeSetMutation::CreateFolder {
                mutation_key: resource_support::idempotency("root"),
                node_id: root,
                parent_node_id: None,
                name: name("Original"),
                sort_key: "a".to_owned(),
            },
            ResourceChangeSetMutation::CreateFolder {
                mutation_key: resource_support::idempotency("child"),
                node_id: child,
                parent_node_id: Some(root),
                name: name("Child"),
                sort_key: "b".to_owned(),
            },
        ],
    };
    let original = app
        .commit_change_set(command.clone())
        .await
        .expect("commit batch");
    app.rename_node(RenameResourceNode {
        context: context(tenant, principal, conversation, workspace, 101),
        identity: mutation("rename-batch-node"),
        expected_revision: original.revision,
        node_id: root,
        name: name("Changed"),
    })
    .await
    .expect("rename batch node");
    let replay = app
        .commit_change_set(command.clone())
        .await
        .expect("replay batch");
    assert!(replay.duplicate);
    assert_eq!(replay.mutations, original.mutations);
    assert_eq!(replay.revision, original.revision);
    for table in ["change_sets", "change_set_mutations"] {
        let result = sqlx::query(&format!(
            "UPDATE resource.{table} SET result_node_snapshot = '{{}}'::jsonb WHERE change_set_id = $1"
        )).bind(command.identity.change_set_id.into_uuid()).execute(&pool).await;
        assert!(
            result.is_err(),
            "snapshot rewrite must be rejected for {table}"
        );
    }
    assert_eq!(
        app.commit_change_set(command)
            .await
            .expect("replay after rejection")
            .mutations,
        original.mutations
    );
}

#[tokio::test]
async fn scope_transfer_retains_prior_ownership_generations_and_historical_receipts() {
    use musteragent_identity::{CommandId, CorrelationId, HomeEpoch, LedgerOffset, UtcTimestamp};
    use musteragent_postgres::{
        PostgresWorkspaceAssignmentPreparationStore, PostgresWorkspaceResourceRootAuthority,
    };
    use musteragent_runtime_resource::{
        EnsureWorkspaceResourceRoot, ResourceCommandSource, WorkspaceResourceRootAuthority,
    };
    use musteragent_runtime_session::workspace_assignment::{
        PrepareWorkspaceAssignment, WorkspaceAssignmentPreparationStore,
    };
    use musteragent_runtime_session::workspace_assignment_commit::{
        WorkspaceAssignmentCommit, WorkspaceAssignmentCommitStore,
    };
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated PostgreSQL");
    let pool = postgres.pool().await.expect("pool");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("baseline");
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
    sqlx::query("UPDATE conversation.conversations SET workspace_id=NULL WHERE conversation_id=$1")
        .bind(conversation.into_uuid())
        .execute(&pool)
        .await
        .expect("ordinary conversation");
    let mut original_context = context(tenant, principal, conversation, workspace, 100);
    if let ResourceCommandSource::Conversation { workspace_id, .. } = &mut original_context.source {
        *workspace_id = None;
    }
    let node = ResourceNodeId::generate();
    let create = CreateResourceFolder {
        context: original_context.clone(),
        identity: mutation("historical-scope-receipt"),
        expected_revision: ResourceRevision::ZERO,
        node_id: node,
        parent_node_id: None,
        name: name("Sources"),
        sort_key: "a".into(),
    };
    let resources = ResourceApplication::new(PostgresResourceStore::new(pool.clone()));
    let original = resources
        .create_folder(create.clone())
        .await
        .expect("original receipt");
    PostgresWorkspaceResourceRootAuthority::new(pool.clone())
        .ensure_root(EnsureWorkspaceResourceRoot {
            actor: original_context.actor,
            workspace_id: workspace,
        })
        .await
        .expect("ready target root");
    let request = PrepareWorkspaceAssignment {
        command_id: CommandId::generate(),
        actor: original_context.actor,
        conversation_id: conversation,
        target_workspace_id: workspace,
        expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
    };
    let assignments = PostgresWorkspaceAssignmentPreparationStore::new(pool.clone());
    assignments
        .prepare(request)
        .await
        .expect("prepare assignment");
    let commit = WorkspaceAssignmentCommit::new(
        request,
        LedgerOffset::ZERO,
        UtcTimestamp::from_unix_millis(200),
        CorrelationId::generate(),
        &musteragent_wire::ProtobufLedgerPayloadCodec,
    )
    .expect("assignment command");
    assignments
        .commit_assignment(commit.clone())
        .await
        .expect("commit assignment");
    let replay = assignments
        .commit_assignment(commit)
        .await
        .expect("replay assignment");
    assert_eq!(
        replay.disposition,
        musteragent_ledger::AppendDisposition::Duplicate
    );
    let arbitrary_transfer = sqlx::query("UPDATE resource.nodes SET scope_id=$1 WHERE node_id=$2")
        .bind(WorkspaceId::generate().into_uuid())
        .bind(node.into_uuid())
        .execute(&pool)
        .await;
    assert!(
        arbitrary_transfer.is_err(),
        "transfer requires authenticated assignment receipt"
    );
    let generations: Vec<(uuid::Uuid, i16, i64)> = sqlx::query_as(
        "SELECT scope_id,scope_kind,ownership_generation FROM resource.scope_history WHERE tenant_id=$1 AND node_id=$2 ORDER BY ownership_generation")
        .bind(tenant.into_uuid()).bind(node.into_uuid()).fetch_all(&pool).await.expect("history");
    assert_eq!(
        generations,
        vec![
            (conversation.into_uuid(), 2, 1),
            (workspace.into_uuid(), 1, 2)
        ]
    );
    let receipt_node: uuid::Uuid = sqlx::query_scalar("SELECT result_node_id FROM resource.change_sets WHERE change_set_id=$1 AND tenant_id=$2 AND scope_kind=2 AND scope_id=$3")
        .bind(create.identity.change_set_id.into_uuid()).bind(tenant.into_uuid()).bind(conversation.into_uuid())
        .fetch_one(&pool).await.expect("historical receipt");
    assert_eq!(receipt_node, node.into_uuid());
    let recovered = resources
        .create_folder(create)
        .await
        .expect("replay original receipt after transfer");
    assert!(recovered.duplicate);
    assert_eq!(recovered.node, original.node);
    let forged = sqlx::query("INSERT INTO resource.scope_history (tenant_id,scope_kind,scope_id,node_id,ownership_generation,evidence_source,created_at_ms,updated_at_ms) VALUES ($1,1,$2,$3,99,1,1,1)")
        .bind(TenantId::generate().into_uuid()).bind(workspace.into_uuid()).bind(node.into_uuid()).execute(&pool).await;
    assert!(
        forged.is_err(),
        "history must prove the live tenant and binding"
    );
}
