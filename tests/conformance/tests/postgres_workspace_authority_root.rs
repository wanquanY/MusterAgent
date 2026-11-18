//! Empty Workspace authority conformance (P1/P2 shared blocker).
//!
//! 0159 proves a Project Resource scope by requiring an existing Conversation
//! with the same workspace_id, so an empty workspace can never materialize its
//! file root and callers are tempted to create a placeholder Conversation as a
//! file container. 0195 introduces `resource.workspace_authority` and accepts a
//! ready authority row as an alternative proof. This test exercises the DB
//! contract: a preparing authority and an unknown workspace are rejected, a
//! ready authority initializes an empty root, and the Conversation count stays
//! zero throughout.

use musteragent_identity::{TenantId, WorkspaceId};
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;

async fn conversation_count(pool: &PgPool, tenant_id: TenantId, workspace_id: WorkspaceId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM conversation.conversations
          WHERE tenant_id = $1 AND workspace_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(workspace_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("count conversations")
}

async fn initialize_scope_head(
    pool: &PgPool,
    tenant_id: TenantId,
    workspace_id: WorkspaceId,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO resource.scope_heads (tenant_id, scope_kind, scope_id, revision, updated_at_ms)
         VALUES ($1, 1, $2, 0, 1)",
    )
    .bind(tenant_id.into_uuid())
    .bind(workspace_id.into_uuid())
    .execute(pool)
    .await
}

#[tokio::test]
async fn empty_workspace_root_requires_ready_authority_and_keeps_no_conversation() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");

    let tenant_id = TenantId::generate();
    let workspace_id = WorkspaceId::generate();
    assert_eq!(conversation_count(&pool, tenant_id, workspace_id).await, 0);

    // A workspace with no authority row is not a real scope.
    assert!(
        initialize_scope_head(&pool, tenant_id, workspace_id)
            .await
            .is_err(),
        "unknown workspace must not materialize a Project root"
    );

    // Preparing authority is durable intent, not a send-ready root.
    sqlx::query(
        "INSERT INTO resource.workspace_authority (
             tenant_id, workspace_id, status, authority_generation, created_at_ms, updated_at_ms
         ) VALUES ($1, $2, 1, 1, 1, 1)",
    )
    .bind(tenant_id.into_uuid())
    .bind(workspace_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed preparing authority");
    assert!(
        initialize_scope_head(&pool, tenant_id, workspace_id)
            .await
            .is_err(),
        "preparing workspace must not materialize a Project root"
    );

    // Ready authority carries its proof timestamp and unlocks the empty root.
    sqlx::query(
        "UPDATE resource.workspace_authority
            SET status = 2, ready_at_ms = 2, updated_at_ms = 2
          WHERE tenant_id = $1 AND workspace_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(workspace_id.into_uuid())
    .execute(&pool)
    .await
    .expect("promote authority to ready");
    initialize_scope_head(&pool, tenant_id, workspace_id)
        .await
        .expect("ready workspace must initialize its empty root");

    // The root is usable without a Conversation: a Project folder needs none.
    let node_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO resource.nodes (
             node_id, tenant_id, scope_kind, scope_id, parent_node_id,
             original_parent_node_id, name, comparison_name, kind, lifecycle,
             file_artifact_id, sort_key, actor_principal_id, actor_principal_kind,
             source_conversation_id, source_invocation_id, source_tool_key,
             created_at_ms, updated_at_ms, revision
         ) VALUES ($1, $2, 1, $3, NULL, NULL, 'Sources', 'sources', 1, 1,
                   NULL, 'a', $4, 1, NULL, NULL, NULL, 1, 1, 1)",
    )
    .bind(node_id)
    .bind(tenant_id.into_uuid())
    .bind(workspace_id.into_uuid())
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .expect("create folder in an empty Project root");

    // The empty root never fabricates a Conversation file container.
    assert_eq!(
        conversation_count(&pool, tenant_id, workspace_id).await,
        0,
        "empty workspace root must not require or create a placeholder Conversation"
    );

    // Identity is retained and Ready cannot silently regress to Preparing.
    assert!(
        sqlx::query(
            "UPDATE resource.workspace_authority SET status = 1, ready_at_ms = NULL
              WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(workspace_id.into_uuid())
        .execute(&pool)
        .await
        .is_err(),
        "ready authority must not silently regress to preparing"
    );
    assert!(
        sqlx::query("DELETE FROM resource.workspace_authority")
            .execute(&pool)
            .await
            .is_err(),
        "workspace authority proof must be retained"
    );
}
