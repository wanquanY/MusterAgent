#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;
use musteragent_identity::{ConversationId, PrincipalId, TenantId, WorkspaceId};
use musteragent_postgres::verify_migrations;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
use uuid::Uuid;

async fn artifact(
    pool: &PgPool,
    tenant: Uuid,
    conversation: Option<Uuid>,
    workspace: Option<Uuid>,
    kind: i16,
) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO artifact.artifacts
        (artifact_id,tenant_id,conversation_id,workspace_id,origin,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms)
        VALUES ($1,$2,$3,$4,3,$5,1,1,'file.txt',1,1)")
        .bind(id).bind(tenant).bind(conversation).bind(workspace).bind(kind).execute(pool).await?;
    Ok(id)
}

async fn version(
    pool: &PgPool,
    artifact: Uuid,
    content: Uuid,
    participant: Option<Uuid>,
    principal: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO artifact.artifact_versions
        (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by,committed_by_principal_id,committed_at_ms)
        VALUES ($1,$2,1,$3,decode(repeat('01',32),'hex'),$4,$5,1)")
        .bind(Uuid::now_v7()).bind(artifact).bind(content).bind(participant).bind(principal).execute(pool).await?;
    Ok(())
}

async fn node(
    pool: &PgPool,
    tenant: Uuid,
    workspace: Uuid,
    artifact: Uuid,
    author: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO resource.nodes
        (node_id,tenant_id,scope_kind,scope_id,name,comparison_name,kind,lifecycle,file_artifact_id,sort_key,
         actor_principal_id,actor_principal_kind,created_at_ms,updated_at_ms,revision)
        VALUES ($1,$2,1,$3,'file.txt','file.txt',2,1,$4,'file.txt',$5,1,1,1,1)")
        .bind(Uuid::now_v7()).bind(tenant).bind(workspace).bind(artifact).bind(author).execute(pool).await?;
    Ok(())
}

#[tokio::test]
async fn native_file_identity_upgrade_preserves_history_and_enforces_real_sources() {
    let postgres = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = postgres.pool().await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let tenant = Uuid::now_v7();
    let author = Uuid::now_v7();
    let old_artifact = Uuid::now_v7();
    let old_conversation = Uuid::now_v7();
    let old_participant = Uuid::now_v7();
    let content = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifact.content_refs
        (content_id,tenant_id,media_type,byte_length,content_hash,storage_ref,created_at_ms)
        VALUES ($1,$2,'text/plain',3,decode(repeat('01',32),'hex'),'fixture://content',1)",
    )
    .bind(content)
    .bind(tenant)
    .execute(&pool)
    .await
    .unwrap();
    // Historical imports can legitimately have no Runtime Conversation shell.
    // Upgrading the schema must not fabricate one or change their author namespace.
    sqlx::query("INSERT INTO artifact.artifacts
        (artifact_id,tenant_id,conversation_id,origin,kind,lifecycle,current_version,title,created_at_ms,updated_at_ms)
        VALUES ($1,$2,$3,2,6,1,1,'history.txt',1,1)")
        .bind(old_artifact).bind(tenant).bind(old_conversation).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO artifact.artifact_versions
        (artifact_version_id,artifact_id,version,content_id,content_hash,committed_by,committed_at_ms)
        VALUES ($1,$2,1,$3,decode(repeat('01',32),'hex'),$4,1)")
        .bind(Uuid::now_v7()).bind(old_artifact).bind(content).bind(old_participant).execute(&pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(a) FROM artifact.artifacts a WHERE artifact_id=$1")
            .bind(old_artifact)
            .fetch_one(&pool)
            .await
            .unwrap();
    let before_version: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(v) FROM artifact.artifact_versions v WHERE artifact_id=$1",
    )
    .bind(old_artifact)
    .fetch_one(&pool)
    .await
    .unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    verify_migrations(&pool).await.unwrap();
    musteragent_conformance::migrate(&pool).await.unwrap();
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(a) FROM artifact.artifacts a WHERE artifact_id=$1")
            .bind(old_artifact)
            .fetch_one(&pool)
            .await
            .unwrap();
    let after_version: serde_json::Value = sqlx::query_scalar(
        "SELECT to_jsonb(v) FROM artifact.artifact_versions v WHERE artifact_id=$1",
    )
    .bind(old_artifact)
    .fetch_one(&pool)
    .await
    .unwrap();
    // Later migrations only add columns. Every original value is unchanged;
    // added columns carry neutral defaults (no source, no erasure job).
    let original_columns = |before: &serde_json::Value, after: &serde_json::Value| {
        let before = before.as_object().unwrap();
        let after = after.as_object().unwrap();
        for (column, value) in after.iter().filter(|(c, _)| !before.contains_key(*c)) {
            assert!(
                value.is_null() || *value == serde_json::Value::Bool(false),
                "added column {column} must not invent history: {value}"
            );
        }
        serde_json::Value::Object(
            after
                .iter()
                .filter(|(column, _)| before.contains_key(*column))
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect(),
        )
    };
    assert_eq!(before, original_columns(&before, &after));
    assert_eq!(
        before_version,
        original_columns(&before_version, &after_version)
    );

    let workspace = Uuid::now_v7();
    let other_workspace = Uuid::now_v7();
    for id in [workspace, other_workspace] {
        sqlx::query("INSERT INTO resource.workspace_roots VALUES ($1,$2,$3,1)")
            .bind(id)
            .bind(tenant)
            .bind(author)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO resource.workspace_authority (tenant_id, workspace_id, status, authority_generation, ready_at_ms, created_at_ms, updated_at_ms) VALUES ($1,$2,2,1,1,1,1)")
            .bind(tenant).bind(id).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO resource.scope_heads VALUES ($1,1,$2,0,1)")
            .bind(tenant)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    assert!(artifact(&pool, tenant, None, None, 6).await.is_err());
    assert!(
        artifact(&pool, tenant, Some(old_conversation), Some(workspace), 6)
            .await
            .is_err()
    );
    assert!(
        artifact(&pool, Uuid::now_v7(), None, Some(workspace), 6)
            .await
            .is_err()
    );
    assert!(
        artifact(&pool, tenant, None, Some(Uuid::now_v7()), 6)
            .await
            .is_err()
    );
    assert!(
        artifact(&pool, tenant, None, Some(workspace), 3)
            .await
            .is_err()
    );
    let file = artifact(&pool, tenant, None, Some(workspace), 6)
        .await
        .unwrap();
    assert!(version(&pool, file, content, None, None).await.is_err());
    assert!(
        version(&pool, file, content, Some(old_participant), Some(author))
            .await
            .is_err()
    );
    assert!(
        version(&pool, file, content, None, Some(Uuid::now_v7()))
            .await
            .is_err()
    );
    assert!(
        version(&pool, file, content, Some(old_participant), None)
            .await
            .is_err()
    );
    version(&pool, file, content, None, Some(author))
        .await
        .unwrap();
    assert!(
        node(&pool, tenant, other_workspace, file, author)
            .await
            .is_err()
    );
    node(&pool, tenant, workspace, file, author).await.unwrap();
    assert!(
        sqlx::query("UPDATE artifact.artifacts SET workspace_id=$1 WHERE artifact_id=$2")
            .bind(other_workspace)
            .bind(file)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("UPDATE artifact.artifacts SET conversation_id=$1,workspace_id=NULL WHERE artifact_id=$2")
        .bind(old_conversation).bind(file).execute(&pool).await.is_err());
    let conversations: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.conversations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let participants: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.participants")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((conversations, participants), (0, 0));
    let versions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resource.file_versions WHERE artifact_id=$1")
            .bind(file)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(versions, 1);
    // A later real Agent participant may edit the native File. Its Workspace
    // association is checked; an unrelated participant cannot reuse that source.
    let conversation = ConversationId::generate();
    resource_support::seed_project_conversations(
        &pool,
        TenantId::from_uuid(tenant).unwrap(),
        PrincipalId::from_uuid(author).unwrap(),
        WorkspaceId::from_uuid(workspace).unwrap(),
        [conversation, ConversationId::generate()],
    )
    .await;
    let participant: Uuid = sqlx::query_scalar("SELECT execution_owner_participant_id FROM conversation.conversations WHERE conversation_id=$1")
        .bind(conversation.into_uuid()).fetch_one(&pool).await.unwrap();
    let editable = artifact(&pool, tenant, None, Some(workspace), 6)
        .await
        .unwrap();
    version(&pool, editable, content, Some(participant), None)
        .await
        .unwrap();
    let unrelated = artifact(&pool, tenant, None, Some(other_workspace), 6)
        .await
        .unwrap();
    assert!(
        version(&pool, unrelated, content, Some(participant), None)
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO resource.scope_heads VALUES ($1,2,$2,0,1)")
        .bind(tenant)
        .bind(conversation.into_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let rejected = sqlx::query("INSERT INTO resource.nodes
        (node_id,tenant_id,scope_kind,scope_id,name,comparison_name,kind,lifecycle,file_artifact_id,sort_key,
         actor_principal_id,actor_principal_kind,created_at_ms,updated_at_ms,revision)
        VALUES ($1,$2,2,$3,'other.txt','other.txt',2,1,$4,'other.txt',$5,1,1,1,1)")
        .bind(Uuid::now_v7()).bind(tenant).bind(conversation.into_uuid()).bind(editable).bind(author).execute(&pool).await;
    assert!(
        rejected.is_err(),
        "native Workspace File cannot enter a Conversation scope through a null identity"
    );
}
