use musteragent_identity::{Actor, PrincipalId, PrincipalKind, TenantId, WorkspaceId};
use musteragent_postgres::{PostgresWorkspaceResourceRootAuthority, migrate};
use musteragent_runtime_resource::{
    EnsureWorkspaceResourceRoot, WorkspaceResourceRootAuthority, WorkspaceResourceRootError,
};
use musteragent_testkit::IsolatedPostgres;

fn request() -> EnsureWorkspaceResourceRoot {
    EnsureWorkspaceResourceRoot {
        actor: Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        workspace_id: WorkspaceId::generate(),
    }
}

#[tokio::test]
async fn concurrent_empty_workspace_registration_has_one_receipt_and_no_conversation() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated postgres");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let input = request();
    let (first, second) = tokio::join!(roots.ensure_root(input), roots.ensure_root(input));
    assert_eq!(
        first.as_ref().expect("first"),
        second.as_ref().expect("second")
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM conversation.conversations")
        .fetch_one(&pool)
        .await
        .expect("conversation count");
    assert_eq!(
        count, 0,
        "root preparation must not create a hidden Conversation"
    );
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM resource.scope_heads WHERE scope_kind = 1 AND scope_id = $1",
    )
    .bind(input.workspace_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("real root");
    assert_eq!(revision, 0);
    sqlx::query("UPDATE resource.scope_heads SET revision = 9 WHERE scope_id = $1")
        .bind(input.workspace_id.into_uuid())
        .execute(&pool)
        .await
        .expect("later project writes");
    assert_eq!(
        first.expect("receipt"),
        roots.ensure_root(input).await.expect("replay")
    );
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE scope_id = $1")
            .bind(input.workspace_id.into_uuid())
            .fetch_one(&pool)
            .await
            .expect("revision");
    assert_eq!(revision, 9, "registration must not reset file revisions");
}

#[tokio::test]
async fn conflicting_owner_tenant_and_non_user_are_rejected_without_extra_roots() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated postgres");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let input = request();
    roots.ensure_root(input).await.expect("register");
    for actor in [
        Actor {
            principal_id: PrincipalId::generate(),
            ..input.actor
        },
        Actor {
            tenant_id: TenantId::generate(),
            ..input.actor
        },
    ] {
        assert_eq!(
            roots
                .ensure_root(EnsureWorkspaceResourceRoot { actor, ..input })
                .await,
            Err(WorkspaceResourceRootError::IdentityConflict)
        );
    }
    let service = EnsureWorkspaceResourceRoot {
        actor: Actor {
            principal_kind: PrincipalKind::Service,
            ..input.actor
        },
        workspace_id: WorkspaceId::generate(),
    };
    assert_eq!(
        roots.ensure_root(service).await,
        Err(WorkspaceResourceRootError::InvalidOwner)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.scope_heads")
        .fetch_one(&pool)
        .await
        .expect("root count");
    assert_eq!(count, 1);
    assert!(
        sqlx::query("UPDATE resource.workspace_roots SET owner_principal_id = $1")
            .bind(PrincipalId::generate().into_uuid())
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM resource.workspace_roots")
            .execute(&pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn failed_scope_initialization_rolls_back_receipt_and_remains_retryable() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("isolated postgres");
    let pool = pg.pool().await.expect("pool");
    migrate(&pool).await.expect("migrations");
    let roots = PostgresWorkspaceResourceRootAuthority::new(pool.clone());
    let input = request();
    sqlx::raw_sql("CREATE FUNCTION resource.fail_root_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test failure'; END $$; CREATE TRIGGER fail_root_test BEFORE INSERT ON resource.scope_heads FOR EACH ROW EXECUTE FUNCTION resource.fail_root_test();")
        .execute(&pool).await.expect("install failure");
    assert_eq!(
        roots.ensure_root(input).await,
        Err(WorkspaceResourceRootError::DependencyUnavailable)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.workspace_roots")
        .fetch_one(&pool)
        .await
        .expect("receipt count");
    assert_eq!(
        count, 0,
        "receipt cannot outlive failed root initialization"
    );
    sqlx::query("DROP TRIGGER fail_root_test ON resource.scope_heads")
        .execute(&pool)
        .await
        .expect("remove failure");
    roots
        .ensure_root(input)
        .await
        .expect("retry initializes the same workspace");
    assert!(
        sqlx::query("INSERT INTO resource.scope_heads VALUES ($1, 1, $2, 0, 0)")
            .bind(TenantId::generate().into_uuid())
            .bind(input.workspace_id.into_uuid())
            .execute(&pool)
            .await
            .is_err(),
        "a root does not prove existence in a foreign tenant"
    );
}
