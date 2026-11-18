use musteragent_identity::{PrincipalKind, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    EnsureWorkspaceResourceRoot, ListWorkspaceResourceChildren, MAX_RESOURCE_PAGE_SIZE,
    WorkspaceResourceRootAuthority, WorkspaceResourceRootError, WorkspaceResourceRootReceipt,
};
use sqlx::{PgPool, Row as _};

#[derive(Clone)]
pub struct PostgresWorkspaceResourceRootAuthority {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresWorkspaceResourceRootAuthority {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
        }
    }

    async fn list_inner(
        &self,
        request: ListWorkspaceResourceChildren,
    ) -> Result<musteragent_runtime_resource::ResourceNodePage, WorkspaceResourceRootError> {
        if request.actor.principal_kind != PrincipalKind::User {
            return Err(WorkspaceResourceRootError::InvalidOwner);
        }
        if request.limit == 0
            || request.limit > MAX_RESOURCE_PAGE_SIZE
            || i64::try_from(request.offset).is_err()
        {
            return Err(WorkspaceResourceRootError::InvalidQuery);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(unavailable)?;
        let owned: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM resource.workspace_roots AS root
             JOIN resource.workspace_authority AS authority USING(tenant_id, workspace_id)
             WHERE root.workspace_id=$1 AND root.tenant_id=$2 AND root.owner_principal_id=$3
             AND authority.status=2)",
        )
        .bind(request.workspace_id.into_uuid())
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if !owned {
            return Err(WorkspaceResourceRootError::IdentityConflict);
        }
        if let Some(parent) = request.parent_node_id {
            let present: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM resource.nodes WHERE node_id=$1 AND tenant_id=$2
                 AND scope_kind=1 AND scope_id=$3 AND kind=1 AND ($4 OR lifecycle=1))",
            )
            .bind(parent.into_uuid())
            .bind(request.actor.tenant_id.into_uuid())
            .bind(request.workspace_id.into_uuid())
            .bind(request.include_trashed)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
            if !present {
                return Err(WorkspaceResourceRootError::IdentityConflict);
            }
        }
        let page = crate::resource::read::page(
            &mut tx,
            request.actor.tenant_id,
            musteragent_resource::ResourceScope::Project(request.workspace_id),
            request.parent_node_id,
            request.include_trashed,
            request.offset,
            request.limit,
        )
        .await
        .map_err(|error| match error {
            musteragent_runtime_resource::ResourceStoreError::DependencyUnavailable => {
                WorkspaceResourceRootError::DependencyUnavailable
            }
            _ => WorkspaceResourceRootError::Corrupt,
        })?;
        tx.commit().await.map_err(unavailable)?;
        Ok(page)
    }

    async fn ensure_inner(
        &self,
        request: EnsureWorkspaceResourceRoot,
    ) -> Result<WorkspaceResourceRootReceipt, WorkspaceResourceRootError> {
        if request.actor.principal_kind != PrincipalKind::User {
            return Err(WorkspaceResourceRootError::InvalidOwner);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(unavailable)?;
        sqlx::query(
            "INSERT INTO resource.workspace_roots
                (workspace_id, tenant_id, owner_principal_id, created_at_ms)
             VALUES ($1, $2, $3, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)
             ON CONFLICT (workspace_id) DO NOTHING",
        )
        .bind(request.workspace_id.into_uuid())
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.actor.principal_id.into_uuid())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        // A separate statement observes the committed winner even when the
        // first insert waited on a concurrent registration of the same key.
        let row = sqlx::query(
            "SELECT tenant_id, owner_principal_id, created_at_ms
             FROM resource.workspace_roots WHERE workspace_id = $1 FOR SHARE",
        )
        .bind(request.workspace_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if row.try_get::<uuid::Uuid, _>("tenant_id").map_err(corrupt)?
            != request.actor.tenant_id.into_uuid()
            || row
                .try_get::<uuid::Uuid, _>("owner_principal_id")
                .map_err(corrupt)?
                != request.actor.principal_id.into_uuid()
        {
            return Err(WorkspaceResourceRootError::IdentityConflict);
        }
        let created_at_ms = row.try_get::<i64, _>("created_at_ms").map_err(corrupt)?;
        // Backend's authenticated root attestation installs readiness together
        // with its immutable owner receipt. A held authority cannot be revived
        // by a repeated ensure request.
        let ready = sqlx::query_scalar::<_, i16>(
            "INSERT INTO resource.workspace_authority
                (tenant_id, workspace_id, status, authority_generation, ready_at_ms, created_at_ms, updated_at_ms)
             VALUES ($1, $2, 2, 1, $3, $3, $3)
             ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET
                status=2,
                authority_generation=resource.workspace_authority.authority_generation + CASE WHEN resource.workspace_authority.status=1 THEN 1 ELSE 0 END,
                ready_at_ms=COALESCE(resource.workspace_authority.ready_at_ms, EXCLUDED.ready_at_ms),
                updated_at_ms=GREATEST(resource.workspace_authority.updated_at_ms, EXCLUDED.updated_at_ms)
             WHERE resource.workspace_authority.status IN (1, 2)
             RETURNING status",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.workspace_id.into_uuid())
        .bind(created_at_ms)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if ready != Some(2) {
            return Err(WorkspaceResourceRootError::IdentityConflict);
        }
        // Both the receipt and the real Resource scope head commit atomically.
        // Do not reset a revision if this is an already populated project.
        sqlx::query(
            "INSERT INTO resource.scope_heads
                (tenant_id, scope_kind, scope_id, revision, updated_at_ms)
             VALUES ($1, 1, $2, 0, $3)
             ON CONFLICT (tenant_id, scope_kind, scope_id) DO NOTHING",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.workspace_id.into_uuid())
        .bind(created_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(WorkspaceResourceRootReceipt {
            tenant_id: request.actor.tenant_id,
            workspace_id: request.workspace_id,
            owner_principal_id: request.actor.principal_id,
            created_at: UtcTimestamp::from_unix_millis(created_at_ms),
        })
    }
}

impl WorkspaceResourceRootAuthority for PostgresWorkspaceResourceRootAuthority {
    fn list_children(
        &self,
        request: ListWorkspaceResourceChildren,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_resource::ResourceNodePage, WorkspaceResourceRootError>,
    > {
        Box::pin(self.list_inner(request))
    }

    fn ensure_root(
        &self,
        request: EnsureWorkspaceResourceRoot,
    ) -> ApplicationFuture<'_, Result<WorkspaceResourceRootReceipt, WorkspaceResourceRootError>>
    {
        Box::pin(self.ensure_inner(request))
    }
}

fn unavailable(_: impl std::fmt::Debug) -> WorkspaceResourceRootError {
    WorkspaceResourceRootError::DependencyUnavailable
}

fn corrupt(_: sqlx::Error) -> WorkspaceResourceRootError {
    WorkspaceResourceRootError::Corrupt
}

impl musteragent_runtime_file_query::WorkspaceFileQueryAuthority
    for PostgresWorkspaceResourceRootAuthority
{
    fn resolve_workspace<'a>(
        &'a self,
        actor: musteragent_identity::Actor,
        workspace: musteragent_identity::WorkspaceId,
        now: UtcTimestamp,
    ) -> ApplicationFuture<
        'a,
        Result<
            musteragent_runtime_file_query::FileQueryContext,
            musteragent_runtime_file_query::FileQueryError,
        >,
    > {
        use musteragent_runtime_file_query::{FileQueryContext, FileQueryError};
        Box::pin(async move {
            if actor.principal_kind != PrincipalKind::User {
                return Err(FileQueryError::Denied);
            }
            let generation: Option<i64> = sqlx::query_scalar(
                "SELECT authority.authority_generation FROM resource.workspace_roots AS root
                 JOIN resource.workspace_authority AS authority USING(tenant_id, workspace_id)
                 WHERE root.tenant_id=$1 AND root.workspace_id=$2 AND root.owner_principal_id=$3 AND authority.status=2")
                .bind(actor.tenant_id.into_uuid()).bind(workspace.into_uuid()).bind(actor.principal_id.into_uuid())
                .fetch_optional(&self.pool).await.map_err(|_| FileQueryError::Unavailable)?;
            let generation = generation.ok_or(FileQueryError::Denied)?;
            let revision = musteragent_content_ref::Sha256::digest(format!(
                "{}:{}:{}:{generation}",
                actor.tenant_id, actor.principal_id, workspace
            ));
            Ok(FileQueryContext {
                dialogue_source: None,
                actor,
                conversation_id: None,
                now,
                authority_revision: revision.to_string(),
                content_allowlist: None,
                project_workspace_id: Some(workspace),
            })
        })
    }
}
