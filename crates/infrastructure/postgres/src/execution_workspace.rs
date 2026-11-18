//! Persistent execution roots use Resource's existing tree and transaction owner.
use musteragent_identity::{
    Actor, ContextBranchId, ExecutionWorkspaceId, PrincipalKind, UtcTimestamp, WorkspaceId,
};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    EnsureExecutionWorkspace, ExecutionWorkspaceAuthority, ExecutionWorkspaceBinding,
    ExecutionWorkspaceError, ExecutionWorkspaceReceipt, ResourceCommandContext,
    ResourceCommandSource, ResourceStoreError,
};
use sqlx::{PgConnection, Row as _};

#[derive(Clone)]
pub struct PostgresExecutionWorkspaceAuthority {
    database: crate::PostgresDatabaseRuntime,
}

impl PostgresExecutionWorkspaceAuthority {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }

    async fn ensure_inner(
        &self,
        request: EnsureExecutionWorkspace,
    ) -> Result<ExecutionWorkspaceReceipt, ExecutionWorkspaceError> {
        if request.actor.principal_kind != PrincipalKind::User {
            return Err(ExecutionWorkspaceError::Unavailable);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
            .await
            .map_err(dependency)?;
        let receipt = ensure_on(&mut tx, request).await?;
        tx.commit().await.map_err(dependency)?;
        Ok(receipt)
    }
}

impl ExecutionWorkspaceAuthority for PostgresExecutionWorkspaceAuthority {
    fn ensure(
        &self,
        request: EnsureExecutionWorkspace,
    ) -> ApplicationFuture<'_, Result<ExecutionWorkspaceReceipt, ExecutionWorkspaceError>> {
        Box::pin(self.ensure_inner(request))
    }
}

pub(crate) async fn ensure_on(
    connection: &mut PgConnection,
    request: EnsureExecutionWorkspace,
) -> Result<ExecutionWorkspaceReceipt, ExecutionWorkspaceError> {
    if request.actor.principal_kind != PrincipalKind::User {
        return Err(ExecutionWorkspaceError::Unavailable);
    }
    // Branch activation is serialized by the original Ledger stream. Lock
    // that owner first, including the initial branch which has no state row.
    if !lock_stream(connection, request.actor, request.conversation_id)
        .await
        .map_err(dependency)?
    {
        return Err(ExecutionWorkspaceError::Unavailable);
    }
    let row = authority_row(connection, request.actor, request.conversation_id, true)
        .await
        .map_err(dependency)?
        .ok_or(ExecutionWorkspaceError::Unavailable)?;
    if row
        .try_get::<i16, _>("control_home_kind")
        .map_err(corrupt)?
        != 1
    {
        return Err(ExecutionWorkspaceError::UnsupportedHome);
    }
    let branch = row
        .try_get::<Option<uuid::Uuid>, _>("active_branch_id")
        .map_err(corrupt)?;
    let epoch = row.try_get::<String, _>("home_epoch").map_err(corrupt)?;
    if branch != request.expected_branch_id.map(ContextBranchId::into_uuid)
        || epoch != request.expected_home_epoch.get().to_string()
    {
        return Err(ExecutionWorkspaceError::AuthorityChanged);
    }
    let project_id = row
        .try_get::<Option<uuid::Uuid>, _>("workspace_id")
        .map_err(corrupt)?
        .map(WorkspaceId::from_uuid)
        .transpose()
        .map_err(corrupt)?;
    sqlx::query(
        "INSERT INTO resource.execution_workspaces
             (execution_workspace_id, tenant_id, conversation_id, branch_id, created_at_ms)
             VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (tenant_id, conversation_id, branch_id) DO NOTHING",
    )
    .bind(ExecutionWorkspaceId::generate().into_uuid())
    .bind(request.actor.tenant_id.into_uuid())
    .bind(request.conversation_id.into_uuid())
    .bind(branch)
    .bind(request.now.unix_millis())
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    // Separate statement observes a concurrent winner under READ COMMITTED.
    let row = sqlx::query(
        "SELECT execution_workspace_id, created_at_ms FROM resource.execution_workspaces
            WHERE tenant_id=$1 AND conversation_id=$2 AND branch_id IS NOT DISTINCT FROM $3",
    )
    .bind(request.actor.tenant_id.into_uuid())
    .bind(request.conversation_id.into_uuid())
    .bind(branch)
    .fetch_one(&mut *connection)
    .await
    .map_err(dependency)?;
    let id =
        ExecutionWorkspaceId::from_uuid(row.try_get("execution_workspace_id").map_err(corrupt)?)
            .map_err(corrupt)?;
    let created_at = UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(corrupt)?);
    sqlx::query(
        "INSERT INTO resource.scope_heads (tenant_id,scope_kind,scope_id,revision,updated_at_ms)
            VALUES ($1,3,$2,0,$3) ON CONFLICT (tenant_id,scope_kind,scope_id) DO NOTHING",
    )
    .bind(request.actor.tenant_id.into_uuid())
    .bind(id.into_uuid())
    .bind(created_at.unix_millis())
    .execute(&mut *connection)
    .await
    .map_err(dependency)?;
    Ok(ExecutionWorkspaceReceipt {
        binding: ExecutionWorkspaceBinding {
            execution_workspace_id: id,
            conversation_id: request.conversation_id,
            branch_id: request.expected_branch_id,
            project_id,
            home_epoch: request.expected_home_epoch,
        },
        created_at,
    })
}

async fn lock_stream(
    connection: &mut PgConnection,
    actor: Actor,
    conversation_id: musteragent_identity::ConversationId,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT true FROM event_store.ledger_streams
        WHERE tenant_id=$1 AND conversation_id=$2 FOR SHARE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_optional(connection)
    .await?
    .unwrap_or(false))
}

async fn authority_row(
    connection: &mut PgConnection,
    actor: Actor,
    conversation_id: musteragent_identity::ConversationId,
    lock: bool,
) -> Result<Option<sqlx::postgres::PgRow>, sqlx::Error> {
    let sql = "SELECT c.workspace_id, c.home_epoch::text AS home_epoch, c.control_home_kind,
        (SELECT active_branch_id FROM conversation.context_branch_state
         WHERE tenant_id=c.tenant_id AND conversation_id=c.conversation_id) AS active_branch_id
        FROM conversation.conversations c JOIN conversation.participants p
          ON p.tenant_id=c.tenant_id AND p.conversation_id=c.conversation_id
         AND p.kind=1 AND p.principal_ref=$3 AND p.left_at_ms IS NULL
        WHERE c.tenant_id=$1 AND c.conversation_id=$2 AND (c.lifecycle=1 OR ($4 AND c.lifecycle=2))";
    let query = if lock {
        format!("{sql} FOR SHARE OF c, p")
    } else {
        sql.to_owned()
    };
    sqlx::query(&query)
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(!lock)
        .fetch_optional(connection)
        .await
}

/// Held through mutation commit; a stale binding is never write authority.
pub(crate) async fn authorize_context(
    connection: &mut PgConnection,
    context: &ResourceCommandContext,
    binding: ExecutionWorkspaceBinding,
    lock: bool,
) -> Result<bool, sqlx::Error> {
    if context.validate().is_err() || context.actor.principal_kind != PrincipalKind::User {
        return Ok(false);
    }
    if lock && !lock_stream(connection, context.actor, binding.conversation_id).await? {
        return Ok(false);
    }
    let Some(row) = authority_row(connection, context.actor, binding.conversation_id, lock).await?
    else {
        return Ok(false);
    };
    if row.try_get::<i16, _>("control_home_kind")? != 1
        || row.try_get::<String, _>("home_epoch")? != binding.home_epoch.get().to_string()
        || row.try_get::<Option<uuid::Uuid>, _>("workspace_id")?
            != binding.project_id.map(WorkspaceId::into_uuid)
        || row.try_get::<Option<uuid::Uuid>, _>("active_branch_id")?
            != binding.branch_id.map(ContextBranchId::into_uuid)
    {
        return Ok(false);
    }
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource.execution_workspaces
        WHERE tenant_id=$1 AND conversation_id=$2 AND execution_workspace_id=$3 AND branch_id IS NOT DISTINCT FROM $4)")
        .bind(context.tenant_id.into_uuid()).bind(binding.conversation_id.into_uuid())
        .bind(binding.execution_workspace_id.into_uuid()).bind(binding.branch_id.map(ContextBranchId::into_uuid))
        .fetch_one(connection).await
}

pub(crate) async fn authorize_read(
    connection: &mut PgConnection,
    context: &ResourceCommandContext,
) -> Result<(), ResourceStoreError> {
    if let ResourceCommandSource::ExecutionWorkspace(binding) = context.source
        && !authorize_context(connection, context, binding, false)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?
    {
        return Err(ResourceStoreError::FileUnavailable);
    }
    Ok(())
}

fn dependency<T>(_: T) -> ExecutionWorkspaceError {
    ExecutionWorkspaceError::DependencyUnavailable
}
fn corrupt<T>(_: T) -> ExecutionWorkspaceError {
    ExecutionWorkspaceError::Corrupt
}

/// Resource readers share the same consistent authority snapshot, regardless
/// of the concrete read adapter (tree, file edit, lexical or vector index).
pub(crate) async fn authorized_resource_read(
    database: &crate::PostgresDatabaseRuntime,
    context: &ResourceCommandContext,
) -> Result<crate::GovernedPostgresTransaction<'static>, ResourceStoreError> {
    let mut tx = database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Resource,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await
        .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
    crate::resource_file_catalog::authorize(&mut tx, context).await?;
    Ok(tx)
}

impl crate::PostgresResourceStore {
    pub(crate) async fn authorized_resource_read(
        &self,
        context: &ResourceCommandContext,
    ) -> Result<crate::GovernedPostgresTransaction<'static>, ResourceStoreError> {
        authorized_resource_read(&self.database, context).await
    }
}

/// Resolves the current binding under the same membership/branch/Home locks as
/// mutations. This allocates directory metadata only, never a resident runtime.
pub(crate) async fn resolve_current_context(
    pool: sqlx::PgPool,
    request: musteragent_runtime_resource::ResolveResourceContext,
) -> Result<ResourceCommandContext, musteragent_runtime_resource::ResourceContextError> {
    use musteragent_runtime_resource::ResourceContextError as Error;
    let database = crate::PostgresDatabaseRuntime::from(pool);
    let mut tx = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Resource)
        .await
        .map_err(|_| Error::DependencyUnavailable)?;
    if !lock_stream(&mut tx, request.actor, request.conversation_id)
        .await
        .map_err(|_| Error::DependencyUnavailable)?
    {
        return Err(Error::ScopeUnavailable);
    }
    let row = authority_row(&mut tx, request.actor, request.conversation_id, true)
        .await
        .map_err(|_| Error::DependencyUnavailable)?
        .ok_or(Error::ScopeUnavailable)?;
    let branch = row
        .try_get::<Option<uuid::Uuid>, _>("active_branch_id")
        .map_err(|_| Error::Corrupt)?
        .map(ContextBranchId::from_uuid)
        .transpose()
        .map_err(|_| Error::Corrupt)?;
    let epoch = row
        .try_get::<String, _>("home_epoch")
        .map_err(|_| Error::Corrupt)?
        .parse::<u64>()
        .map_err(|_| Error::Corrupt)?;
    let receipt = ensure_on(
        &mut tx,
        EnsureExecutionWorkspace {
            actor: request.actor,
            conversation_id: request.conversation_id,
            expected_branch_id: branch,
            expected_home_epoch: musteragent_identity::HomeEpoch::new(epoch)
                .map_err(|_| Error::Corrupt)?,
            now: request.now,
        },
    )
    .await
    .map_err(|e| match e {
        ExecutionWorkspaceError::DependencyUnavailable => Error::DependencyUnavailable,
        ExecutionWorkspaceError::Corrupt => Error::Corrupt,
        _ => Error::ScopeUnavailable,
    })?;
    tx.commit()
        .await
        .map_err(|_| Error::DependencyUnavailable)?;
    Ok(ResourceCommandContext {
        tenant_id: request.actor.tenant_id,
        actor: request.actor,
        source: ResourceCommandSource::ExecutionWorkspace(receipt.binding),
        invocation_id: None,
        tool_key: None,
        now: request.now,
    })
}
