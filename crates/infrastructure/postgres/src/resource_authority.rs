use musteragent_identity::WorkspaceId;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{
    ResolveResourceContext, ResourceCommandContext, ResourceContextAuthority, ResourceContextError,
    ResourceStoreError,
};
use sqlx::{PgPool, Row as _};

/// Fence a previously resolved context against the current Conversation owner.
/// Hold the row through commit, before taking resource/head/edit locks, so a
/// workspace reassignment cannot race a write. A false result may still permit
/// read-only recovery of an exact committed receipt, never a new mutation.
pub(crate) async fn lock_current_resource_context(
    connection: &mut sqlx::PgConnection,
    context: &ResourceCommandContext,
) -> Result<bool, ResourceStoreError> {
    if context.validate().is_err() {
        return Ok(false);
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::Account { .. }
    ) {
        return match crate::agent_resource::account::resolve_owned(connection, context, true).await
        {
            Ok(_) => Ok(true),
            Err(musteragent_runtime_resource::FileEditError::DependencyUnavailable) => {
                Err(ResourceStoreError::DependencyUnavailable)
            }
            Err(musteragent_runtime_resource::FileEditError::Corrupt) => {
                Err(ResourceStoreError::Corrupt)
            }
            Err(_) => Ok(false),
        };
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::DialogueExecution { .. }
    ) {
        return crate::agent_resource::authorize(connection, context, true)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable);
    }
    if matches!(
        context.source,
        musteragent_runtime_resource::ResourceCommandSource::DeviceExecution { .. }
    ) {
        return crate::agent_resource::device::authorize(connection, context, true)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable);
    }
    if let musteragent_runtime_resource::ResourceCommandSource::ExecutionWorkspace(binding) =
        context.source
    {
        if !crate::execution_workspace::authorize_context(connection, context, binding, true)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?
        {
            return Ok(false);
        }
        if !crate::execution_workspace_snapshot::writer_available(connection, context)
            .await
            .map_err(|_| ResourceStoreError::DependencyUnavailable)?
        {
            return Err(ResourceStoreError::WriterBusy);
        }
        return Ok(true);
    }
    let Some(conversation_id) = context.conversation_id() else {
        return Ok(sqlx::query_scalar::<_, bool>(
            "SELECT root.owner_principal_id = $3 AND authority.status=2
             FROM resource.workspace_roots AS root
             JOIN resource.workspace_authority AS authority USING(tenant_id, workspace_id)
             WHERE root.tenant_id = $1 AND root.workspace_id = $2 FOR SHARE OF root, authority",
        )
        .bind(context.tenant_id.into_uuid())
        .bind(context.workspace_id().map(WorkspaceId::into_uuid))
        .bind(context.actor.principal_id.into_uuid())
        .fetch_optional(connection)
        .await
        .map_err(|_| ResourceStoreError::DependencyUnavailable)?
        .unwrap_or(false));
    };
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT workspace_id IS NOT DISTINCT FROM $3 AND lifecycle IN (1, 2) AND purpose='standard'
           FROM conversation.conversations
          WHERE tenant_id = $1 AND conversation_id = $2
          FOR SHARE",
    )
    .bind(context.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(context.workspace_id().map(WorkspaceId::into_uuid))
    .fetch_optional(connection)
    .await
    .map_err(|_| ResourceStoreError::DependencyUnavailable)?
    .unwrap_or(false))
}

#[derive(Clone)]
pub struct PostgresResourceContextAuthority {
    pool: PgPool,
}

impl PostgresResourceContextAuthority {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn resolve_inner(
        &self,
        request: ResolveResourceContext,
    ) -> Result<ResourceCommandContext, ResourceContextError> {
        let row = sqlx::query(
            "SELECT workspace_id, purpose
               FROM conversation.conversations
              WHERE tenant_id = $1 AND conversation_id = $2 AND lifecycle IN (1, 2)",
        )
        .bind(request.actor.tenant_id.into_uuid())
        .bind(request.conversation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ResourceContextError::DependencyUnavailable)?
        .ok_or(ResourceContextError::ScopeUnavailable)?;
        let purpose: String = row
            .try_get("purpose")
            .map_err(|_| ResourceContextError::Corrupt)?;
        if purpose != "standard" {
            let invocation = request
                .invocation_id
                .ok_or(ResourceContextError::ScopeUnavailable)?;
            let mut connection = self
                .pool
                .acquire()
                .await
                .map_err(|_| ResourceContextError::DependencyUnavailable)?;
            let (agent_id, tool_key) = crate::agent_resource::resolve_execution(
                &mut connection,
                request.actor,
                request.conversation_id,
                invocation,
            )
            .await
            .map_err(|_| ResourceContextError::DependencyUnavailable)?
            .ok_or(ResourceContextError::ScopeUnavailable)?;
            return Ok(ResourceCommandContext {
                tenant_id: request.actor.tenant_id,
                actor: request.actor,
                source: musteragent_runtime_resource::ResourceCommandSource::DialogueExecution {
                    conversation_id: request.conversation_id,
                    agent_id,
                },
                invocation_id: Some(invocation),
                tool_key: Some(tool_key),
                now: request.now,
            });
        }
        let workspace_id = row
            .try_get::<Option<uuid::Uuid>, _>("workspace_id")
            .map_err(|_| ResourceContextError::Corrupt)?
            .map(WorkspaceId::from_uuid)
            .transpose()
            .map_err(|_| ResourceContextError::Corrupt)?;
        Ok(ResourceCommandContext {
            tenant_id: request.actor.tenant_id,
            actor: request.actor,
            source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
                conversation_id: request.conversation_id,
                workspace_id,
            },
            invocation_id: None,
            tool_key: None,
            now: request.now,
        })
    }
}

impl ResourceContextAuthority for PostgresResourceContextAuthority {
    fn resolve_execution(
        &self,
        request: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>> {
        Box::pin(crate::execution_workspace::resolve_current_context(
            self.pool.clone(),
            request,
        ))
    }

    fn resolve(
        &self,
        request: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>> {
        Box::pin(self.resolve_inner(request))
    }
}
