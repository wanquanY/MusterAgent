//! Public-entry authorization plus the original Resource rename/trash transaction.
use super::*;
use musteragent_content_ref::Sha256;
use musteragent_identity::{AgentId, IdempotencyKey, ResourceChangeSetId};
use musteragent_runtime_resource::{
    AgentFileManagementChange, AgentFileManagementCommand, AgentFileManagementPort,
    AgentFileManagementState, AgentFileSelector, ResourceCommandContext, ResourceCommandSource,
};

async fn context(
    db: &mut PgConnection,
    selector: AgentFileSelector,
    now: UtcTimestamp,
    lock: bool,
) -> Result<ResourceCommandContext, ResourceStoreError> {
    if selector.actor.principal_kind != PrincipalKind::User {
        return Err(ResourceStoreError::RevisionConflict);
    }
    // Lock the account/Agent before the scope head, matching publication and erasure.
    let query = if lock {
        "SELECT a.agent_id FROM agent.owned_agents a JOIN agent.owner_spaces s ON s.space_id=a.space_id JOIN agent.agent_dialogue_entries e ON e.dialogue_id=a.dialogue_id AND e.tenant_id=s.tenant_id AND e.owner_id=s.owner_id WHERE a.dialogue_id=$1 AND e.entry_id=$2 AND e.kind='artifact' AND e.erasure_job_id IS NULL AND s.tenant_id=$3 AND s.owner_id=$4 AND s.product='musteragent' AND a.status='active' FOR UPDATE OF s FOR SHARE OF a,e"
    } else {
        "SELECT a.agent_id FROM agent.owned_agents a JOIN agent.owner_spaces s ON s.space_id=a.space_id JOIN agent.agent_dialogue_entries e ON e.dialogue_id=a.dialogue_id AND e.tenant_id=s.tenant_id AND e.owner_id=s.owner_id WHERE a.dialogue_id=$1 AND e.entry_id=$2 AND e.kind='artifact' AND e.erasure_job_id IS NULL AND s.tenant_id=$3 AND s.owner_id=$4 AND s.product='musteragent' AND a.status='active'"
    };
    let agent = sqlx::query_scalar::<_, uuid::Uuid>(query)
        .bind(selector.dialogue_id.into_uuid())
        .bind(selector.entry_id.into_uuid())
        .bind(selector.actor.tenant_id.into_uuid())
        .bind(selector.actor.principal_id.into_uuid())
        .fetch_optional(db)
        .await
        .map_err(dependency)?
        .ok_or(ResourceStoreError::RevisionConflict)?;
    Ok(ResourceCommandContext {
        tenant_id: selector.actor.tenant_id,
        actor: selector.actor,
        source: ResourceCommandSource::Account {
            agent_id: AgentId::from_uuid(agent).map_err(|_| ResourceStoreError::Corrupt)?,
            dialogue_id: selector.dialogue_id,
            entry_id: selector.entry_id,
        },
        invocation_id: None,
        tool_key: None,
        now,
    })
}
fn file_error(error: musteragent_runtime_resource::FileEditError) -> ResourceStoreError {
    use musteragent_runtime_resource::FileEditError;
    match error {
        FileEditError::DependencyUnavailable => ResourceStoreError::DependencyUnavailable,
        FileEditError::Corrupt => ResourceStoreError::Corrupt,
        _ => ResourceStoreError::RevisionConflict,
    }
}
impl AgentFileManagementPort for PostgresResourceStore {
    fn inspect(
        &self,
        selector: AgentFileSelector,
    ) -> ApplicationFuture<'_, Result<Option<AgentFileManagementState>, ResourceStoreError>> {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Resource,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            let ctx = context(&mut tx, selector, UtcTimestamp::from_unix_millis(0), false).await?;
            let file =
                match crate::agent_resource::account::resolve_owned(&mut tx, &ctx, false).await {
                    Ok(file) => file,
                    Err(
                        musteragent_runtime_resource::FileEditError::FileUnavailable
                        | musteragent_runtime_resource::FileEditError::UnsupportedFile,
                    ) => return Ok(None),
                    Err(error) => return Err(file_error(error)),
                };
            let node = load_node_on(&mut tx, ctx.tenant_id, file.node_id).await?;
            Ok(Some(AgentFileManagementState {
                revision: node.revision,
            }))
        })
    }
    fn execute(
        &self,
        command: AgentFileManagementCommand,
    ) -> ApplicationFuture<'_, Result<ResourceMutationReceipt, ResourceStoreError>> {
        Box::pin(async move {
            let mut tx = self.write_resource_unit_of_work().await?;
            let ctx = context(&mut tx, command.selector, command.now, true).await?;
            let (scope_kind, scope_id) = encode_scope(ctx.scope());
            let change = match &command.change {
                AgentFileManagementChange::Rename(name) => {
                    serde_json::json!({"rename":name.as_str()})
                }
                AgentFileManagementChange::Trash => serde_json::json!({"trash":true}),
            };
            let fingerprint = Sha256::digest(serde_json::to_vec(&serde_json::json!({"schema":"agent-file-management@1", "actor":ctx.actor.principal_id.to_string(), "tenant":ctx.tenant_id.to_string(), "dialogue":command.selector.dialogue_id.to_string(), "entry":command.selector.entry_id.to_string(), "expected_head":command.expected_head.to_string(), "expected_revision":command.expected_revision.get(), "change":change})).map_err(|_|ResourceStoreError::Corrupt)?);
            let identity = ResourceMutationIdentity {
                change_set_id: ResourceChangeSetId::from_uuid(command.command_id.into_uuid())
                    .map_err(|_| ResourceStoreError::Corrupt)?,
                idempotency_key: IdempotencyKey::new(format!(
                    "agent-file-management:{}",
                    command.command_id
                ))
                .map_err(|_| ResourceStoreError::Corrupt)?,
                command_fingerprint: fingerprint,
            };
            // Receipt recovery also works after Trash. It rechecks current account,
            // Agent and exact public entry, never reopens removed bytes.
            if let Some(row) = sqlx::query("SELECT command_fingerprint,actor_principal_id,status FROM resource.change_sets WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND change_set_id=$4")
                .bind(ctx.tenant_id.into_uuid()).bind(scope_kind).bind(scope_id).bind(identity.change_set_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(dependency)? {
                if row.try_get::<Vec<u8>,_>("command_fingerprint").map_err(corrupt)? != fingerprint.into_bytes() || row.try_get::<uuid::Uuid,_>("actor_principal_id").map_err(corrupt)? != ctx.actor.principal_id.into_uuid() || row.try_get::<i16,_>("status").map_err(corrupt)? != CHANGE_SET_COMMITTED { return Err(ResourceStoreError::RevisionConflict); }
                return duplicate_receipt(&mut tx, &ctx, &identity).await;
            }
            let file = crate::agent_resource::account::resolve_owned(&mut tx, &ctx, true)
                .await
                .map_err(file_error)?;
            let revision: i64 = sqlx::query_scalar("SELECT revision FROM resource.scope_heads WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 FOR UPDATE")
                .bind(ctx.tenant_id.into_uuid()).bind(scope_kind).bind(scope_id).fetch_one(&mut *tx).await.map_err(dependency)?;
            // Lock content head against an edit while checking the user's version.
            sqlx::query("SELECT artifact_id FROM artifact.artifacts WHERE artifact_id=$1 AND tenant_id=$2 FOR UPDATE")
                .bind(file.artifact_id.into_uuid()).bind(ctx.tenant_id.into_uuid()).fetch_one(&mut *tx).await.map_err(dependency)?;
            let file = crate::agent_resource::account::resolve_owned(&mut tx, &ctx, false)
                .await
                .map_err(file_error)?;
            let node = load_node_on(&mut tx, ctx.tenant_id, file.node_id).await?;
            if node.revision != command.expected_revision || file.head_id != command.expected_head {
                return Err(ResourceStoreError::RevisionConflict);
            }
            let expected_revision = decode_revision(revision)?;
            let mutation = match command.change {
                AgentFileManagementChange::Rename(name) => {
                    PreparedResourceMutation::Rename(RenameResourceNode {
                        context: ctx,
                        identity,
                        expected_revision,
                        node_id: file.node_id,
                        name,
                    })
                }
                AgentFileManagementChange::Trash => {
                    PreparedResourceMutation::Trash(TrashResourceNode {
                        context: ctx,
                        identity,
                        expected_revision,
                        node_id: file.node_id,
                    })
                }
            };
            let result = apply_prepared_resource_mutation(&mut tx, &mutation).await?;
            tx.commit()
                .await
                .map_err(|_| ResourceStoreError::DependencyUnavailable)?;
            Ok(result)
        })
    }
}
