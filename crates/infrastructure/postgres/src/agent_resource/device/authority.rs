//! Only the original accepted Device source can resolve the canonical Agent tree.
use super::*;
use musteragent_runtime_resource::{
    ResolveResourceContext, ResourceContextAuthority, ResourceContextError,
};
use sqlx::{PgConnection, Row};

pub(crate) async fn authorize(
    tx: &mut PgConnection,
    context: &ResourceCommandContext,
    lock: bool,
) -> Result<bool, sqlx::Error> {
    let ResourceCommandSource::DeviceExecution {
        conversation_id,
        agent_id,
        intent_id,
        run_id,
        device_id,
        installation_id,
        source_digest,
        snapshot_hash,
        grant_proof_hash,
        work_item,
        ..
    } = context.source
    else {
        return Ok(false);
    };
    if context.validate().is_err() || context.actor.principal_kind != PrincipalKind::User {
        return Ok(false);
    }
    if lock
        && context
            .tool_key
            .as_ref()
            .is_some_and(|key| key.logical_key().starts_with("agenda."))
    {
        sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE tenant_id=$1 AND owner_id=$2 AND product='musteragent' FOR UPDATE")
            .bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx).await?;
    }
    if let Some(task) = work_item {
        return authorize_task(tx, context, task, lock).await;
    }
    let row=sqlx::query("SELECT d.source_envelope,d.source_sha256 FROM agent.device_admissions d JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.admitted_revision WHERE d.intent_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.agent_id=$4 AND d.conversation_id=$5 AND d.run_id=$6 AND d.device_id=$7 AND d.installation_id=$8 AND d.source_digest=$9 AND d.admitted_snapshot_hash=$10 AND v.signed_grant_hash=$11 AND d.admitted_at_ms IS NOT NULL")
        .bind(intent_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid()).bind(agent_id.into_uuid()).bind(conversation_id.into_uuid()).bind(run_id.into_uuid()).bind(device_id.into_uuid()).bind(installation_id.into_uuid()).bind(source_digest.into_bytes().to_vec()).bind(snapshot_hash.into_bytes().to_vec()).bind(grant_proof_hash.into_bytes().to_vec()).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(false) };
    let bytes: Vec<u8> = row.try_get("source_envelope")?;
    if bytes.len() > 4 * 1024 * 1024
        || row.try_get::<Vec<u8>, _>("source_sha256")? != Sha256::digest(&bytes).into_bytes()
    {
        return Ok(false);
    }
    let Ok((device, prepared)) = musteragent_persistence_codec::device_source_codec::decode(&bytes)
    else {
        return Ok(false);
    };
    if prepared.validate().is_err()
        || device != device_id
        || prepared.actor() != context.actor
        || prepared.scope.agent_id != agent_id
    {
        return Ok(false);
    }
    // Original account -> Agent -> configuration lock order. This holds the
    // permission revision through Resource CAS and Artifact version insertion.
    let query = if lock {
        "SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent' AND a.agent_id=$3 FOR SHARE OF s"
    } else {
        "SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent' AND a.agent_id=$3"
    };
    if sqlx::query_scalar::<_, uuid::Uuid>(query)
        .bind(context.tenant_id.into_uuid())
        .bind(context.actor.principal_id.into_uuid())
        .bind(agent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    if lock {
        sqlx::query("SELECT agent_id FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE")
            .bind(agent_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await?;
        sqlx::query(
            "SELECT agent_id FROM agent.owned_agent_configurations WHERE agent_id=$1 FOR SHARE",
        )
        .bind(agent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;
    }
    match crate::agent_resource::device_custody::current_agent(
        tx,
        context.actor,
        agent_id,
        prepared.scope.dialogue_id,
        prepared.configuration.permission_revision.get(),
        prepared.configuration.permission_mode,
    )
    .await
    {
        Ok(_) => Ok(true),
        Err(musteragent_runtime_session::DialogueStoreError::Infrastructure) => Err(
            sqlx::Error::Protocol("Dialogue authority dependency unavailable".into()),
        ),
        Err(_) => Ok(false),
    }
}

#[derive(Clone)]
pub(super) struct Authority {
    pub store: PostgresDeviceResourceStore,
    pub actor: Actor,
    pub target: OriginDeviceExecutionTarget,
    pub request: DeviceResourceRequest,
    pub context: ResourceCommandContext,
}
impl Authority {
    /// Complete remote target qualification before the fresh repeatable-read
    /// snapshot, then recheck exact custody and original current permission.
    /// Shared by accepted-input and execution-directory reads; not new authority.
    pub(super) async fn current_snapshot(
        &self,
    ) -> Result<
        (
            crate::GovernedPostgresTransaction<'static>,
            ResourceCommandContext,
            super::super::device_custody::DeviceCustody,
        ),
        Error,
    > {
        let (_, qualified) = self.current_source().await?;
        let mut tx = crate::begin_transaction(
            &self.store.database,
            crate::PostgresTransactionPolicy::single_attempt(
                &self.store.database,
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionAccess::ReadWrite,
            )
            .with_isolation(crate::PostgresTransactionIsolation::RepeatableRead),
        )
        .await
        .map_err(|_| Error::Infrastructure)?;
        let custody = super::super::device_custody::load_local(
            &mut tx,
            self.actor,
            &self.target,
            self.request.command().admission(),
            true,
        )
        .await?;
        if custody != qualified {
            return Err(Error::Conflict);
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        self.request.validate_at(now)?;
        if !authorize(&mut tx, &self.context, false).await.map_err(db)? {
            return Err(Error::Forbidden);
        }
        let mut context = self.context.clone();
        context.now = now;
        Ok((tx, context, custody))
    }
    pub async fn current(&self) -> Result<ResourceCommandContext, Error> {
        self.current_source().await.map(|(context, _)| context)
    }
    pub async fn current_source(
        &self,
    ) -> Result<
        (
            ResourceCommandContext,
            super::super::device_custody::DeviceCustody,
        ),
        Error,
    > {
        let mut conn = self.store.database.pool().acquire().await.map_err(db)?;
        let now = crate::ledger::database_now_ms(&mut conn)
            .await
            .map_err(|_| Error::Infrastructure)?;
        self.request.validate_at(now)?;
        self.target
            .require_current_at(now)
            .map_err(|_| Error::Forbidden)?;
        let custody = super::super::device_custody::load(
            &mut conn,
            self.actor,
            &self.target,
            self.request.command().admission(),
            self.store.task_targets.as_deref(),
            true,
        )
        .await?;
        if !authorize(&mut conn, &self.context, false)
            .await
            .map_err(db)?
        {
            return Err(Error::Forbidden);
        }
        let mut context = self.context.clone();
        context.now = now;
        Ok((context, custody))
    }
}
impl ResourceContextAuthority for Authority {
    fn resolve(
        &self,
        request: ResolveResourceContext,
    ) -> ApplicationFuture<'_, Result<ResourceCommandContext, ResourceContextError>> {
        Box::pin(async move {
            if request.actor != self.actor
                || Some(request.conversation_id) != self.context.conversation_id()
                || request.invocation_id != self.context.invocation_id
            {
                return Err(ResourceContextError::ScopeUnavailable);
            }
            let mut context = self.current().await.map_err(|e| match e {
                Error::Infrastructure => ResourceContextError::DependencyUnavailable,
                Error::Corrupt => ResourceContextError::Corrupt,
                _ => ResourceContextError::ScopeUnavailable,
            })?;
            context.now = request.now;
            Ok(context)
        })
    }
}

async fn authorize_task(
    tx: &mut PgConnection,
    context: &ResourceCommandContext,
    task: musteragent_runtime_resource::DeviceWorkItemSource,
    lock: bool,
) -> Result<bool, sqlx::Error> {
    let ResourceCommandSource::DeviceExecution {
        conversation_id,
        agent_id,
        intent_id,
        run_id,
        device_id,
        installation_id,
        source_digest,
        snapshot_hash,
        grant_proof_hash,
        ..
    } = context.source
    else {
        return Ok(false);
    };
    let row=sqlx::query("SELECT * FROM collaboration.work_item_device_admitted_custody WHERE run_id=$1 AND work_item_id=$2 AND goal_revision=$3 AND control_revision=$4 AND assignment_epoch=$5 AND admitted_snapshot_hash=$6 AND signed_grant_hash=$7 AND source_digest=$8")
        .bind(run_id.into_uuid()).bind(task.work_item_id.into_uuid()).bind(task.goal_revision as i64).bind(task.control_revision as i64)
        .bind(task.assignment_epoch as i64).bind(snapshot_hash.into_bytes().to_vec()).bind(grant_proof_hash.into_bytes().to_vec())
        .bind(source_digest.into_bytes().to_vec()).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(false) };
    let bytes: Vec<u8> = row.try_get("source_envelope")?;
    if row.try_get::<Vec<u8>, _>("source_sha256")? != Sha256::digest(&bytes).into_bytes() {
        return Ok(false);
    }
    let Ok(source) = musteragent_persistence_codec::work_item_device_source_codec::decode(&bytes)
    else {
        return Ok(false);
    };
    let p = source.preparation();
    if p.actor() != context.actor
        || p.key.executing_agent_id != agent_id
        || p.work_item.source_intent_id != intent_id
        || p.identities().conversation_id != conversation_id
        || source.target().device_id != device_id
        || source.target().installation_id != installation_id
    {
        return Ok(false);
    }
    if lock {
        sqlx::query("SELECT a.agent_id FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR SHARE OF s,a")
            .bind(agent_id.into_uuid()).bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await?;
        sqlx::query(
            "SELECT agent_id FROM agent.owned_agent_configurations WHERE agent_id=$1 FOR SHARE",
        )
        .bind(agent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;
    }
    match crate::work_item_execution::device::require_live_source(tx, p).await {
        Ok(()) => Ok(true),
        Err(musteragent_runtime_session::DialogueStoreError::Infrastructure) => Err(
            sqlx::Error::Protocol("Task authority dependency unavailable".into()),
        ),
        Err(_) => Ok(false),
    }
}
