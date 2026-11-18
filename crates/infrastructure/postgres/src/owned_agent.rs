//! Account activation is one authority transaction; publication remains Agent Owner's job.
use crate::dialogue_codec::*;
use musteragent_definition::owned_agent::*;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use sqlx::{PgPool, Row};

type Error = DialogueStoreError;
#[derive(Clone)]
pub struct PostgresOwnedAgentStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresOwnedAgentStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
        }
    }
    async fn activate_inner(
        &self,
        command: OwnedAgentActivation,
    ) -> Result<OwnedAgentActivationReceipt, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let receipt =
            crate::owned_agent_activation::activate_in_transaction(&mut tx, command).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(receipt)
    }
}
impl OwnedAgentStore for PostgresOwnedAgentStore {
    fn delete_agent(
        &self,
        command: DeleteOwnedAgent,
    ) -> ApplicationFuture<'_, Result<OwnedAgent, OwnedAgentApplicationError>> {
        Box::pin(crate::owned_agent_lifecycle::delete(
            &self.database,
            command,
        ))
    }
    fn configure(
        &self,
        command: ConfigureOwnedAgent,
    ) -> ApplicationFuture<'_, Result<OwnedAgentConfiguration, Error>> {
        Box::pin(self.configure_inner(command))
    }
    fn home_snapshot(
        &self,
        actor: Actor,
    ) -> ApplicationFuture<'_, Result<Option<OwnerSpaceSnapshot>, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Projection,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let row = sqlx::query(
                "SELECT s.*, EXISTS (
                    SELECT 1 FROM agent.owned_agents a
                    JOIN agent.dialogue_user_intents i USING(dialogue_id)
                    WHERE a.agent_id=s.default_agent_id
                    AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id
                 ) AS has_user_input
                 FROM agent.owner_spaces s
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND product='musteragent'",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
            let Some(row) = row else {
                tx.commit()
                    .await
                    .map_err(crate::dialogue_codec::transaction)?;
                return Ok(None);
            };
            let space = space_row(&row)?;

            let sync_epoch = unsigned(row.try_get("sync_epoch").map_err(db)?)?;
            let head_event_seq = unsigned(row.try_get("head_event_seq").map_err(db)?)?;
            let rows=sqlx::query(
            "SELECT a.*,scene.primary_agent_id,c.manifest_revision,c.profile_revision,c.display_name,c.description,c.avatar_content_ref,c.instructions_revision,c.instructions_content_ref,c.permission_revision,c.permission_mode
                 FROM agent.owned_agents a
                 JOIN agent.owned_agent_scenes scene USING(agent_id)
                 LEFT JOIN agent.owned_agent_configurations c USING(agent_id)
                 WHERE a.space_id=$1
                 AND a.status<>'deleted'
                 ORDER BY a.created_at_ms,a.agent_id",
        )
.bind(space.space_id.into_uuid())
.fetch_all(&mut *tx).await.map_err(db)?;
            let activities = crate::dialogue_execution_activity::read_many(&mut tx, actor).await?;
            let agents = rows
                .iter()
                .map(|row| {
                    Ok(OwnedAgentSnapshot {
                        primary_agent_id: AgentId::from_uuid(
                            row.try_get("primary_agent_id").map_err(db)?,
                        )
                        .map_err(|_| Error::Corrupt)?,
                        activity: activities.get(&agent_row(row)?.agent_id).cloned(),
                        agent: agent_row(row)?,
                        configuration: config_row(row)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(Some(OwnerSpaceSnapshot {
                space,
                agents,
                sync_epoch,
                head_event_seq,
            }))
        })
    }
    fn activate(
        &self,
        command: OwnedAgentActivation,
    ) -> ApplicationFuture<'_, Result<OwnedAgentActivationReceipt, Error>> {
        Box::pin(self.activate_inner(command))
    }
    fn load_space(&self, actor: Actor) -> ApplicationFuture<'_, Result<Option<OwnerSpace>, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            sqlx::query(
                "SELECT *
                 FROM agent.owner_spaces
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND product='musteragent'",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .as_ref()
            .map(space_row)
            .transpose()
        })
    }
    fn list(&self, actor: Actor) -> ApplicationFuture<'_, Result<Vec<OwnedAgent>, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            sqlx::query(
                "SELECT a.*
                 FROM agent.owned_agents a
                 JOIN agent.owner_spaces s USING(space_id)
                 WHERE s.tenant_id=$1
                 AND s.owner_id=$2
                 AND s.product='musteragent'
                 AND a.status<>'deleted'
                 ORDER BY a.created_at_ms,a.agent_id",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_all(&self.pool)
            .await
            .map_err(db)?
            .iter()
            .map(agent_row)
            .collect()
        })
    }
    fn configuration(
        &self,
        actor: Actor,
        agent_id: AgentId,
    ) -> ApplicationFuture<'_, Result<Option<OwnedAgentConfiguration>, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            sqlx::query(
                "SELECT c.*
                 FROM agent.owned_agent_configurations c
                 JOIN agent.owned_agents a USING(agent_id)
                 JOIN agent.owner_spaces s USING(space_id)
                 WHERE a.agent_id=$1
                 AND s.tenant_id=$2
                 AND s.owner_id=$3
                 AND a.status<>'deleted'",
            )
            .bind(agent_id.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .as_ref()
            .map(config_row)
            .transpose()
        })
    }
}

impl PostgresOwnedAgentStore {
    async fn configure_inner(
        &self,
        command: ConfigureOwnedAgent,
    ) -> Result<OwnedAgentConfiguration, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let config = configure_in_transaction(&mut tx, &command).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(config)
    }
}

/// Shared account configuration owner. UI commands and authorized Runtime tools
/// apply the same CAS, retention, command receipt and account outbox transaction.
pub(crate) async fn configure_in_transaction(
    tx: &mut sqlx::PgConnection,
    command: &ConfigureOwnedAgent,
) -> Result<OwnedAgentConfiguration, Error> {
    command.validate().map_err(|_| Error::Unavailable)?;
    let actor = command.actor;
    let hash = command.fingerprint();
    if let Some(receipt) =
        command_receipt(tx, actor, command.command_id, "configure_agent", hash).await?
    {
        let Some(wire::owner_command_receipt::Result::Configured(value)) = receipt.result else {
            return Err(Error::Corrupt);
        };
        let config = musteragent_persistence_codec::dialogue_codec::decode_configuration(value)
            .map_err(|_| Error::Corrupt)?;
        return Ok(config);
    }
    let row = sqlx::query(
        "SELECT s.*
                 FROM agent.owner_spaces s
                 JOIN agent.owned_agents a USING(space_id)
                 WHERE a.agent_id=$1
                 AND s.tenant_id=$2
                 AND s.owner_id=$3
                 FOR UPDATE OF s",
    )
    .bind(command.agent_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    let space = space_row(&row)?;
    let status: String =
        sqlx::query_scalar("SELECT status FROM agent.owned_agents WHERE agent_id=$1 FOR SHARE")
            .bind(command.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    if status != "active" {
        return Err(Error::Inactive);
    }
    let row =
        sqlx::query("SELECT * FROM agent.owned_agent_configurations WHERE agent_id=$1 FOR UPDATE")
            .bind(command.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    let config = command.apply(&config_row(&row)?)?;
    let mode = match config.permission_mode {
        musteragent_policy::PermissionMode::Supervised => "supervised",
        musteragent_policy::PermissionMode::Trusted => "trusted",
        musteragent_policy::PermissionMode::FullAccess => "full_access",
        _ => return Err(Error::Corrupt),
    };
    sqlx::query(
            "UPDATE agent.owned_agent_configurations
                 SET manifest_revision=$2,profile_revision=$3,display_name=$4,description=$5,avatar_content_ref=$6,instructions_revision=$7,instructions_content_ref=$8,permission_revision=$9,permission_mode=$10
                 WHERE agent_id=$1",
        )
            .bind(command.agent_id.into_uuid())
.bind(integer(config.manifest_revision.get())?)
.bind(integer(config.profile.revision.get())?)
.bind(&config.profile.display_name)
.bind(&config.profile.description)
.bind(config.profile.avatar_ref.as_ref().map(content_bytes))
.bind(integer(config.instructions_revision.get())?)
.bind(config.instructions_ref.as_ref().map(content_bytes))
.bind(integer(config.permission_revision.get())?)
.bind(mode)
.execute(&mut *tx).await.map_err(db)?;
    retain_configuration(tx, actor, &config).await?;
    if matches!(command.change, AgentConfigurationChange::Permission { .. }) {
        // Wake the original scheduler to settle waits whose pinned authority
        // can no longer accept a response. The configuration owner never edits Runs.
        sqlx::query("SELECT pg_notify('musteragent_runtime_dispatch',$1)")
            .bind(command.agent_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
    }
    let seq = space
        .revision
        .get()
        .checked_add(1)
        .ok_or(Error::RevisionConflict)?;
    sqlx::query(
        "UPDATE agent.owner_spaces
                 SET revision=$2
                 WHERE space_id=$1",
    )
    .bind(space.space_id.into_uuid())
    .bind(integer(seq)?)
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    let value = musteragent_persistence_codec::dialogue_codec::configuration(&config);
    save_receipt(
        tx,
        actor,
        command.command_id,
        "configure_agent",
        hash,
        wire::OwnerCommandReceipt {
            result: Some(wire::owner_command_receipt::Result::Configured(
                value.clone(),
            )),
        },
        command.changed_at,
    )
    .await?;
    outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "owner_space",
            aggregate: space.space_id.into_uuid(),
            cursor: DialogueCursor { epoch: 1, seq },
            event: "configuration_changed",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::ConfigurationChanged(value)),
            },
            at: command.changed_at,
        },
    )
    .await?;
    Ok(config)
}

/// A bounded prompt projection. The workbench directory itself stays complete.
pub(crate) async fn scene_members(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
    primary: AgentId,
) -> Result<Vec<musteragent_execution_context::AgentSceneMember>, Error> {
    let rows = sqlx::query("SELECT a.agent_id,c.display_name,left(c.description,160) AS responsibility
        FROM agent.owned_agent_scenes scene JOIN agent.owned_agents a USING(agent_id)
        JOIN agent.owned_agent_configurations c USING(agent_id) JOIN agent.owner_spaces s ON s.space_id=scene.space_id
        WHERE scene.primary_agent_id=$1 AND a.agent_id<>$1 AND a.status='active' AND s.tenant_id=$2 AND s.owner_id=$3
        ORDER BY a.created_at_ms,a.agent_id LIMIT $4")
        .bind(primary.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .bind(musteragent_execution_context::SCENE_MEMBER_CONTEXT_LIMIT as i64)
        .fetch_all(tx).await.map_err(db)?;
    rows.iter()
        .map(|row| {
            Ok(musteragent_execution_context::AgentSceneMember {
                agent_id: AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                    .map_err(|_| Error::Corrupt)?,
                name: row.try_get("display_name").map_err(db)?,
                responsibility: row.try_get("responsibility").map_err(db)?,
            })
        })
        .collect()
}
