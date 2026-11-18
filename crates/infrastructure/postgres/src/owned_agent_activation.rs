//! The canonical owned activation transaction, shared by direct commands
//! and host-prepared activation. The caller owns commit/rollback; all identity,
//! qualification, command replay and account-feed writes stay in this Owner.
use crate::dialogue_codec::*;
use musteragent_definition::owned_agent::*;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_session::*;
use sqlx::{PgConnection, Row};

type Error = DialogueStoreError;
pub(crate) async fn activate_in_transaction(
    tx: &mut PgConnection,
    command: OwnedAgentActivation,
) -> Result<OwnedAgentActivationReceipt, Error> {
    command.validate().map_err(|_| Error::Unavailable)?;
    let hash = command.fingerprint();
    let actor = command.actor;
    let at = command.agent.created_at;
    if let Some(receipt) =
        command_receipt(&mut *tx, actor, command.command_id, "activate_agent", hash).await?
    {
        let Some(wire::owner_command_receipt::Result::Activated(receipt)) = receipt.result else {
            return Err(Error::Corrupt);
        };
        let mut receipt = decode_activation(receipt)?;
        receipt.disposition = OwnedAgentActivationDisposition::Replayed;
        return Ok(receipt);
    }
    sqlx::query(
            "INSERT INTO agent.owner_spaces(space_id,tenant_id,owner_id,product,revision,created_at_ms) VALUES($1,$2,$3,'musteragent',1,$4) ON CONFLICT(tenant_id,owner_id,product) DO NOTHING",
        )
            .bind(command.space_id.into_uuid())
.bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(at.unix_millis())
.execute(&mut *tx).await.map_err(db)?;
    let row = sqlx::query(
        "SELECT *
                 FROM agent.owner_spaces
                 WHERE tenant_id=$1
                 AND owner_id=$2
                 AND product='musteragent' FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let mut space = space_row(&row)?;
    if command.make_default
        && let Some(default_id) = space.default_agent_id
    {
        let row = sqlx::query(
            "SELECT *
                 FROM agent.owned_agents
                 WHERE agent_id=$1
                 AND space_id=$2
                 AND status='active'",
        )
        .bind(default_id.into_uuid())
        .bind(space.space_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(Error::Corrupt)?;
        let agent = agent_row(&row)?;
        let row = sqlx::query(
            "SELECT *
                 FROM agent.owned_agent_configurations
                 WHERE agent_id=$1",
        )
        .bind(default_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let receipt = OwnedAgentActivationReceipt {
            space,
            agent,
            configuration: config_row(&row)?,
            disposition: OwnedAgentActivationDisposition::ExistingDefault,
        };
        save_receipt(
            &mut *tx,
            actor,
            command.command_id,
            "activate_agent",
            hash,
            wire::OwnerCommandReceipt {
                result: Some(wire::owner_command_receipt::Result::Activated(activation(
                    &receipt,
                ))),
            },
            at,
        )
        .await?;
        return Ok(receipt);
    }
    // Published material must belong to this account's tenant. A foreign
    // global definition ID is not permission to turn it into an instance.
    let qualified = sqlx::query(
        "SELECT d.agent_id
                 FROM agent.agent_definitions d
                 JOIN agent.agent_versions v ON v.agent_id=d.agent_id
                 JOIN artifact.content_refs c ON c.content_id=v.persona_content_id
                 WHERE d.agent_id=$1
                 AND d.agent_kind IN(2,3)
                 AND NOT d.built_in
                 AND d.lifecycle=1
                 AND v.agent_version_id=$2
                 AND v.retired_at_ms IS NULL
                 AND c.tenant_id=$3
                 AND c.state=2
                 FOR SHARE OF d,v",
    )
    .bind(command.agent.agent_id.into_uuid())
    .bind(command.agent.published_version_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if qualified.is_none() {
        return Err(Error::Unavailable);
    }
    let pointer = sqlx::query(
        "SELECT agent_version_id,pointer_version::text AS pointer_version,published_by
                 FROM agent.published_agent_version_pointers
                 WHERE agent_id=$1 FOR UPDATE",
    )
    .bind(command.agent.agent_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    if pointer
        .try_get::<uuid::Uuid, _>("agent_version_id")
        .map_err(db)?
        != command.agent.published_version_id.into_uuid()
        || pointer
            .try_get::<String, _>("pointer_version")
            .map_err(db)?
            != command.published_pointer_version.to_string()
        || pointer
            .try_get::<uuid::Uuid, _>("published_by")
            .map_err(db)?
            != actor.principal_id.into_uuid()
    {
        return Err(Error::RevisionConflict);
    }
    let agent = OwnedAgent {
        space_id: space.space_id,
        ..command.agent
    };
    let config = command.configuration;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents WHERE agent_id=$1)")
            .bind(agent.agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
    if exists {
        return Err(Error::Unavailable);
    }
    sqlx::query(
            "INSERT INTO agent.owned_agents(agent_id,space_id,dialogue_id,published_version_id,status,revision,created_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,'active',1,$5,$5)",
        )
            .bind(agent.agent_id.into_uuid())
.bind(space.space_id.into_uuid())
.bind(agent.dialogue_id.into_uuid())
.bind(agent.published_version_id.into_uuid())
.bind(at.unix_millis())
.execute(&mut *tx).await.map_err(db)?;
    sqlx::query(
            "INSERT INTO agent.agent_dialogues(dialogue_id,agent_id,tenant_id,owner_id,epoch,head_seq,created_at_ms) VALUES($1,$2,$3,$4,1,0,$5)",
        )
            .bind(agent.dialogue_id.into_uuid())
.bind(agent.agent_id.into_uuid())
.bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(at.unix_millis())
.execute(&mut *tx).await.map_err(db)?;
    sqlx::query(
            "INSERT INTO agent.owned_agent_configurations(agent_id,manifest_revision,profile_revision,display_name,description,avatar_content_ref,instructions_revision,instructions_content_ref,permission_revision,permission_mode) VALUES($1,1,1,$2,$3,$4,1,$5,1,$6)",
        )
            .bind(agent.agent_id.into_uuid())
.bind(&config.profile.display_name)
.bind(&config.profile.description)
.bind(config.profile.avatar_ref.as_ref().map(content_bytes))
.bind(config.instructions_ref.as_ref().map(content_bytes))
.bind(match config.permission_mode {
    musteragent_policy::PermissionMode::Supervised => "supervised",
    musteragent_policy::PermissionMode::Trusted => "trusted",
    _ => return Err(Error::Corrupt),
})
.execute(&mut *tx).await.map_err(db)?;
    sqlx::query(
            "INSERT INTO agent.owned_agent_inboxes(agent_id,dialogue_id,epoch,read_seq) VALUES($1,$2,1,0)",
        )
.bind(agent.agent_id.into_uuid())
.bind(agent.dialogue_id.into_uuid())
.execute(&mut *tx).await.map_err(db)?;
    retain_configuration(&mut *tx, actor, &config).await?;
    space.revision = nz(space
        .revision
        .get()
        .checked_add(1)
        .ok_or(Error::RevisionConflict)?)?;
    if command.make_default {
        space.default_agent_id = Some(agent.agent_id)
    }
    sqlx::query("UPDATE agent.owner_spaces SET default_agent_id=$2,revision=$3 WHERE space_id=$1")
        .bind(space.space_id.into_uuid())
        .bind(space.default_agent_id.map(AgentId::into_uuid))
        .bind(integer(space.revision.get())?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    let receipt = OwnedAgentActivationReceipt {
        space,
        agent,
        configuration: config,
        disposition: OwnedAgentActivationDisposition::Created,
    };
    let wire = activation(&receipt);
    save_receipt(
        &mut *tx,
        actor,
        command.command_id,
        "activate_agent",
        hash,
        wire::OwnerCommandReceipt {
            result: Some(wire::owner_command_receipt::Result::Activated(wire.clone())),
        },
        at,
    )
    .await?;
    outbox(
        &mut *tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "owner_space",
            aggregate: receipt.space.space_id.into_uuid(),
            cursor: DialogueCursor {
                epoch: 1,
                seq: receipt.space.revision.get(),
            },
            event: "agent_activated",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::AgentActivated(wire)),
            },
            at,
        },
    )
    .await?;
    Ok(receipt)
}
