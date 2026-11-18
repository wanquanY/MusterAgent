//! The original account transaction fences new inputs before retiring a partner.
use crate::dialogue_codec::*;
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentStatus};
use musteragent_execution_context::DialogueCursor;
use musteragent_persistence_codec::{dialogue_codec as codec, wire};
use musteragent_runtime_session::{
    DeleteOwnedAgent, DialogueStoreError, OwnedAgentApplicationError as Error,
};

pub(crate) async fn delete(
    database: &crate::PostgresDatabaseRuntime,
    command: DeleteOwnedAgent,
) -> Result<OwnedAgent, Error> {
    require_user(command.actor)?;
    let mut tx = database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::Control)
        .await
        .map_err(transaction)?;
    let actor = command.actor;
    let hash = command.fingerprint();
    if let Some(receipt) =
        command_receipt(&mut tx, actor, command.command_id, "delete_agent", hash).await?
    {
        let Some(wire::owner_command_receipt::Result::DeletedAgent(agent)) = receipt.result else {
            return Err(DialogueStoreError::Corrupt.into());
        };
        return codec::decode_agent(agent).map_err(|_| DialogueStoreError::Corrupt.into());
    }
    let row = sqlx::query("SELECT s.* FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s")
        .bind(command.agent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let space = space_row(&row)?;
    let row = sqlx::query("SELECT * FROM agent.owned_agents WHERE agent_id=$1 FOR UPDATE")
        .bind(command.agent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
    let primary: bool = sqlx::query_scalar(
        "SELECT primary_agent_id=agent_id FROM agent.owned_agent_scenes WHERE agent_id=$1",
    )
    .bind(command.agent_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let agent = agent_row(&row)?.change_status(
        command.expected_revision,
        OwnedAgentStatus::Deleted,
        primary,
        command.deleted_at,
    )?;
    // A public receipt is not evidence that execution ended. Consult original
    // admitted Runs and task revisions; an offline/unknown Native run stays busy.
    let busy: bool = sqlx::query_scalar(include_str!("owned_agent_lifecycle.sql"))
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(command.agent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
    if busy {
        return Err(Error::Busy);
    }
    sqlx::query("UPDATE agent.owned_agents SET status='deleted',revision=$2,updated_at_ms=$3 WHERE agent_id=$1")
        .bind(agent.agent_id.into_uuid()).bind(integer(agent.revision.get())?).bind(agent.updated_at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    let revision = space
        .revision
        .get()
        .checked_add(1)
        .ok_or(DialogueStoreError::RevisionConflict)?;
    sqlx::query("UPDATE agent.owner_spaces SET revision=$2 WHERE space_id=$1")
        .bind(space.space_id.into_uuid())
        .bind(integer(revision)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    let value = codec::agent(&agent);
    save_receipt(
        &mut tx,
        actor,
        command.command_id,
        "delete_agent",
        hash,
        wire::OwnerCommandReceipt {
            result: Some(wire::owner_command_receipt::Result::DeletedAgent(
                value.clone(),
            )),
        },
        command.deleted_at,
    )
    .await?;
    outbox(
        &mut tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "owner_space",
            aggregate: space.space_id.into_uuid(),
            cursor: DialogueCursor {
                epoch: 1,
                seq: revision,
            },
            event: "agent_deleted",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::AgentDeleted(value)),
            },
            at: command.deleted_at,
        },
    )
    .await?;
    tx.commit().await.map_err(transaction)?;
    Ok(agent)
}
