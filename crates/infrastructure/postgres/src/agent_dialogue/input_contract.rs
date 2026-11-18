//! New-input eligibility is rechecked under the original space/dialogue locks.
use crate::dialogue_codec::{agent_row, db};
use musteragent_identity::{Actor, AgentId};
use musteragent_runtime_session::DialogueStoreError as Error;
use sqlx::{PgConnection, Row};

pub(super) async fn require(
    tx: &mut PgConnection,
    actor: Actor,
    agent_id: AgentId,
) -> Result<(), Error> {
    let row = sqlx::query("SELECT a.* FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR SHARE OF a")
        .bind(agent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    let agent = agent_row(&row)?;
    let definition = crate::agent_definition::decode_definition(
        &sqlx::query("SELECT * FROM agent.agent_definitions WHERE agent_id=$1 FOR SHARE")
            .bind(agent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let version = crate::agent_definition::decode_version(
        &sqlx::query(crate::agent_definition::VERSION_SELECT_BY_ID)
            .bind(agent.published_version_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?,
    )
    .map_err(|_| Error::Corrupt)?;
    let pointer = sqlx::query("SELECT agent_version_id,published_by FROM agent.published_agent_version_pointers WHERE agent_id=$1 ORDER BY pointer_version DESC LIMIT 1 FOR SHARE")
        .bind(agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
    if pointer
        .try_get::<uuid::Uuid, _>("agent_version_id")
        .map_err(db)?
        != agent.published_version_id.into_uuid()
        || pointer
            .try_get::<uuid::Uuid, _>("published_by")
            .map_err(db)?
            != actor.principal_id.into_uuid()
    {
        return Err(Error::RevisionConflict);
    }
    agent
        .validate_definition(&definition, &version)
        .map_err(|_| Error::Corrupt)?;
    version
        .validate_complete_for_publication()
        .map_err(|_| Error::Corrupt)?;
    Ok(())
}

/// Read-only, bounded history projection qualification. The original Agent and
/// Registry owners validate all identities and immutable material. Retained old
/// bases are not upgraded or advertised as new typed-reply consumers.
pub(crate) async fn reply_capable_agents(
    tx: &mut PgConnection,
    actor: Actor,
    ids: &[uuid::Uuid],
) -> Result<std::collections::HashSet<AgentId>, Error> {
    let agents = sqlx::query("SELECT a.* FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id)
        JOIN LATERAL (SELECT agent_version_id,published_by FROM agent.published_agent_version_pointers p WHERE p.agent_id=a.agent_id ORDER BY pointer_version DESC LIMIT 1) p
          ON p.agent_version_id=a.published_version_id AND p.published_by=s.owner_id
        WHERE a.agent_id=ANY($1) AND s.tenant_id=$2 AND s.owner_id=$3 AND a.status='active'")
        .bind(ids).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_all(&mut *tx).await.map_err(db)?;
    if agents.is_empty() {
        return Ok(Default::default());
    }
    let agents = agents
        .iter()
        .map(agent_row)
        .collect::<Result<Vec<_>, _>>()?;
    let versions = sqlx::query(crate::agent_definition::VERSION_SELECT_BY_IDS)
        .bind(
            agents
                .iter()
                .map(|a| a.published_version_id.into_uuid())
                .collect::<Vec<_>>(),
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
    let versions = versions
        .iter()
        .map(|r| crate::agent_definition::decode_version(r).map_err(|_| Error::Corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    let definitions = sqlx::query("SELECT * FROM agent.agent_definitions WHERE agent_id=ANY($1)")
        .bind(ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
    let definitions = definitions
        .iter()
        .map(|r| crate::agent_definition::decode_definition(r).map_err(|_| Error::Corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = std::collections::HashSet::new();
    for agent in agents {
        let version = versions
            .iter()
            .find(|v| v.agent_version_id == agent.published_version_id)
            .ok_or(Error::Corrupt)?;
        let definition = definitions
            .iter()
            .find(|d| d.agent_id == agent.agent_id)
            .ok_or(Error::Corrupt)?;
        agent
            .validate_definition(definition, version)
            .map_err(|_| Error::Corrupt)?;
        version
            .validate_complete_for_publication()
            .map_err(|_| Error::Corrupt)?;
        result.insert(agent.agent_id);
    }
    Ok(result)
}
