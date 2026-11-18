//! Current Account-owned Agent material shared by Intent and Goal source owners.
//! Caller establishes its business subject first; this lookup grants no Run.
use super::*;
use musteragent_definition::owned_agent::{OwnedAgent, OwnedAgentConfiguration};

pub(crate) struct CurrentOwnedAgentSource {
    pub default_agent_id: AgentId,
    pub agent: OwnedAgent,
    pub configuration: OwnedAgentConfiguration,
    pub identity: AgentAdmissionIdentity,
    pub dialogue: AgentDialogue,
}

pub(crate) async fn load(
    tx: &mut PgConnection,
    actor: Actor,
    dialogue_id: AgentDialogueId,
    lock: bool,
) -> Result<CurrentOwnedAgentSource, DialogueStoreError> {
    require_user(actor)?;
    // Matches the Dialogue owner mutation lock order: space -> dialogue -> Agent/config.
    let suffix = if lock { " FOR SHARE OF s" } else { "" };
    let space_row=sqlx::query(&format!("SELECT s.space_id,s.default_agent_id FROM agent.owner_spaces s JOIN agent.owned_agents a ON a.space_id=s.space_id JOIN agent.agent_dialogues d ON d.agent_id=a.agent_id AND d.tenant_id=s.tenant_id AND d.owner_id=s.owner_id WHERE d.dialogue_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3{suffix}"))
        .bind(dialogue_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let space: uuid::Uuid = space_row.try_get("space_id").map_err(db)?;
    let default_agent_id = AgentId::from_uuid(space_row.try_get("default_agent_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let suffix = if lock { " FOR SHARE" } else { "" };
    let dr=sqlx::query(&format!("SELECT * FROM agent.agent_dialogues WHERE dialogue_id=$1 AND tenant_id=$2 AND owner_id=$3{suffix}"))
        .bind(dialogue_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    let dialogue = dialogue_row(&dr)?;
    let ar = sqlx::query(&format!(
        "SELECT * FROM agent.owned_agents WHERE agent_id=$1 AND space_id=$2{suffix}"
    ))
    .bind(dialogue.agent_id.into_uuid())
    .bind(space)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    let agent = agent_row(&ar)?;
    let cr = sqlx::query(&format!(
        "SELECT * FROM agent.owned_agent_configurations WHERE agent_id=$1{suffix}"
    ))
    .bind(agent.agent_id.into_uuid())
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let configuration = config_row(&cr)?;
    let definition = crate::agent_definition::decode_definition(
        &sqlx::query(&format!(
            "SELECT * FROM agent.agent_definitions WHERE agent_id=$1{suffix}"
        ))
        .bind(agent.agent_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?,
    )
    .map_err(|_| DialogueStoreError::Corrupt)?;
    let version = crate::agent_definition::decode_version(
        &sqlx::query(crate::agent_definition::VERSION_SELECT_BY_ID)
            .bind(agent.published_version_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?,
    )
    .map_err(|_| DialogueStoreError::Corrupt)?;
    let published=sqlx::query(&format!("SELECT agent_version_id,published_by FROM agent.published_agent_version_pointers WHERE agent_id=$1 ORDER BY pointer_version DESC LIMIT 1{suffix}"))
        .bind(agent.agent_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(published) = published else {
        return Err(DialogueStoreError::RevisionConflict);
    };
    if published
        .try_get::<uuid::Uuid, _>("agent_version_id")
        .map_err(db)?
        != agent.published_version_id.into_uuid()
        || published
            .try_get::<uuid::Uuid, _>("published_by")
            .map_err(db)?
            != actor.principal_id.into_uuid()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(CurrentOwnedAgentSource {
        default_agent_id,
        agent,
        configuration,
        dialogue,
        identity: AgentAdmissionIdentity {
            definition,
            version,
        },
    })
}
