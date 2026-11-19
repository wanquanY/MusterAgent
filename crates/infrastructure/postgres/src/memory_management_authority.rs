//! Memory management authority lives beside the original Memory transactions.
use musteragent_identity::{Actor, AgentId, PrincipalKind};

pub(crate) async fn authorize(
    connection: &mut sqlx::PgConnection,
    actor: Actor,
    agent_id: AgentId,
) -> Result<bool, sqlx::Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Ok(false);
    }
    if builtin_main_agent(connection, actor, agent_id).await? {
        return Ok(true);
    }
    let row: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT a.agent_id FROM agent.owned_agents a
         JOIN agent.owner_spaces s USING(space_id)
         WHERE a.agent_id=$1 AND a.status='active' AND s.product='musteragent'
           AND s.tenant_id=$2 AND s.owner_id=$3 FOR SHARE OF a,s",
    )
    .bind(agent_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(connection)
    .await?;
    Ok(row.is_some())
}

/// The built-in `main` Agent is a shared definition, not an owned Agent row, but
/// its memories live in the owner's tenant. The owner of the tenant's space
/// manages them (list, revoke, erase) like an owned Agent's. Any other built-in
/// slot, an inactive definition, or an actor without a space stays denied.
async fn builtin_main_agent(
    connection: &mut sqlx::PgConnection,
    actor: Actor,
    agent_id: AgentId,
) -> Result<bool, sqlx::Error> {
    let row: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT s.space_id FROM agent.agent_definitions d
         JOIN agent.owner_spaces s ON s.product='musteragent' AND s.tenant_id=$2 AND s.owner_id=$3
         WHERE d.agent_id=$1 AND d.built_in AND d.system_slot='main' AND d.lifecycle=1
         FOR SHARE OF d,s",
    )
    .bind(agent_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(connection)
    .await?;
    Ok(row.is_some())
}

/// The Owner-scoped account entry uses its original product space, never a default
/// Agent surrogate. Space/account retirement is checked again after Content IO.
pub(crate) async fn authorize_account(
    connection: &mut sqlx::PgConnection,
    actor: Actor,
) -> Result<bool, sqlx::Error> {
    if actor.principal_kind != PrincipalKind::User {
        return Ok(false);
    }
    let row: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT space_id FROM agent.owner_spaces WHERE product='musteragent' AND tenant_id=$1 AND owner_id=$2 FOR SHARE")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(connection).await?;
    Ok(row.is_some())
}
