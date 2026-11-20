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

/// The built-in `main` Agent is a shared definition, not an owned Agent row,
/// and its memories are scoped to the tenant, not to one user: rows carry no
/// producing principal. A tenant may hold several owner spaces, so access is
/// granted only to the tenant's sole owner. With co-owners present every
/// owner stays denied (fail closed) rather than letting one owner list, read,
/// revoke or erase memories another owner's runs produced. Any other built-in
/// slot, an inactive definition, or an actor without a space stays denied.
///
/// Only the actor's own space row is locked; the global built-in definition
/// is read without a row lock so concurrent memory operations of different
/// tenants never contend on it.
async fn builtin_main_agent(
    connection: &mut sqlx::PgConnection,
    actor: Actor,
    agent_id: AgentId,
) -> Result<bool, sqlx::Error> {
    let is_main: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM agent.agent_definitions
         WHERE agent_id=$1 AND built_in AND system_slot='main' AND lifecycle=1)",
    )
    .bind(agent_id.into_uuid())
    .fetch_one(&mut *connection)
    .await?;
    if !is_main {
        return Ok(false);
    }
    let row: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT s.space_id FROM agent.owner_spaces s
         WHERE s.product='musteragent' AND s.tenant_id=$1 AND s.owner_id=$2
           AND NOT EXISTS(SELECT 1 FROM agent.owner_spaces o
                          WHERE o.product='musteragent' AND o.tenant_id=$1 AND o.owner_id<>$2)
         FOR SHARE OF s",
    )
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
