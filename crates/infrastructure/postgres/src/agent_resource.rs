//! Resource authority for a real Dialogue execution. The persistent Agent owns
//! placement; the sealed invocation retains its original Run provenance.
pub(crate) mod account;
pub(crate) mod device;
pub(crate) mod device_custody;

use musteragent_runtime_resource::{ResourceCommandContext, ResourceCommandSource};
use sqlx::PgConnection;

use crate::dialogue_execution::authority::resolve_execution_on;
pub(crate) use crate::dialogue_execution::authority::{
    resolve_commit_execution, resolve_execution,
};

pub(crate) async fn authorize(
    tx: &mut PgConnection,
    context: &ResourceCommandContext,
    lock: bool,
) -> Result<bool, sqlx::Error> {
    let ResourceCommandSource::DialogueExecution {
        conversation_id,
        agent_id,
    } = context.source
    else {
        return Ok(false);
    };
    let (Some(invocation), Some(tool)) = (context.invocation_id, context.tool_key.as_ref()) else {
        return Ok(false);
    };
    if lock {
        // Existing commit owners lock the Run before entering this function.
        // Account/configuration order matches Dialogue admission/publication.
        let space = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT space_id FROM agent.owned_agents WHERE agent_id=$1",
        )
        .bind(agent_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(space) = space else { return Ok(false) };
        let valid=sqlx::query_scalar::<_,uuid::Uuid>("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(space).bind(context.tenant_id.into_uuid()).bind(context.actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await?;
        if valid.is_none() {
            return Ok(false);
        }
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
    Ok(
        resolve_execution_on(tx, context.actor, conversation_id, invocation, lock)
            .await?
            .is_some_and(|(current, key)| current == agent_id && &key == tool),
    )
}
