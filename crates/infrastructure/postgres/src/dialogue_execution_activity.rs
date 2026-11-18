//! Public activity is a disposable projection of original execution owners.
//! Account reads/streams consume one revision; no client polls private Runs.
use crate::dialogue_codec::{db, transaction};
use musteragent_identity::*;
use musteragent_persistence_codec::{dialogue_execution_activity_codec as codec, wire};
use musteragent_runtime_session::*;
use sqlx::{Row, postgres::PgRow};
pub(crate) mod context;
pub(crate) mod directory;
mod native;
mod projection;
#[cfg(test)]
mod tests;
mod worker;
pub use native::PostgresNativeDialogueActivityStore;

#[derive(Clone)]
pub struct PostgresDialogueActivityProjection {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresDialogueActivityProjection {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        Self {
            database: database.into(),
        }
    }
}
pub(crate) async fn read(
    tx: &mut sqlx::PgConnection,
    agent: AgentId,
    dialogue: AgentDialogueId,
) -> Result<Option<OwnedAgentActivity>, DialogueStoreError> {
    sqlx::query("SELECT * FROM agent.dialogue_execution_activity WHERE agent_id=$1")
        .bind(agent.into_uuid())
        .fetch_optional(tx)
        .await
        .map_err(db)?
        .map(|row| decode(&row, agent, dialogue))
        .transpose()
}
pub(crate) async fn read_many(
    tx: &mut sqlx::PgConnection,
    actor: Actor,
) -> Result<std::collections::BTreeMap<AgentId, OwnedAgentActivity>, DialogueStoreError> {
    let rows=sqlx::query("SELECT p.*,a.dialogue_id FROM agent.dialogue_execution_activity p JOIN agent.owned_agents a USING(agent_id) JOIN agent.owner_spaces s USING(space_id) WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.product='musteragent' AND a.status<>'deleted'")
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_all(tx).await.map_err(db)?;
    rows.iter()
        .map(|row| {
            let agent = AgentId::from_uuid(row.try_get("agent_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            let dialogue = AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
                .map_err(|_| DialogueStoreError::Corrupt)?;
            Ok((agent, decode(row, agent, dialogue)?))
        })
        .collect()
}
fn decode(
    row: &PgRow,
    agent: AgentId,
    dialogue: AgentDialogueId,
) -> Result<OwnedAgentActivity, DialogueStoreError> {
    let revision: i64 = row.try_get("revision").map_err(db)?;
    let detailed = row
        .try_get::<Option<i64>, _>("wait_details_revision")
        .map_err(db)?
        == Some(revision);
    let (approvals, retries, earliest) = if detailed {
        (
            row.try_get::<i64, _>("waiting_approval_count")
                .map_err(db)?,
            row.try_get::<i64, _>("scheduled_retry_count").map_err(db)?,
            row.try_get::<Option<i64>, _>("earliest_retry_at_ms")
                .map_err(db)?,
        )
    } else {
        (0, 0, None)
    };
    let permissions = if row
        .try_get::<Option<i64>, _>("permission_wait_revision")
        .map_err(db)?
        == Some(revision)
    {
        row.try_get::<i64, _>("waiting_permission_count")
            .map_err(db)?
    } else {
        0
    };
    let value = OwnedAgentActivity {
        agent_id: agent,
        dialogue_id: dialogue,
        revision: u64::try_from(revision).map_err(|_| DialogueStoreError::Corrupt)?,
        state: codec::state(i32::from(row.try_get::<i16, _>("state").map_err(db)?))
            .map_err(|_| DialogueStoreError::Corrupt)?,
        step: row
            .try_get::<Option<i16>, _>("step")
            .map_err(db)?
            .map(|v| codec::step(i32::from(v)))
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        active_execution_count: u32::try_from(row.try_get::<i64, _>("active_count").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        waiting_execution_count: u32::try_from(row.try_get::<i64, _>("waiting_count").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        waiting_approval_count: u32::try_from(approvals)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        waiting_permission_count: u32::try_from(permissions)
            .map_err(|_| DialogueStoreError::Corrupt)?,
        scheduled_retry_count: u32::try_from(retries).map_err(|_| DialogueStoreError::Corrupt)?,
        earliest_retry_at: earliest.map(UtcTimestamp::from_unix_millis),
        unconfirmed_execution_count: u32::try_from(
            row.try_get::<i64, _>("unconfirmed_count").map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        foreground_execution_count: u32::try_from(
            row.try_get::<i64, _>("foreground_execution_count")
                .map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        pending_cancellation_count: u32::try_from(
            row.try_get::<i64, _>("pending_cancellation_count")
                .map_err(db)?,
        )
        .map_err(|_| DialogueStoreError::Corrupt)?,
        observed_cancellation_sequence: Some(crate::dialogue_codec::unsigned(
            row.try_get("observed_cancellation_sequence").map_err(db)?,
        )?),
        locally_completed_unconfirmed_count: row
            .try_get::<Option<i64>, _>("locally_completed_unconfirmed_count")
            .map_err(db)?
            .map(|v| u32::try_from(v).map_err(|_| DialogueStoreError::Corrupt))
            .transpose()?,
        unconfirmed_cancellation_count: row
            .try_get::<Option<i64>, _>("unconfirmed_cancellation_count")
            .map_err(db)?
            .map(|v| u32::try_from(v).map_err(|_| DialogueStoreError::Corrupt))
            .transpose()?,
        last_outcome: row
            .try_get::<Option<i16>, _>("last_outcome")
            .map_err(db)?
            .map(|v| codec::outcome(i32::from(v)))
            .transpose()
            .map_err(|_| DialogueStoreError::Corrupt)?,
        observed_at: UtcTimestamp::from_unix_millis(row.try_get("observed_at_ms").map_err(db)?),
    };
    value.validate()?;
    Ok(value)
}
