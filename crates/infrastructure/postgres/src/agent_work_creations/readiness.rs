//! One-time setup delivery, tied to the original creation and original message.
//! Caller owns account -> Goal locks and has finished all source mutations.
use musteragent_goal::{StandingGoal, StandingGoalStatus};
use musteragent_identity::*;
use musteragent_runtime_session::owner_event_sources::EventSourceStoreError as Error;
use sqlx::{PgConnection, Row};

pub(crate) async fn mark_ready(
    tx: &mut PgConnection,
    goal: &StandingGoal,
    _now: UtcTimestamp,
) -> Result<(), Error> {
    if goal.status != StandingGoalStatus::Active {
        return Ok(());
    }
    let row = sqlx::query(
        "SELECT d.creation_id,c.entry_id FROM agent.owner_goal_setup_deliveries d
        JOIN agent.agent_work_creation_publications c USING(creation_id)
        WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.agent_id=$3 AND c.work_kind=3 AND c.work_id=$4
          AND d.ready_at_ms IS NULL FOR UPDATE OF d",
    )
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.owner.into_uuid())
    .bind(goal.target_agent_id.into_uuid())
    .bind(goal.goal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    let Some(row) = row else {
        return Ok(());
    };
    let Some(proof) = crate::owner_event_sources::setup_delivery::proof(tx, goal).await? else {
        return Ok(());
    };
    let now = crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| Error::Infrastructure)?;
    sqlx::query("UPDATE agent.owner_goal_setup_deliveries SET ready_at_ms=$2,goal_revision=$3,source_boundary_proof=$4::jsonb
        WHERE creation_id=$1 AND ready_at_ms IS NULL")
        .bind(row.try_get::<uuid::Uuid,_>("creation_id").map_err(db)?).bind(now.unix_millis())
        .bind(i64::try_from(goal.revision).map_err(|_|Error::Corrupt)?).bind(proof.to_string())
        .execute(&mut *tx).await.map_err(db)?;
    let actor = Actor {
        tenant_id: goal.tenant_id,
        principal_id: goal.owner,
        principal_kind: PrincipalKind::User,
    };
    super::visibility::changed(
        tx,
        actor,
        goal.target_agent_id,
        row.try_get("entry_id").map_err(db)?,
        now,
    )
    .await
    .map_err(dialogue_store_error)
}

fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}
fn dialogue_store_error(error: musteragent_runtime_session::DialogueStoreError) -> Error {
    match error {
        musteragent_runtime_session::DialogueStoreError::Corrupt => Error::Corrupt,
        _ => Error::Infrastructure,
    }
}
