//! Explicit visibility disposition for an existing Agent-created Draft.
//! This owner changes no Goal definition, readiness proof, source or Pursuit.
use super::*;
use musteragent_goal::{StandingGoal, StandingGoalStatus};
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    #[serde(deserialize_with = "planning::goal_id")]
    pub goal_id: GoalId,
    pub expected_revision: u64,
}
pub(crate) struct Prepared {
    goal_id: GoalId,
    expected_revision: u64,
    saved_revision: u64,
    creation_id: uuid::Uuid,
}
impl Prepared {
    pub fn result(&self) -> Value {
        // Return the immutable visibility registration, including on later commands.
        // Current definition changes have their own Goal owner receipts.
        json!({"goal_id":self.goal_id.to_string(), "saved_revision":self.saved_revision,
            "saved_status":"draft", "registration_receipt_id":self.creation_id,
            "saved":true, "monitoring_started":false})
    }
}
pub(crate) async fn prepare(
    database: &PostgresDatabaseRuntime,
    goal: &StandingGoal,
    expected_revision: u64,
) -> Result<Prepared, Error> {
    if !(1..=9_007_199_254_740_991).contains(&expected_revision) {
        return Err(Error::InvalidArguments);
    }
    if goal.revision != expected_revision || goal.status != StandingGoalStatus::Draft {
        return Err(Error::Conflict);
    }
    let mut tx = database
        .read_unit_of_work(
            musteragent_telemetry::DbCommitKind::Projection,
            crate::PostgresTransactionIsolation::RepeatableRead,
        )
        .await
        .map_err(|_| Error::Infrastructure)?;
    let (creation_id, saved_revision) = sqlx::query_as::<_, (uuid::Uuid,Option<i64>)>(
        "SELECT c.creation_id,r.goal_revision FROM agent.agent_work_creation_publications c
         JOIN agent.owner_goal_setup_deliveries d USING(creation_id)
         LEFT JOIN agent.owner_goal_draft_registrations r USING(creation_id)
         WHERE c.tenant_id=$1 AND c.owner_id=$2 AND c.agent_id=$3 AND c.work_kind=3 AND c.work_id=$4")
        .bind(goal.tenant_id.into_uuid()).bind(goal.owner.into_uuid()).bind(goal.target_agent_id.into_uuid())
        .bind(goal.goal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(|_|Error::Infrastructure)?
        .ok_or(Error::Forbidden)?;
    tx.commit().await.map_err(|_| Error::Infrastructure)?;
    Ok(Prepared {
        goal_id: goal.goal_id,
        expected_revision,
        saved_revision: registration_revision(saved_revision, expected_revision)?,
        creation_id,
    })
}
pub(crate) async fn commit(
    tx: &mut sqlx::PgConnection,
    scope: planning::GoalToolScope,
    plan: Prepared,
    custody: effect::Custody,
) -> Result<(), Error> {
    if scope.own_goal.is_some() {
        return Err(Error::Forbidden);
    }
    crate::dialogue_handoff::progress::lock_account(tx, scope.actor)
        .await
        .map_err(store_error)?;
    // Account -> Goal order matches the original source owner. Serialize racing
    // saves and recheck revision after independent reads and content staging.
    let row = sqlx::query("SELECT g.revision::text AS revision,g.status,c.entry_id FROM runtime.standing_goals g
        JOIN agent.agent_work_creation_publications c ON c.tenant_id=g.tenant_id AND c.work_id=g.goal_id
          AND c.owner_id=g.owner_principal_id AND c.agent_id=g.target_agent_id AND c.work_kind=3
        JOIN agent.owner_goal_setup_deliveries d USING(creation_id)
        WHERE g.tenant_id=$1 AND g.owner_principal_id=$2 AND g.target_agent_id=$3 AND g.goal_id=$4
          AND c.creation_id=$5 AND g.mode=2
          AND NOT EXISTS(SELECT 1 FROM runtime.goal_deletions e WHERE e.tenant_id=g.tenant_id AND e.goal_id=g.goal_id)
        FOR UPDATE OF g")
        .bind(scope.actor.tenant_id.into_uuid()).bind(scope.actor.principal_id.into_uuid()).bind(scope.agent_id.into_uuid())
        .bind(plan.goal_id.into_uuid()).bind(plan.creation_id).fetch_optional(&mut *tx).await.map_err(|_|Error::Infrastructure)?
        .ok_or(Error::Forbidden)?;
    if row
        .try_get::<String, _>("revision")
        .map_err(|_| Error::Corrupt)?
        .parse::<u64>()
        .map_err(|_| Error::Corrupt)?
        != plan.expected_revision
        || row
            .try_get::<i16, _>("status")
            .map_err(|_| Error::Corrupt)?
            != 1
    {
        return Err(Error::Conflict);
    }
    let registered = sqlx::query_scalar::<_, i64>(
        "SELECT goal_revision FROM agent.owner_goal_draft_registrations WHERE creation_id=$1",
    )
    .bind(plan.creation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|_| Error::Infrastructure)?;
    if registration_revision(registered, plan.expected_revision)? != plan.saved_revision {
        // A staged result must not acknowledge a different registration after
        // the transport reacquires its original commit lane/custody.
        return Err(Error::Conflict);
    }
    let now = crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| Error::Infrastructure)?;
    let inserted = sqlx::query("INSERT INTO agent.owner_goal_draft_registrations
        (creation_id,goal_revision,registered_at_ms,source_run_id,source_invocation_id,device_source_invocation_id,execution_home)
        VALUES($1,$2,$3,$4,CASE WHEN $6 THEN NULL ELSE $5 END,CASE WHEN $6 THEN $5 ELSE NULL END,
          CASE WHEN $6 THEN 'device' ELSE 'cloud' END) ON CONFLICT(creation_id) DO NOTHING")
        .bind(plan.creation_id).bind(i64::try_from(plan.expected_revision).map_err(|_|Error::InvalidArguments)?)
        .bind(now.unix_millis()).bind(custody.run_id.into_uuid()).bind(custody.invocation_id.into_uuid()).bind(custody.device)
        .execute(&mut *tx).await.map_err(|_|Error::Infrastructure)?.rows_affected();
    if inserted == 1 {
        crate::agent_work_creations::visibility::changed(
            tx,
            scope.actor,
            scope.agent_id,
            row.try_get("entry_id").map_err(|_| Error::Corrupt)?,
            now,
        )
        .await
        .map_err(store_error)?;
    }
    Ok(())
}

fn registration_revision(retained: Option<i64>, current: u64) -> Result<u64, Error> {
    match retained {
        None => Ok(current),
        Some(value) => u64::try_from(value)
            .ok()
            .filter(|v| *v > 0 && *v <= current)
            .ok_or(Error::Corrupt),
    }
}

/// The caller has already resolved an authentic direct-user intent through
/// the original Cloud/Device source owner. Selected-task followups have a
/// narrower purpose and cannot register an unrelated standing draft.
pub(crate) fn require_direct_input(
    verified_direct_user_root: bool,
    selected_task: bool,
) -> Result<(), Error> {
    if !verified_direct_user_root || selected_task {
        Err(Error::Forbidden)
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn later_definition_revision_does_not_relabel_the_original_saved_receipt() {
        assert_eq!(registration_revision(Some(1), 2).unwrap(), 1);
        assert_eq!(registration_revision(None, 2).unwrap(), 2);
        assert!(registration_revision(Some(3), 2).is_err());
    }
    #[test]
    fn selected_task_followups_and_scheduled_inputs_cannot_save_drafts() {
        assert!(require_direct_input(true, false).is_ok());
        for (verified, selected) in [(false, false), (false, true), (true, true)] {
            assert!(matches!(
                require_direct_input(verified, selected),
                Err(Error::Forbidden)
            ));
        }
    }
}
