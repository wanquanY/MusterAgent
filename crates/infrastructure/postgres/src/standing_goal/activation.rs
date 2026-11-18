//! Draft activation commits the original Goal, Pursuit and source boundaries together.
use super::*;

impl PostgresStandingGoalProductStore {
    pub(super) async fn activate_with_pursuit_inner(
        &self,
        goal: StandingGoal,
        pursuit: GoalPursuit,
        expected_goal_revision: u64,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let outcome = activate(&mut tx, &goal, &pursuit, expected_goal_revision).await?;
        if outcome == StandingGoalMutationOutcome::Applied {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(outcome)
    }
}

pub(super) async fn activate(
    transaction: &mut sqlx::PgConnection,
    goal: &StandingGoal,
    pursuit: &GoalPursuit,
    expected_goal_revision: u64,
) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
    goal.validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    pursuit
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    if goal.status != StandingGoalStatus::Active
        || goal.active_pursuit_id != Some(pursuit.pursuit_id)
        || pursuit.goal_id != goal.goal_id
        || pursuit.tenant_id != goal.tenant_id
        || pursuit.revision != 1
    {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    let previous_goal = if let Some(source_run_id) = pursuit.active_run_id {
        assert_source_run(
            &mut *transaction,
            goal.tenant_id,
            goal.conversation_id,
            source_run_id,
        )
        .await?;
        None
    } else {
        // Match source controls/admission: account before Goal. Recheck the
        // current source owner inside the activation transaction.
        crate::owner_event_sources::lock_account_before_goal(&mut *transaction, goal).await?;
        admission::lock_goal(&mut *transaction, goal.tenant_id, goal.goal_id).await?;
        let draft = load_goal_tx(&mut *transaction, goal.tenant_id, goal.goal_id)
            .await?
            .ok_or(PostgresStandingGoalError::NotFound)?;
        if !owner_goal::independent_input_source_active(&mut *transaction, &draft).await? {
            return Ok(StandingGoalMutationOutcome::NotFound);
        }
        if draft.revision != expected_goal_revision {
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        let reason = pursuit
            .wait_barrier
            .as_ref()
            .ok_or(PostgresStandingGoalError::Corrupt)?
            .reason_ref
            .clone();
        if draft
            .activated_waiting_for_input(pursuit.pursuit_id, reason, goal.updated_at)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?
            != (goal.clone(), pursuit.clone())
        {
            return Err(PostgresStandingGoalError::Corrupt);
        }
        Some(draft)
    };
    let goal_rows = update_goal(&mut *transaction, goal, expected_goal_revision).await?;
    let pursuit_rows = if goal_rows == 1 {
        insert_pursuit(&mut *transaction, pursuit).await?
    } else {
        0
    };
    let outcome = if goal_rows == 1 && pursuit_rows == 1 {
        StandingGoalMutationOutcome::Applied
    } else if goal_exists(&mut *transaction, goal.tenant_id, goal.goal_id).await? {
        StandingGoalMutationOutcome::Conflict
    } else {
        StandingGoalMutationOutcome::NotFound
    };
    if outcome == StandingGoalMutationOutcome::Applied
        && let Some(previous) = &previous_goal
    {
        crate::owner_event_sources::goal_changed(transaction, Some(previous), goal).await?;
    }
    Ok(outcome)
}
