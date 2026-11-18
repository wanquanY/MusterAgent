//! Original Goal persistence, reusable inside a Tool's atomic receipt transaction.
//! Callers must roll back any outcome other than Applied.
use super::*;
use musteragent_runtime_automation::{PreparedStandingGoalChange, StandingGoalChangeMutation};
use sqlx::PgConnection;

pub(super) async fn replace(
    tx: &mut PgConnection,
    goal: &StandingGoal,
    expected: u64,
) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
    goal.validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    crate::owner_event_sources::lock_account_before_goal(tx, goal).await?;
    admission::lock_goal(tx, goal.tenant_id, goal.goal_id).await?;
    let previous = load_goal_tx(tx, goal.tenant_id, goal.goal_id).await?;
    let rows = update_goal(tx, goal, expected).await?;
    let outcome = mutation_outcome(tx, goal.tenant_id, goal.goal_id, rows).await?;
    if rows == 1 {
        crate::owner_event_sources::goal_changed(tx, previous.as_ref(), goal).await?;
    }
    Ok(outcome)
}

pub(super) async fn replace_pair(
    tx: &mut PgConnection,
    goal: &StandingGoal,
    pursuit: &GoalPursuit,
    expected_goal: u64,
    expected_pursuit: u64,
) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
    goal.validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    pursuit
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    crate::owner_event_sources::lock_account_before_goal(tx, goal).await?;
    admission::lock_goal(tx, goal.tenant_id, goal.goal_id).await?;
    let previous = load_goal_tx(tx, goal.tenant_id, goal.goal_id).await?;
    for run in [pursuit.active_run_id, pursuit.continuation_source_run_id]
        .into_iter()
        .flatten()
    {
        assert_source_run(tx, goal.tenant_id, goal.conversation_id, run).await?;
    }
    let pursuit_rows = update_pursuit(tx, pursuit, expected_pursuit).await?;
    let goal_rows = if pursuit_rows == 1 {
        update_goal(tx, goal, expected_goal).await?
    } else {
        0
    };
    let outcome = if pursuit_rows == 1 && goal_rows == 1 {
        StandingGoalMutationOutcome::Applied
    } else if goal_exists(tx, goal.tenant_id, goal.goal_id).await? {
        StandingGoalMutationOutcome::Conflict
    } else {
        StandingGoalMutationOutcome::NotFound
    };
    if outcome == StandingGoalMutationOutcome::Applied {
        crate::owner_event_sources::goal_changed(tx, previous.as_ref(), goal).await?;
    }
    Ok(outcome)
}

pub(crate) async fn commit(
    tx: &mut PgConnection,
    change: &PreparedStandingGoalChange,
) -> Result<StandingGoalMutationOutcome, StandingGoalStoreError> {
    commit_inner(tx, change).await.map_err(Into::into)
}

async fn commit_inner(
    tx: &mut PgConnection,
    change: &PreparedStandingGoalChange,
) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
    let goal = &change.inspection.goal;
    let pursuit = || {
        change
            .inspection
            .pursuit
            .as_ref()
            .ok_or(PostgresStandingGoalError::Corrupt)
    };
    match change.mutation {
        StandingGoalChangeMutation::Replace { goal_revision } => {
            replace(tx, goal, goal_revision).await
        }
        StandingGoalChangeMutation::Activate { goal_revision } => {
            activation::activate(tx, goal, pursuit()?, goal_revision).await
        }
        StandingGoalChangeMutation::ReplacePair {
            goal_revision,
            pursuit_revision,
        } => replace_pair(tx, goal, pursuit()?, goal_revision, pursuit_revision).await,
        StandingGoalChangeMutation::Unchanged {
            goal_revision,
            pursuit_revision,
        } => {
            crate::owner_event_sources::lock_account_before_goal(tx, goal).await?;
            admission::lock_goal(tx, goal.tenant_id, goal.goal_id).await?;
            let Some(current) = load_goal_tx(tx, goal.tenant_id, goal.goal_id).await? else {
                return Ok(StandingGoalMutationOutcome::NotFound);
            };
            if current != *goal || current.revision != goal_revision {
                return Ok(StandingGoalMutationOutcome::Conflict);
            }
            if let Some(p) = change.inspection.pursuit.as_ref() {
                let observed: Option<String> = sqlx::query_scalar("SELECT revision::text FROM runtime.goal_pursuits WHERE tenant_id=$1 AND pursuit_id=$2 FOR UPDATE")
                    .bind(goal.tenant_id.into_uuid()).bind(p.pursuit_id.into_uuid()).fetch_optional(tx).await?;
                if observed != pursuit_revision.map(|v| v.to_string()) {
                    return Ok(StandingGoalMutationOutcome::Conflict);
                }
            } else if pursuit_revision.is_some() {
                return Err(PostgresStandingGoalError::Corrupt);
            }
            Ok(StandingGoalMutationOutcome::Applied)
        }
    }
}
