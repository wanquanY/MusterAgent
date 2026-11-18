//! Goal lock, draft CAS and first Pursuit share the original content admission
//! Unit of Work with the message, signed Snapshot, grant and dispatch outbox.
use super::*;
use crate::admission::PostgresAdmissionError;
use musteragent_ledger::AdmissionBatch;
use musteragent_runtime_automation::{GoalStartRejection, StandingGoalStart};

pub(crate) async fn stage(
    transaction: &mut sqlx::PgConnection,
    batch: &AdmissionBatch,
    start: StandingGoalStart,
) -> Result<(), PostgresAdmissionError> {
    admission::lock_goal(transaction, batch.actor.tenant_id, start.goal_id)
        .await
        .map_err(storage_error)?;
    let draft = load_goal_tx(transaction, batch.actor.tenant_id, start.goal_id)
        .await
        .map_err(storage_error)?
        .ok_or(PostgresAdmissionError::GoalStart(
            GoalStartRejection::Unavailable,
        ))?;
    if draft.owner != batch.actor.principal_id
        || draft.conversation_id != batch.append.conversation_id
        || draft.target_agent_id != batch.runtime_snapshot.memory_agent_id
    {
        return Err(PostgresAdmissionError::GoalStart(
            GoalStartRejection::Unavailable,
        ));
    }
    let prior_pursuit: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM runtime.goal_pursuits WHERE tenant_id = $1 AND goal_id = $2)",
    )
    .bind(draft.tenant_id.into_uuid())
    .bind(draft.goal_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await?;
    if draft.revision != start.expected_goal_revision
        || draft.status != StandingGoalStatus::Draft
        || prior_pursuit
    {
        return Err(PostgresAdmissionError::GoalStart(
            GoalStartRejection::Conflict,
        ));
    }
    let (goal, pursuit) = draft
        .admitted_first_run(
            start.pursuit_id,
            batch.run.context.run_id,
            batch.run.admitted_at,
        )
        .map_err(|_| PostgresAdmissionError::GoalStart(GoalStartRejection::Invalid))?;
    assert_source_run(
        transaction,
        goal.tenant_id,
        goal.conversation_id,
        batch.run.context.run_id,
    )
    .await
    .map_err(storage_error)?;
    if update_goal(transaction, &goal, start.expected_goal_revision)
        .await
        .map_err(storage_error)?
        != 1
        || insert_pursuit(transaction, &pursuit)
            .await
            .map_err(storage_error)?
            != 1
    {
        return Err(PostgresAdmissionError::GoalStart(
            GoalStartRejection::Conflict,
        ));
    }
    Ok(())
}

fn storage_error(error: PostgresStandingGoalError) -> PostgresAdmissionError {
    match error {
        PostgresStandingGoalError::DependencyUnavailable => {
            PostgresAdmissionError::GoalSourceUnavailable
        }
        PostgresStandingGoalError::Database(error) => error.into(),
        PostgresStandingGoalError::Transaction(error) => error.into(),
        PostgresStandingGoalError::AssignmentPending => {
            PostgresAdmissionError::WorkspaceAssignmentPending
        }
        PostgresStandingGoalError::NotFound | PostgresStandingGoalError::Corrupt => {
            PostgresAdmissionError::StandingGoalInvariant
        }
    }
}
