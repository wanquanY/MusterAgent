use super::*;

pub(super) async fn guard_creation(
    transaction: &mut sqlx::PgConnection,
    goal: &StandingGoal,
) -> Result<(), PostgresStandingGoalError> {
    // Serialize Goal creation with workspace-assignment preparation. Existing
    // Goal identities still take their ordinary read-only duplicate path.
    sqlx::query("SELECT 1 FROM conversation.conversations WHERE tenant_id=$1 AND conversation_id=$2 FOR SHARE")
        .bind(goal.tenant_id.into_uuid()).bind(goal.conversation_id.into_uuid())
        .fetch_optional(&mut *transaction).await?;
    if crate::workspace_assignment::admission_is_fenced(
        transaction,
        goal.tenant_id,
        goal.conversation_id,
    )
    .await?
    {
        let existing: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2)",
        )
        .bind(goal.tenant_id.into_uuid())
        .bind(goal.goal_id.into_uuid())
        .fetch_one(&mut *transaction)
        .await?;
        if !existing {
            return Err(PostgresStandingGoalError::AssignmentPending);
        }
    }
    Ok(())
}
