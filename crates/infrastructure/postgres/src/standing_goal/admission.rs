//! Goal -> Pursuit -> evaluation claim lock order matches all product writers.
//! Settlement shares the original Ledger/Run/grant/outbox transaction.
use super::*;
use crate::admission::PostgresAdmissionError;
use musteragent_ledger::{StandingGoalAdmissionBatch, StandingGoalAdmissionContext};

pub(crate) async fn lock_goal(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
) -> Result<(), PostgresStandingGoalError> {
    sqlx::query("SELECT goal_id FROM runtime.standing_goals WHERE tenant_id = $1 AND goal_id = $2 FOR UPDATE")
        .bind(tenant_id.into_uuid()).bind(goal_id.into_uuid())
        .fetch_optional(&mut *transaction).await?;
    Ok(())
}

pub(crate) async fn settle(
    transaction: &mut sqlx::PgConnection,
    batch: &StandingGoalAdmissionBatch,
) -> Result<bool, PostgresAdmissionError> {
    let source_run_id = batch.source.parent_run_id();
    let dialogue = matches!(
        batch.source,
        musteragent_ledger::StandingGoalAdmissionSource::OwnerGoal { .. }
    );
    if dialogue {
        crate::dialogue_handoff::progress::lock_account(transaction, batch.actor)
            .await
            .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?;
    }
    lock_goal(transaction, batch.actor.tenant_id, batch.goal_id)
        .await
        .map_err(storage_error)?;
    // Claim transfer also locks this exact Pursuit; the signed external grant
    // cannot retain a claim after a newer worker acquires it.
    sqlx::query("SELECT pursuit_id FROM runtime.goal_pursuits WHERE tenant_id = $1 AND pursuit_id = $2 FOR UPDATE")
        .bind(batch.actor.tenant_id.into_uuid()).bind(batch.pursuit_id.into_uuid())
        .fetch_optional(&mut *transaction).await?;
    let now = crate::ledger::database_now_ms(transaction).await?;
    let goal_current: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM runtime.standing_goals AS goal
         JOIN runtime.goal_pursuits AS pursuit
           ON pursuit.tenant_id = goal.tenant_id AND pursuit.goal_id = goal.goal_id
         WHERE goal.tenant_id = $1 AND goal.goal_id = $2 AND goal.owner_principal_id = $3
           AND goal.conversation_id = $4 AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2
             AND (goal.input_accepted_seq > goal.input_consumed_seq OR pursuit.status = 2))) AND goal.revision = $5::numeric
           AND (goal.active_pursuit_id = $6 OR goal.status = 3) AND pursuit.pursuit_id = $6
           AND pursuit.revision = $7::numeric)",
    )
    .bind(batch.actor.tenant_id.into_uuid())
    .bind(batch.goal_id.into_uuid())
    .bind(batch.actor.principal_id.into_uuid())
    .bind(batch.append.conversation_id.into_uuid())
    .bind(batch.fence.goal_revision.to_string())
    .bind(batch.pursuit_id.into_uuid())
    .bind(batch.fence.pursuit_revision.to_string())
    .fetch_one(&mut *transaction)
    .await?;
    if !goal_current {
        return Ok(false);
    }
    let continuation = matches!(
        batch.context,
        StandingGoalAdmissionContext::Continuation { .. }
    );
    let owns_claim = if continuation {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM runtime.goal_pursuits
             WHERE tenant_id = $1 AND pursuit_id = $2 AND status = 1
               AND continuation_source_run_id IS NOT DISTINCT FROM $3 AND claim_token = $4
               AND claim_lease_expires_at_ms > $5 AND continuation_due_at_ms <= $5
               AND active_run_id IS NULL)",
        )
        .bind(batch.actor.tenant_id.into_uuid())
        .bind(batch.pursuit_id.into_uuid())
        .bind(source_run_id.map(RunId::into_uuid))
        .bind(&batch.fence.claim_token)
        .bind(now.unix_millis())
        .fetch_one(&mut *transaction)
        .await?
    } else {
        sqlx::query(
            "SELECT claim.pursuit_id FROM runtime.goal_evaluation_claims AS claim
             JOIN runtime.goal_pursuits AS pursuit
               ON pursuit.tenant_id = claim.tenant_id AND pursuit.pursuit_id = claim.pursuit_id
             WHERE pursuit.tenant_id = $1 AND pursuit.pursuit_id = $2 AND pursuit.status = 2
               AND pursuit.active_run_id = $3 AND claim.run_id = $3 AND claim.claim_token = $4
               AND claim.claim_lease_expires_at_ms > $5 AND claim.claimed_at_ms <= $5
             FOR UPDATE OF claim",
        )
        .bind(batch.actor.tenant_id.into_uuid())
        .bind(batch.pursuit_id.into_uuid())
        .bind(source_run_id.map(RunId::into_uuid))
        .bind(&batch.fence.claim_token)
        .bind(now.unix_millis())
        .fetch_optional(&mut *transaction)
        .await?
        .is_some()
    };
    if !owns_claim {
        return Ok(false);
    }
    if let StandingGoalAdmissionContext::Continuation {
        input_refs,
        objective_ref,
        ..
    } = &batch.context
    {
        let goal = load_goal_tx(transaction, batch.actor.tenant_id, batch.goal_id)
            .await
            .map_err(storage_error)?
            .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
        if !super::input::bind_run(
            transaction,
            &goal,
            source_run_id,
            batch.run.context.run_id,
            input_refs,
            objective_ref,
        )
        .await
        .map_err(storage_error)?
        {
            return Ok(false);
        }
    }
    if dialogue {
        super::owner_goal::admission::project_run(transaction, batch).await?;
    }
    if continuation {
        let pursuit = load_pursuit_tx(transaction, batch.actor.tenant_id, batch.pursuit_id)
            .await
            .map_err(storage_error)?
            .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
        let admitted = if dialogue {
            let goal = load_goal_tx(transaction, batch.actor.tenant_id, batch.goal_id)
                .await
                .map_err(storage_error)?
                .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
            pursuit.admitted_input_run(&goal, batch.run.context.run_id, now)
        } else {
            pursuit.admitted_continuation(batch.run.context.run_id, now)
        }
        .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?;
        if update_pursuit(transaction, &admitted, batch.fence.pursuit_revision)
            .await
            .map_err(storage_error)?
            != 1
        {
            return Ok(false);
        }
    }
    if let Some(authority) = &batch.continuation_authority {
        let hash = Sha256::digest(authority).into_bytes().to_vec();
        let updated = sqlx::query(
            "UPDATE runtime.goal_pursuits SET continuation_authority = $1, continuation_authority_hash = $2
             WHERE tenant_id = $3 AND pursuit_id = $4
               AND ($5 OR continuation_authority IS NULL OR continuation_authority_hash = $2)",
        )
        .bind(authority).bind(hash).bind(batch.actor.tenant_id.into_uuid())
        .bind(batch.pursuit_id.into_uuid()).bind(continuation)
        .execute(&mut *transaction).await?.rows_affected();
        if updated != 1 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Dialogue execution uses its existing private source owner. The public-chat
/// admission gate cannot authorize these service participants or Agent identities.
pub(crate) async fn validate_dialogue_source(
    transaction: &mut sqlx::PgConnection,
    batch: &StandingGoalAdmissionBatch,
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
) -> Result<bool, PostgresAdmissionError> {
    super::owner_goal::admission::validate(transaction, batch, snapshot).await
}

pub(super) fn storage_error(error: PostgresStandingGoalError) -> PostgresAdmissionError {
    match error {
        PostgresStandingGoalError::DependencyUnavailable => {
            PostgresAdmissionError::GoalSourceUnavailable
        }
        PostgresStandingGoalError::Database(error) => PostgresAdmissionError::Database(error),
        PostgresStandingGoalError::Transaction(error) => PostgresAdmissionError::Transaction(error),
        PostgresStandingGoalError::AssignmentPending => {
            PostgresAdmissionError::WorkspaceAssignmentPending
        }
        PostgresStandingGoalError::NotFound | PostgresStandingGoalError::Corrupt => {
            PostgresAdmissionError::StandingGoalInvariant
        }
    }
}
