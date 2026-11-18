//! Exact original Goal/Pursuit CAS; policy retirement never supplies consent.
use super::*;
use musteragent_runtime_automation::StandingGoalReviewRetirement;
impl PostgresStandingGoalProductStore {
    pub(super) async fn list_completion_reviews_inner(
        &self,
        tenant: TenantId,
        limit: u16,
    ) -> Result<Vec<(GoalId, GoalPursuitId)>, PostgresStandingGoalError> {
        if limit == 0 || limit > 64 {
            return Err(PostgresStandingGoalError::Corrupt);
        }
        // The steady state is one indexed read, with no writer lease or
        // cursor mutation. Legacy candidates are rechecked in the writer below.
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT p.pursuit_id FROM runtime.goal_pursuits p JOIN runtime.standing_goals g ON g.tenant_id=p.tenant_id AND g.goal_id=p.goal_id AND g.active_pursuit_id=p.pursuit_id WHERE p.tenant_id=$1 AND p.status=3 AND p.wait_kind=5 AND g.status IN(2,3))")
            .bind(tenant.into_uuid()).fetch_one(&self.pool).await?;
        if !exists {
            return Ok(Vec::new());
        }
        // A committed per-tenant scan cursor advances even when one old
        // record fails CAS/validation, so other tenants and later pages progress.
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        sqlx::query("INSERT INTO runtime.goal_review_retirement_scans(tenant_id,anchor_pursuit_id) SELECT $1,candidate.pursuit_id FROM (SELECT p.pursuit_id FROM runtime.goal_pursuits p JOIN runtime.standing_goals g ON g.tenant_id=p.tenant_id AND g.goal_id=p.goal_id AND g.active_pursuit_id=p.pursuit_id WHERE p.tenant_id=$1 AND p.status=3 AND p.wait_kind=5 AND g.status IN(2,3) ORDER BY p.pursuit_id LIMIT 1 FOR KEY SHARE OF p) candidate WHERE true ON CONFLICT(tenant_id) DO NOTHING").bind(tenant.into_uuid()).execute(&mut *tx).await?;
        let retained:Option<Option<uuid::Uuid>>=sqlx::query_scalar("SELECT after_pursuit_id FROM runtime.goal_review_retirement_scans WHERE tenant_id=$1 FOR UPDATE").bind(tenant.into_uuid()).fetch_optional(&mut *tx).await?;
        let Some(after) = retained else {
            tx.commit().await?;
            return Ok(Vec::new());
        };
        let rows=sqlx::query("SELECT p.goal_id,p.pursuit_id FROM runtime.goal_pursuits p JOIN runtime.standing_goals g ON g.tenant_id=p.tenant_id AND g.goal_id=p.goal_id AND g.active_pursuit_id=p.pursuit_id WHERE p.tenant_id=$1 AND p.status=3 AND p.wait_kind=5 AND g.status IN(2,3) AND ($2::uuid IS NULL OR p.pursuit_id>$2) ORDER BY p.pursuit_id LIMIT $3").bind(tenant.into_uuid()).bind(after).bind(i64::from(limit)).fetch_all(&mut *tx).await?;
        let cursor: Option<uuid::Uuid> = if rows.len() == usize::from(limit) {
            Some(
                rows.last()
                    .ok_or(PostgresStandingGoalError::Corrupt)?
                    .try_get("pursuit_id")?,
            )
        } else {
            None
        };
        sqlx::query("UPDATE runtime.goal_review_retirement_scans SET after_pursuit_id=$2 WHERE tenant_id=$1").bind(tenant.into_uuid()).bind(cursor).execute(&mut *tx).await?;
        tx.commit().await?;
        rows.iter()
            .map(|row| {
                Ok((
                    GoalId::from_uuid(row.try_get("goal_id")?)
                        .map_err(|_| PostgresStandingGoalError::Corrupt)?,
                    GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
                        .map_err(|_| PostgresStandingGoalError::Corrupt)?,
                ))
            })
            .collect()
    }
    pub(super) async fn retire_completion_review_inner(
        &self,
        r: StandingGoalReviewRetirement,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        r.validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        crate::owner_event_sources::lock_account_before_goal(&mut tx, &r.previous_goal).await?;
        admission::lock_goal(&mut tx, r.goal.tenant_id, r.goal.goal_id).await?;
        let duplicate:Option<uuid::Uuid>=sqlx::query_scalar("SELECT evaluation_id FROM runtime.goal_result_review_retirements WHERE tenant_id=$1 AND prior_evaluation_id=$2").bind(r.goal.tenant_id.into_uuid()).bind(r.prior_evaluation.evaluation_id.into_uuid()).fetch_optional(&mut *tx).await?;
        if duplicate.is_some() {
            let existing = self
                .retirement_evaluation_on(&mut tx, r.goal.tenant_id, r.evaluation.evaluation_id)
                .await?;
            return Ok(if existing.as_ref() == Some(&r.evaluation) {
                StandingGoalMutationOutcome::Applied
            } else {
                StandingGoalMutationOutcome::Conflict
            });
        }
        let before = load_goal_tx(&mut tx, r.goal.tenant_id, r.goal.goal_id).await?;
        let previous = load_pursuit_tx(&mut tx, r.goal.tenant_id, r.pursuit.pursuit_id).await?;
        if before.as_ref() != Some(&r.previous_goal)
            || previous.as_ref() != Some(&r.previous_pursuit)
        {
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        let latest:Option<uuid::Uuid>=sqlx::query_scalar("SELECT evaluation_id FROM runtime.goal_evaluations WHERE tenant_id=$1 AND pursuit_id=$2 ORDER BY occurred_at_ms DESC,evaluation_id DESC LIMIT 1").bind(r.goal.tenant_id.into_uuid()).bind(r.pursuit.pursuit_id.into_uuid()).fetch_optional(&mut *tx).await?;
        if latest != Some(r.prior_evaluation.evaluation_id.into_uuid()) {
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        let original = self
            .retirement_evaluation_on(&mut tx, r.goal.tenant_id, r.prior_evaluation.evaluation_id)
            .await?;
        if original.as_ref() != Some(&r.prior_evaluation) {
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        let completion = rules::completion(&mut tx, &r.goal, r.evaluation.run_id).await?;
        r.evaluation
            .validate(&completion)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        if update_pursuit(&mut tx, &r.pursuit, r.previous_pursuit.revision).await? != 1
            || update_goal(&mut tx, &r.goal, r.previous_goal.revision).await? != 1
        {
            return Err(PostgresStandingGoalError::Corrupt);
        }
        insert_evaluation(&mut tx, &r.evaluation).await?;
        sqlx::query("INSERT INTO runtime.goal_result_review_retirements(tenant_id,goal_id,pursuit_id,prior_evaluation_id,evaluation_id,goal_revision,pursuit_revision,retired_at_ms,policy) VALUES($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8,'direct_goal_delivery@1')").bind(r.goal.tenant_id.into_uuid()).bind(r.goal.goal_id.into_uuid()).bind(r.pursuit.pursuit_id.into_uuid()).bind(r.prior_evaluation.evaluation_id.into_uuid()).bind(r.evaluation.evaluation_id.into_uuid()).bind(r.previous_goal.revision.to_string()).bind(r.previous_pursuit.revision.to_string()).bind(r.evaluation.occurred_at.unix_millis()).execute(&mut *tx).await?;
        crate::owner_event_sources::goal_changed(&mut tx, Some(&r.previous_goal), &r.goal).await?;
        sqlx::query("DELETE FROM runtime.goal_review_retirement_scans WHERE tenant_id=$1 AND NOT EXISTS(SELECT p.pursuit_id FROM runtime.goal_pursuits p JOIN runtime.standing_goals g ON g.tenant_id=p.tenant_id AND g.goal_id=p.goal_id AND g.active_pursuit_id=p.pursuit_id WHERE p.tenant_id=$1 AND p.status=3 AND p.wait_kind=5 AND g.status IN(2,3))")
            .bind(r.goal.tenant_id.into_uuid()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(StandingGoalMutationOutcome::Applied)
    }
    async fn retirement_evaluation_on(
        &self,
        tx: &mut sqlx::PgConnection,
        tenant: TenantId,
        evaluation: GoalEvaluationId,
    ) -> Result<Option<GoalEvaluation>, PostgresStandingGoalError> {
        let row = sqlx::query(&format!(
            "{EVALUATION_SELECT} WHERE evaluation.tenant_id=$1 AND evaluation.evaluation_id=$2"
        ))
        .bind(tenant.into_uuid())
        .bind(evaluation.into_uuid())
        .fetch_optional(&mut *tx)
        .await?;
        match row {
            Some(row) => Ok(Some(
                evaluation_decode::decode_evaluation_on(tx, &row).await?,
            )),
            None => Ok(None),
        }
    }
}
