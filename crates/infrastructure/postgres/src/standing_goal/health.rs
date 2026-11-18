//! Claim-fenced evaluation health. No provider text or credentials enter product state.
use super::*;
use musteragent_runtime_automation::{StandingGoalEvaluationFailure, StandingGoalEvaluationRetry};

impl PostgresStandingGoalProductStore {
    pub(super) async fn load_evaluation_retry_inner(
        &self,
        tenant_id: TenantId,
        pursuit_id: GoalPursuitId,
        run_id: RunId,
    ) -> Result<Option<StandingGoalEvaluationRetry>, PostgresStandingGoalError> {
        let row = sqlx::query("SELECT stage, retryable, runtime_code, correlation_id, first_failed_at_ms, last_failed_at_ms, retry_at_ms, attempts FROM runtime.goal_evaluation_health WHERE tenant_id = $1 AND pursuit_id = $2 AND run_id = $3")
            .bind(tenant_id.into_uuid()).bind(pursuit_id.into_uuid()).bind(run_id.into_uuid())
            .fetch_optional(&self.pool).await?;
        let evaluation_run_id =
            musteragent_runtime_automation::standing_goal_evaluation_run_id(pursuit_id, run_id)
                .ok_or(PostgresStandingGoalError::Corrupt)?;
        let recovery = if row.is_some() {
            sqlx::query("SELECT next_attempt_number FROM runtime.runs WHERE tenant_id = $1 AND run_id = $2 AND activity_kind = 6 AND status = 3 AND wait_reason = 10 AND NOT EXISTS (SELECT 1 FROM runtime.leases WHERE leases.run_id = runs.run_id) AND wait_error_code IS NOT NULL")
                .bind(tenant_id.into_uuid()).bind(evaluation_run_id.into_uuid())
                .fetch_optional(&self.pool).await?
        } else {
            None
        };
        let recovery_attempt = recovery
            .map(|value| {
                let next = u64::try_from(value.try_get::<i32, _>("next_attempt_number")?)
                    .map_err(|_| PostgresStandingGoalError::Corrupt)?;
                u32::try_from(next.saturating_sub(1))
                    .map_err(|_| PostgresStandingGoalError::Corrupt)
            })
            .transpose()?;
        row.map(|row| {
            Ok(StandingGoalEvaluationRetry {
                recovery_run_id: recovery_attempt.map(|_| evaluation_run_id),
                recovery_attempt,
                failure: StandingGoalEvaluationFailure {
                    stage: row.try_get("stage")?,
                    retryable: row.try_get("retryable")?,
                    runtime_code: row.try_get("runtime_code")?,
                    correlation_id: row.try_get("correlation_id")?,
                    occurred_at: UtcTimestamp::from_unix_millis(row.try_get("last_failed_at_ms")?),
                },
                first_failed_at: UtcTimestamp::from_unix_millis(row.try_get("first_failed_at_ms")?),
                retry_at: UtcTimestamp::from_unix_millis(row.try_get("retry_at_ms")?),
                attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)
                    .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            })
        })
        .transpose()
    }

    pub(super) async fn defer_terminal_evaluation_inner(
        &self,
        request: DeferStandingGoalEvaluation,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let updated = sqlx::query("UPDATE runtime.goal_evaluation_claims SET claim_lease_expires_at_ms = $1 WHERE tenant_id = $2 AND pursuit_id = $3 AND run_id = $4 AND claim_token = $5")
            .bind(request.retry_at.unix_millis()).bind(request.tenant_id.into_uuid())
            .bind(request.pursuit_id.into_uuid()).bind(request.run_id.into_uuid()).bind(&request.claim_token)
            .execute(&mut *transaction).await?.rows_affected();
        if updated != 1 {
            transaction.rollback().await?;
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        if let Some(failure) = request.failure {
            sqlx::query("INSERT INTO runtime.goal_evaluation_health(tenant_id, pursuit_id, run_id, stage, retryable, runtime_code, correlation_id, first_failed_at_ms, last_failed_at_ms, retry_at_ms, attempts)
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)
                ON CONFLICT(tenant_id, pursuit_id, run_id) DO UPDATE SET
                    stage = excluded.stage, retryable = excluded.retryable, runtime_code = excluded.runtime_code,
                    correlation_id = excluded.correlation_id, last_failed_at_ms = excluded.last_failed_at_ms,
                    retry_at_ms = excluded.retry_at_ms, attempts = LEAST(runtime.goal_evaluation_health.attempts + 1, 4294967295)")
                .bind(request.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid()).bind(request.run_id.into_uuid())
                .bind(failure.stage).bind(failure.retryable).bind(failure.runtime_code).bind(failure.correlation_id)
                .bind(failure.occurred_at.unix_millis()).bind(failure.occurred_at.unix_millis()).bind(request.retry_at.unix_millis())
                .execute(&mut *transaction).await?;
        } else {
            // Pending proves Evaluation admission succeeded; retire stale failure feedback.
            sqlx::query("DELETE FROM runtime.goal_evaluation_health WHERE tenant_id = $1 AND pursuit_id = $2 AND run_id = $3")
                .bind(request.tenant_id.into_uuid()).bind(request.pursuit_id.into_uuid()).bind(request.run_id.into_uuid())
                .execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(StandingGoalMutationOutcome::Applied)
    }
}
