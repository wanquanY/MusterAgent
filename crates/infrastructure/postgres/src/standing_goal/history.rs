//! Owner-scoped deletion receipts; no objective, credential or message duplication.
use super::*;
use musteragent_runtime_automation::StandingGoalDeletion;

impl PostgresStandingGoalProductStore {
    pub(super) async fn load_deletion_inner(
        &self,
        tenant_id: TenantId,
        goal_id: GoalId,
        principal_id: PrincipalId,
        conversation_id: ConversationId,
    ) -> Result<Option<StandingGoalDeletion>, PostgresStandingGoalError> {
        let row = sqlx::query("SELECT created_at_ms, deleted_at_ms FROM runtime.goal_deletions WHERE tenant_id = $1 AND goal_id = $2 AND owner_principal_id = $3 AND conversation_id = $4")
            .bind(tenant_id.into_uuid()).bind(goal_id.into_uuid()).bind(principal_id.into_uuid()).bind(conversation_id.into_uuid())
            .fetch_optional(&self.pool).await?;
        row.map(|row| {
            Ok(StandingGoalDeletion {
                created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
                deleted_at: UtcTimestamp::from_unix_millis(row.try_get("deleted_at_ms")?),
            })
        })
        .transpose()
    }
    pub(super) async fn delete_inner(
        &self,
        request: DeleteStandingGoal,
    ) -> Result<StandingGoalMutationOutcome, PostgresStandingGoalError> {
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let Some(initial) =
            load_goal_tx(&mut transaction, request.tenant_id, request.goal_id).await?
        else {
            return Ok(StandingGoalMutationOutcome::NotFound);
        };
        crate::owner_event_sources::lock_account_before_goal(&mut transaction, &initial).await?;
        admission::lock_goal(&mut transaction, request.tenant_id, request.goal_id).await?;
        let Some(mut current) =
            load_goal_tx(&mut transaction, request.tenant_id, request.goal_id).await?
        else {
            return Ok(StandingGoalMutationOutcome::NotFound);
        };
        if current.owner != request.principal_id
            || current.revision != request.expected_revision
            || current.status == StandingGoalStatus::Active
        {
            return Ok(StandingGoalMutationOutcome::Conflict);
        }
        if owner_goal::load_binding(&mut transaction, &current)
            .await?
            .is_some()
        {
            // A Dialogue execution session and its published findings retain
            // their exact original Goal/Run foreign keys. Delete the product
            // entry using the existing deletion receipt, not those facts.
            current.revision = current
                .revision
                .checked_add(1)
                .ok_or(PostgresStandingGoalError::Corrupt)?;
            if !current.status.terminal() {
                current.status = StandingGoalStatus::Cancelled;
            }
            current.active_pursuit_id = None;
            let now: i64 = sqlx::query_scalar(
                "SELECT (extract(epoch FROM transaction_timestamp())*1000)::bigint",
            )
            .fetch_one(&mut *transaction)
            .await?;
            current.updated_at = current.updated_at.max(UtcTimestamp::from_unix_millis(now));
            current
                .validate()
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let rows = update_goal(&mut transaction, &current, request.expected_revision).await?;
            if rows != 1 {
                return Err(PostgresStandingGoalError::Corrupt);
            }
            crate::owner_event_sources::goal_deleting(&mut transaction, &current)
                .await
                .map_err(PostgresStandingGoalError::from)?;
            sqlx::query("INSERT INTO runtime.goal_deletions(tenant_id,goal_id,owner_principal_id,conversation_id,created_at_ms,deleted_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
                .bind(current.tenant_id.into_uuid()).bind(current.goal_id.into_uuid()).bind(current.owner.into_uuid())
                .bind(current.conversation_id.into_uuid()).bind(current.created_at.unix_millis()).bind(now)
                .execute(&mut *transaction).await?;
            transaction.commit().await?;
            return Ok(StandingGoalMutationOutcome::Applied);
        }
        let deleted = sqlx::query(
            "DELETE FROM runtime.standing_goals
             WHERE tenant_id = $1 AND goal_id = $2 AND owner_principal_id = $3
               AND revision = $4::numeric AND status <> 2
             RETURNING conversation_id, created_at_ms",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.goal_id.into_uuid())
        .bind(request.principal_id.into_uuid())
        .bind(request.expected_revision.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let rows = u64::from(deleted.is_some());
        if let Some(row) = deleted {
            sqlx::query("INSERT INTO runtime.goal_deletions(tenant_id, goal_id, owner_principal_id, conversation_id, created_at_ms, deleted_at_ms)
                VALUES($1, $2, $3, $4, $5, CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT))")
                .bind(request.tenant_id.into_uuid()).bind(request.goal_id.into_uuid())
                .bind(request.principal_id.into_uuid()).bind(row.try_get::<uuid::Uuid, _>("conversation_id")?)
                .bind(row.try_get::<i64, _>("created_at_ms")?).execute(&mut *transaction).await?;
        }
        let outcome =
            mutation_outcome(&mut transaction, request.tenant_id, request.goal_id, rows).await?;
        if rows == 1 {
            transaction.commit().await?;
        } else {
            transaction.rollback().await?;
        }
        Ok(outcome)
    }
}
