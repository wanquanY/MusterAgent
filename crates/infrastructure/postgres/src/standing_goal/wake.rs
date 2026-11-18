//! Durable wake-up projection for Standing Goal waits.

use musteragent_goal::{GoalPursuitStatus, GoalWaitKind};
use musteragent_identity::{GoalId, GoalPursuitId, TenantId, UtcTimestamp};
use musteragent_runtime_automation::{
    RecordStandingGoalWakeSignal, StandingGoalWakeSignalDisposition, WakeDueStandingGoalWaits,
    WakeEventStandingGoalWaits,
};
use sqlx::Row as _;

use super::{
    PostgresStandingGoalError, PostgresStandingGoalProductStore, load_goal_tx, load_pursuit_tx,
    update_goal, update_pursuit,
};

pub(super) async fn record_signal(
    store: &PostgresStandingGoalProductStore,
    request: RecordStandingGoalWakeSignal,
) -> Result<StandingGoalWakeSignalDisposition, PostgresStandingGoalError> {
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
        .await?;
    let rows = sqlx::query(
        "INSERT INTO runtime.goal_wake_signals (
             tenant_id, signal_id, correlation_key, occurred_at_ms
         ) VALUES ($1, $2, $3, $4)
         ON CONFLICT (tenant_id, signal_id) DO NOTHING",
    )
    .bind(request.tenant_id.into_uuid())
    .bind(&request.signal_id)
    .bind(&request.correlation_key)
    .bind(request.occurred_at.unix_millis())
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    let disposition = if rows == 1 {
        StandingGoalWakeSignalDisposition::Recorded
    } else {
        let existing = sqlx::query(
            "SELECT correlation_key, occurred_at_ms
             FROM runtime.goal_wake_signals
             WHERE tenant_id = $1 AND signal_id = $2",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(&request.signal_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresStandingGoalError::Corrupt)?;
        if existing.try_get::<String, _>("correlation_key")? != request.correlation_key
            || existing.try_get::<i64, _>("occurred_at_ms")? != request.occurred_at.unix_millis()
        {
            return Err(PostgresStandingGoalError::Corrupt);
        }
        StandingGoalWakeSignalDisposition::Existing
    };
    transaction.commit().await?;
    Ok(disposition)
}

pub(super) async fn due(
    store: &PostgresStandingGoalProductStore,
    request: WakeDueStandingGoalWaits,
) -> Result<u16, PostgresStandingGoalError> {
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
        .await?;
    let rows = sqlx::query(
        "SELECT pursuit.pursuit_id, pursuit.goal_id
         FROM runtime.goal_pursuits AS pursuit
         JOIN runtime.standing_goals AS goal
           ON goal.tenant_id = pursuit.tenant_id AND goal.goal_id = pursuit.goal_id
         WHERE pursuit.tenant_id = $1 AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3
           AND pursuit.wait_kind = 4
           AND pursuit.wait_resume_not_before_ms IS NOT NULL
           AND pursuit.wait_resume_not_before_ms <= $2
         ORDER BY pursuit.wait_resume_not_before_ms, pursuit.pursuit_id
         LIMIT $3 FOR UPDATE OF pursuit, goal SKIP LOCKED",
    )
    .bind(request.tenant_id.into_uuid())
    .bind(request.now.unix_millis())
    .bind(i64::from(request.limit))
    .fetch_all(&mut *transaction)
    .await?;
    let mut woken = 0_u16;
    for row in rows {
        let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        if wake_one(
            &mut transaction,
            request.tenant_id,
            goal_id,
            pursuit_id,
            request.now,
            WakeCondition::RetryDue(request.now),
        )
        .await?
        {
            woken = increment(woken)?;
        }
    }
    transaction.commit().await?;
    Ok(woken)
}

pub(super) async fn event(
    store: &PostgresStandingGoalProductStore,
    request: WakeEventStandingGoalWaits,
) -> Result<u16, PostgresStandingGoalError> {
    let mut transaction = store
        .database
        .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
        .await?;
    let mut woken = wake_inputs(&mut transaction, &request).await?;
    let rows = sqlx::query(
        "SELECT pursuit.pursuit_id, pursuit.goal_id, pursuit.wait_kind,
                pursuit.wait_correlation_key, interaction.responded_at_ms
         FROM runtime.goal_pursuits AS pursuit
         JOIN runtime.standing_goals AS goal
           ON goal.tenant_id = pursuit.tenant_id AND goal.goal_id = pursuit.goal_id
         JOIN approval.interactions AS interaction
           ON interaction.tenant_id = pursuit.tenant_id
          AND interaction.interaction_id::text = pursuit.wait_correlation_key
         JOIN approval.interaction_responses AS response
           ON response.interaction_id = interaction.interaction_id
         WHERE pursuit.tenant_id = $1 AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3
           AND pursuit.wait_kind IN (1, 2) AND interaction.status = 2
           AND interaction.responded_at_ms IS NOT NULL
           AND interaction.responded_at_ms >= pursuit.updated_at_ms
           AND ((pursuit.wait_kind = 1 AND interaction.kind <> 1)
             OR (pursuit.wait_kind = 2 AND interaction.kind = 1
                 AND response.response_kind = 1))
         ORDER BY interaction.responded_at_ms, pursuit.pursuit_id
         LIMIT $2 FOR UPDATE OF pursuit, goal SKIP LOCKED",
    )
    .bind(request.tenant_id.into_uuid())
    .bind(i64::from(request.limit - woken))
    .fetch_all(&mut *transaction)
    .await?;
    for row in rows {
        let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let wait_kind = match row.try_get::<i16, _>("wait_kind")? {
            1 => GoalWaitKind::UserInput,
            2 => GoalWaitKind::Approval,
            _ => return Err(PostgresStandingGoalError::Corrupt),
        };
        let correlation_key: String = row.try_get("wait_correlation_key")?;
        let responded_at = UtcTimestamp::from_unix_millis(row.try_get("responded_at_ms")?);
        let wake_at = UtcTimestamp::from_unix_millis(
            request.now.unix_millis().max(responded_at.unix_millis()),
        );
        if wake_one(
            &mut transaction,
            request.tenant_id,
            goal_id,
            pursuit_id,
            wake_at,
            WakeCondition::CorrelatedEvent {
                kind: wait_kind,
                correlation_key: &correlation_key,
            },
        )
        .await?
        {
            woken = increment(woken)?;
        }
    }
    if woken < request.limit {
        let rows = sqlx::query(
            "SELECT pursuit.pursuit_id, pursuit.goal_id,
                    pursuit.wait_correlation_key, signal.signal_id, signal.occurred_at_ms
             FROM runtime.goal_pursuits AS pursuit
             JOIN runtime.standing_goals AS goal
               ON goal.tenant_id = pursuit.tenant_id AND goal.goal_id = pursuit.goal_id
             JOIN runtime.goal_wake_signals AS signal
               ON signal.tenant_id = pursuit.tenant_id
              AND signal.correlation_key = pursuit.wait_correlation_key
             LEFT JOIN runtime.goal_wake_deliveries AS delivery
               ON delivery.tenant_id = signal.tenant_id
              AND delivery.signal_id = signal.signal_id
              AND delivery.pursuit_id = pursuit.pursuit_id
             WHERE pursuit.tenant_id = $1 AND (goal.status = 2 OR (goal.status = 3 AND goal.mode = 2 AND goal.input_accepted_seq > goal.input_consumed_seq)) AND pursuit.status = 3
               AND pursuit.wait_kind = 3 AND signal.correlation_key NOT LIKE 'standing-goal-input:%' AND signal.occurred_at_ms >= pursuit.updated_at_ms
               AND delivery.signal_id IS NULL
             ORDER BY signal.occurred_at_ms, signal.signal_id, pursuit.pursuit_id
             LIMIT $2 FOR UPDATE OF pursuit, goal SKIP LOCKED",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(i64::from(request.limit - woken))
        .fetch_all(&mut *transaction)
        .await?;
        for row in rows {
            let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?;
            let correlation_key: String = row.try_get("wait_correlation_key")?;
            let signal_id: String = row.try_get("signal_id")?;
            let event_at = UtcTimestamp::from_unix_millis(row.try_get("occurred_at_ms")?);
            let wake_at = UtcTimestamp::from_unix_millis(
                request.now.unix_millis().max(event_at.unix_millis()),
            );
            if wake_one(
                &mut transaction,
                request.tenant_id,
                goal_id,
                pursuit_id,
                wake_at,
                WakeCondition::CorrelatedEvent {
                    kind: GoalWaitKind::ExternalDependency,
                    correlation_key: &correlation_key,
                },
            )
            .await?
            {
                sqlx::query(
                    "INSERT INTO runtime.goal_wake_deliveries (
                         tenant_id, signal_id, goal_id, pursuit_id, delivered_at_ms
                     ) VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(request.tenant_id.into_uuid())
                .bind(signal_id)
                .bind(goal_id.into_uuid())
                .bind(pursuit_id.into_uuid())
                .bind(wake_at.unix_millis())
                .execute(&mut *transaction)
                .await?;
                woken = increment(woken)?;
            }
        }
    }
    transaction.commit().await?;
    Ok(woken)
}

enum WakeCondition<'a> {
    RetryDue(UtcTimestamp),
    CorrelatedEvent {
        kind: GoalWaitKind,
        correlation_key: &'a str,
    },
}

async fn wake_one(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    wake_at: UtcTimestamp,
    condition: WakeCondition<'_>,
) -> Result<bool, PostgresStandingGoalError> {
    let mut goal = load_goal_tx(transaction, tenant_id, goal_id)
        .await?
        .ok_or(PostgresStandingGoalError::Corrupt)?;
    let mut pursuit = load_pursuit_tx(transaction, tenant_id, pursuit_id)
        .await?
        .ok_or(PostgresStandingGoalError::Corrupt)?;
    let Some(barrier) = pursuit.wait_barrier.as_ref() else {
        return Ok(false);
    };
    let matches = goal.allows_existing_work()
        && pursuit.status == GoalPursuitStatus::Waiting
        && match condition {
            WakeCondition::RetryDue(now) => {
                barrier.kind == GoalWaitKind::RetryBackoff
                    && barrier.resume_not_before.is_some_and(|at| at <= now)
            }
            WakeCondition::CorrelatedEvent {
                kind,
                correlation_key,
            } => barrier.kind == kind && barrier.correlation_key == correlation_key,
        };
    if !matches {
        return Ok(false);
    }
    let expected_goal_revision = goal.revision;
    let expected_pursuit_revision = pursuit.revision;
    goal.revision = next(goal.revision)?;
    goal.updated_at = wake_at;
    pursuit.revision = next(pursuit.revision)?;
    pursuit.status = GoalPursuitStatus::Pending;
    pursuit.wait_barrier = None;
    pursuit.continuation_due_at = Some(wake_at);
    pursuit.updated_at = wake_at;
    goal.validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    pursuit
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    if update_goal(transaction, &goal, expected_goal_revision).await? != 1
        || update_pursuit(transaction, &pursuit, expected_pursuit_revision).await? != 1
    {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    Ok(true)
}

fn next(value: u64) -> Result<u64, PostgresStandingGoalError> {
    value
        .checked_add(1)
        .ok_or(PostgresStandingGoalError::Corrupt)
}

fn increment(value: u16) -> Result<u16, PostgresStandingGoalError> {
    value
        .checked_add(1)
        .ok_or(PostgresStandingGoalError::Corrupt)
}

/// Selection reads persistent input positions, including events accepted before
/// the model committed its wait. Loss of every process wake hint is harmless.
async fn wake_inputs(
    transaction: &mut sqlx::PgConnection,
    request: &WakeEventStandingGoalWaits,
) -> Result<u16, PostgresStandingGoalError> {
    let rows = sqlx::query("SELECT goal.goal_id,pursuit.pursuit_id FROM runtime.standing_goals goal
        JOIN runtime.goal_pursuits pursuit ON pursuit.tenant_id=goal.tenant_id AND pursuit.goal_id=goal.goal_id
        WHERE goal.tenant_id=$1 AND goal.mode=2 AND goal.status IN (2,3) AND pursuit.status=3
          AND pursuit.wait_kind=3 AND pursuit.wait_correlation_key='standing-goal-input:' || goal.goal_id::text
          AND goal.input_accepted_seq>goal.input_consumed_seq
        ORDER BY goal.updated_at_ms,goal.goal_id LIMIT $2 FOR UPDATE OF goal SKIP LOCKED")
        .bind(request.tenant_id.into_uuid()).bind(i64::from(request.limit)).fetch_all(&mut *transaction).await?;
    let mut woken = 0_u16;
    for row in rows {
        let goal_id = GoalId::from_uuid(row.try_get("goal_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let pursuit_id = GoalPursuitId::from_uuid(row.try_get("pursuit_id")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let correlation_key = format!("standing-goal-input:{goal_id}");
        if wake_one(
            transaction,
            request.tenant_id,
            goal_id,
            pursuit_id,
            request.now,
            WakeCondition::CorrelatedEvent {
                kind: GoalWaitKind::ExternalDependency,
                correlation_key: &correlation_key,
            },
        )
        .await?
        {
            woken = increment(woken)?;
        }
    }
    Ok(woken)
}
