//! Immutable business-rule history in the existing Goal aggregate. Content
//! retention, Run identity and the input batch keep their original owners.
use super::*;
use musteragent_goal::{GoalInputBatchWindow, GoalProcessingRules, MAX_GOAL_INPUT_BATCH};
use musteragent_persistence_codec::goal_processing_rules as codec;

pub(super) async fn retain(
    tx: &mut sqlx::PgConnection,
    goal: &StandingGoal,
) -> Result<Vec<u8>, PostgresStandingGoalError> {
    let rules = GoalProcessingRules::from_goal(goal);
    let bytes = codec::encode(&rules).map_err(|_| PostgresStandingGoalError::Corrupt)?;
    let version = codec::version(&bytes).into_bytes().to_vec();
    sqlx::query("INSERT INTO runtime.goal_processing_rules(tenant_id,goal_id,rules_version,rules_payload,objective_content_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,goal_id,rules_version) DO NOTHING")
        .bind(goal.tenant_id.into_uuid()).bind(goal.goal_id.into_uuid()).bind(&version)
        .bind(&bytes).bind(goal.objective_ref.content_id.into_uuid()).execute(&mut *tx).await?;
    // Even a digest collision or corrupt retained payload cannot silently
    // change the rules under an already accepted version.
    let stored = read(tx, goal.tenant_id, goal.goal_id, &version).await?;
    if stored != rules {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    Ok(version)
}

async fn read(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    version: &[u8],
) -> Result<GoalProcessingRules, PostgresStandingGoalError> {
    let row = sqlx::query("SELECT rules_payload,objective_content_id FROM runtime.goal_processing_rules WHERE tenant_id=$1 AND goal_id=$2 AND rules_version=$3")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(version).fetch_one(&mut *tx).await?;
    let bytes: Vec<u8> = row.try_get("rules_payload")?;
    if codec::version(&bytes).into_bytes().as_slice() != version {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    let rules = codec::decode(&bytes).map_err(|_| PostgresStandingGoalError::Corrupt)?;
    if rules.objective_ref.content_id.into_uuid()
        != row.try_get::<uuid::Uuid, _>("objective_content_id")?
    {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    Ok(rules)
}

/// Freeze only the contiguous prefix with the first pending rule version.
/// A lifecycle-only change does not split a batch; a business edit does.
pub(super) async fn batch_limit(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    consumed: u64,
) -> Result<u16, PostgresStandingGoalError> {
    let rows = sqlx::query("SELECT input_seq,rules_version FROM runtime.goal_inputs WHERE tenant_id=$1 AND goal_id=$2 AND input_seq>$3 ORDER BY input_seq LIMIT $4")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(consumed as i64)
        .bind(i64::from(MAX_GOAL_INPUT_BATCH)).fetch_all(&mut *tx).await?;
    let mut version: Option<Vec<u8>> = None;
    let mut count = 0_u16;
    for row in rows {
        if row.try_get::<i64, _>("input_seq")? != (consumed + u64::from(count) + 1) as i64 {
            return Err(PostgresStandingGoalError::Corrupt);
        }
        let next: Vec<u8> = row.try_get("rules_version")?;
        if version.as_ref().is_some_and(|original| *original != next) {
            break;
        }
        version = Some(next);
        count += 1;
    }
    if count == 0 {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    Ok(count)
}

pub(super) async fn for_batch(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    window: GoalInputBatchWindow,
) -> Result<GoalProcessingRules, PostgresStandingGoalError> {
    let versions: Vec<Vec<u8>> = sqlx::query_scalar("SELECT rules_version FROM runtime.goal_inputs WHERE tenant_id=$1 AND goal_id=$2 AND input_seq BETWEEN $3 AND $4 ORDER BY input_seq")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(window.first as i64).bind(window.last as i64)
        .fetch_all(&mut *tx).await?;
    let version = versions.first().ok_or(PostgresStandingGoalError::Corrupt)?;
    if versions.len() as u64 != window.last - window.first + 1
        || versions.iter().any(|v| v != version)
    {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    read(tx, tenant, goal, version).await
}

pub(super) async fn bind_run(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    run: RunId,
    window: GoalInputBatchWindow,
    objective: &ContentRef,
) -> Result<bool, PostgresStandingGoalError> {
    let rules = for_batch(tx, tenant, goal, window).await?;
    if rules.objective_ref != *objective {
        return Ok(false);
    }
    let bytes = codec::encode(&rules).map_err(|_| PostgresStandingGoalError::Corrupt)?;
    let version = codec::version(&bytes).into_bytes();
    sqlx::query("INSERT INTO runtime.goal_run_processing_rules(tenant_id,goal_id,run_id,rules_version,input_first,input_last) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,goal_id,run_id) DO NOTHING").bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(run.into_uuid()).bind(version.as_slice()).bind(window.first as i64).bind(window.last as i64)
        .execute(&mut *tx).await?;
    let accepted: (Option<i64>, Option<i64>) = sqlx::query_as("SELECT input_first,input_last FROM runtime.goal_run_processing_rules WHERE tenant_id=$1 AND goal_id=$2 AND run_id=$3")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(run.into_uuid()).fetch_one(&mut *tx).await?;
    Ok(
        accepted == (Some(window.first as i64), Some(window.last as i64))
            && for_run(tx, tenant, goal, run).await?.as_ref() == Some(&rules),
    )
}

pub(super) async fn for_run(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    run: RunId,
) -> Result<Option<GoalProcessingRules>, PostgresStandingGoalError> {
    let version: Option<Vec<u8>> = sqlx::query_scalar("SELECT rules_version FROM runtime.goal_run_processing_rules WHERE tenant_id=$1 AND goal_id=$2 AND run_id=$3")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(run.into_uuid()).fetch_optional(&mut *tx).await?;
    match version {
        Some(version) => read(tx, tenant, goal, &version).await.map(Some),
        None => {
            let missing: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2 AND input_batch_run_id=$3")
                .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(run.into_uuid()).fetch_one(&mut *tx).await?;
            if missing != 0 {
                return Err(PostgresStandingGoalError::Corrupt);
            }
            Ok(None)
        }
    }
}

pub(super) async fn completion(
    tx: &mut sqlx::PgConnection,
    goal: &StandingGoal,
    run: Option<RunId>,
) -> Result<GoalCompletionContract, PostgresStandingGoalError> {
    match run {
        Some(run) => Ok(for_run(tx, goal.tenant_id, goal.goal_id, run)
            .await?
            .map(|rules| rules.completion)
            .unwrap_or_else(|| goal.completion.clone())),
        None => Ok(goal.completion.clone()),
    }
}
