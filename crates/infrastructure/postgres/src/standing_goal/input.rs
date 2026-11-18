//! Transactional input acceptance in the original Goal owner. Wake-up delivery
//! is merely a hint; the accepted/consumed positions are the durable obligation.
use super::*;
use musteragent_goal::{GoalInputCursor, StandingGoalMode};
use musteragent_runtime_automation::{AcceptStandingGoalInput, StandingGoalInputOutcome};

impl PostgresStandingGoalProductStore {
    pub(super) async fn accept_input_inner(
        &self,
        request: AcceptStandingGoalInput,
    ) -> Result<StandingGoalInputOutcome, PostgresStandingGoalError> {
        request
            .validate()
            .map_err(|_| PostgresStandingGoalError::Corrupt)?;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::StandingGoal)
            .await?;
        let result = accept_input_on(&mut tx, request).await?;
        tx.commit().await?;
        Ok(result)
    }
}

/// Composition port for an input owner that has already checked its source or
/// collaboration authority in this same transaction. The caller locks that
/// authority before the Goal; this function never commits a second transaction.
pub(crate) async fn accept_input_on(
    tx: &mut sqlx::PgConnection,
    request: AcceptStandingGoalInput,
) -> Result<StandingGoalInputOutcome, PostgresStandingGoalError> {
    request
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    admission::lock_goal(tx, request.tenant_id, request.goal_id).await?;
    let Some(goal) = load_goal_tx(tx, request.tenant_id, request.goal_id).await? else {
        return Ok(StandingGoalInputOutcome::Rejected);
    };
    if goal.owner != request.principal_id || goal.mode != StandingGoalMode::Continuous {
        return Ok(StandingGoalInputOutcome::Rejected);
    }
    let fingerprint = request.fingerprint().into_bytes();
    if let Some(row) = sqlx::query("SELECT input_seq,fingerprint FROM runtime.goal_inputs WHERE tenant_id=$1 AND goal_id=$2 AND input_key=$3")
        .bind(request.tenant_id.into_uuid()).bind(request.goal_id.into_uuid()).bind(&request.input_key)
        .fetch_optional(&mut *tx).await?
    {
        return Ok(if row.try_get::<Vec<u8>, _>("fingerprint")? == fingerprint {
            StandingGoalInputOutcome::Existing { sequence: positive(row.try_get("input_seq")?)? }
        } else { StandingGoalInputOutcome::Rejected });
    }
    if goal.revision != request.expected_goal_revision || goal.status != StandingGoalStatus::Active
    {
        return Ok(StandingGoalInputOutcome::Rejected);
    }
    for reference in [&request.content, &goal.objective_ref] {
        let content: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE tenant_id=$1 AND content_id=$2 AND content_hash=$3 AND byte_length=$4::numeric AND media_type=$5 AND state=2 FOR SHARE)")
        .bind(request.tenant_id.into_uuid()).bind(reference.content_id.into_uuid())
        .bind(reference.sha256.into_bytes().to_vec()).bind(reference.byte_length.to_string())
        .bind(reference.media_type.as_str()).fetch_one(&mut *tx).await?;
        if !content {
            return Ok(StandingGoalInputOutcome::Rejected);
        }
    }
    let rules_version = rules::retain(tx, &goal).await?;
    let mut cursor = load_cursor(tx, request.tenant_id, request.goal_id).await?;
    let sequence = cursor
        .accept()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    sqlx::query("INSERT INTO runtime.goal_inputs(tenant_id,goal_id,input_seq,input_key,fingerprint,content_id,rules_version,accepted_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,(extract(epoch FROM transaction_timestamp())*1000)::bigint)")
        .bind(request.tenant_id.into_uuid()).bind(request.goal_id.into_uuid()).bind(sequence as i64)
        .bind(&request.input_key).bind(fingerprint.as_slice()).bind(request.content.content_id.into_uuid()).bind(rules_version)
        .execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE runtime.standing_goals SET input_accepted_seq=$3 WHERE tenant_id=$1 AND goal_id=$2",
    )
    .bind(request.tenant_id.into_uuid())
    .bind(request.goal_id.into_uuid())
    .bind(sequence as i64)
    .execute(&mut *tx)
    .await?;
    Ok(StandingGoalInputOutcome::Accepted { sequence })
}

pub(super) async fn load_cursor(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
) -> Result<GoalInputCursor, PostgresStandingGoalError> {
    let row = sqlx::query("SELECT input_accepted_seq,input_consumed_seq,input_batch_first,input_batch_last FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).fetch_one(tx).await?;
    let batch = match (
        row.try_get::<Option<i64>, _>("input_batch_first")?,
        row.try_get::<Option<i64>, _>("input_batch_last")?,
    ) {
        (None, None) => None,
        (Some(first), Some(last)) => Some(musteragent_goal::GoalInputBatchWindow {
            first: positive(first)?,
            last: positive(last)?,
        }),
        _ => return Err(PostgresStandingGoalError::Corrupt),
    };
    let cursor = GoalInputCursor {
        accepted: u64::try_from(row.try_get::<i64, _>("input_accepted_seq")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        consumed: u64::try_from(row.try_get::<i64, _>("input_consumed_seq")?)
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        batch,
    };
    cursor
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    Ok(cursor)
}

fn positive(value: i64) -> Result<u64, PostgresStandingGoalError> {
    if value <= 0 {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    Ok(value as u64)
}

/// Called while the original Goal/Pursuit claim transaction owns its locks.
pub(super) async fn freeze(
    tx: &mut sqlx::PgConnection,
    goal: &StandingGoal,
) -> Result<
    (
        Vec<ContentRef>,
        Option<musteragent_goal::GoalProcessingRules>,
    ),
    PostgresStandingGoalError,
> {
    if goal.mode != StandingGoalMode::Continuous {
        return Ok((Vec::new(), None));
    }
    let mut cursor = load_cursor(tx, goal.tenant_id, goal.goal_id).await?;
    if !cursor.pending() {
        return Ok((Vec::new(), None));
    }
    let limit = if cursor.batch.is_some() {
        musteragent_goal::MAX_GOAL_INPUT_BATCH
    } else {
        rules::batch_limit(tx, goal.tenant_id, goal.goal_id, cursor.consumed).await?
    };
    let Some(window) = cursor
        .freeze(limit)
        .map_err(|_| PostgresStandingGoalError::Corrupt)?
    else {
        return Ok((Vec::new(), None));
    };
    sqlx::query("UPDATE runtime.standing_goals SET input_batch_first=$3,input_batch_last=$4 WHERE tenant_id=$1 AND goal_id=$2")
        .bind(goal.tenant_id.into_uuid()).bind(goal.goal_id.into_uuid()).bind(window.first as i64).bind(window.last as i64).execute(&mut *tx).await?;
    let rules = rules::for_batch(tx, goal.tenant_id, goal.goal_id, window).await?;
    Ok((
        references(tx, goal.tenant_id, goal.goal_id, window).await?,
        Some(rules),
    ))
}

pub(super) async fn references(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    goal: GoalId,
    window: musteragent_goal::GoalInputBatchWindow,
) -> Result<Vec<ContentRef>, PostgresStandingGoalError> {
    let rows = sqlx::query("SELECT c.content_id AS input_content_id,c.media_type AS input_media_type,c.byte_length::text AS input_byte_length,c.content_hash AS input_hash
        FROM runtime.goal_inputs i JOIN artifact.content_refs c ON c.tenant_id=i.tenant_id AND c.content_id=i.content_id
        WHERE i.tenant_id=$1 AND i.goal_id=$2 AND i.input_seq BETWEEN $3 AND $4 ORDER BY i.input_seq")
        .bind(tenant.into_uuid()).bind(goal.into_uuid()).bind(window.first as i64).bind(window.last as i64)
        .fetch_all(&mut *tx).await?;
    if rows.len() as u64 != window.last - window.first + 1 {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    rows.iter().map(|row| content_ref(row, "input")).collect()
}

/// Runs inside the original Run/Ledger/grant admission transaction, after its
/// Goal and Pursuit fence checks. A later input cannot replace this batch.
pub(super) async fn bind_run(
    tx: &mut sqlx::PgConnection,
    goal: &StandingGoal,
    source: Option<RunId>,
    target: RunId,
    input_refs: &[ContentRef],
    objective: &ContentRef,
) -> Result<bool, PostgresStandingGoalError> {
    if goal.mode != StandingGoalMode::Continuous {
        return Ok(input_refs.is_empty());
    }
    let cursor = load_cursor(tx, goal.tenant_id, goal.goal_id).await?;
    let Some(window) = cursor.batch else {
        return Ok(input_refs.is_empty());
    };
    if references(tx, goal.tenant_id, goal.goal_id, window).await? != input_refs {
        return Ok(false);
    }
    let current: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT input_batch_run_id FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2",
    )
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.goal_id.into_uuid())
    .fetch_one(&mut *tx)
    .await?;
    if current.is_some_and(|value| {
        Some(value) != source.map(RunId::into_uuid) && value != target.into_uuid()
    }) {
        return Ok(false);
    }
    if !rules::bind_run(tx, goal.tenant_id, goal.goal_id, target, window, objective).await? {
        return Ok(false);
    }
    sqlx::query(
        "UPDATE runtime.standing_goals SET input_batch_run_id=$3 WHERE tenant_id=$1 AND goal_id=$2",
    )
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.goal_id.into_uuid())
    .bind(target.into_uuid())
    .execute(&mut *tx)
    .await?;
    Ok(true)
}

/// The original fenced evaluator calls this only for a completed processing
/// cycle, including one that drained while the Goal was paused. No wake or
/// claim acknowledges business inputs.
pub(super) async fn settle(
    tx: &mut sqlx::PgConnection,
    goal: &StandingGoal,
    run: RunId,
) -> Result<(), PostgresStandingGoalError> {
    if goal.mode != StandingGoalMode::Continuous {
        return Err(PostgresStandingGoalError::Corrupt);
    }
    let mut cursor = load_cursor(tx, goal.tenant_id, goal.goal_id).await?;
    let Some(window) = cursor.batch else {
        return Ok(());
    };
    let current: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT input_batch_run_id FROM runtime.standing_goals WHERE tenant_id=$1 AND goal_id=$2",
    )
    .bind(goal.tenant_id.into_uuid())
    .bind(goal.goal_id.into_uuid())
    .fetch_one(&mut *tx)
    .await?;
    // Initial setup or a separately admitted foreground Run did not receive
    // this batch, so its successful evaluation must not consume it.
    if current != Some(run.into_uuid()) {
        return Ok(());
    }
    cursor
        .settle(window)
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    sqlx::query("UPDATE runtime.standing_goals SET input_consumed_seq=$3,input_batch_first=NULL,input_batch_last=NULL,input_batch_run_id=NULL WHERE tenant_id=$1 AND goal_id=$2").bind(goal.tenant_id.into_uuid()).bind(goal.goal_id.into_uuid()).bind(cursor.consumed as i64).execute(&mut *tx).await?;
    Ok(())
}
