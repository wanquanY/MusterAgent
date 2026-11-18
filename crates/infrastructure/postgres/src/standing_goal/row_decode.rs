//! Row decoders for the durable Standing Goal aggregate.

use super::*;

pub(crate) fn decode_goal(row: &PgRow) -> Result<StandingGoal, PostgresStandingGoalError> {
    let goal = StandingGoal {
        input_progress: musteragent_goal::GoalInputProgress {
            accepted: u64::try_from(row.try_get::<i64, _>("input_accepted_seq")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            consumed: u64::try_from(row.try_get::<i64, _>("input_consumed_seq")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        },
        mode: decode_mode(row.try_get("mode")?)?,
        tenant_id: id(row.try_get("tenant_id")?, "tenant_id")?,
        goal_id: id(row.try_get("goal_id")?, "goal_id")?,
        revision: parse_u64(row.try_get("revision")?, "revision")?,
        owner: id(row.try_get("owner_principal_id")?, "owner_principal_id")?,
        origin: decode_origin(row.try_get("origin")?)?,
        creator_agent_id: optional_id::<AgentId>(
            row.try_get("creator_agent_id")?,
            "creator_agent_id",
        )?,
        target_agent_id: id(row.try_get("target_agent_id")?, "target_agent_id")?,
        conversation_id: id(row.try_get("conversation_id")?, "conversation_id")?,
        title: row.try_get("title")?,
        objective_ref: content_ref(row, "objective")?,
        constraints: decode_json(row, "constraints_json")?,
        completion: GoalCompletionContract {
            risk_class: decode_risk_class(row.try_get("completion_risk_class")?)?,
            success_criteria: decode_json(row, "success_criteria_json")?,
            evidence_requirements: decode_json(row, "evidence_requirements_json")?,
            failure_conditions: decode_json(row, "failure_conditions_json")?,
            minimum_evidence_items: u16::try_from(row.try_get::<i16, _>("minimum_evidence_items")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        },
        budget: GoalBudget {
            max_runs: u32::try_from(row.try_get::<i32, _>("max_runs")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            max_elapsed_millis: parse_u64(
                row.try_get("max_elapsed_millis")?,
                "max_elapsed_millis",
            )?,
            max_model_tokens: parse_u64(row.try_get("max_model_tokens")?, "max_model_tokens")?,
            max_tool_invocations: parse_u64(
                row.try_get("max_tool_invocations")?,
                "max_tool_invocations",
            )?,
            max_consecutive_failures: u32::try_from(
                row.try_get::<i32, _>("max_consecutive_failures")?,
            )
            .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        },
        status: decode_goal_status(row.try_get("status")?)?,
        active_pursuit_id: optional_id(row.try_get("active_pursuit_id")?, "active_pursuit_id")?,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms")?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
    };
    goal.validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    Ok(goal)
}

pub(crate) fn decode_pursuit(row: &PgRow) -> Result<GoalPursuit, PostgresStandingGoalError> {
    let status = decode_pursuit_status(row.try_get("status")?)?;
    let wait_barrier = if status == GoalPursuitStatus::Waiting {
        Some(GoalWaitBarrier {
            kind: decode_wait_kind(
                row.try_get::<Option<i16>, _>("wait_kind")?
                    .ok_or(PostgresStandingGoalError::Corrupt)?,
            )?,
            correlation_key: row
                .try_get::<Option<String>, _>("wait_correlation_key")?
                .ok_or(PostgresStandingGoalError::Corrupt)?,
            reason_ref: content_ref(row, "wait_reason")?,
            resume_not_before: row
                .try_get::<Option<i64>, _>("wait_resume_not_before_ms")?
                .map(UtcTimestamp::from_unix_millis),
            expires_at: row
                .try_get::<Option<i64>, _>("wait_expires_at_ms")?
                .map(UtcTimestamp::from_unix_millis),
        })
    } else {
        None
    };
    let pursuit = GoalPursuit {
        pursuit_id: id(row.try_get("pursuit_id")?, "pursuit_id")?,
        tenant_id: id(row.try_get("tenant_id")?, "tenant_id")?,
        goal_id: id(row.try_get("goal_id")?, "goal_id")?,
        revision: parse_u64(row.try_get("revision")?, "revision")?,
        status,
        active_run_id: optional_id::<RunId>(row.try_get("active_run_id")?, "active_run_id")?,
        continuation_source_run_id: optional_id::<RunId>(
            row.try_get("continuation_source_run_id")?,
            "continuation_source_run_id",
        )?,
        wait_barrier,
        budget_usage: GoalBudgetUsage {
            runs: u32::try_from(row.try_get::<i32, _>("used_runs")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
            elapsed_millis: parse_u64(row.try_get("used_elapsed_millis")?, "used_elapsed_millis")?,
            model_tokens: parse_u64(row.try_get("used_model_tokens")?, "used_model_tokens")?,
            tool_invocations: parse_u64(
                row.try_get("used_tool_invocations")?,
                "used_tool_invocations",
            )?,
            consecutive_failures: u32::try_from(row.try_get::<i32, _>("consecutive_failures")?)
                .map_err(|_| PostgresStandingGoalError::Corrupt)?,
        },
        continuation_due_at: row
            .try_get::<Option<i64>, _>("continuation_due_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
        started_at: UtcTimestamp::from_unix_millis(row.try_get("started_at_ms")?),
        updated_at: UtcTimestamp::from_unix_millis(row.try_get("updated_at_ms")?),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_ms")?
            .map(UtcTimestamp::from_unix_millis),
    };
    pursuit
        .validate()
        .map_err(|_| PostgresStandingGoalError::Corrupt)?;
    Ok(pursuit)
}
