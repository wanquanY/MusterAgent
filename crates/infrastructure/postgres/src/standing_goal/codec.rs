//! Closed persistence-code mappings for Standing Goal state.

use super::*;

pub(super) const fn origin_code(value: GoalOrigin) -> i16 {
    match value {
        GoalOrigin::User => 1,
        GoalOrigin::Agent => 2,
    }
}

pub(super) const fn decode_origin(value: i16) -> Result<GoalOrigin, PostgresStandingGoalError> {
    match value {
        1 => Ok(GoalOrigin::User),
        2 => Ok(GoalOrigin::Agent),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn risk_class_code(value: GoalRiskClass) -> i16 {
    match value {
        GoalRiskClass::Standard => 1,
        GoalRiskClass::HighRisk => 2,
    }
}

pub(super) const fn decode_risk_class(
    value: i16,
) -> Result<GoalRiskClass, PostgresStandingGoalError> {
    match value {
        1 => Ok(GoalRiskClass::Standard),
        2 => Ok(GoalRiskClass::HighRisk),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn goal_status_code(value: StandingGoalStatus) -> i16 {
    match value {
        StandingGoalStatus::Draft => 1,
        StandingGoalStatus::Active => 2,
        StandingGoalStatus::Paused => 3,
        StandingGoalStatus::Achieved => 4,
        StandingGoalStatus::Failed => 5,
        StandingGoalStatus::Cancelled => 6,
    }
}

pub(super) const fn decode_goal_status(
    value: i16,
) -> Result<StandingGoalStatus, PostgresStandingGoalError> {
    match value {
        1 => Ok(StandingGoalStatus::Draft),
        2 => Ok(StandingGoalStatus::Active),
        3 => Ok(StandingGoalStatus::Paused),
        4 => Ok(StandingGoalStatus::Achieved),
        5 => Ok(StandingGoalStatus::Failed),
        6 => Ok(StandingGoalStatus::Cancelled),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn pursuit_status_code(value: GoalPursuitStatus) -> i16 {
    match value {
        GoalPursuitStatus::Pending => 1,
        GoalPursuitStatus::Running => 2,
        GoalPursuitStatus::Waiting => 3,
        GoalPursuitStatus::Paused => 4,
        GoalPursuitStatus::Achieved => 5,
        GoalPursuitStatus::Failed => 6,
        GoalPursuitStatus::Cancelled => 7,
    }
}

pub(super) const fn decode_pursuit_status(
    value: i16,
) -> Result<GoalPursuitStatus, PostgresStandingGoalError> {
    match value {
        1 => Ok(GoalPursuitStatus::Pending),
        2 => Ok(GoalPursuitStatus::Running),
        3 => Ok(GoalPursuitStatus::Waiting),
        4 => Ok(GoalPursuitStatus::Paused),
        5 => Ok(GoalPursuitStatus::Achieved),
        6 => Ok(GoalPursuitStatus::Failed),
        7 => Ok(GoalPursuitStatus::Cancelled),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn wait_kind_code(value: GoalWaitKind) -> i16 {
    match value {
        GoalWaitKind::UserInput => 1,
        GoalWaitKind::Approval => 2,
        GoalWaitKind::ExternalDependency => 3,
        GoalWaitKind::RetryBackoff => 4,
        GoalWaitKind::CompletionConfirmation => 5,
    }
}

pub(super) const fn decode_wait_kind(
    value: i16,
) -> Result<GoalWaitKind, PostgresStandingGoalError> {
    match value {
        1 => Ok(GoalWaitKind::UserInput),
        2 => Ok(GoalWaitKind::Approval),
        3 => Ok(GoalWaitKind::ExternalDependency),
        4 => Ok(GoalWaitKind::RetryBackoff),
        5 => Ok(GoalWaitKind::CompletionConfirmation),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn verdict_code(value: GoalVerdict) -> i16 {
    match value {
        GoalVerdict::Continue => 1,
        GoalVerdict::Waiting => 2,
        GoalVerdict::Achieved => 3,
        GoalVerdict::Failed => 4,
    }
}

pub(super) const fn decode_verdict(value: i16) -> Result<GoalVerdict, PostgresStandingGoalError> {
    match value {
        1 => Ok(GoalVerdict::Continue),
        2 => Ok(GoalVerdict::Waiting),
        3 => Ok(GoalVerdict::Achieved),
        4 => Ok(GoalVerdict::Failed),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}

pub(super) const fn mode_code(value: musteragent_goal::StandingGoalMode) -> i16 {
    match value {
        musteragent_goal::StandingGoalMode::Finite => 1,
        musteragent_goal::StandingGoalMode::Continuous => 2,
    }
}
pub(super) const fn decode_mode(
    value: i16,
) -> Result<musteragent_goal::StandingGoalMode, PostgresStandingGoalError> {
    match value {
        1 => Ok(musteragent_goal::StandingGoalMode::Finite),
        2 => Ok(musteragent_goal::StandingGoalMode::Continuous),
        _ => Err(PostgresStandingGoalError::Corrupt),
    }
}
