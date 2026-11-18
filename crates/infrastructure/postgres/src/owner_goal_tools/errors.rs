use super::*;

pub(super) fn goal_store_error(
    error: musteragent_runtime_automation::StandingGoalStoreError,
) -> Error {
    use musteragent_runtime_automation::StandingGoalStoreError as E;
    match error {
        E::Unavailable => Error::Infrastructure,
        E::Corrupt => Error::Corrupt,
    }
}

pub(super) fn product_error(
    error: musteragent_runtime_automation::StandingGoalProductError,
) -> Error {
    use musteragent_runtime_automation::StandingGoalProductError as E;
    match error {
        E::Unavailable => Error::Infrastructure,
        E::NotFound | E::Unauthorized => Error::Forbidden,
        E::Conflict | E::RunStillActive => Error::Conflict,
        E::Corrupt => Error::Corrupt,
        _ => Error::InvalidArguments,
    }
}

pub(super) fn store_error(error: DialogueStoreError) -> Error {
    match error {
        DialogueStoreError::RevisionConflict | DialogueStoreError::IdempotencyConflict => {
            Error::Conflict
        }
        DialogueStoreError::Infrastructure => Error::Infrastructure,
        DialogueStoreError::Corrupt => Error::Corrupt,
        _ => Error::Forbidden,
    }
}
pub(super) fn creation_error(error: OwnerGoalCreationError) -> Error {
    match error {
        OwnerGoalCreationError::Invalid => Error::InvalidArguments,
        OwnerGoalCreationError::Source(error) => store_error(error),
        OwnerGoalCreationError::Content(_) => Error::Infrastructure,
    }
}
pub(super) fn source_error(error: AgentSelfConfigurationError) -> Error {
    match error {
        AgentSelfConfigurationError::Forbidden => Error::Forbidden,
        AgentSelfConfigurationError::InvalidArguments => Error::InvalidArguments,
        AgentSelfConfigurationError::Conflict => Error::Conflict,
        AgentSelfConfigurationError::StaleFence => Error::StaleFence,
        AgentSelfConfigurationError::Infrastructure => Error::Infrastructure,
        AgentSelfConfigurationError::Corrupt => Error::Corrupt,
    }
}
pub(super) fn lane_error(error: crate::commit_lane::CommitLaneError) -> Error {
    match error {
        crate::commit_lane::CommitLaneError::StaleFence => Error::StaleFence,
        _ => Error::Infrastructure,
    }
}
pub(super) fn commit_error(error: musteragent_runtime_capability::ToolExecutionPortError) -> Error {
    use musteragent_runtime_contract::ErrorCode;
    match error.code {
        ErrorCode::AttemptLeaseStale => Error::StaleFence,
        ErrorCode::RunTransitionInvalid | ErrorCode::IdempotencyConflict => Error::Conflict,
        ErrorCode::PolicyDenied => Error::Forbidden,
        ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout => Error::Infrastructure,
        _ => Error::Corrupt,
    }
}
