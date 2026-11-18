//! Error classification for the PostgreSQL Standing Goal adapter.

use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PostgresStandingGoalError {
    #[error("Conversation workspace assignment is pending")]
    AssignmentPending,
    #[error("standing Goal source dependency is unavailable")]
    DependencyUnavailable,
    #[error("PostgreSQL standing Goal dependency is unavailable")]
    Database(#[from] sqlx::Error),
    #[error("governed PostgreSQL Standing Goal Unit of Work failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("PostgreSQL standing Goal was not found")]
    NotFound,
    #[error("PostgreSQL standing Goal state is corrupt")]
    Corrupt,
}

impl PostgresStandingGoalError {
    pub(super) const fn corrupt(_field: &'static str) -> Self {
        Self::Corrupt
    }
}

impl From<PostgresStandingGoalError> for StandingGoalStoreError {
    fn from(value: PostgresStandingGoalError) -> Self {
        match &value {
            PostgresStandingGoalError::Database(_) | PostgresStandingGoalError::Transaction(_) => {
                tracing::warn!(
                    error = ?value,
                    "PostgreSQL standing Goal dependency failed before product error classification"
                );
            }
            PostgresStandingGoalError::Corrupt => {
                tracing::error!(
                    error = ?value,
                    "PostgreSQL standing Goal state failed validation"
                );
            }
            PostgresStandingGoalError::NotFound
            | PostgresStandingGoalError::AssignmentPending
            | PostgresStandingGoalError::DependencyUnavailable => {}
        }
        match value {
            PostgresStandingGoalError::AssignmentPending
            | PostgresStandingGoalError::DependencyUnavailable => {
                StandingGoalStoreError::Unavailable
            }
            PostgresStandingGoalError::Database(_) | PostgresStandingGoalError::Transaction(_) => {
                StandingGoalStoreError::Unavailable
            }
            PostgresStandingGoalError::NotFound | PostgresStandingGoalError::Corrupt => {
                StandingGoalStoreError::Corrupt
            }
        }
    }
}

impl From<musteragent_runtime_session::owner_event_sources::EventSourceStoreError>
    for PostgresStandingGoalError
{
    fn from(
        value: musteragent_runtime_session::owner_event_sources::EventSourceStoreError,
    ) -> Self {
        match value {
            musteragent_runtime_session::owner_event_sources::EventSourceStoreError::Infrastructure => Self::DependencyUnavailable,
            _ => Self::Corrupt,
        }
    }
}
