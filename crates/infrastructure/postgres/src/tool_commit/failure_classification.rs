use musteragent_runtime_capability::ToolExecutionPortError;
use musteragent_runtime_contract::ErrorCode;

use crate::database_failure::{
    PostgresFailureClass as DatabaseFailureClass, classify as classify_database_failure,
};

pub(super) fn database_port(error: &sqlx::Error) -> ToolExecutionPortError {
    match classify_database_failure(error) {
        DatabaseFailureClass::Unavailable => ToolExecutionPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        DatabaseFailureClass::Invalid => {
            tracing::error!(
                error = ?error,
                "PostgreSQL Tool journal was rejected by a deterministic database error"
            );
            ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            }
        }
    }
}

pub(super) fn transaction_port(error: &crate::PostgresTransactionError) -> ToolExecutionPortError {
    match error.failure_class() {
        crate::PostgresFailureClass::SerializationFailure
        | crate::PostgresFailureClass::Deadlock
        | crate::PostgresFailureClass::PoolAcquireTimeout
        | crate::PostgresFailureClass::LockTimeout
        | crate::PostgresFailureClass::StatementTimeout
        | crate::PostgresFailureClass::Unavailable => ToolExecutionPortError {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        },
        crate::PostgresFailureClass::Constraint | crate::PostgresFailureClass::Other => {
            tracing::error!(
                error = ?error,
                "PostgreSQL Tool journal transaction failed deterministically"
            );
            ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            }
        }
    }
}
