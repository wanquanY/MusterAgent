use musteragent_identity::CorrelationId;
use musteragent_ledger::LedgerError;
use musteragent_runtime_contract::runtime_error;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};

use crate::PostgresLedgerError;
use crate::PostgresRunControlError;
use crate::PostgresSecurityAuditError;
use crate::admission::PostgresAdmissionError;
use crate::database_failure::{PostgresFailureClass, classify as classify_database_failure};
use crate::execution::PostgresExecutionError;

pub(super) fn dependency(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::DependencyUnavailable,
        correlation_id,
        true,
        "PostgreSQL Runtime store is unavailable",
    )
}

pub(super) fn database_dependency(
    error: sqlx::Error,
    operation: &'static str,
    correlation_id: CorrelationId,
) -> RuntimeError {
    tracing::error!(
        correlation_id = %correlation_id,
        database.backend = "postgresql",
        database.operation = operation,
        error = %error,
        error.debug = ?error,
        "PostgreSQL Runtime store operation failed"
    );
    dependency(correlation_id)
}

pub(super) fn corrupt(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::InternalInvariantViolated,
        correlation_id,
        false,
        "Persisted Runtime state is invalid",
    )
}

pub(super) fn map_execution(
    error: PostgresExecutionError,
    correlation_id: CorrelationId,
) -> RuntimeError {
    let code = match error {
        PostgresExecutionError::RunNotCancellable => ErrorCode::RunTransitionInvalid,
        PostgresExecutionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::IdempotencyConflict,
        )) => ErrorCode::IdempotencyConflict,
        PostgresExecutionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::HomeEpochMismatch,
        )) => ErrorCode::ConversationEpochStale,
        PostgresExecutionError::Ledger(PostgresLedgerError::Contract(
            LedgerError::SequenceConflict { .. },
        )) => ErrorCode::ConversationSeqConflict,
        PostgresExecutionError::CorruptIdentity => ErrorCode::InternalInvariantViolated,
        _ => ErrorCode::DependencyUnavailable,
    };
    runtime_error(
        code,
        correlation_id,
        code == ErrorCode::DependencyUnavailable,
        match code {
            ErrorCode::RunTransitionInvalid => "Run cannot be cancelled in its current state",
            ErrorCode::IdempotencyConflict => "Cancel command conflicts with a prior command",
            ErrorCode::ConversationEpochStale => "Conversation ownership epoch is stale",
            ErrorCode::ConversationSeqConflict => "Conversation sequence changed",
            ErrorCode::InternalInvariantViolated => "Persisted Run identity is invalid",
            _ => "PostgreSQL execution store is unavailable",
        },
    )
}

pub(super) fn map_run_control(
    error: PostgresRunControlError,
    correlation_id: CorrelationId,
) -> RuntimeError {
    let (code, retryable, message) = match error {
        PostgresRunControlError::EnqueueContract(_)
        | PostgresRunControlError::ConsumeContract(_) => (
            ErrorCode::InvalidCommand,
            false,
            "Run control command is invalid",
        ),
        PostgresRunControlError::InvalidState => (
            ErrorCode::RunTransitionInvalid,
            false,
            "Run does not accept control in its current state",
        ),
        PostgresRunControlError::ContentNotAuthorized => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Run control content identity or integrity validation failed",
        ),
        PostgresRunControlError::Ledger(error) => return map_ledger(error, correlation_id),
        PostgresRunControlError::Execution(error) => return map_execution(error, correlation_id),
        PostgresRunControlError::Database(_) | PostgresRunControlError::Transaction(_) => (
            ErrorCode::DependencyUnavailable,
            true,
            "PostgreSQL Run control store is unavailable",
        ),
    };
    runtime_error(code, correlation_id, retryable, message)
}

pub(super) fn map_admission(
    error: PostgresAdmissionError,
    correlation_id: CorrelationId,
) -> RuntimeError {
    let (code, retryable, safe_message) = match error {
        PostgresAdmissionError::GoalSourceUnavailable => (
            ErrorCode::DependencyUnavailable,
            true,
            "Standing Goal source dependency is unavailable",
        ),
        PostgresAdmissionError::GoalStart(rejection) => (
            rejection.error_code(),
            false,
            "Goal draft admission was rejected",
        ),
        PostgresAdmissionError::Contract(_) => (
            ErrorCode::InvalidCommand,
            false,
            "Submit Message admission batch is invalid",
        ),
        PostgresAdmissionError::SnapshotGrantProjectionMismatch => (
            ErrorCode::RuntimeSnapshotInvalid,
            false,
            "Runtime Snapshot model projection is not authorized",
        ),
        PostgresAdmissionError::Ledger(PostgresLedgerError::Database(error))
        | PostgresAdmissionError::Database(error)
        | PostgresAdmissionError::Audit(PostgresSecurityAuditError::Database(error)) => {
            return map_admission_database(&error, correlation_id);
        }
        PostgresAdmissionError::Transaction(_)
        | PostgresAdmissionError::Ledger(PostgresLedgerError::Transaction(_))
        | PostgresAdmissionError::Audit(PostgresSecurityAuditError::Transaction(_)) => (
            ErrorCode::DependencyUnavailable,
            true,
            "PostgreSQL admission transaction outcome is unavailable",
        ),
        PostgresAdmissionError::Ledger(error) => return map_ledger(error, correlation_id),
        PostgresAdmissionError::WorkspaceAssignmentPending => (
            ErrorCode::DependencyUnavailable,
            true,
            "Conversation workspace assignment is pending; retry after migration",
        ),
        PostgresAdmissionError::ConversationUnavailable => (
            ErrorCode::ConversationEpochStale,
            false,
            "Conversation admission authority changed",
        ),
        PostgresAdmissionError::ParticipantUnauthorized => (
            ErrorCode::ParticipantNotAuthorized,
            false,
            "Participant is not authorized for this Conversation",
        ),
        PostgresAdmissionError::AgentExecutorRequired => (
            ErrorCode::AgentNotAvailable,
            false,
            "The admitted Agent executor is unavailable",
        ),
        PostgresAdmissionError::MaintenanceAuthorityMismatch => (
            ErrorCode::ParticipantNotAuthorized,
            false,
            "Maintenance Run authority is unavailable",
        ),
        PostgresAdmissionError::ContentRefConflict => (
            ErrorCode::ContentIntegrityFailed,
            false,
            "Message content identity or integrity validation failed",
        ),
        PostgresAdmissionError::ExecutionGrantReplay => (
            ErrorCode::IdempotencyConflict,
            false,
            "Execution Grant was already consumed by another admission",
        ),
        PostgresAdmissionError::ExecutionGrantInvalid => (
            ErrorCode::InvalidCommand,
            false,
            "Execution Grant route chain is invalid",
        ),
        PostgresAdmissionError::MemorySelectionInvalid => (
            ErrorCode::RuntimeSnapshotInvalid,
            false,
            "Runtime Snapshot Memory selection is stale or unauthorized",
        ),
        PostgresAdmissionError::InstinctSelectionInvalid => (
            ErrorCode::RuntimeSnapshotInvalid,
            false,
            "Runtime Snapshot Instinct selection is stale or unauthorized",
        ),
        PostgresAdmissionError::RetrySourceInvalid => (
            ErrorCode::PolicyDenied,
            false,
            "Retry source is not the expected terminal failed Chat Run",
        ),
        PostgresAdmissionError::StandingGoalInvariant
        | PostgresAdmissionError::AttemptNumberOutOfRange(_)
        | PostgresAdmissionError::AuditIdentityMissing
        | PostgresAdmissionError::AuditContract(_)
        | PostgresAdmissionError::Audit(_) => (
            ErrorCode::InternalInvariantViolated,
            false,
            "Admission persistence contract is invalid",
        ),
    };
    runtime_error(code, correlation_id, retryable, safe_message)
}

fn map_admission_database(error: &sqlx::Error, correlation_id: CorrelationId) -> RuntimeError {
    match classify_database_failure(error) {
        PostgresFailureClass::Unavailable => runtime_error(
            ErrorCode::DependencyUnavailable,
            correlation_id,
            true,
            "PostgreSQL admission store is unavailable",
        ),
        PostgresFailureClass::Invalid => runtime_error(
            ErrorCode::InternalInvariantViolated,
            correlation_id,
            false,
            "Admission persistence contract is invalid",
        ),
    }
}

pub(super) fn not_found(code: ErrorCode, correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        code,
        correlation_id,
        false,
        "Runtime resource was not found",
    )
}

pub(super) fn map_ledger(
    error: PostgresLedgerError,
    correlation_id: CorrelationId,
) -> RuntimeError {
    match error {
        PostgresLedgerError::Contract(LedgerError::IdempotencyConflict) => runtime_error(
            ErrorCode::IdempotencyConflict,
            correlation_id,
            false,
            "Command idempotency identity conflicts",
        ),
        PostgresLedgerError::Contract(LedgerError::HomeEpochMismatch) => runtime_error(
            ErrorCode::ConversationEpochStale,
            correlation_id,
            false,
            "Conversation ownership epoch is stale",
        ),
        PostgresLedgerError::Contract(LedgerError::SequenceConflict { .. }) => runtime_error(
            ErrorCode::ConversationSeqConflict,
            correlation_id,
            false,
            "Conversation sequence has advanced",
        ),
        _ => dependency(correlation_id),
    }
}
