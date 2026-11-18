//! Closed diagnostics: never format a database error, provider reference, or payload.

use super::{InvocationId, ModelContractError, PostgresModelInvocationError};

pub(super) fn completion_failed(invocation_id: InvocationId, error: &PostgresModelInvocationError) {
    let code = error_key(error);
    let invocation_id = invocation_id.to_string();
    tracing::error!(
        invocation_id = invocation_id.as_str(),
        model.persistence.error = code,
        "Model terminal transaction failed"
    );
    if let Some(database) = database_error(error)
        && let Some(database) = database.as_database_error()
        && let Some(sqlstate) = database.code()
    {
        tracing::error!(
            invocation_id = invocation_id.as_str(),
            model.persistence.error = code,
            database.sqlstate = sqlstate.as_ref(),
            "Model terminal database rejection"
        );
    }
}

fn database_error(error: &PostgresModelInvocationError) -> Option<&sqlx::Error> {
    match error {
        PostgresModelInvocationError::Sql(error)
        | PostgresModelInvocationError::Transaction(crate::PostgresTransactionError::Database(
            error,
        ))
        | PostgresModelInvocationError::Audit(crate::PostgresSecurityAuditError::Database(error))
        | PostgresModelInvocationError::Audit(crate::PostgresSecurityAuditError::Transaction(
            crate::PostgresTransactionError::Database(error),
        )) => Some(error),
        _ => None,
    }
}

fn error_key(error: &PostgresModelInvocationError) -> &'static str {
    match error {
        PostgresModelInvocationError::Contract(error) => match error {
            ModelContractError::RequestHashMismatch => "request_hash_mismatch",
            ModelContractError::TerminalTimestampMismatch => "terminal_timestamp_mismatch",
            ModelContractError::StartedContainsOutcome => "started_contains_outcome",
            ModelContractError::SucceededOutcomeIncomplete => "succeeded_outcome_incomplete",
            ModelContractError::InvalidEarlyProviderCause => "invalid_early_provider_cause",
            ModelContractError::FailedWithoutError => "failed_without_error",
            ModelContractError::CancelledContainsFailure => "cancelled_contains_failure",
            ModelContractError::UsageTotalBelowComponents => "usage_total_below_components",
            ModelContractError::CacheWriteOneHourExceedsTotal => {
                "cache_write_one_hour_exceeds_total"
            }
            ModelContractError::MeteringPublicationInvalid => "metering_publication_invalid",
            ModelContractError::InvocationNotTerminal => "invocation_not_terminal",
            ModelContractError::UsageFactPublicationMismatch => "usage_fact_publication_mismatch",
            ModelContractError::UsageFactIdentityMismatch => "usage_fact_identity_mismatch",
            ModelContractError::AdmissionRouteInvalid => "admission_route_invalid",
            ModelContractError::AdmissionRouteMismatch => "admission_route_mismatch",
        },
        PostgresModelInvocationError::BeginRequiresStarted => "begin_requires_started",
        PostgresModelInvocationError::IdempotencyConflict => "idempotency_conflict",
        PostgresModelInvocationError::TerminalConflict => "terminal_conflict",
        PostgresModelInvocationError::CorruptState => "corrupt_state",
        PostgresModelInvocationError::NumericRange => "numeric_range",
        PostgresModelInvocationError::Protection(_) => "protection",
        PostgresModelInvocationError::Digest(_) => "digest",
        PostgresModelInvocationError::Audit(_) => "audit",
        PostgresModelInvocationError::Sql(_) => "database",
        PostgresModelInvocationError::Transaction(_) => "transaction",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_do_not_format_underlying_errors() {
        let error = PostgresModelInvocationError::Sql(sqlx::Error::Protocol(
            "secret provider credential and customer image".to_owned(),
        ));
        assert_eq!(error_key(&error), "database");
        assert!(database_error(&error).is_some());
        assert_eq!(
            error_key(&PostgresModelInvocationError::Contract(
                ModelContractError::UsageFactIdentityMismatch,
            )),
            "usage_fact_identity_mismatch"
        );
    }
}
