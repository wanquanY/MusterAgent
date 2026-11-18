/// Closed classification for PostgreSQL failures crossing a retry boundary.
///
/// A SQL statement rejected by PostgreSQL is normally deterministic: replaying
/// the same command cannot fix a constraint, type, permission, or data error.
/// Only transport/pool failures and PostgreSQL's documented transient SQLSTATE
/// classes are retryable.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum PostgresFailureClass {
    Unavailable,
    Invalid,
}

pub(crate) fn classify(error: &sqlx::Error) -> PostgresFailureClass {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => PostgresFailureClass::Unavailable,
        sqlx::Error::Database(database) => database
            .code()
            .as_deref()
            .map_or(PostgresFailureClass::Invalid, classify_sqlstate),
        _ => PostgresFailureClass::Invalid,
    }
}

/// Closed driver-side cause for the `database.failure` log field. Driver
/// messages can carry SQL, bind values, hosts or filenames and are never logged.
pub(crate) fn safe_failure(error: &sqlx::Error) -> &'static str {
    match error {
        sqlx::Error::Database(_) => "database",
        sqlx::Error::PoolTimedOut => "pool_timeout",
        sqlx::Error::PoolClosed => "pool_closed",
        sqlx::Error::Io(_) => "io",
        sqlx::Error::Tls(_) => "tls",
        sqlx::Error::WorkerCrashed => "worker_crashed",
        sqlx::Error::BeginFailed => "begin_failed",
        sqlx::Error::RowNotFound => "row_not_found",
        sqlx::Error::Protocol(_) => "protocol",
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) | sqlx::Error::Encode(_) => {
            "codec"
        }
        _ => "other",
    }
}

fn classify_sqlstate(code: &str) -> PostgresFailureClass {
    if code.starts_with("08")
        || matches!(
            code,
            // Transaction rollback / concurrency conflicts.
            "40001" | "40P01"
                // Lock not available and object not in prerequisite state
                // during recovery.
                | "55P03" | "55006"
                // Query cancelled, including statement_timeout. The caller's
                // cancellation lifecycle governs whether to retry or stop.
                | "57014"
                // Resource exhaustion and server lifecycle states.
                | "53000" | "53100" | "53200" | "53300" | "53400"
                | "57P01" | "57P02" | "57P03"
        )
    {
        PostgresFailureClass::Unavailable
    } else {
        PostgresFailureClass::Invalid
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresFailureClass, classify_sqlstate};

    #[test]
    fn only_transient_sqlstates_are_retryable() {
        for code in ["08006", "40001", "40P01", "55P03", "53300", "57P03"] {
            assert_eq!(classify_sqlstate(code), PostgresFailureClass::Unavailable);
        }
        for code in [
            "22003", "23502", "23503", "23505", "23514", "42501", "42703", "P0001",
        ] {
            assert_eq!(classify_sqlstate(code), PostgresFailureClass::Invalid);
        }
    }

    #[test]
    fn query_cancellation_agrees_with_the_governed_transaction_classifier() {
        // PostgreSQL uses the same SQLSTATE for statement_timeout and explicit
        // query cancellation. Caller cancellation decides whether to stop;
        // neither condition is evidence of a corrupt schema or persisted row.
        assert_eq!(
            crate::transaction_policy::classify_sqlstate("57014"),
            crate::PostgresFailureClass::StatementTimeout
        );
        assert_eq!(
            classify_sqlstate("57014"),
            PostgresFailureClass::Unavailable
        );
    }
}
