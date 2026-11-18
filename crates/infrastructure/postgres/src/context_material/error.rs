use thiserror::Error;

use crate::database_failure::{PostgresFailureClass, classify as classify_database_failure};

#[derive(Debug, Error)]
pub enum PostgresContextMaterialError {
    #[error("Context material contains corrupt or inconsistent durable state")]
    CorruptState,
    #[error("Context material contains corrupt or inconsistent durable state at {0}")]
    CorruptStateAt(&'static str),
    #[error("Context material requires at least one canonical model input")]
    NoModelInput,
    #[error("Context material Attempt lease or fencing token is stale")]
    StaleFence,
    #[error("Context material content is unavailable")]
    Content(#[from] musteragent_runtime_content::ContentStoreError),
    #[error("Context material PostgreSQL query failed")]
    Database(#[from] sqlx::Error),
    #[error("Context material PostgreSQL transaction failed")]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("Dialogue Context source failed at {stage}")]
    DialogueSource {
        stage: &'static str,
        #[source]
        error: musteragent_runtime_session::DialogueStoreError,
    },
    #[error("Dialogue Context authority failed at {stage}")]
    DialogueAuthority {
        stage: &'static str,
        #[source]
        error: musteragent_runtime_session::DialogueStoreError,
    },
    #[error("Context material numeric value exceeds the supported range")]
    Numeric(#[from] std::num::TryFromIntError),
}

impl PostgresContextMaterialError {
    pub(super) fn dialogue_source(
        stage: &'static str,
        error: musteragent_runtime_session::DialogueStoreError,
    ) -> Self {
        Self::DialogueSource { stage, error }
    }

    pub(super) fn dialogue_authority(
        stage: &'static str,
        error: musteragent_runtime_session::DialogueStoreError,
    ) -> Self {
        Self::DialogueAuthority { stage, error }
    }

    pub(super) fn plan_error(&self) -> musteragent_context::ContextPlanError {
        use musteragent_runtime_session::DialogueStoreError;
        if matches!(
            self,
            Self::DialogueAuthority {
                error: DialogueStoreError::Unavailable
                    | DialogueStoreError::Inactive
                    | DialogueStoreError::RevisionConflict,
                ..
            }
        ) {
            return musteragent_context::ContextPlanError {
                early_provider_cause: None,
                code: musteragent_runtime_contract::ErrorCode::PolicyDenied,
                retry_after_ms: None,
                retryable: false,
            };
        }
        if self.retryable() {
            musteragent_context::ContextPlanError::unavailable()
        } else {
            musteragent_context::ContextPlanError::invariant()
        }
    }

    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::CorruptState | Self::CorruptStateAt(_) => "corrupt_state",
            Self::NoModelInput => "no_model_input",
            Self::StaleFence => "stale_fence",
            Self::Content(error) => match error {
                musteragent_runtime_content::ContentStoreError::NotFound => "content_not_found",
                musteragent_runtime_content::ContentStoreError::Unauthorized => {
                    "content_unauthorized"
                }
                musteragent_runtime_content::ContentStoreError::TooLarge => "content_too_large",
                musteragent_runtime_content::ContentStoreError::LengthMismatch => {
                    "content_length_mismatch"
                }
                musteragent_runtime_content::ContentStoreError::HashMismatch => {
                    "content_hash_mismatch"
                }
                musteragent_runtime_content::ContentStoreError::IdentityConflict => {
                    "content_identity_conflict"
                }
                musteragent_runtime_content::ContentStoreError::Unavailable => {
                    "content_dependency_unavailable"
                }
                musteragent_runtime_content::ContentStoreError::Invariant => "content_invariant",
            },
            Self::Database(error) => match classify_database_failure(error) {
                PostgresFailureClass::Unavailable => "database_unavailable",
                PostgresFailureClass::Invalid => "database_invalid",
            },
            Self::Transaction(_) => "database_transaction",
            Self::DialogueAuthority { .. } => "dialogue_authority_rejected",
            Self::DialogueSource { error, .. } => match error {
                musteragent_runtime_session::DialogueStoreError::Infrastructure => {
                    "dialogue_source_unavailable"
                }
                _ => "dialogue_source_rejected",
            },
            Self::Numeric(_) => "numeric",
        }
    }

    pub(super) const fn stage(&self) -> Option<&'static str> {
        match self {
            Self::CorruptStateAt(stage)
            | Self::DialogueSource { stage, .. }
            | Self::DialogueAuthority { stage, .. } => Some(stage),
            _ => None,
        }
    }

    pub(super) fn retryable(&self) -> bool {
        match self {
            Self::Content(musteragent_runtime_content::ContentStoreError::Unavailable)
            | Self::StaleFence => true,
            // Transaction failure_class also drives telemetry and definite-abort
            // replay policy. Its coarse Other is not a Context corruption verdict.
            Self::Database(error)
            | Self::Transaction(crate::PostgresTransactionError::Database(error)) => {
                classify_database_failure(error) == PostgresFailureClass::Unavailable
            }
            Self::Transaction(
                crate::PostgresTransactionError::DeadlineExceeded { .. }
                | crate::PostgresTransactionError::CommitOutcomeUnknown { .. },
            ) => true,
            Self::Transaction(crate::PostgresTransactionError::Policy(_)) => false,
            Self::DialogueSource { error, .. } | Self::DialogueAuthority { error, .. } => matches!(
                error,
                musteragent_runtime_session::DialogueStoreError::Infrastructure
            ),
            Self::Content(_)
            | Self::CorruptState
            | Self::CorruptStateAt(_)
            | Self::NoModelInput
            | Self::Numeric(_) => false,
        }
    }

    pub(super) fn database_sqlstate(&self) -> Option<String> {
        match self {
            Self::Database(sqlx::Error::Database(error))
            | Self::Transaction(crate::PostgresTransactionError::Database(
                sqlx::Error::Database(error),
            )) => error.code().map(|code| code.into_owned()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use musteragent_runtime_content::ContentStoreError;
    use musteragent_runtime_session::DialogueStoreError;

    use super::PostgresContextMaterialError;

    #[test]
    fn governed_read_deadline_retains_transient_classification() {
        let error = PostgresContextMaterialError::Transaction(
            crate::PostgresTransactionError::DeadlineExceeded {
                phase: musteragent_telemetry::DbTransactionPhase::Acquire,
            },
        );
        assert_eq!(error.kind(), "database_transaction");
        assert!(error.retryable());
        assert_eq!(error.database_sqlstate(), None);
    }

    #[test]
    fn only_transient_content_unavailability_is_retryable() {
        let unavailable = PostgresContextMaterialError::Content(ContentStoreError::Unavailable);
        assert_eq!(unavailable.kind(), "content_dependency_unavailable");
        assert!(unavailable.retryable());

        for (error, expected_kind) in [
            (ContentStoreError::NotFound, "content_not_found"),
            (ContentStoreError::Unauthorized, "content_unauthorized"),
            (ContentStoreError::LengthMismatch, "content_length_mismatch"),
            (ContentStoreError::HashMismatch, "content_hash_mismatch"),
            (ContentStoreError::Invariant, "content_invariant"),
        ] {
            let error = PostgresContextMaterialError::Content(error);
            assert_eq!(error.kind(), expected_kind);
            assert!(!error.retryable());
        }
    }

    #[test]
    fn dialogue_source_errors_preserve_stage_and_closed_retry_semantics() {
        for error in [
            DialogueStoreError::Unavailable,
            DialogueStoreError::IdempotencyConflict,
            DialogueStoreError::RevisionConflict,
            DialogueStoreError::Inactive,
            DialogueStoreError::Corrupt,
            DialogueStoreError::Infrastructure,
        ] {
            let transient = error == DialogueStoreError::Infrastructure;
            let error = PostgresContextMaterialError::dialogue_source("dialogue_run_cut", error);
            assert_eq!(error.retryable(), transient);
            assert_eq!(error.stage(), Some("dialogue_run_cut"));
            assert_eq!(
                error.kind(),
                if transient {
                    "dialogue_source_unavailable"
                } else {
                    "dialogue_source_rejected"
                }
            );
        }
    }

    #[test]
    fn only_typed_authority_denials_are_policy_errors() {
        use musteragent_runtime_contract::ErrorCode;
        for source in [
            DialogueStoreError::Unavailable,
            DialogueStoreError::Inactive,
            DialogueStoreError::RevisionConflict,
            DialogueStoreError::Infrastructure,
            DialogueStoreError::Corrupt,
            DialogueStoreError::IdempotencyConflict,
        ] {
            let authority = PostgresContextMaterialError::dialogue_authority("any_stage", source);
            let expected = match source {
                DialogueStoreError::Unavailable
                | DialogueStoreError::Inactive
                | DialogueStoreError::RevisionConflict => ErrorCode::PolicyDenied,
                DialogueStoreError::Infrastructure => ErrorCode::DependencyUnavailable,
                _ => ErrorCode::InternalInvariantViolated,
            };
            assert_eq!(authority.plan_error().code, expected);
            assert_eq!(
                authority.plan_error().retryable,
                source == DialogueStoreError::Infrastructure
            );
            let content = PostgresContextMaterialError::dialogue_source("dialogue_run_cut", source);
            assert_ne!(
                content.plan_error().code,
                ErrorCode::PolicyDenied,
                "diagnostic strings never classify authority"
            );
        }
    }

    #[tokio::test]
    async fn transaction_wrapping_preserves_database_recovery_classification() {
        let pg = musteragent_testkit::IsolatedPostgres::start_with_pgvector()
            .await
            .unwrap();
        let pool = pg.pool().await.unwrap();
        for (code, retryable) in [
            ("57P01", true),
            ("57P02", true),
            ("57P03", true),
            ("53300", true),
            ("55006", true),
            ("57014", true),
            ("40001", true),
            ("55P03", true),
            ("42703", false),
            ("42501", false),
            ("23514", false),
        ] {
            for wrapped in [false, true] {
                let sql = format!(
                    "DO $$ BEGIN RAISE SQLSTATE '{code}' USING MESSAGE = 'isolated classification fixture'; END $$"
                );
                let database = sqlx::query(&sql).execute(&pool).await.unwrap_err();
                let error = if wrapped {
                    PostgresContextMaterialError::Transaction(
                        crate::PostgresTransactionError::Database(database),
                    )
                } else {
                    PostgresContextMaterialError::Database(database)
                };
                assert_eq!(error.database_sqlstate().as_deref(), Some(code));
                assert_eq!(error.retryable(), retryable, "{code}, wrapped={wrapped}");
            }
        }
        pool.close().await;
    }
}
