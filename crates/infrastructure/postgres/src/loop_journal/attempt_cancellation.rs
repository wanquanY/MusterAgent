use super::*;

impl AttemptCancellationStore for PostgresAgentLoopJournal {
    fn acknowledge_user_cancellation(
        &self,
        attempt: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<(), AttemptControlError>> {
        let attempt = attempt.clone();
        Box::pin(async move {
            AgentLoopJournal::acknowledge_cancellation(self, &attempt, None)
                .await
                .map(|_| ())
                .map_err(|error| {
                    tracing::error!(
                        run_id = %attempt.run_id,
                        attempt_id = %attempt.attempt_id,
                        error_code = ?error.code,
                        retryable = error.retryable,
                        "cancellation recovery failed to terminalize Run",
                    );
                    match error.code {
                        ErrorCode::AttemptLeaseStale => AttemptControlError::LeaseLost,
                        ErrorCode::DependencyUnavailable => {
                            AttemptControlError::DatabaseUnavailable
                        }
                        _ => AttemptControlError::ContractViolation,
                    }
                })
        })
    }
}
