//! Preserve original supervision failure identity at the model boundary.
//! Only the original Journal can commit failure/retry or prove a call closed.
use super::*;
use musteragent_execution_liveness::LivenessError;

fn ownership_interruption(error: &LivenessJournalError) -> Option<CancellationCause> {
    if error.code == ErrorCode::AttemptLeaseStale {
        return Some(CancellationCause::LeaseLost);
    }
    // Membership revocation and stale original observation authority must be
    // reconciled by their owner, not terminalized as a new business failure.
    if error.code == ErrorCode::PolicyDenied
        || matches!(
            error.rejection,
            Some(
                LivenessError::BindingMismatch
                    | LivenessError::RevisionMismatch
                    | LivenessError::ClockEpochChanged
                    | LivenessError::ScopeClosed
                    | LivenessError::InvalidPhase
                    | LivenessError::ReconciliationRequired
                    | LivenessError::FreshRecoveryProofRequired
                    | LivenessError::RecoveryProofMismatch
                    | LivenessError::RecoveryWindowExpired
                    | LivenessError::RecoveryStillPending
                    | LivenessError::WaitMismatch
                    | LivenessError::StaleWait
            )
        )
    {
        return Some(CancellationCause::SystemShutdown);
    }
    None
}

fn classify_supervision_error(
    error: ExecutionSupervisionError,
    origin: ExecutionSupervisionFailureOrigin,
) -> (LivenessJournalError, Option<CancellationCause>) {
    // Only the reactor's original host acquire/check can establish a rejected
    // admission. A Journal/clock/boundary failure remains infrastructure even
    // if its code is identical or emergency cleanup records a physical stop.
    let host_admission_failed = origin == ExecutionSupervisionFailureOrigin::HostFence
        && matches!(&error, ExecutionSupervisionError::Journal(_));
    let error = error.into_journal_error();
    let interruption = ownership_interruption(&error)
        .or((!host_admission_failed).then_some(CancellationCause::SystemShutdown));
    (error, interruption)
}

impl DurableAgentLoop {
    pub(super) async fn finish_supervision_error(
        &self,
        context: ModelStepContext<'_>,
        error: ExecutionSupervisionError,
        origin: ExecutionSupervisionFailureOrigin,
        boundary_polled: bool,
        observed: &mut Option<ModelTelemetry>,
    ) -> ModelStep {
        let (error, interruption) = classify_supervision_error(error, origin);
        *observed = Some((TelemetryOutcome::Error, Some(error.code)));
        if error.code == ErrorCode::AttemptLeaseStale {
            return interrupted(CancellationCause::LeaseLost);
        }
        if context.supervisor.is_cancelled() {
            let cause = context
                .supervisor
                .cancellation_cause()
                .unwrap_or(CancellationCause::UserRequested);
            if !boundary_polled {
                // There was no provider poll. The original terminal transaction
                // derives actual invocation/usage facts and checks its fence.
                return self
                    .stop_for_cancellation(context.attempt, context.supervisor, cause, None)
                    .await
                    .into();
            }
            return interrupted(cause);
        }
        if let Some(cause) = interruption {
            return interrupted(cause);
        }
        if boundary_polled {
            // Host rejection is only sufficient before physical work can run.
            // Keep original recovery if that invariant cannot be established.
            return interrupted(CancellationCause::SystemShutdown);
        }
        // A host/preflight failure is not a Provider response: do not synthesize
        // usage, output, retry hints or fallback routes. The original model
        // failure table and RecoveryBudget decide nonretryable terminal vs
        // durable retry backoff; no lease-expiry loop or new timer is introduced.
        self.finish_activity_failure(context, port(error.code, error.retryable).into())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supervision_observation_timeouts_keep_original_recovery_semantics() {
        for cause in [
            ExecutionSupervisionError::JournalRequestExpired,
            ExecutionSupervisionError::BoundaryDrainExpired,
        ] {
            let (error, interruption) =
                classify_supervision_error(cause, ExecutionSupervisionFailureOrigin::OriginalOwner);
            assert_eq!(error.code, ErrorCode::DependencyUnavailable);
            assert!(error.retryable);
            assert_eq!(interruption, Some(CancellationCause::SystemShutdown));
        }
        // The same normalized error code from an explicit host failure still
        // belongs to the original bounded model recovery policy.
        let (error, interruption) = classify_supervision_error(
            ExecutionSupervisionError::Journal(LivenessJournalError {
                code: ErrorCode::DependencyUnavailable,
                retryable: true,
                rejection: None,
            }),
            ExecutionSupervisionFailureOrigin::HostFence,
        );
        assert_eq!(error.code, ErrorCode::DependencyUnavailable);
        assert!(error.retryable);
        assert_eq!(interruption, None);
    }

    #[test]
    fn supervision_origin_preserves_normalized_journal_failures() {
        for (code, retryable) in [
            (ErrorCode::DependencyUnavailable, true),
            (ErrorCode::DependencyUnavailable, false),
            (ErrorCode::InternalInvariantViolated, false),
        ] {
            for (origin, expected) in [
                (
                    ExecutionSupervisionFailureOrigin::OriginalOwner,
                    Some(CancellationCause::SystemShutdown),
                ),
                (ExecutionSupervisionFailureOrigin::HostFence, None),
            ] {
                let (error, interruption) = classify_supervision_error(
                    ExecutionSupervisionError::Journal(LivenessJournalError {
                        code,
                        retryable,
                        rejection: None,
                    }),
                    origin,
                );
                assert_eq!(error.code, code);
                assert_eq!(error.retryable, retryable);
                assert_eq!(interruption, expected);
            }
        }
    }

    #[test]
    fn supervision_ownership_rejections_keep_original_recovery_semantics() {
        assert_eq!(
            ownership_interruption(&LivenessJournalError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
                rejection: None,
            }),
            Some(CancellationCause::LeaseLost)
        );
        for rejection in [
            LivenessError::BindingMismatch,
            LivenessError::RevisionMismatch,
            LivenessError::ClockEpochChanged,
            LivenessError::RecoveryStillPending,
            LivenessError::ReconciliationRequired,
        ] {
            assert_eq!(
                ownership_interruption(&LivenessJournalError {
                    code: ErrorCode::InvalidCommand,
                    retryable: false,
                    rejection: Some(rejection),
                }),
                Some(CancellationCause::SystemShutdown)
            );
        }
        for (code, retryable) in [
            (ErrorCode::InternalInvariantViolated, false),
            (ErrorCode::DependencyUnavailable, true),
            (ErrorCode::DependencyTimeout, true),
        ] {
            assert_eq!(
                ownership_interruption(&LivenessJournalError {
                    code,
                    retryable,
                    rejection: None
                }),
                None
            );
        }
    }
}
