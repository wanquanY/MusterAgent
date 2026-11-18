//! Recovery budgets count failed operations, never ownership/lease attempts.
use std::num::NonZeroU32;

use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_run::RunEvent;
use musteragent_runtime_contract::RetryDisposition;
use musteragent_runtime_model::ModelEvent;
use musteragent_tool::ToolEvent;

/// A read of canonical recovery facts under the active Attempt fence.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct RecoveryBudget {
    /// Failed model starts on the current route, including preflight failures.
    pub provider_failures: u32,
    /// Retryable infrastructure failures since the last committed progress.
    pub no_progress_failures: u32,
    /// Semantic repair failures since the last completed model invocation.
    pub repair_failures: u32,
    /// Distance from the current candidate to the earliest transient failure in this Attempt.
    pub prior_transient_distance: u32,
}

impl RecoveryBudget {
    /// Semantic repair has its own durable budget. Network outages neither
    /// consume nor reset it; Persistent only extends dependency recovery.
    #[must_use]
    pub const fn permits_failure_retry(
        self,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
        failure: crate::ModelFailure,
    ) -> bool {
        self.permits_provider_retry(policy)
            && (!matches!(
                policy,
                musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
            ) || !matches!(
                failure.code,
                musteragent_runtime_contract::ErrorCode::ModelContextExceeded
                    | musteragent_runtime_contract::ErrorCode::ModelResponseInvalid
            ) || self.repair_failures < 2)
    }

    /// `max_attempts` includes the start that is currently failing.
    #[must_use]
    pub const fn permits_provider_retry(
        self,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> bool {
        policy.permits_retry(self.provider_failures)
    }

    #[must_use]
    pub const fn permits_port_retry(
        self,
        policy: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> bool {
        policy.permits_retry(self.no_progress_failures)
    }
}

/// Counts only semantic failures, across worker replacement and outages.
#[derive(Debug, Default)]
pub struct RepairRecovery {
    failures: u32,
    done: bool,
}
impl RepairRecovery {
    pub fn observe_invocation(
        &mut self,
        status: i64,
        code: Option<musteragent_runtime_contract::ErrorCode>,
    ) {
        if self.done {
            return;
        }
        if status == 2 {
            self.done = true;
        } else if status == 3
            && matches!(
                code,
                Some(
                    musteragent_runtime_contract::ErrorCode::ModelContextExceeded
                        | musteragent_runtime_contract::ErrorCode::ModelResponseInvalid
                )
            )
        {
            self.failures = self.failures.saturating_add(1);
        }
    }
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.done
    }
    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
    }
}

/// Reverse-order canonical reducer. InvocationFailed and AttemptFailed in the
/// same atomic provider retry are one provider failure, not a Port failure.
/// Paging does not alter the result; only explicit committed progress ends an
/// episode. Merely claiming a lease, waiting for backoff, or restarting cannot.
#[derive(Debug, Default)]
pub struct NoProgressRecovery {
    failures: u32,
    pending_attempt_failure: bool,
    done: bool,
}

impl NoProgressRecovery {
    pub fn observe(&mut self, payload: &LedgerPayload) {
        if self.done {
            return;
        }
        match payload {
            LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptFailed {
                retry_disposition: RetryDisposition::RetryNewAttempt,
                ..
            })) => {
                self.flush();
                self.pending_attempt_failure = true;
            }
            LedgerPayload::DomainEvent(DomainEvent::Model(ModelEvent::InvocationFailed {
                retry_disposition: RetryDisposition::RetryNewAttempt,
                ..
            })) => {
                self.pending_attempt_failure = false;
            }
            LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptClaimed { .. })) => {
                self.flush()
            }
            LedgerPayload::DomainEvent(DomainEvent::Model(ModelEvent::InvocationCompleted {
                ..
            }))
            | LedgerPayload::DomainEvent(DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                ..
            }))
            | LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptSuspended { .. }))
            | LedgerPayload::DomainEvent(DomainEvent::Interaction(_)) => {
                self.flush();
                self.done = true;
            }
            _ => {}
        }
    }

    fn flush(&mut self) {
        self.failures = self
            .failures
            .saturating_add(u32::from(self.pending_attempt_failure));
        self.pending_attempt_failure = false;
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.done
    }

    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
            .saturating_add(self.pending_attempt_failure as u32)
    }
}

/// Reconstruct one candidate round in reverse journal order. The calling
/// store stops at an older attempt, a successful turn or an unrelated fault.
#[derive(Default)]
pub struct ProviderRoundHistory {
    distance: u32,
    retry_distance: u32,
    done: bool,
}

impl ProviderRoundHistory {
    pub fn observe(
        &mut self,
        same_attempt: bool,
        status: i64,
        failure: Option<musteragent_runtime_contract::ErrorCode>,
    ) {
        use musteragent_runtime_contract::ErrorCode;
        if self.done {
            return;
        }
        if !same_attempt {
            self.done = true;
            return;
        }
        if status == 1 {
            return;
        }
        if status != 3 {
            self.done = true;
            return;
        }
        self.distance = self.distance.saturating_add(1);
        match failure {
            Some(ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout) => {
                self.retry_distance = self.distance
            }
            Some(
                ErrorCode::ModelPaymentRequired
                | ErrorCode::QuotaExceeded
                | ErrorCode::Unauthenticated,
            ) => {}
            _ => self.done = true,
        }
    }

    pub const fn complete(&self) -> bool {
        self.done
    }
    pub const fn prior_transient_distance(&self) -> u32 {
        self.retry_distance
    }
}

/// Minimal invocation facts used by both SQL adapters. Rows are fed in reverse
/// canonical start order; an undispatched abandonment cannot erase previous failures.
#[derive(Debug, Default)]
pub struct ProviderRecoveryHistory {
    route: Option<String>,
    failures: u32,
    done: bool,
}

impl ProviderRecoveryHistory {
    pub fn observe(&mut self, route: &str, status: i64, action_rejected: bool) {
        if self.done {
            return;
        }
        if self
            .route
            .as_deref()
            .is_some_and(|current| current != route)
        {
            self.done = true;
            return;
        }
        self.route.get_or_insert_with(|| route.to_owned());
        if status == 2 || action_rejected {
            self.done = true;
        } else if status == 3 {
            self.failures = self.failures.saturating_add(1);
        }
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.done
    }
    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
    }
}

/// Closed, value-free terminal text. Provider bodies and credentials never enter it.
#[must_use]
pub fn runtime_failure_message(
    code: musteragent_runtime_contract::ErrorCode,
    exhausted: Option<NonZeroU32>,
) -> String {
    match exhausted {
        Some(limit) => format!(
            "Runtime failure: {}. Recovery exhausted after {} failed attempts.",
            code.as_str(),
            limit.get()
        ),
        None => format!("Runtime failure: {}.", code.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn attempt_failure() -> LedgerPayload {
        LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptFailed {
            attempt_number: musteragent_identity::AttemptNumber::new(90).expect("attempt"),
            error: error(),
            retry_disposition: RetryDisposition::RetryNewAttempt,
        }))
    }
    fn error() -> musteragent_runtime_contract::ErrorInfo {
        let code = musteragent_runtime_contract::ErrorCode::DependencyUnavailable;
        musteragent_runtime_contract::ErrorInfo {
            code,
            category: code.category(),
            safe_message: musteragent_runtime_contract::SafeDisplayText::new("dependency")
                .expect("message"),
            retryable: true,
            dependency_ref: None,
            diagnostic_ref: None,
        }
    }
    #[test]
    fn semantic_repair_is_bounded_independently_of_long_outages() {
        use musteragent_runtime_contract::ErrorCode;
        let policy = musteragent_runtime_snapshot::RecoveryPolicySnapshot::production_default();
        let mut repairs = RepairRecovery::default();
        repairs.observe_invocation(3, Some(ErrorCode::ModelContextExceeded));
        for _ in 0..100 {
            repairs.observe_invocation(3, Some(ErrorCode::DependencyUnavailable));
        }
        repairs.observe_invocation(3, Some(ErrorCode::ModelContextExceeded));
        let budget = RecoveryBudget {
            provider_failures: 102,
            repair_failures: repairs.failures(),
            ..RecoveryBudget::default()
        };
        assert!(!budget.permits_failure_retry(
            policy,
            crate::ModelFailure {
                code: ErrorCode::ModelContextExceeded,
                retryable: true
            }
        ));
        assert!(budget.permits_failure_retry(
            policy,
            crate::ModelFailure {
                code: ErrorCode::DependencyUnavailable,
                retryable: true
            }
        ));
        let mut recovered = RepairRecovery::default();
        recovered.observe_invocation(2, None);
        recovered.observe_invocation(3, Some(ErrorCode::ModelContextExceeded));
        assert_eq!(
            recovered.failures(),
            0,
            "completed output ends the prior repair episode"
        );
    }

    #[test]
    fn provider_and_port_failures_are_independent_and_suspend_is_progress() {
        let mut state = NoProgressRecovery::default();
        state.observe(&attempt_failure());
        state.observe(&LedgerPayload::DomainEvent(DomainEvent::Model(
            ModelEvent::InvocationFailed {
                invocation_id: musteragent_identity::InvocationId::generate(),
                error: error(),
                retry_disposition: RetryDisposition::RetryNewAttempt,
            },
        )));
        assert_eq!(state.failures(), 0, "provider retry is not a Port failure");
        state.observe(&attempt_failure());
        state.observe(&attempt_failure());
        state.observe(&LedgerPayload::DomainEvent(DomainEvent::Run(
            RunEvent::AttemptSuspended {
                attempt_number: musteragent_identity::AttemptNumber::new(80).expect("attempt"),
                interaction_id: None,
                invocation_id: None,
            },
        )));
        state.observe(&attempt_failure());
        assert_eq!(
            state.failures(),
            2,
            "only failures after real progress belong to this episode"
        );
        assert!(state.is_complete());
    }
    #[test]
    fn ownership_history_does_not_change_failure_budget() {
        let budget = RecoveryBudget::default();
        assert!(budget.permits_provider_retry(NonZeroU32::new(8).expect("limit").into()));
        assert!(!budget.permits_provider_retry(NonZeroU32::MIN.into()));
        assert!(
            !RecoveryBudget {
                provider_failures: 7,
                ..budget
            }
            .permits_provider_retry(NonZeroU32::new(8).expect("limit").into())
        );
    }
    #[test]
    fn provider_recovery_survives_undispatched_restarts_and_resets_on_response() {
        let mut state = ProviderRecoveryHistory::default();
        for status in [1, 5, 3, 5, 3, 2, 3] {
            state.observe("route-a", status, false);
        }
        assert_eq!(state.failures(), 2);
        assert!(state.is_complete());
    }
    #[test]
    fn fallback_and_action_correction_have_independent_provider_episodes() {
        let mut route = ProviderRecoveryHistory::default();
        route.observe("fallback", 1, false);
        route.observe("primary", 3, false);
        assert_eq!(route.failures(), 0);
        let mut correction = ProviderRecoveryHistory::default();
        correction.observe("primary", 3, false);
        correction.observe("primary", 3, true);
        correction.observe("primary", 3, false);
        assert_eq!(correction.failures(), 1);
    }
}
