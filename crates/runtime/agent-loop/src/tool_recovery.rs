//! Recovery from committed Tool observations, independent of model/Attempt IDs.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::ErrorCode;

/// Includes the first failed execution. Successful work has no lifetime cap.
pub const REPEATED_TOOL_FAILURE_LIMIT: u32 = 3;
/// Different repair attempts are bounded independently of exact arguments.
pub const CAPABILITY_FAILURE_LIMIT: u32 = 8;

/// Original Home facts, never inferred from Assistant prose or result JSON.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolRecoveryOutcome {
    Succeeded,
    Failed(ErrorCode),
    Indeterminate {
        observed_error: Option<ErrorCode>,
        may_have_side_effect: bool,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolRecoveryStopReason {
    RepeatedOperationFailure,
    CapabilityUnavailable,
    CapabilityRecoveryExhausted,
}

/// Stops further automatic work, without rewriting any original Tool outcome.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ToolRecoveryStop {
    pub code: ErrorCode,
    pub reason: ToolRecoveryStopReason,
    pub observed_failures: u32,
}

/// Reverse canonical model/call order. A successful call ends that capability's
/// failure episode; success of another capability, model prose, a new lease or
/// different arguments do not. The original invocation's current terminal fact
/// must be read once, so successful reconciliation replaces an unknown observation.
#[derive(Debug, Default)]
pub struct ToolRecoveryHistory {
    recovered: BTreeSet<String>,
    dependency_failures: BTreeMap<String, u32>,
    capability_failures: BTreeMap<String, u32>,
    operation_failures: BTreeMap<(String, Sha256, ErrorCode), u32>,
}

impl ToolRecoveryHistory {
    #[must_use]
    pub fn observe(
        &mut self,
        tool: &str,
        request_hash: Sha256,
        outcome: ToolRecoveryOutcome,
    ) -> Option<ToolRecoveryStop> {
        // An uncertain write is model feedback, not a Run stop: the original
        // invocation keeps its unknown outcome and the model is told to inspect
        // the target before repeating. It counts toward the same bounded
        // failure episodes as any other failure, so repeating the same
        // uncertain operation still stops automatic work.
        if self.recovered.contains(tool) {
            return None;
        }
        let code = match outcome {
            ToolRecoveryOutcome::Succeeded => {
                self.recovered.insert(tool.to_owned());
                return None;
            }
            ToolRecoveryOutcome::Failed(code) => code,
            ToolRecoveryOutcome::Indeterminate { observed_error, .. } => {
                observed_error.unwrap_or(ErrorCode::DependencyUnavailable)
            }
        };
        let count = self
            .operation_failures
            .entry((tool.to_owned(), request_hash, code))
            .or_default();
        *count = count.saturating_add(1);
        if *count >= REPEATED_TOOL_FAILURE_LIMIT {
            return Some(ToolRecoveryStop {
                code: historical_stop_code(code),
                reason: ToolRecoveryStopReason::RepeatedOperationFailure,
                observed_failures: *count,
            });
        }
        // Parameter changes cannot cure an unavailable dependency. Unknown
        // reads are safe to retry within this same bounded recovery episode.
        if matches!(
            code,
            ErrorCode::DependencyUnavailable | ErrorCode::DependencyTimeout
        ) || matches!(outcome, ToolRecoveryOutcome::Indeterminate { .. })
        {
            let count = self.dependency_failures.entry(tool.to_owned()).or_default();
            *count = count.saturating_add(1);
            if *count >= REPEATED_TOOL_FAILURE_LIMIT {
                return Some(ToolRecoveryStop {
                    code: historical_stop_code(code),
                    reason: ToolRecoveryStopReason::CapabilityUnavailable,
                    observed_failures: *count,
                });
            }
        }
        let count = self.capability_failures.entry(tool.to_owned()).or_default();
        *count = count.saturating_add(1);
        if *count >= CAPABILITY_FAILURE_LIMIT {
            return Some(ToolRecoveryStop {
                code: historical_stop_code(code),
                reason: ToolRecoveryStopReason::CapabilityRecoveryExhausted,
                observed_failures: *count,
            });
        }
        None
    }
}

/// Historical Tool evidence is not authority over the current Attempt. Lease
/// loss observed by an earlier producer must stop automatic work through the
/// ordinary non-retryable failure path, not interrupt every successor lease.
/// The original error and uncertainty remain in that Tool's durable outcome.
const fn historical_stop_code(code: ErrorCode) -> ErrorCode {
    match code {
        ErrorCode::AttemptLeaseStale => ErrorCode::DependencyUnavailable,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(value: u8) -> Sha256 {
        Sha256::digest([value])
    }

    #[test]
    fn repeated_original_operation_is_bounded_without_attempt_or_call_ids() {
        let mut history = ToolRecoveryHistory::default();
        for round in 0..3 {
            let stop = history.observe(
                "artifacts.deliver@1",
                hash(1),
                ToolRecoveryOutcome::Failed(ErrorCode::InvalidCommand),
            );
            assert_eq!(stop.is_some(), round == 2);
        }
    }

    #[test]
    fn changing_arguments_and_unrelated_success_do_not_reset_dependency_failures() {
        let mut history = ToolRecoveryHistory::default();
        for round in 0..3 {
            assert!(
                history
                    .observe(
                        "terminal.exec@1",
                        hash(round),
                        ToolRecoveryOutcome::Succeeded
                    )
                    .is_none()
            );
            let outcome = if round == 1 {
                ToolRecoveryOutcome::Indeterminate {
                    observed_error: Some(ErrorCode::DependencyUnavailable),
                    may_have_side_effect: false,
                }
            } else {
                ToolRecoveryOutcome::Failed(ErrorCode::DependencyTimeout)
            };
            let stop = history.observe("files.list@4", hash(round), outcome);
            assert_eq!(stop.is_some(), round == 2);
            if let Some(stop) = stop {
                assert_eq!(stop.reason, ToolRecoveryStopReason::CapabilityUnavailable);
            }
        }
    }

    #[test]
    fn successful_capability_recovery_allows_long_productive_runs() {
        let mut history = ToolRecoveryHistory::default();
        assert!(
            history
                .observe("files.list@4", hash(1), ToolRecoveryOutcome::Succeeded)
                .is_none()
        );
        for _ in 0..1000 {
            assert!(
                history
                    .observe(
                        "files.list@4",
                        hash(1),
                        ToolRecoveryOutcome::Failed(ErrorCode::DependencyUnavailable)
                    )
                    .is_none()
            );
        }
    }

    #[test]
    fn different_validation_repairs_have_a_bounded_capability_episode() {
        let mut history = ToolRecoveryHistory::default();
        for operation in 0..8 {
            assert!(
                history
                    .observe(
                        "terminal.exec@1",
                        hash(operation),
                        ToolRecoveryOutcome::Succeeded
                    )
                    .is_none()
            );
            let stop = history.observe(
                "artifacts.deliver@1",
                hash(operation),
                ToolRecoveryOutcome::Failed(ErrorCode::InvalidCommand),
            );
            assert_eq!(stop.is_some(), operation == 7);
            if let Some(stop) = stop {
                assert_eq!(
                    stop.reason,
                    ToolRecoveryStopReason::CapabilityRecoveryExhausted
                );
                assert_eq!(stop.observed_failures, 8);
            }
        }
    }

    #[test]
    fn repaired_validation_and_long_successful_tasks_have_no_lifetime_cap() {
        let mut history = ToolRecoveryHistory::default();
        // Scan newest first: three different recent corrections remain allowed.
        for operation in 0..3 {
            assert!(
                history
                    .observe(
                        "artifacts.deliver@1",
                        hash(operation),
                        ToolRecoveryOutcome::Failed(ErrorCode::InvalidCommand)
                    )
                    .is_none()
            );
        }
        assert!(
            history
                .observe(
                    "artifacts.deliver@1",
                    hash(4),
                    ToolRecoveryOutcome::Succeeded
                )
                .is_none()
        );
        for operation in 0..1000 {
            assert!(
                history
                    .observe(
                        "artifacts.deliver@1",
                        hash((operation % 255) as u8),
                        ToolRecoveryOutcome::Failed(ErrorCode::InvalidCommand)
                    )
                    .is_none()
            );
        }
    }

    #[test]
    fn uncertain_write_is_feedback_until_the_same_operation_repeats() {
        let mut history = ToolRecoveryHistory::default();
        let unknown = ToolRecoveryOutcome::Indeterminate {
            observed_error: None,
            may_have_side_effect: true,
        };
        // One unknown publish leaves the model free to inspect and continue.
        assert!(
            history
                .observe("device.workspace.file.publish@1", hash(1), unknown)
                .is_none()
        );
        assert!(
            history
                .observe("device.workspace.file.publish@1", hash(2), unknown)
                .is_none()
        );
        // Blindly repeating the same uncertain operation is still bounded.
        let stop = history
            .observe("device.workspace.file.publish@1", hash(1), unknown)
            .expect("three unknown outcomes of one capability stop automatic work");
        assert_eq!(stop.reason, ToolRecoveryStopReason::CapabilityUnavailable);
        assert_eq!(stop.code, ErrorCode::DependencyUnavailable);
        // A later success of that capability ends the episode (newest first).
        let mut history = ToolRecoveryHistory::default();
        assert!(
            history
                .observe(
                    "dialogue.publish@1",
                    hash(1),
                    ToolRecoveryOutcome::Succeeded
                )
                .is_none()
        );
        for _ in 0..5 {
            assert!(
                history
                    .observe("dialogue.publish@1", hash(1), unknown)
                    .is_none()
            );
        }
    }
    #[test]
    fn historical_fence_never_becomes_current_attempt_control() {
        let mut history = ToolRecoveryHistory::default();
        let fenced = ToolRecoveryOutcome::Indeterminate {
            observed_error: Some(ErrorCode::AttemptLeaseStale),
            may_have_side_effect: true,
        };
        assert!(
            history
                .observe("artifacts.deliver@2", hash(1), fenced)
                .is_none()
        );
        assert!(
            history
                .observe("artifacts.deliver@2", hash(1), fenced)
                .is_none()
        );
        let stop = history
            .observe("artifacts.deliver@2", hash(1), fenced)
            .unwrap();
        assert_eq!(
            stop.reason,
            ToolRecoveryStopReason::RepeatedOperationFailure
        );
        assert_eq!(stop.code, ErrorCode::DependencyUnavailable);
        assert!(
            !crate::ModelFailure {
                code: stop.code,
                retryable: false
            }
            .permits_dependency_recovery()
        );
        let mut history = ToolRecoveryHistory::default();
        for round in 1..=REPEATED_TOOL_FAILURE_LIMIT {
            let stop = history.observe(
                "files.list@5",
                hash(1),
                ToolRecoveryOutcome::Failed(ErrorCode::AttemptLeaseStale),
            );
            if round == REPEATED_TOOL_FAILURE_LIMIT {
                let stop = stop.unwrap();
                assert_eq!(
                    stop.reason,
                    ToolRecoveryStopReason::RepeatedOperationFailure
                );
                assert_eq!(stop.code, ErrorCode::DependencyUnavailable);
            } else {
                assert!(stop.is_none());
            }
        }
        for code in [
            ErrorCode::PolicyDenied,
            ErrorCode::InternalInvariantViolated,
            ErrorCode::DependencyTimeout,
            ErrorCode::RunTransitionInvalid,
        ] {
            assert_eq!(historical_stop_code(code), code);
        }
    }
}
