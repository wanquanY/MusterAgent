//! Explicit context-capacity overflow recovery (proposal §6/§7, phase C).
//!
//! A new versioned compaction policy, `musteragent.context-compaction@7`,
//! admits transparent overflow recovery: when a physical model invocation
//! fails with a verified explicit context-window signal, the Loop seals that
//! invocation as a durable failure fact and schedules a same-route
//! replacement Attempt instead of failing the Run or raising a
//! configuration-repair wait. The replacement plan then compacts through the
//! existing fenced Context machinery before the next request.
//!
//! Policies @1-@6 are frozen: without an explicit @7 through @10 admission the exact
//! legacy classification, hard-estimate behavior and terminal failure
//! semantics continue unchanged. New production admissions seal @10; existing
//! Runs retain their original admission policy.

use musteragent_identity::VersionedKey;
use musteragent_runtime_contract::ErrorCode;

use crate::ModelFailure;

/// Versioned identity of the explicit-overflow compaction policy. Only Runs
/// whose admission Snapshot seals this exact ref may recover transparently;
/// every other admitted policy keeps the frozen fail-stop behavior.
pub use musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V7;

/// Whether the physical failure carried a verified explicit Provider
/// context-capacity signal (structured HTTP error, streamed error event, or
/// terminal stop reason). Unknown 400/413, 429, authentication, output-limit
/// and local content-capacity failures never produce this signal.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ContextCapacitySignal {
    ExplicitOverflow,
    NotCapacity,
}

impl ContextCapacitySignal {
    /// A stream/consumption failure is an explicit overflow only when the
    /// Adapter classified it as `ContextWindowExceeded`; a trusted terminal
    /// stop reason is passed separately by the Loop.
    #[must_use]
    pub const fn from_provider_code(code: musteragent_runtime_model::ProviderErrorCode) -> Self {
        match code {
            musteragent_runtime_model::ProviderErrorCode::ContextWindowExceeded => {
                Self::ExplicitOverflow
            }
            _ => Self::NotCapacity,
        }
    }
}

#[must_use]
pub fn admits_explicit_overflow_recovery(policy_ref: Option<&VersionedKey>) -> bool {
    match policy_ref {
        Some(policy_ref) => {
            musteragent_runtime_snapshot::uses_provider_capacity_recovery(policy_ref.as_str())
        }
        None => false,
    }
}

/// Resolves the durable failure identity for one physical invocation.
///
/// Explicit overflow under an admitted @7 policy with no unresolvable public replay
/// barrier becomes retryable so the frozen failure tables permit a
/// same-route replacement Attempt; the Journal still revalidates the
/// resolution under the Attempt fence. Any other combination keeps the
/// original failure byte-for-byte, so legacy policies and partial public
/// output without an admitted sealed-continuation recovery policy are untouched.
/// The caller resolves the barrier from the same immutable execution policy
/// later used to validate the failure commit.
#[must_use]
pub fn recoverable_overflow_failure(
    failure: ModelFailure,
    signal: ContextCapacitySignal,
    unrecoverable_replay_barrier: bool,
    policy_ref: Option<&VersionedKey>,
) -> ModelFailure {
    if failure.code == ErrorCode::ModelContextExceeded
        && matches!(signal, ContextCapacitySignal::ExplicitOverflow)
        && !unrecoverable_replay_barrier
        && admits_explicit_overflow_recovery(policy_ref)
    {
        return ModelFailure {
            retryable: true,
            ..failure
        };
    }
    failure
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(version: &str) -> VersionedKey {
        VersionedKey::new(version).expect("policy ref")
    }

    #[test]
    fn only_explicitly_supported_policies_admit_overflow_recovery() {
        assert!(admits_explicit_overflow_recovery(Some(&policy(
            CONTEXT_COMPACTION_POLICY_V7
        ))));
        assert!(admits_explicit_overflow_recovery(Some(&policy(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V8
        ))));
        assert!(admits_explicit_overflow_recovery(Some(&policy(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V9
        ))));
        assert!(admits_explicit_overflow_recovery(Some(&policy(
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V10
        ))));
        for current in [
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V11,
            musteragent_runtime_snapshot::CONTEXT_COMPACTION_POLICY_V12,
        ] {
            assert!(
                admits_explicit_overflow_recovery(Some(&policy(current))),
                "{current}"
            );
        }
        for legacy in [
            "musteragent.legacy.context-compaction@1",
            "musteragent.legacy.context-compaction@2",
            "musteragent.legacy.context-compaction@3",
            "musteragent.legacy.context-compaction@4",
            "musteragent.context-compaction@5",
            "musteragent.context-compaction@6",
        ] {
            assert!(
                !admits_explicit_overflow_recovery(Some(&policy(legacy))),
                "{legacy}"
            );
        }
        assert!(!admits_explicit_overflow_recovery(None));
        // Unknown future versions are rejected, not guessed.
        assert!(!admits_explicit_overflow_recovery(Some(&policy(
            "musteragent.context-compaction@13"
        ))));
    }

    #[test]
    fn overflow_becomes_retryable_only_under_every_gate() {
        let failure = ModelFailure {
            code: ErrorCode::ModelContextExceeded,
            retryable: false,
        };
        let recovered = recoverable_overflow_failure(
            failure,
            ContextCapacitySignal::ExplicitOverflow,
            false,
            Some(&policy(CONTEXT_COMPACTION_POLICY_V7)),
        );
        assert!(recovered.retryable);
        assert!(recovered.permits_new_attempt());

        // Legacy policy: byte-identical failure.
        assert_eq!(
            recoverable_overflow_failure(
                failure,
                ContextCapacitySignal::ExplicitOverflow,
                false,
                Some(&policy("musteragent.context-compaction@6")),
            ),
            failure
        );
        // Public output already observed: sealed continuation is a separate
        // contract, so the failure stays terminal here.
        assert_eq!(
            recoverable_overflow_failure(
                failure,
                ContextCapacitySignal::ExplicitOverflow,
                true,
                Some(&policy(CONTEXT_COMPACTION_POLICY_V7)),
            ),
            failure
        );
        // Unclassified failures never enter capacity recovery.
        assert_eq!(
            recoverable_overflow_failure(
                failure,
                ContextCapacitySignal::NotCapacity,
                false,
                Some(&policy(CONTEXT_COMPACTION_POLICY_V7)),
            ),
            failure
        );
        // Other codes are untouched even with an explicit signal present.
        let quota = ModelFailure {
            code: ErrorCode::QuotaExceeded,
            retryable: false,
        };
        assert_eq!(
            recoverable_overflow_failure(
                quota,
                ContextCapacitySignal::ExplicitOverflow,
                false,
                Some(&policy(CONTEXT_COMPACTION_POLICY_V7)),
            ),
            quota
        );
    }
}
