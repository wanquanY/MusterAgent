//! Verify the modern failure command against the original content-addressed snapshot.
use crate::ProtobufSnapshotCodec;
use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_loop::{ModelFailureCommit, ModelFailureResolution};
use musteragent_runtime_snapshot::{RecoveryPolicySnapshot, RunExecutionPolicySnapshot};

#[derive(Clone, Copy)]
pub struct ProviderRoundFence {
    recovery_policy: RecoveryPolicySnapshot,
    transition: Transition,
}
#[derive(Clone, Copy)]
enum Transition {
    Next(u32),
    Exhausted(Option<u32>),
}

impl ProviderRoundFence {
    pub fn for_failure(commit: &ModelFailureCommit) -> Option<Self> {
        let transition = match commit.resolution {
            ModelFailureResolution::ProviderFallbackV1 {
                next_route_ordinal, ..
            } => Transition::Next(next_route_ordinal),
            ModelFailureResolution::ExhaustedProviderRoundV1 {
                retry_route_ordinal,
                ..
            } => Transition::Exhausted(retry_route_ordinal),
            _ => return None,
        };
        Some(Self {
            recovery_policy: commit.recovery_policy,
            transition,
        })
    }

    pub fn permits(self, bytes: &[u8], expected_hash: &[u8], active: u32) -> bool {
        if Sha256::digest(bytes).into_bytes().as_slice() != expected_hash {
            return false;
        }
        let Ok(snapshot) = ProtobufSnapshotCodec.decode_runtime_snapshot(bytes) else {
            return false;
        };
        if snapshot.validate_sealed().is_err() {
            return false;
        }
        let Ok(last) = u32::try_from(snapshot.model.fallback_candidates.len()) else {
            return false;
        };
        self.permits_transition(&snapshot.execution_policy, last, active)
    }
    fn permits_transition(
        self,
        frozen: &RunExecutionPolicySnapshot,
        last: u32,
        active: u32,
    ) -> bool {
        frozen.provider_failover_v1
            && frozen.recovery_policy == self.recovery_policy
            && match self.transition {
                Transition::Next(next) => active.checked_add(1) == Some(next) && next <= last,
                Transition::Exhausted(retry) => {
                    active == last && retry.is_none_or(|ordinal| ordinal <= active)
                }
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn modern_round_is_bounded_by_the_original_route_and_frozen_policy() {
        let policy = RunExecutionPolicySnapshot::production_defaults();
        let fence = ProviderRoundFence {
            recovery_policy: policy.recovery_policy,
            transition: Transition::Next(1),
        };
        assert!(fence.permits_transition(&policy, 1, 0));
        assert!(!fence.permits_transition(&policy, 0, 0));
        assert!(!fence.permits_transition(&policy, 3, 1));
        let old = RunExecutionPolicySnapshot {
            provider_failover_v1: false,
            ..policy
        };
        assert!(!fence.permits_transition(&old, 1, 0));
        let forged = ProviderRoundFence {
            recovery_policy: std::num::NonZeroU32::MIN.into(),
            ..fence
        };
        assert!(!forged.permits_transition(&policy, 1, 0));
        let exhausted = ProviderRoundFence {
            transition: Transition::Exhausted(Some(0)),
            ..fence
        };
        assert!(!exhausted.permits_transition(&policy, 2, 1));
        assert!(exhausted.permits_transition(&policy, 1, 1));
        assert!(!exhausted.permits_transition(&old, 1, 1));
        assert!(
            !ProviderRoundFence {
                transition: Transition::Exhausted(Some(2)),
                ..fence
            }
            .permits_transition(&policy, 1, 1)
        );
    }
}
