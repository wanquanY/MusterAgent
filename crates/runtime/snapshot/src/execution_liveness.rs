use crate::SNAPSHOT_SCHEMA_VERSION;
// Frozen Run-level liveness policies. Tool policies belong to ToolManifest;
// Root and Child use the same model and handoff policy, with distinct scopes.
use musteragent_identity::{UtcTimestamp, VersionedKey};
use musteragent_runtime_contract::execution_liveness::{
    ActivityExecutionTiming, ActivitySource, ExecutionLivenessPolicy, ExecutionLivenessWindows,
    InvocationExecutionTiming,
};

use crate::SnapshotContractError;

pub const MODEL_LIVENESS_POLICY_V1: &str = "musteragent.model-liveness@1";
pub const HANDOFF_LIVENESS_POLICY_V1: &str = "musteragent.handoff-liveness@1";

/// Private fields preserve validation while keeping the existing small, Copy
/// Run policy. The exact sealed wire policies are reconstructed losslessly.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RunLivenessPolicyV1 {
    model: ExecutionLivenessWindows,
    handoff: ExecutionLivenessWindows,
}

impl RunLivenessPolicyV1 {
    #[must_use]
    pub fn production_default() -> Self {
        Self::production_with_model_idle_window(crate::DEFAULT_MODEL_INACTIVITY_TIMEOUT)
            .expect("valid production call windows")
    }

    /// Deployment overrides must change both the model window and its frozen
    /// liveness policy together; the handoff policy remains system-owned.
    pub fn production_with_model_idle_window(
        model_idle_window: std::time::Duration,
    ) -> Result<Self, SnapshotContractError> {
        Self::new(
            ExecutionLivenessWindows::for_idle_window(model_idle_window),
            ExecutionLivenessWindows::for_idle_window(std::time::Duration::from_secs(30)),
        )
    }

    pub fn new(
        model: ExecutionLivenessWindows,
        handoff: ExecutionLivenessWindows,
    ) -> Result<Self, SnapshotContractError> {
        model
            .validate()
            .map_err(|_| SnapshotContractError::InvalidRunExecutionPolicy)?;
        handoff
            .validate()
            .map_err(|_| SnapshotContractError::InvalidRunExecutionPolicy)?;
        Ok(Self { model, handoff })
    }

    pub fn from_policies(
        model: &ExecutionLivenessPolicy,
        handoff: &ExecutionLivenessPolicy,
    ) -> Result<Self, SnapshotContractError> {
        let result = Self::new(model.windows(), handoff.windows())?;
        if &result.model_policy() != model || &result.handoff_policy() != handoff {
            return Err(SnapshotContractError::InvalidRunExecutionPolicy);
        }
        Ok(result)
    }

    #[must_use]
    pub fn model_policy(self) -> ExecutionLivenessPolicy {
        ExecutionLivenessPolicy::new(
            VersionedKey::new(MODEL_LIVENESS_POLICY_V1).expect("constant policy key"),
            self.model,
            [ActivitySource::CanonicalModelEvent],
        )
        .expect("validated Run policy")
    }

    #[must_use]
    pub fn handoff_policy(self) -> ExecutionLivenessPolicy {
        // Dispatching the next invocation ends this scope. Scheduler heartbeat
        // and queue churn cannot perpetually extend an uncompleted handoff.
        ExecutionLivenessPolicy::new(
            VersionedKey::new(HANDOFF_LIVENESS_POLICY_V1).expect("constant policy key"),
            self.handoff,
            [],
        )
        .expect("validated Run policy")
    }
}

impl crate::RuntimeSnapshotContent {
    /// The exact sealed snapshot owns model timing. Independent business
    /// deadlines require their own original admission provenance; callers
    /// cannot convert a legacy duration or a credential expiry into one.
    pub fn validate_model_invocation_timing(
        &self,
        timing: &InvocationExecutionTiming,
    ) -> Result<(), SnapshotContractError> {
        self.validate_liveness_schema()?;
        match (self.execution_policy.liveness, timing) {
            (None, InvocationExecutionTiming::LegacyFixed { .. }) => Ok(()),
            (Some(profile), InvocationExecutionTiming::ActivityV1(activity))
                if &activity.policy == profile.model_policy().reference()
                    && activity.business_deadline.is_none() =>
            {
                Ok(())
            }
            _ => Err(SnapshotContractError::InvalidRunExecutionPolicy),
        }
    }

    /// Both serialization and admission require the sealed activity policies.
    pub fn validate_liveness_schema(&self) -> Result<(), SnapshotContractError> {
        let enabled = runtime_snapshot_has_activity_liveness(self.schema_version.as_str());
        if enabled != self.execution_policy.liveness.is_some()
            || self
                .tools
                .iter()
                .any(|tool| enabled != tool.manifest.execution_policy.liveness().is_some())
        {
            return Err(SnapshotContractError::SchemaMismatch);
        }
        self.execution_policy.validate_model_liveness_window()
    }
}

/// One schema classification for the original Snapshot, Model intent and both
/// execution Homes. Extension Snapshot families keep their own qualification.
pub fn runtime_snapshot_has_activity_liveness(schema: &str) -> bool {
    matches!(schema, SNAPSHOT_SCHEMA_VERSION)
}

impl crate::RunExecutionPolicySnapshot {
    pub(crate) fn validate_model_liveness_window(self) -> Result<(), SnapshotContractError> {
        if self.liveness.is_some_and(|profile| {
            profile.model_policy().idle_window() != self.model_inactivity_timeout
        }) {
            return Err(SnapshotContractError::InvalidRunExecutionPolicy);
        }
        Ok(())
    }
    /// Selects timing for a new physical model intent. Activity timing has no
    /// wall-clock anchor here; its original Home begin transaction owns startup.
    pub fn model_invocation_timing(
        self,
        now: UtcTimestamp,
    ) -> Result<InvocationExecutionTiming, SnapshotContractError> {
        self.validate()?;
        match self.liveness {
            Some(profile) => Ok(InvocationExecutionTiming::ActivityV1(
                ActivityExecutionTiming {
                    policy: profile.model_policy().reference().clone(),
                    business_deadline: None,
                },
            )),
            None => {
                let duration = i64::try_from(self.model_inactivity_timeout.as_millis())
                    .map_err(|_| SnapshotContractError::InvalidRunExecutionPolicy)?;
                let deadline = now
                    .checked_add_millis(duration)
                    .ok_or(SnapshotContractError::InvalidRunExecutionPolicy)?;
                Ok(InvocationExecutionTiming::LegacyFixed { deadline })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn sealed_activity_model_timing_has_no_wall_clock_deadline_or_lease_anchor() {
        let mut policy = crate::RunExecutionPolicySnapshot::legacy_defaults();
        let now = UtcTimestamp::from_unix_millis(1_000);
        assert_eq!(
            policy.model_invocation_timing(now).expect("legacy timing"),
            InvocationExecutionTiming::LegacyFixed {
                deadline: UtcTimestamp::from_unix_millis(301_000)
            }
        );
        let windows = ExecutionLivenessWindows {
            startup: Duration::from_secs(10),
            idle: policy.model_inactivity_timeout,
            max_delivery_lag: Duration::from_secs(1),
            checkpoint_interval: Duration::from_millis(100),
            observation_request: Duration::from_millis(250),
            cleanup: Duration::from_secs(1),
            post_exit_drain: Duration::from_millis(250),
        };
        let profile = RunLivenessPolicyV1::new(windows, windows).expect("profile");
        policy.liveness = Some(profile);
        let expected = InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
            policy: profile.model_policy().reference().clone(),
            business_deadline: None,
        });
        assert_eq!(
            policy
                .model_invocation_timing(now)
                .expect("activity timing"),
            expected
        );
        assert_eq!(
            policy
                .model_invocation_timing(UtcTimestamp::from_unix_millis(i64::MAX))
                .expect("no time arithmetic for activity"),
            expected
        );
        policy.model_inactivity_timeout += Duration::from_secs(1);
        assert!(
            policy.model_invocation_timing(now).is_err(),
            "the legacy idle field cannot contradict the sealed activity profile"
        );
    }
}
