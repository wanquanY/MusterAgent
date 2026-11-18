//! Closed lifetime policy: historical fixed windows and sealed activity policy
//! cannot both govern one physical execution.
use std::sync::Arc;
use std::time::Duration;

use musteragent_runtime_contract::execution_liveness::ExecutionLivenessPolicy;

use crate::ToolContractError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ToolExecutionPolicy {
    /// Immutable historical Tool releases only.
    LegacyFixed {
        timeout: Duration,
    },
    ActivityV1(Arc<ExecutionLivenessPolicy>),
}

impl ToolExecutionPolicy {
    /// Backend media admits a durable asynchronous job. Its historical 30s
    /// timeout bounded the admission RPC, not generation. A non-streaming media
    /// stage can be silent for the provider's 600s read window. Queries and
    /// unchanged task records do not refresh this finite stage budget.
    /// Existing Run snapshots keep their already sealed policy unchanged.
    #[must_use]
    pub fn builtin_activity(
        key: musteragent_identity::VersionedKey,
        default_idle: Duration,
        media_artifact: Option<crate::MediaArtifactKind>,
    ) -> Self {
        let idle = if media_artifact.is_some() {
            default_idle.max(Duration::from_secs(600))
        } else {
            default_idle
        };
        Self::admitted_activity(key, idle)
    }

    /// Shared admission policy for built-in, deployed and private Tools. A
    /// capability may request a longer window; short legacy defaults cannot
    /// silently shorten the product's minimum execution budget.
    #[must_use]
    pub fn admitted_activity(key: musteragent_identity::VersionedKey, idle: Duration) -> Self {
        Self::activity(
            key,
            idle.max(musteragent_runtime_contract::execution_liveness::MIN_TOOL_EXECUTION_WINDOW),
        )
    }

    /// Exact policy construction for immutable historical profiles and explicit
    /// protocol fixtures. New product admissions use `admitted_activity`.
    #[must_use]
    pub fn activity(key: musteragent_identity::VersionedKey, idle: Duration) -> Self {
        use musteragent_runtime_contract::execution_liveness::{
            ActivitySource, ExecutionLivenessWindows,
        };
        Self::ActivityV1(Arc::new(
            ExecutionLivenessPolicy::new(
                key,
                ExecutionLivenessWindows::for_idle_window(idle),
                ActivitySource::ALL,
            )
            .expect("valid tool silence window"),
        ))
    }

    pub fn validate(&self) -> Result<(), ToolContractError> {
        match self {
            Self::LegacyFixed { timeout } if timeout.is_zero() => {
                Err(ToolContractError::ZeroTimeout)
            }
            Self::LegacyFixed { .. } | Self::ActivityV1(_) => Ok(()),
        }
    }

    /// A legacy execution path must reject an activity policy, never reinterpret
    /// the idle or request window as a total operation lifetime.
    pub fn legacy_timeout(&self) -> Result<Duration, ToolContractError> {
        match self {
            Self::LegacyFixed { timeout } => Ok(*timeout),
            Self::ActivityV1(_) => Err(ToolContractError::UnsupportedExecutionPolicy),
        }
    }

    #[must_use]
    pub fn liveness(&self) -> Option<&Arc<ExecutionLivenessPolicy>> {
        match self {
            Self::LegacyFixed { .. } => None,
            Self::ActivityV1(policy) => Some(policy),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::VersionedKey;

    #[test]
    fn media_stage_silence_is_distinct_from_bounded_status_requests() {
        for kind in [
            crate::MediaArtifactKind::Image,
            crate::MediaArtifactKind::Video,
        ] {
            let policy = ToolExecutionPolicy::builtin_activity(
                VersionedKey::new("sample.render@1").expect("valid test fixture"),
                Duration::from_secs(30),
                Some(kind),
            );
            let windows = policy.liveness().expect("valid test fixture").windows();
            assert_eq!(windows.idle, Duration::from_secs(600));
            assert!(windows.observation_request <= Duration::from_secs(10));
            assert!(policy.legacy_timeout().is_err());
        }
        // A prior snapshot remains its own immutable policy, even if its key
        // matches a newly admitted tool with a different profile.
        let pinned = ToolExecutionPolicy::activity(
            VersionedKey::new("artifact.image.create@1").expect("valid test fixture"),
            Duration::from_secs(30),
        );
        assert_eq!(
            pinned
                .liveness()
                .expect("valid test fixture")
                .windows()
                .idle,
            Duration::from_secs(30)
        );
        let other = ToolExecutionPolicy::builtin_activity(
            VersionedKey::new("sample.render@1").expect("valid test fixture"),
            Duration::from_secs(30),
            None,
        );
        assert_eq!(
            other.liveness().expect("valid test fixture").windows().idle,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn every_admitted_cloud_and_device_tool_has_the_minimum_execution_window() {
        for catalog in [
            crate::BuiltinToolCatalog::load_cloud().unwrap(),
            crate::BuiltinToolCatalog::load_device().unwrap(),
        ] {
            for tool in catalog.definitions() {
                let manifest = tool.pin(musteragent_identity::CapabilityProviderId::generate());
                let windows = manifest.execution_policy.liveness().unwrap().windows();
                assert!(
                    windows.startup >= Duration::from_secs(60),
                    "{}",
                    tool.key.as_str()
                );
                assert!(
                    windows.idle >= Duration::from_secs(60),
                    "{}",
                    tool.key.as_str()
                );
                assert!(windows.idle >= tool.timeout, "{}", tool.key.as_str());
            }
        }
    }

    #[test]
    fn admission_floor_preserves_longer_tool_profiles_and_short_observation_cadence() {
        for requested in [1, 5, 60, 180, 600, 3_600] {
            let policy = ToolExecutionPolicy::admitted_activity(
                VersionedKey::new("test.tool@1").expect("key"),
                Duration::from_secs(requested),
            );
            let windows = policy.liveness().expect("activity").windows();
            assert_eq!(windows.startup, Duration::from_secs(requested.max(60)));
            assert_eq!(windows.idle, windows.startup);
            assert!(windows.observation_request <= Duration::from_secs(10));
            assert_eq!(windows.cleanup, Duration::from_secs(5));
        }
    }
}
