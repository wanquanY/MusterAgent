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
    ///
    /// Long-running process work (see [`Self::runs_long_process`]) is bounded
    /// only by its silence:
    /// [`LONG_WORK_SILENCE_WINDOW`](musteragent_runtime_contract::execution_liveness::LONG_WORK_SILENCE_WINDOW)
    /// once it reports activity. Its startup window extends too only when the
    /// process runs on a host the Runtime controls (a device or the server).
    /// A browser-executed Tool ([`crate::requires_browser_execution`]) keeps
    /// the ordinary startup window: if the page holding the browser lease is
    /// lost before the job starts, the call fails promptly instead of stalling
    /// for the full silence window.
    ///
    /// Existing Run snapshots keep their already sealed policy unchanged.
    #[must_use]
    pub fn builtin_activity(
        key: musteragent_identity::VersionedKey,
        default_idle: Duration,
        media_artifact: Option<crate::MediaArtifactKind>,
    ) -> Self {
        use musteragent_runtime_contract::execution_liveness::{
            ExecutionLivenessWindows, LONG_WORK_SILENCE_WINDOW,
        };
        if media_artifact.is_some() {
            return Self::admitted_activity(key, default_idle.max(Duration::from_secs(600)));
        }
        if !Self::runs_long_process(key.as_str()) {
            return Self::admitted_activity(key, default_idle);
        }
        // An arbitrary process has no total lifetime. Its own output and
        // process work renew this window; a legitimate quiet stretch
        // (dependency download, compiler warm-up) must not end it.
        let idle = Self::admitted_window(default_idle.max(LONG_WORK_SILENCE_WINDOW));
        let mut windows = ExecutionLivenessWindows::for_idle_window(idle);
        if crate::requires_browser_execution(key.as_str()) {
            windows.startup = Self::admitted_window(default_idle);
        }
        Self::with_windows(key, windows)
    }

    /// Tools whose execution is an arbitrary, possibly hours-long process
    /// (a shell command, a Python program, a web build), any version. Where
    /// the process runs is decided separately by
    /// [`crate::requires_browser_execution`].
    #[must_use]
    pub fn runs_long_process(key: &str) -> bool {
        matches!(
            key.rsplit_once('@').map_or(key, |(logical, _)| logical),
            "device.terminal.exec" | "data.python.run" | "application.web.build"
        )
    }

    fn admitted_window(window: Duration) -> Duration {
        window.max(musteragent_runtime_contract::execution_liveness::MIN_TOOL_EXECUTION_WINDOW)
    }

    /// Shared admission policy for built-in, deployed and private Tools. A
    /// capability may request a longer window; short legacy defaults cannot
    /// silently shorten the product's minimum execution budget.
    #[must_use]
    pub fn admitted_activity(key: musteragent_identity::VersionedKey, idle: Duration) -> Self {
        Self::activity(key, Self::admitted_window(idle))
    }

    /// Exact policy construction for immutable historical profiles and explicit
    /// protocol fixtures. New product admissions use `admitted_activity`.
    #[must_use]
    pub fn activity(key: musteragent_identity::VersionedKey, idle: Duration) -> Self {
        Self::with_windows(
            key,
            musteragent_runtime_contract::execution_liveness::ExecutionLivenessWindows::for_idle_window(
                idle,
            ),
        )
    }

    fn with_windows(
        key: musteragent_identity::VersionedKey,
        windows: musteragent_runtime_contract::execution_liveness::ExecutionLivenessWindows,
    ) -> Self {
        use musteragent_runtime_contract::execution_liveness::ActivitySource;
        Self::ActivityV1(Arc::new(
            ExecutionLivenessPolicy::new(key, windows, ActivitySource::ALL)
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
    fn long_local_work_is_bounded_by_a_generous_silence_window_only() {
        use musteragent_runtime_contract::execution_liveness::LONG_WORK_SILENCE_WINDOW;
        for (key, browser) in [
            ("device.terminal.exec@3", false),
            ("device.terminal.exec@4", false),
            ("data.python.run@11", true),
            ("application.web.build@1", true),
        ] {
            assert_eq!(crate::requires_browser_execution(key), browser, "{key}");
            let policy = ToolExecutionPolicy::builtin_activity(
                VersionedKey::new(key).expect("valid test fixture"),
                Duration::from_secs(120),
                None,
            );
            assert!(
                policy.legacy_timeout().is_err(),
                "{key} has no total lifetime"
            );
            let windows = policy.liveness().expect("activity").windows();
            assert_eq!(windows.idle, LONG_WORK_SILENCE_WINDOW, "{key}");
            // A lost browser page must fail the call promptly, not after the
            // full silence window; a local or server process may take long
            // to start.
            let startup = if browser {
                Duration::from_secs(120)
            } else {
                LONG_WORK_SILENCE_WINDOW
            };
            assert_eq!(windows.startup, startup, "{key}");
        }
        // The startup floor still applies to a short browser default.
        let short = ToolExecutionPolicy::builtin_activity(
            VersionedKey::new("data.python.run@11").expect("valid test fixture"),
            Duration::from_secs(5),
            None,
        );
        let windows = short.liveness().expect("activity").windows();
        assert_eq!(windows.startup, Duration::from_secs(60));
        assert_eq!(windows.idle, LONG_WORK_SILENCE_WINDOW);
        // A capability asking for even longer silence keeps its own window.
        let longer = ToolExecutionPolicy::builtin_activity(
            VersionedKey::new("data.python.run@11").expect("valid test fixture"),
            Duration::from_secs(3_600),
            None,
        );
        assert_eq!(
            longer.liveness().expect("activity").windows().idle,
            Duration::from_secs(3_600)
        );
        for key in [
            "device.terminal.session.read@2",
            "device.workspace.file.read@5",
        ] {
            let ordinary = ToolExecutionPolicy::builtin_activity(
                VersionedKey::new(key).expect("valid test fixture"),
                Duration::from_secs(120),
                None,
            );
            assert_eq!(
                ordinary.liveness().expect("activity").windows().idle,
                Duration::from_secs(120),
                "{key}"
            );
        }
        // A Run admitted before this window existed keeps its sealed policy.
        let sealed = ToolExecutionPolicy::activity(
            VersionedKey::new("data.python.run@11").expect("valid test fixture"),
            Duration::from_secs(120),
        );
        assert_eq!(
            sealed.liveness().expect("activity").windows().idle,
            Duration::from_secs(120)
        );
    }

    #[test]
    fn long_work_catalog_tools_are_pinned_with_the_silence_window() {
        use musteragent_runtime_contract::execution_liveness::LONG_WORK_SILENCE_WINDOW;
        let mut seen = 0;
        for catalog in [
            crate::BuiltinToolCatalog::load_cloud().unwrap(),
            crate::BuiltinToolCatalog::load_device().unwrap(),
        ] {
            for tool in catalog.definitions() {
                if !ToolExecutionPolicy::runs_long_process(tool.key.as_str()) {
                    continue;
                }
                seen += 1;
                let manifest = tool.pin(musteragent_identity::CapabilityProviderId::generate());
                let windows = manifest.execution_policy.liveness().unwrap().windows();
                assert!(
                    windows.idle >= LONG_WORK_SILENCE_WINDOW,
                    "{}",
                    tool.key.as_str()
                );
            }
        }
        assert!(seen > 0, "no long-work tool in the built-in catalogs");
    }

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
