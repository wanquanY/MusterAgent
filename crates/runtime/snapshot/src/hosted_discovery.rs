//! Frozen shared-project discovery shape. This is a Snapshot contract, never
//! proof of permission: admission and effects still require live Home authority.
use super::{RuntimeSnapshotContent, SnapshotContractError};

impl RuntimeSnapshotContent {
    pub(super) fn validate_hosted_discovery_boundary(&self) -> Result<(), SnapshotContractError> {
        let unavailable = |key: &str| !hosted_executor_capability_available_v4(key);
        if !self.instincts.is_empty()
            || !self.memory.selected.is_empty()
            || self.prompt.sections.iter().any(|section| {
                matches!(
                    section.section_key.as_str(),
                    "user.profile" | "product.active_surface" | "product.selected_capabilities"
                )
            })
            || self
                .capabilities
                .iter()
                .any(|grant| unavailable(grant.key.as_str()))
            || self
                .tools
                .iter()
                .any(|tool| unavailable(tool.manifest.capability_key.as_str()))
        {
            return Err(SnapshotContractError::InvalidDiscovery);
        }
        Ok(())
    }
}

pub const HOSTED_DISCOVERY_POLICY_V4: &str = "musteragent.hosted-project-discovery@4";
pub const HOSTED_EXECUTOR_FILE_BOOTSTRAP_TOOLS: [&str; 8] = [
    "tool.catalog.search",
    "tool.catalog.activate",
    "interaction.request",
    "device.workspace.file.list",
    "device.workspace.file.read",
    "files.list",
    "files.inspect",
    "files.read",
];
pub fn is_hosted_discovery_policy(key: &str) -> bool {
    key == HOSTED_DISCOVERY_POLICY_V4
}
/// Availability is independent of permission. Every effect still needs a signed
/// entitlement and the original live Home/Executor source checks.
pub fn hosted_executor_capability_available_v4(key: &str) -> bool {
    let Some((logical, version)) = key.rsplit_once('@') else {
        return false;
    };
    matches!(
        (logical, version),
        (
            "tool.catalog.search" | "tool.catalog.activate" | "interaction.request",
            "1" | "2"
        ) | ("vision.analyze", "4")
            | (
                "device.workspace.file.list"
                    | "device.workspace.file.find"
                    | "device.workspace.file.search",
                "1" | "2" | "3"
            )
            | (
                "device.workspace.code.diagnostics" | "device.terminal.exec",
                "1" | "2"
            )
            | (
                "device.workspace.file.read" | "device.workspace.file.edit",
                "1" | "2" | "3" | "4" | "5"
            )
            | (
                "device.workspace.file.write"
                    | "device.workspace.file.patch"
                    | "device.workspace.file.diff",
                "1" | "2" | "3" | "4"
            )
            | (
                "device.resource.read"
                    | "device.resource.inspect"
                    | "device.resource.write"
                    | "device.network.https",
                "1"
            )
            | ("device.interaction", "1" | "2")
            | (
                "files.list" | "files.inspect" | "files.search",
                "1" | "2" | "3"
            )
            | ("files.read", "1" | "2" | "3" | "4")
    )
}

#[cfg(test)]
mod file_query_tests {
    use super::*;
    #[test]
    fn current_hosted_policy_preserves_exact_file_authority() {
        for key in [
            "files.list@3",
            "files.inspect@3",
            "files.search@3",
            "files.read@4",
        ] {
            assert!(hosted_executor_capability_available_v4(key));
        }
        for key in [
            "device.file.read@1",
            "workspace.file.read@1",
            "files.read@5",
            "agents.create@1",
        ] {
            assert!(!hosted_executor_capability_available_v4(key));
        }
        assert!(is_hosted_discovery_policy(HOSTED_DISCOVERY_POLICY_V4));
        for version in 1..4 {
            assert!(!is_hosted_discovery_policy(&format!(
                "musteragent.hosted-project-discovery@{version}"
            )));
        }
    }
}
