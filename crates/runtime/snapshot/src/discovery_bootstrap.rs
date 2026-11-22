//! Versioned bootstrap contracts for frozen Snapshot discovery profiles.
use super::*;

pub(super) fn bootstrap_tool_keys_are_valid(
    discovery_policy: &str,
    keys: &[ToolKey],
    device_catalog: bool,
    delegation_catalog: bool,
) -> bool {
    if super::is_cloud_orchestration_discovery_policy(discovery_policy) {
        return !device_catalog
            && keys
                .iter()
                .map(ToolKey::logical_key)
                .eq(F4_CLOUD_BASELINE_TOOLS);
    }
    if discovery_policy == super::CLOUD_SCHEDULE_DISCOVERY_POLICY_V1 {
        return !device_catalog
            && !delegation_catalog
            && keys
                .iter()
                .map(ToolKey::logical_key)
                .eq(F4_CLOUD_BASELINE_TOOLS);
    }
    if super::is_dialogue_family_discovery_policy(discovery_policy) {
        return device_catalog == super::is_dialogue_device_discovery_policy(discovery_policy)
            && [false, true].into_iter().any(|files| {
                keys.iter().map(ToolKey::logical_key).eq(
                    super::dialogue_bootstrap_logical_keys_for_policy(discovery_policy, files),
                )
            });
    }
    if discovery_policy == DISCOVERY_POLICY_V11 {
        let mut expected = if device_catalog && delegation_catalog {
            F7_DEVICE_BASELINE_TOOLS.to_vec()
        } else if device_catalog {
            F4_DEVICE_BASELINE_TOOLS.to_vec()
        } else {
            F4_CLOUD_BASELINE_TOOLS.to_vec()
        };
        expected.push("context.read");
        if !device_catalog && delegation_catalog {
            expected.push("delegation.inspect");
        }
        expected.extend_from_slice(&["files.list", "files.inspect", "files.read"]);
        return keys.iter().map(ToolKey::logical_key).eq(expected);
    }
    discovery_policy == HOSTED_DISCOVERY_POLICY_V4
        && keys
            .iter()
            .map(ToolKey::logical_key)
            .eq(HOSTED_EXECUTOR_FILE_BOOTSTRAP_TOOLS)
}
