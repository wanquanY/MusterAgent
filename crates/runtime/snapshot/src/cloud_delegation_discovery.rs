//! Additive cloud orchestration publication; historical policies stay frozen.
pub const CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1: &str =
    "musteragent.cloud-orchestration-discovery@1";
pub(super) fn allowed(policy: &str, key: &str) -> Option<bool> {
    (policy == CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1).then(|| {
        if key.starts_with("delegation.") {
            matches!(
                key,
                "delegation.spawn@5"
                    | "delegation.send@3"
                    | "delegation.receive@1"
                    | "delegation.list@1"
                    | "delegation.join@4"
                    | "delegation.cancel@2"
                    | "delegation.retry@2"
                    | "delegation.inspect@2"
            )
        } else {
            super::capability_available_for_dialogue_policy(
                super::CLOUD_SCHEDULE_DISCOVERY_POLICY_V1,
                key,
            )
        }
    })
}
