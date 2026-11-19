//! Additive cloud orchestration publication; historical policies stay frozen.
pub const CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1: &str =
    "musteragent.cloud-orchestration-discovery@1";
/// v1 plus conversation delivery (`dialogue.publish@1`, `artifacts.deliver@2`)
/// and the agent work-item tools, for cloud web hosts. v1 stays frozen.
pub const CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2: &str =
    "musteragent.cloud-orchestration-discovery@2";

/// Exact tool and capability keys `@2` adds to the frozen `@1` ceiling.
pub const CLOUD_ORCHESTRATION_V2_ADDITIONS: [&str; 13] = [
    "artifacts.deliver@2",
    "dialogue.publish@1",
    "tasks.amend@2",
    "tasks.control@1",
    "tasks.executions.list@2",
    "tasks.feedback@1",
    "tasks.followup@1",
    "tasks.handoff@1",
    "tasks.inspect@1",
    "tasks.list@1",
    "tasks.start@3",
    "tasks.submit_result@1",
    "tasks.verify@3",
];

/// Whether `policy` is one of the frozen cloud orchestration policies.
#[must_use]
pub fn is_cloud_orchestration_discovery_policy(policy: &str) -> bool {
    matches!(
        policy,
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1 | CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2
    )
}

/// The oldest cloud orchestration policy whose ceiling admits every key.
/// Catalogs that `@1` already admits keep sealing under `@1`, so existing
/// snapshots and their hashes are unchanged.
#[must_use]
pub fn cloud_orchestration_discovery_policy_for<'a>(
    keys: impl IntoIterator<Item = &'a str>,
) -> &'static str {
    if keys
        .into_iter()
        .any(|key| CLOUD_ORCHESTRATION_V2_ADDITIONS.contains(&key))
    {
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2
    } else {
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1
    }
}

pub(super) fn allowed(policy: &str, key: &str) -> Option<bool> {
    match policy {
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1 => Some(allowed_v1(key)),
        CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2 => {
            Some(CLOUD_ORCHESTRATION_V2_ADDITIONS.contains(&key) || allowed_v1(key))
        }
        _ => None,
    }
}

fn allowed_v1(key: &str) -> bool {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_admits_delivery_and_work_item_tools_without_expanding_v1() {
        for key in CLOUD_ORCHESTRATION_V2_ADDITIONS {
            assert_eq!(
                allowed(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2, key),
                Some(true),
                "{key}"
            );
            assert!(
                crate::dialogue_capability_ceiling_for_policy(
                    CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2,
                    key
                ),
                "{key}"
            );
            // The frozen v1 and historical cloud ceilings are unchanged.
            assert_eq!(
                allowed(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, key),
                Some(false),
                "{key}"
            );
            assert!(!crate::dialogue_capability_ceiling_for_policy(
                CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1,
                key
            ));
            assert!(!crate::capability_available_for_dialogue_policy(
                crate::CLOUD_SCHEDULE_DISCOVERY_POLICY_V1,
                key
            ));
            assert!(!crate::capability_available_for_dialogue_policy(
                crate::DISCOVERY_POLICY_V11,
                key
            ));
        }
        // Neighbouring versions stay outside both ceilings.
        for key in [
            "artifacts.deliver@1",
            "artifacts.deliver@3",
            "dialogue.publish@2",
            "tasks.start@2",
            "tasks.verify@4",
        ] {
            assert_eq!(
                allowed(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2, key),
                Some(false),
                "{key}"
            );
        }
    }

    #[test]
    fn v2_keeps_the_entire_v1_ceiling() {
        for key in [
            "delegation.spawn@5",
            "delegation.inspect@2",
            "schedules.create@2",
            "schedules.history@1",
            "system.time.current@1",
        ] {
            assert_eq!(
                allowed(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1, key),
                allowed(CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2, key),
                "{key}"
            );
        }
        assert_eq!(
            allowed(
                CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2,
                "delegation.spawn@4"
            ),
            Some(false)
        );
        assert_eq!(allowed("host.unknown@1", "dialogue.publish@1"), None);
    }

    #[test]
    fn subject_ceilings_still_apply_under_v2() {
        let policy = CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2;
        // A Goal cannot hand off or follow up; a WorkItem cannot follow up.
        assert!(!crate::owner_goal_capability_available(
            policy,
            "tasks.handoff@1"
        ));
        assert!(!crate::owner_goal_capability_available(
            policy,
            "tasks.followup@1"
        ));
        assert!(!crate::agent_work_item_capability_available(
            policy,
            "tasks.followup@1"
        ));
        assert!(crate::owner_goal_capability_available(
            policy,
            "tasks.list@1"
        ));
        assert!(crate::agent_work_item_capability_available(
            policy,
            "tasks.submit_result@1"
        ));
    }

    #[test]
    fn existing_catalogs_keep_sealing_under_v1() {
        assert_eq!(
            cloud_orchestration_discovery_policy_for(["delegation.spawn@5", "files.read@1"]),
            CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V1
        );
        assert_eq!(
            cloud_orchestration_discovery_policy_for(["delegation.spawn@5", "dialogue.publish@1"]),
            CLOUD_ORCHESTRATION_DISCOVERY_POLICY_V2
        );
    }
}
