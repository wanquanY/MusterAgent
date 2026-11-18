//! Additive cloud schedule publication. Historical policy ceilings are frozen.
pub const CLOUD_SCHEDULE_DISCOVERY_POLICY_V1: &str = "musteragent.cloud-schedule-discovery@1";

pub(super) fn allowed(policy: &str, key: &str) -> Option<bool> {
    (policy == CLOUD_SCHEDULE_DISCOVERY_POLICY_V1).then(|| {
        if key.starts_with("schedules.") {
            matches!(
                key,
                "schedules.create@2"
                    | "schedules.update@2"
                    | "schedules.control@2"
                    | "schedules.list@2"
                    | "schedules.history@1"
            )
        } else {
            super::capability_available_for_dialogue_policy(super::DISCOVERY_POLICY_V11, key)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn new_schedule_contract_does_not_expand_historical_or_private_subjects() {
        for key in [
            "schedules.create@2",
            "schedules.update@2",
            "schedules.control@2",
            "schedules.list@2",
            "schedules.history@1",
        ] {
            assert_eq!(allowed(CLOUD_SCHEDULE_DISCOVERY_POLICY_V1, key), Some(true));
            assert!(!crate::capability_available_for_dialogue_policy(
                crate::DISCOVERY_POLICY_V11,
                key
            ));
            assert!(!crate::owner_goal_capability_available(
                CLOUD_SCHEDULE_DISCOVERY_POLICY_V1,
                key
            ));
            assert!(!crate::agent_work_item_capability_available(
                CLOUD_SCHEDULE_DISCOVERY_POLICY_V1,
                key
            ));
        }
        assert_eq!(
            allowed(CLOUD_SCHEDULE_DISCOVERY_POLICY_V1, "schedules.create@1"),
            Some(false)
        );
        assert_eq!(
            allowed(CLOUD_SCHEDULE_DISCOVERY_POLICY_V1, "schedules.create@3"),
            Some(false)
        );
    }
}
