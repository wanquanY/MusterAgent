use musteragent_runtime_snapshot::*;
#[test]
fn current_roles_preserve_kernel_tool_execution_and_delegated_authority() {
    let profiles: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/current-discovery.json")).expect("matrix");
    for profile in profiles.as_array().expect("profiles") {
        let policy = profile["policy"].as_str().expect("policy");
        for item in profile["tools"].as_array().expect("tools") {
            let key = item["key"].as_str().expect("key");
            assert_eq!(
                capability_available_for_dialogue_policy(policy, key),
                item["allowed"].as_bool().expect("allowed"),
                "execution {policy} {key}"
            );
            assert_eq!(
                dialogue_capability_ceiling_for_policy(policy, key),
                item["ceiling"].as_bool().expect("ceiling"),
                "ceiling {policy} {key}"
            );
        }
        assert_eq!(
            serde_json::to_value(dialogue_bootstrap_logical_keys_for_policy(policy, true))
                .expect("bootstrap"),
            profile["bootstrap"]
        );
    }
}
#[test]
fn unknown_and_retired_roles_never_acquire_current_authority() {
    for policy in [
        "musteragent.dialogue-discovery@1",
        "musteragent.dialogue-device-discovery@24",
        "musteragent.dialogue-report-discovery@23",
        "musteragent.legacy.agent-directed-discovery@8",
        "musteragent.agent-directed-discovery@9",
        "musteragent.agent-directed-discovery@10",
        "host.unknown@1",
    ] {
        assert!(!is_dialogue_family_discovery_policy(policy));
        assert!(!capability_available_for_dialogue_policy(
            policy,
            "system.time.current@1"
        ));
        assert!(!dialogue_capability_ceiling_for_policy(
            policy,
            "tasks.submit_result@1"
        ));
    }
}

#[test]
fn source_keys_preserve_retirement_version_and_unknown_tool_boundaries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/source-key-discovery.json"))
            .expect("source-key differential fixture");
    for case in fixture["cases"].as_array().expect("cases") {
        let key = case["key"].as_str().expect("key");
        for (role, policy) in fixture["policies"].as_object().expect("policies") {
            let policy = policy.as_str().expect("policy");
            let expected = |field: &str| {
                case[field]
                    .as_array()
                    .expect("roles")
                    .iter()
                    .any(|value| value.as_str() == Some(role))
            };
            assert_eq!(
                capability_available_for_dialogue_policy(policy, key),
                expected("allowed_roles"),
                "execution {policy} {key}"
            );
            assert_eq!(
                dialogue_capability_ceiling_for_policy(policy, key),
                expected("ceiling_roles"),
                "ceiling {policy} {key}"
            );
        }
    }
}
