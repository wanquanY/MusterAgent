use musteragent_cloud_worker::CloudWorkerSettings;
use std::collections::BTreeMap;

fn example() -> &'static str {
    include_str!("../../../docs/integration/cloud-worker.md")
        .split_once("```toml\n")
        .expect("documented TOML example")
        .1
        .split_once("\n```")
        .expect("complete TOML example")
        .0
}

#[test]
fn documented_configuration_has_explicit_environment_precedence_and_limits() {
    let env = BTreeMap::from([
        (
            "MUSTERAGENT_DATABASE_URL".into(),
            "postgres://fixture/kernel".into(),
        ),
        ("MUSTERAGENT_WORKER_ID".into(), "fixture-worker".into()),
    ]);
    let settings = CloudWorkerSettings::from_values(example(), &env).expect("documented config");
    assert_eq!(settings.database_url, "postgres://fixture/kernel");
    assert_eq!(
        settings.policy().expect("worker limits").worker_id.as_str(),
        "fixture-worker"
    );
    for replacement in ["concurrency = 0", "concurrency = 1001"] {
        assert!(
            CloudWorkerSettings::from_values(
                &example().replace("concurrency = 8", replacement),
                &env
            )
            .is_err()
        );
    }
    assert!(
        CloudWorkerSettings::from_values(
            &example().replace("heartbeat_ms = 5000", "heartbeat_ms = 30000"),
            &env
        )
        .is_err()
    );
    assert!(
        CloudWorkerSettings::from_values(
            &example().replace("/var/lib/kernel/extensions", "relative"),
            &env
        )
        .is_err()
    );
    assert!(
        CloudWorkerSettings::from_values(&format!("unknown_setting = true\n{}", example()), &env)
            .is_err()
    );
}

#[test]
fn each_host_protocol_uses_its_endpoint_and_insecure_remote_urls_are_rejected() {
    for (protocol, suffix) in [
        ("openai-chat", "chat/completions"),
        ("openai-responses", "responses"),
        ("anthropic-messages", "messages"),
    ] {
        let settings = CloudWorkerSettings::from_values(
            &example().replace("openai-chat", protocol),
            &BTreeMap::new(),
        )
        .expect("protocol fixture");
        assert_eq!(
            settings.models[0]
                .endpoint()
                .expect("supported endpoint")
                .invocation_url
                .as_str(),
            format!("https://models.example.test/v1/{suffix}")
        );
    }
    let settings = CloudWorkerSettings::from_values(
        &example().replace("https://models.example.test", "http://models.example.test"),
        &BTreeMap::new(),
    )
    .expect("route fixture");
    assert!(settings.models[0].endpoint().is_err());
    let settings = CloudWorkerSettings::from_values(
        &example().replace("openai-chat", "unknown"),
        &BTreeMap::new(),
    )
    .expect("route fixture");
    assert!(settings.models[0].endpoint().is_err());
}

#[test]
fn storage_publication_strategy_is_explicit_and_defaults_to_standard() {
    use musteragent_object_store::WriteOnceStrategy;
    let parse = |line: &str| {
        CloudWorkerSettings::from_values(
            &example().replace("[object_store]", &format!("[object_store]\n{line}")),
            &BTreeMap::new(),
        )
    };
    assert_eq!(
        parse("").expect("old settings").object_store.write_once,
        WriteOnceStrategy::Standard {}
    );
    assert_eq!(
        parse(r#"write_once = { strategy = "forbid-overwrite-header", header = "x-cos-forbid-overwrite" }"#)
            .expect("vendor settings").object_store.write_once,
        WriteOnceStrategy::ForbidOverwriteHeader { header: "x-cos-forbid-overwrite".into() }
    );
    for line in [
        r#"write_once = { strategy = "auto" }"#,
        r#"write_once = { strategy = "forbid-overwrite-header" }"#,
        r#"write_once = { strategy = "forbid-overwrite-header", header = "x-guard", value = "false" }"#,
        r#"write_once = { strategy = "standard", header = "x-guard" }"#,
    ] {
        assert!(parse(line).is_err(), "{line}");
    }
}
