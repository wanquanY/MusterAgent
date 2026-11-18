use super::*;
use crate::workspace::{WorkspaceToolRelease, WorkspaceToolSelection, parse_release};
use musteragent_content_ref::Sha256;
fn source() -> serde_json::Value {
    let bundle: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../../applications/reference-api/examples/workspace-releases.json"
    ))
    .unwrap();
    bundle["releases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|release| release["plugin_id"] == "workspace.example-clock")
        .unwrap()
        .clone()
}
fn parse(value: &serde_json::Value) -> Result<WorkspaceToolRelease, Error> {
    let bytes = serde_json::to_vec(value).unwrap();
    parse_release(&bytes, &Sha256::digest(&bytes).into_bytes())
}
fn selected(value: &serde_json::Value) -> BuiltinToolDefinition {
    let release = parse(value).unwrap();
    WorkspaceToolSelection {
        plugin_id: release.plugin_id.clone(),
        version: release.version.clone(),
        release_sha256: release.release_sha256.clone(),
        operation_ref: release.tools[0].key.clone(),
        selection_sha256: "c".repeat(64),
    }
    .definition_from_release(&release)
    .unwrap()
}
#[test]
fn only_explicit_role_changes_execution_authority_not_legacy_provides() {
    let mut value = source();
    let current = selected(&value);
    assert_eq!(current.capability_key.as_str(), "core.time.read@1");
    assert_eq!(
        current
            .required_scopes
            .iter()
            .map(DottedKey::as_str)
            .collect::<Vec<_>>(),
        vec!["system.time.read"]
    );
    assert_eq!(
        current.approval,
        musteragent_policy::ApprovalRequirement::Never
    );
    assert_eq!(
        current.egress_policy,
        musteragent_policy::EgressPolicy::None
    );
    value["tools"][0]
        .as_object_mut()
        .unwrap()
        .remove("authority_role");
    let legacy = selected(&value);
    assert_eq!(legacy.capability_key.as_str(), "account.application.read@1");
    assert_eq!(
        legacy.application.as_ref().unwrap().provides.as_deref(),
        Some("core.time.read@1")
    );
    let bytes = legacy.application.unwrap().encode();
    assert!(
        !String::from_utf8(bytes.clone())
            .unwrap()
            .contains("authority_role")
    );
    assert!(
        crate::ApplicationToolBinding::decode(&bytes)
            .unwrap()
            .unwrap()
            .authority_role
            .is_none()
    );
}
#[test]
fn role_contract_is_closed_and_cannot_weaken_transport_or_approval() {
    for (field, value) in [
        ("schema_version", serde_json::json!("future.v2")),
        ("capability_key", serde_json::json!("core.memory.read@1")),
        ("effect", serde_json::json!("write_external")),
        ("required_scopes", serde_json::json!([])),
        ("required_scopes", serde_json::json!(["task:read"])),
        (
            "required_scopes",
            serde_json::json!(["system.time.read", "system.time.read"]),
        ),
        ("required_contexts", serde_json::json!(["unreviewed"])),
        ("unknown", serde_json::json!(true)),
    ] {
        let mut document = source();
        document["tools"][0]["authority_role"][field] = value;
        assert!(parse(&document).is_err(), "{field}");
    }
    let mut mixed = source();
    mixed["tools"][0]["runtime_definition"] = serde_json::json!({});
    assert!(parse(&mixed).is_err());
}
#[test]
fn selected_role_is_frozen_and_reconstructs_without_current_source_lookup() {
    let value = source();
    let original = selected(&value);
    let mut binding = original.application.clone().unwrap();
    binding.authority_witness = Some("{}".into());
    let encoded = binding.encode();
    let retained = crate::ApplicationToolBinding::decode(&encoded)
        .unwrap()
        .unwrap();
    let mut expected = original.clone();
    expected.application = Some(binding);
    let rebuilt = retained
        .workspace_definition(
            &original.title,
            &original.description,
            &original.input_schema_json,
        )
        .unwrap();
    assert_eq!(rebuilt, expected);
    let provider = crate::BUILTIN_RUST_PROVIDER_ID.parse().unwrap();
    let mut manifest = rebuilt.pin(provider);
    assert!(manifest.validate().is_ok());
    manifest.required_scopes =
        BTreeSet::from([DottedKey::new("account.application.read").unwrap()]);
    assert!(manifest.validate().is_err());
    let mut next = value;
    next["version"] = serde_json::json!("2.0.0");
    next["tools"][0]["authority_role"]["approval"] = serde_json::json!("policy");
    assert_ne!(selected(&next).approval, rebuilt.approval);
    assert_eq!(retained.encode(), encoded);
}
