use super::{
    ACTIVE_CLOUD_EXPECTED_TOOL_COUNT, ACTIVE_DEVICE_EXPECTED_TOOL_COUNT, BuiltinToolCatalog,
    BuiltinToolDefinition,
};
use musteragent_identity::ProductId;

#[test]
fn provider_identities_are_generated_from_the_authoritative_release() {
    let release: serde_json::Value =
        serde_json::from_str(include_str!("../../../../contracts/kernel-tools.json"))
            .expect("canonical Capability Release");
    for (field, compiled) in [
        ("rust_provider_id", super::BUILTIN_RUST_PROVIDER_ID),
        ("backend_provider_id", super::BUILTIN_BACKEND_PROVIDER_ID),
    ] {
        assert_eq!(release["authority"][field].as_str(), Some(compiled));
        compiled
            .parse::<musteragent_identity::CapabilityProviderId>()
            .expect("canonical Provider identity");
    }
    assert_ne!(
        super::BUILTIN_RUST_PROVIDER_ID,
        super::BUILTIN_BACKEND_PROVIDER_ID
    );
}

#[test]
fn the_kernel_catalog_is_complete_on_both_execution_homes() {
    let cloud = BuiltinToolCatalog::load_cloud().expect("active Cloud catalog");
    let device = BuiltinToolCatalog::load_device().expect("active Device catalog");
    assert_eq!(cloud.definitions().len(), ACTIVE_CLOUD_EXPECTED_TOOL_COUNT);
    assert_eq!(
        device.definitions().len(),
        ACTIVE_DEVICE_EXPECTED_TOOL_COUNT
    );
    let keys = |definitions: &[BuiltinToolDefinition]| {
        definitions
            .iter()
            .map(|tool| tool.key.logical_key().to_owned())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let cloud_keys = keys(cloud.definitions());
    let device_keys = keys(device.definitions());
    for shared in [
        "web.search",
        "memory.search",
        "skills.view",
        "tool.catalog.search",
    ] {
        assert!(cloud_keys.contains(shared), "{shared} missing in Cloud");
        assert!(device_keys.contains(shared), "{shared} missing on Device");
    }
    assert!(device_keys.contains("device.terminal.exec"));
    assert!(!cloud_keys.iter().any(|key| key.starts_with("device.")));
}

#[test]
fn the_single_kernel_release_is_exact() {
    let reference = crate::compiled_product_release(ProductId::KERNEL);
    assert_eq!(reference.product, ProductId::KERNEL);
    assert!(crate::require_product_release_admission(&reference).is_ok());
    let mut forged = reference.clone();
    forged.digest[0] ^= 1;
    assert!(crate::require_product_release_admission(&forged).is_err());
    assert_eq!(crate::compiled_product_releases_refs(), vec![reference]);
}

#[test]
fn every_kernel_tool_has_an_explicit_validated_sandbox_declaration() {
    use musteragent_sandbox::SandboxCompatibility;
    let document: serde_json::Value =
        serde_json::from_str(include_str!("../../../../contracts/kernel-tools.json"))
            .expect("catalog JSON");
    for (deployment, catalog) in [
        ("cloud", BuiltinToolCatalog::load_cloud().expect("cloud")),
        ("device", BuiltinToolCatalog::load_device().expect("device")),
    ] {
        for raw in document["products"]["musteragent"][deployment]["tools"]
            .as_array()
            .expect("tools")
        {
            let definition =
                BuiltinToolDefinition::from_released_document(raw.clone()).expect("declared tool");
            assert_eq!(
                catalog
                    .by_key(&definition.key)
                    .expect("compiled definition")
                    .sandbox_compatibility,
                definition.sandbox_compatibility
            );
            assert_eq!(
                definition.sandbox_compatibility == SandboxCompatibility::BrowserCompatible,
                matches!(
                    definition.key.as_str(),
                    "data.python.run@11" | "system.time.current@1"
                )
            );
            let mut missing = raw.clone();
            missing["metadata"]
                .as_object_mut()
                .expect("metadata")
                .remove("sandbox_compatibility");
            assert!(BuiltinToolDefinition::from_released_document(missing).is_err());
            let mut invalid = raw.clone();
            invalid["metadata"]["sandbox_compatibility"] = serde_json::json!("automatic");
            assert!(BuiltinToolDefinition::from_released_document(invalid).is_err());
        }
    }
}
