use super::*;
use musteragent_identity::{CapabilityProviderId, ProductId};
use musteragent_tool::{
    BuiltinToolCatalog, CapabilityAssemblyPlan, CapabilityHandlerKind, HostProductRelease,
    MediaArtifactKind, ProductExecutionTarget, compiled_product_release,
    install_host_product_releases,
};

#[test]
fn host_publication_round_trips_its_exact_method_and_artifact_contract() {
    let kernel = compiled_product_release(ProductId::KERNEL);
    let raw: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../../../contracts/kernel-tools.json"))
            .unwrap();
    let mut tool = raw["products"]["musteragent"]["cloud"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["metadata"]["artifact_type"] == "image")
        .unwrap()
        .clone();
    tool["tool_key"] = "sample.illustrate@1".into();
    tool["capability_key"] = "sample.render@1".into();
    tool["target"]["binding"] = "Studio.Illustrate".into();
    let mut document = serde_json::json!({
        "schema_version":"musteragent.host-tool-release.v1", "product":"sample", "profile_id":"sample",
        "release_id":"sample.tools@1", "cloud_head":"cloud", "device_head":"device",
        "skill_schema": "musteragent.runtime.skill-release.v1",
        "skill_release":{"product":kernel.product.as_str(), "release_id":kernel.release_id.as_str(), "digest":kernel.digest},
        "cloud":[tool], "device":[tool]
    });
    let read =
        |v: &serde_json::Value| HostProductRelease::from_json(&serde_json::to_vec(v).unwrap());
    let publication = read(&document).unwrap();
    let reference = publication.reference().clone();
    install_host_product_releases(vec![publication]).unwrap();
    let catalog =
        BuiltinToolCatalog::for_product_release(&reference, ProductExecutionTarget::Device)
            .unwrap();
    let plan = CapabilityAssemblyPlan::from_catalog(&catalog).unwrap();
    let def = plan
        .definitions(CapabilityHandlerKind::BackendMedia)
        .next()
        .unwrap();
    assert_eq!(def.target.media_artifact(), Some(MediaArtifactKind::Image));

    let snap = ToolDefinitionSnapshot {
        manifest: def.pin(CapabilityProviderId::generate()),
        title: def.title.clone(),
        description: def.description.clone(),
        execution_target: def.target.clone(),
        concurrency_class: def.concurrency,
        idempotency: def.idempotency,
        file_operation: def.file_operation.clone(),
        input_schema_json: def.input_schema_json.clone(),
        output_schema_json: def.output_schema_json.clone(),
    };
    let stored = tool_manifest(&snap);
    assert_eq!(stored.execution_target, "Studio.Illustrate");
    assert_eq!(stored.backend_artifact_type.as_deref(), Some("image"));
    let restored =
        decode_tool(wire::ToolManifestSnapshot::decode(stored.encode_to_vec().as_slice()).unwrap())
            .unwrap();
    restored.validate().unwrap();
    assert_eq!(restored, snap);
    assert!(restored.matches_execution_contract(def));
    for (name, artifact_type) in [
        ("Studio.Other", Some("image")),
        ("Studio.Illustrate", Some("video")),
        ("Studio.Illustrate", None),
    ] {
        let mut forged = stored.clone();
        forged.execution_target = name.into();
        forged.backend_artifact_type = artifact_type.map(str::to_owned);
        assert!(!decode_tool(forged).unwrap().matches_execution_contract(def));
    }
    document["cloud"][0]["target"]["binding"] = "Studio.ExecuteTool".into();
    assert!(read(&document).is_err());
    document["cloud"][0]["target"]["binding"] = "Studio.Illustrate".into();
    document["cloud"][0]["metadata"]["artifact_type"] = "other".into();
    assert!(read(&document).is_err());
    document["cloud"][0]["metadata"]["artifact_type"] = "image".into();
    document["cloud"][0]["effect"] = "read".into();
    document["cloud"][0]["idempotency"] = "native".into();
    assert!(read(&document).is_err());
}
