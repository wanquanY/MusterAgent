use std::collections::BTreeSet;

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_identity::{CapabilityProviderId, DottedKey};
use musteragent_policy::{Decision, Effect};
use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget};

use super::{
    SnapshotContractError, ToolDefinitionSnapshot, authorized_tool_catalog,
    execution_target_accepts_location,
};

fn tool(capability: &str) -> ToolDefinitionSnapshot {
    let provider_id = CapabilityProviderId::generate();
    let definition = BuiltinToolCatalog::load_cloud()
        .expect("built-in Tool catalog")
        .definitions()
        .iter()
        .find(|definition| definition.capability_key.as_str() == capability)
        .expect("Tool definition")
        .clone();
    ToolDefinitionSnapshot {
        manifest: definition.pin(provider_id),
        execution_target: definition.target,
        concurrency_class: definition.concurrency,
        idempotency: definition.idempotency,
        file_operation: definition.file_operation,
        title: definition.title,
        description: definition.description,
        tags: Vec::new(),
        when_to_use: None,
        input_schema_json: definition.input_schema_json,
        output_schema_json: definition.output_schema_json,
    }
}

fn grant(tool: &ToolDefinitionSnapshot, location: CapabilityLocation) -> CapabilityGrantSnapshot {
    CapabilityGrantSnapshot {
        key: CapabilityKey::from_versioned_key(tool.manifest.capability_key.clone()),
        provider_id: tool.manifest.capability_provider_id,
        location,
        effect: tool.manifest.effect,
        allowed_scopes: tool.manifest.required_scopes.clone(),
        policy_decision: Decision::Allow,
    }
}

#[test]
fn descriptor_identity_and_hash_use_the_exact_grant_location() {
    let runtime_tool = tool("tool.catalog.search@2");
    assert_eq!(
        runtime_tool.execution_target,
        CapabilityExecutionTarget::RustRuntime
    );
    let cloud = authorized_tool_catalog(
        std::slice::from_ref(&runtime_tool),
        std::slice::from_ref(&grant(&runtime_tool, CapabilityLocation::Cloud)),
    )
    .expect("Cloud grant");
    let device = authorized_tool_catalog(
        std::slice::from_ref(&runtime_tool),
        std::slice::from_ref(&grant(&runtime_tool, CapabilityLocation::Device)),
    )
    .expect("Device grant");
    assert_eq!(cloud.definitions()[0].location, CapabilityLocation::Cloud);
    assert_eq!(device.definitions()[0].location, CapabilityLocation::Device);
    assert_eq!(
        device.definitions()[0].provider_id,
        runtime_tool.manifest.capability_provider_id
    );
    assert_ne!(cloud.catalog_hash(), device.catalog_hash());
    let mut broader_cloud_grant = grant(&runtime_tool, CapabilityLocation::Cloud);
    broader_cloud_grant
        .allowed_scopes
        .insert(DottedKey::new("additional.scope").expect("scope"));
    authorized_tool_catalog(
        std::slice::from_ref(&runtime_tool),
        std::slice::from_ref(&broader_cloud_grant),
    )
    .expect("a grant may authorize more than one Tool requires");

    let backend_tool = tool("web.fetch@2");
    assert!(matches!(
        backend_tool.execution_target,
        CapabilityExecutionTarget::BackendNamed(_)
    ));
    let backend = authorized_tool_catalog(
        std::slice::from_ref(&backend_tool),
        std::slice::from_ref(&grant(&backend_tool, CapabilityLocation::Backend)),
    )
    .expect("Backend grant");
    assert_eq!(
        backend.definitions()[0].location,
        CapabilityLocation::Backend
    );
    assert_eq!(backend.definitions()[0].tool_ref.as_str(), "web.fetch@1");
    assert_eq!(
        backend.definitions()[0].capability_ref.as_str(),
        "web.fetch@2"
    );
}

#[test]
fn invalid_or_ambiguous_grants_fail_closed() {
    let tool = tool("tool.catalog.search@2");
    let valid = grant(&tool, CapabilityLocation::Cloud);
    let mismatched_scopes = BTreeSet::from([DottedKey::new("unexpected.scope").expect("scope")]);
    let cases = [
        (Vec::new(), SnapshotContractError::ToolCapabilityNotGranted),
        (
            vec![valid.clone(), valid.clone()],
            SnapshotContractError::DuplicateCapability,
        ),
        (
            vec![CapabilityGrantSnapshot {
                provider_id: CapabilityProviderId::generate(),
                ..valid.clone()
            }],
            SnapshotContractError::ToolCapabilityNotGranted,
        ),
        (
            vec![CapabilityGrantSnapshot {
                effect: Effect::WriteReversible,
                ..valid.clone()
            }],
            SnapshotContractError::ToolCapabilityEffectMismatch,
        ),
        (
            vec![CapabilityGrantSnapshot {
                allowed_scopes: mismatched_scopes,
                ..valid.clone()
            }],
            SnapshotContractError::ToolCapabilityScopeMismatch,
        ),
        (
            vec![CapabilityGrantSnapshot {
                allowed_scopes: BTreeSet::new(),
                ..valid.clone()
            }],
            SnapshotContractError::CapabilitiesNotCanonical,
        ),
        (
            vec![CapabilityGrantSnapshot {
                policy_decision: Decision::Deny,
                ..valid.clone()
            }],
            SnapshotContractError::CapabilitiesNotCanonical,
        ),
        (
            vec![CapabilityGrantSnapshot {
                location: CapabilityLocation::Backend,
                ..valid.clone()
            }],
            SnapshotContractError::ToolCapabilityLocationMismatch,
        ),
        (
            vec![CapabilityGrantSnapshot {
                location: CapabilityLocation::RemoteDevice,
                ..valid
            }],
            SnapshotContractError::ToolCapabilityLocationMismatch,
        ),
    ];
    for (grants, expected) in cases {
        let actual = authorized_tool_catalog(std::slice::from_ref(&tool), &grants)
            .expect_err("invalid grant must fail closed");
        assert_eq!(
            std::mem::discriminant(&actual),
            std::mem::discriminant(&expected)
        );
    }
}

#[test]
fn execution_target_location_matrix_is_closed_and_shared() {
    let runtime = CapabilityExecutionTarget::RustRuntime;
    let backend = tool("web.fetch@2").execution_target;

    assert!(execution_target_accepts_location(
        &runtime,
        CapabilityLocation::Cloud
    ));
    assert!(execution_target_accepts_location(
        &runtime,
        CapabilityLocation::Device
    ));
    assert!(!execution_target_accepts_location(
        &runtime,
        CapabilityLocation::Backend
    ));
    assert!(!execution_target_accepts_location(
        &runtime,
        CapabilityLocation::RemoteDevice
    ));
    assert!(execution_target_accepts_location(
        &backend,
        CapabilityLocation::Backend
    ));
    assert!(!execution_target_accepts_location(
        &backend,
        CapabilityLocation::Cloud
    ));
    assert!(!execution_target_accepts_location(
        &backend,
        CapabilityLocation::Device
    ));
    assert!(!execution_target_accepts_location(
        &backend,
        CapabilityLocation::RemoteDevice
    ));
}
