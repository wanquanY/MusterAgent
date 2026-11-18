use super::*;
use musteragent_policy::Decision;
use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget, ProductExecutionTarget};
fn capability(
    definition: &musteragent_tool::BuiltinToolDefinition,
    decision: Decision,
) -> CapabilityEntitlement {
    CapabilityEntitlement {
        key: musteragent_capability::CapabilityKey::from_versioned_key(
            definition.capability_key.clone(),
        ),
        provider_id: match definition.target {
            CapabilityExecutionTarget::RustRuntime => musteragent_tool::BUILTIN_RUST_PROVIDER_ID,
            _ => musteragent_tool::BUILTIN_BACKEND_PROVIDER_ID,
        }
        .parse()
        .unwrap(),
        location: match definition.target {
            CapabilityExecutionTarget::RustRuntime => CapabilityLocation::Device,
            _ => CapabilityLocation::Backend,
        },
        effect: definition.effect,
        allowed_scopes: definition.required_scopes.clone(),
        policy_decision: decision,
    }
}
#[test]
fn frozen_native_ceiling_projects_only_exact_shared_provider_without_escalation() {
    let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
    let cloud =
        BuiltinToolCatalog::retained_product_definitions(&release, ProductExecutionTarget::Cloud)
            .unwrap();
    let device =
        BuiltinToolCatalog::retained_product_definitions(&release, ProductExecutionTarget::Device)
            .unwrap();
    let shared = cloud
        .iter()
        .find(|d| d.key.as_str() == "tasks.inspect@1")
        .unwrap();
    for decision in [Decision::Deny, Decision::RequireApproval, Decision::Allow] {
        let original = capability(shared, decision);
        let projected = project_cloud_ceiling(&release, std::slice::from_ref(&original)).unwrap();
        assert_eq!(projected.len(), 1);
        let expected = CapabilityEntitlement {
            location: CapabilityLocation::Cloud,
            ..original
        };
        assert_eq!(
            projected[0], expected,
            "only authenticated deployment placement changes"
        );
    }
    let local: Vec<_> = device
        .iter()
        .filter(|d| !cloud.iter().any(|c| c.capability_key == d.capability_key))
        .map(|d| capability(d, Decision::Allow))
        .collect();
    assert!(
        !local.is_empty(),
        "real immutable Device-only catalog must be exercised"
    );
    assert!(
        project_cloud_ceiling(&release, &local).is_err(),
        "local filesystem/terminal and all Device-only keys cannot delegate Cloud execution"
    );
    let mut source = vec![capability(shared, Decision::RequireApproval)];
    source.extend(local);
    let mut unknown = capability(shared, Decision::Allow);
    unknown.key = musteragent_capability::CapabilityKey::from_versioned_key(
        VersionedKey::new("unreviewed.native@1").unwrap(),
    );
    source.push(unknown);
    let values = project_cloud_ceiling(&release, &source).unwrap();
    assert_eq!(
        values.len(),
        1,
        "unknown, omitted and Device-only keys are not invented by placement"
    );
    assert_eq!(values[0].policy_decision, Decision::RequireApproval);
    let mut bad = capability(shared, Decision::Allow);
    bad.provider_id = CapabilityProviderId::generate();
    assert!(project_cloud_ceiling(&release, &[bad]).is_err());
    let wrong =
        ProductReleaseRef::new(ProductId::KERNEL, release.release_id.clone(), [99; 32]).unwrap();
    assert!(
        project_cloud_ceiling(&wrong, &source).is_err(),
        "never substitute current publication for an unknown frozen digest"
    );
}
