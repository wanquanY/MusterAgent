use musteragent_capability::{CapabilityLocation, CapabilityManifest};
use musteragent_identity::CapabilityProviderId;
use musteragent_tool::{BuiltinToolDefinition, CapabilityExecutionTarget};
use musteragent_tool_executor::builtin_capability_manifest_at_location;

pub fn capability_manifest(
    definition: &BuiltinToolDefinition,
    provider_id: CapabilityProviderId,
) -> Result<CapabilityManifest, musteragent_capability::CapabilityContractError> {
    let location = match definition.target {
        CapabilityExecutionTarget::RustRuntime => CapabilityLocation::Cloud,
        CapabilityExecutionTarget::BackendNamed(_) => CapabilityLocation::Backend,
    };
    builtin_capability_manifest_at_location(definition, provider_id, location)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_tool::{
        BUILTIN_BACKEND_PROVIDER_ID, BUILTIN_RUST_PROVIDER_ID, BuiltinToolCatalog,
    };

    #[test]
    fn cloud_composition_keeps_backend_placement_and_product_provider_identity() {
        let catalog = BuiltinToolCatalog::load_cloud().expect("Cloud release");
        for definition in catalog.definitions() {
            let (provider, location) = match definition.target {
                CapabilityExecutionTarget::RustRuntime => {
                    (BUILTIN_RUST_PROVIDER_ID, CapabilityLocation::Cloud)
                }
                CapabilityExecutionTarget::BackendNamed(_) => {
                    (BUILTIN_BACKEND_PROVIDER_ID, CapabilityLocation::Backend)
                }
            };
            let provider = provider.parse().expect("product identity");
            let manifest = capability_manifest(definition, provider).expect("manifest");
            assert_eq!(manifest.location, location);
            assert_eq!(manifest.provider_id, provider);
            assert_eq!(
                manifest,
                builtin_capability_manifest_at_location(definition, provider, location)
                    .expect("shared mapping")
            );
        }
    }
}
