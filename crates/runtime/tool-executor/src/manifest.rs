//! One immutable catalog-to-Capability mapping for Cloud, Desktop and Mobile.
//! Placement and provider selection belong to the native composition or sealed
//! authority; this pure conversion neither admits a Tool nor grants execution.
use musteragent_capability::{
    CapabilityContractError, CapabilityKey, CapabilityLocation, CapabilityManifest,
    IdempotencyContract,
};
use musteragent_identity::{CapabilityId, CapabilityProviderId, VersionedKey};
use musteragent_tool::{BuiltinToolDefinition, ToolRetryContract};
use uuid::Uuid;

// Historical product namespace. Changing it would change capability identities
// in signed grants and already sealed snapshots across every Runtime home.
const CAPABILITY_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x01,
]);

#[must_use]
pub fn builtin_capability_id(key: &VersionedKey) -> CapabilityId {
    CapabilityId::from_uuid(Uuid::new_v5(
        &CAPABILITY_ID_NAMESPACE,
        key.as_str().as_bytes(),
    ))
    .expect("UUIDv5 capability identity cannot be nil")
}

/// Preserve the catalog's policy/recovery fields exactly. The caller pins the
/// provider and location from its trusted composition or the original signed
/// invocation, not model arguments. Backend-vs-Runtime placement validation
/// remains at those callers; this also supports sealed hosted invocations.
pub fn builtin_capability_manifest_at_location(
    definition: &BuiltinToolDefinition,
    provider_id: CapabilityProviderId,
    location: CapabilityLocation,
) -> Result<CapabilityManifest, CapabilityContractError> {
    let idempotency = match definition.idempotency.retry_contract() {
        ToolRetryContract::ReadOnly => IdempotencyContract::ReadOnly,
        ToolRetryContract::IdempotentByKey => IdempotencyContract::IdempotentByKey,
        ToolRetryContract::QueryBeforeRetry => IdempotencyContract::QueryBeforeRetry,
        ToolRetryContract::NotReplayable => IdempotencyContract::NotReplayable,
    };
    let manifest = CapabilityManifest {
        capability_id: builtin_capability_id(&definition.capability_key),
        key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
        effect: definition.effect,
        approval: definition.approval,
        location,
        provider_id,
        timeout: definition.timeout,
        max_attempts: definition.max_attempts,
        idempotency,
        required_scopes: definition.required_scopes.clone(),
        required_contexts: definition.required_contexts.clone(),
        data_classification: definition.data_classification,
        egress_policy: definition.egress_policy,
    };
    manifest.validate()?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_tool::{BUILTIN_RUST_PROVIDER_ID, BuiltinToolCatalog};

    #[test]
    fn historical_namespace_and_versioned_identity_are_frozen() {
        let key = VersionedKey::new("web.search@2").expect("key");
        assert_eq!(
            builtin_capability_id(&key).to_string(),
            "c4b98e6e-0e30-56f9-b455-cb655c632fa5"
        );
        assert_ne!(
            builtin_capability_id(&key),
            builtin_capability_id(&VersionedKey::new("web.search@1").expect("predecessor"))
        );
    }

    #[test]
    fn every_catalog_field_survives_on_every_execution_location() {
        let provider = BUILTIN_RUST_PROVIDER_ID.parse().expect("provider");
        for catalog in [
            BuiltinToolCatalog::load_cloud(),
            BuiltinToolCatalog::load_device(),
        ] {
            let catalog = catalog.expect("released catalog");
            for definition in catalog.definitions() {
                let original = builtin_capability_manifest_at_location(
                    definition,
                    provider,
                    CapabilityLocation::Cloud,
                )
                .expect("Cloud manifest");
                for location in [
                    CapabilityLocation::Cloud,
                    CapabilityLocation::Device,
                    CapabilityLocation::Backend,
                    CapabilityLocation::RemoteDevice,
                    CapabilityLocation::Browser,
                ] {
                    let manifest =
                        builtin_capability_manifest_at_location(definition, provider, location)
                            .expect("manifest");
                    let mut expected = original.clone();
                    expected.location = location;
                    assert_eq!(manifest, expected);
                    assert_eq!(manifest.key.as_str(), definition.capability_key.as_str());
                    assert_eq!(manifest.provider_id, provider);
                    assert_eq!(manifest.effect, definition.effect);
                    assert_eq!(manifest.approval, definition.approval);
                    assert_eq!(manifest.timeout, definition.timeout);
                    assert_eq!(manifest.max_attempts, definition.max_attempts);
                    assert_eq!(manifest.required_scopes, definition.required_scopes);
                    assert_eq!(manifest.required_contexts, definition.required_contexts);
                    assert_eq!(manifest.data_classification, definition.data_classification);
                    assert_eq!(manifest.egress_policy, definition.egress_policy);
                    assert_eq!(
                        manifest.idempotency.after_unknown_outcome(),
                        definition
                            .idempotency
                            .retry_contract()
                            .after_unknown_outcome()
                    );
                }
            }
        }
    }

    #[test]
    fn malformed_policy_is_rejected_not_normalized() {
        let catalog = BuiltinToolCatalog::load_device().expect("catalog");
        let provider = BUILTIN_RUST_PROVIDER_ID.parse().expect("provider");
        let mut definition = catalog.definitions()[0].clone();
        definition.timeout = std::time::Duration::ZERO;
        assert_eq!(
            builtin_capability_manifest_at_location(
                &definition,
                provider,
                CapabilityLocation::Device
            ),
            Err(CapabilityContractError::ZeroTimeout)
        );
        definition.timeout = std::time::Duration::from_secs(30);
        definition.required_scopes.clear();
        assert_eq!(
            builtin_capability_manifest_at_location(
                &definition,
                provider,
                CapabilityLocation::Device
            ),
            Err(CapabilityContractError::MissingAuthorizationScope)
        );
    }
}
