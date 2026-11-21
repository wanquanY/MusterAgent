//! New publication bindings qualify original materials; legacy bytes stay untouched.
use super::*;

impl RuntimeSnapshotContent {
    /// A new explicit retry can qualify legacy material under a reviewed
    /// publication, without changing the source Snapshot or its old signature.
    /// The command owner supplies the product; no active pointer is consulted.
    pub fn qualify_legacy_retry(
        &self,
        reference: &musteragent_identity::ProductReleaseRef,
        owner: musteragent_identity::ProductId,
    ) -> Result<Self, SnapshotContractError> {
        if self.product_release.is_some() || reference.product != owner {
            return Err(SnapshotContractError::ProductNamespaceMismatch);
        }
        let dialogue = self.discovery.as_ref().is_some_and(|d| {
            crate::is_dialogue_family_discovery_policy(d.discovery_policy_ref.as_str())
        });
        if dialogue && owner != musteragent_identity::ProductId::KERNEL {
            return Err(SnapshotContractError::ProductNamespaceMismatch);
        }
        let mut qualified = self.clone();
        qualified.product_release = Some(reference.clone());
        // Validate all original material under the caller-supplied historical
        // product binding. The process default may belong to another product;
        // it cannot validate a pre-binding Snapshot before qualification.
        qualified.validate()?;
        Ok(qualified)
    }

    pub(super) fn validate_product_release(&self) -> Result<(), SnapshotContractError> {
        let Some(reference) = &self.product_release else {
            return Ok(());
        };
        let invalid = || SnapshotContractError::ProductNamespaceMismatch;
        musteragent_tool::product_release_metadata(reference).map_err(|_| invalid())?;
        for tool in &self.tools {
            let target = if self.capabilities.iter().any(|grant| {
                grant.key.as_str() == tool.manifest.capability_key.as_str()
                    && grant.location == musteragent_capability::CapabilityLocation::Device
            }) {
                musteragent_tool::ProductExecutionTarget::Device
            } else {
                musteragent_tool::ProductExecutionTarget::Cloud
            };
            if let Some(application) = &tool.manifest.application {
                application.validate().map_err(|_| invalid())?;
                // The current installation owner supplied this exact operation.
                // Only the finite Memory profile may retain a current key with
                // its unchanged kernel contract and extra Source provenance.
                let kernel =
                    musteragent_tool::BuiltinToolCatalog::for_product_release(reference, target)
                        .map_err(|_| invalid())?;
                if let Some(original) = kernel.by_key(&tool.manifest.key)
                    && !tool.preserves_memory_kernel_contract(original)
                {
                    return Err(invalid());
                }
                continue;
            }
            let original =
                musteragent_tool::BuiltinToolCatalog::for_product_release(reference, target)
                    .map_err(|_| invalid())?;
            let declared = original.by_key(&tool.manifest.key);
            if declared.is_some_and(|def| tool.matches_execution_contract(def)) {
                continue;
            }
            if tool.execution_target.backend_operation().is_some() {
                // Backend location identifies the RPC provider, independently
                // of the worker home. Admission and physical registration own
                // the home-specific catalog. Here we verify release membership,
                // or admit a host-dispatched capability when its provider is
                // non-builtin and its key collides with no compiled release.
                let alternate = match target {
                    musteragent_tool::ProductExecutionTarget::Cloud => {
                        musteragent_tool::ProductExecutionTarget::Device
                    }
                    musteragent_tool::ProductExecutionTarget::Device => {
                        musteragent_tool::ProductExecutionTarget::Cloud
                    }
                };
                let alternate_catalog =
                    musteragent_tool::BuiltinToolCatalog::for_product_release(reference, alternate)
                        .map_err(|_| invalid())?;
                let alternate_declared = alternate_catalog.by_key(&tool.manifest.key);
                if alternate_declared.is_some_and(|def| tool.matches_execution_contract(def)) {
                    continue;
                }
                if declared.is_some() || alternate_declared.is_some() {
                    return Err(invalid());
                }
                if is_builtin_provider_id(tool.manifest.capability_provider_id) {
                    return Err(invalid());
                }
                for other in musteragent_tool::compiled_product_releases_refs() {
                    for compiled_target in [
                        musteragent_tool::ProductExecutionTarget::Cloud,
                        musteragent_tool::ProductExecutionTarget::Device,
                    ] {
                        let history = musteragent_tool::BuiltinToolCatalog::for_product_release(
                            &other,
                            compiled_target,
                        )
                        .map_err(|_| invalid())?;
                        if history.by_key(&tool.manifest.key).is_some() {
                            return Err(invalid());
                        }
                    }
                }
                continue;
            }
            if declared.is_some() {
                return Err(invalid());
            }
            // Installed capabilities retain their existing authority. Only
            // globally known builtin keys belong to this publication registry.
            for other in musteragent_tool::compiled_product_releases_refs() {
                let history =
                    musteragent_tool::BuiltinToolCatalog::for_product_release(&other, target)
                        .map_err(|_| invalid())?;
                if history.by_key(&tool.manifest.key).is_some() {
                    return Err(invalid());
                }
            }
        }
        // Skill identity/content is bound by validate_skill_discovery_catalog:
        // exact descriptor equality + sealed catalog digest. Requiring today's
        // compiled Skill registry here would invalidate an unchanged old Run.
        Ok(())
    }
}

impl ToolDefinitionSnapshot {
    /// The finite Memory migration adds signed implementation provenance to an
    /// unchanged public contract. It cannot replace kernel authority or use a
    /// model-facing projection to change that contract. Source review and current
    /// consent are still required by admission and execution respectively.
    pub fn preserves_memory_kernel_contract(
        &self,
        original: &musteragent_tool::BuiltinToolDefinition,
    ) -> bool {
        use musteragent_tool::ApplicationImplementation;
        let Some(binding) = &self.manifest.application else {
            return false;
        };
        let write = matches!(
            original.key.as_str(),
            "user_memory.remember@2"
                | "experience.propose@2"
                | "experience.propose@3"
                | "experience.publish@1"
        );
        let read = matches!(
            original.key.as_str(),
            "experience.search@1"
                | "experience.inspect@1"
                | "memory.search@1"
                | "memory.list@1"
                | "memory.get@1"
        );
        if (!write && !read)
            || binding.provides.as_deref() != Some(original.capability_key.as_str())
            || binding.authority_role.is_some()
            || (write
                && !matches!(
                    binding.implementation,
                    Some(
                        ApplicationImplementation::MemoryPlansV1 { .. }
                            | ApplicationImplementation::MemoryPlansV2 { .. }
                    )
                ))
            || (read
                && !matches!(
                    (&binding.implementation, original.key.as_str()),
                    (None, _)
                        | (Some(ApplicationImplementation::MemoryReadV1 { .. }), _)
                        | (
                            Some(ApplicationImplementation::MemoryRankingV1 { .. }),
                            "memory.search@1" | "memory.list@1" | "memory.get@1",
                        )
                ))
        {
            return false;
        }
        let Some(raw) = &binding.runtime_definition else {
            return false;
        };
        let Ok(released) =
            musteragent_tool::BuiltinToolDefinition::from_released_document(raw.clone())
        else {
            return false;
        };
        if &released != original {
            return false;
        }
        let mut unbound = self.clone();
        unbound.manifest.application = None;
        // Discovery may prefix the installed application label in guidance;
        // executable schema/authority and the complete released contract stay exact.
        unbound.matches_execution_contract(original)
    }

    /// Compare execution ownership and contracts while retaining the Run's
    /// original timing and model guidance. Media input projection is owned by
    /// the existing Backend media admission authority.
    pub fn matches_execution_contract(
        &self,
        definition: &musteragent_tool::BuiltinToolDefinition,
    ) -> bool {
        // Exact key identity is part of the full contract equality below.
        // Reject unrelated historical definitions before constructing their
        // manifest and liveness policy; same-key variants still need every
        // original execution/schema comparison.
        if definition.key != self.manifest.key {
            return false;
        }
        let media_projection = definition.target.media_artifact().is_some();
        let mut expected = definition.pin(self.manifest.capability_provider_id);
        expected.execution_policy = self.manifest.execution_policy.clone();
        if media_projection {
            expected.input_schema = self.manifest.input_schema.clone();
        }
        expected == self.manifest
            && definition.target == self.execution_target
            && definition.concurrency == self.concurrency_class
            && definition.idempotency == self.idempotency
            && definition.file_operation == self.file_operation
            && (media_projection || definition.input_schema_json == self.input_schema_json)
            && definition.output_schema_json == self.output_schema_json
    }
}

fn is_builtin_provider_id(id: musteragent_identity::CapabilityProviderId) -> bool {
    let rust = musteragent_tool::BUILTIN_RUST_PROVIDER_ID
        .parse::<musteragent_identity::CapabilityProviderId>()
        .expect("compiled rust provider id");
    let backend = musteragent_tool::BUILTIN_BACKEND_PROVIDER_ID
        .parse::<musteragent_identity::CapabilityProviderId>()
        .expect("compiled backend provider id");
    id == rust || id == backend
}

#[cfg(test)]
#[path = "product_release_tests.rs"]
mod tests;
