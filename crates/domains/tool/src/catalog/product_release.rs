//! Exact product selection supplied by verified admission, never process state.
use super::*;
use musteragent_identity::{ProductId, ProductReleaseRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProductExecutionTarget {
    Cloud,
    Device,
}

pub(super) struct CompiledProductRelease {
    pub product: ProductId,
    pub release_id: &'static str,
    pub digest: [u8; 32],
    pub active: bool,
    pub profile_id: &'static str,
    pub cloud_head: &'static str,
    pub device_head: &'static str,
    pub skill_schema: &'static str,
    pub cloud: fn() -> Result<BuiltinToolCatalog, ToolCatalogError>,
    pub device: fn() -> Result<BuiltinToolCatalog, ToolCatalogError>,
}

impl CompiledProductRelease {
    fn reference(&self) -> ProductReleaseRef {
        ProductReleaseRef::new(
            self.product,
            VersionedKey::new(self.release_id).expect("compiled release"),
            self.digest,
        )
        .expect("compiled digest")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProductReleaseMetadata {
    pub profile_id: &'static str,
    pub cloud_head: &'static str,
    pub device_head: &'static str,
    pub skill_schema: &'static str,
}

fn resolve(
    reference: &ProductReleaseRef,
) -> Result<&'static CompiledProductRelease, ToolCatalogError> {
    compiled_product_releases()
        .iter()
        .find(|r| {
            r.product == reference.product
                && r.release_id == reference.release_id.as_str()
                && r.digest == reference.digest
        })
        .ok_or(ToolCatalogError::UnavailableProductRelease)
}

pub fn product_release_metadata(
    reference: &ProductReleaseRef,
) -> Result<ProductReleaseMetadata, ToolCatalogError> {
    if let Some(r) = host_release(reference) {
        return Ok(ProductReleaseMetadata {
            profile_id: &r.profile_id,
            cloud_head: &r.cloud_head,
            device_head: &r.device_head,
            skill_schema: &r.skill_schema,
        });
    }
    let r = resolve(reference)?;
    Ok(ProductReleaseMetadata {
        profile_id: r.profile_id,
        cloud_head: r.cloud_head,
        device_head: r.device_head,
        skill_schema: r.skill_schema,
    })
}

/// Select the result protocol only from the complete, sealed publication at
/// both execution homes. A partial pair or an unknown release is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskResultProtocol {
    Public,
    Verified,
    Requester,
}

pub fn task_result_protocol(
    release: &ProductReleaseRef,
) -> Result<TaskResultProtocol, ToolCatalogError> {
    let at = |target| {
        let defs = BuiltinToolCatalog::retained_product_definitions(release, target)?;
        let has = |key: &str| defs.iter().any(|d| d.key.as_str() == key);
        match (
            has("tasks.submit_result@1"),
            has("tasks.verify@3"),
            has("tasks.feedback@1"),
        ) {
            (true, true, _) => Ok(TaskResultProtocol::Verified),
            (true, false, true) => Ok(TaskResultProtocol::Requester),
            (false, false, false) => Ok(TaskResultProtocol::Public),
            _ => Err(ToolCatalogError::UnavailableProductRelease),
        }
    };
    let cloud = at(ProductExecutionTarget::Cloud)?;
    if cloud == at(ProductExecutionTarget::Device)? {
        Ok(cloud)
    } else {
        Err(ToolCatalogError::UnavailableProductRelease)
    }
}

pub fn verified_task_results_for_release(
    release: &ProductReleaseRef,
) -> Result<bool, ToolCatalogError> {
    Ok(task_result_protocol(release)? != TaskResultProtocol::Public)
}

/// Compiled execution publications. Sealed history readers do not require
/// their publication to be compiled into this build.
pub fn compiled_product_releases_refs() -> Vec<ProductReleaseRef> {
    compiled_product_releases()
        .iter()
        .map(|r| r.reference())
        .collect()
}

/// Shared admission gate, including private evaluations and explicit retries.
pub fn require_product_release_admission(
    reference: &ProductReleaseRef,
) -> Result<(), ToolCatalogError> {
    if host_release(reference).is_some() {
        Ok(())
    } else {
        resolve(reference).map(|_| ())
    }
}

pub fn compiled_product_release(product: ProductId) -> ProductReleaseRef {
    compiled_product_releases()
        .iter()
        .find(|r| r.product == product && r.active)
        .expect("active product release")
        .reference()
}

impl BuiltinToolCatalog {
    /// Contracts a Snapshot admitted under this publication may carry.
    pub fn retained_product_definitions(
        release: &ProductReleaseRef,
        target: ProductExecutionTarget,
    ) -> Result<std::sync::Arc<[BuiltinToolDefinition]>, ToolCatalogError> {
        Ok(Self::for_product_release(release, target)?.ordered)
    }

    /// Resolve a frozen wire contract through its publication. An absent
    /// publication is only for versioned legacy readers whose owner proves the
    /// product independently; it never means the active release.
    pub fn resolve_historical_contract(
        release: Option<&ProductReleaseRef>,
        product: ProductId,
        target: ProductExecutionTarget,
        key: &ToolKey,
        input: &SchemaRef,
        output: &SchemaRef,
    ) -> Result<BuiltinToolDefinition, ToolCatalogError> {
        let references = match release {
            Some(reference) if reference.product == product => vec![reference.clone()],
            Some(_) => return Err(ToolCatalogError::UnavailableProductRelease),
            None => compiled_product_releases_refs()
                .into_iter()
                .filter(|r| r.product == product)
                .collect(),
        };
        let mut selected: Option<BuiltinToolDefinition> = None;
        for reference in references {
            for definition in Self::retained_product_definitions(&reference, target)?
                .iter()
                .filter(|d| d.key == *key && d.input_schema == *input && d.output_schema == *output)
            {
                if let Some(original) = &selected {
                    let mut execution = definition.clone();
                    execution.title.clone_from(&original.title);
                    execution.description.clone_from(&original.description);
                    if execution != *original {
                        return Err(ToolCatalogError::UnavailableProductRelease);
                    }
                } else {
                    selected = Some(definition.clone());
                }
            }
        }
        selected.ok_or(ToolCatalogError::UnavailableProductRelease)
    }

    pub fn for_product_release(
        release: &ProductReleaseRef,
        target: ProductExecutionTarget,
    ) -> Result<Self, ToolCatalogError> {
        if let Some(host) = host_release(release) {
            return Ok(match target {
                ProductExecutionTarget::Cloud => host.cloud.clone(),
                ProductExecutionTarget::Device => host.device.clone(),
            });
        }
        let release = resolve(release)?;
        match target {
            ProductExecutionTarget::Cloud => (release.cloud)(),
            ProductExecutionTarget::Device => (release.device)(),
        }
    }
}

mod host;
use host::host_release;
pub use host::{
    HostProductRelease, HostPublicationError, install_host_product_releases, product_skill_source,
};
